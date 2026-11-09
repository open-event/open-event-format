
use crate::allocation::{counting_enabled, measure as measure_allocations, resident_high_water_bytes};
use crate::api::FormatUnderTest;
use crate::dataset::generate;
use crate::model::*;
use crate::oe_format::OeFormat;
use crate::parquet::ParquetFormat;
use crate::vortex::VortexFormat;
use anyhow::{Context, Result, anyhow, bail};
use std::fmt::Write as _;
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkSelection {
    pub check_answers: bool,
    pub formats: Vec<BenchmarkFormat>,
    pub operations: Vec<Operation>,
    pub phase_timings: bool,
}

impl Default for BenchmarkSelection {
    fn default() -> Self {
        Self {
            check_answers: true,
            formats: vec![BenchmarkFormat::Oe, BenchmarkFormat::Parquet, BenchmarkFormat::Vortex],
            operations: OPERATIONS.to_vec(),
            phase_timings: false,
        }
    }
}

pub fn compare(spec: &BenchmarkSpec, dir: &Path) -> Result<BenchmarkReport> {
    compare_selected(spec, dir, &BenchmarkSelection::default())
}

pub fn compare_selected(spec: &BenchmarkSpec, dir: &Path, selection: &BenchmarkSelection) -> Result<BenchmarkReport> {
    spec.validate()?;
    if selection.formats.is_empty() {
        bail!("at least one format must be selected");
    }
    if selection.operations.is_empty() {
        bail!("at least one operation must be selected");
    }
    let dataset = generate(spec);
    let mut formats: Vec<Box<dyn FormatUnderTest + '_>> = Vec::with_capacity(selection.formats.len());
    for format in &selection.formats {
        match format {
            BenchmarkFormat::Oe => formats.push(Box::new(
                OeFormat::prepare_selected(&dataset, spec, dir, &selection.operations)
                    .context("publishing the OE file")?,
            )),
            BenchmarkFormat::Parquet => formats.push(Box::new(
                ParquetFormat::prepare(&dataset, spec, dir).context("encoding the Parquet file")?,
            )),
            BenchmarkFormat::Vortex => formats.push(Box::new(
                VortexFormat::prepare(&dataset, spec, dir).context("encoding the Vortex file")?,
            )),
        }
    }

    let scratch = dir.join("scratch");
    std::fs::create_dir_all(&scratch).context("creating the scratch directory")?;

    let format_refs: Vec<&dyn FormatUnderTest> = formats.iter().map(Box::as_ref).collect();
    let mut operations = Vec::with_capacity(selection.operations.len());
    for operation in &selection.operations {
        operations.push(measure(*operation, spec, &format_refs, &scratch, selection)?);
    }

    Ok(BenchmarkReport {
        environment: crate::environment::capture(),
        files: formats
            .iter()
            .map(|format| FormatFile {
                bytes: format.file_bytes(),
                format: format.name().to_owned(),
            })
            .collect(),
        operations,
        resident_high_water_bytes: resident_high_water_bytes(),
        spec: *spec,
    })
}

fn measure(
    operation: Operation,
    spec: &BenchmarkSpec,
    formats: &[&dyn FormatUnderTest],
    scratch: &Path,
    selection: &BenchmarkSelection,
) -> Result<OperationReport> {
    let mut measured = Vec::with_capacity(formats.len());
    for format in formats {
        measured.push((
            format.name(),
            repeat(operation, spec, *format, scratch, selection.phase_timings)?,
        ));
    }

    let first = measured
        .first()
        .ok_or_else(|| anyhow!("a comparison needs at least one format"))?;
    for (name, run) in &measured {
        if selection.check_answers && measured.len() > 1 && run.matched_rows != first.1.matched_rows {
            bail!(
                "{}: {} answered {} rows and {} answered {} — the formats are not doing the same work, so their \
                 timings are not comparable",
                operation.label(),
                first.0,
                first.1.matched_rows,
                name,
                run.matched_rows
            );
        }
    }

    let baseline = measured
        .iter()
        .max_by_key(|(_, run)| run.median_nanos)
        .expect("the measurements are non-empty");
    let baseline_name = baseline.0.to_owned();
    let baseline_bytes = baseline.1.bytes;
    let baseline_median = baseline.1.median_nanos;

    Ok(OperationReport {
        baseline: baseline_name,
        operation,
        results: measured
            .into_iter()
            .map(|(name, run)| {
                let byte_ratio = ratio(baseline_bytes, run.bytes);
                let speedup = ratio(baseline_median, run.median_nanos);
                FormatResult {
                    byte_ratio,
                    format: name.to_owned(),
                    measurement: run,
                    speedup,
                }
            })
            .collect(),
    })
}

fn repeat(
    operation: Operation,
    spec: &BenchmarkSpec,
    format: &dyn FormatUnderTest,
    scratch: &Path,
    phase_timings: bool,
) -> Result<FormatMeasurement> {
    let mut samples = Vec::with_capacity(spec.iterations);
    let mut samples_with_input_conversion = Vec::with_capacity(spec.iterations);
    let mut allocation_samples = Vec::with_capacity(spec.iterations);
    let mut phase_samples: std::collections::BTreeMap<WriterPhase, Vec<u64>> = std::collections::BTreeMap::new();
    let mut last = None;
    for _ in 0..spec.iterations {
        let (answer, allocations) = measure_allocations(|| {
            profile_operation(operation, || {
                if phase_timings {
                    format.run_with_phases(operation, scratch)
                } else {
                    format.run(operation, scratch).map(|measured| (measured, Vec::new()))
                }
            })
        });
        let (measured, phases) =
            answer.with_context(|| format!("{} answering {}", format.name(), operation.label()))?;
        samples.push(measured.elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
        if let Some(conversion) = measured.input_conversion {
            samples_with_input_conversion
                .push((measured.elapsed + conversion).as_nanos().min(u128::from(u64::MAX)) as u64);
        }
        if counting_enabled() {
            allocation_samples.push(AllocationSample {
                allocated_bytes: allocations.bytes,
                allocation_count: allocations.allocations,
                largest_allocation_bytes: allocations.largest_bytes,
                peak_live_bytes: allocations.peak_live_bytes,
                reallocation_count: allocations.reallocations,
            });
        }
        for phase in phases {
            phase_samples
                .entry(phase.phase)
                .or_default()
                .push(phase.elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
        }
        last = Some(measured.outcome);
    }
    let outcome = last.unwrap_or_default();
    let mut measurement = summarize(samples, outcome.bytes, outcome.matched_rows);
    if !samples_with_input_conversion.is_empty() {
        let mut sorted = samples_with_input_conversion.clone();
        sorted.sort_unstable();
        measurement.median_with_input_conversion_nanos = sorted.get(sorted.len() / 2).copied();
        measurement.samples_with_input_conversion_nanos = samples_with_input_conversion;
    }
    if counting_enabled() {
        measurement.allocations = Some(summarize_allocations(allocation_samples));
    }
    measurement.phases = phase_samples
        .into_iter()
        .map(|(phase, samples)| summarize_phase(phase, samples))
        .collect();
    Ok(measurement)
}

macro_rules! operation_signposts {
    ($(($function:ident, $variant:ident)),+ $(,)?) => {
        $(
            #[inline(never)]
            fn $function<T>(work: impl FnOnce() -> T) -> T {
                std::hint::black_box(stringify!($variant));
                let result = std::hint::black_box(work());
                std::hint::black_box(stringify!($variant));
                result
            }
        )+

        fn profile_operation<T>(operation: Operation, work: impl FnOnce() -> T) -> T {
            match operation {
                $(Operation::$variant => $function(work),)+
            }
        }
    };
}

operation_signposts!(
    (profile_create_write, CreateWrite),
    (profile_delete_rows, DeleteRows),
    (profile_diff_versions, DiffVersions),
    (profile_full_text_search, FullTextSearch),
    (profile_full_text_search_absent, FullTextSearchAbsent),
    (profile_full_text_search_rare, FullTextSearchRare),
    (profile_open_metadata, OpenMetadata),
    (profile_point_lookup, PointLookup),
    (profile_read_all_columns, ReadAllColumns),
    (profile_read_one_column, ReadOneColumn),
    (profile_read_payload_field, ReadPayloadField),
    (profile_read_text_column, ReadTextColumn),
    (profile_range_search, RangeSearch),
    (profile_update_rows, UpdateRows),
    (profile_value_search, ValueSearch),
);

pub fn render_table(report: &BenchmarkReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} rows · {} rows per group · {}% range window · {} iterations · seed {:#x}",
        report.spec.row_count,
        report.spec.rows_per_group,
        report.spec.range_percent,
        report.spec.iterations,
        report.spec.seed
    );
    let _ = write!(out, "file size:");
    for file in &report.files {
        let _ = write!(out, " {} {} ·", file.format, human_bytes(file.bytes));
    }
    let _ = writeln!(out);
    let environment = &report.environment;
    let _ = writeln!(
        out,
        "host: {} · {} {} · {} · commit {} · controls: {}\n",
        environment.hostname.as_deref().unwrap_or("?"),
        environment.os,
        environment.arch,
        environment.cpu.as_deref().unwrap_or("?"),
        environment.commit.as_deref().unwrap_or("?"),
        if environment.controls.is_empty() {
            "none".to_owned()
        } else {
            environment.controls.join(" ")
        }
    );
    let _ = writeln!(
        out,
        "{:<18} {:<10} {:>11} {:>8}  {:>11} {:>8}  {:>10}",
        "operation", "format", "median", "vs base", "bytes", "vs base", "rows"
    );
    let _ = writeln!(out, "{}", "-".repeat(87));
    for entry in &report.operations {
        for (index, result) in entry.results.iter().enumerate() {
            let name = if result.format == entry.baseline {
                format!("{}*", result.format)
            } else {
                result.format.clone()
            };
            let _ = writeln!(
                out,
                "{:<18} {:<10} {:>11} {:>7.2}x  {:>11} {:>7.2}x  {:>10}",
                if index == 0 { entry.operation.label() } else { "" },
                name,
                human_nanos(result.measurement.median_nanos),
                result.speedup,
                human_bytes(result.measurement.bytes),
                result.byte_ratio,
                result.measurement.matched_rows
            );
        }
    }
    if report
        .operations
        .iter()
        .flat_map(|entry| &entry.results)
        .any(|result| result.measurement.allocations.is_some())
    {
        let _ = writeln!(
            out,
            "\n{:<18} {:<10} {:>12} {:>12} {:>12} {:>12} {:>10}",
            "operation", "format", "allocations", "allocated", "peak live", "largest", "reallocs"
        );
        let _ = writeln!(out, "{}", "-".repeat(94));
        for entry in &report.operations {
            for (index, result) in entry.results.iter().enumerate() {
                if let Some(memory) = &result.measurement.allocations {
                    let _ = writeln!(
                        out,
                        "{:<18} {:<10} {:>12} {:>12} {:>12} {:>12} {:>10}",
                        if index == 0 { entry.operation.label() } else { "" },
                        result.format,
                        memory.allocation_count,
                        human_bytes(memory.allocated_bytes),
                        human_bytes(memory.peak_live_bytes),
                        human_bytes(memory.largest_allocation_bytes),
                        memory.reallocation_count,
                    );
                }
            }
        }
    }
    if let Some(bytes) = report.resident_high_water_bytes {
        let _ = writeln!(out, "\nprocess resident high-water mark: {}", human_bytes(bytes));
    }
    let _ = writeln!(
        out,
        "\nboth ratios are the row's slowest format — marked * — divided by the row's format: above 1 means that \
         format did better."
    );
    out
}

fn human_nanos(nanos: u64) -> String {
    match nanos {
        0..=9_999 => format!("{nanos}ns"),
        10_000..=9_999_999 => format!("{:.1}us", nanos as f64 / 1_000.0),
        _ => format!("{:.1}ms", nanos as f64 / 1_000_000.0),
    }
}

fn human_bytes(bytes: u64) -> String {
    match bytes {
        0..=9_999 => format!("{bytes}B"),
        10_000..=9_999_999 => format!("{:.1}KiB", bytes as f64 / 1024.0),
        _ => format!("{:.1}MiB", bytes as f64 / (1024.0 * 1024.0)),
    }
}

#[cfg(test)]
#[path = "test/run.rs"]
mod tests;
