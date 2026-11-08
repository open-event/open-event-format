//! Compares the deterministic counts in two benchmark reports — the bytes each operation read, each file's size, and,
//! where both reports were instrumented, allocations and peak memory — so a dedicated benchmark host can turn a
//! configured tolerance into a failing gate. Wall-clock is never compared here.

use crate::model::{AllocationMeasurement, BenchmarkReport, Operation};
use anyhow::{Result, bail, ensure};

/// One measured value that exceeded its baseline plus the configured tolerance.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRegression {
    pub baseline: u64,
    pub candidate: u64,
    pub format: String,
    pub metric: &'static str,
    pub operation: Option<Operation>,
}

/// Finds every bytes-read, file-size, allocation, or resident-memory regression in `candidate` against matching
/// entries in `baseline`.
///
/// Candidate reports may be filtered to fewer formats or operations than the baseline. Every candidate entry must
/// have a matching baseline entry. A baseline written without `alloc-counting` gates only bytes read and file size;
/// one written with it also requires the candidate's allocation figures. `tolerance_fraction` is `0.05` for five
/// percent.
pub fn memory_regressions(
    baseline: &BenchmarkReport,
    candidate: &BenchmarkReport,
    tolerance_fraction: f64,
) -> Result<Vec<MemoryRegression>> {
    ensure!(
        tolerance_fraction.is_finite() && tolerance_fraction >= 0.0,
        "memory regression tolerance must be a finite non-negative fraction"
    );
    ensure!(
        baseline.spec == candidate.spec,
        "memory baseline and candidate benchmark specs differ"
    );
    let mut regressions = Vec::new();
    for operation in &candidate.operations {
        let baseline_operation = baseline
            .operations
            .iter()
            .find(|entry| entry.operation == operation.operation)
            .ok_or_else(|| anyhow::anyhow!("baseline has no {} operation", operation.operation.label()))?;
        for result in &operation.results {
            let baseline_result = baseline_operation
                .results
                .iter()
                .find(|entry| entry.format == result.format)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "baseline has no {} result for {}",
                        result.format,
                        operation.operation.label()
                    )
                })?;
            record_if_regressed(
                &mut regressions,
                Some(operation.operation),
                &result.format,
                "bytes",
                baseline_result.measurement.bytes,
                result.measurement.bytes,
                tolerance_fraction,
            );
            let Some(baseline_memory) = baseline_result.measurement.allocations.as_ref() else {
                continue;
            };
            let candidate_memory = result.measurement.allocations.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "candidate {} {} result has no allocation metrics; rerun with --features alloc-counting",
                    operation.operation.label(),
                    result.format
                )
            })?;
            compare_allocations(
                &mut regressions,
                operation.operation,
                &result.format,
                baseline_memory,
                candidate_memory,
                tolerance_fraction,
            );
        }
    }
    for file in &candidate.files {
        let baseline_file = baseline
            .files
            .iter()
            .find(|entry| entry.format == file.format)
            .ok_or_else(|| anyhow::anyhow!("baseline has no {} file", file.format))?;
        record_if_regressed(
            &mut regressions,
            None,
            &file.format,
            "file_bytes",
            baseline_file.bytes,
            file.bytes,
            tolerance_fraction,
        );
    }

    if let (Some(baseline_bytes), Some(candidate_bytes)) =
        (baseline.resident_high_water_bytes, candidate.resident_high_water_bytes)
    {
        record_if_regressed(
            &mut regressions,
            None,
            "process",
            "resident_high_water_bytes",
            baseline_bytes,
            candidate_bytes,
            tolerance_fraction,
        );
    }
    Ok(regressions)
}

/// Fails with every regressed metric named, suitable for a benchmark-host or CI command.
pub fn check_memory_regressions(
    baseline: &BenchmarkReport,
    candidate: &BenchmarkReport,
    tolerance_fraction: f64,
) -> Result<()> {
    let regressions = memory_regressions(baseline, candidate, tolerance_fraction)?;
    if regressions.is_empty() {
        return Ok(());
    }
    let details = regressions
        .iter()
        .map(|regression| {
            let scope = regression.operation.map_or_else(
                || regression.format.clone(),
                |operation| format!("{} {}", operation.label(), regression.format),
            );
            format!(
                "{scope} {}: {} > {}",
                regression.metric, regression.candidate, regression.baseline
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    bail!("benchmark regression exceeded the configured tolerance: {details}")
}

fn compare_allocations(
    regressions: &mut Vec<MemoryRegression>,
    operation: Operation,
    format: &str,
    baseline: &AllocationMeasurement,
    candidate: &AllocationMeasurement,
    tolerance_fraction: f64,
) {
    for (metric, baseline, candidate) in [
        (
            "allocation_count",
            baseline.allocation_count,
            candidate.allocation_count,
        ),
        ("allocated_bytes", baseline.allocated_bytes, candidate.allocated_bytes),
        (
            "largest_allocation_bytes",
            baseline.largest_allocation_bytes,
            candidate.largest_allocation_bytes,
        ),
        ("peak_live_bytes", baseline.peak_live_bytes, candidate.peak_live_bytes),
        (
            "reallocation_count",
            baseline.reallocation_count,
            candidate.reallocation_count,
        ),
    ] {
        record_if_regressed(
            regressions,
            Some(operation),
            format,
            metric,
            baseline,
            candidate,
            tolerance_fraction,
        );
    }
}

fn record_if_regressed(
    regressions: &mut Vec<MemoryRegression>,
    operation: Option<Operation>,
    format: &str,
    metric: &'static str,
    baseline: u64,
    candidate: u64,
    tolerance_fraction: f64,
) {
    let threshold = baseline as f64 * (1.0 + tolerance_fraction);
    if candidate as f64 > threshold {
        regressions.push(MemoryRegression {
            baseline,
            candidate,
            format: format.to_owned(),
            metric,
            operation,
        });
    }
}

#[cfg(test)]
#[path = "test/regression.rs"]
mod tests;
