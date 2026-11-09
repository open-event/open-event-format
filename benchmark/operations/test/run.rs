use super::*;
use crate::model::{FormatMeasurement, Operation};
use tempfile::tempdir;

fn spec() -> BenchmarkSpec {
    BenchmarkSpec {
        iterations: 2,
        mutation_percent: 5,
        range_percent: 10,
        row_count: 500,
        rows_per_group: 64,
        seed: 11,
    }
}

const FORMATS: &[&str] = &["oe", "parquet", "vortex"];

fn measurement(report: &BenchmarkReport, wanted: Operation, format: &str) -> FormatMeasurement {
    report
        .operations
        .iter()
        .find(|entry| entry.operation == wanted)
        .and_then(|entry| entry.results.iter().find(|result| result.format == format))
        .map(|result| result.measurement.clone())
        .unwrap()
}

#[test]
fn every_format_answers_every_operation_the_same_way() {
    let dir = tempdir().unwrap();
    let report = compare(&spec(), dir.path()).unwrap();

    assert_eq!(report.operations.len(), OPERATIONS.len());
    assert_eq!(
        report.files.iter().map(|file| file.format.as_str()).collect::<Vec<_>>(),
        FORMATS
    );
    for entry in &report.operations {
        let names: Vec<&str> = entry.results.iter().map(|result| result.format.as_str()).collect();
        assert_eq!(names, FORMATS, "{} measured the wrong formats", entry.operation.label());
        let rows: Vec<u64> = entry
            .results
            .iter()
            .map(|result| result.measurement.matched_rows)
            .collect();
        assert!(
            rows.windows(2).all(|pair| pair[0] == pair[1]),
            "{} disagreed: {rows:?}",
            entry.operation.label()
        );
    }
}

#[test]
fn each_row_is_measured_against_its_own_slowest_format() {
    let dir = tempdir().unwrap();
    let report = compare(&spec(), dir.path()).unwrap();
    for entry in &report.operations {
        let slowest = entry
            .results
            .iter()
            .max_by_key(|result| result.measurement.median_nanos)
            .unwrap();
        assert_eq!(slowest.format, entry.baseline, "{}", entry.operation.label());
        assert_eq!(slowest.speedup, 1.0, "{}", entry.operation.label());
        for result in &entry.results {
            assert!(
                result.speedup >= 1.0,
                "{} reported {} at {}x against the row's slowest format",
                entry.operation.label(),
                result.format,
                result.speedup
            );
        }
    }
}

#[test]
fn every_search_finds_rows_on_every_format() {
    let dir = tempdir().unwrap();
    let report = compare(&spec(), dir.path()).unwrap();
    let searches = [
        Operation::FullTextSearch,
        Operation::PointLookup,
        Operation::RangeSearch,
        Operation::ReadPayloadField,
        Operation::ValueSearch,
    ];
    for operation in searches {
        for format in FORMATS {
            let found = measurement(&report, operation, format).matched_rows;
            assert!(found > 0, "{} found nothing on {format}", operation.label());
        }
    }
    for format in FORMATS {
        assert_eq!(measurement(&report, Operation::PointLookup, format).matched_rows, 1);
        assert_eq!(
            measurement(&report, Operation::ReadAllColumns, format).matched_rows,
            spec().row_count as u64
        );
    }
}

#[test]
fn the_absent_and_rare_searches_prune_where_the_dense_one_cannot() {
    let dir = tempdir().unwrap();
    let report = compare(&spec(), dir.path()).unwrap();

    for format in FORMATS {
        assert_eq!(
            measurement(&report, Operation::FullTextSearchAbsent, format).matched_rows,
            0,
            "no row carries the absent needle on {format}"
        );
    }
    assert_eq!(
        measurement(&report, Operation::FullTextSearchRare, "oe").matched_rows,
        1,
        "one row in the fixture carries the rare needle"
    );

    assert_eq!(
        measurement(&report, Operation::FullTextSearchAbsent, "oe").bytes,
        0,
        "the absent search must read no column bytes"
    );

    let oe_rare = measurement(&report, Operation::FullTextSearchRare, "oe").bytes;
    let parquet_rare = measurement(&report, Operation::FullTextSearchRare, "parquet").bytes;
    assert!(
        oe_rare > 0,
        "the rare search must still read the granules it cannot rule out"
    );
    assert!(
        oe_rare < parquet_rare,
        "the rare search read {oe_rare} bytes against parquet's {parquet_rare}"
    );
}

#[test]
fn the_diff_finds_exactly_the_amended_rows() {
    let dir = tempdir().unwrap();
    let spec = spec();
    let report = compare(&spec, dir.path()).unwrap();
    for format in FORMATS {
        assert_eq!(
            measurement(&report, Operation::DiffVersions, format).matched_rows,
            spec.mutated_row_count() as u64,
            "{format} disagreed"
        );
    }
}

#[test]
fn reading_one_column_touches_fewer_bytes_than_reading_all_of_them() {
    let dir = tempdir().unwrap();
    let report = compare(&spec(), dir.path()).unwrap();
    for format in FORMATS {
        let one = measurement(&report, Operation::ReadOneColumn, format).bytes;
        let all = measurement(&report, Operation::ReadAllColumns, format).bytes;
        assert!(one < all, "{format} read {one} of {all}");
    }
}

#[test]
fn the_rendered_table_names_every_operation_and_format() {
    let dir = tempdir().unwrap();
    let report = compare(&spec(), dir.path()).unwrap();
    let table = render_table(&report);
    for operation in OPERATIONS {
        assert!(table.contains(operation.label()), "{} is missing", operation.label());
    }
    for format in FORMATS {
        assert!(table.contains(format), "{format} is missing");
    }
    for entry in &report.operations {
        assert!(
            table.contains(&format!("{}*", entry.baseline)),
            "{} does not mark its baseline",
            entry.operation.label()
        );
    }
}

#[test]
fn only_oe_writes_report_a_median_with_input_conversion() {
    let dir = tempdir().unwrap();
    let report = compare(&spec(), dir.path()).unwrap();
    for operation in OPERATIONS {
        for format in FORMATS {
            let measured = measurement(&report, *operation, format);
            let converts = *format == "oe" && matches!(operation, Operation::CreateWrite | Operation::UpdateRows);
            assert_eq!(
                measured.median_with_input_conversion_nanos.is_some(),
                converts,
                "{} on {format}",
                operation.label()
            );
            assert_eq!(
                measured.samples_with_input_conversion_nanos.len(),
                if converts { spec().iterations } else { 0 },
                "{} on {format}",
                operation.label()
            );
        }
    }
}

#[test]
fn a_filtered_oe_operation_matches_the_full_comparison_answer_and_bytes() {
    let full_dir = tempdir().unwrap();
    let filtered_dir = tempdir().unwrap();
    let spec = BenchmarkSpec {
        iterations: 1,
        ..spec()
    };
    let full = compare(&spec, full_dir.path()).unwrap();
    let selection = BenchmarkSelection {
        formats: vec![BenchmarkFormat::Oe],
        operations: vec![Operation::CreateWrite],
        ..BenchmarkSelection::default()
    };
    let filtered = compare_selected(&spec, filtered_dir.path(), &selection).unwrap();

    let expected = measurement(&full, Operation::CreateWrite, "oe");
    let actual = measurement(&filtered, Operation::CreateWrite, "oe");
    assert_eq!(actual.bytes, expected.bytes);
    assert_eq!(actual.matched_rows, expected.matched_rows);
    assert!(actual.phases.is_empty());
    assert_eq!(filtered.files.len(), 1);
    assert_eq!(filtered.operations.len(), 1);
    assert!(!filtered_dir.path().join("base.oe").exists());
    assert!(!filtered_dir.path().join("base.parquet").exists());
    assert!(!filtered_dir.path().join("base.vortex").exists());
}

#[test]
fn toml_reports_raw_samples_and_optional_oe_phases() {
    let dir = tempdir().unwrap();
    let spec = BenchmarkSpec {
        iterations: 2,
        ..spec()
    };
    let selection = BenchmarkSelection {
        formats: vec![BenchmarkFormat::Oe],
        operations: vec![Operation::CreateWrite],
        phase_timings: true,
        ..BenchmarkSelection::default()
    };
    let report = compare_selected(&spec, dir.path(), &selection).unwrap();
    let measured = measurement(&report, Operation::CreateWrite, "oe");
    assert_eq!(measured.samples_nanos.len(), spec.iterations);
    assert_eq!(measured.samples_with_input_conversion_nanos.len(), spec.iterations);
    assert!(
        measured
            .median_with_input_conversion_nanos
            .is_some_and(|median| median >= measured.median_nanos)
    );
    assert!(
        measured
            .samples_nanos
            .iter()
            .zip(&measured.samples_with_input_conversion_nanos)
            .all(|(own, inclusive)| inclusive >= own)
    );
    assert!(
        measured
            .phases
            .iter()
            .all(|phase| phase.samples_nanos.len() == spec.iterations)
    );
    assert!(
        measured
            .phases
            .iter()
            .any(|phase| phase.phase == WriterPhase::BlockEncoding)
    );
    assert!(
        measured
            .phases
            .iter()
            .any(|phase| phase.phase == WriterPhase::DurableWrite)
    );
    if crate::allocation::counting_enabled() {
        let allocations = measured.allocations.as_ref().unwrap();
        assert_eq!(allocations.samples.len(), spec.iterations);
        assert!(allocations.allocation_count > 0);
        assert!(allocations.allocated_bytes > 0);
        assert!(allocations.peak_live_bytes > 0);
        assert!(allocations.largest_allocation_bytes > 0);
    } else {
        assert!(measured.allocations.is_none());
    }
    #[cfg(unix)]
    assert!(report.resident_high_water_bytes.is_some());

    let encoded = toml::to_string(&report).unwrap();
    let decoded: BenchmarkReport = toml::from_str(&encoded).unwrap();
    assert_eq!(decoded, report);
}
