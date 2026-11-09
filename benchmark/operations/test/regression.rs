use super::*;
use crate::model::{
    AllocationSample, BenchmarkSpec, Environment, FormatFile, FormatMeasurement, FormatResult, OperationReport,
    summarize, summarize_allocations,
};

fn report(memory: AllocationSample, resident_high_water_bytes: Option<u64>) -> BenchmarkReport {
    let mut measurement: FormatMeasurement = summarize(vec![10], 100, 1);
    measurement.allocations = Some(summarize_allocations(vec![memory]));
    BenchmarkReport {
        environment: Environment::default(),
        files: Vec::new(),
        operations: vec![OperationReport {
            baseline: "oe".to_owned(),
            operation: Operation::CreateWrite,
            results: vec![FormatResult {
                byte_ratio: 1.0,
                format: "oe".to_owned(),
                measurement,
                speedup: 1.0,
            }],
        }],
        resident_high_water_bytes,
        spec: BenchmarkSpec::default(),
    }
}

fn sample(value: u64) -> AllocationSample {
    AllocationSample {
        allocated_bytes: value,
        allocation_count: value,
        largest_allocation_bytes: value,
        peak_live_bytes: value,
        reallocation_count: value,
    }
}

#[test]
fn a_candidate_within_the_configured_tolerance_passes() {
    let baseline = report(sample(100), Some(1_000));
    let candidate = report(sample(105), Some(1_050));
    assert!(check_memory_regressions(&baseline, &candidate, 0.05).is_ok());
}

#[test]
fn every_memory_metric_can_fail_the_gate() {
    let baseline = report(sample(100), Some(1_000));
    let candidate = report(sample(106), Some(1_051));
    let regressions = memory_regressions(&baseline, &candidate, 0.05).unwrap();
    assert_eq!(regressions.len(), 6);
    for metric in [
        "allocation_count",
        "allocated_bytes",
        "largest_allocation_bytes",
        "peak_live_bytes",
        "reallocation_count",
        "resident_high_water_bytes",
    ] {
        assert!(
            regressions.iter().any(|regression| regression.metric == metric),
            "{metric}"
        );
    }
    let error = check_memory_regressions(&baseline, &candidate, 0.05)
        .unwrap_err()
        .to_string();
    assert!(error.contains("create_write oe peak_live_bytes"), "{error}");
}

#[test]
fn an_uninstrumented_candidate_is_rejected_instead_of_treated_as_zero() {
    let baseline = report(sample(100), None);
    let mut candidate = report(sample(100), None);
    candidate.operations[0].results[0].measurement.allocations = None;
    let error = memory_regressions(&baseline, &candidate, 0.05).unwrap_err().to_string();
    assert!(error.contains("--features alloc-counting"), "{error}");
}

#[test]
fn bytes_read_and_file_size_gate_without_allocation_metrics() {
    let mut baseline = report(sample(100), None);
    baseline.operations[0].results[0].measurement.allocations = None;
    baseline.files.push(FormatFile {
        bytes: 1_000,
        format: "oe".to_owned(),
    });
    let mut candidate = baseline.clone();
    assert!(memory_regressions(&baseline, &candidate, 0.0).unwrap().is_empty());

    candidate.operations[0].results[0].measurement.bytes = 101;
    candidate.files[0].bytes = 1_001;
    let regressions = memory_regressions(&baseline, &candidate, 0.0).unwrap();
    assert_eq!(regressions.len(), 2);
    assert!(
        regressions
            .iter()
            .any(|regression| { regression.metric == "bytes" && regression.operation == Some(Operation::CreateWrite) })
    );
    assert!(
        regressions
            .iter()
            .any(|regression| regression.metric == "file_bytes" && regression.operation.is_none())
    );
    let error = check_memory_regressions(&baseline, &candidate, 0.0)
        .unwrap_err()
        .to_string();
    assert!(error.contains("oe file_bytes: 1001 > 1000"), "{error}");
}

#[test]
fn a_file_missing_from_the_baseline_is_an_error() {
    let baseline = report(sample(100), None);
    let mut candidate = baseline.clone();
    candidate.files.push(FormatFile {
        bytes: 1,
        format: "parquet".to_owned(),
    });
    let error = memory_regressions(&baseline, &candidate, 0.0).unwrap_err().to_string();
    assert!(error.contains("no parquet file"), "{error}");
}
