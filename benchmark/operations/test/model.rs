use super::*;

#[test]
fn summarize_keeps_the_fastest_sample_and_the_median() {
    let measured = summarize(vec![50, 10, 30, 40, 20], 1_024, 7);
    assert_eq!(measured.min_nanos, 10);
    assert_eq!(measured.median_nanos, 30);
    assert_eq!(measured.bytes, 1_024);
    assert_eq!(measured.matched_rows, 7);
}

#[test]
fn allocation_summaries_keep_raw_samples_and_each_metrics_median() {
    let measured = summarize_allocations(vec![
        AllocationSample {
            allocation_count: 30,
            allocated_bytes: 100,
            peak_live_bytes: 3,
            largest_allocation_bytes: 40,
            reallocation_count: 2,
        },
        AllocationSample {
            allocation_count: 10,
            allocated_bytes: 300,
            peak_live_bytes: 1,
            largest_allocation_bytes: 20,
            reallocation_count: 3,
        },
        AllocationSample {
            allocation_count: 20,
            allocated_bytes: 200,
            peak_live_bytes: 2,
            largest_allocation_bytes: 30,
            reallocation_count: 1,
        },
    ]);
    assert_eq!(measured.allocation_count, 20);
    assert_eq!(measured.allocated_bytes, 200);
    assert_eq!(measured.peak_live_bytes, 2);
    assert_eq!(measured.largest_allocation_bytes, 30);
    assert_eq!(measured.reallocation_count, 2);
    assert_eq!(measured.samples[0].allocation_count, 30);
}

#[test]
fn a_ratio_against_zero_is_reported_as_zero() {
    assert_eq!(ratio(100, 0), 0.0);
    assert_eq!(ratio(100, 50), 2.0);
}

#[test]
fn a_spec_that_would_mutate_no_rows_is_rejected() {
    let spec = BenchmarkSpec {
        mutation_percent: 1,
        row_count: 50,
        ..BenchmarkSpec::default()
    };
    assert!(spec.validate().is_err());
    assert!(BenchmarkSpec::default().validate().is_ok());
}

#[test]
fn every_operation_reports_under_its_own_name() {
    let mut names: Vec<&str> = OPERATIONS.iter().map(|operation| operation.label()).collect();
    names.sort_unstable();
    let count = names.len();
    names.dedup();
    assert_eq!(names.len(), count);
}
