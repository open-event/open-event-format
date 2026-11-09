use oe::benchmarks::{S3DurableLookupMeasurement, s3_durable_lookup_gate_passes};

#[test]
fn s3_durable_cold_lookup_is_not_measured_against_the_nvme_target() {
    assert!(s3_durable_lookup_gate_passes(&S3DurableLookupMeasurement {
        dependent_round_trips: 2,
        elapsed_millis: 12.0,
        per_request_millis_budget: 8.0,
    }));

    assert!(!s3_durable_lookup_gate_passes(&S3DurableLookupMeasurement {
        dependent_round_trips: 3,
        elapsed_millis: 4.0,
        per_request_millis_budget: 8.0,
    }));

    assert!(!s3_durable_lookup_gate_passes(&S3DurableLookupMeasurement {
        dependent_round_trips: 2,
        elapsed_millis: 20.0,
        per_request_millis_budget: 8.0,
    }));
}
