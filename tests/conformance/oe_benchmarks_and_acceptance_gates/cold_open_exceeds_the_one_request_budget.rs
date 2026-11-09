use oe::benchmarks::{BenchGateRegistry, ColdOpenMeasurement, CommittedBaseline, cold_open_gate_passes};

#[test]
fn cold_open_exceeds_the_one_request_budget() {
    assert!(cold_open_gate_passes(&ColdOpenMeasurement {
        justification_recorded: false,
        request_count: 1,
    }));

    assert!(!cold_open_gate_passes(&ColdOpenMeasurement {
        justification_recorded: false,
        request_count: 3,
    }));

    assert!(cold_open_gate_passes(&ColdOpenMeasurement {
        justification_recorded: true,
        request_count: 3,
    }));

    let mut registry = BenchGateRegistry::default();
    registry.register_benchmark(
        "entity-id-cold-open-s3-durable",
        "bench:oe-cold-open-requests-s3-durable",
    );
    registry.commit_baseline(CommittedBaseline {
        benchmark_id: "bench:oe-cold-open-requests-s3-durable".to_owned(),
        equivalence_verified: false,
        measured_value: 1.0,
        tolerance_fraction: 0.0,
    });
    registry.mark_ready("entity-id-cold-open-s3-durable").unwrap();
    assert!(registry.is_ready("entity-id-cold-open-s3-durable"));

    let regression = registry
        .detect_regression(
            "entity-id-cold-open-s3-durable",
            "bench:oe-cold-open-requests-s3-durable",
            3.0,
        )
        .expect("cold-open regression detected");
    assert_eq!(regression.benchmark_id, "bench:oe-cold-open-requests-s3-durable");
    assert_eq!(regression.baseline_value, 1.0);
    assert_eq!(regression.measured_value, 3.0);
    assert!(
        !registry.is_ready("entity-id-cold-open-s3-durable"),
        "cold-open path must leave the ready set once it regresses beyond the one-request budget"
    );
}
