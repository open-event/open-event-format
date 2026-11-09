use oe::benchmarks::{BenchGateRegistry, CommittedBaseline};

#[test]
fn regressed_baseline_drops_a_capability_from_the_ready_set() {
    let mut registry = BenchGateRegistry::default();

    registry.register_benchmark("oe-entity-lookup", "bench:oe-point-lookup-cold");
    registry.commit_baseline(CommittedBaseline {
        benchmark_id: "bench:oe-point-lookup-cold".to_owned(),
        equivalence_verified: false,
        measured_value: 3.0,
        tolerance_fraction: 0.10,
    });
    registry.mark_ready("oe-entity-lookup").unwrap();
    assert!(registry.is_ready("oe-entity-lookup"));

    let no_regression = registry.detect_regression("oe-entity-lookup", "bench:oe-point-lookup-cold", 3.2);
    assert!(no_regression.is_none());
    assert!(
        registry.is_ready("oe-entity-lookup"),
        "still ready after acceptable measurement"
    );

    let regression = registry.detect_regression("oe-entity-lookup", "bench:oe-point-lookup-cold", 5.5);
    assert!(regression.is_some(), "regression detected");
    assert!(
        !registry.is_ready("oe-entity-lookup"),
        "capability must leave the ready set after regression"
    );
}
