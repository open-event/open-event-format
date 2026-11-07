//! Checks that when a later harness run shows a previously-ready capability's baseline missing its gate or regressing
//! beyond tolerance, the capability is removed from the ready set until the regression is resolved.

use oe::benchmarks::{BenchGateRegistry, CommittedBaseline};

/// conformance:
/// oe-benchmarks-and-acceptance-gates/acceptance-gates-are-mechanically-enforced-by-the-committed-harness/
/// regressed-baseline-drops-a-capability-from-the-ready-set
#[test]
fn regressed_baseline_drops_a_capability_from_the_ready_set() {
    let mut registry = BenchGateRegistry::default();

    registry.register_benchmark("oe-entity-lookup", "bench:oe-point-lookup-cold");
    registry.commit_baseline(CommittedBaseline {
        benchmark_id: "bench:oe-point-lookup-cold".to_owned(),
        equivalence_verified: false,
        measured_value: 3.0, // 3 ms baseline
        tolerance_fraction: 0.10,
    });
    registry.mark_ready("oe-entity-lookup").unwrap();
    assert!(registry.is_ready("oe-entity-lookup"));

    // A measurement within tolerance does not drop the capability.
    let no_regression = registry.detect_regression(
        "oe-entity-lookup",
        "bench:oe-point-lookup-cold",
        3.2, // 3.2 ms — within 10% of 3.0
    );
    assert!(no_regression.is_none());
    assert!(
        registry.is_ready("oe-entity-lookup"),
        "still ready after acceptable measurement"
    );

    // A measurement beyond tolerance drops the capability from the ready set.
    let regression = registry.detect_regression(
        "oe-entity-lookup",
        "bench:oe-point-lookup-cold",
        5.5, // 5.5 ms — well beyond 10% of 3.0
    );
    assert!(regression.is_some(), "regression detected");
    assert!(
        !registry.is_ready("oe-entity-lookup"),
        "capability must leave the ready set after regression"
    );
}
