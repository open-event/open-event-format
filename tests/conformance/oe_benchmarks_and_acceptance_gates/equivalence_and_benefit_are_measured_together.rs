use oe::benchmarks::{AcceleratedBaselineError, BenchGateRegistry, CommittedBaseline};

#[test]
fn equivalence_and_benefit_are_measured_together() {
    let mut registry = BenchGateRegistry::default();

    let not_verified = CommittedBaseline {
        benchmark_id: "bench:blake3-avx512".to_owned(),
        equivalence_verified: false,
        measured_value: 6.0,
        tolerance_fraction: 0.05,
    };
    assert_eq!(
        registry.commit_accelerated_baseline(not_verified),
        Err(AcceleratedBaselineError::EquivalenceNotVerified)
    );

    let verified = CommittedBaseline {
        benchmark_id: "bench:blake3-avx512".to_owned(),
        equivalence_verified: true,
        measured_value: 6.0,
        tolerance_fraction: 0.05,
    };
    registry.commit_accelerated_baseline(verified).unwrap();
}
