use oe::benchmarks::{BenchGateRegistry, ReadinessError};

#[test]
fn capability_cannot_be_marked_ready_on_an_unmeasured_gate() {
    let mut registry = BenchGateRegistry::default();

    assert_eq!(
        registry.mark_ready("oe-numeric-scan"),
        Err(ReadinessError::NoBenchmark {
            capability: "oe-numeric-scan".to_owned(),
        })
    );
    assert!(!registry.is_ready("oe-numeric-scan"));

    registry.register_benchmark("oe-numeric-scan", "bench:oe-scan-avx512");
    registry.mark_ready("oe-numeric-scan").unwrap();
    assert!(registry.is_ready("oe-numeric-scan"));
}
