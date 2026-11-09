use oe::benchmarks::{FlushTargetMeasurements, flush_default_selectable};

#[test]
fn default_flush_target_requires_separate_measurement() {
    assert!(!flush_default_selectable(&FlushTargetMeasurements {
        four_kib_benchmarked: false,
        sixteen_kib_benchmarked: false,
    }));

    assert!(!flush_default_selectable(&FlushTargetMeasurements {
        four_kib_benchmarked: true,
        sixteen_kib_benchmarked: false,
    }));

    assert!(!flush_default_selectable(&FlushTargetMeasurements {
        four_kib_benchmarked: false,
        sixteen_kib_benchmarked: true,
    }));

    assert!(flush_default_selectable(&FlushTargetMeasurements {
        four_kib_benchmarked: true,
        sixteen_kib_benchmarked: true,
    }));
}
