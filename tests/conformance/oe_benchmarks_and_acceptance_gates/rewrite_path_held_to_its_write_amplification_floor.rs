use oe::benchmarks::{WafMeasurement, waf_gate_passes};

#[test]
fn rewrite_path_held_to_its_write_amplification_floor() {
    assert!(waf_gate_passes(&WafMeasurement {
        ceiling: 1.2,
        measured_waf: 1.2,
    }));

    assert!(waf_gate_passes(&WafMeasurement {
        ceiling: 1.2,
        measured_waf: 1.05,
    }));

    assert!(!waf_gate_passes(&WafMeasurement {
        ceiling: 1.2,
        measured_waf: 1.6,
    }));
}
