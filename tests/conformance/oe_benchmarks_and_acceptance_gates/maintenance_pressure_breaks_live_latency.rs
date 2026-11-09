use oe::benchmarks::maintenance_gate_passes;

#[test]
fn maintenance_pressure_breaks_live_latency() {
    let bound_micros = 1_000_u64;

    assert!(maintenance_gate_passes(500, bound_micros));
    assert!(maintenance_gate_passes(bound_micros, bound_micros));

    assert!(!maintenance_gate_passes(bound_micros + 1, bound_micros));
    assert!(!maintenance_gate_passes(5_000, bound_micros));
}
