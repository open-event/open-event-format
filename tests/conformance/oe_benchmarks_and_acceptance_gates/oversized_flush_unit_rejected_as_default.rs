use oe::benchmarks::{NEGATIVE_CONTROL_FLUSH_BYTES, flush_unit_valid_as_default};

#[test]
fn oversized_flush_unit_rejected_as_default() {
    assert!(!flush_unit_valid_as_default(NEGATIVE_CONTROL_FLUSH_BYTES));
    assert!(!flush_unit_valid_as_default(256 * 1024));
    assert!(!flush_unit_valid_as_default(1024 * 1024));

    assert!(flush_unit_valid_as_default(64 * 1024));
    assert!(flush_unit_valid_as_default(16 * 1024));
    assert!(flush_unit_valid_as_default(4 * 1024));
}
