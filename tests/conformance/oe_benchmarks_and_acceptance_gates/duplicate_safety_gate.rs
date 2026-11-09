use oe::benchmarks::SnapshotSafetyChecker;

#[test]
fn duplicate_safety_gate() {
    let mut checker = SnapshotSafetyChecker::default();

    for id in [0xAABB_u128, 0xCCDD, 0xEEFF] {
        checker.record_oe(id);
    }

    assert!(checker.is_duplicate(0xAABB), "id seen in both layers is a duplicate");
    assert!(checker.is_duplicate(0xCCDD));

    assert!(
        !checker.is_duplicate(0x1234),
        "id unique to live-overlay is not a duplicate"
    );
    assert!(!checker.is_duplicate(0x0000));
}
