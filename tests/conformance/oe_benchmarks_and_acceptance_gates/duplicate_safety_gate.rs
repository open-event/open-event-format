//! Checks that no event_id can appear from both OE and the live-overlay layer in the same query snapshot. The checker
//! detects this violation so the query path can be validated for readiness.

use oe::benchmarks::SnapshotSafetyChecker;

/// conformance: oe-benchmarks-and-acceptance-gates/real-time-query-safety-gates/duplicate-safety-gate
#[test]
fn duplicate_safety_gate() {
    let mut checker = SnapshotSafetyChecker::default();

    // Record a set of event ids returned from OE.
    for id in [0xAABB_u128, 0xCCDD, 0xEEFF] {
        checker.record_oe(id);
    }

    // A live-overlay row whose id also appears in OE is a duplicate.
    assert!(checker.is_duplicate(0xAABB), "id seen in both layers is a duplicate");
    assert!(checker.is_duplicate(0xCCDD));

    // A live-overlay row whose id did not appear in OE is safe.
    assert!(
        !checker.is_duplicate(0x1234),
        "id unique to live-overlay is not a duplicate"
    );
    assert!(!checker.is_duplicate(0x0000));
}
