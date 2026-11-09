use oe::deletes::DerivedColumnLedger;
use oe::events::EventId;
use oe::typed_id::TypedIdTestExt;

#[test]
fn stale_score_never_served_after_correction() {
    let corrected_event = EventId::new_test_id(1);
    let hawkes_child = EventId::new_test_id(2);
    let unrelated_event = EventId::new_test_id(3);

    let mut ledger = DerivedColumnLedger::new();
    ledger.republish(corrected_event, 0.92, 10);
    ledger.republish(hawkes_child, 0.41, 10);
    ledger.republish(unrelated_event, 0.05, 10);

    ledger.invalidate(corrected_event, &[hawkes_child]);

    assert_eq!(ledger.published_value(corrected_event), None);
    assert_eq!(ledger.published_value(hawkes_child), None);
    assert_eq!(ledger.published_value(unrelated_event), Some(0.05));

    assert_eq!(ledger.pending_recomputes(), &[corrected_event, hawkes_child]);

    ledger.republish(corrected_event, 0.15, 11);
    ledger.republish(hawkes_child, 0.09, 11);
    assert_eq!(ledger.published_value(corrected_event), Some(0.15));
    assert_eq!(ledger.published_value(hawkes_child), Some(0.09));
    assert!(ledger.pending_recomputes().is_empty());
}
