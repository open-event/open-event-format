use oe::deletes::{CorrectionMetadata, CorrectionType, DeletionVector, latest_corrected_deletion_vector};
use oe::events::EventId;
use oe::typed_id::TypedIdTestExt;

#[test]
fn latest_corrected_view() {
    let original_event = EventId::new_test_id(0xDEAD);
    let correcting_event = EventId::new_test_id(0xBEEF);
    let untouched_event = EventId::new_test_id(0xCAFE);

    let events = [
        (0u64, original_event),
        (1u64, correcting_event),
        (2u64, untouched_event),
    ];

    let correction = CorrectionMetadata {
        correction_epoch: 2,
        correction_generation: 1,
        correction_sequence: 99,
        correction_type: CorrectionType::Replacement,
        corrects_event_id: original_event,
    };

    let raw_history = DeletionVector::new();
    assert!(!raw_history.is_deleted(0));

    let latest_corrected = latest_corrected_deletion_vector(&events, &[correction]);
    assert!(latest_corrected.is_deleted(0));
    assert!(!latest_corrected.is_deleted(1));
    assert!(!latest_corrected.is_deleted(2));
    assert_eq!(latest_corrected.deleted_count(), 1);
}
