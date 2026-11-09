use oe::events::{SequenceRange, TimestampValue};

#[test]
fn late_event_with_old_occurred_at() {
    let snapshot_range = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 20,
    };

    let late_event_range = SequenceRange {
        epoch: 1,
        first_sequence: 7,
        last_sequence: 7,
    };

    assert!(
        snapshot_range.contains(&late_event_range),
        "snapshot watermark includes the late event by sequence"
    );

    let old_occurred_at = TimestampValue::from_physical_nanos(0);
    let _ = old_occurred_at;
}
