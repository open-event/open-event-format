use super::*;

#[test]
fn sequence_point_orders_epoch_before_sequence() {
    let early_epoch_high_sequence = SequencePoint {
        epoch: 1,
        sequence: u64::MAX,
    };
    let late_epoch_low_sequence = SequencePoint { epoch: 2, sequence: 0 };
    assert!(late_epoch_low_sequence > early_epoch_high_sequence);

    let lower = SequencePoint { epoch: 5, sequence: 3 };
    let higher = SequencePoint { epoch: 5, sequence: 4 };
    assert!(higher > lower);
}

#[test]
fn sequence_range_orders_epoch_before_sequences() {
    let early_epoch_high_sequences = SequenceRange {
        epoch: 1,
        first_sequence: 900,
        last_sequence: u64::MAX,
    };
    let late_epoch_low_sequences = SequenceRange {
        epoch: 2,
        first_sequence: 0,
        last_sequence: 1,
    };
    assert!(late_epoch_low_sequences > early_epoch_high_sequences);

    let lower = SequenceRange {
        epoch: 3,
        first_sequence: 0,
        last_sequence: 5,
    };
    let higher = SequenceRange {
        epoch: 3,
        first_sequence: 1,
        last_sequence: 2,
    };
    assert!(higher > lower);
}
