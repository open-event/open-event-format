use crate::support;
use oe::artifacts::batch::{build_batch, decode_batch, envelope_of};
use oe::events::families::{Caller, column_allowed};

#[test]
fn event_written_with_complete_envelope() {
    let input = support::event(3);
    let payload = build_batch(std::slice::from_ref(&input), 1, 0).unwrap();
    let decoded = decode_batch(&payload, 1).unwrap();
    let envelope = envelope_of(&decoded.events[0], support::tenant());
    assert_eq!(envelope, input.envelope);
    assert_eq!(envelope.occurred_at, input.envelope.occurred_at);
    assert!(!envelope.source.is_empty() && !envelope.event_type.is_empty());
}

#[test]
fn internal_sequence_key_is_a_physical_alias() {
    use oe::events::SequencePoint;
    let a = SequencePoint { epoch: 1, sequence: 9 };
    let b = SequencePoint { epoch: 2, sequence: 1 };
    assert!(a < b, "alias ordering is (epoch, sequence) tuple ordering");
    assert!(!column_allowed("sequence_key", Caller::Public));
}
