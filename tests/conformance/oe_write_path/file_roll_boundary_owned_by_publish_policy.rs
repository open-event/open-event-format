use oe::invariants::MonotonicClock;
use oe::invariants::sim::SimClock;
use oe::writer::publish::{OpenFileState, RollPolicy, RollTrigger, should_roll};

#[test]
fn high_volume_tenant_rolls_on_bytes() {
    let clock = SimClock::new(7);
    let policy = RollPolicy::default();
    let state = OpenFileState {
        opened_at_monotonic_nanos: clock.monotonic_nanos(),
        compressed_bytes: policy.byte_target + 1,
    };
    assert_eq!(should_roll(&state, &policy, &clock), Some(RollTrigger::ByteTarget));
}

#[test]
fn low_volume_tenant_rolls_on_time() {
    let clock = SimClock::new(7);
    let policy = RollPolicy::default();
    let state = OpenFileState {
        opened_at_monotonic_nanos: clock.monotonic_nanos(),
        compressed_bytes: 512,
    };
    assert_eq!(should_roll(&state, &policy, &clock), None);
    clock.advance(policy.max_open_nanos + 1);
    assert_eq!(should_roll(&state, &policy, &clock), Some(RollTrigger::TimeWindow));
}
