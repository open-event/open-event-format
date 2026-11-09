use crate::support;
use oe::artifacts::segment::replay_segment;
use oe::invariants::Clock;
use oe::writer::retry::SafeRetryStore;
use oe::writer::sim::SimSafeRetryStore;

#[test]
fn replay_within_guard_window() {
    let mut world = support::World::new(101);
    world.ingest(2);
    let now = world.clock.now_nanos();
    assert!(world.retry.duplicate_within_guard(support::tenant(), (1_000, 7), now));
    assert!(world.retry.duplicate_within_guard(support::tenant(), (1_001, 7), now));
    assert!(!world.retry.duplicate_within_guard(support::tenant(), (9_999, 7), now));
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let mut rebuilt = SimSafeRetryStore::new();
    rebuilt.reconstruct_from_replay(&replay.frames, support::SHARD, now + 1);
    assert!(rebuilt.duplicate_within_guard(support::tenant(), (1_000, 7), now));
}
