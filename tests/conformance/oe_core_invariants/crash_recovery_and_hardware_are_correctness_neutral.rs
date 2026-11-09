use crate::support;
use oe::artifacts::overlay::{FreshRead, LiveOverlayStore};
use oe::artifacts::segment::replay_segment;
use oe::events::SequenceRange;

#[test]
fn fresh_query_after_restart_waits_for_rebuild() {
    let mut world = support::World::new(21);
    let range = world.ingest(3);
    world.storage.crash();
    let mut overlay = LiveOverlayStore::new();
    assert!(matches!(
        overlay.fresh_read(support::tenant(), range),
        FreshRead::Behind { .. }
    ));
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    overlay.rebuild_from_replay(&replay, 1, 1, |_, _| false).unwrap();
    assert!(matches!(
        overlay.fresh_read(support::tenant(), range),
        FreshRead::Ready(_)
    ));
    let again = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    assert_eq!(again.frames.len(), replay.frames.len());
    let _ = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 3,
    };
}
