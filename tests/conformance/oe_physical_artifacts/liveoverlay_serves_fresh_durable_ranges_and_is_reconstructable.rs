use crate::support;
use oe::artifacts::overlay::{FreshRead, LiveOverlayStore};
use oe::artifacts::segment::replay_segment;
use oe::events::SequenceRange;
use oe::lifecycle::ManifestGeneration;

#[test]
fn node_behind_on_a_range() {
    let mut world = support::World::new(81);
    let range = world.ingest(3);
    let mut overlay = LiveOverlayStore::new();
    let FreshRead::Behind { missing } = overlay.fresh_read(support::tenant(), range) else {
        panic!("must report behind");
    };
    assert_eq!(missing, range);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    overlay.rebuild_from_replay(&replay, 1, 1, |_, _| false).unwrap();
    assert!(matches!(
        overlay.fresh_read(support::tenant(), range),
        FreshRead::Ready(_)
    ));
}

#[test]
fn premature_eviction_prevented() {
    let mut world = support::World::new(83);
    let range = world.ingest(2);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let mut overlay = LiveOverlayStore::new();
    overlay.rebuild_from_replay(&replay, 1, 1, |_, _| false).unwrap();
    let empty = ManifestGeneration::default();
    assert!(!empty.covers(&range, support::tenant()));
    assert_eq!(overlay.evict_covered(&empty), 0);
    assert_eq!(overlay.segment_count(), 1);
    let _ = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 2,
    };
}
