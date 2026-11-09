use crate::support;
use oe::artifacts::segment::replay_segment;
use oe::events::SequencePoint;
use oe::writer::retry::{RetryReceipt, SafeRetryStore, StatusClass};
use oe::writer::sim::SimSafeRetryStore;

#[test]
fn safe_retry_row_disagrees_with_oej() {
    let mut world = support::World::new(41);
    world.ingest(2);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let mut store = SimSafeRetryStore::new();
    store.record(RetryReceipt {
        delivery_identity: (5_000, 1),
        tenant_id: support::tenant(),
        commit: SequencePoint {
            epoch: 9,
            sequence: 999,
        },
        shard: support::SHARD,
        frame_offset: 0,
        status_class: StatusClass::Acknowledged,
        expiry_physical_nanos: i64::MAX,
        dedupe: (0, 0),
    });
    store.reconstruct_from_replay(&replay.frames, support::SHARD, i64::MAX);
    let repaired = store.lookup(support::tenant(), (5_000, 1)).unwrap();
    assert_eq!(repaired.commit, SequencePoint { epoch: 1, sequence: 1 });
    assert_eq!(repaired.dedupe, (1_000, 7));
}

#[test]
fn missing_safe_retry_row_after_recovery() {
    let mut world = support::World::new(43);
    world.ingest(3);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let mut store = SimSafeRetryStore::new();
    assert!(store.is_empty());
    store.reconstruct_from_replay(&replay.frames, support::SHARD, i64::MAX);
    assert_eq!(store.len(), 3);
    let receipt = store.lookup(support::tenant(), (5_001, 1)).unwrap();
    assert_eq!(receipt.commit.sequence, 2);
    assert_eq!(receipt.dedupe, (1_001, 7));
}
