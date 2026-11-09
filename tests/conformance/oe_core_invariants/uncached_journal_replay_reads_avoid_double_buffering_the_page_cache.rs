use crate::support;
use oe::artifacts::segment::{ReplayTail, replay_segment};
use oe::file::model::{Atomicity, DurabilityMode};
use oe::invariants::sim::SimJournalStorage;

#[test]
fn replay_scan_does_not_evict_hot_pages() {
    let replay_with = |durability_mode: DurabilityMode| {
        let mut world = support::World::new(88);
        world.storage = SimJournalStorage::new().with_atomicity(Atomicity {
            atomic_frame_multiple: 4096,
            awupf_bytes: 0,
            durability_mode,
            untorn_write_bytes: 0,
        });
        world.ingest(8);
        replay_segment(&world.storage, support::SHARD, 1, 1).unwrap()
    };

    let buffered = replay_with(DurabilityMode::Buffered);
    let direct = replay_with(DurabilityMode::DirectIo);

    assert!(!buffered.frames.is_empty(), "frames recovered and BLAKE3-verified");
    assert_eq!(
        buffered.frames.len(),
        direct.frames.len(),
        "same frame count regardless of durability mode"
    );
    assert_eq!(
        buffered.segment_chain_blake3, direct.segment_chain_blake3,
        "identical BLAKE3 chain anchor"
    );
    assert_eq!(
        matches!(buffered.tail, ReplayTail::Clean),
        matches!(direct.tail, ReplayTail::Clean),
        "same tail status"
    );
}
