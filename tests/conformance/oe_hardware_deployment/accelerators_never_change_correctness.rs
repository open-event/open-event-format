use crate::support;
use oe::artifacts::segment::{ReplayTail, replay_segment};
use oe::file::model::{Atomicity, DurabilityMode};
use oe::invariants::sim::SimJournalStorage;

#[test]
fn accelerator_path_matches_software_path() {
    let frames_and_chain = |atomicity: Atomicity| -> (Option<[u8; 32]>, Vec<Vec<u8>>) {
        let mut world = support::World::new(20);
        world.storage = SimJournalStorage::new().with_atomicity(atomicity);
        world.ingest(3);
        let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
        assert!(matches!(replay.tail, ReplayTail::Clean));
        assert!(
            replay.segment_chain_blake3.is_some(),
            "BLAKE3 chain present on both paths"
        );
        (
            replay.segment_chain_blake3,
            replay.frames.into_iter().map(|f| f.frame_bytes).collect(),
        )
    };

    let (chain_fast, frames_fast) = frames_and_chain(Atomicity {
        atomic_frame_multiple: 4096,
        awupf_bytes: 4096,
        durability_mode: DurabilityMode::DirectIo,
        untorn_write_bytes: 4096,
    });
    let (chain_soft, frames_soft) = frames_and_chain(Atomicity::buffered_unprobed());

    assert_eq!(
        frames_fast, frames_soft,
        "frame bytes identical on accelerated and software paths"
    );
    assert_eq!(chain_fast, chain_soft, "BLAKE3 segment chain identical on both paths");
}
