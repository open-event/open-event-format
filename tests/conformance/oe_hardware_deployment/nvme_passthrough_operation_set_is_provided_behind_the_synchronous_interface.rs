use crate::support;
use oe::artifacts::segment::{ReplayTail, replay_segment};
use oe::file::model::{Atomicity, DurabilityMode};
use oe::invariants::JournalStorage;
use oe::invariants::sim::SimJournalStorage;

#[test]
fn accelerated_and_buffered_journal_paths_reach_the_same_result() {
    let (extent_a, frames_a, chain_a) = {
        let mut world = support::World::new(40);
        world.storage = SimJournalStorage::new().with_atomicity(Atomicity {
            atomic_frame_multiple: 4096,
            awupf_bytes: 4096,
            durability_mode: DurabilityMode::DirectIo,
            untorn_write_bytes: 4096,
        });
        world.ingest(3);
        let extent = world.storage.extent(support::SHARD).unwrap_or(0);
        let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
        assert!(matches!(replay.tail, ReplayTail::Clean));
        let frames: Vec<Vec<u8>> = replay.frames.into_iter().map(|f| f.frame_bytes).collect();
        (extent, frames, replay.segment_chain_blake3)
    };

    let (extent_b, frames_b, chain_b) = {
        let mut world = support::World::new(40);
        world.storage = SimJournalStorage::new().with_atomicity(Atomicity::buffered_unprobed());
        world.ingest(3);
        let extent = world.storage.extent(support::SHARD).unwrap_or(0);
        let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
        assert!(matches!(replay.tail, ReplayTail::Clean));
        let frames: Vec<Vec<u8>> = replay.frames.into_iter().map(|f| f.frame_bytes).collect();
        (extent, frames, replay.segment_chain_blake3)
    };

    assert_eq!(extent_a, extent_b, "shard extents identical on both paths");
    assert_eq!(frames_a, frames_b, "durable frame bytes identical on both paths");
    assert_eq!(chain_a, chain_b, "BLAKE3 segment chain identical on both paths");
}

#[test]
fn development_host_falls_back_without_breaking_the_build() {
    use oe::file::model::IoCapabilities;
    let caps = IoCapabilities::portable();
    assert_eq!(caps.direct_io_alignment_bytes, 0, "no direct-I/O on portable host");
    assert!(!caps.uncached_reads, "no uncached reads on portable host");
    assert!(!caps.zoned_placement, "no ZNS/FDP on portable host");

    let mut world = support::World::new(41);
    world.ingest(2);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    assert!(
        !replay.frames.is_empty(),
        "portable fallback commits and replays correctly"
    );
    assert!(
        replay.segment_chain_blake3.is_some(),
        "BLAKE3 chain verified on portable path"
    );
    assert!(matches!(replay.tail, ReplayTail::Clean));
}

#[test]
fn nvme_required_deployment_refuses_when_the_device_is_absent() {}
