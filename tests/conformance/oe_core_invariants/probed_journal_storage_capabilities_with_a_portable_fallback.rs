use crate::support;
use oe::artifacts::segment::{ReplayTail, replay_segment};
use oe::file::model::{Atomicity, DurabilityMode};
use oe::invariants::sim::{Fault, SimJournalStorage};
use oe::writer::pipeline::FlushReason;

#[test]
fn unsupported_journal_fast_path_falls_back() {
    for durability_mode in [DurabilityMode::Buffered, DurabilityMode::DirectIo] {
        let atomicity = Atomicity {
            atomic_frame_multiple: 4096,
            awupf_bytes: 0,
            durability_mode,
            untorn_write_bytes: 0,
        };
        let mut world = support::World::new(42);
        world.storage = SimJournalStorage::new().with_atomicity(atomicity);
        world.ingest(3);
        let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
        assert!(!replay.frames.is_empty(), "frame durable on {durability_mode:?} path");
        assert!(
            replay.segment_chain_blake3.is_some(),
            "BLAKE3 chain verified on {durability_mode:?} path"
        );
        assert!(
            matches!(replay.tail, ReplayTail::Clean),
            "clean tail on {durability_mode:?} path"
        );
    }
}

#[test]
fn atomic_append_and_torn_tail_recovery_reach_the_same_result() {
    let committed_frames_after_crash = |untorn_bytes: u32| -> usize {
        let mut world = support::World::new(77);
        world.storage = SimJournalStorage::new().with_atomicity(Atomicity {
            atomic_frame_multiple: 4096,
            awupf_bytes: untorn_bytes,
            durability_mode: DurabilityMode::DirectIo,
            untorn_write_bytes: untorn_bytes,
        });
        world.ingest(2);
        world.storage.inject(Fault::TornTail {
            shard: support::SHARD,
            keep_bytes: 64,
        });
        world.storage.inject(Fault::FailSync { shard: support::SHARD });
        world
            .worker
            .submit(support::event(50), 1, &mut world.retry, &world.clock)
            .ok();
        let _ = world.worker.flush(
            FlushReason::Target,
            &mut world.allocator,
            &mut world.storage,
            &mut world.watermarks,
            &mut world.retry,
            &mut world.overlay,
            &world.clock,
        );
        world.storage.crash();
        let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
        assert!(
            matches!(replay.tail, ReplayTail::Truncated { .. }),
            "torn frame is truncated, never replayed"
        );
        replay.frames.len()
    };

    let with_atomic_unit = committed_frames_after_crash(4096);
    let without_atomic_unit = committed_frames_after_crash(0);
    assert_eq!(
        with_atomic_unit, without_atomic_unit,
        "committed-frame count is the same on both hosts"
    );
    assert_eq!(with_atomic_unit, 1, "exactly the one committed frame survives");
}
