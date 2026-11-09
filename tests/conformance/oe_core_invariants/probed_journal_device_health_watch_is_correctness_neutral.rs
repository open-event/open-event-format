use crate::support;
use oe::artifacts::segment::{ReplayTail, replay_segment};
use oe::file::model::{Atomicity, DurabilityMode};
use oe::invariants::JournalStorage;
use oe::invariants::sim::{Fault, SimJournalStorage};
use oe::writer::pipeline::FlushReason;

#[test]
fn journal_device_error_raises_an_early_warning_without_gating_the_hot_path() {
    let mut world = support::World::new(55);
    world.ingest(2);
    world.ingest(2);

    world.storage.corrupt_durable_byte(support::SHARD, 100);

    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    assert!(
        matches!(
            replay.tail,
            ReplayTail::Truncated {
                trailing_nonzero: true,
                ..
            }
        ),
        "corruption caught by BLAKE3, flagged as early warning"
    );

    let extent_before = world.storage.extent(support::SHARD).unwrap();
    world.ingest(1);
    let extent_after = world.storage.extent(support::SHARD).unwrap();
    assert!(
        extent_after > extent_before,
        "hot path append+sync succeeded after corruption detection"
    );
}

#[test]
fn unsupported_host_keeps_the_reactive_path() {
    let mut world = support::World::new(66);
    world.storage = SimJournalStorage::new().with_atomicity(Atomicity {
        atomic_frame_multiple: 4096,
        awupf_bytes: 0,
        durability_mode: DurabilityMode::Buffered,
        untorn_write_bytes: 0,
    });

    world.ingest(2);

    world.storage.inject(Fault::TornTail {
        shard: support::SHARD,
        keep_bytes: 64,
    });
    world.storage.inject(Fault::FailSync { shard: support::SHARD });
    world
        .worker
        .submit(support::event(77), 1, &mut world.retry, &world.clock)
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
    assert_eq!(replay.frames.len(), 1, "committed frame intact via reactive path");
    assert!(
        matches!(replay.tail, ReplayTail::Truncated { .. }),
        "torn tail truncated, never replayed as a partial frame"
    );

    let atomicity = world.storage.atomicity(support::SHARD).unwrap();
    assert_eq!(atomicity.untorn_write_bytes, 0, "no atomic-write unit on this host");
}
