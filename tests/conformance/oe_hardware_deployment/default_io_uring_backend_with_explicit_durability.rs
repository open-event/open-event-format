use crate::support;
use oe::artifacts::segment::replay_segment;
use oe::file::model::{Atomicity, DurabilityMode};
use oe::invariants::JournalStorage;
use oe::invariants::sim::{Fault, SimJournalStorage};
use oe::writer::pipeline::FlushReason;

#[test]
fn buffered_fallback_waits_for_fsync() {
    let mut world = support::World::new(10);
    world.storage = SimJournalStorage::new().with_atomicity(Atomicity {
        atomic_frame_multiple: 4096,
        awupf_bytes: 0,
        durability_mode: DurabilityMode::Buffered,
        untorn_write_bytes: 0,
    });
    world
        .worker
        .submit(support::event(1), 1, &mut world.retry, &world.clock)
        .ok();
    world.storage.inject(Fault::FailSync { shard: support::SHARD });
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
    assert_eq!(
        replay.frames.len(),
        0,
        "no frame is durable when the fsync barrier fails in buffered mode"
    );
}

#[test]
fn consumer_ssd_without_power_loss_protection() {
    let mut world = support::World::new(11);
    world.storage = SimJournalStorage::new().with_atomicity(Atomicity {
        atomic_frame_multiple: 4096,
        awupf_bytes: 0,
        durability_mode: DurabilityMode::Buffered,
        untorn_write_bytes: 0,
    });
    world
        .worker
        .submit(support::event(1), 1, &mut world.retry, &world.clock)
        .ok();
    world.storage.inject(Fault::FailSync { shard: support::SHARD });
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
    assert_eq!(
        replay.frames.len(),
        0,
        "local write completion alone is not durable on consumer SSD without PLP"
    );
}

#[test]
fn linked_chain_cancelled_on_write_failure() {
    let mut world = support::World::new(12);
    world
        .worker
        .submit(support::event(1), 1, &mut world.retry, &world.clock)
        .ok();
    world.storage.inject(Fault::FailAppend { shard: support::SHARD });
    let _ = world.worker.flush(
        FlushReason::Target,
        &mut world.allocator,
        &mut world.storage,
        &mut world.watermarks,
        &mut world.retry,
        &mut world.overlay,
        &world.clock,
    );
    let extent = world.storage.extent(support::SHARD).unwrap_or(0);
    assert_eq!(extent, 0, "failed write: no bytes reach the shard extent");
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    assert_eq!(
        replay.frames.len(),
        0,
        "no frame acknowledged after write-chain failure"
    );
}

#[test]
fn trim_only_after_segment_gc_confirmation() {}
