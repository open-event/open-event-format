use crate::support;
use oe::artifacts::segment::replay_segment;
use oe::events::{SequencePoint, SequenceRange};
use oe::invariants::MonotonicClock;
use oe::writer::pipeline::{FlushReason, RouteDependency, WorkerCommitPipeline};
use oe::writer::reserve::LEASE_NANOS;

#[test]
fn log_steal_cas_fails() {
    let mut world = support::World::new(61);
    world
        .worker
        .submit(support::event(0), 1, &mut world.retry, &world.clock)
        .unwrap();
    world
        .worker
        .submit(support::event(1), 1, &mut world.retry, &world.clock)
        .unwrap();
    let (region, copied) = world.worker.queue().copy_pending().unwrap();
    assert_eq!(copied.len(), 2);
    assert!(world.worker.queue_mut().commit_claim(region), "owner claims first");
    let mut thief = WorkerCommitPipeline::new(
        2,
        support::SHARD,
        support::tenant(),
        RouteDependency::AppendOnly,
        1 << 20,
    );
    assert_eq!(
        thief.steal_from(world.worker.queue_mut(), 1, true).unwrap(),
        0,
        "failed CAS discards the copy"
    );
}

#[test]
fn abandoned_reservation_closed_by_void_record() {
    let mut world = support::World::new(63);
    let lease = world.allocator.reserve(4, world.clock.monotonic_nanos());
    world.clock.advance(LEASE_NANOS + 1);
    let voided = world
        .worker
        .commit_voids(
            &mut world.allocator,
            &mut world.storage,
            &mut world.watermarks,
            &mut world.overlay,
            &world.clock,
        )
        .unwrap();
    assert_eq!(voided, vec![lease.range]);
    assert_eq!(
        world.watermarks.commit_watermark(),
        Some(SequencePoint { epoch: 1, sequence: 4 })
    );
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    assert!(replay.frames[0].header.is_void_record());
    assert_eq!(replay.frames[0].header.event_count, 0);
    let _ = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 4,
    };
}

#[test]
fn low_load_force_commit() {
    use oe::writer::reserve::FORCE_COMMIT_IDLE_NANOS;
    let mut world = support::World::new(65);
    world
        .worker
        .submit(support::event(0), 1, &mut world.retry, &world.clock)
        .unwrap();
    assert_eq!(world.worker.should_flush(&world.clock), None);
    world.clock.advance(FORCE_COMMIT_IDLE_NANOS * 2);
    assert_eq!(world.worker.should_flush(&world.clock), Some(FlushReason::ForceCommit));
    let result = world
        .worker
        .flush(
            FlushReason::ForceCommit,
            &mut world.allocator,
            &mut world.storage,
            &mut world.watermarks,
            &mut world.retry,
            &mut world.overlay,
            &world.clock,
        )
        .unwrap();
    assert_eq!(result.frame_len, 4096);
}
