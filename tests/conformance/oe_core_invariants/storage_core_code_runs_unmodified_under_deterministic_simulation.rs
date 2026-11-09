use crate::support;
use oe::artifacts::segment::{ReplayTail, replay_segment};
use oe::invariants::sim::Fault;
use oe::writer::pipeline::FlushReason;

#[test]
fn fault_injection_requires_no_production_code_changes() {
    let run = |seed: u64| -> (usize, bool) {
        let mut world = support::World::new(seed);
        world.ingest(2);
        world
            .worker
            .submit(support::event(7), 1, &mut world.retry, &world.clock)
            .unwrap();
        world.storage.inject(Fault::TornTail {
            shard: support::SHARD,
            keep_bytes: 64,
        });
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
        let torn = matches!(replay.tail, ReplayTail::Truncated { .. });
        (replay.frames.len(), torn)
    };
    assert_eq!(run(42), run(42));
    let (frames, torn) = run(42);
    assert_eq!(frames, 1, "the durable frame survives; the torn one is truncated");
    assert!(torn);
}
