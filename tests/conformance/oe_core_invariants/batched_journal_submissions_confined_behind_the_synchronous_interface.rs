use crate::support;
use oe::artifacts::segment::replay_segment;
use oe::writer::pipeline::{CommitState, FlushReason};

#[test]
fn a_worker_batches_frames_without_a_global_commit_thread() {
    let run = |seed: u64| -> (CommitState, usize) {
        let mut world = support::World::new(seed);
        for i in 0..3 {
            world
                .worker
                .submit(support::event(i), 1, &mut world.retry, &world.clock)
                .unwrap();
        }
        let result = world
            .worker
            .flush(
                FlushReason::Target,
                &mut world.allocator,
                &mut world.storage,
                &mut world.watermarks,
                &mut world.retry,
                &mut world.overlay,
                &world.clock,
            )
            .unwrap();
        let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
        (result.state, replay.frames.len())
    };

    assert_eq!(run(300), run(300), "simulation is reproducible from its seed");
    let (state, frames) = run(300);
    assert_eq!(
        state,
        CommitState::Committed,
        "ack only after the durability barrier completes"
    );
    assert_eq!(frames, 1, "all events batched into one frame, no global commit thread");
}
