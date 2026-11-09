use crate::support;
use oe::artifacts::NORMAL_FRAME_SIZES;
use oe::artifacts::segment::replay_segment;
use oe::writer::pipeline::{CommitState, FlushReason};

#[test]
fn append_only_ingest_avoids_dependency_machinery() {
    let mut world = support::World::new(31);
    world
        .worker
        .submit(support::event(0), 1, &mut world.retry, &world.clock)
        .unwrap();
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
    assert_eq!(result.state, CommitState::Committed);
    assert!(NORMAL_FRAME_SIZES.contains(&result.frame_len));
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    assert_eq!(replay.frames.len(), 1, "one worker-flushed frame, no barriers");
}
