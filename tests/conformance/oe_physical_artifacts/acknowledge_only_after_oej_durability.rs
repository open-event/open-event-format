use crate::support;
use oe::events::SequenceRange;
use oe::writer::pipeline::{AckVisibility, CommitState, FlushReason, WorkerCommitPipeline};

#[test]
fn strict_read_after_ack_visibility() {
    let mut world = support::World::new(71);
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
    let receipt = &result.receipts[0];
    assert!(WorkerCommitPipeline::ack_ready(
        receipt,
        result.state,
        AckVisibility::Durability,
        &world.watermarks
    ));
    assert!(!WorkerCommitPipeline::ack_ready(
        receipt,
        result.state,
        AckVisibility::ReadAfterAck,
        &world.watermarks
    ));
    world.watermarks.record_visible(SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 1,
    });
    assert!(WorkerCommitPipeline::ack_ready(
        receipt,
        result.state,
        AckVisibility::ReadAfterAck,
        &world.watermarks
    ));
}
