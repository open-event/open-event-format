use crate::support;
use oe::writer::pipeline::{CommitState, FlushReason, Submission};
use oe::writer::retry::SafeRetryStore;

#[test]
fn append_only_commit() {
    let mut world = support::World::new(91);
    assert_eq!(
        world
            .worker
            .submit(support::event(0), 1, &mut world.retry, &world.clock)
            .unwrap(),
        Submission::Ready
    );
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
    assert_eq!(result.receipts.len(), 1);
    assert!(
        world.retry.lookup(support::tenant(), (5_000, 1)).is_some(),
        "receipt recorded"
    );
}
