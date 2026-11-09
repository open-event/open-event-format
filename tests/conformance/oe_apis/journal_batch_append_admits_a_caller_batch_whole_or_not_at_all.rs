use crate::support;
use oe::artifacts::batch::EventInput;
use oe::writer::error::QueueError;
use oe::writer::pipeline::{FlushReason, RouteDependency, Submission, WorkerCommitPipeline};
use oe::writer::queue::PendingRecord;

#[test]
fn a_batch_that_does_not_fit_admits_nothing() {
    let mut world = support::World::new(7);
    let mut worker = WorkerCommitPipeline::new(
        1,
        support::SHARD,
        support::tenant(),
        RouteDependency::AppendOnly,
        8 * 1024,
    );
    let batch: Vec<EventInput> = (100..106).map(support::event).collect();
    let batch_records: Vec<PendingRecord> = batch
        .iter()
        .cloned()
        .map(|event| PendingRecord {
            epoch: 1,
            event,
            tenant_id: support::tenant(),
        })
        .collect();

    let mut fillers = 0u64;
    while worker.queue().can_admit(&batch_records).unwrap() {
        worker
            .submit(support::event(fillers), 1, &mut world.retry, &world.clock)
            .unwrap();
        fillers += 1;
    }

    let clean_before = worker.queue().clean_cursor();
    let dirty_before = worker.queue().dirty_cursor();
    let pending_before = worker.queue().pending_bytes();
    assert_eq!(
        worker.submit_batch(batch.clone(), 1, &mut world.retry, &world.clock),
        Err(QueueError::Full),
        "a batch the queue cannot hold whole is refused as one unit"
    );
    assert_eq!(worker.queue().clean_cursor(), clean_before);
    assert_eq!(worker.queue().dirty_cursor(), dirty_before);
    assert_eq!(worker.queue().pending_bytes(), pending_before);

    let result = worker
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
    assert_eq!(result.receipts.len(), fillers as usize);

    assert_eq!(
        worker.submit_batch(batch, 1, &mut world.retry, &world.clock).unwrap(),
        vec![Submission::Ready; 6]
    );
    let result = worker
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
    assert_eq!(result.receipts.len(), 6);
    assert_eq!(
        result.range.last_sequence - result.range.first_sequence + 1,
        6,
        "the admitted batch occupies one contiguous sequence range"
    );
}

#[test]
fn an_admitted_batch_commits_one_contiguous_sub_range_in_submission_order() {
    let mut world = support::World::new(11);
    world
        .worker
        .submit(support::event(0), 1, &mut world.retry, &world.clock)
        .unwrap();
    world
        .worker
        .submit(support::event(1), 1, &mut world.retry, &world.clock)
        .unwrap();
    let batch: Vec<EventInput> = (200..203).map(support::event).collect();
    assert_eq!(
        world
            .worker
            .submit_batch(batch, 1, &mut world.retry, &world.clock)
            .unwrap(),
        vec![Submission::Ready; 3]
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
    assert_eq!(result.range.first_sequence, 1);
    assert_eq!(result.range.last_sequence, 5);
    assert_eq!(result.receipts.len(), 5, "one receipt position per submitted event");

    for (row, receipt) in result.receipts[2..].iter().enumerate() {
        assert_eq!(receipt.commit.sequence, 3 + row as u64);
        assert_eq!(receipt.delivery_identity, (5_000 + 200 + row as u64, 1));
    }
}
