use super::*;
use crate::artifacts::batch::PayloadInput;
use crate::artifacts::frame::decode_frame;
use crate::artifacts::overlay::FreshRead;
use crate::artifacts::segment::{ReplayTail, replay_segment};
use crate::events::variant::VariantValue;
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TimestampValue};
use crate::invariants::Clock;
use crate::invariants::sim::{Fault, SimClock, SimJournalStorage};
use crate::typed_id::TypedIdTestExt;
use crate::writer::sim::SimSafeRetryStore;

fn event(i: u64) -> EventInput {
    EventInput {
        envelope: EventEnvelope {
            event_id: EventId::new_test_id(0xAA00 + u128::from(i)),
            tenant_id: TenantId::new_test_id(9),
            stream_id: StreamId(1),
            stream_sequence: i,
            occurred_at: TimestampValue::from_physical_nanos(10 + i as i64),
            ingested_at: TimestampValue::from_physical_nanos(20 + i as i64),
            source: "crm".into(),
            event_type: "deal.updated".into(),
            entity_type: "opportunity".into(),
            entity_id_hash_low: i,
            entity_id_hash_high: 0,
            entity_id: Some(format!("opp-{i}")),
            actor_id_hash_low: 0,
            actor_id: None,
            account_id_hash_low: 0,
            account_id: None,
            trace_id_hash_low: 0,
            dedupe_hash_low: 1000 + i,
            dedupe_hash_high: 7,
            schema_version: 1,
            flags: EventFlags(0),
        },
        payload: PayloadInput::Variant(VariantValue::Int(i as i64)),
        source_schema: None,
        source_delivery: None,
        connector_delivery_hash_low: 5000 + i,
        connector_delivery_hash_high: 1,
        provenance: None,
        relationships: None,
    }
}

fn pipeline() -> WorkerCommitPipeline {
    WorkerCommitPipeline::new(
        3,
        ShardId(0),
        TenantId::new_test_id(9),
        RouteDependency::AppendOnly,
        1 << 20,
    )
}

#[test]
fn append_only_flush_commits_after_durability() {
    let clock = SimClock::new(42);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    for i in 0..3 {
        assert_eq!(
            worker.submit(event(i), 1, &mut retry, &clock).unwrap(),
            Submission::Ready
        );
    }
    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(result.state, CommitState::Committed);
    assert_eq!(result.range.first_sequence, 1);
    assert_eq!(result.range.last_sequence, 3);
    assert_eq!(retry.len(), 3);
    assert_eq!(
        watermarks.commit_watermark(),
        Some(crate::events::SequencePoint { epoch: 1, sequence: 3 })
    );
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.tail, ReplayTail::Clean);
    assert_eq!(replay.frames.len(), 1);
    assert_eq!(replay.frames[0].header.event_count, 3);
}

#[test]
fn submit_rejects_an_event_whose_tenant_does_not_match_the_worker() {
    let clock = SimClock::new(42);
    let mut retry = SimSafeRetryStore::new();
    let mut worker = pipeline();
    let mut foreign = event(0);
    foreign.envelope.tenant_id = TenantId::new_test_id(42);
    assert_eq!(
        worker.submit(foreign, 1, &mut retry, &clock),
        Err(QueueError::TenantMismatch),
        "a cross-tenant event is diagnosed as a tenant mismatch, not a codec failure"
    );
    assert_eq!(worker.queue().pending_bytes(), 0);
}

#[test]
fn a_zeroed_preallocated_tail_is_never_replayed_or_acknowledged() {
    use crate::invariants::JournalStorage;

    let clock = SimClock::new(42);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    for i in 0..3 {
        worker.submit(event(i), 1, &mut retry, &clock).unwrap();
    }
    worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();

    storage.append(ShardId(0), &vec![0u8; 4096]).unwrap();
    storage.sync(ShardId(0)).unwrap();

    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(
        replay.tail,
        ReplayTail::Clean,
        "a zeroed preallocated tail is a clean segment end, not a torn or corrupt frame",
    );
    assert_eq!(
        replay.frames.len(),
        1,
        "only the committed frame replays; the zeroed preallocated tail is never acknowledged as a frame",
    );
    assert_eq!(replay.frames[0].header.event_count, 3);
}

#[test]
fn force_commit_fires_on_idle_not_immediately() {
    let clock = SimClock::new(7);
    let mut retry = SimSafeRetryStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    assert_eq!(worker.should_flush(&clock), None);
    clock.advance(FORCE_COMMIT_IDLE_NANOS * 2);
    assert_eq!(worker.should_flush(&clock), Some(FlushReason::ForceCommit));
}

#[test]
fn force_commit_produces_minimum_frame() {
    let clock = SimClock::new(7);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    clock.advance(FORCE_COMMIT_IDLE_NANOS * 2);
    let result = worker
        .flush(
            FlushReason::ForceCommit,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(result.frame_len, 4096);
}

#[test]
fn expired_lease_closed_by_void_record() {
    let clock = SimClock::new(11);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    let lease = allocator.reserve(5, clock.monotonic_nanos());
    assert_eq!(lease.range.first_sequence, 1);
    clock.advance(super::super::reserve::LEASE_NANOS + 1);
    assert_eq!(allocator.reserve(1, clock.monotonic_nanos()).range.first_sequence, 6);
    let voided = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap();
    assert_eq!(
        voided,
        vec![SequenceRange {
            epoch: 1,
            first_sequence: 1,
            last_sequence: 5
        }]
    );
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.frames.len(), 1);
    assert!(replay.frames[0].header.is_void_record());
    assert_eq!(replay.frames[0].header.event_count, 0);
    assert_eq!(
        watermarks.commit_watermark(),
        Some(crate::events::SequencePoint { epoch: 1, sequence: 5 })
    );
}

struct LatencySimJournalStorage<'a> {
    clock: &'a SimClock,
    inner: SimJournalStorage,
    sync_latency_nanos: u64,
}

impl crate::invariants::JournalStorage for LatencySimJournalStorage<'_> {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, crate::error::StorageError> {
        self.inner.append(shard, frame)
    }

    fn sync(&mut self, shard: ShardId) -> Result<(), crate::error::StorageError> {
        let result = self.inner.sync(shard);
        self.clock.advance(self.sync_latency_nanos);
        result
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), crate::error::StorageError> {
        self.inner.truncate(shard, len)
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, crate::error::StorageError> {
        self.inner.read(shard, offset, len)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, crate::error::StorageError> {
        self.inner.extent(shard)
    }

    fn atomicity(&self, shard: ShardId) -> Result<crate::invariants::ShardAtomicity, crate::error::StorageError> {
        self.inner.atomicity(shard)
    }
}

#[test]
fn durability_barrier_outlasting_the_lease_deadline_does_not_void_the_durable_frame() {
    let clock = SimClock::new(5);
    let mut storage = LatencySimJournalStorage {
        clock: &clock,
        inner: SimJournalStorage::new(),
        sync_latency_nanos: super::super::reserve::LEASE_NANOS + 1,
    };
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();

    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .expect("a frame made durable before its lease deadline check must not be rejected");
    assert_eq!(result.state, CommitState::Committed);

    assert_eq!(allocator.outstanding_leases(), 0);
    let voided = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap();
    assert!(
        voided.is_empty(),
        "the already-durable range must not also be closed by a void record"
    );

    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.frames.len(), 1);
    assert_eq!(
        replay.frames[0].header.event_count, 1,
        "the real event frame must survive, not a void"
    );
    assert!(!replay.frames[0].header.is_void_record());
}

#[test]
fn steal_claim_failure_discards_copied_bytes() {
    let clock = SimClock::new(3);
    let mut retry = SimSafeRetryStore::new();
    let mut owner = pipeline();
    owner.submit(event(0), 1, &mut retry, &clock).unwrap();
    owner.submit(event(1), 1, &mut retry, &clock).unwrap();
    let (region, copied) = owner.queue().copy_pending().unwrap();
    assert_eq!(copied.len(), 2);
    assert!(owner.queue_mut().commit_claim(region));
    assert!(!owner.queue_mut().commit_claim(region));
    let mut thief = pipeline();
    assert_eq!(thief.steal_from(owner.queue_mut(), 1, true).unwrap(), 0);
}

#[test]
fn steal_rules_tenant_epoch_and_group() {
    let clock = SimClock::new(3);
    let mut retry = SimSafeRetryStore::new();
    let mut owner = pipeline();
    owner.submit(event(0), 1, &mut retry, &clock).unwrap();
    let mut thief = pipeline();
    assert_eq!(thief.steal_from(owner.queue_mut(), 1, false).unwrap(), 0);
    assert_eq!(thief.steal_from(owner.queue_mut(), 2, true).unwrap(), 0);
    assert_eq!(thief.steal_from(owner.queue_mut(), 1, true).unwrap(), 1);
    assert_eq!(owner.queue().pending_bytes(), 0);
    assert!(thief.queue().pending_bytes() > 0);
}

#[test]
fn steal_into_a_full_thief_claims_nothing_and_loses_no_records() {
    let clock = SimClock::new(3);
    let mut retry = SimSafeRetryStore::new();
    let mut owner = pipeline();
    for i in 0..40 {
        owner.submit(event(i), 1, &mut retry, &clock).unwrap();
    }
    let before = owner.queue().pending_bytes();
    let mut thief = WorkerCommitPipeline::new(3, ShardId(0), TenantId::new_test_id(9), RouteDependency::AppendOnly, 1);
    assert_eq!(thief.steal_from(owner.queue_mut(), 1, true).unwrap(), 0);
    assert_eq!(
        owner.queue().pending_bytes(),
        before,
        "the source keeps its records when the steal cannot be admitted"
    );
    let (_, still_there) = owner.queue().copy_pending().unwrap();
    assert_eq!(still_there.len(), 40, "no record was dropped by a rejected steal");
}

#[test]
fn steal_lets_the_source_reclaim_its_storage() {
    let clock = SimClock::new(3);
    let mut retry = SimSafeRetryStore::new();
    let mut owner = WorkerCommitPipeline::new(
        3,
        ShardId(0),
        TenantId::new_test_id(9),
        RouteDependency::AppendOnly,
        4 * 1024,
    );
    let mut thief = pipeline();
    for i in 0..64 {
        owner.submit(event(i), 1, &mut retry, &clock).unwrap();
        assert_eq!(thief.steal_from(owner.queue_mut(), 1, true).unwrap(), 1);
        assert_eq!(owner.queue().pending_bytes(), 0);
    }
}

#[test]
fn crash_loses_unsynced_frame_and_replay_is_idempotent() {
    let clock = SimClock::new(5);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    storage.inject(Fault::FailSync { shard: ShardId(0) });
    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::Storage(_)));
    storage.crash();
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert!(replay.frames.is_empty());
    let again = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(again.frames.len(), replay.frames.len());
    assert_eq!(again.tail, replay.tail);
}

#[test]
fn torn_tail_truncates_and_safe_retry_rebuilds_from_oej() {
    let clock = SimClock::new(5);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    worker.submit(event(1), 1, &mut retry, &clock).unwrap();
    storage.inject(Fault::TornTail {
        shard: ShardId(0),
        keep_bytes: 100,
    });
    storage.inject(Fault::FailSync { shard: ShardId(0) });
    let _ = worker.flush(
        FlushReason::Target,
        &mut allocator,
        &mut storage,
        &mut watermarks,
        &mut retry,
        &mut overlay,
        &clock,
    );
    storage.crash();
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.frames.len(), 1);
    assert!(matches!(replay.tail, ReplayTail::Truncated { .. }));
    let mut rebuilt = SimSafeRetryStore::new();
    rebuilt.reconstruct_from_replay(&replay.frames, ShardId(0), clock.now_nanos() + 1);
    assert_eq!(rebuilt.len(), 1);
    let receipt = rebuilt.lookup(TenantId::new_test_id(9), (5000, 1)).unwrap();
    assert_eq!(receipt.commit.sequence, 1);
    assert!(rebuilt.duplicate_within_guard(TenantId::new_test_id(9), (1000, 7), clock.now_nanos()));
}

#[test]
fn safe_retry_receipts_do_not_collide_across_tenants() {
    let tenant_a = TenantId::new_test_id(9);
    let tenant_b = TenantId::new_test_id(10);
    let delivery = (5000, 1);
    let dedupe = (1000, 7);
    let receipt = |tenant, sequence| RetryReceipt {
        commit: SequencePoint { epoch: 1, sequence },
        dedupe,
        delivery_identity: delivery,
        expiry_physical_nanos: 1_000,
        frame_offset: 0,
        shard: ShardId(0),
        status_class: StatusClass::Acknowledged,
        tenant_id: tenant,
    };
    let mut retry = SimSafeRetryStore::new();
    retry.record(receipt(tenant_a, 1));
    retry.record(receipt(tenant_b, 2));
    assert_eq!(retry.len(), 2);
    assert_eq!(retry.lookup(tenant_a, delivery).unwrap().commit.sequence, 1);
    assert_eq!(retry.lookup(tenant_b, delivery).unwrap().commit.sequence, 2);
    assert!(retry.duplicate_within_guard(tenant_a, dedupe, 0));
    assert!(retry.duplicate_within_guard(tenant_b, dedupe, 0));
    assert!(retry.lookup(TenantId::new_test_id(11), delivery).is_none());
    assert!(!retry.duplicate_within_guard(TenantId::new_test_id(11), dedupe, 0));
}

struct FailNthAppendStorage {
    fail_on_append: usize,
    inner: SimJournalStorage,
    seen_appends: usize,
}

impl crate::invariants::JournalStorage for FailNthAppendStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, crate::error::StorageError> {
        self.seen_appends += 1;
        if self.seen_appends == self.fail_on_append {
            return Err(crate::error::StorageError::InjectedFault {
                kind: "fail-nth-append",
            });
        }
        self.inner.append(shard, frame)
    }

    fn sync(&mut self, shard: ShardId) -> Result<(), crate::error::StorageError> {
        self.inner.sync(shard)
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), crate::error::StorageError> {
        self.inner.truncate(shard, len)
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, crate::error::StorageError> {
        self.inner.read(shard, offset, len)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, crate::error::StorageError> {
        self.inner.extent(shard)
    }

    fn atomicity(&self, shard: ShardId) -> Result<crate::invariants::ShardAtomicity, crate::error::StorageError> {
        self.inner.atomicity(shard)
    }
}

#[test]
fn a_failed_void_write_keeps_later_expired_ranges_outstanding() {
    let clock = SimClock::new(11);
    let mut storage = FailNthAppendStorage {
        fail_on_append: 2,
        inner: SimJournalStorage::new(),
        seen_appends: 0,
    };
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    allocator.reserve(1, clock.monotonic_nanos());
    allocator.reserve(1, clock.monotonic_nanos());
    allocator.reserve(1, clock.monotonic_nanos());
    clock.advance(super::super::reserve::LEASE_NANOS + 1);

    let error = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap_err();
    assert!(matches!(error, FlushError::Storage(_)));
    assert_eq!(
        allocator.outstanding_leases(),
        2,
        "a failed void write drops no still-uncovered range",
    );

    let voided = worker
        .commit_voids(
            &mut allocator,
            &mut storage.inner,
            &mut watermarks,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(voided.len(), 2);
    assert_eq!(allocator.outstanding_leases(), 0);
}

#[test]
fn a_sync_failure_after_a_successful_append_is_not_closed_by_a_void() {
    use crate::invariants::JournalStorage;

    let clock = SimClock::new(9);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();

    storage.inject(Fault::FailSync { shard: ShardId(0) });
    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::Storage(_)));
    assert_eq!(
        allocator.outstanding_leases(),
        0,
        "an appended range is accepted as potentially-durable, not left outstanding for a void",
    );

    clock.advance(super::super::reserve::LEASE_NANOS + 1);
    let voided = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap();
    assert!(
        voided.is_empty(),
        "no void may cover a range whose appended frame can still become durable",
    );

    storage.sync(ShardId(0)).unwrap();
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.frames.len(), 1);
    assert!(!replay.frames[0].header.is_void_record());
    assert_eq!(replay.frames[0].header.event_count, 1);
}

struct UnreadableAfterFailedSyncStorage {
    inner: SimJournalStorage,
}

impl crate::invariants::JournalStorage for UnreadableAfterFailedSyncStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, crate::error::StorageError> {
        self.inner.append(shard, frame)
    }

    fn sync(&mut self, _shard: ShardId) -> Result<(), crate::error::StorageError> {
        Err(crate::error::StorageError::Io {
            op: "fail-sync",
            detail: String::new(),
        })
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), crate::error::StorageError> {
        self.inner.truncate(shard, len)
    }

    fn read(&self, _shard: ShardId, _offset: u64, _len: u32) -> Result<Vec<u8>, crate::error::StorageError> {
        Err(crate::error::StorageError::Io {
            op: "fail-read",
            detail: String::new(),
        })
    }

    fn extent(&self, shard: ShardId) -> Result<u64, crate::error::StorageError> {
        self.inner.extent(shard)
    }

    fn atomicity(&self, shard: ShardId) -> Result<crate::invariants::ShardAtomicity, crate::error::StorageError> {
        self.inner.atomicity(shard)
    }
}

#[test]
fn an_unreadable_offset_after_a_failed_sync_is_not_treated_as_proof_the_append_is_absent() {
    let clock = SimClock::new(17);
    let mut storage = UnreadableAfterFailedSyncStorage {
        inner: SimJournalStorage::new(),
    };
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();

    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::Storage(_)));
    assert_eq!(
        allocator.outstanding_leases(),
        0,
        "an append whose presence cannot be disproved is accepted as potentially-durable",
    );
    clock.advance(super::super::reserve::LEASE_NANOS + 1);
    let voided = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap();
    assert!(
        voided.is_empty(),
        "no void may cover a range the appended frame may still occupy",
    );
}

#[test]
fn a_mixed_epoch_flush_commits_the_matching_prefix_and_drops_nothing() {
    let clock = SimClock::new(21);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    worker.submit(event(1), 1, &mut retry, &clock).unwrap();
    worker.submit(event(2), 2, &mut retry, &clock).unwrap();

    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(result.range.epoch, 1);
    assert_eq!(result.range.first_sequence, 1);
    assert_eq!(result.range.last_sequence, 2);
    let (_, remaining) = worker.queue().copy_pending().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].epoch, 2);
    let mut next_allocator = SequenceAllocator::new(2);
    let result = worker
        .flush(
            FlushReason::Target,
            &mut next_allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(result.range.epoch, 2);
    assert_eq!(result.range.first_sequence, 1);
    assert_eq!(retry.len(), 3, "every accepted record is eventually receipted");
}

#[test]
fn a_flush_whose_queue_front_mismatches_claims_and_drops_nothing() {
    let clock = SimClock::new(22);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 2, &mut retry, &clock).unwrap();

    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::MixedTenantOrEpoch));
    let (_, remaining) = worker.queue().copy_pending().unwrap();
    assert_eq!(
        remaining.len(),
        1,
        "the mismatched record stays queued instead of being claimed and dropped"
    );
    assert_eq!(
        allocator.outstanding_leases(),
        0,
        "no lease is taken for an unflushable queue"
    );
}

struct CorruptReadStorage {
    inner: SimJournalStorage,
}

impl crate::invariants::JournalStorage for CorruptReadStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, crate::error::StorageError> {
        self.inner.append(shard, frame)
    }

    fn sync(&mut self, shard: ShardId) -> Result<(), crate::error::StorageError> {
        self.inner.sync(shard)
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), crate::error::StorageError> {
        self.inner.truncate(shard, len)
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, crate::error::StorageError> {
        let mut bytes = self.inner.read(shard, offset, len)?;
        if let Some(byte) = bytes.get_mut(200) {
            *byte ^= 0xFF;
        }
        Ok(bytes)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, crate::error::StorageError> {
        self.inner.extent(shard)
    }

    fn atomicity(&self, shard: ShardId) -> Result<crate::invariants::ShardAtomicity, crate::error::StorageError> {
        self.inner.atomicity(shard)
    }
}

#[test]
fn post_sync_verification_reads_back_the_stored_bytes() {
    let clock = SimClock::new(13);
    let mut storage = CorruptReadStorage {
        inner: SimJournalStorage::new(),
    };
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();

    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::Format(_)));
    let receipt = retry
        .lookup(TenantId::new_test_id(9), (5000, 1))
        .expect("an unverified but possibly durable frame leaves retry metadata");
    assert_eq!(receipt.status_class, StatusClass::Indeterminate);
    assert!(
        !retry.duplicate_within_guard(TenantId::new_test_id(9), (1000, 7), clock.now_nanos()),
        "an indeterminate receipt is never an acknowledgement",
    );
    assert_eq!(
        watermarks.commit_watermark(),
        None,
        "durability is not recorded for an unverified frame"
    );
    assert_eq!(allocator.outstanding_leases(), 0);
    clock.advance(super::super::reserve::LEASE_NANOS + 1);
    let voided = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap();
    assert!(voided.is_empty());
}

#[test]
fn read_after_ack_acknowledges_an_earlier_epoch_once_it_is_visible() {
    let mut watermarks = WatermarkTracker::new();
    watermarks.record_visible(SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 3,
    });
    watermarks.record_visible(SequenceRange {
        epoch: 2,
        first_sequence: 1,
        last_sequence: 1,
    });
    let receipt = RetryReceipt {
        commit: SequencePoint { epoch: 1, sequence: 2 },
        dedupe: (1000, 7),
        delivery_identity: (5000, 1),
        expiry_physical_nanos: 1_000,
        frame_offset: 0,
        shard: ShardId(0),
        status_class: StatusClass::Acknowledged,
        tenant_id: TenantId::new_test_id(9),
    };
    assert!(
        WorkerCommitPipeline::ack_ready(
            &receipt,
            CommitState::Committed,
            AckVisibility::ReadAfterAck,
            &watermarks
        ),
        "an event visible in its own (earlier) epoch acknowledges even after a later epoch begins"
    );
    let unpublished = RetryReceipt {
        commit: SequencePoint { epoch: 1, sequence: 4 },
        ..receipt.clone()
    };
    assert!(!WorkerCommitPipeline::ack_ready(
        &unpublished,
        CommitState::Committed,
        AckVisibility::ReadAfterAck,
        &watermarks
    ));
}

struct LosingAppendStorage {
    inner: SimJournalStorage,
}

impl crate::invariants::JournalStorage for LosingAppendStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, crate::error::StorageError> {
        self.inner.append(shard, &vec![0u8; frame.len()])
    }

    fn sync(&mut self, _shard: ShardId) -> Result<(), crate::error::StorageError> {
        Err(crate::error::StorageError::InjectedFault { kind: "fail-sync" })
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), crate::error::StorageError> {
        self.inner.truncate(shard, len)
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, crate::error::StorageError> {
        self.inner.read(shard, offset, len)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, crate::error::StorageError> {
        self.inner.extent(shard)
    }

    fn atomicity(&self, shard: ShardId) -> Result<crate::invariants::ShardAtomicity, crate::error::StorageError> {
        self.inner.atomicity(shard)
    }
}

#[test]
fn a_sync_failure_over_bytes_that_never_landed_leaves_its_range_to_be_voided() {
    let clock = SimClock::new(23);
    let mut storage = LosingAppendStorage {
        inner: SimJournalStorage::new(),
    };
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();

    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::Storage(_)));
    assert_eq!(
        allocator.outstanding_leases(),
        1,
        "a range whose bytes never landed stays outstanding so a void can close it",
    );
    assert_eq!(retry.len(), 0, "nothing was appended, so nothing is receipted");

    clock.advance(super::super::reserve::LEASE_NANOS + 1);
    let voided = worker
        .commit_voids(
            &mut allocator,
            &mut storage.inner,
            &mut watermarks,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(voided.len(), 1);
    assert_eq!(allocator.outstanding_leases(), 0);
}

#[test]
fn a_sync_failure_over_appended_bytes_leaves_an_indeterminate_receipt_to_guard_a_retry() {
    let clock = SimClock::new(29);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    let submitted = event(0);
    let delivery = (
        submitted.connector_delivery_hash_low,
        submitted.connector_delivery_hash_high,
    );
    let dedupe = (submitted.envelope.dedupe_hash_low, submitted.envelope.dedupe_hash_high);
    worker.submit(submitted, 1, &mut retry, &clock).unwrap();

    storage.inject(Fault::FailSync { shard: ShardId(0) });
    assert!(matches!(
        worker
            .flush(
                FlushReason::Target,
                &mut allocator,
                &mut storage,
                &mut watermarks,
                &mut retry,
                &mut overlay,
                &clock,
            )
            .unwrap_err(),
        FlushError::Storage(_)
    ));

    let receipt = retry
        .lookup(worker.tenant_id, delivery)
        .expect("a retry of this delivery must find where the appended events went");
    assert_eq!(receipt.status_class, StatusClass::Indeterminate);
    assert_eq!(receipt.commit.sequence, 1);
    assert!(
        !retry.duplicate_within_guard(worker.tenant_id, dedupe, 0),
        "an indeterminate receipt is not an acknowledgement, so it never arms the replay guard",
    );
    assert_eq!(
        watermarks.commit_watermark(),
        None,
        "durability is not recorded for an unsynced frame"
    );
}

#[test]
fn a_retry_after_a_failed_sync_is_not_appended_a_second_time() {
    use crate::invariants::JournalStorage;

    let clock = SimClock::new(61);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    storage.inject(Fault::FailSync { shard: ShardId(0) });
    assert!(matches!(
        worker
            .flush(
                FlushReason::Target,
                &mut allocator,
                &mut storage,
                &mut watermarks,
                &mut retry,
                &mut overlay,
                &clock,
            )
            .unwrap_err(),
        FlushError::Storage(_)
    ));
    let indeterminate = retry.lookup(worker.tenant_id, (5000, 1)).unwrap();
    assert_eq!(indeterminate.status_class, StatusClass::Indeterminate);

    assert_eq!(
        worker.submit(event(0), 1, &mut retry, &clock).unwrap(),
        Submission::Unresolved(indeterminate.clone()),
        "a retry of a delivery of unknown durability is held, not queued"
    );
    assert_eq!(
        worker
            .submit_batch(vec![event(0), event(1)], 1, &mut retry, &clock)
            .unwrap(),
        vec![Submission::Unresolved(indeterminate), Submission::Ready],
        "a batch retry holds only the unresolved delivery"
    );
    assert!(
        worker.queue().pending_bytes() > 0,
        "the new event in the batch is queued"
    );
    worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();

    storage.sync(ShardId(0)).unwrap();
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    retry.reconstruct_from_replay(&replay.frames, ShardId(0), clock.now_nanos() + ACK_REPLAY_GUARD_NANOS);
    let settled = retry.lookup(worker.tenant_id, (5000, 1)).unwrap();
    assert_eq!(settled.status_class, StatusClass::Acknowledged);
    assert_eq!(settled.commit.sequence, 1);
    assert_eq!(
        worker.submit(event(0), 1, &mut retry, &clock).unwrap(),
        Submission::Duplicate(settled)
    );

    let copies: usize = replay
        .frames
        .iter()
        .map(|frame| {
            let (header, payload) = decode_frame(&frame.frame_bytes).unwrap();
            decode_batch(payload, header.event_count)
                .unwrap()
                .events
                .iter()
                .filter(|event| {
                    (
                        event.variable.connector_delivery_hash_low,
                        event.variable.connector_delivery_hash_high,
                    ) == (5000, 1)
                })
                .count()
        })
        .sum();
    assert_eq!(
        replay.frames.len(),
        2,
        "the first attempt and the new event, nothing else"
    );
    assert_eq!(copies, 1, "the retried delivery is stored once");
}

struct MisdirectedReadStorage {
    inner: SimJournalStorage,
    other_frame: Vec<u8>,
}

impl crate::invariants::JournalStorage for MisdirectedReadStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, crate::error::StorageError> {
        self.inner.append(shard, frame)
    }

    fn sync(&mut self, shard: ShardId) -> Result<(), crate::error::StorageError> {
        self.inner.sync(shard)
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), crate::error::StorageError> {
        self.inner.truncate(shard, len)
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, crate::error::StorageError> {
        if len as usize == self.other_frame.len() {
            return Ok(self.other_frame.clone());
        }
        self.inner.read(shard, offset, len)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, crate::error::StorageError> {
        self.inner.extent(shard)
    }

    fn atomicity(&self, shard: ShardId) -> Result<crate::invariants::ShardAtomicity, crate::error::StorageError> {
        self.inner.atomicity(shard)
    }
}

#[test]
fn post_sync_verification_rejects_a_different_valid_frame_read_back() {
    let clock = SimClock::new(31);
    let other_frame = {
        let mut worker = pipeline();
        let mut allocator = SequenceAllocator::new(1);
        let mut watermarks = WatermarkTracker::new();
        let mut retry = SimSafeRetryStore::new();
        let mut overlay = LiveOverlayStore::new();
        let mut storage = SimJournalStorage::new();
        worker.submit(event(41), 1, &mut retry, &clock).unwrap();
        let result = worker
            .flush(
                FlushReason::Target,
                &mut allocator,
                &mut storage,
                &mut watermarks,
                &mut retry,
                &mut overlay,
                &clock,
            )
            .unwrap();
        crate::invariants::JournalStorage::read(&storage, ShardId(0), result.frame_offset, result.frame_len).unwrap()
    };

    let mut storage = MisdirectedReadStorage {
        inner: SimJournalStorage::new(),
        other_frame,
    };
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();

    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::Format(_)));
    assert_eq!(
        retry
            .lookup(TenantId::new_test_id(9), (5000, 1))
            .map(|receipt| receipt.status_class),
        Some(StatusClass::Indeterminate),
    );
    assert_eq!(
        watermarks.commit_watermark(),
        None,
        "durability is not recorded for a frame that could not be verified"
    );
}

#[test]
fn a_committed_event_is_readable_through_fresh_read_as_soon_as_flush_returns() {
    let clock = SimClock::new(37);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    for i in 0..3 {
        worker.submit(event(i), 1, &mut retry, &clock).unwrap();
    }

    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();

    let FreshRead::Ready(segments) = overlay.fresh_read(worker.tenant_id, result.range) else {
        panic!("a committed frame must be in the overlay when flush returns");
    };
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].range(), result.range);
    assert_eq!(segments[0].batch.num_rows(), 3);
}

#[test]
fn a_hardened_transactional_frame_is_not_published_to_the_overlay() {
    let clock = SimClock::new(41);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = WorkerCommitPipeline::new(
        3,
        ShardId(0),
        TenantId::new_test_id(9),
        RouteDependency::Transactional,
        1 << 20,
    );
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();

    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(result.state, CommitState::Hardened);
    assert_eq!(overlay.segment_count(), 0, "only COMMITTED frames become visible");
}

#[test]
fn a_committed_void_is_passed_over_by_fresh_read() {
    let clock = SimClock::new(43);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    allocator.reserve(2, clock.monotonic_nanos());
    clock.advance(super::super::reserve::LEASE_NANOS + 1);

    let voided = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap();
    assert_eq!(voided.len(), 1);
    assert!(matches!(
        overlay.fresh_read(worker.tenant_id, voided[0]),
        FreshRead::Ready(segments) if segments.is_empty()
    ));
}

#[test]
fn a_retry_inside_the_guard_window_commits_once_and_returns_the_original_receipt() {
    let clock = SimClock::new(47);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    assert_eq!(
        worker.submit(event(0), 1, &mut retry, &clock).unwrap(),
        Submission::Ready
    );
    let first = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    let original = first.receipts[0].clone();

    assert_eq!(
        worker.submit(event(0), 1, &mut retry, &clock).unwrap(),
        Submission::Duplicate(original),
        "the retry is answered with the first commit's receipt"
    );
    assert_eq!(worker.queue().pending_bytes(), 0, "the retry is not queued");
    assert!(matches!(
        worker.flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        ),
        Err(FlushError::NothingToFlush)
    ));
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.frames.len(), 1, "the event is stored once");
}

#[test]
fn a_batch_retry_skips_only_the_events_already_committed() {
    let clock = SimClock::new(53);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    let first = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();

    let submissions = worker
        .submit_batch(vec![event(0), event(1)], 1, &mut retry, &clock)
        .unwrap();
    assert_eq!(
        submissions,
        vec![Submission::Duplicate(first.receipts[0].clone()), Submission::Ready]
    );
    let second = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(second.receipts.len(), 1, "only the new event commits");
    assert_eq!(second.receipts[0].dedupe, (1001, 7));
}

#[test]
fn a_retry_after_the_guard_window_is_queued_again() {
    let clock = SimClock::new(59);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    worker.submit(event(0), 1, &mut retry, &clock).unwrap();
    worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();

    clock.advance(ACK_REPLAY_GUARD_NANOS as u64 + 1);
    assert_eq!(
        worker.submit(event(0), 1, &mut retry, &clock).unwrap(),
        Submission::Ready
    );
}

struct CountingStorage {
    appends: u32,
    fail_append: Option<u32>,
    inner: SimJournalStorage,
    syncs: u32,
}

impl CountingStorage {
    fn new(fail_append: Option<u32>) -> Self {
        Self {
            appends: 0,
            fail_append,
            inner: SimJournalStorage::new(),
            syncs: 0,
        }
    }
}

impl crate::invariants::JournalStorage for CountingStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, crate::error::StorageError> {
        self.appends += 1;
        if self.fail_append == Some(self.appends) {
            return Err(crate::error::StorageError::InjectedFault { kind: "append" });
        }
        self.inner.append(shard, frame)
    }

    fn sync(&mut self, shard: ShardId) -> Result<(), crate::error::StorageError> {
        self.syncs += 1;
        self.inner.sync(shard)
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), crate::error::StorageError> {
        self.inner.truncate(shard, len)
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, crate::error::StorageError> {
        self.inner.read(shard, offset, len)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, crate::error::StorageError> {
        self.inner.extent(shard)
    }

    fn atomicity(&self, shard: ShardId) -> Result<crate::invariants::ShardAtomicity, crate::error::StorageError> {
        self.inner.atomicity(shard)
    }
}

fn queue_past_three_targets(worker: &mut WorkerCommitPipeline, retry: &mut SimSafeRetryStore, clock: &SimClock) -> u64 {
    let mut count = 0;
    while worker.queue().pending_bytes() <= 3 * u64::from(crate::artifacts::DEFAULT_FLUSH_TARGET) {
        worker.submit(event(count), 1, retry, clock).unwrap();
        count += 1;
    }
    count
}

#[test]
fn a_burst_past_the_flush_target_commits_as_several_frames_behind_one_sync() {
    let clock = SimClock::new(61);
    let mut storage = CountingStorage::new(None);
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    let count = queue_past_three_targets(&mut worker, &mut retry, &clock);
    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(
        storage.syncs, 1,
        "every frame of the flush shares one durability barrier"
    );
    assert!(storage.appends >= 4, "{} frames", storage.appends);
    assert_eq!((result.range.first_sequence, result.range.last_sequence), (1, count));
    assert_eq!(result.receipts.len() as u64, count);
    assert_eq!(
        watermarks.commit_watermark(),
        Some(SequencePoint {
            epoch: 1,
            sequence: count
        })
    );
    assert_eq!(worker.queue().pending_bytes(), 0);

    let replay = replay_segment(&storage.inner, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.tail, ReplayTail::Clean);
    assert_eq!(replay.frames.len() as u32, storage.appends);
    let mut next = 1;
    for frame in &replay.frames {
        assert_eq!(frame.header.first_sequence, next);
        assert!(frame.header.frame_len <= crate::artifacts::MAX_NORMAL_FRAME);
        next = frame.header.last_sequence + 1;
    }
    assert_eq!(next, count + 1);
    assert_eq!(
        result.frame_offset + u64::from(result.frame_len),
        storage.inner.extent(ShardId(0)).unwrap()
    );
    for receipt in &result.receipts {
        let frame = replay
            .frames
            .iter()
            .find(|frame| (frame.header.first_sequence..=frame.header.last_sequence).contains(&receipt.commit.sequence))
            .unwrap();
        assert_eq!(receipt.frame_offset, frame.offset);
        assert_eq!(receipt.status_class, StatusClass::Acknowledged);
    }
}

#[test]
fn a_frame_failing_to_append_mid_flush_leaves_the_frames_before_it_possibly_durable() {
    let clock = SimClock::new(67);
    let mut storage = CountingStorage::new(Some(2));
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    let count = queue_past_three_targets(&mut worker, &mut retry, &clock);
    let error = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap_err();
    assert!(matches!(error, FlushError::Storage(_)));
    assert_eq!(storage.syncs, 0, "nothing is synced once a frame fails to append");
    assert_eq!(watermarks.commit_watermark(), None);

    let first = retry.lookup(TenantId::new_test_id(9), (5000, 1)).unwrap();
    assert_eq!(first.status_class, StatusClass::Indeterminate);
    assert!(retry.lookup(TenantId::new_test_id(9), (5000 + count - 1, 1)).is_none());
    assert!(worker.queue().pending_bytes() > 0);
    assert_eq!(allocator.outstanding_leases(), 1);
    clock.advance(super::super::reserve::LEASE_NANOS + 1);
    let voided = worker
        .commit_voids(&mut allocator, &mut storage, &mut watermarks, &mut overlay, &clock)
        .unwrap();
    assert_eq!(voided.len(), 1);
    assert!(voided[0].first_sequence > first.commit.sequence);

    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert_eq!(result.range.first_sequence, voided[0].last_sequence + 1);
    assert_eq!(worker.queue().pending_bytes(), 0);
}

#[test]
fn on_disk_journal_and_retry_store_survive_a_restart_and_rebuild_from_replay() {
    use crate::invariants::JournalStorage;
    use crate::invariants::disk::FileJournalStorage;
    use crate::writer::disk::FileSafeRetryStore;

    let dir = tempfile::tempdir().unwrap();
    let clock = SimClock::new(5);
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut overlay = LiveOverlayStore::new();
    {
        let mut storage = FileJournalStorage::open(dir.path().join("journal")).unwrap();
        let mut retry = FileSafeRetryStore::open(dir.path().join("retry")).unwrap();
        let mut worker = pipeline();
        worker.submit(event(0), 1, &mut retry, &clock).unwrap();
        worker
            .flush(
                FlushReason::Target,
                &mut allocator,
                &mut storage,
                &mut watermarks,
                &mut retry,
                &mut overlay,
                &clock,
            )
            .unwrap();
        assert!(retry.take_failure().is_none());
    }
    let journal_dir = dir.path().join("journal");
    let mut storage = FileJournalStorage::open(&journal_dir).unwrap();
    let path = storage.shard_path(ShardId(0));
    let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    std::io::Write::write_all(&mut file, &[0x5A; 100]).unwrap();
    drop(file);

    let mut retry = FileSafeRetryStore::open(dir.path().join("retry")).unwrap();
    assert!(retry.lookup(TenantId::new_test_id(9), (5000, 1)).is_some());
    assert!(retry.duplicate_within_guard(TenantId::new_test_id(9), (1000, 7), clock.now_nanos()));

    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    assert_eq!(replay.frames.len(), 1);
    let ReplayTail::Truncated { offset: valid_len, .. } = replay.tail else {
        panic!("torn tail not detected: {:?}", replay.tail);
    };
    storage.truncate(ShardId(0), valid_len).unwrap();
    assert_eq!(storage.extent(ShardId(0)).unwrap(), valid_len);

    let mut rebuilt = FileSafeRetryStore::open(dir.path().join("rebuilt")).unwrap();
    rebuilt.reconstruct_from_replay(&replay.frames, ShardId(0), clock.now_nanos() + 1);
    drop(rebuilt);
    let rebuilt = FileSafeRetryStore::open(dir.path().join("rebuilt")).unwrap();
    assert_eq!(rebuilt.len(), 1);
    assert_eq!(
        rebuilt.lookup(TenantId::new_test_id(9), (5000, 1)).unwrap().commit,
        retry.lookup(TenantId::new_test_id(9), (5000, 1)).unwrap().commit
    );
}

/// A safe-retry store that counts how its receipts arrive.
struct CountingRetryStore {
    batches: Vec<usize>,
    inner: SimSafeRetryStore,
    singles: usize,
}

impl SafeRetryStore for CountingRetryStore {
    fn record(&mut self, receipt: RetryReceipt) {
        self.singles += 1;
        self.inner.record(receipt);
    }
    fn record_batch(&mut self, receipts: &[RetryReceipt]) {
        self.batches.push(receipts.len());
        for receipt in receipts {
            self.inner.record(receipt.clone());
        }
    }
    fn evict_expired(&mut self, now_physical_nanos: i64) {
        self.inner.evict_expired(now_physical_nanos);
    }
    fn lookup(&self, tenant_id: TenantId, delivery_identity: (u64, u64)) -> Option<RetryReceipt> {
        self.inner.lookup(tenant_id, delivery_identity)
    }
    fn acknowledged_within_guard(
        &mut self,
        tenant_id: TenantId,
        dedupe: (u64, u64),
        now_physical_nanos: i64,
    ) -> Option<RetryReceipt> {
        self.inner
            .acknowledged_within_guard(tenant_id, dedupe, now_physical_nanos)
    }
}

#[test]
fn a_flush_records_the_receipts_of_all_its_frames_as_one_batch() {
    let clock = SimClock::new(61);
    let mut storage = CountingStorage::new(None);
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = CountingRetryStore {
        batches: Vec::new(),
        inner: SimSafeRetryStore::new(),
        singles: 0,
    };
    let mut overlay = LiveOverlayStore::new();
    let mut worker = pipeline();
    let count = queue_past_three_targets(&mut worker, &mut retry.inner, &clock);
    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    assert!(storage.appends >= 4, "{} frames", storage.appends);
    assert_eq!(retry.batches, vec![count as usize]);
    assert_eq!(retry.singles, 0);
    assert_eq!(result.receipts.len() as u64, count);
    for receipt in &result.receipts {
        assert_eq!(
            retry.lookup(receipt.tenant_id, receipt.delivery_identity).as_ref(),
            Some(receipt)
        );
    }
}
