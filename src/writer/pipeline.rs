use super::error::{FlushError, LeaseError, QueueError};
use super::queue::{CommitQueue, PendingRecord, QueueRegion, peek_steal_raw};
use super::reserve::{FORCE_COMMIT_IDLE_NANOS, SequenceAllocator};
use super::retry::{RetryReceipt, SafeRetryStore, StatusClass};
use crate::artifacts::batch::{EventInput, build_batch, decode_batch};
use crate::artifacts::frame::{FLAG_VOID_RECORD, FrameBuildInput, build_frame, decode_frame};
use crate::artifacts::overlay::{LiveOverlayStore, convert_frame};
use crate::artifacts::watermark::WatermarkTracker;
use crate::error::FormatError;
use crate::events::{SequencePoint, SequenceRange, TenantId};
use crate::invariants::{JournalStorage, MonotonicClock, ShardId};

const ACK_REPLAY_GUARD_NANOS: i64 = 60_000_000_000;

const FORCE_COMMIT_JITTER_DIVISOR: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitState {
    Committed,
    Hardened,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteDependency {
    AppendOnly,
    Transactional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckVisibility {
    Durability,
    ReadAfterAck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    ForceCommit,
    Target,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Submission {
    Duplicate(RetryReceipt),
    Ready,
    Unresolved(RetryReceipt),
}

#[derive(Debug)]
pub struct FlushResult {
    pub frame_len: u32,
    pub frame_offset: u64,
    pub range: SequenceRange,
    pub reason: FlushReason,
    pub receipts: Vec<RetryReceipt>,
    pub state: CommitState,
}

struct StagedFrame {
    bytes: Vec<u8>,
    events: Vec<EventInput>,
    lease_id: u64,
    offset: u64,
    range: SequenceRange,
    region: QueueRegion,
}

#[derive(Debug)]
pub struct WorkerCommitPipeline {
    ack_guard_nanos: i64,
    durable_batch_counter: u64,
    flush_target_bytes: u64,
    last_activity_monotonic: u64,
    queue: CommitQueue,
    pub route: RouteDependency,
    schema_generation: u64,
    pub shard: ShardId,
    pub tenant_id: TenantId,
    pub worker_id: u32,
    writer_local_batch_counter: u64,
}

impl WorkerCommitPipeline {
    pub fn new(
        worker_id: u32,
        shard: ShardId,
        tenant_id: TenantId,
        route: RouteDependency,
        queue_capacity: usize,
    ) -> Self {
        Self {
            worker_id,
            shard,
            tenant_id,
            route,
            queue: CommitQueue::new(queue_capacity),
            flush_target_bytes: u64::from(super::super::artifacts::DEFAULT_FLUSH_TARGET),
            last_activity_monotonic: 0,
            durable_batch_counter: 0,
            writer_local_batch_counter: 0,
            schema_generation: 1,
            ack_guard_nanos: ACK_REPLAY_GUARD_NANOS,
        }
    }

    pub fn queue(&self) -> &CommitQueue {
        &self.queue
    }

    pub fn queue_mut(&mut self) -> &mut CommitQueue {
        &mut self.queue
    }

    pub fn submit(
        &mut self,
        event: EventInput,
        epoch: u64,
        retry: &mut dyn SafeRetryStore,
        clock: &dyn MonotonicClock,
    ) -> Result<Submission, QueueError> {
        if event.envelope.tenant_id != self.tenant_id {
            return Err(QueueError::TenantMismatch);
        }
        if let Some(submission) = self.earlier_attempt(&event, retry, clock) {
            return Ok(submission);
        }
        self.queue.push(&PendingRecord {
            tenant_id: self.tenant_id,
            epoch,
            event,
        })?;
        self.last_activity_monotonic = clock.monotonic_nanos();
        Ok(Submission::Ready)
    }

    pub fn submit_batch(
        &mut self,
        events: Vec<EventInput>,
        epoch: u64,
        retry: &mut dyn SafeRetryStore,
        clock: &dyn MonotonicClock,
    ) -> Result<Vec<Submission>, QueueError> {
        if events.iter().any(|event| event.envelope.tenant_id != self.tenant_id) {
            return Err(QueueError::TenantMismatch);
        }
        let mut submissions = Vec::with_capacity(events.len());
        let mut records = Vec::with_capacity(events.len());
        for event in events {
            match self.earlier_attempt(&event, retry, clock) {
                Some(submission) => submissions.push(submission),
                None => {
                    submissions.push(Submission::Ready);
                    records.push(PendingRecord {
                        tenant_id: self.tenant_id,
                        epoch,
                        event,
                    });
                }
            }
        }
        if records.is_empty() {
            return Ok(submissions);
        }
        self.queue.release_hardened();
        let Some(admission) = self.queue.admit(&records)? else {
            return Err(QueueError::Full);
        };
        self.queue.push_admitted(&admission)?;
        self.last_activity_monotonic = clock.monotonic_nanos();
        Ok(submissions)
    }

    fn earlier_attempt(
        &self,
        event: &EventInput,
        retry: &mut dyn SafeRetryStore,
        clock: &dyn MonotonicClock,
    ) -> Option<Submission> {
        let now = clock.now_nanos();
        let dedupe = (event.envelope.dedupe_hash_low, event.envelope.dedupe_hash_high);
        if let Some(receipt) = retry.acknowledged_within_guard(self.tenant_id, dedupe, now) {
            return Some(Submission::Duplicate(receipt));
        }
        let delivery_identity = (event.connector_delivery_hash_low, event.connector_delivery_hash_high);
        retry
            .lookup(self.tenant_id, delivery_identity)
            .filter(|receipt| receipt.status_class == StatusClass::Indeterminate && receipt.expiry_physical_nanos > now)
            .map(Submission::Unresolved)
    }

    pub fn steal_from(
        &mut self,
        peer: &mut CommitQueue,
        epoch: u64,
        same_steal_group: bool,
    ) -> Result<usize, QueueError> {
        let Some((region, records)) = peek_steal_raw(peer, self.tenant_id, epoch, same_steal_group) else {
            return Ok(0);
        };
        let Some((admission, count)) = self.queue.admit_raw(records)? else {
            return Ok(0);
        };
        if !peer.commit_claim(region) {
            return Ok(0);
        }
        self.queue.push_admitted(&admission)?;
        Ok(count)
    }

    pub fn should_flush(&self, clock: &dyn MonotonicClock) -> Option<FlushReason> {
        let pending = self.queue.pending_bytes();
        if pending == 0 {
            return None;
        }
        if pending >= self.flush_target_bytes {
            return Some(FlushReason::Target);
        }
        let jitter = clock.jitter(FORCE_COMMIT_IDLE_NANOS / FORCE_COMMIT_JITTER_DIVISOR);
        let idle_threshold = FORCE_COMMIT_IDLE_NANOS - jitter;
        let idle = clock.monotonic_nanos().saturating_sub(self.last_activity_monotonic);
        if idle >= idle_threshold {
            return Some(FlushReason::ForceCommit);
        }
        None
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "a flush wires every store it commits into exactly once"
    )]
    pub fn flush(
        &mut self,
        reason: FlushReason,
        allocator: &mut SequenceAllocator,
        storage: &mut dyn JournalStorage,
        watermarks: &mut WatermarkTracker,
        retry: &mut dyn SafeRetryStore,
        overlay: &mut LiveOverlayStore,
        clock: &dyn MonotonicClock,
    ) -> Result<FlushResult, FlushError> {
        let now = clock.now_nanos();
        let mut frames = Vec::new();
        if let Err(error) = self.append_frames(allocator, storage, clock, now, &mut frames) {
            self.abandon_possibly_durable(&frames, allocator, now, retry);
            return Err(error);
        }
        let (Some(first), Some(last)) = (frames.first(), frames.last()) else {
            return Err(FlushError::NothingToFlush);
        };
        let range = SequenceRange {
            epoch: first.range.epoch,
            first_sequence: first.range.first_sequence,
            last_sequence: last.range.last_sequence,
        };
        let frame_offset = first.offset;

        if let Err(error) = storage.sync(self.shard) {
            let (absent, present): (Vec<StagedFrame>, Vec<StagedFrame>) = frames.into_iter().partition(|frame| {
                storage
                    .read(self.shard, frame.offset, frame.bytes.len() as u32)
                    .is_ok_and(|stored| stored != frame.bytes)
            });
            for frame in &absent {
                self.queue.abort_flush(frame.region);
            }
            self.abandon_possibly_durable(&present, allocator, now, retry);
            return Err(FlushError::Storage(error));
        }
        self.durable_batch_counter += frames.len() as u64;
        self.writer_local_batch_counter += frames.len() as u64;

        for frame in &frames {
            let verified = storage
                .read(self.shard, frame.offset, frame.bytes.len() as u32)
                .map_err(FlushError::Storage)
                .and_then(|stored| {
                    if stored == frame.bytes {
                        Ok(())
                    } else {
                        Err(FlushError::Format(FormatError::Structural {
                            rule: "frame read back after sync differs from the frame appended",
                        }))
                    }
                });
            if let Err(error) = verified {
                self.abandon_possibly_durable(&frames, allocator, now, retry);
                return Err(error);
            }
        }

        for frame in &frames {
            if let Err(error) = allocator.harden(frame.lease_id, clock.monotonic_nanos())
                && (error != LeaseError::Expired || allocator.accept_late(frame.lease_id).is_err())
            {
                for claimed in &frames {
                    self.queue.abort_flush(claimed.region);
                }
                return Err(FlushError::Lease(error));
            }
        }
        for frame in &frames {
            watermarks.record_durable(frame.range);
            self.queue.mark_hardened(frame.region);
        }
        self.queue.release_hardened();

        let mut receipts = Vec::new();
        for frame in &frames {
            receipts.extend(self.record_receipts(
                &frame.events,
                frame.range,
                frame.offset,
                now,
                StatusClass::Acknowledged,
                retry,
            ));
        }

        let state = match self.route {
            RouteDependency::AppendOnly => CommitState::Committed,
            RouteDependency::Transactional => CommitState::Hardened,
        };
        if state == CommitState::Committed {
            for frame in &frames {
                self.publish_to_overlay(&frame.bytes, overlay)?;
            }
        }
        Ok(FlushResult {
            range,
            frame_offset,
            frame_len: frames.iter().map(|frame| frame.bytes.len() as u32).sum(),
            state,
            receipts,
            reason,
        })
    }

    fn append_frames(
        &mut self,
        allocator: &mut SequenceAllocator,
        storage: &mut dyn JournalStorage,
        clock: &dyn MonotonicClock,
        now: i64,
        frames: &mut Vec<StagedFrame>,
    ) -> Result<(), FlushError> {
        let epoch = allocator.epoch();
        loop {
            let (region, records) = self
                .queue
                .copy_matching_prefix(self.tenant_id, epoch, self.flush_target_bytes)
                .map_err(FlushError::Queue)?;
            if records.is_empty() {
                if !frames.is_empty() {
                    return Ok(());
                }
                if self.queue.pending_bytes() > 0 {
                    return Err(FlushError::MixedTenantOrEpoch);
                }
                return Err(FlushError::NothingToFlush);
            }
            if !self.queue.claim_for_flush(region) {
                return if frames.is_empty() {
                    Err(FlushError::NothingToFlush)
                } else {
                    Ok(())
                };
            }

            let lease = allocator.reserve(records.len() as u64, clock.monotonic_nanos());
            let events: Vec<EventInput> = records.into_iter().map(|record| record.event).collect();
            let batch_number = frames.len() as u64 + 1;
            match self.build_and_append(&events, lease.range, now, batch_number, storage) {
                Ok((bytes, offset)) => frames.push(StagedFrame {
                    bytes,
                    events,
                    lease_id: lease.lease_id,
                    offset,
                    range: lease.range,
                    region,
                }),
                Err(error) => {
                    self.queue.abort_flush(region);
                    return Err(error);
                }
            }
        }
    }

    fn abandon_possibly_durable(
        &mut self,
        frames: &[StagedFrame],
        allocator: &mut SequenceAllocator,
        now: i64,
        retry: &mut dyn SafeRetryStore,
    ) {
        for frame in frames {
            let _ = allocator.accept_late(frame.lease_id);
            self.record_receipts(
                &frame.events,
                frame.range,
                frame.offset,
                now,
                StatusClass::Indeterminate,
                retry,
            );
            self.queue.abort_flush(frame.region);
        }
    }

    fn record_receipts(
        &self,
        events: &[EventInput],
        range: SequenceRange,
        frame_offset: u64,
        now: i64,
        status_class: StatusClass,
        retry: &mut dyn SafeRetryStore,
    ) -> Vec<RetryReceipt> {
        let mut receipts = Vec::with_capacity(events.len());
        for (row, event) in events.iter().enumerate() {
            let receipt = RetryReceipt {
                delivery_identity: (event.connector_delivery_hash_low, event.connector_delivery_hash_high),
                tenant_id: self.tenant_id,
                commit: SequencePoint {
                    epoch: range.epoch,
                    sequence: range.first_sequence + row as u64,
                },
                shard: self.shard,
                frame_offset,
                status_class,
                expiry_physical_nanos: now + self.ack_guard_nanos,
                dedupe: (event.envelope.dedupe_hash_low, event.envelope.dedupe_hash_high),
            };
            retry.record(receipt.clone());
            receipts.push(receipt);
        }
        retry.evict_expired(now);
        receipts
    }

    fn publish_to_overlay(&self, frame_bytes: &[u8], overlay: &mut LiveOverlayStore) -> Result<(), FlushError> {
        let (header, payload) = decode_frame(frame_bytes).map_err(FlushError::Format)?;
        let decoded = decode_batch(payload, header.event_count).map_err(FlushError::Format)?;
        let segment = convert_frame(&header, &decoded, u64::from(self.shard.0), 0, None, |_, _| false)
            .map_err(FlushError::Format)?;
        if let Some(segment) = segment {
            overlay.publish(segment);
        }
        Ok(())
    }

    fn build_and_append(
        &self,
        events: &[EventInput],
        range: SequenceRange,
        now: i64,
        batch_number: u64,
        storage: &mut dyn JournalStorage,
    ) -> Result<(Vec<u8>, u64), FlushError> {
        let payload = build_batch(events, self.schema_generation, 0).map_err(FlushError::Format)?;
        let frame_bytes = build_frame(
            &FrameBuildInput {
                flags: 0,
                tenant_id: self.tenant_id,
                writer_id: self.worker_id,
                epoch: range.epoch,
                first_sequence: range.first_sequence,
                last_sequence: range.last_sequence,
                event_count: events.len() as u32,
                durable_batch_id: self.durable_batch_counter + batch_number,
                writer_local_batch_id: self.writer_local_batch_counter + batch_number,
                schema_generation: self.schema_generation,
                dictionary_generation_hint: 0,
                created_at_physical: now,
                committed_at_physical: now,
            },
            &payload,
        )
        .map_err(FlushError::Format)?;
        let frame_offset = storage.append(self.shard, &frame_bytes).map_err(FlushError::Storage)?;
        Ok((frame_bytes, frame_offset))
    }

    pub fn commit_voids(
        &mut self,
        allocator: &mut SequenceAllocator,
        storage: &mut dyn JournalStorage,
        watermarks: &mut WatermarkTracker,
        overlay: &mut LiveOverlayStore,
        clock: &dyn MonotonicClock,
    ) -> Result<Vec<SequenceRange>, FlushError> {
        let now = clock.now_nanos();
        let mut voided = Vec::new();
        let cutoff = clock.monotonic_nanos();
        let mut cursor = None;
        while let Some((lease_id, range)) = allocator.next_expired_after(cutoff, cursor) {
            cursor = Some(lease_id);
            let durable_batch_id = self.durable_batch_counter + 1;
            let writer_local_batch_id = self.writer_local_batch_counter + 1;
            let frame_bytes = build_frame(
                &FrameBuildInput {
                    flags: FLAG_VOID_RECORD,
                    tenant_id: self.tenant_id,
                    writer_id: self.worker_id,
                    epoch: range.epoch,
                    first_sequence: range.first_sequence,
                    last_sequence: range.last_sequence,
                    event_count: 0,
                    durable_batch_id,
                    writer_local_batch_id,
                    schema_generation: self.schema_generation,
                    dictionary_generation_hint: 0,
                    created_at_physical: now,
                    committed_at_physical: now,
                },
                &[],
            )
            .map_err(FlushError::Format)?;
            storage.append(self.shard, &frame_bytes).map_err(FlushError::Storage)?;
            storage.sync(self.shard).map_err(FlushError::Storage)?;
            allocator.close_voided(lease_id);
            self.durable_batch_counter = durable_batch_id;
            self.writer_local_batch_counter = writer_local_batch_id;
            watermarks.record_durable(range);
            watermarks.record_visible(range);
            overlay.publish_void(self.tenant_id, range);
            voided.push(range);
        }
        Ok(voided)
    }

    pub fn ack_ready(
        receipt: &RetryReceipt,
        state: CommitState,
        visibility: AckVisibility,
        watermarks: &WatermarkTracker,
    ) -> bool {
        if state != CommitState::Committed {
            return false;
        }
        match visibility {
            AckVisibility::Durability => true,
            AckVisibility::ReadAfterAck => watermarks.visibly_covers(receipt.commit),
        }
    }
}

#[cfg(test)]
#[path = "test/pipeline.rs"]
mod tests;
