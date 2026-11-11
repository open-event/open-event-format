use crate::artifacts::batch::decode_batch;
use crate::artifacts::frame::decode_frame;
use crate::artifacts::segment::ReplayedFrame;
use crate::events::{SequencePoint, TenantId};
use crate::invariants::ShardId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    Acknowledged,
    Indeterminate,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryReceipt {
    pub commit: SequencePoint,
    pub dedupe: (u64, u64),
    pub delivery_identity: (u64, u64),
    pub expiry_physical_nanos: i64,
    pub frame_offset: u64,
    pub shard: ShardId,
    pub status_class: StatusClass,
    pub tenant_id: TenantId,
}

pub trait SafeRetryStore {
    fn record(&mut self, receipt: RetryReceipt);

    fn record_batch(&mut self, receipts: &[RetryReceipt]) {
        for receipt in receipts {
            self.record(receipt.clone());
        }
    }

    fn evict_expired(&mut self, now_physical_nanos: i64);

    fn lookup(&self, tenant_id: TenantId, delivery_identity: (u64, u64)) -> Option<RetryReceipt>;

    fn acknowledged_within_guard(
        &mut self,
        tenant_id: TenantId,
        dedupe: (u64, u64),
        now_physical_nanos: i64,
    ) -> Option<RetryReceipt>;

    fn duplicate_within_guard(&mut self, tenant_id: TenantId, dedupe: (u64, u64), now_physical_nanos: i64) -> bool {
        self.acknowledged_within_guard(tenant_id, dedupe, now_physical_nanos)
            .is_some()
    }

    fn reconstruct_from_replay(&mut self, frames: &[ReplayedFrame], shard: ShardId, guard_expiry_physical_nanos: i64) {
        for frame in frames {
            if frame.header.is_void_record() {
                continue;
            }
            let Ok((header, payload)) = decode_frame(&frame.frame_bytes) else {
                continue;
            };
            let Ok(batch) = decode_batch(payload, header.event_count) else {
                continue;
            };
            for (row, event) in batch.events.iter().enumerate() {
                let delivery_identity = (
                    event.variable.connector_delivery_hash_low,
                    event.variable.connector_delivery_hash_high,
                );
                let truth = RetryReceipt {
                    delivery_identity,
                    tenant_id: header.tenant_id,
                    commit: SequencePoint {
                        epoch: header.epoch,
                        sequence: header.first_sequence + row as u64,
                    },
                    shard,
                    frame_offset: frame.offset,
                    status_class: StatusClass::Acknowledged,
                    expiry_physical_nanos: guard_expiry_physical_nanos,
                    dedupe: (event.fixed.dedupe_hash_low, event.fixed.dedupe_hash_high),
                };
                match self.lookup(header.tenant_id, delivery_identity) {
                    Some(existing)
                        if existing.commit == truth.commit
                            && existing.dedupe == truth.dedupe
                            && existing.status_class != StatusClass::Indeterminate => {}
                    _ => {
                        self.record(truth);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "test/retry.rs"]
mod tests;
