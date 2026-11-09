use super::constant::{STATE_CHUNK_GRACE_NANOS, STATE_CHUNK_SWEEP_PAGE_RECORDS};
use crate::artifacts::state_chunk::{ObjectStoreStateChunks, StateChunk, StateRoot, read_state_chunk};
use crate::clock::Clock;
use crate::error::{StateChunkError, StorageError};
use crate::events::TenantId;
use crate::layout::required_features;
use crate::lifecycle::{ManifestGeneration, OeFileEntry};
use crate::object_store::{ObjectStore, state_chunk_object_key};
use crate::security::StateChunkKey;

pub trait StateChunkLedger {
    fn touch(&self, address: &[u8; 32], now_nanos: i64) -> Result<bool, StorageError>;

    fn clear_marks(&self) -> Result<(), StorageError>;

    fn mark(&self, address: &[u8; 32]) -> Result<bool, StorageError>;

    fn records_after(
        &self,
        after: Option<&[u8; 32]>,
        limit: usize,
        page: &mut Vec<StateChunkRecord>,
    ) -> Result<(), StorageError>;

    fn claim(&self, address: &[u8; 32], unused_since_nanos: i64) -> Result<bool, StorageError>;

    fn forget(&self, address: &[u8; 32]) -> Result<(), StorageError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateChunkRecord {
    pub address: [u8; 32],
    pub claimed: bool,
    pub last_used_nanos: i64,
    pub marked: bool,
}

pub trait FileStateRoots {
    fn state_roots(&self, entry: &OeFileEntry) -> Result<Vec<StateRoot>, StateChunkError>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateChunkSweep {
    pub deleted: Vec<[u8; 32]>,
    pub marked: u64,
}

pub fn sweep_state_chunks(
    head: &ManifestGeneration,
    tenant_id: TenantId,
    roots: &dyn FileStateRoots,
    key: &StateChunkKey,
    ledger: &dyn StateChunkLedger,
    objects: &dyn ObjectStore,
    clock: &dyn Clock,
) -> Result<StateChunkSweep, StateChunkError> {
    ledger.clear_marks()?;
    let chunks = ObjectStoreStateChunks { objects, tenant_id };
    let mut sweep = StateChunkSweep::default();
    for entry in head.files.iter().filter(|entry| {
        entry.tenant_id == tenant_id && entry.required_feature_flags & required_features::GRAPH_STATE_ROOTS != 0
    }) {
        for root in roots.state_roots(entry)? {
            let mut pending = vec![(root.address, None)];
            while let Some((address, level)) = pending.pop() {
                if !ledger.mark(&address)? {
                    continue;
                }
                sweep.marked += 1;
                if level == Some(0) {
                    continue;
                }
                if let StateChunk::Interior { children, level } = read_state_chunk(&chunks, key, &address, level)? {
                    pending.extend(children.into_iter().map(|(_, child)| (child, Some(level - 1))));
                }
            }
        }
    }

    let unused_since = clock.now_nanos().saturating_sub(STATE_CHUNK_GRACE_NANOS);
    let mut page = Vec::with_capacity(STATE_CHUNK_SWEEP_PAGE_RECORDS);
    let mut after = None;
    loop {
        ledger.records_after(after.as_ref(), STATE_CHUNK_SWEEP_PAGE_RECORDS, &mut page)?;
        for record in &page {
            if record.claimed
                || (!record.marked
                    && record.last_used_nanos <= unused_since
                    && ledger.claim(&record.address, unused_since)?)
            {
                objects.delete(&state_chunk_object_key(tenant_id, &record.address))?;
                ledger.forget(&record.address)?;
                sweep.deleted.push(record.address);
            }
        }
        match page.last() {
            Some(last) if page.len() == STATE_CHUNK_SWEEP_PAGE_RECORDS => after = Some(last.address),
            _ => return Ok(sweep),
        }
    }
}

#[cfg(test)]
#[path = "test/state_gc.rs"]
mod tests;
