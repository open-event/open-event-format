use crate::error::{ExternalIdIndexError, StorageError};
use crate::events::TenantId;
use crate::layout::reader::OeFile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalIdLocation {
    pub file_id: u128,
    pub generation: u64,
    pub row_ordinal: u64,
}

pub trait ExternalIdStore {
    fn get(&self, tenant_id: TenantId, external_id: &[u8]) -> Result<Option<ExternalIdLocation>, StorageError>;

    fn put(
        &mut self,
        tenant_id: TenantId,
        external_id: &[u8],
        location: ExternalIdLocation,
    ) -> Result<(), StorageError>;
}

pub fn record_file(store: &mut impl ExternalIdStore, file: &OeFile) -> Result<usize, ExternalIdIndexError> {
    let header = file.header();
    let mut written = 0;
    for (row_ordinal, external_id) in file.external_ids()? {
        let newer_recorded = store
            .get(header.tenant_id, &external_id)?
            .is_some_and(|recorded| recorded.generation > header.generation_id);
        if newer_recorded {
            continue;
        }
        store.put(
            header.tenant_id,
            &external_id,
            ExternalIdLocation {
                file_id: header.file_id,
                generation: header.generation_id,
                row_ordinal,
            },
        )?;
        written += 1;
    }
    Ok(written)
}

#[cfg(test)]
#[path = "test/external_id.rs"]
mod tests;
