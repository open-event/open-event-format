pub use super::error::IntrospectionError;
use crate::events::TenantId;
use std::fmt;

pub const WITHHELD_FROM_INTROSPECTION: [&str; 4] =
    ["embedding", "local_path", "object_store_credential", "payload_bytes"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntrospectionCaller {
    Admin,
    Public,
    Tenant(TenantId),
}

#[derive(Clone, PartialEq)]
pub struct OeFileRecord {
    pub created_at: i64,
    pub embedding_sample: Vec<f32>,
    pub file_seal: [u8; 32],
    pub file_id: u128,
    pub file_size: u64,
    pub generation_id: u64,
    pub granule_count: u64,
    pub local_path: String,
    pub object_store_credential: String,
    pub payload_bytes: Vec<u8>,
    pub row_count: u64,
    pub tenant_id: TenantId,
}

impl fmt::Debug for OeFileRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OeFileRecord")
            .field("created_at", &self.created_at)
            .field("file_seal", &self.file_seal)
            .field("file_id", &self.file_id)
            .field("file_size", &self.file_size)
            .field("generation_id", &self.generation_id)
            .field("granule_count", &self.granule_count)
            .field("row_count", &self.row_count)
            .field("tenant_id", &self.tenant_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemOeFilesRow {
    pub created_at: i64,
    pub file_seal: [u8; 32],
    pub file_id: u128,
    pub file_size: u64,
    pub generation_id: u64,
    pub granule_count: u64,
    pub row_count: u64,
}

impl SystemOeFilesRow {
    pub const COLUMN_NAMES: [&'static str; 7] = [
        "created_at",
        "file_seal",
        "file_id",
        "file_size",
        "generation_id",
        "granule_count",
        "row_count",
    ];

    fn from_record(record: &OeFileRecord) -> Self {
        Self {
            created_at: record.created_at,
            file_seal: record.file_seal,
            file_id: record.file_id,
            file_size: record.file_size,
            generation_id: record.generation_id,
            granule_count: record.granule_count,
            row_count: record.row_count,
        }
    }
}

pub fn system_oe_files(
    caller: IntrospectionCaller,
    records: &[OeFileRecord],
) -> Result<Vec<SystemOeFilesRow>, IntrospectionError> {
    match caller {
        IntrospectionCaller::Public => Err(IntrospectionError::Unauthorized),
        IntrospectionCaller::Admin => Ok(records.iter().map(SystemOeFilesRow::from_record).collect()),
        IntrospectionCaller::Tenant(tenant) => Ok(records
            .iter()
            .filter(|record| record.tenant_id == tenant)
            .map(SystemOeFilesRow::from_record)
            .collect()),
    }
}

#[cfg(test)]
#[path = "test/introspection.rs"]
mod tests;
