//! The DataFusion-visible `system.*` tables operators and benchmark tooling use to inspect OE storage — kept
//! public-safe.
//!
//! These tables (`system.oe_files`, `system.oe_columns`, `system.oe_granules`, `system.oe_rewrites`) expose
//! file/column/granule metadata for diagnostics. They are internal/admin surfaces: a query must come from the owning
//! tenant or an admin, and the rows returned never carry secrets — no object-store credentials, local filesystem paths,
//! payload bytes, or embedding values. The internal record holds those secrets; the system-table row is a separate type
//! that simply has no field for them, so they cannot leak by construction.

pub use super::error::IntrospectionError;
use crate::events::TenantId;
use std::fmt;

/// The fields an introspection result must never carry, named so a test can assert the public column set excludes every
/// one of them.
pub const WITHHELD_FROM_INTROSPECTION: [&str; 4] =
    ["embedding", "local_path", "object_store_credential", "payload_bytes"];

/// Who is asking an introspection query. Only a tenant (for its own files) or an admin may read the system tables; a
/// public caller may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntrospectionCaller {
    Admin,
    Public,
    Tenant(TenantId),
}

/// One OE file as the engine holds it internally: public-safe metadata *and* the secrets that must never cross the
/// introspection boundary. The secrets are dropped when this is projected to a [`SystemOeFilesRow`].
#[derive(Clone, PartialEq)]
pub struct OeFileRecord {
    pub created_at: i64,
    /// Internal analytical column — never exposed.
    pub embedding_sample: Vec<f32>,
    pub file_seal: [u8; 32],
    pub file_id: u128,
    pub file_size: u64,
    pub generation_id: u64,
    pub granule_count: u64,
    /// On-disk location — never exposed.
    pub local_path: String,
    /// Object-store credential — never exposed.
    pub object_store_credential: String,
    /// Raw payload bytes — never exposed.
    pub payload_bytes: Vec<u8>,
    pub row_count: u64,
    pub tenant_id: TenantId,
}

impl fmt::Debug for OeFileRecord {
    /// Written by hand so a stray `{:?}` — a log line, a panic message, a failed assertion — prints only the
    /// public-safe fields, never the credential, local path, payload bytes, or embedding sample.
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

/// One row of `system.oe_files`: public-safe columns only. There is no field for credentials, local paths, payload
/// bytes, or embeddings, so none can leak.
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
    /// The public column names of `system.oe_files`, used to prove the table withholds every secret field.
    pub const COLUMN_NAMES: [&'static str; 7] = [
        "created_at",
        "file_seal",
        "file_id",
        "file_size",
        "generation_id",
        "granule_count",
        "row_count",
    ];

    /// Projects an internal record to its public-safe row, dropping every secret.
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

/// Serves `system.oe_files`: enforces tenant/admin authorization and returns only public-safe rows, scoped to the rows
/// the caller may see.
///
/// A public caller is refused outright. An admin sees every file; a tenant sees only its own. Either way the rows omit
/// credentials, local paths, payload bytes, and embedding values.
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
