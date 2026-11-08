//! Saved deletion vectors: the deleted rows of a published file, stored once beside its manifest entry so readers load
//! them instead of rebuilding them on every open.
//!
//! A deletion vector is published as an immutable, generationed, manifest-native record ([`StoredDeletionVector`])
//! carrying the identity fields the spec requires and a BLAKE3 digest over its identity and encoded bytes. Recording a
//! further delete or correction never edits the published record: it publishes the next generation, which holds the
//! union of every earlier generation's rows and the new ones, and the manifest entry's
//! [`OeFileEntry::deletion_vector`] moves to it in the next manifest generation.
//!
//! See: oe-deletes-and-corrections/spec.md

use crate::corrections::CorrectionResolver;
use crate::deletes::{
    CorrectionMetadata, DeletionAggregateDelta, DeletionVector, DeletionVectorEncoding, DeletionVectorRef,
    RowPositionDomain, decode_deletion_vector, encode_deletion_vector,
};
use crate::error::{EntityScanError, FormatError};
use crate::events::{EventId, SequencePoint, SequenceRange, TenantId};
use crate::layout::entity_scan::DeletesAndCorrections;
use crate::lifecycle::{ManifestGeneration, OeFileEntry};

/// Domain-separation context for a stored deletion vector's BLAKE3 digest.
const STORED_DELETION_VECTOR_CONTEXT: &str = "oe 2026 stored deletion vector v1";

/// One published generation of a file's row-level deletion vector, stored manifest-native in the file's
/// [`OeFileEntry`]. Immutable once published: a later delete publishes a new generation instead of editing this one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredDeletionVector {
    /// Domain-separated BLAKE3 over the identity fields and `encoded`; checked on every load.
    pub blake3: [u8; 32],
    /// Number of deleted rows in this generation, repeated outside the encoded bytes so a planner can subtract it
    /// without decoding.
    pub deleted_count: u64,
    /// Precomputed sum of the deleted rows for the file's numeric aggregate column, when the publisher had it.
    #[serde(default)]
    pub deleted_sum: Option<i128>,
    /// Starts at 1 and rises by exactly one per published generation.
    pub deletion_vector_generation: u64,
    /// The deleted rows in [`encode_deletion_vector`]'s wire form.
    pub encoded: Vec<u8>,
    pub target_file_id: u128,
    pub target_projection_id: Option<u64>,
    pub target_sequence_range: SequenceRange,
}

impl StoredDeletionVector {
    /// Publishes the next generation for the file `entry` describes: the union of `entry`'s current vector (verified
    /// first) and `additions`. The first generation is 1. `deleted_sum`, when given, is the sum for the whole new
    /// generation, not just the additions.
    pub fn next_generation(
        entry: &OeFileEntry,
        additions: &DeletionVector,
        deleted_sum: Option<i128>,
    ) -> Result<Self, FormatError> {
        let (mut merged, generation) = match &entry.deletion_vector {
            Some(prior) => (prior.load(entry.file_id)?, prior.deletion_vector_generation + 1),
            None => (DeletionVector::new(), 1),
        };
        merged.union_with(additions);
        let (_, encoded) = encode_deletion_vector(&merged);
        let mut stored = Self {
            blake3: [0; 32],
            deleted_count: merged.deleted_count(),
            deleted_sum,
            deletion_vector_generation: generation,
            encoded,
            target_file_id: entry.file_id,
            target_projection_id: None,
            target_sequence_range: entry.coverage,
        };
        stored.blake3 = stored.digest();
        Ok(stored)
    }

    /// Verifies the digest and identity, then decodes the deleted rows. Refuses a vector naming another file, one
    /// whose bytes or identity fields were altered, or one whose decoded count disagrees with `deleted_count`.
    pub fn load(&self, expected_file_id: u128) -> Result<DeletionVector, FormatError> {
        if self.target_file_id != expected_file_id {
            return Err(FormatError::Structural {
                rule: "stored deletion vector names a different target file",
            });
        }
        if self.deletion_vector_generation == 0 {
            return Err(FormatError::Structural {
                rule: "stored deletion vector generation must start at 1",
            });
        }
        if self.digest() != self.blake3 {
            return Err(FormatError::Structural {
                rule: "stored deletion vector BLAKE3 mismatch",
            });
        }
        let vector = decode_deletion_vector(&self.encoded)?;
        if vector.deleted_count() != self.deleted_count {
            return Err(FormatError::Structural {
                rule: "stored deletion vector deleted_count disagrees with its rows",
            });
        }
        Ok(vector)
    }

    /// The spec's identity record for this vector.
    pub fn to_ref(&self) -> DeletionVectorRef {
        DeletionVectorRef {
            blake3: self.blake3,
            deleted_count: self.deleted_count,
            deletion_vector_generation: self.deletion_vector_generation,
            encoding: DeletionVectorEncoding::Inline,
            row_position_domain: RowPositionDomain::PrimaryRowsetOrdinal,
            target_file_id: self.target_file_id,
            target_projection_id: self.target_projection_id,
            target_sequence_range: self.target_sequence_range,
        }
    }

    /// The aggregate adjustment co-published with this generation; covered by the same digest.
    pub fn aggregate_delta(&self) -> DeletionAggregateDelta {
        DeletionAggregateDelta {
            deleted_count: self.deleted_count,
            deleted_sum: self.deleted_sum,
        }
    }

    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new_derive_key(STORED_DELETION_VECTOR_CONTEXT);
        hasher.update(&self.target_file_id.to_le_bytes());
        match self.target_projection_id {
            Some(id) => hasher.update(&[1]).update(&id.to_le_bytes()),
            None => hasher.update(&[0]),
        };
        let range = self.target_sequence_range;
        hasher.update(&range.epoch.to_le_bytes());
        hasher.update(&range.first_sequence.to_le_bytes());
        hasher.update(&range.last_sequence.to_le_bytes());
        hasher.update(&self.deletion_vector_generation.to_le_bytes());
        hasher.update(&self.deleted_count.to_le_bytes());
        match self.deleted_sum {
            Some(sum) => hasher.update(&[1]).update(&sum.to_le_bytes()),
            None => hasher.update(&[0]),
        };
        hasher.update(&(self.encoded.len() as u64).to_le_bytes());
        hasher.update(&self.encoded);
        *hasher.finalize().as_bytes()
    }
}

impl OeFileEntry {
    /// Loads and verifies this file's saved deletion vector, or `None` when nothing in it was ever deleted.
    pub fn load_deletion_vector(&self) -> Result<Option<DeletionVector>, FormatError> {
        self.deletion_vector
            .as_ref()
            .map(|stored| stored.load(self.file_id))
            .transpose()
    }
}

impl ManifestGeneration {
    /// Records `additions` as deleted in file `file_id` of this (unpublished, next) generation by publishing the
    /// file's next deletion-vector generation into its entry. Returns the new deletion-vector generation, or `None`
    /// when no entry has that id. An empty `additions` still publishes nothing new and returns the current generation.
    pub fn publish_deletions(
        &mut self,
        file_id: u128,
        additions: &DeletionVector,
        deleted_sum: Option<i128>,
    ) -> Result<Option<u64>, FormatError> {
        let Some(entry) = self.files.iter_mut().find(|entry| entry.file_id == file_id) else {
            return Ok(None);
        };
        if additions.deleted_count() == 0 && deleted_sum.is_none() {
            return Ok(Some(
                entry
                    .deletion_vector
                    .as_ref()
                    .map_or(0, |v| v.deletion_vector_generation),
            ));
        }
        let stored = StoredDeletionVector::next_generation(entry, additions, deleted_sum)?;
        let generation = stored.deletion_vector_generation;
        entry.deletion_vector = Some(stored);
        Ok(Some(generation))
    }
}

/// A [`DeletesAndCorrections`] backed by a published manifest generation's saved deletion vectors and a
/// [`CorrectionResolver`], so an entity scan reads stored vectors rather than rebuilding them per open.
pub struct ManifestDeletes<'a> {
    pub corrections: &'a CorrectionResolver,
    pub manifest: &'a ManifestGeneration,
}

impl DeletesAndCorrections for ManifestDeletes<'_> {
    fn deletion_vector(&self, tenant_id: TenantId, file_id: u128) -> Result<Option<DeletionVector>, EntityScanError> {
        let Some(entry) = self
            .manifest
            .files
            .iter()
            .find(|entry| entry.file_id == file_id && entry.tenant_id == tenant_id)
        else {
            return Ok(None);
        };
        Ok(entry.load_deletion_vector()?)
    }

    fn is_correction(&self, _tenant_id: TenantId, point: SequencePoint) -> Result<bool, EntityScanError> {
        Ok(self.corrections.is_correction_at(point))
    }

    fn latest_correction(
        &self,
        _tenant_id: TenantId,
        event_id: EventId,
    ) -> Result<Option<CorrectionMetadata>, EntityScanError> {
        Ok(self.corrections.latest_correction(event_id))
    }
}

#[cfg(test)]
#[path = "test/deletion_store.rs"]
mod tests;
