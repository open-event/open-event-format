use crate::corrections::CorrectionResolver;
use crate::deletes::{
    CorrectionMetadata, DeletionAggregateDelta, DeletionVector, DeletionVectorEncoding, DeletionVectorRef,
    RowPositionDomain, decode_deletion_vector, encode_deletion_vector,
};
use crate::error::{EntityScanError, FormatError};
use crate::events::{EventId, SequencePoint, SequenceRange, TenantId};
use crate::layout::entity_scan::DeletesAndCorrections;
use crate::lifecycle::{ManifestGeneration, OeFileEntry};
use crate::object_store::{ObjectStore, PutOutcome};

const STORED_DELETION_VECTOR_CONTEXT: &str = "oe 2026 stored deletion vector v1";

const DELETION_VECTOR_OBJECT_CONTEXT: &str = "oe 2026 deletion vector object v1";

const DELETION_VECTOR_OBJECT_MAGIC: &[u8; 8] = b"OEDVOBJ1";

pub const INLINE_DELETION_VECTOR_LIMIT: usize = 16 * 1024;

pub fn deletion_vector_object_key(tenant_id: TenantId, file_id: u128, generation: u64) -> String {
    format!("oe/{tenant_id}/{file_id:032x}.dv{generation:016x}")
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeletionVectorObjectRef {
    pub blake3: [u8; 32],
    pub key: String,
    pub len: u64,
}

fn unavailable(error: impl std::fmt::Display) -> FormatError {
    FormatError::Unavailable {
        detail: error.to_string(),
    }
}

fn object_digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(DELETION_VECTOR_OBJECT_CONTEXT)
        .update(bytes)
        .finalize()
        .as_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredDeletionVector {
    pub blake3: [u8; 32],
    pub deleted_count: u64,
    #[serde(default)]
    pub deleted_sum: Option<i128>,
    pub deletion_vector_generation: u64,
    pub encoded: Vec<u8>,
    pub target_file_id: u128,
    pub target_projection_id: Option<u64>,
    pub target_sequence_range: SequenceRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<DeletionVectorObjectRef>,
}

impl StoredDeletionVector {
    pub fn next_generation_in(
        entry: &OeFileEntry,
        additions: &DeletionVector,
        deleted_sum: Option<i128>,
        objects: &dyn ObjectStore,
    ) -> Result<Self, FormatError> {
        Self::next_generation_with_limit(
            entry,
            additions,
            deleted_sum,
            Some(objects),
            INLINE_DELETION_VECTOR_LIMIT,
        )
    }

    pub(crate) fn next_generation_with_limit(
        entry: &OeFileEntry,
        additions: &DeletionVector,
        deleted_sum: Option<i128>,
        objects: Option<&dyn ObjectStore>,
        inline_limit: usize,
    ) -> Result<Self, FormatError> {
        let (mut merged, generation) = match &entry.deletion_vector {
            Some(prior) => (
                prior.load_with(entry.file_id, objects)?,
                prior.deletion_vector_generation + 1,
            ),
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
            object: None,
        };
        stored.blake3 = stored.digest();
        if let Some(objects) = objects
            && stored.encoded.len() > inline_limit
        {
            let key = deletion_vector_object_key(entry.tenant_id, entry.file_id, generation);
            let bytes = stored.object_bytes();
            match objects.put_if_absent(&key, &bytes).map_err(unavailable)? {
                PutOutcome::Written { .. } => {}
                PutOutcome::PreconditionFailed => {
                    let existing = objects.get(&key).map_err(unavailable)?;
                    if existing.is_none_or(|existing| existing.bytes != bytes) {
                        return Err(FormatError::Structural {
                            rule: "deletion vector object key already holds different bytes",
                        });
                    }
                }
            }
            stored.object = Some(DeletionVectorObjectRef {
                blake3: object_digest(&bytes),
                key,
                len: bytes.len() as u64,
            });
            stored.encoded = Vec::new();
        }
        Ok(stored)
    }

    pub fn is_external(&self) -> bool {
        self.object.is_some()
    }

    fn object_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 8 + 8 + 32 + self.encoded.len());
        bytes.extend_from_slice(DELETION_VECTOR_OBJECT_MAGIC);
        bytes.extend_from_slice(&self.target_file_id.to_le_bytes());
        bytes.extend_from_slice(&self.deletion_vector_generation.to_le_bytes());
        bytes.extend_from_slice(&self.deleted_count.to_le_bytes());
        bytes.extend_from_slice(&self.blake3);
        bytes.extend_from_slice(&self.encoded);
        bytes
    }

    fn fetch_encoded(
        &self,
        object: &DeletionVectorObjectRef,
        objects: &dyn ObjectStore,
    ) -> Result<Vec<u8>, FormatError> {
        let Some(stored) = objects.get(&object.key).map_err(unavailable)? else {
            return Err(FormatError::Unavailable {
                detail: format!("deletion vector object {} is missing", object.key),
            });
        };
        let bytes = stored.bytes;
        if bytes.len() as u64 != object.len {
            return Err(FormatError::Structural {
                rule: "deletion vector object length disagrees with its ref",
            });
        }
        if object_digest(&bytes) != object.blake3 {
            return Err(FormatError::Blake3Mismatch {
                scope: "deletion vector object",
            });
        }
        let header = 8 + 16 + 8 + 8 + 32;
        let (head, encoded) = bytes.split_at_checked(header).ok_or(FormatError::Truncated {
            what: "deletion vector object header",
        })?;
        let mut expected = Vec::with_capacity(header);
        expected.extend_from_slice(DELETION_VECTOR_OBJECT_MAGIC);
        expected.extend_from_slice(&self.target_file_id.to_le_bytes());
        expected.extend_from_slice(&self.deletion_vector_generation.to_le_bytes());
        expected.extend_from_slice(&self.deleted_count.to_le_bytes());
        expected.extend_from_slice(&self.blake3);
        if head != expected.as_slice() {
            return Err(FormatError::Structural {
                rule: "deletion vector object identity disagrees with its ref",
            });
        }
        Ok(encoded.to_vec())
    }

    pub fn load_with(
        &self,
        expected_file_id: u128,
        objects: Option<&dyn ObjectStore>,
    ) -> Result<DeletionVector, FormatError> {
        match &self.object {
            None => self.load(expected_file_id),
            Some(object) => {
                let objects = objects.ok_or(FormatError::Structural {
                    rule: "external deletion vector needs an object store to load",
                })?;
                let mut inline = self.clone();
                inline.encoded = self.fetch_encoded(object, objects)?;
                inline.object = None;
                inline.load(expected_file_id)
            }
        }
    }

    pub fn next_generation(
        entry: &OeFileEntry,
        additions: &DeletionVector,
        deleted_sum: Option<i128>,
    ) -> Result<Self, FormatError> {
        Self::next_generation_with_limit(entry, additions, deleted_sum, None, usize::MAX)
    }

    pub fn load(&self, expected_file_id: u128) -> Result<DeletionVector, FormatError> {
        if self.object.is_some() {
            return Err(FormatError::Structural {
                rule: "external deletion vector needs an object store to load",
            });
        }
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

    pub fn to_ref(&self) -> DeletionVectorRef {
        DeletionVectorRef {
            blake3: self.blake3,
            deleted_count: self.deleted_count,
            deletion_vector_generation: self.deletion_vector_generation,
            encoding: if self.object.is_some() {
                DeletionVectorEncoding::NativeBlock
            } else {
                DeletionVectorEncoding::Inline
            },
            row_position_domain: RowPositionDomain::PrimaryRowsetOrdinal,
            target_file_id: self.target_file_id,
            target_projection_id: self.target_projection_id,
            target_sequence_range: self.target_sequence_range,
        }
    }

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
    pub fn load_deletion_vector(&self) -> Result<Option<DeletionVector>, FormatError> {
        self.deletion_vector
            .as_ref()
            .map(|stored| stored.load(self.file_id))
            .transpose()
    }

    pub fn load_deletion_vector_from(&self, objects: &dyn ObjectStore) -> Result<Option<DeletionVector>, FormatError> {
        self.deletion_vector
            .as_ref()
            .map(|stored| stored.load_with(self.file_id, Some(objects)))
            .transpose()
    }

    pub fn deletion_vector_object_key(&self) -> Option<&str> {
        self.deletion_vector
            .as_ref()
            .and_then(|stored| stored.object.as_ref())
            .map(|object| object.key.as_str())
    }
}

pub fn deletion_vector_object_keys(generation: &ManifestGeneration) -> Vec<String> {
    generation
        .files
        .iter()
        .filter_map(|entry| entry.deletion_vector_object_key().map(str::to_owned))
        .collect()
}

pub fn dropped_deletion_vector_object_keys(previous: &ManifestGeneration, next: &ManifestGeneration) -> Vec<String> {
    let kept: hashbrown::HashSet<String> = deletion_vector_object_keys(next).into_iter().collect();
    deletion_vector_object_keys(previous)
        .into_iter()
        .filter(|key| !kept.contains(key))
        .collect()
}

impl ManifestGeneration {
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

    pub fn publish_deletions_in(
        &mut self,
        file_id: u128,
        additions: &DeletionVector,
        deleted_sum: Option<i128>,
        objects: &dyn ObjectStore,
    ) -> Result<Option<u64>, FormatError> {
        self.publish_deletions_with_limit(file_id, additions, deleted_sum, objects, INLINE_DELETION_VECTOR_LIMIT)
    }

    pub(crate) fn publish_deletions_with_limit(
        &mut self,
        file_id: u128,
        additions: &DeletionVector,
        deleted_sum: Option<i128>,
        objects: &dyn ObjectStore,
        inline_limit: usize,
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
        let stored = StoredDeletionVector::next_generation_with_limit(
            entry,
            additions,
            deleted_sum,
            Some(objects),
            inline_limit,
        )?;
        let generation = stored.deletion_vector_generation;
        entry.deletion_vector = Some(stored);
        Ok(Some(generation))
    }
}

pub struct ManifestDeletes<'a> {
    pub corrections: &'a CorrectionResolver,
    pub manifest: &'a ManifestGeneration,
    pub objects: Option<&'a dyn ObjectStore>,
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
        Ok(match self.objects {
            Some(objects) => entry.load_deletion_vector_from(objects)?,
            None => entry.load_deletion_vector()?,
        })
    }

    fn field_redactions(
        &self,
        tenant_id: TenantId,
        file_id: u128,
    ) -> Result<Option<crate::field_deletion_store::FieldRedactions>, EntityScanError> {
        let Some(entry) = self
            .manifest
            .files
            .iter()
            .find(|entry| entry.file_id == file_id && entry.tenant_id == tenant_id)
        else {
            return Ok(None);
        };
        if entry.field_deletion_vectors.is_empty() {
            return Ok(None);
        }
        Ok(Some(entry.load_field_deletion_vectors()?))
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
