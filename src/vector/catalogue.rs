//! Records vector segments and their deletes in the manifest: each [`VectorSegmentEntry`] names a segment and the
//! current generation of its [`SegmentDeletes`], stored as an immutable, create-only object in the [`ObjectStore`]
//! and named by key and BLAKE3 digest. Publishing more deletes writes the next generation's object and moves the entry
//! to it in the next manifest generation; a search loads the generation its snapshot names
//! ([`load_segment_deletes`]), so it never sees a half-published set.
//!
//! See: oe-vector-columns/spec.md, oe-deletes-and-corrections/spec.md

use super::deletes::SegmentDeletes;
use super::error::VectorError;
use crate::lifecycle::ManifestGeneration;
use crate::object_store::{ObjectStore, PutOutcome};

/// Domain-separation context for a stored segment-deletes object's BLAKE3 digest.
const SEGMENT_DELETES_CONTEXT: &str = "oe 2026 vector segment deletes v1";

/// Where one generation of a segment's deletes is stored, and what it must hash to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentDeletesRef {
    /// Domain-separated BLAKE3 over the stored bytes ([`SegmentDeletes::encode`]); checked on every load.
    pub blake3: [u8; 32],
    /// Rows deleted at this generation, repeated outside the object so a planner can size a search without loading it.
    pub deleted_count: u64,
    /// The [`SegmentDeletes::generation`] stored in the object.
    pub generation: u64,
    /// The object's key in the object store ([`segment_deletes_object_key`]).
    pub object_key: String,
}

/// One vector segment in the manifest's catalogue.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VectorSegmentEntry {
    /// The current generation of the segment's deletes, or `None` while nothing in it has been deleted.
    #[serde(default)]
    pub deletes: Option<SegmentDeletesRef>,
    /// The segment's identity, chosen by the caller (stable for the segment's lifetime).
    pub segment_id: u128,
}

/// The object key one generation of segment `segment_id`'s deletes is stored under.
pub fn segment_deletes_object_key(segment_id: u128, generation: u64) -> String {
    format!("vector-deletes/{segment_id:032x}/{generation:020}.del")
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(SEGMENT_DELETES_CONTEXT);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn unavailable(error: crate::error::StorageError) -> VectorError {
    VectorError::Unavailable {
        detail: error.to_string(),
    }
}

impl ManifestGeneration {
    /// The catalogue entry for vector segment `segment_id`, if this generation holds one.
    pub fn vector_segment(&self, segment_id: u128) -> Option<&VectorSegmentEntry> {
        self.vector_segments.iter().find(|entry| entry.segment_id == segment_id)
    }

    /// Adds vector segment `segment_id` to this (unpublished, next) generation's catalogue with no deletes; a segment
    /// already listed is left as it is.
    pub fn register_vector_segment(&mut self, segment_id: u128) {
        if self.vector_segment(segment_id).is_none() {
            self.vector_segments.push(VectorSegmentEntry {
                deletes: None,
                segment_id,
            });
        }
    }
}

/// Stores `deletes` as an immutable object and points segment `segment_id`'s catalogue entry in `manifest` (the next,
/// unpublished generation) at it, adding the entry if needed. Returns the new reference; the caller publishes
/// `manifest` as usual.
///
/// `deletes` must be a later generation than the one the entry names. The object is written create-only: a retry
/// that finds the identical object already stored succeeds, while different bytes under the same key are refused.
pub fn publish_segment_deletes(
    store: &dyn ObjectStore,
    manifest: &mut ManifestGeneration,
    segment_id: u128,
    deletes: &SegmentDeletes,
) -> Result<SegmentDeletesRef, VectorError> {
    let current = manifest
        .vector_segment(segment_id)
        .and_then(|entry| entry.deletes.as_ref())
        .map_or(0, |deletes| deletes.generation);
    if deletes.generation() <= current {
        return Err(VectorError::InvalidInput {
            rule: format!(
                "segment deletes generation {} does not follow the published generation {current}",
                deletes.generation()
            ),
        });
    }
    let bytes = deletes.encode();
    let reference = SegmentDeletesRef {
        blake3: digest(&bytes),
        deleted_count: deletes.deleted_count(),
        generation: deletes.generation(),
        object_key: segment_deletes_object_key(segment_id, deletes.generation()),
    };
    match store
        .put_if_absent(&reference.object_key, &bytes)
        .map_err(unavailable)?
    {
        PutOutcome::Written { .. } => {}
        PutOutcome::PreconditionFailed => {
            let existing = store.get(&reference.object_key).map_err(unavailable)?;
            if existing.is_none_or(|object| digest(&object.bytes) != reference.blake3) {
                return Err(VectorError::InvalidInput {
                    rule: format!(
                        "a different segment deletes object is already stored at {}",
                        reference.object_key
                    ),
                });
            }
        }
    }
    manifest.register_vector_segment(segment_id);
    if let Some(entry) = manifest
        .vector_segments
        .iter_mut()
        .find(|entry| entry.segment_id == segment_id)
    {
        entry.deletes = Some(reference.clone());
    }
    Ok(reference)
}

/// Loads the deletes `manifest` names for segment `segment_id`: generation 0 (nothing deleted) when the segment has
/// none or is not catalogued, otherwise the stored object, verified against its digest and generation.
pub fn load_segment_deletes(
    store: &dyn ObjectStore,
    manifest: &ManifestGeneration,
    segment_id: u128,
) -> Result<SegmentDeletes, VectorError> {
    let Some(reference) = manifest
        .vector_segment(segment_id)
        .and_then(|entry| entry.deletes.as_ref())
    else {
        return Ok(SegmentDeletes::new());
    };
    let object = store
        .get(&reference.object_key)
        .map_err(unavailable)?
        .ok_or_else(|| VectorError::Unavailable {
            detail: format!("segment deletes object {} is missing", reference.object_key),
        })?;
    if digest(&object.bytes) != reference.blake3 {
        return Err(VectorError::Authentication {
            what: "segment deletes BLAKE3 mismatch",
        });
    }
    let deletes = SegmentDeletes::decode(&object.bytes)?;
    if deletes.generation() != reference.generation || deletes.deleted_count() != reference.deleted_count {
        return Err(VectorError::Authentication {
            what: "segment deletes disagree with their manifest reference",
        });
    }
    Ok(deletes)
}

#[cfg(test)]
#[path = "test/catalogue.rs"]
mod tests;
