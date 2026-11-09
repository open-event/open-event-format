use super::deletes::SegmentDeletes;
use super::error::VectorError;
use crate::lifecycle::ManifestGeneration;
use crate::object_store::{ObjectStore, PutOutcome};

const SEGMENT_DELETES_CONTEXT: &str = "oe 2026 vector segment deletes v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentDeletesRef {
    pub blake3: [u8; 32],
    pub deleted_count: u64,
    pub generation: u64,
    pub object_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VectorSegmentEntry {
    #[serde(default)]
    pub deletes: Option<SegmentDeletesRef>,
    pub segment_id: u128,
}

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
    pub fn vector_segment(&self, segment_id: u128) -> Option<&VectorSegmentEntry> {
        self.vector_segments.iter().find(|entry| entry.segment_id == segment_id)
    }

    pub fn register_vector_segment(&mut self, segment_id: u128) {
        if self.vector_segment(segment_id).is_none() {
            self.vector_segments.push(VectorSegmentEntry {
                deletes: None,
                segment_id,
            });
        }
    }
}

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

pub fn search_catalogued_segments<S: super::segment::SegmentSource>(
    store: &dyn ObjectStore,
    manifest: &ManifestGeneration,
    segments: &[super::segment::SegmentReader<S>],
    segment_ids: &[u128],
    search: &super::search::VectorSearch<'_>,
) -> Result<Vec<super::search::SearchHit>, VectorError> {
    if segments.len() != segment_ids.len() {
        return Err(VectorError::Unavailable {
            detail: "each searched segment needs exactly one catalogue id".to_owned(),
        });
    }
    let deletes = segment_ids
        .iter()
        .map(|&id| load_segment_deletes(store, manifest, id))
        .collect::<Result<Vec<_>, _>>()?;
    let refs: Vec<Option<&SegmentDeletes>> = deletes.iter().map(Some).collect();
    super::search::search_segments_with_deletes(segments, &refs, search)
}

#[cfg(test)]
#[path = "test/catalogue.rs"]
mod tests;
