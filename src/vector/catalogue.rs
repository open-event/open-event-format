use super::deletes::SegmentDeletes;
use super::error::VectorError;
use super::ivf::PartitionIndex;
use super::model::EmbeddingModel;
use super::seal::WrappingKey;
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
    /// Optional, droppable partition index (IVF) for the segment; absent means search scans the segment.
    #[serde(default)]
    pub index: Option<SegmentIndexRef>,
    pub segment_id: u128,
}

/// A published partition index, bound to the segment content it was built from.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentIndexRef {
    pub blake3: [u8; 32],
    pub content_hash: [u8; 32],
    pub model_digest: [u8; 32],
    pub object_key: String,
    pub partitions: u32,
    pub sealed: bool,
}

const SEGMENT_INDEX_CONTEXT: &str = "oe 2026 vector partition index object v1";

pub fn segment_index_object_key(segment_id: u128, digest: &[u8; 32]) -> String {
    let hex: String = digest.iter().take(16).map(|byte| format!("{byte:02x}")).collect();
    format!("vector-index/{segment_id:032x}/{hex}.ivf")
}

fn index_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(SEGMENT_INDEX_CONTEXT);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

/// Stores `index` (sealed with `key` when given) and records it on the segment's manifest entry, replacing any
/// earlier index reference.
pub fn publish_segment_index(
    store: &dyn ObjectStore,
    manifest: &mut ManifestGeneration,
    segment_id: u128,
    index: &PartitionIndex,
    key: Option<&WrappingKey>,
) -> Result<SegmentIndexRef, VectorError> {
    let bytes = index.encode(key)?;
    let blake3 = index_digest(&bytes);
    let reference = SegmentIndexRef {
        blake3,
        content_hash: index.content_hash,
        model_digest: model_digest(&index.model),
        object_key: segment_index_object_key(segment_id, &blake3),
        partitions: index.partitions.len() as u32,
        sealed: key.is_some(),
    };
    match store
        .put_if_absent(&reference.object_key, &bytes)
        .map_err(unavailable)?
    {
        PutOutcome::Written { .. } => {}
        PutOutcome::PreconditionFailed => {
            let existing = store.get(&reference.object_key).map_err(unavailable)?;
            if existing.is_none_or(|object| index_digest(&object.bytes) != reference.blake3) {
                return Err(VectorError::InvalidInput {
                    rule: format!(
                        "a different partition index is already stored at {}",
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
        entry.index = Some(reference.clone());
    }
    Ok(reference)
}

/// Removes the segment's index reference; search falls back to scanning. The object itself is left for GC.
pub fn drop_segment_index(manifest: &mut ManifestGeneration, segment_id: u128) -> Option<SegmentIndexRef> {
    manifest
        .vector_segments
        .iter_mut()
        .find(|entry| entry.segment_id == segment_id)
        .and_then(|entry| entry.index.take())
}

fn model_digest(model: &EmbeddingModel) -> [u8; 32] {
    let mut out = crate::file::bytes::Writer::new();
    model.encode_into(&mut out);
    *blake3::hash(out.bytes()).as_bytes()
}

/// Loads the segment's published index, if any, and checks it against the reference and `segment`: object
/// digest, segment content hash, model and row count. Any mismatch is an error.
pub fn load_segment_index<S: super::segment::SegmentSource>(
    store: &dyn ObjectStore,
    manifest: &ManifestGeneration,
    segment_id: u128,
    segment: &super::segment::SegmentReader<S>,
    key: Option<&WrappingKey>,
) -> Result<Option<PartitionIndex>, VectorError> {
    let Some(reference) = manifest
        .vector_segment(segment_id)
        .and_then(|entry| entry.index.as_ref())
    else {
        return Ok(None);
    };
    if reference.content_hash != segment.manifest().content_hash {
        return Err(VectorError::Authentication {
            what: "partition index reference names other segment content",
        });
    }
    if reference.model_digest != model_digest(&segment.manifest().model) {
        return Err(VectorError::ModelMismatch {
            detail: "partition index was built for another embedding model".to_owned(),
        });
    }
    let object = store
        .get(&reference.object_key)
        .map_err(unavailable)?
        .ok_or_else(|| VectorError::Unavailable {
            detail: format!("partition index object {} is missing", reference.object_key),
        })?;
    if index_digest(&object.bytes) != reference.blake3 {
        return Err(VectorError::Authentication {
            what: "partition index BLAKE3 mismatch",
        });
    }
    let index = PartitionIndex::decode(&object.bytes, key)?;
    index.check(segment)?;
    if index.partitions.len() as u32 != reference.partitions {
        return Err(VectorError::Authentication {
            what: "partition index disagrees with its manifest reference",
        });
    }
    Ok(Some(index))
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
                index: None,
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

/// `search_catalogued_segments` using each segment's published partition index where one loads and checks out.
/// An index that is missing, tampered with or bound to other content is ignored (counted in
/// `SearchStats::indexes_rejected`) and that segment is scanned, so the answer never depends on the index.
pub fn search_catalogued_segments_indexed<S: super::segment::SegmentSource>(
    store: &dyn ObjectStore,
    manifest: &ManifestGeneration,
    segments: &[super::segment::SegmentReader<S>],
    segment_ids: &[u128],
    search: &super::search::VectorSearch<'_>,
    key: Option<&WrappingKey>,
    options: super::search::SearchOptions,
) -> Result<(Vec<super::search::SearchHit>, super::search::SearchStats), VectorError> {
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
    let mut rejected = 0;
    let indexes: Vec<Option<PartitionIndex>> = segments
        .iter()
        .zip(segment_ids)
        .map(|(segment, &id)| {
            load_segment_index(store, manifest, id, segment, key).unwrap_or_else(|_| {
                rejected += 1;
                None
            })
        })
        .collect();
    let index_refs: Vec<Option<&PartitionIndex>> = indexes.iter().map(Option::as_ref).collect();
    let (hits, mut stats) = super::search::search_segments_indexed(segments, &refs, &index_refs, search, options)?;
    stats.indexes_rejected += rejected;
    Ok((hits, stats))
}

#[cfg(test)]
#[path = "test/catalogue.rs"]
mod tests;
