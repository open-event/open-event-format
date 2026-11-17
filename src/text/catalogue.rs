//! Publishing and loading BM25 index artifacts: external, content-addressed, droppable objects. The reference
//! (`TextIndexRef`) is what a manifest records; dropping it leaves the segment without text ranking and leaves the
//! object for the sweeper.

use super::index::{Bm25Index, TextIndexError};
use crate::object_store::{ObjectStore, PutOutcome};
use crate::vector::WrappingKey;

const TEXT_INDEX_CONTEXT: &str = "oe 2026 bm25 text index object v1";

/// A published BM25 index, bound to the segment content it was built from.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TextIndexRef {
    pub blake3: [u8; 32],
    pub content_hash: [u8; 32],
    pub doc_count: u32,
    pub object_key: String,
    pub sealed: bool,
    pub segment_id: u128,
}

pub fn text_index_object_key(segment_id: u128, digest: &[u8; 32]) -> String {
    let hex: String = digest.iter().take(16).map(|byte| format!("{byte:02x}")).collect();
    format!("text-index/{segment_id:032x}/{hex}.bm25")
}

pub fn text_index_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(TEXT_INDEX_CONTEXT);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn unavailable(error: crate::error::StorageError) -> TextIndexError {
    TextIndexError::Unavailable {
        detail: error.to_string(),
    }
}

/// Stores `index` (sealed with `key` when given) with `put_if_absent`; re-publishing identical bytes is a no-op.
pub fn publish_text_index(
    store: &dyn ObjectStore,
    index: &Bm25Index,
    key: Option<&WrappingKey>,
) -> Result<TextIndexRef, TextIndexError> {
    let bytes = index.encode(key)?;
    let blake3 = text_index_digest(&bytes);
    let reference = TextIndexRef {
        blake3,
        content_hash: index.content_hash,
        doc_count: index.doc_count(),
        object_key: text_index_object_key(index.segment_id, &blake3),
        sealed: key.is_some(),
        segment_id: index.segment_id,
    };
    match store
        .put_if_absent(&reference.object_key, &bytes)
        .map_err(unavailable)?
    {
        PutOutcome::Written { .. } => {}
        PutOutcome::PreconditionFailed => {
            let existing = store.get(&reference.object_key).map_err(unavailable)?;
            if existing.is_none_or(|object| text_index_digest(&object.bytes) != reference.blake3) {
                return Err(TextIndexError::Mismatch {
                    detail: format!("a different text index is already stored at {}", reference.object_key),
                });
            }
        }
    }
    Ok(reference)
}

/// Loads the index `reference` names, checking its BLAKE3, that it covers `segment_id` with `content_hash` (the
/// segment's current content), and that the decoded artifact agrees with the reference.
pub fn load_text_index(
    store: &dyn ObjectStore,
    reference: &TextIndexRef,
    segment_id: u128,
    content_hash: &[u8; 32],
    key: Option<&WrappingKey>,
) -> Result<Bm25Index, TextIndexError> {
    if reference.segment_id != segment_id || &reference.content_hash != content_hash {
        return Err(TextIndexError::Mismatch {
            detail: "text index reference names other segment content".to_owned(),
        });
    }
    let object = store
        .get(&reference.object_key)
        .map_err(unavailable)?
        .ok_or_else(|| TextIndexError::Unavailable {
            detail: format!("text index object {} is missing", reference.object_key),
        })?;
    if text_index_digest(&object.bytes) != reference.blake3 {
        return Err(TextIndexError::Authentication {
            what: "text index BLAKE3 mismatch",
        });
    }
    let index = Bm25Index::decode(&object.bytes, key)?;
    if index.segment_id != segment_id || &index.content_hash != content_hash || index.doc_count() != reference.doc_count
    {
        return Err(TextIndexError::Mismatch {
            detail: "text index artifact disagrees with its reference".to_owned(),
        });
    }
    Ok(index)
}
