use crate::lifecycle::{FooterMirrorObject, IndexArtifactRef, ManifestGeneration, OeFileEntry, Retirement};
use hashbrown::{HashMap, HashSet};
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ETag(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUpload {
    pub key: String,
    pub upload_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStat {
    pub crc64_nvme: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutOutcome {
    PreconditionFailed,
    Written { etag: ETag },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub bytes: Vec<u8>,
    pub etag: ETag,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedPart {
    pub etag: ETag,
    pub part_number: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum StoredGeneration {
    Checkpoint(ManifestGeneration),
    Delta(GenerationDelta),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GenerationDelta {
    pub(crate) added_artifacts: Vec<IndexArtifactRef>,
    pub(crate) checkpoint: u64,
    pub(crate) footer_mirror: Option<FooterMirrorObject>,
    pub(crate) generation: u64,
    pub(crate) removed_artifact_keys: Vec<String>,
    pub(crate) removed_file_ids: Vec<u128>,
    pub(crate) retirements: Vec<Retirement>,
    pub(crate) upserted_files: Vec<OeFileEntry>,
    #[serde(default)]
    pub(crate) vector_segments: Vec<crate::vector::VectorSegmentEntry>,
}

impl GenerationDelta {
    pub(crate) fn between(checkpoint: &ManifestGeneration, next: &ManifestGeneration) -> Self {
        let (upserted_files, removed_file_ids) = diff(&checkpoint.files, &next.files, |entry| entry.file_id);
        let (added_artifacts, removed_artifact_keys) =
            diff(&checkpoint.index_artifacts, &next.index_artifacts, |artifact| {
                artifact.object_key.as_str()
            });
        let removed_artifact_keys = removed_artifact_keys.into_iter().map(str::to_owned).collect();
        Self {
            added_artifacts,
            checkpoint: checkpoint.generation,
            footer_mirror: next.footer_mirror.clone(),
            generation: next.generation,
            removed_artifact_keys,
            removed_file_ids,
            retirements: next.retirements.clone(),
            upserted_files,
            vector_segments: next.vector_segments.clone(),
        }
    }

    pub(crate) fn apply(self, checkpoint: ManifestGeneration) -> ManifestGeneration {
        ManifestGeneration {
            files: apply(checkpoint.files, self.upserted_files, &self.removed_file_ids, |entry| {
                entry.file_id
            }),
            footer_mirror: self.footer_mirror,
            generation: self.generation,
            index_artifacts: apply(
                checkpoint.index_artifacts,
                self.added_artifacts,
                &self.removed_artifact_keys,
                |artifact| artifact.object_key.clone(),
            ),
            retirements: self.retirements,
            vector_segments: self.vector_segments,
        }
    }
}

fn diff<'a, T: Clone + PartialEq, K: Eq + Hash>(
    base: &'a [T],
    next: &'a [T],
    key: impl Fn(&'a T) -> K,
) -> (Vec<T>, Vec<K>) {
    let base_by_key: HashMap<K, &T> = base.iter().map(|item| (key(item), item)).collect();
    let next_keys: HashSet<K> = next.iter().map(&key).collect();
    let changed = next
        .iter()
        .filter(|&item| base_by_key.get(&key(item)) != Some(&item))
        .cloned()
        .collect();
    let removed = base
        .iter()
        .map(&key)
        .filter(|base_key| !next_keys.contains(base_key))
        .collect();
    (changed, removed)
}

fn apply<T, K: Eq + Hash>(base: Vec<T>, upserted: Vec<T>, removed: &[K], key: impl Fn(&T) -> K) -> Vec<T> {
    let removed: HashSet<&K> = removed.iter().collect();
    let position: HashMap<K, usize> = upserted
        .iter()
        .enumerate()
        .map(|(index, item)| (key(item), index))
        .collect();
    let mut upserted: Vec<Option<T>> = upserted.into_iter().map(Some).collect();
    let mut result = Vec::with_capacity(base.len() + upserted.len());
    for item in base {
        let item_key = key(&item);
        if removed.contains(&item_key) {
            continue;
        }
        let replacement = position
            .get(&item_key)
            .and_then(|&index| upserted.get_mut(index))
            .and_then(Option::take);
        result.push(replacement.unwrap_or(item));
    }
    result.extend(upserted.into_iter().flatten());
    result
}

#[cfg(test)]
#[path = "test/model.rs"]
mod tests;
