use super::publish::PublishFailure;
use super::retire::SweepPolicy;
use crate::clock::Clock;
use crate::deletion_store::{deletion_vector_object_keys, dropped_deletion_vector_object_keys};
use crate::error::StorageError;
use crate::invariants::PublishedSet;
use crate::lifecycle::{ManifestGeneration, OeFileEntry, retired_artifact_keys};
use crate::object_store::ObjectStore;
use hashbrown::HashSet;
use std::collections::BTreeMap;

pub trait DroppedObjectLedger {
    fn swept_through(&self) -> Result<u64, StorageError>;

    fn set_swept_through(&mut self, generation: u64) -> Result<(), StorageError>;

    fn note_dropped(&mut self, key: &str, since_nanos: i64) -> Result<(), StorageError>;

    fn dropped(&self) -> Result<Vec<(String, i64)>, StorageError>;

    fn forget(&mut self, key: &str) -> Result<(), StorageError>;
}

#[derive(Debug, Default, Clone)]
pub struct SimDroppedObjectLedger {
    dropped: BTreeMap<String, i64>,
    swept_through: u64,
}

impl DroppedObjectLedger for SimDroppedObjectLedger {
    fn swept_through(&self) -> Result<u64, StorageError> {
        Ok(self.swept_through)
    }

    fn set_swept_through(&mut self, generation: u64) -> Result<(), StorageError> {
        self.swept_through = generation;
        Ok(())
    }

    fn note_dropped(&mut self, key: &str, since_nanos: i64) -> Result<(), StorageError> {
        self.dropped.entry(key.to_owned()).or_insert(since_nanos);
        Ok(())
    }

    fn dropped(&self) -> Result<Vec<(String, i64)>, StorageError> {
        Ok(self.dropped.iter().map(|(key, since)| (key.clone(), *since)).collect())
    }

    fn forget(&mut self, key: &str) -> Result<(), StorageError> {
        self.dropped.remove(key);
        Ok(())
    }
}

pub type SidecarKeys = dyn Fn(&OeFileEntry) -> Vec<String>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectSweepReport {
    pub deleted: Vec<String>,
    pub noted: Vec<String>,
}

fn named_objects(generation: &ManifestGeneration, into: &mut HashSet<String>) {
    if let Some(mirror) = &generation.footer_mirror {
        into.insert(mirror.path.clone());
    }
    into.extend(
        generation
            .index_artifacts
            .iter()
            .map(|artifact| artifact.object_key.clone()),
    );
}

pub fn sweep_dropped_objects(
    published: &dyn PublishedSet,
    objects: &dyn ObjectStore,
    ledger: &mut dyn DroppedObjectLedger,
    policy: &SweepPolicy,
    oldest_live_snapshot: u64,
    sidecars: Option<&SidecarKeys>,
    clock: &dyn Clock,
) -> Result<ObjectSweepReport, PublishFailure> {
    let safety_window = policy.safety_window_nanos.max(policy.in_flight_query_horizon_nanos);
    let now = clock.now_nanos();
    let (head_id, _) = published.head().map_err(PublishFailure::Publish)?;
    let horizon = oldest_live_snapshot.min(head_id);
    let mut report = ObjectSweepReport::default();

    let mut from = ledger.swept_through().map_err(PublishFailure::Storage)?;
    if from < horizon {
        let mut previous = published.generation(from).map_err(PublishFailure::Publish)?;
        while from < horizon {
            let next = published.generation(from + 1).map_err(PublishFailure::Publish)?;
            let mut dropped = retired_artifact_keys(&previous, &next);
            dropped.extend(dropped_deletion_vector_object_keys(&previous, &next));
            if let Some(mirror) = &previous.footer_mirror
                && next.footer_mirror.as_ref().is_none_or(|kept| kept.path != mirror.path)
            {
                dropped.push(mirror.path.clone());
            }
            let kept_files: HashSet<u128> = next.files.iter().map(|entry| entry.file_id).collect();
            if let Some(sidecars) = sidecars {
                for entry in previous
                    .files
                    .iter()
                    .filter(|entry| !kept_files.contains(&entry.file_id))
                {
                    dropped.extend(sidecars(entry));
                }
            }
            for key in dropped {
                ledger.note_dropped(&key, now).map_err(PublishFailure::Storage)?;
                report.noted.push(key);
            }
            previous = next;
            from += 1;
            ledger.set_swept_through(from).map_err(PublishFailure::Storage)?;
        }
    }

    let due: Vec<String> = ledger
        .dropped()
        .map_err(PublishFailure::Storage)?
        .into_iter()
        .filter(|(_, since)| u64::try_from(now.saturating_sub(*since)).unwrap_or(0) >= safety_window)
        .map(|(key, _)| key)
        .collect();
    if due.is_empty() {
        return Ok(report);
    }
    let mut still_named = HashSet::new();
    for generation in horizon..=head_id {
        let generation = published.generation(generation).map_err(PublishFailure::Publish)?;
        named_objects(&generation, &mut still_named);
        still_named.extend(deletion_vector_object_keys(&generation));
        if let Some(sidecars) = sidecars {
            for entry in &generation.files {
                still_named.extend(sidecars(entry));
            }
        }
    }
    for key in due {
        if !still_named.contains(&key) {
            objects.delete(&key).map_err(PublishFailure::Storage)?;
            report.deleted.push(key.clone());
        }
        ledger.forget(&key).map_err(PublishFailure::Storage)?;
    }
    Ok(report)
}

#[cfg(test)]
#[path = "test/sweep.rs"]
mod tests;
