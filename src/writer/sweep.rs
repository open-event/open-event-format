//! Deletes the objects that ride a catalogue generation without being files - footer mirrors and index artifacts -
//! and the sidecar objects of files the catalogue has forgotten (deletion vectors, say), once no query can still
//! fetch them.
//!
//! [`sweep_retired_files`](super::retire::sweep_retired_files) deletes the replaced files themselves. Everything else
//! a generation names leaves the catalogue simply by being omitted from the next generation, so nothing in the
//! catalogue remembers it was dropped or when. This sweep walks the generations no live query reads any more, notes
//! every object a generation dropped in the application's durable [`DroppedObjectLedger`] with the time it was found,
//! and deletes an object only once:
//!
//! - every generation that still names it is older than the oldest generation a running query reads, and
//! - it has waited the safety window (never shorter than the in-flight-query horizon) since it was noted.
//!
//! An object any generation from the oldest live one to the head still names is never deleted; one that comes back
//! into the catalogue is forgotten instead. The object is deleted before the ledger forgets it, so a crash in between
//! repeats a harmless delete.
//!
//! See: oe-file-lifecycle/spec.md

use super::publish::PublishFailure;
use super::retire::SweepPolicy;
use crate::clock::Clock;
use crate::error::StorageError;
use crate::invariants::PublishedSet;
use crate::lifecycle::{ManifestGeneration, OeFileEntry, retired_artifact_keys};
use crate::object_store::ObjectStore;
use hashbrown::HashSet;
use std::collections::BTreeMap;

/// The application's durable record of objects the catalogue has dropped but the store still holds, and of how far
/// through the catalogue's generations the sweep has looked. One ledger per catalogue; one sweep at a time.
pub trait DroppedObjectLedger {
    /// The generation the sweep has walked up to: every object dropped by a generation at or before it is noted.
    fn swept_through(&self) -> Result<u64, StorageError>;

    /// Records that the walk has reached `generation`.
    fn set_swept_through(&mut self, generation: u64) -> Result<(), StorageError>;

    /// Notes that the catalogue dropped `key` at `since_nanos`. Noting a key already noted keeps the earlier time.
    fn note_dropped(&mut self, key: &str, since_nanos: i64) -> Result<(), StorageError>;

    /// Every noted key, with the time it was noted.
    fn dropped(&self) -> Result<Vec<(String, i64)>, StorageError>;

    /// Forgets `key`: it was deleted, or the catalogue names it again.
    fn forget(&mut self, key: &str) -> Result<(), StorageError>;
}

/// An in-memory [`DroppedObjectLedger`] for tests and simulation.
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

/// What one object sweep did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectSweepReport {
    /// Object keys deleted from the store.
    pub deleted: Vec<String>,
    /// Object keys newly noted as dropped; they wait out the safety window.
    pub noted: Vec<String>,
}

/// Every object key `generation` names besides its files' own objects: its footer mirror and its index artifacts.
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

/// Notes and deletes the objects the catalogue has dropped: footer mirrors, index artifacts, and the sidecar objects
/// (`sidecars`, typically a file's deletion-vector objects) of each file entry that left the catalogue.
///
/// `oldest_live_snapshot` is the oldest generation any running query reads (the head when none runs), the same value
/// [`sweep_retired_files`](super::retire::sweep_retired_files) takes. The walk compares each pair of consecutive
/// generations from the ledger's watermark up to `oldest_live_snapshot` and notes what the later one dropped; a file
/// entry that disappeared was deleted by the file sweeper, so its sidecars are noted too. Then each noted key that has
/// waited `policy`'s safety window is deleted from `objects`, unless a generation from `oldest_live_snapshot` to the
/// head still names it (it is then forgotten, not deleted).
///
/// The application runs this periodically, after [`sweep_retired_files`](super::retire::sweep_retired_files). It
/// publishes no generation.
pub fn sweep_dropped_objects(
    published: &dyn PublishedSet,
    objects: &dyn ObjectStore,
    ledger: &mut dyn DroppedObjectLedger,
    policy: &SweepPolicy,
    oldest_live_snapshot: u64,
    sidecars: &dyn Fn(&OeFileEntry) -> Vec<String>,
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
            if let Some(mirror) = &previous.footer_mirror
                && next.footer_mirror.as_ref().is_none_or(|kept| kept.path != mirror.path)
            {
                dropped.push(mirror.path.clone());
            }
            let kept_files: HashSet<u128> = next.files.iter().map(|entry| entry.file_id).collect();
            for entry in previous
                .files
                .iter()
                .filter(|entry| !kept_files.contains(&entry.file_id))
            {
                dropped.extend(sidecars(entry));
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
        for entry in &generation.files {
            still_named.extend(sidecars(entry));
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
