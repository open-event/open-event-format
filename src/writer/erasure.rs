//! Works off the erasure-reconciliation backlog: re-keys payloads that were stored under the tenant key before their
//! subject was resolved, so each ends up sealed under its subject's own key and erasing the subject reaches it.
//!
//! A file holding backlog events is rewritten with each resolved event's payload sealed for its subject, and the
//! rewrite is published in place of the original through the compaction publication (one generation: the rewrite
//! `Active`, the original `Outdated`); the sweeper later deletes the original, the last copy of the payload in the
//! clear. A resolved event whose subject has already been erased keeps its envelope and loses its payload, which is
//! exactly what erasure would have done had the subject been known at ingest.
//!
//! See: oe-deletes-and-corrections/spec.md

use super::build::{BuildLifecycle, BuildPayload, OeBuildConfig, build_oe_file};
use super::merge::{open_stored_input, read_build_rows};
use super::publish::{OePublisher, PeerNotices, PublishFailure};
use super::subject_seal::seal_subject_rows;
use crate::clock::Clock;
use crate::deletes::SubjectId;
use crate::error::SubjectPayloadError;
use crate::events::EventId;
use crate::invariants::PublishedSet;
use crate::lifecycle::PartState;
use crate::object_store::ObjectStore;
use crate::security::{FileErasureBacklog, SubjectKeyStore};
use hashbrown::HashMap;

/// What re-keying one file did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RekeyReport {
    /// Events whose payload was dropped because their subject had already been erased.
    pub dropped: Vec<EventId>,
    /// The generation that published the rewrite, or `None` when the file held no resolved backlog event.
    pub generation: Option<u64>,
    /// The rewrite's file id.
    pub output_file_id: Option<u128>,
    /// Events now sealed under their subject's key.
    pub sealed: Vec<EventId>,
}

/// Why re-keying failed.
#[derive(Debug)]
pub enum RekeyError {
    /// Reading, building, or publishing the rewrite failed; nothing was marked re-keyed.
    Publish(PublishFailure),
    /// A subject's key could not be checked out.
    Seal(SubjectPayloadError),
    /// The backlog could not be read or written.
    Backlog(crate::error::StorageError),
}

/// Rewrites file `file_id` with every resolved backlog event it holds sealed under its subject's key from `keys`,
/// publishes the rewrite in place of the file, and marks those events re-keyed in `backlog`.
///
/// `file_id` must be an `Active` file of `config`'s tenant. Unresolved events, and rows not in the backlog, are carried
/// over unchanged. Backlog entries naming this file whose rows it does not hold are left alone. Nothing is marked
/// re-keyed unless the publication won. Entries for events of a file that has since been replaced (by a compaction,
/// say) are re-pointed with [`FileErasureBacklog::retarget`], which
/// [`Maintenance`](super::maintenance::Maintenance) does after every compaction it publishes, or with
/// [`retarget_backlog_after_compaction`].
#[expect(
    clippy::too_many_arguments,
    reason = "the re-keying rewrite wires every interface exactly once"
)]
pub fn rekey_backlog_file(
    publisher: &mut OePublisher,
    file_id: u128,
    backlog: &FileErasureBacklog,
    keys: &dyn SubjectKeyStore,
    config: &OeBuildConfig,
    published: &mut dyn PublishedSet,
    objects: &dyn ObjectStore,
    notices: &mut dyn PeerNotices,
    clock: &dyn Clock,
) -> Result<RekeyReport, RekeyError> {
    let resolved: HashMap<EventId, SubjectId> = backlog
        .pending()
        .map_err(RekeyError::Backlog)?
        .into_iter()
        .filter(|entry| entry.file_id == file_id)
        .filter_map(|entry| entry.subject.map(|subject| (entry.event_id, subject)))
        .collect();
    let mut report = RekeyReport::default();
    if resolved.is_empty() {
        return Ok(report);
    }
    let (_, head) = published
        .head()
        .map_err(|error| RekeyError::Publish(PublishFailure::Publish(error)))?;
    let entry = head
        .files
        .iter()
        .find(|entry| {
            entry.file_id == file_id && entry.tenant_id == config.tenant_id && entry.part_state == PartState::Active
        })
        .ok_or(RekeyError::Publish(PublishFailure::Verification(
            "a re-keyed file is not an active file of this tenant",
        )))?;
    let file = open_stored_input(file_id, &entry.file_seal, config, objects).map_err(RekeyError::Publish)?;
    let mut rows = read_build_rows(&file).map_err(|error| RekeyError::Publish(PublishFailure::Format(error)))?;
    for row in &mut rows {
        let event_id = row.envelope.event_id;
        let Some(subject) = resolved.get(&event_id) else {
            continue;
        };
        match keys
            .sealing_key(*subject)
            .map_err(|error| RekeyError::Seal(SubjectPayloadError::KeyStore(error)))?
        {
            Some(_) => {
                row.subject = Some(*subject);
                report.sealed.push(event_id);
            }
            None => {
                row.payload = BuildPayload::None;
                report.dropped.push(event_id);
            }
        }
    }
    if report.sealed.is_empty() && report.dropped.is_empty() {
        return Ok(report);
    }
    seal_subject_rows(&mut rows, keys).map_err(RekeyError::Seal)?;
    let config = OeBuildConfig {
        lifecycle: BuildLifecycle::RewriteOrCompaction,
        ..config.clone()
    };
    let built = build_oe_file(rows, &config).map_err(|error| RekeyError::Publish(PublishFailure::Format(error)))?;
    let published = publisher
        .publish_compaction(built, &config, &[file_id], published, objects, notices, clock)
        .map_err(RekeyError::Publish)?;
    for event_id in report.sealed.iter().chain(&report.dropped) {
        backlog.mark_rekeyed(*event_id).map_err(RekeyError::Backlog)?;
    }
    report.generation = Some(published.generation);
    report.output_file_id = Some(published.entry.file_id);
    Ok(report)
}

/// Re-points every backlog entry waiting in one of `inputs` at `output`, the file a compaction just published in their
/// place, so [`rekey_backlog_file`] finds the events where they now live.
pub fn retarget_backlog_after_compaction(
    backlog: &FileErasureBacklog,
    inputs: &[u128],
    output: u128,
) -> Result<(), crate::error::StorageError> {
    let pending = backlog.pending()?;
    for input in inputs {
        if pending.iter().any(|entry| entry.file_id == *input) {
            backlog.retarget(*input, output)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "test/erasure.rs"]
mod tests;
