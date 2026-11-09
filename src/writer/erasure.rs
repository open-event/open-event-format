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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RekeyReport {
    pub dropped: Vec<EventId>,
    pub generation: Option<u64>,
    pub output_file_id: Option<u128>,
    pub sealed: Vec<EventId>,
}

#[derive(Debug)]
pub enum RekeyError {
    Publish(PublishFailure),
    Seal(SubjectPayloadError),
    Backlog(crate::error::StorageError),
}

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
