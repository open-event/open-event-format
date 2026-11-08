//! Executes a compaction: reads the rows of a job's input files back out of the object store, merges them into one
//! file, and publishes it in place of its inputs.
//!
//! [`plan_compaction_cycle`](crate::lifecycle::plan_compaction_cycle) decides which files merge; this module does the
//! merge. The output is published through [`OePublisher::publish_compaction`], so the output enters `Active` and every
//! input moves to `Outdated` in one manifest generation (one conditional head advance), and the inputs are deleted
//! later by [`sweep_retired_files`](super::retire::sweep_retired_files).
//!
//! Rows are read back column by column: the envelope from the required columns, the payload as the reader rebuilds
//! it. A subject-sealed payload is opaque bytes and is carried over as it is, so a compaction neither needs nor
//! touches any subject key, and a payload whose subject has been erased stays a tombstone in the output. Derived
//! columns (promoted, shredded, free-text) are rebuilt from the payload under the caller's build configuration.
//!
//! Files this merge cannot carry over faithfully are refused before anything is built: graph files (they merge per
//! subject with their graph rebuilt), and files holding signed-provenance, relationship, or source-form columns.
//!
//! See: oe-file-lifecycle/spec.md

use super::build::{BuildLifecycle, OeBuildConfig, OeRow, build_oe_file};
use super::publish::{OePublisher, PeerNotices, PublishFailure, Published};
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::clock::Clock;
use crate::columns::column_ids;
use crate::encoding::ColumnData;
use crate::error::FormatError;
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TimestampValue};
use crate::invariants::PublishedSet;
use crate::layout::reader::{OeFile, PayloadRead};
use crate::lifecycle::{CompactionJob, PartState};
use crate::object_store::{ObjectStore, oe_object_key};

/// Reads every row of `file` back into the shape the writer builds from, in row order.
///
/// Refuses (with [`FormatError::Structural`]) a graph file and a file holding provenance, relationship, or
/// source-form columns, since those are not carried over. The connector delivery hashes, source schema and delivery
/// are journal lineage that no OE file stores, so they come back zero or empty.
pub fn read_event_rows(file: &OeFile) -> Result<Vec<OeRow>, FormatError> {
    let footer = file.footer();
    if footer.graph.is_some() {
        return Err(FormatError::Structural {
            rule: "a graph file merges per subject, not through an event-file merge",
        });
    }
    if footer
        .columns
        .iter()
        .any(|column| column.column_id >= column_ids::PROVENANCE_BASE)
    {
        return Err(FormatError::Structural {
            rule: "a file with provenance, relationship, or source-form columns cannot be merged row by row",
        });
    }
    let tenant_id = file.header().tenant_id;
    let dictionaries = &footer.dictionaries;
    let mut rows = Vec::with_capacity(usize::try_from(file.header().row_count).unwrap_or(0));
    for granule in &footer.granules {
        let id = granule.granule_id;
        let u64s = |column| -> Result<Vec<u64>, FormatError> {
            match file.read_column(column, id)?.data {
                ColumnData::U64(values) => Ok(values),
                _ => Err(FormatError::Structural {
                    rule: "an envelope column holds unsigned 64-bit values",
                }),
            }
        };
        let i64s = |column| -> Result<Vec<i64>, FormatError> {
            match file.read_column(column, id)?.data {
                ColumnData::I64(values) => Ok(values),
                _ => Err(FormatError::Structural {
                    rule: "a timestamp column holds signed 64-bit values",
                }),
            }
        };
        let strings = |column| -> Result<Vec<Option<String>>, FormatError> {
            match file.read_column(column, id)?.data {
                ColumnData::Strings(values) => Ok((0..values.len())
                    .map(|row| values.get(row).flatten().map(str::to_owned))
                    .collect()),
                _ => Err(FormatError::Structural {
                    rule: "an id column holds strings",
                }),
            }
        };
        let event_ids = match file.read_column(column_ids::EVENT_ID, id)?.data {
            ColumnData::U128(values) => values,
            _ => {
                return Err(FormatError::Structural {
                    rule: "the event id column holds 128-bit ids",
                });
            }
        };
        let epochs = u64s(column_ids::EPOCH)?;
        let sequences = u64s(column_ids::SEQUENCE)?;
        let stream_ids = u64s(column_ids::STREAM_ID)?;
        let stream_sequences = u64s(column_ids::STREAM_SEQUENCE)?;
        let occurred = i64s(column_ids::OCCURRED_AT)?;
        let ingested = i64s(column_ids::INGESTED_AT)?;
        let sources = u64s(column_ids::SOURCE_ID)?;
        let event_types = u64s(column_ids::EVENT_TYPE_ID)?;
        let entity_types = u64s(column_ids::ENTITY_TYPE_ID)?;
        let entity_low = u64s(column_ids::ENTITY_ID_HASH_LOW)?;
        let entity_high = u64s(column_ids::ENTITY_ID_HASH_HIGH)?;
        let flags = u64s(column_ids::FLAGS)?;
        let schema_versions = u64s(column_ids::SCHEMA_VERSION)?;
        let entity_ids = strings(column_ids::ENTITY_ID)?;
        let actor_ids = strings(column_ids::ACTOR_ID)?;
        let account_ids = strings(column_ids::ACCOUNT_ID)?;
        let actor_low = u64s(column_ids::ACTOR_ID_HASH_LOW)?;
        let account_low = u64s(column_ids::ACCOUNT_ID_HASH_LOW)?;
        let trace_low = u64s(column_ids::TRACE_ID_HASH_LOW)?;
        let dedupe_low = u64s(column_ids::DEDUPE_HASH_LOW)?;
        let dedupe_high = u64s(column_ids::DEDUPE_HASH_HIGH)?;

        let missing = || FormatError::RefOutOfRange {
            what: "row beyond its granule's envelope columns",
        };
        let name = |values: &[String], index: u64| {
            usize::try_from(index)
                .ok()
                .and_then(|index| values.get(index))
                .cloned()
                .ok_or(FormatError::RefOutOfRange {
                    what: "dictionary id beyond the file dictionary",
                })
        };
        for row in 0..granule.row_count as usize {
            let at = |values: &[u64]| values.get(row).copied().ok_or_else(missing);
            let text = |values: &[Option<String>]| values.get(row).cloned().ok_or_else(missing);
            let event_id = *event_ids.get(row).ok_or_else(missing)?;
            let payload = match file.payload(granule.first_row_ordinal + row as u64)? {
                PayloadRead::Value(value) => PayloadInput::Variant(value),
                PayloadRead::External(reference) => PayloadInput::ExternalRef(reference),
                PayloadRead::None => PayloadInput::None,
            };
            let envelope = EventEnvelope {
                account_id: text(&account_ids)?,
                account_id_hash_low: at(&account_low)?,
                actor_id: text(&actor_ids)?,
                actor_id_hash_low: at(&actor_low)?,
                dedupe_hash_high: at(&dedupe_high)?,
                dedupe_hash_low: at(&dedupe_low)?,
                entity_id: text(&entity_ids)?,
                entity_id_hash_high: at(&entity_high)?,
                entity_id_hash_low: at(&entity_low)?,
                entity_type: name(&dictionaries.entity_type, at(&entity_types)?)?,
                event_id: EventId::from_uuid(uuid::Uuid::from_u128(event_id)),
                event_type: name(&dictionaries.event_type, at(&event_types)?)?,
                flags: EventFlags(at(&flags)? as u32),
                ingested_at: TimestampValue::from_physical_nanos(*ingested.get(row).ok_or_else(missing)?),
                occurred_at: TimestampValue::from_physical_nanos(*occurred.get(row).ok_or_else(missing)?),
                schema_version: at(&schema_versions)? as u32,
                source: name(&dictionaries.source, at(&sources)?)?,
                stream_id: StreamId(at(&stream_ids)?),
                stream_sequence: at(&stream_sequences)?,
                tenant_id,
                trace_id_hash_low: at(&trace_low)?,
            };
            rows.push(OeRow {
                epoch: at(&epochs)?,
                event: EventInput {
                    connector_delivery_hash_high: 0,
                    connector_delivery_hash_low: 0,
                    envelope,
                    payload,
                    provenance: None,
                    relationships: None,
                    source_delivery: None,
                    source_schema: None,
                },
                sequence: at(&sequences)?,
            });
        }
    }
    Ok(rows)
}

/// Reads the input file `file_id` of `config`'s tenant from `objects`, checked against the seal its catalogue entry
/// records.
pub(crate) fn open_stored_input(
    file_id: u128,
    seal: &[u8; 32],
    config: &OeBuildConfig,
    objects: &dyn ObjectStore,
) -> Result<OeFile, PublishFailure> {
    let stored = objects
        .get(&oe_object_key(config.tenant_id, file_id))
        .map_err(PublishFailure::Storage)?
        .ok_or(PublishFailure::Verification(
            "a compaction input's stored file is missing",
        ))?;
    OeFile::open(stored.bytes, Some(seal)).map_err(PublishFailure::Format)
}

/// Runs one compaction job end to end: reads every row of the job's input files from `objects`, builds them into one
/// file under `config` (as a rewrite-or-compaction build), and publishes it with
/// [`OePublisher::publish_compaction`], which makes the output `Active` and every input `Outdated` in one generation.
///
/// The inputs must still be `Active` in the head generation; their stored bytes are checked against the seal their
/// entries record before any row is read. Nothing is published when an input is missing, fails its seal, or holds rows
/// this merge cannot carry over (see [`read_event_rows`]). The replaced inputs stay in the store until
/// [`sweep_retired_files`](super::retire::sweep_retired_files) deletes them.
pub fn execute_compaction(
    publisher: &mut OePublisher,
    job: &CompactionJob,
    config: &OeBuildConfig,
    published: &mut dyn PublishedSet,
    objects: &dyn ObjectStore,
    notices: &mut dyn PeerNotices,
    clock: &dyn Clock,
) -> Result<Published, PublishFailure> {
    let (_, head) = published.head().map_err(PublishFailure::Publish)?;
    let mut rows = Vec::new();
    for file_id in &job.file_ids {
        let entry = head
            .files
            .iter()
            .find(|entry| {
                entry.file_id == *file_id
                    && entry.tenant_id == config.tenant_id
                    && entry.part_state == PartState::Active
            })
            .ok_or(PublishFailure::Verification(
                "a compaction input is not an active file of this tenant",
            ))?;
        let file = open_stored_input(*file_id, &entry.file_seal, config, objects)?;
        rows.extend(read_event_rows(&file).map_err(PublishFailure::Format)?);
    }
    rows.sort_by_key(|row| (row.epoch, row.sequence));
    let config = OeBuildConfig {
        lifecycle: BuildLifecycle::RewriteOrCompaction,
        ..config.clone()
    };
    let built = build_oe_file(rows, &config).map_err(PublishFailure::Format)?;
    publisher.publish_compaction(built, &config, &job.file_ids, published, objects, notices, clock)
}

#[cfg(test)]
#[path = "test/merge.rs"]
pub(crate) mod tests;
