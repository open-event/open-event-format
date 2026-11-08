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
//! Every column the builder writes from a row comes back: the envelope, the payload, signed provenance, relationship
//! references, the source form (raw payload and external id), and the connector delivery lineage of a file built with
//! [`OeBuildConfig::delivery_lineage`]. A graph file merges with its graph rebuilt by
//! [`rebuild_graph_input`](super::compaction::rebuild_graph_input) from its inputs and the subject's later files.
//!
//! See: oe-file-lifecycle/spec.md

use super::build::{BuildLifecycle, BuildRow, OeBuildConfig, OeRow, SharedStrings, build_oe_file};
use super::compaction::rebuild_graph_input;
use super::publish::{OePublisher, PeerNotices, PublishFailure, Published};
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::clock::Clock;
use crate::columns::column_ids;
use crate::encoding::ColumnData;
use crate::error::FormatError;
use crate::events::provenance::{SignatureScheme, SignedEventProvenance, hex_bytes};
use crate::events::relationships::{EventRelationships, RelationshipKind};
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TimestampValue};
use crate::invariants::PublishedSet;
use crate::layout::Profile;
use crate::layout::reader::{OeFile, PayloadRead};
use crate::lifecycle::{CompactionJob, OeFileEntry, PartState};
use crate::object_store::{ObjectStore, oe_object_key};

/// One row read back from a file: the event row, plus the source form only a [`BuildRow`] can carry.
struct StoredRow {
    external_id: Option<Vec<u8>>,
    raw_payload: Option<Vec<u8>>,
    row: OeRow,
}

/// One optional column's values for every row of a granule: `None` where the row stores no value, and all `None` when
/// the file has no such column.
fn optional_column(
    file: &OeFile,
    column_id: u32,
    granule_id: u32,
    rows: usize,
) -> Result<Vec<Option<ColumnValue>>, FormatError> {
    if !file.footer().columns.iter().any(|column| column.column_id == column_id) {
        return Ok(vec![None; rows]);
    }
    let read = file.read_column(column_id, granule_id)?;
    let stored: Vec<Option<ColumnValue>> = match &read.data {
        ColumnData::Strings(values) => (0..values.len())
            .map(|index| {
                values
                    .get(index)
                    .flatten()
                    .map(|text| ColumnValue::Text(text.to_owned()))
            })
            .collect(),
        ColumnData::I64(values) => values.iter().map(|&value| Some(ColumnValue::Int(value))).collect(),
        _ => {
            return Err(FormatError::Structural {
                rule: "an optional row column holds strings or signed 64-bit values",
            });
        }
    };
    if stored.len() == rows {
        return Ok(stored);
    }
    // Dense: only the rows whose presence bit is set store a value, in row order.
    let mut dense = stored.into_iter();
    (0..rows)
        .map(|row| {
            let present = read
                .presence
                .get(row / 8)
                .is_some_and(|byte| byte & (1 << (row % 8)) != 0);
            Ok(if present { dense.next().flatten() } else { None })
        })
        .collect()
}

/// One stored value of an optional column.
#[derive(Clone)]
enum ColumnValue {
    Int(i64),
    Text(String),
}

impl ColumnValue {
    fn text(self) -> Result<String, FormatError> {
        match self {
            ColumnValue::Text(text) => Ok(text),
            ColumnValue::Int(_) => Err(FormatError::Structural {
                rule: "a text column holds strings",
            }),
        }
    }

    fn int(self) -> Result<i64, FormatError> {
        match self {
            ColumnValue::Int(value) => Ok(value),
            ColumnValue::Text(_) => Err(FormatError::Structural {
                rule: "an integer column holds signed 64-bit values",
            }),
        }
    }
}

fn hex_of(text: &str) -> Result<Vec<u8>, FormatError> {
    hex_simd::decode_to_vec(text.as_bytes()).map_err(|_| FormatError::Structural {
        rule: "a stored id is lowercase hex",
    })
}

/// The provenance one row stores across the provenance family, or `None` when the row is unsigned.
fn provenance_at(
    columns: &mut [std::vec::IntoIter<Option<ColumnValue>>; 6],
) -> Result<Option<SignedEventProvenance>, FormatError> {
    let [author, signature, scheme, event_id, kind, claimed] = columns.each_mut().map(|column| column.next().flatten());
    let (Some(author), Some(signature), Some(scheme), Some(event_id), Some(kind), Some(claimed)) =
        (author, signature, scheme, event_id, kind, claimed)
    else {
        return Ok(None);
    };
    let malformed = |_| FormatError::Structural {
        rule: "stored provenance is the protocol's lowercase hex",
    };
    Ok(Some(SignedEventProvenance {
        author_pubkey: hex_bytes(&author.text()?).map_err(malformed)?,
        claimed_at: TimestampValue::from_physical_nanos(claimed.int()?),
        protocol_event_id: hex_bytes(&event_id.text()?).map_err(malformed)?,
        protocol_kind: u32::try_from(kind.int()?).map_err(|_| FormatError::Structural {
            rule: "a stored protocol kind fits in u32",
        })?,
        scheme: SignatureScheme::from_str(&scheme.text()?).ok_or(FormatError::Structural {
            rule: "a stored signature scheme is in the registry",
        })?,
        signature: hex_bytes(&signature.text()?).map_err(malformed)?,
    }))
}

/// Reads every row of `file` back, every column the builder writes from a row included.
fn read_stored_rows(file: &OeFile) -> Result<Vec<StoredRow>, FormatError> {
    let footer = file.footer();
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
        let row_count = granule.row_count as usize;
        let optional = |column| optional_column(file, column, id, row_count).map(Vec::into_iter);
        let mut provenance = [
            optional(column_ids::AUTHOR_PUBKEY)?,
            optional(column_ids::SIGNATURE)?,
            optional(column_ids::SIGNATURE_SCHEME)?,
            optional(column_ids::PROTOCOL_EVENT_ID)?,
            optional(column_ids::PROTOCOL_KIND)?,
            optional(column_ids::CLAIMED_AT)?,
        ];
        let mut relationships = RelationshipKind::ALL
            .iter()
            .map(|kind| Ok((*kind, optional(kind.column_id())?)))
            .collect::<Result<Vec<_>, FormatError>>()?;
        let mut raw_payloads = optional(column_ids::RAW_PAYLOAD)?;
        let mut external_ids = optional(column_ids::EXTERNAL_ID)?;
        let mut source_schemas = optional(column_ids::SOURCE_SCHEMA)?;
        let mut source_deliveries = optional(column_ids::SOURCE_DELIVERY)?;
        let mut delivery_hashes = optional(column_ids::CONNECTOR_DELIVERY_HASH)?;

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
        for row in 0..row_count {
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
            let provenance = provenance_at(&mut provenance)?;
            let mut refs = Vec::new();
            for (kind, column) in &mut relationships {
                if let Some(text) = column.next().flatten() {
                    refs.extend(
                        EventRelationships::parse_column_text(*kind, &text.text()?).map_err(|_| {
                            FormatError::Structural {
                                rule: "a stored relationship reference is canonical column text",
                            }
                        })?,
                    );
                }
            }
            let relationships = if refs.is_empty() {
                None
            } else {
                Some(EventRelationships::new(refs).map_err(|_| FormatError::Structural {
                    rule: "stored relationships declare at most one parent and one root",
                })?)
            };
            let next_text = |column: &mut std::vec::IntoIter<Option<ColumnValue>>| {
                column.next().flatten().map(ColumnValue::text).transpose()
            };
            let (hash_high, hash_low) = match next_text(&mut delivery_hashes)? {
                Some(hex) => {
                    let bytes: [u8; 16] = hex_bytes(&hex).map_err(|_| FormatError::Structural {
                        rule: "a stored connector delivery hash is 32 lowercase hex digits",
                    })?;
                    let value = u128::from_be_bytes(bytes);
                    ((value >> 64) as u64, value as u64)
                }
                None => (0, 0),
            };
            rows.push(StoredRow {
                external_id: next_text(&mut external_ids)?.map(|hex| hex_of(&hex)).transpose()?,
                raw_payload: next_text(&mut raw_payloads)?.map(String::into_bytes),
                row: OeRow {
                    epoch: at(&epochs)?,
                    event: EventInput {
                        connector_delivery_hash_high: hash_high,
                        connector_delivery_hash_low: hash_low,
                        envelope,
                        payload,
                        provenance,
                        relationships,
                        source_delivery: next_text(&mut source_deliveries)?,
                        source_schema: next_text(&mut source_schemas)?,
                    },
                    sequence: at(&sequences)?,
                },
            });
        }
    }
    Ok(rows)
}

/// Reads every row of `file` back into the event shape the writer builds from, in row order: the envelope, the
/// payload (a subject-sealed one as its opaque bytes), signed provenance, relationship references, and the connector
/// delivery lineage of a file built with [`OeBuildConfig::delivery_lineage`] (zero and empty otherwise, since no
/// other file stores it). Relationship references come back grouped by kind, in the order the file stores them.
///
/// A [`OeRow`] has no place for an event's source form, so a file that stores raw payloads or external ids is
/// refused with [`FormatError::Structural`]; read it with [`read_build_rows`] instead.
pub fn read_event_rows(file: &OeFile) -> Result<Vec<OeRow>, FormatError> {
    read_stored_rows(file)?
        .into_iter()
        .map(|stored| {
            if stored.external_id.is_some() || stored.raw_payload.is_some() {
                return Err(FormatError::Structural {
                    rule: "an event row has no place for a source form; read the file as build rows",
                });
            }
            Ok(stored.row)
        })
        .collect()
}

/// Reads every row of `file` back into the writer's own row shape, in row order, with everything
/// [`read_event_rows`] returns plus the source form (raw payload and external id). Building these rows again under the
/// same configuration reproduces the file's logical rows column for column.
pub fn read_build_rows(file: &OeFile) -> Result<Vec<BuildRow>, FormatError> {
    let mut shared = SharedStrings::default();
    Ok(read_stored_rows(file)?
        .into_iter()
        .map(|stored| {
            let mut row = shared.build_row(stored.row);
            row.external_id = stored.external_id;
            row.raw_payload = stored.raw_payload;
            row
        })
        .collect())
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
/// entries record before any row is read. Event-file inputs merge in `(epoch, sequence)` order with every row their
/// deletion vectors remove dropped. Graph-file inputs (a job from
/// [`plan_graph_compaction_cycle`](crate::lifecycle::plan_graph_compaction_cycle)) must be one subject's files that
/// follow on; they merge in event order with the graph rebuilt from them and every later `Active` file of the subject,
/// and the output is a graph file whatever `config`'s profile. A graph input with a deletion vector is refused, since
/// dropping a row would renumber the subject's events. Nothing is published when an input is missing, fails its seal,
/// or does not read back (see [`read_build_rows`]). The replaced inputs stay in the store until
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
    let mut entries: Vec<&OeFileEntry> = Vec::with_capacity(job.file_ids.len());
    for file_id in &job.file_ids {
        entries.push(
            head.files
                .iter()
                .find(|entry| {
                    entry.file_id == *file_id
                        && entry.tenant_id == config.tenant_id
                        && entry.part_state == PartState::Active
                })
                .ok_or(PublishFailure::Verification(
                    "a compaction input is not an active file of this tenant",
                ))?,
        );
    }
    let graph_inputs = entries.iter().filter(|entry| entry.graph_summary.is_some()).count();
    if graph_inputs != 0 && graph_inputs != entries.len() {
        return Err(PublishFailure::Verification(
            "a compaction merges graph files only with graph files",
        ));
    }
    let mut config = OeBuildConfig {
        lifecycle: BuildLifecycle::RewriteOrCompaction,
        ..config.clone()
    };
    let mut rows = Vec::new();
    if graph_inputs == 0 {
        for entry in &entries {
            let file = open_stored_input(entry.file_id, &entry.file_seal, &config, objects)?;
            let deletes = entry.load_deletion_vector().map_err(PublishFailure::Format)?;
            let file_rows = read_build_rows(&file).map_err(PublishFailure::Format)?;
            // Rows come back in primary-rowset ordinal order, the domain saved deletion vectors address, so a merge
            // physically drops every row the input's current deletion vector removes.
            rows.extend(
                file_rows
                    .into_iter()
                    .enumerate()
                    .filter(|(ordinal, _)| {
                        !deletes
                            .as_ref()
                            .is_some_and(|vector| vector.is_deleted(*ordinal as u64))
                    })
                    .map(|(_, row)| row),
            );
        }
        rows.sort_by_key(|row| (row.epoch, row.sequence));
    } else {
        let min_sn = |entry: &OeFileEntry| entry.graph_summary.as_ref().map_or(0, |summary| summary.min_event_sn);
        entries.sort_by_key(|entry| min_sn(entry));
        let subject = entries
            .first()
            .and_then(|entry| entry.graph_summary.as_ref())
            .map(|summary| summary.subject.clone())
            .unwrap_or_default();
        let end_sn = entries
            .iter()
            .filter_map(|entry| entry.graph_summary.as_ref())
            .map(|summary| summary.max_event_sn)
            .max()
            .unwrap_or(0);
        let mut sources = Vec::with_capacity(entries.len());
        for entry in &entries {
            if entry.deletion_vector.is_some() {
                return Err(PublishFailure::Verification(
                    "a graph compaction input carries a deletion vector",
                ));
            }
            let file = open_stored_input(entry.file_id, &entry.file_seal, &config, objects)?;
            rows.extend(read_build_rows(&file).map_err(PublishFailure::Format)?);
            sources.push(file);
        }
        let mut later_entries: Vec<&OeFileEntry> = head
            .files
            .iter()
            .filter(|entry| {
                entry.tenant_id == config.tenant_id
                    && entry.part_state == PartState::Active
                    && entry
                        .graph_summary
                        .as_ref()
                        .is_some_and(|later| later.subject == subject && later.min_event_sn > end_sn)
            })
            .collect();
        later_entries.sort_by_key(|entry| min_sn(entry));
        let later = later_entries
            .iter()
            .map(|entry| open_stored_input(entry.file_id, &entry.file_seal, &config, objects))
            .collect::<Result<Vec<_>, _>>()?;
        config.graph = Some(rebuild_graph_input(&sources, &later).map_err(PublishFailure::Format)?);
        config.profile = Profile::Graph;
    }
    let built = build_oe_file(rows, &config).map_err(PublishFailure::Format)?;
    publisher.publish_compaction(built, &config, &job.file_ids, published, objects, notices, clock)
}

#[cfg(test)]
#[path = "test/merge.rs"]
pub(crate) mod tests;
