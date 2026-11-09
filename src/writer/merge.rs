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
use super::graph_renumber::{RenumberSource, renumber_graph};
use super::publish::{OePublisher, PeerNotices, PublishFailure, Published};
use super::retire::GraphRewriteOutput;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::clock::Clock;
use crate::columns::column_ids;
use crate::encoding::ColumnData;
use crate::error::FormatError;
use crate::events::provenance::{SignatureScheme, SignedEventProvenance, hex_bytes};
use crate::events::relationships::{EventRelationships, RelationshipKind};
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TimestampValue};
use crate::field_deletion_store::FieldRedactions;
use crate::invariants::PublishedSet;
use crate::layout::Profile;
use crate::layout::graph::EdgeKind;
use crate::layout::reader::{OeFile, PayloadRead};
use crate::layout::required_features;
use crate::layout::source_form::expand_external_id;
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
///
/// With `redactions`, every row a field-level deletion vector touches comes back redacted: its redacted payload fields
/// as `Null` (an external payload reference, which cannot be redacted in place, as no payload) and its raw payload
/// dropped, so the rows can be rebuilt into a file that needs no field deletion vector.
fn read_stored_rows(file: &OeFile, redactions: Option<&FieldRedactions>) -> Result<Vec<StoredRow>, FormatError> {
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
        let flags = u64s(column_ids::FLAGS)?;
        let schema_versions = u64s(column_ids::SCHEMA_VERSION)?;
        let entity_ids = strings(column_ids::ENTITY_ID)?;
        // A file that omits the entity hash blocks derives each row's hashes from its `entity_id`.
        let (entity_low, entity_high): (Vec<u64>, Vec<u64>) =
            if file.footer().required_feature_flags & crate::layout::required_features::ENTITY_HASH_COLUMNS_ELIDED != 0
            {
                entity_ids
                    .iter()
                    .map(|id| crate::events::derived_entity_id_hash(id.as_deref()))
                    .unzip()
            } else {
                (
                    u64s(column_ids::ENTITY_ID_HASH_LOW)?,
                    u64s(column_ids::ENTITY_ID_HASH_HIGH)?,
                )
            };
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
        let elides_event_ids =
            file.footer().required_feature_flags & required_features::EXTERNAL_ID_EVENT_ID_ELISION != 0;
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
            let ordinal = granule.first_row_ordinal + row as u64;
            let payload = file.payload(ordinal)?;
            let redacted = redactions.is_some_and(|redactions| redactions.withholds_raw_payload(ordinal));
            let payload = match redactions {
                Some(redactions) if redacted => redactions.redact_payload(ordinal, payload),
                _ => payload,
            };
            let payload = match payload {
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
                external_id: next_text(&mut external_ids)?
                    .map(|stored| {
                        let mut id = Vec::new();
                        expand_external_id(&stored, elides_event_ids, || Ok(event_id), &mut id).map(|()| id)
                    })
                    .transpose()?,
                raw_payload: next_text(&mut raw_payloads)?
                    .filter(|_| !redacted)
                    .map(String::into_bytes),
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
    read_stored_rows(file, None)?
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
    read_build_rows_redacted(file, &FieldRedactions::default())
}

/// [`read_build_rows`] with `redactions` (the file's field-level deletion vectors) applied physically: each affected
/// row's redacted payload fields come back as `Null`, an external payload reference as no payload, and its raw payload
/// is dropped. Rebuilding these rows yields a file whose stored columns no longer hold the redacted values, so it
/// needs no field deletion vector.
pub fn read_build_rows_redacted(file: &OeFile, redactions: &FieldRedactions) -> Result<Vec<BuildRow>, FormatError> {
    let mut shared = SharedStrings::default();
    let redactions = (!redactions.is_empty()).then_some(redactions);
    Ok(read_stored_rows(file, redactions)?
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
/// deletion vectors remove dropped (an external deletion-vector object is fetched from `objects`) and every field their
/// field-level deletion vectors redact physically nulled, with the raw payload of each affected row dropped (see
/// [`read_build_rows_redacted`]); the output therefore starts with neither kind of vector. Graph-file inputs (a job from
/// [`plan_graph_compaction_cycle`](crate::lifecycle::plan_graph_compaction_cycle)) must be one subject's files that
/// follow on; they merge in event order with the graph rebuilt from them and every later `Active` file of the subject,
/// and the output is a graph file whatever `config`'s profile.
///
/// A graph input with a row-level deletion vector cannot simply drop its rows: a graph file addresses events by
/// subject-wide event SN with no gaps, and the subject's later files name parents in the merged range by those SNs.
/// So such a job renumbers instead (see [`super::graph_renumber`]): it rewrites the merged range *and* every later
/// `Active` graph file of the subject - plus any earlier file whose inbound edges name renumbered children - with every
/// deleted row of every rewritten file dropped, one old-to-new SN map applied to event SNs, edges, inbound edges,
/// state roots, unresolved parents and `subject_sequence` relationships, and edges to a deleted event re-parented onto
/// its nearest surviving ancestors. All outputs and the retirement of every rewritten file go out in one generation
/// through [`OePublisher::publish_graph_rewrite`]; the returned publication is the merged output's. A rewrite that
/// would leave some output with no surviving event is refused. A graph input's field-level redactions are applied in
/// every case, since they leave every row in place. Nothing is published when an input is missing, fails its seal,
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
            let deletes = entry
                .load_deletion_vector_from(objects)
                .map_err(PublishFailure::Format)?;
            let redactions = entry.load_field_deletion_vectors().map_err(PublishFailure::Format)?;
            let file_rows = read_build_rows_redacted(&file, &redactions).map_err(PublishFailure::Format)?;
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
        if entries.iter().any(|entry| entry.deletion_vector.is_some()) {
            return execute_graph_renumbering(
                publisher, &head, &entries, &subject, end_sn, &config, published, objects, notices, clock,
            );
        }
        let mut sources = Vec::with_capacity(entries.len());
        for entry in &entries {
            let file = open_stored_input(entry.file_id, &entry.file_seal, &config, objects)?;
            let redactions = entry.load_field_deletion_vectors().map_err(PublishFailure::Format)?;
            rows.extend(read_build_rows_redacted(&file, &redactions).map_err(PublishFailure::Format)?);
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

/// The graph compaction of `entries` (sorted by SN, one subject's files ending at `end_sn`) when an input carries a
/// row deletion vector: renumbers the subject from the merged range (or from the earliest earlier file whose inbound
/// edges name a child at or past it) to its last `Active` file, and publishes every rewritten file in one generation.
#[expect(
    clippy::too_many_arguments,
    reason = "the compaction publish wires every interface exactly once"
)]
fn execute_graph_renumbering(
    publisher: &mut OePublisher,
    head: &crate::lifecycle::ManifestGeneration,
    entries: &[&OeFileEntry],
    subject: &[u8],
    end_sn: u64,
    config: &OeBuildConfig,
    published: &mut dyn PublishedSet,
    objects: &dyn ObjectStore,
    notices: &mut dyn PeerNotices,
    clock: &dyn Clock,
) -> Result<Published, PublishFailure> {
    let summary_of = |entry: &OeFileEntry| entry.graph_summary.clone();
    let start_sn = entries
        .first()
        .and_then(|entry| summary_of(entry))
        .map_or(0, |summary| summary.min_event_sn);
    let mut subject_files: Vec<&OeFileEntry> = head
        .files
        .iter()
        .filter(|entry| {
            entry.tenant_id == config.tenant_id
                && entry.part_state == PartState::Active
                && entry
                    .graph_summary
                    .as_ref()
                    .is_some_and(|summary| summary.subject == subject)
        })
        .collect();
    subject_files.sort_by_key(|entry| entry.graph_summary.as_ref().map_or(0, |summary| summary.min_event_sn));
    let min_sn = |entry: &OeFileEntry| entry.graph_summary.as_ref().map_or(0, |summary| summary.min_event_sn);
    let max_sn = |entry: &OeFileEntry| entry.graph_summary.as_ref().map_or(0, |summary| summary.max_event_sn);
    let later: Vec<&OeFileEntry> = subject_files
        .iter()
        .copied()
        .filter(|entry| min_sn(entry) > end_sn)
        .collect();
    let earlier: Vec<&OeFileEntry> = subject_files
        .iter()
        .copied()
        .filter(|entry| max_sn(entry) < start_sn)
        .collect();

    // An earlier file that lists a child at or past the first renumbered SN as an inbound edge must be rewritten too,
    // and so must every file after it; repeat until no earlier file outside the region names a child inside it.
    let mut region_start = start_sn;
    let mut opened_earlier: Vec<Option<OeFile>> = earlier.iter().map(|_| None).collect();
    loop {
        let mut moved = false;
        for (index, entry) in earlier.iter().enumerate() {
            if min_sn(entry) >= region_start {
                continue;
            }
            let Some(slot) = opened_earlier.get_mut(index) else {
                continue;
            };
            if slot.is_none() {
                *slot = Some(open_stored_input(entry.file_id, &entry.file_seal, config, objects)?);
            }
            let Some(file) = slot.as_ref() else {
                continue;
            };
            if inbound_reaches(file, region_start).map_err(PublishFailure::Format)? {
                region_start = min_sn(entry);
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }

    let source = |entry: &OeFileEntry| -> Result<RenumberSource, PublishFailure> {
        let file = open_stored_input(entry.file_id, &entry.file_seal, config, objects)?;
        let deleted = entry
            .load_deletion_vector_from(objects)
            .map_err(PublishFailure::Format)?;
        let redactions = entry.load_field_deletion_vectors().map_err(PublishFailure::Format)?;
        let rows = read_build_rows_redacted(&file, &redactions).map_err(PublishFailure::Format)?;
        Ok(RenumberSource { deleted, file, rows })
    };
    let mut groups: Vec<Vec<RenumberSource>> = Vec::new();
    let mut group_inputs: Vec<Vec<u128>> = Vec::new();
    for entry in earlier.iter().filter(|entry| min_sn(entry) >= region_start) {
        groups.push(vec![source(entry)?]);
        group_inputs.push(vec![entry.file_id]);
    }
    let merged_index = groups.len();
    groups.push(entries.iter().map(|entry| source(entry)).collect::<Result<_, _>>()?);
    group_inputs.push(entries.iter().map(|entry| entry.file_id).collect());
    for entry in &later {
        groups.push(vec![source(entry)?]);
        group_inputs.push(vec![entry.file_id]);
    }
    let renumbered = renumber_graph(groups).map_err(PublishFailure::Format)?;
    let mut outputs = Vec::with_capacity(renumbered.files.len());
    for (file, inputs) in renumbered.files.into_iter().zip(group_inputs) {
        let config = OeBuildConfig {
            graph: Some(file.graph),
            profile: Profile::Graph,
            ..config.clone()
        };
        outputs.push(GraphRewriteOutput {
            built: build_oe_file(file.rows, &config).map_err(PublishFailure::Format)?,
            inputs,
        });
    }
    let config = OeBuildConfig {
        profile: Profile::Graph,
        ..config.clone()
    };
    publisher
        .publish_graph_rewrite(outputs, &config, published, objects, notices, clock)?
        .into_iter()
        .nth(merged_index)
        .ok_or(PublishFailure::Verification(
            "a graph rewrite publishes its merged output",
        ))
}

/// Whether `file` records an inbound edge (a child in a later file) whose child is at or past `sn`.
fn inbound_reaches(file: &OeFile, sn: u64) -> Result<bool, FormatError> {
    let footer = file.footer();
    let Some(directory) = footer.graph.as_ref() else {
        return Ok(false);
    };
    let end = directory.first_sn.saturating_add(footer.exact_counts.row_count);
    for granule in &footer.granules {
        let edges = file.granule_edges(granule.granule_id)?;
        let first = directory.first_sn + granule.first_row_ordinal;
        for parent in first..first + u64::from(granule.row_count) {
            for kind in [EdgeKind::Auth, EdgeKind::Prev] {
                if edges
                    .children_iter(parent, kind)
                    .any(|child| child >= end && child >= sn)
                {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
#[path = "test/merge.rs"]
pub(crate) mod tests;
