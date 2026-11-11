use super::build::{BuildLifecycle, BuildRow, OeBuildConfig, OeRow, SharedStrings, build_oe_file};
use super::compaction::rebuild_graph_input;
use super::graph_renumber::{RenumberSource, renumber_graph};
use super::publish::{OePublisher, PeerNotices, PublishFailure, Published};
use super::retire::{MergedGraphOutput, MergedInput};
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

struct StoredRow {
    external_id: Option<Vec<u8>>,
    raw_payload: Option<Vec<u8>>,
    row: OeRow,
}

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

pub fn read_build_rows(file: &OeFile) -> Result<Vec<BuildRow>, FormatError> {
    read_build_rows_redacted(file, &FieldRedactions::default())
}

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
    let mut inputs = Vec::with_capacity(entries.len());
    if graph_inputs == 0 {
        for entry in &entries {
            let file = open_stored_input(entry.file_id, &entry.file_seal, &config, objects)?;
            inputs.push(MergedInput::read(entry, file.header()));
            let deletes = entry
                .load_deletion_vector_from(objects)
                .map_err(PublishFailure::Format)?;
            let redactions = entry.load_field_deletion_vectors().map_err(PublishFailure::Format)?;
            let file_rows = read_build_rows_redacted(&file, &redactions).map_err(PublishFailure::Format)?;
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
            inputs.push(MergedInput::read(entry, file.header()));
            let redactions = entry.load_field_deletion_vectors().map_err(PublishFailure::Format)?;
            rows.extend(read_build_rows_redacted(&file, &redactions).map_err(PublishFailure::Format)?);
            sources.push(file);
        }
        inputs.sort_by_key(|input| job.file_ids.iter().position(|file_id| *file_id == input.file_id));
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
    publisher.publish_merged(built, &config, &inputs, published, objects, notices, clock)
}

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

    let source =
        |entry: &OeFileEntry, opened: Option<OeFile>| -> Result<(RenumberSource, MergedInput), PublishFailure> {
            let file = match opened {
                Some(file) => file,
                None => open_stored_input(entry.file_id, &entry.file_seal, config, objects)?,
            };
            let input = MergedInput::read(entry, file.header());
            let deleted = entry
                .load_deletion_vector_from(objects)
                .map_err(PublishFailure::Format)?;
            let redactions = entry.load_field_deletion_vectors().map_err(PublishFailure::Format)?;
            let rows = read_build_rows_redacted(&file, &redactions).map_err(PublishFailure::Format)?;
            Ok((RenumberSource { deleted, file, rows }, input))
        };
    let mut groups: Vec<Vec<RenumberSource>> = Vec::new();
    let mut group_inputs: Vec<Vec<MergedInput>> = Vec::new();
    for (index, entry) in earlier.iter().enumerate() {
        if min_sn(entry) < region_start {
            continue;
        }
        let opened = opened_earlier.get_mut(index).and_then(Option::take);
        let (renumbered, input) = source(entry, opened)?;
        groups.push(vec![renumbered]);
        group_inputs.push(vec![input]);
    }
    let merged_index = groups.len();
    let (merged_sources, merged_inputs): (Vec<_>, Vec<_>) = entries
        .iter()
        .map(|entry| source(entry, None))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();
    groups.push(merged_sources);
    group_inputs.push(merged_inputs);
    for entry in &later {
        let (renumbered, input) = source(entry, None)?;
        groups.push(vec![renumbered]);
        group_inputs.push(vec![input]);
    }
    let renumbered = renumber_graph(groups).map_err(PublishFailure::Format)?;
    let mut outputs = Vec::with_capacity(renumbered.files.len());
    for (file, inputs) in renumbered.files.into_iter().zip(group_inputs) {
        let config = OeBuildConfig {
            graph: Some(file.graph),
            profile: Profile::Graph,
            ..config.clone()
        };
        outputs.push(MergedGraphOutput {
            built: build_oe_file(file.rows, &config).map_err(PublishFailure::Format)?,
            inputs,
        });
    }
    let config = OeBuildConfig {
        profile: Profile::Graph,
        ..config.clone()
    };
    publisher
        .publish_merged_graph_rewrite(outputs, &config, published, objects, notices, clock)?
        .into_iter()
        .nth(merged_index)
        .ok_or(PublishFailure::Verification(
            "a graph rewrite publishes its merged output",
        ))
}

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
