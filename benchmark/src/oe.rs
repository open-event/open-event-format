//! OE: events with their envelope columns and a typed payload that the writer splits into per-field columns. Queries
//! read whole columns of the granules the footer cannot rule out.

use crate::dataset::{Column, Dataset};
use crate::format::{Format, Opened};
use crate::query::{Col, Partial, Predicate, Query, fold_chunk, needed};
use anyhow::{Result, anyhow};
use oe::artifacts::batch::{EventInput, PayloadInput};
use oe::columns::{FreetextDeclaration, PromotionPlan, column_ids};
use oe::encoding::ColumnData;
use oe::events::variant::VariantValue;
use oe::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use oe::indexes::stable_hash;
use oe::layout::reader::OeFile;
use oe::layout::{LayoutTargets, Profile};
use oe::security::FooterEncryption;
use oe::typed_id::TypedIdTestExt;
use oe::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, SharedStrings, build_oe_file};
use std::collections::BTreeMap;

pub struct Oe;

fn config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 42,
        footer_dek: None,
        footer_encryption: FooterEncryption::Plaintext,
        freetext: FreetextDeclaration { fields: Vec::new() },
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: Profile::Event,
        promotion: PromotionPlan { columns: Vec::new() },
        targets: LayoutTargets {
            index_granularity: 8192,
            index_granularity_bytes: 1 << 20,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
            stripe_target_bytes: 4 << 20,
        },
        tenant_id: TenantId::new_test_id(7),
        time_series_bucket_nanos: None,
    }
}

impl Format for Oe {
    fn name(&self) -> &'static str {
        "oe"
    }

    fn write(&self, dataset: &Dataset) -> Result<Vec<u8>> {
        let entity = dataset.strs("entity_id");
        let occurred = dataset.i64s("occurred_at");
        let event_type = dataset.strs("event_type");
        let source = dataset.strs("source");
        let payload_columns: Vec<(&str, &Column)> = dataset
            .columns
            .iter()
            .filter_map(|(name, column)| name.strip_prefix("payload.").map(|field| (field, column)))
            .collect();
        let mut shared = SharedStrings::default();
        let mut rows = Vec::with_capacity(dataset.rows);
        for i in 0..dataset.rows {
            let mut fields = BTreeMap::new();
            for (name, column) in &payload_columns {
                let value = match column {
                    Column::F64(values) => VariantValue::Double(values[i]),
                    Column::I64(values) => VariantValue::Int(values[i]),
                    Column::Str(values) => VariantValue::String(values[i].clone()),
                };
                fields.insert((*name).to_owned(), value);
            }
            rows.push(shared.build_row(OeRow {
                epoch: 1,
                event: EventInput {
                    connector_delivery_hash_high: 0,
                    connector_delivery_hash_low: 0,
                    envelope: EventEnvelope {
                        account_id: None,
                        account_id_hash_low: 0,
                        actor_id: None,
                        actor_id_hash_low: 0,
                        dedupe_hash_high: 0,
                        dedupe_hash_low: i as u64,
                        entity_id: Some(entity[i].clone()),
                        entity_id_hash_high: 0,
                        entity_id_hash_low: stable_hash(entity[i].as_bytes()),
                        entity_type: "entity".to_owned(),
                        event_id: EventId::new_test_id(0xA000 + i as u128),
                        event_type: event_type[i].clone(),
                        flags: EventFlags(0),
                        ingested_at: TimestampValue::from_physical_nanos(occurred[i] + 1_000),
                        occurred_at: TimestampValue::from_physical_nanos(occurred[i]),
                        schema_version: 1,
                        source: source[i].clone(),
                        stream_id: StreamId(1),
                        stream_sequence: i as u64,
                        tenant_id: TenantId::new_test_id(7),
                        trace_id_hash_low: 0,
                    },
                    payload: PayloadInput::Variant(VariantValue::Object(fields)),
                    provenance: None,
                    relationships: None,
                    source_delivery: None,
                    source_schema: None,
                },
                sequence: i as u64 + 1,
            }));
        }
        Ok(build_oe_file(rows, &config())?.bytes)
    }

    fn open(&self, _dataset: &Dataset, bytes: Vec<u8>) -> Result<Box<dyn Opened>> {
        Ok(Box::new(OeReader {
            file: OeFile::open(bytes, None)?,
        }))
    }
}

struct OeReader {
    file: OeFile,
}

impl OeReader {
    fn column_id(&self, name: &str) -> Result<u32> {
        let physical = match name {
            "event_id" => Some(column_ids::EVENT_ID),
            "occurred_at" => Some(column_ids::OCCURRED_AT),
            "entity_id" => Some(column_ids::ENTITY_ID),
            "event_type" => Some(column_ids::EVENT_TYPE_ID),
            "source" => Some(column_ids::SOURCE_ID),
            _ => None,
        };
        match physical {
            Some(id) => Ok(id),
            None => self
                .file
                .footer()
                .columns
                .iter()
                .find(|column| column.name == name)
                .map(|column| column.column_id)
                .ok_or_else(|| anyhow!("no column {name}")),
        }
    }
}

impl Opened for OeReader {
    fn query(&self, query: &Query) -> Result<Partial> {
        let names = needed(query);
        let ids: Vec<u32> = names.iter().map(|n| self.column_id(n)).collect::<Result<_>>()?;
        let footer = self.file.footer();
        let mut granules = footer.granules.iter().collect::<Vec<_>>();
        for predicate in &query.predicates {
            match predicate {
                Predicate::RangeI64 {
                    column: "occurred_at",
                    high,
                    low,
                } => {
                    let keep: Vec<u32> = self
                        .file
                        .granules_for_occurred(*low, *high - 1)
                        .iter()
                        .map(|g| g.granule_id)
                        .collect();
                    granules.retain(|g| keep.contains(&g.granule_id));
                }
                Predicate::EqStr {
                    column: "entity_id",
                    value,
                } => {
                    let keep: Vec<u32> = self
                        .file
                        .granules_for_entity_hash(stable_hash(value.as_bytes()))
                        .iter()
                        .map(|g| g.granule_id)
                        .collect();
                    granules.retain(|g| keep.contains(&g.granule_id));
                }
                _ => {}
            }
        }
        let mut total = Partial::default();
        for granule in granules {
            let rows = granule.row_count as usize;
            let reads = ids
                .iter()
                .map(|id| self.file.read_column(*id, granule.granule_id).map(|read| read.data))
                .collect::<Result<Vec<ColumnData>, _>>()?;
            let hex: Vec<Vec<String>> = reads
                .iter()
                .map(|data| match data {
                    ColumnData::U128(values) => values.iter().map(|v| format!("{v:032x}")).collect(),
                    _ => Vec::new(),
                })
                .collect();
            let columns: Vec<(&str, Col)> = names
                .iter()
                .zip(&reads)
                .zip(&ids)
                .zip(&hex)
                .map(|(((name, data), id), hex)| {
                    let col = match data {
                        ColumnData::F64(values) => Col::F64(values),
                        ColumnData::I64(values) => Col::I64(values),
                        ColumnData::Strings(values) => Col::Str(values.iter().map(|v| v.unwrap_or("")).collect()),
                        ColumnData::U64(values) => {
                            let dictionary = match *id {
                                column_ids::EVENT_TYPE_ID => &footer.dictionaries.event_type,
                                column_ids::SOURCE_ID => &footer.dictionaries.source,
                                _ => panic!("unexpected u64 column {name}"),
                            };
                            Col::Str(values.iter().map(|v| dictionary[*v as usize].as_str()).collect())
                        }
                        ColumnData::U128(_) => Col::Str(hex.iter().map(String::as_str).collect()),
                        ColumnData::Decimal { .. } => panic!("unexpected decimal column {name}"),
                    };
                    (*name, col)
                })
                .collect();
            total.add(&fold_chunk(&columns, rows, query));
        }
        Ok(total)
    }
}
