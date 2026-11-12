
use crate::dataset::{Column, Dataset};
use crate::format::{Format, Opened};
use crate::query::{Col, Partial, Predicate, Query, fold_chunk, needed};
use anyhow::{Result, anyhow};
use oe::artifacts::batch::{EventInput, PayloadInput, build_batch};
use oe::artifacts::frame::{FrameBuildInput, build_frame};
use oe::columns::{FreetextDeclaration, PromotionPlan, column_ids};
use oe::encoding::{ColumnData, SideStream, decode_block, take_block_rows};
use oe::encoding::predicate::{
    NumericPredicate, StringPredicate, filter_numeric_block_or_decode, filter_string_block_or_decode,
    filter_string_block_shared,
};
use oe::events::variant::VariantValue;
use oe::deletes::{DeletionVector, encode_deletion_vector};
use oe::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef, TargetIdSpace};
use oe::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use oe::indexes::stable_hash;
use oe::layout::reader::OeFile;
use oe::layout::{LayoutTargets, Profile};
use oe::security::FooterEncryption;
use oe::typed_id::TypedIdTestExt;
use oe::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, SharedStrings, build_oe_file};
use std::collections::BTreeMap;

const SPARSE_TAKE_DIVISOR: usize = 8;

pub struct Oe {
    pub lazy: bool,
}

fn config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 42,
        delivery_lineage: false,
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

pub struct EventColumns<'a> {
    entity: &'a [String],
    event_ids: &'a [String],
    event_type: &'a [String],
    occurred: &'a [i64],
    payload: Vec<(&'a str, &'a Column)>,
    roots: Option<&'a Vec<String>>,
    source: &'a [String],
}

impl<'a> EventColumns<'a> {
    pub fn new(dataset: &'a Dataset) -> Self {
        let roots = dataset.columns.iter().find(|(name, _)| name == "thread_root").map(|(_, column)| match column {
            Column::Str(values) => values,
            _ => panic!("thread_root is text"),
        });
        Self {
            entity: dataset.strs("entity_id"),
            event_ids: dataset.strs("event_id"),
            event_type: dataset.strs("event_type"),
            occurred: dataset.i64s("occurred_at"),
            payload: dataset
                .columns
                .iter()
                .filter_map(|(name, column)| name.strip_prefix("payload.").map(|field| (field, column)))
                .collect(),
            roots,
            source: dataset.strs("source"),
        }
    }

    pub fn input(&self, i: usize) -> Result<EventInput> {
        let mut fields = BTreeMap::new();
        for (name, column) in &self.payload {
            let value = match column {
                Column::F64(values) => VariantValue::Double(values[i]),
                Column::I64(values) => VariantValue::Int(values[i]),
                Column::Str(values) => VariantValue::String(values[i].clone()),
            };
            fields.insert((*name).to_owned(), value);
        }
        let relationships = match self.roots {
            Some(roots) if !roots[i].is_empty() => {
                let root = u128::from_str_radix(&roots[i], 16)?;
                Some(EventRelationships::new(vec![RelationshipRef::new(
                    RelationshipKind::Root,
                    TargetIdSpace::EventId,
                    root.to_be_bytes().to_vec(),
                )?])?)
            }
            _ => None,
        };
        Ok(EventInput {
            connector_delivery_hash_high: 0,
            connector_delivery_hash_low: 0,
            envelope: EventEnvelope {
                account_id: None,
                account_id_hash_low: 0,
                actor_id: None,
                actor_id_hash_low: 0,
                dedupe_hash_high: 0,
                dedupe_hash_low: i as u64,
                entity_id: Some(self.entity[i].clone()),
                entity_id_hash_high: 0,
                entity_id_hash_low: stable_hash(self.entity[i].as_bytes()),
                entity_type: "entity".to_owned(),
                event_id: EventId::new_test_id(0xA000 + i as u128),
                event_type: self.event_type[i].clone(),
                flags: EventFlags(0),
                ingested_at: TimestampValue::from_physical_nanos(self.occurred[i] + 1_000),
                occurred_at: TimestampValue::from_physical_nanos(self.occurred[i]),
                schema_version: 1,
                source: self.source[i].clone(),
                stream_id: StreamId(1),
                stream_sequence: i as u64,
                tenant_id: TenantId::new_test_id(7),
                trace_id_hash_low: 0,
            },
            payload: PayloadInput::Variant(VariantValue::Object(fields)),
            provenance: None,
            relationships,
            source_delivery: None,
            source_schema: None,
        })
    }
}

pub fn journal_frame(events: &[EventInput], first: usize, batch: u64) -> Result<Vec<u8>> {
    let payload = build_batch(events, 1, 0)?;
    let input = FrameBuildInput {
        committed_at_physical: 42,
        created_at_physical: 42,
        dictionary_generation_hint: 0,
        durable_batch_id: batch,
        epoch: 1,
        event_count: events.len() as u32,
        first_sequence: first as u64 + 1,
        flags: 0,
        last_sequence: (first + events.len()) as u64,
        schema_generation: 1,
        tenant_id: TenantId::new_test_id(7),
        writer_id: 1,
        writer_local_batch_id: batch,
    };
    Ok(build_frame(&input, &payload)?)
}

impl Format for Oe {
    fn name(&self) -> &'static str {
        if self.lazy { "oe (lazy open)" } else { "oe" }
    }

    fn write(&self, dataset: &Dataset) -> Result<Vec<u8>> {
        let columns = EventColumns::new(dataset);
        let mut shared = SharedStrings::default();
        let mut rows = Vec::with_capacity(dataset.rows);
        for i in 0..dataset.rows {
            let mut row = shared.build_row(OeRow {
                epoch: 1,
                event: columns.input(i)?,
                sequence: i as u64 + 1,
            });
            row.external_id = Some(columns.event_ids[i].as_bytes().to_vec());
            rows.push(row);
        }
        Ok(std::sync::Arc::unwrap_or_clone(build_oe_file(rows, &config())?.bytes))
    }

    fn open(&self, _dataset: &Dataset, bytes: Vec<u8>) -> Result<Box<dyn Opened>> {
        let file = if self.lazy {
            OeFile::open_lazy(bytes, None, None)?
        } else {
            OeFile::open(bytes, None)?
        };
        Ok(Box::new(OeReader { file }))
    }
}

fn hex_arena(values: &[u128]) -> String {
    const PAIRS: [[u8; 2]; 256] = {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut pairs = [[0u8; 2]; 256];
        let mut byte = 0;
        while byte < 256 {
            pairs[byte] = [DIGITS[byte >> 4], DIGITS[byte & 0xF]];
            byte += 1;
        }
        pairs
    };
    let mut out = vec![0u8; values.len() * 32];
    for (value, digits) in values.iter().zip(out.chunks_exact_mut(32)) {
        for (byte, pair) in value.to_be_bytes().iter().zip(digits.chunks_exact_mut(2)) {
            pair.copy_from_slice(&PAIRS[usize::from(*byte)]);
        }
    }
    String::from_utf8(out).unwrap_or_default()
}

struct OeReader {
    file: OeFile,
}

impl OeReader {
    fn rows_partial(&self, ordinals: &[u64], query: &Query) -> Result<Partial> {
        let granules = &self.file.footer().granules;
        let mut totals = Partial::default();
        let mut i = 0;
        while i < ordinals.len() {
            let granule = granules
                .iter()
                .find(|g| ordinals[i] >= g.first_row_ordinal && ordinals[i] < g.first_row_ordinal + u64::from(g.row_count))
                .ok_or_else(|| anyhow!("row {} is in no granule", ordinals[i]))?;
            let end = granule.first_row_ordinal + u64::from(granule.row_count);
            let mut picked = Vec::new();
            while i < ordinals.len() && ordinals[i] < end {
                picked.push((ordinals[i] - granule.first_row_ordinal) as usize);
                i += 1;
            }
            let mut integers: Vec<Vec<i64>> = Vec::new();
            for name in &query.fold {
                let read = self.file.read_column(self.column_id(name)?, granule.granule_id)?;
                match read.data {
                    ColumnData::I64(values) => integers.push(picked.iter().map(|&p| values[p]).collect()),
                    _ => return Err(anyhow!("index lookups fold integer columns only")),
                }
            }
            let columns: Vec<(&str, Col)> = query.fold.iter().zip(&integers).map(|(n, v)| (*n, Col::I64(v))).collect();
            let plain = Query {
                fold: query.fold.clone(),
                last_n: None,
                name: query.name,
                predicates: Vec::new(),
            };
            totals.add(&fold_chunk(&columns, picked.len(), &plain), None);
        }
        Ok(totals)
    }

    fn query_on_keys(&self, query: &Query) -> Result<Option<Partial>> {
        let footer = self.file.footer();
        let mut entity = None;
        let mut event_type = None;
        for predicate in &query.predicates {
            match predicate {
                Predicate::EqStr { column: "entity_id", value } => entity = Some((stable_hash(value.as_bytes()), value.as_str())),
                Predicate::EqStr { column: "event_type", value } => match footer.dictionaries.event_type.iter().position(|t| t == value) {
                    Some(code) => event_type = Some(code as u64),
                    None => return Ok(Some(Partial::default())),
                },
                _ => return Ok(None),
            }
        }
        let Some((low, entity_value)) = entity else { return Ok(None) };
        let fold_ids: Vec<u32> = query.fold.iter().map(|n| self.column_id(n)).collect::<Result<_>>()?;
        let mut total = Partial::default();
        let entity_test = StringPredicate::Equals(entity_value.to_owned());
        let translated = self.file.shared_dictionary_predicate(column_ids::ENTITY_ID, &entity_test);
        let type_test = event_type.map(|code| NumericPredicate::Equals(i128::from(code)));
        for granule in self.file.granules_for_entity_hash(low) {
            let id = granule.granule_id;
            let raw = self.file.read_column_raw(column_ids::ENTITY_ID, id)?;
            let keep = if raw.pipeline.side_stream()? == SideStream::FileScopeDictionary {
                match filter_string_block_shared(raw.pipeline, &raw.body, &entity_test, translated.as_ref())? {
                    Some(keep) => keep,
                    None => match self.file.read_column(column_ids::ENTITY_ID, id)?.data {
                        ColumnData::Strings(values) => entity_test.filter_decoded(&values),
                        _ => return Err(anyhow!("entity_id is not a string column")),
                    },
                }
            } else {
                filter_string_block_or_decode(raw.pipeline, &raw.body, &entity_test)?
            };
            let mut picked: Vec<usize> = keep.iter().enumerate().filter(|(_, k)| **k).map(|(i, _)| i).collect();
            if picked.is_empty() {
                continue;
            }
            if let Some(test) = &type_test {
                let raw = self.file.read_column_raw(column_ids::EVENT_TYPE_ID, id)?;
                if picked.len() * SPARSE_TAKE_DIVISOR < keep.len() {
                    let verdicts = test.filter_decoded(&take_block_rows(raw.pipeline, &raw.body, &picked)?);
                    let mut verdict = verdicts.iter();
                    picked.retain(|_| verdict.next().copied().unwrap_or(false));
                } else {
                    let verdicts = filter_numeric_block_or_decode(raw.pipeline, &raw.body, test)?;
                    picked.retain(|&row| verdicts.get(row).copied().unwrap_or(false));
                }
                if picked.is_empty() {
                    continue;
                }
            }
            let mut partial = Partial { rows: picked.len() as u64, sums: Vec::new(), tail: Vec::new() };
            for (name, column) in query.fold.iter().zip(&fold_ids) {
                let ColumnData::I64(values) = self.take_column(*column, id, &picked, keep.len())? else {
                    return Ok(None);
                };
                partial.sums.push(values.iter().map(|&v| v as i128).sum());
                if let (Some(n), "occurred_at") = (query.last_n, *name) {
                    partial.tail = values[values.len().saturating_sub(n)..].to_vec();
                }
            }
            total.add(&partial, query.last_n);
        }
        Ok(Some(total))
    }

    fn take_column(&self, column: u32, granule: u32, rows: &[usize], row_count: usize) -> Result<ColumnData> {
        if let Ok(raw) = self.file.read_column_raw(column, granule)
            && raw.presence.is_empty()
        {
            if rows.len() == row_count {
                return Ok(decode_block(raw.pipeline, &raw.body)?);
            }
            return Ok(take_block_rows(raw.pipeline, &raw.body, rows)?);
        }
        let ColumnData::I64(values) = self.file.read_column(column, granule)?.data else {
            return Err(anyhow!("fold column is not i64"));
        };
        Ok(ColumnData::I64(rows.iter().map(|&row| values[row]).collect()))
    }

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
    fn redact(&self, dataset: &Dataset, rows: &[usize]) -> Result<Option<u64>> {
        let ids = dataset.strs("event_id");
        let mut vector = DeletionVector::new();
        for &row in rows {
            let ordinal = self
                .file
                .row_by_external_id(ids[row].as_bytes())?
                .ok_or_else(|| anyhow!("event {} not found", ids[row]))?;
            vector.mark_deleted(ordinal);
        }
        let (_, bytes) = encode_deletion_vector(&vector);
        assert_eq!(vector.deleted_count(), rows.len() as u64, "every redacted event was found");
        Ok(Some(bytes.len() as u64))
    }

    fn query(&self, query: &Query) -> Result<Partial> {
        let by_index = query.predicates.iter().find_map(|p| match p {
            Predicate::EqStr { column: "thread_root", value } => {
                let root = u128::from_str_radix(value, 16).ok()?;
                Some(self.file.thread_rows(TargetIdSpace::EventId, &root.to_be_bytes()))
            }
            Predicate::EqStr { column: "event_id", value } => {
                Some(self.file.row_by_external_id(value.as_bytes()).map(|row| row.into_iter().collect()))
            }
            _ => None,
        });
        if let Some(rows) = by_index {
            return self.rows_partial(&rows?, query);
        }
        if let Some(partial) = self.query_on_keys(query)? {
            return Ok(partial);
        }
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
            let hex: Vec<String> = reads
                .iter()
                .map(|data| match data {
                    ColumnData::U128(values) => hex_arena(values),
                    _ => String::new(),
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
                        ColumnData::U128(values) => {
                            Col::Str((0..values.len()).map(|i| hex.get(i * 32..i * 32 + 32).unwrap_or("")).collect())
                        }
                        ColumnData::Decimal { .. } => panic!("unexpected decimal column {name}"),
                    };
                    (*name, col)
                })
                .collect();
            total.add(&fold_chunk(&columns, rows, query), query.last_n);
        }
        Ok(total)
    }
}
