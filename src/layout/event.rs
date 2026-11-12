use super::footer::GranuleEntry;
use super::reader::{ColumnRead, OeFile, PayloadRead};
use super::required_features;
use crate::artifacts::batch::{PAYLOAD_FLAG_EXTERNAL_REF, decode_variant_dictionary};
use crate::artifacts::overlay::LiveOverlaySegment;
use crate::columns::column_ids;
use crate::encoding::{ColumnData, StringColumn};
use crate::error::FormatError;
use crate::events::variant::{KeyDictionary, VariantRef};
use crate::events::{
    EventEnvelope, EventFlags, EventId, SequencePoint, StreamId, TimestampValue, derived_entity_id_hash,
};
use crate::file::bytes::slice;
use arrow_array::*;
use uuid::Uuid;

impl OeFile {
    pub fn envelope(&self, row_ordinal: u64) -> Result<EventEnvelope, FormatError> {
        let granule = self.granule_for_row(row_ordinal)?;
        let index = row_ordinal - granule.first_row_ordinal;
        self.cached_envelope_columns(granule.granule_id)?
            .envelope(self, index as usize)
    }

    /// Decoded envelope columns of `granule_id`, reusing the last granule's decode across calls.
    pub(crate) fn cached_envelope_columns(
        &self,
        granule_id: u32,
    ) -> Result<std::sync::Arc<EnvelopeColumns>, FormatError> {
        if let Some((cached_id, columns)) = self
            .envelope_columns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            && *cached_id == granule_id
        {
            return Ok(columns.clone());
        }
        let columns = std::sync::Arc::new(EnvelopeColumns::read(self, granule_id)?);
        *self
            .envelope_columns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((granule_id, columns.clone()));
        Ok(columns)
    }

    pub fn event(&self, row_ordinal: u64) -> Result<(EventEnvelope, PayloadRead), FormatError> {
        Ok((self.envelope(row_ordinal)?, self.payload(row_ordinal)?))
    }

    pub fn granule_envelopes(&self, granule_id: u32) -> Result<Vec<EventEnvelope>, FormatError> {
        let columns = EnvelopeColumns::read(self, granule_id)?;
        (0..columns.rows).map(|row| columns.envelope(self, row)).collect()
    }

    pub fn entity_granules(
        &self,
        entity_id_hash_low: u64,
        entity_id_hash_high: u64,
        from: SequencePoint,
        to: SequencePoint,
    ) -> Vec<&GranuleEntry> {
        let admits = |column_id: u32, granule_id: u32, value: u64| {
            let bounds = self
                .block_page_stats(column_id, granule_id)
                .and_then(|stats| stats.min_i128.zip(stats.max_i128));
            match bounds {
                Some((min, max)) => {
                    let value = i128::from(value);
                    (min <= value && value <= max, min == value && max == value)
                }
                None => (true, false),
            }
        };
        let mut candidates: Vec<(&GranuleEntry, bool, SequencePoint)> = self
            .granules_for_entity_hash(entity_id_hash_low)
            .into_iter()
            .filter_map(|granule| {
                let (low_admits, low_only) =
                    admits(column_ids::ENTITY_ID_HASH_LOW, granule.granule_id, entity_id_hash_low);
                let (high_admits, high_only) =
                    admits(column_ids::ENTITY_ID_HASH_HIGH, granule.granule_id, entity_id_hash_high);
                (low_admits && high_admits).then_some((granule, low_only && high_only, last_point(granule)))
            })
            .collect();
        let mut next_first = None;
        for (granule, only_entity, highest) in candidates.iter_mut().rev() {
            if *only_entity {
                next_first = Some(first_point(granule));
            } else if let Some(next_first) = next_first {
                *highest = (*highest).min(next_first);
            }
        }
        let mut previous_last = None;
        candidates
            .into_iter()
            .filter(|&(granule, only_entity, highest)| {
                let mut lowest = first_point(granule);
                if only_entity {
                    previous_last = Some(last_point(granule));
                } else if let Some(previous_last) = previous_last {
                    lowest = lowest.max(previous_last);
                }
                lowest <= highest && lowest <= to && from <= highest
            })
            .map(|(granule, _, _)| granule)
            .collect()
    }

    pub(crate) fn granule_for_row(&self, row_ordinal: u64) -> Result<&GranuleEntry, FormatError> {
        let granules = &self.footer().granules;
        let index = granules.partition_point(|granule| granule.first_row_ordinal <= row_ordinal);
        index
            .checked_sub(1)
            .and_then(|index| granules.get(index))
            .filter(|granule| row_ordinal < granule.first_row_ordinal + u64::from(granule.row_count))
            .ok_or(FormatError::RefOutOfRange { what: "row ordinal" })
    }
}

impl LiveOverlaySegment {
    pub fn event(&self, row: usize) -> Result<(EventEnvelope, PayloadRead), FormatError> {
        OverlayRows::new(self)?.event(row)
    }
}

/// One overlay segment's v1 columns resolved by name once, plus its variant dictionary decoded on first use, so
/// reading many rows of a segment neither searches the schema nor re-decodes the dictionary per row.
pub(crate) struct OverlayRows<'a> {
    segment: &'a LiveOverlaySegment,
    dictionary: Option<KeyDictionary>,
    account_id: &'a StringArray,
    account_id_hash_low: &'a UInt64Array,
    actor_id: &'a StringArray,
    actor_id_hash_low: &'a UInt64Array,
    dedupe_hash_high: &'a UInt64Array,
    dedupe_hash_low: &'a UInt64Array,
    entity_id: &'a StringArray,
    entity_id_hash_high: &'a UInt64Array,
    entity_id_hash_low: &'a UInt64Array,
    entity_type: &'a StringArray,
    event_id: &'a FixedSizeBinaryArray,
    event_type: &'a StringArray,
    flags: &'a UInt32Array,
    ingested_at: &'a TimestampNanosecondArray,
    occurred_at: &'a TimestampNanosecondArray,
    payload_flags: &'a UInt32Array,
    payload_ref: &'a UInt64Array,
    schema_version: &'a UInt32Array,
    source: &'a StringArray,
    stream_id: &'a UInt64Array,
    stream_sequence: &'a UInt64Array,
    trace_id_hash_low: &'a UInt64Array,
}

impl<'a> OverlayRows<'a> {
    pub(crate) fn new(segment: &'a LiveOverlaySegment) -> Result<Self, FormatError> {
        Ok(Self {
            segment,
            dictionary: None,
            account_id: overlay_column(segment, "account_id")?,
            account_id_hash_low: overlay_column(segment, "account_id_hash_low")?,
            actor_id: overlay_column(segment, "actor_id")?,
            actor_id_hash_low: overlay_column(segment, "actor_id_hash_low")?,
            dedupe_hash_high: overlay_column(segment, "dedupe_hash_high")?,
            dedupe_hash_low: overlay_column(segment, "dedupe_hash_low")?,
            entity_id: overlay_column(segment, "entity_id")?,
            entity_id_hash_high: overlay_column(segment, "entity_id_hash_high")?,
            entity_id_hash_low: overlay_column(segment, "entity_id_hash_low")?,
            entity_type: overlay_column(segment, "entity_type")?,
            event_id: overlay_column(segment, "event_id")?,
            event_type: overlay_column(segment, "event_type")?,
            flags: overlay_column(segment, "flags")?,
            ingested_at: overlay_column(segment, "ingested_at")?,
            occurred_at: overlay_column(segment, "occurred_at")?,
            payload_flags: overlay_column(segment, "payload_flags")?,
            payload_ref: overlay_column(segment, "payload_ref")?,
            schema_version: overlay_column(segment, "schema_version")?,
            source: overlay_column(segment, "source")?,
            stream_id: overlay_column(segment, "stream_id")?,
            stream_sequence: overlay_column(segment, "stream_sequence")?,
            trace_id_hash_low: overlay_column(segment, "trace_id_hash_low")?,
        })
    }

    pub(crate) fn event(&mut self, row: usize) -> Result<(EventEnvelope, PayloadRead), FormatError> {
        if row >= self.segment.batch.num_rows() {
            return Err(FormatError::RefOutOfRange { what: "overlay row" });
        }
        let time_at = |column: &TimestampNanosecondArray| TimestampValue::from_physical_nanos(column.value(row));
        let text_at = |column: &StringArray| column.is_valid(row).then(|| column.value(row).to_owned());
        let required_text_at = |column: &StringArray| {
            text_at(column).ok_or(FormatError::Structural {
                rule: "LiveOverlay required string column holds a null",
            })
        };
        let event_id: [u8; 16] = self
            .event_id
            .value(row)
            .try_into()
            .map_err(|_| FormatError::Structural {
                rule: "event_id must be 16 bytes",
            })?;
        let envelope = EventEnvelope {
            account_id: text_at(self.account_id),
            account_id_hash_low: self.account_id_hash_low.value(row),
            actor_id: text_at(self.actor_id),
            actor_id_hash_low: self.actor_id_hash_low.value(row),
            dedupe_hash_high: self.dedupe_hash_high.value(row),
            dedupe_hash_low: self.dedupe_hash_low.value(row),
            entity_id: text_at(self.entity_id),
            entity_id_hash_high: self.entity_id_hash_high.value(row),
            entity_id_hash_low: self.entity_id_hash_low.value(row),
            entity_type: required_text_at(self.entity_type)?,
            event_id: EventId::from_uuid(Uuid::from_u128(u128::from_le_bytes(event_id))),
            event_type: required_text_at(self.event_type)?,
            flags: EventFlags(self.flags.value(row)),
            ingested_at: time_at(self.ingested_at),
            occurred_at: time_at(self.occurred_at),
            schema_version: self.schema_version.value(row),
            source: required_text_at(self.source)?,
            stream_id: StreamId(self.stream_id.value(row)),
            stream_sequence: self.stream_sequence.value(row),
            tenant_id: self.segment.meta.tenant_id,
            trace_id_hash_low: self.trace_id_hash_low.value(row),
        };
        let payload_ref = self.payload_ref.value(row);
        let len = (payload_ref >> 32) as usize;
        let payload = if len == 0 {
            PayloadRead::None
        } else {
            let bytes = slice(
                &self.segment.meta.payload_arena,
                (payload_ref & 0xFFFF_FFFF) as usize,
                len,
                "overlay payload",
            )?;
            if self.payload_flags.value(row) & PAYLOAD_FLAG_EXTERNAL_REF != 0 {
                PayloadRead::External(String::from_utf8(bytes.to_vec()).map_err(|_| FormatError::InvalidUtf8 {
                    what: "overlay external payload reference",
                })?)
            } else {
                let dictionary = match &mut self.dictionary {
                    Some(dictionary) => dictionary,
                    slot @ None => slot.insert(decode_variant_dictionary(&self.segment.meta.variant_dictionary)?),
                };
                PayloadRead::Value(VariantRef::new(bytes).decode(dictionary)?)
            }
        };
        Ok((envelope, payload))
    }
}

fn first_point(granule: &GranuleEntry) -> SequencePoint {
    SequencePoint {
        epoch: granule.first_epoch,
        sequence: granule.first_sequence,
    }
}

fn last_point(granule: &GranuleEntry) -> SequencePoint {
    SequencePoint {
        epoch: granule.last_epoch,
        sequence: granule.last_sequence,
    }
}

pub(crate) fn overlay_column<'a, T: Array + 'static>(
    segment: &'a LiveOverlaySegment,
    name: &'static str,
) -> Result<&'a T, FormatError> {
    segment
        .batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or(FormatError::Structural {
            rule: "LiveOverlay batch is missing a v1 column",
        })
}

pub(crate) fn read_u64s(file: &OeFile, column_id: u32, granule_id: u32, rows: usize) -> Result<Vec<u64>, FormatError> {
    match file.read_column(column_id, granule_id)?.data {
        ColumnData::U64(values) if values.len() == rows => Ok(values),
        _ => Err(FormatError::Structural {
            rule: "an envelope column holds one u64 per row",
        }),
    }
}

fn read_i64s(file: &OeFile, column_id: u32, granule_id: u32, rows: usize) -> Result<Vec<i64>, FormatError> {
    match file.read_column(column_id, granule_id)?.data {
        ColumnData::I64(values) if values.len() == rows => Ok(values),
        _ => Err(FormatError::Structural {
            rule: "an envelope column holds one i64 per row",
        }),
    }
}

fn read_u128s(file: &OeFile, column_id: u32, granule_id: u32, rows: usize) -> Result<Vec<u128>, FormatError> {
    match file.read_column(column_id, granule_id)?.data {
        ColumnData::U128(values) if values.len() == rows => Ok(values),
        _ => Err(FormatError::Structural {
            rule: "an envelope column holds one u128 per row",
        }),
    }
}

fn read_strings(file: &OeFile, column_id: u32, granule_id: u32) -> Result<(Vec<u8>, StringColumn), FormatError> {
    let ColumnRead { data, presence } = file.read_column(column_id, granule_id)?;
    let ColumnData::Strings(values) = data else {
        return Err(FormatError::Structural {
            rule: "an envelope identity column holds strings",
        });
    };
    Ok((presence, values))
}

#[derive(Debug)]
struct IdColumn {
    dense_index: Option<Vec<Option<u32>>>,
    values: StringColumn,
}

impl IdColumn {
    fn new((presence, values): (Vec<u8>, StringColumn), rows: usize) -> Self {
        let dense_index = (values.len() != rows).then(|| {
            let mut next = 0u32;
            (0..rows)
                .map(|row| {
                    presence
                        .get(row / 8)
                        .is_some_and(|byte| byte & (1 << (row % 8)) != 0)
                        .then(|| {
                            next += 1;
                            next - 1
                        })
                })
                .collect()
        });
        Self { dense_index, values }
    }

    fn at(&self, row: usize) -> Option<String> {
        let index = match &self.dense_index {
            Some(dense_index) => dense_index.get(row).copied().flatten()? as usize,
            None => row,
        };
        self.values.get(index).flatten().map(str::to_owned)
    }
}

pub(crate) fn entity_hash_columns_elided(file: &OeFile) -> bool {
    file.footer().required_feature_flags & required_features::ENTITY_HASH_COLUMNS_ELIDED != 0
}

pub(crate) fn read_entity_hashes(
    file: &OeFile,
    granule_id: u32,
    rows: usize,
) -> Result<(Vec<u64>, Vec<u64>), FormatError> {
    if !entity_hash_columns_elided(file) {
        return Ok((
            read_u64s(file, column_ids::ENTITY_ID_HASH_LOW, granule_id, rows)?,
            read_u64s(file, column_ids::ENTITY_ID_HASH_HIGH, granule_id, rows)?,
        ));
    }
    Ok(derive_entity_hashes(
        &read_strings(file, column_ids::ENTITY_ID, granule_id)?,
        rows,
    ))
}

fn derive_entity_hashes((presence, values): &(Vec<u8>, StringColumn), rows: usize) -> (Vec<u64>, Vec<u64>) {
    let mut lows = Vec::with_capacity(rows);
    let mut highs = Vec::with_capacity(rows);
    let dense = values.len() == rows;
    let mut next = 0usize;
    for row in 0..rows {
        let id = if dense {
            values.get(row).flatten()
        } else if presence.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0) {
            next += 1;
            values.get(next - 1).flatten()
        } else {
            None
        };
        let (low, high) = derived_entity_id_hash(id);
        lows.push(low);
        highs.push(high);
    }
    (lows, highs)
}

fn dictionary_entry<'a>(values: &'a [String], ids: &[u64], row: usize) -> Result<&'a str, FormatError> {
    let id = ids.get(row).copied().unwrap_or_default();
    values
        .get(id as usize)
        .map(String::as_str)
        .ok_or(FormatError::RefOutOfRange {
            what: "file dictionary id",
        })
}

#[derive(Debug)]
pub(crate) struct EnvelopeColumns {
    account_id: IdColumn,
    account_id_hash_low: Vec<u64>,
    actor_id: IdColumn,
    actor_id_hash_low: Vec<u64>,
    dedupe_hash_high: Vec<u64>,
    dedupe_hash_low: Vec<u64>,
    entity_id: IdColumn,
    entity_id_hash_high: Vec<u64>,
    entity_id_hash_low: Vec<u64>,
    entity_type_id: Vec<u64>,
    event_id: Vec<u128>,
    event_type_id: Vec<u64>,
    flags: Vec<u64>,
    ingested_at: Vec<i64>,
    occurred_at: Vec<i64>,
    rows: usize,
    schema_version: Vec<u64>,
    source_id: Vec<u64>,
    stream_id: Vec<u64>,
    stream_sequence: Vec<u64>,
    trace_id_hash_low: Vec<u64>,
}

impl EnvelopeColumns {
    pub(crate) fn read(file: &OeFile, granule_id: u32) -> Result<Self, FormatError> {
        let rows = file
            .granule_entry(granule_id)
            .ok_or(FormatError::RefOutOfRange { what: "granule id" })?
            .row_count as usize;
        let u64s = |column_id| read_u64s(file, column_id, granule_id, rows);
        let stream_id = u64s(column_ids::STREAM_ID)?;
        let stream_sequence = u64s(column_ids::STREAM_SEQUENCE)?;
        let occurred_at = read_i64s(file, column_ids::OCCURRED_AT, granule_id, rows)?;
        let ingested_at = read_i64s(file, column_ids::INGESTED_AT, granule_id, rows)?;
        let source_id = u64s(column_ids::SOURCE_ID)?;
        let event_type_id = u64s(column_ids::EVENT_TYPE_ID)?;
        let entity_type_id = u64s(column_ids::ENTITY_TYPE_ID)?;
        let flags = u64s(column_ids::FLAGS)?;
        let schema_version = u64s(column_ids::SCHEMA_VERSION)?;
        let event_id = read_u128s(file, column_ids::EVENT_ID, granule_id, rows)?;
        let entity_id = read_strings(file, column_ids::ENTITY_ID, granule_id)?;
        let (entity_id_hash_low, entity_id_hash_high) = if entity_hash_columns_elided(file) {
            derive_entity_hashes(&entity_id, rows)
        } else {
            (
                u64s(column_ids::ENTITY_ID_HASH_LOW)?,
                u64s(column_ids::ENTITY_ID_HASH_HIGH)?,
            )
        };
        let entity_id = IdColumn::new(entity_id, rows);
        let actor_id = IdColumn::new(read_strings(file, column_ids::ACTOR_ID, granule_id)?, rows);
        let account_id = IdColumn::new(read_strings(file, column_ids::ACCOUNT_ID, granule_id)?, rows);
        let actor_id_hash_low = u64s(column_ids::ACTOR_ID_HASH_LOW)?;
        let account_id_hash_low = u64s(column_ids::ACCOUNT_ID_HASH_LOW)?;
        let trace_id_hash_low = u64s(column_ids::TRACE_ID_HASH_LOW)?;
        let dedupe_hash_low = u64s(column_ids::DEDUPE_HASH_LOW)?;
        let dedupe_hash_high = u64s(column_ids::DEDUPE_HASH_HIGH)?;
        let dictionary = &file.footer().dictionaries;
        for row in 0..rows {
            dictionary_entry(&dictionary.entity_type, &entity_type_id, row)?;
            dictionary_entry(&dictionary.event_type, &event_type_id, row)?;
            dictionary_entry(&dictionary.source, &source_id, row)?;
        }
        Ok(Self {
            account_id,
            account_id_hash_low,
            actor_id,
            actor_id_hash_low,
            dedupe_hash_high,
            dedupe_hash_low,
            entity_id,
            entity_id_hash_high,
            entity_id_hash_low,
            entity_type_id,
            event_id,
            event_type_id,
            flags,
            ingested_at,
            occurred_at,
            rows,
            schema_version,
            source_id,
            stream_id,
            stream_sequence,
            trace_id_hash_low,
        })
    }

    pub(crate) fn envelope(&self, file: &OeFile, row: usize) -> Result<EventEnvelope, FormatError> {
        if row >= self.rows {
            return Err(FormatError::RefOutOfRange { what: "row ordinal" });
        }
        let at = |values: &[u64]| values.get(row).copied().unwrap_or_default();
        let dictionary = &file.footer().dictionaries;
        Ok(EventEnvelope {
            account_id: self.account_id.at(row),
            account_id_hash_low: at(&self.account_id_hash_low),
            actor_id: self.actor_id.at(row),
            actor_id_hash_low: at(&self.actor_id_hash_low),
            dedupe_hash_high: at(&self.dedupe_hash_high),
            dedupe_hash_low: at(&self.dedupe_hash_low),
            entity_id: self.entity_id.at(row),
            entity_id_hash_high: at(&self.entity_id_hash_high),
            entity_id_hash_low: at(&self.entity_id_hash_low),
            entity_type: dictionary_entry(&dictionary.entity_type, &self.entity_type_id, row)?.to_owned(),
            event_id: EventId::from_uuid(Uuid::from_u128(self.event_id.get(row).copied().unwrap_or_default())),
            event_type: dictionary_entry(&dictionary.event_type, &self.event_type_id, row)?.to_owned(),
            flags: EventFlags(at(&self.flags) as u32),
            ingested_at: TimestampValue::from_physical_nanos(self.ingested_at.get(row).copied().unwrap_or_default()),
            occurred_at: TimestampValue::from_physical_nanos(self.occurred_at.get(row).copied().unwrap_or_default()),
            schema_version: at(&self.schema_version) as u32,
            source: dictionary_entry(&dictionary.source, &self.source_id, row)?.to_owned(),
            stream_id: StreamId(at(&self.stream_id)),
            stream_sequence: at(&self.stream_sequence),
            tenant_id: file.header().tenant_id,
            trace_id_hash_low: at(&self.trace_id_hash_low),
        })
    }
}

#[cfg(test)]
#[path = "test/event.rs"]
mod tests;
