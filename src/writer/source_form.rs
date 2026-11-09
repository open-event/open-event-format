use super::build::BuildRow;
use super::constant::REFERENCE_FILTER_BITS_PER_KEY;
use crate::columns::{ColumnSpec, SOURCE_FORM_COLUMNS, column_ids};
use crate::encoding::{ColumnData, StringColumn};
use crate::error::FormatError;
use crate::events::constant::EXTERNAL_ID_MAX_BYTES;
use crate::events::provenance::hex_lower_into;
use crate::events::relationships::RelationshipKind;
use crate::indexes::probabilistic::SplitBlockBloomFilter;
use crate::indexes::stable_hash;
use crate::layout::footer::{ExternalIdEntry, Footer, ReferenceFilterEntry};
use crate::layout::required_features;
use crate::layout::source_form::external_id_sentinel;

pub(crate) type SourceFormBlock = (u32, Vec<u8>, ColumnData);

pub(crate) struct SourceFormIndexes {
    pub(crate) external_ids: Vec<ExternalIdEntry>,
    pub(crate) reference_filters: Option<Vec<ReferenceFilterEntry>>,
    pub(crate) elides_event_ids: bool,
}

impl SourceFormIndexes {
    pub(crate) fn from_footer(footer: &Footer) -> Self {
        Self {
            external_ids: truncate_external_ids(footer.external_ids.clone()),
            reference_filters: footer.reference_filters.clone(),
            elides_event_ids: footer.required_feature_flags & required_features::EXTERNAL_ID_EVENT_ID_ELISION != 0,
        }
    }
}

pub(crate) fn carried_columns(
    source: Option<&Footer>,
    rows: &[BuildRow],
    delivery_lineage: bool,
) -> Vec<&'static ColumnSpec> {
    SOURCE_FORM_COLUMNS
        .iter()
        .filter(|spec| match source {
            Some(footer) => footer.columns.iter().any(|column| column.column_id == spec.column_id),
            None => {
                (delivery_lineage || !is_lineage_column(spec.column_id))
                    && rows.iter().any(|row| carries(row, spec.column_id))
            }
        })
        .collect()
}

pub(crate) fn is_lineage_column(column_id: u32) -> bool {
    matches!(
        column_id,
        column_ids::SOURCE_SCHEMA | column_ids::SOURCE_DELIVERY | column_ids::CONNECTOR_DELIVERY_HASH
    )
}

pub(crate) fn build(
    rows: &[BuildRow],
    granule_row_ranges: &[(usize, usize)],
    carried: &[&ColumnSpec],
) -> Result<(Vec<Vec<SourceFormBlock>>, SourceFormIndexes), FormatError> {
    let blocks = granule_row_ranges
        .iter()
        .map(|&(start, end)| {
            let granule = rows.get(start..end).unwrap_or_default();
            carried
                .iter()
                .map(|spec| column_block(granule, spec.column_id))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let indexes = SourceFormIndexes {
        external_ids: external_id_index(rows),
        reference_filters: reference_filters(rows, granule_row_ranges),
        elides_event_ids: carried.iter().any(|spec| spec.column_id == column_ids::EXTERNAL_ID)
            && rows.iter().any(|row| {
                row.external_id
                    .as_deref()
                    .and_then(|id| external_id_sentinel(id, row.envelope.event_id.uuid().as_u128()))
                    .is_some()
            }),
    };
    Ok((blocks, indexes))
}

fn carries(row: &BuildRow, column_id: u32) -> bool {
    match column_id {
        column_ids::EXTERNAL_ID => row.external_id.is_some(),
        column_ids::RAW_PAYLOAD => row.raw_payload.is_some(),
        column_ids::SOURCE_SCHEMA => row
            .delivery
            .as_ref()
            .is_some_and(|lineage| lineage.source_schema.is_some()),
        column_ids::SOURCE_DELIVERY => row
            .delivery
            .as_ref()
            .is_some_and(|lineage| lineage.source_delivery.is_some()),
        column_ids::CONNECTOR_DELIVERY_HASH => row.delivery.as_ref().is_some_and(|lineage| {
            lineage.connector_delivery_hash_high != 0 || lineage.connector_delivery_hash_low != 0
        }),
        _ => false,
    }
}

fn stored_text(row: &BuildRow, column_id: u32) -> Result<Option<&str>, FormatError> {
    Ok(match column_id {
        column_ids::RAW_PAYLOAD => match &row.raw_payload {
            Some(bytes) => {
                Some(simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 { what: "raw payload" })?)
            }
            None => None,
        },
        column_ids::SOURCE_SCHEMA => row
            .delivery
            .as_ref()
            .and_then(|lineage| lineage.source_schema.as_deref()),
        column_ids::SOURCE_DELIVERY => row
            .delivery
            .as_ref()
            .and_then(|lineage| lineage.source_delivery.as_deref()),
        _ => None,
    })
}

fn column_block(rows: &[BuildRow], column_id: u32) -> Result<SourceFormBlock, FormatError> {
    let mut presence = vec![0u8; rows.len().div_ceil(8)];
    let mut values = StringColumn::new();
    if column_id == column_ids::EXTERNAL_ID {
        let mut scratch = String::new();
        for (index, row) in rows.iter().enumerate() {
            let Some(id) = &row.external_id else { continue };
            if id.is_empty() || id.len() > EXTERNAL_ID_MAX_BYTES {
                return Err(FormatError::Structural {
                    rule: "an external id is 1 to 255 bytes",
                });
            }
            if let Some(byte) = presence.get_mut(index / 8) {
                *byte |= 1 << (index % 8);
            }
            if let Some(sentinel) = external_id_sentinel(id, row.envelope.event_id.uuid().as_u128()) {
                values.push(Some(sentinel));
                continue;
            }
            scratch.clear();
            hex_lower_into(id, &mut scratch);
            values.push(Some(&scratch));
        }
        return Ok((column_id, presence, ColumnData::Strings(values)));
    }
    if column_id == column_ids::CONNECTOR_DELIVERY_HASH {
        for (index, row) in rows.iter().enumerate() {
            let Some(lineage) = row.delivery.as_ref().filter(|_| carries(row, column_id)) else {
                continue;
            };
            if let Some(byte) = presence.get_mut(index / 8) {
                *byte |= 1 << (index % 8);
            }
            let hex = format!(
                "{:016x}{:016x}",
                lineage.connector_delivery_hash_high, lineage.connector_delivery_hash_low
            );
            values.push(Some(&hex));
        }
        return Ok((column_id, presence, ColumnData::Strings(values)));
    }
    for (index, row) in rows.iter().enumerate() {
        if let Some(text) = stored_text(row, column_id)? {
            if let Some(byte) = presence.get_mut(index / 8) {
                *byte |= 1 << (index % 8);
            }
            values.push(Some(text));
        }
    }
    Ok((column_id, presence, ColumnData::Strings(values)))
}

fn truncate_external_ids(mut entries: Vec<ExternalIdEntry>) -> Vec<ExternalIdEntry> {
    for entry in &mut entries {
        entry.id_hash = ExternalIdEntry::truncated_hash(entry.id_hash);
    }
    entries.sort_unstable_by_key(|entry| (entry.id_hash, entry.row_ordinal));
    entries
}

fn external_id_index(rows: &[BuildRow]) -> Vec<ExternalIdEntry> {
    let mut entries: Vec<ExternalIdEntry> = rows
        .iter()
        .enumerate()
        .filter_map(|(ordinal, row)| {
            row.external_id.as_ref().map(|id| ExternalIdEntry {
                id_hash: ExternalIdEntry::truncated_hash(stable_hash(id)),
                row_ordinal: ordinal as u64,
            })
        })
        .collect();
    entries.sort_unstable_by_key(|entry| (entry.id_hash, entry.row_ordinal));
    entries
}

fn reference_filters(rows: &[BuildRow], granule_row_ranges: &[(usize, usize)]) -> Option<Vec<ReferenceFilterEntry>> {
    if !rows.iter().any(|row| row.relationships.is_some()) {
        return None;
    }
    let mut filters = Vec::new();
    let mut text = String::new();
    let mut hashes = Vec::new();
    for kind in RelationshipKind::ALL {
        for (granule_id, &(start, end)) in granule_row_ranges.iter().enumerate() {
            hashes.clear();
            for relationships in rows
                .get(start..end)
                .unwrap_or_default()
                .iter()
                .filter_map(|row| row.relationships.as_ref())
            {
                for reference in relationships.refs().iter().filter(|reference| reference.kind == kind) {
                    text.clear();
                    reference.column_value_into(&mut text);
                    hashes.push(stable_hash(text.as_bytes()));
                }
            }
            if hashes.is_empty() {
                continue;
            }
            hashes.sort_unstable();
            hashes.dedup();
            filters.push(ReferenceFilterEntry {
                column_id: kind.column_id(),
                filter: SplitBlockBloomFilter::build(&hashes, REFERENCE_FILTER_BITS_PER_KEY).encode(),
                granule_id: granule_id as u32,
            });
        }
    }
    filters.sort_by_key(|entry| (entry.column_id, entry.granule_id));
    Some(filters)
}

#[cfg(test)]
#[path = "test/source_form.rs"]
pub(crate) mod tests;
