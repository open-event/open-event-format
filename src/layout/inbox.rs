use super::entity_scan::{DeletesAndCorrections, EntityEvent};
use super::event::{EnvelopeColumns, entity_hash_columns_elided, read_entity_hashes};
use super::footer::{GranuleEntry, InboxRecipientEntry};
use super::optional_features;
use super::reader::scan::ScanColumn;
use super::reader::{ColumnRead, OeFile, granules_overlapping_rows};
use crate::columns::column_ids;
use crate::deletes::{DeletionVector, RowSelection};
use crate::encoding::ColumnData;
use crate::error::{FormatError, InboxError};
use crate::events::{EventEnvelope, SequencePoint, TenantId};
use crate::field_deletion_store::FieldRedactions;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Recipient {
    pub entity_id_hash_high: u64,
    pub entity_id_hash_low: u64,
}

pub trait AckWatermarks {
    fn acked_through(&self, tenant_id: TenantId, recipient: Recipient) -> Result<Option<SequencePoint>, InboxError>;
}

pub fn read_inbox(
    files: &[&OeFile],
    tenant_id: TenantId,
    recipient: Recipient,
    watermarks: &dyn AckWatermarks,
    deletes: &dyn DeletesAndCorrections,
) -> Result<Vec<EntityEvent>, InboxError> {
    let acked = watermarks.acked_through(tenant_id, recipient)?;
    let mut events = Vec::new();
    for file in files.iter().filter(|file| file.header().tenant_id == tenant_id) {
        let granules = recipient_granules(file, recipient, acked);
        if granules.is_empty() {
            continue;
        }
        let deleted = deletes.deletion_vector(tenant_id, file.header().file_id)?;
        let redactions = deletes.field_redactions(tenant_id, file.header().file_id)?;
        read_rows(
            file,
            &granules,
            recipient,
            acked,
            (deleted.as_ref(), redactions.as_ref()),
            &mut events,
        )?;
    }
    events.sort_by_key(|event| event.point);
    Ok(events)
}

impl OeFile {
    pub fn inbox_recipients(&self) -> Option<&[InboxRecipientEntry]> {
        let directory = &self.footer().inbox_recipients;
        (self.usable_optional_features() & optional_features::INBOX_RECIPIENT_DIRECTORY != 0 && !directory.is_empty())
            .then_some(directory.as_slice())
    }

    pub fn fully_acked(&self, watermarks: &dyn AckWatermarks) -> Result<bool, InboxError> {
        let Some(directory) = self.inbox_recipients() else {
            return Ok(false);
        };
        let tenant_id = self.header().tenant_id;
        for entry in directory {
            let acked = watermarks.acked_through(tenant_id, recipient_of(entry))?;
            if acked.is_none_or(|acked| acked < last_point(entry)) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn recipient_granules(file: &OeFile, recipient: Recipient, acked: Option<SequencePoint>) -> Vec<&GranuleEntry> {
    let Some(directory) = file.inbox_recipients() else {
        let from = acked.unwrap_or(SequencePoint { epoch: 0, sequence: 0 });
        let to = SequencePoint {
            epoch: u64::MAX,
            sequence: u64::MAX,
        };
        return file.entity_granules(recipient.entity_id_hash_low, recipient.entity_id_hash_high, from, to);
    };
    let key = (recipient.entity_id_hash_low, recipient.entity_id_hash_high);
    let found = directory.binary_search_by_key(&key, |entry| (entry.entity_id_hash_low, entry.entity_id_hash_high));
    let Some(entry) = found.ok().and_then(|index| directory.get(index)) else {
        return Vec::new();
    };
    if acked.is_some_and(|acked| last_point(entry) <= acked) {
        return Vec::new();
    }
    let end = entry.first_row_ordinal.saturating_add(entry.row_count);
    granules_overlapping_rows(&file.footer().granules, &(entry.first_row_ordinal..end))
        .iter()
        .filter(|granule| {
            granule.first_row_ordinal < end
                && entry.first_row_ordinal < granule.first_row_ordinal + u64::from(granule.row_count)
        })
        .collect()
}

fn read_rows(
    file: &OeFile,
    granules: &[&GranuleEntry],
    recipient: Recipient,
    acked: Option<SequencePoint>,
    (deleted, redactions): (Option<&DeletionVector>, Option<&FieldRedactions>),
    out: &mut Vec<EntityEvent>,
) -> Result<(), FormatError> {
    let no_deletes = DeletionVector::new();
    let granule_ids: Vec<u32> = granules.iter().map(|granule| granule.granule_id).collect();
    let elided = entity_hash_columns_elided(file);
    let columns: &[u32] = if elided {
        &[column_ids::EPOCH, column_ids::SEQUENCE]
    } else {
        &[
            column_ids::EPOCH,
            column_ids::SEQUENCE,
            column_ids::ENTITY_ID_HASH_LOW,
            column_ids::ENTITY_ID_HASH_HIGH,
        ]
    };
    file.scan_projected_serial_live_redacted(
        columns,
        &granule_ids,
        deleted.unwrap_or(&no_deletes),
        redactions,
        |batch, selection| {
            let rows = batch.row_count as usize;
            let first_row_ordinal = file
                .granule_entry(batch.granule_id)
                .map(|granule| granule.first_row_ordinal)
                .ok_or(FormatError::RefOutOfRange {
                    what: "scanned granule missing from the granule directory",
                })?;
            let mut scanned = batch
                .columns
                .into_iter()
                .map(|column| scanned_u64s(column, rows))
                .collect::<Result<Vec<_>, _>>()?;
            if elided {
                let (lows, highs) = read_entity_hashes(file, batch.granule_id, rows)?;
                scanned.push(lows);
                scanned.push(highs);
            }
            let [epochs, sequences, lows, highs] =
                <[Vec<u64>; 4]>::try_from(scanned).map_err(|_| FormatError::Structural {
                    rule: "a scan batch holds one block per projected column",
                })?;
            let mut envelopes = None;
            let keys = epochs.iter().zip(&sequences).zip(lows.iter().zip(&highs));
            for (row_in_granule, ((&epoch, &sequence), (&low, &high))) in keys.enumerate() {
                let point = SequencePoint { epoch, sequence };
                let row_ordinal = first_row_ordinal + row_in_granule as u64;
                let live = match &selection {
                    RowSelection::All => true,
                    RowSelection::None => false,
                    RowSelection::Some(keep) => keep.get(row_in_granule).copied().unwrap_or(false),
                };
                if !live
                    || low != recipient.entity_id_hash_low
                    || high != recipient.entity_id_hash_high
                    || acked.is_some_and(|acked| point <= acked)
                {
                    continue;
                }
                let envelope = take_envelope(file, batch.granule_id, &mut envelopes, row_in_granule)?;
                out.push(EntityEvent {
                    envelope,
                    payload: match redactions {
                        Some(redactions) => file.payload_redacted(row_ordinal, redactions)?,
                        None => file.payload(row_ordinal)?,
                    },
                    point,
                });
            }
            Ok(())
        },
    )
}

fn scanned_u64s(column: ScanColumn, rows: usize) -> Result<Vec<u64>, FormatError> {
    match column {
        ScanColumn::Materialized(ColumnRead {
            data: ColumnData::U64(values),
            ..
        }) if values.len() == rows => Ok(values),
        _ => Err(FormatError::Structural {
            rule: "an envelope column holds one u64 per row",
        }),
    }
}

#[inline(never)]
fn take_envelope(
    file: &OeFile,
    granule_id: u32,
    envelopes: &mut Option<EnvelopeColumns>,
    row_in_granule: usize,
) -> Result<EventEnvelope, FormatError> {
    let columns = match envelopes {
        Some(columns) => columns,
        None => envelopes.insert(EnvelopeColumns::read(file, granule_id)?),
    };
    columns
        .envelope(file, row_in_granule)
        .map_err(|_| FormatError::RefOutOfRange { what: "row in granule" })
}

fn recipient_of(entry: &InboxRecipientEntry) -> Recipient {
    Recipient {
        entity_id_hash_high: entry.entity_id_hash_high,
        entity_id_hash_low: entry.entity_id_hash_low,
    }
}

fn last_point(entry: &InboxRecipientEntry) -> SequencePoint {
    SequencePoint {
        epoch: entry.last_epoch,
        sequence: entry.last_sequence,
    }
}

#[cfg(test)]
#[path = "test/inbox.rs"]
mod tests;
