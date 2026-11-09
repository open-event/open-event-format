//! Per-recipient message queues - to-device messages, device inboxes, an outgoing federation queue - stored as inbox
//! files and drained by acknowledgement.
//!
//! An inbox file is an ordinary file with the [`Profile::Inbox`](super::Profile::Inbox) profile whose rows are grouped
//! by recipient (the row's entity) and, within a recipient, kept in queue order (`(epoch, sequence)`). Its footer
//! carries a recipient directory saying where each recipient's rows are and the newest one among them. Each recipient
//! acknowledges its queue up to a point; reading the queue never returns a row at or below that point, nor a row the
//! application deleted, and a file every recipient has acknowledged in full can be retired.
//!
//! See: oe-file-layout/spec.md

use super::entity_scan::{DeletesAndCorrections, EntityEvent};
use super::event::{entity_hash_columns_elided, read_entity_hashes};
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
use std::vec::IntoIter;

/// Who an inbox row is addressed to: the row's entity, by the two identity hashes every row stores beside its entity
/// id.
///
/// See: oe-file-layout/spec.md
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Recipient {
    pub entity_id_hash_high: u64,
    pub entity_id_hash_low: u64,
}

/// How far each recipient has acknowledged its queue: everything at or below that point is done with and is never
/// served again.
///
/// Acknowledgements must survive restarts, so the application keeps them in its own durable storage and backs this
/// interface with it. A recipient's watermark only ever moves forward.
///
/// See: oe-file-lifecycle/spec.md
pub trait AckWatermarks {
    /// The newest queue point `recipient` has acknowledged, or `None` when it has acknowledged nothing yet.
    fn acked_through(&self, tenant_id: TenantId, recipient: Recipient) -> Result<Option<SequencePoint>, InboxError>;
}

/// Reads `recipient`'s queue from `files`, oldest first: every row addressed to it that it has not yet acknowledged
/// and the application has not deleted.
///
/// Hand it the inbox files of the snapshot being read; files of another tenant are ignored. The acknowledgement point
/// comes from `watermarks` and the deleted rows from `deletes`, so a row at or below the recipient's watermark, or one
/// deleted in its file, is never returned.
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
    /// This file's recipient directory, one entry per recipient in row order, or `None` when the file carries none (it
    /// is not an inbox file, or its directory was dropped).
    pub fn inbox_recipients(&self) -> Option<&[InboxRecipientEntry]> {
        let directory = &self.footer().inbox_recipients;
        (self.usable_optional_features() & optional_features::INBOX_RECIPIENT_DIRECTORY != 0 && !directory.is_empty())
            .then_some(directory.as_slice())
    }

    /// True when every recipient with rows in this file has acknowledged its newest one, so nothing in the file can be
    /// served again and it may be retired. False for a file without a recipient directory, which cannot tell.
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

/// The granules of `file` that can hold rows of `recipient` above `acked`. With a recipient directory these are the
/// granules overlapping the recipient's row range, and none when it has no rows or has acknowledged all of them;
/// without one, the granules its entity hash bounds admit.
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

/// Adds the rows of `granules` that belong to `recipient`, sit above `acked`, and are not deleted.
fn read_rows(
    file: &OeFile,
    granules: &[&GranuleEntry],
    recipient: Recipient,
    acked: Option<SequencePoint>,
    (deleted, redactions): (Option<&DeletionVector>, Option<&FieldRedactions>),
    out: &mut Vec<EntityEvent>,
) -> Result<(), FormatError> {
    // The key columns come through the live scan: a granule the deletion vector empties is never decoded, and one it
    // leaves untouched needs no per-row deletion test.
    let no_deletes = DeletionVector::new();
    let granule_ids: Vec<u32> = granules.iter().map(|granule| granule.granule_id).collect();
    // A file that omits the entity hash blocks has its recipient hashes derived from `entity_id`, per granule.
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
                .footer()
                .granules
                .iter()
                .find(|granule| granule.granule_id == batch.granule_id)
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

/// A scanned key column's values, one `u64` per row.
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

/// Moves row `row_in_granule`'s envelope out of its granule, decoding the whole granule the first time a row of it is
/// needed. Rows must come in increasing order: `envelopes` holds the granule's envelopes from the row after the last
/// one taken, and the ones skipped over are dropped.
#[inline(never)]
fn take_envelope(
    file: &OeFile,
    granule_id: u32,
    envelopes: &mut Option<(IntoIter<EventEnvelope>, usize)>,
    row_in_granule: usize,
) -> Result<EventEnvelope, FormatError> {
    if envelopes.is_none() {
        *envelopes = Some((file.granule_envelopes(granule_id)?.into_iter(), 0));
    }
    envelopes
        .as_mut()
        .and_then(|(envelopes, next)| {
            let envelope = envelopes.nth(row_in_granule - *next);
            *next = row_in_granule + 1;
            envelope
        })
        .ok_or(FormatError::RefOutOfRange { what: "row in granule" })
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
