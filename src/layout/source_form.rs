//! Reads back what a file keeps of each event's original form: the exact payload bytes it arrived as, its external
//! protocol id, and its multi-signer provenance - and finds events by external id through the file's own index
//! instead of scanning every granule.
//!
//! See: oe-file-layout/spec.md

use super::footer::{Footer, GranuleEntry};
use super::reader::{ColumnRead, OeFile};
use super::required_features;
use crate::columns::column_ids;
use crate::encoding::ColumnData;
use crate::error::FormatError;
use crate::indexes::probabilistic::SplitBlockBloomFilter;
use crate::indexes::stable_hash;
use std::borrow::Cow;

impl OeFile {
    /// The event's payload exactly as it arrived, byte for byte - for a signed JSON event, the bytes whose signature
    /// re-verifies. `None` when the row's stream did not keep raw payloads, or the file carries none at all.
    ///
    /// A deletion or correction applies to this the same way it applies to [`Self::payload`]: a caller drops deleted
    /// rows before reading either, and a field-level redaction withholds the whole raw payload (see
    /// [`FieldDeletionVector::withholds_raw_payload`](crate::deletes::FieldDeletionVector::withholds_raw_payload)).
    pub fn raw_payload(&self, row_ordinal: u64) -> Result<Option<Cow<'_, [u8]>>, FormatError> {
        Ok(self
            .source_form_text(column_ids::RAW_PAYLOAD, row_ordinal)?
            .map(|text| Cow::Owned(text.into_bytes())))
    }

    /// The event's external protocol id (a protocol `$...` id, say), or `None` when the row carries none.
    pub fn external_id(&self, row_ordinal: u64) -> Result<Option<Vec<u8>>, FormatError> {
        let elision = self.external_id_elision_declared();
        self.with_source_form_text(column_ids::EXTERNAL_ID, row_ordinal, |stored| {
            stored
                .map(|stored| {
                    let mut id = Vec::new();
                    expand_external_id(stored, elision, || self.cached_event_id(row_ordinal), &mut id)?;
                    Ok(id)
                })
                .transpose()
        })?
    }

    /// The row that holds the event with `external_id`, found through the file's external-id index without scanning
    /// a granule. When the file holds the id more than once, the last row - the newest event - answers. `None` when the
    /// file does not hold the id.
    pub fn row_by_external_id(&self, external_id: &[u8]) -> Result<Option<u64>, FormatError> {
        let mut newest = None;
        self.for_each_row_with_external_id(external_id, |row| newest = Some(row))?;
        Ok(newest)
    }

    /// Every row that holds the event with `external_id`, in row order. Usually one row, or none.
    pub fn rows_with_external_id(&self, external_id: &[u8]) -> Result<Vec<u64>, FormatError> {
        let mut rows = Vec::new();
        self.for_each_row_with_external_id(external_id, |row| rows.push(row))?;
        Ok(rows)
    }

    /// Hands `found` every row that holds `external_id`, in row order, confirming each index candidate against the
    /// stored id. One decode buffer serves every candidate, so a lookup allocates no per-candidate id.
    fn for_each_row_with_external_id(&self, external_id: &[u8], mut found: impl FnMut(u64)) -> Result<(), FormatError> {
        // The index keys on the hash's high 32 bits (an older file's full hashes are sorted by them all the same), so
        // ids that share those bits are all candidates and confirmation tells them apart.
        let key = (stable_hash(external_id) >> 32) as u32;
        let entries = &self.footer().external_ids;
        let first = entries.partition_point(|entry| entry.key() < key);
        let elision = self.external_id_elision_declared();
        let mut decoded = Vec::new();
        for entry in entries
            .get(first..)
            .unwrap_or_default()
            .iter()
            .take_while(|entry| entry.key() == key)
        {
            // Two ids can share a hash, so every candidate is confirmed against the stored id.
            let matches = self.with_source_form_text(column_ids::EXTERNAL_ID, entry.row_ordinal, |stored| {
                let Some(stored) = stored else {
                    return Ok(false);
                };
                expand_external_id(
                    stored,
                    elision,
                    || self.cached_event_id(entry.row_ordinal),
                    &mut decoded,
                )?;
                Ok::<_, FormatError>(decoded == external_id)
            })??;
            if matches {
                found(entry.row_ordinal);
            }
        }
        Ok(())
    }

    /// Every row's external id, in row order - what a cross-file index records for this file.
    pub fn external_ids(&self) -> Result<Vec<(u64, Vec<u8>)>, FormatError> {
        let mut ids = Vec::with_capacity(self.footer().external_ids.len());
        if !has_column(self.footer(), column_ids::EXTERNAL_ID) {
            return Ok(ids);
        }
        let elision = self.external_id_elision_declared();
        for granule in &self.footer().granules {
            let read = self.read_column(column_ids::EXTERNAL_ID, granule.granule_id)?;
            // The event-id block is read only when some row of the granule elides its id.
            let mut event_ids: Option<ColumnRead> = None;
            for_each_present_string(&read, granule.row_count as usize, |row, stored| {
                let mut id = Vec::new();
                expand_external_id(
                    stored,
                    elision,
                    || {
                        if event_ids.is_none() {
                            event_ids = Some(self.read_column(column_ids::EVENT_ID, granule.granule_id)?);
                        }
                        match event_ids.as_ref().map(|read| &read.data) {
                            Some(ColumnData::U128(values)) => values.get(row).copied().ok_or(MISSING_EVENT_ID),
                            _ => Err(MISSING_EVENT_ID),
                        }
                    },
                    &mut id,
                )?;
                ids.push((granule.first_row_ordinal + row as u64, id));
                Ok(())
            })?;
        }
        Ok(ids)
    }

    /// Whether the file declares that some external id is stored as an event-id sentinel.
    fn external_id_elision_declared(&self) -> bool {
        self.footer().required_feature_flags & required_features::EXTERNAL_ID_EVENT_ID_ELISION != 0
    }

    /// One row's event id, read through the decoded-block cache so a run of point reads decodes each granule's
    /// event-id block once.
    pub(crate) fn cached_event_id(&self, row_ordinal: u64) -> Result<u128, FormatError> {
        let granule = granule_holding(self.footer(), row_ordinal)?;
        self.with_cached_u128_at(
            column_ids::EVENT_ID,
            granule.granule_id,
            row_ordinal - granule.first_row_ordinal,
        )?
        .ok_or(MISSING_EVENT_ID)
    }

    /// One row's text in a source-form column, or `None` when the file has no such column or the row has no value.
    /// The granule's block is decoded once and kept in the reader's decoded-block cache, so a run of point reads (an
    /// event-id lookup confirming each candidate, say) pays one decode per granule.
    fn source_form_text(&self, column_id: u32, row_ordinal: u64) -> Result<Option<String>, FormatError> {
        self.with_source_form_text(column_id, row_ordinal, |text| text.map(str::to_owned))
    }

    /// Runs `visit` on one row's text in a source-form column, borrowed from the cached block instead of copied out;
    /// it sees `None` when the file has no such column or the row has no value.
    fn with_source_form_text<T>(
        &self,
        column_id: u32,
        row_ordinal: u64,
        visit: impl FnOnce(Option<&str>) -> T,
    ) -> Result<T, FormatError> {
        if !has_column(self.footer(), column_id) {
            return Ok(visit(None));
        }
        let granule = granule_holding(self.footer(), row_ordinal)?;
        self.with_cached_string_at(
            column_id,
            granule.granule_id,
            granule.row_count,
            row_ordinal - granule.first_row_ordinal,
            visit,
        )
    }
}

/// The stored text of an external id that is its row's event id as 16 raw big-endian bytes. Stored ids are otherwise
/// lowercase hex of even length, so a one-letter non-hex sentinel cannot be mistaken for one.
pub(crate) const EXTERNAL_ID_IS_RAW_EVENT_ID: &str = "r";
/// The stored text of an external id that is its row's event id as 32-character lowercase hex text.
pub(crate) const EXTERNAL_ID_IS_HEX_EVENT_ID: &str = "h";

const MISSING_EVENT_ID: FormatError = FormatError::Structural {
    rule: "an elided external id's row has an event id",
};

/// The sentinel to store instead of `id` when it only repeats the row's event id, or `None` when it must be stored.
pub(crate) fn external_id_sentinel(id: &[u8], event_id: u128) -> Option<&'static str> {
    if id == event_id.to_be_bytes() {
        return Some(EXTERNAL_ID_IS_RAW_EVENT_ID);
    }
    (id.len() == 32 && id == event_id_hex(event_id)).then_some(EXTERNAL_ID_IS_HEX_EVENT_ID)
}

/// The event id's 32-character lowercase hex text, as ASCII bytes.
fn event_id_hex(event_id: u128) -> [u8; 32] {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 32];
    for (slot, shift) in out.iter_mut().zip((0..32u32).rev()) {
        let nibble = (event_id >> (shift * 4)) as usize & 0xf;
        *slot = DIGITS.get(nibble).copied().unwrap_or(b'0');
    }
    out
}

/// Expands one stored external id into `out`: lowercase hex decodes to its bytes, and a sentinel - honoured only when
/// the file declares [`required_features::EXTERNAL_ID_EVENT_ID_ELISION`] - rebuilds the id from the row's event id,
/// which `event_id` reads only then.
pub(crate) fn expand_external_id(
    stored: &str,
    elision_declared: bool,
    event_id: impl FnOnce() -> Result<u128, FormatError>,
    out: &mut Vec<u8>,
) -> Result<(), FormatError> {
    out.clear();
    match stored {
        EXTERNAL_ID_IS_RAW_EVENT_ID | EXTERNAL_ID_IS_HEX_EVENT_ID if !elision_declared => {
            Err(FormatError::Structural {
                rule: "an elided external id appears only in a file that declares external-id elision",
            })
        }
        EXTERNAL_ID_IS_RAW_EVENT_ID => {
            out.extend_from_slice(&event_id()?.to_be_bytes());
            Ok(())
        }
        EXTERNAL_ID_IS_HEX_EVENT_ID => {
            out.extend_from_slice(&event_id_hex(event_id()?));
            Ok(())
        }
        hex => {
            out.resize(hex.len() / 2, 0);
            hex_simd::decode(hex.as_bytes(), hex_simd::Out::from_slice(out))
                .map(|_| ())
                .map_err(|_| FormatError::Structural {
                    rule: "a stored external id is lowercase hex",
                })
        }
    }
}

/// Whether granule `granule_id` might hold a reference whose stored text is `reference_text` in relationship column
/// `column_id`. Always `true` for a file written before the reference filters existed; `false` for a granule the
/// filters show holds no reference of that kind at all.
pub(crate) fn reference_may_be_in(
    footer: &Footer,
    column_id: u32,
    granule_id: u32,
    reference_text: &str,
) -> Result<bool, FormatError> {
    let Some(filters) = &footer.reference_filters else {
        return Ok(true);
    };
    match filters.binary_search_by_key(&(column_id, granule_id), |entry| (entry.column_id, entry.granule_id)) {
        Ok(index) => match filters.get(index) {
            Some(entry) => {
                SplitBlockBloomFilter::contains_encoded(&entry.filter, stable_hash(reference_text.as_bytes()))
            }
            None => Ok(true),
        },
        Err(_) => Ok(false),
    }
}

fn has_column(footer: &Footer, column_id: u32) -> bool {
    footer.columns.iter().any(|column| column.column_id == column_id)
}

fn granule_holding(footer: &Footer, row_ordinal: u64) -> Result<&GranuleEntry, FormatError> {
    let index = footer
        .granules
        .partition_point(|granule| granule.first_row_ordinal + u64::from(granule.row_count) <= row_ordinal);
    footer
        .granules
        .get(index)
        .filter(|granule| granule.first_row_ordinal <= row_ordinal)
        .ok_or(FormatError::RefOutOfRange {
            what: "row ordinal past the file's last row",
        })
}

/// Hands every present value of a string block with its granule row to `visit`, in row order, in one walk over the
/// block without collecting them. Stops at the first error `visit` returns.
fn for_each_present_string<'a>(
    read: &'a ColumnRead,
    row_count: usize,
    mut visit: impl FnMut(usize, &'a str) -> Result<(), FormatError>,
) -> Result<(), FormatError> {
    let ColumnData::Strings(values) = &read.data else {
        return Ok(());
    };
    if values.len() == row_count {
        for (row, value) in values.iter().enumerate() {
            if let Some(text) = value {
                visit(row, text)?;
            }
        }
        return Ok(());
    }
    let mut dense = values.iter();
    for row in 0..row_count {
        let present = read
            .presence
            .get(row / 8)
            .is_some_and(|byte| byte & (1 << (row % 8)) != 0);
        if present && let Some(text) = dense.next().flatten() {
            visit(row, text)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "test/source_form.rs"]
mod tests;
