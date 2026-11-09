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
    pub fn raw_payload(&self, row_ordinal: u64) -> Result<Option<Cow<'_, [u8]>>, FormatError> {
        Ok(self
            .source_form_text(column_ids::RAW_PAYLOAD, row_ordinal)?
            .map(|text| Cow::Owned(text.into_bytes())))
    }

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

    pub fn row_by_external_id(&self, external_id: &[u8]) -> Result<Option<u64>, FormatError> {
        let mut newest = None;
        self.for_each_row_with_external_id(external_id, |row| newest = Some(row))?;
        Ok(newest)
    }

    pub fn rows_with_external_id(&self, external_id: &[u8]) -> Result<Vec<u64>, FormatError> {
        let mut rows = Vec::new();
        self.for_each_row_with_external_id(external_id, |row| rows.push(row))?;
        Ok(rows)
    }

    fn for_each_row_with_external_id(&self, external_id: &[u8], mut found: impl FnMut(u64)) -> Result<(), FormatError> {
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

    pub fn external_ids(&self) -> Result<Vec<(u64, Vec<u8>)>, FormatError> {
        let mut ids = Vec::with_capacity(self.footer().external_ids.len());
        if !has_column(self.footer(), column_ids::EXTERNAL_ID) {
            return Ok(ids);
        }
        let elision = self.external_id_elision_declared();
        for granule in &self.footer().granules {
            let read = self.read_column(column_ids::EXTERNAL_ID, granule.granule_id)?;
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

    fn external_id_elision_declared(&self) -> bool {
        self.footer().required_feature_flags & required_features::EXTERNAL_ID_EVENT_ID_ELISION != 0
    }

    pub(crate) fn cached_event_id(&self, row_ordinal: u64) -> Result<u128, FormatError> {
        let granule = granule_holding(self.footer(), row_ordinal)?;
        self.with_cached_u128_at(
            column_ids::EVENT_ID,
            granule.granule_id,
            row_ordinal - granule.first_row_ordinal,
        )?
        .ok_or(MISSING_EVENT_ID)
    }

    fn source_form_text(&self, column_id: u32, row_ordinal: u64) -> Result<Option<String>, FormatError> {
        self.with_source_form_text(column_id, row_ordinal, |text| text.map(str::to_owned))
    }

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

pub(crate) const EXTERNAL_ID_IS_RAW_EVENT_ID: &str = "r";
pub(crate) const EXTERNAL_ID_IS_HEX_EVENT_ID: &str = "h";

const MISSING_EVENT_ID: FormatError = FormatError::Structural {
    rule: "an elided external id's row has an event id",
};

pub(crate) fn external_id_sentinel(id: &[u8], event_id: u128) -> Option<&'static str> {
    if id == event_id.to_be_bytes() {
        return Some(EXTERNAL_ID_IS_RAW_EVENT_ID);
    }
    (id.len() == 32 && id == event_id_hex(event_id)).then_some(EXTERNAL_ID_IS_HEX_EVENT_ID)
}

fn event_id_hex(event_id: u128) -> [u8; 32] {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 32];
    for (slot, shift) in out.iter_mut().zip((0..32u32).rev()) {
        let nibble = (event_id >> (shift * 4)) as usize & 0xf;
        *slot = DIGITS.get(nibble).copied().unwrap_or(b'0');
    }
    out
}

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
