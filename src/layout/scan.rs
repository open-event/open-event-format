use super::*;
use crate::deletes::{DeletionVector, RowSelection};
use crate::encoding::predicate::scan_in_parallel;
use crate::field_deletion_store::FieldRedactions;
use crate::layout::constant::SCAN_WINDOW_BYTES;
use crate::layout::footer::ColumnKind;
use arrow_array::Array;

#[derive(Debug)]
pub struct ScanBatch {
    pub bytes: u64,
    pub columns: Vec<ScanColumn>,
    pub granule_id: u32,
    pub row_count: u32,
}

#[derive(Debug)]
pub enum ScanColumn {
    Materialized(ColumnRead),
    Views { presence: Vec<u8>, views: StringViewArray },
}

struct ScanBlock {
    aliased: bool,
    column_id: u32,
    granule_id: u32,
    mark: ColumnMark,
    string: bool,
}

struct ScanGranule {
    blocks: std::ops::Range<usize>,
    granule_id: u32,
    row_count: u32,
}

const OUT_OF_STEP: FormatError = FormatError::Structural {
    rule: "scan window out of step with its block list",
};

fn estimated_decode_bytes(mark: &ColumnMark) -> u64 {
    mark.uncompressed_size
        .saturating_add(u64::from(mark.row_count).saturating_mul(16))
}

impl OeFile {
    pub fn scan_projected(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        sink: impl FnMut(ScanBatch) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_projected_inner(column_ids, granule_ids, true, SCAN_WINDOW_BYTES, sink)
    }

    pub fn scan_projected_serial(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        sink: impl FnMut(ScanBatch) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_projected_inner(column_ids, granule_ids, false, SCAN_WINDOW_BYTES, sink)
    }

    pub fn scan_projected_live(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        deleted: &DeletionVector,
        sink: impl FnMut(ScanBatch, RowSelection) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_live_inner(column_ids, granule_ids, deleted, true, sink)
    }

    pub fn scan_projected_serial_live(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        deleted: &DeletionVector,
        sink: impl FnMut(ScanBatch, RowSelection) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_live_inner(column_ids, granule_ids, deleted, false, sink)
    }

    pub fn scan_projected_redacted(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        redactions: Option<&FieldRedactions>,
        mut sink: impl FnMut(ScanBatch) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_projected_inner(column_ids, granule_ids, true, SCAN_WINDOW_BYTES, |mut batch| {
            self.mask_redacted(&mut batch, column_ids, redactions)?;
            sink(batch)
        })
    }

    pub fn scan_projected_serial_redacted(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        redactions: Option<&FieldRedactions>,
        mut sink: impl FnMut(ScanBatch) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_projected_inner(column_ids, granule_ids, false, SCAN_WINDOW_BYTES, |mut batch| {
            self.mask_redacted(&mut batch, column_ids, redactions)?;
            sink(batch)
        })
    }

    pub fn scan_projected_live_redacted(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        deleted: &DeletionVector,
        redactions: Option<&FieldRedactions>,
        mut sink: impl FnMut(ScanBatch, RowSelection) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_live_inner(column_ids, granule_ids, deleted, true, |mut batch, selection| {
            self.mask_redacted(&mut batch, column_ids, redactions)?;
            sink(batch, selection)
        })
    }

    pub fn scan_projected_serial_live_redacted(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        deleted: &DeletionVector,
        redactions: Option<&FieldRedactions>,
        mut sink: impl FnMut(ScanBatch, RowSelection) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        self.scan_live_inner(column_ids, granule_ids, deleted, false, |mut batch, selection| {
            self.mask_redacted(&mut batch, column_ids, redactions)?;
            sink(batch, selection)
        })
    }

    pub fn mask_redacted(
        &self,
        batch: &mut ScanBatch,
        column_ids: &[u32],
        redactions: Option<&FieldRedactions>,
    ) -> Result<(), FormatError> {
        let Some(redactions) = redactions.filter(|redactions| !redactions.is_empty()) else {
            return Ok(());
        };
        let entry = self.granule_entry(batch.granule_id).ok_or(OUT_OF_STEP)?;
        let first = entry.first_row_ordinal;
        let mut masked: Vec<Vec<u32>> = vec![Vec::new(); column_ids.len()];
        let mut withholding: Vec<u32> = Vec::new();
        let mut projected: HashMap<Vec<u32>, Vec<usize>> = HashMap::new();
        for row in 0..batch.row_count {
            let ordinal = first + u64::from(row);
            withholding.clear();
            withholding.extend(
                (0u32..)
                    .zip(&redactions.vectors)
                    .filter(|(_, vector)| vector.withholds_raw_payload(ordinal))
                    .map(|(index, _)| index),
            );
            if withholding.is_empty() {
                continue;
            }
            if !projected.contains_key(withholding.as_slice()) {
                let ids = redactions.redacted_column_ids(self, ordinal);
                let positions = column_ids
                    .iter()
                    .enumerate()
                    .filter(|(_, column_id)| ids.binary_search(column_id).is_ok())
                    .map(|(position, _)| position)
                    .collect();
                projected.insert(withholding.clone(), positions);
            }
            for &position in projected.get(withholding.as_slice()).map_or(&[][..], Vec::as_slice) {
                if let Some(rows) = masked.get_mut(position) {
                    rows.push(row);
                }
            }
        }
        for (column, rows) in batch.columns.iter_mut().zip(&masked) {
            if !rows.is_empty() {
                mask_column(column, batch.row_count, rows)?;
            }
        }
        Ok(())
    }

    fn scan_live_inner(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        deleted: &DeletionVector,
        fan_out: bool,
        mut sink: impl FnMut(ScanBatch, RowSelection) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        let mut live = Vec::with_capacity(granule_ids.len());
        for &granule_id in granule_ids {
            let entry = self.granule_entry(granule_id).ok_or(FormatError::RefOutOfRange {
                what: "scanned granule missing from the granule directory",
            })?;
            if deleted.deleted_in(entry.first_row_ordinal, entry.row_count) < u64::from(entry.row_count) {
                live.push(granule_id);
            }
        }
        self.scan_projected_inner(column_ids, &live, fan_out, SCAN_WINDOW_BYTES, |batch| {
            let entry = self.granule_entry(batch.granule_id).ok_or(OUT_OF_STEP)?;
            let selection = deleted.selection(entry.first_row_ordinal, entry.row_count, None);
            sink(batch, selection)
        })
    }

    fn scan_projected_inner(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        fan_out: bool,
        window_bytes: u64,
        mut sink: impl FnMut(ScanBatch) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        let string_columns = column_ids
            .iter()
            .map(|column_id| {
                self.footer
                    .columns
                    .iter()
                    .find(|column| column.column_id == *column_id)
                    .map(|column| column.kind == ColumnKind::String)
                    .ok_or(FormatError::RefOutOfRange {
                        what: "projected column missing from the footer",
                    })
            })
            .collect::<Result<Vec<bool>, _>>()?;
        let aliased = !self.aliased_extents.is_empty();
        let mut window: Vec<ScanGranule> = Vec::new();
        let mut blocks: Vec<ScanBlock> = Vec::new();
        let mut window_used = 0u64;
        for &granule_id in granule_ids {
            let row_count = self
                .granule_entry(granule_id)
                .ok_or(FormatError::RefOutOfRange {
                    what: "scanned granule missing from the granule directory",
                })?
                .row_count;
            let first = blocks.len();
            let mut estimated_bytes = 0u64;
            for (&column_id, &string) in column_ids.iter().zip(&string_columns) {
                let mark = self.mark(column_id, 0, granule_id)?.ok_or(FormatError::RefOutOfRange {
                    what: "no mark for (column, projection, granule)",
                })?;
                estimated_bytes = estimated_bytes.saturating_add(estimated_decode_bytes(&mark));
                blocks.push(ScanBlock {
                    aliased,
                    column_id,
                    granule_id,
                    mark,
                    string,
                });
            }
            let first = if !window.is_empty() && window_used.saturating_add(estimated_bytes) > window_bytes {
                let delivered = blocks.get(..first).ok_or(OUT_OF_STEP)?;
                self.decode_window(&window, delivered, fan_out, &mut sink)?;
                window.clear();
                window_used = 0;
                blocks.drain(..first);
                0
            } else {
                first
            };
            window_used = window_used.saturating_add(estimated_bytes);
            window.push(ScanGranule {
                blocks: first..blocks.len(),
                granule_id,
                row_count,
            });
        }
        if !window.is_empty() {
            self.decode_window(&window, &blocks, fan_out, &mut sink)?;
        }
        Ok(())
    }

    pub(crate) fn block_page_stats(&self, column_id: u32, granule_id: u32) -> Option<&PageStats> {
        self.index.page_stats_index.get(&(column_id, granule_id))
    }

    pub(crate) fn granule_entry(&self, granule_id: u32) -> Option<&GranuleEntry> {
        let granules = &self.footer.granules;
        granules
            .get(granule_id as usize)
            .filter(|granule| granule.granule_id == granule_id)
            .or_else(|| granules.iter().find(|granule| granule.granule_id == granule_id))
    }

    fn decode_window(
        &self,
        window: &[ScanGranule],
        blocks: &[ScanBlock],
        fan_out: bool,
        sink: &mut impl FnMut(ScanBatch) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        let inflated_bytes: u64 = blocks.iter().map(|block| block.mark.uncompressed_size).sum();
        if !(fan_out && scan_in_parallel(window.len(), inflated_bytes / window.len().max(1) as u64)) {
            for granule in window {
                let own = blocks.get(granule.blocks.clone()).ok_or(OUT_OF_STEP)?;
                let columns = own
                    .iter()
                    .map(|block| self.scan_block(block))
                    .collect::<Result<_, _>>()?;
                sink(Self::batch(granule, own, columns))?;
            }
            return Ok(());
        }
        let decoded: Vec<ScanColumn> = blocks
            .par_iter()
            .map(|block| self.scan_block(block))
            .collect::<Result<_, _>>()?;
        let mut decoded = decoded.into_iter();
        for granule in window {
            let own = blocks.get(granule.blocks.clone()).ok_or(OUT_OF_STEP)?;
            let columns = decoded.by_ref().take(own.len()).collect();
            sink(Self::batch(granule, own, columns))?;
        }
        Ok(())
    }

    fn batch(granule: &ScanGranule, own: &[ScanBlock], columns: Vec<ScanColumn>) -> ScanBatch {
        ScanBatch {
            bytes: own.iter().map(|block| block.mark.compressed_size).sum(),
            columns,
            granule_id: granule.granule_id,
            row_count: granule.row_count,
        }
    }

    #[inline(never)]
    fn scan_block(&self, block: &ScanBlock) -> Result<ScanColumn, FormatError> {
        if block.string {
            if block.mark.page_count > 1 {
                if let Some((presence, views)) = self.read_column_string_views(block.column_id, block.granule_id)? {
                    return Ok(ScanColumn::Views { presence, views });
                }
            } else if block.mark.compressed_size != 0 {
                let raw = self.read_column_raw_with_mark(block.granule_id, &block.mark)?;
                if let Some(views) = self.decode_string_block_views(block.column_id, raw.pipeline, &raw.body)? {
                    validate_block_counts(&raw.presence, views.len(), raw.row_count as usize)?;
                    return Ok(ScanColumn::Views {
                        presence: raw.presence.into_owned(),
                        views,
                    });
                }
            }
        }
        if !block.aliased && block.mark.page_count <= 1 && block.mark.compressed_size != 0 {
            let raw = self.read_column_raw_with_mark(block.granule_id, &block.mark)?;
            let data = decode_block_shared(raw.pipeline, &raw.body, self.shared_alphabet(block.column_id))?;
            validate_block_counts(&raw.presence, data.row_count(), raw.row_count as usize)?;
            return Ok(ScanColumn::Materialized(ColumnRead {
                presence: raw.presence.into_owned(),
                data,
            }));
        }
        let read = self.read_column_with_mark(block.column_id, block.granule_id, &block.mark)?;
        Ok(ScanColumn::Materialized(read))
    }
}

#[cfg(test)]
#[path = "test/scan.rs"]
mod tests;

fn bit_set(presence: &[u8], row: u32) -> bool {
    presence
        .get(row as usize / 8)
        .is_some_and(|byte| byte & (1 << (row % 8)) != 0)
}

fn value_indexes(presence: &[u8], rows: &[u32]) -> Vec<usize> {
    if presence.is_empty() {
        return rows.iter().map(|&row| row as usize).collect();
    }
    let mut indexes = Vec::with_capacity(rows.len());
    let mut at = 0u32;
    let mut before = 0usize;
    for &row in rows {
        while at < row {
            if at.is_multiple_of(8) && row - at >= 8 {
                before += presence
                    .get(at as usize / 8)
                    .map_or(0, |byte| byte.count_ones() as usize);
                at += 8;
            } else {
                before += usize::from(bit_set(presence, at));
                at += 1;
            }
        }
        if bit_set(presence, row) {
            indexes.push(before);
        }
    }
    indexes
}

fn dense_presence(row_count: u32) -> Vec<u8> {
    let mut dense = vec![0u8; (row_count as usize).div_ceil(8)];
    for row in 0..row_count {
        if let Some(byte) = dense.get_mut(row as usize / 8) {
            *byte |= 1 << (row % 8);
        }
    }
    dense
}

fn clear_bits(presence: &mut [u8], rows: &[u32]) {
    for &row in rows {
        if let Some(byte) = presence.get_mut(row as usize / 8) {
            *byte &= !(1 << (row % 8));
        }
    }
}

fn mask_column(column: &mut ScanColumn, row_count: u32, rows: &[u32]) -> Result<(), FormatError> {
    match column {
        ScanColumn::Views { presence, views } => {
            if presence.is_empty() {
                *presence = dense_presence(row_count);
            }
            let hidden = value_indexes(presence, rows);
            if hidden.is_empty() {
                return Ok(());
            }
            clear_bits(presence, rows);
            let mut hidden = hidden.into_iter().peekable();
            *views = views
                .iter()
                .enumerate()
                .filter(|(index, _)| hidden.next_if_eq(index).is_none())
                .map(|(_, value)| value)
                .collect();
            validate_block_counts(presence, views.len(), row_count as usize)?;
        }
        ScanColumn::Materialized(read) => {
            if read.presence.is_empty() {
                read.presence = dense_presence(row_count);
            }
            let dropped = value_indexes(&read.presence, rows);
            if dropped.is_empty() {
                return Ok(());
            }
            clear_bits(&mut read.presence, rows);
            fn retain_indexed<T>(values: &mut Vec<T>, dropped: &[usize]) {
                let mut dropped = dropped.iter().copied().peekable();
                let mut index = 0;
                values.retain(|_| {
                    let kept = dropped.next_if_eq(&index).is_none();
                    index += 1;
                    kept
                });
            }
            match &mut read.data {
                ColumnData::Decimal { values, .. } => retain_indexed(values, &dropped),
                ColumnData::F64(values) => retain_indexed(values, &dropped),
                ColumnData::I64(values) => retain_indexed(values, &dropped),
                ColumnData::U128(values) => retain_indexed(values, &dropped),
                ColumnData::U64(values) => retain_indexed(values, &dropped),
                ColumnData::Strings(values) => {
                    let mut rebuilt = crate::encoding::StringColumn::with_capacity(values.len(), values.text_len());
                    let mut dropped = dropped.iter().copied().peekable();
                    for (index, value) in values.iter().enumerate() {
                        if dropped.next_if_eq(&index).is_none() {
                            rebuilt.push(value);
                        }
                    }
                    *values = rebuilt;
                }
            }
            validate_block_counts(&read.presence, read.data.row_count(), row_count as usize)?;
        }
    }
    Ok(())
}
