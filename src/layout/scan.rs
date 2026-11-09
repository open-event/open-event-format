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
        for row in 0..batch.row_count {
            let ordinal = first + u64::from(row);
            if !redactions.withholds_raw_payload(ordinal) {
                continue;
            }
            let ids = redactions.redacted_column_ids(self, ordinal);
            for (rows, column_id) in masked.iter_mut().zip(column_ids) {
                if ids.binary_search(column_id).is_ok() {
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

    fn granule_entry(&self, granule_id: u32) -> Option<&GranuleEntry> {
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

fn value_index(presence: &[u8], row: u32) -> Option<usize> {
    if presence.is_empty() {
        return Some(row as usize);
    }
    if !bit_set(presence, row) {
        return None;
    }
    let whole = row as usize / 8;
    let before: u32 = presence
        .get(..whole)
        .map_or(0, |bytes| bytes.iter().map(|b| b.count_ones()).sum());
    let partial = presence
        .get(whole)
        .map_or(0, |byte| (byte & ((1u8 << (row % 8)) - 1)).count_ones());
    Some((before + partial) as usize)
}

fn mask_column(column: &mut ScanColumn, row_count: u32, rows: &[u32]) -> Result<(), FormatError> {
    match column {
        ScanColumn::Views { presence, views } => {
            let hidden: Vec<usize> = rows.iter().filter_map(|&row| value_index(presence, row)).collect();
            if hidden.is_empty() {
                return Ok(());
            }
            *views = views
                .iter()
                .enumerate()
                .map(|(index, value)| if hidden.contains(&index) { None } else { value })
                .collect();
        }
        ScanColumn::Materialized(read) => {
            if read.presence.is_empty() {
                let mut dense = vec![0u8; (row_count as usize).div_ceil(8)];
                for row in 0..row_count {
                    if let Some(byte) = dense.get_mut(row as usize / 8) {
                        *byte |= 1 << (row % 8);
                    }
                }
                read.presence = dense;
            }
            let mut dropped: Vec<usize> = rows
                .iter()
                .filter_map(|&row| value_index(&read.presence, row))
                .collect();
            if dropped.is_empty() {
                return Ok(());
            }
            dropped.sort_unstable();
            for &row in rows {
                if let Some(byte) = read.presence.get_mut(row as usize / 8) {
                    *byte &= !(1 << (row % 8));
                }
            }
            let keep = |index: usize| dropped.binary_search(&index).is_err();
            fn retain_indexed<T>(values: &mut Vec<T>, keep: impl Fn(usize) -> bool) {
                let mut index = 0;
                values.retain(|_| {
                    let kept = keep(index);
                    index += 1;
                    kept
                });
            }
            match &mut read.data {
                ColumnData::Decimal { values, .. } => retain_indexed(values, keep),
                ColumnData::F64(values) => retain_indexed(values, keep),
                ColumnData::I64(values) => retain_indexed(values, keep),
                ColumnData::U128(values) => retain_indexed(values, keep),
                ColumnData::U64(values) => retain_indexed(values, keep),
                ColumnData::Strings(values) => {
                    let mut rebuilt = crate::encoding::StringColumn::with_capacity(values.len(), values.text_len());
                    for (index, value) in values.iter().enumerate() {
                        if keep(index) {
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
