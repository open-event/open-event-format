use super::constant::MAX_RLE_EXPANSION_VALUES;
use super::{
    ColumnData, DictionaryValues, FsstDecodeTable, IntValue, MAX_BLOCK_VALUES, NullStream, NullStreamForm, PipelineId,
    SideStream, StringColumn, Transform, ValueKind, decode_block_shared_with_body, fastlanes_value_at,
    fsst_decompress_value, offset_window, read_packed_stream, read_string_header, take_delta_bitpack,
    with_trailing_removed,
};
use crate::error::FormatError;
use crate::file::bytes::{Reader, slice};
use crate::indexes::rank_select::BitRankDirectory;
use std::borrow::Cow;

const NULL_CODE: u32 = u32::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryCodes<'a> {
    codes: Vec<u32>,
    dictionary: Cow<'a, [String]>,
    sorted: bool,
}

impl<'a> DictionaryCodes<'a> {
    pub fn len(&self) -> usize {
        self.codes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    pub fn dictionary(&self) -> &[String] {
        &self.dictionary
    }

    pub fn code(&self, row: usize) -> Option<Option<u32>> {
        let code = *self.codes.get(row)?;
        Some((code != NULL_CODE).then_some(code))
    }

    pub fn raw_codes(&self) -> &[u32] {
        &self.codes
    }

    pub fn value(&self, row: usize) -> Option<Option<&str>> {
        Some(
            self.code(row)?
                .and_then(|code| self.dictionary.get(code as usize))
                .map(String::as_str),
        )
    }

    pub fn code_counts(&self) -> Vec<u64> {
        let mut counts = vec![0u64; self.dictionary.len()];
        for &code in &self.codes {
            if let Some(slot) = counts.get_mut(code as usize) {
                *slot += 1;
            }
        }
        counts
    }

    pub fn equals_mask(&self, needle: &str) -> Vec<bool> {
        if !self.sorted {
            let equal: Vec<bool> = self.dictionary.iter().map(|entry| entry == needle).collect();
            return self
                .codes
                .iter()
                .map(|&code| equal.get(code as usize).copied().unwrap_or(false))
                .collect();
        }
        match self.dictionary.binary_search_by(|entry| entry.as_str().cmp(needle)) {
            Ok(index) => {
                let target = index as u32;
                self.codes.iter().map(|&code| code == target).collect()
            }
            Err(_) => vec![false; self.codes.len()],
        }
    }

    pub fn gather(&self, rows: &[usize]) -> Result<StringColumn, FormatError> {
        let mut out = StringColumn::with_capacity(rows.len(), 0);
        for &row in rows {
            let value = self.value(row).ok_or(FormatError::RefOutOfRange { what: "take row" })?;
            out.push(value);
        }
        Ok(out)
    }

    pub fn to_strings(&self) -> StringColumn {
        let mut out = StringColumn::with_capacity(self.codes.len(), 0);
        for row in 0..self.codes.len() {
            out.push(self.value(row).flatten());
        }
        out
    }

    pub fn into_owned(self) -> DictionaryCodes<'static> {
        DictionaryCodes {
            codes: self.codes,
            dictionary: Cow::Owned(self.dictionary.into_owned()),
            sorted: self.sorted,
        }
    }
}

pub fn decode_dictionary_codes<'a>(
    pipeline: PipelineId,
    bytes: &[u8],
    shared: Option<&'a [String]>,
) -> Result<Option<DictionaryCodes<'a>>, FormatError> {
    if pipeline.value_kind()? != ValueKind::String || pipeline.transform()? != Transform::DictionaryString {
        return Ok(None);
    }
    let side = pipeline.side_stream()?;
    with_trailing_removed(pipeline.compression()?, bytes, |body| {
        dictionary_codes_from_body(body, side, shared).map(Some)
    })
}

pub(super) fn bounded_row_count(nulls: &NullStream<'_>) -> Result<usize, FormatError> {
    if matches!(nulls.form, NullStreamForm::Runs(_)) && nulls.row_count() > MAX_BLOCK_VALUES {
        return Err(FormatError::Structural {
            rule: "null stream row count beyond the block bound",
        });
    }
    Ok(nulls.row_count())
}

pub(super) fn dictionary_codes_from_body<'a>(
    body: &[u8],
    side: SideStream,
    shared: Option<&'a [String]>,
) -> Result<DictionaryCodes<'a>, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = NullStream::read(&mut reader)?;
    let dictionary: Cow<'a, [String]> = if side == SideStream::FileScopeDictionary {
        Cow::Borrowed(shared.ok_or(FormatError::Structural {
            rule: "shared-scope dictionary block without its file alphabet",
        })?)
    } else {
        let dict_count = reader.u32("dictionary count")? as usize;
        let values = DictionaryValues::read(&mut reader, dict_count, side)?;
        Cow::Owned(values.entries()?.into_iter().map(str::to_owned).collect())
    };
    if dictionary.len() >= NULL_CODE as usize {
        return Err(FormatError::Structural {
            rule: "dictionary too large for its code space",
        });
    }
    let stored = super::bitunpack(&mut reader)?;
    let mut codes = Vec::with_capacity(bounded_row_count(&nulls)?);
    let mut next = 0usize;
    nulls.each_row(|present| {
        if !present {
            codes.push(NULL_CODE);
            return Ok(());
        }
        let code = *stored.get(next).ok_or(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        })?;
        if code >= dictionary.len() as u64 {
            return Err(FormatError::RefOutOfRange {
                what: "dictionary code",
            });
        }
        codes.push(code as u32);
        next += 1;
        Ok(())
    })?;
    if next != stored.len() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let sorted = dictionary
        .windows(2)
        .all(|pair| matches!(pair, [low, high] if low < high));
    Ok(DictionaryCodes {
        codes,
        dictionary,
        sorted,
    })
}

pub fn fsst_uncompressed_len(pipeline: PipelineId, bytes: &[u8]) -> Result<Option<usize>, FormatError> {
    if pipeline.value_kind()? == ValueKind::String && pipeline.transform()? == Transform::FsstPackedString {
        return with_trailing_removed(pipeline.compression()?, bytes, |body| {
            fsst_uncompressed_len(
                PipelineId::new(Transform::FsstString, super::Compression::None, ValueKind::String),
                &super::fsst_packed_as_classic(body)?,
            )
        });
    }
    if pipeline.value_kind()? != ValueKind::String || pipeline.transform()? != Transform::FsstString {
        return Ok(None);
    }
    with_trailing_removed(pipeline.compression()?, bytes, |body| {
        let mut reader = Reader::new(body);
        let header = read_string_header(Transform::FsstString, &mut reader)?;
        let table = slice(
            body,
            header.offsets_start,
            header.present_count.saturating_add(1).saturating_mul(4),
            "string offset",
        )?;
        let bounds = offset_window(table, 0, header.present_count + 1)?;
        let (first, last) = (
            bounds.first().copied().unwrap_or(0),
            bounds.last().copied().unwrap_or(0),
        );
        if bounds.windows(2).any(|pair| matches!(pair, [lo, hi] if hi < lo)) {
            return Err(FormatError::Structural {
                rule: "fsst offsets must be non-decreasing",
            });
        }
        let data = slice(body, header.offsets_start + table.len(), last, "fsst data")?;
        let codes = data.get(first..).ok_or(FormatError::Truncated { what: "fsst data" })?;
        let mut total = 0usize;
        for pair in bounds.windows(2) {
            let [lo, hi] = pair else { continue };
            let value = codes
                .get(lo - first..hi - first)
                .ok_or(FormatError::Truncated { what: "fsst value" })?;
            total += fsst_codes_len(header.fsst()?, value)?;
        }
        Ok(Some(total))
    })
}

fn fsst_codes_len(table: &FsstDecodeTable, codes: &[u8]) -> Result<usize, FormatError> {
    let mut total = 0usize;
    let mut rest = codes;
    while let Some((&code, after)) = rest.split_first() {
        if code == fsst::ESCAPE_CODE {
            let [_literal, after @ ..] = after else {
                return Err(FormatError::Truncated {
                    what: "fsst escaped byte",
                });
            };
            total += 1;
            rest = after;
            continue;
        }
        let len = table.lengths.get(usize::from(code)).copied().unwrap_or(0);
        if len == 0 {
            return Err(FormatError::RefOutOfRange { what: "fsst code" });
        }
        total += usize::from(len);
        rest = after;
    }
    Ok(total)
}

pub fn take_block_rows_shared(
    pipeline: PipelineId,
    bytes: &[u8],
    shared: Option<&[String]>,
    rows: &[usize],
) -> Result<ColumnData, FormatError> {
    let transform = pipeline.transform()?;
    let kind = pipeline.value_kind()?;
    with_trailing_removed(pipeline.compression()?, bytes, |body| match (kind, transform) {
        (ValueKind::String, Transform::FsstString) => Ok(ColumnData::Strings(take_fsst(body, rows)?)),
        (ValueKind::String, Transform::FsstPackedString) => Ok(ColumnData::Strings(take_fsst_packed(body, rows)?)),
        (ValueKind::String, Transform::RawString) => Ok(ColumnData::Strings(take_raw(body, rows)?)),
        (ValueKind::String, Transform::DictionaryString) => Ok(ColumnData::Strings(take_dictionary(
            body,
            pipeline.side_stream()?,
            shared,
            rows,
        )?)),
        (ValueKind::U64, Transform::ForBitpack) => Ok(ColumnData::U64(take_for_bitpack(body, rows)?)),
        (ValueKind::I64, Transform::ForBitpack) => Ok(ColumnData::I64(take_for_bitpack(body, rows)?)),
        (ValueKind::U64, Transform::DeltaBitpack) => Ok(ColumnData::U64(take_delta_bitpack(body, rows)?)),
        (ValueKind::I64, Transform::DeltaBitpack) => Ok(ColumnData::I64(take_delta_bitpack(body, rows)?)),
        (ValueKind::U64, Transform::Rle) => Ok(ColumnData::U64(RunDirectory::read(body)?.take(rows)?)),
        (ValueKind::I64, Transform::Rle) => Ok(ColumnData::I64(RunDirectory::read(body)?.take(rows)?)),
        _ => gather_column(&decode_block_shared_with_body(pipeline, body, shared)?, rows),
    })
}

fn take_for_bitpack<T: IntValue>(body: &[u8], rows: &[usize]) -> Result<Vec<T>, FormatError> {
    let mut reader = Reader::new(body);
    let base = reader.u64("for base")?;
    let packed = read_packed_stream(&mut reader)?;
    rows.iter()
        .map(|&row| {
            if row >= packed.count {
                return Err(FormatError::RefOutOfRange { what: "take row" });
            }
            base.checked_add(fastlanes_value_at(packed.packed, packed.width, row))
                .map(T::from_code)
                .ok_or(FormatError::Structural {
                    rule: "frame-of-reference overflow",
                })
        })
        .collect()
}

pub fn take_block_rows(pipeline: PipelineId, bytes: &[u8], rows: &[usize]) -> Result<ColumnData, FormatError> {
    take_block_rows_shared(pipeline, bytes, None, rows)
}

struct PresentRank<'n, 'a> {
    before: usize,
    nulls: &'n NullStream<'a>,
    row: usize,
    run: usize,
}

impl<'n, 'a> PresentRank<'n, 'a> {
    fn new(nulls: &'n NullStream<'a>) -> Self {
        Self {
            before: 0,
            nulls,
            row: 0,
            run: 0,
        }
    }

    fn position(&mut self, row: usize) -> Result<Option<usize>, FormatError> {
        if row >= self.nulls.row_count() {
            return Err(FormatError::RefOutOfRange { what: "take row" });
        }
        if row < self.row {
            self.before = 0;
            self.row = 0;
            self.run = 0;
        }
        match &self.nulls.form {
            NullStreamForm::AllAbsent => Ok(None),
            NullStreamForm::AllPresent => Ok(Some(row)),
            NullStreamForm::Raw(bits) => {
                self.before += count_bits_between(bits, self.row, row);
                self.row = row;
                Ok(self.nulls.is_present(row).then_some(self.before))
            }
            NullStreamForm::Runs(runs) => {
                self.row = row;
                loop {
                    match runs.get(self.run) {
                        Some((start, end)) if end as usize <= row => {
                            self.before += (end - start) as usize;
                            self.run += 1;
                        }
                        Some((start, _)) if start as usize <= row => {
                            return Ok(Some(self.before + (row - start as usize)));
                        }
                        _ => return Ok(None),
                    }
                }
            }
        }
    }
}

fn count_bits_between(bits: &[u8], from: usize, to: usize) -> usize {
    if from >= to {
        return 0;
    }
    let byte = |index: usize| bits.get(index).copied().unwrap_or(0);
    let from_mask = 0xFFu8 << (from % 8);
    let to_mask = !(0xFFu8 << (to % 8));
    let (first, last) = (from / 8, to / 8);
    if first == last {
        return (byte(first) & from_mask & to_mask).count_ones() as usize;
    }
    let whole = bits.get(first + 1..last.min(bits.len())).unwrap_or(&[]);
    (byte(first) & from_mask).count_ones() as usize
        + whole.iter().map(|byte| byte.count_ones() as usize).sum::<usize>()
        + (byte(last) & to_mask).count_ones() as usize
}

fn offset_pair(table: &[u8], position: usize) -> Result<(usize, usize), FormatError> {
    let entry = |index: usize| {
        let at = index.checked_mul(4)?;
        let bytes: [u8; 4] = table.get(at..at.checked_add(4)?)?.try_into().ok()?;
        Some(u32::from_le_bytes(bytes) as usize)
    };
    entry(position)
        .zip(position.checked_add(1).and_then(entry))
        .ok_or(FormatError::Truncated { what: "string offset" })
}

fn take_fsst(body: &[u8], rows: &[usize]) -> Result<StringColumn, FormatError> {
    let header = read_string_header(Transform::FsstString, &mut Reader::new(body))?;
    let table = slice(
        body,
        header.offsets_start,
        header.present_count.saturating_add(1).saturating_mul(4),
        "string offset",
    )?;
    let arena_len = offset_window(table, header.present_count, 1)?
        .first()
        .copied()
        .unwrap_or(0);
    let data = slice(body, header.offsets_start + table.len(), arena_len, "fsst data")?;
    let decode = header.fsst()?;
    let mut out = StringColumn::with_capacity(rows.len(), 0);
    let mut buffer = Vec::new();
    let mut rank = PresentRank::new(&header.nulls);
    for &row in rows {
        let Some(position) = rank.position(row)? else {
            out.push(None);
            continue;
        };
        let (start, end) = offset_pair(table, position)?;
        if end < start {
            return Err(FormatError::Structural {
                rule: "fsst offsets must be non-decreasing",
            });
        }
        let compressed = slice(data, start, end - start, "fsst value")?;
        buffer.clear();
        fsst_decompress_value(decode, compressed, &mut buffer)?;
        let text = simdutf8::basic::from_utf8(&buffer).map_err(|_| FormatError::InvalidUtf8 { what: "fsst value" })?;
        out.push(Some(text));
    }
    Ok(out)
}

fn take_fsst_packed(body: &[u8], rows: &[usize]) -> Result<StringColumn, FormatError> {
    let mut reader = Reader::new(body);
    let header = read_string_header(Transform::FsstString, &mut reader)?;
    let lens = super::bitunpack(&mut reader)?;
    if lens.len() != header.present_count {
        return Err(FormatError::Structural {
            rule: "fsst packed length count disagrees with the present count",
        });
    }
    let mut starts = Vec::with_capacity(lens.len() + 1);
    let mut offset = 0usize;
    starts.push(0);
    for len in &lens {
        offset = usize::try_from(*len)
            .ok()
            .and_then(|len| offset.checked_add(len))
            .ok_or(FormatError::Structural {
                rule: "fsst packed lengths disagree with the arena",
            })?;
        starts.push(offset);
    }
    let data = reader.take(reader.remaining(), "fsst data")?;
    if data.len() != offset {
        return Err(FormatError::Structural {
            rule: "fsst packed lengths disagree with the arena",
        });
    }
    let decode = header.fsst()?;
    let mut out = StringColumn::with_capacity(rows.len(), 0);
    let mut buffer = Vec::new();
    let mut rank = PresentRank::new(&header.nulls);
    for &row in rows {
        let Some(position) = rank.position(row)? else {
            out.push(None);
            continue;
        };
        let (Some(&start), Some(&end)) = (starts.get(position), starts.get(position + 1)) else {
            return Err(FormatError::Truncated { what: "string offset" });
        };
        let compressed = slice(data, start, end - start, "fsst value")?;
        buffer.clear();
        fsst_decompress_value(decode, compressed, &mut buffer)?;
        let text = simdutf8::basic::from_utf8(&buffer).map_err(|_| FormatError::InvalidUtf8 { what: "fsst value" })?;
        out.push(Some(text));
    }
    Ok(out)
}

fn take_raw(body: &[u8], rows: &[usize]) -> Result<StringColumn, FormatError> {
    let header = read_string_header(Transform::RawString, &mut Reader::new(body))?;
    let table = slice(
        body,
        header.offsets_start,
        header.present_count.saturating_add(1).saturating_mul(4),
        "string offset",
    )?;
    let arena_len = offset_window(table, header.present_count, 1)?
        .first()
        .copied()
        .unwrap_or(0);
    let data = slice(body, header.offsets_start + table.len(), arena_len, "raw string data")?;
    let mut out = StringColumn::with_capacity(rows.len(), 0);
    let mut rank = PresentRank::new(&header.nulls);
    for &row in rows {
        let Some(position) = rank.position(row)? else {
            out.push(None);
            continue;
        };
        let (start, end) = offset_pair(table, position)?;
        if end < start {
            return Err(FormatError::Structural {
                rule: "raw offsets must be non-decreasing",
            });
        }
        let bytes = slice(data, start, end - start, "raw string value")?;
        let text = simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 {
            what: "raw string value",
        })?;
        out.push(Some(text));
    }
    Ok(out)
}

fn take_dictionary(
    body: &[u8],
    side: SideStream,
    shared: Option<&[String]>,
    rows: &[usize],
) -> Result<StringColumn, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = NullStream::read(&mut reader)?;
    let values;
    let mut alphabet: Option<&[String]> = None;
    let entries: Vec<&str> = if side == SideStream::FileScopeDictionary {
        alphabet = Some(shared.ok_or(FormatError::Structural {
            rule: "shared-scope dictionary block without its file alphabet",
        })?);
        Vec::new()
    } else {
        let dict_count = reader.u32("dictionary count")? as usize;
        values = DictionaryValues::read(&mut reader, dict_count, side)?;
        values.entries()?
    };
    let entry_at = |code: usize| match alphabet {
        Some(alphabet) => alphabet.get(code).map(String::as_str),
        None => entries.get(code).copied(),
    };
    let packed = read_packed_stream(&mut reader)?;
    if packed.count != nulls.present_count() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let mut out = StringColumn::with_capacity(rows.len(), 0);
    let mut rank = PresentRank::new(&nulls);
    for &row in rows {
        let Some(position) = rank.position(row)? else {
            out.push(None);
            continue;
        };
        let code = fastlanes_value_at(packed.packed, packed.width, position) as usize;
        out.push(Some(entry_at(code).ok_or(FormatError::RefOutOfRange {
            what: "dictionary code",
        })?));
    }
    Ok(out)
}

#[derive(Debug)]
struct RunDirectory {
    values: Vec<u64>,
    ends: Vec<usize>,
}

impl RunDirectory {
    fn read(body: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(body);
        let run_count = reader.u32("rle run count")? as usize;
        let mut values = Vec::with_capacity(run_count.min(body.len() / 12));
        let mut ends = Vec::with_capacity(values.capacity());
        let mut total = 0usize;
        for _ in 0..run_count {
            let value = reader.u64("rle value")?;
            let count = reader.u32("rle count")? as usize;
            total += count;
            if total > MAX_RLE_EXPANSION_VALUES {
                return Err(FormatError::Structural {
                    rule: "rle expansion too large",
                });
            }
            values.push(value);
            ends.push(total);
        }
        Ok(Self { values, ends })
    }

    fn row_count(&self) -> usize {
        self.ends.last().copied().unwrap_or(0)
    }

    fn take<T: IntValue>(&self, rows: &[usize]) -> Result<Vec<T>, FormatError> {
        rows.iter()
            .map(|&row| {
                let run = self.ends.partition_point(|&end| end <= row);
                self.values
                    .get(run)
                    .map(|&value| T::from_code(value))
                    .ok_or(FormatError::RefOutOfRange { what: "take row" })
            })
            .collect()
    }

    fn filter<T: IntValue>(&self, selection: &BitRankDirectory<'_>) -> Vec<T> {
        let mut out = Vec::with_capacity(selection.total() as usize);
        let mut before = 0u64;
        for (&value, &end) in self.values.iter().zip(&self.ends) {
            let through = selection.rank(end);
            out.extend(std::iter::repeat_n(T::from_code(value), (through - before) as usize));
            before = through;
            if end >= selection.len() {
                break;
            }
        }
        out
    }

    fn range<T: IntValue>(&self, start: usize, end: usize) -> Vec<T> {
        let end = end.min(self.row_count());
        let mut out = Vec::with_capacity(end.saturating_sub(start));
        let mut run = self.ends.partition_point(|&run_end| run_end <= start);
        let mut row = start;
        while row < end {
            let (Some(&value), Some(&run_end)) = (self.values.get(run), self.ends.get(run)) else {
                break;
            };
            let upto = run_end.min(end);
            out.extend(std::iter::repeat_n(T::from_code(value), upto - row));
            row = upto;
            run += 1;
        }
        out
    }
}

pub(super) fn decode_rle_range(
    body: &[u8],
    kind: ValueKind,
    start: usize,
    end: usize,
) -> Result<ColumnData, FormatError> {
    let directory = RunDirectory::read(body)?;
    Ok(match kind {
        ValueKind::I64 => ColumnData::I64(directory.range(start, end)),
        _ => ColumnData::U64(directory.range(start, end)),
    })
}

pub fn filter_block_by_selection(
    pipeline: PipelineId,
    bytes: &[u8],
    shared: Option<&[String]>,
    selection: &[u8],
) -> Result<ColumnData, FormatError> {
    let transform = pipeline.transform()?;
    let kind = pipeline.value_kind()?;
    with_trailing_removed(pipeline.compression()?, bytes, |body| {
        if transform == Transform::Rle && matches!(kind, ValueKind::U64 | ValueKind::I64) {
            let directory = RunDirectory::read(body)?;
            let ranks = BitRankDirectory::new(selection, directory.row_count());
            return Ok(match kind {
                ValueKind::I64 => ColumnData::I64(directory.filter(&ranks)),
                _ => ColumnData::U64(directory.filter(&ranks)),
            });
        }
        let full = decode_block_shared_with_body(pipeline, body, shared)?;
        let rows: Vec<usize> = (0..full.row_count())
            .filter(|&row| selection.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0))
            .collect();
        gather_column(&full, &rows)
    })
}

pub(super) fn gather_column(data: &ColumnData, rows: &[usize]) -> Result<ColumnData, FormatError> {
    fn pick<T: Copy>(values: &[T], rows: &[usize]) -> Result<Vec<T>, FormatError> {
        rows.iter()
            .map(|&row| {
                values
                    .get(row)
                    .copied()
                    .ok_or(FormatError::RefOutOfRange { what: "take row" })
            })
            .collect()
    }
    Ok(match data {
        ColumnData::U64(values) => ColumnData::U64(pick(values, rows)?),
        ColumnData::I64(values) => ColumnData::I64(pick(values, rows)?),
        ColumnData::F64(values) => ColumnData::F64(pick(values, rows)?),
        ColumnData::U128(values) => ColumnData::U128(pick(values, rows)?),
        ColumnData::Decimal { scale, values } => ColumnData::Decimal {
            scale: *scale,
            values: pick(values, rows)?,
        },
        ColumnData::Strings(values) => {
            let mut out = StringColumn::with_capacity(rows.len(), 0);
            for &row in rows {
                out.push(values.get(row).ok_or(FormatError::RefOutOfRange { what: "take row" })?);
            }
            ColumnData::Strings(out)
        }
    })
}

#[cfg(test)]
#[path = "test/lazy.rs"]
mod tests;
