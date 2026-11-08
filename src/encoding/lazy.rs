//! Lazy and sparse decode paths over encoded blocks: a dictionary string block projected to its codes plus its
//! dictionary (no per-row string materialisation), a row "take" that decodes only the requested rows of FSST,
//! dictionary, raw and run-length blocks, a run-length filter that counts each run's selected rows with a rank
//! directory, and an FSST block's uncompressed size computed from its codes without decoding them.
//!
//! Every path here is exact: it answers what a whole-block [`decode_block_shared`] followed by the same gather or
//! filter would, and the tests hold each one to that oracle.
//!
//! See: oe-encodings-and-compression/spec.md

use super::constant::MAX_RLE_EXPANSION_VALUES;
use super::{
    ColumnData, DictionaryValues, FsstDecodeTable, IntValue, NullStream, PipelineId, SideStream, StringColumn,
    Transform, ValueKind, decode_block_shared_with_body, fastlanes_value_at, fsst_decompress_value, offset_window,
    read_packed_stream, read_string_header, with_trailing_removed,
};
use crate::error::FormatError;
use crate::file::bytes::{Reader, slice};
use crate::indexes::rank_select::BitRankDirectory;
use std::borrow::Cow;

/// Code a [`DictionaryCodes`] row holds when it carries no value.
const NULL_CODE: u32 = u32::MAX;

/// A dictionary string block projected to its codes and its dictionary, without materialising one string per row.
///
/// Every code is checked against the dictionary once, when the projection is built, so later gathers, equality tests
/// and grouping read codes with no further validation. A reader that only compares, groups or gathers a few rows
/// should take this form instead of [`super::decode_block`]'s whole [`StringColumn`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryCodes<'a> {
    /// One code per row, [`NULL_CODE`] for a row without a value; every other code indexes `dictionary`.
    codes: Vec<u32>,
    dictionary: Cow<'a, [String]>,
}

impl<'a> DictionaryCodes<'a> {
    /// Rows in the block.
    pub fn len(&self) -> usize {
        self.codes.len()
    }

    /// Whether the block has no rows.
    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    /// The block's dictionary, in code order (sorted, so code order is value order).
    pub fn dictionary(&self) -> &[String] {
        &self.dictionary
    }

    /// Row `row`'s code: `None` past the last row, `Some(None)` for a row without a value.
    pub fn code(&self, row: usize) -> Option<Option<u32>> {
        let code = *self.codes.get(row)?;
        Some((code != NULL_CODE).then_some(code))
    }

    /// Every row's code, [`u32::MAX`] marking a row without a value.
    pub fn raw_codes(&self) -> &[u32] {
        &self.codes
    }

    /// Row `row`'s value: `None` past the last row, `Some(None)` for a row without a value.
    pub fn value(&self, row: usize) -> Option<Option<&str>> {
        Some(
            self.code(row)?
                .and_then(|code| self.dictionary.get(code as usize))
                .map(String::as_str),
        )
    }

    /// Rows per dictionary entry — a group-by over the block answered from the codes alone.
    pub fn code_counts(&self) -> Vec<u64> {
        let mut counts = vec![0u64; self.dictionary.len()];
        for &code in &self.codes {
            if let Some(slot) = counts.get_mut(code as usize) {
                *slot += 1;
            }
        }
        counts
    }

    /// Rows whose value equals `needle`, as a mask: one dictionary binary search, then one code compare per row.
    pub fn equals_mask(&self, needle: &str) -> Vec<bool> {
        match self.dictionary.binary_search_by(|entry| entry.as_str().cmp(needle)) {
            Ok(index) => {
                let target = index as u32;
                self.codes.iter().map(|&code| code == target).collect()
            }
            Err(_) => vec![false; self.codes.len()],
        }
    }

    /// The values of `rows`, in the order given, reusing the codes validated when the projection was built.
    pub fn gather(&self, rows: &[usize]) -> Result<StringColumn, FormatError> {
        let mut out = StringColumn::with_capacity(rows.len(), 0);
        for &row in rows {
            let value = self.value(row).ok_or(FormatError::RefOutOfRange { what: "take row" })?;
            out.push(value);
        }
        Ok(out)
    }

    /// Materialises every row, the same column [`super::decode_block_shared`] returns.
    pub fn to_strings(&self) -> StringColumn {
        let mut out = StringColumn::with_capacity(self.codes.len(), 0);
        for row in 0..self.codes.len() {
            out.push(self.value(row).flatten());
        }
        out
    }

    /// Detaches the projection from the shared alphabet it may borrow.
    pub fn into_owned(self) -> DictionaryCodes<'static> {
        DictionaryCodes {
            codes: self.codes,
            dictionary: Cow::Owned(self.dictionary.into_owned()),
        }
    }
}

/// Projects a dictionary string block to [`DictionaryCodes`]: the codes plus the dictionary, with no per-row string.
/// Returns `Ok(None)` for any other pipeline. `shared` is the column's file-scope alphabet, required for a block
/// recording [`SideStream::FileScopeDictionary`] and borrowed rather than copied.
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
    let mut codes = Vec::with_capacity(nulls.row_count());
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
    Ok(DictionaryCodes { codes, dictionary })
}

/// The plaintext size of an FSST string block's present values, summed from the symbol lengths their codes name
/// without decoding a single value (a trailing stage, if any, is still inflated). Returns `Ok(None)` for any other
/// pipeline.
pub fn fsst_uncompressed_len(pipeline: PipelineId, bytes: &[u8]) -> Result<Option<usize>, FormatError> {
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
        // Values lie end to end, so the whole run of codes is summed at once — except that an escape must not
        // straddle a value boundary, which a forged block could do; walking per value keeps that a format error.
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

/// Plaintext bytes one value's FSST codes expand to, from the symbol length table alone.
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

/// The values at logical rows `rows` of one block, in the order given — the same values a whole-block
/// [`super::decode_block_shared`] followed by a gather returns.
///
/// FSST blocks decompress only the requested values through their offset table; dictionary blocks read only the
/// requested codes; raw blocks slice only the requested values; run-length blocks binary-search a run directory.
/// A trailing stage (seekable-Zstandard frames included) is inflated first. Every other block falls back to a whole-block decode and a gather.
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
        (ValueKind::String, Transform::RawString) => Ok(ColumnData::Strings(take_raw(body, rows)?)),
        (ValueKind::String, Transform::DictionaryString) => Ok(ColumnData::Strings(take_dictionary(
            body,
            pipeline.side_stream()?,
            shared,
            rows,
        )?)),
        (ValueKind::U64, Transform::ForBitpack) => Ok(ColumnData::U64(take_for_bitpack(body, rows)?)),
        (ValueKind::I64, Transform::ForBitpack) => Ok(ColumnData::I64(take_for_bitpack(body, rows)?)),
        (ValueKind::U64, Transform::Rle) => Ok(ColumnData::U64(RunDirectory::read(body)?.take(rows)?)),
        (ValueKind::I64, Transform::Rle) => Ok(ColumnData::I64(RunDirectory::read(body)?.take(rows)?)),
        _ => gather_column(&decode_block_shared_with_body(pipeline, body, shared)?, rows),
    })
}

/// The values at `rows` of a frame-of-reference bitpacked block, each read straight out of its FastLanes lane.
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

/// [`take_block_rows_shared`] for a block with no file-scope alphabet.
pub fn take_block_rows(pipeline: PipelineId, bytes: &[u8], rows: &[usize]) -> Result<ColumnData, FormatError> {
    take_block_rows_shared(pipeline, bytes, None, rows)
}

/// The dense present-value position of `row`, `None` when it carries no value; a row past the block is an error.
fn present_position_of(nulls: &NullStream<'_>, row: usize) -> Result<Option<usize>, FormatError> {
    if row >= nulls.row_count() {
        return Err(FormatError::RefOutOfRange { what: "take row" });
    }
    Ok(nulls.is_present(row).then(|| nulls.present_before(row)))
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
    for &row in rows {
        let Some(position) = present_position_of(&header.nulls, row)? else {
            out.push(None);
            continue;
        };
        let bounds = offset_window(table, position, 2)?;
        let [start, end] = bounds.as_slice() else {
            return Err(FormatError::Truncated { what: "string offset" });
        };
        if end < start {
            return Err(FormatError::Structural {
                rule: "fsst offsets must be non-decreasing",
            });
        }
        let compressed = slice(data, *start, end - start, "fsst value")?;
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
    for &row in rows {
        let Some(position) = present_position_of(&header.nulls, row)? else {
            out.push(None);
            continue;
        };
        let bounds = offset_window(table, position, 2)?;
        let [start, end] = bounds.as_slice() else {
            return Err(FormatError::Truncated { what: "string offset" });
        };
        if end < start {
            return Err(FormatError::Structural {
                rule: "raw offsets must be non-decreasing",
            });
        }
        let bytes = slice(data, *start, end - start, "raw string value")?;
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
    let entries: Vec<&str> = if side == SideStream::FileScopeDictionary {
        shared
            .ok_or(FormatError::Structural {
                rule: "shared-scope dictionary block without its file alphabet",
            })?
            .iter()
            .map(String::as_str)
            .collect()
    } else {
        let dict_count = reader.u32("dictionary count")? as usize;
        values = DictionaryValues::read(&mut reader, dict_count, side)?;
        values.entries()?
    };
    // Only the requested rows' codes are read, each straight out of its FastLanes lane.
    let packed = read_packed_stream(&mut reader)?;
    if packed.count != nulls.present_count() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let mut out = StringColumn::with_capacity(rows.len(), 0);
    for &row in rows {
        let Some(position) = present_position_of(&nulls, row)? else {
            out.push(None);
            continue;
        };
        let code = fastlanes_value_at(packed.packed, packed.width, position) as usize;
        out.push(Some(entries.get(code).copied().ok_or(FormatError::RefOutOfRange {
            what: "dictionary code",
        })?));
    }
    Ok(out)
}

/// A run-length block's runs with the cumulative row count at each run's end — the directory that maps a row to its
/// run by binary search and a row range to the runs it overlaps.
#[derive(Debug)]
struct RunDirectory {
    values: Vec<u64>,
    /// `ends[i]` = rows in runs `0..=i`.
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

    /// The values of the rows `selection` sets, in row order: each run's surviving count is two rank-directory
    /// lookups, so a run of a million rows costs the same as a run of one.
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

    /// Rows `[start, end)`, expanding only the runs that overlap them.
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

/// Rows `[start, end)` of a run-length block body, expanding only the overlapping runs.
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

/// The values of the rows a packed LSB-0 `selection` bitmap sets (bit `i` selects row `i`), in row order — what a
/// whole-block decode followed by that filter returns. A run-length block counts each run's selected rows through a
/// [`BitRankDirectory`] in one pass over the runs; every other block decodes and filters.
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

/// `rows` of already-decoded `data`, in the order given.
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
