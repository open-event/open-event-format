use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::indexes::Exactness;

const ENCODED_ROW_RANGE_BYTES: usize = 16;

const RUN_COUNT_PREFIX_BYTES: usize = 4;

const INDEX_ENTRY_HEADER_BYTES: usize = 12;

const CHECKSUM_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    pub end: u64,
    pub start: u64,
}

impl RowRange {
    fn is_empty(self) -> bool {
        self.end <= self.start
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoaringRangeBitmap {
    runs: Vec<RowRange>,
}

impl RoaringRangeBitmap {
    pub fn exactness() -> Exactness {
        Exactness::Exact
    }

    pub fn from_rows<I: IntoIterator<Item = u64>>(rows: I) -> Self {
        let mut sorted: Vec<u64> = rows.into_iter().collect();
        sorted.sort_unstable();
        sorted.dedup();
        let mut runs: Vec<RowRange> = Vec::new();
        for row in sorted {
            match runs.last_mut() {
                Some(last) if last.end == row => last.end = row + 1,
                _ => runs.push(RowRange {
                    start: row,
                    end: row + 1,
                }),
            }
        }
        Self { runs }
    }

    pub fn from_packed_bits(bits: &[u8]) -> Self {
        let mut runs: Vec<RowRange> = Vec::new();
        let mut open: Option<u64> = None;
        let mut words = bits.chunks_exact(8);
        let mut base = 0u64;
        for word in words.by_ref() {
            let word = u64::from_le_bytes(word.try_into().unwrap_or([0; 8]));
            push_word_runs(&mut runs, &mut open, base, word);
            base += 64;
        }
        if !words.remainder().is_empty() {
            let mut tail = [0u8; 8];
            for (slot, byte) in tail.iter_mut().zip(words.remainder()) {
                *slot = *byte;
            }
            push_word_runs(&mut runs, &mut open, base, u64::from_le_bytes(tail));
        }
        if let Some(start) = open {
            runs.push(RowRange {
                start,
                end: bits.len() as u64 * 8,
            });
        }
        Self { runs }
    }

    pub fn from_ranges<I: IntoIterator<Item = RowRange>>(ranges: I) -> Self {
        let mut runs: Vec<RowRange> = ranges.into_iter().filter(|r| !r.is_empty()).collect();
        runs.sort_unstable_by_key(|r| (r.start, r.end));
        runs.dedup_by(|range, last| {
            let merges = range.start <= last.end;
            if merges {
                last.end = last.end.max(range.end);
            }
            merges
        });
        Self { runs }
    }

    pub fn ranges(&self) -> &[RowRange] {
        &self.runs
    }

    pub(crate) fn into_ranges(self) -> Vec<RowRange> {
        self.runs
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn count(&self) -> u64 {
        self.runs.iter().map(|r| r.end.saturating_sub(r.start)).sum()
    }

    pub fn contains(&self, row: u64) -> bool {
        let idx = self.runs.partition_point(|r| r.start <= row);
        match idx.checked_sub(1).and_then(|i| self.runs.get(i)) {
            Some(run) => row < run.end,
            None => false,
        }
    }

    pub fn difference(&self, other: &Self) -> Self {
        let mut runs: Vec<RowRange> = Vec::new();
        let mut bi = 0usize;
        for a in &self.runs {
            let mut cur = a.start;
            while let Some(b) = other.runs.get(bi) {
                if b.end > cur {
                    break;
                }
                bi += 1;
            }
            let mut oi = bi;
            while let Some(b) = other.runs.get(oi) {
                if b.start >= a.end {
                    break;
                }
                let gap_end = b.start.min(a.end);
                if cur < gap_end {
                    runs.push(RowRange {
                        start: cur,
                        end: gap_end,
                    });
                }
                if b.end > cur {
                    cur = b.end;
                }
                oi += 1;
            }
            if cur < a.end {
                runs.push(RowRange { start: cur, end: a.end });
            }
        }
        Self { runs }
    }

    pub fn intersect(&self, other: &Self) -> Self {
        let mut runs: Vec<RowRange> = Vec::new();
        intersect_runs_into(&self.runs, &other.runs, &mut runs);
        Self { runs }
    }

    fn union(&self, other: &Self) -> Self {
        let mut runs: Vec<RowRange> = Vec::with_capacity(self.runs.len() + other.runs.len());
        let mut a = 0usize;
        let mut b = 0usize;
        loop {
            let next = match (self.runs.get(a), other.runs.get(b)) {
                (Some(left), Some(right)) if left.start <= right.start => {
                    a += 1;
                    *left
                }
                (_, Some(right)) => {
                    b += 1;
                    *right
                }
                (Some(left), None) => {
                    a += 1;
                    *left
                }
                (None, None) => break,
            };
            match runs.last_mut() {
                Some(last) if next.start <= last.end => last.end = last.end.max(next.end),
                _ => runs.push(next),
            }
        }
        Self { runs }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(self.encoded_len());
        self.encode_into(&mut out);
        out.into_bytes()
    }

    fn encoded_len(&self) -> usize {
        encoded_runs_len(self.runs.len())
    }

    fn encode_into(&self, out: &mut Writer) {
        put_encoded_runs(out, &self.runs);
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        let count = reader.u32("bitmap run count")? as usize;
        let mut runs: Vec<RowRange> = Vec::with_capacity(reader.capacity_hint(count, ENCODED_ROW_RANGE_BYTES));
        for _ in 0..count {
            let start = reader.u64("bitmap run start")?;
            let end = reader.u64("bitmap run end")?;
            if end <= start {
                return Err(FormatError::Structural {
                    rule: "bitmap run must be non-empty",
                });
            }
            if runs.last().is_some_and(|last| start <= last.end) {
                return Err(FormatError::Structural {
                    rule: "bitmap runs must be sorted and non-adjacent",
                });
            }
            runs.push(RowRange { start, end });
        }
        Ok(Self { runs })
    }
}

pub(crate) fn encoded_runs_len(run_count: usize) -> usize {
    RUN_COUNT_PREFIX_BYTES + run_count * ENCODED_ROW_RANGE_BYTES
}

pub(crate) fn put_encoded_runs(out: &mut Writer, runs: &[RowRange]) {
    out.put_u32(runs.len() as u32);
    for run in runs {
        out.put_u64(run.start);
        out.put_u64(run.end);
    }
}

fn intersect_runs_into(left: &[RowRange], right: &[RowRange], out: &mut Vec<RowRange>) {
    out.clear();
    let mut a = 0usize;
    let mut b = 0usize;
    while let (Some(l), Some(r)) = (left.get(a), right.get(b)) {
        let start = l.start.max(r.start);
        let end = l.end.min(r.end);
        if start < end {
            match out.last_mut() {
                Some(last) if start <= last.end => last.end = last.end.max(end),
                _ => out.push(RowRange { start, end }),
            }
        }
        if l.end <= r.end {
            a += 1;
        } else {
            b += 1;
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BitmapIndex {
    entries: Vec<(u64, RoaringRangeBitmap)>,
}

impl BitmapIndex {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn insert<I: IntoIterator<Item = u64>>(&mut self, value: u64, rows: I) {
        let added = RoaringRangeBitmap::from_rows(rows);
        match self.entries.binary_search_by_key(&value, |(v, _)| *v) {
            Ok(idx) => {
                if let Some((_, existing)) = self.entries.get_mut(idx) {
                    if existing.is_empty() {
                        *existing = added;
                    } else if !added.is_empty() {
                        *existing = existing.union(&added);
                    }
                }
            }
            Err(idx) => self.entries.insert(idx, (value, added)),
        }
    }

    pub fn bitmap(&self, value: u64) -> Option<&RoaringRangeBitmap> {
        self.entries
            .binary_search_by_key(&value, |(v, _)| *v)
            .ok()
            .and_then(|idx| self.entries.get(idx).map(|(_, b)| b))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn intersect_values(&self, values: &[u64]) -> RoaringRangeBitmap {
        let Some((first, rest)) = values.split_first() else {
            return RoaringRangeBitmap::default();
        };
        let Some(first) = self.bitmap(*first) else {
            return RoaringRangeBitmap::default();
        };
        let Some((second, rest)) = rest.split_first() else {
            return first.clone();
        };
        let Some(second) = self.bitmap(*second) else {
            return RoaringRangeBitmap::default();
        };
        let mut runs: Vec<RowRange> = Vec::new();
        intersect_runs_into(&first.runs, &second.runs, &mut runs);
        let mut scratch: Vec<RowRange> = Vec::new();
        for value in rest {
            if runs.is_empty() {
                break;
            }
            let Some(next) = self.bitmap(*value) else {
                return RoaringRangeBitmap::default();
            };
            intersect_runs_into(&runs, &next.runs, &mut scratch);
            std::mem::swap(&mut runs, &mut scratch);
        }
        RoaringRangeBitmap { runs }
    }

    pub fn encode(&self) -> Vec<u8> {
        let capacity = RUN_COUNT_PREFIX_BYTES
            + self
                .entries
                .iter()
                .map(|(_, bitmap)| INDEX_ENTRY_HEADER_BYTES + bitmap.encoded_len())
                .sum::<usize>();
        let mut out = Writer::with_capacity(capacity);
        out.put_u32(self.entries.len() as u32);
        for (value, bitmap) in &self.entries {
            out.put_u64(*value);
            out.put_u32(bitmap.encoded_len() as u32);
            bitmap.encode_into(&mut out);
        }
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        let count = reader.u32("bitmap index value count")? as usize;
        let mut entries: Vec<(u64, RoaringRangeBitmap)> =
            Vec::with_capacity(reader.capacity_hint(count, INDEX_ENTRY_HEADER_BYTES));
        for _ in 0..count {
            let value = reader.u64("bitmap index value")?;
            if entries.last().is_some_and(|(last, _)| value <= *last) {
                return Err(FormatError::Structural {
                    rule: "bitmap index values must be sorted and unique",
                });
            }
            let block_len = reader.u32("bitmap block length")? as usize;
            let block = reader.take(block_len, "bitmap block")?;
            entries.push((value, RoaringRangeBitmap::decode(block)?));
        }
        Ok(Self { entries })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitmapBlockInfo {
    pub checksum_verified: bool,
    pub directly_intersectable: bool,
    pub feature_declared: bool,
}

pub fn bitmap_block_checksum(block: &[u8]) -> [u8; CHECKSUM_BYTES] {
    *crate::file::integrity::hash_tree(block).as_bytes()
}

pub fn verify_bitmap_block(
    block: &[u8],
    expected: &[u8; CHECKSUM_BYTES],
    directly_intersectable: bool,
    feature_declared: bool,
) -> BitmapBlockInfo {
    BitmapBlockInfo {
        checksum_verified: &bitmap_block_checksum(block) == expected,
        directly_intersectable,
        feature_declared,
    }
}

pub fn can_use_bitmap(info: &BitmapBlockInfo) -> bool {
    info.checksum_verified && info.directly_intersectable && info.feature_declared
}

fn push_word_runs(runs: &mut Vec<RowRange>, open: &mut Option<u64>, base: u64, word: u64) {
    let mut steps = word ^ ((word << 1) | u64::from(open.is_some()));
    while steps != 0 {
        let row = base + u64::from(steps.trailing_zeros());
        match open.take() {
            Some(start) => runs.push(RowRange { start, end: row }),
            None => *open = Some(row),
        }
        steps &= steps - 1;
    }
}

#[cfg(test)]
#[path = "test/bitmap.rs"]
mod tests;
