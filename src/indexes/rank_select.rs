use super::bitmap::{RoaringRangeBitmap, RowRange};

#[derive(Debug, Clone)]
pub struct RankSelect {
    cumulative: Vec<u64>,
    runs: Vec<RowRange>,
    total: u64,
}

impl RankSelect {
    pub fn from_bitmap(bitmap: &RoaringRangeBitmap) -> Self {
        Self::from_runs(bitmap.ranges().to_vec())
    }

    fn from_runs(runs: Vec<RowRange>) -> Self {
        let mut cumulative = Vec::with_capacity(runs.len());
        let mut acc = 0u64;
        for run in &runs {
            cumulative.push(acc);
            acc += run.end.saturating_sub(run.start);
        }
        Self {
            cumulative,
            runs,
            total: acc,
        }
    }

    pub fn from_packed_bits(bits: &[u8]) -> Self {
        Self::from_runs(RoaringRangeBitmap::from_packed_bits(bits).into_ranges())
    }

    pub fn rank(&self, position: u64) -> u64 {
        if position == 0 || self.runs.is_empty() {
            return 0;
        }
        let full_idx = self.runs.partition_point(|r| r.end <= position);
        let full_count = self.cumulative.get(full_idx).copied().unwrap_or(self.total);
        let partial = self
            .runs
            .get(full_idx)
            .filter(|r| r.start < position)
            .map_or(0, |r| position - r.start);
        full_count + partial
    }

    pub fn select(&self, k: u64) -> Option<u64> {
        if k >= self.total {
            return None;
        }
        let idx = self.cumulative.partition_point(|&c| c <= k);
        let run_idx = idx.saturating_sub(1);
        let run = self.runs.get(run_idx)?;
        let offset = k - self.cumulative.get(run_idx).copied().unwrap_or(0);
        Some(run.start + offset)
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn rank_and_contains(&self, position: u64) -> (u64, bool) {
        if self.runs.is_empty() {
            return (0, false);
        }
        let full_idx = self.runs.partition_point(|r| r.end <= position);
        let full_count = self.cumulative.get(full_idx).copied().unwrap_or(self.total);
        let run = self.runs.get(full_idx);
        let contains = run.is_some_and(|r| r.start <= position);
        let partial = run.filter(|r| r.start < position).map_or(0, |r| position - r.start);
        (full_count + partial, contains)
    }
}

const RANK_SUPERBLOCK_BITS: usize = 512;

#[derive(Debug, Clone)]
pub struct BitRankDirectory<'a> {
    bits: &'a [u8],
    len: usize,
    superblocks: Vec<u64>,
}

impl<'a> BitRankDirectory<'a> {
    pub fn new(bits: &'a [u8], len: usize) -> Self {
        let len = len.min(bits.len().saturating_mul(8));
        let full = len / RANK_SUPERBLOCK_BITS;
        let mut superblocks = Vec::with_capacity(full + 1);
        let mut running = 0u64;
        superblocks.push(0);
        for block in bits.chunks(RANK_SUPERBLOCK_BITS / 8).take(full) {
            running += popcount_bytes(block);
            superblocks.push(running);
        }
        Self { bits, len, superblocks }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn rank(&self, position: usize) -> u64 {
        let position = position.min(self.len);
        let block = position / RANK_SUPERBLOCK_BITS;
        let base = self.superblocks.get(block).copied().unwrap_or(0);
        let start_byte = block * (RANK_SUPERBLOCK_BITS / 8);
        let end_byte = position / 8;
        let whole = self.bits.get(start_byte..end_byte).map_or(0, popcount_bytes);
        let tail_bits = position % 8;
        let tail = if tail_bits == 0 {
            0
        } else {
            self.bits
                .get(end_byte)
                .map_or(0, |byte| u64::from((byte & ((1u8 << tail_bits) - 1)).count_ones()))
        };
        base + whole + tail
    }

    pub fn count_range(&self, start: usize, end: usize) -> u64 {
        if end <= start {
            return 0;
        }
        self.rank(end) - self.rank(start)
    }

    pub fn total(&self) -> u64 {
        self.rank(self.len)
    }
}

fn popcount_bytes(bytes: &[u8]) -> u64 {
    let mut words = bytes.chunks_exact(8);
    let mut count = 0u64;
    for word in words.by_ref() {
        count += u64::from(u64::from_le_bytes(word.try_into().unwrap_or([0; 8])).count_ones());
    }
    for byte in words.remainder() {
        count += u64::from(byte.count_ones());
    }
    count
}

#[cfg(test)]
#[path = "test/rank_select.rs"]
mod tests;
