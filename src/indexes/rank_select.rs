//! Constant-time rank and select over a compressed run-list bitmap.
//!
//! `RankSelect` wraps a `RoaringRangeBitmap` with one precomputed cumulative-count array so each query costs a single
//! binary search over the (typically small) run list — never a linear scan over individual rows.
//!
//! - **rank(pos)**: how many set bits appear before position `pos`.
//! - **select(k)**: physical position of the k-th set bit (0-indexed).
//!
//! Both answers are exact and identical to what a full materialisation of the bitmap into a bit-vector and a naive scan
//! would produce. The structure is derivable from and always consistent with the bitmap it indexes.
//!
//! The scan uses this to translate filtered logical positions into physical row offsets for late-materialisation
//! gather, and to apply the deletion-vector anti-join as bitmap operations rather than a row-by-row walk.

use super::bitmap::{RoaringRangeBitmap, RowRange};

/// Rank and select acceleration over a `RoaringRangeBitmap`.
///
/// Built once from a bitmap with `from_bitmap`; each query (`rank`, `select`) then costs one binary search over the
/// precomputed cumulative-count array. The structure is exact: it is derivable from the bitmap and every answer equals
/// what a full materialisation would produce.
#[derive(Debug, Clone)]
pub struct RankSelect {
    /// `cumulative[i]` = total set rows in `runs[0..i]`.
    cumulative: Vec<u64>,
    runs: Vec<RowRange>,
    total: u64,
}

impl RankSelect {
    /// Builds the rank/select index from a bitmap. Precomputes one cumulative-count entry per run; subsequent queries
    /// cost O(log runs).
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

    /// Builds the rank/select index straight from a packed LSB-0 presence bitmap, deriving the run list from the byte
    /// scan ([`RoaringRangeBitmap::from_packed_bits`]) without ever materializing one row id per set bit. Every answer
    /// is identical to building over `RoaringRangeBitmap::from_rows(<every set row>)` of the same bits — the raw path
    /// stays the equivalence oracle in tests.
    pub fn from_packed_bits(bits: &[u8]) -> Self {
        Self::from_runs(RoaringRangeBitmap::from_packed_bits(bits).into_ranges())
    }

    /// How many set bits (live or non-null rows) appear strictly before `position`. Returns 0 when `position` is 0 or
    /// the bitmap is empty.
    ///
    /// Exact: identical to counting set bits in `[0, position)` over the materialized bitmap.
    pub fn rank(&self, position: u64) -> u64 {
        if position == 0 || self.runs.is_empty() {
            return 0;
        }
        // Runs whose end <= position are entirely before position.
        let full_idx = self.runs.partition_point(|r| r.end <= position);
        let full_count = self.cumulative.get(full_idx).copied().unwrap_or(self.total);
        // A partial overlap: runs[full_idx] may start before position.
        let partial = self
            .runs
            .get(full_idx)
            .filter(|r| r.start < position)
            .map_or(0, |r| position - r.start);
        full_count + partial
    }

    /// Physical position of the `k`-th set bit (0-indexed). Returns `None` when `k >= total`.
    ///
    /// Exact: same position a linear scan of the materialised bitmap returns.
    pub fn select(&self, k: u64) -> Option<u64> {
        if k >= self.total {
            return None;
        }
        // Find the last run whose cumulative prefix count is <= k; the k-th bit lives in that run.
        let idx = self.cumulative.partition_point(|&c| c <= k);
        let run_idx = idx.saturating_sub(1);
        let run = self.runs.get(run_idx)?;
        let offset = k - self.cumulative.get(run_idx).copied().unwrap_or(0);
        Some(run.start + offset)
    }

    /// Total number of set bits across all runs.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// `(rank(position), contains(position))` from one binary search instead of two: how many set bits appear
    /// strictly before `position`, together with whether `position` itself is set. Exact and identical to calling
    /// [`Self::rank`] at `position` and `position + 1` and comparing — the answer a caller needing both from a single
    /// bit position would otherwise pay two searches for.
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

/// Bits per [`BitRankDirectory`] superblock: eight 64-bit words, so a rank query costs one directory read plus at most
/// eight hardware popcounts.
const RANK_SUPERBLOCK_BITS: usize = 512;

/// A rank directory over a packed LSB-0 bit vector (bit `i` is bit `i % 8` of byte `i / 8`), such as a row selection.
///
/// One cumulative count is stored per [`RANK_SUPERBLOCK_BITS`]-bit superblock, so `rank(pos)` is one directory read
/// plus a POPCNT over at most eight words, and the selected count inside any row range `[start, end)` is two ranks.
/// A run-length block filtered by such a selection counts each run's surviving rows that way in one pass over the
/// runs, never one test per row.
#[derive(Debug, Clone)]
pub struct BitRankDirectory<'a> {
    bits: &'a [u8],
    len: usize,
    /// `superblocks[i]` = set bits in `[0, i * RANK_SUPERBLOCK_BITS)`.
    superblocks: Vec<u64>,
}

impl<'a> BitRankDirectory<'a> {
    /// Indexes the first `len` bits of `bits`; bits at or past `len`, or past the end of `bits`, count as unset.
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

    /// Number of bits the directory covers.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the directory covers no bits.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set bits strictly before `position` (clamped to [`Self::len`]).
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

    /// Set bits in `[start, end)`.
    pub fn count_range(&self, start: usize, end: usize) -> u64 {
        if end <= start {
            return 0;
        }
        self.rank(end) - self.rank(start)
    }

    /// Total set bits.
    pub fn total(&self) -> u64 {
        self.rank(self.len)
    }
}

/// Popcount of a byte slice, eight bytes per hardware popcount.
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
