use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::indexes::Exactness;
use multiversion::multiversion;

const SPLIT_BLOCK_BLOOM_MAGIC: u32 = 0x5342_4631;

const BINARY_FUSE_MAGIC: u32 = 0x4246_5532;

const LANES_PER_BLOCK: usize = 8;

const BLOCK_BITS: u64 = LANES_PER_BLOCK as u64 * 32;

const LANE_BYTES: usize = 4;

const SPLIT_BLOCK_BLOOM_HEADER_BYTES: usize = 8;

const LANE_SALT: [u32; LANES_PER_BLOCK] = [
    0x47b6_137b,
    0x4429_0993,
    0xd6e4_98a5,
    0x8b1d_0e9d,
    0x9f49_6df1,
    0x14db_dcdd,
    0xb96c_ff5b,
    0xc2b2_ae35,
];

const MIN_BLOCKS: usize = 1;

fn mix64(key: u64) -> u64 {
    let mut z = key.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn mix64_seeded(key: u64, seed: u64) -> u64 {
    mix64(key ^ seed.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitBlockBloomFilter {
    lanes: Vec<u32>,
}

impl SplitBlockBloomFilter {
    pub fn build(keys: &[u64], bits_per_key: u32) -> Self {
        Self::build_from_iter(keys.iter().copied(), keys.len(), bits_per_key)
    }

    pub(crate) fn build_from_iter(keys: impl IntoIterator<Item = u64>, key_count: usize, bits_per_key: u32) -> Self {
        let block_count = Self::block_count_for(key_count, bits_per_key);
        let mut filter = SplitBlockBloomFilter {
            lanes: vec![0u32; block_count.saturating_mul(LANES_PER_BLOCK)],
        };
        for key in keys {
            filter.insert(key);
        }
        filter
    }

    fn block_count_for(key_count: usize, bits_per_key: u32) -> usize {
        let bits_per_key = bits_per_key.max(1) as u64;
        let total_bits = (key_count as u64).saturating_mul(bits_per_key);
        let blocks = total_bits.div_ceil(BLOCK_BITS);
        (blocks as usize).max(MIN_BLOCKS)
    }

    fn block_index(hash: u64, block_count: usize) -> usize {
        let hi = hash >> 32;
        (hi.wrapping_mul(block_count as u64) >> 32) as usize
    }

    fn block_count(&self) -> usize {
        self.lanes.len() / LANES_PER_BLOCK
    }

    fn insert(&mut self, key: u64) {
        let hash = mix64(key);
        let base = Self::block_index(hash, self.block_count()).wrapping_mul(LANES_PER_BLOCK);
        let mask = Self::lane_mask(hash as u32);
        let Some(block) = self.lanes.get_mut(base..base.wrapping_add(LANES_PER_BLOCK)) else {
            return;
        };
        for (lane, bit) in block.iter_mut().zip(mask) {
            *lane |= bit;
        }
    }

    fn lane_mask(lo: u32) -> [u32; LANES_PER_BLOCK] {
        let mut mask = [0u32; LANES_PER_BLOCK];
        for (bit, salt) in mask.iter_mut().zip(LANE_SALT) {
            *bit = 1u32 << (lo.wrapping_mul(salt) >> 27);
        }
        mask
    }

    pub fn contains(&self, key: u64) -> bool {
        probe_block(&self.lanes, self.block_count(), key)
    }

    pub fn contains_encoded(bytes: &[u8], key: u64) -> Result<bool, FormatError> {
        let mut reader = Reader::new(bytes);
        if reader.u32("split-block bloom magic")? != SPLIT_BLOCK_BLOOM_MAGIC {
            return Err(FormatError::Structural {
                rule: "split-block bloom filter bad magic",
            });
        }
        let block_count = reader.u32("split-block bloom block count")? as usize;
        if block_count < MIN_BLOCKS {
            return Err(FormatError::Structural {
                rule: "split-block bloom filter has no blocks",
            });
        }
        let lane_bytes = reader.take(
            block_count.saturating_mul(LANES_PER_BLOCK).saturating_mul(4),
            "split-block bloom lanes",
        )?;
        let hash = mix64(key);
        let base = Self::block_index(hash, block_count)
            .wrapping_mul(LANES_PER_BLOCK)
            .wrapping_mul(4);
        let Some(block) = lane_bytes.get(base..base.wrapping_add(LANES_PER_BLOCK * 4)) else {
            return Err(FormatError::Structural {
                rule: "split-block bloom block index outside the filter",
            });
        };
        let mut missing = 0u32;
        for (lane, bit) in block.chunks_exact(4).zip(Self::lane_mask(hash as u32)) {
            let lane = u32::from_le_bytes(lane.try_into().unwrap_or_default());
            missing |= !lane & bit;
        }
        Ok(missing == 0)
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::InexactNoFalseNegative
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(self.encoded_len());
        self.encode_into(&mut out);
        out.into_bytes()
    }

    pub(crate) fn encoded_len(&self) -> usize {
        SPLIT_BLOCK_BLOOM_HEADER_BYTES + self.lanes.len() * LANE_BYTES
    }

    pub(crate) fn encode_into(&self, out: &mut Writer) {
        out.put_u32(SPLIT_BLOCK_BLOOM_MAGIC);
        out.put_u32(self.block_count() as u32);
        out.put_u32_slice(&self.lanes);
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        if reader.u32("split-block bloom magic")? != SPLIT_BLOCK_BLOOM_MAGIC {
            return Err(FormatError::Structural {
                rule: "split-block bloom filter bad magic",
            });
        }
        let block_count = reader.u32("split-block bloom block count")? as usize;
        if block_count < MIN_BLOCKS {
            return Err(FormatError::Structural {
                rule: "split-block bloom filter has no blocks",
            });
        }
        let lane_count = block_count.saturating_mul(LANES_PER_BLOCK);
        let lanes = reader.u32_vec(lane_count, "split-block bloom lanes")?;
        Ok(SplitBlockBloomFilter { lanes })
    }
}

#[multiversion(targets = "simd")]
fn probe_block(lanes: &[u32], block_count: usize, key: u64) -> bool {
    let hash = mix64(key);
    let base = SplitBlockBloomFilter::block_index(hash, block_count).wrapping_mul(LANES_PER_BLOCK);
    let mask = SplitBlockBloomFilter::lane_mask(hash as u32);
    let Some(block) = lanes.get(base..base.wrapping_add(LANES_PER_BLOCK)) else {
        return false;
    };
    let mut missing = 0u32;
    for (lane, bit) in block.iter().zip(mask) {
        missing |= !lane & bit;
    }
    missing == 0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryFuseFilter {
    fingerprints: Vec<u8>,
    seed: u64,
    segment_len: u32,
}

#[derive(Default)]
struct FuseScratch {
    counts: Vec<u32>,
    precomputed: Vec<([usize; FUSE_SEGMENTS], u8)>,
    queue: Vec<usize>,
    stack: Vec<(usize, usize)>,
    xor_index: Vec<u64>,
}

const FUSE_MAX_SEEDS: u32 = 64;

const FUSE_CAPACITY_SLACK: usize = 48;

const FUSE_SEGMENTS: usize = 3;

const FUSE_LOAD_FACTOR_PERCENT: usize = 123;

const FUSE_HEADER_BYTES: usize = 16;

impl BinaryFuseFilter {
    pub fn build(keys: &[u64]) -> Result<Self, FormatError> {
        let mut distinct: Vec<u64> = keys.to_vec();
        distinct.sort_unstable();
        distinct.dedup();

        let n = distinct.len();
        let capacity = n.saturating_mul(FUSE_LOAD_FACTOR_PERCENT) / 100 + FUSE_CAPACITY_SLACK;
        let segment_len = (capacity.div_ceil(FUSE_SEGMENTS)).max(1) as u32;

        let mut scratch = FuseScratch::default();
        for attempt in 0..FUSE_MAX_SEEDS {
            let seed = 0x51_2b_9f_01_u64.wrapping_mul(u64::from(attempt) + 1);
            if let Some(filter) = Self::try_build(&distinct, segment_len, seed, &mut scratch) {
                return Ok(filter);
            }
        }
        Err(FormatError::Structural {
            rule: "binary fuse filter could not peel key set within reseed budget",
        })
    }

    fn slots_and_fingerprint(key: u64, segment_len: u32, seed: u64) -> ([usize; FUSE_SEGMENTS], u8) {
        let h = mix64_seeded(key, seed);
        let seg = segment_len as u64;
        let i0 = Self::slot_in_segment(h as u32, seg);
        let i1 = Self::slot_in_segment((h >> 16) as u32, seg);
        let i2 = Self::slot_in_segment((h >> 32) as u32, seg);
        let base1 = seg;
        let base2 = seg.wrapping_mul(2);
        let slots = [
            i0 as usize,
            base1.wrapping_add(i1) as usize,
            base2.wrapping_add(i2) as usize,
        ];
        let fp = (h as u8) | 1;
        (slots, fp)
    }

    fn slot_in_segment(word: u32, segment_len: u64) -> u64 {
        (u64::from(word).wrapping_mul(segment_len)) >> 32
    }

    fn try_build(distinct: &[u64], segment_len: u32, seed: u64, scratch: &mut FuseScratch) -> Option<Self> {
        let array_len = (segment_len as usize).checked_mul(FUSE_SEGMENTS)?;
        let FuseScratch {
            counts,
            precomputed,
            queue,
            stack,
            xor_index,
        } = scratch;

        precomputed.clear();
        precomputed.extend(
            distinct
                .iter()
                .map(|&key| Self::slots_and_fingerprint(key, segment_len, seed)),
        );

        xor_index.clear();
        xor_index.resize(array_len, 0);
        counts.clear();
        counts.resize(array_len, 0);
        for (index, &(slots, _)) in precomputed.iter().enumerate() {
            for &s in &slots {
                let xi = xor_index.get_mut(s)?;
                *xi ^= index as u64;
                let c = counts.get_mut(s)?;
                *c = c.wrapping_add(1);
            }
        }

        stack.clear();
        stack.reserve(distinct.len());
        queue.clear();
        queue.extend((0..array_len).filter(|&s| counts.get(s) == Some(&1)));

        while let Some(slot) = queue.pop() {
            if counts.get(slot).copied().unwrap_or(0) != 1 {
                continue;
            }
            let index = xor_index.get(slot).copied().unwrap_or(0) as usize;
            let (slots, _) = *precomputed.get(index)?;
            stack.push((index, slot));
            for &s in &slots {
                let c = counts.get_mut(s)?;
                *c = c.wrapping_sub(1);
                let xi = xor_index.get_mut(s)?;
                *xi ^= index as u64;
                if counts.get(s) == Some(&1) {
                    queue.push(s);
                }
            }
        }

        if stack.len() != distinct.len() {
            return None;
        }

        let mut fingerprints = vec![0u8; array_len];
        for &(index, slot) in stack.iter().rev() {
            let (slots, fp) = *precomputed.get(index)?;
            let mut value = fp;
            for &s in &slots {
                if s != slot {
                    value ^= fingerprints.get(s).copied().unwrap_or(0);
                }
            }
            if let Some(cell) = fingerprints.get_mut(slot) {
                *cell = value;
            }
        }

        Some(BinaryFuseFilter {
            fingerprints,
            seed,
            segment_len,
        })
    }

    pub fn contains(&self, key: u64) -> bool {
        let (slots, fp) = Self::slots_and_fingerprint(key, self.segment_len, self.seed);
        let mut acc = 0u8;
        for &s in &slots {
            acc ^= self.fingerprints.get(s).copied().unwrap_or(0);
        }
        acc == fp
    }

    pub fn contains_encoded(bytes: &[u8], key: u64) -> Result<bool, FormatError> {
        let (fingerprints, seed, segment_len) = Self::parse(bytes)?;
        let (slots, fp) = Self::slots_and_fingerprint(key, segment_len, seed);
        let mut acc = 0u8;
        for &s in &slots {
            acc ^= fingerprints.get(s).copied().unwrap_or(0);
        }
        Ok(acc == fp)
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::InexactNoFalseNegative
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(FUSE_HEADER_BYTES + self.fingerprints.len());
        out.put_u32(BINARY_FUSE_MAGIC);
        out.put_u32(self.segment_len);
        out.put_u64(self.seed);
        out.put_slice(&self.fingerprints);
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let (fingerprints, seed, segment_len) = Self::parse(bytes)?;
        Ok(BinaryFuseFilter {
            fingerprints: fingerprints.to_vec(),
            seed,
            segment_len,
        })
    }

    pub(crate) fn parse(bytes: &[u8]) -> Result<(&[u8], u64, u32), FormatError> {
        let mut reader = Reader::new(bytes);
        if reader.u32("binary fuse magic")? != BINARY_FUSE_MAGIC {
            return Err(FormatError::Structural {
                rule: "binary fuse filter bad magic",
            });
        }
        let segment_len = reader.u32("binary fuse segment length")?;
        if segment_len == 0 {
            return Err(FormatError::Structural {
                rule: "binary fuse filter has empty segment",
            });
        }
        let seed = reader.u64("binary fuse seed")?;
        let array_len = (segment_len as usize)
            .checked_mul(FUSE_SEGMENTS)
            .ok_or(FormatError::Structural {
                rule: "binary fuse filter length overflow",
            })?;
        let fingerprints = reader.take(array_len, "binary fuse fingerprints")?;
        Ok((fingerprints, seed, segment_len))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipFilterChoice {
    BinaryFuse,
    SplitBlockBloom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterRecommendation {
    UseBitmapOrValueSet,
    UseMembershipFilter(MembershipFilterChoice),
}

const LOW_CARDINALITY_DISTINCT_FLOOR: u64 = 256;

const HIGH_CARDINALITY_RATIO_PERCENT: u64 = 10;

pub fn should_build_probabilistic_filter(distinct_count: u64, row_count: u64) -> bool {
    if row_count == 0 {
        return false;
    }
    if distinct_count < LOW_CARDINALITY_DISTINCT_FLOOR {
        return false;
    }
    distinct_count.saturating_mul(100) >= row_count.saturating_mul(HIGH_CARDINALITY_RATIO_PERCENT)
}

pub fn choose_membership_filter(immutable_known_key_set: bool) -> MembershipFilterChoice {
    if immutable_known_key_set {
        MembershipFilterChoice::BinaryFuse
    } else {
        MembershipFilterChoice::SplitBlockBloom
    }
}

pub fn recommend_filter(distinct_count: u64, row_count: u64, immutable_known_key_set: bool) -> FilterRecommendation {
    if should_build_probabilistic_filter(distinct_count, row_count) {
        FilterRecommendation::UseMembershipFilter(choose_membership_filter(immutable_known_key_set))
    } else {
        FilterRecommendation::UseBitmapOrValueSet
    }
}

#[cfg(test)]
#[path = "test/probabilistic.rs"]
mod tests;
