pub mod artifact;
pub mod bitmap;
pub mod bool_pruning;
pub mod constant_flags;
pub mod entity_summary;
pub mod external_id;
pub mod learned_position;
pub mod minmax;
pub mod model;
pub mod ndv;
pub mod path_presence;
pub mod probabilistic;
pub mod pruning;
pub mod range_filter;
pub mod rank_select;
pub mod summary;
pub mod text_token;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exactness {
    Exact,
    InexactNoFalseNegative,
}

impl Exactness {
    pub fn requires_residual_filter(self) -> bool {
        matches!(self, Exactness::InexactNoFalseNegative)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Granularity {
    File,
    Granule,
    MiniBlock,
    Page,
    Stripe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipIndexKind {
    AccountHashFilter,
    BinaryFuseFilter,
    ContextLocator,
    EntityHashFilter,
    LearnedPosition,
    MinMax,
    OpportunityHashFilter,
    PathPresence,
    RangeFilter,
    RibbonFilter,
    SequenceRange,
    SplitBlockBloomFilter,
    TextToken,
    TimeRange,
}

pub fn stable_hash(bytes: &[u8]) -> u64 {
    stable_hash_of(bytes, |b| b)
}

pub fn stable_hash_ascii_lowercase(bytes: &[u8]) -> u64 {
    stable_hash_of(bytes, |b| b.to_ascii_lowercase())
}

const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

const FNV_CHUNK_WEIGHTS: [u64; 8] = {
    let second = FNV_PRIME.wrapping_mul(FNV_PRIME);
    let third = second.wrapping_mul(FNV_PRIME);
    let fourth = third.wrapping_mul(FNV_PRIME);
    let fifth = fourth.wrapping_mul(FNV_PRIME);
    let sixth = fifth.wrapping_mul(FNV_PRIME);
    let seventh = sixth.wrapping_mul(FNV_PRIME);
    [seventh, sixth, fifth, fourth, third, second, FNV_PRIME, 1]
};
const FNV_PRIME_POW8: u64 = FNV_PRIME.wrapping_mul(FNV_CHUNK_WEIGHTS[0]);

fn stable_hash_of(bytes: &[u8], byte: impl Fn(u8) -> u8) -> u64 {
    let mut h = FNV_OFFSET_BASIS;
    let mut rest = bytes;
    while let Some((chunk, tail)) = rest.split_first_chunk::<8>() {
        h = h.wrapping_mul(FNV_PRIME_POW8);
        for (&b, weight) in chunk.iter().zip(FNV_CHUNK_WEIGHTS) {
            h = h.wrapping_add(u64::from(byte(b)).wrapping_mul(weight));
        }
        rest = tail;
    }
    for &b in rest {
        h = h.wrapping_mul(FNV_PRIME).wrapping_add(u64::from(byte(b)));
    }
    let mut z = h.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
