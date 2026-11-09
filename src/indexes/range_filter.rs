use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::indexes::Exactness;

const RANGE_FILTER_MAGIC: u32 = 0x5247_4631;

pub const DEFAULT_BUCKET_COUNT: u32 = 4096;

const OCCUPANCY_WORDS: usize = (DEFAULT_BUCKET_COUNT / 64) as usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeFilter {
    bucket_count: u32,
    occupied: Vec<u32>,
}

impl RangeFilter {
    pub fn build(keys: &[u64]) -> Self {
        Self::build_with_buckets(keys, DEFAULT_BUCKET_COUNT)
    }

    pub fn build_with_buckets(keys: &[u64], bucket_count: u32) -> Self {
        let bucket_count = bucket_count.max(1);
        if keys.is_empty() {
            return RangeFilter {
                bucket_count,
                occupied: Vec::new(),
            };
        }
        if bucket_count > DEFAULT_BUCKET_COUNT {
            let mut occupied: Vec<u32> = keys.iter().map(|&key| Self::bucket_of(key, bucket_count)).collect();
            occupied.sort_unstable();
            occupied.dedup();
            return RangeFilter { bucket_count, occupied };
        }
        let mut words = [0u64; OCCUPANCY_WORDS];
        for &key in keys {
            let bucket = Self::bucket_of(key, bucket_count);
            if let Some(word) = words.get_mut((bucket / 64) as usize) {
                *word |= 1u64 << (bucket % 64);
            }
        }
        let mut occupied = Vec::with_capacity(words.iter().map(|word| word.count_ones() as usize).sum());
        for (index, &word) in words.iter().enumerate() {
            let mut bits = word;
            while bits != 0 {
                occupied.push(index as u32 * 64 + bits.trailing_zeros());
                bits &= bits - 1;
            }
        }
        RangeFilter { bucket_count, occupied }
    }

    fn bucket_of(key: u64, bucket_count: u32) -> u32 {
        let product = (key as u128).wrapping_mul(bucket_count as u128);
        (product >> 64) as u32
    }

    pub fn range_nonempty(&self, lo: u64, hi: u64) -> bool {
        if lo > hi {
            return false;
        }
        let lo_bucket = Self::bucket_of(lo, self.bucket_count);
        let hi_bucket = Self::bucket_of(hi, self.bucket_count);
        match self.occupied.binary_search(&lo_bucket) {
            Ok(_) => true,
            Err(pos) => matches!(self.occupied.get(pos), Some(&bucket) if bucket <= hi_bucket),
        }
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::InexactNoFalseNegative
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(12 + self.occupied.len() * 4);
        out.put_u32(RANGE_FILTER_MAGIC);
        out.put_u32(self.bucket_count);
        out.put_u32(self.occupied.len() as u32);
        for &bucket in &self.occupied {
            out.put_u32(bucket);
        }
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        if reader.u32("range filter magic")? != RANGE_FILTER_MAGIC {
            return Err(FormatError::Structural {
                rule: "range filter bad magic",
            });
        }
        let bucket_count = reader.u32("range filter bucket count")?;
        if bucket_count == 0 {
            return Err(FormatError::Structural {
                rule: "range filter has no buckets",
            });
        }
        let occupied_len = reader.u32("range filter occupied count")? as usize;
        let hint = reader.capacity_hint(occupied_len, 4);
        let mut occupied = Vec::with_capacity(hint);
        let mut previous: Option<u32> = None;
        for _ in 0..occupied_len {
            let bucket = reader.u32("range filter occupied bucket")?;
            if bucket >= bucket_count {
                return Err(FormatError::Structural {
                    rule: "range filter bucket id out of range",
                });
            }
            if let Some(prev) = previous
                && bucket <= prev
            {
                return Err(FormatError::Structural {
                    rule: "range filter buckets not strictly ascending",
                });
            }
            previous = Some(bucket);
            occupied.push(bucket);
        }
        Ok(RangeFilter { bucket_count, occupied })
    }
}

#[cfg(test)]
#[path = "test/range_filter.rs"]
mod tests;
