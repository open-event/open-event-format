use crate::encoding::ColumnData;
use hashbrown::HashSet;

pub const NDV_EXACT_MAX: usize = 1024;

const NDV_REGISTERS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NdvSketch {
    exact: Option<HashSet<u64>>,
    registers: [u8; NDV_REGISTERS],
}

impl NdvSketch {
    pub fn new() -> Self {
        Self {
            exact: Some(HashSet::new()),
            registers: [0u8; NDV_REGISTERS],
        }
    }

    pub fn observe_hash(&mut self, hash: u64) {
        if let Some(exact) = &mut self.exact {
            exact.insert(hash);
            if exact.len() > NDV_EXACT_MAX {
                self.exact = None;
            }
        }
        let register = (hash >> 56) as usize;
        let rank = ((hash << 8) | 1).leading_zeros() as u8 + 1;
        if let Some(slot) = self.registers.get_mut(register)
            && *slot < rank
        {
            *slot = rank;
        }
    }

    pub fn observe_column(&mut self, data: &ColumnData) {
        match data {
            ColumnData::U64(values) => {
                for value in values {
                    self.observe_hash(mix(*value));
                }
            }
            ColumnData::I64(values) => {
                for value in values {
                    self.observe_hash(mix(*value as u64));
                }
            }
            ColumnData::F64(values) => {
                for value in values {
                    self.observe_hash(mix(value.to_bits()));
                }
            }
            ColumnData::U128(values) => {
                for value in values {
                    self.observe_hash(mix((*value as u64) ^ mix((*value >> 64) as u64)));
                }
            }
            ColumnData::Decimal { values, .. } => {
                for value in values {
                    self.observe_hash(mix((*value as u64) ^ mix((*value >> 64) as u64)));
                }
            }
            ColumnData::Strings(values) => {
                for value in values.iter().flatten() {
                    self.observe_hash(hash_bytes(value.as_bytes()));
                }
            }
        }
    }

    pub fn merge(&mut self, other: &NdvSketch) {
        match (&mut self.exact, &other.exact) {
            (Some(mine), Some(theirs)) => {
                if mine.len() + theirs.len() <= NDV_EXACT_MAX {
                    mine.extend(theirs.iter().copied());
                } else {
                    let room = NDV_EXACT_MAX.saturating_sub(mine.len());
                    let absent = theirs
                        .iter()
                        .filter(|hash| !mine.contains(*hash))
                        .take(room + 1)
                        .count();
                    if absent > room {
                        self.exact = None;
                    } else {
                        mine.extend(theirs.iter().copied());
                    }
                }
            }
            _ => self.exact = None,
        }
        for (mine, theirs) in self.registers.iter_mut().zip(other.registers.iter()) {
            *mine = (*mine).max(*theirs);
        }
    }

    pub fn distinct(&self) -> (u64, bool) {
        if let Some(exact) = &self.exact {
            return (exact.len() as u64, true);
        }
        let m = NDV_REGISTERS as f64;
        let sum: f64 = self.registers.iter().map(|r| 2.0f64.powi(-i32::from(*r))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let mut estimate = alpha * m * m / sum;
        let zeros = self.registers.iter().filter(|r| **r == 0).count();
        if estimate <= 2.5 * m && zeros > 0 {
            estimate = m * (m / zeros as f64).ln();
        }
        (estimate.round() as u64, false)
    }
}

impl Default for NdvSketch {
    fn default() -> Self {
        Self::new()
    }
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    super::stable_hash(bytes)
}

#[cfg(test)]
#[path = "test/ndv.rs"]
mod tests;
