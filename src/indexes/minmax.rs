use super::Exactness;
use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NumericMinMax {
    pub max: Option<i128>,
    pub min: Option<i128>,
    pub null_count: u32,
    pub row_count: u32,
}

impl NumericMinMax {
    pub fn build(values: &[Option<i128>]) -> NumericMinMax {
        let mut min = i128::MAX;
        let mut max = i128::MIN;
        let mut any_value = false;
        let mut null_count: u32 = 0;
        for value in values {
            match value {
                Some(value) => {
                    min = min.min(*value);
                    max = max.max(*value);
                    any_value = true;
                }
                None => null_count = null_count.saturating_add(1),
            }
        }
        NumericMinMax {
            max: any_value.then_some(max),
            min: any_value.then_some(min),
            null_count,
            row_count: values.len() as u32,
        }
    }

    pub fn can_prune_range(&self, lo: i128, hi: i128) -> bool {
        if lo > hi {
            return true;
        }
        match (self.min, self.max) {
            (Some(min), Some(max)) => hi < min || lo > max,
            _ => true,
        }
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::Exact
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::new();
        out.put_u32(self.row_count);
        out.put_u32(self.null_count);
        put_opt_i128(&mut out, self.min);
        put_opt_i128(&mut out, self.max);
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<NumericMinMax, FormatError> {
        let mut reader = Reader::new(bytes);
        let row_count = reader.u32("numeric minmax row count")?;
        let null_count = reader.u32("numeric minmax null count")?;
        let min = read_opt_i128(&mut reader)?;
        let max = read_opt_i128(&mut reader)?;
        if null_count > row_count {
            return Err(FormatError::Structural {
                rule: "numeric minmax null count exceeds row count",
            });
        }
        match (min, max) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(FormatError::Structural {
                    rule: "numeric minmax bounds must be both present or both absent",
                });
            }
            (Some(min), Some(max)) if min > max => {
                return Err(FormatError::Structural {
                    rule: "numeric minmax min exceeds max",
                });
            }
            (None, None) if row_count > null_count => {
                return Err(FormatError::Structural {
                    rule: "numeric minmax absent bounds require an all-null block",
                });
            }
            (Some(_), Some(_)) if row_count == null_count => {
                return Err(FormatError::Structural {
                    rule: "numeric minmax bounds over a block with no non-null rows",
                });
            }
            _ => {}
        }
        Ok(NumericMinMax {
            max,
            min,
            null_count,
            row_count,
        })
    }

    pub fn decode_for_granule(bytes: &[u8], granule_row_count: u32) -> Result<NumericMinMax, FormatError> {
        let zone_map = Self::decode(bytes)?;
        if zone_map.row_count != granule_row_count {
            return Err(FormatError::Structural {
                rule: "numeric minmax row count disagrees with its granule",
            });
        }
        Ok(zone_map)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StringMinMax {
    pub is_truncated: bool,
    pub max: Option<Vec<u8>>,
    pub min: Vec<u8>,
}

impl StringMinMax {
    pub fn build(values: &[Option<&[u8]>], max_stored_len: usize) -> StringMinMax {
        let mut true_min: Option<&[u8]> = None;
        let mut true_max: Option<&[u8]> = None;
        for value in values.iter().flatten() {
            true_min = Some(match true_min {
                Some(current) if current <= *value => current,
                _ => *value,
            });
            true_max = Some(match true_max {
                Some(current) if current >= *value => current,
                _ => *value,
            });
        }
        let (Some(true_min), Some(true_max)) = (true_min, true_max) else {
            return StringMinMax {
                is_truncated: false,
                max: Some(Vec::new()),
                min: Vec::new(),
            };
        };

        let min_fits = true_min.len() <= max_stored_len;
        let max_fits = true_max.len() <= max_stored_len;

        let min = if min_fits {
            true_min.to_vec()
        } else {
            truncate_down(true_min, max_stored_len)
        };
        let max = if max_fits {
            Some(true_max.to_vec())
        } else {
            truncate_up(true_max, max_stored_len)
        };
        StringMinMax {
            is_truncated: !(min_fits && max_fits),
            max,
            min,
        }
    }

    pub fn can_prune(&self, predicate_lo: &[u8], predicate_hi: &[u8]) -> bool {
        if predicate_lo > predicate_hi {
            return true;
        }
        if predicate_hi < self.min.as_slice() {
            return true;
        }
        match &self.max {
            Some(max) => predicate_lo > max.as_slice(),
            None => false,
        }
    }

    pub fn exactness(&self) -> Exactness {
        if self.is_truncated {
            Exactness::InexactNoFalseNegative
        } else {
            Exactness::Exact
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::new();
        out.put_u8(u8::from(self.is_truncated));
        out.put_u32(self.min.len() as u32);
        out.put_slice(&self.min);
        match &self.max {
            Some(max) => {
                out.put_u8(1);
                out.put_u32(max.len() as u32);
                out.put_slice(max);
            }
            None => out.put_u8(0),
        }
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<StringMinMax, FormatError> {
        let mut reader = Reader::new(bytes);
        let is_truncated = reader.u8("string minmax truncated flag")? != 0;
        let min_len = reader.u32("string minmax min length")? as usize;
        let min = reader.take(min_len, "string minmax min")?.to_vec();
        let max = if reader.u8("string minmax max tag")? == 1 {
            let max_len = reader.u32("string minmax max length")? as usize;
            Some(reader.take(max_len, "string minmax max")?.to_vec())
        } else {
            None
        };
        match &max {
            Some(max) if min > *max => {
                return Err(FormatError::Structural {
                    rule: "string minmax min exceeds max",
                });
            }
            None if !is_truncated => {
                return Err(FormatError::Structural {
                    rule: "string minmax exact entry must have a finite max",
                });
            }
            _ => {}
        }
        Ok(StringMinMax { is_truncated, max, min })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceRange {
    pub first_epoch: u64,
    pub first_sequence: u64,
    pub last_epoch: u64,
    pub last_sequence: u64,
}

impl SequenceRange {
    pub fn build(positions: &[(u64, u64)]) -> Option<SequenceRange> {
        let mut first: Option<(u64, u64)> = None;
        let mut last: Option<(u64, u64)> = None;
        for &position in positions {
            first = Some(first.map_or(position, |current| current.min(position)));
            last = Some(last.map_or(position, |current| current.max(position)));
        }
        let (first, last) = (first?, last?);
        Some(SequenceRange {
            first_epoch: first.0,
            first_sequence: first.1,
            last_epoch: last.0,
            last_sequence: last.1,
        })
    }

    pub fn can_prune(&self, lo: (u64, u64), hi: (u64, u64)) -> bool {
        if lo > hi {
            return true;
        }
        let first = (self.first_epoch, self.first_sequence);
        let last = (self.last_epoch, self.last_sequence);
        hi < first || lo > last
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::Exact
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::new();
        out.put_u64(self.first_epoch);
        out.put_u64(self.first_sequence);
        out.put_u64(self.last_epoch);
        out.put_u64(self.last_sequence);
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<SequenceRange, FormatError> {
        let mut reader = Reader::new(bytes);
        let range = SequenceRange {
            first_epoch: reader.u64("sequence range first epoch")?,
            first_sequence: reader.u64("sequence range first sequence")?,
            last_epoch: reader.u64("sequence range last epoch")?,
            last_sequence: reader.u64("sequence range last sequence")?,
        };
        if (range.first_epoch, range.first_sequence) > (range.last_epoch, range.last_sequence) {
            return Err(FormatError::Structural {
                rule: "sequence range first exceeds last",
            });
        }
        Ok(range)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    pub max_physical: i64,
    pub min_physical: i64,
}

impl TimeRange {
    pub fn build(values: &[i64]) -> Option<TimeRange> {
        let (&first, rest) = values.split_first()?;
        let (min, max) = rest
            .iter()
            .fold((first, first), |(min, max), &value| (min.min(value), max.max(value)));
        Some(TimeRange {
            max_physical: max,
            min_physical: min,
        })
    }

    pub fn can_prune(&self, lo: i64, hi: i64) -> bool {
        if lo > hi {
            return true;
        }
        hi < self.min_physical || lo > self.max_physical
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::Exact
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::new();
        out.put_i64(self.min_physical);
        out.put_i64(self.max_physical);
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<TimeRange, FormatError> {
        let mut reader = Reader::new(bytes);
        let min_physical = reader.i64("time range min")?;
        let max_physical = reader.i64("time range max")?;
        if min_physical > max_physical {
            return Err(FormatError::Structural {
                rule: "time range min exceeds max",
            });
        }
        Ok(TimeRange {
            max_physical,
            min_physical,
        })
    }
}

fn truncate_down(value: &[u8], max_stored_len: usize) -> Vec<u8> {
    value.get(..max_stored_len).unwrap_or(value).to_vec()
}

fn truncate_up(value: &[u8], max_stored_len: usize) -> Option<Vec<u8>> {
    let mut prefix: Vec<u8> = value.get(..max_stored_len).unwrap_or(value).to_vec();
    while let Some(last) = prefix.last().copied() {
        if last < 0xFF {
            let len = prefix.len();
            if let Some(slot) = prefix.get_mut(len - 1) {
                *slot = last + 1;
            }
            return Some(prefix);
        }
        prefix.pop();
    }
    None
}

fn put_opt_i128(out: &mut Writer, value: Option<i128>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            out.put_u128(value as u128);
        }
        None => out.put_u8(0),
    }
}

fn read_opt_i128(reader: &mut Reader<'_>) -> Result<Option<i128>, FormatError> {
    match reader.u8("option tag")? {
        0 => Ok(None),
        1 => Ok(Some(reader.u128("i128")? as i128)),
        _ => Err(FormatError::Structural {
            rule: "numeric minmax option tag must be 0 or 1",
        }),
    }
}

#[cfg(test)]
#[path = "test/minmax.rs"]
mod tests;
