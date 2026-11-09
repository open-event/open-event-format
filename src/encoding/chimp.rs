use super::MAX_BLOCK_VALUES;
use super::constant::{CHIMP_LEADING_ZEROS, CHIMP_TRAILING_ZERO_THRESHOLD};
use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};

const NO_STORED_LEADING: u32 = u32::MAX;

pub(super) fn encode_chimp(values: &[f64], out: &mut Writer) {
    out.put_u32(values.len() as u32);
    let length_at = out.len();
    out.put_u32(0);
    let stream_start = out.len();
    let mut bits = BitWriter::new(out);
    let mut previous = 0u64;
    let mut stored_leading = NO_STORED_LEADING;
    for (index, value) in values.iter().enumerate() {
        let current = value.to_bits();
        if index == 0 {
            bits.put(current, 64);
            previous = current;
            continue;
        }
        let xor = previous ^ current;
        previous = current;
        if xor == 0 {
            bits.put(0b00, 2);
            stored_leading = NO_STORED_LEADING;
            continue;
        }
        let (code, leading) = round_leading_zeros(xor.leading_zeros());
        let trailing = xor.trailing_zeros();
        if trailing > CHIMP_TRAILING_ZERO_THRESHOLD {
            let centre = 64 - leading - trailing;
            bits.put(0b01, 2);
            bits.put(code, 3);
            bits.put(u64::from(centre), 6);
            bits.put(xor >> trailing, centre);
            stored_leading = NO_STORED_LEADING;
        } else if leading == stored_leading {
            bits.put(0b10, 2);
            bits.put(xor, 64 - leading);
        } else {
            bits.put(0b11, 2);
            bits.put(code, 3);
            bits.put(xor, 64 - leading);
            stored_leading = leading;
        }
    }
    bits.finish();
    let stream_len = out.len() - stream_start;
    out.patch_u32(length_at, stream_len as u32);
}

pub(super) fn decode_chimp(reader: &mut Reader<'_>) -> Result<Vec<f64>, FormatError> {
    decode_chimp_range(reader, 0, usize::MAX)
}

pub(super) fn decode_chimp_range(reader: &mut Reader<'_>, start: usize, end: usize) -> Result<Vec<f64>, FormatError> {
    let count = reader.u32("chimp value count")? as usize;
    let len = reader.u32("chimp stream length")? as usize;
    let stream = reader.take(len, "chimp stream")?;
    if count == 0 {
        if len != 0 {
            return Err(FormatError::Structural {
                rule: "an empty chimp block stores no stream",
            });
        }
        return Ok(Vec::new());
    }
    let needed_bits = (count as u64 - 1).saturating_mul(2).saturating_add(64);
    if count > MAX_BLOCK_VALUES || needed_bits > (len as u64).saturating_mul(8) {
        return Err(FormatError::Structural {
            rule: "chimp value count beyond what the stream can hold",
        });
    }
    let mut bits = BitReader { bit: 0, bytes: stream };
    let keep = start..end;
    let mut values = Vec::with_capacity(end.min(count).saturating_sub(start));
    let mut previous = bits.take(64)?;
    if keep.contains(&0) {
        values.push(f64::from_bits(previous));
    }
    let mut stored_leading = NO_STORED_LEADING;
    for index in 1..count {
        let xor = match bits.take(2)? {
            0b00 => {
                stored_leading = NO_STORED_LEADING;
                0
            }
            0b01 => {
                let leading = leading_zeros_for(bits.take(3)?);
                let centre = bits.take(6)? as u32;
                if centre == 0 || leading + centre > 64 {
                    return Err(FormatError::Structural {
                        rule: "chimp centre bits do not fit the value",
                    });
                }
                stored_leading = NO_STORED_LEADING;
                bits.take(centre)? << (64 - leading - centre)
            }
            0b10 => {
                if stored_leading == NO_STORED_LEADING {
                    return Err(FormatError::Structural {
                        rule: "chimp reuses a leading-zero count no value has set",
                    });
                }
                bits.take(64 - stored_leading)?
            }
            _ => {
                let leading = leading_zeros_for(bits.take(3)?);
                stored_leading = leading;
                bits.take(64 - leading)?
            }
        };
        previous ^= xor;
        if keep.contains(&index) {
            values.push(f64::from_bits(previous));
        }
    }
    if bits.bit.div_ceil(8) != stream.len() || bits.padding()? != 0 {
        return Err(FormatError::Structural {
            rule: "chimp stream continues past its last value",
        });
    }
    Ok(values)
}

fn round_leading_zeros(leading_zeros: u32) -> (u64, u32) {
    let code = CHIMP_LEADING_ZEROS
        .iter()
        .rposition(|&rounded| rounded <= leading_zeros)
        .unwrap_or(0);
    (code as u64, leading_zeros_for(code as u64))
}

fn leading_zeros_for(code: u64) -> u32 {
    CHIMP_LEADING_ZEROS.get(code as usize).copied().unwrap_or(0)
}

struct BitWriter<'a> {
    accumulator: u128,
    out: &'a mut Writer,
    pending: u32,
}

impl<'a> BitWriter<'a> {
    fn new(out: &'a mut Writer) -> Self {
        Self {
            accumulator: 0,
            out,
            pending: 0,
        }
    }

    fn put(&mut self, value: u64, width: u32) {
        if width == 0 {
            return;
        }
        let field = if width == 64 {
            value
        } else {
            value & ((1u64 << width) - 1)
        };
        self.accumulator = (self.accumulator << width) | u128::from(field);
        self.pending += width;
        while self.pending >= 8 {
            self.pending -= 8;
            self.out.put_u8((self.accumulator >> self.pending) as u8);
        }
        self.accumulator &= (1u128 << self.pending) - 1;
    }

    fn finish(self) {
        if self.pending > 0 {
            self.out.put_u8((self.accumulator << (8 - self.pending)) as u8);
        }
    }
}

struct BitReader<'a> {
    bit: usize,
    bytes: &'a [u8],
}

impl BitReader<'_> {
    fn take(&mut self, width: u32) -> Result<u64, FormatError> {
        if width == 0 {
            return Ok(0);
        }
        let end = self.bit + width as usize;
        let window = self
            .bytes
            .get(self.bit / 8..end.div_ceil(8))
            .ok_or(FormatError::Truncated { what: "chimp stream" })?;
        let gathered = window
            .iter()
            .fold(0u128, |gathered, byte| (gathered << 8) | u128::from(*byte));
        let unused_tail = window.len() * 8 - (self.bit % 8) - width as usize;
        self.bit = end;
        Ok(((gathered >> unused_tail) & ((1u128 << width) - 1)) as u64)
    }

    fn padding(&self) -> Result<u8, FormatError> {
        if self.bit.is_multiple_of(8) {
            return Ok(0);
        }
        let last = self
            .bytes
            .get(self.bit / 8)
            .ok_or(FormatError::Truncated { what: "chimp stream" })?;
        Ok(last & (0xFF >> (self.bit % 8)))
    }
}

#[cfg(test)]
#[path = "test/chimp.rs"]
mod tests;
