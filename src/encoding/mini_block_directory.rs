use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};

pub struct MiniBlockDirectory {
    block_starts: Vec<usize>,
    body_end: usize,
    body_start: usize,
    plain_capacity_hint: usize,
    plain_len: usize,
}

impl MiniBlockDirectory {
    pub fn block_count(&self) -> usize {
        self.block_starts.len()
    }

    pub fn body_start(&self) -> usize {
        self.body_start
    }

    pub fn plain_capacity_hint(&self) -> usize {
        self.plain_capacity_hint
    }

    pub fn plain_len(&self) -> usize {
        self.plain_len
    }

    pub fn byte_range(&self, index: usize) -> Result<(usize, usize), FormatError> {
        let start = *self.block_starts.get(index).ok_or(FormatError::RefOutOfRange {
            what: "mini-block directory index",
        })?;
        let end = self.block_starts.get(index + 1).copied().unwrap_or(self.body_end);
        Ok((start, end - start))
    }

    pub fn ranges(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.ranges_from(0)
    }

    pub fn ranges_from(&self, first: usize) -> impl Iterator<Item = (usize, usize)> + '_ {
        let starts = self.block_starts.get(first..).unwrap_or(&[]);
        let ends = starts.iter().skip(1).copied().chain(std::iter::once(self.body_end));
        starts.iter().zip(ends).map(|(&start, end)| (start, end - start))
    }
}

pub fn encode(plain_len: usize, block_lens: &[usize]) -> Vec<u8> {
    let mut out = Writer::with_capacity(8 + block_lens.len() * 4);
    out.put_u32(block_lens.len() as u32);
    out.put_u32(plain_len as u32);
    for len in block_lens {
        out.put_u32(*len as u32);
    }
    out.into_bytes()
}

pub fn decode(bytes: &[u8]) -> Result<MiniBlockDirectory, FormatError> {
    let mut reader = Reader::new(bytes);
    let block_count = reader.u32("mini-block count")? as usize;
    let plain_len = reader.u32("mini-block plain length")? as usize;
    let plain_capacity_hint = reader.capacity_hint(plain_len, 1);
    let mut block_starts = Vec::with_capacity(reader.capacity_hint(block_count, 4));
    let len_table = reader.take(block_count.saturating_mul(4), "mini-block compressed length")?;
    let body_start = reader.position();
    let mut offset = body_start;
    for len in len_table.as_chunks::<4>().0 {
        block_starts.push(offset);
        let len = u32::from_le_bytes(*len) as usize;
        offset = offset.checked_add(len).ok_or(FormatError::Structural {
            rule: "mini-block compressed lengths overflow the address space",
        })?;
    }
    Ok(MiniBlockDirectory {
        block_starts,
        body_end: offset,
        body_start,
        plain_capacity_hint,
        plain_len,
    })
}

#[cfg(test)]
#[path = "test/mini_block_directory.rs"]
mod tests;
