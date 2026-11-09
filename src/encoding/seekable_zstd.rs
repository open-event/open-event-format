use crate::error::FormatError;
use std::borrow::Cow;
use std::cell::RefCell;
use zeekstd::{BytesWrapper, SeekTable};
use zstd::bulk::{Compressor, Decompressor};

pub const FRAME_BYTES: u32 = 64 * 1024;

const LEVEL: i32 = 3;

const MAX_RANGE_BYTES: u64 = 1 << 28;

thread_local! {
    static DECODER: RefCell<Decompressor<'static>> = RefCell::new(Decompressor::default());

    static SPARE_FRAME: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };

    #[cfg(test)]
    static FRAMES_INFLATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_frames_inflated() -> usize {
    FRAMES_INFLATED.with(|count| count.replace(0))
}

fn failed(rule: &'static str) -> FormatError {
    FormatError::Structural { rule }
}

fn inflate_exact(compressed: &[u8], expected: u64, plain: &mut Vec<u8>) -> Result<(), FormatError> {
    if expected > MAX_RANGE_BYTES {
        return Err(failed(
            "seekable zstd range is inverted or exceeds the per-call ceiling",
        ));
    }
    plain.clear();
    plain.reserve_exact(expected as usize);
    let written = DECODER
        .with(|slot| slot.borrow_mut().decompress_to_buffer(compressed, plain))
        .map_err(|_| failed("seekable zstd frame failed to decompress"))?;
    if written as u64 != expected {
        return Err(failed(
            "seekable zstd range decompressed short of its seek table's length",
        ));
    }
    Ok(())
}

fn decompress_frame(stored: &[u8], table: &SeekTable, index: u32, plain: &mut Vec<u8>) -> Result<(), FormatError> {
    let outside = || failed("seekable zstd frame index is outside the seek table");
    let start = table.frame_start_comp(index).map_err(|_| outside())?;
    let end = table.frame_end_comp(index).map_err(|_| outside())?;
    let expected = table.frame_size_decomp(index).map_err(|_| outside())?;
    let compressed = usize::try_from(start)
        .ok()
        .zip(usize::try_from(end).ok())
        .and_then(|(start, end)| stored.get(start..end))
        .ok_or_else(|| failed("seekable zstd frame runs past the stored bytes"))?;
    #[cfg(test)]
    FRAMES_INFLATED.with(|count| count.set(count.get() + 1));
    inflate_exact(compressed, expected, plain)
}

pub fn compress(bytes: &[u8]) -> Option<Vec<u8>> {
    compress_with_breaks(bytes, &[])
}

pub fn compress_with_breaks(bytes: &[u8], breaks: &[usize]) -> Option<Vec<u8>> {
    compress_with_breaks_keeping(bytes, breaks, EVERY_FRAME_COMPRESSED)
}

pub const EVERY_FRAME_COMPRESSED: (usize, usize) = (1, 0);

pub fn compress_with_breaks_keeping(bytes: &[u8], breaks: &[usize], bar: (usize, usize)) -> Option<Vec<u8>> {
    compress_with_breaks_by(bytes, breaks, |_| bar)
}

pub fn compress_with_breaks_by(
    bytes: &[u8],
    breaks: &[usize],
    bar_at: impl Fn(usize) -> (usize, usize),
) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut table = SeekTable::new();
    let mut compressor = Compressor::new(LEVEL).ok()?;
    let mut scratch = Vec::with_capacity(zstd::zstd_safe::compress_bound(FRAME_BYTES as usize));
    let mut consumed = 0;
    for end in breaks.iter().copied().chain([bytes.len()]) {
        if end <= consumed || end > bytes.len() {
            continue;
        }
        for (index, frame) in bytes.get(consumed..end)?.chunks(FRAME_BYTES as usize).enumerate() {
            scratch.clear();
            compressor.compress_to_buffer(frame, &mut scratch).ok()?;
            let stored_start = out.len();
            let bar = bar_at(consumed + index * FRAME_BYTES as usize);
            if scratch.len().saturating_mul(bar.1) <= frame.len().saturating_mul(bar.0) {
                out.extend_from_slice(&scratch);
            } else {
                push_raw_frame(frame, &mut out);
            }
            table
                .log_frame(
                    u32::try_from(out.len() - stored_start).ok()?,
                    u32::try_from(frame.len()).ok()?,
                )
                .ok()?;
        }
        consumed = end;
    }
    let mut serializer = table.into_serializer();
    let table_start = out.len();
    out.resize(table_start + serializer.encoded_len(), 0);
    let written = serializer.write_into(out.get_mut(table_start..)?);
    out.truncate(table_start + written);
    Some(out)
}

fn push_raw_frame(plain: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&ZSTD_FRAME_MAGIC.to_le_bytes());
    out.push(0b1010_0000);
    out.extend_from_slice(&(plain.len() as u32).to_le_bytes());
    let mut blocks = plain.chunks(MAX_RAW_BLOCK_BYTES).peekable();
    if blocks.peek().is_none() {
        out.extend_from_slice(&[1, 0, 0]);
    }
    while let Some(block) = blocks.next() {
        let last = u32::from(blocks.peek().is_none());
        let header = last | ((block.len() as u32) << 3);
        out.extend_from_slice(&header.to_le_bytes()[..3]);
        out.extend_from_slice(block);
    }
}

const ZSTD_FRAME_MAGIC: u32 = 0xFD2F_B528;

const MAX_RAW_BLOCK_BYTES: usize = 128 * 1024;

pub fn seek_table(stored: &[u8]) -> Result<SeekTable, FormatError> {
    SeekTable::from_seekable(&mut BytesWrapper::new(stored))
        .map_err(|_| failed("seekable zstd seek table is unreadable"))
}

pub fn decompress_range(stored: &[u8], table: &SeekTable, start: u64, end: u64) -> Result<Vec<u8>, FormatError> {
    let len = end
        .checked_sub(start)
        .filter(|len| *len <= MAX_RANGE_BYTES)
        .ok_or_else(|| failed("seekable zstd range is inverted or exceeds the per-call ceiling"))?;
    if end > table.size_decomp() {
        return Err(failed("seekable zstd range is out of the seek table's bounds"));
    }
    let mut plain = Vec::with_capacity(len as usize);
    if len == 0 {
        return Ok(plain);
    }
    let mut frame = Vec::new();
    for index in table.frame_index_decomp(start)..=table.frame_index_decomp(end - 1) {
        let frame_start = table
            .frame_start_decomp(index)
            .map_err(|_| failed("seekable zstd frame index is outside the seek table"))?;
        decompress_frame(stored, table, index, &mut frame)?;
        let from = start.saturating_sub(frame_start) as usize;
        let to = ((end - frame_start) as usize).min(frame.len());
        plain.extend_from_slice(
            frame
                .get(from..to.max(from))
                .ok_or_else(|| failed("seekable zstd frame is shorter than its seek table entry"))?,
        );
    }
    if plain.len() != len as usize {
        return Err(failed(
            "seekable zstd range decompressed short of its seek table's length",
        ));
    }
    Ok(plain)
}

pub fn decompress_all(stored: &[u8]) -> Result<Vec<u8>, FormatError> {
    let mut plain = Vec::new();
    decompress_all_into(stored, &mut plain)?;
    Ok(plain)
}

pub fn decompress_all_into(stored: &[u8], plain: &mut Vec<u8>) -> Result<(), FormatError> {
    let table = seek_table(stored)?;
    inflate_exact(stored, table.size_decomp(), plain)
}

pub fn decompress_prefix_into(
    stored: &[u8],
    plain: &mut Vec<u8>,
    mut needed: impl FnMut(&[u8]) -> Option<usize>,
) -> Result<(), FormatError> {
    let table = seek_table(stored)?;
    let total = table.size_decomp();
    if total > MAX_RANGE_BYTES {
        return Err(failed(
            "seekable zstd range is inverted or exceeds the per-call ceiling",
        ));
    }
    plain.clear();
    if table.num_frames() == 0 {
        return inflate_exact(stored, total, plain);
    }
    plain.reserve_exact(total as usize);
    let frames = table.num_frames();
    let mut next = 0;
    while next < frames {
        inflate_frames_onto(stored, &table, next, next, plain)?;
        next += 1;
        let Some(want) = needed(plain) else { continue };
        let want = want as u64;
        if want > total {
            return Err(failed("seekable zstd prefix runs past the stored plaintext"));
        }
        if want > plain.len() as u64 {
            inflate_frames_onto(stored, &table, next, table.frame_index_decomp(want - 1), plain)?;
        }
        break;
    }
    Ok(())
}

fn inflate_frames_onto(
    stored: &[u8],
    table: &SeekTable,
    first: u32,
    last: u32,
    plain: &mut Vec<u8>,
) -> Result<(), FormatError> {
    let outside = || failed("seekable zstd frame index is outside the seek table");
    let start = table.frame_start_comp(first).map_err(|_| outside())?;
    let end = table.frame_end_comp(last).map_err(|_| outside())?;
    let expected_end = table.frame_end_decomp(last).map_err(|_| outside())?;
    let expected_start = table.frame_start_decomp(first).map_err(|_| outside())?;
    if plain.len() as u64 != expected_start {
        return Err(failed("seekable zstd frames inflated out of order"));
    }
    let compressed = usize::try_from(start)
        .ok()
        .zip(usize::try_from(end).ok())
        .and_then(|(start, end)| stored.get(start..end))
        .ok_or_else(|| failed("seekable zstd frame runs past the stored bytes"))?;
    let expected = (expected_end - expected_start) as usize;
    plain.reserve(expected);
    #[cfg(test)]
    FRAMES_INFLATED.with(|count| count.set(count.get() + (last - first + 1) as usize));
    let mut cursor = std::io::Cursor::new(std::mem::take(plain));
    cursor.set_position(expected_start);
    let result = DECODER.with(|slot| slot.borrow_mut().decompress_to_buffer(compressed, &mut cursor));
    *plain = cursor.into_inner();
    let written = result.map_err(|_| failed("seekable zstd frame failed to decompress"))?;
    if written != expected || plain.len() as u64 != expected_end {
        return Err(failed(
            "seekable zstd range decompressed short of its seek table's length",
        ));
    }
    Ok(())
}

pub struct Window<'a> {
    frames: Vec<(u32, Vec<u8>)>,
    released: u32,
    spare: Vec<u8>,
    stored: &'a [u8],
    table: SeekTable,
}

impl<'a> Window<'a> {
    pub fn open(stored: &'a [u8]) -> Result<Self, FormatError> {
        let table = seek_table(stored)?;
        let spare = SPARE_FRAME.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
        Ok(Self {
            frames: Vec::new(),
            released: 0,
            spare,
            stored,
            table,
        })
    }

    pub fn plain_len(&self) -> usize {
        self.table.size_decomp() as usize
    }

    pub fn first_frame_len(&self) -> usize {
        self.table.frame_end_decomp(0).map_or(0, |end| end as usize)
    }

    #[cfg(test)]
    pub(crate) fn decompressed_frames(&self) -> usize {
        self.frames.len()
    }

    #[cfg(test)]
    pub(crate) fn decompressed_frame_indexes(&self) -> Vec<u32> {
        self.frames.iter().map(|(index, _)| *index).collect()
    }

    pub fn read(&mut self, start: usize, len: usize) -> Result<Vec<u8>, FormatError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let (first, last, end) = self.span(start, len)?;
        let mut plain = Vec::with_capacity(len);
        for index in first..=last {
            let frame_start = self.frame_start(index)?;
            let frame = self.frame(index)?;
            let from = start.saturating_sub(frame_start);
            let to = (end - frame_start).min(frame.len());
            plain.extend_from_slice(
                frame
                    .get(from..to.max(from))
                    .ok_or_else(|| failed("seekable zstd frame is shorter than its seek table entry"))?,
            );
        }
        if plain.len() != len {
            return Err(failed("seekable zstd window range decompressed short of its length"));
        }
        Ok(plain)
    }

    pub fn read_forward(&mut self, start: usize, len: usize) -> Result<Cow<'_, [u8]>, FormatError> {
        if len == 0 {
            return Ok(Cow::Borrowed(&[]));
        }
        let (first, last, end) = self.span(start, len)?;
        if self.released < first {
            self.release(self.released, first);
            self.released = first;
        }
        if first != last {
            return self.read(start, len).map(Cow::Owned);
        }
        let frame_start = self.frame_start(first)?;
        let frame = self.frame(first)?;
        frame
            .get(start - frame_start..end - frame_start)
            .map(Cow::Borrowed)
            .ok_or_else(|| failed("seekable zstd frame is shorter than its seek table entry"))
    }

    fn span(&self, start: usize, len: usize) -> Result<(u32, u32, usize), FormatError> {
        let end = start
            .checked_add(len)
            .filter(|end| *end <= self.plain_len())
            .ok_or(FormatError::Truncated {
                what: "seekable zstd window range",
            })?;
        let first = self.table.frame_index_decomp(start as u64);
        let last = self.table.frame_index_decomp(end as u64 - 1);
        Ok((first, last, end))
    }

    fn frame_start(&self, index: u32) -> Result<usize, FormatError> {
        self.table
            .frame_start_decomp(index)
            .map(|start| start as usize)
            .map_err(|_| failed("seekable zstd frame index is outside the seek table"))
    }

    fn frame(&mut self, index: u32) -> Result<&[u8], FormatError> {
        let position = match self.frames.binary_search_by_key(&index, |(cached, _)| *cached) {
            Ok(position) => position,
            Err(position) => {
                let mut buffer = std::mem::take(&mut self.spare);
                decompress_frame(self.stored, &self.table, index, &mut buffer)?;
                self.frames.insert(position, (index, buffer));
                position
            }
        };
        self.frames
            .get(position)
            .map(|(_, buffer)| buffer.as_slice())
            .ok_or_else(|| failed("seekable zstd frame index is outside the seek table"))
    }

    fn release(&mut self, from: u32, to: u32) {
        let low = self.frames.partition_point(|(index, _)| *index < from);
        let high = self.frames.partition_point(|(index, _)| *index < to);
        for (_, buffer) in self.frames.drain(low..high) {
            if self.spare.capacity() == 0 {
                self.spare = buffer;
            }
        }
    }
}

impl Drop for Window<'_> {
    fn drop(&mut self) {
        let spare = std::mem::take(&mut self.spare);
        let buffer = if spare.capacity() > 0 {
            spare
        } else {
            self.frames
                .drain(..)
                .next()
                .map(|(_, buffer)| buffer)
                .unwrap_or_default()
        };
        if buffer.capacity() <= FRAME_BYTES as usize {
            SPARE_FRAME.with(|slot| *slot.borrow_mut() = buffer);
        }
    }
}

#[cfg(test)]
#[path = "test/seekable_zstd.rs"]
mod tests;
