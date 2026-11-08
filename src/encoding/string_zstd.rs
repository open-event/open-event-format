//! The [`Compression::StringZstd`] trailing stage: a string block relaid for an entropy coder, then compressed whole
//! with Zstandard — what a Parquet column chunk of repeated identifiers or free text stores, adapted to OE's block
//! layouts.
//!
//! A classic string body is laid out for compressed-form kernels, not for Zstandard: its four-byte offset tables are
//! a run of ever-growing integers no match finder can shorten, and a dictionary's FastLanes-transposed codes scatter
//! each code's bits across words. Relaying replaces each offset table with a zero byte after every value (the values'
//! own text then carries their boundaries, which Zstandard matches along with them) and the transposed codes with one
//! byte plane per eight bits of code width. Undoing the stage inflates the relaid body and lays it back out exactly
//! as the classic body it came from, so every reader path past [`super::remove_trailing`] sees the transform's
//! ordinary bytes and decodes, filters and point-reads them as it always has.
//!
//! Stored form: the classic body's byte length (`u32`), a relayout tag (`u8`), then the relaid body as an ordinary
//! [`Compression::Zstd3`] stored block (its own `u32` length prefix and one Zstandard frame).
//!
//! | tag | classic body | relaid body |
//! |-----|--------------|-------------|
//! | 0 | [`Transform::RawString`] | null stream, present count, each value followed by `0x00` |
//! | 1 | [`Transform::DictionaryString`] with plain entries | null stream, entry count, each entry followed by `0x00`, code count, code width, `ceil(width / 8)` byte planes of the codes (least significant first) |
//!
//! A body whose values (or entries) contain a zero byte is not relaid; the encoder stores it under plain
//! [`Compression::Zstd3`] instead. A file holding any such block declares the `STRING_ZSTD_BLOCKS` required feature.

use super::decompressor::Decompressor;
use super::{Compression, NullStream, SideStream, Transform, bitpack, bitunpack, compress_zstd_framed, read_offsets};
use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};

/// Relayout tag of a [`Transform::RawString`] body.
const TAG_RAW: u8 = 0;

/// Relayout tag of a local [`Transform::DictionaryString`] body with plain entries.
const TAG_DICTIONARY: u8 = 1;

thread_local! {
    /// One inflated relaid body per thread, reused block after block instead of a fresh allocation per inflate.
    static RELAID_SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Ceiling on the classic body length a stored block may claim, matching the plain Zstandard stage's: the decode
/// reserves this many bytes up front, so a forged claim must not force an arbitrary allocation.
const MAX_CLASSIC_BYTES: usize = 1 << 28;

/// Where `body`'s null stream ends: the relaid and classic layouts both open with it, byte for byte.
fn null_stream_end(body: &[u8]) -> Result<usize, FormatError> {
    let mut reader = Reader::new(body);
    NullStream::read(&mut reader)?;
    Ok(reader.position())
}

/// Appends `values`, each followed by a zero byte, or returns `false` (leaving `out` part-written) when one contains a
/// zero byte, which would end it early.
fn put_terminated<'a>(values: impl Iterator<Item = &'a [u8]>, out: &mut Writer) -> bool {
    for value in values {
        if value.contains(&0) {
            return false;
        }
        out.put_slice(value);
        out.put_u8(0);
    }
    true
}

/// The values of a classic offset table plus arena, in order. Malformed tables yield an error.
fn arena_values<'a>(offsets: &[usize], arena: &'a [u8]) -> Result<Vec<&'a [u8]>, FormatError> {
    offsets
        .windows(2)
        .map(|pair| match pair {
            [start, end] => arena.get(*start..*end).ok_or(FormatError::Structural {
                rule: "string offsets out of the arena",
            }),
            _ => Err(FormatError::Structural {
                rule: "string offsets out of the arena",
            }),
        })
        .collect()
}

/// The relaid form of a classic `transform` body, or `None` when the body has no relayout (a dictionary with FSST or
/// file-scope entries, any other transform, a value containing a zero byte, or a body that does not parse).
fn relay(transform: Transform, side: SideStream, classic: &[u8]) -> Option<(u8, Vec<u8>)> {
    let nulls_end = null_stream_end(classic).ok()?;
    let mut reader = Reader::new(classic);
    reader.take(nulls_end, "null stream").ok()?;
    let mut out = Writer::with_capacity(classic.len());
    out.put_slice(classic.get(..nulls_end)?);
    match (transform, side) {
        (Transform::RawString, SideStream::None) => {
            let count = reader.u32("raw present count").ok()? as usize;
            let offsets = read_offsets(&mut reader, count).ok()?;
            let arena = reader.take(offsets.last().copied().unwrap_or(0), "raw arena").ok()?;
            if reader.remaining() != 0 {
                return None;
            }
            out.put_u32(count as u32);
            put_terminated(arena_values(&offsets, arena).ok()?.into_iter(), &mut out).then_some(())?;
            Some((TAG_RAW, out.into_bytes()))
        }
        (Transform::DictionaryString, SideStream::None) => {
            let dict_count = reader.u32("dictionary count").ok()? as usize;
            let offsets = read_offsets(&mut reader, dict_count).ok()?;
            let entries = reader
                .take(offsets.last().copied().unwrap_or(0), "dictionary data")
                .ok()?;
            let codes_start = reader.position();
            let codes = bitunpack(&mut reader).ok()?;
            if reader.remaining() != 0 {
                return None;
            }
            let width = *classic.get(codes_start + 4)?;
            out.put_u32(dict_count as u32);
            put_terminated(arena_values(&offsets, entries).ok()?.into_iter(), &mut out).then_some(())?;
            out.put_u32(codes.len() as u32);
            out.put_u8(width);
            for plane in 0..usize::from(width).div_ceil(8) {
                for code in &codes {
                    out.put_u8((code >> (plane * 8)) as u8);
                }
            }
            Some((TAG_DICTIONARY, out.into_bytes()))
        }
        _ => None,
    }
}

/// Reads `count` zero-terminated values off `reader` and writes their classic offset table, then their arena, in one
/// pass: each terminator is found with `memchr`, its offset patched into the table reserved up front, and the value's
/// bytes appended behind it.
fn put_unterminated(reader: &mut Reader<'_>, count: usize, out: &mut Writer) -> Result<(), FormatError> {
    let rest = reader.take(reader.remaining(), "terminated values")?;
    // Every value costs at least its terminator, so a count beyond the bytes left is refused before reserving for it.
    if count > rest.len() {
        return Err(FormatError::Structural {
            rule: "relaid string value without its terminator",
        });
    }
    let table = out.len();
    out.reserve_bytes(count.saturating_add(1).saturating_mul(4));
    let mut position = 0usize;
    let mut offset = 0u32;
    for index in 0..count {
        let tail = rest.get(position..).unwrap_or_default();
        let len = memchr::memchr(0, tail).ok_or(FormatError::Structural {
            rule: "relaid string value without its terminator",
        })?;
        offset = u32::try_from(len)
            .ok()
            .and_then(|len| offset.checked_add(len))
            .ok_or(FormatError::Structural {
                rule: "string arena offsets exceed the u32 range the format stores",
            })?;
        out.patch_u32(table + (index + 1) * 4, offset);
        out.put_slice(tail.get(..len).unwrap_or_default());
        position += len + 1;
    }
    // Whatever follows the values belongs to the caller: hand it back by moving the reader just past them.
    *reader = Reader::new(rest.get(position..).unwrap_or_default());
    Ok(())
}

/// Lays a relaid body back out as its classic body.
fn unrelay(tag: u8, relaid: &[u8], classic_len: usize, plain: &mut Vec<u8>) -> Result<(), FormatError> {
    let nulls_end = null_stream_end(relaid)?;
    let mut reader = Reader::new(relaid);
    let nulls = reader.take(nulls_end, "null stream")?;
    let mut buffer = std::mem::take(plain);
    buffer.clear();
    buffer.reserve(classic_len);
    let mut out = Writer::from_vec(buffer);
    out.put_slice(nulls);
    match tag {
        TAG_RAW => {
            let count = reader.u32("raw present count")? as usize;
            out.put_u32(count as u32);
            put_unterminated(&mut reader, count, &mut out)?;
        }
        TAG_DICTIONARY => {
            let dict_count = reader.u32("dictionary count")? as usize;
            out.put_u32(dict_count as u32);
            put_unterminated(&mut reader, dict_count, &mut out)?;
            let count = reader.u32("dictionary code count")? as usize;
            let width = reader.u8("dictionary code width")?;
            if width > 64 {
                return Err(FormatError::Structural {
                    rule: "bit width beyond 64",
                });
            }
            let planes = usize::from(width).div_ceil(8);
            let bytes = reader.take(count.saturating_mul(planes), "dictionary code planes")?;
            let mut codes = vec![0u64; count];
            for plane in 0..planes {
                let plane_bytes = bytes.get(plane * count..(plane + 1) * count).unwrap_or_default();
                for (code, &byte) in codes.iter_mut().zip(plane_bytes) {
                    *code |= u64::from(byte) << (plane * 8);
                }
            }
            if width < 64 && codes.iter().any(|&code| code >> width != 0) {
                return Err(FormatError::Structural {
                    rule: "relaid dictionary code wider than its width",
                });
            }
            bitpack(&codes, u32::from(width), &mut out);
        }
        _ => {
            return Err(FormatError::Structural {
                rule: "unknown string relayout tag",
            });
        }
    }
    if reader.remaining() != 0 {
        return Err(FormatError::Structural {
            rule: "relaid string body has trailing bytes",
        });
    }
    *plain = out.into_bytes();
    if plain.len() != classic_len {
        return Err(FormatError::Structural {
            rule: "relaid string body disagrees with its classic length",
        });
    }
    Ok(())
}

/// The [`Compression::StringZstd`] stored form of a classic `transform` body, compressed at Zstandard `level`, or
/// `None` when the body has no relayout or Zstandard failed. The relayout is undone and compared against `classic`
/// before it is kept, so a stored block always restores the exact body it was made from.
pub(crate) fn compress(transform: Transform, side: SideStream, classic: &[u8], level: i32) -> Option<Vec<u8>> {
    let (tag, relaid) = relay(transform, side, classic)?;
    let mut restored = Vec::new();
    unrelay(tag, &relaid, classic.len(), &mut restored).ok()?;
    if restored != classic {
        return None;
    }
    let framed = compress_zstd_framed(&relaid, level);
    if framed.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(5 + framed.len());
    out.extend_from_slice(&u32::try_from(classic.len()).ok()?.to_le_bytes());
    out.push(tag);
    out.extend_from_slice(&framed);
    Some(out)
}

/// Undoes a [`Compression::StringZstd`] stage into `plain` (emptied first): `inflate` undoes the inner
/// [`Compression::Zstd3`] stage, then the relaid body is laid back out as the classic body.
pub(crate) fn decompress_into(
    bytes: &[u8],
    plain: &mut Vec<u8>,
    inflate: &dyn Decompressor,
) -> Result<(), FormatError> {
    let mut reader = Reader::new(bytes);
    let classic_len = reader.u32("string zstd classic length")? as usize;
    if classic_len > MAX_CLASSIC_BYTES {
        return Err(FormatError::Structural {
            rule: "string zstd classic length exceeds the per-block ceiling",
        });
    }
    let tag = reader.u8("string zstd relayout tag")?;
    let inner = reader.take(reader.remaining(), "string zstd body")?;
    RELAID_SCRATCH.with(|cell| {
        let mut relaid = cell.take();
        let inflated = inflate.decompress_into(Compression::Zstd3, inner, &mut relaid);
        let restored = inflated.and_then(|()| unrelay(tag, &relaid, classic_len, plain));
        if relaid.capacity() <= MAX_CLASSIC_BYTES / 16 {
            cell.replace(relaid);
        }
        restored
    })
}

#[cfg(test)]
#[path = "test/string_zstd.rs"]
mod tests;
