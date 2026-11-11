use super::decompressor::Decompressor;
use super::{
    Compression, FASTLANES_LANES, FASTLANES_VECTOR, MAX_BLOCK_VALUES, NullStream, SideStream, Transform, bitpack,
    bitunpack, read_offsets,
};
use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};

const TAG_RAW: u8 = 0;

const TAG_DICTIONARY: u8 = 1;

pub(crate) const TAG_LZ4: u8 = 0x80;

thread_local! {
    static RELAID_SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

const MAX_CLASSIC_BYTES: usize = 1 << 28;

fn null_stream_end(body: &[u8]) -> Result<usize, FormatError> {
    let mut reader = Reader::new(body);
    NullStream::read(&mut reader)?;
    Ok(reader.position())
}

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

fn put_unterminated(reader: &mut Reader<'_>, count: usize, out: &mut Writer) -> Result<(), FormatError> {
    let rest = reader.take(reader.remaining(), "terminated values")?;
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
    *reader = Reader::new(rest.get(position..).unwrap_or_default());
    Ok(())
}

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
            if width == 0 && count > MAX_BLOCK_VALUES {
                return Err(FormatError::Structural {
                    rule: "zero-width bitpack count beyond the block bound",
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

pub(crate) fn compress(transform: Transform, side: SideStream, classic: &[u8]) -> Option<Vec<u8>> {
    let (tag, relaid) = relay(transform, side, classic)?;
    let mut restored = Vec::new();
    unrelay(tag, &relaid, classic.len(), &mut restored).ok()?;
    if restored != classic {
        return None;
    }
    let framed = lz4_flex::compress_prepend_size(&relaid);
    let mut out = Vec::with_capacity(5 + framed.len());
    out.extend_from_slice(&u32::try_from(classic.len()).ok()?.to_le_bytes());
    out.push(tag | TAG_LZ4);
    out.extend_from_slice(&framed);
    Some(out)
}

pub(crate) fn decompress_into(
    bytes: &[u8],
    plain: &mut Vec<u8>,
    inflate: &dyn Decompressor,
) -> Result<(), FormatError> {
    let (classic_len, tag, inner) = read_header(bytes)?;
    with_inflated(tag, inner, inflate, |relaid| {
        unrelay(tag & !TAG_LZ4, relaid, classic_len, plain)
    })
}

fn read_header(bytes: &[u8]) -> Result<(usize, u8, &[u8]), FormatError> {
    let mut reader = Reader::new(bytes);
    let classic_len = reader.u32("string zstd classic length")? as usize;
    if classic_len > MAX_CLASSIC_BYTES {
        return Err(FormatError::Structural {
            rule: "string zstd classic length exceeds the per-block ceiling",
        });
    }
    let tag = reader.u8("string zstd relayout tag")?;
    let inner = reader.take(reader.remaining(), "string zstd body")?;
    Ok((classic_len, tag, inner))
}

fn with_inflated<R>(
    tag: u8,
    inner: &[u8],
    inflate: &dyn Decompressor,
    consume: impl FnOnce(&[u8]) -> Result<R, FormatError>,
) -> Result<R, FormatError> {
    RELAID_SCRATCH.with(|cell| {
        let mut relaid = cell.take();
        let consumed = inflate
            .decompress_into(inner_codec(tag), inner, &mut relaid)
            .and_then(|()| consume(&relaid));
        if relaid.capacity() <= MAX_CLASSIC_BYTES / 16 {
            cell.replace(relaid);
        }
        consumed
    })
}

fn inner_codec(tag: u8) -> Compression {
    if tag & TAG_LZ4 != 0 {
        Compression::Lz4
    } else {
        Compression::Zstd3
    }
}

pub(crate) enum Relaid<'a> {
    Raw {
        nulls: NullStream<'a>,
        count: usize,
        values: &'a str,
    },
    Dictionary {
        nulls: NullStream<'a>,
        entries: Vec<&'a str>,
        codes: Vec<u64>,
    },
}

pub(crate) struct TerminatedValues<'a> {
    rest: &'a str,
}

impl<'a> TerminatedValues<'a> {
    pub(crate) fn new(values: &'a str) -> Self {
        Self { rest: values }
    }

    pub(crate) fn next_value(&mut self) -> Result<&'a str, FormatError> {
        let len = memchr::memchr(0, self.rest.as_bytes()).ok_or(FormatError::Structural {
            rule: "relaid string value without its terminator",
        })?;
        let value = self.rest.get(..len).unwrap_or_default();
        self.rest = self.rest.get(len + 1..).unwrap_or_default();
        Ok(value)
    }

    pub(crate) fn finish(self) -> Result<(), FormatError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(FormatError::Structural {
                rule: "relaid string body has trailing bytes",
            })
        }
    }
}

fn terminated_end(bytes: &[u8], count: usize) -> Result<usize, FormatError> {
    let missing = FormatError::Structural {
        rule: "relaid string value without its terminator",
    };
    if count > bytes.len() {
        return Err(missing);
    }
    let mut end = 0usize;
    let mut zeros = memchr::memchr_iter(0, bytes);
    for _ in 0..count {
        end = zeros.next().ok_or(missing.clone())? + 1;
    }
    Ok(end)
}

fn classic_strings_len(count: usize, text: usize) -> Option<usize> {
    count.checked_add(2)?.checked_mul(4)?.checked_add(text)
}

fn gather_planes(bytes: &[u8], count: usize, planes: usize) -> Vec<u64> {
    let mut codes: Vec<u64> = match bytes.get(..count) {
        Some(low) if planes > 0 => low.iter().map(|&byte| u64::from(byte)).collect(),
        _ => vec![0; count],
    };
    for plane in 1..planes {
        let plane_bytes = bytes.get(plane * count..(plane + 1) * count).unwrap_or_default();
        for (code, &byte) in codes.iter_mut().zip(plane_bytes) {
            *code |= u64::from(byte) << (plane * 8);
        }
    }
    codes
}

fn parse(tag: u8, relaid: &[u8], classic_len: usize) -> Result<Relaid<'_>, FormatError> {
    let mut reader = Reader::new(relaid);
    let nulls = NullStream::read(&mut reader)?;
    let nulls_end = reader.position();
    let (parsed, body_len) = match tag {
        TAG_RAW => {
            let count = reader.u32("raw present count")? as usize;
            let rest = reader.take(reader.remaining(), "terminated values")?;
            let values = simdutf8::basic::from_utf8(rest).map_err(|_| FormatError::InvalidUtf8 {
                what: "raw string value",
            })?;
            let text = rest.len().checked_sub(count).ok_or(FormatError::Structural {
                rule: "relaid string value without its terminator",
            })?;
            (Relaid::Raw { nulls, count, values }, classic_strings_len(count, text))
        }
        TAG_DICTIONARY => {
            let dict_count = reader.u32("dictionary count")? as usize;
            let rest = reader.take(reader.remaining(), "terminated values")?;
            let end = terminated_end(rest, dict_count)?;
            let region = simdutf8::basic::from_utf8(rest.get(..end).unwrap_or_default()).map_err(|_| {
                FormatError::InvalidUtf8 {
                    what: "dictionary entry",
                }
            })?;
            let mut entries = Vec::with_capacity(dict_count);
            let mut walk = TerminatedValues::new(region);
            for _ in 0..dict_count {
                entries.push(walk.next_value()?);
            }
            walk.finish()?;
            let mut reader = Reader::new(rest.get(end..).unwrap_or_default());
            let count = reader.u32("dictionary code count")? as usize;
            let width = reader.u8("dictionary code width")?;
            if width > 64 {
                return Err(FormatError::Structural {
                    rule: "bit width beyond 64",
                });
            }
            if width == 0 && count > MAX_BLOCK_VALUES {
                return Err(FormatError::Structural {
                    rule: "zero-width bitpack count beyond the block bound",
                });
            }
            let planes = usize::from(width).div_ceil(8);
            let bytes = reader.take(count.saturating_mul(planes), "dictionary code planes")?;
            if reader.remaining() != 0 {
                return Err(FormatError::Structural {
                    rule: "relaid string body has trailing bytes",
                });
            }
            let codes = gather_planes(bytes, count, planes);
            if width < 64 && codes.iter().any(|&code| code >> width != 0) {
                return Err(FormatError::Structural {
                    rule: "relaid dictionary code wider than its width",
                });
            }
            let packed = if width == 0 {
                Some(0)
            } else {
                count
                    .div_ceil(FASTLANES_VECTOR)
                    .checked_mul(FASTLANES_LANES * usize::from(width) * 8)
            };
            let len = classic_strings_len(dict_count, end - dict_count)
                .and_then(|len| len.checked_add(5)?.checked_add(packed?));
            (Relaid::Dictionary { nulls, entries, codes }, len)
        }
        _ => {
            return Err(FormatError::Structural {
                rule: "unknown string relayout tag",
            });
        }
    };
    if body_len.and_then(|len| len.checked_add(nulls_end)) != Some(classic_len) {
        return Err(FormatError::Structural {
            rule: "relaid string body disagrees with its classic length",
        });
    }
    Ok(parsed)
}

pub(crate) fn with_relaid<R>(
    transform: Transform,
    side: SideStream,
    bytes: &[u8],
    decode: impl FnOnce(Relaid<'_>) -> Result<R, FormatError>,
) -> Result<Option<R>, FormatError> {
    let (classic_len, tag, inner) = read_header(bytes)?;
    let expected = match (transform, side) {
        (Transform::RawString, SideStream::None) => TAG_RAW,
        (Transform::DictionaryString, SideStream::None) => TAG_DICTIONARY,
        _ => return Ok(None),
    };
    if tag & !TAG_LZ4 != expected {
        return Ok(None);
    }
    let inflate = super::decompressor::active_decompressor();
    with_inflated(tag, inner, inflate.as_ref(), |relaid| {
        decode(parse(tag & !TAG_LZ4, relaid, classic_len)?)
    })
    .map(Some)
}

#[cfg(test)]
#[path = "test/string_zstd.rs"]
mod tests;
