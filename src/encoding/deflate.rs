use super::mini_block_directory;
use crate::error::FormatError;
use crate::file::bytes::slice;
use miniz_oxide::deflate::core::{
    CompressorOxide, TDEFLFlush, TDEFLStatus, compress_to_output, create_comp_flags_from_zip_params,
};
use miniz_oxide::inflate::TINFLStatus;
use miniz_oxide::inflate::core::inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF;
use miniz_oxide::inflate::core::{DecompressorOxide, decompress as inflate_granule};
use std::cell::RefCell;

pub const GRANULE_BYTES: usize = 4096;

const DEFLATE_LEVEL: u8 = 6;

const MAX_GRANULE_UNCOMPRESSED_BYTES: usize = GRANULE_BYTES + 65536;

const DEFLATE_FLAGS: u32 = create_comp_flags_from_zip_params(DEFLATE_LEVEL as i32, 0, 0);

thread_local! {
    static COMPRESSOR: RefCell<CompressorOxide> = RefCell::new(CompressorOxide::new(DEFLATE_FLAGS));
    static DECOMPRESSOR: RefCell<DecompressorOxide> = RefCell::new(DecompressorOxide::new());
}

pub fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(bytes.len());
    let mut granule_lens = Vec::with_capacity(bytes.len().div_ceil(GRANULE_BYTES));

    COMPRESSOR.with(|cell| {
        let mut compressor = cell.borrow_mut();
        for granule in bytes.chunks(GRANULE_BYTES) {
            compressor.reset();
            let before = body.len();
            deflate_granule(&mut compressor, granule, |chunk| body.extend_from_slice(chunk));
            granule_lens.push(body.len() - before);
        }
    });

    let mut out = mini_block_directory::encode(bytes.len(), &granule_lens);
    out.extend_from_slice(&body);
    out
}

pub(crate) fn compressed_len(bytes: &[u8]) -> usize {
    let mut len = 8 + 4 * bytes.len().div_ceil(GRANULE_BYTES);
    COMPRESSOR.with(|cell| {
        let mut compressor = cell.borrow_mut();
        for granule in bytes.chunks(GRANULE_BYTES) {
            compressor.reset();
            deflate_granule(&mut compressor, granule, |chunk| len += chunk.len());
        }
    });
    len
}

fn deflate_granule(compressor: &mut CompressorOxide, granule: &[u8], mut sink: impl FnMut(&[u8])) {
    let mut remaining = granule;
    loop {
        let (status, bytes_in) = compress_to_output(compressor, remaining, TDEFLFlush::Finish, |chunk| {
            sink(chunk);
            true
        });
        remaining = remaining.get(bytes_in..).unwrap_or(&[]);
        match status {
            TDEFLStatus::Done => break,
            TDEFLStatus::Okay => continue,
            _ => unreachable!("miniz_oxide deflate failed to compress well-formed input"),
        }
    }
}

pub fn plain_len(bytes: &[u8]) -> Result<usize, FormatError> {
    Ok(mini_block_directory::decode(bytes)?.plain_len())
}

pub fn granule_byte_range(bytes: &[u8], granule: usize) -> Result<(usize, usize), FormatError> {
    mini_block_directory::decode(bytes)?.byte_range(granule)
}

pub fn decompress(bytes: &[u8]) -> Result<Vec<u8>, FormatError> {
    let mut plain = Vec::new();
    decompress_into(bytes, &mut plain)?;
    Ok(plain)
}

pub(crate) fn decompress_into(bytes: &[u8], plain: &mut Vec<u8>) -> Result<(), FormatError> {
    plain.clear();
    let directory = mini_block_directory::decode(bytes)?;
    let plain_len = directory.plain_len();
    if plain_len > directory.block_count().saturating_mul(MAX_GRANULE_UNCOMPRESSED_BYTES) {
        return Err(FormatError::Structural {
            rule: "deflate plain length exceeds what its granule count can hold",
        });
    }
    plain.reserve(directory.plain_capacity_hint());
    for (granule, (start, len)) in directory.ranges().enumerate() {
        inflate_expected(
            slice(bytes, start, len, "deflate granule")?,
            expected_granule_len(plain_len, granule),
            plain,
        )?;
    }
    if plain.len() != plain_len {
        return Err(FormatError::Structural {
            rule: "deflate page decompressed to a length other than the directory declares",
        });
    }
    Ok(())
}

pub fn decompress_granule(bytes: &[u8], granule: usize) -> Result<Vec<u8>, FormatError> {
    let directory = mini_block_directory::decode(bytes)?;
    let (start, len) = directory.byte_range(granule)?;
    let mut plain = Vec::new();
    inflate_expected(
        slice(bytes, start, len, "deflate granule")?,
        expected_granule_len(directory.plain_len(), granule),
        &mut plain,
    )?;
    Ok(plain)
}

pub struct Page<'a> {
    bytes: &'a [u8],
    directory: mini_block_directory::MiniBlockDirectory,
}

impl<'a> Page<'a> {
    pub fn open(bytes: &'a [u8]) -> Result<Self, FormatError> {
        Ok(Self {
            bytes,
            directory: mini_block_directory::decode(bytes)?,
        })
    }

    pub fn plain_len(&self) -> usize {
        self.directory.plain_len()
    }

    pub fn granule_count(&self) -> usize {
        self.directory.block_count()
    }

    pub fn decompress_granules(&self, first: usize, last: usize) -> Result<Vec<u8>, FormatError> {
        if first > last || last >= self.directory.block_count() {
            return Err(FormatError::RefOutOfRange {
                what: "mini-block directory index",
            });
        }
        let plain_len = self.directory.plain_len();
        let capacity = ((last - first + 1) * GRANULE_BYTES).min(self.directory.plain_capacity_hint());
        let mut plain = Vec::with_capacity(capacity);
        for (offset, (start, len)) in self.directory.ranges_from(first).take(last - first + 1).enumerate() {
            inflate_expected(
                slice(self.bytes, start, len, "deflate granule")?,
                expected_granule_len(plain_len, first + offset),
                &mut plain,
            )?;
        }
        Ok(plain)
    }
}

fn expected_granule_len(plain_len: usize, granule: usize) -> usize {
    GRANULE_BYTES.min(plain_len.saturating_sub(granule.saturating_mul(GRANULE_BYTES)))
}

fn inflate_expected(compressed: &[u8], expected: usize, out: &mut Vec<u8>) -> Result<(), FormatError> {
    let before = out.len();
    inflate_into(compressed, expected.max(1), MAX_GRANULE_UNCOMPRESSED_BYTES, out)?;
    if out.len() - before != expected {
        out.truncate(before);
        return Err(FormatError::Structural {
            rule: "deflate granule length disagrees with the directory's plain length",
        });
    }
    Ok(())
}

fn inflate_into(
    compressed: &[u8],
    initial_guess: usize,
    hard_limit: usize,
    out: &mut Vec<u8>,
) -> Result<(), FormatError> {
    let start = out.len();
    let mut cap = initial_guess.clamp(1, hard_limit);
    out.resize(start + cap, 0);
    let mut remaining = compressed;

    let result = DECOMPRESSOR.with(|cell| -> Result<usize, FormatError> {
        let mut decompressor = cell.borrow_mut();
        decompressor.init();
        let mut out_pos = start;
        loop {
            let (status, in_consumed, out_consumed) = inflate_granule(
                &mut decompressor,
                remaining,
                out,
                out_pos,
                TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF,
            );
            out_pos += out_consumed;
            match status {
                TINFLStatus::Done => return Ok(out_pos),
                TINFLStatus::HasMoreOutput if cap < hard_limit => {
                    remaining = remaining.get(in_consumed..).unwrap_or(&[]);
                    cap = cap.saturating_mul(2).min(hard_limit);
                    out.resize(start + cap, 0);
                }
                _ => {
                    return Err(FormatError::Structural {
                        rule: "deflate granule failed to decompress",
                    });
                }
            }
        }
    });

    match result {
        Ok(out_pos) => {
            out.truncate(out_pos);
            Ok(())
        }
        Err(err) => {
            out.truncate(start);
            Err(err)
        }
    }
}

#[cfg(test)]
#[path = "test/deflate.rs"]
mod tests;
