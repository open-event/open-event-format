use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer, slice};
use ahash::{AHashMap, AHashSet};
use arrow_array::StringViewArray;
use arrow_array::builder::{BooleanBufferBuilder, make_view};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use constant::{
    ALP_CONVERT_MAGIC, ALP_EXACT_CONVERT_MAX_WIDTH, ALP_MAX_EXCEPTION_SAMPLE_DIVISOR, ALP_MAX_SCALED_MAGNITUDE,
    ALP_RD_CODE_MISS, ALP_RD_MAX_DICT_LEN, ALP_RD_MAX_RIGHT_WIDTH, ALP_RD_MIN_RIGHT_WIDTH, ALP_VECTOR_ESCAPE_SENTINEL,
    BYTE_STREAM_SPLIT_TILE, DECODE_OPTIMIZED_THRESHOLD_DEN, DECODE_OPTIMIZED_THRESHOLD_NUM,
    DICTIONARY_FSST_MAX_VALUE_LEN, DICTIONARY_MAX_CARDINALITY_DIVISOR, DICTIONARY_TRIAL_MAX_DISTINCT_DEN,
    DICTIONARY_TRIAL_MAX_DISTINCT_NUM, FASTLANES_SPARSE_MAX_VALUES, FSST_LOSSY_PHT_HASH_MULTIPLIER,
    FSST_LOSSY_PHT_HASH_SHIFT, FSST_MAX_AVERAGE_VALUE_LEN, FSST_MAX_SYMBOL_COUNT, FSST_MIN_SAMPLE_COUNT,
    FSST_PER_FRAME_BAR_MIN_BLOCK_BYTES, FSST_TRAILING_MAX_RATIO_DEN, FSST_TRAILING_MAX_RATIO_NUM,
    FSST_VALUE_KEYS_MAX_PLAINTEXT_DIVISOR, MAX_RLE_EXPANSION_VALUES, PREFIX_KEY_BYTES, PREFIX_SUM_CHUNK,
    REPLAY_TRIP_DEN, REPLAY_TRIP_NUM, SIZE_OPTIMIZED_SAVINGS_DIVISOR, STRING_ZSTD_MIN_SAVINGS_DEN,
    STRING_ZSTD_MIN_SAVINGS_NUM, TRANSFORM_SAMPLE_RUNS, TRANSFORM_SAMPLE_SIZE, ZSTD_LENGTH_PREFIX_BYTES,
};
use multiversion::multiversion;
use std::borrow::Cow;
use std::cell::RefCell;
use zstd::{bulk::Compressor as ZstdCompressor, zstd_safe::compress_bound};

use chimp::{decode_chimp, decode_chimp_range, encode_chimp};
pub use lazy::{
    DictionaryCodes, decode_dictionary_codes, filter_block_by_selection, fsst_uncompressed_len, take_block_rows,
    take_block_rows_shared,
};
pub(crate) use presence::{PresenceRank, count_present_before, count_set_bits, present_position};
use scratch::{with_arena_buffer, with_unpack_buffer};
use sidecar::{PrefixKey, StringFingerprint};
pub use string_column::StringColumn;

mod chimp;
mod constant;
pub mod decompressor;
pub mod deflate;
pub mod descriptor;
pub mod global_dict;
pub(crate) mod lazy;
pub mod mini_block_directory;
pub mod predicate;
mod presence;
mod scratch;
pub mod seekable_zstd;
pub mod sidecar;
mod string_column;
mod string_zstd;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeStrategy {
    DecodeOptimized,
    NoTrailing,
    SizeOptimized,
}

impl CascadeStrategy {
    pub fn for_fresh_publication() -> Self {
        Self::DecodeOptimized
    }

    pub fn for_rewrite_or_compaction() -> Self {
        Self::SizeOptimized
    }
}

pub(crate) const MAX_BLOCK_VALUES: usize = 1 << 24;

pub const MAX_CASCADE_DEPTH: usize = 3;

pub fn cascade_level_fits(depth: usize) -> bool {
    depth < MAX_CASCADE_DEPTH
}

pub fn cascade_depth(pipeline: PipelineId) -> Result<usize, FormatError> {
    let mut depth = 1;
    if pipeline.side_stream()? != SideStream::None {
        depth += 1;
    }
    if pipeline.compression()? != Compression::None {
        depth += 1;
    }
    Ok(depth)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideStream {
    FileScopeDictionary = 3,
    ForBitpack = 1,
    FsstDictionaryValues = 2,
    FsstValueKeys = 5,
    None = 0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transform {
    Alp = 6,
    AlpRd = 13,
    ByteStreamSplit = 12,
    Chimp = 14,
    Decimal128 = 7,
    DeltaBitpack = 2,
    DictionaryString = 8,
    ForBitpack = 1,
    FsstString = 9,
    FsstPackedString = 15,
    OccurredResidual = 16,
    PlainF64 = 5,
    PlainU128 = 4,
    PlainU64 = 0,
    RawString = 10,
    Rle = 3,
}

impl Transform {
    pub fn is_per_value_addressable(self) -> bool {
        matches!(
            self,
            Transform::Alp
                | Transform::DictionaryString
                | Transform::FsstString
                | Transform::FsstPackedString
                | Transform::RawString
        )
    }

    pub fn supports_framed_range(self) -> bool {
        matches!(
            self,
            Transform::PlainU64 | Transform::PlainF64 | Transform::ByteStreamSplit
        )
    }

    pub fn keeps_range_access_under_framing(self) -> bool {
        self.supports_framed_range() || matches!(self, Transform::FsstString | Transform::RawString)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Deflate = 4,
    Lz4 = 1,
    None = 0,
    SeekableZstd = 5,
    StringZstd = 6,
    Zstd1 = 2,
    Zstd3 = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Decimal = 4,
    F64 = 3,
    I64 = 1,
    String = 5,
    U128 = 2,
    U64 = 0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineId(pub u32);

impl PipelineId {
    pub fn new(transform: Transform, compression: Compression, kind: ValueKind) -> Self {
        Self((transform as u32) | ((compression as u32) << 8) | ((kind as u32) << 16))
    }

    pub fn with_side_stream(self, side: SideStream) -> Self {
        Self((self.0 & 0x00FF_FFFF) | ((side as u32) << 24))
    }

    pub fn with_transform(self, transform: Transform) -> Self {
        Self((self.0 & !0xFF) | transform as u32)
    }

    pub fn side_stream(self) -> Result<SideStream, FormatError> {
        Ok(match (self.0 >> 24) & 0xFF {
            0 => SideStream::None,
            1 => SideStream::ForBitpack,
            2 => SideStream::FsstDictionaryValues,
            3 => SideStream::FileScopeDictionary,
            5 => SideStream::FsstValueKeys,
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown side stream in pipeline id",
                });
            }
        })
    }

    pub fn transform(self) -> Result<Transform, FormatError> {
        Ok(match self.0 & 0xFF {
            0 => Transform::PlainU64,
            1 => Transform::ForBitpack,
            2 => Transform::DeltaBitpack,
            3 => Transform::Rle,
            4 => Transform::PlainU128,
            5 => Transform::PlainF64,
            6 => Transform::Alp,
            7 => Transform::Decimal128,
            8 => Transform::DictionaryString,
            9 => Transform::FsstString,
            10 => Transform::RawString,
            12 => Transform::ByteStreamSplit,
            13 => Transform::AlpRd,
            14 => Transform::Chimp,
            15 => Transform::FsstPackedString,
            16 => Transform::OccurredResidual,
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown transform in pipeline id",
                });
            }
        })
    }

    pub fn compression(self) -> Result<Compression, FormatError> {
        Ok(match (self.0 >> 8) & 0xFF {
            0 => Compression::None,
            1 => Compression::Lz4,
            2 => Compression::Zstd1,
            3 => Compression::Zstd3,
            4 => Compression::Deflate,
            5 => Compression::SeekableZstd,
            6 => Compression::StringZstd,
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown compression in pipeline id",
                });
            }
        })
    }

    pub fn value_kind(self) -> Result<ValueKind, FormatError> {
        Ok(match (self.0 >> 16) & 0xFF {
            0 => ValueKind::U64,
            1 => ValueKind::I64,
            2 => ValueKind::U128,
            3 => ValueKind::F64,
            4 => ValueKind::Decimal,
            5 => ValueKind::String,
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown value kind in pipeline id",
                });
            }
        })
    }

    pub fn supports_byte_range_extraction(self) -> Result<bool, FormatError> {
        let transform = self.transform()?;
        let compression = self.compression()?;
        Ok(transform.is_per_value_addressable()
            || (transform.supports_framed_range() && compression == Compression::SeekableZstd)
            || (matches!(transform, Transform::PlainU64 | Transform::PlainF64) && compression == Compression::Deflate))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Decimal { values: Vec<i128>, scale: u8 },
    F64(Vec<f64>),
    I64(Vec<i64>),
    Strings(StringColumn),
    U128(Vec<u128>),
    U64(Vec<u64>),
}

impl ColumnData {
    pub fn row_count(&self) -> usize {
        match self {
            ColumnData::U64(values) => values.len(),
            ColumnData::I64(values) => values.len(),
            ColumnData::U128(values) => values.len(),
            ColumnData::F64(values) => values.len(),
            ColumnData::Decimal { values, .. } => values.len(),
            ColumnData::Strings(values) => values.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BlockStats {
    pub max_f64: Option<f64>,
    pub max_i128: Option<i128>,
    pub min_f64: Option<f64>,
    pub min_i128: Option<i128>,
    pub null_count: u32,
    pub row_count: u32,
}

#[derive(Debug, Clone)]
pub struct EncodedBlock {
    pub bytes: Vec<u8>,
    pub fsst: Option<FsstTable>,
    pub pipeline: PipelineId,
    pub stats: BlockStats,
    pub uncompressed_len: u64,
}

fn bits_needed(max: u64) -> u32 {
    64 - max.leading_zeros()
}

const FASTLANES_VECTOR: usize = 1024;

const FASTLANES_LANES: usize = 16;

const FASTLANES_ROWS: usize = 8;

const FASTLANES_ORDER: [usize; FASTLANES_ROWS] = [0, 4, 2, 6, 1, 5, 3, 7];

const fn fastlanes_words_per_lane(bits: usize) -> usize {
    bits
}

fn bitpack(values: &[u64], width: u32, out: &mut Writer) {
    out.put_u32(values.len() as u32);
    out.put_u8(width as u8);
    if width == 0 {
        return;
    }
    let bits = width as usize;
    let words_per_vector = FASTLANES_LANES * fastlanes_words_per_lane(bits);
    let vectors = values.len().div_ceil(FASTLANES_VECTOR);
    let value_mask = mask(width);
    let packed = out.reserve_bytes(vectors * words_per_vector * 8);
    let mut scratch = [0u64; FASTLANES_VECTOR];
    let mut padded = [0u64; FASTLANES_VECTOR];
    for (packed_vector, in_vector) in packed
        .chunks_exact_mut(words_per_vector * 8)
        .zip(values.chunks(FASTLANES_VECTOR))
    {
        let in_vector = if in_vector.len() == FASTLANES_VECTOR {
            in_vector
        } else {
            for (slot, value) in padded.iter_mut().zip(in_vector) {
                *slot = *value;
            }
            &padded
        };
        pack_vector_into(in_vector, &mut scratch, packed_vector, width, value_mask);
    }
}

fn bitpack_with(len: usize, width: u32, out: &mut Writer, lane: impl Fn(usize) -> u64) {
    out.put_u32(len as u32);
    out.put_u8(width as u8);
    if width == 0 {
        return;
    }
    let bits = width as usize;
    let words_per_vector = FASTLANES_LANES * fastlanes_words_per_lane(bits);
    let vectors = len.div_ceil(FASTLANES_VECTOR);
    let value_mask = mask(width);
    let packed = out.reserve_bytes(vectors * words_per_vector * 8);
    let mut buffer = [0u64; FASTLANES_VECTOR];
    let mut scratch = [0u64; FASTLANES_VECTOR];
    for (vector_index, packed_vector) in packed.chunks_exact_mut(words_per_vector * 8).enumerate() {
        let base = vector_index * FASTLANES_VECTOR;
        let this_len = FASTLANES_VECTOR.min(len.saturating_sub(base));
        for (offset, slot) in buffer.iter_mut().enumerate() {
            *slot = if offset < this_len { lane(base + offset) } else { 0 };
        }
        pack_vector_into(&buffer, &mut scratch, packed_vector, width, value_mask);
    }
}

fn pack_vector_into(
    in_vector: &[u64],
    scratch: &mut [u64; FASTLANES_VECTOR],
    packed_vector: &mut [u8],
    width: u32,
    value_mask: u64,
) {
    let words = scratch.get_mut(..packed_vector.len() / 8).unwrap_or(&mut []);
    words.fill(0);
    bitpack_vector(in_vector, words, width, value_mask);
    if cfg!(target_endian = "little") {
        packed_vector.copy_from_slice(bytemuck::cast_slice(words));
    } else {
        for (bytes, word) in packed_vector.chunks_exact_mut(8).zip(words.iter()) {
            bytes.copy_from_slice(&word.to_le_bytes());
        }
    }
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn bitpack_vector(in_vector: &[u64], packed_vector: &mut [u64], width: u32, value_mask: u64) {
    let bits = width as usize;
    for (sub_block, in_sub) in in_vector.chunks_exact(FASTLANES_ROWS * FASTLANES_LANES).enumerate() {
        let vl_block = FASTLANES_ORDER.get(sub_block).copied().unwrap_or(0);
        for (row, in_row) in in_sub.chunks_exact(FASTLANES_LANES).enumerate() {
            let bit = (vl_block * FASTLANES_ROWS + row) * bits;
            let word = bit / 64;
            let offset = (bit % 64) as u32;
            let spill = offset + width > 64;
            let rows_needed = if spill { 2 } else { 1 };
            let Some(word_rows) = packed_vector.get_mut(word * FASTLANES_LANES..(word + rows_needed) * FASTLANES_LANES)
            else {
                continue;
            };
            let (low_row, high_row) = word_rows.split_at_mut(FASTLANES_LANES);
            for (slot, value) in low_row.iter_mut().zip(in_row) {
                *slot |= (value & value_mask) << offset;
            }
            if spill {
                for (slot, value) in high_row.iter_mut().zip(in_row) {
                    *slot |= (value & value_mask) >> (64 - offset);
                }
            }
        }
    }
}

fn mask(bits: u32) -> u64 {
    if bits >= 64 { u64::MAX } else { (1u64 << bits) - 1 }
}

struct PackedStream<'a> {
    count: usize,
    packed: &'a [u8],
    width: u32,
}

fn read_packed_stream<'a>(reader: &mut Reader<'a>) -> Result<PackedStream<'a>, FormatError> {
    let count = reader.u32("bitpack count")? as usize;
    let width = u32::from(reader.u8("bitpack width")?);
    if width > 64 {
        return Err(FormatError::Structural {
            rule: "bit width beyond 64",
        });
    }
    if width == 0 {
        if count > MAX_BLOCK_VALUES {
            return Err(FormatError::Structural {
                rule: "zero-width bitpack count beyond the block bound",
            });
        }
        return Ok(PackedStream {
            count,
            packed: &[],
            width,
        });
    }
    let vectors = count.div_ceil(FASTLANES_VECTOR);
    let total_words = vectors * FASTLANES_LANES * fastlanes_words_per_lane(width as usize);
    let packed = reader.take(total_words.saturating_mul(8), "bitpack")?;
    Ok(PackedStream { count, packed, width })
}

impl PackedStream<'_> {
    fn padded_count(&self) -> usize {
        if self.width == 0 {
            self.count
        } else {
            self.count.div_ceil(FASTLANES_VECTOR) * FASTLANES_VECTOR
        }
    }

    fn unpack_into<T: Copy>(&self, out: &mut [T], lane: impl Fn(u64) -> T) {
        if self.width == 0 {
            out.fill(lane(0));
            return;
        }
        unpack_vectors(self.packed, self.width, out, lane);
    }

    fn for_each_vector(&self, mut consume: impl FnMut(&[u64])) {
        let mut buffer = [0u64; FASTLANES_VECTOR];
        let bytes_per_vector = FASTLANES_LANES * fastlanes_words_per_lane(self.width as usize) * 8;
        let mut packed_vectors = self.packed.chunks(bytes_per_vector.max(1));
        let mut remaining = self.count;
        while remaining > 0 {
            let rows = remaining.min(FASTLANES_VECTOR);
            if self.width != 0 {
                let Some(packed_vector) = packed_vectors.next() else {
                    return;
                };
                unpack_vectors(packed_vector, self.width, &mut buffer, |packed| packed);
            }
            consume(buffer.get(..rows).unwrap_or(&[]));
            remaining -= rows;
        }
    }
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn unpack_vectors<T: Copy>(packed: &[u8], width: u32, out: &mut [T], lane: impl Fn(u64) -> T) {
    let bits = width as usize;
    let bytes_per_vector = FASTLANES_LANES * fastlanes_words_per_lane(bits) * 8;
    let value_mask = mask(width);
    for (out_vector, packed_vector) in out
        .chunks_exact_mut(FASTLANES_VECTOR)
        .zip(packed.chunks_exact(bytes_per_vector))
    {
        for (sub_block, out_sub) in out_vector
            .chunks_exact_mut(FASTLANES_ROWS * FASTLANES_LANES)
            .enumerate()
        {
            let vl_block = FASTLANES_ORDER.get(sub_block).copied().unwrap_or(0);
            for (row, out_row) in out_sub.chunks_exact_mut(FASTLANES_LANES).enumerate() {
                let bit = (vl_block * FASTLANES_ROWS + row) * bits;
                let word = bit / 64;
                let offset = (bit % 64) as u32;
                let Some(low_row) = lane_row(packed_vector, word) else {
                    continue;
                };
                let low_words = lane_words(low_row);
                if offset + width <= 64 {
                    for (slot, low) in out_row.iter_mut().zip(low_words) {
                        *slot = lane((low >> offset) & value_mask);
                    }
                } else if let Some(high_row) = lane_row(packed_vector, word + 1) {
                    let high_words = lane_words(high_row);
                    for ((slot, low), high) in out_row.iter_mut().zip(low_words).zip(high_words) {
                        *slot = lane(((low >> offset) | (high << (64 - offset))) & value_mask);
                    }
                }
            }
        }
    }
}

fn lane_row(packed_vector: &[u8], word: usize) -> Option<&[u8]> {
    packed_vector.get(word * FASTLANES_LANES * 8..(word + 1) * FASTLANES_LANES * 8)
}

fn lane_words(row: &[u8]) -> [u64; FASTLANES_LANES] {
    let mut words = [0u64; FASTLANES_LANES];
    for (slot, chunk) in words.iter_mut().zip(row.chunks_exact(8)) {
        *slot = u64::from_le_bytes(chunk.try_into().unwrap_or([0; 8]));
    }
    words
}

fn bitunpack_into<T: Copy + Default>(reader: &mut Reader<'_>, lane: impl Fn(u64) -> T) -> Result<Vec<T>, FormatError> {
    let stream = read_packed_stream(reader)?;
    let mut values = vec![T::default(); stream.padded_count()];
    stream.unpack_into(&mut values, lane);
    values.truncate(stream.count);
    Ok(values)
}

fn bitunpack(reader: &mut Reader<'_>) -> Result<Vec<u64>, FormatError> {
    bitunpack_into(reader, |packed| packed)
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

#[inline]
fn alp_narrow_int_to_f64(packed: u64) -> f64 {
    let int = unzigzag(packed);
    f64::from_bits(ALP_CONVERT_MAGIC.to_bits().wrapping_add(int as u64)) - ALP_CONVERT_MAGIC
}

trait IntValue: Copy + Default + bytemuck::AnyBitPattern {
    fn from_bits(bits: u64) -> Self;

    fn from_code(code: u64) -> Self;

    fn to_bits(self) -> u64;
}

impl IntValue for i64 {
    fn from_bits(bits: u64) -> Self {
        bits as i64
    }

    fn from_code(code: u64) -> Self {
        unzigzag(code)
    }

    fn to_bits(self) -> u64 {
        self as u64
    }
}

impl IntValue for u64 {
    fn from_bits(bits: u64) -> Self {
        bits
    }

    fn from_code(code: u64) -> Self {
        code
    }

    fn to_bits(self) -> u64 {
        self
    }
}

fn encode_plain_u64(values: &[u64], out: &mut Writer) {
    out.put_u32(values.len() as u32);
    out.put_u64_slice(values);
}

fn decode_plain_u64<T: IntValue>(reader: &mut Reader<'_>) -> Result<Vec<T>, FormatError> {
    let count = reader.u32("plain count")? as usize;
    let mut values = reader.u64_vec(count, "plain value")?;
    for value in &mut values {
        *value = T::from_code(*value).to_bits();
    }
    Ok(bytemuck::cast_vec(values))
}

fn encode_for_bitpack(values: &[u64], out: &mut Writer) {
    let base = values.iter().copied().min().unwrap_or(0);
    let max_delta = values.iter().map(|v| v.wrapping_sub(base)).max().unwrap_or(0);
    out.put_u64(base);
    bitpack_with(values.len(), bits_needed(max_delta), out, |i| {
        values.get(i).map_or(0, |value| value.wrapping_sub(base))
    });
}

fn encode_for_bitpack_i64(values: &[i64], out: &mut Writer) {
    let zig_at = |i: usize| values.get(i).copied().map(zigzag).unwrap_or(0);
    let base = (0..values.len()).map(zig_at).min().unwrap_or(0);
    let max_delta = (0..values.len())
        .map(|i| zig_at(i).wrapping_sub(base))
        .max()
        .unwrap_or(0);
    out.put_u64(base);
    bitpack_with(values.len(), bits_needed(max_delta), out, |i| {
        zig_at(i).wrapping_sub(base)
    });
}

fn decode_for_bitpack<T: IntValue>(reader: &mut Reader<'_>) -> Result<Vec<T>, FormatError> {
    let base = reader.u64("for base")?;
    let stream = read_packed_stream(reader)?;
    if base.checked_add(mask(stream.width)).is_none() {
        let mut deltas = vec![0u64; stream.padded_count()];
        stream.unpack_into(&mut deltas, |packed| packed);
        deltas.truncate(stream.count);
        return deltas
            .into_iter()
            .map(|delta| {
                base.checked_add(delta)
                    .map(T::from_code)
                    .ok_or(FormatError::Structural {
                        rule: "frame-of-reference overflow",
                    })
            })
            .collect();
    }
    let mut values = vec![T::default(); stream.padded_count()];
    stream.unpack_into(&mut values, |packed| T::from_code(base.wrapping_add(packed)));
    values.truncate(stream.count);
    Ok(values)
}

fn encode_delta_bitpack(values: &[u64], out: &mut Writer) {
    let first = values.first().copied().unwrap_or(0);
    out.put_u64(first);
    let len = values.len().saturating_sub(1);
    let delta_at = |i: usize| match (values.get(i), values.get(i + 1)) {
        (Some(previous), Some(next)) => zigzag(next.wrapping_sub(*previous) as i64),
        _ => 0,
    };
    let max = (0..len).map(delta_at).max().unwrap_or(0);
    bitpack_with(len, bits_needed(max), out, delta_at);
}

fn encode_delta_bitpack_i64(values: &[i64], out: &mut Writer) {
    let zig_at = |i: usize| values.get(i).copied().map(zigzag).unwrap_or(0);
    out.put_u64(zig_at(0));
    let len = values.len().saturating_sub(1);
    let delta_at = |i: usize| zigzag(zig_at(i + 1).wrapping_sub(zig_at(i)) as i64);
    let max = (0..len).map(delta_at).max().unwrap_or(0);
    bitpack_with(len, bits_needed(max), out, delta_at);
}

fn decode_delta_bitpack<T: IntValue>(reader: &mut Reader<'_>) -> Result<Vec<T>, FormatError> {
    let first = reader.u64("delta first")?;
    let stream = read_packed_stream(reader)?;
    let mut values = vec![T::default(); stream.padded_count() + 1];
    if let Some((head, deltas)) = values.split_first_mut() {
        *head = T::from_code(first);
        stream.unpack_into(deltas, |packed| T::from_bits(unzigzag(packed) as u64));
    }
    values.truncate(stream.count + 1);
    if let Some(deltas) = values.get_mut(1..) {
        prefix_sum(deltas, first);
    }
    Ok(values)
}

fn take_delta_bitpack<T: IntValue>(body: &[u8], rows: &[usize]) -> Result<Vec<T>, FormatError> {
    let mut reader = Reader::new(body);
    let first = reader.u64("delta first")?;
    let stream = read_packed_stream(&mut reader)?;
    let mut out = Vec::with_capacity(rows.len());
    if !rows.is_sorted() {
        let values = decode_delta_bitpack::<T>(&mut Reader::new(body))?;
        return rows
            .iter()
            .map(|&row| {
                values
                    .get(row)
                    .copied()
                    .ok_or(FormatError::RefOutOfRange { what: "take row" })
            })
            .collect();
    }
    if rows.last().is_some_and(|&row| row > stream.count) {
        return Err(FormatError::RefOutOfRange { what: "take row" });
    }
    let mut wanted = rows.iter().copied().peekable();
    while wanted.next_if_eq(&0).is_some() {
        out.push(T::from_code(first));
    }
    let mut buffer = [0u64; FASTLANES_VECTOR];
    let bytes_per_vector = FASTLANES_LANES * fastlanes_words_per_lane(stream.width as usize) * 8;
    let mut carry = first;
    let mut start = 1usize;
    let mut vectors = stream.packed.chunks(bytes_per_vector.max(1));
    while let Some(&row) = wanted.peek() {
        if stream.width != 0 {
            let Some(packed) = vectors.next() else {
                return Err(FormatError::RefOutOfRange { what: "take row" });
            };
            unpack_vectors(packed, stream.width, &mut buffer, |code| unzigzag(code) as u64);
        }
        let end = start + FASTLANES_VECTOR;
        let mut position = start;
        let mut next = row;
        loop {
            if next >= end {
                break;
            }
            for delta in buffer.get(position - start..=next - start).unwrap_or(&[]) {
                carry = carry.wrapping_add(*delta);
            }
            out.push(T::from_code(carry));
            position = next + 1;
            wanted.next();
            match wanted.peek() {
                Some(&n) => next = n,
                None => return Ok(out),
            }
        }
        for delta in buffer.get(position - start..).unwrap_or(&[]) {
            carry = carry.wrapping_add(*delta);
        }
        start = end;
    }
    Ok(out)
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn prefix_sum<T: IntValue>(values: &mut [T], carry: u64) -> u64 {
    let mut carry = carry;
    let mut chunks = values.chunks_exact_mut(PREFIX_SUM_CHUNK);
    for chunk in &mut chunks {
        let mut sums = [0u64; PREFIX_SUM_CHUNK];
        let mut chunk_total = 0u64;
        for (sum, slot) in sums.iter_mut().zip(chunk.iter()) {
            chunk_total = chunk_total.wrapping_add(slot.to_bits());
            *sum = chunk_total;
        }
        for (slot, sum) in chunk.iter_mut().zip(sums) {
            *slot = T::from_code(carry.wrapping_add(sum));
        }
        carry = carry.wrapping_add(chunk_total);
    }
    for slot in chunks.into_remainder() {
        carry = carry.wrapping_add(slot.to_bits());
        *slot = T::from_code(carry);
    }
    carry
}

fn encode_rle(values: &[u64], out: &mut Writer) {
    encode_rle_iter(values.iter().copied(), out);
}

fn encode_rle_iter(values: impl Iterator<Item = u64> + Clone, out: &mut Writer) {
    let mut run_count = 0u32;
    let mut previous = None;
    for value in values.clone() {
        if previous != Some(value) {
            run_count += 1;
            previous = Some(value);
        }
    }
    out.put_u32(run_count);
    let mut current: Option<(u64, u32)> = None;
    for value in values {
        match current.as_mut() {
            Some((run_value, count)) if *run_value == value => *count += 1,
            _ => {
                if let Some((run_value, count)) = current {
                    out.put_u64(run_value);
                    out.put_u32(count);
                }
                current = Some((value, 1));
            }
        }
    }
    if let Some((run_value, count)) = current {
        out.put_u64(run_value);
        out.put_u32(count);
    }
}

fn decode_rle<T: IntValue>(reader: &mut Reader<'_>) -> Result<Vec<T>, FormatError> {
    let run_count = reader.u32("rle run count")? as usize;
    let mut values = Vec::new();
    for _ in 0..run_count {
        let value = reader.u64("rle value")?;
        let count = reader.u32("rle count")? as usize;
        if values.len() + count > MAX_RLE_EXPANSION_VALUES {
            return Err(FormatError::Structural {
                rule: "rle expansion too large",
            });
        }
        values.extend(std::iter::repeat_n(T::from_code(value), count));
    }
    Ok(values)
}

fn rle_size(values: &[u64]) -> usize {
    let mut runs = 0usize;
    let mut previous: Option<u64> = None;
    for value in values {
        if previous != Some(*value) {
            runs += 1;
            previous = Some(*value);
        }
    }
    4 + runs * 12
}

fn packed_size(packed_len: usize, block_len: usize, width: u32) -> usize {
    if width == 0 {
        return 0;
    }
    if block_len < FASTLANES_VECTOR {
        return FASTLANES_LANES * fastlanes_words_per_lane(width as usize) * 8;
    }
    (packed_len * width as usize).div_ceil(8)
}

fn for_size(values: &[u64], block_len: usize) -> usize {
    let base = values.iter().copied().min().unwrap_or(0);
    let max_delta = values.iter().map(|v| v.wrapping_sub(base)).max().unwrap_or(0);
    8 + 5 + packed_size(values.len(), block_len, bits_needed(max_delta))
}

fn delta_size(values: &[u64], block_len: usize) -> usize {
    let max = values
        .windows(2)
        .filter_map(|pair| match pair {
            [previous, value] => Some(zigzag(value.wrapping_sub(*previous) as i64)),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    8 + 5 + packed_size(values.len().saturating_sub(1), block_len, bits_needed(max))
}

fn transform_sample<T: Copy>(values: &[T]) -> Option<Vec<T>> {
    transform_sample_mapped(values, |value| value)
}

fn transform_sample_mapped<T: Copy, U>(values: &[T], map: impl Fn(T) -> U) -> Option<Vec<U>> {
    if values.len() <= TRANSFORM_SAMPLE_SIZE {
        return None;
    }
    let run = TRANSFORM_SAMPLE_SIZE / TRANSFORM_SAMPLE_RUNS;
    let span = values.len() - run;
    let mut sample = Vec::with_capacity(TRANSFORM_SAMPLE_SIZE);
    for i in 0..TRANSFORM_SAMPLE_RUNS {
        let start = span * i / (TRANSFORM_SAMPLE_RUNS - 1);
        if let Some(window) = values.get(start..start + run) {
            sample.extend(window.iter().map(|value| map(*value)));
        }
    }
    Some(sample)
}

const DECODE_COST_DEN: usize = 16;

fn decode_cost_num(transform: Transform, strategy: CascadeStrategy) -> usize {
    match (strategy, transform) {
        (CascadeStrategy::DecodeOptimized | CascadeStrategy::NoTrailing, Transform::Rle) => 17,
        _ => DECODE_COST_DEN,
    }
}

fn choose_u64_transform(values: &[u64], strategy: CascadeStrategy) -> Transform {
    let sample = transform_sample(values);
    let sample: &[u64] = sample.as_deref().unwrap_or(values);
    score_u64_candidates(sample, values.len(), strategy)
}

fn score_u64_candidates(sample: &[u64], block_len: usize, strategy: CascadeStrategy) -> Transform {
    if sample.is_empty() {
        return Transform::PlainU64;
    }
    let plain = 4 + sample.len() * 8;
    let candidates = [
        (Transform::ForBitpack, for_size(sample, block_len)),
        (Transform::DeltaBitpack, delta_size(sample, block_len)),
        (Transform::Rle, rle_size(sample)),
        (Transform::PlainU64, plain),
    ];
    candidates
        .into_iter()
        .min_by_key(|(transform, size)| *size * decode_cost_num(*transform, strategy))
        .map(|(transform, _)| transform)
        .unwrap_or(Transform::PlainU64)
}

fn choose_i64_transform(values: &[i64], strategy: CascadeStrategy) -> Transform {
    let mapped =
        transform_sample_mapped(values, zigzag).unwrap_or_else(|| values.iter().map(|value| zigzag(*value)).collect());
    score_u64_candidates(&mapped, values.len(), strategy)
}

fn encode_u64_with(transform: Transform, values: &[u64], out: &mut Writer) {
    match transform {
        Transform::ForBitpack => encode_for_bitpack(values, out),
        Transform::DeltaBitpack => encode_delta_bitpack(values, out),
        Transform::Rle => encode_rle(values, out),
        _ => encode_plain_u64(values, out),
    }
}

fn encode_i64_with(transform: Transform, values: &[i64], out: &mut Writer) {
    match transform {
        Transform::ForBitpack => encode_for_bitpack_i64(values, out),
        Transform::DeltaBitpack => encode_delta_bitpack_i64(values, out),
        Transform::Rle => encode_rle_iter(values.iter().map(|value| zigzag(*value)), out),
        _ => {
            out.put_u32(values.len() as u32);
            let mut chunk = [0u64; FASTLANES_VECTOR];
            for slice in values.chunks(FASTLANES_VECTOR) {
                for (slot, value) in chunk.iter_mut().zip(slice) {
                    *slot = zigzag(*value);
                }
                out.put_u64_slice(chunk.get(..slice.len()).unwrap_or_default());
            }
        }
    }
}

fn decode_ints_with<T: IntValue>(transform: Transform, reader: &mut Reader<'_>) -> Result<Vec<T>, FormatError> {
    match transform {
        Transform::ForBitpack => decode_for_bitpack(reader),
        Transform::DeltaBitpack => decode_delta_bitpack(reader),
        Transform::Rle => decode_rle(reader),
        Transform::PlainU64 => decode_plain_u64(reader),
        _ => Err(FormatError::Structural {
            rule: "transform does not decode to u64",
        }),
    }
}

const ALP_POWERS: [f64; 19] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
];

#[cfg(test)]
fn alp_try(value: f64, power: f64) -> Option<i64> {
    let scaled = value * power;
    if !scaled.is_finite() || scaled.abs() >= ALP_MAX_SCALED_MAGNITUDE {
        return None;
    }
    let rounded = scaled.round();
    let as_int = rounded as i64;
    if ((as_int as f64) / power).to_bits() == value.to_bits() {
        Some(as_int)
    } else {
        None
    }
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn alp_probe_chunk(values: &[f64], power: f64, zigzags: &mut [u64], encodable: &mut [bool]) {
    for ((value, zigzag_slot), encodable_slot) in values.iter().zip(zigzags.iter_mut()).zip(encodable.iter_mut()) {
        let scaled = value * power;
        let in_range = scaled.abs() < ALP_MAX_SCALED_MAGNITUDE;
        let truncated = scaled.trunc();
        let fraction = scaled - truncated;
        let adjust = if fraction.abs() >= 0.5 {
            1.0f64.copysign(scaled)
        } else {
            0.0
        };
        let rounded = truncated + adjust;
        let ok = in_range && (rounded / power).to_bits() == value.to_bits();
        let as_int = rounded as i64;
        *zigzag_slot = if ok { zigzag(as_int) } else { 0 };
        *encodable_slot = ok;
    }
}

fn plain_exception_index_size(count: usize) -> usize {
    count * 4
}

fn cascaded_exception_index_size(count: usize, max_delta: u64) -> usize {
    let width = bits_needed(max_delta) as usize;
    let vectors = count.div_ceil(FASTLANES_VECTOR);
    8 + 5 + vectors * width * FASTLANES_LANES * 8
}

fn choose_exception_side_stream(count: usize, max_delta: u64, strategy: CascadeStrategy) -> SideStream {
    if count == 0 || !cascade_level_fits(1) {
        return SideStream::None;
    }
    let plain = plain_exception_index_size(count);
    let cascaded = cascaded_exception_index_size(count, max_delta);
    let threshold = match strategy {
        CascadeStrategy::DecodeOptimized | CascadeStrategy::NoTrailing => {
            plain * DECODE_OPTIMIZED_THRESHOLD_NUM / DECODE_OPTIMIZED_THRESHOLD_DEN
        }
        CascadeStrategy::SizeOptimized => plain - plain / SIZE_OPTIMIZED_SAVINGS_DIVISOR,
    };
    if cascaded < threshold {
        SideStream::ForBitpack
    } else {
        SideStream::None
    }
}

fn alp_viable(encodable: &[bool]) -> bool {
    if encodable.is_empty() {
        return true;
    }
    let run = (encodable.len() / TRANSFORM_SAMPLE_RUNS).max(1);
    let mut pathological = 0usize;
    let mut runs = 0usize;
    for chunk in encodable.chunks(run) {
        runs += 1;
        let exceptions = chunk.iter().filter(|ok| !**ok).count();
        if exceptions * ALP_MAX_EXCEPTION_SAMPLE_DIVISOR > chunk.len() {
            pathological += 1;
        }
    }
    pathological * 2 <= runs && pathological < runs
}

fn encode_alp(values: &[f64], strategy: CascadeStrategy, out: &mut Writer) -> Option<SideStream> {
    let sample = transform_sample(values);
    let sample: &[f64] = sample.as_deref().unwrap_or(values);
    let mut zigzags = vec![0u64; sample.len()];
    let mut encodable = vec![false; sample.len()];
    let mut best: Option<(usize, usize)> = None;
    for (index, power) in ALP_POWERS.iter().enumerate() {
        alp_probe_chunk(sample, *power, &mut zigzags, &mut encodable);
        let exceptions = encodable.iter().filter(|ok| !**ok).count();
        if best.is_none_or(|(_, current)| exceptions < current) {
            best = Some((index, exceptions));
        }
        if exceptions == 0 {
            break;
        }
    }
    let (exponent_index, _) = best?;
    let power = ALP_POWERS.get(exponent_index).copied().unwrap_or(1.0);
    alp_probe_chunk(sample, power, &mut zigzags, &mut encodable);
    if !alp_viable(&encodable) {
        return None;
    }
    let vector_scratch = values.len().min(FASTLANES_VECTOR);
    zigzags.resize(vector_scratch, 0);
    encodable.resize(vector_scratch, false);
    let mut ints = Vec::with_capacity(values.len());
    let mut exceptions: Vec<(u32, u64)> = Vec::new();
    let mut escaped: Vec<u32> = Vec::new();
    for (vector_index, vector) in values.chunks(FASTLANES_VECTOR).enumerate() {
        let (Some(vector_zigzags), Some(vector_encodable)) =
            (zigzags.get_mut(..vector.len()), encodable.get_mut(..vector.len()))
        else {
            continue;
        };
        alp_probe_chunk(vector, power, vector_zigzags, vector_encodable);
        let vector_exceptions = vector_encodable.iter().filter(|ok| !**ok).count();
        if vector_exceptions * ALP_MAX_EXCEPTION_SAMPLE_DIVISOR > vector.len() {
            escaped.push(vector_index as u32);
            ints.extend(std::iter::repeat_n(0u64, vector.len()));
            continue;
        }
        let base = vector_index * FASTLANES_VECTOR;
        ints.extend_from_slice(vector_zigzags);
        for (offset, (ok, value)) in vector_encodable.iter().zip(vector).enumerate() {
            if !*ok {
                exceptions.push(((base + offset) as u32, value.to_bits()));
            }
        }
    }
    let index_base = exceptions.iter().map(|(index, _)| u64::from(*index)).min().unwrap_or(0);
    let index_max_delta = exceptions
        .iter()
        .map(|(index, _)| u64::from(*index).wrapping_sub(index_base))
        .max()
        .unwrap_or(0);
    let side = choose_exception_side_stream(exceptions.len(), index_max_delta, strategy);
    if escaped.is_empty() {
        out.put_u8(exponent_index as u8);
    } else {
        out.put_u8(ALP_VECTOR_ESCAPE_SENTINEL);
        out.put_u8(exponent_index as u8);
        out.put_u32(escaped.len() as u32);
        for vector_index in &escaped {
            out.put_u32(*vector_index);
        }
    }
    out.put_u32(exceptions.len() as u32);
    match side {
        SideStream::None => {
            for (index, bits) in &exceptions {
                out.put_u32(*index);
                out.put_u64(*bits);
            }
        }
        SideStream::ForBitpack => {
            out.put_u64(index_base);
            bitpack_with(exceptions.len(), bits_needed(index_max_delta), out, |i| {
                exceptions
                    .get(i)
                    .map_or(0, |(index, _)| u64::from(*index).wrapping_sub(index_base))
            });
            for (_, bits) in &exceptions {
                out.put_u64(*bits);
            }
        }
        SideStream::FileScopeDictionary | SideStream::FsstDictionaryValues | SideStream::FsstValueKeys => {
            unreachable!("the exception side-stream chooser never picks a string block's side stream")
        }
    }
    let max = ints.iter().copied().max().unwrap_or(0);
    bitpack(&ints, bits_needed(max), out);
    for vector_index in &escaped {
        let start = *vector_index as usize * FASTLANES_VECTOR;
        let end = (start + FASTLANES_VECTOR).min(values.len());
        for value in values.get(start..end).unwrap_or_default() {
            out.put_u64(value.to_bits());
        }
    }
    Some(side)
}

fn read_alp_escaped_vectors(reader: &mut Reader<'_>) -> Result<Vec<u32>, FormatError> {
    let count = reader.u32("alp escaped vector count")? as usize;
    if count > MAX_BLOCK_VALUES / FASTLANES_VECTOR {
        return Err(FormatError::Structural {
            rule: "alp escaped vector count beyond the block bound",
        });
    }
    let mut escaped = Vec::with_capacity(count);
    for _ in 0..count {
        let index = reader.u32("alp escaped vector index")?;
        if escaped.last().is_some_and(|last| *last >= index) {
            return Err(FormatError::Structural {
                rule: "alp escaped vector indexes must be strictly ascending",
            });
        }
        escaped.push(index);
    }
    Ok(escaped)
}

fn apply_alp_escapes(reader: &mut Reader<'_>, escaped: &[u32], values: &mut [f64]) -> Result<(), FormatError> {
    for vector_index in escaped {
        let start = *vector_index as usize * FASTLANES_VECTOR;
        if start >= values.len() {
            return Err(FormatError::RefOutOfRange {
                what: "alp escaped vector index",
            });
        }
        let end = (start + FASTLANES_VECTOR).min(values.len());
        for slot in values.get_mut(start..end).unwrap_or_default() {
            *slot = f64::from_bits(reader.u64("alp escaped value")?);
        }
    }
    Ok(())
}

enum AlpExceptions<'a> {
    Interleaved(&'a [u8]),
    Separate { bits: &'a [u8], indexes: Vec<u64> },
}

impl AlpExceptions<'_> {
    fn try_for_each(&self, mut patch: impl FnMut(u32, u64) -> Result<(), FormatError>) -> Result<(), FormatError> {
        match self {
            Self::Interleaved(records) => {
                for record in records.chunks_exact(size_of::<u32>() + size_of::<u64>()) {
                    let (index, bits) = record.split_at_checked(size_of::<u32>()).unwrap_or((&[], &[]));
                    patch(
                        u32::from_le_bytes(index.try_into().unwrap_or([0; 4])),
                        u64::from_le_bytes(bits.try_into().unwrap_or([0; 8])),
                    )?;
                }
            }
            Self::Separate { bits, indexes } => {
                for (index, word) in indexes.iter().zip(bits.chunks_exact(size_of::<u64>())) {
                    patch(*index as u32, u64::from_le_bytes(word.try_into().unwrap_or([0; 8])))?;
                }
            }
        }
        Ok(())
    }
}

fn decode_alp_exceptions<'a>(reader: &mut Reader<'a>, side: SideStream) -> Result<AlpExceptions<'a>, FormatError> {
    let exception_count = reader.u32("alp exception count")? as usize;
    match side {
        SideStream::FileScopeDictionary | SideStream::FsstDictionaryValues | SideStream::FsstValueKeys => {
            Err(FormatError::Structural {
                rule: "alp blocks never carry a string block's side stream",
            })
        }
        SideStream::None => {
            let mut records = *reader;
            for _ in 0..exception_count {
                reader.u32("alp exception index")?;
                reader.u64("alp exception bits")?;
            }
            let records = records.take(reader.position() - records.position(), "alp exception index")?;
            Ok(AlpExceptions::Interleaved(records))
        }
        SideStream::ForBitpack => {
            let indexes: Vec<u64> = decode_for_bitpack(reader)?;
            if indexes.len() != exception_count {
                return Err(FormatError::Structural {
                    rule: "alp exception index stream disagrees with the exception count",
                });
            }
            let mut bits = *reader;
            for index in &indexes {
                reader.u64("alp exception bits")?;
                u32::try_from(*index).map_err(|_| FormatError::RefOutOfRange {
                    what: "alp exception index",
                })?;
            }
            let bits = bits.take(reader.position() - bits.position(), "alp exception bits")?;
            Ok(AlpExceptions::Separate { bits, indexes })
        }
    }
}

fn decode_alp(reader: &mut Reader<'_>, side: SideStream) -> Result<Vec<f64>, FormatError> {
    let mut exponent_index = reader.u8("alp exponent")? as usize;
    let mut escaped = Vec::new();
    if exponent_index == usize::from(ALP_VECTOR_ESCAPE_SENTINEL) {
        exponent_index = reader.u8("alp exponent")? as usize;
        escaped = read_alp_escaped_vectors(reader)?;
    }
    let power = ALP_POWERS.get(exponent_index).copied().ok_or(FormatError::Structural {
        rule: "alp exponent out of range",
    })?;
    let exceptions = decode_alp_exceptions(reader, side)?;
    let stream = read_packed_stream(reader)?;
    let mut values = vec![0f64; stream.padded_count()];
    if stream.width <= ALP_EXACT_CONVERT_MAX_WIDTH {
        stream.unpack_into(&mut values, |packed| alp_narrow_int_to_f64(packed) / power);
    } else {
        stream.unpack_into(&mut values, |packed| (unzigzag(packed) as f64) / power);
    }
    values.truncate(stream.count);
    exceptions.try_for_each(|index, bits| {
        let slot = values.get_mut(index as usize).ok_or(FormatError::RefOutOfRange {
            what: "alp exception index",
        })?;
        *slot = f64::from_bits(bits);
        Ok(())
    })?;
    apply_alp_escapes(reader, &escaped, &mut values)?;
    Ok(values)
}

fn encode_plain_f64(values: &[f64], out: &mut Writer) {
    out.put_u32(values.len() as u32);
    out.put_u64_slice(bytemuck::cast_slice(values));
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn write_byte_stream_split(values: &[f64], out: &mut [u8]) {
    if values.is_empty() {
        return;
    }
    let bits: &[u64] = bytemuck::cast_slice(values);
    let mut planes = out.chunks_exact_mut(values.len());
    let mut plane_tiles: [_; 8] = std::array::from_fn(|_| {
        planes
            .next()
            .unwrap_or_default()
            .chunks_exact_mut(BYTE_STREAM_SPLIT_TILE)
    });
    let mut tiles = bits.chunks_exact(BYTE_STREAM_SPLIT_TILE);
    for tile in tiles.by_ref() {
        let Ok(tile): Result<&[u64; BYTE_STREAM_SPLIT_TILE], _> = tile.try_into() else {
            continue;
        };
        for (plane_index, plane_tile) in plane_tiles.iter_mut().enumerate() {
            let Some(Ok(dest)): Option<Result<&mut [u8; BYTE_STREAM_SPLIT_TILE], _>> =
                plane_tile.next().map(TryInto::try_into)
            else {
                continue;
            };
            let shift = (8 * plane_index) as u32;
            for (slot, value) in dest.iter_mut().zip(tile) {
                *slot = (value >> shift) as u8;
            }
        }
    }
    let tail = tiles.remainder();
    for (plane_index, plane_tile) in plane_tiles.into_iter().enumerate() {
        let shift = (8 * plane_index) as u32;
        for (slot, value) in plane_tile.into_remainder().iter_mut().zip(tail) {
            *slot = (value >> shift) as u8;
        }
    }
}

fn choose_float_fallback(values: &[f64]) -> Transform {
    let sample = transform_sample(values);
    let sample: &[f64] = sample.as_deref().unwrap_or(values);
    if sample.is_empty() {
        return Transform::PlainF64;
    }
    let mut trial = vec![0u8; sample.len() * 8];
    for (bytes, value) in trial.chunks_exact_mut(8).zip(sample) {
        bytes.copy_from_slice(&value.to_le_bytes());
    }
    let plain_compressed = deflate::compressed_len(&trial);
    write_byte_stream_split(sample, &mut trial);
    let bss_compressed = deflate::compressed_len(&trial);
    let (_, _, alp_rd_estimate) = alp_rd_choose_split(sample);
    if alp_rd_estimate < plain_compressed.min(bss_compressed) {
        Transform::AlpRd
    } else if bss_compressed < plain_compressed {
        Transform::ByteStreamSplit
    } else {
        Transform::PlainF64
    }
}

fn encode_byte_stream_split(values: &[f64], out: &mut Writer) {
    out.put_u32(values.len() as u32);
    write_byte_stream_split(values, out.reserve_bytes(values.len() * 8));
}

fn decode_byte_stream_split(reader: &mut Reader<'_>) -> Result<Vec<f64>, FormatError> {
    let count = reader.u32("byte-stream-split count")? as usize;
    let mut planes: [&[u8]; 8] = [&[]; 8];
    for plane in planes.iter_mut() {
        *plane = reader.take(count, "byte-stream-split plane")?;
    }
    Ok(bytemuck::cast_vec(join_byte_stream_split(&planes, count)))
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn join_byte_stream_split(planes: &[&[u8]; 8], count: usize) -> Vec<u64> {
    let mut bits = Vec::with_capacity(count);
    let mut plane_tiles = planes.map(|plane| plane.chunks_exact(BYTE_STREAM_SPLIT_TILE));
    for _ in 0..count / BYTE_STREAM_SPLIT_TILE {
        let mut tile = [0u64; BYTE_STREAM_SPLIT_TILE];
        for (plane_index, plane_tile) in plane_tiles.iter_mut().enumerate() {
            let Some(Ok(source)): Option<Result<&[u8; BYTE_STREAM_SPLIT_TILE], _>> =
                plane_tile.next().map(TryInto::try_into)
            else {
                continue;
            };
            let shift = (8 * plane_index) as u32;
            for (slot, byte) in tile.iter_mut().zip(source) {
                *slot |= u64::from(*byte) << shift;
            }
        }
        bits.extend_from_slice(&tile);
    }
    let mut tail = [0u64; BYTE_STREAM_SPLIT_TILE];
    for (plane_index, plane_tile) in plane_tiles.iter().enumerate() {
        let shift = (8 * plane_index) as u32;
        for (slot, byte) in tail.iter_mut().zip(plane_tile.remainder()) {
            *slot |= u64::from(*byte) << shift;
        }
    }
    bits.extend_from_slice(tail.get(..count % BYTE_STREAM_SPLIT_TILE).unwrap_or(&[]));
    bits
}

fn alp_rd_choose_split(sample: &[f64]) -> (u32, Vec<u16>, usize) {
    let mut lefts: Vec<u16> = Vec::with_capacity(sample.len());
    let mut best_top = [(0usize, 0u16); ALP_RD_MAX_DICT_LEN];
    let mut best_len = 0;
    let mut best_estimate = usize::MAX;
    let mut best_width = ALP_RD_MAX_RIGHT_WIDTH;
    for right_width in ALP_RD_MIN_RIGHT_WIDTH..=ALP_RD_MAX_RIGHT_WIDTH {
        lefts.clear();
        lefts.extend(sample.iter().map(|value| (value.to_bits() >> right_width) as u16));
        lefts.sort_unstable();
        let mut top = [(0usize, 0u16); ALP_RD_MAX_DICT_LEN];
        let mut top_len = 0;
        for run in lefts.chunk_by(|a, b| a == b) {
            let Some(&key) = run.first() else {
                continue;
            };
            let count = run.len();
            let position = top
                .get(..top_len)
                .unwrap_or_default()
                .iter()
                .position(|&(kept, _)| kept < count)
                .unwrap_or(top_len);
            let Some(tail) = top.get_mut(position..).filter(|tail| !tail.is_empty()) else {
                continue;
            };
            tail.rotate_right(1);
            if let Some(slot) = tail.first_mut() {
                *slot = (count, key);
            }
            top_len = (top_len + 1).min(ALP_RD_MAX_DICT_LEN);
        }
        let kept = top.get(..top_len).unwrap_or_default();
        let covered: usize = kept.iter().map(|&(count, _)| count).sum();
        let exceptions = sample.len() - covered;
        let code_bits = bits_needed(top_len.saturating_sub(1) as u64) as usize;
        let estimate =
            2 + 2 * top_len + 4 + exceptions * 6 + (sample.len() * (code_bits + right_width as usize)).div_ceil(8);
        if estimate < best_estimate {
            best_top = top;
            best_len = top_len;
            best_estimate = estimate;
            best_width = right_width;
        }
    }
    let dict = best_top.iter().take(best_len).map(|&(_, key)| key).collect();
    (best_width, dict, best_estimate)
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn alp_rd_split_chunk(
    values: &[f64],
    right_width: u32,
    dict: &[u16; ALP_RD_MAX_DICT_LEN],
    dict_len: usize,
    codes: &mut [u64],
    rights: &mut [u64],
) {
    let right_mask = mask(right_width);
    for ((value, code_slot), right_slot) in values.iter().zip(codes.iter_mut()).zip(rights.iter_mut()) {
        let bits = value.to_bits();
        *right_slot = bits & right_mask;
        let left = (bits >> right_width) as u16;
        let mut code = ALP_RD_CODE_MISS;
        for (entry_index, entry) in dict.iter().enumerate().rev() {
            if entry_index < dict_len && *entry == left {
                code = entry_index as u64;
            }
        }
        *code_slot = code;
    }
}

fn encode_alp_rd(values: &[f64], out: &mut Writer) {
    let sample = transform_sample(values);
    let sample: &[f64] = sample.as_deref().unwrap_or(values);
    let (right_width, dict, _) = alp_rd_choose_split(sample);
    let mut padded_dict = [0u16; ALP_RD_MAX_DICT_LEN];
    for (slot, entry) in padded_dict.iter_mut().zip(&dict) {
        *slot = *entry;
    }
    out.put_u8(right_width as u8);
    out.put_u8(dict.len() as u8);
    for left in &dict {
        out.put_u16(*left);
    }
    let exception_count_at = out.len();
    out.put_u32(0);
    let mut exception_count = 0u32;
    let mut codes = [0u64; FASTLANES_VECTOR];
    let mut rights = [0u64; FASTLANES_VECTOR];
    let mut max_code = 0u64;
    let mut max_right = 0u64;
    for (vector_index, value_chunk) in values.chunks(FASTLANES_VECTOR).enumerate() {
        alp_rd_split_chunk(
            value_chunk,
            right_width,
            &padded_dict,
            dict.len(),
            &mut codes,
            &mut rights,
        );
        for (offset, ((code, right), value)) in codes.iter().zip(&rights).zip(value_chunk).enumerate() {
            if *code == ALP_RD_CODE_MISS {
                exception_count += 1;
                out.put_u32((vector_index * FASTLANES_VECTOR + offset) as u32);
                out.put_u16((value.to_bits() >> right_width) as u16);
            } else {
                max_code = max_code.max(*code);
            }
            max_right = max_right.max(*right);
        }
    }
    out.patch_u32(exception_count_at, exception_count);
    alp_rd_bitpack_vectors(values.len(), bits_needed(max_code), out, |vector_index, buffer| {
        let start = vector_index * FASTLANES_VECTOR;
        let value_chunk = values
            .get(start..values.len().min(start + FASTLANES_VECTOR))
            .unwrap_or(&[]);
        buffer.fill(0);
        alp_rd_split_chunk(value_chunk, right_width, &padded_dict, dict.len(), buffer, &mut rights);
        for code in buffer.iter_mut() {
            if *code == ALP_RD_CODE_MISS {
                *code = 0;
            }
        }
    });
    let right_mask = mask(right_width);
    bitpack_with(values.len(), bits_needed(max_right), out, |i| {
        values.get(i).map_or(0, |value| value.to_bits() & right_mask)
    });
}

fn alp_rd_bitpack_vectors(
    len: usize,
    width: u32,
    out: &mut Writer,
    mut fill: impl FnMut(usize, &mut [u64; FASTLANES_VECTOR]),
) {
    out.put_u32(len as u32);
    out.put_u8(width as u8);
    if width == 0 {
        return;
    }
    let words_per_vector = FASTLANES_LANES * fastlanes_words_per_lane(width as usize);
    let vectors = len.div_ceil(FASTLANES_VECTOR);
    let value_mask = mask(width);
    let packed = out.reserve_bytes(vectors * words_per_vector * 8);
    let mut buffer = [0u64; FASTLANES_VECTOR];
    let mut scratch = [0u64; FASTLANES_VECTOR];
    for (vector_index, packed_vector) in packed.chunks_exact_mut(words_per_vector * 8).enumerate() {
        fill(vector_index, &mut buffer);
        pack_vector_into(&buffer, &mut scratch, packed_vector, width, value_mask);
    }
}

fn decode_alp_rd(reader: &mut Reader<'_>) -> Result<Vec<f64>, FormatError> {
    let right_width = u32::from(reader.u8("alp-rd right width")?);
    if !(ALP_RD_MIN_RIGHT_WIDTH..=ALP_RD_MAX_RIGHT_WIDTH).contains(&right_width) {
        return Err(FormatError::Structural {
            rule: "alp-rd right width out of range",
        });
    }
    let dict_len = reader.u8("alp-rd dictionary length")? as usize;
    if dict_len > ALP_RD_MAX_DICT_LEN {
        return Err(FormatError::Structural {
            rule: "alp-rd dictionary too long",
        });
    }
    let left_limit = 1u64 << (64 - right_width);
    let mut shifted_lefts = [0u64; ALP_RD_MAX_DICT_LEN];
    for slot in shifted_lefts.iter_mut().take(dict_len) {
        let left = reader.u16("alp-rd dictionary entry")?;
        if u64::from(left) >= left_limit {
            return Err(FormatError::Structural {
                rule: "alp-rd left part exceeds its width",
            });
        }
        *slot = u64::from(left) << right_width;
    }
    let exception_count = reader.u32("alp-rd exception count")? as usize;
    if exception_count > MAX_BLOCK_VALUES {
        return Err(FormatError::Structural {
            rule: "alp-rd exception count beyond the block bound",
        });
    }
    let mut exceptions = Vec::with_capacity(reader.capacity_hint(exception_count, 6));
    for _ in 0..exception_count {
        let index = reader.u32("alp-rd exception index")?;
        let left = reader.u16("alp-rd exception left part")?;
        if u64::from(left) >= left_limit {
            return Err(FormatError::Structural {
                rule: "alp-rd left part exceeds its width",
            });
        }
        exceptions.push((index, left));
    }
    let codes = read_packed_stream(reader)?;
    let rights = read_packed_stream(reader)?;
    if codes.count != rights.count {
        return Err(FormatError::Structural {
            rule: "alp-rd code and right-part streams disagree",
        });
    }
    let dict_len = dict_len as u64;
    let mut values = vec![0u64; codes.padded_count()];
    codes.unpack_into(&mut values, |code| {
        let left = shifted_lefts
            .get(code as usize % ALP_RD_MAX_DICT_LEN)
            .copied()
            .unwrap_or(0);
        left | u64::from(code >= dict_len)
    });
    let mut invalid = 0u64;
    let mut value_vectors = values.chunks_mut(FASTLANES_VECTOR);
    rights.for_each_vector(|right_vector| {
        if let Some(value_vector) = value_vectors.next() {
            invalid |= alp_rd_join_rights(value_vector, right_vector, right_width);
        }
    });
    if invalid != 0 {
        return Err(alp_rd_first_invalid_row(&codes, &rights, dict_len, right_width));
    }
    values.truncate(codes.count);
    let right_mask = mask(right_width);
    for (index, left) in exceptions {
        let slot = values.get_mut(index as usize).ok_or(FormatError::RefOutOfRange {
            what: "alp-rd exception index",
        })?;
        *slot = (u64::from(left) << right_width) | (*slot & right_mask);
    }
    Ok(bytemuck::cast_vec(values))
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn alp_rd_join_rights(values: &mut [u64], rights: &[u64], right_width: u32) -> u64 {
    let mut invalid = 0u64;
    for (slot, right) in values.iter_mut().zip(rights) {
        invalid |= (*slot & 1) | (right >> right_width);
        *slot |= right;
    }
    invalid
}

fn alp_rd_first_invalid_row(
    codes: &PackedStream<'_>,
    rights: &PackedStream<'_>,
    dict_len: u64,
    right_width: u32,
) -> FormatError {
    let mut code_values = vec![0u64; codes.padded_count()];
    codes.unpack_into(&mut code_values, |code| code);
    let mut right_values = vec![0u64; rights.padded_count()];
    rights.unpack_into(&mut right_values, |right| right);
    for (code, right) in code_values.iter().zip(&right_values).take(codes.count) {
        if *right >> right_width != 0 {
            return FormatError::Structural {
                rule: "alp-rd right part exceeds its width",
            };
        }
        if *code >= dict_len {
            return FormatError::RefOutOfRange { what: "alp-rd code" };
        }
    }
    FormatError::RefOutOfRange { what: "alp-rd code" }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NullStreamForm<'a> {
    AllAbsent,
    AllPresent,
    Raw(Cow<'a, [u8]>),
    Runs(NullRuns<'a>),
}

#[derive(Debug, Clone)]
enum NullRuns<'a> {
    Encoded(&'a [u8]),
    Pairs(Vec<(u32, u32)>),
}

impl NullRuns<'_> {
    fn len(&self) -> usize {
        match self {
            NullRuns::Encoded(bytes) => bytes.len() / 8,
            NullRuns::Pairs(pairs) => pairs.len(),
        }
    }

    fn get(&self, index: usize) -> Option<(u32, u32)> {
        match self {
            NullRuns::Encoded(bytes) => {
                let offset = index.checked_mul(8)?;
                let start = bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?;
                let end = bytes
                    .get(offset.checked_add(4)?..offset.checked_add(8)?)?
                    .try_into()
                    .ok()?;
                Some((u32::from_le_bytes(start), u32::from_le_bytes(end)))
            }
            NullRuns::Pairs(pairs) => pairs.get(index).copied(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.iter_from(0)
    }

    fn iter_from(&self, first: usize) -> impl Iterator<Item = (u32, u32)> + '_ {
        (first..self.len()).filter_map(|index| self.get(index))
    }

    fn partition_point(&self, mut keep_going: impl FnMut((u32, u32)) -> bool) -> usize {
        let (mut low, mut high) = (0usize, self.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if self.get(middle).is_some_and(&mut keep_going) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }
}

impl PartialEq for NullRuns<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for NullRuns<'_> {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NullStream<'a> {
    form: NullStreamForm<'a>,
    row_count: usize,
}

fn each_present_run(presence: &[bool], mut emit: impl FnMut(usize, usize)) {
    let mut run_start: Option<usize> = None;
    for (index, &present) in presence.iter().enumerate() {
        match (present, run_start) {
            (true, None) => run_start = Some(index),
            (false, Some(start)) => {
                emit(start, index);
                run_start = None;
            }
            _ => {}
        }
    }
    if let Some(start) = run_start {
        emit(start, presence.len());
    }
}

impl<'a> NullStream<'a> {
    fn from_values(values: &StringColumn) -> Self {
        let presence = values.presence();
        let row_count = presence.len();
        let mut present_count = 0usize;
        let mut run_count = 0usize;
        let mut previous_present = false;
        for &present in presence {
            present_count += usize::from(present);
            run_count += usize::from(present && !previous_present);
            previous_present = present;
        }
        let form = if present_count == 0 {
            NullStreamForm::AllAbsent
        } else if present_count == row_count {
            NullStreamForm::AllPresent
        } else if 4 + run_count * 8 < row_count.div_ceil(8) {
            let mut runs: Vec<(u32, u32)> = Vec::with_capacity(run_count);
            each_present_run(presence, |start, end| runs.push((start as u32, end as u32)));
            NullStreamForm::Runs(NullRuns::Pairs(runs))
        } else {
            let mut raw = vec![0u8; row_count.div_ceil(8)];
            for (byte, rows) in raw.iter_mut().zip(presence.chunks(8)) {
                *byte = rows
                    .iter()
                    .enumerate()
                    .fold(0u8, |bits, (bit, &present)| bits | (u8::from(present) << bit));
            }
            NullStreamForm::Raw(Cow::Owned(raw))
        };
        Self { form, row_count }
    }

    fn write(&self, out: &mut Writer) {
        out.put_u32(self.row_count as u32);
        match &self.form {
            NullStreamForm::AllPresent => out.put_u8(0),
            NullStreamForm::AllAbsent => out.put_u8(1),
            NullStreamForm::Runs(runs) => {
                out.put_u8(2);
                out.put_u32(runs.len() as u32);
                for (start, end) in runs.iter() {
                    out.put_u32(start);
                    out.put_u32(end);
                }
            }
            NullStreamForm::Raw(bits) => {
                out.put_u8(3);
                out.put_slice(bits);
            }
        }
    }

    fn read(reader: &mut Reader<'a>) -> Result<Self, FormatError> {
        let row_count = reader.u32("string row count")? as usize;
        let form = match reader.u8("null stream form")? {
            0..=2 if row_count > MAX_BLOCK_VALUES => {
                return Err(FormatError::Structural {
                    rule: "null stream row count beyond the block bound",
                });
            }
            0 => NullStreamForm::AllPresent,
            1 => NullStreamForm::AllAbsent,
            2 => {
                let run_count = reader.u32("null stream run count")? as usize;
                let mut table = *reader;
                let mut previous_end: Option<u32> = None;
                for _ in 0..run_count {
                    let start = reader.u32("null stream run start")?;
                    let end = reader.u32("null stream run end")?;
                    if end <= start || end as usize > row_count {
                        return Err(FormatError::Structural {
                            rule: "null stream run must be non-empty and within the row count",
                        });
                    }
                    if previous_end.is_some_and(|previous_end| start <= previous_end) {
                        return Err(FormatError::Structural {
                            rule: "null stream runs must be sorted and non-adjacent",
                        });
                    }
                    previous_end = Some(end);
                }
                let consumed = reader.position() - table.position();
                NullStreamForm::Runs(NullRuns::Encoded(table.take(consumed, "null stream runs")?))
            }
            3 => NullStreamForm::Raw(Cow::Borrowed(reader.take(row_count.div_ceil(8), "null bitmap")?)),
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown null stream form",
                });
            }
        };
        Ok(Self { form, row_count })
    }

    fn into_owned(self) -> NullStream<'static> {
        let form = match self.form {
            NullStreamForm::AllAbsent => NullStreamForm::AllAbsent,
            NullStreamForm::AllPresent => NullStreamForm::AllPresent,
            NullStreamForm::Raw(bits) => NullStreamForm::Raw(Cow::Owned(bits.into_owned())),
            NullStreamForm::Runs(NullRuns::Pairs(pairs)) => NullStreamForm::Runs(NullRuns::Pairs(pairs)),
            NullStreamForm::Runs(runs) => NullStreamForm::Runs(NullRuns::Pairs(runs.iter().collect())),
        };
        NullStream {
            form,
            row_count: self.row_count,
        }
    }

    fn row_count(&self) -> usize {
        self.row_count
    }

    fn present_count(&self) -> usize {
        match &self.form {
            NullStreamForm::AllAbsent => 0,
            NullStreamForm::AllPresent => self.row_count,
            NullStreamForm::Raw(bits) => count_set_bits(bits, self.row_count),
            NullStreamForm::Runs(runs) => runs.iter().map(|(start, end)| (end - start) as usize).sum(),
        }
    }

    fn is_present(&self, row: usize) -> bool {
        if row >= self.row_count {
            return false;
        }
        match &self.form {
            NullStreamForm::AllAbsent => false,
            NullStreamForm::AllPresent => true,
            NullStreamForm::Raw(bits) => bits.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0),
            NullStreamForm::Runs(runs) => {
                let idx = runs.partition_point(|(start, _)| start as usize <= row);
                idx.checked_sub(1)
                    .and_then(|i| runs.get(i))
                    .is_some_and(|(_, end)| row < end as usize)
            }
        }
    }

    fn present_before(&self, row: usize) -> usize {
        let row = row.min(self.row_count);
        match &self.form {
            NullStreamForm::AllAbsent => 0,
            NullStreamForm::AllPresent => row,
            NullStreamForm::Raw(bits) => count_present_before(bits, row),
            NullStreamForm::Runs(runs) => runs
                .iter()
                .take_while(|&(start, _)| (start as usize) < row)
                .map(|(start, end)| (end as usize).min(row) - start as usize)
                .sum(),
        }
    }

    fn each_row(&self, mut row: impl FnMut(bool) -> Result<(), FormatError>) -> Result<(), FormatError> {
        match &self.form {
            NullStreamForm::AllAbsent => {
                for _ in 0..self.row_count {
                    row(false)?;
                }
            }
            NullStreamForm::AllPresent => {
                for _ in 0..self.row_count {
                    row(true)?;
                }
            }
            NullStreamForm::Raw(bits) => {
                for index in 0..self.row_count {
                    row(bits.get(index / 8).is_some_and(|byte| byte & (1 << (index % 8)) != 0))?;
                }
            }
            NullStreamForm::Runs(runs) => {
                let mut cursor = 0usize;
                for (start, end) in runs.iter() {
                    for _ in cursor..start as usize {
                        row(false)?;
                    }
                    for _ in start as usize..end as usize {
                        row(true)?;
                    }
                    cursor = end as usize;
                }
                for _ in cursor..self.row_count {
                    row(false)?;
                }
            }
        }
        Ok(())
    }

    fn each_row_range(
        &self,
        start: usize,
        end: usize,
        mut row: impl FnMut(bool) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        let end = end.min(self.row_count);
        if start >= end {
            return Ok(());
        }
        match &self.form {
            NullStreamForm::AllAbsent => {
                for _ in start..end {
                    row(false)?;
                }
            }
            NullStreamForm::AllPresent => {
                for _ in start..end {
                    row(true)?;
                }
            }
            NullStreamForm::Raw(bits) => {
                for index in start..end {
                    row(bits.get(index / 8).is_some_and(|byte| byte & (1 << (index % 8)) != 0))?;
                }
            }
            NullStreamForm::Runs(runs) => {
                let mut cursor = start;
                let first = runs.partition_point(|(_, run_end)| (run_end as usize) <= start);
                for (run_start, run_end) in runs.iter_from(first) {
                    let run_start = (run_start as usize).max(start);
                    let run_end = (run_end as usize).min(end);
                    if run_start >= end {
                        break;
                    }
                    for _ in cursor..run_start {
                        row(false)?;
                    }
                    for _ in run_start..run_end {
                        row(true)?;
                    }
                    cursor = run_end;
                }
                for _ in cursor..end {
                    row(false)?;
                }
            }
        }
        Ok(())
    }
}

fn present_values(values: &StringColumn) -> Vec<&str> {
    let mut present = Vec::with_capacity(values.len() - values.null_count());
    present.extend(values.iter_present());
    present
}

fn write_offsets(lens: impl Iterator<Item = usize>, out: &mut Writer) -> Result<(), FormatError> {
    let mut offset = 0u32;
    out.put_u32(offset);
    for len in lens {
        offset = u32::try_from(len)
            .ok()
            .and_then(|len| offset.checked_add(len))
            .ok_or(FormatError::Structural {
                rule: "string arena offsets exceed the u32 range the format stores",
            })?;
        out.put_u32(offset);
    }
    Ok(())
}

fn read_offsets(reader: &mut Reader<'_>, count: usize) -> Result<Vec<usize>, FormatError> {
    Ok(decode_offsets(
        reader.take(count.saturating_add(1).saturating_mul(4), "string offset")?,
    ))
}

fn decode_offsets(bytes: &[u8]) -> Vec<usize> {
    bytes
        .chunks_exact(4)
        .map(|entry| u32::from_le_bytes(entry.try_into().unwrap_or([0; 4])) as usize)
        .collect()
}

fn offset_window(table: &[u8], first: usize, count: usize) -> Result<Vec<usize>, FormatError> {
    Ok(decode_offsets(slice(
        table,
        first.saturating_mul(4),
        count.saturating_mul(4),
        "string offset",
    )?))
}

fn write_fsst_dictionary_values(
    dictionary: &[&str],
    strategy: CascadeStrategy,
    out: &mut Writer,
) -> Result<bool, FormatError> {
    if !cascade_level_fits(1) || dictionary.len() < FSST_MIN_SAMPLE_COUNT {
        return Ok(false);
    }
    if dictionary
        .iter()
        .any(|value| value.len() > DICTIONARY_FSST_MAX_VALUE_LEN)
    {
        return Ok(false);
    }
    let corpus: Vec<&[u8]> = dictionary.iter().map(|value| value.as_bytes()).collect();
    let compressor = fsst::Compressor::train(&corpus);
    let start = out.len();
    let symbols = compressor.symbol_table();
    let lengths = compressor.symbol_lengths();
    out.put_u16(symbols.len() as u16);
    for (symbol, length) in symbols.iter().zip(lengths.iter()) {
        out.put_u64(symbol.to_u64());
        out.put_u8(*length);
    }
    let offsets_start = out.len();
    out.reserve_bytes((dictionary.len() + 1) * 4);
    let arena_start = out.len();
    let mut offsets_overflow = false;
    let mut scratch = Vec::new();
    for (index, value) in corpus.iter().enumerate() {
        compress_fsst_value(&compressor, value, &mut scratch);
        out.put_slice(&scratch);
        match u32::try_from(out.len() - arena_start) {
            Ok(offset) => out.patch_u32(offsets_start + (index + 1) * 4, offset),
            Err(_) => offsets_overflow = true,
        }
    }
    let plain = (dictionary.len() + 1) * 4 + corpus.iter().map(|value| value.len()).sum::<usize>();
    let compressed = out.len() - start;
    let threshold = match strategy {
        CascadeStrategy::DecodeOptimized | CascadeStrategy::NoTrailing => {
            plain * DECODE_OPTIMIZED_THRESHOLD_NUM / DECODE_OPTIMIZED_THRESHOLD_DEN
        }
        CascadeStrategy::SizeOptimized => plain - plain / SIZE_OPTIMIZED_SAVINGS_DIVISOR,
    };
    if compressed >= threshold {
        out.truncate(start);
        return Ok(false);
    }
    if offsets_overflow {
        return Err(FormatError::Structural {
            rule: "string arena offsets exceed the u32 range the format stores",
        });
    }
    Ok(true)
}

#[cfg(test)]
fn encode_dictionary_string(
    values: &StringColumn,
    present: &[&str],
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
    out: &mut Writer,
) -> Result<SideStream, FormatError> {
    encode_dictionary_string_with(values, present, strategy, shared, true, out)
}

#[cfg(test)]
fn encode_dictionary_string_with(
    values: &StringColumn,
    present: &[&str],
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
    fsst_values: bool,
    out: &mut Writer,
) -> Result<SideStream, FormatError> {
    encode_prepared_dictionary(values, &prepare_dictionary(present), strategy, shared, fsst_values, out)
}

#[derive(Default)]
struct PreparedDictionary<'a> {
    codes: Vec<u32>,
    dictionary: Vec<&'a str>,
}

fn prepare_dictionary<'a>(present: &[&'a str]) -> PreparedDictionary<'a> {
    prepare_dictionary_within(present, present.len()).unwrap_or_default()
}

fn prepare_dictionary_within<'a>(present: &[&'a str], max_distinct: usize) -> Option<PreparedDictionary<'a>> {
    let mut first_seen: AHashMap<&str, u32> = AHashMap::new();
    let mut sorted: Vec<(&'a str, u32)> = Vec::new();
    let mut codes = Vec::with_capacity(present.len());
    for &value in present {
        let next = sorted.len() as u32;
        let id = *first_seen.entry(value).or_insert(next);
        if id == next {
            sorted.push((value, id));
            if sorted.len() > max_distinct {
                return None;
            }
        }
        codes.push(id);
    }
    sorted.sort_unstable();
    let mut rank = vec![0u32; sorted.len()];
    for (position, (_, id)) in sorted.iter().enumerate() {
        if let Some(slot) = rank.get_mut(*id as usize) {
            *slot = position as u32;
        }
    }
    for code in &mut codes {
        *code = rank.get(*code as usize).copied().unwrap_or(0);
    }
    Some(PreparedDictionary {
        codes,
        dictionary: sorted.into_iter().map(|(value, _)| value).collect(),
    })
}

fn encode_prepared_dictionary(
    values: &StringColumn,
    prepared: &PreparedDictionary<'_>,
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
    fsst_values: bool,
    out: &mut Writer,
) -> Result<SideStream, FormatError> {
    NullStream::from_values(values).write(out);
    let PreparedDictionary { codes, dictionary } = prepared;
    let local_code = |row: usize| codes.get(row).map_or(0, |&code| u64::from(code));
    if let Some(alphabet) = shared
        && dictionary
            .iter()
            .all(|value| alphabet.binary_search_by(|entry| entry.as_str().cmp(value)).is_ok())
    {
        let alphabet_index_of: Vec<u64> = dictionary
            .iter()
            .map(|value| {
                alphabet
                    .binary_search_by(|entry| entry.as_str().cmp(value))
                    .map(|index| index as u64)
                    .unwrap_or(0)
            })
            .collect();
        let max = alphabet_index_of.iter().copied().max().unwrap_or(0);
        let mut shared_out = Writer::new();
        bitpack_with(codes.len(), bits_needed(max), &mut shared_out, |row| {
            alphabet_index_of.get(local_code(row) as usize).copied().unwrap_or(0)
        });
        let shared_bytes = shared_out.into_bytes();
        let mut local_out = Writer::new();
        let local_side = encode_local_dictionary_with(
            dictionary,
            codes.len(),
            local_code,
            strategy,
            fsst_values,
            &mut local_out,
        )?;
        let local_bytes = local_out.into_bytes();
        if shared_bytes.len() < local_bytes.len() {
            out.put_slice(&shared_bytes);
            return Ok(SideStream::FileScopeDictionary);
        }
        out.put_slice(&local_bytes);
        return Ok(local_side);
    }
    encode_local_dictionary_with(dictionary, codes.len(), local_code, strategy, fsst_values, out)
}

#[cfg(test)]
fn encode_local_dictionary(
    dictionary: &[&str],
    count: usize,
    code: impl Fn(usize) -> u64,
    strategy: CascadeStrategy,
    out: &mut Writer,
) -> Result<SideStream, FormatError> {
    encode_local_dictionary_with(dictionary, count, code, strategy, true, out)
}

fn encode_local_dictionary_with(
    dictionary: &[&str],
    count: usize,
    code: impl Fn(usize) -> u64,
    strategy: CascadeStrategy,
    fsst_values: bool,
    out: &mut Writer,
) -> Result<SideStream, FormatError> {
    out.put_u32(dictionary.len() as u32);
    let side = if fsst_values && write_fsst_dictionary_values(dictionary, strategy, out)? {
        SideStream::FsstDictionaryValues
    } else {
        write_offsets(dictionary.iter().map(|value| value.len()), out)?;
        for value in dictionary {
            out.put_slice(value.as_bytes());
        }
        SideStream::None
    };
    let max = dictionary.len().saturating_sub(1) as u64;
    bitpack_with(count, bits_needed(max), out, code);
    Ok(side)
}

fn encode_fsst_string(
    values: &StringColumn,
    present: &[&str],
    replay: Option<&FsstTable>,
    out: &mut Writer,
) -> Result<(SideStream, Option<FsstTable>, Vec<usize>), FormatError> {
    NullStream::from_values(values).write(out);
    let table = match replay {
        Some(table) => Cow::Borrowed(table),
        None => {
            let corpus: Vec<&[u8]> = present.iter().map(|value| value.as_bytes()).collect();
            Cow::Owned(FsstTable(fsst::Compressor::train(&corpus)))
        }
    };
    let compressor = &table.0;
    let symbols = compressor.symbol_table();
    let lengths = compressor.symbol_lengths();
    out.put_u16(symbols.len() as u16);
    for (symbol, length) in symbols.iter().zip(lengths.iter()) {
        out.put_u64(symbol.to_u64());
        out.put_u8(*length);
    }
    out.put_u32(present.len() as u32);
    let offsets_start = out.len();
    out.reserve_bytes((present.len() + 1) * 4);
    let mut scratch = Vec::new();
    let mut offset = 0u32;
    for (index, value) in present.iter().enumerate() {
        compress_fsst_value(compressor, value.as_bytes(), &mut scratch);
        offset = u32::try_from(scratch.len())
            .ok()
            .and_then(|len| offset.checked_add(len))
            .ok_or(FormatError::Structural {
                rule: "string arena offsets exceed the u32 range the format stores",
            })?;
        out.put_slice(&scratch);
        out.patch_u32(offsets_start + (index + 1) * 4, offset);
    }
    let trained = match table {
        Cow::Owned(table) => Some(table),
        Cow::Borrowed(_) => None,
    };
    if !fsst_value_keys_pay(present) {
        return Ok((SideStream::None, trained, Vec::new()));
    }
    let prefixes_start = out.len();
    for value in present {
        out.put_slice(&PrefixKey::of(value).to_stored());
    }
    let fingerprints_start = out.len();
    for value in present {
        out.put_u32(StringFingerprint::of(value.as_bytes()).to_bits());
    }
    Ok((
        SideStream::FsstValueKeys,
        trained,
        vec![prefixes_start, fingerprints_start],
    ))
}

fn fsst_value_keys_pay(present: &[&str]) -> bool {
    if !cascade_level_fits(1) {
        return false;
    }
    let keys = present
        .len()
        .saturating_mul(PREFIX_KEY_BYTES + StringFingerprint::STORED_BYTES);
    let plaintext: usize = present.iter().map(|value| value.len()).sum();
    keys.saturating_mul(FSST_VALUE_KEYS_MAX_PLAINTEXT_DIVISOR) <= plaintext
}

fn compress_fsst_value(compressor: &fsst::Compressor, plaintext: &[u8], scratch: &mut Vec<u8>) {
    scratch.clear();
    if plaintext.is_empty() {
        return;
    }
    scratch.reserve(plaintext.len() * 2);
    // SAFETY: `compress_into` requires the output buffer's capacity to cover the worst-case compressed size, which is
    unsafe { compressor.compress_into(plaintext, scratch) };
}

fn encode_raw_string(values: &StringColumn, present: &[&str], out: &mut Writer) -> Result<(), FormatError> {
    NullStream::from_values(values).write(out);
    out.put_u32(present.len() as u32);
    write_offsets(present.iter().map(|value| value.len()), out)?;
    for value in present {
        out.put_slice(value.as_bytes());
    }
    Ok(())
}

fn merge_nulls<'a>(
    nulls: &NullStream<'_>,
    count: usize,
    text_bytes: usize,
    value: impl Fn(usize) -> Result<&'a str, FormatError>,
) -> Result<StringColumn, FormatError> {
    let mut values = StringColumn::with_capacity(nulls.row_count(), text_bytes);
    let mut next = 0usize;
    nulls.each_row(|set| {
        if set {
            if next >= count {
                return Err(FormatError::Structural {
                    rule: "null bitmap disagrees with present count",
                });
            }
            values.push(Some(value(next)?));
            next += 1;
        } else {
            values.push(None);
        }
        Ok(())
    })?;
    if next != count {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    Ok(values)
}

enum DictionaryValues<'a> {
    Fsst(FsstArena),
    Plain { data: &'a [u8], offsets: Vec<usize> },
}

impl<'a> DictionaryValues<'a> {
    fn read(reader: &mut Reader<'a>, dict_count: usize, side: SideStream) -> Result<Self, FormatError> {
        let fsst = match side {
            SideStream::FsstDictionaryValues => Some(FsstDecodeTable::read(reader)?),
            _ => None,
        };
        let offsets = read_offsets(reader, dict_count)?;
        let data_len = offsets.last().copied().unwrap_or(0);
        let data = reader.take(data_len, "dictionary data")?;
        Ok(match &fsst {
            Some(table) => Self::Fsst(fsst_decompress_arena(
                table,
                data,
                &offsets,
                "dictionary entry",
                "dictionary offsets must be non-decreasing",
            )?),
            None => Self::Plain { data, offsets },
        })
    }

    fn entries(&self) -> Result<Vec<&str>, FormatError> {
        match self {
            Self::Fsst(arena) => (0..arena.len())
                .map(|index| {
                    let entry = arena.value(index).ok_or(FormatError::InvalidUtf8 {
                        what: "dictionary entry",
                    })?;
                    if entry.len() > DICTIONARY_FSST_MAX_VALUE_LEN {
                        return Err(FormatError::Structural {
                            rule: "dictionary entry exceeds the FSST value cap",
                        });
                    }
                    Ok(entry)
                })
                .collect(),
            Self::Plain { data, offsets } => offsets
                .iter()
                .zip(offsets.iter().skip(1))
                .map(|(&start, &end)| {
                    if end < start {
                        return Err(FormatError::Structural {
                            rule: "dictionary offsets must be non-decreasing",
                        });
                    }
                    let bytes = slice(data, start, end - start, "dictionary entry")?;
                    simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 {
                        what: "dictionary entry",
                    })
                })
                .collect(),
        }
    }

    fn into_view_arena(self) -> Result<(Vec<u8>, Vec<usize>), FormatError> {
        match self {
            Self::Fsst(arena) => {
                for (&start, &end) in arena.bounds.iter().zip(arena.bounds.iter().skip(1)) {
                    let entry = arena.text.get(start..end).ok_or(FormatError::InvalidUtf8 {
                        what: "dictionary entry",
                    })?;
                    if entry.len() > DICTIONARY_FSST_MAX_VALUE_LEN {
                        return Err(FormatError::Structural {
                            rule: "dictionary entry exceeds the FSST value cap",
                        });
                    }
                }
                Ok((arena.text.into_bytes(), arena.bounds))
            }
            Self::Plain { data, offsets } => {
                if offsets
                    .windows(2)
                    .any(|pair| matches!(pair, [start, end] if end < start))
                {
                    return Err(FormatError::Structural {
                        rule: "dictionary offsets must be non-decreasing",
                    });
                }
                let first = offsets.first().copied().unwrap_or(0);
                let last = offsets.last().copied().unwrap_or(first);
                let bytes = slice(data, first, last - first, "dictionary entry")?;
                let text = simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 {
                    what: "dictionary entry",
                })?;
                let mut bounds = offsets;
                for bound in &mut bounds {
                    *bound -= first;
                    if !text.is_char_boundary(*bound) {
                        return Err(FormatError::InvalidUtf8 {
                            what: "dictionary entry",
                        });
                    }
                }
                Ok((bytes.to_vec(), bounds))
            }
        }
    }
}

fn read_dictionary_values(
    reader: &mut Reader<'_>,
    dict_count: usize,
    side: SideStream,
) -> Result<Vec<String>, FormatError> {
    let dictionary = DictionaryValues::read(reader, dict_count, side)?;
    Ok(dictionary.entries()?.into_iter().map(str::to_owned).collect())
}

fn decode_dictionary_string(
    reader: &mut Reader<'_>,
    side: SideStream,
    shared: Option<&[String]>,
) -> Result<StringColumn, FormatError> {
    let nulls = NullStream::read(reader)?;
    if side == SideStream::FileScopeDictionary {
        let alphabet = shared.ok_or(FormatError::Structural {
            rule: "shared-scope dictionary block without its file alphabet",
        })?;
        let codes = bitunpack(reader)?;
        return merge_nulls_dictionary(&nulls, &codes, alphabet);
    }
    let dict_count = reader.u32("dictionary count")? as usize;
    let dictionary = DictionaryValues::read(reader, dict_count, side)?;
    let entries = dictionary.entries()?;
    let codes = bitunpack(reader)?;
    merge_nulls_dictionary(&nulls, &codes, &entries)
}

fn merge_nulls_dictionary<T: AsRef<str>>(
    nulls: &NullStream<'_>,
    codes: &[u64],
    entries: &[T],
) -> Result<StringColumn, FormatError> {
    if codes.iter().any(|&code| code >= entries.len() as u64) {
        return Err(FormatError::RefOutOfRange {
            what: "dictionary code",
        });
    }
    if nulls.present_count() != codes.len() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let entry_at = |code: u64| entries.get(code as usize).map_or("", AsRef::as_ref);
    let text_bytes: u64 = codes.iter().map(|&code| entry_at(code).len() as u64).sum();
    let row_count = nulls.row_count();
    let mut text = String::with_capacity(text_bytes as usize);
    let mut offsets = Vec::with_capacity(row_count + 1);
    offsets.push(0u64);
    let mut present = Vec::with_capacity(row_count);
    if matches!(nulls.form, NullStreamForm::AllPresent) {
        present.resize(row_count, true);
        for &code in codes {
            text.push_str(entry_at(code));
            offsets.push(text.len() as u64);
        }
    } else {
        let mut next = codes.iter();
        nulls.each_row(|set| {
            if set {
                let code = next.next().copied().unwrap_or(0);
                text.push_str(entry_at(code));
            }
            offsets.push(text.len() as u64);
            present.push(set);
            Ok(())
        })?;
    }
    Ok(StringColumn::from_parts(offsets, present, text))
}

const FSST_LOSSY_PHT_SIZE: u64 = 1 << 11;

fn fsst_lossy_pht_slot(symbol_word: u64) -> u64 {
    let prefix_3_bytes = symbol_word & 0xFF_FF_FF;
    let hash =
        prefix_3_bytes.wrapping_mul(FSST_LOSSY_PHT_HASH_MULTIPLIER) ^ (prefix_3_bytes >> FSST_LOSSY_PHT_HASH_SHIFT);
    hash & (FSST_LOSSY_PHT_SIZE - 1)
}

fn read_fsst_compressor(reader: &mut Reader<'_>) -> Result<fsst::Compressor, FormatError> {
    Ok(FsstDecodeTable::read(reader)?.compressor())
}

struct FsstArena {
    bounds: Vec<usize>,
    text: String,
}

impl FsstArena {
    fn len(&self) -> usize {
        self.bounds.len().saturating_sub(1)
    }

    fn value(&self, index: usize) -> Option<&str> {
        let (start, end) = (self.bounds.get(index)?, self.bounds.get(index + 1)?);
        self.text.get(*start..*end)
    }
}

fn fsst_decompress_arena(
    table: &FsstDecodeTable,
    data: &[u8],
    offsets: &[usize],
    what: &'static str,
    rule: &'static str,
) -> Result<FsstArena, FormatError> {
    let mut buffer = Vec::new();
    let bounds = fsst_decompress_values_with_table(table, data, offsets, what, rule, &mut buffer)?;
    buffer.shrink_to_fit();
    simdutf8::basic::from_utf8(&buffer).map_err(|_| FormatError::InvalidUtf8 { what })?;
    // SAFETY: validated as UTF-8 just above.
    let text = unsafe { String::from_utf8_unchecked(buffer) };
    Ok(FsstArena { bounds, text })
}

pub(crate) fn with_fsst_arena_bytes<R>(
    compressor: &fsst::Compressor,
    data: &[u8],
    offsets: &[usize],
    what: &'static str,
    rule: &'static str,
    search: impl FnOnce(&[u8], &[usize]) -> R,
) -> Result<R, FormatError> {
    with_arena_buffer(|buffer| {
        let bounds = fsst_decompress_values(compressor, data, offsets, what, rule, buffer)?;
        Ok(search(buffer, &bounds))
    })
}

fn fsst_decompress_values(
    compressor: &fsst::Compressor,
    data: &[u8],
    offsets: &[usize],
    what: &'static str,
    rule: &'static str,
    buffer: &mut Vec<u8>,
) -> Result<Vec<usize>, FormatError> {
    let table = FsstDecodeTable::new(compressor);
    fsst_decompress_values_with_table(&table, data, offsets, what, rule, buffer)
}

fn fsst_decompress_values_with_table(
    table: &FsstDecodeTable,
    data: &[u8],
    offsets: &[usize],
    what: &'static str,
    rule: &'static str,
    buffer: &mut Vec<u8>,
) -> Result<Vec<usize>, FormatError> {
    let Some((&arena_start, ends)) = offsets.split_first() else {
        return Ok(Vec::new());
    };
    if arena_start > data.len() {
        return Err(FormatError::Truncated { what });
    }
    let base = buffer.len();
    let mut bounds = Vec::with_capacity(offsets.len());
    bounds.push(base);
    let codes_end = ends.iter().copied().max().unwrap_or(arena_start).min(data.len());
    buffer.reserve(codes_end.saturating_sub(arena_start) * constant::FSST_MAX_SYMBOL_BYTES);
    let out = buffer.spare_capacity_mut().as_mut_ptr().cast::<u8>();
    let mut written = 0usize;
    let mut value_start = arena_start;
    for &value_end in ends {
        if value_end < value_start {
            return Err(FormatError::Structural { rule });
        }
        let codes = data
            .get(value_start..value_end)
            .ok_or(FormatError::Truncated { what })?;
        // SAFETY: `written` is at most eight times the code bytes decoded so far, and those with `codes` all lie in
        // `data[arena_start..codes_end]`, so `slot` has at least eight bytes per byte of `codes` still spare.
        let slot = unsafe { out.add(written) };
        // SAFETY: `slot` has the room `decode_fsst_codes` asks for, as just shown.
        written += unsafe { table.decode_fsst_codes(codes, slot) }?;
        bounds.push(base + written);
        value_start = value_end;
    }
    // SAFETY: the decodes initialized exactly `written` bytes at the front of the spare capacity.
    unsafe { buffer.set_len(base + written) };
    Ok(bounds)
}

fn fsst_decompress_value(table: &FsstDecodeTable, compressed: &[u8], buffer: &mut Vec<u8>) -> Result<(), FormatError> {
    buffer.reserve(compressed.len() * constant::FSST_MAX_SYMBOL_BYTES);
    let out = buffer.spare_capacity_mut().as_mut_ptr().cast::<u8>();
    // SAFETY: `out` is the front of the spare capacity, which the reserve above sized for these codes.
    let written = unsafe { table.decode_fsst_codes(compressed, out) }?;
    // SAFETY: `decode_fsst_codes` initialized exactly `written` bytes at the front of the spare capacity.
    unsafe { buffer.set_len(buffer.len() + written) };
    Ok(())
}

struct FsstDecodeTable {
    lengths: [u8; 256],
    symbol_count: usize,
    symbols: [u64; 256],
}

#[derive(Clone, Copy)]
struct FsstShortCode {
    code: u8,
    escape: bool,
    len: u8,
}

pub(crate) struct FsstNeedleEncoder {
    has_suffix_code: u8,
    lengths: [u8; 256],
    long_head: [u8; 256],
    long_next: [u8; 256],
    long_slot: [u16; 256],
    one_byte: [FsstShortCode; 256],
    symbols: [u64; 256],
    two_head: [u8; 256],
    two_next: [u8; 256],
}

fn at<T: Copy + Default>(array: &[T; 256], index: u8) -> T {
    array.get(usize::from(index)).copied().unwrap_or_default()
}

fn put<T>(array: &mut [T; 256], index: u8, value: T) {
    if let Some(slot) = array.get_mut(usize::from(index)) {
        *slot = value;
    }
}

fn fsst_symbol_mask(symbol: u64) -> u64 {
    let null_bytes = (symbol.leading_zeros() / 8) as usize;
    u64::MAX >> (64 - 8 * (8 - null_bytes).max(1))
}

impl FsstNeedleEncoder {
    fn new(table: &FsstDecodeTable) -> Self {
        let mut encoder = Self {
            has_suffix_code: 0,
            lengths: table.lengths,
            long_head: [0; 256],
            long_next: [0; 256],
            long_slot: [0; 256],
            one_byte: [FsstShortCode {
                code: fsst::ESCAPE_CODE,
                escape: true,
                len: 1,
            }; 256],
            symbols: table.symbols,
            two_head: [0; 256],
            two_next: [0; 256],
        };
        let mut prefix_head = [0u8; 256];
        let mut prefix_next = [0u8; 256];
        let defined = table
            .symbols
            .iter()
            .copied()
            .zip(table.lengths.iter().copied())
            .take(table.symbol_count.min(FSST_MAX_SYMBOL_COUNT));
        for ((symbol, len), code) in defined.clone().zip(0u8..) {
            let link = code + 1;
            let first = symbol as u8;
            match len {
                1 => put(
                    &mut encoder.one_byte,
                    first,
                    FsstShortCode {
                        code,
                        escape: false,
                        len: 1,
                    },
                ),
                2 => {
                    put(&mut encoder.two_next, code, at(&encoder.two_head, first));
                    put(&mut encoder.two_head, first, link);
                }
                _ => {
                    let slot = fsst_lossy_pht_slot(symbol) as u16;
                    put(&mut encoder.long_slot, code, slot);
                    put(&mut encoder.long_next, code, at(&encoder.long_head, slot as u8));
                    put(&mut encoder.long_head, slot as u8, link);
                    put(&mut prefix_next, code, at(&prefix_head, first));
                    put(&mut prefix_head, first, link);
                }
            }
        }
        for ((symbol, len), code) in defined.zip(0u8..) {
            if len != 2 {
                break;
            }
            let first2 = symbol as u16;
            let mut link = at(&prefix_head, symbol as u8);
            while let Some(other) = link.checked_sub(1) {
                if other >= code && at(&encoder.symbols, other) as u16 == first2 {
                    encoder.has_suffix_code = code;
                    return encoder;
                }
                link = at(&prefix_next, other);
            }
        }
        encoder
    }

    fn two_byte_code(&self, key: u16) -> Option<u8> {
        let mut link = at(&self.two_head, key as u8);
        while let Some(code) = link.checked_sub(1) {
            if at(&self.symbols, code) as u16 == key {
                return Some(code);
            }
            link = at(&self.two_next, code);
        }
        None
    }

    fn long_code(&self, slot: u16) -> Option<(u8, u8, u64)> {
        let mut link = at(&self.long_head, slot as u8);
        while let Some(code) = link.checked_sub(1) {
            if at(&self.long_slot, code) == slot {
                return Some((code, at(&self.lengths, code), at(&self.symbols, code)));
            }
            link = at(&self.long_next, code);
        }
        None
    }

    fn short_code(&self, word: u64) -> FsstShortCode {
        match self.two_byte_code(word as u16) {
            Some(code) => FsstShortCode {
                code,
                escape: false,
                len: 2,
            },
            None => self
                .one_byte
                .get(usize::from(word as u8))
                .copied()
                .unwrap_or(FsstShortCode {
                    code: fsst::ESCAPE_CODE,
                    escape: true,
                    len: 1,
                }),
        }
    }

    pub(crate) fn compress(&self, plaintext: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(plaintext.len() * 2);
        let mut position = 0;
        while let Some(rest) = plaintext.get(position..).filter(|rest| !rest.is_empty()) {
            let mut bytes = [0u8; 8];
            for (slot, byte) in bytes.iter_mut().zip(rest) {
                *slot = *byte;
            }
            let word = u64::from_le_bytes(bytes);
            let short = self.short_code(word);
            if (short.escape || short.code >= self.has_suffix_code)
                && let Some((code, len, symbol)) = self.long_code(fsst_lossy_pht_slot(word) as u16)
                && word & fsst_symbol_mask(symbol) == symbol
            {
                out.push(code);
                position += len as usize;
                continue;
            }
            out.push(short.code);
            if short.escape {
                out.push(bytes[0]);
            }
            position += short.len as usize;
        }
        out
    }
}

impl FsstDecodeTable {
    pub(crate) fn needle_encoder(&self) -> FsstNeedleEncoder {
        FsstNeedleEncoder::new(self)
    }

    fn new(compressor: &fsst::Compressor) -> Self {
        let mut table = Self {
            lengths: [0; 256],
            symbol_count: compressor.symbol_lengths().len(),
            symbols: [0; 256],
        };
        let slots = table.lengths.iter_mut().zip(table.symbols.iter_mut());
        let defined = compressor.symbol_table().iter().zip(compressor.symbol_lengths());
        for ((slot_length, slot_symbol), (symbol, &len)) in slots.zip(defined) {
            *slot_length = len;
            *slot_symbol = symbol.to_u64();
        }
        table
    }

    fn compressor(&self) -> fsst::Compressor {
        let lengths: Vec<u8> = self.lengths.iter().copied().take(self.symbol_count).collect();
        let symbols: Vec<fsst::Symbol> = self
            .symbols
            .iter()
            .take(self.symbol_count)
            .map(|word| fsst::Symbol::from_slice(&word.to_le_bytes()))
            .collect();
        fsst::Compressor::rebuild_from(symbols, lengths)
    }

    fn read(reader: &mut Reader<'_>) -> Result<Self, FormatError> {
        let symbol_count = reader.u16("fsst symbol count")? as usize;
        if symbol_count > FSST_MAX_SYMBOL_COUNT {
            return Err(FormatError::Structural {
                rule: "fsst symbol count beyond 255",
            });
        }
        let mut table = Self {
            lengths: [0; 256],
            symbol_count,
            symbols: [0; 256],
        };
        let mut occupied_long_slots = [0u64; FSST_LOSSY_PHT_SIZE as usize / u64::BITS as usize];
        let mut expected = 2u8;
        for index in 0..symbol_count {
            let word = reader.u64("fsst symbol")?;
            let len = reader.u8("fsst symbol length")?;
            if !(1..=8).contains(&len) {
                return Err(FormatError::Structural {
                    rule: "fsst symbol length outside 1..=8",
                });
            }
            if expected == 1 {
                if len != 1 {
                    return Err(FormatError::Structural {
                        rule: "fsst symbol lengths out of order",
                    });
                }
            } else if len == 1 {
                expected = 1;
            } else if len < expected {
                return Err(FormatError::Structural {
                    rule: "fsst symbol lengths out of order",
                });
            } else {
                expected = len;
            }
            if len >= 3 {
                let slot = fsst_lossy_pht_slot(word) as usize;
                let word_index = slot / u64::BITS as usize;
                let bit = 1u64 << (slot % u64::BITS as usize);
                let occupied = occupied_long_slots.get_mut(word_index).ok_or(FormatError::Structural {
                    rule: "fsst long-symbol slot beyond the decoder hash",
                })?;
                if *occupied & bit != 0 {
                    return Err(FormatError::Structural {
                        rule: "fsst long-symbol table collides in the decoder hash",
                    });
                }
                *occupied |= bit;
            }
            let (Some(length_slot), Some(symbol_slot)) = (table.lengths.get_mut(index), table.symbols.get_mut(index))
            else {
                return Err(FormatError::Structural {
                    rule: "fsst symbol count beyond 255",
                });
            };
            *length_slot = len;
            *symbol_slot = word;
        }
        Ok(table)
    }

    #[inline(always)]
    unsafe fn decode_fsst_codes(&self, codes: &[u8], out: *mut u8) -> Result<usize, FormatError> {
        let mut rest = codes;
        let mut written = 0usize;
        while let Some((chunk, after)) = rest.split_first_chunk::<{ size_of::<u64>() }>() {
            let word = u64::from_le_bytes(*chunk);
            let escapes = fsst_escape_bytes(word);
            if escapes == 0 {
                let mut defined = true;
                for shift in (0..u64::BITS).step_by(u8::BITS as usize) {
                    // SAFETY: as the caller guarantees; `written` is at most eight times the codes consumed so far.
                    defined &= unsafe { self.emit_fsst_symbol((word >> shift) as u8, out, &mut written) };
                }
                if !defined {
                    return Err(FormatError::RefOutOfRange { what: "fsst code" });
                }
                rest = after;
                continue;
            }
            let clean = (escapes.trailing_zeros() / u8::BITS) as usize;
            for shift in (0..clean as u32 * u8::BITS).step_by(u8::BITS as usize) {
                // SAFETY: as the caller guarantees; `written` is at most eight times the codes consumed so far.
                if !unsafe { self.emit_fsst_symbol((word >> shift) as u8, out, &mut written) } {
                    return Err(FormatError::RefOutOfRange { what: "fsst code" });
                }
            }
            let [_escape, literal, after @ ..] = rest.get(clean..).unwrap_or_default() else {
                return Err(FormatError::Truncated {
                    what: "fsst escaped byte",
                });
            };
            // SAFETY: `written` is at most eight times the codes consumed so far, inside the room the caller
            let slot = unsafe { out.add(written) };
            // SAFETY: one byte of the room the caller guarantees.
            unsafe { slot.write(*literal) };
            written = written.wrapping_add(1);
            rest = after;
        }
        while let Some((&code, after)) = rest.split_first() {
            if code == fsst::ESCAPE_CODE {
                let Some((&literal, after)) = after.split_first() else {
                    return Err(FormatError::Truncated {
                        what: "fsst escaped byte",
                    });
                };
                // SAFETY: `written` is at most eight times the codes consumed so far, inside the room the caller
                let slot = unsafe { out.add(written) };
                // SAFETY: one byte of the room the caller guarantees.
                unsafe { slot.write(literal) };
                written = written.wrapping_add(1);
                rest = after;
            } else {
                // SAFETY: as the caller guarantees; `written` is at most eight times the codes consumed so far.
                if !unsafe { self.emit_fsst_symbol(code, out, &mut written) } {
                    return Err(FormatError::RefOutOfRange { what: "fsst code" });
                }
                rest = after;
            }
        }
        Ok(written)
    }

    #[inline(always)]
    unsafe fn emit_fsst_symbol(&self, code: u8, out: *mut u8, written: &mut usize) -> bool {
        let len = self.lengths.get(usize::from(code)).copied().unwrap_or(0);
        let symbol = self.symbols.get(usize::from(code)).copied().unwrap_or(0);
        // SAFETY: the caller guarantees room at `out.add(*written)`.
        let slot = unsafe { out.add(*written) };
        // SAFETY: eight bytes of the room the caller guarantees; only `len` of them count.
        unsafe { slot.cast::<u64>().write_unaligned(symbol) };
        *written = written.wrapping_add(usize::from(len));
        len != 0
    }
}

fn fsst_escape_bytes(word: u64) -> u64 {
    const { assert!(fsst::ESCAPE_CODE == u8::MAX) };
    let inverted = !word;
    inverted.wrapping_sub(0x0101_0101_0101_0101) & !inverted & 0x8080_8080_8080_8080
}

pub(crate) fn with_fsst_selected_bytes<R>(
    compressor: &fsst::Compressor,
    data: &[u8],
    offsets: &[usize],
    selected: &[usize],
    what: &'static str,
    rule: &'static str,
    read: impl FnOnce(&[u8], &[usize]) -> R,
) -> Result<R, FormatError> {
    let table = FsstDecodeTable::new(compressor);
    with_arena_buffer(|buffer| {
        let mut bounds = Vec::with_capacity(selected.len() + 1);
        bounds.push(buffer.len());
        for index in selected {
            let (Some(&start), Some(&end)) = (offsets.get(*index), offsets.get(index + 1)) else {
                return Err(FormatError::RefOutOfRange { what });
            };
            if end < start {
                return Err(FormatError::Structural { rule });
            }
            let compressed = slice(data, start, end - start, what)?;
            fsst_decompress_value(&table, compressed, buffer)?;
            bounds.push(buffer.len());
        }
        Ok(read(buffer, &bounds))
    })
}

fn decode_fsst_string(reader: &mut Reader<'_>) -> Result<StringColumn, FormatError> {
    let nulls = NullStream::read(reader)?;
    let table = FsstDecodeTable::read(reader)?;
    let present_count = reader.u32("fsst present count")? as usize;
    let offsets = read_offsets(reader, present_count)?;
    let data_len = offsets.last().copied().unwrap_or(0);
    let data = reader.take(data_len, "fsst data")?;
    let arena = fsst_decompress_arena(
        &table,
        data,
        &offsets,
        "fsst value",
        "fsst offsets must be non-decreasing",
    )?;
    merge_nulls_arena(&nulls, arena)
}

fn decode_fsst_packed_string(body: &[u8]) -> Result<StringColumn, FormatError> {
    let packed = fsst_packed_values(body)?;
    let mut reader = Reader::new(body);
    let nulls = NullStream::read(&mut reader)?;
    let table = FsstDecodeTable::read(&mut reader)?;
    let arena = fsst_decompress_arena(
        &table,
        packed.arena,
        &packed.offsets,
        "fsst value",
        "fsst offsets must be non-decreasing",
    )?;
    merge_nulls_arena(&nulls, arena)
}

fn merge_nulls_arena(nulls: &NullStream<'_>, arena: FsstArena) -> Result<StringColumn, FormatError> {
    let FsstArena { bounds, text } = arena;
    let count = bounds.len().saturating_sub(1);
    if !bounds.iter().all(|&bound| text.is_char_boundary(bound)) {
        return Err(FormatError::InvalidUtf8 { what: "fsst value" });
    }
    if nulls.present_count() != count {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let row_count = nulls.row_count();
    let mut present = Vec::with_capacity(row_count);
    let offsets: Vec<u64> = match nulls.form {
        NullStreamForm::AllPresent => {
            present.resize(row_count, true);
            if count == 0 {
                vec![0]
            } else {
                bounds.iter().map(|&bound| bound as u64).collect()
            }
        }
        _ => {
            let mut offsets = Vec::with_capacity(row_count + 1);
            offsets.push(0u64);
            let mut next = 0usize;
            nulls.each_row(|set| {
                if set {
                    next += 1;
                }
                offsets.push(bounds.get(next).copied().unwrap_or(0) as u64);
                present.push(set);
                Ok(())
            })?;
            offsets
        }
    };
    Ok(StringColumn::from_parts(offsets, present, text))
}

fn decode_raw_string(reader: &mut Reader<'_>) -> Result<StringColumn, FormatError> {
    let nulls = NullStream::read(reader)?;
    let present_count = reader.u32("raw present count")? as usize;
    let offsets = read_offsets(reader, present_count)?;
    let data_len = offsets.last().copied().unwrap_or(0);
    let data = reader.take(data_len, "raw string data")?;
    let raw = RawValues::validated(data, &offsets)?;
    merge_nulls(
        &nulls,
        offsets.len().saturating_sub(1),
        offsets_text_len(&offsets, data),
        |index| raw.value(&offsets, index),
    )
}

fn relaid_string_column(relaid: string_zstd::Relaid<'_>) -> Result<StringColumn, FormatError> {
    let (nulls, count, values) = match relaid {
        string_zstd::Relaid::Dictionary { nulls, entries, codes } => {
            return merge_nulls_dictionary(&nulls, &codes, &entries);
        }
        string_zstd::Relaid::Raw { nulls, count, values } => (nulls, count, values),
    };
    if nulls.present_count() != count {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let rows = nulls.row_count();
    let mut text = String::with_capacity(values.len().saturating_sub(count));
    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0u64);
    let mut present = Vec::with_capacity(rows);
    let mut walk = string_zstd::TerminatedValues::new(values);
    if matches!(nulls.form, NullStreamForm::AllPresent) {
        present.resize(rows, true);
        for _ in 0..count {
            text.push_str(walk.next_value()?);
            offsets.push(text.len() as u64);
        }
    } else {
        nulls.each_row(|set| {
            if set {
                text.push_str(walk.next_value()?);
            }
            offsets.push(text.len() as u64);
            present.push(set);
            Ok(())
        })?;
    }
    walk.finish()?;
    Ok(StringColumn::from_parts(offsets, present, text))
}

fn relaid_string_views(relaid: string_zstd::Relaid<'_>) -> Result<StringViewArray, FormatError> {
    let (nulls, arena, views) = match relaid {
        string_zstd::Relaid::Dictionary { nulls, entries, codes } => {
            let (arena, bounds) = concatenate(&entries);
            let entry_views = views_over(&arena, &bounds)?;
            let views = dictionary_views_by_row(&nulls, &entry_views, &codes)?;
            (nulls, arena, views)
        }
        string_zstd::Relaid::Raw { nulls, count, values } => {
            let mut arena = Vec::with_capacity(values.len().saturating_sub(count));
            let mut stored = Vec::with_capacity(count);
            let mut walk = string_zstd::TerminatedValues::new(values);
            for _ in 0..count {
                let value = walk.next_value()?.as_bytes();
                let start = u32::try_from(arena.len()).map_err(|_| FormatError::Structural {
                    rule: "string arena exceeds the u32 range a view addresses",
                })?;
                stored.push(make_view(value, 0, start));
                arena.extend_from_slice(value);
            }
            walk.finish()?;
            if u32::try_from(arena.len()).is_err() {
                return Err(FormatError::Structural {
                    rule: "string arena exceeds the u32 range a view addresses",
                });
            }
            let views = views_by_row(&nulls, stored.into_iter())?;
            (nulls, arena, views)
        }
    };
    // SAFETY: every view addresses bytes inside `arena` and describes valid UTF-8. The dictionary arm's entry views
    let array = unsafe {
        StringViewArray::new_unchecked(
            ScalarBuffer::from(views),
            vec![Buffer::from(arena)],
            null_buffer(&nulls),
        )
    };
    Ok(array)
}

pub fn decode_string_block_views(pipeline: PipelineId, bytes: &[u8]) -> Result<Option<StringViewArray>, FormatError> {
    decode_string_block_views_shared(pipeline, bytes, None)
}

pub fn decode_string_block_views_shared(
    pipeline: PipelineId,
    bytes: &[u8],
    shared: Option<&[String]>,
) -> Result<Option<StringViewArray>, FormatError> {
    decode_string_block_views_shared_prepared(pipeline, bytes, shared, None)
}

#[derive(Debug, Clone)]
pub(crate) struct SharedStringViewDictionary {
    arena: Buffer,
    entry_views: Vec<u128>,
}

pub(crate) fn prepare_shared_string_view_dictionary(
    shared: &[String],
) -> Result<SharedStringViewDictionary, FormatError> {
    let mut arena = Vec::with_capacity(view_arena_len(shared.iter().map(String::len))?);
    let mut entry_views = Vec::with_capacity(shared.len());
    for value in shared {
        entry_views.push(make_view(value.as_bytes(), 0, arena.len() as u32));
        arena.extend_from_slice(value.as_bytes());
    }
    Ok(SharedStringViewDictionary {
        arena: Buffer::from(arena),
        entry_views,
    })
}

fn view_arena_len(lens: impl IntoIterator<Item = usize>) -> Result<usize, FormatError> {
    lens.into_iter()
        .try_fold(0usize, usize::checked_add)
        .filter(|&total| u32::try_from(total).is_ok())
        .ok_or(FormatError::Structural {
            rule: "string arena larger than a view offset can address",
        })
}

pub(crate) fn decode_string_block_views_shared_prepared(
    pipeline: PipelineId,
    bytes: &[u8],
    shared: Option<&[String]>,
    prepared: Option<&SharedStringViewDictionary>,
) -> Result<Option<StringViewArray>, FormatError> {
    if pipeline.value_kind()? != ValueKind::String {
        return Ok(None);
    }
    let transform = pipeline.transform()?;
    if !matches!(
        transform,
        Transform::DictionaryString | Transform::FsstString | Transform::FsstPackedString | Transform::RawString
    ) {
        return Ok(None);
    }
    if pipeline.compression()? == Compression::StringZstd
        && let Some(array) = string_zstd::with_relaid(transform, pipeline.side_stream()?, bytes, relaid_string_views)?
    {
        return Ok(Some(array));
    }
    with_trailing_removed(pipeline.compression()?, bytes, |body| {
        decode_string_block_views_with_body(pipeline, transform, body, shared, prepared)
    })
}

fn decode_string_block_views_with_body(
    pipeline: PipelineId,
    transform: Transform,
    body: &[u8],
    shared: Option<&[String]>,
    prepared: Option<&SharedStringViewDictionary>,
) -> Result<Option<StringViewArray>, FormatError> {
    let packed = match transform {
        Transform::FsstPackedString => Some(fsst_packed_values(body)?),
        _ => None,
    };
    let mut reader = Reader::new(body);
    let nulls = NullStream::read(&mut reader)?;

    let (arena, views): (Buffer, Vec<u128>) = match transform {
        Transform::DictionaryString => {
            let side = pipeline.side_stream()?;
            if side == SideStream::FileScopeDictionary {
                let owned_dictionary;
                let dictionary = match prepared {
                    Some(dictionary) => dictionary,
                    None => {
                        let alphabet = shared.ok_or(FormatError::Structural {
                            rule: "shared-scope dictionary block without its file alphabet",
                        })?;
                        owned_dictionary = prepare_shared_string_view_dictionary(alphabet)?;
                        &owned_dictionary
                    }
                };
                let codes = bitunpack(&mut reader)?;
                (
                    dictionary.arena.clone(),
                    dictionary_views_by_row(&nulls, &dictionary.entry_views, &codes)?,
                )
            } else {
                let dict_count = reader.u32("dictionary count")? as usize;
                let (arena, bounds) = DictionaryValues::read(&mut reader, dict_count, side)?.into_view_arena()?;
                let entry_views = views_over(&arena, &bounds)?;
                let codes = bitunpack(&mut reader)?;
                (
                    Buffer::from(arena),
                    dictionary_views_by_row(&nulls, &entry_views, &codes)?,
                )
            }
        }
        Transform::FsstString | Transform::FsstPackedString => {
            let table = FsstDecodeTable::read(&mut reader)?;
            let (offsets, data) = match packed {
                Some(packed) => (packed.offsets, packed.arena),
                None => {
                    let present_count = reader.u32("fsst present count")? as usize;
                    let offsets = read_offsets(&mut reader, present_count)?;
                    let data_len = offsets.last().copied().unwrap_or(0);
                    (offsets, reader.take(data_len, "fsst data")?)
                }
            };
            let mut arena = Vec::new();
            let bounds = fsst_decompress_values_with_table(
                &table,
                data,
                &offsets,
                "fsst value",
                "fsst offsets must be non-decreasing",
                &mut arena,
            )?;
            let views = string_views_by_row(&nulls, &arena, &bounds, "fsst value")?;
            arena.shrink_to_fit();
            (Buffer::from(arena), views)
        }
        Transform::RawString => {
            let present_count = reader.u32("raw present count")? as usize;
            let offsets = read_offsets(&mut reader, present_count)?;
            let data_len = offsets.last().copied().unwrap_or(0);
            let data = reader.take(data_len, "raw string data")?;
            let views = string_views_by_row(&nulls, data, &offsets, "raw string value")?;
            (Buffer::from(data.to_vec()), views)
        }
        _ => unreachable!("transform was screened to a string encoding above"),
    };

    // SAFETY: `new_unchecked` needs every view to address bytes inside the data buffer and to describe valid UTF-8.
    let array = unsafe { StringViewArray::new_unchecked(ScalarBuffer::from(views), vec![arena], null_buffer(&nulls)) };
    Ok(Some(array))
}

fn string_views_by_row(
    nulls: &NullStream,
    arena: &[u8],
    bounds: &[usize],
    what: &'static str,
) -> Result<Vec<u128>, FormatError> {
    let text = simdutf8::basic::from_utf8(arena).map_err(|_| FormatError::InvalidUtf8 { what })?;
    check_value_bounds(text, bounds)?;
    let views = bounds.windows(2).map(|pair| {
        let &[start, end] = pair else { return 0 };
        make_view(arena.get(start..end).unwrap_or_default(), 0, start as u32)
    });
    views_by_row(nulls, views)
}

fn check_value_bounds(text: &str, bounds: &[usize]) -> Result<(), FormatError> {
    let mut previous = 0;
    for &bound in bounds {
        if bound < previous {
            return Err(FormatError::Structural {
                rule: "string offsets must be non-decreasing",
            });
        }
        if bound > text.len() {
            return Err(FormatError::Truncated { what: "string value" });
        }
        if !text.is_char_boundary(bound) {
            return Err(FormatError::InvalidUtf8 { what: "string value" });
        }
        previous = bound;
    }
    if u32::try_from(previous).is_err() {
        return Err(FormatError::Structural {
            rule: "string arena larger than a view offset can address",
        });
    }
    Ok(())
}

fn dictionary_views_by_row(nulls: &NullStream, entries: &[u128], codes: &[u64]) -> Result<Vec<u128>, FormatError> {
    if codes.iter().any(|&code| code >= entries.len() as u64) {
        return Err(FormatError::RefOutOfRange {
            what: "dictionary code",
        });
    }
    let views = codes
        .iter()
        .map(|&code| entries.get(code as usize).copied().unwrap_or(0));
    views_by_row(nulls, views)
}

fn concatenate(values: &[&str]) -> (Vec<u8>, Vec<usize>) {
    let mut bounds = Vec::with_capacity(values.len() + 1);
    bounds.push(0);
    let mut arena = Vec::with_capacity(values.iter().map(|value| value.len()).sum());
    for value in values {
        arena.extend_from_slice(value.as_bytes());
        bounds.push(arena.len());
    }
    (arena, bounds)
}

fn views_over(arena: &[u8], bounds: &[usize]) -> Result<Vec<u128>, FormatError> {
    let Some((_, ends)) = bounds.split_first() else {
        return Ok(Vec::new());
    };
    if bounds
        .windows(2)
        .any(|pair| matches!(pair, [start, end] if end < start))
    {
        return Err(FormatError::Structural {
            rule: "string offsets must be non-decreasing",
        });
    }
    let last = bounds.last().copied().unwrap_or(0);
    if last > arena.len() {
        return Err(FormatError::Truncated { what: "string value" });
    }
    if u32::try_from(last).is_err() {
        return Err(FormatError::Structural {
            rule: "string arena larger than a view offset can address",
        });
    }
    let mut views = Vec::with_capacity(ends.len());
    views.extend(
        bounds
            .iter()
            .zip(ends)
            .map(|(&start, &end)| make_view(arena.get(start..end).unwrap_or_default(), 0, start as u32)),
    );
    Ok(views)
}

fn views_by_row(nulls: &NullStream, mut values: impl ExactSizeIterator<Item = u128>) -> Result<Vec<u128>, FormatError> {
    if values.len() != nulls.present_count() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let rows = nulls.row_count();
    Ok(match &nulls.form {
        NullStreamForm::AllAbsent => vec![0; rows],
        NullStreamForm::AllPresent => values.collect(),
        NullStreamForm::Raw(bits) => {
            let mut views = vec![0u128; rows];
            for (row, slot) in views.iter_mut().enumerate() {
                if bits.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0) {
                    *slot = values.next().unwrap_or(0);
                }
            }
            views
        }
        NullStreamForm::Runs(runs) => {
            let mut views = vec![0u128; rows];
            for (start, end) in runs.iter() {
                for slot in views.get_mut(start as usize..end as usize).into_iter().flatten() {
                    *slot = values.next().unwrap_or(0);
                }
            }
            views
        }
    })
}

fn null_buffer(nulls: &NullStream) -> Option<NullBuffer> {
    match &nulls.form {
        NullStreamForm::AllPresent => None,
        NullStreamForm::AllAbsent => Some(NullBuffer::new_null(nulls.row_count())),
        NullStreamForm::Raw(bits) => Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from_slice_ref(bits),
            0,
            nulls.row_count(),
        ))),
        NullStreamForm::Runs(runs) => {
            let mut builder = BooleanBufferBuilder::new(nulls.row_count());
            for (start, end) in runs.iter() {
                let (start, end) = (start as usize, end as usize);
                if builder.len() < start {
                    builder.append_n(start - builder.len(), false);
                }
                if end > start {
                    builder.append_n(end - start, true);
                }
            }
            if builder.len() < nulls.row_count() {
                builder.append_n(nulls.row_count() - builder.len(), false);
            }
            Some(NullBuffer::new(builder.finish()))
        }
    }
}

fn shared_alphabet_covers(shared: Option<&[String]>, present: &[&str]) -> bool {
    let Some(alphabet) = shared else {
        return false;
    };
    let mut seen: AHashSet<&str> = AHashSet::new();
    present
        .iter()
        .all(|value| !seen.insert(value) || alphabet.binary_search_by(|entry| entry.as_str().cmp(value)).is_ok())
}

fn dictionary_trial_max_distinct(present_count: usize) -> usize {
    present_count * DICTIONARY_TRIAL_MAX_DISTINCT_NUM / DICTIONARY_TRIAL_MAX_DISTINCT_DEN
}

#[derive(Default)]
struct StringTrial<'a> {
    dictionary: Option<Option<PreparedDictionary<'a>>>,
    present: Option<Vec<&'a str>>,
}

fn choose_string_transform(present: &[&str]) -> Transform {
    if present.is_empty() {
        return Transform::RawString;
    }
    let mut distinct: Vec<&str> = transform_sample(present).unwrap_or_else(|| present.to_vec());
    let sample_len = distinct.len();
    let average_len = distinct.iter().map(|value| value.len()).sum::<usize>() / sample_len.max(1);
    distinct.sort_unstable();
    distinct.dedup();
    if distinct.len() * DICTIONARY_MAX_CARDINALITY_DIVISOR <= sample_len {
        Transform::DictionaryString
    } else if average_len <= FSST_MAX_AVERAGE_VALUE_LEN && sample_len >= FSST_MIN_SAMPLE_COUNT {
        Transform::FsstString
    } else {
        Transform::RawString
    }
}

fn min_max<T: Copy + PartialOrd>(values: &[T]) -> Option<(T, T)> {
    let mut iter = values.iter().copied();
    let first = iter.next()?;
    Some(iter.fold((first, first), |(min, max), v| {
        (if v < min { v } else { min }, if v > max { v } else { max })
    }))
}

fn min_max_f64(values: &[f64]) -> Option<(f64, f64)> {
    let mut iter = values.iter().copied();
    let first = iter.next()?;
    Some(iter.fold((first, first), |(min, max), v| (f64::min(min, v), f64::max(max, v))))
}

fn i64_stats(values: &[i64]) -> BlockStats {
    let bounds = min_max(values);
    BlockStats {
        row_count: values.len() as u32,
        null_count: 0,
        min_i128: bounds.map(|(min, _)| i128::from(min)),
        max_i128: bounds.map(|(_, max)| i128::from(max)),
        min_f64: None,
        max_f64: None,
    }
}

fn stats_for(data: &ColumnData) -> BlockStats {
    let mut stats = BlockStats {
        row_count: data.row_count() as u32,
        null_count: 0,
        min_i128: None,
        max_i128: None,
        min_f64: None,
        max_f64: None,
    };
    match data {
        ColumnData::U64(values) => {
            if let Some((min, max)) = min_max(values) {
                stats.min_i128 = Some(i128::from(min));
                stats.max_i128 = Some(i128::from(max));
            }
        }
        ColumnData::I64(values) => return i64_stats(values),
        ColumnData::U128(values) => {
            if let Some((min, max)) = min_max(values)
                && let (Ok(min), Ok(max)) = (i128::try_from(min), i128::try_from(max))
            {
                stats.min_i128 = Some(min);
                stats.max_i128 = Some(max);
            }
        }
        ColumnData::Decimal { values, .. } => {
            if let Some((min, max)) = min_max(values) {
                stats.min_i128 = Some(min);
                stats.max_i128 = Some(max);
            }
        }
        ColumnData::F64(values) => {
            if let Some((min, max)) = min_max_f64(values) {
                stats.min_f64 = Some(min);
                stats.max_f64 = Some(max);
            }
        }
        ColumnData::Strings(values) => {
            stats.null_count = values.null_count() as u32;
        }
    }
    stats
}

const TRAILING_FULL_TRIAL_MAX_BYTES: usize = 1 << 15;

const TRAILING_SAMPLE_WINDOWS: usize = 8;

const TRAILING_SAMPLE_WINDOW_BYTES: usize = 2048;

#[cfg(test)]
fn pick_trailing(bytes: Vec<u8>, threshold: usize, lz4: Vec<u8>, zstd_framed: Vec<u8>) -> (Compression, Vec<u8>) {
    pick_trailing_candidate(threshold, lz4, zstd_framed).unwrap_or((Compression::None, bytes))
}

fn pick_trailing_candidate(threshold: usize, lz4: Vec<u8>, zstd_framed: Vec<u8>) -> Option<(Compression, Vec<u8>)> {
    let lz4_wins = !lz4.is_empty() && lz4.len() < threshold;
    let zstd_wins = !zstd_framed.is_empty() && zstd_framed.len() < threshold && zstd_framed.len() < lz4.len();
    if zstd_wins {
        Some((Compression::Zstd1, zstd_framed))
    } else if lz4_wins {
        Some((Compression::Lz4, lz4))
    } else {
        None
    }
}

thread_local! {
    static TRAILING_SAMPLE_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn with_trailing_sample<R>(bytes: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
    TRAILING_SAMPLE_SCRATCH.with(|cell| {
        let mut sample = cell.borrow_mut();
        sample.clear();
        let window = TRAILING_SAMPLE_WINDOW_BYTES.min(bytes.len());
        let span = bytes.len().saturating_sub(window);
        for i in 0..TRAILING_SAMPLE_WINDOWS {
            let start = if TRAILING_SAMPLE_WINDOWS > 1 {
                span * i / (TRAILING_SAMPLE_WINDOWS - 1)
            } else {
                0
            };
            if let Some(window_bytes) = bytes.get(start..start + window) {
                sample.extend_from_slice(window_bytes);
            }
        }
        f(&sample)
    })
}

thread_local! {
    static ZSTD_ENCODER: RefCell<ZstdCompressor<'static>> = RefCell::new(ZstdCompressor::default());

    static ZSTD_FRAME_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn compress_zstd_into(bytes: &[u8], level: i32, out: &mut Vec<u8>) {
    out.clear();
    out.reserve(compress_bound(bytes.len()));
    ZSTD_ENCODER.with(|slot| {
        let mut encoder = slot.borrow_mut();
        let compressed = encoder
            .set_compression_level(level)
            .and_then(|()| encoder.compress_to_buffer(bytes, out));
        if compressed.is_err() {
            out.clear();
        }
    });
}

pub(crate) fn compress_zstd(bytes: &[u8], level: i32) -> Vec<u8> {
    let mut out = Vec::new();
    compress_zstd_into(bytes, level, &mut out);
    out
}

fn compress_zstd_framed(bytes: &[u8], level: i32) -> Vec<u8> {
    ZSTD_FRAME_SCRATCH.with(|cell| {
        let mut frame = cell.borrow_mut();
        compress_zstd_into(bytes, level, &mut frame);
        if frame.is_empty() {
            return Vec::new();
        }
        let mut framed = Vec::with_capacity(ZSTD_LENGTH_PREFIX_BYTES + frame.len());
        framed.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        framed.extend_from_slice(&frame);
        framed
    })
}

fn trailing_threshold(strategy: CascadeStrategy, len: usize) -> Option<usize> {
    match strategy {
        CascadeStrategy::DecodeOptimized => Some(len * DECODE_OPTIMIZED_THRESHOLD_NUM / DECODE_OPTIMIZED_THRESHOLD_DEN),
        CascadeStrategy::NoTrailing => None,
        CascadeStrategy::SizeOptimized => Some(len - len / SIZE_OPTIMIZED_SAVINGS_DIVISOR),
    }
}

fn trailing_candidate(bytes: &[u8], strategy: CascadeStrategy) -> Option<(Compression, Vec<u8>)> {
    let threshold = trailing_threshold(strategy, bytes.len())?;
    if bytes.len() <= TRAILING_FULL_TRIAL_MAX_BYTES {
        let lz4 = lz4_flex::compress_prepend_size(bytes);
        let zstd_framed = compress_zstd_framed(bytes, 1);
        return pick_trailing_candidate(threshold, lz4, zstd_framed);
    }
    let (sample_lz4, sample_zstd) = with_trailing_sample(bytes, |sample| {
        (
            lz4_flex::compress_prepend_size(sample).len(),
            compress_zstd(sample, 1).len(),
        )
    });
    if sample_zstd < sample_lz4 {
        let framed = compress_zstd_framed(bytes, 1);
        (!framed.is_empty() && framed.len() < threshold).then_some((Compression::Zstd1, framed))
    } else {
        let lz4 = lz4_flex::compress_prepend_size(bytes);
        (!lz4.is_empty() && lz4.len() < threshold).then_some((Compression::Lz4, lz4))
    }
}

fn apply_trailing(bytes: Vec<u8>, strategy: CascadeStrategy) -> (Compression, Vec<u8>) {
    match trailing_candidate(&bytes, strategy) {
        Some(compressed) => compressed,
        None => (Compression::None, bytes),
    }
}

fn captured_trailing_candidate(
    body: &[u8],
    captured: Compression,
    strategy: CascadeStrategy,
) -> Option<(Compression, Vec<u8>)> {
    let threshold = trailing_threshold(strategy, body.len())?;
    match captured {
        Compression::Lz4 => {
            let lz4 = lz4_flex::compress_prepend_size(body);
            (!lz4.is_empty() && lz4.len() < threshold).then_some((Compression::Lz4, lz4))
        }
        Compression::Zstd1 | Compression::Zstd3 => {
            let level = if captured == Compression::Zstd3 { 3 } else { 1 };
            let framed = compress_zstd_framed(body, level);
            (!framed.is_empty() && framed.len() < threshold).then_some((captured, framed))
        }
        Compression::Deflate | Compression::SeekableZstd | Compression::StringZstd | Compression::None => None,
    }
}

fn apply_captured_trailing(body: Vec<u8>, captured: Compression, strategy: CascadeStrategy) -> (Compression, Vec<u8>) {
    match captured_trailing_candidate(&body, captured, strategy) {
        Some(compressed) => compressed,
        None => (Compression::None, body),
    }
}

fn apply_seekable_frames(
    transform: Transform,
    body: Vec<u8>,
    strategy: CascadeStrategy,
    frame_breaks: &[usize],
) -> (Compression, Vec<u8>) {
    if !transform.keeps_range_access_under_framing() {
        return (Compression::None, body);
    }
    let Some(threshold) = trailing_threshold(strategy, body.len()) else {
        return (Compression::None, body);
    };
    let (codes, threshold) = if transform == Transform::FsstString && body.len() >= FSST_PER_FRAME_BAR_MIN_BLOCK_BYTES {
        (0..frame_breaks.first().copied().unwrap_or(body.len()), body.len())
    } else {
        (0..0, threshold)
    };
    let bar_at = |frame_start: usize| {
        if codes.contains(&frame_start) {
            (FSST_TRAILING_MAX_RATIO_NUM, FSST_TRAILING_MAX_RATIO_DEN)
        } else {
            seekable_zstd::EVERY_FRAME_COMPRESSED
        }
    };
    let Some(candidate) = seekable_zstd::compress_with_breaks_by(&body, frame_breaks, bar_at) else {
        return (Compression::None, body);
    };
    if candidate.len() < threshold {
        (Compression::SeekableZstd, candidate)
    } else {
        (Compression::None, body)
    }
}

pub fn remove_trailing(compression: Compression, bytes: &[u8]) -> Result<Cow<'_, [u8]>, FormatError> {
    match compression {
        Compression::None => Ok(Cow::Borrowed(bytes)),
        other => Ok(Cow::Owned(
            decompressor::active_decompressor().decompress(other, bytes)?,
        )),
    }
}

pub fn remove_trailing_into<'a>(
    compression: Compression,
    bytes: &'a [u8],
    scratch: &'a mut Vec<u8>,
) -> Result<&'a [u8], FormatError> {
    match compression {
        Compression::None => Ok(bytes),
        other => {
            decompressor::active_decompressor().decompress_into(other, bytes, scratch)?;
            Ok(scratch)
        }
    }
}

pub fn with_trailing_removed<R>(
    compression: Compression,
    bytes: &[u8],
    consume: impl FnOnce(&[u8]) -> Result<R, FormatError>,
) -> Result<R, FormatError> {
    if compression == Compression::None {
        return consume(bytes);
    }
    decompressor::with_inflate_buffer(|scratch| consume(remove_trailing_into(compression, bytes, scratch)?))
}

pub fn encode_occurred_residual(ingested: &[i64], occurred: &[i64], strategy: CascadeStrategy) -> Option<EncodedBlock> {
    if ingested.is_empty() || ingested.len() != occurred.len() {
        return None;
    }
    let residuals: Vec<i64> = ingested
        .iter()
        .zip(occurred)
        .map(|(ingested, occurred)| ingested.wrapping_sub(*occurred))
        .collect();
    let inner = encode_block_with_strategy(&ColumnData::I64(residuals), false, strategy);
    let mut out = Writer::with_capacity(4 + inner.bytes.len());
    out.put_u32(inner.pipeline.0);
    out.put_slice(&inner.bytes);
    Some(EncodedBlock {
        bytes: out.into_bytes(),
        fsst: None,
        pipeline: PipelineId::new(Transform::OccurredResidual, Compression::None, ValueKind::I64),
        stats: i64_stats(ingested),
        uncompressed_len: 4 + inner.uncompressed_len,
    })
}

pub fn occurred_residual_inner(body: &[u8]) -> Result<(PipelineId, &[u8]), FormatError> {
    let mut reader = Reader::new(body);
    let inner = PipelineId(reader.u32("occurred residual inner pipeline")?);
    let rest = reader.take(reader.remaining(), "occurred residual inner block")?;
    let valid = inner.value_kind()? == ValueKind::I64
        && inner.side_stream()? == SideStream::None
        && matches!(
            inner.transform()?,
            Transform::ForBitpack | Transform::DeltaBitpack | Transform::Rle | Transform::PlainU64
        );
    inner.compression()?;
    if !valid {
        return Err(FormatError::Structural {
            rule: "occurred residual inner block must be a plain i64 integer block",
        });
    }
    Ok((inner, rest))
}

pub fn resolve_occurred_residual(body: &[u8], occurred: &[i64]) -> Result<(PipelineId, Vec<u8>), FormatError> {
    let (inner, rest) = occurred_residual_inner(body)?;
    let ColumnData::I64(mut values) = decode_block(inner, rest)? else {
        return Err(FormatError::Structural {
            rule: "occurred residual inner block must decode to i64",
        });
    };
    if values.len() != occurred.len() {
        return Err(FormatError::Structural {
            rule: "occurred residual count must equal the granule's occurred_at count",
        });
    }
    for (value, occurred) in values.iter_mut().zip(occurred) {
        *value = value.wrapping_add(*occurred);
    }
    let mut out = Writer::with_capacity(4 + values.len() * 8);
    encode_i64_with(Transform::PlainU64, &values, &mut out);
    Ok((
        PipelineId::new(Transform::PlainU64, Compression::None, ValueKind::I64),
        out.into_bytes(),
    ))
}

pub fn encode_block(data: &ColumnData, random_access: bool) -> EncodedBlock {
    encode_block_with_strategy(data, random_access, CascadeStrategy::DecodeOptimized)
}

pub fn encode_block_with_strategy(data: &ColumnData, random_access: bool, strategy: CascadeStrategy) -> EncodedBlock {
    encode_block_inner(data, random_access, strategy, None, None, None, None, false)
}

pub fn encode_block_with_shared_dictionary(
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
) -> EncodedBlock {
    encode_block_inner(data, random_access, strategy, None, None, None, shared, false)
}

pub fn encode_block_with_xor_floats(data: &ColumnData, random_access: bool, strategy: CascadeStrategy) -> EncodedBlock {
    let selected = encode_block_inner(data, random_access, strategy, None, None, None, None, false);
    if !matches!(data, ColumnData::F64(_)) {
        return selected;
    }
    let xor = encode_chosen_form(
        data,
        random_access,
        strategy,
        Some(Transform::Chimp),
        None,
        None,
        None,
        false,
    );
    if xor.bytes.len() < selected.bytes.len() {
        xor
    } else {
        selected
    }
}

#[derive(Clone)]
pub struct FsstTable(fsst::Compressor);

impl std::fmt::Debug for FsstTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsstTable")
            .field("symbols", &self.0.symbol_table().len())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ReplayCapture {
    pub compression: Compression,
    pub decoded_len: u64,
    pub fsst: Option<FsstTable>,
    pub raw_len: u64,
    pub transform: Transform,
}

impl ReplayCapture {
    pub fn from_head(
        data: &ColumnData,
        pipeline: PipelineId,
        decoded_len: u64,
        fsst: Option<FsstTable>,
    ) -> Option<Self> {
        let transform = pipeline.transform().ok()?;
        if !transform_matches_kind(transform, data) {
            return None;
        }
        Some(Self {
            compression: pipeline.compression().ok()?,
            decoded_len,
            fsst: fsst.filter(|_| matches!(transform, Transform::FsstString | Transform::FsstPackedString)),
            raw_len: raw_value_bytes(data).max(1),
            transform,
        })
    }
}

fn transform_matches_kind(transform: Transform, data: &ColumnData) -> bool {
    match data {
        ColumnData::U64(_) | ColumnData::I64(_) => matches!(
            transform,
            Transform::DeltaBitpack | Transform::ForBitpack | Transform::PlainU64 | Transform::Rle
        ),
        ColumnData::Strings(_) => matches!(
            transform,
            Transform::DictionaryString | Transform::FsstString | Transform::FsstPackedString | Transform::RawString
        ),
        ColumnData::Decimal { .. } | ColumnData::F64(_) | ColumnData::U128(_) => false,
    }
}

fn raw_value_bytes(data: &ColumnData) -> u64 {
    match data {
        ColumnData::U64(values) => (values.len() * 8) as u64,
        ColumnData::I64(values) => (values.len() * 8) as u64,
        ColumnData::F64(values) => (values.len() * 8) as u64,
        ColumnData::U128(values) => (values.len() * 16) as u64,
        ColumnData::Decimal { values, .. } => (values.len() * 16) as u64,
        ColumnData::Strings(values) => (values.len() + values.text_len()) as u64,
    }
}

pub fn encode_block_replayed(
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    capture: Option<&ReplayCapture>,
    shared: Option<&[String]>,
) -> (EncodedBlock, bool) {
    encode_block_replayed_with(data, random_access, strategy, capture, shared, false)
}

pub fn encode_block_replayed_with(
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    capture: Option<&ReplayCapture>,
    shared: Option<&[String]>,
    string_zstd: bool,
) -> (EncodedBlock, bool) {
    if let Some(capture) = capture
        && transform_matches_kind(capture.transform, data)
    {
        let block = encode_block_inner(
            data,
            random_access,
            strategy,
            Some(capture.transform),
            Some(capture.compression),
            capture.fsst.as_ref(),
            shared,
            string_zstd,
        );
        let raw = raw_value_bytes(data).max(1);
        if u128::from(block.uncompressed_len) * u128::from(capture.raw_len) * u128::from(REPLAY_TRIP_DEN)
            <= u128::from(capture.decoded_len) * u128::from(raw) * u128::from(REPLAY_TRIP_NUM)
            && replay_within_plain_form(&block, data, random_access, strategy, shared)
        {
            return (block, true);
        }
    }
    (
        encode_block_inner(data, random_access, strategy, None, None, None, shared, string_zstd),
        false,
    )
}

fn plain_form(
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
) -> EncodedBlock {
    let transform = match data {
        ColumnData::Strings(_) => Transform::RawString,
        _ => Transform::PlainU64,
    };
    encode_chosen_form(
        data,
        random_access,
        strategy,
        Some(transform),
        Some(Compression::None),
        None,
        shared,
        false,
    )
}

fn replay_within_plain_form(
    block: &EncodedBlock,
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
) -> bool {
    if block.bytes.len() as u64 <= raw_value_bytes(data) {
        return true;
    }
    block.bytes.len() <= plain_form(data, random_access, strategy, shared).bytes.len()
}

pub fn encode_block_forced_for_conformance(
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    transform: Transform,
) -> Result<EncodedBlock, FormatError> {
    let mandatory = matches!(
        (data, transform),
        (ColumnData::Decimal { .. }, Transform::Decimal128)
            | (ColumnData::U128(_), Transform::PlainU128)
            | (
                ColumnData::F64(_),
                Transform::Alp | Transform::AlpRd | Transform::ByteStreamSplit | Transform::Chimp | Transform::PlainF64
            )
    );
    if !mandatory && !transform_matches_kind(transform, data) {
        return Err(FormatError::Structural {
            rule: "forced transform is not valid for the block's value kind",
        });
    }
    Ok(encode_block_inner(
        data,
        random_access,
        strategy,
        Some(transform),
        None,
        None,
        None,
        false,
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent selection input threaded from the public entry points"
)]
fn encode_block_inner(
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    forced: Option<Transform>,
    replay_trailing: Option<Compression>,
    replay_fsst: Option<&FsstTable>,
    shared: Option<&[String]>,
    string_zstd: bool,
) -> EncodedBlock {
    let block = encode_chosen_form(
        data,
        random_access,
        strategy,
        forced,
        replay_trailing,
        replay_fsst,
        shared,
        string_zstd,
    );
    if forced.is_some() || replay_trailing.is_some() || block.bytes.len() as u64 <= raw_value_bytes(data) {
        return block;
    }
    let plain = plain_form(data, random_access, strategy, shared);
    if plain.bytes.len() < block.bytes.len() {
        plain
    } else {
        block
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent selection input threaded from the public entry points"
)]
fn encode_chosen_form(
    data: &ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    forced: Option<Transform>,
    replay_trailing: Option<Compression>,
    replay_fsst: Option<&FsstTable>,
    shared: Option<&[String]>,
    string_zstd: bool,
) -> EncodedBlock {
    let mut trial = StringTrial::default();
    let block = encode_selected_form(
        data,
        random_access,
        strategy,
        forced,
        replay_trailing,
        replay_fsst,
        shared,
        &mut trial,
    );
    if string_zstd
        && let ColumnData::Strings(values) = data
        && (forced.is_none() || replay_trailing.is_some_and(|captured| captured != Compression::None))
        && let Some(candidate) = string_zstd_candidate(values, strategy, shared, block.bytes.len(), block.stats, trial)
    {
        return candidate;
    }
    block
}

fn string_zstd_candidate<'a>(
    values: &'a StringColumn,
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
    current_len: usize,
    stats: BlockStats,
    trial: StringTrial<'a>,
) -> Option<EncodedBlock> {
    if strategy == CascadeStrategy::NoTrailing || !cascade_level_fits(2) {
        return None;
    }
    let present = trial.present.unwrap_or_else(|| present_values(values));
    if present.is_empty() {
        return None;
    }
    let bar = current_len.saturating_mul(STRING_ZSTD_MIN_SAVINGS_NUM) / STRING_ZSTD_MIN_SAVINGS_DEN;
    let mut best: Option<(Transform, SideStream, Compression, Vec<u8>, u64)> = None;
    let mut consider = |transform: Transform, side: SideStream, body: &[u8]| {
        let keeps =
            |len: usize| len != 0 && len < bar && best.as_ref().is_none_or(|(_, _, _, kept, _)| len < kept.len());
        let (compression, stored) = match string_zstd::compress_if(transform, side, body, keeps) {
            string_zstd::Relayout::Kept(stored) => (Compression::StringZstd, stored),
            string_zstd::Relayout::Refused => return,
            string_zstd::Relayout::Unavailable => {
                let stored = lz4_flex::compress_prepend_size(body);
                if !keeps(stored.len()) {
                    return;
                }
                (Compression::Lz4, stored)
            }
        };
        best = Some((transform, side, compression, stored, body.len() as u64));
    };
    let mut raw = Writer::new();
    if encode_raw_string(values, &present, &mut raw).is_ok() {
        consider(Transform::RawString, SideStream::None, raw.bytes());
    }
    let max_distinct = dictionary_trial_max_distinct(present.len());
    let prepared = match trial.dictionary {
        Some(prepared) => prepared.filter(|prepared| prepared.dictionary.len() <= max_distinct),
        None => prepare_dictionary_within(&present, max_distinct),
    };
    if let Some(prepared) = prepared {
        let mut dictionary = Writer::new();
        if let Ok(side) = encode_prepared_dictionary(values, &prepared, strategy, shared, false, &mut dictionary) {
            consider(Transform::DictionaryString, side, dictionary.bytes());
        }
    }
    let (transform, side, compression, bytes, uncompressed_len) = best?;
    Some(EncodedBlock {
        pipeline: PipelineId::new(transform, compression, ValueKind::String).with_side_stream(side),
        bytes,
        fsst: None,
        stats,
        uncompressed_len,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent selection input threaded from the public entry points"
)]
fn encode_selected_form<'a>(
    data: &'a ColumnData,
    random_access: bool,
    strategy: CascadeStrategy,
    forced: Option<Transform>,
    replay_trailing: Option<Compression>,
    replay_fsst: Option<&FsstTable>,
    shared: Option<&[String]>,
    trial: &mut StringTrial<'a>,
) -> EncodedBlock {
    let forced =
        forced.filter(|transform| transform_matches_kind(*transform, data) || matches!(data, ColumnData::F64(_)));
    let stats = stats_for(data);
    let mut out = Writer::with_capacity(raw_value_bytes(data) as usize);
    let mut side = SideStream::None;
    let mut fsst = None;
    let mut frame_breaks = Vec::new();
    let mut dictionary_by_trial = false;
    let (transform, kind) = match data {
        ColumnData::U64(values) => {
            let transform = forced.unwrap_or_else(|| choose_u64_transform(values, strategy));
            encode_u64_with(transform, values, &mut out);
            (transform, ValueKind::U64)
        }
        ColumnData::I64(values) => {
            let transform = forced.unwrap_or_else(|| choose_i64_transform(values, strategy));
            encode_i64_with(transform, values, &mut out);
            (transform, ValueKind::I64)
        }
        ColumnData::U128(values) => {
            out.put_u32(values.len() as u32);
            out.put_u128_slice(values);
            (Transform::PlainU128, ValueKind::U128)
        }
        ColumnData::F64(values) => {
            let alp = match forced {
                Some(Transform::AlpRd | Transform::ByteStreamSplit | Transform::Chimp | Transform::PlainF64) => None,
                _ => encode_alp(values, strategy, &mut out),
            };
            if let Some(chosen) = alp {
                side = chosen;
                (Transform::Alp, ValueKind::F64)
            } else {
                out.clear();
                let transform = match forced {
                    Some(
                        forced @ (Transform::AlpRd
                        | Transform::ByteStreamSplit
                        | Transform::Chimp
                        | Transform::PlainF64),
                    ) => forced,
                    _ => choose_float_fallback(values),
                };
                match transform {
                    Transform::AlpRd => encode_alp_rd(values, &mut out),
                    Transform::ByteStreamSplit => encode_byte_stream_split(values, &mut out),
                    Transform::Chimp => encode_chimp(values, &mut out),
                    _ => encode_plain_f64(values, &mut out),
                }
                (transform, ValueKind::F64)
            }
        }
        ColumnData::Decimal { values, scale } => {
            out.put_u8(*scale);
            out.put_u32(values.len() as u32);
            out.put_u128_slice(bytemuck::cast_slice(values));
            (Transform::Decimal128, ValueKind::Decimal)
        }
        ColumnData::Strings(values) => {
            let present = present_values(values);
            let mut transform = forced.unwrap_or_else(|| choose_string_transform(&present));
            let may_pack = forced != Some(Transform::FsstString) || replay_trailing.is_some();
            if transform == Transform::FsstPackedString {
                transform = Transform::FsstString;
            }
            let mut encoded = match transform {
                Transform::DictionaryString => {
                    let prepared = prepare_dictionary(&present);
                    let encoded = encode_prepared_dictionary(values, &prepared, strategy, shared, true, &mut out)
                        .map(|side| (side, None, Vec::new()));
                    trial.dictionary = Some(Some(prepared));
                    encoded
                }
                Transform::FsstString => encode_fsst_string(values, &present, replay_fsst, &mut out),
                _ => encode_raw_string(values, &present, &mut out).map(|()| (SideStream::None, None, Vec::new())),
            };
            if may_pack
                && transform == Transform::FsstString
                && matches!(encoded, Ok((SideStream::None, _, _)))
                && let Some(packed) = fsst_classic_as_packed(out.bytes())
                && packed.len() < out.len()
            {
                out = Writer::from_vec(packed);
                transform = Transform::FsstPackedString;
            }
            if (forced.is_none() || replay_trailing.is_some())
                && matches!(transform, Transform::FsstString | Transform::FsstPackedString)
                && encoded.is_ok()
            {
                let prepared = prepare_dictionary_within(&present, dictionary_trial_max_distinct(present.len()))
                    .or_else(|| shared_alphabet_covers(shared, &present).then(|| prepare_dictionary(&present)));
                if let Some(prepared) = &prepared {
                    let mut body = Writer::new();
                    if let Ok(side) = encode_prepared_dictionary(values, prepared, strategy, shared, true, &mut body)
                        && body.len() < out.len()
                    {
                        out = body;
                        transform = Transform::DictionaryString;
                        encoded = Ok((side, None, Vec::new()));
                        dictionary_by_trial = true;
                    }
                }
                trial.dictionary = Some(prepared);
            }
            #[expect(
                clippy::expect_used,
                reason = "the writer bounds a granule far below u32 arena offsets"
            )]
            let parts =
                encoded.expect("string-block arena offsets fit u32: the writer bounds a granule far below that");
            (side, fsst, frame_breaks) = parts;
            trial.present = Some(present);
            (transform, ValueKind::String)
        }
    };
    let body = out.into_bytes();
    let uncompressed_len = body.len() as u64;
    let depth_so_far = 1 + usize::from(side != SideStream::None);
    let (compression, bytes) = if random_access {
        apply_seekable_frames(transform, body, strategy, &frame_breaks)
    } else if !cascade_level_fits(depth_so_far) {
        (Compression::None, body)
    } else if dictionary_by_trial
        || matches!(
            transform,
            Transform::FsstString | Transform::FsstPackedString | Transform::DeltaBitpack | Transform::ForBitpack
        )
    {
        let candidate = match replay_trailing {
            Some(captured) => captured_trailing_candidate(&body, captured, strategy),
            None => trailing_candidate(&body, strategy),
        };
        match candidate {
            Some((compression, bytes))
                if bytes.len() * FSST_TRAILING_MAX_RATIO_DEN <= body.len() * FSST_TRAILING_MAX_RATIO_NUM =>
            {
                (compression, bytes)
            }
            _ => (Compression::None, body),
        }
    } else if let Some(captured) = replay_trailing {
        apply_captured_trailing(body, captured, strategy)
    } else {
        apply_trailing(body, strategy)
    };
    EncodedBlock {
        pipeline: PipelineId::new(transform, compression, kind).with_side_stream(side),
        bytes,
        fsst,
        stats,
        uncompressed_len,
    }
}

fn fastlanes_value_at(packed: &[u8], width: u32, index: usize) -> u64 {
    if width == 0 {
        return 0;
    }
    let bits = width as usize;
    let bytes_per_vector = FASTLANES_LANES * fastlanes_words_per_lane(bits) * 8;
    let within = index % FASTLANES_VECTOR;
    let sub_block = within / (FASTLANES_ROWS * FASTLANES_LANES);
    let row = (within / FASTLANES_LANES) % FASTLANES_ROWS;
    let lane = within % FASTLANES_LANES;
    let vector_start = (index / FASTLANES_VECTOR) * bytes_per_vector;
    let Some(packed_vector) = packed.get(vector_start..vector_start + bytes_per_vector) else {
        return 0;
    };
    let vl_block = FASTLANES_ORDER.get(sub_block).copied().unwrap_or(0);
    let bit = (vl_block * FASTLANES_ROWS + row) * bits;
    let (word, offset) = (bit / 64, (bit % 64) as u32);
    let value_mask = mask(width);
    let Some(low) = lane_word(packed_vector, word, lane) else {
        return 0;
    };
    if offset + width <= 64 {
        return (low >> offset) & value_mask;
    }
    match lane_word(packed_vector, word + 1, lane) {
        Some(high) => ((low >> offset) | (high << (64 - offset))) & value_mask,
        None => 0,
    }
}

fn lane_word(packed_vector: &[u8], word: usize, lane: usize) -> Option<u64> {
    let start = (word * FASTLANES_LANES + lane) * 8;
    let bytes = packed_vector.get(start..start + 8)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

fn unpack_fastlanes_range_into<'a>(
    packed: &[u8],
    width: u32,
    start: usize,
    end: usize,
    values: &'a mut Vec<u64>,
) -> &'a [u64] {
    let vector_start = start / FASTLANES_VECTOR;
    if end.saturating_sub(start) <= FASTLANES_SPARSE_MAX_VALUES {
        let first_row = vector_start * FASTLANES_VECTOR;
        values.clear();
        values.extend((start..end).map(|index| fastlanes_value_at(packed, width, index - first_row)));
        return values.as_slice();
    }
    let vectors = end.div_ceil(FASTLANES_VECTOR).saturating_sub(vector_start);
    let stream = PackedStream {
        count: vectors * FASTLANES_VECTOR,
        packed,
        width,
    };
    values.clear();
    values.resize(stream.padded_count(), 0);
    stream.unpack_into(values, |packed| packed);
    let first_row = vector_start * FASTLANES_VECTOR;
    let tail = end.saturating_sub(first_row).min(values.len());
    let head = start.saturating_sub(first_row).min(tail);
    values.get(head..tail).unwrap_or(&[])
}

fn decode_alp_range(
    reader: &mut Reader<'_>,
    side: SideStream,
    start: usize,
    end: usize,
) -> Result<Vec<f64>, FormatError> {
    let mut exponent_index = reader.u8("alp exponent")? as usize;
    let mut escaped = Vec::new();
    if exponent_index == usize::from(ALP_VECTOR_ESCAPE_SENTINEL) {
        exponent_index = reader.u8("alp exponent")? as usize;
        escaped = read_alp_escaped_vectors(reader)?;
    }
    let power = ALP_POWERS.get(exponent_index).copied().ok_or(FormatError::Structural {
        rule: "alp exponent out of range",
    })?;
    let exceptions = decode_alp_exceptions(reader, side)?;
    let count = reader.u32("bitpack count")? as usize;
    let width = u32::from(reader.u8("bitpack width")?);
    if width > 64 {
        return Err(FormatError::Structural {
            rule: "bit width beyond 64",
        });
    }
    let effective_end = end.min(count);
    let effective_len = effective_end.saturating_sub(start);
    if effective_len == 0 {
        return Ok(Vec::new());
    }
    let mut consumed_words = 0usize;
    let unscale = |packed: u64| (unzigzag(packed) as f64) / power;
    let mut values: Vec<f64> = if width == 0 {
        if count > MAX_BLOCK_VALUES {
            return Err(FormatError::Structural {
                rule: "zero-width bitpack count beyond the block bound",
            });
        }
        vec![unscale(0); effective_len]
    } else {
        let bits_per_val = width as usize;
        let words_per_lane = fastlanes_words_per_lane(bits_per_val);
        let vector_start = start / FASTLANES_VECTOR;
        let vector_end = effective_end.div_ceil(FASTLANES_VECTOR);
        let words_per_vector = FASTLANES_LANES * words_per_lane;
        let skip_words = vector_start * words_per_vector;
        reader.take(skip_words.saturating_mul(8), "bitpack skip word")?;
        let needed_words = (vector_end - vector_start) * words_per_vector;
        let packed_bytes = reader.take(needed_words.saturating_mul(8), "bitpack word")?;
        consumed_words = skip_words + needed_words;
        with_unpack_buffer(|buffer| {
            unpack_fastlanes_range_into(packed_bytes, width, start, effective_end, buffer)
                .iter()
                .map(|packed| unscale(*packed))
                .collect()
        })
    };
    exceptions.try_for_each(|index, bits| {
        let index = index as usize;
        if index >= start
            && index < end
            && let Some(slot) = values.get_mut(index - start)
        {
            *slot = f64::from_bits(bits);
        }
        Ok(())
    })?;
    if !escaped.is_empty() {
        let total_words = if width == 0 {
            0
        } else {
            count.div_ceil(FASTLANES_VECTOR) * FASTLANES_LANES * fastlanes_words_per_lane(width as usize)
        };
        reader.take(
            total_words.saturating_sub(consumed_words).saturating_mul(8),
            "bitpack skip word",
        )?;
        for vector_index in &escaped {
            let vector_start = *vector_index as usize * FASTLANES_VECTOR;
            if vector_start >= count {
                return Err(FormatError::RefOutOfRange {
                    what: "alp escaped vector index",
                });
            }
            let vector_end = (vector_start + FASTLANES_VECTOR).min(count);
            let raw = reader.take((vector_end - vector_start) * 8, "alp escaped value")?;
            for row in vector_start.max(start)..vector_end.min(effective_end) {
                let word = raw
                    .get((row - vector_start) * 8..(row - vector_start) * 8 + 8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                if let Some(slot) = values.get_mut(row - start) {
                    *slot = f64::from_bits(word);
                }
            }
        }
    }
    Ok(values)
}

fn decode_dictionary_string_range(
    body: &[u8],
    side: SideStream,
    shared: Option<&[String]>,
    start: usize,
    end: usize,
) -> Result<StringColumn, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = NullStream::read(&mut reader)?;
    let effective_end = end.min(nulls.row_count());
    let effective_len = effective_end.saturating_sub(start);
    if effective_len == 0 {
        return Ok(StringColumn::new());
    }
    let present_start = nulls.present_before(start);
    let present_end = nulls.present_before(effective_end);
    let dictionary;
    let mut entries: Vec<&str> = Vec::new();
    let mut alphabet: Option<&[String]> = None;
    match side {
        SideStream::FileScopeDictionary => {
            alphabet = Some(shared.ok_or(FormatError::Structural {
                rule: "shared-scope dictionary block without its file alphabet",
            })?);
        }
        _ => {
            let dict_count = reader.u32("dictionary count")? as usize;
            dictionary = DictionaryValues::read(&mut reader, dict_count, side)?;
            entries = dictionary.entries()?;
        }
    }
    let count = reader.u32("bitpack count")? as usize;
    let width = u32::from(reader.u8("bitpack width")?);
    if width > 64 {
        return Err(FormatError::Structural {
            rule: "bit width beyond 64",
        });
    }
    if present_end > count {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let entry_at = |code: u64| match alphabet {
        Some(alphabet) => alphabet.get(code as usize).map(String::as_str),
        None => entries.get(code as usize).copied(),
    };
    let materialize = |codes: &[u64]| {
        merge_nulls_range(
            &nulls,
            codes.len(),
            codes
                .iter()
                .map(|&code| entry_at(code).map_or(0, str::len))
                .fold(0usize, usize::saturating_add),
            |index| {
                codes
                    .get(index)
                    .and_then(|&code| entry_at(code))
                    .ok_or(FormatError::RefOutOfRange {
                        what: "dictionary code",
                    })
            },
            start,
            effective_end,
        )
    };
    if width == 0 {
        return merge_nulls_range(
            &nulls,
            present_end - present_start,
            entry_at(0)
                .map_or(0, str::len)
                .saturating_mul(present_end - present_start),
            |_| {
                entry_at(0).ok_or(FormatError::RefOutOfRange {
                    what: "dictionary code",
                })
            },
            start,
            effective_end,
        );
    }
    let bits = width as usize;
    let words_per_lane = fastlanes_words_per_lane(bits);
    let vector_start = present_start / FASTLANES_VECTOR;
    let vector_end = present_end.div_ceil(FASTLANES_VECTOR);
    let words_per_vector = FASTLANES_LANES * words_per_lane;
    reader.take((vector_start * words_per_vector).saturating_mul(8), "bitpack skip word")?;
    let packed_bytes = reader.take(
        ((vector_end - vector_start) * words_per_vector).saturating_mul(8),
        "bitpack word",
    )?;
    with_unpack_buffer(|buffer| {
        materialize(unpack_fastlanes_range_into(
            packed_bytes,
            width,
            present_start,
            present_end,
            buffer,
        ))
    })
}

struct StringHeader<'a> {
    fsst: Option<FsstDecodeTable>,
    nulls: NullStream<'a>,
    offsets_start: usize,
    present_count: usize,
}

impl StringHeader<'_> {
    fn fsst(&self) -> Result<&FsstDecodeTable, FormatError> {
        self.fsst.as_ref().ok_or(FormatError::Structural {
            rule: "fsst block without its symbol table",
        })
    }

    fn compressor(&self) -> Result<fsst::Compressor, FormatError> {
        Ok(self.fsst()?.compressor())
    }
}

fn read_string_header<'a>(transform: Transform, reader: &mut Reader<'a>) -> Result<StringHeader<'a>, FormatError> {
    let nulls = NullStream::read(reader)?;
    let fsst = match transform {
        Transform::FsstString => Some(FsstDecodeTable::read(reader)?),
        _ => None,
    };
    let present_count = reader.u32(match transform {
        Transform::FsstString => "fsst present count",
        _ => "raw present count",
    })? as usize;
    Ok(StringHeader {
        fsst,
        nulls,
        offsets_start: reader.position(),
        present_count,
    })
}

fn fsst_shared_prefix(body: &[u8]) -> Result<(usize, usize), FormatError> {
    let mut reader = Reader::new(body);
    NullStream::read(&mut reader)?;
    let symbols = reader.u16("fsst symbol count")? as usize;
    reader.take(symbols.saturating_mul(9), "fsst symbol table")?;
    let count = reader.u32("fsst present count")? as usize;
    Ok((reader.position(), count))
}

struct FsstPackedValues<'a> {
    arena: &'a [u8],
    offsets: Vec<usize>,
    prefix_len: usize,
}

fn fsst_packed_values(body: &[u8]) -> Result<FsstPackedValues<'_>, FormatError> {
    let (prefix_len, count) = fsst_shared_prefix(body)?;
    let mut reader = Reader::new(body);
    reader.take(prefix_len, "fsst prefix")?;
    let lens = bitunpack(&mut reader)?;
    if lens.len() != count {
        return Err(FormatError::Structural {
            rule: "fsst packed length count disagrees with the present count",
        });
    }
    let arena = reader.take(reader.remaining(), "fsst data")?;
    let mut offsets = Vec::with_capacity(count + 1);
    offsets.push(0usize);
    let mut offset = 0u64;
    for len in lens {
        offset = offset.saturating_add(len);
        let stored = u32::try_from(offset).map_err(|_| FormatError::Structural {
            rule: "string arena offsets exceed the u32 range the format stores",
        })?;
        offsets.push(stored as usize);
    }
    if offset != arena.len() as u64 {
        return Err(FormatError::Structural {
            rule: "fsst packed lengths disagree with the arena",
        });
    }
    Ok(FsstPackedValues {
        arena,
        offsets,
        prefix_len,
    })
}

pub(crate) fn fsst_packed_as_classic(body: &[u8]) -> Result<Vec<u8>, FormatError> {
    let FsstPackedValues {
        arena,
        offsets,
        prefix_len,
    } = fsst_packed_values(body)?;
    let prefix = body.get(..prefix_len).ok_or(FormatError::Structural {
        rule: "fsst prefix out of range",
    })?;
    let mut out = Writer::with_capacity(prefix_len + offsets.len() * 4 + arena.len());
    out.put_slice(prefix);
    for offset in &offsets {
        out.put_u32(*offset as u32);
    }
    out.put_slice(arena);
    Ok(out.into_bytes())
}

fn fsst_classic_as_packed(body: &[u8]) -> Option<Vec<u8>> {
    let (prefix_len, count) = fsst_shared_prefix(body).ok()?;
    let table_len = count.checked_add(1)?.checked_mul(4)?;
    let arena_start = prefix_len.checked_add(table_len)?;
    let offsets = decode_offsets(body.get(prefix_len..arena_start)?);
    let lens: Vec<u64> = offsets
        .windows(2)
        .map(|pair| match pair {
            [start, end] => end.saturating_sub(*start) as u64,
            _ => 0,
        })
        .collect();
    let width = bits_needed(lens.iter().copied().max().unwrap_or(0));
    let mut out = Writer::with_capacity(body.len());
    out.put_slice(body.get(..prefix_len)?);
    bitpack(&lens, width, &mut out);
    out.put_slice(body.get(arena_start..)?);
    Some(out.into_bytes())
}

pub(crate) fn classic_fsst_view(
    pipeline: PipelineId,
    body: &[u8],
) -> Result<Option<(PipelineId, Vec<u8>)>, FormatError> {
    if pipeline.transform()? != Transform::FsstPackedString {
        return Ok(None);
    }
    if pipeline.value_kind()? != ValueKind::String {
        return Err(FormatError::Structural {
            rule: "packed fsst transform on a non-string block",
        });
    }
    Ok(Some((
        pipeline.with_transform(Transform::FsstString),
        fsst_packed_as_classic(body)?,
    )))
}

fn offsets_text_len(offsets: &[usize], data: &[u8]) -> usize {
    let first = offsets.first().copied().unwrap_or(0);
    let last = offsets.last().copied().unwrap_or(0);
    last.saturating_sub(first).min(data.len())
}

fn merge_nulls_range<'a>(
    nulls: &NullStream,
    count: usize,
    text_bytes: usize,
    value: impl Fn(usize) -> Result<&'a str, FormatError>,
    start: usize,
    end: usize,
) -> Result<StringColumn, FormatError> {
    let mut result = StringColumn::with_capacity(end.saturating_sub(start), text_bytes);
    let mut next = 0usize;
    nulls.each_row_range(start, end, |row_present| {
        if row_present {
            if next >= count {
                return Err(FormatError::Structural {
                    rule: "null bitmap disagrees with present count",
                });
            }
            result.push(Some(value(next)?));
            next += 1;
        } else {
            result.push(None);
        }
        Ok(())
    })?;
    Ok(result)
}

fn value_bytes_for_range<'a>(
    body: &'a [u8],
    header: &StringHeader<'_>,
    present_start: usize,
    present_end: usize,
    what: &'static str,
) -> Result<(&'a [u8], Vec<usize>), FormatError> {
    let table = slice(
        body,
        header.offsets_start,
        header.present_count.saturating_add(1).saturating_mul(4),
        "string offset",
    )?;
    let offsets = offset_window(table, present_start, present_end - present_start + 1)?;
    let arena_len = offset_window(table, header.present_count, 1)?
        .first()
        .copied()
        .unwrap_or(0);
    let data = slice(body, header.offsets_start + table.len(), arena_len, what)?;
    Ok((data, offsets))
}

fn decode_fsst_string_range(body: &[u8], start: usize, end: usize) -> Result<StringColumn, FormatError> {
    let header = read_string_header(Transform::FsstString, &mut Reader::new(body))?;
    let effective_end = end.min(header.nulls.row_count());
    if effective_end <= start {
        return Ok(StringColumn::new());
    }
    let present_start = header.nulls.present_before(start);
    let present_end = header.nulls.present_before(effective_end);
    let (data, offsets) = value_bytes_for_range(body, &header, present_start, present_end, "fsst data")?;
    fsst_range_column(&header, data, &offsets, start, effective_end)
}

fn decode_fsst_packed_string_range(body: &[u8], start: usize, end: usize) -> Result<StringColumn, FormatError> {
    let packed = fsst_packed_values(body)?;
    let header = read_string_header(Transform::FsstString, &mut Reader::new(body))?;
    let effective_end = end.min(header.nulls.row_count());
    if effective_end <= start {
        return Ok(StringColumn::new());
    }
    let present_start = header.nulls.present_before(start);
    let present_end = header.nulls.present_before(effective_end);
    let offsets = packed
        .offsets
        .get(present_start..=present_end)
        .ok_or(FormatError::Truncated { what: "string offset" })?;
    fsst_range_column(&header, packed.arena, offsets, start, effective_end)
}

fn fsst_range_column(
    header: &StringHeader<'_>,
    data: &[u8],
    offsets: &[usize],
    start: usize,
    end: usize,
) -> Result<StringColumn, FormatError> {
    let arena = fsst_decompress_arena(
        header.fsst()?,
        data,
        offsets,
        "fsst value",
        "fsst offsets must be non-decreasing",
    )?;
    merge_nulls_range(
        &header.nulls,
        arena.len(),
        arena.text.len(),
        |index| fsst_arena_value(&arena, index),
        start,
        end,
    )
}

fn fsst_arena_value(arena: &FsstArena, index: usize) -> Result<&str, FormatError> {
    arena
        .value(index)
        .ok_or(FormatError::InvalidUtf8 { what: "fsst value" })
}

fn decode_raw_string_range(body: &[u8], start: usize, end: usize) -> Result<StringColumn, FormatError> {
    let header = read_string_header(Transform::RawString, &mut Reader::new(body))?;
    let effective_end = end.min(header.nulls.row_count());
    if effective_end <= start {
        return Ok(StringColumn::new());
    }
    let present_start = header.nulls.present_before(start);
    let present_end = header.nulls.present_before(effective_end);
    let (data, offsets) = value_bytes_for_range(body, &header, present_start, present_end, "raw string data")?;
    let raw = RawValues::validated(data, &offsets)?;
    merge_nulls_range(
        &header.nulls,
        offsets.len().saturating_sub(1),
        offsets_text_len(&offsets, data),
        |index| raw.value(&offsets, index),
        start,
        effective_end,
    )
}

fn raw_string_value<'a>(data: &'a [u8], offsets: &[usize], index: usize) -> Result<&'a str, FormatError> {
    let (start, end) = (
        offsets.get(index).copied().unwrap_or(0),
        offsets.get(index + 1).copied().unwrap_or(0),
    );
    if end < start {
        return Err(FormatError::Structural {
            rule: "raw string offsets must be non-decreasing",
        });
    }
    simdutf8::basic::from_utf8(slice(data, start, end - start, "raw string")?)
        .map_err(|_| FormatError::InvalidUtf8 { what: "raw string" })
}

enum RawValues<'a> {
    Bytes(&'a [u8]),
    Text { base: usize, text: &'a str },
}

impl<'a> RawValues<'a> {
    fn validated(data: &'a [u8], offsets: &[usize]) -> Result<Self, FormatError> {
        if let Some((base, text)) = raw_values_text(data, offsets) {
            return Ok(Self::Text { base, text });
        }
        for index in 0..offsets.len().saturating_sub(1) {
            raw_string_value(data, offsets, index)?;
        }
        Ok(Self::Bytes(data))
    }

    fn value(&self, offsets: &[usize], index: usize) -> Result<&'a str, FormatError> {
        match *self {
            Self::Bytes(data) => raw_string_value(data, offsets, index),
            Self::Text { base, text } => offsets
                .get(index)
                .zip(offsets.get(index + 1))
                .and_then(|(start, end)| text.get(start.checked_sub(base)?..end.checked_sub(base)?))
                .ok_or(FormatError::InvalidUtf8 { what: "raw string" }),
        }
    }
}

fn raw_values_text<'a>(data: &'a [u8], offsets: &[usize]) -> Option<(usize, &'a str)> {
    let (first, last) = (*offsets.first()?, *offsets.last()?);
    if offsets
        .windows(2)
        .any(|pair| matches!(pair, [start, end] if end < start))
    {
        return None;
    }
    let text = simdutf8::basic::from_utf8(data.get(first..last)?).ok()?;
    offsets
        .iter()
        .all(|&offset| text.is_char_boundary(offset - first))
        .then_some((first, text))
}

fn read_windowed_string_header(
    transform: Transform,
    window: &mut seekable_zstd::Window<'_>,
) -> Result<StringHeader<'static>, FormatError> {
    let plain_len = window.plain_len();
    let mut probe = window.first_frame_len().min(plain_len);
    loop {
        let head = window.read_forward(0, probe)?;
        match read_string_header(transform, &mut Reader::new(&head)) {
            Err(FormatError::Truncated { .. }) if probe < plain_len => {
                probe = probe.saturating_mul(2).min(plain_len);
            }
            header => {
                return header.map(|header| StringHeader {
                    fsst: header.fsst,
                    nulls: header.nulls.into_owned(),
                    offsets_start: header.offsets_start,
                    present_count: header.present_count,
                });
            }
        }
    }
}

fn decode_string_framed_range(
    transform: Transform,
    bytes: &[u8],
    start: usize,
    end: usize,
) -> Result<StringColumn, FormatError> {
    let mut window = seekable_zstd::Window::open(bytes)?;
    let header = read_windowed_string_header(transform, &mut window)?;
    let effective_end = end.min(header.nulls.row_count());
    if effective_end <= start {
        return Ok(StringColumn::new());
    }
    let present_start = header.nulls.present_before(start);
    let present_end = header.nulls.present_before(effective_end);
    let mut offsets = decode_offsets(&window.read_forward(
        header.offsets_start + present_start * 4,
        (present_end - present_start + 1) * 4,
    )?);
    let (&first, &last) = match (offsets.first(), offsets.last()) {
        (Some(first), Some(last)) if first <= last => (first, last),
        _ => {
            return Err(FormatError::Structural {
                rule: "string offsets must be non-decreasing",
            });
        }
    };
    let arena_start = header.offsets_start + (header.present_count + 1) * 4;
    let data = window.read_forward(arena_start + first, last - first)?;
    for offset in &mut offsets {
        *offset = offset.saturating_sub(first);
    }
    match transform {
        Transform::FsstString => {
            let arena = fsst_decompress_arena(
                header.fsst()?,
                &data,
                &offsets,
                "fsst value",
                "fsst offsets must be non-decreasing",
            )?;
            merge_nulls_range(
                &header.nulls,
                arena.len(),
                arena.text.len(),
                |index| fsst_arena_value(&arena, index),
                start,
                effective_end,
            )
        }
        _ => {
            let raw = RawValues::validated(&data, &offsets)?;
            merge_nulls_range(
                &header.nulls,
                offsets.len().saturating_sub(1),
                offsets_text_len(&offsets, &data),
                |index| raw.value(&offsets, index),
                start,
                effective_end,
            )
        }
    }
}

fn decode_plain_framed_range(
    kind: ValueKind,
    bytes: &[u8],
    start: usize,
    end: usize,
) -> Result<ColumnData, FormatError> {
    let mut window = seekable_zstd::Window::open(bytes)?;
    let count = {
        let header = window.read_forward(0, 4)?;
        u32::from_le_bytes(header[..].try_into().unwrap_or([0; 4])) as usize
    };
    let expected_plain_len = count.checked_mul(8).and_then(|body| body.checked_add(4));
    if expected_plain_len != Some(window.plain_len()) {
        return Err(FormatError::Structural {
            rule: "framed plain body length disagrees with its row-count header",
        });
    }
    let effective_end = end.min(count);
    if effective_end <= start {
        return Ok(plain_rows(kind, &[]));
    }
    let byte_start = 4 + start * 8;
    let row_bytes = window.read_forward(byte_start, (effective_end - start) * 8)?;
    Ok(plain_rows(kind, &row_bytes))
}

fn decode_plain_deflate_range(
    kind: ValueKind,
    bytes: &[u8],
    start: usize,
    end: usize,
) -> Result<ColumnData, FormatError> {
    let page = deflate::Page::open(bytes)?;
    let count = page
        .plain_len()
        .checked_sub(4)
        .filter(|body| body % 8 == 0)
        .map(|body| body / 8)
        .ok_or(FormatError::Structural {
            rule: "deflate plain body is not a row-count header plus whole rows",
        })?;
    let effective_end = end.min(count);
    if effective_end <= start {
        return Ok(plain_rows(kind, &[]));
    }
    let byte_start = 4 + start * 8;
    let byte_end = 4 + effective_end * 8;
    let first = byte_start / deflate::GRANULE_BYTES;
    let plain = page.decompress_granules(first, (byte_end - 1) / deflate::GRANULE_BYTES)?;
    if first == 0
        && plain
            .get(..4)
            .map(|header| u32::from_le_bytes(header.try_into().unwrap_or([0; 4])) as usize)
            != Some(count)
    {
        return Err(FormatError::Structural {
            rule: "deflate plain body length disagrees with its row-count header",
        });
    }
    let offset = first * deflate::GRANULE_BYTES;
    let row_bytes = slice(&plain, byte_start - offset, byte_end - byte_start, "deflate row range")?;
    Ok(plain_rows(kind, row_bytes))
}

fn plain_rows(kind: ValueKind, row_bytes: &[u8]) -> ColumnData {
    let values: Vec<u64> = row_bytes
        .chunks_exact(8)
        .map(|word| u64::from_le_bytes(word.try_into().unwrap_or([0; 8])))
        .collect();
    match kind {
        ValueKind::F64 => ColumnData::F64(values.into_iter().map(f64::from_bits).collect()),
        ValueKind::I64 => ColumnData::I64(values.into_iter().map(unzigzag).collect()),
        _ => ColumnData::U64(values),
    }
}

fn decode_byte_stream_split_framed_range(bytes: &[u8], start: usize, end: usize) -> Result<ColumnData, FormatError> {
    let mut window = seekable_zstd::Window::open(bytes)?;
    let count = {
        let header = window.read_forward(0, 4)?;
        u32::from_le_bytes(header[..].try_into().unwrap_or([0; 4])) as usize
    };
    let expected_plain_len = count.checked_mul(8).and_then(|body| body.checked_add(4));
    if expected_plain_len != Some(window.plain_len()) {
        return Err(FormatError::Structural {
            rule: "framed byte-stream-split body length disagrees with its row-count header",
        });
    }
    let effective_end = end.min(count);
    if effective_end <= start {
        return Ok(ColumnData::F64(Vec::new()));
    }
    let row_span = effective_end - start;
    let mut bits = vec![0u64; row_span];
    for plane_index in 0..8 {
        let plane = window.read_forward(4 + plane_index * count + start, row_span)?;
        let shift = (8 * plane_index) as u32;
        for (slot, byte) in bits.iter_mut().zip(plane.iter()) {
            *slot |= u64::from(*byte) << shift;
        }
    }
    Ok(ColumnData::F64(bytemuck::cast_vec(bits)))
}

fn retain_range<T>(mut values: Vec<T>, start: usize, end: usize) -> Vec<T> {
    values.truncate(end.min(values.len()));
    values.drain(..start.min(values.len()));
    values
}

fn slice_column_data(data: ColumnData, start: usize, end: usize) -> ColumnData {
    match data {
        ColumnData::Decimal { values, scale } => ColumnData::Decimal {
            scale,
            values: retain_range(values, start, end),
        },
        ColumnData::F64(values) => ColumnData::F64(retain_range(values, start, end)),
        ColumnData::I64(values) => ColumnData::I64(retain_range(values, start, end)),
        ColumnData::Strings(values) => ColumnData::Strings(values.slice(start, end)),
        ColumnData::U128(values) => ColumnData::U128(retain_range(values, start, end)),
        ColumnData::U64(values) => ColumnData::U64(retain_range(values, start, end)),
    }
}

pub fn decode_block_range(
    pipeline: PipelineId,
    bytes: &[u8],
    start: usize,
    end: usize,
) -> Result<ColumnData, FormatError> {
    decode_block_range_shared(pipeline, bytes, None, start, end)
}

pub fn decode_block_range_shared(
    pipeline: PipelineId,
    bytes: &[u8],
    shared: Option<&[String]>,
    start: usize,
    end: usize,
) -> Result<ColumnData, FormatError> {
    if start >= end {
        return Ok(match pipeline.value_kind()? {
            ValueKind::Decimal => ColumnData::Decimal {
                scale: 0,
                values: Vec::new(),
            },
            ValueKind::F64 => ColumnData::F64(Vec::new()),
            ValueKind::I64 => ColumnData::I64(Vec::new()),
            ValueKind::String => ColumnData::Strings(StringColumn::new()),
            ValueKind::U128 => ColumnData::U128(Vec::new()),
            ValueKind::U64 => ColumnData::U64(Vec::new()),
        });
    }
    let transform = pipeline.transform()?;
    if transform.keeps_range_access_under_framing() && pipeline.compression()? == Compression::SeekableZstd {
        return match transform {
            Transform::ByteStreamSplit => decode_byte_stream_split_framed_range(bytes, start, end),
            Transform::FsstString | Transform::RawString => Ok(ColumnData::Strings(decode_string_framed_range(
                transform, bytes, start, end,
            )?)),
            _ => decode_plain_framed_range(pipeline.value_kind()?, bytes, start, end),
        };
    }
    if matches!(transform, Transform::PlainU64 | Transform::PlainF64) && pipeline.compression()? == Compression::Deflate
    {
        return decode_plain_deflate_range(pipeline.value_kind()?, bytes, start, end);
    }
    with_trailing_removed(pipeline.compression()?, bytes, |body| {
        match (pipeline.value_kind()?, transform) {
            (ValueKind::F64, Transform::Alp) => {
                let mut reader = Reader::new(body);
                Ok(ColumnData::F64(decode_alp_range(
                    &mut reader,
                    pipeline.side_stream()?,
                    start,
                    end,
                )?))
            }
            (ValueKind::F64, Transform::Chimp) => {
                let mut reader = Reader::new(body);
                Ok(ColumnData::F64(decode_chimp_range(&mut reader, start, end)?))
            }
            (ValueKind::String, Transform::DictionaryString) => Ok(ColumnData::Strings(
                decode_dictionary_string_range(body, pipeline.side_stream()?, shared, start, end)?,
            )),
            (ValueKind::String, Transform::FsstString) => {
                Ok(ColumnData::Strings(decode_fsst_string_range(body, start, end)?))
            }
            (ValueKind::String, Transform::FsstPackedString) => {
                Ok(ColumnData::Strings(decode_fsst_packed_string_range(body, start, end)?))
            }
            (ValueKind::String, Transform::RawString) => {
                Ok(ColumnData::Strings(decode_raw_string_range(body, start, end)?))
            }
            (kind @ (ValueKind::U64 | ValueKind::I64), Transform::Rle) => {
                lazy::decode_rle_range(body, kind, start, end)
            }
            _ => {
                debug_assert!(
                    !transform.is_per_value_addressable(),
                    "transform {transform:?} claims byte-range extraction support but decode_block_range has no fast path for it"
                );
                let full = decode_block_shared_with_body(pipeline, body, shared)?;
                Ok(slice_column_data(full, start, end))
            }
        }
    })
}

pub fn decode_block(pipeline: PipelineId, bytes: &[u8]) -> Result<ColumnData, FormatError> {
    decode_block_shared(pipeline, bytes, None)
}

pub fn decode_block_shared(
    pipeline: PipelineId,
    bytes: &[u8],
    shared: Option<&[String]>,
) -> Result<ColumnData, FormatError> {
    let transform = pipeline.transform()?;
    if pipeline.compression()? == Compression::SeekableZstd
        && matches!(transform, Transform::FsstString | Transform::RawString)
    {
        return decompressor::with_inflate_buffer(|scratch| {
            seekable_zstd::decompress_prefix_into(bytes, scratch, |head| string_values_end(transform, head))?;
            decode_block_shared_with_body(pipeline, scratch, shared)
        });
    }
    if pipeline.compression()? == Compression::StringZstd
        && pipeline.value_kind()? == ValueKind::String
        && let Some(column) = string_zstd::with_relaid(transform, pipeline.side_stream()?, bytes, relaid_string_column)?
    {
        return Ok(ColumnData::Strings(column));
    }
    with_trailing_removed(pipeline.compression()?, bytes, |body| {
        decode_block_shared_with_body(pipeline, body, shared)
    })
}

fn string_values_end(transform: Transform, head: &[u8]) -> Option<usize> {
    let header = read_string_header(transform, &mut Reader::new(head)).ok()?;
    let arena_start = header
        .offsets_start
        .checked_add(header.present_count.checked_add(1)?.checked_mul(4)?)?;
    let last_at = arena_start.checked_sub(4)?;
    let last = u32::from_le_bytes(head.get(last_at..arena_start)?.try_into().ok()?);
    arena_start.checked_add(last as usize)
}

pub fn decode_block_shared_with_body(
    pipeline: PipelineId,
    body: &[u8],
    shared: Option<&[String]>,
) -> Result<ColumnData, FormatError> {
    let transform = pipeline.transform()?;
    if transform == Transform::FsstPackedString {
        if pipeline.value_kind()? != ValueKind::String {
            return Err(FormatError::Structural {
                rule: "packed fsst transform on a non-string block",
            });
        }
        return Ok(ColumnData::Strings(decode_fsst_packed_string(body)?));
    }
    let mut reader = Reader::new(body);
    match pipeline.value_kind()? {
        ValueKind::U64 => Ok(ColumnData::U64(decode_ints_with(transform, &mut reader)?)),
        ValueKind::I64 => Ok(ColumnData::I64(decode_ints_with(transform, &mut reader)?)),
        ValueKind::U128 => {
            if transform != Transform::PlainU128 {
                return Err(FormatError::Structural {
                    rule: "u128 columns use the plain transform",
                });
            }
            let count = reader.u32("u128 count")? as usize;
            let bytes = reader.take(count.saturating_mul(16), "u128 value")?;
            Ok(ColumnData::U128(
                bytes
                    .chunks_exact(16)
                    .map(|word| u128::from_le_bytes(word.try_into().unwrap_or([0; 16])))
                    .collect(),
            ))
        }
        ValueKind::F64 => match transform {
            Transform::Alp => Ok(ColumnData::F64(decode_alp(&mut reader, pipeline.side_stream()?)?)),
            Transform::PlainF64 => {
                let count = reader.u32("f64 count")? as usize;
                let bytes = reader.take(count.saturating_mul(8), "f64 bits")?;
                Ok(ColumnData::F64(
                    bytes
                        .chunks_exact(8)
                        .map(|word| f64::from_bits(u64::from_le_bytes(word.try_into().unwrap_or([0; 8]))))
                        .collect(),
                ))
            }
            Transform::AlpRd => Ok(ColumnData::F64(decode_alp_rd(&mut reader)?)),
            Transform::ByteStreamSplit => Ok(ColumnData::F64(decode_byte_stream_split(&mut reader)?)),
            Transform::Chimp => Ok(ColumnData::F64(decode_chimp(&mut reader)?)),
            _ => Err(FormatError::Structural {
                rule: "f64 columns use alp, alp-rd, plain, byte-stream-split, or chimp transforms",
            }),
        },
        ValueKind::Decimal => {
            if transform != Transform::Decimal128 {
                return Err(FormatError::Structural {
                    rule: "money columns use the decimal128 transform",
                });
            }
            let scale = reader.u8("decimal scale")?;
            let count = reader.u32("decimal count")? as usize;
            let bytes = reader.take(count.saturating_mul(16), "decimal value")?;
            Ok(ColumnData::Decimal {
                values: bytes
                    .chunks_exact(16)
                    .map(|word| u128::from_le_bytes(word.try_into().unwrap_or([0; 16])) as i128)
                    .collect(),
                scale,
            })
        }
        ValueKind::String => match transform {
            Transform::DictionaryString => Ok(ColumnData::Strings(decode_dictionary_string(
                &mut reader,
                pipeline.side_stream()?,
                shared,
            )?)),
            Transform::FsstString => Ok(ColumnData::Strings(decode_fsst_string(&mut reader)?)),
            Transform::RawString => Ok(ColumnData::Strings(decode_raw_string(&mut reader)?)),
            _ => Err(FormatError::Structural {
                rule: "string columns use dictionary, fsst, or raw transforms",
            }),
        },
    }
}

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
