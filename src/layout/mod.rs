use crate::error::FormatError;
use crate::events::TenantId;
use crate::file::bytes::{Reader, Writer, slice};
use constant::OE_MAGIC;
use std::borrow::Cow;
use uuid::Uuid;

pub use remote::{RemoteGraph, RemoteGraphOptions, RemoteState};

pub mod api;
pub mod audit;
pub mod cache;
pub mod clustering;
pub mod constant;
pub mod entity_scan;
pub mod event;
pub mod footer;
pub mod graph;
pub mod inbox;
pub mod log;
pub mod mirror;
pub mod reader;
mod remote;
pub mod snapshot;
pub mod source_form;
pub mod time_series;

pub const HEADER_BLOCK_LEN: usize = 4096;

pub const MAX_PAGE_BYTES: u64 = 1024 * 1024;

pub const MAX_ELIDED_BLOCK_ROWS: usize = 1 << 24;

pub mod presence_forms {
    pub const ALL_ABSENT: u8 = 3;
    pub const ALL_PRESENT: u8 = 2;
    pub const EMPTY: u8 = 1;
    pub const RAW: u8 = 0;
    pub const SET_POSITIONS: u8 = 5;
    pub const SET_RUNS: u8 = 4;
}

pub fn encode_presence(presence: &[u8], row_count: u32, out: &mut Writer) {
    if presence.is_empty() {
        out.put_u8(presence_forms::EMPTY);
        return;
    }
    let rows = row_count as usize;
    let covered = presence.get(..rows.div_ceil(8)).unwrap_or(presence);
    let (mut set_count, mut run_count, mut carry) = (0usize, 0usize, 0u64);
    for word in presence_words(covered, rows) {
        set_count += word.count_ones() as usize;
        run_count += (word & !((word << 1) | carry)).count_ones() as usize;
        carry = word >> 63;
    }
    if set_count == rows {
        out.put_u8(presence_forms::ALL_PRESENT);
        return;
    }
    if set_count == 0 {
        out.put_u8(presence_forms::ALL_ABSENT);
        return;
    }
    let raw_size = 4 + presence.len();
    let runs_size = 4 + run_count * 8;
    let positions_size = 4 + set_count * 4;
    if raw_size <= runs_size && raw_size <= positions_size {
        out.put_u8(presence_forms::RAW);
        out.put_u32(presence.len() as u32);
        out.put_slice(presence);
    } else if runs_size <= positions_size {
        out.put_u8(presence_forms::SET_RUNS);
        out.put_u32(run_count as u32);
        let mut start = None;
        let mut carry = 0u64;
        for (index, word) in presence_words(covered, rows).enumerate() {
            let mut edges = word ^ ((word << 1) | carry);
            while edges != 0 {
                let row = index * 64 + edges.trailing_zeros() as usize;
                edges &= edges - 1;
                match start.take() {
                    None => start = Some(row),
                    Some(first) => {
                        out.put_u32(first as u32);
                        out.put_u32((row - first) as u32);
                    }
                }
            }
            carry = word >> 63;
        }
        if let Some(first) = start {
            out.put_u32(first as u32);
            out.put_u32((rows.min(covered.len() * 8) - first) as u32);
        }
    } else {
        out.put_u8(presence_forms::SET_POSITIONS);
        out.put_u32(set_count as u32);
        for (index, mut word) in presence_words(covered, rows).enumerate() {
            while word != 0 {
                out.put_u32((index * 64 + word.trailing_zeros() as usize) as u32);
                word &= word - 1;
            }
        }
    }
}

fn presence_words(covered: &[u8], rows: usize) -> impl Iterator<Item = u64> + '_ {
    covered.chunks(8).enumerate().map(move |(index, chunk)| {
        let mut bytes = [0u8; 8];
        if let Some(prefix) = bytes.get_mut(..chunk.len()) {
            prefix.copy_from_slice(chunk);
        }
        let word = u64::from_le_bytes(bytes);
        match rows.saturating_sub(index * 64) {
            valid if valid < 64 => word & ((1u64 << valid) - 1),
            _ => word,
        }
    })
}

pub fn decode_presence_frame<'a>(
    reader: &mut Reader<'a>,
    row_count: u32,
    compressed_presence: bool,
) -> Result<Cow<'a, [u8]>, FormatError> {
    if compressed_presence {
        decode_presence(reader, row_count)
    } else {
        let len = reader.u32("presence length")? as usize;
        Ok(Cow::Borrowed(reader.take(len, "presence bitmap")?))
    }
}

pub fn decode_presence_ranges(
    reader: &mut Reader<'_>,
    row_count: u32,
    compressed_presence: bool,
) -> Result<Option<crate::indexes::bitmap::RoaringRangeBitmap>, FormatError> {
    use crate::indexes::bitmap::{RoaringRangeBitmap, RowRange};
    if !compressed_presence {
        let len = reader.u32("presence length")? as usize;
        let bitmap = reader.take(len, "presence bitmap")?;
        if bitmap.is_empty() {
            return Ok(None);
        }
        return Ok(Some(RoaringRangeBitmap::from_packed_bits(bitmap)));
    }
    let form = reader.u8("presence form")?;
    let rows = u64::from(row_count);
    Ok(match form {
        presence_forms::EMPTY => None,
        presence_forms::ALL_PRESENT => Some(RoaringRangeBitmap::from_ranges([RowRange { start: 0, end: rows }])),
        presence_forms::ALL_ABSENT => Some(RoaringRangeBitmap::default()),
        presence_forms::RAW => {
            let len = reader.u32("presence length")? as usize;
            if len != (rows as usize).div_ceil(8) {
                return Err(FormatError::Structural {
                    rule: "raw presence length must cover exactly the block's row count",
                });
            }
            Some(RoaringRangeBitmap::from_packed_bits(
                reader.take(len, "presence bitmap")?,
            ))
        }
        presence_forms::SET_RUNS => {
            let count = reader.u32("presence run count")? as usize;
            if count > rows as usize {
                return Err(FormatError::Structural {
                    rule: "presence run count beyond the block's row count",
                });
            }
            let mut ranges = Vec::with_capacity(count.min(reader.remaining() / 8));
            for _ in 0..count {
                let start = u64::from(reader.u32("presence run start")?);
                let len = u64::from(reader.u32("presence run length")?);
                let end = start.saturating_add(len);
                if end > rows {
                    return Err(FormatError::RefOutOfRange {
                        what: "presence row beyond the block's row count",
                    });
                }
                ranges.push(RowRange { start, end });
            }
            Some(RoaringRangeBitmap::from_ranges(ranges))
        }
        presence_forms::SET_POSITIONS => {
            let count = reader.u32("presence position count")? as usize;
            if count > rows as usize {
                return Err(FormatError::Structural {
                    rule: "presence position count beyond the block's row count",
                });
            }
            let mut positions = Vec::with_capacity(count.min(reader.remaining() / 4));
            for _ in 0..count {
                let row = u64::from(reader.u32("presence position")?);
                if row >= rows {
                    return Err(FormatError::RefOutOfRange {
                        what: "presence row beyond the block's row count",
                    });
                }
                positions.push(row);
            }
            Some(RoaringRangeBitmap::from_rows(positions))
        }
        _ => {
            return Err(FormatError::Structural {
                rule: "unknown presence form",
            });
        }
    })
}

pub fn decode_presence<'a>(reader: &mut Reader<'a>, row_count: u32) -> Result<Cow<'a, [u8]>, FormatError> {
    let form = reader.u8("presence form")?;
    let rows = row_count as usize;
    if rows > MAX_ELIDED_BLOCK_ROWS && form != presence_forms::EMPTY && form != presence_forms::RAW {
        return Err(FormatError::Structural {
            rule: "presence row count beyond the block bound",
        });
    }
    match form {
        presence_forms::EMPTY => Ok(Cow::Borrowed(&[])),
        presence_forms::ALL_PRESENT => {
            let mut bitmap = vec![0xFFu8; rows.div_ceil(8)];
            if !rows.is_multiple_of(8)
                && let Some(last) = bitmap.last_mut()
            {
                *last = (1u8 << (rows % 8)) - 1;
            }
            Ok(Cow::Owned(bitmap))
        }
        presence_forms::ALL_ABSENT => Ok(Cow::Owned(vec![0u8; rows.div_ceil(8)])),
        presence_forms::RAW => {
            let len = reader.u32("presence length")? as usize;
            if len != rows.div_ceil(8) {
                return Err(FormatError::Structural {
                    rule: "raw presence length must cover exactly the block's row count",
                });
            }
            Ok(Cow::Borrowed(reader.take(len, "presence bitmap")?))
        }
        presence_forms::SET_RUNS => {
            let count = reader.u32("presence run count")? as usize;
            if count > rows {
                return Err(FormatError::Structural {
                    rule: "presence run count beyond the block's row count",
                });
            }
            let mut bitmap = vec![0u8; rows.div_ceil(8)];
            for _ in 0..count {
                let start = reader.u32("presence run start")? as usize;
                let len = reader.u32("presence run length")? as usize;
                let end = start.saturating_add(len);
                if end > rows {
                    return Err(FormatError::RefOutOfRange {
                        what: "presence row beyond the block's row count",
                    });
                }
                fill_bit_range(&mut bitmap, start, end);
            }
            Ok(Cow::Owned(bitmap))
        }
        presence_forms::SET_POSITIONS => {
            let count = reader.u32("presence position count")? as usize;
            if count > rows {
                return Err(FormatError::Structural {
                    rule: "presence position count beyond the block's row count",
                });
            }
            let mut bitmap = vec![0u8; rows.div_ceil(8)];
            for _ in 0..count {
                let row = reader.u32("presence position")? as usize;
                if row >= rows {
                    return Err(FormatError::RefOutOfRange {
                        what: "presence row beyond the block's row count",
                    });
                }
                if let Some(byte) = bitmap.get_mut(row / 8) {
                    *byte |= 1 << (row % 8);
                }
            }
            Ok(Cow::Owned(bitmap))
        }
        _ => Err(FormatError::Structural {
            rule: "unknown presence form",
        }),
    }
}

fn fill_bit_range(bitmap: &mut [u8], start: usize, end: usize) {
    if start >= end {
        return;
    }
    let start_byte = start / 8;
    let end_byte = (end - 1) / 8;
    let low_mask = 0xFFu8 << (start % 8);
    if start_byte == end_byte {
        let high_mask = 0xFFu8 >> (7 - (end - 1) % 8);
        if let Some(byte) = bitmap.get_mut(start_byte) {
            *byte |= low_mask & high_mask;
        }
        return;
    }
    if let Some(byte) = bitmap.get_mut(start_byte) {
        *byte |= low_mask;
    }
    if let Some(interior) = bitmap.get_mut(start_byte + 1..end_byte) {
        interior.fill(0xFF);
    }
    if let Some(byte) = bitmap.get_mut(end_byte) {
        *byte |= 0xFFu8 >> (7 - (end - 1) % 8);
    }
}

pub const EMPTY_VALUE_ROW_OFFSET: u32 = u32::MAX;

#[derive(Debug, Clone, Copy)]
pub struct LayoutTargets {
    pub index_granularity: usize,
    pub index_granularity_bytes: usize,
    pub max_stripe_bytes: usize,
    pub min_bytes_for_wide: usize,
    pub stripe_target_bytes: usize,
}

impl Default for LayoutTargets {
    fn default() -> Self {
        Self {
            index_granularity: 8192,
            index_granularity_bytes: 10 * 1024 * 1024,
            stripe_target_bytes: 256 * 1024 * 1024,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutClass {
    Compact = 0,
    Wide = 1,
}

impl LayoutClass {
    pub fn from_u8(value: u8) -> Result<Self, FormatError> {
        match value {
            0 => Ok(LayoutClass::Compact),
            1 => Ok(LayoutClass::Wide),
            _ => Err(FormatError::Structural {
                rule: "unknown layout class",
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Profile {
    Audit,
    #[default]
    Event,
    Graph,
    Inbox,
    Log,
    Snapshot,
    TimeSeries,
}

impl Profile {
    pub fn code(self) -> u8 {
        match self {
            Profile::Event => 0,
            Profile::Graph => 1,
            Profile::Snapshot => 2,
            Profile::Audit => 3,
            Profile::Log => 4,
            Profile::Inbox => 5,
            Profile::TimeSeries => 6,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self, FormatError> {
        match value {
            0 => Ok(Profile::Event),
            1 => Ok(Profile::Graph),
            2 => Ok(Profile::Snapshot),
            3 => Ok(Profile::Audit),
            4 => Ok(Profile::Log),
            5 => Ok(Profile::Inbox),
            6 => Ok(Profile::TimeSeries),
            _ => Err(FormatError::Structural {
                rule: "unknown profile",
            }),
        }
    }
}

pub mod required_features {
    pub const ENVELOPE_COLUMNS: u64 = 1 << 0;
    pub const PAYLOAD_ARENA: u64 = 1 << 1;
    pub const STRIPE_DIRECTORY: u64 = 1 << 2;
    pub const GRANULE_DIRECTORY: u64 = 1 << 3;
    pub const MARKS_PER_COLUMN: u64 = 1 << 4;
    pub const PAGE_METADATA: u64 = 1 << 5;
    pub const MINMAX_SKIP_INDEXES: u64 = 1 << 6;
    pub const SEQUENCE_SKIP_INDEX: u64 = 1 << 7;
    pub const TIME_SKIP_INDEX: u64 = 1 << 8;
    pub const EXACT_FILE_COUNTS: u64 = 1 << 9;
    pub const EXACT_SOURCE_TYPE_ENTITY_COUNTS: u64 = 1 << 10;
    pub const CHECKSUM_DIRECTORY: u64 = 1 << 11;
    pub const FOOTER_DIRECTORY: u64 = 1 << 12;
    pub const LAYOUT_CLASS: u64 = 1 << 13;
    pub const STRIPE_RELATIVE_MARKS: u64 = 1 << 14;
    pub const COLUMNAR_MARKS: u64 = 1 << 15;

    pub const FOOTER_ENCRYPTED: u64 = 1 << 16;

    pub const ELIDED_CONSTANT_BLOCKS: u64 = 1 << 17;

    pub const COMPRESSED_PRESENCE: u64 = 1 << 18;

    pub const SPARSE_SHREDDED_COLUMNS: u64 = 1 << 19;

    pub const SHARED_DICTIONARIES: u64 = 1 << 20;

    pub const STRIPE_MARKS_PAGES: u64 = 1 << 21;

    pub const STRIPE_VERIFIED_STREAMING: u64 = 1 << 22;

    pub const GRAPH_DENSE_IDS: u64 = 1 << 23;

    pub const GRAPH_CSR_FORWARD: u64 = 1 << 24;

    pub const GRAPH_CSR_REVERSE: u64 = 1 << 25;

    pub const GRAPH_EVENT_ID_INDEX: u64 = 1 << 26;

    pub const GRAPH_CHAIN_COVER: u64 = 1 << 27;

    pub const GRAPH_STATE_ROOTS: u64 = 1 << 28;

    pub const GRAPH: u64 = GRAPH_DENSE_IDS
        | GRAPH_CSR_FORWARD
        | GRAPH_CSR_REVERSE
        | GRAPH_EVENT_ID_INDEX
        | GRAPH_CHAIN_COVER
        | GRAPH_STATE_ROOTS;

    pub const AUDIT_HASH_CHAIN: u64 = 1 << 29;

    pub const XOR_FLOAT_BLOCKS: u64 = 1 << 30;

    pub const VECTOR_COLUMNS: u64 = 1 << 31;

    pub const EXTERNAL_ID_EVENT_ID_ELISION: u64 = 1 << 32;

    pub const PACKED_STRING_OFFSETS: u64 = 1 << 33;

    pub const OCCURRED_RELATIVE_INGESTED_AT: u64 = 1 << 34;

    pub const ENTITY_HASH_COLUMNS_ELIDED: u64 = 1 << 35;

    pub const EXTERNAL_ID_INDEX_32: u64 = 1 << 36;

    pub const STRING_ZSTD_BLOCKS: u64 = 1 << 37;

    pub const PACKED_SHARED_DICTIONARIES: u64 = 1 << 38;

    pub const STRING_LZ4_BLOCKS: u64 = 1 << 39;

    pub const KNOWN: u64 = (1 << 40) - 1;

    pub const ALL: u64 = KNOWN
        & !FOOTER_ENCRYPTED
        & !GRAPH
        & !AUDIT_HASH_CHAIN
        & !XOR_FLOAT_BLOCKS
        & !VECTOR_COLUMNS
        & !EXTERNAL_ID_EVENT_ID_ELISION
        & !PACKED_STRING_OFFSETS
        & !OCCURRED_RELATIVE_INGESTED_AT
        & !EXTERNAL_ID_INDEX_32
        & !ENTITY_HASH_COLUMNS_ELIDED
        & !STRING_ZSTD_BLOCKS
        & !PACKED_SHARED_DICTIONARIES
        & !STRING_LZ4_BLOCKS;
}

pub mod optional_features {
    pub const TRAINED_ZSTD_RESIDUAL_DICTIONARIES: u64 = 1 << 0;
    pub const VARIANT_SHREDDED_FIELD_BLOCKS: u64 = 1 << 1;
    pub const FREETEXT_COLUMNS: u64 = 1 << 2;
    pub const OE_NATIVE_DELETION_VECTORS: u64 = 1 << 4;
    pub const OE_LATE_EVENTS: u64 = 1 << 8;
    pub const PAGE_IO_ALIGNMENT: u64 = 1 << 6;
    pub const PER_PAGE_MARKS: u64 = 1 << 5;
    pub const PROJECTIONS: u64 = 1 << 3;
    pub const TEXT_TOKEN_FILTER_OFFSETS: u64 = 1 << 9;
    pub const TYPED_COLUMN_ROW_OFFSETS: u64 = 1 << 7;

    pub const ENTITY_HASH_POINT_FILTERS: u64 = 1 << 10;

    pub const SNAPSHOT_KEY_INDEX: u64 = 1 << 11;

    pub const INBOX_RECIPIENT_DIRECTORY: u64 = 1 << 12;

    pub const TIME_SERIES_ROLLUPS: u64 = 1 << 13;

    pub const DELIVERY_LINEAGE: u64 = 1 << 14;

    pub const ENTITY_SUMMARY: u64 = 1 << 15;

    pub const KNOWN: u64 = (1 << 16) - 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OeHeader {
    pub created_at_physical: i64,
    pub feature_flags: u64,
    pub file_id: u128,
    pub footer_pointer_hint: u64,
    pub generation_id: u64,
    pub layout_class: LayoutClass,
    pub max_epoch: u64,
    pub max_ingested_at_physical: i64,
    pub max_occurred_at_physical: i64,
    pub max_sequence: u64,
    pub min_epoch: u64,
    pub min_ingested_at_physical: i64,
    pub min_occurred_at_physical: i64,
    pub min_sequence: u64,
    pub profile: Profile,
    pub projection_count: u32,
    pub row_count: u64,
    pub tenant_id: TenantId,
    pub version_major: u16,
    pub version_minor: u16,
}

const HEADER_FIXED_LEN: usize = 4 + 2 + 2 + 4 + 16 + 16 + 8 + 1 + 4 + 8 * 12 + 8 + 32;

const PROFILED_HEADER_FIXED_LEN: usize = HEADER_FIXED_LEN + 1;

impl OeHeader {
    pub fn may_contain_sequence(&self, epoch: u64, first: u64, last: u64) -> bool {
        if epoch < self.min_epoch || epoch > self.max_epoch {
            return false;
        }
        let lower_ok = epoch != self.min_epoch || self.min_sequence <= last;
        let upper_ok = epoch != self.max_epoch || first <= self.max_sequence;
        lower_ok && upper_ok
    }

    pub fn may_contain_occurred(&self, min_nanos: i64, max_nanos: i64) -> bool {
        self.min_occurred_at_physical <= max_nanos && min_nanos <= self.max_occurred_at_physical
    }
}

pub fn encode_header(header: &OeHeader) -> Vec<u8> {
    let profiled = header.profile != Profile::Event;
    let fixed_len = if profiled {
        PROFILED_HEADER_FIXED_LEN
    } else {
        HEADER_FIXED_LEN
    };
    let mut out = Writer::with_capacity(HEADER_BLOCK_LEN);
    out.put_slice(&OE_MAGIC);
    out.put_u16(header.version_major);
    out.put_u16(header.version_minor);
    out.put_u32(fixed_len as u32);
    out.put_u128(header.file_id);
    out.put_u128(header.tenant_id.uuid().as_u128());
    out.put_u64(header.generation_id);
    out.put_u8(header.layout_class as u8);
    out.put_u32(header.projection_count);
    out.put_i64(header.created_at_physical);
    out.put_i64(header.min_occurred_at_physical);
    out.put_i64(header.max_occurred_at_physical);
    out.put_i64(header.min_ingested_at_physical);
    out.put_i64(header.max_ingested_at_physical);
    out.put_u64(header.min_epoch);
    out.put_u64(header.max_epoch);
    out.put_u64(header.min_sequence);
    out.put_u64(header.max_sequence);
    out.put_u64(header.row_count);
    out.put_u64(header.feature_flags);
    out.put_u64(header.footer_pointer_hint);
    if profiled {
        out.put_u8(header.profile.code());
    }
    let crc = crate::file::integrity::crc64_nvme(out.bytes());
    out.put_u64(crc);
    let digest = blake3::hash(out.bytes());
    out.put_slice(digest.as_bytes());
    let mut block = out.into_bytes();
    block.resize(HEADER_BLOCK_LEN, 0);
    block
}

pub fn decode_header(block: &[u8]) -> Result<OeHeader, FormatError> {
    let mut preamble = Reader::new(slice(block, 0, HEADER_FIXED_LEN, "oe header")?);
    let magic = preamble.take(OE_MAGIC.len(), "oe magic")?;
    if magic != OE_MAGIC {
        return Err(FormatError::BadMagic { expected: "OEF1" });
    }
    let version_major = preamble.u16("version_major")?;
    let version_minor = preamble.u16("version_minor")?;
    if version_major != 1 {
        return Err(FormatError::UnsupportedVersion {
            field: "OEHeader.version_major",
            found: u32::from(version_major),
        });
    }
    let fixed_len = match preamble.u32("header_len")? as usize {
        HEADER_FIXED_LEN => HEADER_FIXED_LEN,
        PROFILED_HEADER_FIXED_LEN => PROFILED_HEADER_FIXED_LEN,
        _ => {
            return Err(FormatError::Structural {
                rule: "oe header_len mismatch",
            });
        }
    };
    let fixed = slice(block, 0, fixed_len, "oe header")?;
    let mut reader = Reader::new(fixed);
    reader.take(OE_MAGIC.len() + 2 + 2 + 4, "oe header preamble")?;
    let file_id = reader.u128("file_id")?;
    let tenant_id = TenantId::from_uuid(Uuid::from_u128(reader.u128("tenant_id")?));
    let generation_id = reader.u64("generation_id")?;
    let layout_class = LayoutClass::from_u8(reader.u8("layout_class")?)?;
    let projection_count = reader.u32("projection_count")?;
    let created_at_physical = reader.i64("created_at")?;
    let min_occurred_at_physical = reader.i64("min_occurred_at")?;
    let max_occurred_at_physical = reader.i64("max_occurred_at")?;
    let min_ingested_at_physical = reader.i64("min_ingested_at")?;
    let max_ingested_at_physical = reader.i64("max_ingested_at")?;
    let min_epoch = reader.u64("min_epoch")?;
    let max_epoch = reader.u64("max_epoch")?;
    let min_sequence = reader.u64("min_sequence")?;
    let max_sequence = reader.u64("max_sequence")?;
    let row_count = reader.u64("row_count")?;
    let feature_flags = reader.u64("feature_flags")?;
    let footer_pointer_hint = reader.u64("footer_pointer_hint")?;
    let profile = if fixed_len == PROFILED_HEADER_FIXED_LEN {
        Profile::from_u8(reader.u8("profile")?)?
    } else {
        Profile::Event
    };
    let stored_crc = reader.u64("header_crc64")?;
    let stored_blake3 = reader.take(32, "header_blake3")?;

    let crc_scope = slice(block, 0, fixed_len - 8 - 32, "oe header crc scope")?;
    if crate::file::integrity::crc64_nvme(crc_scope) != stored_crc {
        return Err(FormatError::HeaderCrcMismatch);
    }
    let blake3_scope = slice(block, 0, fixed_len - 32, "oe header blake3 scope")?;
    if blake3::hash(blake3_scope).as_bytes() != stored_blake3 {
        return Err(FormatError::Blake3Mismatch { scope: "oe header" });
    }
    let padding = slice(
        block,
        fixed_len,
        HEADER_BLOCK_LEN.saturating_sub(fixed_len),
        "oe header padding",
    )?;
    if padding.iter().any(|byte| *byte != 0) {
        return Err(FormatError::ReservedNotZero {
            field: "oe header padding",
        });
    }
    Ok(OeHeader {
        version_major,
        version_minor,
        file_id,
        tenant_id,
        generation_id,
        layout_class,
        projection_count,
        created_at_physical,
        min_occurred_at_physical,
        max_occurred_at_physical,
        min_ingested_at_physical,
        max_ingested_at_physical,
        min_epoch,
        max_epoch,
        min_sequence,
        max_sequence,
        row_count,
        feature_flags,
        footer_pointer_hint,
        profile,
    })
}

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
