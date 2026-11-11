use super::graph::{ChainCover, DepthRange, EventIdIndex, UnresolvedEdge, decode_unresolved, encode_unresolved};
use super::snapshot::SnapshotKeyIndex;
use super::time_series::TimeSeriesRollups;
use super::{Profile, optional_features, required_features};
use crate::artifacts::state_chunk::StateRoot;
use crate::compat::EscapeHatch;
use crate::encoding::{ColumnData, PipelineId, decode_block, encode_block};
use crate::error::FormatError;
use crate::events::TimestampValue;
use crate::file::bytes::{Reader, Writer, slice};
use crate::vector::VectorColumnMeta;
use hashbrown::HashMap;
use std::collections::BTreeMap;
use std::ops::Range;

pub mod sections {
    pub const AUDIT: u32 = 35;
    pub const CLUSTERING_METADATA: u32 = 16;
    pub const COLUMNS: u32 = 1;
    pub const DICTIONARIES: u32 = 5;
    pub const EMBEDDING_ROW_OFFSETS: u32 = 19;
    pub const ENTITY_HASH_FILTERS: u32 = 27;
    pub const ESCAPE_HATCHES: u32 = 13;
    pub const EXACT_COUNTS: u32 = 7;
    pub const EXTERNAL_IDS: u32 = 28;
    pub const FREETEXT: u32 = 11;
    pub const FREETEXT_ROW_OFFSETS: u32 = 18;
    pub const GRANULES: u32 = 3;
    pub const GRAPH_EDGES: u32 = 31;
    pub const GRAPH_EVENT_IDS: u32 = 32;
    pub const GRAPH_CHAIN_COVER: u32 = 33;
    pub const GRAPH_DEPTH: u32 = 40;
    pub const GRAPH_UNRESOLVED: u32 = 37;
    pub const GRAPH_STATE_ROOTS: u32 = 38;
    pub const INBOX_RECIPIENTS: u32 = 36;
    pub const INTEGRITY_GAPS: u32 = 26;
    pub const IO_ALIGNMENT: u32 = 15;
    pub const MARKS: u32 = 4;
    pub const PAGE_DIRECTORY: u32 = 14;
    pub const PAGE_MINMAX: u32 = 17;
    pub const PAGE_STATS: u32 = 6;
    pub const PAYLOAD_GRANULES: u32 = 8;
    pub const PRESENCE: u32 = 9;
    pub const PROFILE: u32 = 30;
    pub const REFERENCE_FILTERS: u32 = 29;
    pub const SHREDDED: u32 = 10;
    pub const SHARED_DICTIONARIES: u32 = 24;
    pub const SNAPSHOT_KEYS: u32 = 34;
    pub const SPARSE_KEYS: u32 = 23;
    pub const STRIPE_CHECKSUMS: u32 = 12;
    pub const STRIPE_PROOFS: u32 = 25;
    pub const STRIPE_NDV: u32 = 22;
    pub const STRIPES: u32 = 2;
    pub const TEXT_TOKEN: u32 = 20;
    pub const TEXT_TOKEN_OFFSETS: u32 = 21;
    pub const TIME_SERIES_ROLLUPS: u32 = 39;
    pub const VECTOR_COLUMNS: u32 = 41;
    pub const ENTITY_SUMMARY: u32 = 42;
    pub const EXTERNAL_IDS_PACKED: u32 = 43;
    pub const EXTERNAL_IDS_32: u32 = 44;
    pub const PACKED_SHARED_DICTIONARIES: u32 = 45;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Decimal = 4,
    F64 = 3,
    I64 = 1,
    String = 5,
    U128 = 2,
    U64 = 0,
    Vector = 6,
}

impl ColumnKind {
    pub fn from_u8(value: u8) -> Result<Self, FormatError> {
        Ok(match value {
            0 => ColumnKind::U64,
            1 => ColumnKind::I64,
            2 => ColumnKind::U128,
            3 => ColumnKind::F64,
            4 => ColumnKind::Decimal,
            5 => ColumnKind::String,
            6 => ColumnKind::Vector,
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown column kind",
                });
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescriptor {
    pub column_id: u32,
    pub internal_only: bool,
    pub kind: ColumnKind,
    pub name: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripeEntry {
    pub byte_len: u64,
    pub file_offset: u64,
    pub first_row_ordinal: u64,
    pub row_count: u64,
    pub stripe_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrityGapEntry {
    pub blake3: [u8; 32],
    pub byte_len: u64,
    pub file_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripeProofEntry {
    pub chunk_group_bytes: u32,
    pub stripe_id: u32,
    pub tree_len: u64,
    pub tree_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GranuleEntry {
    pub compressed_bytes_estimate: u64,
    pub first_epoch: u64,
    pub first_row_ordinal: u64,
    pub first_sequence: u64,
    pub granule_id: u32,
    pub last_epoch: u64,
    pub last_sequence: u64,
    pub max_ingested_at_physical: i64,
    pub max_occurred_at_physical: i64,
    pub min_ingested_at_physical: i64,
    pub min_occurred_at_physical: i64,
    pub row_count: u32,
    pub stripe_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnMark {
    pub codec_pipeline_id: PipelineId,
    pub column_id: u32,
    pub compressed_offset: u64,
    pub compressed_size: u64,
    pub first_value_offset: Option<u64>,
    pub granule_id: u32,
    pub page_count: u32,
    pub projection_id: u32,
    pub row_count: u32,
    pub uncompressed_offset: u64,
    pub uncompressed_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarksPageEntry {
    pub column_id: u32,
    pub page_len: u64,
    pub page_offset: u64,
    pub projection_id: u32,
    pub stripe_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageStats {
    pub column_id: u32,
    pub granule_id: u32,
    pub max_f64: Option<f64>,
    pub max_i128: Option<i128>,
    pub min_f64: Option<f64>,
    pub min_i128: Option<i128>,
    pub null_count: u32,
    pub row_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageDirectoryEntry {
    pub column_id: u32,
    pub compressed_len: u64,
    pub compressed_offset: u64,
    pub first_row_ordinal: u64,
    pub granule_id: u32,
    pub max_f64: Option<f64>,
    pub max_i128: Option<i128>,
    pub max_occurred_at_physical: i64,
    pub max_sequence: u64,
    pub min_f64: Option<f64>,
    pub min_i128: Option<i128>,
    pub min_occurred_at_physical: i64,
    pub min_sequence: u64,
    pub null_count: u32,
    pub page_index: u32,
    pub projection_id: u32,
    pub row_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityHashFilterEntry {
    pub granule_id: u32,
    pub index_len: u64,
    pub index_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDirectory {
    pub blocks: Vec<GraphBlockEntry>,
    pub chain_cover: Option<ChainCover>,
    pub depth: Option<DepthRange>,
    pub event_ids: Option<EventIdIndex>,
    pub first_sn: u64,
    pub subject: Vec<u8>,
    pub unresolved: Vec<UnresolvedEdge>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphBlockEntry {
    pub granule_id: u32,
    pub len: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateRootEntry {
    pub root: StateRoot,
    pub row_ordinal: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalIdEntry {
    pub id_hash: u64,
    pub row_ordinal: u64,
}

impl ExternalIdEntry {
    pub fn key(&self) -> u32 {
        (self.id_hash >> 32) as u32
    }

    pub fn truncated_hash(id_hash: u64) -> u64 {
        id_hash & !0xFFFF_FFFF
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboxRecipientEntry {
    pub entity_id_hash_high: u64,
    pub entity_id_hash_low: u64,
    pub first_row_ordinal: u64,
    pub last_epoch: u64,
    pub last_sequence: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceFilterEntry {
    pub column_id: u32,
    pub filter: Vec<u8>,
    pub granule_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextTokenEntry {
    pub column_id: u32,
    pub granule_id: u32,
    pub index_bytes: Vec<u8>,
    pub page_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextTokenOffsetsEntry {
    pub column_id: u32,
    pub granule_id: u32,
    pub index_len: u64,
    pub index_offset: u64,
    pub page_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripeNdvEntry {
    pub column_id: u32,
    pub distinct_count: u64,
    pub exact: bool,
    pub stripe_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageMinMax {
    pub column_id: u32,
    pub granule_id: u32,
    pub max_i128: Option<i128>,
    pub min_i128: Option<i128>,
    pub null_count: u32,
    pub page_index: u32,
    pub row_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileDictionaries {
    pub entity_type: Vec<String>,
    pub event_type: Vec<String>,
    pub source: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExactCounts {
    pub by_entity_type: Vec<(u32, u64)>,
    pub by_event_type: Vec<(u32, u64)>,
    pub by_source: Vec<(u32, u64)>,
    pub row_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResidualCompression {
    None = 0,
    Zstd3 = 1,
    ZstdSeekable = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadGranule {
    pub dictionary_len: u64,
    pub dictionary_offset: u64,
    pub granule_id: u32,
    pub offsets_len: u64,
    pub offsets_offset: u64,
    pub residual_compression: ResidualCompression,
    pub residual_len: u64,
    pub residual_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PresenceEntry {
    pub column_id: u32,
    pub since_schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShredEntry {
    pub column_id: u32,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedDictionaryEntry {
    pub column_id: u32,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedSharedDictionary {
    pub column_id: u32,
    pub value_count: u32,
    pub plain_len: u32,
    pub compressed: Vec<u8>,
}

pub const PACKED_SHARED_DICTIONARY_MAX_PLAIN_BYTES: u32 = 1 << 28;

const PACKED_SHARED_DICTIONARY_ZSTD_LEVEL: i32 = 19;

const PACKED_SHARED_DICTIONARY_LARGE_PLAIN_BYTES: usize = 1 << 20;

const PACKED_SHARED_DICTIONARY_LARGE_ZSTD_LEVEL: i32 = 1;

impl PackedSharedDictionary {
    pub fn pack(column_id: u32, values: &[String]) -> Option<Self> {
        let mut plain = Vec::with_capacity(values.len() * 2 + values.iter().map(String::len).sum::<usize>());
        for value in values {
            plain.extend_from_slice(&u16::try_from(value.len()).ok()?.to_le_bytes());
        }
        for value in values {
            plain.extend_from_slice(value.as_bytes());
        }
        let plain_len = u32::try_from(plain.len())
            .ok()
            .filter(|len| *len <= PACKED_SHARED_DICTIONARY_MAX_PLAIN_BYTES)?;
        let level = if plain.len() > PACKED_SHARED_DICTIONARY_LARGE_PLAIN_BYTES {
            PACKED_SHARED_DICTIONARY_LARGE_ZSTD_LEVEL
        } else {
            PACKED_SHARED_DICTIONARY_ZSTD_LEVEL
        };
        let compressed = zstd::bulk::compress(&plain, level).ok()?;
        Some(Self {
            column_id,
            value_count: u32::try_from(values.len()).ok()?,
            plain_len,
            compressed,
        })
    }

    pub fn values(&self) -> Result<Vec<String>, FormatError> {
        Ok(self.text()?.to_values())
    }

    pub fn text(&self) -> Result<SharedAlphabetText, FormatError> {
        let count = self.value_count as usize;
        let plain_len = self.plain_len as usize;
        if self.plain_len > PACKED_SHARED_DICTIONARY_MAX_PLAIN_BYTES || count.saturating_mul(2) > plain_len {
            return Err(FormatError::Structural {
                rule: "packed shared dictionary plaintext length out of range",
            });
        }
        let mut plain = Vec::new();
        crate::encoding::decompressor::zstd_decompress_into(&self.compressed, plain_len, &mut plain)?;
        let (lengths, text) = plain.split_at_checked(count * 2).ok_or(FormatError::Structural {
            rule: "packed shared dictionary plaintext length out of range",
        })?;
        let text = simdutf8::basic::from_utf8(text).map_err(|_| FormatError::InvalidUtf8 {
            what: "shared dictionary value",
        })?;
        let mut ends: Vec<u32> = Vec::with_capacity(count);
        let mut start = 0usize;
        let mut previous: Option<&str> = None;
        for pair in lengths.chunks_exact(2) {
            let len = match pair {
                [lo, hi] => usize::from(u16::from_le_bytes([*lo, *hi])),
                _ => 0,
            };
            let end = start + len;
            let value = text.get(start..end).ok_or(FormatError::Structural {
                rule: "packed shared dictionary lengths do not split its text into values",
            })?;
            if previous.is_some_and(|previous| previous >= value) {
                return Err(FormatError::Structural {
                    rule: "shared dictionary alphabet must be strictly ascending",
                });
            }
            previous = Some(value);
            ends.push(end as u32);
            start = end;
        }
        if start != text.len() {
            return Err(FormatError::Structural {
                rule: "packed shared dictionary has trailing bytes",
            });
        }
        Ok(SharedAlphabetText {
            text: text.to_owned(),
            ends,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedAlphabetText {
    text: String,
    ends: Vec<u32>,
}

impl SharedAlphabetText {
    pub fn entries(&self) -> Vec<&str> {
        let mut start = 0usize;
        self.ends
            .iter()
            .map(|&end| {
                let value = self.text.get(start..end as usize).unwrap_or_default();
                start = end as usize;
                value
            })
            .collect()
    }

    pub fn to_values(&self) -> Vec<String> {
        self.entries().into_iter().map(str::to_owned).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreetextEntry {
    pub column_id: u32,
    pub declared_field: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreetextRowOffsets {
    pub bytes_len: u64,
    pub bytes_offset: u64,
    pub column_id: u32,
    pub granule_id: u32,
    pub offsets_len: u64,
    pub offsets_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingRowOffsets {
    pub bytes_len: u64,
    pub bytes_offset: u64,
    pub column_id: u32,
    pub granule_id: u32,
    pub offsets_len: u64,
    pub offsets_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortednessProof {
    pub column_names: Vec<String>,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusteringEntry {
    pub clustering_quality: f32,
    pub granule_id: u32,
    pub projection_id: u32,
    pub sortedness_proof: Option<SortednessProof>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionMode {
    Compliance,
    Governance,
}

impl RetentionMode {
    pub fn code(self) -> u8 {
        match self {
            RetentionMode::Governance => 0,
            RetentionMode::Compliance => 1,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self, FormatError> {
        match value {
            0 => Ok(RetentionMode::Governance),
            1 => Ok(RetentionMode::Compliance),
            _ => Err(FormatError::Structural {
                rule: "unknown retention mode",
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLockRetention {
    pub mode: RetentionMode,
    pub retain_until: TimestampValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditTrail {
    pub chain_end: [u8; 32],
    pub chain_start: [u8; 32],
    pub legal_hold: bool,
    pub retention: Option<ObjectLockRetention>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub audit: Option<AuditTrail>,
    pub clustering: Vec<ClusteringEntry>,
    pub columns: Vec<ColumnDescriptor>,
    pub dictionaries: FileDictionaries,
    pub embedding_row_offsets: Vec<EmbeddingRowOffsets>,
    pub entity_hash_filters: Vec<EntityHashFilterEntry>,
    pub entity_summary: Option<crate::indexes::entity_summary::EntitySummary>,
    pub escape_hatches: Vec<EscapeHatch>,
    pub exact_counts: ExactCounts,
    pub external_ids: Vec<ExternalIdEntry>,
    pub format_version: (u16, u16),
    pub freetext: Vec<FreetextEntry>,
    pub freetext_row_offsets: Vec<FreetextRowOffsets>,
    pub granules: Vec<GranuleEntry>,
    pub graph: Option<GraphDirectory>,
    pub inbox_recipients: Vec<InboxRecipientEntry>,
    pub integrity_gaps: Vec<IntegrityGapEntry>,
    pub io_alignment_bytes: u32,
    pub marks: Vec<ColumnMark>,
    pub marks_directory: Vec<MarksPageEntry>,
    pub marks_page_offsets: Vec<MarksPageEntry>,
    pub marks_pages: Vec<u8>,
    pub optional_feature_flags: u64,
    pub page_directory: Vec<PageDirectoryEntry>,
    pub page_minmax: Vec<PageMinMax>,
    pub page_stats: Vec<PageStats>,
    pub payload_granules: Vec<PayloadGranule>,
    pub presence: Vec<PresenceEntry>,
    pub profile: Profile,
    pub reference_filters: Option<Vec<ReferenceFilterEntry>>,
    pub required_feature_flags: u64,
    pub schema_fingerprint: [u8; 32],
    pub shared_dictionaries: Vec<SharedDictionaryEntry>,
    pub packed_shared_dictionaries: Vec<PackedSharedDictionary>,
    pub shredded: Vec<ShredEntry>,
    pub snapshot_keys: Option<SnapshotKeyIndex>,
    pub sparse_keys: Vec<ShredEntry>,
    pub state_roots: Vec<StateRootEntry>,
    pub stripe_checksums: Vec<[u8; 32]>,
    pub stripe_proofs: Vec<StripeProofEntry>,
    pub stripe_ndv: Vec<StripeNdvEntry>,
    pub stripes: Vec<StripeEntry>,
    pub text_token_indexes: Vec<TextTokenEntry>,
    pub text_token_offsets: Vec<TextTokenOffsetsEntry>,
    pub time_series_rollups: Option<TimeSeriesRollups>,
    pub vector_columns: Vec<VectorColumnMeta>,
}

impl Footer {
    pub fn all_granules_sorted_for(&self, projection_id: u32, column_names: &[&str], direction: SortDirection) -> bool {
        if self.granules.is_empty() {
            return false;
        }
        let by_granule: HashMap<u32, &ClusteringEntry> = self
            .clustering
            .iter()
            .filter(|entry| entry.projection_id == projection_id)
            .map(|entry| (entry.granule_id, entry))
            .collect();
        self.granules.iter().all(|granule| {
            by_granule
                .get(&granule.granule_id)
                .and_then(|entry| entry.sortedness_proof.as_ref())
                .is_some_and(|proof| {
                    proof.direction == direction
                        && proof.column_names.len() == column_names.len()
                        && proof.column_names.iter().zip(column_names).all(|(a, b)| a == b)
                })
        })
    }
}

fn put_string(out: &mut Writer, value: &str) {
    out.put_u32(value.len() as u32);
    out.put_slice(value.as_bytes());
}

fn read_string(reader: &mut Reader<'_>) -> Result<String, FormatError> {
    let len = reader.u32("string length")? as usize;
    let bytes = reader.take(len, "string")?;
    simdutf8::basic::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| FormatError::InvalidUtf8 { what: "footer string" })
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
            rule: "footer option tag must be 0 or 1",
        }),
    }
}

fn put_opt_f64(out: &mut Writer, value: Option<f64>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            out.put_u64(value.to_bits());
        }
        None => out.put_u8(0),
    }
}

fn read_opt_f64(reader: &mut Reader<'_>) -> Result<Option<f64>, FormatError> {
    match reader.u8("option tag")? {
        0 => Ok(None),
        1 => Ok(Some(f64::from_bits(reader.u64("f64")?))),
        _ => Err(FormatError::Structural {
            rule: "footer option tag must be 0 or 1",
        }),
    }
}

fn put_opt_u64(out: &mut Writer, value: Option<u64>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            out.put_u64(value);
        }
        None => out.put_u8(0),
    }
}

fn read_opt_u64(reader: &mut Reader<'_>) -> Result<Option<u64>, FormatError> {
    match reader.u8("option tag")? {
        0 => Ok(None),
        1 => Ok(Some(reader.u64("u64")?)),
        _ => Err(FormatError::Structural {
            rule: "footer option tag must be 0 or 1",
        }),
    }
}

fn put_counts(out: &mut Writer, counts: &[(u32, u64)]) {
    out.put_u32(counts.len() as u32);
    for (id, count) in counts {
        out.put_u32(*id);
        out.put_u64(*count);
    }
}

fn read_counts(reader: &mut Reader<'_>) -> Result<Vec<(u32, u64)>, FormatError> {
    let count = reader.u32("count entries")? as usize;
    let count = bounded_count(count, 12, reader, "count entries exceeds input")?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let id = reader.u32("count id")?;
        let value = reader.u64("count value")?;
        entries.push((id, value));
    }
    Ok(entries)
}

fn put_dictionary(out: &mut Writer, values: &[String]) {
    out.put_u32(values.len() as u32);
    for value in values {
        put_string(out, value);
    }
}

fn read_dictionary(reader: &mut Reader<'_>) -> Result<Vec<String>, FormatError> {
    let count = reader.u32("dictionary count")? as usize;
    let count = bounded_count(count, 4, reader, "dictionary count exceeds input")?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_string(reader)?);
    }
    Ok(values)
}

struct SectionArea {
    bytes: Writer,
    directory: Vec<(u32, Range<usize>)>,
}

impl SectionArea {
    fn push(&mut self, id: u32, write: impl FnOnce(&mut Writer)) {
        let start = self.bytes.len();
        write(&mut self.bytes);
        self.directory.push((id, start..self.bytes.len()));
    }
}

fn encode_sections(footer: &Footer) -> SectionArea {
    let mut sections = SectionArea {
        bytes: Writer::new(),
        directory: Vec::new(),
    };

    sections.push(sections::COLUMNS, |out| {
        out.put_u32(footer.columns.len() as u32);
        for column in &footer.columns {
            out.put_u32(column.column_id);
            put_string(out, &column.name);
            out.put_u8(column.kind as u8);
            out.put_u8(u8::from(column.nullable));
            out.put_u8(u8::from(column.internal_only));
        }
    });

    sections.push(sections::STRIPES, |out| {
        out.put_u32(footer.stripes.len() as u32);
        for stripe in &footer.stripes {
            out.put_u32(stripe.stripe_id);
            out.put_u64(stripe.file_offset);
            out.put_u64(stripe.byte_len);
            out.put_u64(stripe.first_row_ordinal);
            out.put_u64(stripe.row_count);
        }
    });

    sections.push(sections::GRANULES, |out| {
        out.put_u32(footer.granules.len() as u32);
        for granule in &footer.granules {
            out.put_u32(granule.granule_id);
            out.put_u32(granule.stripe_id);
            out.put_u64(granule.first_row_ordinal);
            out.put_u32(granule.row_count);
            out.put_u64(granule.first_epoch);
            out.put_u64(granule.first_sequence);
            out.put_u64(granule.last_epoch);
            out.put_u64(granule.last_sequence);
            out.put_i64(granule.min_occurred_at_physical);
            out.put_i64(granule.max_occurred_at_physical);
            out.put_i64(granule.min_ingested_at_physical);
            out.put_i64(granule.max_ingested_at_physical);
            out.put_u64(granule.compressed_bytes_estimate);
        }
    });

    sections.push(sections::MARKS, |out| {
        if footer.required_feature_flags & required_features::STRIPE_MARKS_PAGES != 0 {
            out.put_u32(footer.marks_page_offsets.len() as u32);
            for entry in &footer.marks_page_offsets {
                out.put_u32(entry.projection_id);
                out.put_u32(entry.column_id);
                out.put_u32(entry.stripe_id);
                out.put_u64(entry.page_offset);
                out.put_u64(entry.page_len);
            }
        } else if footer.required_feature_flags & required_features::COLUMNAR_MARKS != 0 {
            let granule_stripe_ids = footer
                .granules
                .iter()
                .map(|granule| (granule.granule_id, granule.stripe_id))
                .collect::<BTreeMap<_, _>>();
            #[expect(
                clippy::expect_used,
                reason = "the footer builder only records marks for granules it also lists in the granule directory"
            )]
            let columnar = encode_columnar_marks(&footer.marks, &footer.page_directory, &granule_stripe_ids)
                .expect("columnar marks encode: every mark's granule must appear in the granule directory");
            out.put_slice(&columnar);
        } else {
            out.put_u32(footer.marks.len() as u32);
            for mark in &footer.marks {
                out.put_u32(mark.column_id);
                out.put_u32(mark.projection_id);
                out.put_u32(mark.granule_id);
                out.put_u64(mark.compressed_offset);
                out.put_u64(mark.compressed_size);
                out.put_u64(mark.uncompressed_offset);
                out.put_u64(mark.uncompressed_size);
                out.put_u32(mark.row_count);
                out.put_u32(mark.page_count);
                out.put_u32(mark.codec_pipeline_id.0);
                put_opt_u64(out, mark.first_value_offset);
            }
        }
    });

    sections.push(sections::DICTIONARIES, |out| {
        put_dictionary(out, &footer.dictionaries.source);
        put_dictionary(out, &footer.dictionaries.event_type);
        put_dictionary(out, &footer.dictionaries.entity_type);
    });

    sections.push(sections::PAGE_STATS, |out| {
        out.put_u32(footer.page_stats.len() as u32);
        for stats in &footer.page_stats {
            out.put_u32(stats.column_id);
            out.put_u32(stats.granule_id);
            out.put_u32(stats.row_count);
            out.put_u32(stats.null_count);
            put_opt_i128(out, stats.min_i128);
            put_opt_i128(out, stats.max_i128);
            put_opt_f64(out, stats.min_f64);
            put_opt_f64(out, stats.max_f64);
        }
    });

    sections.push(sections::EXACT_COUNTS, |out| {
        out.put_u64(footer.exact_counts.row_count);
        put_counts(out, &footer.exact_counts.by_source);
        put_counts(out, &footer.exact_counts.by_event_type);
        put_counts(out, &footer.exact_counts.by_entity_type);
    });

    sections.push(sections::PAYLOAD_GRANULES, |out| {
        out.put_u32(footer.payload_granules.len() as u32);
        for payload in &footer.payload_granules {
            out.put_u32(payload.granule_id);
            out.put_u64(payload.dictionary_offset);
            out.put_u64(payload.dictionary_len);
            out.put_u64(payload.offsets_offset);
            out.put_u64(payload.offsets_len);
            out.put_u64(payload.residual_offset);
            out.put_u64(payload.residual_len);
            out.put_u8(payload.residual_compression as u8);
        }
    });

    sections.push(sections::PRESENCE, |out| {
        out.put_u32(footer.presence.len() as u32);
        for entry in &footer.presence {
            out.put_u32(entry.column_id);
            out.put_u32(entry.since_schema_version);
        }
    });

    sections.push(sections::SHREDDED, |out| {
        out.put_u32(footer.shredded.len() as u32);
        for entry in &footer.shredded {
            out.put_u32(entry.column_id);
            put_string(out, &entry.path);
        }
    });

    sections.push(sections::FREETEXT, |out| {
        out.put_u32(footer.freetext.len() as u32);
        for entry in &footer.freetext {
            out.put_u32(entry.column_id);
            put_string(out, &entry.declared_field);
        }
    });

    sections.push(sections::STRIPE_CHECKSUMS, |out| {
        out.put_u32(footer.stripe_checksums.len() as u32);
        for checksum in &footer.stripe_checksums {
            out.put_slice(checksum);
        }
    });

    sections.push(sections::INTEGRITY_GAPS, |out| {
        out.put_u32(footer.integrity_gaps.len() as u32);
        for gap in &footer.integrity_gaps {
            out.put_u64(gap.file_offset);
            out.put_u64(gap.byte_len);
            out.put_slice(&gap.blake3);
        }
    });

    sections.push(sections::STRIPE_PROOFS, |out| {
        out.put_u32(footer.stripe_proofs.len() as u32);
        for proof in &footer.stripe_proofs {
            out.put_u32(proof.stripe_id);
            out.put_u32(proof.chunk_group_bytes);
            out.put_u64(proof.tree_offset);
            out.put_u64(proof.tree_len);
        }
    });

    if !footer.freetext_row_offsets.is_empty() {
        sections.push(sections::FREETEXT_ROW_OFFSETS, |out| {
            out.put_u32(footer.freetext_row_offsets.len() as u32);
            for entry in &footer.freetext_row_offsets {
                out.put_u32(entry.column_id);
                out.put_u32(entry.granule_id);
                out.put_u64(entry.offsets_offset);
                out.put_u64(entry.offsets_len);
                out.put_u64(entry.bytes_offset);
                out.put_u64(entry.bytes_len);
            }
        });
    }

    if !footer.embedding_row_offsets.is_empty() {
        sections.push(sections::EMBEDDING_ROW_OFFSETS, |out| {
            out.put_u32(footer.embedding_row_offsets.len() as u32);
            for entry in &footer.embedding_row_offsets {
                out.put_u32(entry.column_id);
                out.put_u32(entry.granule_id);
                out.put_u64(entry.offsets_offset);
                out.put_u64(entry.offsets_len);
                out.put_u64(entry.bytes_offset);
                out.put_u64(entry.bytes_len);
            }
        });
    }

    if !footer.clustering.is_empty() {
        sections.push(sections::CLUSTERING_METADATA, |out| {
            out.put_u32(footer.clustering.len() as u32);
            for entry in &footer.clustering {
                out.put_u32(entry.projection_id);
                out.put_u32(entry.granule_id);
                out.put_u32(entry.clustering_quality.to_bits());
                match &entry.sortedness_proof {
                    None => out.put_u8(0),
                    Some(proof) => {
                        out.put_u8(1);
                        out.put_u8(match proof.direction {
                            SortDirection::Ascending => 0,
                            SortDirection::Descending => 1,
                        });
                        out.put_u32(proof.column_names.len() as u32);
                        for name in &proof.column_names {
                            put_string(out, name);
                        }
                    }
                }
            }
        });
    }

    if footer.io_alignment_bytes > 0 {
        sections.push(sections::IO_ALIGNMENT, |out| {
            out.put_u32(footer.io_alignment_bytes);
        });
    }

    if !footer.escape_hatches.is_empty() {
        sections.push(sections::ESCAPE_HATCHES, |out| {
            out.put_u32(footer.escape_hatches.len() as u32);
            for hatch in &footer.escape_hatches {
                out.put_u32(hatch.min_reader_version);
                put_string(out, &hatch.portable_decoder_ref);
                out.put_u64(hatch.optional_feature_bit);
            }
        });
    }

    if footer.required_feature_flags & required_features::COLUMNAR_MARKS == 0 && !footer.page_directory.is_empty() {
        sections.push(sections::PAGE_DIRECTORY, |out| {
            out.put_u32(footer.page_directory.len() as u32);
            for entry in &footer.page_directory {
                out.put_u32(entry.column_id);
                out.put_u32(entry.projection_id);
                out.put_u32(entry.granule_id);
                out.put_u32(entry.page_index);
                out.put_u64(entry.compressed_offset);
                out.put_u64(entry.compressed_len);
                out.put_u64(entry.first_row_ordinal);
                out.put_u32(entry.row_count);
                out.put_u32(entry.null_count);
                put_opt_i128(out, entry.min_i128);
                put_opt_i128(out, entry.max_i128);
                put_opt_f64(out, entry.min_f64);
                put_opt_f64(out, entry.max_f64);
                out.put_u64(entry.min_sequence);
                out.put_u64(entry.max_sequence);
                out.put_i64(entry.min_occurred_at_physical);
                out.put_i64(entry.max_occurred_at_physical);
            }
        });
    }

    if !footer.page_minmax.is_empty() {
        sections.push(sections::PAGE_MINMAX, |out| {
            out.put_u32(footer.page_minmax.len() as u32);
            for entry in &footer.page_minmax {
                out.put_u32(entry.column_id);
                out.put_u32(entry.granule_id);
                out.put_u32(entry.page_index);
                out.put_u32(entry.row_count);
                out.put_u32(entry.null_count);
                put_opt_i128(out, entry.min_i128);
                put_opt_i128(out, entry.max_i128);
            }
        });
    }

    if !footer.text_token_indexes.is_empty() {
        sections.push(sections::TEXT_TOKEN, |out| {
            out.put_u32(footer.text_token_indexes.len() as u32);
            for entry in &footer.text_token_indexes {
                out.put_u32(entry.column_id);
                out.put_u32(entry.granule_id);
                out.put_u32(entry.page_index);
                out.put_u32(entry.index_bytes.len() as u32);
                out.put_slice(&entry.index_bytes);
            }
        });
    }

    if !footer.text_token_offsets.is_empty() {
        sections.push(sections::TEXT_TOKEN_OFFSETS, |out| {
            out.put_u32(footer.text_token_offsets.len() as u32);
            for entry in &footer.text_token_offsets {
                out.put_u32(entry.column_id);
                out.put_u32(entry.granule_id);
                out.put_u32(entry.page_index);
                out.put_u64(entry.index_offset);
                out.put_u64(entry.index_len);
            }
        });
    }

    if !footer.entity_hash_filters.is_empty() {
        sections.push(sections::ENTITY_HASH_FILTERS, |out| {
            out.put_u32(footer.entity_hash_filters.len() as u32);
            for entry in &footer.entity_hash_filters {
                out.put_u32(entry.granule_id);
                out.put_u64(entry.index_offset);
                out.put_u64(entry.index_len);
            }
        });
    }

    if !footer.external_ids.is_empty() {
        if footer.external_ids.iter().all(|e| e.id_hash & 0xFFFF_FFFF == 0) {
            sections.push(sections::EXTERNAL_IDS_32, |out| {
                encode_external_ids_32(&footer.external_ids, out);
            });
        } else {
            sections.push(sections::EXTERNAL_IDS_PACKED, |out| {
                encode_external_ids_packed(&footer.external_ids, out);
            });
        }
    }

    if !footer.inbox_recipients.is_empty() {
        sections.push(sections::INBOX_RECIPIENTS, |out| {
            out.put_u32(footer.inbox_recipients.len() as u32);
            for entry in &footer.inbox_recipients {
                out.put_u64(entry.entity_id_hash_low);
                out.put_u64(entry.entity_id_hash_high);
                out.put_u64(entry.first_row_ordinal);
                out.put_u64(entry.row_count);
                out.put_u64(entry.last_epoch);
                out.put_u64(entry.last_sequence);
            }
        });
    }

    if let Some(filters) = &footer.reference_filters {
        sections.push(sections::REFERENCE_FILTERS, |out| {
            out.put_u32(filters.len() as u32);
            for entry in filters {
                out.put_u32(entry.column_id);
                out.put_u32(entry.granule_id);
                out.put_u32(entry.filter.len() as u32);
                out.put_slice(&entry.filter);
            }
        });
    }

    if !footer.stripe_ndv.is_empty() {
        sections.push(sections::STRIPE_NDV, |out| {
            out.put_u32(footer.stripe_ndv.len() as u32);
            for entry in &footer.stripe_ndv {
                out.put_u32(entry.column_id);
                out.put_u32(entry.stripe_id);
                out.put_u64(entry.distinct_count);
                out.put_u8(u8::from(entry.exact));
            }
        });
    }

    if !footer.shared_dictionaries.is_empty() {
        sections.push(sections::SHARED_DICTIONARIES, |out| {
            out.put_u32(footer.shared_dictionaries.len() as u32);
            for entry in &footer.shared_dictionaries {
                out.put_u32(entry.column_id);
                out.put_u32(entry.values.len() as u32);
                for value in &entry.values {
                    out.put_u16(value.len() as u16);
                    out.put_slice(value.as_bytes());
                }
            }
        });
    }

    if !footer.packed_shared_dictionaries.is_empty() {
        sections.push(sections::PACKED_SHARED_DICTIONARIES, |out| {
            out.put_u32(footer.packed_shared_dictionaries.len() as u32);
            for entry in &footer.packed_shared_dictionaries {
                out.put_u32(entry.column_id);
                out.put_u32(entry.value_count);
                out.put_u32(entry.plain_len);
                out.put_u32(entry.compressed.len() as u32);
                out.put_slice(&entry.compressed);
            }
        });
    }

    if footer.profile != Profile::Event {
        sections.push(sections::PROFILE, |out| out.put_u8(footer.profile.code()));
    }

    if let Some(audit) = &footer.audit {
        sections.push(sections::AUDIT, |out| {
            out.put_slice(&audit.chain_start);
            out.put_slice(&audit.chain_end);
            out.put_u8(u8::from(audit.legal_hold));
            match audit.retention {
                Some(retention) => {
                    out.put_u8(1);
                    out.put_u8(retention.mode.code());
                    out.put_i64(retention.retain_until.physical_nanos());
                }
                None => out.put_u8(0),
            }
        });
    }

    if let Some(graph) = &footer.graph {
        sections.push(sections::GRAPH_EDGES, |out| {
            out.put_u8(graph.subject.len() as u8);
            out.put_slice(&graph.subject);
            out.put_u64(graph.first_sn);
            out.put_u32(graph.blocks.len() as u32);
            for entry in &graph.blocks {
                out.put_u32(entry.granule_id);
                out.put_u64(entry.offset);
                out.put_u64(entry.len);
            }
        });
        sections.push(sections::GRAPH_UNRESOLVED, |out| {
            encode_unresolved(out, &graph.unresolved)
        });
        if let Some(index) = &graph.event_ids {
            sections.push(sections::GRAPH_EVENT_IDS, |out| index.encode(out));
        }
        if let Some(cover) = &graph.chain_cover {
            sections.push(sections::GRAPH_CHAIN_COVER, |out| cover.encode(out));
        }
        if let Some(depth) = graph.depth {
            sections.push(sections::GRAPH_DEPTH, |out| {
                out.put_u64(depth.min);
                out.put_u64(depth.max);
            });
        }
    }

    if !footer.state_roots.is_empty() {
        sections.push(sections::GRAPH_STATE_ROOTS, |out| {
            out.put_u32(footer.state_roots.len() as u32);
            for entry in &footer.state_roots {
                out.put_u64(entry.row_ordinal);
                out.put_slice(&entry.root.address);
                out.put_u32(entry.root.entry_count);
            }
        });
    }

    if let Some(index) = &footer.snapshot_keys {
        sections.push(sections::SNAPSHOT_KEYS, |out| out.put_slice(&index.encode()));
    }

    if let Some(rollups) = &footer.time_series_rollups {
        sections.push(sections::TIME_SERIES_ROLLUPS, |out| rollups.encode_into(out));
    }

    if let Some(summary) = &footer.entity_summary {
        sections.push(sections::ENTITY_SUMMARY, |out| summary.encode_into(out));
    }

    if !footer.vector_columns.is_empty() {
        sections.push(sections::VECTOR_COLUMNS, |out| {
            out.put_u32(footer.vector_columns.len() as u32);
            for meta in &footer.vector_columns {
                meta.encode_into(out);
            }
        });
    }

    if !footer.sparse_keys.is_empty() {
        sections.push(sections::SPARSE_KEYS, |out| {
            out.put_u32(footer.sparse_keys.len() as u32);
            for entry in &footer.sparse_keys {
                out.put_u32(entry.column_id);
                out.put_u16(entry.path.len() as u16);
                out.put_slice(entry.path.as_bytes());
            }
        });
    }

    sections
}

const PREAMBLE_LEN: usize = 2 + 2 + 8 + 8 + 32 + 4;

const DIRECTORY_ENTRY_LEN: usize = 4 + 8 + 8 + 32;

pub fn encode_footer(footer: &Footer) -> Vec<u8> {
    assemble_footer(footer, encode_sections(footer))
}

fn assemble_footer(footer: &Footer, sections: SectionArea) -> Vec<u8> {
    let area = sections.bytes.into_bytes();
    let mut out = Writer::with_capacity(PREAMBLE_LEN + sections.directory.len() * DIRECTORY_ENTRY_LEN + area.len());
    out.put_u16(footer.format_version.0);
    out.put_u16(footer.format_version.1);
    out.put_u64(footer.required_feature_flags);
    out.put_u64(footer.optional_feature_flags);
    out.put_slice(&footer.schema_fingerprint);
    out.put_u32(sections.directory.len() as u32);
    for (id, range) in &sections.directory {
        let bytes = area.get(range.clone()).unwrap_or_default();
        out.put_u32(*id);
        out.put_u64(range.start as u64);
        out.put_u64(bytes.len() as u64);
        out.put_slice(crate::file::integrity::hash_tree(bytes).as_bytes());
    }
    out.put_slice(&area);
    out.into_bytes()
}

fn decode_section<'a>(
    directory: &[(u32, u64, u64, [u8; 32])],
    section_area: &'a [u8],
    id: u32,
) -> Result<Option<&'a [u8]>, FormatError> {
    let Some((_, offset, len, checksum)) = directory.iter().find(|(sid, ..)| *sid == id) else {
        return Ok(None);
    };
    let bytes = slice(section_area, *offset as usize, *len as usize, "section")?;
    if crate::file::integrity::hash_tree(bytes).as_bytes() != checksum {
        return Err(FormatError::Blake3Mismatch {
            scope: "footer section",
        });
    }
    Ok(Some(bytes))
}

fn read_hash(reader: &mut Reader<'_>, what: &'static str) -> Result<[u8; 32], FormatError> {
    let mut hash = [0u8; 32];
    hash.copy_from_slice(reader.take(32, what)?);
    Ok(hash)
}

fn decode_audit_trail(bytes: &[u8]) -> Result<AuditTrail, FormatError> {
    let mut reader = Reader::new(bytes);
    let chain_start = read_hash(&mut reader, "audit chain start")?;
    let chain_end = read_hash(&mut reader, "audit chain end")?;
    let legal_hold = match reader.u8("audit legal hold")? {
        0 => false,
        1 => true,
        _ => {
            return Err(FormatError::Structural {
                rule: "audit legal hold must be 0 or 1",
            });
        }
    };
    let retention = match reader.u8("audit retention tag")? {
        0 => None,
        1 => Some(ObjectLockRetention {
            mode: RetentionMode::from_u8(reader.u8("audit retention mode")?)?,
            retain_until: TimestampValue::from_physical_nanos(reader.i64("audit retain until")?),
        }),
        _ => {
            return Err(FormatError::Structural {
                rule: "footer option tag must be 0 or 1",
            });
        }
    };
    if reader.remaining() != 0 {
        return Err(FormatError::Structural {
            rule: "audit section has trailing bytes",
        });
    }
    Ok(AuditTrail {
        chain_end,
        chain_start,
        legal_hold,
        retention,
    })
}

fn require<'a>(section: Option<&'a [u8]>, rule: &'static str) -> Result<&'a [u8], FormatError> {
    section.ok_or(FormatError::Structural { rule })
}

const EXTERNAL_ID_BLOCK: usize = 128;

fn bit_width(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

struct BitPacker {
    bytes: Vec<u8>,
    acc: u128,
    bits: u32,
}

impl BitPacker {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            acc: 0,
            bits: 0,
        }
    }

    fn put(&mut self, value: u64, width: u8) {
        if width == 0 {
            return;
        }
        let mask = if width >= 64 { u64::MAX } else { (1u64 << width) - 1 };
        self.acc |= u128::from(value & mask) << self.bits;
        self.bits += u32::from(width);
        while self.bits >= 8 {
            self.bytes.push(self.acc as u8);
            self.acc >>= 8;
            self.bits -= 8;
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bits > 0 {
            self.bytes.push(self.acc as u8);
        }
        self.bytes
    }
}

fn unpack_bits(bytes: &[u8], width: u8, n: usize, mut sink: impl FnMut(u64)) -> Result<(), FormatError> {
    if width == 0 {
        for _ in 0..n {
            sink(0);
        }
        return Ok(());
    }
    let width = usize::from(width);
    if bytes.len() * 8 < n * width {
        return Err(FormatError::Structural {
            rule: "external id packed bits truncated",
        });
    }
    let mut padded = Vec::with_capacity(bytes.len() + 16);
    padded.extend_from_slice(bytes);
    padded.resize(bytes.len() + 16, 0);
    let mask = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
    let mut bit = 0usize;
    for _ in 0..n {
        let start = bit / 8;
        let window: [u8; 16] =
            padded
                .get(start..start + 16)
                .and_then(|w| w.try_into().ok())
                .ok_or(FormatError::Structural {
                    rule: "external id packed bits truncated",
                })?;
        sink(((u128::from_le_bytes(window) >> (bit % 8)) as u64) & mask);
        bit += width;
    }
    Ok(())
}

pub fn encode_external_ids_packed(entries: &[ExternalIdEntry], out: &mut Writer) {
    out.put_u32(entries.len() as u32);
    let Some(first) = entries.first() else {
        return;
    };
    let max_ordinal = entries.iter().map(|entry| entry.row_ordinal).max().unwrap_or(0);
    let ordinal_width = bit_width(max_ordinal);
    out.put_u8(ordinal_width);
    out.put_u64(first.id_hash);
    let deltas: Vec<u64> = entries
        .windows(2)
        .map(|pair| match pair {
            [a, b] => b.id_hash.wrapping_sub(a.id_hash),
            _ => 0,
        })
        .collect();
    for block in deltas.chunks(EXTERNAL_ID_BLOCK) {
        let width = block.iter().copied().map(bit_width).max().unwrap_or(0);
        out.put_u8(width);
        let mut packer = BitPacker::new();
        for &delta in block {
            packer.put(delta, width);
        }
        out.put_slice(&packer.finish());
    }
    let mut packer = BitPacker::new();
    for entry in entries {
        packer.put(entry.row_ordinal, ordinal_width);
    }
    out.put_slice(&packer.finish());
}

fn check_external_id_ordinals_fit(count: usize, ordinal_width: u8, reader: &Reader<'_>) -> Result<(), FormatError> {
    if ordinal_width < bit_width((count - 1) as u64) {
        return Err(FormatError::Structural {
            rule: "external id ordinal width cannot hold one distinct row per entry",
        });
    }
    if packed_len(count, ordinal_width)? > reader.remaining() {
        return Err(FormatError::Structural {
            rule: "external id count exceeds input",
        });
    }
    Ok(())
}

fn packed_len(count: usize, width: u8) -> Result<usize, FormatError> {
    count
        .checked_mul(usize::from(width))
        .map(|bits| bits.div_ceil(8))
        .ok_or(FormatError::Structural {
            rule: "external id packed length overflows",
        })
}

pub fn decode_external_ids_packed(bytes: &[u8]) -> Result<Vec<ExternalIdEntry>, FormatError> {
    let mut r = Reader::new(bytes);
    let count = r.u32("external id count")? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let ordinal_width = r.u8("external id ordinal width")?;
    let mut hash = r.u64("external id first hash")?;
    let blocks = (count - 1).div_ceil(EXTERNAL_ID_BLOCK);
    if blocks > r.remaining() || ordinal_width > 64 {
        return Err(FormatError::Structural {
            rule: "external id count exceeds input",
        });
    }
    check_external_id_ordinals_fit(count, ordinal_width, &r)?;
    let mut entries = Vec::with_capacity(count);
    entries.push(ExternalIdEntry {
        id_hash: hash,
        row_ordinal: 0,
    });
    let mut left = count - 1;
    while left > 0 {
        let n = left.min(EXTERNAL_ID_BLOCK);
        let width = r.u8("external id delta width")?;
        if width > 64 {
            return Err(FormatError::Structural {
                rule: "external id delta width exceeds 64",
            });
        }
        let bytes = r.take(packed_len(n, width)?, "external id deltas")?;
        unpack_bits(bytes, width, n, |delta| {
            hash = hash.wrapping_add(delta);
            entries.push(ExternalIdEntry {
                id_hash: hash,
                row_ordinal: 0,
            });
        })?;
        left -= n;
    }
    let bytes = r.take(packed_len(count, ordinal_width)?, "external id rows")?;
    let mut slots = entries.iter_mut();
    unpack_bits(bytes, ordinal_width, count, |row| {
        if let Some(entry) = slots.next() {
            entry.row_ordinal = row;
        }
    })?;
    if r.remaining() != 0 {
        return Err(FormatError::Structural {
            rule: "external id index has trailing bytes",
        });
    }
    Ok(entries)
}

pub fn encode_external_ids_32(entries: &[ExternalIdEntry], out: &mut Writer) {
    out.put_u32(entries.len() as u32);
    let Some(first) = entries.first() else {
        return;
    };
    let max_ordinal = entries.iter().map(|entry| entry.row_ordinal).max().unwrap_or(0);
    let ordinal_width = bit_width(max_ordinal);
    out.put_u8(ordinal_width);
    out.put_u32(first.key());
    let deltas: Vec<u64> = entries
        .windows(2)
        .map(|pair| match pair {
            [a, b] => u64::from(b.key().wrapping_sub(a.key())),
            _ => 0,
        })
        .collect();
    for block in deltas.chunks(EXTERNAL_ID_BLOCK) {
        let width = block.iter().copied().map(bit_width).max().unwrap_or(0);
        out.put_u8(width);
        let mut packer = BitPacker::new();
        for &delta in block {
            packer.put(delta, width);
        }
        out.put_slice(&packer.finish());
    }
    let mut packer = BitPacker::new();
    for entry in entries {
        packer.put(entry.row_ordinal, ordinal_width);
    }
    out.put_slice(&packer.finish());
}

pub fn decode_external_ids_32(bytes: &[u8]) -> Result<Vec<ExternalIdEntry>, FormatError> {
    let mut r = Reader::new(bytes);
    let count = r.u32("external id count")? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let ordinal_width = r.u8("external id ordinal width")?;
    let mut key = u64::from(r.u32("external id first key")?);
    let blocks = (count - 1).div_ceil(EXTERNAL_ID_BLOCK);
    if blocks > r.remaining() || ordinal_width > 64 {
        return Err(FormatError::Structural {
            rule: "external id count exceeds input",
        });
    }
    check_external_id_ordinals_fit(count, ordinal_width, &r)?;
    let mut entries = Vec::with_capacity(count);
    entries.push(ExternalIdEntry {
        id_hash: key << 32,
        row_ordinal: 0,
    });
    let mut left = count - 1;
    while left > 0 {
        let n = left.min(EXTERNAL_ID_BLOCK);
        let width = r.u8("external id delta width")?;
        if width > 32 {
            return Err(FormatError::Structural {
                rule: "external id key delta width exceeds 32",
            });
        }
        let bytes = r.take(packed_len(n, width)?, "external id deltas")?;
        unpack_bits(bytes, width, n, |delta| {
            key += delta;
            entries.push(ExternalIdEntry {
                id_hash: key << 32,
                row_ordinal: 0,
            });
        })?;
        if key > u64::from(u32::MAX) {
            return Err(FormatError::Structural {
                rule: "external id key exceeds 32 bits",
            });
        }
        left -= n;
    }
    let bytes = r.take(packed_len(count, ordinal_width)?, "external id rows")?;
    let mut slots = entries.iter_mut();
    unpack_bits(bytes, ordinal_width, count, |row| {
        if let Some(entry) = slots.next() {
            entry.row_ordinal = row;
        }
    })?;
    if r.remaining() != 0 {
        return Err(FormatError::Structural {
            rule: "external id index has trailing bytes",
        });
    }
    Ok(entries)
}

fn bounded_count(
    count: usize,
    min_entry_bytes: usize,
    reader: &Reader<'_>,
    rule: &'static str,
) -> Result<usize, FormatError> {
    if count > reader.remaining() / min_entry_bytes.max(1) {
        return Err(FormatError::Structural { rule });
    }
    Ok(count)
}

pub fn decode_footer(blob: &[u8]) -> Result<Footer, FormatError> {
    let mut reader = Reader::new(blob);
    let format_version = (reader.u16("footer version major")?, reader.u16("footer version minor")?);
    let required_feature_flags = reader.u64("required feature flags")?;
    let optional_feature_flags = reader.u64("optional feature flags")?;
    let fingerprint_bytes = reader.take(32, "schema fingerprint")?;
    let mut schema_fingerprint = [0u8; 32];
    schema_fingerprint.copy_from_slice(fingerprint_bytes);
    let section_count = reader.u32("section count")? as usize;
    let section_count = bounded_count(
        section_count,
        DIRECTORY_ENTRY_LEN,
        &reader,
        "section count exceeds input",
    )?;
    let mut directory = Vec::with_capacity(section_count);
    for _ in 0..section_count {
        let id = reader.u32("section id")?;
        let offset = reader.u64("section offset")?;
        let len = reader.u64("section len")?;
        let checksum_bytes = reader.take(32, "section checksum")?;
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(checksum_bytes);
        directory.push((id, offset, len, checksum));
    }
    let section_area = reader.take(reader.remaining(), "section area")?;

    let columns_bytes = require(
        decode_section(&directory, section_area, sections::COLUMNS)?,
        "footer missing column directory",
    )?;
    let mut r = Reader::new(columns_bytes);
    let count = r.u32("column count")? as usize;
    let count = bounded_count(count, 11, &r, "column count exceeds input")?;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let column_id = r.u32("column id")?;
        let name = read_string(&mut r)?;
        let kind = ColumnKind::from_u8(r.u8("column kind")?)?;
        let nullable = r.u8("column nullable")? != 0;
        let internal_only = r.u8("column internal")? != 0;
        columns.push(ColumnDescriptor {
            column_id,
            name,
            kind,
            nullable,
            internal_only,
        });
    }

    let stripes_bytes = require(
        decode_section(&directory, section_area, sections::STRIPES)?,
        "footer missing stripe directory",
    )?;
    let mut r = Reader::new(stripes_bytes);
    let count = r.u32("stripe count")? as usize;
    let count = bounded_count(count, 36, &r, "stripe count exceeds input")?;
    let mut stripes = Vec::with_capacity(count);
    for _ in 0..count {
        stripes.push(StripeEntry {
            stripe_id: r.u32("stripe id")?,
            file_offset: r.u64("stripe offset")?,
            byte_len: r.u64("stripe len")?,
            first_row_ordinal: r.u64("stripe first row")?,
            row_count: r.u64("stripe rows")?,
        });
    }

    let granules_bytes = require(
        decode_section(&directory, section_area, sections::GRANULES)?,
        "footer missing granule directory",
    )?;
    let mut r = Reader::new(granules_bytes);
    let count = r.u32("granule count")? as usize;
    let count = bounded_count(count, 92, &r, "granule count exceeds input")?;
    let mut granules = Vec::with_capacity(count);
    for _ in 0..count {
        granules.push(GranuleEntry {
            granule_id: r.u32("granule id")?,
            stripe_id: r.u32("granule stripe")?,
            first_row_ordinal: r.u64("granule first row")?,
            row_count: r.u32("granule rows")?,
            first_epoch: r.u64("granule first epoch")?,
            first_sequence: r.u64("granule first sequence")?,
            last_epoch: r.u64("granule last epoch")?,
            last_sequence: r.u64("granule last sequence")?,
            min_occurred_at_physical: r.i64("granule min occurred")?,
            max_occurred_at_physical: r.i64("granule max occurred")?,
            min_ingested_at_physical: r.i64("granule min ingested")?,
            max_ingested_at_physical: r.i64("granule max ingested")?,
            compressed_bytes_estimate: r.u64("granule bytes estimate")?,
        });
    }

    let marks_bytes = require(
        decode_section(&directory, section_area, sections::MARKS)?,
        "footer missing marks",
    )?;
    let (marks, marks_directory, marks_page_offsets, marks_pages) =
        if required_feature_flags & required_features::STRIPE_MARKS_PAGES != 0 {
            let (marks_page_offsets, _) = decode_columnar_marks_directory(marks_bytes)?;
            (Vec::new(), Vec::new(), marks_page_offsets, Vec::new())
        } else if required_feature_flags & required_features::COLUMNAR_MARKS != 0 {
            let (marks_directory, pages_area) = decode_columnar_marks_directory(marks_bytes)?;
            (Vec::new(), marks_directory, Vec::new(), pages_area.to_vec())
        } else {
            let mut r = Reader::new(marks_bytes);
            let count = r.u32("mark count")? as usize;
            let count = bounded_count(count, 57, &r, "mark count exceeds input")?;
            let mut marks = Vec::with_capacity(count);
            for _ in 0..count {
                marks.push(ColumnMark {
                    column_id: r.u32("mark column")?,
                    projection_id: r.u32("mark projection")?,
                    granule_id: r.u32("mark granule")?,
                    compressed_offset: r.u64("mark offset")?,
                    compressed_size: r.u64("mark size")?,
                    uncompressed_offset: r.u64("mark uoffset")?,
                    uncompressed_size: r.u64("mark usize")?,
                    row_count: r.u32("mark rows")?,
                    page_count: r.u32("mark pages")?,
                    codec_pipeline_id: PipelineId(r.u32("mark pipeline")?),
                    first_value_offset: read_opt_u64(&mut r)?,
                });
            }
            (marks, Vec::new(), Vec::new(), Vec::new())
        };

    let dictionaries_bytes = require(
        decode_section(&directory, section_area, sections::DICTIONARIES)?,
        "footer missing dictionaries",
    )?;
    let mut r = Reader::new(dictionaries_bytes);
    let dictionaries = FileDictionaries {
        source: read_dictionary(&mut r)?,
        event_type: read_dictionary(&mut r)?,
        entity_type: read_dictionary(&mut r)?,
    };

    let stats_bytes = require(
        decode_section(&directory, section_area, sections::PAGE_STATS)?,
        "footer missing page stats",
    )?;
    let mut r = Reader::new(stats_bytes);
    let count = r.u32("stats count")? as usize;
    let count = bounded_count(count, 20, &r, "stats count exceeds input")?;
    let mut page_stats = Vec::with_capacity(count);
    for _ in 0..count {
        page_stats.push(PageStats {
            column_id: r.u32("stats column")?,
            granule_id: r.u32("stats granule")?,
            row_count: r.u32("stats rows")?,
            null_count: r.u32("stats nulls")?,
            min_i128: read_opt_i128(&mut r)?,
            max_i128: read_opt_i128(&mut r)?,
            min_f64: read_opt_f64(&mut r)?,
            max_f64: read_opt_f64(&mut r)?,
        });
    }

    let counts_bytes = require(
        decode_section(&directory, section_area, sections::EXACT_COUNTS)?,
        "footer missing exact counts",
    )?;
    let mut r = Reader::new(counts_bytes);
    let exact_counts = ExactCounts {
        row_count: r.u64("exact rows")?,
        by_source: read_counts(&mut r)?,
        by_event_type: read_counts(&mut r)?,
        by_entity_type: read_counts(&mut r)?,
    };

    let payload_bytes = require(
        decode_section(&directory, section_area, sections::PAYLOAD_GRANULES)?,
        "footer missing payload granules",
    )?;
    let mut r = Reader::new(payload_bytes);
    let count = r.u32("payload granule count")? as usize;
    let count = bounded_count(count, 53, &r, "payload granule count exceeds input")?;
    let mut payload_granules = Vec::with_capacity(count);
    for _ in 0..count {
        payload_granules.push(PayloadGranule {
            granule_id: r.u32("payload granule id")?,
            dictionary_offset: r.u64("payload dict offset")?,
            dictionary_len: r.u64("payload dict len")?,
            offsets_offset: r.u64("payload offsets offset")?,
            offsets_len: r.u64("payload offsets len")?,
            residual_offset: r.u64("payload residual offset")?,
            residual_len: r.u64("payload residual len")?,
            residual_compression: match r.u8("payload residual compression")? {
                0 => ResidualCompression::None,
                1 => ResidualCompression::Zstd3,
                2 => ResidualCompression::ZstdSeekable,
                _ => {
                    return Err(FormatError::Structural {
                        rule: "unknown residual compression",
                    });
                }
            },
        });
    }

    let presence_bytes = require(
        decode_section(&directory, section_area, sections::PRESENCE)?,
        "footer missing presence map",
    )?;
    let mut r = Reader::new(presence_bytes);
    let count = r.u32("presence count")? as usize;
    let count = bounded_count(count, 8, &r, "presence count exceeds input")?;
    let mut presence = Vec::with_capacity(count);
    for _ in 0..count {
        presence.push(PresenceEntry {
            column_id: r.u32("presence column")?,
            since_schema_version: r.u32("presence since")?,
        });
    }

    let shredded_bytes = require(
        decode_section(&directory, section_area, sections::SHREDDED)?,
        "footer missing shred plan",
    )?;
    let mut r = Reader::new(shredded_bytes);
    let count = r.u32("shred count")? as usize;
    let count = bounded_count(count, 8, &r, "shred count exceeds input")?;
    let mut shredded = Vec::with_capacity(count);
    for _ in 0..count {
        shredded.push(ShredEntry {
            column_id: r.u32("shred column")?,
            path: read_string(&mut r)?,
        });
    }

    let freetext_bytes = require(
        decode_section(&directory, section_area, sections::FREETEXT)?,
        "footer missing freetext declaration",
    )?;
    let mut r = Reader::new(freetext_bytes);
    let count = r.u32("freetext count")? as usize;
    let count = bounded_count(count, 8, &r, "freetext count exceeds input")?;
    let mut freetext = Vec::with_capacity(count);
    for _ in 0..count {
        freetext.push(FreetextEntry {
            column_id: r.u32("freetext column")?,
            declared_field: read_string(&mut r)?,
        });
    }

    let checksum_bytes = require(
        decode_section(&directory, section_area, sections::STRIPE_CHECKSUMS)?,
        "footer missing stripe checksums",
    )?;
    let mut r = Reader::new(checksum_bytes);
    let count = r.u32("stripe checksum count")? as usize;
    let count = bounded_count(count, 32, &r, "stripe checksum count exceeds input")?;
    let mut stripe_checksums = Vec::with_capacity(count);
    for _ in 0..count {
        let bytes = r.take(32, "stripe checksum")?;
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(bytes);
        stripe_checksums.push(checksum);
    }

    let gap_bytes = require(
        decode_section(&directory, section_area, sections::INTEGRITY_GAPS)?,
        "footer missing integrity gap commitments",
    )?;
    let mut r = Reader::new(gap_bytes);
    let count = r.u32("integrity gap count")? as usize;
    let count = bounded_count(count, 48, &r, "integrity gap count exceeds input")?;
    let mut integrity_gaps = Vec::with_capacity(count);
    for _ in 0..count {
        let file_offset = r.u64("integrity gap offset")?;
        let byte_len = r.u64("integrity gap length")?;
        let mut blake3 = [0u8; 32];
        blake3.copy_from_slice(r.take(32, "integrity gap checksum")?);
        integrity_gaps.push(IntegrityGapEntry {
            blake3,
            byte_len,
            file_offset,
        });
    }

    let proof_bytes = require(
        decode_section(&directory, section_area, sections::STRIPE_PROOFS)?,
        "footer missing stripe proof geometry",
    )?;
    let mut r = Reader::new(proof_bytes);
    let count = r.u32("stripe proof count")? as usize;
    let count = bounded_count(count, 24, &r, "stripe proof count exceeds input")?;
    let mut stripe_proofs = Vec::with_capacity(count);
    for _ in 0..count {
        stripe_proofs.push(StripeProofEntry {
            stripe_id: r.u32("stripe proof id")?,
            chunk_group_bytes: r.u32("stripe proof chunk group")?,
            tree_offset: r.u64("stripe proof tree offset")?,
            tree_len: r.u64("stripe proof tree length")?,
        });
    }

    let io_alignment_bytes =
        if let Some(align_bytes) = decode_section(&directory, section_area, sections::IO_ALIGNMENT)? {
            let mut r = Reader::new(align_bytes);
            r.u32("io alignment bytes")?
        } else {
            0
        };

    let mut clustering = Vec::new();
    if let Some(clustering_bytes) = decode_section(&directory, section_area, sections::CLUSTERING_METADATA)? {
        let mut r = Reader::new(clustering_bytes);
        let count = r.u32("clustering entry count")? as usize;
        let count = bounded_count(count, 13, &r, "clustering entry count exceeds input")?;
        clustering = Vec::with_capacity(count);
        for _ in 0..count {
            let projection_id = r.u32("clustering projection id")?;
            let granule_id = r.u32("clustering granule id")?;
            let quality_bits = r.u32("clustering quality bits")?;
            let clustering_quality = f32::from_bits(quality_bits);
            let sortedness_proof = match r.u8("clustering proof tag")? {
                0 => None,
                1 => {
                    let direction = match r.u8("clustering sort direction")? {
                        0 => SortDirection::Ascending,
                        1 => SortDirection::Descending,
                        _ => {
                            return Err(FormatError::Structural {
                                rule: "unknown sort direction",
                            });
                        }
                    };
                    let name_count = r.u32("clustering column name count")? as usize;
                    let name_count = bounded_count(name_count, 4, &r, "column name count exceeds input")?;
                    let mut column_names = Vec::with_capacity(name_count);
                    for _ in 0..name_count {
                        column_names.push(read_string(&mut r)?);
                    }
                    Some(SortednessProof {
                        column_names,
                        direction,
                    })
                }
                _ => {
                    return Err(FormatError::Structural {
                        rule: "unknown clustering proof tag",
                    });
                }
            };
            clustering.push(ClusteringEntry {
                clustering_quality,
                granule_id,
                projection_id,
                sortedness_proof,
            });
        }
    }

    let mut page_minmax = Vec::new();
    if let Some(minmax_bytes) = decode_section(&directory, section_area, sections::PAGE_MINMAX)? {
        let mut r = Reader::new(minmax_bytes);
        let count = r.u32("page minmax count")? as usize;
        let count = bounded_count(count, 20, &r, "page minmax count exceeds input")?;
        page_minmax = Vec::with_capacity(count);
        for _ in 0..count {
            page_minmax.push(PageMinMax {
                column_id: r.u32("page minmax column")?,
                granule_id: r.u32("page minmax granule")?,
                page_index: r.u32("page minmax page")?,
                row_count: r.u32("page minmax rows")?,
                null_count: r.u32("page minmax nulls")?,
                min_i128: read_opt_i128(&mut r)?,
                max_i128: read_opt_i128(&mut r)?,
            });
        }
    }

    let mut escape_hatches = Vec::new();
    if let Some(hatches_bytes) = decode_section(&directory, section_area, sections::ESCAPE_HATCHES)? {
        let mut r = Reader::new(hatches_bytes);
        let count = r.u32("escape hatch count")? as usize;
        let count = bounded_count(count, 16, &r, "escape hatch count exceeds input")?;
        escape_hatches = Vec::with_capacity(count);
        for _ in 0..count {
            let min_reader_version = r.u32("escape hatch min reader version")?;
            let portable_decoder_ref = read_string(&mut r)?;
            let optional_feature_bit = r.u64("escape hatch optional feature bit")?;
            escape_hatches.push(EscapeHatch {
                min_reader_version,
                optional_feature_bit,
                portable_decoder_ref,
            });
        }
    }

    let mut freetext_row_offsets = Vec::new();
    if let Some(entries_bytes) = decode_section(&directory, section_area, sections::FREETEXT_ROW_OFFSETS)? {
        let mut r = Reader::new(entries_bytes);
        let count = r.u32("freetext row offsets count")? as usize;
        let count = bounded_count(count, 40, &r, "freetext row offsets count exceeds input")?;
        freetext_row_offsets = Vec::with_capacity(count);
        for _ in 0..count {
            freetext_row_offsets.push(FreetextRowOffsets {
                column_id: r.u32("freetext row offsets column")?,
                granule_id: r.u32("freetext row offsets granule")?,
                offsets_offset: r.u64("freetext row offsets offset")?,
                offsets_len: r.u64("freetext row offsets len")?,
                bytes_offset: r.u64("freetext row bytes offset")?,
                bytes_len: r.u64("freetext row bytes len")?,
            });
        }
    }

    let mut embedding_row_offsets = Vec::new();
    if let Some(entries_bytes) = decode_section(&directory, section_area, sections::EMBEDDING_ROW_OFFSETS)? {
        let mut r = Reader::new(entries_bytes);
        let count = r.u32("embedding row offsets count")? as usize;
        let count = bounded_count(count, 40, &r, "embedding row offsets count exceeds input")?;
        embedding_row_offsets = Vec::with_capacity(count);
        for _ in 0..count {
            embedding_row_offsets.push(EmbeddingRowOffsets {
                column_id: r.u32("embedding row offsets column")?,
                granule_id: r.u32("embedding row offsets granule")?,
                offsets_offset: r.u64("embedding row offsets offset")?,
                offsets_len: r.u64("embedding row offsets len")?,
                bytes_offset: r.u64("embedding row bytes offset")?,
                bytes_len: r.u64("embedding row bytes len")?,
            });
        }
    }

    let mut page_directory = Vec::new();
    if let Some(dir_bytes) = decode_section(&directory, section_area, sections::PAGE_DIRECTORY)? {
        let mut r = Reader::new(dir_bytes);
        let count = r.u32("page directory count")? as usize;
        let count = bounded_count(count, 84, &r, "page directory count exceeds input")?;
        page_directory = Vec::with_capacity(count);
        for _ in 0..count {
            let entry = PageDirectoryEntry {
                column_id: r.u32("page dir column")?,
                projection_id: r.u32("page dir projection")?,
                granule_id: r.u32("page dir granule")?,
                page_index: r.u32("page dir index")?,
                compressed_offset: r.u64("page dir compressed offset")?,
                compressed_len: r.u64("page dir compressed len")?,
                first_row_ordinal: r.u64("page dir first row")?,
                row_count: r.u32("page dir rows")?,
                null_count: r.u32("page dir nulls")?,
                min_i128: read_opt_i128(&mut r)?,
                max_i128: read_opt_i128(&mut r)?,
                min_f64: read_opt_f64(&mut r)?,
                max_f64: read_opt_f64(&mut r)?,
                min_sequence: r.u64("page dir min seq")?,
                max_sequence: r.u64("page dir max seq")?,
                min_occurred_at_physical: r.i64("page dir min occurred")?,
                max_occurred_at_physical: r.i64("page dir max occurred")?,
            };
            validate_page_directory_entry(&entry)?;
            page_directory.push(entry);
        }
    }

    let mut text_token_indexes = Vec::new();
    if let Some(token_bytes) = decode_section(&directory, section_area, sections::TEXT_TOKEN)? {
        let mut r = Reader::new(token_bytes);
        let count = r.u32("text token entry count")? as usize;
        let count = bounded_count(count, 16, &r, "text token entry count exceeds input")?;
        text_token_indexes = Vec::with_capacity(count);
        for _ in 0..count {
            let column_id = r.u32("text token column")?;
            let granule_id = r.u32("text token granule")?;
            let page_index = r.u32("text token page")?;
            let len = r.u32("text token index length")? as usize;
            let index_bytes = r.take(len, "text token index bytes")?.to_vec();
            text_token_indexes.push(TextTokenEntry {
                column_id,
                granule_id,
                index_bytes,
                page_index,
            });
        }
    }

    let mut text_token_offsets = Vec::new();
    if let Some(offset_bytes) = decode_section(&directory, section_area, sections::TEXT_TOKEN_OFFSETS)? {
        let mut r = Reader::new(offset_bytes);
        let count = r.u32("text token offsets entry count")? as usize;
        let count = bounded_count(count, 28, &r, "text token offsets entry count exceeds input")?;
        text_token_offsets = Vec::with_capacity(count);
        for _ in 0..count {
            let column_id = r.u32("text token offsets column")?;
            let granule_id = r.u32("text token offsets granule")?;
            let page_index = r.u32("text token offsets page")?;
            let index_offset = r.u64("text token offsets offset")?;
            let index_len = r.u64("text token offsets length")?;
            text_token_offsets.push(TextTokenOffsetsEntry {
                column_id,
                granule_id,
                index_len,
                index_offset,
                page_index,
            });
        }
    }

    let mut entity_hash_filters = Vec::new();
    if let Some(filter_bytes) = decode_section(&directory, section_area, sections::ENTITY_HASH_FILTERS)? {
        let mut r = Reader::new(filter_bytes);
        let count = r.u32("entity hash filter count")? as usize;
        let count = bounded_count(count, 20, &r, "entity hash filter count exceeds input")?;
        entity_hash_filters = Vec::with_capacity(count);
        for _ in 0..count {
            let granule_id = r.u32("entity hash filter granule")?;
            let index_offset = r.u64("entity hash filter offset")?;
            let index_len = r.u64("entity hash filter length")?;
            entity_hash_filters.push(EntityHashFilterEntry {
                granule_id,
                index_len,
                index_offset,
            });
        }
    }

    let mut external_ids = Vec::new();
    if let Some(index_bytes) = decode_section(&directory, section_area, sections::EXTERNAL_IDS_32)? {
        external_ids = decode_external_ids_32(index_bytes)?;
    } else if let Some(index_bytes) = decode_section(&directory, section_area, sections::EXTERNAL_IDS_PACKED)? {
        external_ids = decode_external_ids_packed(index_bytes)?;
    } else if let Some(index_bytes) = decode_section(&directory, section_area, sections::EXTERNAL_IDS)? {
        let mut r = Reader::new(index_bytes);
        let count = r.u32("external id count")? as usize;
        let count = bounded_count(count, 16, &r, "external id count exceeds input")?;
        external_ids = Vec::with_capacity(count);
        for _ in 0..count {
            let id_hash = r.u64("external id hash")?;
            let row_ordinal = r.u64("external id row")?;
            external_ids.push(ExternalIdEntry { id_hash, row_ordinal });
        }
    }

    let mut inbox_recipients = Vec::new();
    if let Some(directory_bytes) = decode_section(&directory, section_area, sections::INBOX_RECIPIENTS)? {
        let mut r = Reader::new(directory_bytes);
        let count = r.u32("inbox recipient count")? as usize;
        let count = bounded_count(count, 48, &r, "inbox recipient count exceeds input")?;
        inbox_recipients = Vec::with_capacity(count);
        for _ in 0..count {
            let entity_id_hash_low = r.u64("inbox recipient hash low")?;
            let entity_id_hash_high = r.u64("inbox recipient hash high")?;
            let first_row_ordinal = r.u64("inbox recipient first row")?;
            let row_count = r.u64("inbox recipient row count")?;
            let last_epoch = r.u64("inbox recipient last epoch")?;
            let last_sequence = r.u64("inbox recipient last sequence")?;
            inbox_recipients.push(InboxRecipientEntry {
                entity_id_hash_high,
                entity_id_hash_low,
                first_row_ordinal,
                last_epoch,
                last_sequence,
                row_count,
            });
        }
    }

    let mut reference_filters = None;
    if let Some(filter_bytes) = decode_section(&directory, section_area, sections::REFERENCE_FILTERS)? {
        let mut r = Reader::new(filter_bytes);
        let count = r.u32("reference filter count")? as usize;
        let count = bounded_count(count, 12, &r, "reference filter count exceeds input")?;
        let mut filters = Vec::with_capacity(count);
        for _ in 0..count {
            let column_id = r.u32("reference filter column")?;
            let granule_id = r.u32("reference filter granule")?;
            let len = r.u32("reference filter length")? as usize;
            let filter = r.take(len, "reference filter bytes")?.to_vec();
            filters.push(ReferenceFilterEntry {
                column_id,
                filter,
                granule_id,
            });
        }
        reference_filters = Some(filters);
    }

    let mut stripe_ndv = Vec::new();
    if let Some(ndv_bytes) = decode_section(&directory, section_area, sections::STRIPE_NDV)? {
        let mut r = Reader::new(ndv_bytes);
        let count = r.u32("stripe ndv entry count")? as usize;
        let count = bounded_count(count, 17, &r, "stripe ndv entry count exceeds input")?;
        stripe_ndv = Vec::with_capacity(count);
        for _ in 0..count {
            let column_id = r.u32("stripe ndv column")?;
            let stripe_id = r.u32("stripe ndv stripe")?;
            let distinct_count = r.u64("stripe ndv distinct count")?;
            let exact = r.u8("stripe ndv exact flag")? != 0;
            stripe_ndv.push(StripeNdvEntry {
                column_id,
                distinct_count,
                exact,
                stripe_id,
            });
        }
    }

    let mut shared_dictionaries = Vec::new();
    if let Some(shared_bytes) = decode_section(&directory, section_area, sections::SHARED_DICTIONARIES)? {
        let mut r = Reader::new(shared_bytes);
        let count = r.u32("shared dictionary count")? as usize;
        let count = bounded_count(count, 8, &r, "shared dictionary count exceeds input")?;
        shared_dictionaries = Vec::with_capacity(count);
        for _ in 0..count {
            let column_id = r.u32("shared dictionary column")?;
            let value_count = r.u32("shared dictionary value count")? as usize;
            let value_count = bounded_count(value_count, 2, &r, "shared dictionary value count exceeds input")?;
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                let len = r.u16("shared dictionary value length")? as usize;
                values.push(
                    simdutf8::basic::from_utf8(r.take(len, "shared dictionary value")?)
                        .map_err(|_| FormatError::InvalidUtf8 {
                            what: "shared dictionary value",
                        })?
                        .to_owned(),
                );
            }
            if !values
                .windows(2)
                .all(|pair| matches!(pair, [prev, next] if prev < next))
            {
                return Err(FormatError::Structural {
                    rule: "shared dictionary alphabet must be strictly ascending",
                });
            }
            shared_dictionaries.push(SharedDictionaryEntry { column_id, values });
        }
    }

    let mut packed_shared_dictionaries: Vec<PackedSharedDictionary> = Vec::new();
    if let Some(packed_bytes) = decode_section(&directory, section_area, sections::PACKED_SHARED_DICTIONARIES)? {
        let mut r = Reader::new(packed_bytes);
        let count = r.u32("packed shared dictionary count")? as usize;
        let count = bounded_count(count, 16, &r, "packed shared dictionary count exceeds input")?;
        packed_shared_dictionaries = Vec::with_capacity(count);
        for _ in 0..count {
            let column_id = r.u32("packed shared dictionary column")?;
            let value_count = r.u32("packed shared dictionary value count")?;
            let plain_len = r.u32("packed shared dictionary plaintext length")?;
            let compressed_len = r.u32("packed shared dictionary compressed length")? as usize;
            let compressed = r.take(compressed_len, "packed shared dictionary bytes")?.to_vec();
            if plain_len > PACKED_SHARED_DICTIONARY_MAX_PLAIN_BYTES || u64::from(value_count) * 2 > u64::from(plain_len)
            {
                return Err(FormatError::Structural {
                    rule: "packed shared dictionary plaintext length out of range",
                });
            }
            if shared_dictionaries.iter().any(|entry| entry.column_id == column_id)
                || packed_shared_dictionaries
                    .iter()
                    .any(|entry| entry.column_id == column_id)
            {
                return Err(FormatError::Structural {
                    rule: "a column carries more than one shared dictionary alphabet",
                });
            }
            packed_shared_dictionaries.push(PackedSharedDictionary {
                column_id,
                value_count,
                plain_len,
                compressed,
            });
        }
        if r.remaining() != 0 {
            return Err(FormatError::Structural {
                rule: "packed shared dictionary section has trailing bytes",
            });
        }
    }
    if !packed_shared_dictionaries.is_empty()
        && required_feature_flags & required_features::PACKED_SHARED_DICTIONARIES == 0
    {
        return Err(FormatError::Structural {
            rule: "packed shared dictionaries without their required feature",
        });
    }

    let mut sparse_keys = Vec::new();
    if let Some(sparse_bytes) = decode_section(&directory, section_area, sections::SPARSE_KEYS)? {
        let mut r = Reader::new(sparse_bytes);
        let count = r.u32("sparse key count")? as usize;
        let count = bounded_count(count, 6, &r, "sparse key count exceeds input")?;
        sparse_keys = Vec::with_capacity(count);
        for _ in 0..count {
            let column_id = r.u32("sparse key column")?;
            let len = r.u16("sparse key path length")? as usize;
            let path = simdutf8::basic::from_utf8(r.take(len, "sparse key path")?)
                .map_err(|_| FormatError::InvalidUtf8 {
                    what: "sparse key path",
                })?
                .to_owned();
            sparse_keys.push(ShredEntry { column_id, path });
        }
    }

    let profile = match decode_section(&directory, section_area, sections::PROFILE)? {
        None => Profile::Event,
        Some(bytes) => {
            let mut r = Reader::new(bytes);
            let profile = Profile::from_u8(r.u8("profile")?)?;
            if r.remaining() != 0 {
                return Err(FormatError::Structural {
                    rule: "profile section has trailing bytes",
                });
            }
            profile
        }
    };

    let audit = match decode_section(&directory, section_area, sections::AUDIT)? {
        None => None,
        Some(bytes) => Some(decode_audit_trail(bytes)?),
    };
    if audit.is_some() != (required_feature_flags & required_features::AUDIT_HASH_CHAIN != 0)
        || (audit.is_some() && profile != Profile::Audit)
    {
        return Err(FormatError::Structural {
            rule: "an audit section, the audit_hash_chain feature and the audit profile appear together",
        });
    }
    let mut graph = match decode_section(&directory, section_area, sections::GRAPH_EDGES)? {
        None => None,
        Some(bytes) => {
            let mut r = Reader::new(bytes);
            let subject_len = usize::from(r.u8("graph subject length")?);
            let subject = r.take(subject_len, "graph subject")?.to_vec();
            let first_sn = r.u64("graph first sequence number")?;
            let count = r.u32("graph block count")? as usize;
            let count = bounded_count(count, 20, &r, "graph block count exceeds input")?;
            let mut blocks = Vec::with_capacity(count);
            for _ in 0..count {
                let granule_id = r.u32("graph block granule")?;
                let offset = r.u64("graph block offset")?;
                let len = r.u64("graph block length")?;
                blocks.push(GraphBlockEntry {
                    granule_id,
                    len,
                    offset,
                });
            }
            if r.remaining() != 0 || subject.is_empty() {
                return Err(FormatError::Structural {
                    rule: "graph section must name a subject and end with its last block",
                });
            }
            Some(GraphDirectory {
                blocks,
                chain_cover: None,
                depth: None,
                event_ids: None,
                first_sn,
                subject,
                unresolved: Vec::new(),
            })
        }
    };
    let graph_flags = required_features::GRAPH_DENSE_IDS
        | required_features::GRAPH_CSR_FORWARD
        | required_features::GRAPH_CSR_REVERSE;
    let declared = required_feature_flags & graph_flags;
    match &mut graph {
        Some(graph) => {
            if declared != graph_flags || profile != Profile::Graph {
                return Err(FormatError::Structural {
                    rule: "graph edges ride only a graph-profile file declaring the dense id and both edge features",
                });
            }
            let end_sn = graph
                .first_sn
                .checked_add(exact_counts.row_count)
                .ok_or(FormatError::Structural {
                    rule: "graph sequence numbers overflow",
                })?;
            let unresolved = decode_section(&directory, section_area, sections::GRAPH_UNRESOLVED)?.ok_or(
                FormatError::Structural {
                    rule: "a graph edge section comes with its unresolved-parent section",
                },
            )?;
            graph.unresolved = decode_unresolved(unresolved, graph.first_sn, end_sn)?;
            let event_ids = decode_section(&directory, section_area, sections::GRAPH_EVENT_IDS)?;
            if event_ids.is_some() != (required_feature_flags & required_features::GRAPH_EVENT_ID_INDEX != 0) {
                return Err(FormatError::Structural {
                    rule: "a graph event id index and the graph_event_id_index feature appear together",
                });
            }
            graph.event_ids = event_ids
                .map(|bytes| EventIdIndex::decode(bytes, graph.first_sn, end_sn))
                .transpose()?;
            let chain_cover = decode_section(&directory, section_area, sections::GRAPH_CHAIN_COVER)?;
            if chain_cover.is_some() != (required_feature_flags & required_features::GRAPH_CHAIN_COVER != 0) {
                return Err(FormatError::Structural {
                    rule: "a graph chain cover and the graph_chain_cover feature appear together",
                });
            }
            graph.chain_cover = chain_cover.map(ChainCover::decode).transpose()?;
            if let Some(bytes) = decode_section(&directory, section_area, sections::GRAPH_DEPTH)? {
                let mut r = Reader::new(bytes);
                let depth = DepthRange {
                    min: r.u64("graph min depth")?,
                    max: r.u64("graph max depth")?,
                };
                if r.remaining() != 0 || depth.min > depth.max {
                    return Err(FormatError::Structural {
                        rule: "graph depth section holds a minimum then a maximum it does not pass",
                    });
                }
                graph.depth = Some(depth);
            }
        }
        None if declared != 0
            || required_feature_flags
                & (required_features::GRAPH_EVENT_ID_INDEX | required_features::GRAPH_CHAIN_COVER)
                != 0
            || decode_section(&directory, section_area, sections::GRAPH_UNRESOLVED)?.is_some()
            || decode_section(&directory, section_area, sections::GRAPH_EVENT_IDS)?.is_some()
            || decode_section(&directory, section_area, sections::GRAPH_CHAIN_COVER)?.is_some()
            || decode_section(&directory, section_area, sections::GRAPH_DEPTH)?.is_some() =>
        {
            return Err(FormatError::Structural {
                rule: "a file declaring graph edge features must carry the graph section",
            });
        }
        None => {}
    }

    let state_roots = match decode_section(&directory, section_area, sections::GRAPH_STATE_ROOTS)? {
        None => Vec::new(),
        Some(bytes) => {
            let mut r = Reader::new(bytes);
            let count = r.u32("state root count")? as usize;
            let count = bounded_count(count, 44, &r, "state root count exceeds input")?;
            let mut entries: Vec<StateRootEntry> = Vec::with_capacity(count);
            for _ in 0..count {
                let row_ordinal = r.u64("state root row")?;
                let address = r.take(32, "state root address")?;
                let entry_count = r.u32("state root entry count")?;
                if entries.last().is_some_and(|last| last.row_ordinal >= row_ordinal)
                    || row_ordinal >= exact_counts.row_count
                {
                    return Err(FormatError::Structural {
                        rule: "state roots name rows of the file in strictly ascending order",
                    });
                }
                entries.push(StateRootEntry {
                    root: StateRoot {
                        address: address.try_into().unwrap_or([0; 32]),
                        entry_count,
                    },
                    row_ordinal,
                });
            }
            if r.remaining() != 0 || entries.is_empty() {
                return Err(FormatError::Structural {
                    rule: "state roots section must hold at least one root and end with its last",
                });
            }
            entries
        }
    };
    if state_roots.is_empty() == (required_feature_flags & required_features::GRAPH_STATE_ROOTS != 0)
        || (!state_roots.is_empty() && (profile != Profile::Graph || graph.is_none()))
    {
        return Err(FormatError::Structural {
            rule: "state roots, the graph_state_roots feature and a graph file with edges appear together",
        });
    }

    let snapshot_keys = decode_section(&directory, section_area, sections::SNAPSHOT_KEYS)?
        .map(SnapshotKeyIndex::decode)
        .transpose()?;
    if snapshot_keys
        .as_ref()
        .is_some_and(|index| index.granule_first_keys.len() != granules.len())
    {
        return Err(FormatError::Structural {
            rule: "snapshot key index must hold one first key per granule",
        });
    }

    let entity_summary = decode_section(&directory, section_area, sections::ENTITY_SUMMARY)?
        .map(crate::indexes::entity_summary::EntitySummary::decode)
        .transpose()?;
    if entity_summary.is_some() != (optional_feature_flags & optional_features::ENTITY_SUMMARY != 0) {
        return Err(FormatError::Structural {
            rule: "entity summary section is present exactly when ENTITY_SUMMARY is declared",
        });
    }

    let time_series_rollups = decode_section(&directory, section_area, sections::TIME_SERIES_ROLLUPS)?
        .map(TimeSeriesRollups::decode)
        .transpose()?;
    if let Some(rollups) = &time_series_rollups {
        let float_column = |column_id: &u32| {
            columns
                .iter()
                .any(|column| column.column_id == *column_id && column.kind == ColumnKind::F64)
        };
        if profile != Profile::TimeSeries
            || rollups.row_count() != exact_counts.row_count
            || !rollups.column_ids.iter().all(float_column)
        {
            return Err(FormatError::Structural {
                rule: "time-series rollups cover a time-series file's rows and float columns",
            });
        }
    }

    let vector_columns = match decode_section(&directory, section_area, sections::VECTOR_COLUMNS)? {
        Some(bytes) => {
            let mut reader = Reader::new(bytes);
            let count = reader.u32("vector column count")? as usize;
            let count = bounded_count(count, 4, &reader, "vector column count exceeds section")?;
            let mut metas = Vec::with_capacity(count);
            for _ in 0..count {
                metas.push(VectorColumnMeta::decode_from(&mut reader)?);
            }
            if reader.remaining() != 0 {
                return Err(FormatError::Structural {
                    rule: "vector column section has trailing bytes",
                });
            }
            metas
        }
        None => Vec::new(),
    };
    let vector_column_count = columns
        .iter()
        .filter(|column| column.kind == ColumnKind::Vector)
        .count();
    let declared = required_feature_flags & required_features::VECTOR_COLUMNS != 0;
    if vector_column_count != vector_columns.len()
        || declared == vector_columns.is_empty()
        || !vector_columns.iter().all(|meta| {
            columns
                .iter()
                .any(|column| column.column_id == meta.column_id && column.kind == ColumnKind::Vector)
        })
    {
        return Err(FormatError::Structural {
            rule: "vector column metadata covers exactly the file's vector columns",
        });
    }

    let footer = Footer {
        audit,
        clustering,
        columns,
        dictionaries,
        embedding_row_offsets,
        entity_hash_filters,
        entity_summary,
        escape_hatches,
        exact_counts,
        external_ids,
        format_version,
        freetext,
        freetext_row_offsets,
        granules,
        graph,
        inbox_recipients,
        integrity_gaps,
        io_alignment_bytes,
        marks,
        marks_directory,
        marks_page_offsets,
        marks_pages,
        optional_feature_flags,
        page_directory,
        page_minmax,
        page_stats,
        payload_granules,
        presence,
        profile,
        reference_filters,
        required_feature_flags,
        schema_fingerprint,
        shared_dictionaries,
        packed_shared_dictionaries,
        shredded,
        snapshot_keys,
        sparse_keys,
        state_roots,
        stripe_checksums,
        stripe_proofs,
        stripe_ndv,
        stripes,
        text_token_indexes,
        text_token_offsets,
        time_series_rollups,
        vector_columns,
    };
    crate::indexes::summary::validate_granule_row_counts(&footer)?;
    Ok(footer)
}

fn encode_u64_column(out: &mut Writer, values: impl IntoIterator<Item = u64>) {
    let encoded = encode_block(&ColumnData::U64(values.into_iter().collect()), false);
    out.put_u32(encoded.pipeline.0);
    out.put_u32(encoded.bytes.len() as u32);
    out.put_slice(&encoded.bytes);
}

fn skip_u64_column(reader: &mut Reader<'_>, what: &'static str) -> Result<(), FormatError> {
    let _pipeline = reader.u32("columnar marks column pipeline id")?;
    let len = reader.u32("columnar marks column byte length")? as usize;
    reader.take(len, what)?;
    Ok(())
}

fn decode_u64_column(reader: &mut Reader<'_>, what: &'static str) -> Result<Vec<u64>, FormatError> {
    let pipeline = PipelineId(reader.u32("columnar marks column pipeline id")?);
    let len = reader.u32("columnar marks column byte length")? as usize;
    let bytes = reader.take(len, what)?;
    match decode_block(pipeline, bytes)? {
        ColumnData::U64(values) => Ok(values),
        _ => Err(FormatError::Structural {
            rule: "columnar marks field must decode to a u64 column",
        }),
    }
}

fn u32_from_u64(value: u64, rule: &'static str) -> Result<u32, FormatError> {
    u32::try_from(value).map_err(|_| FormatError::Structural { rule })
}

fn validate_page_directory_entry(entry: &PageDirectoryEntry) -> Result<(), FormatError> {
    if entry.null_count > entry.row_count {
        return Err(FormatError::Structural {
            rule: "page directory null count exceeds row count",
        });
    }
    match (entry.min_i128, entry.max_i128) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(FormatError::Structural {
                rule: "page directory integer bounds must be both present or both absent",
            });
        }
        (Some(min), Some(max)) if min > max => {
            return Err(FormatError::Structural {
                rule: "page directory integer min exceeds max",
            });
        }
        _ => {}
    }
    match (entry.min_f64, entry.max_f64) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(FormatError::Structural {
                rule: "page directory float bounds must be both present or both absent",
            });
        }
        (Some(min), Some(max))
            if !matches!(
                min.partial_cmp(&max),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ) =>
        {
            return Err(FormatError::Structural {
                rule: "page directory float min exceeds max",
            });
        }
        _ => {}
    }
    if entry.min_sequence > entry.max_sequence {
        return Err(FormatError::Structural {
            rule: "page directory min sequence exceeds max sequence",
        });
    }
    if entry.min_occurred_at_physical > entry.max_occurred_at_physical {
        return Err(FormatError::Structural {
            rule: "page directory min occurred_at exceeds max occurred_at",
        });
    }
    Ok(())
}

fn encode_marks_page(marks: &[ColumnMark], pages: &[PageDirectoryEntry]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(marks.len() as u32);
    encode_u64_column(&mut out, marks.iter().map(|mark| u64::from(mark.granule_id)));
    encode_u64_column(&mut out, marks.iter().map(|mark| mark.compressed_offset));
    encode_u64_column(&mut out, marks.iter().map(|mark| mark.compressed_size));
    encode_u64_column(&mut out, marks.iter().map(|mark| mark.uncompressed_offset));
    encode_u64_column(&mut out, marks.iter().map(|mark| mark.uncompressed_size));
    encode_u64_column(&mut out, marks.iter().map(|mark| u64::from(mark.row_count)));
    encode_u64_column(&mut out, marks.iter().map(|mark| u64::from(mark.page_count)));
    encode_u64_column(&mut out, marks.iter().map(|mark| u64::from(mark.codec_pipeline_id.0)));
    for mark in marks {
        put_opt_u64(&mut out, mark.first_value_offset);
    }
    encode_page_directory_columns(&mut out, pages);
    out.into_bytes()
}

fn encode_page_directory_columns(out: &mut Writer, pages: &[PageDirectoryEntry]) {
    out.put_u32(pages.len() as u32);
    encode_u64_column(out, pages.iter().map(|page| u64::from(page.granule_id)));
    encode_u64_column(out, pages.iter().map(|page| u64::from(page.page_index)));
    encode_u64_column(out, pages.iter().map(|page| page.compressed_offset));
    encode_u64_column(out, pages.iter().map(|page| page.compressed_len));
    encode_u64_column(out, pages.iter().map(|page| page.first_row_ordinal));
    encode_u64_column(out, pages.iter().map(|page| u64::from(page.row_count)));
    encode_u64_column(out, pages.iter().map(|page| u64::from(page.null_count)));
    encode_u64_column(out, pages.iter().map(|page| page.min_sequence));
    encode_u64_column(out, pages.iter().map(|page| page.max_sequence));
    encode_u64_column(out, pages.iter().map(|page| page.min_occurred_at_physical as u64));
    encode_u64_column(out, pages.iter().map(|page| page.max_occurred_at_physical as u64));
    for page in pages {
        put_opt_i128(out, page.min_i128);
        put_opt_i128(out, page.max_i128);
        put_opt_f64(out, page.min_f64);
        put_opt_f64(out, page.max_f64);
    }
}

pub fn encode_stripe_marks_pages(
    stripe_id: u32,
    marks: &[ColumnMark],
    page_directory: &[PageDirectoryEntry],
) -> (Vec<MarksPageEntry>, Vec<u8>) {
    let mut groups: BTreeMap<(u32, u32), Vec<ColumnMark>> = BTreeMap::new();
    for mark in marks {
        groups
            .entry((mark.projection_id, mark.column_id))
            .or_default()
            .push(*mark);
    }
    let mut page_groups: BTreeMap<(u32, u32), Vec<PageDirectoryEntry>> = BTreeMap::new();
    for page in page_directory {
        page_groups
            .entry((page.projection_id, page.column_id))
            .or_default()
            .push(*page);
    }
    let mut pages = Vec::new();
    let mut directory = Vec::with_capacity(groups.len());
    for ((projection_id, column_id), mut group_marks) in groups {
        group_marks.sort_by_key(|mark| mark.granule_id);
        let mut group_pages = page_groups.remove(&(projection_id, column_id)).unwrap_or_default();
        group_pages.sort_by_key(|page| (page.granule_id, page.page_index));
        let page_offset = pages.len() as u64;
        let page_bytes = encode_marks_page(&group_marks, &group_pages);
        directory.push(MarksPageEntry {
            column_id,
            page_len: page_bytes.len() as u64,
            page_offset,
            projection_id,
            stripe_id,
        });
        pages.extend_from_slice(&page_bytes);
    }
    (directory, pages)
}

pub(crate) fn decode_marks_page(
    column_id: u32,
    projection_id: u32,
    bytes: &[u8],
) -> Result<Vec<ColumnMark>, FormatError> {
    let mut r = Reader::new(bytes);
    let count = r.u32("columnar marks page count")? as usize;
    let count = bounded_count(count, 1, &r, "columnar marks page count exceeds input")?;

    let granule_ids = decode_u64_column(&mut r, "columnar marks granule ids")?;
    let compressed_offsets = decode_u64_column(&mut r, "columnar marks compressed offsets")?;
    let compressed_sizes = decode_u64_column(&mut r, "columnar marks compressed sizes")?;
    let uncompressed_offsets = decode_u64_column(&mut r, "columnar marks uncompressed offsets")?;
    let uncompressed_sizes = decode_u64_column(&mut r, "columnar marks uncompressed sizes")?;
    let row_counts = decode_u64_column(&mut r, "columnar marks row counts")?;
    let page_counts = decode_u64_column(&mut r, "columnar marks page counts")?;
    let codec_pipeline_ids = decode_u64_column(&mut r, "columnar marks codec pipeline ids")?;

    let lens = [
        granule_ids.len(),
        compressed_offsets.len(),
        compressed_sizes.len(),
        uncompressed_offsets.len(),
        uncompressed_sizes.len(),
        row_counts.len(),
        page_counts.len(),
        codec_pipeline_ids.len(),
    ];
    if lens.iter().any(|len| *len != count) {
        return Err(FormatError::Structural {
            rule: "columnar marks column length mismatch",
        });
    }

    let mut granule_ids = granule_ids.into_iter();
    let mut compressed_offsets = compressed_offsets.into_iter();
    let mut compressed_sizes = compressed_sizes.into_iter();
    let mut uncompressed_offsets = uncompressed_offsets.into_iter();
    let mut uncompressed_sizes = uncompressed_sizes.into_iter();
    let mut row_counts = row_counts.into_iter();
    let mut page_counts = page_counts.into_iter();
    let mut codec_pipeline_ids = codec_pipeline_ids.into_iter();

    let exhausted = || FormatError::Structural {
        rule: "columnar marks column exhausted before mark count",
    };
    let mut marks = Vec::with_capacity(count);
    for _ in 0..count {
        let first_value_offset = read_opt_u64(&mut r)?;
        marks.push(ColumnMark {
            codec_pipeline_id: PipelineId(u32_from_u64(
                codec_pipeline_ids.next().ok_or_else(exhausted)?,
                "columnar marks codec pipeline id exceeds u32",
            )?),
            column_id,
            compressed_offset: compressed_offsets.next().ok_or_else(exhausted)?,
            compressed_size: compressed_sizes.next().ok_or_else(exhausted)?,
            first_value_offset,
            granule_id: u32_from_u64(
                granule_ids.next().ok_or_else(exhausted)?,
                "columnar marks granule id exceeds u32",
            )?,
            page_count: u32_from_u64(
                page_counts.next().ok_or_else(exhausted)?,
                "columnar marks page count exceeds u32",
            )?,
            projection_id,
            row_count: u32_from_u64(
                row_counts.next().ok_or_else(exhausted)?,
                "columnar marks row count exceeds u32",
            )?,
            uncompressed_offset: uncompressed_offsets.next().ok_or_else(exhausted)?,
            uncompressed_size: uncompressed_sizes.next().ok_or_else(exhausted)?,
        });
    }
    Ok(marks)
}

pub(crate) fn decode_marks_page_directory(
    column_id: u32,
    projection_id: u32,
    bytes: &[u8],
) -> Result<Vec<PageDirectoryEntry>, FormatError> {
    let mut r = Reader::new(bytes);
    let count = r.u32("columnar marks page count")? as usize;
    let count = bounded_count(count, 1, &r, "columnar marks page count exceeds input")?;
    for what in [
        "columnar marks granule ids",
        "columnar marks compressed offsets",
        "columnar marks compressed sizes",
        "columnar marks uncompressed offsets",
        "columnar marks uncompressed sizes",
        "columnar marks row counts",
        "columnar marks page counts",
        "columnar marks codec pipeline ids",
    ] {
        skip_u64_column(&mut r, what)?;
    }
    for _ in 0..count {
        read_opt_u64(&mut r)?;
    }
    decode_page_directory_columns(&mut r, column_id, projection_id)
}

pub(crate) fn decode_marks_page_extents(bytes: &[u8]) -> Result<Vec<(u32, u64, u64)>, FormatError> {
    let mut r = Reader::new(bytes);
    let count = r.u32("columnar marks page count")? as usize;
    let count = bounded_count(count, 1, &r, "columnar marks page count exceeds input")?;

    let granule_ids = decode_u64_column(&mut r, "columnar marks granule ids")?;
    let compressed_offsets = decode_u64_column(&mut r, "columnar marks compressed offsets")?;
    let compressed_sizes = decode_u64_column(&mut r, "columnar marks compressed sizes")?;
    skip_u64_column(&mut r, "columnar marks uncompressed offsets")?;
    skip_u64_column(&mut r, "columnar marks uncompressed sizes")?;
    skip_u64_column(&mut r, "columnar marks row counts")?;
    let page_counts = decode_u64_column(&mut r, "columnar marks page counts")?;

    let lens = [
        granule_ids.len(),
        compressed_offsets.len(),
        compressed_sizes.len(),
        page_counts.len(),
    ];
    if lens.iter().any(|len| *len != count) {
        return Err(FormatError::Structural {
            rule: "columnar marks column length mismatch",
        });
    }

    let mut extents = Vec::with_capacity(count);
    for index in 0..count {
        let (Some(granule_id), Some(offset), Some(size), Some(page_count)) = (
            granule_ids.get(index),
            compressed_offsets.get(index),
            compressed_sizes.get(index),
            page_counts.get(index),
        ) else {
            return Err(FormatError::Structural {
                rule: "columnar marks column exhausted before mark count",
            });
        };
        if *page_count > 1 || *size == 0 {
            continue;
        }
        if *size > super::MAX_PAGE_BYTES {
            return Err(FormatError::Structural {
                rule: "page/chunk exceeds the maximum size",
            });
        }
        extents.push((
            u32_from_u64(*granule_id, "columnar marks granule id exceeds u32")?,
            *offset,
            *size,
        ));
    }
    Ok(extents)
}

fn decode_page_directory_columns(
    r: &mut Reader<'_>,
    column_id: u32,
    projection_id: u32,
) -> Result<Vec<PageDirectoryEntry>, FormatError> {
    let count = r.u32("columnar page directory count")? as usize;
    let count = bounded_count(count, 4, r, "columnar page directory count exceeds input")?;

    let granule_ids = decode_u64_column(r, "columnar page directory granule ids")?;
    let page_indices = decode_u64_column(r, "columnar page directory page indices")?;
    let compressed_offsets = decode_u64_column(r, "columnar page directory compressed offsets")?;
    let compressed_lens = decode_u64_column(r, "columnar page directory compressed lens")?;
    let first_row_ordinals = decode_u64_column(r, "columnar page directory first row ordinals")?;
    let row_counts = decode_u64_column(r, "columnar page directory row counts")?;
    let null_counts = decode_u64_column(r, "columnar page directory null counts")?;
    let min_sequences = decode_u64_column(r, "columnar page directory min sequences")?;
    let max_sequences = decode_u64_column(r, "columnar page directory max sequences")?;
    let min_occurred = decode_u64_column(r, "columnar page directory min occurred")?;
    let max_occurred = decode_u64_column(r, "columnar page directory max occurred")?;

    let lens = [
        granule_ids.len(),
        page_indices.len(),
        compressed_offsets.len(),
        compressed_lens.len(),
        first_row_ordinals.len(),
        row_counts.len(),
        null_counts.len(),
        min_sequences.len(),
        max_sequences.len(),
        min_occurred.len(),
        max_occurred.len(),
    ];
    if lens.iter().any(|len| *len != count) {
        return Err(FormatError::Structural {
            rule: "columnar page directory column length mismatch",
        });
    }

    let mut granule_ids = granule_ids.into_iter();
    let mut page_indices = page_indices.into_iter();
    let mut compressed_offsets = compressed_offsets.into_iter();
    let mut compressed_lens = compressed_lens.into_iter();
    let mut first_row_ordinals = first_row_ordinals.into_iter();
    let mut row_counts = row_counts.into_iter();
    let mut null_counts = null_counts.into_iter();
    let mut min_sequences = min_sequences.into_iter();
    let mut max_sequences = max_sequences.into_iter();
    let mut min_occurred = min_occurred.into_iter();
    let mut max_occurred = max_occurred.into_iter();

    let exhausted = || FormatError::Structural {
        rule: "columnar page directory column exhausted before entry count",
    };
    let mut page_directory = Vec::with_capacity(count);
    for _ in 0..count {
        let min_i128 = read_opt_i128(r)?;
        let max_i128 = read_opt_i128(r)?;
        let min_f64 = read_opt_f64(r)?;
        let max_f64 = read_opt_f64(r)?;
        let entry = PageDirectoryEntry {
            column_id,
            compressed_len: compressed_lens.next().ok_or_else(exhausted)?,
            compressed_offset: compressed_offsets.next().ok_or_else(exhausted)?,
            first_row_ordinal: first_row_ordinals.next().ok_or_else(exhausted)?,
            granule_id: u32_from_u64(
                granule_ids.next().ok_or_else(exhausted)?,
                "columnar page directory granule id exceeds u32",
            )?,
            max_f64,
            max_i128,
            max_occurred_at_physical: max_occurred.next().ok_or_else(exhausted)? as i64,
            max_sequence: max_sequences.next().ok_or_else(exhausted)?,
            min_f64,
            min_i128,
            min_occurred_at_physical: min_occurred.next().ok_or_else(exhausted)? as i64,
            min_sequence: min_sequences.next().ok_or_else(exhausted)?,
            null_count: u32_from_u64(
                null_counts.next().ok_or_else(exhausted)?,
                "columnar page directory null count exceeds u32",
            )?,
            page_index: u32_from_u64(
                page_indices.next().ok_or_else(exhausted)?,
                "columnar page directory page index exceeds u32",
            )?,
            projection_id,
            row_count: u32_from_u64(
                row_counts.next().ok_or_else(exhausted)?,
                "columnar page directory row count exceeds u32",
            )?,
        };
        validate_page_directory_entry(&entry)?;
        page_directory.push(entry);
    }
    Ok(page_directory)
}

pub fn encode_columnar_marks(
    marks: &[ColumnMark],
    page_directory: &[PageDirectoryEntry],
    granule_stripe_ids: &BTreeMap<u32, u32>,
) -> Result<Vec<u8>, FormatError> {
    let mut groups: BTreeMap<(u32, u32, u32), Vec<ColumnMark>> = BTreeMap::new();
    for mark in marks {
        let stripe_id = *granule_stripe_ids
            .get(&mark.granule_id)
            .ok_or(FormatError::Structural {
                rule: "columnar marks encode missing granule stripe mapping",
            })?;
        groups
            .entry((mark.projection_id, mark.column_id, stripe_id))
            .or_default()
            .push(*mark);
    }

    let mut page_groups: BTreeMap<(u32, u32, u32), Vec<PageDirectoryEntry>> = BTreeMap::new();
    for page in page_directory {
        let stripe_id = *granule_stripe_ids
            .get(&page.granule_id)
            .ok_or(FormatError::Structural {
                rule: "columnar marks encode missing granule stripe mapping",
            })?;
        page_groups
            .entry((page.projection_id, page.column_id, stripe_id))
            .or_default()
            .push(*page);
    }

    let mut pages = Vec::new();
    let mut directory = Vec::with_capacity(groups.len());
    for ((projection_id, column_id, stripe_id), mut group_marks) in groups {
        group_marks.sort_by_key(|mark| mark.granule_id);
        let mut group_pages = page_groups
            .remove(&(projection_id, column_id, stripe_id))
            .unwrap_or_default();
        group_pages.sort_by_key(|page| (page.granule_id, page.page_index));
        let page_offset = pages.len() as u64;
        let page_bytes = encode_marks_page(&group_marks, &group_pages);
        let page_len = page_bytes.len() as u64;
        pages.extend_from_slice(&page_bytes);
        directory.push(MarksPageEntry {
            column_id,
            page_len,
            page_offset,
            projection_id,
            stripe_id,
        });
    }

    let mut out = Writer::new();
    out.put_u32(directory.len() as u32);
    for entry in &directory {
        out.put_u32(entry.projection_id);
        out.put_u32(entry.column_id);
        out.put_u32(entry.stripe_id);
        out.put_u64(entry.page_offset);
        out.put_u64(entry.page_len);
    }
    out.put_slice(&pages);
    Ok(out.into_bytes())
}

pub fn decode_columnar_marks_directory(bytes: &[u8]) -> Result<(Vec<MarksPageEntry>, &[u8]), FormatError> {
    let mut r = Reader::new(bytes);
    let group_count = r.u32("columnar marks directory count")? as usize;
    let group_count = bounded_count(group_count, 28, &r, "columnar marks directory count exceeds input")?;
    let mut directory = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        let projection_id = r.u32("columnar marks directory projection")?;
        let column_id = r.u32("columnar marks directory column")?;
        let stripe_id = r.u32("columnar marks directory stripe")?;
        let page_offset = r.u64("columnar marks directory page offset")?;
        let page_len = r.u64("columnar marks directory page len")?;
        directory.push(MarksPageEntry {
            column_id,
            page_len,
            page_offset,
            projection_id,
            stripe_id,
        });
    }
    let pages_area = r.take(r.remaining(), "columnar marks pages")?;
    Ok((directory, pages_area))
}

pub fn decode_columnar_marks_page(
    pages_area: &[u8],
    entry: &MarksPageEntry,
) -> Result<(Vec<ColumnMark>, Vec<PageDirectoryEntry>), FormatError> {
    let page_bytes = slice(
        pages_area,
        entry.page_offset as usize,
        entry.page_len as usize,
        "columnar marks page",
    )?;
    Ok((
        decode_marks_page(entry.column_id, entry.projection_id, page_bytes)?,
        decode_marks_page_directory(entry.column_id, entry.projection_id, page_bytes)?,
    ))
}

pub fn decode_columnar_marks(bytes: &[u8]) -> Result<(Vec<ColumnMark>, Vec<PageDirectoryEntry>), FormatError> {
    let (directory, pages_area) = decode_columnar_marks_directory(bytes)?;
    let mut marks = Vec::new();
    let mut page_directory = Vec::new();
    for entry in &directory {
        let (group_marks, group_pages) = decode_columnar_marks_page(pages_area, entry)?;
        marks.extend(group_marks);
        page_directory.extend(group_pages);
    }
    Ok((marks, page_directory))
}

#[cfg(test)]
#[path = "test/footer.rs"]
mod tests;
