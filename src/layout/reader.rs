use super::constant::{FOOTER_TAIL_TRAILER_LEN, OE_MAGIC};
use super::footer::{
    ColumnMark, EmbeddingRowOffsets, Footer, FreetextRowOffsets, GranuleEntry, MarksPageEntry, PageDirectoryEntry,
    PageStats, PayloadGranule, ResidualCompression, SharedAlphabetText, StripeEntry, TextTokenOffsetsEntry,
    decode_footer, decode_marks_page, decode_marks_page_directory, decode_marks_page_extents,
};
use super::graph::GranuleEdges;
use super::remote::{FileBytes, RemoteFile};
use super::{EMPTY_VALUE_ROW_OFFSET, HEADER_BLOCK_LEN, OeHeader, decode_header, optional_features, required_features};
use crate::artifacts::batch::decode_variant_dictionary;
use crate::columns::column_ids;
use crate::compat;
use crate::encoding::decompressor::active_decompressor;
use crate::encoding::predicate::{SharedDictionaryPredicate, StringPredicate, translate_for_shared_dictionary};
use crate::encoding::{
    ColumnData, Compression, PipelineId, PresenceRank, SharedStringViewDictionary, SideStream, Transform, ValueKind,
    count_set_bits, decode_block_range_shared, decode_block_shared, decode_string_block_views_shared_prepared,
    fsst_uncompressed_len, prepare_shared_string_view_dictionary, present_position, resolve_occurred_residual,
    seekable_zstd, take_block_rows_shared,
};
use crate::error::FormatError;
use crate::events::provenance::hex_lower;
use crate::events::relationships::{RelationshipKind, RelationshipRef, TargetIdSpace};
use crate::events::variant::{KeyDictionary, PathSegment, VariantRef, VariantValue};
use crate::file::bytes::{Reader, slice};
use crate::file::constant::{TREE_TRAILER_LEN, TREE_TRAILER_MAGIC};
use crate::file::integrity::RangeFault;
use crate::indexes::probabilistic::SplitBlockBloomFilter;
use crate::indexes::rank_select::RankSelect;
use crate::indexes::text_token::TextTokenIndex;
use crate::security::AeadScheme;
use arrow_array::builder::StringViewBuilder;
use arrow_array::{Array, StringViewArray};
use dashmap::{DashMap, DashSet};
use hashbrown::{HashMap, HashSet};
use memchr::memmem;
use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock};
use zeekstd::SeekTable;

#[path = "scan.rs"]
pub mod scan;
pub use scan::*;

#[path = "query_scan.rs"]
pub mod query_scan;

fn concat_column_data(a: ColumnData, b: ColumnData) -> Result<ColumnData, FormatError> {
    match (a, b) {
        (ColumnData::U64(mut av), ColumnData::U64(bv)) => {
            av.extend(bv);
            Ok(ColumnData::U64(av))
        }
        (ColumnData::I64(mut av), ColumnData::I64(bv)) => {
            av.extend(bv);
            Ok(ColumnData::I64(av))
        }
        (ColumnData::U128(mut av), ColumnData::U128(bv)) => {
            av.extend(bv);
            Ok(ColumnData::U128(av))
        }
        (ColumnData::F64(mut av), ColumnData::F64(bv)) => {
            av.extend(bv);
            Ok(ColumnData::F64(av))
        }
        (ColumnData::Strings(mut av), ColumnData::Strings(bv)) => {
            av.append(&bv);
            Ok(ColumnData::Strings(av))
        }
        (
            ColumnData::Decimal { values: mut av, scale },
            ColumnData::Decimal {
                values: bv,
                scale: b_scale,
            },
        ) => {
            if scale != b_scale {
                return Err(FormatError::Structural {
                    rule: "multi-page mark pages must have the same decimal scale",
                });
            }
            av.extend(bv);
            Ok(ColumnData::Decimal { values: av, scale })
        }
        _ => Err(FormatError::Structural {
            rule: "multi-page mark pages must have the same column type",
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDistinctEstimate {
    pub distinct_count: u64,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadRead {
    External(String),
    None,
    Value(VariantValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadRef {
    pub row_ordinal: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PayloadBatch {
    pub payloads: Vec<PayloadRead>,
}

enum ResidualBytes<'a> {
    InArena {
        arena: Arc<Vec<u8>>,
        end: usize,
        start: usize,
    },
    InFile(&'a [u8]),
}

impl std::ops::Deref for ResidualBytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            ResidualBytes::InArena { arena, end, start } => arena.get(*start..*end).unwrap_or_default(),
            ResidualBytes::InFile(bytes) => bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnRead {
    pub data: ColumnData,
    pub presence: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RawColumnBlock<'a> {
    pub body: Cow<'a, [u8]>,
    pub pipeline: PipelineId,
    pub presence: Cow<'a, [u8]>,
    pub row_count: u32,
}

#[derive(Debug)]
struct CachedColumn {
    data: ColumnData,
    dense: bool,
    rank: RankSelect,
}

impl CachedColumn {
    fn value_at(&self, row_in_granule: u64) -> Option<VariantValue> {
        if self.dense {
            return OeFile::typed_variant_at(&self.data, usize::try_from(row_in_granule).ok()?);
        }
        let (before, present) = self.rank.rank_and_contains(row_in_granule);
        if !present {
            return None;
        }
        OeFile::typed_variant_at(&self.data, before as usize)
    }
}

#[derive(Debug, Default)]
struct PackedAlphabetSlot {
    text: OnceLock<Option<SharedAlphabetText>>,
    values: OnceLock<Option<Vec<String>>>,
}

#[derive(Debug)]
struct CachedColumnSlot {
    bytes: u64,
    column: Arc<CachedColumn>,
    last_used: u64,
}

#[derive(Debug)]
enum PathBlock<'a> {
    Located {
        rank: PresenceRank,
        raw: RawColumnBlock<'a>,
        taken: Option<TakenRows>,
    },
    Whole(Arc<CachedColumn>),
}

#[derive(Debug)]
struct TakenRows {
    rows: Vec<u64>,
    values: Vec<Option<VariantValue>>,
}

#[derive(Debug)]
struct PathGroup<'a> {
    block: Option<PathBlock<'a>>,
    freetext_column: Option<u32>,
    granule: &'a GranuleEntry,
    rows: Vec<u64>,
    shredded_column: Option<u32>,
}

fn batch_rows_in_granule(refs: &[PayloadRef], order: &[usize], from: usize, granule: &GranuleEntry) -> Vec<u64> {
    let end = granule.first_row_ordinal.saturating_add(u64::from(granule.row_count));
    let mut rows: Vec<u64> = order
        .get(from..)
        .unwrap_or_default()
        .iter()
        .filter_map(|&index| refs.get(index).map(|reference| reference.row_ordinal))
        .take_while(|&row| row >= granule.first_row_ordinal && row < end)
        .map(|row| row - granule.first_row_ordinal)
        .collect();
    rows.dedup();
    rows
}

#[derive(Debug)]
struct PayloadGroup<'a> {
    dictionary: Option<Arc<KeyDictionary>>,
    field_blocks: HashMap<u32, PathBlock<'a>>,
    flags: Arc<CachedColumn>,
    granule: &'a GranuleEntry,
    payload: &'a PayloadGranule,
    rows: Vec<u64>,
}

#[derive(Debug)]
struct InflatedResidualSlot {
    arena: Arc<Vec<u8>>,
    last_used: u64,
}

#[derive(Debug)]
struct DictionarySlot {
    bytes: u64,
    dictionary: Arc<KeyDictionary>,
    last_used: u64,
}

#[derive(Debug)]
struct GraphEdgesSlot {
    bytes: u64,
    edges: Arc<GranuleEdges>,
    last_used: u64,
}

#[derive(Debug)]
struct AliasedExtentSlot {
    bytes: u64,
    last_used: u64,
    read: Arc<ColumnRead>,
}

fn column_data_bytes(data: &ColumnData) -> u64 {
    match data {
        ColumnData::U64(values) => (values.len() * 8) as u64,
        ColumnData::I64(values) => (values.len() * 8) as u64,
        ColumnData::F64(values) => (values.len() * 8) as u64,
        ColumnData::U128(values) => (values.len() * 16) as u64,
        ColumnData::Decimal { values, .. } => (values.len() * 16) as u64,
        ColumnData::Strings(values) => (values.len() * 9 + values.text_len()) as u64,
    }
}

fn dictionary_bytes(dictionary: &KeyDictionary) -> u64 {
    dictionary.keys().map(|key| key.len() as u64 + 24).sum()
}

#[cfg(test)]
fn presence_set_rows(presence: &[u8]) -> impl Iterator<Item = u64> + '_ {
    presence.iter().enumerate().flat_map(|(byte_index, &byte)| {
        (0..8u32).filter_map(move |bit| (byte & (1 << bit) != 0).then_some((byte_index as u64) * 8 + u64::from(bit)))
    })
}

fn validate_block_counts(presence: &[u8], decoded_rows: usize, declared_rows: usize) -> Result<(), FormatError> {
    if presence.is_empty() {
        if decoded_rows != declared_rows {
            return Err(FormatError::Structural {
                rule: "dense column block value count must equal its declared row count",
            });
        }
        return Ok(());
    }
    let present = count_set_bits(presence, presence.len() * 8);
    if present != decoded_rows {
        return Err(FormatError::Structural {
            rule: "sparse column block value count must equal its presence set-bit count",
        });
    }
    if presence.len() * 8 < declared_rows {
        return Err(FormatError::Structural {
            rule: "presence bitmap too short to cover its declared row count",
        });
    }
    let boundary_byte = declared_rows / 8;
    let boundary_bit = declared_rows % 8;
    if let Some(&byte) = presence.get(boundary_byte) {
        let high_bits = 0xFFu8 << boundary_bit;
        if byte & high_bits != 0 {
            return Err(FormatError::Structural {
                rule: "presence bit set at or beyond the declared row count",
            });
        }
    }
    if presence
        .get(boundary_byte.saturating_add(1)..)
        .is_some_and(|tail| tail.iter().any(|&byte| byte != 0))
    {
        return Err(FormatError::Structural {
            rule: "presence bit set at or beyond the declared row count",
        });
    }
    Ok(())
}

fn append_presence_bits(dst: &mut Vec<u8>, dst_start_bit: usize, src: &[u8], len: usize) {
    if len == 0 {
        return;
    }
    let needed = (dst_start_bit + len).div_ceil(8);
    if dst.len() < needed {
        dst.resize(needed, 0);
    }
    if !dst_start_bit.is_multiple_of(8) {
        for bit in 0..len {
            if src.get(bit / 8).is_some_and(|byte| byte & (1 << (bit % 8)) != 0) {
                let dst_bit = dst_start_bit + bit;
                if let Some(byte) = dst.get_mut(dst_bit / 8) {
                    *byte |= 1 << (dst_bit % 8);
                }
            }
        }
        return;
    }
    let dst_byte_start = dst_start_bit / 8;
    let full_bytes = len / 8;
    for i in 0..full_bytes {
        if let Some(dst_byte) = dst.get_mut(dst_byte_start + i) {
            *dst_byte |= src.get(i).copied().unwrap_or(0);
        }
    }
    let rem = len % 8;
    if rem > 0
        && let Some(dst_byte) = dst.get_mut(dst_byte_start + full_bytes)
    {
        let mask = (1u8 << rem) - 1;
        *dst_byte |= src.get(full_bytes).copied().unwrap_or(0) & mask;
    }
}

fn append_all_present_bits(dst: &mut Vec<u8>, dst_start_bit: usize, len: usize) {
    if len == 0 {
        return;
    }
    let needed = (dst_start_bit + len).div_ceil(8);
    if dst.len() < needed {
        dst.resize(needed, 0);
    }
    for bit in dst_start_bit..dst_start_bit + len {
        if let Some(byte) = dst.get_mut(bit / 8) {
            *byte |= 1 << (bit % 8);
        }
    }
}

fn owned_utf8(bytes: &[u8], what: &'static str) -> Result<String, FormatError> {
    simdutf8::basic::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| FormatError::InvalidUtf8 { what })
}

pub(super) fn granules_overlapping_rows<'a>(granules: &'a [GranuleEntry], rows: &Range<u64>) -> &'a [GranuleEntry] {
    let first = granules.partition_point(|granule| {
        granule.first_row_ordinal.saturating_add(u64::from(granule.row_count)) <= rows.start
    });
    let last = granules.partition_point(|granule| granule.first_row_ordinal < rows.end);
    granules.get(first..last).unwrap_or_default()
}

pub const SPECULATIVE_TAIL_BYTES: u64 = 256 * 1024;

const POINT_READS_BEFORE_WHOLE_GRANULE_DECODE: u32 = 32;

pub const DECODED_CACHE_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

const POINT_PROBE_ENTRIES_MAX: usize = 1 << 20;

const FOOTER_DEK_LEN: usize = 32;

const SEAL_LEN: usize = 32;

const ROW_OFFSET_ENTRY_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailRange {
    pub exact: bool,
    pub len: u64,
    pub start: u64,
}

pub fn tail_range(file_size: u64, footer_len: Option<u64>, tree_len: Option<u64>) -> TailRange {
    match footer_len {
        Some(footer_len) => {
            let len = footer_len.saturating_add(tree_len.unwrap_or(0)).min(file_size);
            TailRange {
                exact: true,
                len,
                start: file_size - len,
            }
        }
        None => {
            let len = SPECULATIVE_TAIL_BYTES.min(file_size);
            TailRange {
                exact: false,
                len,
                start: file_size - len,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OeFooter {
    authenticated: bool,
    footer: Footer,
    proof_trees: Vec<u8>,
    usable_optional_features: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedStripeRead {
    pub file_offset: u64,
    pub length: u64,
    proof_length: u64,
    proof_offset: u64,
    requested_length: u64,
    requested_offset: u64,
    pub stripe_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderCommitment {
    pub blake3: [u8; 32],
    pub feature_flags: u64,
    pub file_id: u128,
}

impl HeaderCommitment {
    pub fn from_header_block(header_block: &[u8]) -> Result<Self, FormatError> {
        let header = decode_header(header_block)?;
        Ok(Self::of(header_block, &header))
    }

    fn of(header_block: &[u8], header: &OeHeader) -> Self {
        Self {
            blake3: *crate::file::integrity::hash_tree(header_block).as_bytes(),
            feature_flags: header.feature_flags,
            file_id: header.file_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripeRange {
    pub length: u64,
    pub stripe_id: u32,
    pub stripe_offset: u64,
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the footer variant is the common outcome and is moved out at once; boxing it would cost an allocation per open"
)]
pub enum SpeculativeTail {
    NeedsExactTail { tail_len: u64 },
    Opened(OeFooter),
}

fn outboard_tree_len(tail: &[u8]) -> Result<Option<u64>, FormatError> {
    if tail.len() < 4 || slice(tail, tail.len() - 4, 4, "object trailing magic")? != TREE_TRAILER_MAGIC {
        return Ok(None);
    }
    if tail.len() < TREE_TRAILER_LEN {
        return Err(FormatError::Truncated {
            what: "outboard tree trailer",
        });
    }
    let len_bytes = slice(tail, tail.len() - TREE_TRAILER_LEN, 8, "outboard tree length")?;
    Ok(Some(u64::from_le_bytes(len_bytes.try_into().unwrap_or([0; 8]))))
}

fn strip_outboard_tree(tail: &[u8]) -> Result<&[u8], FormatError> {
    let Some(tree_len) = outboard_tree_len(tail)? else {
        return Ok(tail);
    };
    let content_end = tail
        .len()
        .checked_sub(TREE_TRAILER_LEN)
        .and_then(|n| n.checked_sub(tree_len as usize))
        .ok_or(FormatError::Truncated { what: "outboard tree" })?;
    Ok(slice(tail, 0, content_end, "outboard content tail")?)
}

fn split_outboard_tree(tail: &[u8]) -> Result<(&[u8], &[u8]), FormatError> {
    let Some(tree_len) = outboard_tree_len(tail)? else {
        return Ok((tail, &[]));
    };
    let content_end = tail
        .len()
        .checked_sub(TREE_TRAILER_LEN)
        .and_then(|n| n.checked_sub(tree_len as usize))
        .ok_or(FormatError::Truncated { what: "outboard tree" })?;
    Ok((
        slice(tail, 0, content_end, "outboard content tail")?,
        slice(tail, content_end, tree_len as usize, "stripe proof trees")?,
    ))
}

fn validate_stripe_proofs(footer: &Footer, proof_trees: &[u8]) -> Result<(), FormatError> {
    if footer.stripe_proofs.len() != footer.stripes.len() {
        return Err(FormatError::Structural {
            rule: "stripe proof count must match the stripe directory",
        });
    }
    let mut cursor = 0u64;
    let mut seen = HashSet::with_capacity(footer.stripe_proofs.len());
    for stripe in &footer.stripes {
        let proof = footer
            .stripe_proofs
            .iter()
            .find(|proof| proof.stripe_id == stripe.stripe_id)
            .ok_or(FormatError::Structural {
                rule: "every stripe must have proof geometry",
            })?;
        if !seen.insert(proof.stripe_id) {
            return Err(FormatError::Structural {
                rule: "stripe proof ids must be unique",
            });
        }
        let group = proof.chunk_group_bytes as usize;
        if group < blake3::CHUNK_LEN || !group.is_power_of_two() || !group.is_multiple_of(blake3::CHUNK_LEN) {
            return Err(FormatError::Structural {
                rule: "stripe proof chunk group must be a power-of-two BLAKE3 chunk multiple",
            });
        }
        let stripe_len = usize::try_from(stripe.byte_len).map_err(|_| FormatError::Structural {
            rule: "stripe proof input length does not fit in memory",
        })?;
        let expected_len = crate::file::integrity::outboard_tree_bytes(stripe_len, group) as u64;
        if proof.tree_offset != cursor || proof.tree_len != expected_len {
            return Err(FormatError::Structural {
                rule: "stripe proof entries must exactly partition the expected outboard trees",
            });
        }
        cursor = cursor.checked_add(proof.tree_len).ok_or(FormatError::Structural {
            rule: "stripe proof range overflows",
        })?;
    }
    if cursor != proof_trees.len() as u64 && !proof_trees.is_empty() {
        return Err(FormatError::Structural {
            rule: "stripe proof directory must cover the complete proof appendix",
        });
    }
    if seen.len() != footer.stripe_proofs.len() {
        return Err(FormatError::Structural {
            rule: "stripe proof directory must not contain unknown stripe ids",
        });
    }
    Ok(())
}

fn footer_blob_len(content: &[u8]) -> Result<usize, FormatError> {
    if content.len() < FOOTER_TAIL_TRAILER_LEN {
        return Err(FormatError::Truncated { what: "footer tail" });
    }
    if slice(content, content.len() - OE_MAGIC.len(), OE_MAGIC.len(), "oe tail magic")? != OE_MAGIC {
        return Err(FormatError::BadMagic { expected: "OEF1" });
    }
    let len_bytes = slice(content, content.len() - FOOTER_TAIL_TRAILER_LEN, 8, "footer length")?;
    Ok(u64::from_le_bytes(len_bytes.try_into().unwrap_or([0; 8])) as usize)
}

fn decrypt_footer_tail(body_tail: &[u8], dek: &[u8; FOOTER_DEK_LEN], file_id: u128) -> Result<Vec<u8>, FormatError> {
    let scheme = AeadScheme::AesGcm256;
    let enc_len = footer_blob_len(body_tail)?;
    let nonce_len = scheme.nonce_len();
    if enc_len < nonce_len {
        return Err(FormatError::Truncated {
            what: "encrypted footer nonce",
        });
    }
    let enc_start = body_tail
        .len()
        .checked_sub(FOOTER_TAIL_TRAILER_LEN)
        .and_then(|n| n.checked_sub(enc_len))
        .ok_or(FormatError::Truncated {
            what: "encrypted footer",
        })?;
    let nonce = slice(body_tail, enc_start, nonce_len, "encrypted footer nonce")?;
    let ciphertext = slice(
        body_tail,
        enc_start + nonce_len,
        enc_len - nonce_len,
        "encrypted footer ciphertext",
    )?;
    let plaintext = scheme
        .open(dek, nonce, &file_id.to_le_bytes(), ciphertext)
        .ok_or(FormatError::Blake3Mismatch {
            scope: "encrypted footer",
        })?;
    let blob_len = plaintext.len() as u64;
    let mut content = plaintext;
    content.extend_from_slice(&blob_len.to_le_bytes());
    content.extend_from_slice(&OE_MAGIC);
    Ok(content)
}

impl OeFooter {
    pub fn open(tail_bytes: &[u8]) -> Result<Self, FormatError> {
        let (content, proof_trees) = split_outboard_tree(tail_bytes)?;
        let blob_len = footer_blob_len(content)?;
        Self::from_content_tail(content, blob_len, proof_trees, false)
    }

    pub fn open_speculative(tail_bytes: &[u8]) -> Result<SpeculativeTail, FormatError> {
        match Self::tail_shortfall(tail_bytes)? {
            Some(tail_len) => Ok(SpeculativeTail::NeedsExactTail { tail_len }),
            None => Ok(SpeculativeTail::Opened(Self::open(tail_bytes)?)),
        }
    }

    pub fn tail_shortfall(tail_bytes: &[u8]) -> Result<Option<u64>, FormatError> {
        if let Some(tree_len) = outboard_tree_len(tail_bytes)?
            && (tail_bytes.len() as u64) < (TREE_TRAILER_LEN as u64).saturating_add(tree_len)
        {
            return Ok(Some(
                tree_len
                    .saturating_add(TREE_TRAILER_LEN as u64)
                    .saturating_add(SPECULATIVE_TAIL_BYTES),
            ));
        }
        let (content, _) = split_outboard_tree(tail_bytes)?;
        let blob_len = footer_blob_len(content)?;
        if content.len() - FOOTER_TAIL_TRAILER_LEN >= blob_len {
            return Ok(None);
        }
        let tree_trailer = (tail_bytes.len() - content.len()) as u64;
        Ok(Some(
            (blob_len as u64)
                .saturating_add(FOOTER_TAIL_TRAILER_LEN as u64)
                .saturating_add(tree_trailer),
        ))
    }

    pub fn open_authenticated(
        header_block: &[u8],
        tail_bytes: &[u8],
        file_size: u64,
        expected_seal: &[u8; SEAL_LEN],
        footer_dek: Option<&[u8; FOOTER_DEK_LEN]>,
    ) -> Result<Self, FormatError> {
        let header = decode_header(header_block)?;
        let opened = Self::open_authenticated_tail(
            tail_bytes,
            file_size,
            expected_seal,
            &HeaderCommitment::of(header_block, &header),
            footer_dek,
        )?;
        if header.row_count != opened.footer.exact_counts.row_count {
            return Err(FormatError::Structural {
                rule: "header row_count must match exact counts",
            });
        }
        if header.profile != opened.footer.profile {
            return Err(FormatError::Structural {
                rule: "header profile must match footer profile",
            });
        }
        Ok(opened)
    }

    pub fn open_authenticated_tail(
        tail_bytes: &[u8],
        file_size: u64,
        expected_seal: &[u8; SEAL_LEN],
        header: &HeaderCommitment,
        footer_dek: Option<&[u8; FOOTER_DEK_LEN]>,
    ) -> Result<Self, FormatError> {
        compat::check_features(header.feature_flags, 0)?;
        let (raw_content_tail, proof_trees) = split_outboard_tree(tail_bytes)?;
        let raw_blob_len = footer_blob_len(raw_content_tail)?;
        let raw_blob_start = raw_content_tail
            .len()
            .checked_sub(FOOTER_TAIL_TRAILER_LEN)
            .and_then(|n| n.checked_sub(raw_blob_len))
            .ok_or(FormatError::Truncated { what: "footer" })?;
        let raw_footer = slice(raw_content_tail, raw_blob_start, raw_blob_len, "footer region")?;
        let raw_trailer = slice(
            raw_content_tail,
            raw_content_tail.len() - FOOTER_TAIL_TRAILER_LEN,
            FOOTER_TAIL_TRAILER_LEN,
            "footer trailer",
        )?;

        let mut opened = if header.feature_flags & required_features::FOOTER_ENCRYPTED != 0 {
            let dek = footer_dek.ok_or(FormatError::Structural {
                rule: "encrypted footer requires a file DEK to open",
            })?;
            let plaintext_tail = decrypt_footer_tail(raw_content_tail, dek, header.file_id)?;
            let plaintext_len = footer_blob_len(&plaintext_tail)?;
            Self::from_content_tail(&plaintext_tail, plaintext_len, proof_trees, true)?
        } else {
            Self::from_content_tail(raw_content_tail, raw_blob_len, proof_trees, true)?
        };

        let appendix_len = (tail_bytes.len() - raw_content_tail.len()) as u64;
        let content_len = file_size.checked_sub(appendix_len).ok_or(FormatError::Truncated {
            what: "OE content length",
        })?;
        let footer_len = (raw_blob_len as u64).saturating_add(FOOTER_TAIL_TRAILER_LEN as u64);
        let data_end = content_len
            .checked_sub(footer_len)
            .ok_or(FormatError::Truncated { what: "OE data area" })?;
        let data_end_usize = usize::try_from(data_end).map_err(|_| FormatError::Structural {
            rule: "OE data length does not fit in memory",
        })?;
        let commitments = crate::integrity::declared_data_commitments(
            data_end_usize,
            &opened.footer.stripes,
            &opened.footer.stripe_checksums,
            &opened.footer.integrity_gaps,
        )?;
        let actual = crate::integrity::derive_file_seal_from_header_hash(
            HEADER_BLOCK_LEN as u64,
            &header.blake3,
            raw_footer,
            raw_trailer,
            content_len,
            &commitments,
        );
        if &actual != expected_seal {
            return Err(FormatError::Blake3Mismatch { scope: "oe file seal" });
        }
        opened.authenticated = true;
        Ok(opened)
    }

    fn from_content_tail(
        content: &[u8],
        blob_len: usize,
        proof_trees: &[u8],
        authenticated: bool,
    ) -> Result<Self, FormatError> {
        let (footer, usable_optional_features) = Self::decode_content_tail(content, blob_len, proof_trees)?;
        Ok(Self {
            authenticated,
            footer,
            proof_trees: proof_trees.to_vec(),
            usable_optional_features,
        })
    }

    fn decode_content_tail(content: &[u8], blob_len: usize, proof_trees: &[u8]) -> Result<(Footer, u64), FormatError> {
        let blob_start = content
            .len()
            .checked_sub(FOOTER_TAIL_TRAILER_LEN)
            .and_then(|n| n.checked_sub(blob_len))
            .ok_or(FormatError::Truncated { what: "footer" })?;
        let footer_blob = slice(content, blob_start, blob_len, "footer blob")?;
        let footer = decode_footer(footer_blob)?;
        compat::check_format_version(footer.format_version.0, footer.format_version.1)?;
        let usable_optional_features =
            compat::check_features(footer.required_feature_flags, footer.optional_feature_flags)?;
        validate_stripe_proofs(&footer, proof_trees)?;
        Ok((footer, usable_optional_features))
    }

    pub fn plan_verified_stripe_read(
        &self,
        stripe_id: u32,
        stripe_offset: u64,
        length: u64,
    ) -> Result<VerifiedStripeRead, FormatError> {
        self.plan_stripe_read(stripe_id, stripe_offset, length, false)
    }

    pub fn plan_whole_stripe_read(
        &self,
        stripe_id: u32,
        stripe_offset: u64,
        length: u64,
    ) -> Result<VerifiedStripeRead, FormatError> {
        self.plan_stripe_read(stripe_id, stripe_offset, length, true)
    }

    fn plan_stripe_read(
        &self,
        stripe_id: u32,
        stripe_offset: u64,
        length: u64,
        whole: bool,
    ) -> Result<VerifiedStripeRead, FormatError> {
        if !self.authenticated {
            return Err(FormatError::Structural {
                rule: "verified stripe reads require a manifest-authenticated footer",
            });
        }
        let stripe = self
            .footer
            .stripes
            .iter()
            .find(|stripe| stripe.stripe_id == stripe_id)
            .ok_or(FormatError::RefOutOfRange { what: "stripe id" })?;
        let end = stripe_offset
            .checked_add(length)
            .ok_or(FormatError::RefOutOfRange { what: "stripe range" })?;
        if end > stripe.byte_len {
            return Err(FormatError::RefOutOfRange { what: "stripe range" });
        }
        if stripe.byte_len == 0 {
            return Err(FormatError::Structural {
                rule: "verified streaming does not admit empty stripes",
            });
        }
        let proof = self
            .footer
            .stripe_proofs
            .iter()
            .find(|proof| proof.stripe_id == stripe_id)
            .ok_or(FormatError::Structural {
                rule: "stripe proof geometry missing",
            })?;
        let effective_offset = if length == 0 {
            stripe_offset.min(stripe.byte_len - 1)
        } else {
            stripe_offset
        };
        let effective_end = if length == 0 { effective_offset + 1 } else { end };
        let (proof_offset, proof_end) = if whole || proof.tree_len == 0 || self.proof_trees.is_empty() {
            (0, stripe.byte_len)
        } else {
            let group = u64::from(proof.chunk_group_bytes);
            (
                (effective_offset / group) * group,
                effective_end.div_ceil(group).saturating_mul(group).min(stripe.byte_len),
            )
        };
        let proof_length = proof_end.checked_sub(proof_offset).ok_or(FormatError::Structural {
            rule: "stripe proof fetch range is inverted",
        })?;
        Ok(VerifiedStripeRead {
            file_offset: stripe
                .file_offset
                .checked_add(proof_offset)
                .ok_or(FormatError::Structural {
                    rule: "verified stripe file offset overflows",
                })?,
            length: proof_length,
            proof_length: effective_end - effective_offset,
            proof_offset: effective_offset,
            requested_length: length,
            requested_offset: stripe_offset,
            stripe_id,
        })
    }

    pub fn verify_stripe_range<'a>(
        &self,
        read: &VerifiedStripeRead,
        fetched: &'a [u8],
    ) -> Result<&'a [u8], FormatError> {
        if !self.authenticated || fetched.len() as u64 != read.length {
            return Err(FormatError::Structural {
                rule: "verified stripe response does not match its authenticated read plan",
            });
        }
        self.verify_stripe_range_in(read, fetched, read.file_offset)
            .map_err(|_| FormatError::Blake3Mismatch { scope: "stripe proof" })
    }

    pub fn verify_stripe_range_in<'a>(
        &self,
        read: &VerifiedStripeRead,
        fetched: &'a [u8],
        fetched_file_offset: u64,
    ) -> Result<&'a [u8], RangeFault> {
        if !self.authenticated {
            return Err(RangeFault::Corrupt);
        }
        let position = self
            .footer
            .stripes
            .iter()
            .position(|stripe| stripe.stripe_id == read.stripe_id)
            .ok_or(RangeFault::Corrupt)?;
        let stripe = self.footer.stripes.get(position).ok_or(RangeFault::Corrupt)?;
        let checksum = self.footer.stripe_checksums.get(position).ok_or(RangeFault::Corrupt)?;
        let fetched_start = fetched_file_offset
            .checked_sub(stripe.file_offset)
            .ok_or(RangeFault::Corrupt)?;
        let fetched_end = fetched_start
            .checked_add(fetched.len() as u64)
            .ok_or(RangeFault::Corrupt)?;
        let planned_end = read
            .proof_offset
            .checked_add(read.proof_length)
            .ok_or(RangeFault::Corrupt)?;
        if fetched_end > stripe.byte_len || read.proof_offset < fetched_start || planned_end > fetched_end {
            return Err(RangeFault::Corrupt);
        }
        if fetched_start == 0 && fetched_end == stripe.byte_len {
            let matches = crate::file::integrity::hash_tree(fetched).as_bytes() == checksum;
            if !matches {
                return Err(RangeFault::Corrupt);
            }
        } else {
            let proof = self
                .footer
                .stripe_proofs
                .iter()
                .find(|proof| proof.stripe_id == read.stripe_id)
                .ok_or(RangeFault::TreeUnusable)?;
            let tree = slice(
                &self.proof_trees,
                proof.tree_offset as usize,
                proof.tree_len as usize,
                "stripe proof tree",
            )
            .map_err(|_| RangeFault::TreeUnusable)?;
            crate::file::integrity::verify_range_from_slice(
                stripe.byte_len,
                fetched,
                fetched_start,
                tree,
                checksum,
                read.proof_offset,
                read.proof_length,
                proof.chunk_group_bytes as usize,
            )?;
        }
        let requested_start = read
            .requested_offset
            .checked_sub(fetched_start)
            .ok_or(RangeFault::Corrupt)? as usize;
        slice(
            fetched,
            requested_start,
            read.requested_length as usize,
            "verified stripe range",
        )
        .map_err(|_| RangeFault::Corrupt)
    }

    fn stripe_of_granule(&self, granule_id: u32) -> Result<&StripeEntry, FormatError> {
        let granule = self
            .footer
            .granules
            .iter()
            .find(|granule| granule.granule_id == granule_id)
            .ok_or(FormatError::RefOutOfRange { what: "granule id" })?;
        self.footer
            .stripes
            .iter()
            .find(|stripe| stripe.stripe_id == granule.stripe_id)
            .ok_or(FormatError::RefOutOfRange {
                what: "granule references a stripe absent from the stripe directory",
            })
    }

    pub fn marks_page_range(&self, column_id: u32, projection_id: u32, stripe_id: u32) -> Option<StripeRange> {
        if self.footer.required_feature_flags & required_features::STRIPE_MARKS_PAGES == 0 {
            return None;
        }
        self.footer
            .marks_page_offsets
            .iter()
            .find(|entry| {
                entry.stripe_id == stripe_id && entry.column_id == column_id && entry.projection_id == projection_id
            })
            .map(|entry| StripeRange {
                length: entry.page_len,
                stripe_id,
                stripe_offset: entry.page_offset,
            })
    }

    pub fn stripe_marks(
        &self,
        column_id: u32,
        projection_id: u32,
        stripe_id: u32,
        page: Option<&[u8]>,
    ) -> Result<Vec<ColumnMark>, FormatError> {
        let flags = self.footer.required_feature_flags;
        if flags & required_features::STRIPE_MARKS_PAGES != 0 {
            return match page {
                Some(page) => decode_marks_page(column_id, projection_id, page),
                None if self.marks_page_range(column_id, projection_id, stripe_id).is_none() => Ok(Vec::new()),
                None => Err(FormatError::Structural {
                    rule: "stripe marks live beside the stripe: fetch its marks page first",
                }),
            };
        }
        if flags & required_features::COLUMNAR_MARKS != 0 {
            let mut marks = Vec::new();
            for entry in self.footer.marks_directory.iter().filter(|entry| {
                entry.stripe_id == stripe_id && entry.column_id == column_id && entry.projection_id == projection_id
            }) {
                let page = slice(
                    &self.footer.marks_pages,
                    entry.page_offset as usize,
                    entry.page_len as usize,
                    "columnar marks page",
                )?;
                marks.extend(decode_marks_page(column_id, projection_id, page)?);
            }
            return Ok(marks);
        }
        let granules: HashSet<u32> = self
            .footer
            .granules
            .iter()
            .filter(|granule| granule.stripe_id == stripe_id)
            .map(|granule| granule.granule_id)
            .collect();
        Ok(self
            .footer
            .marks
            .iter()
            .filter(|mark| {
                mark.column_id == column_id
                    && mark.projection_id == projection_id
                    && granules.contains(&mark.granule_id)
            })
            .copied()
            .collect())
    }

    pub fn block_range(&self, mark: &ColumnMark) -> Result<StripeRange, FormatError> {
        let stripe = self.stripe_of_granule(mark.granule_id)?;
        let stripe_offset = if self.footer.required_feature_flags & required_features::STRIPE_RELATIVE_MARKS != 0 {
            mark.compressed_offset
        } else {
            mark.compressed_offset
                .checked_sub(stripe.file_offset)
                .ok_or(FormatError::RefOutOfRange {
                    what: "mark lies before its stripe",
                })?
        };
        if stripe_offset
            .checked_add(mark.compressed_size)
            .is_none_or(|end| end > stripe.byte_len)
        {
            return Err(FormatError::RefOutOfRange {
                what: "mark lies outside its stripe",
            });
        }
        Ok(StripeRange {
            length: mark.compressed_size,
            stripe_id: stripe.stripe_id,
            stripe_offset,
        })
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn proof_trees_present(&self) -> bool {
        !self.proof_trees.is_empty() || self.footer.stripe_proofs.iter().all(|proof| proof.tree_len == 0)
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    pub fn usable_optional_features(&self) -> u64 {
        self.usable_optional_features
    }
}

const BLOCK_ROWS_BEYOND_BOUND: &str = "column block row count beyond the block bound";

fn block_rows_beyond_bound(row_count: u32) -> bool {
    row_count as usize > super::MAX_ELIDED_BLOCK_ROWS
}

#[derive(Debug)]
struct ColumnarMarksLayout {
    granule_stripe_id: HashMap<u32, u32>,
    pages_in_data_area: bool,
    stripe_bases: HashMap<u32, u64>,
    stripe_pages: HashMap<u32, Vec<MarksPageEntry>>,
}

#[derive(Debug)]
struct ColumnarMarks {
    decoded: HashMap<(u32, u32, u32), ColumnMark>,
    decoded_page_directories: HashSet<(u32, u32, u32)>,
    decoded_pages: HashSet<(u32, u32, u32)>,
    footer: Arc<Footer>,
    layout: Arc<ColumnarMarksLayout>,
    page_marks: BTreeMap<(u32, u32, u32, u32), PageDirectoryEntry>,
    shared_extents_found: HashSet<u32>,
    pending_aliased: Vec<(u32, u64, u64)>,
}

impl ColumnarMarks {
    fn new(footer: &Arc<Footer>, pages_in_data_area: bool) -> Self {
        let layout = ColumnarMarksLayout {
            granule_stripe_id: footer
                .granules
                .iter()
                .map(|granule| (granule.granule_id, granule.stripe_id))
                .collect(),
            pages_in_data_area,
            stripe_bases: footer
                .stripes
                .iter()
                .map(|stripe| (stripe.stripe_id, stripe.file_offset))
                .collect(),
            stripe_pages: {
                let directory = if pages_in_data_area {
                    &footer.marks_page_offsets
                } else {
                    &footer.marks_directory
                };
                let mut by_stripe: HashMap<u32, Vec<MarksPageEntry>> = HashMap::new();
                for entry in directory {
                    by_stripe.entry(entry.stripe_id).or_default().push(*entry);
                }
                by_stripe
            },
        };
        Self::over(Arc::clone(footer), Arc::new(layout))
    }

    fn over(footer: Arc<Footer>, layout: Arc<ColumnarMarksLayout>) -> Self {
        Self {
            decoded: HashMap::new(),
            decoded_page_directories: HashSet::new(),
            decoded_pages: HashSet::new(),
            footer,
            layout,
            page_marks: BTreeMap::new(),
            pending_aliased: Vec::new(),
            shared_extents_found: HashSet::new(),
        }
    }

    fn fresh(&self) -> Self {
        Self::over(Arc::clone(&self.footer), Arc::clone(&self.layout))
    }

    fn page_bytes<'a>(&'a self, file: &'a OeFile, entry: &MarksPageEntry) -> Result<&'a [u8], FormatError> {
        if !self.layout.pages_in_data_area {
            return Ok(slice(
                &self.footer.marks_pages,
                entry.page_offset as usize,
                entry.page_len as usize,
                "columnar marks page",
            )?);
        }
        let position = self
            .layout
            .stripe_bases
            .get(&entry.stripe_id)
            .copied()
            .and_then(|stripe_base| stripe_base.checked_add(entry.page_offset))
            .and_then(|position| usize::try_from(position).ok())
            .ok_or(FormatError::RefOutOfRange {
                what: "marks page references a stripe absent from the stripe directory",
            })?;
        file.data(position, entry.page_len as usize, "stripe marks page")
    }

    fn find_shared_extents(&mut self, file: &OeFile, stripe_id: u32) -> Result<(), FormatError> {
        if self.shared_extents_found.contains(&stripe_id) {
            return Ok(());
        }
        let entries = self.layout.stripe_pages.get(&stripe_id).cloned().unwrap_or_default();
        let mut extent_counts: HashMap<(u64, u64), (u32, u32)> = HashMap::new();
        for entry in &entries {
            let extents = {
                let page_bytes = self.page_bytes(file, entry)?;
                decode_marks_page_extents(page_bytes)?
            };
            for (granule_id, offset, size) in extents {
                let counted = extent_counts.entry((offset, size)).or_insert((0, granule_id));
                counted.0 += 1;
            }
        }
        for ((offset, size), (count, granule)) in extent_counts {
            if count > 1 {
                self.pending_aliased.push((granule, offset, size));
            }
        }
        self.shared_extents_found.insert(stripe_id);
        Ok(())
    }

    fn ensure_column_decoded(
        &mut self,
        file: &OeFile,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
    ) -> Result<(), FormatError> {
        let stripe_id = *self
            .layout
            .granule_stripe_id
            .get(&granule_id)
            .ok_or(FormatError::RefOutOfRange {
                what: "granule missing from stripe index",
            })?;
        self.find_shared_extents(file, stripe_id)?;
        if self.decoded_pages.contains(&(column_id, projection_id, stripe_id)) {
            return Ok(());
        }
        let entries: Vec<MarksPageEntry> = self
            .layout
            .stripe_pages
            .get(&stripe_id)
            .map(|pages| {
                pages
                    .iter()
                    .filter(|entry| entry.column_id == column_id && entry.projection_id == projection_id)
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        for entry in &entries {
            let marks = {
                let page_bytes = self.page_bytes(file, entry)?;
                decode_marks_page(entry.column_id, entry.projection_id, page_bytes)?
            };
            for mark in marks {
                if block_rows_beyond_bound(mark.row_count) {
                    return Err(FormatError::Structural {
                        rule: BLOCK_ROWS_BEYOND_BOUND,
                    });
                }
                self.decoded
                    .insert((mark.column_id, mark.projection_id, mark.granule_id), mark);
            }
        }
        self.decoded_pages.insert((column_id, projection_id, stripe_id));
        Ok(())
    }

    fn ensure_page_directory_decoded(
        &mut self,
        file: &OeFile,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
    ) -> Result<(), FormatError> {
        let stripe_id = *self
            .layout
            .granule_stripe_id
            .get(&granule_id)
            .ok_or(FormatError::RefOutOfRange {
                what: "granule missing from stripe index",
            })?;
        if self
            .decoded_page_directories
            .contains(&(column_id, projection_id, stripe_id))
        {
            return Ok(());
        }
        let entries: Vec<MarksPageEntry> = self
            .layout
            .stripe_pages
            .get(&stripe_id)
            .map(|pages| {
                pages
                    .iter()
                    .filter(|entry| entry.column_id == column_id && entry.projection_id == projection_id)
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        for entry in &entries {
            let page_directory = {
                let page_bytes = self.page_bytes(file, entry)?;
                decode_marks_page_directory(entry.column_id, entry.projection_id, page_bytes)?
            };
            for page in page_directory {
                if page.compressed_len > super::MAX_PAGE_BYTES {
                    return Err(FormatError::Structural {
                        rule: "page/chunk exceeds the maximum size",
                    });
                }
                if block_rows_beyond_bound(page.row_count) {
                    return Err(FormatError::Structural {
                        rule: BLOCK_ROWS_BEYOND_BOUND,
                    });
                }
                self.page_marks.insert(
                    (page.column_id, page.projection_id, page.granule_id, page.page_index),
                    page,
                );
            }
        }
        self.decoded_page_directories
            .insert((column_id, projection_id, stripe_id));
        Ok(())
    }

    fn mark(
        &mut self,
        file: &OeFile,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
    ) -> Result<Option<ColumnMark>, FormatError> {
        self.ensure_column_decoded(file, column_id, projection_id, granule_id)?;
        Ok(self.decoded.get(&(column_id, projection_id, granule_id)).copied())
    }

    fn page_mark(
        &mut self,
        file: &OeFile,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
        page_index: u32,
    ) -> Result<Option<PageDirectoryEntry>, FormatError> {
        self.ensure_page_directory_decoded(file, column_id, projection_id, granule_id)?;
        Ok(self
            .page_marks
            .get(&(column_id, projection_id, granule_id, page_index))
            .copied())
    }

    fn mark_if_decoded(
        &self,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
    ) -> Result<Option<Option<ColumnMark>>, FormatError> {
        let stripe_id = *self
            .layout
            .granule_stripe_id
            .get(&granule_id)
            .ok_or(FormatError::RefOutOfRange {
                what: "granule missing from stripe index",
            })?;
        if !self.decoded_pages.contains(&(column_id, projection_id, stripe_id)) {
            return Ok(None);
        }
        Ok(Some(self.decoded.get(&(column_id, projection_id, granule_id)).copied()))
    }

    fn page_mark_if_decoded(
        &self,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
        page_index: u32,
    ) -> Result<Option<Option<PageDirectoryEntry>>, FormatError> {
        let stripe_id = *self
            .layout
            .granule_stripe_id
            .get(&granule_id)
            .ok_or(FormatError::RefOutOfRange {
                what: "granule missing from stripe index",
            })?;
        if !self
            .decoded_page_directories
            .contains(&(column_id, projection_id, stripe_id))
        {
            return Ok(None);
        }
        Ok(Some(
            self.page_marks
                .get(&(column_id, projection_id, granule_id, page_index))
                .copied(),
        ))
    }

    fn page_directory_if_decoded(
        &self,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
    ) -> Result<Option<Vec<PageDirectoryEntry>>, FormatError> {
        let stripe_id = *self
            .layout
            .granule_stripe_id
            .get(&granule_id)
            .ok_or(FormatError::RefOutOfRange {
                what: "granule missing from stripe index",
            })?;
        if !self
            .decoded_page_directories
            .contains(&(column_id, projection_id, stripe_id))
        {
            return Ok(None);
        }
        let span = (column_id, projection_id, granule_id, 0)..=(column_id, projection_id, granule_id, u32::MAX);
        Ok(Some(self.page_marks.range(span).map(|(_, entry)| *entry).collect()))
    }
}

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "one value per open reader; boxing the columnar state would add an indirection to every mark read"
)]
enum Marks {
    Columnar(RwLock<ColumnarMarks>),
    Eager(Arc<HashMap<(u32, u32, u32), ColumnMark>>),
}

#[derive(Debug, Clone, Default)]
struct FileIndex {
    embedding_row_offsets: HashMap<(u32, u32), EmbeddingRowOffsets>,
    entity_hash_filters_by_granule: HashMap<u32, usize>,
    freetext_row_offsets: HashMap<(u32, u32), FreetextRowOffsets>,
    granule_stripe_base: HashMap<u32, u64>,
    initial_aliased_extents: Vec<(u64, u64)>,
    page_marks: BTreeMap<(u32, u32, u32, u32), PageDirectoryEntry>,
    page_stats_index: HashMap<(u32, u32), PageStats>,
    payload_granule_index: HashMap<u32, PayloadGranule>,
    shared_alphabets: HashMap<u32, Arc<Vec<String>>>,
    snapshot_first_keys_in_order: bool,
    stripe_position_by_id: HashMap<u32, usize>,
    text_token_indexes_by_key: HashMap<(u32, u32, u32), usize>,
    text_token_offsets: HashMap<(u32, u32, u32), TextTokenOffsetsEntry>,
    usable_optional_features: u64,
}

#[derive(Debug)]
pub(super) struct StripeVerification {
    verified: Vec<OnceLock<bool>>,
}

impl StripeVerification {
    fn new(stripes: usize) -> Self {
        Self {
            verified: (0..stripes).map(|_| OnceLock::new()).collect(),
        }
    }

    fn ensure_verified(
        &self,
        index: usize,
        stripe: &StripeEntry,
        checksum: &[u8; 32],
        bytes: &[u8],
    ) -> Result<(), FormatError> {
        let slot = self.verified.get(index).ok_or(FormatError::Structural {
            rule: "stripe missing from the lazy verification state",
        })?;
        let matches = match slot.get() {
            Some(matches) => *matches,
            None => {
                let stripe_bytes = slice(bytes, stripe.file_offset as usize, stripe.byte_len as usize, "stripe")?;
                *slot.get_or_init(|| crate::file::integrity::hash_tree(stripe_bytes).as_bytes() == checksum)
            }
        };
        if matches {
            Ok(())
        } else {
            Err(FormatError::Blake3Mismatch { scope: "stripe" })
        }
    }
}

#[derive(Debug)]
pub struct OeFile {
    aliased_extent_cache: DashMap<(u64, u64, u32), AliasedExtentSlot, ahash::RandomState>,
    aliased_extent_cache_bytes: AtomicU64,
    aliased_extents: DashSet<(u64, u64), ahash::RandomState>,
    block_reads: AtomicU64,
    bytes: FileBytes,
    cache_access_counter: AtomicU64,
    column_cache: DashMap<(u32, u32), CachedColumnSlot, ahash::RandomState>,
    column_cache_bytes: AtomicU64,
    decoded_cache_budget: u64,
    dictionary_decodes: AtomicU64,
    footer: Arc<Footer>,
    granule_dictionaries: DashMap<u32, DictionarySlot, ahash::RandomState>,
    granule_dictionaries_bytes: AtomicU64,
    graph_edges: DashMap<u32, GraphEdgesSlot, ahash::RandomState>,
    graph_edges_bytes: AtomicU64,
    header: OeHeader,
    index: Arc<FileIndex>,
    inflated_residuals: DashMap<(u32, u32), InflatedResidualSlot, ahash::RandomState>,
    inflated_residuals_bytes: AtomicU64,
    lazy_stripes: Option<Arc<StripeVerification>>,
    marks: Marks,
    packed_shared_alphabets: HashMap<u32, PackedAlphabetSlot>,
    shared_string_views: DashMap<u32, SharedStringViewDictionary>,
    point_probe_counts: DashMap<(u32, u32), u32, ahash::RandomState>,
    residual_seek_tables: DashMap<u32, Arc<SeekTable>, ahash::RandomState>,
    pub(super) snapshot_first_keys_in_order: bool,
    stripe_relative: bool,
    text_token_cache: DashMap<(u32, u32, u32), Arc<TextTokenIndex>, ahash::RandomState>,
    usable_optional_features: u64,
}

impl OeFile {
    pub fn open(bytes: Vec<u8>, expected_seal: Option<&[u8; SEAL_LEN]>) -> Result<Self, FormatError> {
        Self::open_with_keys(bytes, expected_seal, None)
    }

    pub fn open_with_keys(
        bytes: Vec<u8>,
        expected_seal: Option<&[u8; SEAL_LEN]>,
        footer_dek: Option<&[u8; FOOTER_DEK_LEN]>,
    ) -> Result<Self, FormatError> {
        Self::open_inner(bytes, expected_seal, footer_dek, false)
    }

    pub fn open_lazy(
        bytes: Vec<u8>,
        expected_seal: Option<&[u8; SEAL_LEN]>,
        footer_dek: Option<&[u8; FOOTER_DEK_LEN]>,
    ) -> Result<Self, FormatError> {
        Self::open_inner(bytes, expected_seal, footer_dek, true)
    }

    fn open_inner(
        bytes: Vec<u8>,
        expected_seal: Option<&[u8; SEAL_LEN]>,
        footer_dek: Option<&[u8; FOOTER_DEK_LEN]>,
        lazy: bool,
    ) -> Result<Self, FormatError> {
        let content_len = match outboard_tree_len(&bytes)? {
            Some(tree_len) => bytes
                .len()
                .checked_sub(TREE_TRAILER_LEN)
                .and_then(|n| n.checked_sub(tree_len as usize))
                .ok_or(FormatError::Truncated { what: "outboard tree" })?,
            None => bytes.len(),
        };
        let content = slice(&bytes, 0, content_len, "oe content")?;
        let (_, proof_trees) = split_outboard_tree(&bytes)?;
        let encoded_footer_len = footer_blob_len(content)?;
        let footer_start = content_len
            .checked_sub(FOOTER_TAIL_TRAILER_LEN)
            .and_then(|n| n.checked_sub(encoded_footer_len))
            .ok_or(FormatError::Truncated { what: "footer" })?;
        let header_block = slice(&bytes, 0, HEADER_BLOCK_LEN, "oe header block")?;
        let header = decode_header(header_block)?;

        compat::check_features(header.feature_flags, 0)?;

        let body_tail_len = content_len
            .checked_sub(HEADER_BLOCK_LEN)
            .ok_or(FormatError::Truncated { what: "oe body tail" })?;
        let body_tail = slice(&bytes, HEADER_BLOCK_LEN, body_tail_len, "oe body tail")?;
        let opened_footer = if header.feature_flags & required_features::FOOTER_ENCRYPTED != 0 {
            let dek = footer_dek.ok_or(FormatError::Structural {
                rule: "encrypted footer requires a file DEK to open",
            })?;
            let stripped_tail = strip_outboard_tree(body_tail)?;
            let plaintext = decrypt_footer_tail(stripped_tail, dek, header.file_id)?;
            let plaintext_len = footer_blob_len(&plaintext)?;
            OeFooter::decode_content_tail(&plaintext, plaintext_len, proof_trees)?
        } else {
            let plaintext_len = footer_blob_len(body_tail)?;
            OeFooter::decode_content_tail(body_tail, plaintext_len, proof_trees)?
        };
        let (footer, usable_optional_features) = opened_footer;
        if header.profile != footer.profile {
            return Err(FormatError::Structural {
                rule: "header profile must match footer profile",
            });
        }

        if footer.stripe_checksums.len() != footer.stripes.len() {
            return Err(FormatError::Structural {
                rule: "stripe checksum count must match the stripe directory",
            });
        }
        let commitments = crate::integrity::verify_declared_data_commitments(
            content,
            footer_start,
            &footer.stripes,
            &footer.stripe_checksums,
            &footer.integrity_gaps,
        )?;
        if let Some(expected) = expected_seal {
            let footer_blob = slice(content, footer_start, encoded_footer_len, "footer region")?;
            let footer_trailer = slice(
                content,
                content_len - FOOTER_TAIL_TRAILER_LEN,
                FOOTER_TAIL_TRAILER_LEN,
                "footer trailer",
            )?;
            let actual = crate::integrity::derive_file_seal(
                header_block,
                footer_blob,
                footer_trailer,
                content_len as u64,
                &commitments,
            );
            if &actual != expected {
                return Err(FormatError::Blake3Mismatch { scope: "oe file seal" });
            }
        }
        let lazy_stripes = if lazy {
            Some(Arc::new(StripeVerification::new(footer.stripes.len())))
        } else {
            let stripe_bytes = footer
                .stripes
                .iter()
                .map(|stripe| slice(&bytes, stripe.file_offset as usize, stripe.byte_len as usize, "stripe"))
                .collect::<Result<Vec<_>, _>>()?;
            let roots = crate::file::integrity::hash_segments(&stripe_bytes);
            if roots
                .iter()
                .zip(&footer.stripe_checksums)
                .any(|(root, checksum)| root.as_bytes() != checksum)
            {
                return Err(FormatError::Blake3Mismatch { scope: "stripe" });
            }
            None
        };
        Self::assemble(
            FileBytes::Local(Arc::new(bytes)),
            header,
            footer,
            usable_optional_features,
            lazy_stripes,
            DECODED_CACHE_BUDGET_BYTES,
        )
    }

    pub fn fresh_reader(&self) -> Result<Self, FormatError> {
        let marks = match &self.marks {
            Marks::Eager(map) => Marks::Eager(Arc::clone(map)),
            Marks::Columnar(state) => Marks::Columnar(RwLock::new(
                state.read().unwrap_or_else(PoisonError::into_inner).fresh(),
            )),
        };
        Ok(Self::over_index(
            self.bytes.fresh(),
            self.header.clone(),
            Arc::clone(&self.footer),
            Arc::clone(&self.index),
            marks,
            self.lazy_stripes.clone(),
            self.decoded_cache_budget,
        ))
    }

    pub(super) fn assemble(
        bytes: FileBytes,
        header: OeHeader,
        footer: Footer,
        usable_optional_features: u64,
        lazy_stripes: Option<Arc<StripeVerification>>,
        decoded_cache_budget: u64,
    ) -> Result<Self, FormatError> {
        if header.row_count != footer.exact_counts.row_count {
            return Err(FormatError::Structural {
                rule: "header row_count must match exact counts",
            });
        }

        let mark_oversize = footer
            .marks
            .iter()
            .any(|mark| mark.page_count <= 1 && mark.compressed_size > super::MAX_PAGE_BYTES);
        let page_oversize = footer
            .page_directory
            .iter()
            .any(|entry| entry.compressed_len > super::MAX_PAGE_BYTES);
        if mark_oversize || page_oversize {
            return Err(FormatError::Structural {
                rule: "page/chunk exceeds the maximum size",
            });
        }
        if footer.marks.iter().any(|mark| block_rows_beyond_bound(mark.row_count))
            || footer
                .page_directory
                .iter()
                .any(|entry| block_rows_beyond_bound(entry.row_count))
        {
            return Err(FormatError::Structural {
                rule: BLOCK_ROWS_BEYOND_BOUND,
            });
        }

        let footer = Arc::new(footer);
        let marks = if footer.required_feature_flags & required_features::COLUMNAR_MARKS != 0 {
            Marks::Columnar(RwLock::new(ColumnarMarks::new(
                &footer,
                footer.required_feature_flags & required_features::STRIPE_MARKS_PAGES != 0,
            )))
        } else {
            Marks::Eager(Arc::new(
                footer
                    .marks
                    .iter()
                    .map(|mark| ((mark.column_id, mark.projection_id, mark.granule_id), *mark))
                    .collect(),
            ))
        };
        let page_marks = footer
            .page_directory
            .iter()
            .map(|e| ((e.column_id, e.projection_id, e.granule_id, e.page_index), *e))
            .collect();
        let freetext_row_offsets = footer
            .freetext_row_offsets
            .iter()
            .map(|entry| ((entry.column_id, entry.granule_id), *entry))
            .collect();
        let embedding_row_offsets = footer
            .embedding_row_offsets
            .iter()
            .map(|entry| ((entry.column_id, entry.granule_id), *entry))
            .collect();
        let text_token_offsets = footer
            .text_token_offsets
            .iter()
            .map(|entry| ((entry.column_id, entry.granule_id, entry.page_index), *entry))
            .collect();
        let text_token_indexes_by_key = footer
            .text_token_indexes
            .iter()
            .enumerate()
            .map(|(index, entry)| ((entry.column_id, entry.granule_id, entry.page_index), index))
            .collect();
        let entity_hash_filters_by_granule = footer
            .entity_hash_filters
            .iter()
            .enumerate()
            .map(|(index, entry)| (entry.granule_id, index))
            .collect();
        let payload_granule_index = footer
            .payload_granules
            .iter()
            .map(|payload| (payload.granule_id, *payload))
            .collect();
        let page_stats_index = footer
            .page_stats
            .iter()
            .map(|stats| ((stats.column_id, stats.granule_id), *stats))
            .collect();
        if footer.granules.windows(2).any(|pair| {
            let [previous, next] = pair else {
                return false;
            };
            previous
                .first_row_ordinal
                .checked_add(u64::from(previous.row_count))
                .is_none_or(|end| end > next.first_row_ordinal)
        }) {
            return Err(FormatError::Structural {
                rule: "granule directory must be strictly ascending by first_row_ordinal with no overlap",
            });
        }
        if footer
            .granules
            .last()
            .is_some_and(|last| last.first_row_ordinal.checked_add(u64::from(last.row_count)).is_none())
        {
            return Err(FormatError::Structural {
                rule: "granule row range overflows",
            });
        }
        let stripe_base_by_id: HashMap<u32, u64> = footer
            .stripes
            .iter()
            .map(|stripe| (stripe.stripe_id, stripe.file_offset))
            .collect();
        let mut stripe_position_by_id: HashMap<u32, usize> = HashMap::with_capacity(footer.stripes.len());
        for (position, stripe) in footer.stripes.iter().enumerate() {
            stripe_position_by_id.entry(stripe.stripe_id).or_insert(position);
        }
        let snapshot_first_keys_in_order = footer
            .snapshot_keys
            .as_ref()
            .is_some_and(|index| index.first_keys_in_order());
        let stripe_relative = footer.required_feature_flags & required_features::STRIPE_RELATIVE_MARKS != 0;
        if stripe_relative
            && footer
                .granules
                .iter()
                .any(|granule| !stripe_base_by_id.contains_key(&granule.stripe_id))
        {
            return Err(FormatError::Structural {
                rule: "granule references a stripe absent from the stripe directory",
            });
        }
        let granule_stripe_base: HashMap<u32, u64> = footer
            .granules
            .iter()
            .filter_map(|granule| {
                stripe_base_by_id
                    .get(&granule.stripe_id)
                    .map(|base| (granule.granule_id, *base))
            })
            .collect();
        let mut extent_marks: HashMap<(u64, u64), u32> = HashMap::new();
        for mark in &footer.marks {
            if mark.page_count <= 1 && mark.compressed_size > 0 {
                let base = if stripe_relative {
                    granule_stripe_base.get(&mark.granule_id).copied().unwrap_or(0)
                } else {
                    0
                };
                if let Some(offset) = base.checked_add(mark.compressed_offset) {
                    *extent_marks.entry((offset, mark.compressed_size)).or_default() += 1;
                }
            }
        }
        let initial_aliased_extents: Vec<(u64, u64)> = extent_marks
            .into_iter()
            .filter_map(|(extent, count)| (count > 1).then_some(extent))
            .collect();
        let shared_alphabets: HashMap<u32, Arc<Vec<String>>> = footer
            .shared_dictionaries
            .iter()
            .map(|entry| (entry.column_id, Arc::new(entry.values.clone())))
            .collect();
        let index = FileIndex {
            embedding_row_offsets,
            entity_hash_filters_by_granule,
            freetext_row_offsets,
            granule_stripe_base,
            initial_aliased_extents,
            page_marks,
            page_stats_index,
            payload_granule_index,
            shared_alphabets,
            snapshot_first_keys_in_order,
            stripe_position_by_id,
            text_token_indexes_by_key,
            text_token_offsets,
            usable_optional_features,
        };
        Ok(Self::over_index(
            bytes,
            header,
            footer,
            Arc::new(index),
            marks,
            lazy_stripes,
            decoded_cache_budget,
        ))
    }

    fn over_index(
        bytes: FileBytes,
        header: OeHeader,
        footer: Arc<Footer>,
        index: Arc<FileIndex>,
        marks: Marks,
        lazy_stripes: Option<Arc<StripeVerification>>,
        decoded_cache_budget: u64,
    ) -> Self {
        let packed_shared_alphabets: HashMap<u32, PackedAlphabetSlot> = footer
            .packed_shared_dictionaries
            .iter()
            .map(|entry| (entry.column_id, PackedAlphabetSlot::default()))
            .collect();
        Self {
            aliased_extent_cache: DashMap::default(),
            aliased_extent_cache_bytes: AtomicU64::new(0),
            aliased_extents: index.initial_aliased_extents.iter().copied().collect(),
            block_reads: AtomicU64::new(0),
            bytes,
            cache_access_counter: AtomicU64::new(0),
            column_cache: DashMap::default(),
            column_cache_bytes: AtomicU64::new(0),
            decoded_cache_budget,
            dictionary_decodes: AtomicU64::new(0),
            granule_dictionaries: DashMap::default(),
            granule_dictionaries_bytes: AtomicU64::new(0),
            graph_edges: DashMap::default(),
            graph_edges_bytes: AtomicU64::new(0),
            header,
            inflated_residuals: DashMap::default(),
            inflated_residuals_bytes: AtomicU64::new(0),
            lazy_stripes,
            marks,
            point_probe_counts: DashMap::default(),
            residual_seek_tables: DashMap::default(),
            packed_shared_alphabets,
            shared_string_views: DashMap::default(),
            snapshot_first_keys_in_order: index.snapshot_first_keys_in_order,
            stripe_relative: footer.required_feature_flags & required_features::STRIPE_RELATIVE_MARKS != 0,
            text_token_cache: DashMap::default(),
            usable_optional_features: index.usable_optional_features,
            footer,
            index,
        }
    }

    pub fn granule_dictionary_decodes(&self) -> u64 {
        self.dictionary_decodes.load(Ordering::Relaxed)
    }

    pub fn residual_cache_bytes(&self) -> u64 {
        self.inflated_residuals_bytes.load(Ordering::Relaxed)
    }

    pub fn column_block_reads(&self) -> u64 {
        self.block_reads.load(Ordering::Relaxed)
    }

    pub(super) fn data(&self, position: usize, len: usize, what: &'static str) -> Result<&[u8], FormatError> {
        let file = match &self.bytes {
            FileBytes::Local(file) => file,
            FileBytes::Remote(remote) => return remote.read(position as u64, len, what),
        };
        let bytes = slice(file, position, len, what)?;
        if let Some(verification) = &self.lazy_stripes {
            let end = position + len;
            for (index, (stripe, checksum)) in self
                .footer
                .stripes
                .iter()
                .zip(&self.footer.stripe_checksums)
                .enumerate()
            {
                let stripe_end = stripe.file_offset.saturating_add(stripe.byte_len) as usize;
                if (stripe.file_offset as usize) < end && position < stripe_end {
                    verification.ensure_verified(index, stripe, checksum, file)?;
                }
            }
        }
        Ok(bytes)
    }

    pub(super) fn remote(&self) -> Option<&RemoteFile> {
        match &self.bytes {
            FileBytes::Local(_) => None,
            FileBytes::Remote(remote) => Some(remote),
        }
    }

    fn compressed_presence(&self) -> bool {
        self.footer.required_feature_flags & required_features::COMPRESSED_PRESENCE != 0
    }

    pub fn column_distinct_estimate(&self, column: u32) -> Option<ColumnDistinctEstimate> {
        let mut combined: Option<ColumnDistinctEstimate> = None;
        for entry in self.footer.stripe_ndv.iter().filter(|entry| entry.column_id == column) {
            combined = Some(match combined {
                None => ColumnDistinctEstimate {
                    distinct_count: entry.distinct_count,
                    exact: entry.exact,
                },
                Some(previous) => ColumnDistinctEstimate {
                    distinct_count: previous.distinct_count.saturating_add(entry.distinct_count),
                    exact: false,
                },
            });
        }
        combined
    }

    fn stripe_base_for(&self, granule_id: u32) -> u64 {
        if self.stripe_relative {
            self.index.granule_stripe_base.get(&granule_id).copied().unwrap_or(0)
        } else {
            0
        }
    }

    pub(super) fn file_position(&self, granule_id: u32, offset: u64) -> Result<usize, FormatError> {
        self.stripe_base_for(granule_id)
            .checked_add(offset)
            .and_then(|position| usize::try_from(position).ok())
            .ok_or(FormatError::RefOutOfRange {
                what: "offset beyond file range",
            })
    }

    pub fn mark(&self, column_id: u32, projection_id: u32, granule_id: u32) -> Result<Option<ColumnMark>, FormatError> {
        match &self.marks {
            Marks::Eager(map) => Ok(map.get(&(column_id, projection_id, granule_id)).copied()),
            Marks::Columnar(state) => {
                if let Some(mark) = state.read().unwrap_or_else(PoisonError::into_inner).mark_if_decoded(
                    column_id,
                    projection_id,
                    granule_id,
                )? {
                    return Ok(mark);
                }
                let mut guard = state.write().unwrap_or_else(PoisonError::into_inner);
                let mark = guard.mark(self, column_id, projection_id, granule_id)?;
                let pending = std::mem::take(&mut guard.pending_aliased);
                drop(guard);
                self.publish_pending_aliases(pending);
                Ok(mark)
            }
        }
    }

    fn publish_pending_aliases(&self, pending: Vec<(u32, u64, u64)>) {
        if pending.is_empty() {
            return;
        }
        for (granule, offset, size) in pending {
            if let Some(absolute) = self.stripe_base_for(granule).checked_add(offset) {
                self.aliased_extents.insert((absolute, size));
            }
        }
    }

    pub fn page_directory_for(
        &self,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
    ) -> Result<Vec<PageDirectoryEntry>, FormatError> {
        let span = (column_id, projection_id, granule_id, 0)..=(column_id, projection_id, granule_id, u32::MAX);
        match &self.marks {
            Marks::Eager(_) => Ok(self.index.page_marks.range(span).map(|(_, entry)| *entry).collect()),
            Marks::Columnar(state) => {
                if let Some(entries) = state
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .page_directory_if_decoded(column_id, projection_id, granule_id)?
                {
                    return Ok(entries);
                }
                let mut guard = state.write().unwrap_or_else(PoisonError::into_inner);
                guard.ensure_page_directory_decoded(self, column_id, projection_id, granule_id)?;
                let entries = guard.page_marks.range(span).map(|(_, entry)| *entry).collect();
                let pending = std::mem::take(&mut guard.pending_aliased);
                drop(guard);
                self.publish_pending_aliases(pending);
                Ok(entries)
            }
        }
    }

    fn page_mark(
        &self,
        column_id: u32,
        projection_id: u32,
        granule_id: u32,
        page_index: u32,
    ) -> Result<Option<PageDirectoryEntry>, FormatError> {
        match &self.marks {
            Marks::Eager(_) => Ok(self
                .index
                .page_marks
                .get(&(column_id, projection_id, granule_id, page_index))
                .copied()),
            Marks::Columnar(state) => {
                if let Some(entry) = state
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .page_mark_if_decoded(column_id, projection_id, granule_id, page_index)?
                {
                    return Ok(entry);
                }
                let mut guard = state.write().unwrap_or_else(PoisonError::into_inner);
                let entry = guard.page_mark(self, column_id, projection_id, granule_id, page_index)?;
                let pending = std::mem::take(&mut guard.pending_aliased);
                drop(guard);
                self.publish_pending_aliases(pending);
                Ok(entry)
            }
        }
    }

    pub fn header(&self) -> &OeHeader {
        &self.header
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    pub fn usable_optional_features(&self) -> u64 {
        self.usable_optional_features
    }

    pub fn optional_feature_plan(
        &self,
        feature_bit: u64,
        reader_version: u32,
        fleet: &dyn compat::PortableDecoderFleet,
    ) -> compat::OptionalBlockPlan {
        let hatch = self
            .footer
            .escape_hatches
            .iter()
            .find(|hatch| hatch.optional_feature_bit == feature_bit);
        compat::plan_optional_block(feature_bit, reader_version, hatch, fleet)
    }

    pub fn granules_for_sequence(&self, epoch: u64, first: u64, last: u64) -> Vec<&GranuleEntry> {
        self.footer
            .granules
            .iter()
            .filter(|granule| {
                granule.first_epoch <= epoch
                    && epoch <= granule.last_epoch
                    && granule.first_sequence <= last
                    && first <= granule.last_sequence
            })
            .collect()
    }

    pub fn granules_for_occurred(&self, min_nanos: i64, max_nanos: i64) -> Vec<&GranuleEntry> {
        self.footer
            .granules
            .iter()
            .filter(|granule| {
                granule.min_occurred_at_physical <= max_nanos && min_nanos <= granule.max_occurred_at_physical
            })
            .collect()
    }

    pub fn granules_for_entity_hash(&self, entity_id_hash_low: u64) -> Vec<&GranuleEntry> {
        if self.usable_optional_features & optional_features::ENTITY_HASH_POINT_FILTERS == 0 {
            return self.footer.granules.iter().collect();
        }
        self.footer
            .granules
            .iter()
            .filter(|granule| {
                self.index
                    .entity_hash_filters_by_granule
                    .get(&granule.granule_id)
                    .and_then(|&index| self.footer.entity_hash_filters.get(index))
                    .and_then(|entry| {
                        let position = self.file_position(entry.granule_id, entry.index_offset).ok()?;
                        let bytes = self
                            .data(position, entry.index_len as usize, "entity hash filter")
                            .ok()?;
                        SplitBlockBloomFilter::contains_encoded(bytes, entity_id_hash_low).ok()
                    })
                    .unwrap_or(true)
            })
            .collect()
    }

    pub fn read_column_raw(&self, column_id: u32, granule_id: u32) -> Result<RawColumnBlock<'_>, FormatError> {
        let mark = self.mark(column_id, 0, granule_id)?.ok_or(FormatError::RefOutOfRange {
            what: "no mark for (column, projection, granule)",
        })?;
        self.read_column_raw_with_mark(granule_id, &mark)
    }

    fn read_column_raw_with_mark(&self, granule_id: u32, mark: &ColumnMark) -> Result<RawColumnBlock<'_>, FormatError> {
        if mark.page_count > 1 {
            return Err(FormatError::RefOutOfRange {
                what: "multi-page column block has no single raw block",
            });
        }
        if mark.compressed_size == 0 {
            return Err(FormatError::Structural {
                rule: "elided constant block has no raw bytes",
            });
        }
        compat::validate_pipeline_window_pinned(mark.codec_pipeline_id.0, self.footer.format_version)?;
        self.block_reads.fetch_add(1, Ordering::Relaxed);
        let block = self.data(
            self.file_position(granule_id, mark.compressed_offset)?,
            mark.compressed_size as usize,
            "column block",
        )?;
        let mut reader = Reader::new(block);
        let presence = super::decode_presence_frame(&mut reader, mark.row_count, self.compressed_presence())?;
        let body = reader.take(reader.remaining(), "block body")?;
        self.resolve_occurred_residual(
            mark.column_id,
            granule_id,
            mark.page_count,
            RawColumnBlock {
                body: Cow::Borrowed(body),
                pipeline: mark.codec_pipeline_id,
                presence,
                row_count: mark.row_count,
            },
        )
    }

    fn resolve_occurred_residual<'a>(
        &'a self,
        column_id: u32,
        granule_id: u32,
        page_count: u32,
        raw: RawColumnBlock<'a>,
    ) -> Result<RawColumnBlock<'a>, FormatError> {
        if raw.pipeline.transform()? != Transform::OccurredResidual {
            return Ok(raw);
        }
        if column_id != column_ids::INGESTED_AT || page_count > 1 {
            return Err(FormatError::Structural {
                rule: "an occurred-relative block is legal only on a single-page ingested_at block",
            });
        }
        let occurred = self.cached_column(column_ids::OCCURRED_AT, granule_id)?;
        let ColumnData::I64(occurred_values) = &occurred.data else {
            return Err(FormatError::Structural {
                rule: "an occurred-relative block needs an i64 occurred_at",
            });
        };
        let (pipeline, body) = resolve_occurred_residual(&raw.body, occurred_values)?;
        Ok(RawColumnBlock {
            body: Cow::Owned(body),
            pipeline,
            presence: raw.presence,
            row_count: raw.row_count,
        })
    }

    pub fn read_page_raw(
        &self,
        column_id: u32,
        granule_id: u32,
        page_index: u32,
    ) -> Result<RawColumnBlock<'_>, FormatError> {
        let mark = self.mark(column_id, 0, granule_id)?.ok_or(FormatError::RefOutOfRange {
            what: "no mark for (column, projection, granule)",
        })?;
        self.read_page_raw_with_mark(column_id, granule_id, page_index, &mark)
    }

    fn read_page_raw_with_mark(
        &self,
        column_id: u32,
        granule_id: u32,
        page_index: u32,
        mark: &ColumnMark,
    ) -> Result<RawColumnBlock<'_>, FormatError> {
        if self.usable_optional_features & optional_features::PER_PAGE_MARKS != 0
            && let Some(entry) = self.page_mark(column_id, 0, granule_id, page_index)?
        {
            if entry.compressed_len == 0 {
                return Err(FormatError::Structural {
                    rule: "elided constant block has no raw bytes",
                });
            }
            self.block_reads.fetch_add(1, Ordering::Relaxed);
            let block = self.data(
                self.file_position(granule_id, entry.compressed_offset)?,
                entry.compressed_len as usize,
                "page block",
            )?;
            let mut reader = Reader::new(block);
            let presence = super::decode_presence_frame(&mut reader, entry.row_count, self.compressed_presence())?;
            let pipeline = if mark.page_count > 1 {
                PipelineId(reader.u32("page pipeline id")?)
            } else {
                mark.codec_pipeline_id
            };
            compat::validate_pipeline_window_pinned(pipeline.0, self.footer.format_version)?;
            let body = reader.take(reader.remaining(), "block body")?;
            return self.resolve_occurred_residual(
                column_id,
                granule_id,
                mark.page_count,
                RawColumnBlock {
                    body: Cow::Borrowed(body),
                    pipeline,
                    presence,
                    row_count: entry.row_count,
                },
            );
        }
        if mark.page_count > 1 {
            return Err(FormatError::RefOutOfRange {
                what: "no page directory entry for multi-page mark",
            });
        }
        if page_index != 0 {
            return Err(FormatError::RefOutOfRange {
                what: "page index beyond single-page column",
            });
        }
        self.read_column_raw_with_mark(granule_id, mark)
    }

    pub fn shared_alphabet(&self, column_id: u32) -> Option<&[String]> {
        if let Some(values) = self.index.shared_alphabets.get(&column_id) {
            return Some(values.as_slice());
        }
        let slot = self.packed_shared_alphabets.get(&column_id)?;
        slot.values
            .get_or_init(|| self.packed_alphabet_text(column_id).map(SharedAlphabetText::to_values))
            .as_deref()
    }

    fn packed_alphabet_text(&self, column_id: u32) -> Option<&SharedAlphabetText> {
        self.packed_shared_alphabets
            .get(&column_id)?
            .text
            .get_or_init(|| {
                self.footer
                    .packed_shared_dictionaries
                    .iter()
                    .find(|entry| entry.column_id == column_id)
                    .and_then(|entry| entry.text().ok())
            })
            .as_ref()
    }

    pub fn shared_dictionary_predicate(
        &self,
        column_id: u32,
        predicate: &StringPredicate,
    ) -> Option<SharedDictionaryPredicate> {
        if let Some(values) = self.index.shared_alphabets.get(&column_id) {
            return Some(translate_for_shared_dictionary(predicate, values.as_slice()));
        }
        let slot = self.packed_shared_alphabets.get(&column_id)?;
        if let Some(Some(values)) = slot.values.get() {
            return Some(translate_for_shared_dictionary(predicate, values.as_slice()));
        }
        let text = self.packed_alphabet_text(column_id)?;
        Some(translate_for_shared_dictionary(predicate, &text.entries()))
    }

    pub fn decode_string_block_views(
        &self,
        column_id: u32,
        pipeline: PipelineId,
        body: &[u8],
    ) -> Result<Option<StringViewArray>, FormatError> {
        let shared = self.shared_alphabet(column_id);
        let uses_shared_dictionary = pipeline.value_kind()? == ValueKind::String
            && pipeline.transform()? == Transform::DictionaryString
            && pipeline.side_stream()? == SideStream::FileScopeDictionary;
        if !uses_shared_dictionary {
            return decode_string_block_views_shared_prepared(pipeline, body, shared, None);
        }

        if let Some(prepared) = self.shared_string_views.get(&column_id) {
            return decode_string_block_views_shared_prepared(pipeline, body, shared, Some(&prepared));
        }
        let alphabet = shared.ok_or(FormatError::Structural {
            rule: "shared-scope dictionary block without its file alphabet",
        })?;
        let prepared = prepare_shared_string_view_dictionary(alphabet)?;
        let prepared = self.shared_string_views.entry(column_id).or_insert(prepared);
        decode_string_block_views_shared_prepared(pipeline, body, shared, Some(&prepared))
    }

    pub fn prepared_string_view_dictionary_count(&self) -> usize {
        self.shared_string_views.len()
    }

    fn materialize_elided_constant(
        &self,
        column_id: u32,
        granule_id: u32,
        mark: &ColumnMark,
    ) -> Result<ColumnRead, FormatError> {
        let stats = self
            .index
            .page_stats_index
            .get(&(column_id, granule_id))
            .ok_or(FormatError::Structural {
                rule: "elided block without page stats",
            })?;
        let constant = match (stats.null_count, stats.min_i128, stats.max_i128) {
            (0, Some(min), Some(max)) if min == max && stats.row_count == mark.row_count => min,
            _ => {
                return Err(FormatError::Structural {
                    rule: "elided block's stats do not prove a constant",
                });
            }
        };
        if mark.row_count as usize > super::MAX_ELIDED_BLOCK_ROWS {
            return Err(FormatError::Structural {
                rule: "elided block row count beyond the block bound",
            });
        }
        let rows = mark.row_count as usize;
        let data = match mark.codec_pipeline_id.value_kind()? {
            ValueKind::I64 => ColumnData::I64(vec![
                i64::try_from(constant).map_err(|_| FormatError::RefOutOfRange {
                    what: "elided i64 constant",
                })?;
                rows
            ]),
            ValueKind::U64 => ColumnData::U64(vec![
                u64::try_from(constant).map_err(|_| FormatError::RefOutOfRange {
                    what: "elided u64 constant",
                })?;
                rows
            ]),
            _ => {
                return Err(FormatError::Structural {
                    rule: "elided blocks carry only integer kinds",
                });
            }
        };
        Ok(ColumnRead {
            data,
            presence: Vec::new(),
        })
    }

    pub fn read_column(&self, column_id: u32, granule_id: u32) -> Result<ColumnRead, FormatError> {
        let mark = self.mark(column_id, 0, granule_id)?.ok_or(FormatError::RefOutOfRange {
            what: "no mark for (column, projection, granule)",
        })?;
        self.read_column_with_mark(column_id, granule_id, &mark)
    }

    fn read_column_with_mark(
        &self,
        column_id: u32,
        granule_id: u32,
        mark: &ColumnMark,
    ) -> Result<ColumnRead, FormatError> {
        if mark.page_count <= 1 {
            if mark.compressed_size == 0 {
                return self.materialize_elided_constant(column_id, granule_id, mark);
            }
            let extent = (
                self.file_position(granule_id, mark.compressed_offset)? as u64,
                mark.compressed_size,
            );
            let column_free_decode = mark.codec_pipeline_id.side_stream() != Ok(SideStream::FileScopeDictionary)
                && mark.codec_pipeline_id.transform() != Ok(Transform::OccurredResidual);
            let shared_key = (column_free_decode && self.aliased_extents.contains(&extent)).then_some((
                extent.0,
                extent.1,
                mark.codec_pipeline_id.0,
            ));
            if let Some(key) = shared_key {
                let tick = self.cache_access_counter.fetch_add(1, Ordering::Relaxed);
                let hit = self.aliased_extent_cache.get_mut(&key).map(|mut slot| {
                    slot.last_used = tick;
                    Arc::clone(&slot.read)
                });
                if let Some(shared) = hit {
                    validate_block_counts(&shared.presence, shared.data.row_count(), mark.row_count as usize)?;
                    return Ok((*shared).clone());
                }
            }
            let raw = self.read_column_raw_with_mark(granule_id, mark)?;
            let data = decode_block_shared(raw.pipeline, &raw.body, self.shared_alphabet(column_id))?;
            validate_block_counts(&raw.presence, data.row_count(), raw.row_count as usize)?;
            let read = ColumnRead {
                presence: raw.presence.into_owned(),
                data,
            };
            if let Some(key) = shared_key {
                let tick = self.cache_access_counter.fetch_add(1, Ordering::Relaxed);
                let bytes = column_data_bytes(&read.data);
                let shared = Arc::new(read.clone());
                let slot = self.aliased_extent_cache.entry(key).or_insert(AliasedExtentSlot {
                    bytes,
                    last_used: tick,
                    read: Arc::clone(&shared),
                });
                if Arc::ptr_eq(&slot.read, &shared) {
                    self.aliased_extent_cache_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
                drop(slot);
                self.shed_aliased_extents();
            }
            return Ok(read);
        }

        let mut combined_presence: Vec<u8> = Vec::new();
        let mut combined_data: Option<ColumnData> = None;
        let mut row_offset = 0usize;
        for page_idx in 0..mark.page_count {
            let page_rows = self
                .page_mark(column_id, 0, granule_id, page_idx)?
                .map(|entry| entry.row_count as usize)
                .ok_or(FormatError::RefOutOfRange {
                    what: "no page directory entry for multi-page mark",
                })?;
            let page = self.read_page_with_mark(column_id, granule_id, page_idx, mark)?;
            if page.presence.is_empty() {
                if !combined_presence.is_empty() {
                    append_all_present_bits(&mut combined_presence, row_offset, page_rows);
                }
            } else {
                if combined_presence.is_empty() && row_offset != 0 {
                    append_all_present_bits(&mut combined_presence, 0, row_offset);
                }
                append_presence_bits(&mut combined_presence, row_offset, &page.presence, page_rows);
            }
            row_offset += page_rows;
            if row_offset > mark.row_count as usize {
                return Err(FormatError::Structural {
                    rule: "sum of page row counts must equal the mark row count",
                });
            }
            combined_data = Some(match combined_data {
                None => page.data,
                Some(existing) => concat_column_data(existing, page.data)?,
            });
        }
        let data = combined_data.ok_or(FormatError::RefOutOfRange {
            what: "no pages for multi-page mark",
        })?;
        if row_offset != mark.row_count as usize {
            return Err(FormatError::Structural {
                rule: "sum of page row counts must equal the mark row count",
            });
        }
        validate_block_counts(&combined_presence, data.row_count(), mark.row_count as usize)?;
        Ok(ColumnRead {
            presence: combined_presence,
            data,
        })
    }

    pub fn read_column_string_views(
        &self,
        column_id: u32,
        granule_id: u32,
    ) -> Result<Option<(Vec<u8>, StringViewArray)>, FormatError> {
        let mark = self.mark(column_id, 0, granule_id)?.ok_or(FormatError::RefOutOfRange {
            what: "no mark for (column, projection, granule)",
        })?;
        if mark.page_count > 1 {
            let mut combined_presence = Vec::new();
            let mut pages = Vec::new();
            let mut row_offset = 0usize;
            for page_index in 0..mark.page_count {
                let raw = self.read_page_raw_with_mark(column_id, granule_id, page_index, &mark)?;
                let page_rows = raw.row_count as usize;
                let Some(views) = self.decode_string_block_views(column_id, raw.pipeline, &raw.body)? else {
                    return Ok(None);
                };
                validate_block_counts(&raw.presence, views.len(), page_rows)?;
                if raw.presence.is_empty() {
                    if !combined_presence.is_empty() {
                        append_all_present_bits(&mut combined_presence, row_offset, page_rows);
                    }
                } else {
                    if combined_presence.is_empty() && row_offset != 0 {
                        append_all_present_bits(&mut combined_presence, 0, row_offset);
                    }
                    append_presence_bits(&mut combined_presence, row_offset, &raw.presence, page_rows);
                }
                row_offset += page_rows;
                if row_offset > mark.row_count as usize {
                    return Err(FormatError::Structural {
                        rule: "sum of page row counts must equal the mark row count",
                    });
                }
                pages.push(views);
            }
            if row_offset != mark.row_count as usize {
                return Err(FormatError::Structural {
                    rule: "sum of page row counts must equal the mark row count",
                });
            }
            let mut builder = StringViewBuilder::with_capacity(pages.iter().map(Array::len).sum());
            for views in &pages {
                builder.append_array(views);
            }
            let views = builder.finish();
            validate_block_counts(&combined_presence, views.len(), mark.row_count as usize)?;
            return Ok(Some((combined_presence, views)));
        }
        let raw = self.read_column_raw_with_mark(granule_id, &mark)?;
        let Some(views) = self.decode_string_block_views(column_id, raw.pipeline, &raw.body)? else {
            return Ok(None);
        };
        validate_block_counts(&raw.presence, views.len(), raw.row_count as usize)?;
        Ok(Some((raw.presence.into_owned(), views)))
    }

    pub fn text_token_filter(
        &self,
        column_id: u32,
        granule_id: u32,
        page_index: u32,
    ) -> Result<Option<Arc<TextTokenIndex>>, FormatError> {
        let key = (column_id, granule_id, page_index);
        if let Some(cached) = self.text_token_cache.get(&key) {
            return Ok(Some(Arc::clone(&cached)));
        }
        let Some(decoded) = self.decode_text_token_filter(column_id, granule_id, page_index)? else {
            return Ok(None);
        };
        let decoded = Arc::new(decoded);
        let entry = Arc::clone(&self.text_token_cache.entry(key).or_insert(decoded));
        Ok(Some(entry))
    }

    fn decode_text_token_filter(
        &self,
        column_id: u32,
        granule_id: u32,
        page_index: u32,
    ) -> Result<Option<TextTokenIndex>, FormatError> {
        if self.usable_optional_features & optional_features::TEXT_TOKEN_FILTER_OFFSETS != 0
            && let Some(entry) = self.index.text_token_offsets.get(&(column_id, granule_id, page_index))
        {
            let bytes = self.data(
                self.file_position(granule_id, entry.index_offset)?,
                entry.index_len as usize,
                "text token filter",
            )?;
            return TextTokenIndex::decode(bytes).map(Some);
        }
        self.index
            .text_token_indexes_by_key
            .get(&(column_id, granule_id, page_index))
            .and_then(|&index| self.footer.text_token_indexes.get(index))
            .map(|entry| TextTokenIndex::decode(&entry.index_bytes))
            .transpose()
    }

    pub fn bulk_read_family(&self, column_id: u32) -> Result<Vec<(u32, ColumnRead)>, FormatError> {
        let mut granule_ids = Vec::new();
        for granule in &self.footer.granules {
            if self.mark(column_id, 0, granule.granule_id)?.is_some() {
                granule_ids.push(granule.granule_id);
            }
        }
        granule_ids.sort_unstable();
        granule_ids.dedup();
        let mut reads = Vec::with_capacity(granule_ids.len());
        for granule_id in granule_ids {
            reads.push((granule_id, self.read_column(column_id, granule_id)?));
        }
        Ok(reads)
    }

    pub fn string_block_plaintext_bytes(&self, column_id: u32, granule_id: u32) -> Result<Option<u64>, FormatError> {
        let Some(mark) = self.mark(column_id, 0, granule_id)? else {
            return Ok(None);
        };
        if mark.page_count > 1 || mark.compressed_size == 0 {
            return Ok(None);
        }
        let raw = self.read_column_raw_with_mark(granule_id, &mark)?;
        Ok(fsst_uncompressed_len(raw.pipeline, &raw.body)?.map(|len| len as u64))
    }

    pub fn read_page(&self, column_id: u32, granule_id: u32, page_index: u32) -> Result<ColumnRead, FormatError> {
        let mark = self.mark(column_id, 0, granule_id)?.ok_or(FormatError::RefOutOfRange {
            what: "no mark for (column, projection, granule)",
        })?;
        self.read_page_with_mark(column_id, granule_id, page_index, &mark)
    }

    fn read_page_with_mark(
        &self,
        column_id: u32,
        granule_id: u32,
        page_index: u32,
        mark: &ColumnMark,
    ) -> Result<ColumnRead, FormatError> {
        if page_index == 0 && mark.page_count <= 1 && mark.compressed_size == 0 {
            return self.materialize_elided_constant(column_id, granule_id, mark);
        }
        let raw = self.read_page_raw_with_mark(column_id, granule_id, page_index, mark)?;
        let data = decode_block_shared(raw.pipeline, &raw.body, self.shared_alphabet(column_id))?;
        validate_block_counts(&raw.presence, data.row_count(), raw.row_count as usize)?;
        Ok(ColumnRead {
            data,
            presence: raw.presence.into_owned(),
        })
    }

    fn granule_of_row(&self, row_ordinal: u64) -> Result<&GranuleEntry, FormatError> {
        let granules = &self.footer.granules;
        let index = granules.partition_point(|granule| granule.first_row_ordinal <= row_ordinal);
        index
            .checked_sub(1)
            .and_then(|index| granules.get(index))
            .filter(|granule| {
                granule
                    .first_row_ordinal
                    .checked_add(u64::from(granule.row_count))
                    .is_some_and(|end| row_ordinal < end)
            })
            .ok_or(FormatError::RefOutOfRange {
                what: "row ordinal beyond granule directory",
            })
    }

    fn payload_granule(&self, granule_id: u32) -> Result<&PayloadGranule, FormatError> {
        self.index
            .payload_granule_index
            .get(&granule_id)
            .ok_or(FormatError::RefOutOfRange {
                what: "granule missing from payload index",
            })
    }

    fn residual_slot(&self, payload: &PayloadGranule, row_in_granule: u64) -> Result<Option<(u64, u64)>, FormatError> {
        let offsets = self.data(
            self.file_position(payload.granule_id, payload.offsets_offset)?,
            payload.offsets_len as usize,
            "payload offsets",
        )?;
        let entry_index = if offsets.len() == ROW_OFFSET_ENTRY_LEN {
            0
        } else {
            row_in_granule as usize
        };
        let entry = slice(
            offsets,
            entry_index * ROW_OFFSET_ENTRY_LEN,
            ROW_OFFSET_ENTRY_LEN,
            "payload offset entry",
        )?;
        let mut reader = Reader::new(entry);
        let offset = reader.u32("payload offset")?;
        let len = reader.u32("payload len")?;
        if len == 0 {
            return Ok(None);
        }
        Ok(Some((u64::from(offset), u64::from(len))))
    }

    fn residual_bytes(
        &self,
        payload: &PayloadGranule,
        offset: u64,
        len: u64,
    ) -> Result<ResidualBytes<'_>, FormatError> {
        match payload.residual_compression {
            ResidualCompression::None => {
                if offset.checked_add(len).is_none_or(|end| end > payload.residual_len) {
                    return Err(FormatError::Truncated { what: "residual value" });
                }
                let position = self
                    .file_position(payload.granule_id, payload.residual_offset)?
                    .checked_add(offset as usize)
                    .ok_or(FormatError::RefOutOfRange {
                        what: "offset beyond file range",
                    })?;
                Ok(ResidualBytes::InFile(self.data(
                    position,
                    len as usize,
                    "residual value",
                )?))
            }
            ResidualCompression::Zstd3 => {
                let stored = self.residual_block(payload)?;
                let arena = self.inflated_residual(payload.granule_id, 0, || {
                    active_decompressor().decompress(Compression::Zstd3, stored)
                })?;
                let start = offset as usize;
                let end = start + slice(&arena, start, len as usize, "residual value")?.len();
                Ok(ResidualBytes::InArena { arena, end, start })
            }
            ResidualCompression::ZstdSeekable => {
                let stored = self.residual_block(payload)?;
                let table = self.residual_seek_table(payload, stored)?;
                let end = offset.checked_add(len).ok_or(FormatError::RefOutOfRange {
                    what: "residual value span",
                })?;
                let frame = table.frame_index_decomp(offset);
                let bound = |result: Result<u64, zeekstd::Error>| {
                    result.map_err(|_| FormatError::RefOutOfRange {
                        what: "residual frame index",
                    })
                };
                let frame_start = bound(table.frame_start_decomp(frame))?;
                let frame_end = bound(table.frame_end_decomp(frame))?;
                if end > frame_end {
                    let value = seekable_zstd::decompress_range(stored, &table, offset, end)?;
                    return Ok(ResidualBytes::InArena {
                        end: value.len(),
                        arena: Arc::new(value),
                        start: 0,
                    });
                }
                let arena = self.inflated_residual(payload.granule_id, frame, || {
                    seekable_zstd::decompress_range(stored, &table, frame_start, frame_end)
                })?;
                let start = (offset - frame_start) as usize;
                let end = start + slice(&arena, start, len as usize, "residual value")?.len();
                Ok(ResidualBytes::InArena { arena, end, start })
            }
        }
    }

    fn residual_block(&self, payload: &PayloadGranule) -> Result<&[u8], FormatError> {
        self.data(
            self.file_position(payload.granule_id, payload.residual_offset)?,
            payload.residual_len as usize,
            "residual block",
        )
    }

    fn residual_seek_table(&self, payload: &PayloadGranule, stored: &[u8]) -> Result<Arc<SeekTable>, FormatError> {
        if let Some(table) = self.residual_seek_tables.get(&payload.granule_id) {
            return Ok(Arc::clone(&table));
        }
        let table = Arc::new(seekable_zstd::seek_table(stored)?);
        Ok(Arc::clone(
            &self.residual_seek_tables.entry(payload.granule_id).or_insert(table),
        ))
    }

    fn inflated_residual(
        &self,
        granule_id: u32,
        frame: u32,
        inflate: impl FnOnce() -> Result<Vec<u8>, FormatError>,
    ) -> Result<Arc<Vec<u8>>, FormatError> {
        let key = (granule_id, frame);
        let tick = self.cache_access_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(mut slot) = self.inflated_residuals.get_mut(&key) {
            slot.last_used = tick;
            return Ok(Arc::clone(&slot.arena));
        }
        let inflated = Arc::new(inflate()?);
        let bytes = inflated.len() as u64;
        let arena = {
            let slot = self.inflated_residuals.entry(key).or_insert(InflatedResidualSlot {
                arena: Arc::clone(&inflated),
                last_used: tick,
            });
            if Arc::ptr_eq(&slot.arena, &inflated) {
                self.inflated_residuals_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Arc::clone(&slot.arena)
        };
        while self.inflated_residuals_bytes.load(Ordering::Relaxed) > self.decoded_cache_budget
            && self.inflated_residuals.len() > 1
        {
            let Some(evict) = self
                .inflated_residuals
                .iter()
                .filter(|slot| *slot.key() != key)
                .min_by_key(|slot| slot.last_used)
                .map(|slot| *slot.key())
            else {
                break;
            };
            if let Some((_, evicted)) = self.inflated_residuals.remove(&evict) {
                self.inflated_residuals_bytes
                    .fetch_sub(evicted.arena.len() as u64, Ordering::Relaxed);
            }
        }
        self.shed_aliased_extents();
        Ok(arena)
    }

    fn shed_aliased_extents(&self) {
        if self.aliased_extent_cache.is_empty() {
            return;
        }
        let others = self.column_cache_bytes.load(Ordering::Relaxed)
            + self.inflated_residuals_bytes.load(Ordering::Relaxed)
            + self.granule_dictionaries_bytes.load(Ordering::Relaxed)
            + self.graph_edges_bytes.load(Ordering::Relaxed);
        let headroom = self.decoded_cache_budget.saturating_sub(others);
        while self.aliased_extent_cache_bytes.load(Ordering::Relaxed) > headroom
            && !self.aliased_extent_cache.is_empty()
        {
            let Some(evict) = self
                .aliased_extent_cache
                .iter()
                .min_by_key(|slot| slot.last_used)
                .map(|slot| *slot.key())
            else {
                break;
            };
            if let Some((_, evicted)) = self.aliased_extent_cache.remove(&evict) {
                self.aliased_extent_cache_bytes
                    .fetch_sub(evicted.bytes, Ordering::Relaxed);
            }
        }
    }

    fn freetext_row_slot(
        &self,
        entry: &FreetextRowOffsets,
        row_in_granule: u64,
    ) -> Result<Option<(u64, u64)>, FormatError> {
        let offsets = self.data(
            self.file_position(entry.granule_id, entry.offsets_offset)?,
            entry.offsets_len as usize,
            "freetext row offsets",
        )?;
        let slot = slice(
            offsets,
            row_in_granule as usize * ROW_OFFSET_ENTRY_LEN,
            ROW_OFFSET_ENTRY_LEN,
            "freetext row offset entry",
        )?;
        let mut reader = Reader::new(slot);
        let offset = reader.u32("freetext row offset")?;
        let len = reader.u32("freetext row len")?;
        if len == 0 {
            return Ok((offset == EMPTY_VALUE_ROW_OFFSET).then_some((0, 0)));
        }
        Ok(Some((u64::from(offset), u64::from(len))))
    }

    fn freetext_row_bytes(&self, entry: &FreetextRowOffsets, offset: u64, len: u64) -> Result<&[u8], FormatError> {
        let bytes = self.data(
            self.file_position(entry.granule_id, entry.bytes_offset)?,
            entry.bytes_len as usize,
            "freetext row bytes",
        )?;
        Ok(slice(bytes, offset as usize, len as usize, "freetext row value")?)
    }

    pub fn embedding_row_slot(
        &self,
        column_id: u32,
        granule_id: u32,
        row_in_granule: u64,
    ) -> Result<Option<(u64, u64)>, FormatError> {
        let Some(entry) = self.index.embedding_row_offsets.get(&(column_id, granule_id)) else {
            return Ok(None);
        };
        let offsets = self.data(
            self.file_position(entry.granule_id, entry.offsets_offset)?,
            entry.offsets_len as usize,
            "embedding row offsets",
        )?;
        let slot = slice(
            offsets,
            row_in_granule as usize * ROW_OFFSET_ENTRY_LEN,
            ROW_OFFSET_ENTRY_LEN,
            "embedding row offset entry",
        )?;
        let mut reader = Reader::new(slot);
        let offset = reader.u32("embedding row offset")?;
        let len = reader.u32("embedding row len")?;
        if len == 0 {
            return Ok(None);
        }
        Ok(Some((u64::from(offset), u64::from(len))))
    }

    pub fn embedding_row_bytes(
        &self,
        column_id: u32,
        granule_id: u32,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, FormatError> {
        let entry =
            self.index
                .embedding_row_offsets
                .get(&(column_id, granule_id))
                .ok_or(FormatError::RefOutOfRange {
                    what: "embedding row offsets missing for column/granule",
                })?;
        let bytes = self.data(
            self.file_position(entry.granule_id, entry.bytes_offset)?,
            entry.bytes_len as usize,
            "embedding row bytes",
        )?;
        Ok(slice(bytes, offset as usize, len as usize, "embedding row value")?.to_vec())
    }

    fn freetext_value_for_row(
        &self,
        column_id: u32,
        granule: &GranuleEntry,
        row_in_granule: u64,
    ) -> Result<Option<VariantValue>, FormatError> {
        if self.usable_optional_features & optional_features::TYPED_COLUMN_ROW_OFFSETS != 0
            && let Some(entry) = self.index.freetext_row_offsets.get(&(column_id, granule.granule_id))
        {
            let Some((offset, len)) = self.freetext_row_slot(entry, row_in_granule)? else {
                return Ok(None);
            };
            let bytes = self.freetext_row_bytes(entry, offset, len)?;
            let text = simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 {
                what: "freetext row value",
            })?;
            return Ok(Some(VariantValue::String(text.to_owned())));
        }
        self.shredded_value_for_row(column_id, granule, row_in_granule)
    }

    pub fn embedding_value_for_row(&self, column_id: u32, row_ordinal: u64) -> Result<Option<Vec<u8>>, FormatError> {
        let granule = self.granule_of_row(row_ordinal)?;
        let row_in_granule = row_ordinal - granule.first_row_ordinal;
        if self.usable_optional_features & optional_features::TYPED_COLUMN_ROW_OFFSETS != 0
            && self
                .index
                .embedding_row_offsets
                .contains_key(&(column_id, granule.granule_id))
        {
            let Some((offset, len)) = self.embedding_row_slot(column_id, granule.granule_id, row_in_granule)? else {
                return Ok(None);
            };
            return Ok(Some(self.embedding_row_bytes(
                column_id,
                granule.granule_id,
                offset,
                len,
            )?));
        }
        let read = self.read_column(column_id, granule.granule_id)?;
        let ColumnData::Strings(values) = &read.data else {
            return Ok(None);
        };
        let row = row_in_granule as usize;
        let position = if read.presence.is_empty() {
            Some(row)
        } else {
            present_position(&read.presence, row)
        };
        Ok(position
            .and_then(|position| values.get(position).flatten())
            .map(|value| value.as_bytes().to_vec()))
    }

    fn granule_dictionary(&self, payload: &PayloadGranule) -> Result<Arc<KeyDictionary>, FormatError> {
        let tick = self.cache_access_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(mut slot) = self.granule_dictionaries.get_mut(&payload.granule_id) {
            slot.last_used = tick;
            return Ok(Arc::clone(&slot.dictionary));
        }
        self.dictionary_decodes.fetch_add(1, Ordering::Relaxed);
        let bytes = self.data(
            self.file_position(payload.granule_id, payload.dictionary_offset)?,
            payload.dictionary_len as usize,
            "granule dictionary",
        )?;
        let decoded = Arc::new(decode_variant_dictionary(bytes)?);
        let bytes = dictionary_bytes(&decoded);
        let dictionary = {
            let slot = self
                .granule_dictionaries
                .entry(payload.granule_id)
                .or_insert(DictionarySlot {
                    bytes,
                    dictionary: Arc::clone(&decoded),
                    last_used: tick,
                });
            if Arc::ptr_eq(&slot.dictionary, &decoded) {
                self.granule_dictionaries_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Arc::clone(&slot.dictionary)
        };
        while self.granule_dictionaries_bytes.load(Ordering::Relaxed) > self.decoded_cache_budget
            && self.granule_dictionaries.len() > 1
        {
            let Some(evict) = self
                .granule_dictionaries
                .iter()
                .filter(|slot| *slot.key() != payload.granule_id)
                .min_by_key(|slot| slot.last_used)
                .map(|slot| *slot.key())
            else {
                break;
            };
            if let Some((_, evicted)) = self.granule_dictionaries.remove(&evict) {
                self.granule_dictionaries_bytes
                    .fetch_sub(evicted.bytes, Ordering::Relaxed);
            }
        }
        self.shed_aliased_extents();
        Ok(dictionary)
    }

    pub fn cached_granule_edges(&self, granule_id: u32) -> Result<Arc<GranuleEdges>, FormatError> {
        let tick = self.cache_access_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(mut slot) = self.graph_edges.get_mut(&granule_id) {
            slot.last_used = tick;
            return Ok(Arc::clone(&slot.edges));
        }
        let decoded = Arc::new(self.granule_edges(granule_id)?);
        let bytes = decoded.decoded_bytes();
        let edges = {
            let slot = self.graph_edges.entry(granule_id).or_insert(GraphEdgesSlot {
                bytes,
                edges: Arc::clone(&decoded),
                last_used: tick,
            });
            if Arc::ptr_eq(&slot.edges, &decoded) {
                self.graph_edges_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Arc::clone(&slot.edges)
        };
        while self.graph_edges_bytes.load(Ordering::Relaxed) > self.decoded_cache_budget && self.graph_edges.len() > 1 {
            let Some(evict) = self
                .graph_edges
                .iter()
                .filter(|slot| *slot.key() != granule_id)
                .min_by_key(|slot| slot.last_used)
                .map(|slot| *slot.key())
            else {
                break;
            };
            if let Some((_, evicted)) = self.graph_edges.remove(&evict) {
                self.graph_edges_bytes.fetch_sub(evicted.bytes, Ordering::Relaxed);
            }
        }
        self.shed_aliased_extents();
        Ok(edges)
    }

    fn row_payload_flags(&self, granule: &GranuleEntry, row_in_granule: u64) -> Result<u64, FormatError> {
        let flags = self.cached_column(crate::columns::column_ids::PAYLOAD_FLAGS, granule.granule_id)?;
        Self::payload_flags_at(&flags, row_in_granule)
    }

    fn payload_flags_at(flags: &CachedColumn, row_in_granule: u64) -> Result<u64, FormatError> {
        match &flags.data {
            ColumnData::U64(values) => values
                .get(row_in_granule as usize)
                .copied()
                .ok_or(FormatError::RefOutOfRange {
                    what: "row beyond payload_flags block",
                }),
            _ => Err(FormatError::Structural {
                rule: "payload_flags must be a u64 column",
            }),
        }
    }

    fn typed_variant_at(data: &ColumnData, position: usize) -> Option<VariantValue> {
        match data {
            ColumnData::I64(values) => values.get(position).map(|v| VariantValue::Int(*v)),
            ColumnData::F64(values) => values.get(position).map(|v| VariantValue::Double(*v)),
            ColumnData::Decimal { values, scale } => values.get(position).map(|v| VariantValue::Decimal {
                unscaled: *v,
                scale: *scale,
            }),
            ColumnData::Strings(values) => values
                .get(position)
                .flatten()
                .map(|v| VariantValue::String(v.to_owned())),
            _ => None,
        }
    }

    fn shredded_value_for_row(
        &self,
        column_id: u32,
        granule: &GranuleEntry,
        row_in_granule: u64,
    ) -> Result<Option<VariantValue>, FormatError> {
        let cache_hit = {
            let tick = self.cache_access_counter.fetch_add(1, Ordering::Relaxed);
            self.column_cache
                .get_mut(&(column_id, granule.granule_id))
                .map(|mut slot| {
                    slot.last_used = tick;
                    Arc::clone(&slot.column)
                })
        };
        if let Some(column) = cache_hit {
            return Ok(column.value_at(row_in_granule));
        }
        if let Some(mark) = self.mark(column_id, 0, granule.granule_id)?
            && mark.page_count <= 1
            && mark.compressed_size > 0
            && self.count_point_probe(column_id, granule.granule_id) <= POINT_READS_BEFORE_WHOLE_GRANULE_DECODE
        {
            let raw = self.read_column_raw_with_mark(granule.granule_id, &mark)?;
            if raw.pipeline.supports_byte_range_extraction()? {
                let row = row_in_granule as usize;
                let position = if raw.presence.is_empty() {
                    row
                } else {
                    match present_position(&raw.presence, row) {
                        Some(position) => position,
                        None => return Ok(None),
                    }
                };
                let data = decode_block_range_shared(
                    raw.pipeline,
                    &raw.body,
                    self.shared_alphabet(column_id),
                    position,
                    position + 1,
                )?;
                return Ok(Self::typed_variant_at(&data, 0));
            }
        }
        Ok(self
            .cached_column(column_id, granule.granule_id)?
            .value_at(row_in_granule))
    }

    fn cached_column(&self, column_id: u32, granule_id: u32) -> Result<Arc<CachedColumn>, FormatError> {
        let key = (column_id, granule_id);
        let tick = self.cache_access_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(mut slot) = self.column_cache.get_mut(&key) {
            slot.last_used = tick;
            return Ok(Arc::clone(&slot.column));
        }
        let read = self.read_column(column_id, granule_id)?;
        let bytes = column_data_bytes(&read.data);
        let rank = RankSelect::from_packed_bits(&read.presence);
        let decoded = Arc::new(CachedColumn {
            data: read.data,
            dense: read.presence.is_empty(),
            rank,
        });
        let column = {
            let slot = self.column_cache.entry(key).or_insert(CachedColumnSlot {
                bytes,
                column: Arc::clone(&decoded),
                last_used: tick,
            });
            if Arc::ptr_eq(&slot.column, &decoded) {
                self.column_cache_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Arc::clone(&slot.column)
        };
        while self.column_cache_bytes.load(Ordering::Relaxed) > self.decoded_cache_budget && self.column_cache.len() > 1
        {
            let Some(evict) = self
                .column_cache
                .iter()
                .filter(|slot| *slot.key() != key)
                .min_by_key(|slot| slot.last_used)
                .map(|slot| *slot.key())
            else {
                break;
            };
            if let Some((_, evicted)) = self.column_cache.remove(&evict) {
                self.column_cache_bytes.fetch_sub(evicted.bytes, Ordering::Relaxed);
            }
        }
        self.shed_aliased_extents();
        Ok(column)
    }

    pub(super) fn sorted_string_row(
        &self,
        column_id: u32,
        granule_id: u32,
        row_count: u32,
        (from, to): (usize, usize),
        key: &str,
    ) -> Result<Option<usize>, FormatError> {
        let column = self.cached_column(column_id, granule_id)?;
        let ColumnData::Strings(values) = &column.data else {
            return Err(FormatError::Structural {
                rule: "a sorted key column is a string column",
            });
        };
        if values.len() != row_count as usize {
            return Err(FormatError::Structural {
                rule: "a sorted key column stores every row's key",
            });
        }
        let end = to.saturating_add(1).min(values.len());
        let (mut low, mut high) = (from.min(end), end);
        while low < high {
            let middle = low + (high - low) / 2;
            match values.get(middle).flatten() {
                Some(value) if value < key => low = middle + 1,
                _ => high = middle,
            }
        }
        Ok((low < end && values.get(low).flatten() == Some(key)).then_some(low))
    }

    pub(super) fn with_cached_string_at<T>(
        &self,
        column_id: u32,
        granule_id: u32,
        row_count: u32,
        row: u64,
        visit: impl FnOnce(Option<&str>) -> T,
    ) -> Result<T, FormatError> {
        let column = self.cached_column(column_id, granule_id)?;
        let ColumnData::Strings(values) = &column.data else {
            return Ok(visit(None));
        };
        let position = if values.len() == row_count as usize {
            Some(row)
        } else {
            let (before, present) = column.rank.rank_and_contains(row);
            present.then_some(before)
        };
        Ok(visit(
            position.and_then(|position| values.get(position as usize).flatten()),
        ))
    }

    pub(super) fn with_cached_u128_at(
        &self,
        column_id: u32,
        granule_id: u32,
        row: u64,
    ) -> Result<Option<u128>, FormatError> {
        let column = self.cached_column(column_id, granule_id)?;
        let ColumnData::U128(values) = &column.data else {
            return Ok(None);
        };
        Ok(usize::try_from(row).ok().and_then(|row| values.get(row).copied()))
    }

    pub fn with_decoded_cache_budget(mut self, bytes: u64) -> Self {
        self.decoded_cache_budget = bytes;
        self
    }

    pub fn decoded_cache_bytes(&self) -> u64 {
        self.aliased_extent_cache_bytes.load(Ordering::Relaxed)
            + self.column_cache_bytes.load(Ordering::Relaxed)
            + self.granule_dictionaries_bytes.load(Ordering::Relaxed)
            + self.graph_edges_bytes.load(Ordering::Relaxed)
            + self.inflated_residuals_bytes.load(Ordering::Relaxed)
    }

    fn count_point_probe(&self, column_id: u32, granule_id: u32) -> u32 {
        if self.point_probe_counts.len() >= POINT_PROBE_ENTRIES_MAX
            && !self.point_probe_counts.contains_key(&(column_id, granule_id))
        {
            self.point_probe_counts.clear();
        }
        let mut count = self.point_probe_counts.entry((column_id, granule_id)).or_insert(0);
        *count = count.saturating_add(1);
        *count
    }

    pub(crate) fn stripe_by_id(&self, stripe_id: u32) -> Option<&StripeEntry> {
        self.index
            .stripe_position_by_id
            .get(&stripe_id)
            .and_then(|&position| self.footer().stripes.get(position))
    }

    pub fn payload(&self, row_ordinal: u64) -> Result<PayloadRead, FormatError> {
        let granule = self.granule_of_row(row_ordinal)?;
        let payload = self.payload_granule(granule.granule_id)?;
        self.payload_in_granule(granule, payload, row_ordinal - granule.first_row_ordinal)
    }

    pub fn read_payloads(&self, refs: &[PayloadRef]) -> Result<PayloadBatch, FormatError> {
        let mut order: Vec<usize> = (0..refs.len()).collect();
        order.sort_by_key(|&index| refs.get(index).map(|payload_ref| payload_ref.row_ordinal));
        let mut payloads = vec![PayloadRead::None; refs.len()];
        let mut group: Option<PayloadGroup<'_>> = None;
        for (from, &index) in order.iter().enumerate() {
            let Some(row_ordinal) = refs.get(index).map(|payload_ref| payload_ref.row_ordinal) else {
                continue;
            };
            let in_group = group.as_ref().is_some_and(|group| {
                group.granule.first_row_ordinal <= row_ordinal
                    && group
                        .granule
                        .first_row_ordinal
                        .checked_add(u64::from(group.granule.row_count))
                        .is_some_and(|end| row_ordinal < end)
            });
            if !in_group {
                let granule = self.granule_of_row(row_ordinal)?;
                let payload = self.payload_granule(granule.granule_id)?;
                let rows = batch_rows_in_granule(refs, &order, from, granule);
                group = Some(self.payload_group(granule, payload, rows)?);
            }
            if let Some(group) = group.as_mut() {
                let row_in_granule = row_ordinal - group.granule.first_row_ordinal;
                let payload = self.payload_in_group(group, row_in_granule)?;
                if let Some(slot) = payloads.get_mut(index) {
                    *slot = payload;
                }
            }
        }
        Ok(PayloadBatch { payloads })
    }

    fn payload_in_granule(
        &self,
        granule: &GranuleEntry,
        payload: &PayloadGranule,
        row_in_granule: u64,
    ) -> Result<PayloadRead, FormatError> {
        let Some((offset, len)) = self.residual_slot(payload, row_in_granule)? else {
            return Ok(PayloadRead::None);
        };
        let bytes = self.residual_bytes(payload, offset, len)?;
        let flags = self.row_payload_flags(granule, row_in_granule)?;
        if flags & u64::from(crate::artifacts::batch::PAYLOAD_FLAG_EXTERNAL_REF) != 0 {
            return Ok(PayloadRead::External(owned_utf8(&bytes, "external payload reference")?));
        }
        let dictionary = self.granule_dictionary(payload)?;
        let mut value = VariantRef::new(&bytes).decode(&dictionary)?;
        if let VariantValue::Object(fields) = &mut value {
            for entry in &self.footer.shredded {
                if let Some(shredded) = self.shredded_value_for_row(entry.column_id, granule, row_in_granule)? {
                    fields.insert(entry.path.clone(), shredded);
                }
            }
            for entry in &self.footer.freetext {
                if let Some(text) = self.freetext_value_for_row(entry.column_id, granule, row_in_granule)? {
                    fields.insert(entry.declared_field.clone(), text);
                }
            }
        }
        Ok(PayloadRead::Value(value))
    }

    fn payload_group<'a>(
        &'a self,
        granule: &'a GranuleEntry,
        payload: &'a PayloadGranule,
        rows: Vec<u64>,
    ) -> Result<PayloadGroup<'a>, FormatError> {
        Ok(PayloadGroup {
            dictionary: None,
            field_blocks: HashMap::new(),
            flags: self.cached_column(crate::columns::column_ids::PAYLOAD_FLAGS, granule.granule_id)?,
            granule,
            payload,
            rows,
        })
    }

    fn payload_in_group<'a>(
        &'a self,
        group: &mut PayloadGroup<'a>,
        row_in_granule: u64,
    ) -> Result<PayloadRead, FormatError> {
        let Some((offset, len)) = self.residual_slot(group.payload, row_in_granule)? else {
            return Ok(PayloadRead::None);
        };
        let bytes = self.residual_bytes(group.payload, offset, len)?;
        let flags = Self::payload_flags_at(&group.flags, row_in_granule)?;
        if flags & u64::from(crate::artifacts::batch::PAYLOAD_FLAG_EXTERNAL_REF) != 0 {
            return Ok(PayloadRead::External(owned_utf8(&bytes, "external payload reference")?));
        }
        let dictionary = match &mut group.dictionary {
            Some(dictionary) => &*dictionary,
            slot @ None => &*slot.insert(self.granule_dictionary(group.payload)?),
        };
        let mut value = VariantRef::new(&bytes).decode(dictionary)?;
        if let VariantValue::Object(fields) = &mut value {
            for entry in &self.footer.shredded {
                if let Some(shredded) = self.field_value_in_group(group, entry.column_id, row_in_granule)? {
                    fields.insert(entry.path.clone(), shredded);
                }
            }
            for entry in &self.footer.freetext {
                if let Some(text) = self.freetext_value_in_group(group, entry.column_id, row_in_granule)? {
                    fields.insert(entry.declared_field.clone(), text);
                }
            }
        }
        Ok(PayloadRead::Value(value))
    }

    fn field_value_in_group<'a>(
        &'a self,
        group: &mut PayloadGroup<'a>,
        column_id: u32,
        row_in_granule: u64,
    ) -> Result<Option<VariantValue>, FormatError> {
        let block = match group.field_blocks.entry(column_id) {
            hashbrown::hash_map::Entry::Occupied(entry) => &*entry.into_mut(),
            hashbrown::hash_map::Entry::Vacant(entry) => {
                &*entry.insert(self.path_block(column_id, group.granule.granule_id, &group.rows)?)
            }
        };
        self.value_in_block(block, column_id, row_in_granule)
    }

    fn freetext_value_in_group<'a>(
        &'a self,
        group: &mut PayloadGroup<'a>,
        column_id: u32,
        row_in_granule: u64,
    ) -> Result<Option<VariantValue>, FormatError> {
        if self.usable_optional_features & optional_features::TYPED_COLUMN_ROW_OFFSETS != 0
            && let Some(entry) = self
                .index
                .freetext_row_offsets
                .get(&(column_id, group.granule.granule_id))
        {
            let Some((offset, len)) = self.freetext_row_slot(entry, row_in_granule)? else {
                return Ok(None);
            };
            let bytes = self.freetext_row_bytes(entry, offset, len)?;
            let text = simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 {
                what: "freetext row value",
            })?;
            return Ok(Some(VariantValue::String(text.to_owned())));
        }
        self.field_value_in_group(group, column_id, row_in_granule)
    }

    pub fn payload_path(&self, row_ordinal: u64, path: &str) -> Result<Option<VariantValue>, FormatError> {
        let granule = self.granule_of_row(row_ordinal)?;
        let row_in_granule = row_ordinal - granule.first_row_ordinal;
        if let Some(entry) = self.footer.shredded.iter().find(|entry| entry.path == path)
            && let Some(value) = self.shredded_value_for_row(entry.column_id, granule, row_in_granule)?
        {
            return Ok(Some(value));
        }
        if let Some(entry) = self.footer.freetext.iter().find(|entry| entry.declared_field == path) {
            return self.freetext_value_for_row(entry.column_id, granule, row_in_granule);
        }
        if self.row_payload_flags(granule, row_in_granule)?
            & u64::from(crate::artifacts::batch::PAYLOAD_FLAG_EXTERNAL_REF)
            != 0
        {
            return Ok(None);
        }
        let payload = self.payload_granule(granule.granule_id)?;
        let Some((offset, len)) = self.residual_slot(payload, row_in_granule)? else {
            return Ok(None);
        };
        let bytes = self.residual_bytes(payload, offset, len)?;
        let dictionary = self.granule_dictionary(payload)?;
        let target = VariantRef::new(&bytes).get_path(&dictionary, &[PathSegment::Field(path)])?;
        target.map(|reference| reference.decode(&dictionary)).transpose()
    }

    pub fn read_payload_paths(
        &self,
        refs: &[PayloadRef],
        path: &str,
    ) -> Result<Vec<Option<VariantValue>>, FormatError> {
        let shredded_column = self
            .footer
            .shredded
            .iter()
            .find(|entry| entry.path == path)
            .map(|entry| entry.column_id);
        let freetext_column = self
            .footer
            .freetext
            .iter()
            .find(|entry| entry.declared_field == path)
            .map(|entry| entry.column_id);
        let mut order: Vec<usize> = (0..refs.len()).collect();
        order.sort_by_key(|&index| refs.get(index).map(|payload_ref| payload_ref.row_ordinal));
        let mut values = vec![None; refs.len()];
        let mut group: Option<PathGroup<'_>> = None;
        for (from, &index) in order.iter().enumerate() {
            let Some(row_ordinal) = refs.get(index).map(|payload_ref| payload_ref.row_ordinal) else {
                continue;
            };
            let in_group = group.as_ref().is_some_and(|group| {
                group.granule.first_row_ordinal <= row_ordinal
                    && group
                        .granule
                        .first_row_ordinal
                        .checked_add(u64::from(group.granule.row_count))
                        .is_some_and(|end| row_ordinal < end)
            });
            if !in_group {
                let granule = self.granule_of_row(row_ordinal)?;
                group = Some(PathGroup {
                    block: None,
                    freetext_column,
                    granule,
                    rows: batch_rows_in_granule(refs, &order, from, granule),
                    shredded_column,
                });
            }
            if let Some(group) = group.as_mut() {
                let row_in_granule = row_ordinal - group.granule.first_row_ordinal;
                let value = self.payload_path_in_granule(group, row_in_granule, path)?;
                if let Some(slot) = values.get_mut(index) {
                    *slot = value;
                }
            }
        }
        Ok(values)
    }

    fn payload_path_in_granule<'a>(
        &'a self,
        group: &mut PathGroup<'a>,
        row_in_granule: u64,
        path: &str,
    ) -> Result<Option<VariantValue>, FormatError> {
        if let Some(column_id) = group.shredded_column {
            let granule_id = group.granule.granule_id;
            let block = match &mut group.block {
                Some(block) => block,
                slot @ None => slot.insert(self.path_block(column_id, granule_id, &group.rows)?),
            };
            if let Some(value) = self.value_in_block(block, column_id, row_in_granule)? {
                return Ok(Some(value));
            }
        }
        if let Some(column_id) = group.freetext_column {
            return self.freetext_value_for_row(column_id, group.granule, row_in_granule);
        }
        if self.row_payload_flags(group.granule, row_in_granule)?
            & u64::from(crate::artifacts::batch::PAYLOAD_FLAG_EXTERNAL_REF)
            != 0
        {
            return Ok(None);
        }
        let payload = self.payload_granule(group.granule.granule_id)?;
        let Some((offset, len)) = self.residual_slot(payload, row_in_granule)? else {
            return Ok(None);
        };
        let bytes = self.residual_bytes(payload, offset, len)?;
        let dictionary = self.granule_dictionary(payload)?;
        let target = VariantRef::new(&bytes).get_path(&dictionary, &[PathSegment::Field(path)])?;
        target.map(|reference| reference.decode(&dictionary)).transpose()
    }

    fn path_block(&self, column_id: u32, granule_id: u32, rows: &[u64]) -> Result<PathBlock<'_>, FormatError> {
        let mark = self.mark(column_id, 0, granule_id)?;
        let addressable = match &mark {
            Some(mark) => {
                mark.page_count <= 1
                    && mark.compressed_size > 0
                    && mark.codec_pipeline_id.supports_byte_range_extraction()?
            }
            None => false,
        };
        if let Some(mark) = mark.as_ref().filter(|_| addressable) {
            let raw = self.read_column_raw_with_mark(granule_id, mark)?;
            let rank = PresenceRank::new(&raw.presence);
            let taken = if rows.len() > 1 {
                self.take_located_rows(&rank, &raw, column_id, rows)
            } else {
                None
            };
            return Ok(PathBlock::Located { rank, raw, taken });
        }
        Ok(PathBlock::Whole(self.cached_column(column_id, granule_id)?))
    }

    fn take_located_rows(
        &self,
        rank: &PresenceRank,
        raw: &RawColumnBlock<'_>,
        column_id: u32,
        rows: &[u64],
    ) -> Option<TakenRows> {
        let mut positions = Vec::with_capacity(rows.len());
        let slots: Vec<Option<usize>> = rows
            .iter()
            .map(|&row| {
                let row = row as usize;
                let position = if raw.presence.is_empty() {
                    Some(row)
                } else {
                    rank.position(&raw.presence, row)
                };
                position.map(|position| {
                    positions.push(position);
                    positions.len() - 1
                })
            })
            .collect();
        let data = take_block_rows_shared(raw.pipeline, &raw.body, self.shared_alphabet(column_id), &positions).ok()?;
        let values = slots
            .into_iter()
            .map(|slot| slot.and_then(|slot| Self::typed_variant_at(&data, slot)))
            .collect();
        Some(TakenRows {
            rows: rows.to_vec(),
            values,
        })
    }

    fn value_in_block(
        &self,
        block: &PathBlock<'_>,
        column_id: u32,
        row_in_granule: u64,
    ) -> Result<Option<VariantValue>, FormatError> {
        let (rank, raw) = match block {
            PathBlock::Located { rank, raw, taken } => {
                if let Some(taken) = taken
                    && let Ok(slot) = taken.rows.binary_search(&row_in_granule)
                    && let Some(value) = taken.values.get(slot)
                {
                    return Ok(value.clone());
                }
                (rank, raw)
            }
            PathBlock::Whole(column) => return Ok(column.value_at(row_in_granule)),
        };
        let row = row_in_granule as usize;
        let position = if raw.presence.is_empty() {
            row
        } else {
            match rank.position(&raw.presence, row) {
                Some(position) => position,
                None => return Ok(None),
            }
        };
        let data = decode_block_range_shared(
            raw.pipeline,
            &raw.body,
            self.shared_alphabet(column_id),
            position,
            position + 1,
        )?;
        Ok(Self::typed_variant_at(&data, 0))
    }

    pub fn rows_referencing(
        &self,
        kind: RelationshipKind,
        space: TargetIdSpace,
        target_ref: &[u8],
    ) -> Result<Vec<u64>, FormatError> {
        let column_id = kind.column_id();
        if !self.footer.columns.iter().any(|column| column.column_id == column_id) {
            return Ok(Vec::new());
        }
        let needle = format!("{}:{}", space.as_str(), hex_lower(target_ref));
        let finder = memmem::Finder::new(needle.as_bytes());
        let mut rows = Vec::new();
        for granule in &self.footer.granules {
            if !super::source_form::reference_may_be_in(&self.footer, column_id, granule.granule_id, &needle)? {
                continue;
            }
            let read = self.read_column(column_id, granule.granule_id)?;
            for_each_present_string(&read, granule.row_count as usize, |row, text| {
                let haystack = text.as_bytes();
                let whole = finder.find_iter(haystack).any(|at| {
                    let starts = at == 0 || haystack.get(at - 1) == Some(&b' ');
                    starts && haystack.get(at + needle.len()).is_none_or(|byte| *byte == b' ')
                });
                if whole {
                    rows.push(granule.first_row_ordinal + row as u64);
                }
            });
        }
        Ok(rows)
    }

    pub fn thread_rows(&self, space: TargetIdSpace, root_target: &[u8]) -> Result<Vec<u64>, FormatError> {
        self.rows_referencing(RelationshipKind::Root, space, root_target)
    }

    pub fn resolve_reference(&self, reference: &RelationshipRef) -> Result<Vec<u64>, FormatError> {
        match reference.space {
            TargetIdSpace::EventId => {
                let target: [u8; 16] = match reference.target_ref.as_slice().try_into() {
                    Ok(target) => target,
                    Err(_) => return Ok(Vec::new()),
                };
                let target = u128::from_be_bytes(target);
                let mut rows = Vec::new();
                for granule in &self.footer.granules {
                    let read = self.read_column(column_ids::EVENT_ID, granule.granule_id)?;
                    if let ColumnData::U128(values) = &read.data {
                        for (row, value) in values.iter().enumerate() {
                            if *value == target {
                                rows.push(granule.first_row_ordinal + row as u64);
                            }
                        }
                    }
                }
                Ok(rows)
            }
            TargetIdSpace::ExternalId => self.rows_with_external_id(&reference.target_ref),
            TargetIdSpace::ProtocolEventId => {
                if !self
                    .footer
                    .columns
                    .iter()
                    .any(|column| column.column_id == column_ids::PROTOCOL_EVENT_ID)
                {
                    return Ok(Vec::new());
                }
                let needle = hex_lower(&reference.target_ref);
                let mut rows = Vec::new();
                for granule in &self.footer.granules {
                    let read = self.read_column(column_ids::PROTOCOL_EVENT_ID, granule.granule_id)?;
                    for_each_present_string(&read, granule.row_count as usize, |row, text| {
                        if text == needle {
                            rows.push(granule.first_row_ordinal + row as u64);
                        }
                    });
                }
                Ok(rows)
            }
            TargetIdSpace::SubjectSequence => Ok(<[u8; 8]>::try_from(reference.target_ref.as_slice())
                .ok()
                .and_then(|target| self.graph_row_of(u64::from_be_bytes(target)))
                .into_iter()
                .collect()),
        }
    }
}

fn for_each_present_string(read: &ColumnRead, row_count: usize, mut visit: impl FnMut(usize, &str)) {
    let ColumnData::Strings(values) = &read.data else {
        return;
    };
    if values.len() == row_count {
        for (row, value) in values.iter().enumerate() {
            if let Some(text) = value {
                visit(row, text);
            }
        }
        return;
    }
    let mut dense = values.iter();
    for row in 0..row_count {
        let present = read
            .presence
            .get(row / 8)
            .is_some_and(|byte| byte & (1 << (row % 8)) != 0);
        if present && let Some(Some(text)) = dense.next() {
            visit(row, text);
        }
    }
}

#[cfg(test)]
#[path = "test/reader.rs"]
mod tests;
