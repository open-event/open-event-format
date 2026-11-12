use super::audit::{AuditSettings, audit_trail, refuse_audit_rewrite};
use super::constant::DEFAULT_TIME_SERIES_BUCKET_NANOS;
use super::source_form::{self, SourceFormIndexes};
use crate::artifacts::batch::{
    EncodedPayload, EventInput, PAYLOAD_FLAG_EXTERNAL_REF, PayloadInput, encode_variant_dictionary,
};
use crate::columns::{
    FreetextDeclaration, PROVENANCE_COLUMNS, PathStatistics, PromotionPlan, RELATIONSHIP_COLUMNS, REQUIRED_COLUMNS,
    SHARED_DICTIONARY_MAX_VALUES, UNION_SHARED_DICTIONARY_COLUMNS, UNION_SHARED_DICTIONARY_MAX_VALUES, column_ids,
    promoted_present, validate_promotion_plan,
};
use crate::deletes::SubjectId;
use crate::encoding::{
    BlockStats, CascadeStrategy, ColumnData, Compression, FsstTable, ReplayCapture, SideStream, StringColumn,
    Transform, ValueKind, count_set_bits, encode_block_replayed_with, encode_block_with_shared_dictionary,
    encode_block_with_xor_floats, encode_occurred_residual, seekable_zstd,
};
use crate::error::FormatError;
use crate::events::provenance::{SignedEventProvenance, hex_lower_into};
use crate::events::relationships::{EventRelationships, RelationshipKind};
use crate::events::variant::{
    EncodeScratch, KeyDictionary, VariantRef, VariantValue, encode_object_fields_into_with_scratch,
    encode_value_into_with_scratch,
};
use crate::events::{EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::file::bytes::Writer;
use crate::file::constant::{CHUNK_GROUP_BYTES, TREE_TRAILER_MAGIC};
use crate::indexes::ndv::NdvSketch;
use crate::indexes::probabilistic::SplitBlockBloomFilter;
use crate::indexes::text_token::TextTokenIndex;
use crate::invariants::EncodeExecutor;
use crate::invariants::io::ThreadPoolEncodeExecutor;
use crate::layout::footer::{
    ClusteringEntry, ColumnDescriptor, ColumnKind, ColumnMark, EntityHashFilterEntry, ExactCounts, FileDictionaries,
    Footer, FreetextEntry, FreetextRowOffsets, GranuleEntry, GraphBlockEntry, GraphDirectory, InboxRecipientEntry,
    MarksPageEntry, PackedSharedDictionary, PageDirectoryEntry, PageMinMax, PageStats, PayloadGranule, PresenceEntry,
    ResidualCompression, SharedDictionaryEntry, ShredEntry, SortDirection, SortednessProof, StripeEntry,
    StripeNdvEntry, StripeProofEntry, TextTokenEntry, TextTokenOffsetsEntry, encode_stripe_marks_pages,
};
use crate::layout::graph::{ChainCover, EventIdIndex, GraphInput, encode_graph_blocks, state_root_entries};
use crate::layout::reader::{OeFile, PayloadRead};
use crate::layout::snapshot::SnapshotKeyIndex;
use crate::layout::time_series::{FloatAggregate, RollupBucket, TimeSeriesRollups};
use crate::layout::{
    EMPTY_VALUE_ROW_OFFSET, HEADER_BLOCK_LEN, LayoutClass, LayoutTargets, MAX_PAGE_BYTES, OeHeader, Profile,
    encode_header, optional_features, required_features,
};
use crate::security::{AeadScheme, FooterEncryption};
use crate::writer::projection::{RowKey, RowOrder};
use hashbrown::{HashMap, HashSet};
use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BuildPhase {
    SourceDecoding,
    Normalization,
    DictionaryConstruction,
    PathStatistics,
    GranuleConstruction,
    BlockEncoding,
    Layout,
    FooterConstruction,
    Integrity,
    FileAssembly,
    RowDestruction,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildProfile {
    pub phases: Vec<(BuildPhase, u64)>,
}

struct PhaseClock<'a> {
    phase: BuildPhase,
    profile: Option<&'a mut BuildProfile>,
    started: Option<Instant>,
}

impl<'a> PhaseClock<'a> {
    fn new(profile: Option<&'a mut BuildProfile>, phase: BuildPhase) -> Self {
        let started = profile.as_ref().map(|_| Instant::now());
        Self {
            phase,
            profile,
            started,
        }
    }

    fn transition(&mut self, next: BuildPhase) {
        self.finish_current();
        self.phase = next;
        self.started = self.profile.as_ref().map(|_| Instant::now());
    }

    fn enabled(&self) -> bool {
        self.profile.is_some()
    }

    fn finish(mut self) {
        self.finish_current();
    }

    fn into_profile(mut self) -> Option<&'a mut BuildProfile> {
        self.finish_current();
        self.profile.take()
    }

    fn finish_current(&mut self) {
        if let (Some(profile), Some(started)) = (self.profile.as_deref_mut(), self.started.take()) {
            let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            if let Some((_, total)) = profile.phases.iter_mut().find(|(phase, _)| *phase == self.phase) {
                *total = total.saturating_add(elapsed);
            } else {
                profile.phases.push((self.phase, elapsed));
            }
        }
    }
}

#[inline(never)]
fn profile_granule_construction<T>(work: impl FnOnce() -> T) -> T {
    std::hint::black_box("oe_writer_granule_construction");
    let result = work();
    std::hint::black_box("oe_writer_granule_construction");
    result
}

#[inline(never)]
fn profile_block_encoding<T>(work: impl FnOnce() -> T) -> T {
    std::hint::black_box("oe_writer_block_encoding");
    let result = work();
    std::hint::black_box("oe_writer_block_encoding");
    result
}

macro_rules! worker_phase_signpost {
    ($function:ident, $name:literal) => {
        #[inline(never)]
        fn $function<T>(work: impl FnOnce() -> T) -> T {
            std::hint::black_box($name);
            let result = work();
            std::hint::black_box($name);
            result
        }
    };
}

worker_phase_signpost!(profile_integrity, "oe_writer_integrity");
worker_phase_signpost!(profile_layout, "oe_writer_layout");
worker_phase_signpost!(profile_footer_metadata, "oe_writer_footer_metadata");
worker_phase_signpost!(profile_path_statistics, "oe_writer_path_statistics");

#[inline(always)]
fn maybe_profile_granule_construction<T>(enabled: bool, work: impl FnOnce() -> T) -> T {
    if enabled {
        profile_granule_construction(work)
    } else {
        work()
    }
}

#[inline(always)]
fn maybe_profile_block_encoding<T>(enabled: bool, work: impl FnOnce() -> T) -> T {
    if enabled { profile_block_encoding(work) } else { work() }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OeRow {
    pub epoch: u64,
    pub event: EventInput,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OeRowChange<R = OeRow> {
    pub after: R,
    pub before: R,
    pub row_ordinal: u64,
}

pub type FieldName = Arc<str>;

#[derive(Clone, Debug, PartialEq)]
pub enum BuildPayload {
    Encoded(EncodedPayload),
    ExternalRef(String),
    None,
    Object(Vec<(FieldName, VariantValue)>),
    Whole(VariantValue),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BuildEnvelope {
    pub account_id: Option<String>,
    pub account_id_hash_low: u64,
    pub actor_id: Option<String>,
    pub actor_id_hash_low: u64,
    pub dedupe_hash_high: u64,
    pub dedupe_hash_low: u64,
    pub entity_id: Option<String>,
    pub entity_id_hash_high: u64,
    pub entity_id_hash_low: u64,
    pub entity_type: Arc<str>,
    pub event_id: EventId,
    pub event_type: Arc<str>,
    pub flags: EventFlags,
    pub ingested_at: TimestampValue,
    pub occurred_at: TimestampValue,
    pub schema_version: u32,
    pub source: Arc<str>,
    pub stream_id: StreamId,
    pub stream_sequence: u64,
    pub tenant_id: TenantId,
    pub trace_id_hash_low: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BuildRow {
    pub delivery: Option<Box<DeliveryLineage>>,
    pub envelope: BuildEnvelope,
    pub epoch: u64,
    pub external_id: Option<Vec<u8>>,
    pub payload: BuildPayload,
    pub provenance: Option<Box<SignedEventProvenance>>,
    pub raw_payload: Option<Vec<u8>>,
    pub relationships: Option<EventRelationships>,
    pub sequence: u64,
    pub subject: Option<SubjectId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveryLineage {
    pub connector_delivery_hash_high: u64,
    pub connector_delivery_hash_low: u64,
    pub source_delivery: Option<String>,
    pub source_schema: Option<String>,
}

impl DeliveryLineage {
    pub fn of(event: &EventInput) -> Option<Box<Self>> {
        let lineage = Self {
            connector_delivery_hash_high: event.connector_delivery_hash_high,
            connector_delivery_hash_low: event.connector_delivery_hash_low,
            source_delivery: event.source_delivery.clone(),
            source_schema: event.source_schema.clone(),
        };
        (lineage != Self::default()).then(|| Box::new(lineage))
    }
}

pub trait BuildInput: Clone + Send {
    fn into_build_rows(rows: Vec<Self>) -> Vec<BuildRow>;

    fn build_changes(changes: &[OeRowChange<Self>]) -> Cow<'_, [OeRowChange<BuildRow>]>;
}

impl BuildInput for BuildRow {
    fn into_build_rows(rows: Vec<Self>) -> Vec<BuildRow> {
        rows
    }

    fn build_changes(changes: &[OeRowChange<Self>]) -> Cow<'_, [OeRowChange<BuildRow>]> {
        Cow::Borrowed(changes)
    }
}

impl BuildInput for OeRow {
    fn into_build_rows(rows: Vec<Self>) -> Vec<BuildRow> {
        rows.into_par_iter()
            .map_init(SharedStrings::default, |shared, row| shared.build_row(row))
            .collect()
    }

    fn build_changes(changes: &[OeRowChange<Self>]) -> Cow<'_, [OeRowChange<BuildRow>]> {
        let mut shared = SharedStrings::default();
        Cow::Owned(
            changes
                .iter()
                .map(|change| OeRowChange {
                    after: shared.build_row(change.after.clone()),
                    before: shared.build_row(change.before.clone()),
                    row_ordinal: change.row_ordinal,
                })
                .collect(),
        )
    }
}

#[derive(Default)]
pub struct SharedStrings {
    strings: HashSet<Arc<str>>,
}

impl SharedStrings {
    pub fn share(&mut self, text: &str) -> Arc<str> {
        if let Some(shared) = self.strings.get(text) {
            return shared.clone();
        }
        let shared: Arc<str> = Arc::from(text);
        self.strings.insert(shared.clone());
        shared
    }

    pub fn build_row(&mut self, row: OeRow) -> BuildRow {
        let OeRow { epoch, event, sequence } = row;
        let delivery = DeliveryLineage::of(&event);
        let EventInput {
            envelope,
            payload,
            provenance,
            relationships,
            ..
        } = event;
        BuildRow {
            delivery,
            envelope: BuildEnvelope {
                account_id: envelope.account_id,
                account_id_hash_low: envelope.account_id_hash_low,
                actor_id: envelope.actor_id,
                actor_id_hash_low: envelope.actor_id_hash_low,
                dedupe_hash_high: envelope.dedupe_hash_high,
                dedupe_hash_low: envelope.dedupe_hash_low,
                entity_id: envelope.entity_id,
                entity_id_hash_high: envelope.entity_id_hash_high,
                entity_id_hash_low: envelope.entity_id_hash_low,
                entity_type: self.share(&envelope.entity_type),
                event_id: envelope.event_id,
                event_type: self.share(&envelope.event_type),
                flags: envelope.flags,
                ingested_at: envelope.ingested_at,
                occurred_at: envelope.occurred_at,
                schema_version: envelope.schema_version,
                source: self.share(&envelope.source),
                stream_id: envelope.stream_id,
                stream_sequence: envelope.stream_sequence,
                tenant_id: envelope.tenant_id,
                trace_id_hash_low: envelope.trace_id_hash_low,
            },
            epoch,
            external_id: None,
            payload: match payload {
                PayloadInput::Encoded(encoded) => BuildPayload::Encoded(encoded),
                PayloadInput::ExternalRef(reference) => BuildPayload::ExternalRef(reference),
                PayloadInput::None => BuildPayload::None,
                PayloadInput::Variant(value) => self.build_payload(value),
            },
            provenance: provenance.map(Box::new),
            raw_payload: None,
            relationships,
            sequence,
            subject: None,
        }
    }

    fn build_payload(&mut self, value: VariantValue) -> BuildPayload {
        match value {
            VariantValue::Object(fields) => BuildPayload::Object(
                fields
                    .into_iter()
                    .map(|(name, value)| (self.share(&name), value))
                    .collect(),
            ),
            whole => BuildPayload::Whole(whole),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnalyticalColumn {
    pub column_id: u32,
    pub data: ColumnData,
    pub internal_only: bool,
    pub kind: ColumnKind,
    pub name: String,
    pub substring_searchable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildLifecycle {
    FreshPublication,
    FreshPublicationWithoutTrailing,
    RewriteOrCompaction,
}

impl BuildLifecycle {
    fn cascade_strategy(self) -> CascadeStrategy {
        match self {
            BuildLifecycle::FreshPublication => CascadeStrategy::for_fresh_publication(),
            BuildLifecycle::FreshPublicationWithoutTrailing => CascadeStrategy::NoTrailing,
            BuildLifecycle::RewriteOrCompaction => CascadeStrategy::for_rewrite_or_compaction(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OeBuildConfig {
    pub analytical_columns: Vec<AnalyticalColumn>,
    pub audit: Option<AuditSettings>,
    pub created_at_physical: i64,
    pub footer_dek: Option<[u8; 32]>,
    pub footer_encryption: FooterEncryption,
    pub freetext: FreetextDeclaration,
    pub freetext_row_offset_index: bool,
    pub generation_id: u64,
    pub graph: Option<GraphInput>,
    pub io_alignment_bytes: u32,
    pub lifecycle: BuildLifecycle,
    pub page_size_rows: usize,
    pub profile: Profile,
    pub promotion: PromotionPlan,
    pub targets: LayoutTargets,
    pub tenant_id: TenantId,
    pub time_series_bucket_nanos: Option<i64>,
    pub delivery_lineage: bool,
}

const REPLAY_SEGMENT_GRANULES: usize = 16;

#[derive(Debug)]
pub struct BuiltOe {
    pub aliased_block_bytes: u64,
    pub encoded_blocks: u64,
    pub entity_summary: crate::indexes::entity_summary::EntitySummary,
    pub reused_source_block_bytes: u64,
    pub reused_source_blocks: u64,
    pub bytes: Vec<u8>,
    pub file_seal: [u8; 32],
    pub file_crc64_nvme: u64,
    pub file_id: u128,
    pub footer: Footer,
    pub footer_len: u64,
    pub header: OeHeader,
    pub total_len: u64,
    pub tree_len: Option<u64>,
}

#[derive(Clone)]
struct UpdateReuse<'a> {
    changed_columns_by_granule: HashMap<u32, HashSet<u32>>,
    changed_payload_granules: HashSet<u32>,
    path_statistics_changed: bool,
    payload_only: bool,
    source: &'a BuiltOe,
    sparse: Option<SparsePlan>,
}

impl UpdateReuse<'_> {
    fn block_changed(&self, granule_id: u32, column_id: u32) -> bool {
        self.changed_columns_by_granule.get(&granule_id).is_some_and(|columns| {
            columns.contains(&column_id)
                || (column_id == column_ids::INGESTED_AT && columns.contains(&column_ids::OCCURRED_AT))
        })
    }
}

#[derive(Clone)]
struct SparsePlan {
    build: Vec<Option<HashSet<u32>>>,
    rows_cover_file: bool,
}

impl SparsePlan {
    fn builds(&self, granule_index: usize) -> Option<&HashSet<u32>> {
        self.build.get(granule_index).and_then(Option::as_ref)
    }
}

fn static_routes_compatible(source: &Footer, config: &OeBuildConfig) -> bool {
    source
        .freetext
        .iter()
        .map(|entry| entry.declared_field.as_str())
        .eq(config.freetext.fields.iter().map(String::as_str))
        && source.presence.len() == config.promotion.columns.len()
        && config.promotion.columns.iter().enumerate().all(|(index, promoted)| {
            let column_id = column_ids::PROMOTED_BASE + index as u32;
            source.presence.iter().any(|entry| {
                entry.column_id == column_id && entry.since_schema_version == promoted.since_schema_version
            }) && source.columns.iter().any(|column| {
                column.column_id == column_id && column.name == promoted.name && column.kind == promoted.kind
            })
        })
        && source
            .columns
            .iter()
            .filter(|column| (column_ids::CONTEXT_BASE..column_ids::PROVENANCE_BASE).contains(&column.column_id))
            .count()
            == config.analytical_columns.len()
        && config.analytical_columns.iter().all(|analytical| {
            source.columns.iter().any(|column| {
                column.column_id == analytical.column_id
                    && column.name == analytical.name
                    && column.kind == analytical.kind
                    && column.internal_only == analytical.internal_only
            })
        })
}

fn source_shared_alphabets(footer: &Footer) -> Option<BTreeMap<u32, Vec<String>>> {
    let mut alphabets: BTreeMap<u32, Vec<String>> = footer
        .shared_dictionaries
        .iter()
        .map(|entry| (entry.column_id, entry.values.clone()))
        .collect();
    for entry in &footer.packed_shared_dictionaries {
        alphabets.insert(entry.column_id, entry.values().ok()?);
    }
    Some(alphabets)
}

fn union_alphabet<'a>(granule_values: Vec<Box<dyn Iterator<Item = &'a str> + Send + 'a>>) -> Option<Vec<String>> {
    fn code_bits(distinct: usize) -> u64 {
        u64::from(usize::BITS - distinct.saturating_sub(1).leading_zeros())
    }
    let mut union: ahash::AHashSet<&str> = ahash::AHashSet::new();
    let mut local_bits = 0u64;
    let mut rows = 0u64;
    let locals: Vec<(ahash::AHashSet<&str>, u64, u64)> = granule_values
        .into_par_iter()
        .map(|values| {
            let mut local: ahash::AHashSet<&str> = ahash::AHashSet::new();
            let mut present = 0u64;
            let mut text_bytes = 0u64;
            for value in values {
                present += 1;
                text_bytes += value.len() as u64;
                local.insert(value);
            }
            (local, present, text_bytes)
        })
        .collect();
    for (local, present, text_bytes) in locals {
        union.extend(local.iter().copied());
        if union.len() > UNION_SHARED_DICTIONARY_MAX_VALUES {
            return None;
        }
        let distinct_bytes: u64 = local.iter().map(|value| value.len() as u64).sum();
        let dictionary_bits = distinct_bytes * 4 + present * code_bits(local.len());
        local_bits += dictionary_bits.min(text_bytes * 4);
        rows += present;
    }
    if union.len() < 2 || union.iter().any(|value| value.len() > usize::from(u16::MAX)) {
        return None;
    }
    let union_bytes: u64 = union.iter().map(|value| value.len() as u64).sum();
    let shared_bits = rows * code_bits(union.len()) + union_bytes * 4;
    if shared_bits >= local_bits {
        return None;
    }
    let mut alphabet: Vec<String> = union.into_iter().map(str::to_owned).collect();
    alphabet.sort_unstable();
    Some(alphabet)
}

fn shareable_column(column_id: u32) -> bool {
    column_id < column_ids::FREETEXT_BASE || (column_ids::CONTEXT_BASE..column_ids::EMBEDDING_BASE).contains(&column_id)
}

fn plan_sparse_update(
    reuse: &UpdateReuse<'_>,
    config: &OeBuildConfig,
    carries_provenance: bool,
    carries_relationships: bool,
    rows_cover_file: bool,
) -> Option<SparsePlan> {
    let footer = &reuse.source.footer;
    let has_column = |column_id: u32| footer.columns.iter().any(|column| column.column_id == column_id);
    let hot_columns = column_ids::PROMOTED_BASE..column_ids::PROMOTED_BASE + config.promotion.columns.len() as u32;
    let file_wide_column = |column_id: u32| {
        hot_columns.contains(&column_id)
            || (shareable_column(column_id)
                && footer
                    .columns
                    .iter()
                    .any(|column| column.column_id == column_id && column.kind == ColumnKind::String))
    };
    if config.profile == Profile::TimeSeries
        || !reuse.payload_only
        || reuse.path_statistics_changed
        || !config.analytical_columns.is_empty()
        || !static_routes_compatible(footer, config)
        || carries_provenance != has_column(column_ids::AUTHOR_PUBKEY)
        || carries_relationships != has_column(column_ids::PARENT_REF)
        || footer.io_alignment_bytes != config.io_alignment_bytes
        || (!config.freetext.fields.is_empty()
            && config.freetext_row_offset_index == footer.freetext_row_offsets.is_empty())
        || reuse
            .changed_columns_by_granule
            .values()
            .flatten()
            .any(|&column_id| file_wide_column(column_id))
    {
        return None;
    }
    let granule_count = footer.granules.len();
    let mut expected_pages: HashMap<u32, u32> = HashMap::with_capacity(granule_count);
    for (index, granule) in footer.granules.iter().enumerate() {
        let row_count = granule.row_count as usize;
        let forced = row_count == config.targets.index_granularity
            || index + 1 == granule_count
            || footer
                .granules
                .get(index + 1)
                .is_some_and(|next| next.first_epoch != granule.last_epoch);
        if row_count > config.targets.index_granularity
            || (!forced && config.targets.index_granularity_bytes == usize::MAX)
        {
            return None;
        }
        let pages = if config.page_size_rows > 0 && row_count > config.page_size_rows {
            row_count.div_ceil(config.page_size_rows) as u32
        } else {
            1
        };
        expected_pages.insert(granule.granule_id, pages);
    }
    if footer
        .marks
        .iter()
        .any(|mark| mark.projection_id == 0 && expected_pages.get(&mark.granule_id).copied() != Some(mark.page_count))
    {
        return None;
    }
    let mut build: Vec<Option<HashSet<u32>>> = vec![None; granule_count];
    let mut granule_positions: HashMap<u32, usize> = HashMap::with_capacity(granule_count);
    for (index, granule) in footer.granules.iter().enumerate() {
        granule_positions.entry(granule.granule_id).or_insert(index);
    }
    for (granule_id, changed) in &reuse.changed_columns_by_granule {
        let index = *granule_positions.get(granule_id)?;
        let head = index - index % REPLAY_SEGMENT_GRANULES;
        for target in [index, head] {
            if let Some(slot) = build.get_mut(target) {
                slot.get_or_insert_default().extend(changed.iter().copied());
            }
        }
    }
    Some(SparsePlan { build, rows_cover_file })
}

struct SourceIndex<'a> {
    freetext_row_offsets: HashMap<(u32, u32), &'a FreetextRowOffsets>,
    granules: HashMap<u32, &'a GranuleEntry>,
    marks: HashMap<(u32, u32), &'a ColumnMark>,
    page_stats: HashMap<(u32, u32), &'a PageStats>,
    pages: HashMap<(u32, u32), Vec<&'a PageDirectoryEntry>>,
    payload_granules: HashMap<u32, &'a PayloadGranule>,
    source: &'a BuiltOe,
    stripe_offsets: HashMap<u32, u64>,
    text_token_indexes: HashMap<(u32, u32), Vec<&'a TextTokenEntry>>,
    text_token_offsets: HashMap<(u32, u32), Vec<&'a TextTokenOffsetsEntry>>,
}

impl<'a> SourceIndex<'a> {
    fn new(source: &'a BuiltOe) -> Self {
        let footer = &source.footer;
        let mut granules = HashMap::with_capacity(footer.granules.len());
        for granule in &footer.granules {
            granules.entry(granule.granule_id).or_insert(granule);
        }
        let mut stripe_offsets = HashMap::with_capacity(footer.stripes.len());
        for stripe in &footer.stripes {
            stripe_offsets.entry(stripe.stripe_id).or_insert(stripe.file_offset);
        }
        let mut marks: HashMap<(u32, u32), &ColumnMark> = HashMap::with_capacity(footer.marks.len());
        for mark in footer.marks.iter().filter(|mark| mark.projection_id == 0) {
            marks.entry((mark.column_id, mark.granule_id)).or_insert(mark);
        }
        let mut page_stats = HashMap::with_capacity(footer.page_stats.len());
        for stats in &footer.page_stats {
            page_stats.entry((stats.column_id, stats.granule_id)).or_insert(stats);
        }
        let mut pages: HashMap<(u32, u32), Vec<&PageDirectoryEntry>> = HashMap::default();
        for page in footer.page_directory.iter().filter(|page| page.projection_id == 0) {
            let key = (page.column_id, page.granule_id);
            if marks.get(&key).is_some_and(|mark| mark.page_count > 1) {
                pages.entry(key).or_default().push(page);
            }
        }
        for entries in pages.values_mut() {
            entries.sort_by_key(|page| page.page_index);
        }
        let mut payload_granules = HashMap::with_capacity(footer.payload_granules.len());
        for payload in &footer.payload_granules {
            payload_granules.entry(payload.granule_id).or_insert(payload);
        }
        let mut freetext_row_offsets = HashMap::with_capacity(footer.freetext_row_offsets.len());
        for index in &footer.freetext_row_offsets {
            freetext_row_offsets
                .entry((index.column_id, index.granule_id))
                .or_insert(index);
        }
        let mut text_token_indexes: HashMap<(u32, u32), Vec<&TextTokenEntry>> = HashMap::default();
        for token in &footer.text_token_indexes {
            text_token_indexes
                .entry((token.column_id, token.granule_id))
                .or_default()
                .push(token);
        }
        let mut text_token_offsets: HashMap<(u32, u32), Vec<&TextTokenOffsetsEntry>> = HashMap::default();
        for token in &footer.text_token_offsets {
            text_token_offsets
                .entry((token.column_id, token.granule_id))
                .or_default()
                .push(token);
        }
        Self {
            freetext_row_offsets,
            granules,
            marks,
            page_stats,
            pages,
            payload_granules,
            source,
            stripe_offsets,
            text_token_indexes,
            text_token_offsets,
        }
    }

    fn extent(&self, granule_id: u32, offset: u64, len: u64) -> Option<&'a [u8]> {
        let granule = self.granules.get(&granule_id)?;
        let stripe_offset = *self.stripe_offsets.get(&granule.stripe_id)?;
        let start = usize::try_from(stripe_offset.checked_add(offset)?).ok()?;
        let end = start.checked_add(usize::try_from(len).ok()?)?;
        self.source.bytes.get(start..end)
    }
}

fn variant_value_bytes(value: &VariantValue) -> usize {
    match value {
        VariantValue::Array(items) => items.iter().map(variant_value_bytes).sum(),
        VariantValue::Binary(bytes) => bytes.len(),
        VariantValue::Object(fields) => fields.values().map(variant_value_bytes).sum(),
        VariantValue::String(text) => text.len(),
        VariantValue::Decimal { .. } | VariantValue::Uuid(_) => 16,
        _ => 8,
    }
}

fn collect_key_names<'a>(value: &'a VariantValue, out: &mut Vec<&'a str>) {
    match value {
        VariantValue::Array(items) => {
            for item in items {
                collect_key_names(item, out);
            }
        }
        VariantValue::Object(fields) => {
            for (key, field_value) in fields {
                out.push(key.as_str());
                collect_key_names(field_value, out);
            }
        }
        _ => {}
    }
}

const NON_OBJECT_FIELD_SHAPE: u32 = u32::MAX;

const RECENT_SHAPES: usize = 4;

const ENTITY_HASH_FILTER_BITS_PER_KEY: u32 = 10;

const MARKS_PAGE_BYTES_PER_BLOCK: usize = 512;

const PROOF_APPENDIX_TRAILER_BYTES: usize = 8 + TREE_TRAILER_MAGIC.len();

const PADDED_REGIONS_PER_GRANULE: usize = 4;

const RESIDUAL_ROW_FRAMING_BYTES: usize = 4;

const MIN_METADATA_GRANULES_PER_JOB: usize = 4;

const SHREDDED_COLUMNS_MAX: usize = (column_ids::FREETEXT_BASE - column_ids::SHREDDED_BASE) as usize;

#[derive(Default)]
struct FieldIdentityCatalog {
    ids: HashMap<FieldName, u32>,
    names: Vec<FieldName>,
    recent_shapes: Vec<u32>,
    scratch_field_ids: Vec<u32>,
    shape_ids: HashMap<Box<[u32]>, u32>,
    shapes: Vec<Box<[u32]>>,
}

impl FieldIdentityCatalog {
    fn field_id(&self, path: &str) -> Option<u32> {
        self.ids.get(path).copied()
    }

    fn field_names(&self) -> &[FieldName] {
        &self.names
    }

    fn shape(&self, shape_id: u32) -> &[u32] {
        self.shapes.get(shape_id as usize).map(Box::as_ref).unwrap_or_default()
    }

    fn same_shape(&self, shape_id: u32, fields: &[(FieldName, VariantValue)]) -> bool {
        let ids = self.shape(shape_id);
        ids.len() == fields.len()
            && ids.iter().zip(fields).all(|(&field_id, (path, _))| {
                self.names
                    .get(field_id as usize)
                    .is_some_and(|known| Arc::ptr_eq(known, path) || **known == **path)
            })
    }

    fn intern_object(&mut self, fields: &[(FieldName, VariantValue)]) -> Result<u32, FormatError> {
        if let Some(position) = self
            .recent_shapes
            .iter()
            .position(|&shape_id| self.same_shape(shape_id, fields))
        {
            let shape_id = self.recent_shapes.remove(position);
            self.recent_shapes.insert(0, shape_id);
            return Ok(shape_id);
        }

        let mut field_ids = std::mem::take(&mut self.scratch_field_ids);
        let interned = self.intern_field_ids(fields, &mut field_ids);
        self.scratch_field_ids = field_ids;
        let shape_id = interned?;
        self.recent_shapes.truncate(RECENT_SHAPES - 1);
        self.recent_shapes.insert(0, shape_id);
        Ok(shape_id)
    }

    fn intern_field_ids(
        &mut self,
        fields: &[(FieldName, VariantValue)],
        field_ids: &mut Vec<u32>,
    ) -> Result<u32, FormatError> {
        field_ids.clear();
        field_ids.reserve(fields.len());
        let mut previous: Option<&str> = None;
        for (path, _) in fields {
            if previous.is_some_and(|previous| previous >= &**path) {
                return Err(FormatError::Structural {
                    rule: "a payload object's fields are in ascending key order with no key repeated",
                });
            }
            previous = Some(path);
            let field_id = self.intern_name(path)?;
            field_ids.push(field_id);
        }
        self.intern_shape(field_ids)
    }

    fn intern_name(&mut self, name: &FieldName) -> Result<u32, FormatError> {
        if let Some(&field_id) = self.ids.get(&**name) {
            return Ok(field_id);
        }
        let field_id = u32::try_from(self.names.len()).map_err(|_| FormatError::Structural {
            rule: "a OE file cannot intern more than u32::MAX payload fields",
        })?;
        self.names.push(name.clone());
        self.ids.insert(name.clone(), field_id);
        Ok(field_id)
    }

    fn intern_shape(&mut self, field_ids: &[u32]) -> Result<u32, FormatError> {
        if let Some(&shape_id) = self.shape_ids.get(field_ids) {
            return Ok(shape_id);
        }
        let shape_id = u32::try_from(self.shapes.len()).map_err(|_| FormatError::Structural {
            rule: "a OE file cannot intern more than u32::MAX payload shapes",
        })?;
        let shape: Box<[u32]> = field_ids.into();
        self.shape_ids.insert(shape.clone(), shape_id);
        self.shapes.push(shape);
        Ok(shape_id)
    }
}

struct DictionaryIndex<'a> {
    entity_type: HashMap<&'a str, u64>,
    event_type: HashMap<&'a str, u64>,
    source: HashMap<&'a str, u64>,
}

fn index_dictionary(values: &[String]) -> HashMap<&str, u64> {
    values
        .iter()
        .enumerate()
        .map(|(id, value)| (value.as_str(), id as u64))
        .collect()
}

const DICTIONARY_SAMPLE_ROWS: usize = 1_024;

const DICTIONARY_SAMPLE_MIN_REPEAT: usize = 2;

fn observe_dictionary_values<'a>(sets: &mut [HashSet<&'a str>; 3], row: &'a BuildRow) {
    let [entity_type, event_type, source] = sets;
    let envelope = &row.envelope;
    entity_type.insert(&envelope.entity_type);
    event_type.insert(&envelope.event_type);
    source.insert(&envelope.source);
}

fn build_file_dictionaries(rows: &[BuildRow]) -> FileDictionaries {
    let sample_rows = rows.len().min(DICTIONARY_SAMPLE_ROWS);
    let mut sample = <[HashSet<&str>; 3]>::default();
    for row in rows.iter().take(sample_rows) {
        observe_dictionary_values(&mut sample, row);
    }
    let lists: [Vec<&str>; 3] = if sample
        .iter()
        .all(|set| set.len() * DICTIONARY_SAMPLE_MIN_REPEAT <= sample_rows)
    {
        rows.par_iter()
            .fold(<[HashSet<&str>; 3]>::default, |mut sets, row| {
                observe_dictionary_values(&mut sets, row);
                sets
            })
            .reduce(<[HashSet<&str>; 3]>::default, |mut merged, sets| {
                for (merged, set) in merged.iter_mut().zip(sets) {
                    merged.extend(set);
                }
                merged
            })
            .map(|set| set.into_iter().collect())
    } else {
        [
            rows.iter().map(|row| &*row.envelope.entity_type).collect(),
            rows.iter().map(|row| &*row.envelope.event_type).collect(),
            rows.iter().map(|row| &*row.envelope.source).collect(),
        ]
    };
    let [entity_type, event_type, source] = lists.map(|mut list| {
        list.par_sort_unstable();
        list.dedup();
        list.into_iter().map(str::to_owned).collect()
    });
    FileDictionaries {
        entity_type,
        event_type,
        source,
    }
}

fn build_dictionary_index(dictionaries: &FileDictionaries) -> DictionaryIndex<'_> {
    DictionaryIndex {
        entity_type: index_dictionary(&dictionaries.entity_type),
        event_type: index_dictionary(&dictionaries.event_type),
        source: index_dictionary(&dictionaries.source),
    }
}

fn dictionary_id(index: &HashMap<&str, u64>, value: &str) -> u64 {
    index.get(value).copied().unwrap_or(0)
}

struct PendingBlock {
    column_id: u32,
    data: ColumnData,
    presence: Vec<u8>,
    random_access: bool,
}

const NORMALIZE_BATCH_ROWS: usize = 2_048;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NormalizedPayload {
    ExternalRef,
    None,
    Object { batch: u32, shape_id: u32, start: usize },
    Whole { batch: u32, start: usize },
}

struct NormalizedPayloads {
    identities: FieldIdentityCatalog,
    rows: Vec<NormalizedPayload>,
    values: Vec<Vec<VariantValue>>,
}

struct NormalizedBatch {
    identities: FieldIdentityCatalog,
    rows: Vec<NormalizedPayload>,
    values: Vec<VariantValue>,
}

impl NormalizedPayloads {
    fn normalize(rows: &mut [BuildRow], ranges: &[(usize, usize)]) -> Result<Self, FormatError> {
        let row_count = rows.len();
        let mut batch_rows: Vec<(usize, &mut [BuildRow])> = Vec::new();
        let mut remaining: &mut [BuildRow] = rows;
        let mut consumed = 0usize;
        for &(start, end) in ranges {
            let rest = std::mem::take(&mut remaining);
            let (_, rest) = rest.split_at_mut(start.saturating_sub(consumed).min(rest.len()));
            let (range_rows, rest) = rest.split_at_mut(end.saturating_sub(start).min(rest.len()));
            remaining = rest;
            consumed = end;
            for (offset, chunk) in range_rows.chunks_mut(NORMALIZE_BATCH_ROWS).enumerate() {
                batch_rows.push((start + offset * NORMALIZE_BATCH_ROWS, chunk));
            }
        }
        let batches: Vec<(usize, Result<NormalizedBatch, FormatError>)> = batch_rows
            .into_par_iter()
            .map(|(first_row, chunk)| (first_row, NormalizedBatch::normalize(chunk)))
            .collect();
        let mut normalized = Self {
            identities: FieldIdentityCatalog::default(),
            rows: Vec::with_capacity(row_count),
            values: Vec::with_capacity(batches.len()),
        };
        let mut batch_field_ids: Vec<u32> = Vec::new();
        let mut shape_field_ids: Vec<u32> = Vec::new();
        for (index, (first_row, batch)) in batches.into_iter().enumerate() {
            let batch = batch?;
            let batch_index = u32::try_from(index).map_err(|_| FormatError::Structural {
                rule: "a OE file cannot normalize more than u32::MAX row batches",
            })?;
            batch_field_ids.clear();
            for name in batch.identities.field_names() {
                batch_field_ids.push(normalized.identities.intern_name(name)?);
            }
            let mut batch_shape_ids: Vec<u32> = Vec::with_capacity(batch.identities.shapes.len());
            for shape in &batch.identities.shapes {
                shape_field_ids.clear();
                shape_field_ids.extend(
                    shape
                        .iter()
                        .map(|&field_id| batch_field_ids.get(field_id as usize).copied().unwrap_or_default()),
                );
                batch_shape_ids.push(normalized.identities.intern_shape(&shape_field_ids)?);
            }
            normalized.rows.resize(first_row, NormalizedPayload::None);
            normalized.rows.extend(batch.rows.iter().map(|entry| match *entry {
                NormalizedPayload::Object { shape_id, start, .. } => NormalizedPayload::Object {
                    batch: batch_index,
                    shape_id: batch_shape_ids.get(shape_id as usize).copied().unwrap_or_default(),
                    start,
                },
                NormalizedPayload::Whole { start, .. } => NormalizedPayload::Whole {
                    batch: batch_index,
                    start,
                },
                carried => carried,
            }));
            normalized.values.push(batch.values);
        }
        normalized.rows.resize(row_count, NormalizedPayload::None);
        Ok(normalized)
    }

    fn restore(self, rows: &mut [BuildRow]) {
        let Self {
            identities,
            rows: entries,
            values,
        } = self;
        let mut arenas: Vec<std::vec::IntoIter<VariantValue>> = values.into_iter().map(Vec::into_iter).collect();
        for (row, entry) in rows.iter_mut().zip(entries) {
            let payload = match entry {
                NormalizedPayload::Object { batch, shape_id, .. } => {
                    let Some(arena) = arenas.get_mut(batch as usize) else {
                        continue;
                    };
                    let fields = identities
                        .shape(shape_id)
                        .iter()
                        .map(|&field_id| identities.names.get(field_id as usize).cloned().unwrap_or_default())
                        .zip(arena.by_ref())
                        .collect();
                    BuildPayload::Object(fields)
                }
                NormalizedPayload::Whole { batch, .. } => {
                    let Some(value) = arenas.get_mut(batch as usize).and_then(Iterator::next) else {
                        continue;
                    };
                    BuildPayload::Whole(value)
                }
                NormalizedPayload::ExternalRef | NormalizedPayload::None => continue,
            };
            row.payload = payload;
        }
    }

    fn identities(&self) -> &FieldIdentityCatalog {
        &self.identities
    }

    fn payload(&self, row_index: usize) -> NormalizedPayload {
        self.rows.get(row_index).copied().unwrap_or(NormalizedPayload::None)
    }

    fn shape_id(&self, payload: NormalizedPayload) -> u32 {
        match payload {
            NormalizedPayload::Object { shape_id, .. } => shape_id,
            _ => NON_OBJECT_FIELD_SHAPE,
        }
    }

    fn field_ids(&self, payload: NormalizedPayload) -> &[u32] {
        self.identities.shape(self.shape_id(payload))
    }

    fn field_names(&self, payload: NormalizedPayload) -> impl Iterator<Item = &str> {
        self.field_ids(payload)
            .iter()
            .map(|&field_id| self.field_name(field_id))
    }

    fn field_name(&self, field_id: u32) -> &str {
        self.identities
            .field_names()
            .get(field_id as usize)
            .map_or("", |name| name)
    }

    fn values(&self, payload: NormalizedPayload) -> &[VariantValue] {
        let (batch, start, len) = match payload {
            NormalizedPayload::ExternalRef | NormalizedPayload::None => return &[],
            NormalizedPayload::Object { batch, shape_id, start } => {
                (batch, start, self.identities.shape(shape_id).len())
            }
            NormalizedPayload::Whole { batch, start } => (batch, start, 1),
        };
        self.values
            .get(batch as usize)
            .and_then(|arena| arena.get(start..start + len))
            .unwrap_or_default()
    }

    fn range(&self, start: usize, row_count: usize) -> &[NormalizedPayload] {
        self.rows.get(start..start + row_count).unwrap_or_default()
    }

    fn collect_key_names_into<'a>(&'a self, payload: NormalizedPayload, out: &mut Vec<&'a str>) {
        let mut names = self.field_names(payload);
        for value in self.values(payload) {
            if let Some(name) = names.next() {
                out.push(name);
            }
            collect_key_names(value, out);
        }
    }

    fn release(self) {
        self.values.into_par_iter().for_each(drop);
    }
}

impl NormalizedBatch {
    fn normalize(rows: &mut [BuildRow]) -> Result<Self, FormatError> {
        let value_count: usize = rows
            .iter()
            .map(|row| match &row.payload {
                BuildPayload::Object(fields) => fields.len(),
                BuildPayload::Encoded(_) | BuildPayload::Whole(_) => 1,
                BuildPayload::ExternalRef(_) | BuildPayload::None => 0,
            })
            .sum();
        let mut batch = Self {
            identities: FieldIdentityCatalog::default(),
            rows: Vec::with_capacity(rows.len()),
            values: Vec::with_capacity(value_count),
        };
        let mut shared = SharedStrings::default();
        for row in rows {
            let payload = match std::mem::replace(&mut row.payload, BuildPayload::None) {
                BuildPayload::Encoded(encoded) => {
                    let value = VariantRef::new(&encoded.bytes);
                    match value.decode_object_fields(&encoded.dictionary)? {
                        Some(fields) => BuildPayload::Object(
                            fields
                                .into_iter()
                                .map(|(name, value)| (shared.share(name), value))
                                .collect(),
                        ),
                        None => BuildPayload::Whole(value.decode(&encoded.dictionary)?),
                    }
                }
                payload => payload,
            };
            let start = batch.values.len();
            let entry = match payload {
                BuildPayload::Object(fields) => {
                    let shape_id = batch.identities.intern_object(&fields)?;
                    batch.values.extend(fields.into_iter().map(|(_, value)| value));
                    NormalizedPayload::Object {
                        batch: 0,
                        shape_id,
                        start,
                    }
                }
                BuildPayload::Whole(whole) => {
                    batch.values.push(whole);
                    NormalizedPayload::Whole { batch: 0, start }
                }
                BuildPayload::ExternalRef(reference) => {
                    row.payload = BuildPayload::ExternalRef(reference);
                    NormalizedPayload::ExternalRef
                }
                BuildPayload::Encoded(_) | BuildPayload::None => NormalizedPayload::None,
            };
            batch.rows.push(entry);
        }
        Ok(batch)
    }
}

enum Residual<'a> {
    Fields(Range<usize>),
    Whole(&'a VariantValue),
}

#[derive(Default)]
struct FieldRoute {
    freetext_index: Option<usize>,
    promoted_indices: Vec<usize>,
    shred_index: Option<usize>,
}

fn build_field_routes<'a>(
    identities: &FieldIdentityCatalog,
    promotion: &'a PromotionPlan,
    shred_plan: &'a [ShredEntry],
    freetext: &'a [FreetextEntry],
) -> Vec<FieldRoute> {
    let mut routes: Vec<FieldRoute> = (0..identities.names.len()).map(|_| FieldRoute::default()).collect();
    for (index, promoted) in promotion.columns.iter().enumerate() {
        if let Some(route) = identities
            .field_id(promoted.path.as_str())
            .and_then(|field_id| routes.get_mut(field_id as usize))
        {
            route.promoted_indices.push(index);
        }
    }
    for (index, entry) in shred_plan.iter().enumerate() {
        if let Some(route) = identities
            .field_id(entry.path.as_str())
            .and_then(|field_id| routes.get_mut(field_id as usize))
        {
            route.shred_index.get_or_insert(index);
        }
    }
    for (index, entry) in freetext.iter().enumerate() {
        if let Some(route) = identities
            .field_id(entry.declared_field.as_str())
            .and_then(|field_id| routes.get_mut(field_id as usize))
        {
            route.freetext_index.get_or_insert(index);
        }
    }
    routes
}

impl FieldRoute {
    fn routes_anywhere(&self) -> bool {
        self.freetext_index.is_some() || self.shred_index.is_some() || !self.promoted_indices.is_empty()
    }
}

const FIELD_STAYS_RESIDUAL: u32 = u32::MAX;

struct ShapeRoutes {
    by_shape: Vec<Box<[u32]>>,
    residual_fields_by_shape: Vec<usize>,
}

impl ShapeRoutes {
    fn field_routes(&self, shape_id: u32) -> &[u32] {
        self.by_shape
            .get(shape_id as usize)
            .map(Box::as_ref)
            .unwrap_or_default()
    }

    fn residual_fields(&self, shape_id: u32) -> usize {
        self.residual_fields_by_shape
            .get(shape_id as usize)
            .copied()
            .unwrap_or_default()
    }
}

fn build_shape_routes(identities: &FieldIdentityCatalog, routes: &[FieldRoute]) -> ShapeRoutes {
    ShapeRoutes {
        by_shape: identities
            .shapes
            .iter()
            .map(|shape| {
                shape
                    .iter()
                    .map(|&field_id| {
                        if routes.get(field_id as usize).is_some_and(FieldRoute::routes_anywhere) {
                            field_id
                        } else {
                            FIELD_STAYS_RESIDUAL
                        }
                    })
                    .collect()
            })
            .collect(),
        residual_fields_by_shape: identities
            .shapes
            .iter()
            .map(|shape| {
                shape
                    .iter()
                    .filter(|&&field_id| {
                        routes
                            .get(field_id as usize)
                            .is_none_or(|route| route.shred_index.is_none() && route.freetext_index.is_none())
                    })
                    .count()
            })
            .collect(),
    }
}

struct GranulePieces {
    blocks: Vec<PendingBlock>,
    by_entity_type: Vec<u64>,
    by_event_type: Vec<u64>,
    by_source: Vec<u64>,
    dictionary_bytes: Vec<u8>,
    entry: GranuleEntry,
    freetext_row_offsets: Vec<(u32, Vec<u8>, Vec<u8>)>,
    elided_entity_hashes: Vec<u64>,
    has_internal_late_event: bool,
    offsets_bytes: Vec<u8>,
    residual_bytes: Vec<u8>,
}

impl GranulePieces {
    fn borrowed(entry: GranuleEntry) -> Self {
        Self {
            blocks: Vec::new(),
            by_entity_type: Vec::new(),
            by_event_type: Vec::new(),
            by_source: Vec::new(),
            dictionary_bytes: Vec::new(),
            elided_entity_hashes: Vec::new(),
            entry,
            freetext_row_offsets: Vec::new(),
            has_internal_late_event: false,
            offsets_bytes: Vec::new(),
            residual_bytes: Vec::new(),
        }
    }
}

#[derive(Default)]
struct PayloadByteEstimator<'a> {
    key_scratch: Vec<&'a str>,
    previous_distinct_keys: usize,
    previous_key_names: Vec<&'a str>,
}

impl<'a> PayloadByteEstimator<'a> {
    fn estimate(&mut self, payloads: &'a NormalizedPayloads, entry: NormalizedPayload, row: &BuildRow) -> usize {
        match entry {
            NormalizedPayload::ExternalRef => match &row.payload {
                BuildPayload::ExternalRef(reference) => reference.len(),
                _ => 0,
            },
            NormalizedPayload::None => 0,
            NormalizedPayload::Object { .. } | NormalizedPayload::Whole { .. } => {
                self.key_scratch.clear();
                payloads.collect_key_names_into(entry, &mut self.key_scratch);
                if self.key_scratch != self.previous_key_names {
                    self.previous_key_names.clear();
                    self.previous_key_names.extend_from_slice(&self.key_scratch);
                    self.key_scratch.sort_unstable();
                    self.key_scratch.dedup();
                    self.previous_distinct_keys = self.key_scratch.len();
                }
                64 + self.previous_distinct_keys * 16
                    + payloads.values(entry).iter().map(variant_value_bytes).sum::<usize>()
            }
        }
    }
}

fn granule_cut_reproduces(
    rows: &[BuildRow],
    payloads: &NormalizedPayloads,
    first: usize,
    targets: &LayoutTargets,
    byte_cut: bool,
) -> bool {
    if targets.index_granularity_bytes == usize::MAX {
        return true;
    }
    let mut estimator = PayloadByteEstimator::default();
    let mut payload_bytes = 0usize;
    for (index, row) in rows.iter().enumerate() {
        payload_bytes =
            payload_bytes.saturating_add(estimator.estimate(payloads, payloads.payload(first + index), row));
        let reached = payload_bytes >= targets.index_granularity_bytes;
        let last = index + 1 == rows.len();
        if reached != last && (reached || byte_cut) {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone)]
enum TypedColumn {
    Decimal(Vec<(i128, u8)>),
    Empty,
    F64(Vec<f64>),
    I64(Vec<i64>),
    Strings(StringColumn),
}

impl TypedColumn {
    fn for_kind(kind: ColumnKind) -> Self {
        match kind {
            ColumnKind::Decimal => TypedColumn::Decimal(Vec::new()),
            ColumnKind::F64 => TypedColumn::F64(Vec::new()),
            ColumnKind::I64 => TypedColumn::I64(Vec::new()),
            ColumnKind::String => TypedColumn::Strings(StringColumn::new()),
            ColumnKind::U128 | ColumnKind::U64 | ColumnKind::Vector => TypedColumn::Empty,
        }
    }

    fn push_variant(&mut self, value: &VariantValue) -> bool {
        match (self, value) {
            (TypedColumn::I64(values), VariantValue::Int(v)) => values.push(*v),
            (TypedColumn::I64(values), VariantValue::Timestamp(v)) => values.push(v.physical_nanos()),
            (TypedColumn::F64(values), VariantValue::Double(v)) => values.push(*v),
            (TypedColumn::F64(values), VariantValue::Float(v)) => values.push(f64::from(*v)),
            (TypedColumn::Decimal(values), VariantValue::Decimal { unscaled, scale }) => {
                values.push((*unscaled, *scale))
            }
            (TypedColumn::Strings(values), VariantValue::String(v)) => values.push(Some(v)),
            _ => return false,
        }
        true
    }

    fn push_variant_shred(&mut self, value: &VariantValue, column_scale: &mut Option<u8>) -> bool {
        match (self, value) {
            (TypedColumn::I64(values), VariantValue::Int(v)) => values.push(*v),
            (TypedColumn::F64(values), VariantValue::Double(v)) => values.push(*v),
            (TypedColumn::Decimal(values), VariantValue::Decimal { unscaled, scale }) => match column_scale {
                Some(established) if *established != *scale => return false,
                _ => {
                    *column_scale = Some(*scale);
                    values.push((*unscaled, *scale));
                }
            },
            (TypedColumn::Strings(values), VariantValue::String(v)) => values.push(Some(v)),
            _ => return false,
        }
        true
    }

    fn into_column_data(self) -> Result<ColumnData, FormatError> {
        match self {
            TypedColumn::Decimal(values) => {
                let scale = values.iter().map(|(_, scale)| *scale).max().unwrap_or(0);
                let rescaled = values
                    .iter()
                    .map(|(unscaled, value_scale)| {
                        let lift = u32::from(scale.saturating_sub(*value_scale));
                        10i128
                            .checked_pow(lift)
                            .and_then(|factor| unscaled.checked_mul(factor))
                            .ok_or(FormatError::Structural {
                                rule: "decimal value overflows i128 when rescaled to the column scale",
                            })
                    })
                    .collect::<Result<Vec<i128>, FormatError>>()?;
                Ok(ColumnData::Decimal {
                    values: rescaled,
                    scale,
                })
            }
            TypedColumn::Empty => Ok(ColumnData::Strings(StringColumn::new())),
            TypedColumn::F64(values) => Ok(ColumnData::F64(values)),
            TypedColumn::I64(values) => Ok(ColumnData::I64(values)),
            TypedColumn::Strings(values) => Ok(ColumnData::Strings(values)),
        }
    }

    fn into_shredded_column_data(self) -> Result<ColumnData, FormatError> {
        let TypedColumn::Decimal(values) = self else {
            return self.into_column_data();
        };
        let mut scale: Option<u8> = None;
        let unscaled = values
            .iter()
            .map(|(unscaled, value_scale)| match scale {
                Some(established) if established != *value_scale => Err(FormatError::Structural {
                    rule: "a shredded decimal column stores every value at one scale",
                }),
                _ => {
                    scale = Some(*value_scale);
                    Ok(*unscaled)
                }
            })
            .collect::<Result<Vec<i128>, FormatError>>()?;
        Ok(ColumnData::Decimal {
            scale: scale.unwrap_or(0),
            values: unscaled,
        })
    }
}

fn slice_presence_bits(full: &[u8], start: usize, len: usize) -> Vec<u8> {
    let byte_count = len.div_ceil(8);
    let mut result = vec![0u8; byte_count];
    for i in 0..len {
        let src_idx = start + i;
        if full.get(src_idx / 8).is_some_and(|b| b & (1 << (src_idx % 8)) != 0)
            && let Some(byte) = result.get_mut(i / 8)
        {
            *byte |= 1 << (i % 8);
        }
    }
    result
}

fn aggregate_stats(stats: impl IntoIterator<Item = BlockStats>) -> BlockStats {
    let mut null_count = 0u32;
    let mut row_count = 0u32;
    let mut min_i128: Option<i128> = None;
    let mut max_i128: Option<i128> = None;
    let mut min_f64: Option<f64> = None;
    let mut max_f64: Option<f64> = None;
    for s in stats {
        null_count += s.null_count;
        row_count += s.row_count;
        min_i128 = match (min_i128, s.min_i128) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, v) | (v, None) => v,
        };
        max_i128 = match (max_i128, s.max_i128) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, v) | (v, None) => v,
        };
        min_f64 = match (min_f64, s.min_f64) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, v) | (v, None) => v,
        };
        max_f64 = match (max_f64, s.max_f64) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, v) | (v, None) => v,
        };
    }
    BlockStats {
        max_f64,
        max_i128,
        min_f64,
        min_i128,
        null_count,
        row_count,
    }
}

fn seal_footer(dek: &[u8; 32], file_id: u128, footer_blob: &[u8]) -> Result<Vec<u8>, FormatError> {
    let scheme = AeadScheme::AesGcm256;
    let mut hasher = blake3::Hasher::new_keyed(dek);
    hasher.update(b"oe-footer-nonce-v1");
    hasher.update(&file_id.to_le_bytes());
    hasher.update_rayon(footer_blob);
    let digest = hasher.finalize();
    let nonce = digest
        .as_bytes()
        .get(..scheme.nonce_len())
        .ok_or(FormatError::Structural {
            rule: "footer nonce longer than the keyed digest",
        })?
        .to_vec();
    let ciphertext = scheme
        .seal(dek, &nonce, &file_id.to_le_bytes(), footer_blob)
        .ok_or(FormatError::Structural {
            rule: "footer seal failed",
        })?;
    let mut sealed = nonce;
    sealed.extend_from_slice(&ciphertext);
    Ok(sealed)
}

fn build_entity_hash_filters(granules: &[GranulePieces]) -> BTreeMap<u32, Vec<u8>> {
    let mut distinct: HashSet<u64> = HashSet::default();
    let mut filters = BTreeMap::new();
    for granule in granules {
        let stored = granule.blocks.iter().find_map(|block| {
            (block.column_id == column_ids::ENTITY_ID_HASH_LOW)
                .then_some(match &block.data {
                    ColumnData::U64(values) => Some(values.as_slice()),
                    _ => None,
                })
                .flatten()
        });
        let elided = (!granule.elided_entity_hashes.is_empty()).then_some(granule.elided_entity_hashes.as_slice());
        let Some(hashes) = stored.or(elided) else {
            continue;
        };
        distinct.clear();
        distinct.extend(hashes.iter().copied());
        let filter = SplitBlockBloomFilter::build_from_iter(
            distinct.iter().copied(),
            distinct.len(),
            ENTITY_HASH_FILTER_BITS_PER_KEY,
        )
        .encode();
        filters.insert(granule.entry.granule_id, filter);
    }
    filters
}

struct FooterMetadataBatch {
    by_entity_type: Vec<u64>,
    by_event_type: Vec<u64>,
    by_source: Vec<u64>,
    clustering: Vec<ClusteringEntry>,
    entity_hash_filters: BTreeMap<u32, Vec<u8>>,
    ndv_by_column_stripe: BTreeMap<(u32, u32), NdvSketch>,
}

struct ScheduledFooterMetadata {
    clustering: Vec<ClusteringEntry>,
    entity_hash_filters: BTreeMap<u32, Vec<u8>>,
    exact_counts: ExactCounts,
    stripe_ndv: Vec<StripeNdvEntry>,
}

fn metadata_job_count(granule_count: usize, parallelism: usize) -> usize {
    if granule_count == 0 {
        return 0;
    }
    let useful_batches = granule_count.div_ceil(MIN_METADATA_GRANULES_PER_JOB);
    useful_batches.min(parallelism.max(1)).max(1)
}

fn batch_range(item_count: usize, job_count: usize, job_index: usize) -> Range<usize> {
    let start = item_count.saturating_mul(job_index) / job_count.max(1);
    let end = item_count.saturating_mul(job_index.saturating_add(1)) / job_count.max(1);
    start..end
}

fn build_footer_metadata_batch(
    granules: &[GranulePieces],
    entity_type_count: usize,
    event_type_count: usize,
    source_count: usize,
    hot_columns: &Range<u32>,
) -> FooterMetadataBatch {
    let mut batch = FooterMetadataBatch {
        by_entity_type: vec![0; entity_type_count],
        by_event_type: vec![0; event_type_count],
        by_source: vec![0; source_count],
        clustering: Vec::with_capacity(granules.len()),
        entity_hash_filters: build_entity_hash_filters(granules),
        ndv_by_column_stripe: BTreeMap::new(),
    };
    for granule in granules {
        for (target, source) in [
            (&mut batch.by_entity_type, &granule.by_entity_type),
            (&mut batch.by_event_type, &granule.by_event_type),
            (&mut batch.by_source, &granule.by_source),
        ] {
            for (total, count) in target.iter_mut().zip(source) {
                *total += count;
            }
        }
        batch.clustering.push(ClusteringEntry {
            clustering_quality: 1.0,
            granule_id: granule.entry.granule_id,
            projection_id: 0,
            sortedness_proof: Some(SortednessProof {
                column_names: vec!["epoch".to_owned(), "sequence".to_owned()],
                direction: SortDirection::Ascending,
            }),
        });
        for block in &granule.blocks {
            if hot_columns.contains(&block.column_id) {
                batch
                    .ndv_by_column_stripe
                    .entry((block.column_id, granule.entry.stripe_id))
                    .or_default()
                    .observe_column(&block.data);
            }
        }
    }
    batch
}

fn finish_footer_metadata(batches: Vec<FooterMetadataBatch>, row_count: u64) -> ScheduledFooterMetadata {
    let mut batches = batches.into_iter();
    let mut merged = batches.next().unwrap_or_else(|| FooterMetadataBatch {
        by_entity_type: Vec::new(),
        by_event_type: Vec::new(),
        by_source: Vec::new(),
        clustering: Vec::new(),
        entity_hash_filters: BTreeMap::new(),
        ndv_by_column_stripe: BTreeMap::new(),
    });
    for batch in batches {
        for (target, source) in [
            (&mut merged.by_entity_type, batch.by_entity_type),
            (&mut merged.by_event_type, batch.by_event_type),
            (&mut merged.by_source, batch.by_source),
        ] {
            for (total, count) in target.iter_mut().zip(source) {
                *total += count;
            }
        }
        merged.clustering.extend(batch.clustering);
        merged.entity_hash_filters.extend(batch.entity_hash_filters);
        for (key, sketch) in batch.ndv_by_column_stripe {
            merged.ndv_by_column_stripe.entry(key).or_default().merge(&sketch);
        }
    }
    let indexed_counts = |counts: Vec<u64>| {
        counts
            .into_iter()
            .enumerate()
            .map(|(index, count)| (index as u32, count))
            .collect()
    };
    let stripe_ndv = merged
        .ndv_by_column_stripe
        .into_iter()
        .map(|((column_id, stripe_id), sketch)| {
            let (distinct_count, exact) = sketch.distinct();
            StripeNdvEntry {
                column_id,
                distinct_count,
                exact,
                stripe_id,
            }
        })
        .collect();
    ScheduledFooterMetadata {
        clustering: merged.clustering,
        entity_hash_filters: merged.entity_hash_filters,
        exact_counts: ExactCounts {
            row_count,
            by_source: indexed_counts(merged.by_source),
            by_event_type: indexed_counts(merged.by_event_type),
            by_entity_type: indexed_counts(merged.by_entity_type),
        },
        stripe_ndv,
    }
}

fn borrow_footer_metadata(index: &SourceIndex<'_>) -> Result<ScheduledFooterMetadata, FormatError> {
    let source = index.source;
    let mut entity_hash_filters = BTreeMap::new();
    for entry in &source.footer.entity_hash_filters {
        let filter = index
            .extent(entry.granule_id, entry.index_offset, entry.index_len)
            .ok_or(FormatError::Structural {
                rule: "a source identity-hash filter must lie inside its stripe",
            })?;
        entity_hash_filters.insert(entry.granule_id, filter.to_vec());
    }
    Ok(ScheduledFooterMetadata {
        clustering: source.footer.clustering.clone(),
        entity_hash_filters,
        exact_counts: source.footer.exact_counts.clone(),
        stripe_ndv: source.footer.stripe_ndv.clone(),
    })
}

fn build_scheduled_footer_metadata(
    granules: &[GranulePieces],
    dictionaries: &FileDictionaries,
    hot_columns: &Range<u32>,
    row_count: u64,
    encode: &dyn EncodeExecutor,
    profile_workers: bool,
) -> ScheduledFooterMetadata {
    let job_count = metadata_job_count(granules.len(), encode.parallelism());
    let slots: Vec<Mutex<Option<FooterMetadataBatch>>> = (0..job_count).map(|_| Mutex::new(None)).collect();
    encode.run_jobs(job_count, &|job_index| {
        let work = || {
            let range = batch_range(granules.len(), job_count, job_index);
            build_footer_metadata_batch(
                granules.get(range).unwrap_or_default(),
                dictionaries.entity_type.len(),
                dictionaries.event_type.len(),
                dictionaries.source.len(),
                hot_columns,
            )
        };
        let batch = if profile_workers {
            profile_footer_metadata(work)
        } else {
            work()
        };
        if let Some(slot) = slots.get(job_index) {
            *slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
        }
    });
    #[expect(clippy::expect_used, reason = "the executor runs every job before returning")]
    let batches = slots
        .into_iter()
        .map(|slot| {
            slot.into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .expect("the executor runs every metadata batch")
        })
        .collect();
    finish_footer_metadata(batches, row_count)
}

fn granules_have_late_events(granules: &[GranulePieces]) -> bool {
    let mut max_occurred_so_far = i64::MIN;
    for granule in granules {
        if granule.has_internal_late_event || granule.entry.min_occurred_at_physical < max_occurred_so_far {
            return true;
        }
        max_occurred_so_far = max_occurred_so_far.max(granule.entry.max_occurred_at_physical);
    }
    false
}

fn compress_residual(residual: Vec<u8>) -> (Vec<u8>, ResidualCompression) {
    if residual.is_empty() {
        return (residual, ResidualCompression::None);
    }
    let Some(compressed) = seekable_zstd::compress(&residual) else {
        return (residual, ResidualCompression::None);
    };
    if compressed.len() + 4 >= residual.len() {
        return (residual, ResidualCompression::None);
    }
    (compressed, ResidualCompression::ZstdSeekable)
}

fn granule_for_ordinal(footer: &Footer, ordinal: u64) -> Option<u32> {
    let after = footer
        .granules
        .partition_point(|granule| granule.first_row_ordinal <= ordinal);
    let granule = footer.granules.get(after.checked_sub(1)?)?;
    let end = granule.first_row_ordinal.checked_add(u64::from(granule.row_count))?;
    (ordinal < end).then_some(granule.granule_id)
}

fn mark_all_columns(source: &BuiltOe, changed: &mut HashSet<u32>) {
    changed.extend(source.footer.columns.iter().map(|column| column.column_id));
}

fn mark_payload_change(
    source: &BuiltOe,
    config: &OeBuildConfig,
    before: &BuildPayload,
    after: &BuildPayload,
    changed: &mut HashSet<u32>,
    path_statistics_changed: &mut bool,
) -> bool {
    if before == after {
        return false;
    }
    let (BuildPayload::Object(before), BuildPayload::Object(after)) = (before, after) else {
        *path_statistics_changed = true;
        changed.insert(column_ids::PAYLOAD_FLAGS);
        changed.extend(
            source
                .footer
                .columns
                .iter()
                .filter(|column| column.column_id >= column_ids::PROMOTED_BASE)
                .map(|column| column.column_id),
        );
        return true;
    };

    let mut paths: HashSet<&str> = before.iter().map(|(name, _)| &**name).collect();
    paths.extend(after.iter().map(|(name, _)| &**name));
    let mut residual_changed = false;
    for path in paths {
        let before_value = field_value(before, path);
        let after_value = field_value(after, path);
        if before_value == after_value {
            continue;
        }
        *path_statistics_changed |= before_value.is_none()
            || after_value.is_none()
            || before_value.zip(after_value).is_some_and(|(before, after)| {
                std::mem::discriminant(before) != std::mem::discriminant(after)
                    || matches!(before, VariantValue::Array(_) | VariantValue::Object(_))
            });
        for (index, promoted) in config.promotion.columns.iter().enumerate() {
            if promoted.path == path {
                changed.insert(column_ids::PROMOTED_BASE + index as u32);
            }
        }
        let shredded = source.footer.shredded.iter().find(|entry| entry.path == path);
        let freetext = source.footer.freetext.iter().find(|entry| entry.declared_field == path);
        if let Some(entry) = shredded {
            changed.insert(entry.column_id);
        }
        if let Some(entry) = freetext {
            changed.insert(entry.column_id);
        }
        let shred_kind = shredded.and_then(|entry| {
            source
                .footer
                .columns
                .iter()
                .find(|column| column.column_id == entry.column_id)
                .map(|column| column.kind)
        });
        let moves_out = |value: &VariantValue| {
            matches!(
                (shred_kind, value),
                (Some(ColumnKind::I64), VariantValue::Int(_))
                    | (Some(ColumnKind::F64), VariantValue::Double(_))
                    | (Some(ColumnKind::String), VariantValue::String(_))
            ) || (freetext.is_some() && matches!(value, VariantValue::String(_)))
        };
        residual_changed |= shred_kind == Some(ColumnKind::Decimal)
            || [before_value, after_value]
                .into_iter()
                .flatten()
                .any(|value| !moves_out(value));
    }
    if !config.analytical_columns.is_empty() {
        changed.extend(config.analytical_columns.iter().map(|column| column.column_id));
    }
    residual_changed
}

fn field_value<'a>(fields: &'a [(FieldName, VariantValue)], path: &str) -> Option<&'a VariantValue> {
    fields
        .binary_search_by(|(name, _)| (**name).cmp(path))
        .ok()
        .and_then(|index| fields.get(index))
        .map(|(_, value)| value)
}

fn plan_update_reuse_from<'a, 'b>(
    source: &'a BuiltOe,
    before_len: usize,
    after_len: usize,
    changes: impl Iterator<Item = (u64, &'b BuildRow, &'b BuildRow)>,
    config: &OeBuildConfig,
) -> Option<UpdateReuse<'a>> {
    if config.footer_encryption != FooterEncryption::Plaintext
        || source.header.feature_flags & required_features::FOOTER_ENCRYPTED != 0
        || source.header.tenant_id != config.tenant_id
        || source.header.row_count != before_len as u64
        || before_len != after_len
        || source.footer.format_version != (1, 0)
        || (source.header.profile == Profile::Inbox) != (config.profile == Profile::Inbox)
    {
        return None;
    }

    let mut changed_columns_by_granule: HashMap<u32, HashSet<u32>> = HashMap::default();
    let mut changed_payload_granules: HashSet<u32> = HashSet::default();
    let mut path_statistics_changed = false;
    let mut payload_only = true;
    for (ordinal, before, after) in changes {
        if before == after {
            continue;
        }
        let granule_id = granule_for_ordinal(&source.footer, ordinal)?;
        let changed = changed_columns_by_granule.entry(granule_id).or_default();

        let mut normalized_envelope = before.envelope.clone();
        normalized_envelope.dedupe_hash_low = after.envelope.dedupe_hash_low;
        normalized_envelope.dedupe_hash_high = after.envelope.dedupe_hash_high;
        let only_payload_and_dedupe = before.epoch == after.epoch
            && before.sequence == after.sequence
            && normalized_envelope == after.envelope
            && before.provenance == after.provenance
            && before.relationships == after.relationships
            && before.external_id == after.external_id
            && before.raw_payload == after.raw_payload
            && before.delivery == after.delivery;
        if !only_payload_and_dedupe {
            mark_all_columns(source, changed);
            changed_payload_granules.insert(granule_id);
            payload_only = false;
            continue;
        }
        if before.envelope.dedupe_hash_low != after.envelope.dedupe_hash_low {
            changed.insert(column_ids::DEDUPE_HASH_LOW);
        }
        if before.envelope.dedupe_hash_high != after.envelope.dedupe_hash_high {
            changed.insert(column_ids::DEDUPE_HASH_HIGH);
        }
        if mark_payload_change(
            source,
            config,
            &before.payload,
            &after.payload,
            changed,
            &mut path_statistics_changed,
        ) {
            changed_payload_granules.insert(granule_id);
        }
    }
    for changed in changed_columns_by_granule.values_mut() {
        if changed.contains(&column_ids::OCCURRED_AT) {
            changed.insert(column_ids::INGESTED_AT);
        }
    }
    Some(UpdateReuse {
        changed_columns_by_granule,
        changed_payload_granules,
        path_statistics_changed,
        payload_only,
        source,
        sparse: None,
    })
}

fn with_sparse_plan<'a>(mut reuse: UpdateReuse<'a>, after: &[BuildRow], config: &OeBuildConfig) -> UpdateReuse<'a> {
    let carries_provenance = after.par_iter().any(|row| row.provenance.is_some());
    let carries_relationships = after.par_iter().any(|row| row.relationships.is_some());
    reuse.sparse = plan_sparse_update(&reuse, config, carries_provenance, carries_relationships, true);
    reuse
}

fn plan_change_set_reuse<'a>(
    source: &'a BuiltOe,
    after: &[BuildRow],
    changes: &[OeRowChange<BuildRow>],
    config: &OeBuildConfig,
) -> Option<UpdateReuse<'a>> {
    let sparse_enough = changes.len().saturating_mul(4) < after.len();
    let changes_match = sparse_enough
        && changes
            .iter()
            .all(|change| after.get(change.row_ordinal as usize) == Some(&change.after));
    changes_match
        .then(|| {
            plan_update_reuse_from(
                source,
                source.header.row_count as usize,
                after.len(),
                changes
                    .iter()
                    .map(|change| (change.row_ordinal, &change.before, &change.after)),
                config,
            )
        })
        .flatten()
        .map(|reuse| with_sparse_plan(reuse, after, config))
}

fn plan_source_update<'a>(
    source: &'a BuiltOe,
    changes: &[OeRowChange<BuildRow>],
    config: &OeBuildConfig,
) -> Result<(Vec<BuildRow>, UpdateReuse<'a>), FormatError> {
    let row_count = source.header.row_count as usize;
    let has_column = |column_id: u32| source.footer.columns.iter().any(|column| column.column_id == column_id);
    let plan = plan_update_reuse_from(
        source,
        row_count,
        row_count,
        changes
            .iter()
            .map(|change| (change.row_ordinal, &change.before, &change.after)),
        config,
    )
    .and_then(|reuse| {
        let sparse = plan_sparse_update(
            &reuse,
            config,
            has_column(column_ids::AUTHOR_PUBKEY),
            has_column(column_ids::PARENT_REF),
            false,
        )?;
        Some(UpdateReuse {
            sparse: Some(sparse),
            ..reuse
        })
    });
    let Some(reuse) = plan else {
        return Err(FormatError::Structural {
            rule: "the change set cannot be applied from the source alone; update from the full replacement rows",
        });
    };
    let Some(plan) = reuse.sparse.as_ref() else {
        return Err(FormatError::Structural {
            rule: "a changes-only update carries its sparse plan",
        });
    };

    let file = OeFile::open_lazy(source.bytes.clone(), Some(&source.file_seal), None)?;
    let u64_column = |column_id: u32, granule_id: u32| -> Result<Vec<u64>, FormatError> {
        match file.read_column(column_id, granule_id)?.data {
            ColumnData::U64(values) => Ok(values),
            _ => Err(FormatError::Structural {
                rule: "a source envelope column decodes as u64 values",
            }),
        }
    };
    let mut rows: Vec<BuildRow> = Vec::new();
    let mut built: Vec<(GranuleEntry, usize)> = Vec::new();
    let mut shared = SharedStrings::default();
    for (granule, build) in source.footer.granules.iter().zip(&plan.build) {
        if build.is_none() {
            continue;
        }
        built.push((*granule, rows.len()));
        let payloads = (0..u64::from(granule.row_count))
            .map(|row| file.payload(granule.first_row_ordinal + row))
            .collect::<Result<Vec<PayloadRead>, FormatError>>()?;
        let sequence = u64_column(column_ids::SEQUENCE, granule.granule_id)?;
        let dedupe_low = u64_column(column_ids::DEDUPE_HASH_LOW, granule.granule_id)?;
        let dedupe_high = u64_column(column_ids::DEDUPE_HASH_HIGH, granule.granule_id)?;
        let schema_version = u64_column(column_ids::SCHEMA_VERSION, granule.granule_id)?;
        if [&sequence, &dedupe_low, &dedupe_high, &schema_version]
            .iter()
            .any(|values| values.len() != payloads.len())
        {
            return Err(FormatError::Structural {
                rule: "a source granule's columns hold one value per row",
            });
        }
        for (((payload, &sequence), (&dedupe_hash_low, &dedupe_hash_high)), &schema_version) in payloads
            .into_iter()
            .zip(&sequence)
            .zip(dedupe_low.iter().zip(&dedupe_high))
            .zip(&schema_version)
        {
            let schema_version = u32::try_from(schema_version).map_err(|_| FormatError::Structural {
                rule: "a stored schema version fits in u32",
            })?;
            rows.push(BuildRow {
                delivery: None,
                envelope: BuildEnvelope {
                    account_id: None,
                    account_id_hash_low: 0,
                    actor_id: None,
                    actor_id_hash_low: 0,
                    dedupe_hash_high,
                    dedupe_hash_low,
                    entity_id: None,
                    entity_id_hash_high: 0,
                    entity_id_hash_low: 0,
                    entity_type: shared.share(""),
                    event_id: EventId::from_uuid(uuid::Uuid::nil()),
                    event_type: shared.share(""),
                    flags: EventFlags(0),
                    ingested_at: TimestampValue::default(),
                    occurred_at: TimestampValue::default(),
                    schema_version,
                    source: shared.share(""),
                    stream_id: StreamId(0),
                    stream_sequence: 0,
                    tenant_id: config.tenant_id,
                    trace_id_hash_low: 0,
                },
                epoch: granule.first_epoch,
                external_id: None,
                payload: match payload {
                    PayloadRead::External(reference) => BuildPayload::ExternalRef(reference),
                    PayloadRead::None => BuildPayload::None,
                    PayloadRead::Value(value) => shared.build_payload(value),
                },
                provenance: None,
                raw_payload: None,
                relationships: None,
                sequence,
                subject: None,
            });
        }
    }
    for change in changes {
        let local = built.iter().find_map(|(granule, local_start)| {
            let offset = change.row_ordinal.checked_sub(granule.first_row_ordinal)?;
            (offset < u64::from(granule.row_count)).then_some(local_start + offset as usize)
        });
        match local.and_then(|local| rows.get_mut(local)) {
            Some(row) => *row = change.after.clone(),
            None => {
                return Err(FormatError::Structural {
                    rule: "every change lands in a granule the sparse plan builds",
                });
            }
        }
    }
    Ok((rows, reuse))
}

fn order_update_rows(rows: &mut [BuildRow], config: &OeBuildConfig) {
    if config.profile == Profile::Inbox {
        rows.par_sort_unstable_by_key(|row| RowOrder::Entity.key(row));
    }
}

fn plan_update_reuse<'a>(
    source: &'a BuiltOe,
    before: &[BuildRow],
    after: &[BuildRow],
    config: &OeBuildConfig,
) -> Option<UpdateReuse<'a>> {
    plan_update_reuse_from(
        source,
        before.len(),
        after.len(),
        before
            .iter()
            .zip(after)
            .enumerate()
            .map(|(ordinal, (before, after))| (ordinal as u64, before, after)),
        config,
    )
}

/// Builds a file on the global rayon pool: granules, blocks, footer metadata
/// and stripe layout run as parallel jobs, and the bytes match
/// [`build_oe_file_with_executor`] with [`crate::invariants::sim::SerialEncodeExecutor`]. On a
/// one-thread pool the jobs run inline.
pub fn build_oe_file<R: BuildInput>(rows: Vec<R>, config: &OeBuildConfig) -> Result<BuiltOe, FormatError> {
    build_oe_file_with_executor(rows, config, &ThreadPoolEncodeExecutor)
}

pub fn build_oe_file_with_executor<R: BuildInput>(
    rows: Vec<R>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<BuiltOe, FormatError> {
    build_oe_file_sealed(R::into_build_rows(rows), config, encode, None, None, None)
}

pub fn update_oe_file_with_executor<R: BuildInput>(
    source: &BuiltOe,
    before: &[R],
    after: Vec<R>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<BuiltOe, FormatError> {
    refuse_audit_rewrite(source.header.profile, config.profile)?;
    let mut before = R::into_build_rows(before.to_vec());
    let mut after = R::into_build_rows(after);
    order_update_rows(&mut before, config);
    order_update_rows(&mut after, config);
    let reuse = plan_update_reuse(source, &before, &after, config).map(|reuse| with_sparse_plan(reuse, &after, config));
    build_oe_file_sealed(after, config, encode, None, None, reuse.as_ref())
}

pub fn update_oe_file_from_changes_with_executor<R: BuildInput>(
    source: &BuiltOe,
    after: Vec<R>,
    changes: &[OeRowChange<R>],
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<BuiltOe, FormatError> {
    refuse_audit_rewrite(source.header.profile, config.profile)?;
    let mut after = R::into_build_rows(after);
    order_update_rows(&mut after, config);
    let reuse = plan_change_set_reuse(source, &after, &R::build_changes(changes), config);
    build_oe_file_sealed(after, config, encode, None, None, reuse.as_ref())
}

pub fn update_oe_file_from_source_changes_with_executor<R: BuildInput>(
    source: &BuiltOe,
    changes: &[OeRowChange<R>],
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<BuiltOe, FormatError> {
    refuse_audit_rewrite(source.header.profile, config.profile)?;
    let (rows, reuse) = plan_source_update(source, &R::build_changes(changes), config)?;
    build_oe_file_sealed(rows, config, encode, None, None, Some(&reuse))
}

pub fn update_oe_file_profiled_with_executor<R: BuildInput>(
    source: &BuiltOe,
    before: &[R],
    after: Vec<R>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<(BuiltOe, BuildProfile), FormatError> {
    refuse_audit_rewrite(source.header.profile, config.profile)?;
    let mut before = R::into_build_rows(before.to_vec());
    let mut after = R::into_build_rows(after);
    order_update_rows(&mut before, config);
    order_update_rows(&mut after, config);
    let reuse = plan_update_reuse(source, &before, &after, config).map(|reuse| with_sparse_plan(reuse, &after, config));
    let mut profile = BuildProfile::default();
    let built = build_oe_file_sealed(after, config, encode, None, Some(&mut profile), reuse.as_ref())?;
    Ok((built, profile))
}

pub fn update_oe_file_from_changes_profiled_with_executor<R: BuildInput>(
    source: &BuiltOe,
    after: Vec<R>,
    changes: &[OeRowChange<R>],
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<(BuiltOe, BuildProfile), FormatError> {
    refuse_audit_rewrite(source.header.profile, config.profile)?;
    let mut after = R::into_build_rows(after);
    order_update_rows(&mut after, config);
    let reuse = plan_change_set_reuse(source, &after, &R::build_changes(changes), config);
    let mut profile = BuildProfile::default();
    let built = build_oe_file_sealed(after, config, encode, None, Some(&mut profile), reuse.as_ref())?;
    Ok((built, profile))
}

pub fn update_oe_file_from_source_changes_profiled_with_executor<R: BuildInput>(
    source: &BuiltOe,
    changes: &[OeRowChange<R>],
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<(BuiltOe, BuildProfile), FormatError> {
    refuse_audit_rewrite(source.header.profile, config.profile)?;
    let mut profile = BuildProfile::default();
    let decoding = PhaseClock::new(Some(&mut profile), BuildPhase::SourceDecoding);
    let planned = plan_source_update(source, &R::build_changes(changes), config);
    decoding.finish();
    let (rows, reuse) = planned?;
    let built = build_oe_file_sealed(rows, config, encode, None, Some(&mut profile), Some(&reuse))?;
    Ok((built, profile))
}

fn check_rows(
    rows: &[BuildRow],
    tenant_id: TenantId,
    order: RowOrder,
) -> Result<Option<(RowKey, RowKey)>, &'static str> {
    let mut previous: Option<RowKey> = None;
    for row in rows {
        let key = order.key(row);
        if previous.is_some_and(|previous| previous >= key) {
            return Err(order.rule());
        }
        previous = Some(key);
        if row.envelope.tenant_id != tenant_id {
            return Err("a OE file carries one tenant");
        }
        if row.subject.is_some() {
            return Err("a row naming a subject is sealed with seal_subject_rows before the build");
        }
    }
    Ok(rows.first().map(|row| order.key(row)).zip(previous))
}

fn inbox_recipient_directory(rows: &[BuildRow]) -> Vec<InboxRecipientEntry> {
    let mut directory: Vec<InboxRecipientEntry> = Vec::new();
    for (ordinal, row) in rows.iter().enumerate() {
        let envelope = &row.envelope;
        match directory.last_mut() {
            Some(entry)
                if entry.entity_id_hash_low == envelope.entity_id_hash_low
                    && entry.entity_id_hash_high == envelope.entity_id_hash_high =>
            {
                entry.last_epoch = row.epoch;
                entry.last_sequence = row.sequence;
                entry.row_count += 1;
            }
            _ => directory.push(InboxRecipientEntry {
                entity_id_hash_high: envelope.entity_id_hash_high,
                entity_id_hash_low: envelope.entity_id_hash_low,
                first_row_ordinal: ordinal as u64,
                last_epoch: row.epoch,
                last_sequence: row.sequence,
                row_count: 1,
            }),
        }
    }
    directory
}

fn time_series_rollups(
    granules: &[GranulePieces],
    float_columns: Vec<u32>,
    bucket_nanos: i64,
) -> Result<TimeSeriesRollups, FormatError> {
    let missing = FormatError::Structural {
        rule: "a time-series granule holds every rolled-up column",
    };
    let mut buckets: Vec<RollupBucket> = Vec::new();
    for granule in granules {
        let block = |column_id: u32| granule.blocks.iter().find(|block| block.column_id == column_id);
        let Some(ColumnData::I64(occurred)) = block(column_ids::OCCURRED_AT).map(|block| &block.data) else {
            return Err(missing);
        };
        let mut columns = Vec::with_capacity(float_columns.len());
        for &column_id in &float_columns {
            let Some(PendingBlock {
                data: ColumnData::F64(values),
                presence,
                ..
            }) = block(column_id)
            else {
                return Err(missing);
            };
            columns.push((presence.as_slice(), values.as_slice(), 0usize));
        }
        for (row, &at) in occurred.iter().enumerate() {
            let bucket = at.div_euclid(bucket_nanos);
            if buckets.last().is_none_or(|last| last.bucket != bucket) {
                buckets.push(RollupBucket {
                    aggregates: vec![FloatAggregate::default(); float_columns.len()],
                    bucket,
                    first_row_ordinal: granule.entry.first_row_ordinal + row as u64,
                    row_count: 0,
                });
            }
            let Some(current) = buckets.last_mut() else {
                continue;
            };
            current.row_count += 1;
            for ((presence, values, taken), aggregate) in columns.iter_mut().zip(&mut current.aggregates) {
                let present =
                    presence.is_empty() || presence.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0);
                if present {
                    if let Some(value) = values.get(*taken) {
                        aggregate.add(*value);
                    }
                    *taken += 1;
                }
            }
        }
    }
    Ok(TimeSeriesRollups {
        bucket_nanos,
        buckets,
        column_ids: float_columns,
    })
}

fn sparse_fallback(
    mut rows: Vec<BuildRow>,
    payloads: NormalizedPayloads,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
    sink: Option<&mut dyn FnMut(u64, Vec<u8>)>,
    phase_clock: PhaseClock<'_>,
    reuse: &UpdateReuse<'_>,
) -> Result<BuiltOe, FormatError> {
    if !reuse.sparse.as_ref().is_some_and(|plan| plan.rows_cover_file) {
        return Err(FormatError::Structural {
            rule: "the change set moves a granule or stripe boundary; update from the full replacement rows",
        });
    }
    payloads.restore(&mut rows);
    let profile = phase_clock.into_profile();
    let full = UpdateReuse {
        sparse: None,
        ..reuse.clone()
    };
    build_oe_file_sealed(rows, config, encode, sink, profile, Some(&full))
}

pub fn build_oe_file_profiled_with_executor<R: BuildInput>(
    rows: Vec<R>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<(BuiltOe, BuildProfile), FormatError> {
    let mut profile = BuildProfile::default();
    let built = build_oe_file_sealed(R::into_build_rows(rows), config, encode, None, Some(&mut profile), None)?;
    Ok((built, profile))
}

#[derive(Debug)]
pub struct StreamedOe {
    pub file_seal: [u8; 32],
    pub file_crc64_nvme: u64,
    pub file_id: u128,
    pub footer: Footer,
    pub footer_len: u64,
    pub header: OeHeader,
    pub total_len: u64,
}

impl From<BuiltOe> for StreamedOe {
    fn from(built: BuiltOe) -> Self {
        Self {
            file_seal: built.file_seal,
            file_crc64_nvme: built.file_crc64_nvme,
            file_id: built.file_id,
            footer_len: built.footer_len,
            total_len: built.total_len,
            footer: built.footer,
            header: built.header,
        }
    }
}

pub fn build_oe_file_streamed<R: BuildInput>(
    rows: Vec<R>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
    sink: &mut dyn FnMut(u64, Vec<u8>),
) -> Result<StreamedOe, FormatError> {
    build_oe_file_sealed(R::into_build_rows(rows), config, encode, Some(sink), None, None).map(StreamedOe::from)
}

pub fn build_oe_file_streamed_profiled<R: BuildInput>(
    rows: Vec<R>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
    sink: &mut dyn FnMut(u64, Vec<u8>),
) -> Result<(StreamedOe, BuildProfile), FormatError> {
    let mut profile = BuildProfile::default();
    let built = build_oe_file_sealed(
        R::into_build_rows(rows),
        config,
        encode,
        Some(sink),
        Some(&mut profile),
        None,
    )?;
    Ok((StreamedOe::from(built), profile))
}

fn build_oe_file_sealed(
    mut rows: Vec<BuildRow>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
    sink: Option<&mut dyn FnMut(u64, Vec<u8>)>,
    profile: Option<&mut BuildProfile>,
    update_reuse: Option<&UpdateReuse<'_>>,
) -> Result<BuiltOe, FormatError> {
    let order = if config.profile == Profile::Inbox {
        rows.par_sort_unstable_by_key(|row| RowOrder::Entity.key(row));
        RowOrder::Entity
    } else {
        RowOrder::Sequence
    };
    build_oe_file_sealed_in_order(rows, config, encode, sink, profile, update_reuse, order)
}

pub(crate) fn build_oe_file_in_order(
    rows: Vec<BuildRow>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
    order: RowOrder,
) -> Result<BuiltOe, FormatError> {
    build_oe_file_sealed_in_order(rows, config, encode, None, None, None, order)
}

fn build_oe_file_sealed_in_order(
    mut rows: Vec<BuildRow>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
    mut sink: Option<&mut dyn FnMut(u64, Vec<u8>)>,
    profile: Option<&mut BuildProfile>,
    update_reuse: Option<&UpdateReuse<'_>>,
    order: RowOrder,
) -> Result<BuiltOe, FormatError> {
    let mut phase_clock = PhaseClock::new(profile, BuildPhase::Normalization);
    let profile_workers = phase_clock.enabled();
    let sparse_reuse = update_reuse.filter(|reuse| reuse.sparse.is_some());
    let sparse: Option<&SparsePlan> = sparse_reuse.and_then(|reuse| reuse.sparse.as_ref());
    let source = sparse_reuse.map(|reuse| reuse.source);
    let source_index = update_reuse.map(|reuse| SourceIndex::new(reuse.source));
    let (row_count, first_key, last_key) = match source {
        Some(source) => {
            let (Some(first), Some(last)) = (source.footer.granules.first(), source.footer.granules.last()) else {
                return Err(FormatError::Structural {
                    rule: "a OE file must carry at least one row",
                });
            };
            (
                source.header.row_count as usize,
                (first.first_epoch, first.first_sequence),
                (last.last_epoch, last.last_sequence),
            )
        }
        None => {
            let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
                return Err(FormatError::Structural {
                    rule: "a OE file must carry at least one row",
                });
            };
            match order {
                RowOrder::Entity => {
                    let points = rows.iter().map(|row| (row.epoch, row.sequence));
                    let first = points.clone().min().unwrap_or((first.epoch, first.sequence));
                    let last = points.max().unwrap_or((last.epoch, last.sequence));
                    (rows.len(), first, last)
                }
                RowOrder::Sequence => (rows.len(), (first.epoch, first.sequence), (last.epoch, last.sequence)),
            }
        }
    };
    if config.graph.is_some()
        && (config.profile != Profile::Graph || update_reuse.is_some() || order != RowOrder::Sequence)
    {
        return Err(FormatError::Structural {
            rule: "graph edges are built only into a whole graph-profile file in sequence order",
        });
    }
    let checked: Vec<Result<Option<(RowKey, RowKey)>, &'static str>> = rows
        .par_chunks(NORMALIZE_BATCH_ROWS)
        .map(|chunk| check_rows(chunk, config.tenant_id, order))
        .collect();
    let mut previous: Option<RowKey> = None;
    for chunk in checked {
        let Some((first, last)) = chunk.map_err(|rule| FormatError::Structural { rule })? else {
            continue;
        };
        if previous.is_some_and(|previous| previous >= first) {
            return Err(FormatError::Structural { rule: order.rule() });
        }
        previous = Some(last);
    }
    let audit = audit_trail(&rows, config, order)?;
    let graph_event_ids = match &config.graph {
        Some(graph) => EventIdIndex::build(
            rows.iter()
                .zip(graph.first_sn..)
                .filter_map(|(row, sn)| Some((row.external_id.as_deref()?, sn))),
        )?,
        None => None,
    };
    let inbox_recipients = if config.profile == Profile::Inbox {
        if order != RowOrder::Entity || sparse.is_some_and(|plan| !plan.rows_cover_file) {
            return Err(FormatError::Structural {
                rule: "an inbox file is built from all of its rows in recipient order",
            });
        }
        inbox_recipient_directory(&rows)
    } else {
        Vec::new()
    };
    let time_series_bucket_nanos = match (config.profile, config.time_series_bucket_nanos) {
        (Profile::TimeSeries, width) => {
            let width = width.unwrap_or(DEFAULT_TIME_SERIES_BUCKET_NANOS);
            if width <= 0 {
                return Err(FormatError::Structural {
                    rule: "a time-series bucket width is positive",
                });
            }
            if rows.iter().zip(rows.iter().skip(1)).any(|(before, after)| {
                after.envelope.occurred_at.physical_nanos() < before.envelope.occurred_at.physical_nanos()
            }) {
                return Err(FormatError::Structural {
                    rule: "a time-series file's rows never go back in occurred_at",
                });
            }
            Some(width)
        }
        (_, Some(_)) => {
            return Err(FormatError::Structural {
                rule: "only a time-series file takes a rollup bucket width",
            });
        }
        (_, None) => None,
    };
    let sparse_ranges: Option<Vec<(usize, usize)>> = sparse.zip(source).map(|(plan, source)| {
        let mut cursor = 0usize;
        source
            .footer
            .granules
            .iter()
            .enumerate()
            .map(|(index, granule)| {
                let start = cursor;
                if plan.rows_cover_file || plan.builds(index).is_some() {
                    cursor += granule.row_count as usize;
                }
                (start, cursor)
            })
            .collect()
    });
    if let Some(ranges) = &sparse_ranges
        && ranges.last().map_or(0, |range| range.1) != rows.len()
    {
        return Err(FormatError::Structural {
            rule: "a sparse update's rows are its built granules' rows in granule order",
        });
    }
    let normalized_ranges: Vec<(usize, usize)> = match (sparse, &sparse_ranges) {
        (Some(plan), Some(ranges)) => ranges
            .iter()
            .enumerate()
            .filter(|(index, _)| plan.builds(*index).is_some())
            .map(|(_, range)| *range)
            .collect(),
        _ => vec![(0, rows.len())],
    };
    let payloads = NormalizedPayloads::normalize(&mut rows, &normalized_ranges)?;
    let field_identities = payloads.identities();
    if let (Some(plan), Some(source), Some(ranges), Some(reuse)) = (sparse, source, &sparse_ranges, sparse_reuse) {
        let granule_count = source.footer.granules.len();
        let reproduces =
            source
                .footer
                .granules
                .iter()
                .zip(ranges)
                .enumerate()
                .all(|(index, (granule, &(start, end)))| {
                    plan.builds(index).is_none() || {
                        let forced = granule.row_count as usize == config.targets.index_granularity
                            || index + 1 == granule_count
                            || source
                                .footer
                                .granules
                                .get(index + 1)
                                .is_some_and(|next| next.first_epoch != granule.last_epoch);
                        granule_cut_reproduces(
                            rows.get(start..end).unwrap_or_default(),
                            &payloads,
                            start,
                            &config.targets,
                            !forced,
                        )
                    }
                });
        if !reproduces {
            return sparse_fallback(rows, payloads, config, encode, sink, phase_clock, reuse);
        }
    }
    if config.io_alignment_bytes > 0
        && (!config.io_alignment_bytes.is_power_of_two() || config.io_alignment_bytes as usize > HEADER_BLOCK_LEN)
    {
        return Err(FormatError::Structural {
            rule: "io_alignment_bytes must be a power of two no larger than the header block",
        });
    }
    validate_promotion_plan(&config.promotion)?;
    if config.promotion.columns.len() > (column_ids::SHREDDED_BASE - column_ids::PROMOTED_BASE) as usize
        || config.freetext.fields.len() > (column_ids::CONTEXT_BASE - column_ids::FREETEXT_BASE) as usize
    {
        return Err(FormatError::Structural {
            rule: "a promotion plan or free-text declaration holds more columns than its column id range",
        });
    }

    if config
        .analytical_columns
        .iter()
        .any(|col| col.data.row_count() != row_count)
    {
        return Err(FormatError::Structural {
            rule: "each analytical column must carry exactly one value per row",
        });
    }

    if config
        .analytical_columns
        .iter()
        .any(|col| col.substring_searchable && (col.kind != ColumnKind::String || col.internal_only))
    {
        return Err(FormatError::Structural {
            rule: "only a public string analytical column may be declared substring-searchable",
        });
    }

    phase_clock.transition(BuildPhase::DictionaryConstruction);
    let dictionaries_unchanged = update_reuse.is_some_and(|reuse| {
        !reuse.changed_columns_by_granule.values().any(|columns| {
            [
                column_ids::SOURCE_ID,
                column_ids::EVENT_TYPE_ID,
                column_ids::ENTITY_TYPE_ID,
            ]
            .iter()
            .any(|column_id| columns.contains(column_id))
        })
    });
    let dictionaries = if dictionaries_unchanged {
        update_reuse
            .map(|reuse| reuse.source.footer.dictionaries.clone())
            .unwrap_or_default()
    } else {
        build_file_dictionaries(&rows)
    };
    let dictionary_index = build_dictionary_index(&dictionaries);

    phase_clock.transition(BuildPhase::PathStatistics);
    let observe_rows = |entries: &[NormalizedPayload]| {
        let mut statistics = PathStatistics::for_field_ids(field_identities.names.len());
        for &entry in entries {
            statistics.observe_field_ids(payloads.field_ids(entry), payloads.values(entry));
        }
        statistics
    };
    let merge_statistics = |mut left: PathStatistics, right: PathStatistics| {
        left.merge(right);
        left
    };
    let reuse_path_plan = update_reuse
        .is_some_and(|reuse| !reuse.path_statistics_changed && static_routes_compatible(&reuse.source.footer, config));
    let statistics = if reuse_path_plan {
        None
    } else if profile_workers {
        payloads
            .rows
            .par_chunks(1_024)
            .map(|entries| profile_path_statistics(|| observe_rows(entries)))
            .reduce(
                || PathStatistics::for_field_ids(field_identities.names.len()),
                merge_statistics,
            )
            .into()
    } else {
        Some(
            payloads
                .rows
                .par_iter()
                .fold(
                    || PathStatistics::for_field_ids(field_identities.names.len()),
                    |mut statistics, &entry| {
                        statistics.observe_field_ids(payloads.field_ids(entry), payloads.values(entry));
                        statistics
                    },
                )
                .reduce(
                    || PathStatistics::for_field_ids(field_identities.names.len()),
                    merge_statistics,
                ),
        )
    };
    let promoted_paths: Vec<&str> = config
        .promotion
        .columns
        .iter()
        .map(|column| column.path.as_str())
        .collect();
    let freetext_paths: Vec<&str> = config.freetext.fields.iter().map(String::as_str).collect();
    let excluded: Vec<&str> = promoted_paths.iter().chain(freetext_paths.iter()).copied().collect();
    let excluded_field_ids: HashSet<u32> = excluded
        .iter()
        .filter_map(|path| field_identities.field_id(path))
        .collect();
    let (shred_plan, sparse_keys, shred_kinds) = if reuse_path_plan {
        let source = &update_reuse
            .ok_or(FormatError::Structural {
                rule: "reuse path plan has a source",
            })?
            .source
            .footer;
        let shred_plan = source.shredded.clone();
        let sparse_keys = source.sparse_keys.clone();
        let shred_kinds = shred_plan
            .iter()
            .filter_map(|entry| {
                source
                    .columns
                    .iter()
                    .find(|column| column.column_id == entry.column_id)
                    .map(|column| (entry.path.clone(), column.kind))
            })
            .collect();
        (shred_plan, sparse_keys, shred_kinds)
    } else {
        let statistics = statistics.ok_or(FormatError::Structural {
            rule: "full path analysis produces statistics",
        })?;
        let field_names: Vec<String> = field_identities
            .field_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        let mut candidates = statistics.shred_candidates_by_id(&field_names, &excluded_field_ids);
        let mut sparse = statistics.sparse_candidates_by_id(&field_names, &excluded_field_ids);
        candidates.truncate(SHREDDED_COLUMNS_MAX);
        sparse.truncate(SHREDDED_COLUMNS_MAX - candidates.len());
        let shred_plan: Vec<ShredEntry> = candidates
            .iter()
            .chain(sparse.iter())
            .enumerate()
            .map(|(index, (path, _kind))| ShredEntry {
                column_id: column_ids::SHREDDED_BASE + index as u32,
                path: path.clone(),
            })
            .collect();
        let sparse_keys: Vec<ShredEntry> = shred_plan.iter().skip(candidates.len()).cloned().collect();
        let shred_kinds: HashMap<String, ColumnKind> = candidates.into_iter().chain(sparse).collect();
        (shred_plan, sparse_keys, shred_kinds)
    };

    let elide_entity_hash = match source {
        Some(source) => source.header.feature_flags & required_features::ENTITY_HASH_COLUMNS_ELIDED != 0,
        None => {
            order == RowOrder::Sequence
                && config.profile != Profile::Inbox
                && rows.par_iter().all(|row| {
                    crate::events::derived_entity_id_hash(row.envelope.entity_id.as_deref())
                        == (row.envelope.entity_id_hash_low, row.envelope.entity_id_hash_high)
                })
        }
    };
    let keeps_column = |column_id: u32| {
        !(elide_entity_hash
            && (column_id == column_ids::ENTITY_ID_HASH_LOW || column_id == column_ids::ENTITY_ID_HASH_HIGH))
    };

    let mut columns: Vec<ColumnDescriptor> = REQUIRED_COLUMNS
        .iter()
        .filter(|spec| keeps_column(spec.column_id))
        .map(|spec| ColumnDescriptor {
            column_id: spec.column_id,
            name: spec.name.to_owned(),
            kind: spec.kind,
            nullable: spec.nullable,
            internal_only: spec.internal_only,
        })
        .collect();
    columns.reserve(
        config.promotion.columns.len()
            + shred_plan.len()
            + config.freetext.fields.len()
            + config.analytical_columns.len()
            + PROVENANCE_COLUMNS.len()
            + RELATIONSHIP_COLUMNS.len(),
    );
    let mut presence_entries = Vec::with_capacity(config.promotion.columns.len());
    for (index, promoted) in config.promotion.columns.iter().enumerate() {
        let column_id = column_ids::PROMOTED_BASE + index as u32;
        columns.push(ColumnDescriptor {
            column_id,
            name: promoted.name.clone(),
            kind: promoted.kind,
            nullable: true,
            internal_only: false,
        });
        presence_entries.push(PresenceEntry {
            column_id,
            since_schema_version: promoted.since_schema_version,
        });
    }
    for entry in &shred_plan {
        columns.push(ColumnDescriptor {
            column_id: entry.column_id,
            name: format!("payload.{}", entry.path),
            kind: shred_kinds
                .get(entry.path.as_str())
                .copied()
                .unwrap_or(ColumnKind::String),
            nullable: true,
            internal_only: false,
        });
    }
    let source_has_column = |column_id: u32| {
        source.is_some_and(|source| source.footer.columns.iter().any(|column| column.column_id == column_id))
    };
    let carries_provenance = match source {
        Some(_) => source_has_column(column_ids::AUTHOR_PUBKEY),
        None => rows.par_iter().any(|row| row.provenance.is_some()),
    };
    if carries_provenance {
        for spec in PROVENANCE_COLUMNS {
            columns.push(ColumnDescriptor {
                column_id: spec.column_id,
                name: spec.name.to_owned(),
                kind: spec.kind,
                nullable: spec.nullable,
                internal_only: spec.internal_only,
            });
        }
    }
    let carries_relationships = match source {
        Some(_) => source_has_column(column_ids::PARENT_REF),
        None => rows.par_iter().any(|row| row.relationships.is_some()),
    };
    if carries_relationships {
        for spec in RELATIONSHIP_COLUMNS {
            columns.push(ColumnDescriptor {
                column_id: spec.column_id,
                name: spec.name.to_owned(),
                kind: spec.kind,
                nullable: spec.nullable,
                internal_only: spec.internal_only,
            });
        }
    }
    let source_form_columns =
        source_form::carried_columns(source.map(|source| &source.footer), &rows, config.delivery_lineage);
    columns.extend(source_form_columns.iter().map(|spec| ColumnDescriptor {
        column_id: spec.column_id,
        name: spec.name.to_owned(),
        kind: spec.kind,
        nullable: spec.nullable,
        internal_only: spec.internal_only,
    }));
    let freetext_entries: Vec<FreetextEntry> = config
        .freetext
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| FreetextEntry {
            column_id: column_ids::FREETEXT_BASE + index as u32,
            declared_field: field.clone(),
        })
        .collect();
    for entry in &freetext_entries {
        columns.push(ColumnDescriptor {
            column_id: entry.column_id,
            name: format!("freetext.{}", entry.declared_field),
            kind: ColumnKind::String,
            nullable: true,
            internal_only: false,
        });
    }
    for col in &config.analytical_columns {
        columns.push(ColumnDescriptor {
            column_id: col.column_id,
            internal_only: col.internal_only,
            kind: col.kind,
            name: col.name.clone(),
            nullable: true,
        });
    }

    let mut seen_column_ids = HashSet::with_capacity(columns.len());
    if !columns.iter().all(|column| seen_column_ids.insert(column.column_id)) {
        return Err(FormatError::Structural {
            rule: "every column in a OE file must have a distinct column id",
        });
    }

    phase_clock.transition(BuildPhase::GranuleConstruction);
    let mut granule_row_ranges: Vec<(usize, usize)> = sparse_ranges.unwrap_or_default();
    let mut start = 0usize;
    let mut payload_bytes = 0usize;
    let byte_limit_enabled = config.targets.index_granularity_bytes != usize::MAX;
    let payload_estimates: Vec<Vec<usize>> = if byte_limit_enabled && sparse.is_none() {
        rows.par_chunks(NORMALIZE_BATCH_ROWS)
            .zip(payloads.rows.par_chunks(NORMALIZE_BATCH_ROWS))
            .map(|(rows, entries)| {
                let mut estimator = PayloadByteEstimator::default();
                rows.iter()
                    .zip(entries)
                    .map(|(row, &entry)| estimator.estimate(&payloads, entry, row))
                    .collect()
            })
            .collect()
    } else {
        Vec::new()
    };
    let mut payload_estimates = payload_estimates.iter().flatten().copied();
    for (index, row) in rows
        .iter()
        .enumerate()
        .take(if sparse.is_some() { 0 } else { rows.len() })
    {
        if byte_limit_enabled {
            payload_bytes = payload_bytes.saturating_add(payload_estimates.next().unwrap_or_default());
        }
        let rows_in_granule = index - start + 1;
        let epoch_boundary = rows.get(index + 1).is_some_and(|next| next.epoch != row.epoch);
        if rows_in_granule >= config.targets.index_granularity
            || (byte_limit_enabled && payload_bytes >= config.targets.index_granularity_bytes)
            || epoch_boundary
        {
            granule_row_ranges.push((start, index + 1));
            start = index + 1;
            payload_bytes = 0;
        }
    }
    if sparse.is_none() && start < rows.len() {
        granule_row_ranges.push((start, rows.len()));
    }
    let snapshot_keys = match (config.profile, source) {
        (Profile::Snapshot, Some(_)) => {
            return Err(FormatError::Structural {
                rule: "a snapshot file is replaced whole, never sparsely updated",
            });
        }
        (Profile::Snapshot, None) => {
            if rows.iter().any(|row| row.epoch != first_key.0) {
                return Err(FormatError::Structural {
                    rule: "a snapshot file holds one epoch",
                });
            }
            let keys = rows
                .iter()
                .map(|row| row.envelope.entity_id.as_deref())
                .collect::<Option<Vec<&str>>>()
                .ok_or(FormatError::Structural {
                    rule: "every snapshot row carries an entity_id",
                })?;
            let granule_first_rows: Vec<usize> = granule_row_ranges.iter().map(|&(start, _)| start).collect();
            Some(SnapshotKeyIndex::build(&keys, &granule_first_rows)?)
        }
        _ => None,
    };
    let (source_form_blocks, source_form_indexes) = match source {
        Some(source) => (Vec::new(), SourceFormIndexes::from_footer(&source.footer)),
        None => source_form::build(&rows, &granule_row_ranges, &source_form_columns)?,
    };

    let mut row_slices: Vec<Mutex<&mut [BuildRow]>> = Vec::with_capacity(granule_row_ranges.len());
    let mut remaining: &mut [BuildRow] = &mut rows;
    for (start, end) in &granule_row_ranges {
        let row_count = end.saturating_sub(*start).min(remaining.len());
        let (slice, rest) = std::mem::take(&mut remaining).split_at_mut(row_count);
        remaining = rest;
        row_slices.push(Mutex::new(slice));
    }
    let granule_slots: Vec<Mutex<Option<Result<GranulePieces, FormatError>>>> =
        row_slices.iter().map(|_| Mutex::new(None)).collect();
    let field_routes = build_field_routes(field_identities, &config.promotion, &shred_plan, &freetext_entries);
    let shape_routes = build_shape_routes(field_identities, &field_routes);
    encode.run_jobs(row_slices.len(), &|granule_index| {
        maybe_profile_granule_construction(profile_workers, || {
            let (Some(granule_rows), Some(slot), Some(&(start, _))) = (
                row_slices.get(granule_index),
                granule_slots.get(granule_index),
                granule_row_ranges.get(granule_index),
            ) else {
                return;
            };
            let mut granule_rows = granule_rows.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut build = |granule_id: u32, first_row_ordinal: u64, wanted: Option<&HashSet<u32>>, residual| {
                let granule_payloads = payloads.range(start, granule_rows.len());
                build_granule(
                    granule_id,
                    first_row_ordinal,
                    &mut granule_rows,
                    granule_payloads,
                    &dictionaries,
                    &dictionary_index,
                    &config.promotion,
                    &shred_plan,
                    &shred_kinds,
                    &freetext_entries,
                    &field_routes,
                    &shape_routes,
                    &payloads,
                    config.freetext_row_offset_index,
                    carries_provenance,
                    carries_relationships,
                    elide_entity_hash,
                    wanted,
                    residual,
                )
            };
            let pieces = match sparse.zip(source.and_then(|source| source.footer.granules.get(granule_index))) {
                Some((plan, source_entry)) => match plan.builds(granule_index) {
                    None => Ok(GranulePieces::borrowed(*source_entry)),
                    Some(columns) => {
                        let rebuild_residual = sparse_reuse
                            .is_some_and(|reuse| reuse.changed_payload_granules.contains(&source_entry.granule_id));
                        build(
                            source_entry.granule_id,
                            source_entry.first_row_ordinal,
                            Some(columns),
                            rebuild_residual,
                        )
                        .map(|mut pieces| {
                            pieces.entry = GranuleEntry {
                                compressed_bytes_estimate: if rebuild_residual {
                                    pieces.entry.compressed_bytes_estimate
                                } else {
                                    source_entry.compressed_bytes_estimate
                                },
                                ..*source_entry
                            };
                            pieces
                        })
                    }
                },
                None => build(granule_index as u32, start as u64, None, true),
            };
            *slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(pieces);
        });
    });
    drop(field_routes);
    drop(dictionary_index);
    drop(row_slices);
    let mut granules: Vec<GranulePieces> = Vec::with_capacity(granule_slots.len());
    for slot in granule_slots {
        granules.push(
            slot.into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .ok_or(FormatError::Structural {
                    rule: "the executor runs every job",
                })??,
        );
    }
    for (granule, blocks) in granules.iter_mut().zip(source_form_blocks) {
        granule
            .blocks
            .extend(blocks.into_iter().map(|(column_id, presence, data)| PendingBlock {
                column_id,
                data,
                presence,
                random_access: true,
            }));
    }
    let time_series_rollups = match time_series_bucket_nanos {
        Some(width) => {
            let float_columns: Vec<u32> = columns
                .iter()
                .filter(|column| {
                    column.kind == ColumnKind::F64
                        && (column_ids::PROMOTED_BASE..column_ids::FREETEXT_BASE).contains(&column.column_id)
                })
                .map(|column| column.column_id)
                .collect();
            (!float_columns.is_empty())
                .then(|| time_series_rollups(&granules, float_columns, width))
                .transpose()?
        }
        None => None,
    };

    phase_clock.transition(BuildPhase::BlockEncoding);

    let block_columns: Vec<u32> = REQUIRED_COLUMNS
        .iter()
        .map(|spec| spec.column_id)
        .filter(|&column_id| keeps_column(column_id))
        .chain((0..config.promotion.columns.len()).map(|index| column_ids::PROMOTED_BASE + index as u32))
        .chain(shred_plan.iter().map(|entry| entry.column_id))
        .chain(freetext_entries.iter().map(|entry| entry.column_id))
        .chain(
            carries_provenance
                .then_some(PROVENANCE_COLUMNS)
                .into_iter()
                .flatten()
                .map(|spec| spec.column_id),
        )
        .chain(
            carries_relationships
                .then_some(RELATIONSHIP_COLUMN_IDS)
                .into_iter()
                .flatten(),
        )
        .chain(source_form_columns.iter().map(|spec| spec.column_id))
        .collect();
    let source_block_values: HashMap<(u32, u32), usize> = source
        .into_iter()
        .flat_map(|source| &source.footer.page_stats)
        .map(|stats| ((stats.column_id, stats.granule_id), stats.row_count as usize))
        .collect();
    let mut freetext_row_offsets_by_granule: Vec<Vec<FreetextOffsetIndex<'_>>> = Vec::with_capacity(granules.len());
    for granule in granules.iter_mut() {
        let mut built = std::mem::take(&mut granule.freetext_row_offsets).into_iter().peekable();
        let mut indexes = Vec::new();
        for entry in &freetext_entries {
            if built
                .peek()
                .is_some_and(|(column_id, _, _)| *column_id == entry.column_id)
            {
                if let Some((column_id, bytes, offsets)) = built.next() {
                    indexes.push((column_id, Cow::Owned(bytes), Cow::Owned(offsets)));
                }
                continue;
            }
            let Some(source_index) = source_index
                .as_ref()
                .filter(|_| source.is_some() && config.freetext_row_offset_index)
            else {
                continue;
            };
            let (bytes, offsets) = source_index
                .freetext_row_offsets
                .get(&(entry.column_id, granule.entry.granule_id))
                .and_then(|index| {
                    Some((
                        source_index.extent(index.granule_id, index.bytes_offset, index.bytes_len)?,
                        source_index.extent(index.granule_id, index.offsets_offset, index.offsets_len)?,
                    ))
                })
                .ok_or(FormatError::Structural {
                    rule: "a sparse update borrows free-text row indexes the source stores",
                })?;
            indexes.push((entry.column_id, Cow::Borrowed(bytes), Cow::Borrowed(offsets)));
        }
        freetext_row_offsets_by_granule.push(indexes);
    }

    let mut stripe_of_granule: Vec<u32> = Vec::with_capacity(granules.len());
    let mut stripe_id = 0u32;
    let mut stripe_bytes = 0usize;
    for (granule_index, granule) in granules.iter().enumerate() {
        let granule_bytes: usize = block_columns
            .iter()
            .map(|&column_id| {
                let values = match granule.blocks.iter().find(|block| block.column_id == column_id) {
                    Some(block) => block.data.row_count(),
                    None => source_block_values
                        .get(&(column_id, granule.entry.granule_id))
                        .copied()
                        .unwrap_or_default(),
                };
                values * 8
            })
            .sum::<usize>()
            + granule.entry.compressed_bytes_estimate as usize
            + freetext_row_offsets_by_granule
                .get(granule_index)
                .map(|indexes| {
                    indexes
                        .iter()
                        .map(|(_, bytes, offsets)| bytes.len() + offsets.len())
                        .sum::<usize>()
                })
                .unwrap_or_default();
        if (config.profile == Profile::Snapshot || stripe_bytes + granule_bytes > config.targets.stripe_target_bytes)
            && stripe_bytes > 0
        {
            stripe_id += 1;
            stripe_bytes = 0;
        }
        if granule_bytes > config.targets.max_stripe_bytes {
            return Err(FormatError::Structural {
                rule: "stripe clamp: a single granule exceeds the maximum stripe size",
            });
        }
        stripe_bytes += granule_bytes;
        stripe_of_granule.push(stripe_id);
    }
    for (granule, assigned) in granules.iter_mut().zip(stripe_of_granule.iter()) {
        granule.entry.stripe_id = *assigned;
    }

    struct EncodedPiece<'a> {
        body: Cow<'a, [u8]>,
        column_id: u32,
        granule_id: u32,
        pages: Vec<(u64, crate::encoding::PipelineId, crate::encoding::BlockStats, u32, u32)>,
        pipeline: crate::encoding::PipelineId,
        prefix: Cow<'a, [u8]>,
        row_count: u32,
        stats: crate::encoding::BlockStats,
        stripe_id: u32,
        uncompressed_len: u64,
    }

    impl EncodedPiece<'_> {
        fn len(&self) -> usize {
            self.prefix.len() + self.body.len()
        }

        fn first_byte(&self) -> Option<u8> {
            self.prefix.first().or_else(|| self.body.first()).copied()
        }

        fn write_to(&self, out: &mut Writer) {
            out.put_slice(self.prefix.as_ref());
            out.put_slice(self.body.as_ref());
        }

        fn alias_key(&self) -> u64 {
            use std::hash::Hasher;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            hasher.write(self.prefix.as_ref());
            hasher.write(self.body.as_ref());
            hasher.finish()
        }
    }

    struct PayloadGeometry<'a> {
        dictionary: Cow<'a, [u8]>,
        freetext_row_offsets: Vec<FreetextOffsetIndex<'a>>,
        granule_id: u32,
        offsets: Cow<'a, [u8]>,
        residual: Cow<'a, [u8]>,
        residual_compression: ResidualCompression,
    }

    fn encode_mini_block(
        data: &ColumnData,
        presence: &[u8],
        random_access: bool,
        strategy: CascadeStrategy,
        shared: Option<&[String]>,
    ) -> (
        Vec<u8>,
        Vec<u8>,
        crate::encoding::PipelineId,
        crate::encoding::BlockStats,
        u64,
    ) {
        let encoded = encode_block_with_shared_dictionary(data, random_access, strategy, shared);
        let mut prefix = Writer::with_capacity(presence.len() + 5);
        crate::layout::encode_presence(presence, data.row_count() as u32, &mut prefix);
        let uncompressed_len = 1 + presence.len() as u64 + encoded.uncompressed_len;
        (
            prefix.into_bytes(),
            encoded.bytes,
            encoded.pipeline,
            encoded.stats,
            uncompressed_len,
        )
    }

    fn slice_column_data(data: &ColumnData, start: usize, end: usize) -> ColumnData {
        fn span<T: Clone>(values: &[T], start: usize, end: usize) -> Vec<T> {
            values.get(start..end).map(<[T]>::to_vec).unwrap_or_default()
        }
        match data {
            ColumnData::Decimal { values, scale } => ColumnData::Decimal {
                values: span(values, start, end),
                scale: *scale,
            },
            ColumnData::F64(v) => ColumnData::F64(span(v, start, end)),
            ColumnData::I64(v) => ColumnData::I64(span(v, start, end)),
            ColumnData::Strings(v) => ColumnData::Strings(v.slice(start, end)),
            ColumnData::U128(v) => ColumnData::U128(span(v, start, end)),
            ColumnData::U64(v) => ColumnData::U64(span(v, start, end)),
        }
    }

    fn encoded_text_tokens(data: &ColumnData, include_ngrams: bool) -> Option<Vec<u8>> {
        let ColumnData::Strings(values) = data else {
            return None;
        };
        Some(TextTokenIndex::build_from_iter(values.iter_present(), include_ngrams).encode())
    }

    let searchable_text_columns: HashMap<u32, bool> = config
        .promotion
        .columns
        .iter()
        .enumerate()
        .filter(|(_, promoted)| promoted.kind == ColumnKind::String)
        .map(|(index, promoted)| (column_ids::PROMOTED_BASE + index as u32, promoted.substring_searchable))
        .chain(freetext_entries.iter().map(|entry| (entry.column_id, true)))
        .chain(
            config
                .analytical_columns
                .iter()
                .filter(|col| col.kind == ColumnKind::String && !col.internal_only)
                .map(|col| (col.column_id, col.substring_searchable)),
        )
        .collect();

    struct BlockProduct<'a> {
        fsst: Option<FsstTable>,
        piece: EncodedPiece<'a>,
        reused: bool,
        text_tokens: Vec<TextTokenEntry>,
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is an independent per-block input"
    )]
    fn encode_block_piece(
        block: &PendingBlock,
        entry: &GranuleEntry,
        stripe: u32,
        page_size: usize,
        strategy: CascadeStrategy,
        searchable_text_columns: &HashMap<u32, bool>,
        replay: Option<&ReplayCapture>,
        shared: Option<&[String]>,
        xor_floats: bool,
        occurred: Option<&[i64]>,
    ) -> BlockProduct<'static> {
        let encode = |data: &ColumnData| {
            if xor_floats && matches!(data, ColumnData::F64(_)) {
                encode_block_with_xor_floats(data, block.random_access, strategy)
            } else {
                let string_zstd = block.random_access
                    && (column_ids::SHREDDED_BASE..column_ids::CONTEXT_BASE).contains(&block.column_id);
                encode_block_replayed_with(data, block.random_access, strategy, replay, shared, string_zstd).0
            }
        };
        let mut text_tokens: Vec<TextTokenEntry> = Vec::new();
        let mut fsst = None;
        let granule_row_count = entry.row_count as usize;

        let piece = if page_size > 0 && granule_row_count > page_size && block.presence.is_empty() {
            let page_count = granule_row_count.div_ceil(page_size);
            let mut all_bytes = Writer::with_capacity(page_count * 5);
            let mut page_info = Vec::with_capacity(page_count);
            let mut uncompressed_total = 0u64;
            for page_idx in 0..page_count {
                let page_start = page_idx * page_size;
                let page_len = (page_start + page_size).min(granule_row_count) - page_start;
                let page_data = slice_column_data(&block.data, page_start, page_start + page_len);
                let mut encoded = encode(&page_data);
                if fsst.is_none() {
                    fsst = encoded.fsst.take();
                }
                let before = all_bytes.len() as u64;
                crate::layout::encode_presence(&[], page_len as u32, &mut all_bytes);
                all_bytes.put_u32(encoded.pipeline.0);
                all_bytes.put_slice(&encoded.bytes);
                let page_bytes = all_bytes.len() as u64 - before;
                uncompressed_total += 5 + encoded.uncompressed_len;
                if let Some(&include_ngrams) = searchable_text_columns.get(&block.column_id)
                    && let Some(index_bytes) = encoded_text_tokens(&page_data, include_ngrams)
                {
                    text_tokens.push(TextTokenEntry {
                        column_id: block.column_id,
                        granule_id: entry.granule_id,
                        index_bytes,
                        page_index: page_idx as u32,
                    });
                }
                page_info.push((
                    page_bytes,
                    encoded.pipeline,
                    encoded.stats,
                    page_start as u32,
                    page_len as u32,
                ));
            }
            let first_pipeline = page_info
                .first()
                .map(|(_, p, _, _, _)| *p)
                .unwrap_or(crate::encoding::PipelineId(0));
            let agg_stats = aggregate_stats(page_info.iter().map(|(_, _, s, _, _)| *s));
            EncodedPiece {
                column_id: block.column_id,
                granule_id: entry.granule_id,
                stripe_id: stripe,
                body: Cow::Owned(all_bytes.into_bytes()),
                prefix: Cow::Borrowed(&[]),
                pipeline: first_pipeline,
                stats: agg_stats,
                row_count: entry.row_count,
                pages: page_info,
                uncompressed_len: uncompressed_total,
            }
        } else if page_size > 0 && granule_row_count > page_size && !block.presence.is_empty() {
            let page_count = granule_row_count.div_ceil(page_size);
            let mut all_bytes = Writer::with_capacity(block.presence.len() + page_count * 5);
            let mut page_info = Vec::with_capacity(page_count);
            let mut value_offset = 0usize;
            let mut uncompressed_total = 0u64;
            for page_idx in 0..page_count {
                let page_start = page_idx * page_size;
                let page_len = (page_start + page_size).min(granule_row_count) - page_start;
                let page_presence = slice_presence_bits(&block.presence, page_start, page_len);
                let present_count = count_set_bits(&page_presence, page_len);
                let page_data = slice_column_data(&block.data, value_offset, value_offset + present_count);
                value_offset += present_count;
                let mut encoded = encode(&page_data);
                if fsst.is_none() {
                    fsst = encoded.fsst.take();
                }
                let before = all_bytes.len() as u64;
                crate::layout::encode_presence(&page_presence, page_len as u32, &mut all_bytes);
                all_bytes.put_u32(encoded.pipeline.0);
                all_bytes.put_slice(&encoded.bytes);
                let page_bytes = all_bytes.len() as u64 - before;
                uncompressed_total += 5 + page_presence.len() as u64 + encoded.uncompressed_len;
                if let Some(&include_ngrams) = searchable_text_columns.get(&block.column_id)
                    && let Some(index_bytes) = encoded_text_tokens(&page_data, include_ngrams)
                {
                    text_tokens.push(TextTokenEntry {
                        column_id: block.column_id,
                        granule_id: entry.granule_id,
                        index_bytes,
                        page_index: page_idx as u32,
                    });
                }
                page_info.push((
                    page_bytes,
                    encoded.pipeline,
                    encoded.stats,
                    page_start as u32,
                    page_len as u32,
                ));
            }
            let first_pipeline = page_info
                .first()
                .map(|(_, p, _, _, _)| *p)
                .unwrap_or(crate::encoding::PipelineId(0));
            let agg_stats = aggregate_stats(page_info.iter().map(|(_, _, s, _, _)| *s));
            EncodedPiece {
                column_id: block.column_id,
                granule_id: entry.granule_id,
                stripe_id: stripe,
                body: Cow::Owned(all_bytes.into_bytes()),
                prefix: Cow::Borrowed(&[]),
                pipeline: first_pipeline,
                stats: agg_stats,
                row_count: entry.row_count,
                pages: page_info,
                uncompressed_len: uncompressed_total,
            }
        } else {
            let mut encoded = encode(&block.data);
            if block.column_id == column_ids::INGESTED_AT
                && block.presence.is_empty()
                && let (ColumnData::I64(ingested), Some(occurred)) = (&block.data, occurred)
                && encoded.stats.min_i128 != encoded.stats.max_i128
                && let Some(residual) = encode_occurred_residual(ingested, occurred, strategy)
                && residual.bytes.len() < encoded.bytes.len()
            {
                encoded = residual;
            }
            fsst = encoded.fsst.take();
            let mut prefix = Writer::with_capacity(block.presence.len() + 8);
            crate::layout::encode_presence(&block.presence, entry.row_count, &mut prefix);
            let uncompressed_len = 1 + block.presence.len() as u64 + encoded.uncompressed_len;
            if let Some(&include_ngrams) = searchable_text_columns.get(&block.column_id)
                && let Some(index_bytes) = encoded_text_tokens(&block.data, include_ngrams)
            {
                text_tokens.push(TextTokenEntry {
                    column_id: block.column_id,
                    granule_id: entry.granule_id,
                    index_bytes,
                    page_index: 0,
                });
            }
            EncodedPiece {
                column_id: block.column_id,
                granule_id: entry.granule_id,
                stripe_id: stripe,
                body: Cow::Owned(encoded.bytes),
                prefix: Cow::Owned(prefix.into_bytes()),
                pipeline: encoded.pipeline,
                stats: encoded.stats,
                row_count: entry.row_count,
                pages: Vec::new(),
                uncompressed_len,
            }
        };
        BlockProduct {
            fsst,
            piece,
            reused: false,
            text_tokens,
        }
    }

    fn reuse_block_piece<'a>(
        index: &SourceIndex<'a>,
        entry: &GranuleEntry,
        column_id: u32,
        stripe_id: u32,
        page_size_rows: usize,
    ) -> Option<BlockProduct<'a>> {
        let source = index.source;
        let key = (column_id, entry.granule_id);
        let source_granule = index.granules.get(&entry.granule_id).filter(|granule| {
            granule.first_row_ordinal == entry.first_row_ordinal && granule.row_count == entry.row_count
        })?;
        let mark = *index.marks.get(&key)?;
        let expected_pages = if page_size_rows > 0 && entry.row_count as usize > page_size_rows {
            (entry.row_count as usize).div_ceil(page_size_rows) as u32
        } else {
            1
        };
        if mark.row_count != entry.row_count || mark.page_count != expected_pages {
            return None;
        }
        let stats = index.page_stats.get(&key)?;
        let stats = BlockStats {
            max_f64: stats.max_f64,
            max_i128: stats.max_i128,
            min_f64: stats.min_f64,
            min_i128: stats.min_i128,
            null_count: stats.null_count,
            row_count: stats.row_count,
        };
        let stripe_offset = *index.stripe_offsets.get(&source_granule.stripe_id)?;
        let start = usize::try_from(stripe_offset.checked_add(mark.compressed_offset)?).ok()?;
        let end = start.checked_add(usize::try_from(mark.compressed_size).ok()?)?;
        let stored = source.bytes.get(start..end)?;
        let (body, prefix) = if mark.compressed_size == 0 {
            (
                Cow::Borrowed(&[][..]),
                Cow::Owned(vec![crate::layout::presence_forms::EMPTY]),
            )
        } else {
            (Cow::Borrowed(stored), Cow::Borrowed(&[][..]))
        };
        let pages = if mark.page_count > 1 {
            let page_entries = index.pages.get(&key).map(Vec::as_slice).unwrap_or_default();
            if page_entries.len() != mark.page_count as usize {
                return None;
            }
            page_entries
                .iter()
                .map(|page| {
                    Some((
                        page.compressed_len,
                        mark.codec_pipeline_id,
                        BlockStats {
                            max_f64: page.max_f64,
                            max_i128: page.max_i128,
                            min_f64: page.min_f64,
                            min_i128: page.min_i128,
                            null_count: page.null_count,
                            row_count: page.row_count,
                        },
                        u32::try_from(page.first_row_ordinal.checked_sub(entry.first_row_ordinal)?).ok()?,
                        page.row_count,
                    ))
                })
                .collect::<Option<Vec<_>>>()?
        } else {
            Vec::new()
        };

        let mut text_tokens: Vec<TextTokenEntry> = index
            .text_token_indexes
            .get(&key)
            .into_iter()
            .flatten()
            .map(|token| (*token).clone())
            .collect();
        for offsets in index.text_token_offsets.get(&key).into_iter().flatten() {
            let start = usize::try_from(stripe_offset.checked_add(offsets.index_offset)?).ok()?;
            let end = start.checked_add(usize::try_from(offsets.index_len).ok()?)?;
            text_tokens.push(TextTokenEntry {
                column_id,
                granule_id: entry.granule_id,
                index_bytes: source.bytes.get(start..end)?.to_vec(),
                page_index: offsets.page_index,
            });
        }
        Some(BlockProduct {
            fsst: None,
            piece: EncodedPiece {
                body,
                column_id,
                granule_id: entry.granule_id,
                pages,
                pipeline: mark.codec_pipeline_id,
                prefix,
                row_count: entry.row_count,
                stats,
                stripe_id,
                uncompressed_len: mark.uncompressed_size,
            },
            reused: true,
            text_tokens,
        })
    }

    let reuse_source_alphabets = reuse_path_plan
        && update_reuse.is_some_and(|reuse| {
            !reuse.changed_columns_by_granule.values().any(|changed| {
                changed.iter().any(|column_id| {
                    shareable_column(*column_id)
                        && reuse
                            .source
                            .footer
                            .columns
                            .iter()
                            .any(|column| column.column_id == *column_id && column.kind == ColumnKind::String)
                })
            })
        });
    let source_shared: Option<BTreeMap<u32, Vec<String>>> = match update_reuse {
        Some(reuse) => source_shared_alphabets(&reuse.source.footer),
        None => Some(BTreeMap::new()),
    };
    let mut union_columns: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    let mut shared_alphabets: BTreeMap<u32, Vec<String>> = if reuse_source_alphabets {
        union_columns.extend(
            update_reuse
                .into_iter()
                .flat_map(|reuse| &reuse.source.footer.packed_shared_dictionaries)
                .map(|entry| entry.column_id),
        );
        source_shared.clone().unwrap_or_default()
    } else {
        BTreeMap::new()
    };
    if granules.len() > 1 && !reuse_source_alphabets {
        fn observe_alphabet<'a>(
            column_id: u32,
            values: impl Iterator<Item = &'a str>,
            candidates: &mut BTreeMap<u32, (std::collections::BTreeSet<&'a str>, usize)>,
            settled: &mut HashSet<u32>,
        ) {
            if settled.contains(&column_id) {
                return;
            }
            let mut set: std::collections::BTreeSet<&'a str> = std::collections::BTreeSet::new();
            for value in values {
                if set.len() > SHARED_DICTIONARY_MAX_VALUES {
                    break;
                }
                set.insert(value);
            }
            match candidates.entry(column_id) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert((set, 1));
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    let (first, matching) = slot.get_mut();
                    settled.insert(column_id);
                    if *first == set {
                        *matching += 1;
                    }
                }
            }
        }
        let mut candidates: BTreeMap<u32, (std::collections::BTreeSet<&str>, usize)> = BTreeMap::new();
        let mut settled: HashSet<u32> = HashSet::default();
        for granule in &granules {
            for block in &granule.blocks {
                if shareable_column(block.column_id)
                    && let ColumnData::Strings(values) = &block.data
                {
                    observe_alphabet(block.column_id, values.iter_present(), &mut candidates, &mut settled);
                }
            }
        }
        for col in &config.analytical_columns {
            if shareable_column(col.column_id)
                && let ColumnData::Strings(values) = &col.data
            {
                for (g_start, g_end) in &granule_row_ranges {
                    let present = (*g_start..*g_end).filter_map(|index| values.get(index).flatten());
                    observe_alphabet(col.column_id, present, &mut candidates, &mut settled);
                }
            }
        }
        for (column_id, (first, matching)) in candidates {
            if matching > 1
                && !first.is_empty()
                && first.len() <= SHARED_DICTIONARY_MAX_VALUES
                && first.iter().all(|value| value.len() <= u16::MAX as usize)
            {
                shared_alphabets.insert(column_id, first.iter().map(|value| (*value).to_owned()).collect());
            }
        }
        for column_id in UNION_SHARED_DICTIONARY_COLUMNS {
            if shared_alphabets.contains_key(&column_id) {
                continue;
            }
            let mut granule_values: Vec<Box<dyn Iterator<Item = &str> + Send + '_>> = Vec::new();
            for granule in &granules {
                for block in &granule.blocks {
                    if block.column_id == column_id
                        && let ColumnData::Strings(values) = &block.data
                    {
                        granule_values.push(Box::new(values.iter_present()));
                    }
                }
            }
            for col in &config.analytical_columns {
                if col.column_id == column_id
                    && let ColumnData::Strings(values) = &col.data
                {
                    for (g_start, g_end) in &granule_row_ranges {
                        granule_values.push(Box::new(
                            (*g_start..*g_end).filter_map(|index| values.get(index).flatten()),
                        ));
                    }
                }
            }
            if let Some(alphabet) = union_alphabet(granule_values) {
                shared_alphabets.insert(column_id, alphabet);
                union_columns.insert(column_id);
            }
        }
    }

    let update_reuse = update_reuse.filter(|reuse| {
        reuse.source.footer.columns == columns
            && reuse.source.footer.dictionaries == dictionaries
            && reuse.source.footer.freetext == freetext_entries
            && reuse.source.footer.io_alignment_bytes == config.io_alignment_bytes
            && reuse.source.footer.presence == presence_entries
            && reuse.source.footer.shredded == shred_plan
            && source_shared.as_ref().is_some_and(|source_shared| {
                source_shared
                    .iter()
                    .all(|(column_id, values)| shared_alphabets.get(column_id) == Some(values))
            })
            && reuse.source.footer.granules.len() == granules.len()
            && reuse
                .source
                .footer
                .granules
                .iter()
                .zip(&granules)
                .all(|(source, candidate)| {
                    source.granule_id == candidate.entry.granule_id
                        && source.first_row_ordinal == candidate.entry.first_row_ordinal
                        && source.row_count == candidate.entry.row_count
                        && source.stripe_id == candidate.entry.stripe_id
                })
    });
    if let Some(reuse) = sparse_reuse
        && update_reuse.is_none()
    {
        return sparse_fallback(rows, payloads, config, encode, sink, phase_clock, reuse);
    }

    let job_list: Vec<(usize, u32)> = (0..granules.len())
        .flat_map(|granule_idx| block_columns.iter().map(move |&column_id| (granule_idx, column_id)))
        .collect();
    let slots: Vec<Mutex<Option<Result<BlockProduct, FormatError>>>> =
        job_list.iter().map(|_| Mutex::new(None)).collect();
    let is_head = |granule_idx: usize| granule_idx.is_multiple_of(REPLAY_SEGMENT_GRANULES);
    let (head_jobs, tail_jobs): (Vec<usize>, Vec<usize>) =
        (0..job_list.len()).partition(|idx| job_list.get(*idx).is_some_and(|(granule_idx, _)| is_head(*granule_idx)));
    let run_phase = |jobs: &[usize], captures: &HashMap<(u32, usize), ReplayCapture>| {
        encode.run_jobs(jobs.len(), &|phase_idx| {
            maybe_profile_block_encoding(profile_workers, || {
                let (Some(&(granule_idx, column_id)), Some(slot)) = (
                    jobs.get(phase_idx).and_then(|job_idx| job_list.get(*job_idx)),
                    jobs.get(phase_idx).and_then(|job_idx| slots.get(*job_idx)),
                ) else {
                    return;
                };
                let Some(granule) = granules.get(granule_idx) else {
                    return;
                };
                if let Some(product) = update_reuse
                    .zip(source_index.as_ref())
                    .and_then(|(reuse, source_index)| {
                        (!reuse.block_changed(granule.entry.granule_id, column_id))
                            .then(|| {
                                reuse_block_piece(
                                    source_index,
                                    &granule.entry,
                                    column_id,
                                    granule.entry.stripe_id,
                                    config.page_size_rows,
                                )
                            })
                            .flatten()
                    })
                {
                    *slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Ok(product));
                    return;
                }
                let product = match granule.blocks.iter().find(|block| block.column_id == column_id) {
                    Some(block) => Ok(encode_block_piece(
                        block,
                        &granule.entry,
                        granule.entry.stripe_id,
                        config.page_size_rows,
                        config.lifecycle.cascade_strategy(),
                        &searchable_text_columns,
                        captures.get(&(column_id, granule_idx / REPLAY_SEGMENT_GRANULES)),
                        shared_alphabets.get(&column_id).map(Vec::as_slice),
                        config.profile == Profile::TimeSeries,
                        (column_id == column_ids::INGESTED_AT)
                            .then(|| {
                                granule.blocks.iter().find_map(|occurred| match occurred {
                                    PendingBlock {
                                        column_id: column_ids::OCCURRED_AT,
                                        data: ColumnData::I64(values),
                                        presence,
                                        ..
                                    } if presence.is_empty() => Some(values.as_slice()),
                                    _ => None,
                                })
                            })
                            .flatten(),
                    )),
                    None => Err(FormatError::Structural {
                        rule: "a sparse update encodes only blocks it built",
                    }),
                };
                *slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(product);
            });
        });
    };
    run_phase(&head_jobs, &HashMap::default());
    let mut captures: HashMap<(u32, usize), ReplayCapture> = HashMap::default();
    for &job_idx in &head_jobs {
        let Some(&(granule_idx, column_id)) = job_list.get(job_idx) else {
            continue;
        };
        let Some(block) = granules
            .get(granule_idx)
            .and_then(|granule| granule.blocks.iter().find(|block| block.column_id == column_id))
        else {
            continue;
        };
        let head = slots.get(job_idx).and_then(|slot| {
            let mut guard = slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.as_mut().and_then(|product| product.as_mut().ok()).map(|product| {
                (
                    product.piece.pipeline,
                    product.piece.uncompressed_len,
                    product.fsst.take(),
                )
            })
        });
        if let Some((pipeline, decoded_len, fsst)) = head
            && let Some(capture) = ReplayCapture::from_head(&block.data, pipeline, decoded_len, fsst)
        {
            captures.insert((block.column_id, granule_idx / REPLAY_SEGMENT_GRANULES), capture);
        }
    }
    run_phase(&tail_jobs, &captures);

    let mut pieces: Vec<EncodedPiece<'_>> = Vec::with_capacity(slots.len());
    let mut text_token_indexes: Vec<TextTokenEntry> =
        Vec::with_capacity(searchable_text_columns.len().saturating_mul(granules.len()));
    let mut reused_source_block_bytes = 0u64;
    let mut reused_source_blocks = 0u64;
    for slot in slots {
        let product = slot
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ok_or(FormatError::Structural {
                rule: "the executor runs every job",
            })??;
        text_token_indexes.extend(product.text_tokens);
        if product.reused {
            reused_source_block_bytes += product.piece.len() as u64;
            reused_source_blocks += 1;
        }
        pieces.push(product.piece);
    }

    let mut payload_geometry: Vec<PayloadGeometry<'_>> = Vec::with_capacity(granules.len());
    for (granule_index, granule) in granules.iter_mut().enumerate() {
        let freetext_row_offsets = freetext_row_offsets_by_granule
            .get_mut(granule_index)
            .map(std::mem::take)
            .unwrap_or_default();
        let reused_geometry = update_reuse
            .zip(source_index.as_ref())
            .and_then(|(reuse, source_index)| {
                if reuse.changed_payload_granules.contains(&granule.entry.granule_id) {
                    return None;
                }
                let payload = source_index.payload_granules.get(&granule.entry.granule_id)?;
                let extent = |offset: u64, len: u64| source_index.extent(granule.entry.granule_id, offset, len);
                Some((
                    extent(payload.dictionary_offset, payload.dictionary_len)?,
                    extent(payload.offsets_offset, payload.offsets_len)?,
                    extent(payload.residual_offset, payload.residual_len)?,
                    payload.residual_compression,
                ))
            });
        if let Some((dictionary, offsets, residual, residual_compression)) = reused_geometry {
            payload_geometry.push(PayloadGeometry {
                granule_id: granule.entry.granule_id,
                dictionary: Cow::Borrowed(dictionary),
                freetext_row_offsets,
                offsets: Cow::Borrowed(offsets),
                residual: Cow::Borrowed(residual),
                residual_compression,
            });
            continue;
        }
        let (residual, residual_compression) = match config.lifecycle {
            BuildLifecycle::FreshPublication | BuildLifecycle::FreshPublicationWithoutTrailing => {
                (std::mem::take(&mut granule.residual_bytes), ResidualCompression::None)
            }
            BuildLifecycle::RewriteOrCompaction => compress_residual(std::mem::take(&mut granule.residual_bytes)),
        };
        payload_geometry.push(PayloadGeometry {
            granule_id: granule.entry.granule_id,
            dictionary: Cow::Owned(std::mem::take(&mut granule.dictionary_bytes)),
            freetext_row_offsets,
            offsets: Cow::Owned(std::mem::take(&mut granule.offsets_bytes)),
            residual: Cow::Owned(residual),
            residual_compression,
        });
    }

    struct AnalyticalProduct {
        piece: EncodedPiece<'static>,
        text_token: Option<TextTokenEntry>,
    }
    let analytical_jobs: Vec<(usize, usize)> = (0..config.analytical_columns.len())
        .flat_map(|col_idx| (0..granules.len()).map(move |granule_idx| (col_idx, granule_idx)))
        .collect();
    let analytical_slots: Vec<Mutex<Option<AnalyticalProduct>>> =
        analytical_jobs.iter().map(|_| Mutex::new(None)).collect();
    encode.run_jobs(analytical_jobs.len(), &|job_idx| {
        maybe_profile_block_encoding(profile_workers, || {
            let (Some(&(col_idx, granule_idx)), Some(slot)) =
                (analytical_jobs.get(job_idx), analytical_slots.get(job_idx))
            else {
                return;
            };
            let (Some(col), Some(&(g_start, g_end)), Some(granule)) = (
                config.analytical_columns.get(col_idx),
                granule_row_ranges.get(granule_idx),
                granules.get(granule_idx),
            ) else {
                return;
            };
            let granule_data = slice_column_data(&col.data, g_start, g_end);
            let (prefix, body, pipeline, stats, uncompressed_len) = encode_mini_block(
                &granule_data,
                &[],
                false,
                config.lifecycle.cascade_strategy(),
                shared_alphabets.get(&col.column_id).map(Vec::as_slice),
            );
            let text_token = searchable_text_columns
                .get(&col.column_id)
                .and_then(|&include_ngrams| encoded_text_tokens(&granule_data, include_ngrams))
                .map(|index_bytes| TextTokenEntry {
                    column_id: col.column_id,
                    granule_id: granule.entry.granule_id,
                    index_bytes,
                    page_index: 0,
                });
            let piece = EncodedPiece {
                body: Cow::Owned(body),
                column_id: col.column_id,
                granule_id: granule.entry.granule_id,
                pages: Vec::new(),
                pipeline,
                prefix: Cow::Owned(prefix),
                row_count: granule.entry.row_count,
                stats,
                stripe_id: stripe_of_granule.get(granule_idx).copied().unwrap_or_default(),
                uncompressed_len,
            };
            *slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(AnalyticalProduct { piece, text_token });
        });
    });
    for slot in analytical_slots {
        let product = slot
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ok_or(FormatError::Structural {
                rule: "the executor runs every job",
            })?;
        if let Some(text_token) = product.text_token {
            text_token_indexes.push(text_token);
        }
        pieces.push(product.piece);
    }
    let encoded_blocks = pieces.len() as u64 - reused_source_blocks;
    let xor_float_blocks = pieces.iter().any(|piece| {
        std::iter::once(piece.pipeline)
            .chain(piece.pages.iter().map(|page| page.1))
            .any(|pipeline| pipeline.transform() == Ok(Transform::Chimp))
    });
    let occurred_relative_ingested_at = pieces.iter().any(|piece| {
        std::iter::once(piece.pipeline)
            .chain(piece.pages.iter().map(|page| page.1))
            .any(|pipeline| pipeline.transform() == Ok(Transform::OccurredResidual))
    });
    let string_zstd_blocks = pieces.iter().any(|piece| {
        std::iter::once(piece.pipeline)
            .chain(piece.pages.iter().map(|page| page.1))
            .any(|pipeline| pipeline.compression() == Ok(Compression::StringZstd))
    });
    let packed_string_offsets = pieces.iter().any(|piece| {
        std::iter::once(piece.pipeline)
            .chain(piece.pages.iter().map(|page| page.1))
            .any(|pipeline| pipeline.transform() == Ok(Transform::FsstPackedString))
    });

    let hot_columns = column_ids::PROMOTED_BASE..column_ids::PROMOTED_BASE + config.promotion.columns.len() as u32;
    phase_clock.transition(BuildPhase::FooterConstruction);
    let mut scheduled_metadata = match source_index.as_ref().filter(|_| source.is_some()) {
        Some(source_index) => borrow_footer_metadata(source_index)?,
        None => build_scheduled_footer_metadata(
            &granules,
            &dictionaries,
            &hot_columns,
            row_count as u64,
            encode,
            profile_workers,
        ),
    };
    if order == RowOrder::Entity {
        for entry in &mut scheduled_metadata.clustering {
            entry.sortedness_proof = Some(order.sortedness_proof());
        }
    }
    phase_clock.transition(BuildPhase::Layout);

    let mut text_tokens_by_granule: BTreeMap<u32, Vec<TextTokenEntry>> = BTreeMap::new();
    for entry in text_token_indexes {
        text_tokens_by_granule.entry(entry.granule_id).or_default().push(entry);
    }

    for piece in &pieces {
        let oversize = if piece.pages.is_empty() {
            piece.len() as u64 > MAX_PAGE_BYTES
        } else {
            piece.pages.iter().any(|(page_bytes, ..)| *page_bytes > MAX_PAGE_BYTES)
        };
        if oversize {
            return Err(FormatError::Structural {
                rule: "column block exceeds the maximum page size",
            });
        }
    }

    let estimated: usize = pieces.iter().map(EncodedPiece::len).sum::<usize>()
        + payload_geometry
            .iter()
            .map(|g| g.dictionary.len() + g.offsets.len() + g.residual.len())
            .sum::<usize>();
    let layout_class = if estimated < config.targets.min_bytes_for_wide {
        LayoutClass::Compact
    } else {
        LayoutClass::Wide
    };

    let entity_hash_filters_by_granule = &scheduled_metadata.entity_hash_filters;
    match layout_class {
        LayoutClass::Compact => {
            pieces.sort_by_key(|piece| (piece.stripe_id, piece.granule_id, piece.column_id));
        }
        LayoutClass::Wide => {
            pieces.sort_by_key(|piece| (piece.stripe_id, piece.column_id, piece.granule_id));
        }
    }

    let granule_by_id: BTreeMap<u32, &GranuleEntry> = granules.iter().map(|g| (g.entry.granule_id, &g.entry)).collect();

    let block_alignment = if config.io_alignment_bytes > 0 {
        (config.io_alignment_bytes as usize).max(64)
    } else {
        64
    };
    let mut payload_idx_by_stripe: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (idx, stripe) in stripe_of_granule.iter().enumerate() {
        payload_idx_by_stripe.entry(*stripe).or_default().push(idx);
    }
    let mut stripe_piece_ranges: Vec<(u32, Range<usize>)> = Vec::new();
    for (idx, piece) in pieces.iter().enumerate() {
        match stripe_piece_ranges.last_mut() {
            Some((stripe_id, range)) if *stripe_id == piece.stripe_id => range.end = idx + 1,
            _ => stripe_piece_ranges.push((piece.stripe_id, idx..idx + 1)),
        }
    }

    struct LaidOutStripe {
        aliased_block_bytes: u64,
        bytes: Vec<u8>,
        crc: crate::file::integrity::StreamingCrc64Nvme,
        entity_hash_filters: Vec<EntityHashFilterEntry>,
        freetext_row_offsets: Vec<FreetextRowOffsets>,
        graph_blocks: Vec<GraphBlockEntry>,
        integrity: crate::file::integrity::BuiltOutboard,
        marks: Vec<ColumnMark>,
        marks_page_offsets: Vec<MarksPageEntry>,
        page_directory: Vec<PageDirectoryEntry>,
        page_minmax: Vec<PageMinMax>,
        page_stats: Vec<PageStats>,
        payload_granules: Vec<PayloadGranule>,
        stripe_len: u64,
        text_token_offsets: Vec<TextTokenOffsetsEntry>,
    }

    struct StripeLayout<'a, 'p> {
        block_alignment: usize,
        entity_hash_filters_by_granule: &'a BTreeMap<u32, Vec<u8>>,
        granule_by_id: &'a BTreeMap<u32, &'a GranuleEntry>,
        graph_blocks_by_granule: &'a BTreeMap<u32, Vec<u8>>,
        hot_columns: Range<u32>,
        payload_geometry: &'a [PayloadGeometry<'p>],
        payload_idx_by_stripe: &'a BTreeMap<u32, Vec<usize>>,
        pieces: &'a [EncodedPiece<'p>],
        profile_workers: bool,
        stripe_piece_ranges: &'a [(u32, Range<usize>)],
        text_tokens_by_granule: &'a BTreeMap<u32, Vec<TextTokenEntry>>,
    }

    impl StripeLayout<'_, '_> {
        fn lay_out(&self, index: usize) -> LaidOutStripe {
            let (stripe_id, piece_range) = self.stripe_piece_ranges.get(index).cloned().unwrap_or_default();
            let pieces = self.pieces.get(piece_range).unwrap_or_default();
            let geometry: Vec<&PayloadGeometry<'_>> = self
                .payload_idx_by_stripe
                .get(&stripe_id)
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .filter_map(|&idx| self.payload_geometry.get(idx))
                .collect();
            let text_tokens = |granule_id: u32| {
                self.text_tokens_by_granule
                    .get(&granule_id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
            };
            let text_token_count: usize = geometry.iter().map(|g| text_tokens(g.granule_id).len()).sum();
            let entity_filter_count = geometry
                .iter()
                .filter(|g| self.entity_hash_filters_by_granule.contains_key(&g.granule_id))
                .count();
            let estimated: usize = pieces.iter().map(EncodedPiece::len).sum::<usize>().saturating_add(
                geometry
                    .iter()
                    .map(|g| g.dictionary.len() + g.offsets.len() + g.residual.len())
                    .sum::<usize>(),
            );
            let filter_bytes: usize = geometry
                .iter()
                .map(|g| {
                    text_tokens(g.granule_id)
                        .iter()
                        .map(|entry| entry.index_bytes.len())
                        .sum::<usize>()
                        .saturating_add(
                            self.entity_hash_filters_by_granule
                                .get(&g.granule_id)
                                .map_or(0, Vec::len),
                        )
                })
                .sum();
            let padded_regions = pieces
                .len()
                .saturating_add(geometry.len().saturating_mul(PADDED_REGIONS_PER_GRANULE))
                .saturating_add(text_token_count)
                .saturating_add(entity_filter_count);
            let page_entries: usize = pieces.iter().map(|piece| piece.pages.len().max(1)).sum();
            let hot_page_entries: usize = pieces
                .iter()
                .filter(|piece| self.hot_columns.contains(&piece.column_id))
                .map(|piece| piece.pages.len())
                .sum();
            let capacity = estimated
                .saturating_add(filter_bytes)
                .saturating_add(padded_regions.saturating_mul(self.block_alignment))
                .saturating_add(page_entries.saturating_mul(MARKS_PAGE_BYTES_PER_BLOCK));
            let mut data = Writer::with_capacity(capacity);
            let mut marks: Vec<ColumnMark> = Vec::with_capacity(pieces.len());
            let mut page_directory: Vec<PageDirectoryEntry> = Vec::with_capacity(page_entries);
            let mut page_minmax: Vec<PageMinMax> = Vec::with_capacity(hot_page_entries);
            let mut page_stats: Vec<PageStats> = Vec::with_capacity(pieces.len());
            let mut payload_granules: Vec<PayloadGranule> = Vec::with_capacity(geometry.len());
            let mut freetext_row_offsets: Vec<FreetextRowOffsets> =
                Vec::with_capacity(geometry.iter().map(|g| g.freetext_row_offsets.len()).sum());
            let mut text_token_offsets: Vec<TextTokenOffsetsEntry> = Vec::with_capacity(text_token_count);
            let mut entity_hash_filters: Vec<EntityHashFilterEntry> = Vec::with_capacity(entity_filter_count);
            let mut graph_blocks: Vec<GraphBlockEntry> = Vec::new();
            let mut aliased_block_bytes = 0u64;
            let mut block_extent_by_hash: HashMap<u64, (u64, u64)> = HashMap::default();
            for piece in pieces {
                let elided = piece.pages.is_empty()
                    && piece.row_count > 0
                    && piece.stats.null_count == 0
                    && piece.stats.row_count == piece.row_count
                    && matches!(piece.pipeline.value_kind(), Ok(ValueKind::I64 | ValueKind::U64))
                    && piece.stats.min_i128.is_some()
                    && piece.stats.min_i128 == piece.stats.max_i128
                    && piece.first_byte() == Some(crate::layout::presence_forms::EMPTY);
                let compressed_len = if elided { 0 } else { piece.len() as u64 };
                let relative_offset = if elided {
                    0
                } else {
                    let hash = piece.alias_key();
                    let alias = block_extent_by_hash.get(&hash).copied().filter(|&(rel, len)| {
                        let at = rel as usize;
                        let split = at + piece.prefix.len();
                        len == compressed_len
                            && data.bytes().get(at..split) == Some(piece.prefix.as_ref())
                            && data.bytes().get(split..at + len as usize) == Some(piece.body.as_ref())
                    });
                    if let Some((rel, _)) = alias {
                        aliased_block_bytes += compressed_len;
                        rel
                    } else {
                        data.pad_to(self.block_alignment);
                        let rel = data.len() as u64;
                        piece.write_to(&mut data);
                        block_extent_by_hash.insert(hash, (rel, compressed_len));
                        rel
                    }
                };
                let page_count = if piece.pages.is_empty() {
                    1
                } else {
                    piece.pages.len() as u32
                };
                marks.push(ColumnMark {
                    codec_pipeline_id: piece.pipeline,
                    column_id: piece.column_id,
                    projection_id: 0,
                    granule_id: piece.granule_id,
                    compressed_offset: relative_offset,
                    compressed_size: compressed_len,
                    uncompressed_offset: 0,
                    uncompressed_size: piece.uncompressed_len,
                    row_count: piece.row_count,
                    page_count,
                    first_value_offset: None,
                });
                page_stats.push(PageStats {
                    column_id: piece.column_id,
                    granule_id: piece.granule_id,
                    max_f64: piece.stats.max_f64,
                    max_i128: piece.stats.max_i128,
                    min_f64: piece.stats.min_f64,
                    min_i128: piece.stats.min_i128,
                    null_count: piece.stats.null_count,
                    row_count: piece.stats.row_count,
                });
                if let Some(ge) = self.granule_by_id.get(&piece.granule_id) {
                    if piece.pages.is_empty() {
                        page_directory.push(PageDirectoryEntry {
                            column_id: piece.column_id,
                            compressed_len,
                            compressed_offset: relative_offset,
                            first_row_ordinal: ge.first_row_ordinal,
                            granule_id: piece.granule_id,
                            max_f64: piece.stats.max_f64,
                            max_i128: piece.stats.max_i128,
                            max_occurred_at_physical: ge.max_occurred_at_physical,
                            max_sequence: ge.last_sequence,
                            min_f64: piece.stats.min_f64,
                            min_i128: piece.stats.min_i128,
                            min_occurred_at_physical: ge.min_occurred_at_physical,
                            min_sequence: ge.first_sequence,
                            null_count: piece.stats.null_count,
                            page_index: 0,
                            projection_id: 0,
                            row_count: piece.row_count,
                        });
                    } else {
                        let mut page_offset = 0u64;
                        for (page_index, (page_len, _, stats, first_row_in_granule, row_count)) in
                            piece.pages.iter().enumerate()
                        {
                            page_directory.push(PageDirectoryEntry {
                                column_id: piece.column_id,
                                compressed_len: *page_len,
                                compressed_offset: relative_offset + page_offset,
                                first_row_ordinal: ge.first_row_ordinal + u64::from(*first_row_in_granule),
                                granule_id: piece.granule_id,
                                max_f64: stats.max_f64,
                                max_i128: stats.max_i128,
                                max_occurred_at_physical: ge.max_occurred_at_physical,
                                max_sequence: ge.last_sequence,
                                min_f64: stats.min_f64,
                                min_i128: stats.min_i128,
                                min_occurred_at_physical: ge.min_occurred_at_physical,
                                min_sequence: ge.first_sequence,
                                null_count: stats.null_count,
                                page_index: page_index as u32,
                                projection_id: 0,
                                row_count: *row_count,
                            });
                            if self.hot_columns.contains(&piece.column_id) {
                                page_minmax.push(PageMinMax {
                                    column_id: piece.column_id,
                                    granule_id: piece.granule_id,
                                    max_i128: stats.max_i128,
                                    min_i128: stats.min_i128,
                                    null_count: stats.null_count,
                                    page_index: page_index as u32,
                                    row_count: *row_count,
                                });
                            }
                            page_offset += *page_len;
                        }
                    }
                }
            }

            for geometry in &geometry {
                data.pad_to(self.block_alignment);
                let dictionary_offset = data.len() as u64;
                data.put_slice(&geometry.dictionary);
                data.pad_to(self.block_alignment);
                let offsets_offset = data.len() as u64;
                data.put_slice(&geometry.offsets);
                data.pad_to(self.block_alignment);
                let residual_offset = data.len() as u64;
                data.put_slice(&geometry.residual);
                payload_granules.push(PayloadGranule {
                    granule_id: geometry.granule_id,
                    dictionary_offset,
                    dictionary_len: geometry.dictionary.len() as u64,
                    offsets_offset,
                    offsets_len: geometry.offsets.len() as u64,
                    residual_offset,
                    residual_len: geometry.residual.len() as u64,
                    residual_compression: geometry.residual_compression,
                });
                for (column_id, bytes, offsets) in &geometry.freetext_row_offsets {
                    data.pad_to(self.block_alignment);
                    let offsets_offset = data.len() as u64;
                    data.put_slice(offsets);
                    data.pad_to(self.block_alignment);
                    let bytes_offset = data.len() as u64;
                    data.put_slice(bytes);
                    freetext_row_offsets.push(FreetextRowOffsets {
                        bytes_len: bytes.len() as u64,
                        bytes_offset,
                        column_id: *column_id,
                        granule_id: geometry.granule_id,
                        offsets_len: offsets.len() as u64,
                        offsets_offset,
                    });
                }
                if let Some(block) = self.graph_blocks_by_granule.get(&geometry.granule_id) {
                    data.pad_to(self.block_alignment);
                    let offset = data.len() as u64;
                    data.put_slice(block);
                    graph_blocks.push(GraphBlockEntry {
                        granule_id: geometry.granule_id,
                        len: block.len() as u64,
                        offset,
                    });
                }
            }

            for geometry in &geometry {
                let granule_id = geometry.granule_id;
                for entry in text_tokens(granule_id) {
                    data.pad_to(self.block_alignment);
                    let index_offset = data.len() as u64;
                    data.put_slice(&entry.index_bytes);
                    text_token_offsets.push(TextTokenOffsetsEntry {
                        column_id: entry.column_id,
                        granule_id: entry.granule_id,
                        index_len: entry.index_bytes.len() as u64,
                        index_offset,
                        page_index: entry.page_index,
                    });
                }
                if let Some(filter) = self.entity_hash_filters_by_granule.get(&granule_id) {
                    data.pad_to(self.block_alignment);
                    let index_offset = data.len() as u64;
                    data.put_slice(filter);
                    entity_hash_filters.push(EntityHashFilterEntry {
                        granule_id,
                        index_len: filter.len() as u64,
                        index_offset,
                    });
                }
            }
            let (stripe_marks_directory, stripe_marks_blob) =
                encode_stripe_marks_pages(stripe_id, &marks, &page_directory);
            data.pad_to(self.block_alignment);
            let blob_offset = data.len() as u64;
            data.put_slice(&stripe_marks_blob);
            let marks_page_offsets = stripe_marks_directory
                .into_iter()
                .map(|entry| MarksPageEntry {
                    page_offset: blob_offset + entry.page_offset,
                    ..entry
                })
                .collect();

            let stripe_len = data.len() as u64;
            if index + 1 < self.stripe_piece_ranges.len() {
                data.pad_to(self.block_alignment);
            }
            let bytes = data.into_bytes();
            let hash = || {
                crate::file::integrity::build_outboard_tree_and_root(
                    bytes.get(..stripe_len as usize).unwrap_or_default(),
                    CHUNK_GROUP_BYTES,
                )
            };
            let integrity = if self.profile_workers {
                profile_integrity(hash)
            } else {
                hash()
            };
            let mut crc = crate::file::integrity::StreamingCrc64Nvme::new();
            crc.update(&bytes);
            LaidOutStripe {
                aliased_block_bytes,
                bytes,
                crc,
                entity_hash_filters,
                freetext_row_offsets,
                graph_blocks,
                integrity,
                marks,
                marks_page_offsets,
                page_directory,
                page_minmax,
                page_stats,
                payload_granules,
                stripe_len,
                text_token_offsets,
            }
        }
    }

    let graph_blocks_by_granule = match &config.graph {
        Some(graph) => {
            let entries: Vec<GranuleEntry> = granules.iter().map(|granule| granule.entry).collect();
            encode_graph_blocks(graph, row_count as u64, &entries)?
        }
        None => BTreeMap::new(),
    };
    let state_roots = match &config.graph {
        Some(graph) => state_root_entries(graph, row_count as u64)?,
        None => Vec::new(),
    };

    let layout = StripeLayout {
        block_alignment,
        entity_hash_filters_by_granule,
        granule_by_id: &granule_by_id,
        graph_blocks_by_granule: &graph_blocks_by_granule,
        hot_columns: hot_columns.clone(),
        payload_geometry: &payload_geometry,
        payload_idx_by_stripe: &payload_idx_by_stripe,
        pieces: &pieces,
        profile_workers,
        stripe_piece_ranges: &stripe_piece_ranges,
        text_tokens_by_granule: &text_tokens_by_granule,
    };
    let slots: Vec<Mutex<Option<LaidOutStripe>>> = stripe_piece_ranges.iter().map(|_| Mutex::new(None)).collect();
    encode.run_jobs(slots.len(), &|job_index| {
        let work = || layout.lay_out(job_index);
        let laid = if profile_workers { profile_layout(work) } else { work() };
        if let Some(slot) = slots.get(job_index) {
            *slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(laid);
        }
    });
    #[expect(clippy::expect_used, reason = "the executor runs every job before returning")]
    let laid_out: Vec<LaidOutStripe> = slots
        .into_iter()
        .map(|slot| {
            slot.into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .expect("the executor runs every layout job")
        })
        .collect();

    let mut file_scope_columns: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for piece in &pieces {
        let recorded =
            std::iter::once(piece.pipeline).chain(piece.pages.iter().map(|(_, pipeline, _, _, _)| *pipeline));
        for pipeline in recorded {
            if pipeline.side_stream() == Ok(SideStream::FileScopeDictionary) {
                file_scope_columns.insert(piece.column_id);
            }
        }
    }
    drop(pieces);

    let mut stripe_rows: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
    for (granule, stripe_id) in granules.iter().zip(&stripe_of_granule) {
        let entry = stripe_rows.entry(*stripe_id).or_insert((u64::MAX, 0));
        entry.0 = entry.0.min(granule.entry.first_row_ordinal);
        entry.1 += u64::from(granule.entry.row_count);
    }

    let stripe_count = laid_out.len();
    let mut stripes: Vec<StripeEntry> = Vec::with_capacity(stripe_count);
    let mut stripe_checksums: Vec<[u8; 32]> = Vec::with_capacity(stripe_count);
    let mut stripe_proofs = Vec::with_capacity(stripe_count);
    let mut proof_trees: Vec<Vec<u8>> = Vec::with_capacity(stripe_count);
    let mut proof_tree_bytes_len = 0usize;
    let mut stripe_bytes: Vec<Vec<u8>> = Vec::with_capacity(stripe_count);
    let mut assembled_data_crc = crate::file::integrity::StreamingCrc64Nvme::new();
    let mut data_end = HEADER_BLOCK_LEN as u64;
    let mut aliased_block_bytes = 0u64;
    let mut marks: Vec<ColumnMark> = Vec::with_capacity(laid_out.iter().map(|laid| laid.marks.len()).sum());
    let mut page_directory: Vec<PageDirectoryEntry> =
        Vec::with_capacity(laid_out.iter().map(|laid| laid.page_directory.len()).sum());
    let mut page_minmax: Vec<PageMinMax> = Vec::with_capacity(laid_out.iter().map(|laid| laid.page_minmax.len()).sum());
    let mut page_stats: Vec<PageStats> = Vec::with_capacity(laid_out.iter().map(|laid| laid.page_stats.len()).sum());
    let mut payload_granules: Vec<PayloadGranule> = Vec::with_capacity(payload_geometry.len());
    let mut freetext_row_offsets: Vec<FreetextRowOffsets> =
        Vec::with_capacity(laid_out.iter().map(|laid| laid.freetext_row_offsets.len()).sum());
    let mut text_token_offsets: Vec<TextTokenOffsetsEntry> =
        Vec::with_capacity(laid_out.iter().map(|laid| laid.text_token_offsets.len()).sum());
    let mut entity_hash_filters: Vec<EntityHashFilterEntry> = Vec::with_capacity(entity_hash_filters_by_granule.len());
    let mut marks_page_offsets: Vec<MarksPageEntry> =
        Vec::with_capacity(laid_out.iter().map(|laid| laid.marks_page_offsets.len()).sum());
    let mut graph_blocks: Vec<GraphBlockEntry> = Vec::with_capacity(graph_blocks_by_granule.len());
    for ((stripe_id, _), laid) in stripe_piece_ranges.iter().zip(laid_out) {
        let (first_row, row_count) = stripe_rows.get(stripe_id).copied().unwrap_or((u64::MAX, 0));
        stripes.push(StripeEntry {
            stripe_id: *stripe_id,
            file_offset: data_end,
            byte_len: laid.stripe_len,
            first_row_ordinal: if first_row == u64::MAX { 0 } else { first_row },
            row_count,
        });
        stripe_checksums.push(laid.integrity.root);
        let tree_len = laid.integrity.tree.as_deref().map_or(0, <[u8]>::len);
        stripe_proofs.push(StripeProofEntry {
            chunk_group_bytes: CHUNK_GROUP_BYTES as u32,
            stripe_id: *stripe_id,
            tree_len: tree_len as u64,
            tree_offset: proof_tree_bytes_len as u64,
        });
        proof_tree_bytes_len = proof_tree_bytes_len.saturating_add(tree_len);
        proof_trees.extend(laid.integrity.tree);
        assembled_data_crc.combine(&laid.crc);
        data_end += laid.bytes.len() as u64;
        aliased_block_bytes += laid.aliased_block_bytes;
        marks.extend(laid.marks);
        page_directory.extend(laid.page_directory);
        page_minmax.extend(laid.page_minmax);
        page_stats.extend(laid.page_stats);
        payload_granules.extend(laid.payload_granules);
        freetext_row_offsets.extend(laid.freetext_row_offsets);
        text_token_offsets.extend(laid.text_token_offsets);
        entity_hash_filters.extend(laid.entity_hash_filters);
        marks_page_offsets.extend(laid.marks_page_offsets);
        graph_blocks.extend(laid.graph_blocks);
        stripe_bytes.push(laid.bytes);
    }

    phase_clock.transition(BuildPhase::Integrity);
    let gaps: BTreeMap<usize, &[u8]> = stripes
        .iter()
        .zip(&stripe_bytes)
        .filter_map(|(stripe, bytes)| {
            let gap = bytes.get(stripe.byte_len as usize..)?;
            (!gap.is_empty()).then_some(((stripe.file_offset + stripe.byte_len) as usize, gap))
        })
        .collect();
    let data_commitments =
        crate::integrity::data_commitments_with(data_end as usize, &stripes, &stripe_checksums, |range| {
            gaps.get(&range.start).copied().filter(|gap| gap.len() == range.len())
        })?;
    let integrity_gaps = crate::integrity::integrity_gaps(&data_commitments);

    phase_clock.transition(BuildPhase::FileAssembly);
    if let Some(sink) = sink.as_deref_mut() {
        for (stripe, bytes) in stripes.iter().zip(stripe_bytes.drain(..)) {
            sink(stripe.file_offset, bytes);
        }
    }

    phase_clock.transition(BuildPhase::FooterConstruction);
    let ScheduledFooterMetadata {
        clustering,
        entity_hash_filters: _,
        exact_counts,
        stripe_ndv,
    } = scheduled_metadata;

    let mut optional_flags = 0u64;
    if !shred_plan.is_empty() {
        optional_flags |= optional_features::VARIANT_SHREDDED_FIELD_BLOCKS;
    }
    if !freetext_entries.is_empty() {
        optional_flags |= optional_features::FREETEXT_COLUMNS;
    }
    if !freetext_row_offsets.is_empty() {
        optional_flags |= optional_features::TYPED_COLUMN_ROW_OFFSETS;
    }
    if !page_directory.is_empty() {
        optional_flags |= optional_features::PER_PAGE_MARKS;
    }
    if config.io_alignment_bytes > 0 {
        optional_flags |= optional_features::PAGE_IO_ALIGNMENT;
    }
    let late_events = match source {
        Some(source) => source.footer.optional_feature_flags & optional_features::OE_LATE_EVENTS != 0,
        None => granules_have_late_events(&granules),
    };
    if late_events {
        optional_flags |= optional_features::OE_LATE_EVENTS;
    }
    if !text_token_offsets.is_empty() {
        optional_flags |= optional_features::TEXT_TOKEN_FILTER_OFFSETS;
    }
    if !entity_hash_filters.is_empty() {
        optional_flags |= optional_features::ENTITY_HASH_POINT_FILTERS;
    }
    if !inbox_recipients.is_empty() {
        optional_flags |= optional_features::INBOX_RECIPIENT_DIRECTORY;
    }
    if snapshot_keys.is_some() {
        optional_flags |= optional_features::SNAPSHOT_KEY_INDEX;
    }
    if time_series_rollups.is_some() {
        optional_flags |= optional_features::TIME_SERIES_ROLLUPS;
    }
    if columns
        .iter()
        .any(|column| source_form::is_lineage_column(column.column_id))
    {
        optional_flags |= optional_features::DELIVERY_LINEAGE;
    }

    let entity_summary = match sparse_reuse {
        Some(reuse) => reuse.source.entity_summary.clone(),
        None => {
            let mut keys: Vec<crate::indexes::entity_summary::EntityKey> = rows
                .par_iter()
                .map(|row| {
                    crate::indexes::entity_summary::EntityKey::new(
                        row.envelope.entity_id_hash_high,
                        row.envelope.entity_id_hash_low,
                    )
                })
                .collect();
            keys.par_sort_unstable();
            keys.dedup();
            crate::indexes::entity_summary::EntitySummary::from_keys(keys)
        }
    };
    optional_flags |= optional_features::ENTITY_SUMMARY;

    let mut schema_hash = blake3::Hasher::new();
    for column in &columns {
        schema_hash.update(&column.column_id.to_le_bytes());
        schema_hash.update(column.name.as_bytes());
        schema_hash.update(&[column.kind as u8, u8::from(column.nullable)]);
    }

    let mut shared_dictionaries: Vec<SharedDictionaryEntry> = Vec::new();
    let mut packed_shared_dictionaries: Vec<PackedSharedDictionary> = Vec::new();
    for (column_id, values) in shared_alphabets {
        if !file_scope_columns.contains(&column_id) {
            continue;
        }
        match union_columns
            .contains(&column_id)
            .then(|| PackedSharedDictionary::pack(column_id, &values))
            .flatten()
        {
            Some(packed) => packed_shared_dictionaries.push(packed),
            None => shared_dictionaries.push(SharedDictionaryEntry { column_id, values }),
        }
    }

    let mut required_feature_flags = required_features::ALL;
    if audit.is_some() {
        required_feature_flags |= required_features::AUDIT_HASH_CHAIN;
    }
    if config.graph.is_some() {
        required_feature_flags |= required_features::GRAPH_DENSE_IDS
            | required_features::GRAPH_CSR_FORWARD
            | required_features::GRAPH_CSR_REVERSE;
    }
    if graph_event_ids.is_some() {
        required_feature_flags |= required_features::GRAPH_EVENT_ID_INDEX;
    }
    if config.graph.as_ref().is_some_and(|graph| graph.chain_cover.is_some()) {
        required_feature_flags |= required_features::GRAPH_CHAIN_COVER;
    }
    if xor_float_blocks {
        required_feature_flags |= required_features::XOR_FLOAT_BLOCKS;
    }
    if packed_string_offsets {
        required_feature_flags |= required_features::PACKED_STRING_OFFSETS;
    }
    if occurred_relative_ingested_at {
        required_feature_flags |= required_features::OCCURRED_RELATIVE_INGESTED_AT;
    }
    if string_zstd_blocks {
        required_feature_flags |= required_features::STRING_ZSTD_BLOCKS | required_features::STRING_LZ4_BLOCKS;
    }
    if !state_roots.is_empty() {
        required_feature_flags |= required_features::GRAPH_STATE_ROOTS;
    }
    if !source_form_indexes.external_ids.is_empty() {
        required_feature_flags |= required_features::EXTERNAL_ID_INDEX_32;
    }
    if source_form_indexes.elides_event_ids {
        required_feature_flags |= required_features::EXTERNAL_ID_EVENT_ID_ELISION;
    }
    if elide_entity_hash {
        required_feature_flags |= required_features::ENTITY_HASH_COLUMNS_ELIDED;
    }
    if !packed_shared_dictionaries.is_empty() {
        required_feature_flags |= required_features::PACKED_SHARED_DICTIONARIES;
    }
    let footer = Footer {
        audit,
        clustering,
        columns,
        dictionaries,
        embedding_row_offsets: Vec::new(),
        entity_hash_filters,
        entity_summary: Some(entity_summary.clone()),
        escape_hatches: Vec::new(),
        exact_counts,
        external_ids: source_form_indexes.external_ids,
        format_version: (1, 0),
        freetext: freetext_entries,
        freetext_row_offsets,
        granules: granules.iter().map(|granule| granule.entry).collect(),
        graph: config.graph.as_ref().map(|graph| GraphDirectory {
            blocks: graph_blocks,
            chain_cover: graph.chain_cover.as_ref().map(ChainCover::build),
            depth: graph.depth,
            event_ids: graph_event_ids,
            first_sn: graph.first_sn,
            subject: graph.subject.clone(),
            unresolved: {
                let mut unresolved = graph.unresolved.clone();
                unresolved.sort_by_key(|edge| edge.child);
                unresolved
            },
        }),
        inbox_recipients,
        integrity_gaps,
        io_alignment_bytes: config.io_alignment_bytes,
        marks,
        marks_directory: Vec::new(),
        marks_page_offsets,
        marks_pages: Vec::new(),
        optional_feature_flags: optional_flags,
        page_directory,
        page_minmax,
        page_stats,
        payload_granules,
        presence: presence_entries,
        profile: config.profile,
        reference_filters: source_form_indexes.reference_filters,
        required_feature_flags,
        schema_fingerprint: *schema_hash.finalize().as_bytes(),
        shared_dictionaries,
        packed_shared_dictionaries,
        shredded: shred_plan,
        snapshot_keys,
        sparse_keys,
        state_roots,
        stripe_checksums,
        stripe_proofs,
        stripe_ndv,
        stripes,
        text_token_indexes: Vec::new(),
        text_token_offsets,
        time_series_rollups,
        vector_columns: Vec::new(),
    };

    let file_id = crate::integrity::derive_file_id(
        config.tenant_id.uuid().as_u128().to_le_bytes(),
        first_key.0,
        last_key.0,
        first_key.1,
        last_key.1,
        row_count as u64,
        &footer.schema_fingerprint,
        layout_class as u8,
        &data_commitments,
    );

    let header = OeHeader {
        version_major: 1,
        version_minor: 0,
        file_id,
        tenant_id: config.tenant_id,
        generation_id: config.generation_id,
        layout_class,
        projection_count: 1,
        created_at_physical: config.created_at_physical,
        min_occurred_at_physical: granules
            .iter()
            .map(|granule| granule.entry.min_occurred_at_physical)
            .min()
            .unwrap_or(0),
        max_occurred_at_physical: granules
            .iter()
            .map(|granule| granule.entry.max_occurred_at_physical)
            .max()
            .unwrap_or(0),
        min_ingested_at_physical: granules
            .iter()
            .map(|granule| granule.entry.min_ingested_at_physical)
            .min()
            .unwrap_or(0),
        max_ingested_at_physical: granules
            .iter()
            .map(|granule| granule.entry.max_ingested_at_physical)
            .max()
            .unwrap_or(0),
        min_epoch: first_key.0,
        max_epoch: last_key.0,
        min_sequence: first_key.1,
        max_sequence: last_key.1,
        profile: config.profile,
        row_count: row_count as u64,
        feature_flags: match config.footer_encryption {
            FooterEncryption::Encrypted => required_feature_flags | required_features::FOOTER_ENCRYPTED,
            FooterEncryption::Plaintext => required_feature_flags,
        },
        footer_pointer_hint: data_end,
    };

    let header_bytes = encode_header(&header);
    let footer_blob = crate::layout::footer::encode_footer(&footer);
    let footer_region = match config.footer_encryption {
        FooterEncryption::Encrypted => {
            let dek = config.footer_dek.ok_or(FormatError::Structural {
                rule: "encrypted footer requires a file DEK",
            })?;
            seal_footer(&dek, file_id, &footer_blob)?
        }
        FooterEncryption::Plaintext => footer_blob,
    };
    let mut trailer = (footer_region.len() as u64).to_le_bytes().to_vec();
    trailer.extend_from_slice(b"OEF1");
    let footer_len = footer_region.len() as u64 + 12;

    phase_clock.transition(BuildPhase::Integrity);
    let sealed_content_len = data_end + footer_region.len() as u64 + trailer.len() as u64;
    let file_seal = crate::integrity::derive_file_seal(
        &header_bytes,
        &footer_region,
        &trailer,
        sealed_content_len,
        &data_commitments,
    );

    let proof_appendix_len = if proof_tree_bytes_len == 0 {
        0
    } else {
        proof_tree_bytes_len.saturating_add(PROOF_APPENDIX_TRAILER_BYTES)
    };
    let proof_tree_len_bytes = (proof_tree_bytes_len as u64).to_le_bytes();
    let tree_len = (proof_appendix_len != 0).then_some(proof_appendix_len as u64);
    let total_len = sealed_content_len + proof_appendix_len as u64;

    let mut crc = crate::file::integrity::StreamingCrc64Nvme::new();
    crc.update(&header_bytes);
    crc.combine(&assembled_data_crc);
    crc.update(&footer_region);
    crc.update(&trailer);
    for tree in &proof_trees {
        crc.update(tree);
    }
    if proof_appendix_len != 0 {
        crc.update(&proof_tree_len_bytes);
        crc.update(&TREE_TRAILER_MAGIC);
    }
    let file_crc64_nvme = crc.finalize();

    phase_clock.transition(BuildPhase::FileAssembly);
    let tail = [footer_region, trailer]
        .into_iter()
        .chain(proof_trees)
        .chain((proof_appendix_len != 0).then(|| proof_tree_len_bytes.to_vec()))
        .chain((proof_appendix_len != 0).then(|| TREE_TRAILER_MAGIC.to_vec()));
    let file = match sink {
        Some(sink) => {
            let mut at = data_end;
            for bytes in tail {
                let len = bytes.len() as u64;
                sink(at, bytes);
                at += len;
            }
            sink(0, header_bytes);
            Vec::new()
        }
        None => {
            let mut file = Vec::with_capacity(total_len as usize);
            file.extend_from_slice(&header_bytes);
            for bytes in stripe_bytes {
                file.extend_from_slice(&bytes);
            }
            for bytes in tail {
                file.extend_from_slice(&bytes);
            }
            file
        }
    };

    phase_clock.transition(BuildPhase::RowDestruction);
    payloads.release();
    rows.into_par_iter().for_each(drop);
    phase_clock.finish();

    Ok(BuiltOe {
        aliased_block_bytes,
        encoded_blocks,
        entity_summary,
        reused_source_block_bytes,
        reused_source_blocks,
        bytes: file,
        file_seal,
        file_crc64_nvme,
        file_id,
        footer,
        footer_len,
        header,
        total_len,
        tree_len,
    })
}

const RELATIONSHIP_KIND_COLUMNS: [RelationshipKind; 6] = [
    RelationshipKind::Parent,
    RelationshipKind::Root,
    RelationshipKind::Link,
    RelationshipKind::Related,
    RelationshipKind::Prev,
    RelationshipKind::Auth,
];

const RELATIONSHIP_COLUMN_IDS: [u32; 6] = [
    column_ids::PARENT_REF,
    column_ids::ROOT_REF,
    column_ids::LINKED_REFS,
    column_ids::RELATED_REFS,
    column_ids::PREV_REFS,
    column_ids::AUTH_REFS,
];

type EncodedResidual = (Vec<u8>, Vec<u8>, Vec<u8>);

type FreetextOffsetIndex<'a> = (u32, Cow<'a, [u8]>, Cow<'a, [u8]>);

fn encode_residual(
    residuals: &[Option<Residual<'_>>],
    residual_fields: &[(&str, &VariantValue)],
    external_refs: &[Option<&str>],
    residual_value_bytes: usize,
    payloads: &NormalizedPayloads,
) -> Result<EncodedResidual, FormatError> {
    let row_count = residuals.len();
    let field_name_count = payloads.identities().field_names().len();
    let mut key_names: HashSet<&str> = HashSet::with_capacity(field_name_count.min(residual_fields.len()));
    let mut nested_names: Vec<&str> = Vec::new();
    let mut seen_names: HashSet<*const str> = HashSet::default();
    let mut previous_fields: &[(&str, &VariantValue)] = &[];
    for residual in residuals.iter().flatten() {
        match residual {
            Residual::Fields(range) => {
                let fields = residual_fields.get(range.clone()).unwrap_or_default();
                let repeats = fields.len() == previous_fields.len()
                    && fields
                        .iter()
                        .zip(previous_fields)
                        .all(|((name, _), (previous, _))| std::ptr::eq::<str>(*name, *previous));
                for &(path, value) in fields {
                    if !repeats && seen_names.insert(std::ptr::from_ref::<str>(path)) {
                        key_names.insert(path);
                    }
                    collect_key_names(value, &mut nested_names);
                }
                previous_fields = fields;
            }
            Residual::Whole(value) => collect_key_names(value, &mut nested_names),
        }
        key_names.extend(nested_names.drain(..));
    }
    let dictionary = KeyDictionary::build(key_names.into_iter().map(str::to_owned));
    let dictionary_bytes = encode_variant_dictionary(&dictionary);

    let residual_capacity = residual_value_bytes.saturating_add(row_count.saturating_mul(RESIDUAL_ROW_FRAMING_BYTES));
    let mut residual = Writer::with_capacity(residual_capacity);
    let mut offsets = Writer::with_capacity(row_count * 8);
    let mut scratch = EncodeScratch::new();
    let mut previous: Option<(usize, usize)> = None;
    for (row_residual, external) in residuals.iter().zip(external_refs.iter()) {
        match (row_residual, external) {
            (Some(value), _) => {
                let start = residual.len();
                match value {
                    Residual::Fields(range) => encode_object_fields_into_with_scratch(
                        residual_fields.get(range.clone()).unwrap_or_default(),
                        &dictionary,
                        &mut scratch,
                        &mut residual,
                    )?,
                    Residual::Whole(value) => {
                        encode_value_into_with_scratch(value, &dictionary, &mut scratch, &mut residual)?
                    }
                }
                let len = residual.len() - start;
                let repeated = previous.filter(|&(prev_start, prev_len)| {
                    prev_len == len
                        && residual.bytes().get(prev_start..prev_start + len) == residual.bytes().get(start..)
                });
                let slot_start = match repeated {
                    Some((prev_start, _)) => {
                        residual.truncate(start);
                        prev_start
                    }
                    None => start,
                };
                previous = Some((slot_start, len));
                offsets.put_u32(slot_start as u32);
                offsets.put_u32(len as u32);
            }
            (None, Some(reference)) => {
                offsets.put_u32(residual.len() as u32);
                offsets.put_u32(reference.len() as u32);
                residual.put_slice(reference.as_bytes());
            }
            (None, None) => {
                offsets.put_u32(0);
                offsets.put_u32(0);
            }
        }
    }
    let mut offsets = offsets.into_bytes();
    if let Some((first, rest)) = offsets.split_first_chunk::<RESIDUAL_OFFSET_ENTRY_LEN>()
        && rest.chunks_exact(RESIDUAL_OFFSET_ENTRY_LEN).all(|entry| entry == first)
    {
        offsets.truncate(RESIDUAL_OFFSET_ENTRY_LEN);
    }
    Ok((dictionary_bytes, offsets, residual.into_bytes()))
}

const RESIDUAL_OFFSET_ENTRY_LEN: usize = 8;

fn min_max<T: Ord + Copy>(values: impl IntoIterator<Item = T>) -> Option<(T, T)> {
    let mut values = values.into_iter();
    let first = values.next()?;
    Some(values.fold((first, first), |(min, max), value| (min.min(value), max.max(value))))
}

#[expect(clippy::too_many_arguments, reason = "single internal call site")]
fn build_granule(
    granule_id: u32,
    first_row_ordinal: u64,
    rows: &mut [BuildRow],
    granule_payloads: &[NormalizedPayload],
    dictionaries: &FileDictionaries,
    dictionary_index: &DictionaryIndex<'_>,
    promotion: &PromotionPlan,
    shred_plan: &[ShredEntry],
    shred_kinds: &HashMap<String, ColumnKind>,
    freetext: &[FreetextEntry],
    field_routes: &[FieldRoute],
    shape_routes: &ShapeRoutes,
    payloads: &NormalizedPayloads,
    freetext_row_offset_index: bool,
    carries_provenance: bool,
    carries_relationships: bool,
    elide_entity_hash: bool,
    wanted: Option<&HashSet<u32>>,
    build_residual: bool,
) -> Result<GranulePieces, FormatError> {
    let row_count = rows.len();
    let wants = |column_id: u32| wanted.is_none_or(|columns| columns.contains(&column_id));
    let ((first_epoch, first_sequence), (last_epoch, last_sequence)) =
        min_max(rows.iter().map(|row| (row.epoch, row.sequence))).ok_or(FormatError::Structural {
            rule: "granules are non-empty",
        })?;
    let mut epoch = Vec::with_capacity(row_count);
    let mut sequence = Vec::with_capacity(row_count);
    let mut stream_id = Vec::with_capacity(row_count);
    let mut stream_sequence = Vec::with_capacity(row_count);
    let mut occurred = Vec::with_capacity(row_count);
    let mut ingested = Vec::with_capacity(row_count);
    let mut source_id = Vec::with_capacity(row_count);
    let mut event_type_id = Vec::with_capacity(row_count);
    let mut entity_type_id = Vec::with_capacity(row_count);
    let mut by_source = vec![0u64; dictionaries.source.len()];
    let mut by_event_type = vec![0u64; dictionaries.event_type.len()];
    let mut by_entity_type = vec![0u64; dictionaries.entity_type.len()];
    let mut entity_hash_low = Vec::with_capacity(row_count);
    let mut entity_hash_high = Vec::with_capacity(row_count);
    let mut payload_ref = Vec::with_capacity(row_count);
    let mut flags = Vec::with_capacity(row_count);
    let mut schema_version = Vec::with_capacity(row_count);
    let mut event_id = Vec::with_capacity(row_count);
    let (account_id_bytes, actor_id_bytes, entity_id_bytes) = rows.iter().fold((0, 0, 0), |totals, row| {
        let envelope = &row.envelope;
        (
            totals.0 + envelope.account_id.as_ref().map_or(0, String::len),
            totals.1 + envelope.actor_id.as_ref().map_or(0, String::len),
            totals.2 + envelope.entity_id.as_ref().map_or(0, String::len),
        )
    });
    let mut entity_id = StringColumn::with_capacity(row_count, entity_id_bytes);
    let mut actor_id = StringColumn::with_capacity(row_count, actor_id_bytes);
    let mut account_id = StringColumn::with_capacity(row_count, account_id_bytes);
    let mut actor_hash_low = Vec::with_capacity(row_count);
    let mut account_hash_low = Vec::with_capacity(row_count);
    let mut trace_hash_low = Vec::with_capacity(row_count);
    let mut dedupe_low = Vec::with_capacity(row_count);
    let mut dedupe_high = Vec::with_capacity(row_count);
    let mut payload_flags = Vec::with_capacity(row_count);

    let mut promoted_values: Vec<(Vec<u8>, TypedColumn)> = promotion
        .columns
        .iter()
        .map(|promoted| (vec![0u8; row_count.div_ceil(8)], TypedColumn::for_kind(promoted.kind)))
        .collect();
    let mut shredded_values: Vec<(Vec<u8>, TypedColumn)> = shred_plan
        .iter()
        .map(|entry| {
            let kind = shred_kinds
                .get(entry.path.as_str())
                .copied()
                .unwrap_or(ColumnKind::String);
            (vec![0u8; row_count.div_ceil(8)], TypedColumn::for_kind(kind))
        })
        .collect();
    let mut shred_column_scales: Vec<Option<u8>> = vec![None; shred_plan.len()];
    let mut freetext_values: Vec<(Vec<u8>, StringColumn)> = freetext
        .iter()
        .map(|_| (vec![0u8; row_count.div_ceil(8)], StringColumn::new()))
        .collect();

    let signed_rows = if carries_provenance {
        rows.iter().filter(|row| row.provenance.is_some()).count()
    } else {
        0
    };
    let mut provenance_presence = vec![0u8; row_count.div_ceil(8)];
    let mut author_pubkey = StringColumn::with_capacity(signed_rows, 0);
    let mut signature = StringColumn::with_capacity(signed_rows, 0);
    let mut signature_scheme = StringColumn::with_capacity(signed_rows, 0);
    let mut protocol_event_id = StringColumn::with_capacity(signed_rows, 0);
    let mut protocol_kind: Vec<i64> = Vec::with_capacity(signed_rows);
    let mut claimed_at: Vec<i64> = Vec::with_capacity(signed_rows);

    let mut relationship_columns: [(Vec<u8>, StringColumn); 6] =
        std::array::from_fn(|_| (vec![0u8; row_count.div_ceil(8)], StringColumn::new()));

    let residual_field_estimate: usize = granule_payloads
        .iter()
        .map(|&entry| shape_routes.residual_fields(payloads.shape_id(entry)))
        .sum();
    let mut residual_fields: Vec<(&str, &VariantValue)> = Vec::with_capacity(residual_field_estimate);
    let mut residual_value_bytes = 0usize;
    let mut residuals: Vec<Option<Residual>> = Vec::with_capacity(row_count);
    let mut external_refs: Vec<Option<&str>> = Vec::with_capacity(row_count);
    let mut hex_scratch = String::new();
    let mut relationship_scratch = String::new();

    for (row_index, row) in rows.iter_mut().enumerate() {
        let envelope = &mut row.envelope;
        epoch.push(row.epoch);
        sequence.push(row.sequence);
        stream_id.push(envelope.stream_id.0);
        stream_sequence.push(envelope.stream_sequence);
        occurred.push(envelope.occurred_at.physical_nanos());
        ingested.push(envelope.ingested_at.physical_nanos());
        let this_source_id = dictionary_id(&dictionary_index.source, &envelope.source);
        let this_event_type_id = dictionary_id(&dictionary_index.event_type, &envelope.event_type);
        let this_entity_type_id = dictionary_id(&dictionary_index.entity_type, &envelope.entity_type);
        if let Some(count) = by_source.get_mut(this_source_id as usize) {
            *count += 1;
        }
        if let Some(count) = by_event_type.get_mut(this_event_type_id as usize) {
            *count += 1;
        }
        if let Some(count) = by_entity_type.get_mut(this_entity_type_id as usize) {
            *count += 1;
        }
        source_id.push(this_source_id);
        event_type_id.push(this_event_type_id);
        entity_type_id.push(this_entity_type_id);
        entity_hash_low.push(envelope.entity_id_hash_low);
        entity_hash_high.push(envelope.entity_id_hash_high);
        payload_ref.push(first_row_ordinal + row_index as u64);
        flags.push(u64::from(envelope.flags.0));
        schema_version.push(u64::from(envelope.schema_version));
        event_id.push(envelope.event_id.uuid().as_u128());
        if wants(column_ids::ENTITY_ID) {
            entity_id.push(envelope.entity_id.take().as_deref());
        }
        if wants(column_ids::ACTOR_ID) {
            actor_id.push(envelope.actor_id.take().as_deref());
        }
        if wants(column_ids::ACCOUNT_ID) {
            account_id.push(envelope.account_id.take().as_deref());
        }
        actor_hash_low.push(envelope.actor_id_hash_low);
        account_hash_low.push(envelope.account_id_hash_low);
        trace_hash_low.push(envelope.trace_id_hash_low);
        dedupe_low.push(envelope.dedupe_hash_low);
        dedupe_high.push(envelope.dedupe_hash_high);

        if let Some(signed) = &row.provenance {
            if let Some(byte) = provenance_presence.get_mut(row_index / 8) {
                *byte |= 1 << (row_index % 8);
            }
            hex_scratch.clear();
            hex_lower_into(&signed.author_pubkey, &mut hex_scratch);
            author_pubkey.push(Some(&hex_scratch));
            hex_scratch.clear();
            hex_lower_into(&signed.signature, &mut hex_scratch);
            signature.push(Some(&hex_scratch));
            signature_scheme.push(Some(signed.scheme.as_str()));
            hex_scratch.clear();
            hex_lower_into(&signed.protocol_event_id, &mut hex_scratch);
            protocol_event_id.push(Some(&hex_scratch));
            protocol_kind.push(i64::from(signed.protocol_kind));
            claimed_at.push(signed.claimed_at.physical_nanos());
        }

        if let Some(relationships) = &row.relationships {
            for (slot, kind) in relationship_columns.iter_mut().zip(RELATIONSHIP_KIND_COLUMNS) {
                if relationships.column_text_into(kind, &mut relationship_scratch) {
                    if let Some(byte) = slot.0.get_mut(row_index / 8) {
                        *byte |= 1 << (row_index % 8);
                    }
                    slot.1.push(Some(&relationship_scratch));
                }
            }
        }

        let entry = granule_payloads
            .get(row_index)
            .copied()
            .unwrap_or(NormalizedPayload::None);
        match entry {
            NormalizedPayload::None => {
                payload_flags.push(0u64);
                residuals.push(None);
                external_refs.push(None);
            }
            NormalizedPayload::ExternalRef => {
                payload_flags.push(u64::from(PAYLOAD_FLAG_EXTERNAL_REF));
                residuals.push(None);
                external_refs.push(match &row.payload {
                    BuildPayload::ExternalRef(reference) => Some(reference.as_str()),
                    _ => None,
                });
            }
            NormalizedPayload::Whole { .. } => {
                payload_flags.push(0u64);
                external_refs.push(None);
                let Some(value) = payloads.values(entry).first() else {
                    residuals.push(None);
                    continue;
                };
                residual_value_bytes = residual_value_bytes.saturating_add(variant_value_bytes(value));
                residuals.push(Some(Residual::Whole(value)));
            }
            NormalizedPayload::Object { shape_id, .. } => {
                payload_flags.push(0u64);
                external_refs.push(None);
                let field_values = payloads.values(entry);
                let shape_field_ids = payloads.identities().shape(shape_id);
                let shape_field_routes = shape_routes.field_routes(shape_id);
                debug_assert_eq!(field_values.len(), shape_field_routes.len());
                let start = residual_fields.len();
                for ((field_value, &name_id), &field_id) in
                    field_values.iter().zip(shape_field_ids).zip(shape_field_routes)
                {
                    let path = payloads.field_name(name_id);
                    let Some(route) = field_routes.get(field_id as usize) else {
                        residual_value_bytes = residual_value_bytes.saturating_add(variant_value_bytes(field_value));
                        residual_fields.push((path, field_value));
                        continue;
                    };
                    for &index in &route.promoted_indices {
                        if wants(column_ids::PROMOTED_BASE + index as u32)
                            && let Some(promoted) = promotion.columns.get(index)
                            && promoted_present(promoted, envelope.schema_version)
                            && let Some((bitmap, values)) = promoted_values.get_mut(index)
                            && values.push_variant(field_value)
                            && let Some(byte) = bitmap.get_mut(row_index / 8)
                        {
                            *byte |= 1 << (row_index % 8);
                        }
                    }
                    if let Some(index) = route.shred_index
                        && let Some(scale) = shred_column_scales.get_mut(index)
                        && let Some((bitmap, values)) = shredded_values.get_mut(index)
                        && values.push_variant_shred(field_value, scale)
                    {
                        if let Some(byte) = bitmap.get_mut(row_index / 8) {
                            *byte |= 1 << (row_index % 8);
                        }
                        continue;
                    }
                    if let Some(index) = route.freetext_index
                        && let VariantValue::String(text) = field_value
                    {
                        if freetext.get(index).is_some_and(|entry| wants(entry.column_id))
                            && let Some((bitmap, values)) = freetext_values.get_mut(index)
                        {
                            if let Some(byte) = bitmap.get_mut(row_index / 8) {
                                *byte |= 1 << (row_index % 8);
                            }
                            values.push(Some(text));
                        }
                        continue;
                    }
                    residual_value_bytes = residual_value_bytes.saturating_add(variant_value_bytes(field_value));
                    residual_fields.push((path, field_value));
                }
                residuals.push(Some(Residual::Fields(start..residual_fields.len())));
            }
        }
    }

    let indexed_freetext: &[FreetextEntry] = if freetext_row_offset_index { freetext } else { &[] };
    let mut freetext_row_offsets: Vec<(u32, Vec<u8>, Vec<u8>)> = Vec::with_capacity(indexed_freetext.len());
    for (entry, (presence, values)) in indexed_freetext
        .iter()
        .zip(freetext_values.iter())
        .filter(|(entry, _)| wants(entry.column_id))
    {
        let mut bytes = Writer::with_capacity(values.text_len());
        let mut offsets = Writer::with_capacity(row_count * 8);
        let mut next_value = 0usize;
        for row_index in 0..row_count {
            let present = presence
                .get(row_index / 8)
                .is_some_and(|byte| byte & (1 << (row_index % 8)) != 0);
            let text = if present {
                let text = values.get(next_value).flatten();
                next_value += 1;
                text
            } else {
                None
            };
            let Some(text) = text else {
                offsets.put_u32(0);
                offsets.put_u32(0);
                continue;
            };
            if bytes.len() > u32::MAX as usize || text.len() > u32::MAX as usize {
                return Err(FormatError::Structural {
                    rule: "free-text arena exceeds the u32 per-row offset range",
                });
            }
            let offset = if text.is_empty() {
                EMPTY_VALUE_ROW_OFFSET
            } else {
                bytes.len() as u32
            };
            offsets.put_u32(offset);
            offsets.put_u32(text.len() as u32);
            bytes.put_slice(text.as_bytes());
        }
        freetext_row_offsets.push((entry.column_id, bytes.into_bytes(), offsets.into_bytes()));
    }

    let (dictionary_bytes, offsets_bytes, residual_bytes) = if build_residual {
        encode_residual(
            &residuals,
            &residual_fields,
            &external_refs,
            residual_value_bytes,
            payloads,
        )?
    } else {
        (Vec::new(), Vec::new(), Vec::new())
    };

    let mut max_occurred_so_far = i64::MIN;
    let mut min_occurred = i64::MAX;
    let mut has_internal_late_event = false;
    for &occurred_at in &occurred {
        if occurred_at < max_occurred_so_far {
            has_internal_late_event = true;
        }
        max_occurred_so_far = max_occurred_so_far.max(occurred_at);
        min_occurred = min_occurred.min(occurred_at);
    }
    let (min_occurred, max_occurred) = if occurred.is_empty() {
        (0, 0)
    } else {
        (min_occurred, max_occurred_so_far)
    };
    let (min_ingested, max_ingested) = min_max(ingested.iter().copied()).unwrap_or((0, 0));

    let entry = GranuleEntry {
        granule_id,
        stripe_id: 0,
        first_row_ordinal,
        row_count: row_count as u32,
        first_epoch,
        first_sequence,
        last_epoch,
        last_sequence,
        min_occurred_at_physical: min_occurred,
        max_occurred_at_physical: max_occurred,
        min_ingested_at_physical: min_ingested,
        max_ingested_at_physical: max_ingested,
        compressed_bytes_estimate: residual_bytes.len() as u64,
    };

    let required_blocks = [
        PendingBlock {
            column_id: column_ids::EPOCH,
            presence: Vec::new(),
            data: ColumnData::U64(epoch),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::SEQUENCE,
            presence: Vec::new(),
            data: ColumnData::U64(sequence),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::STREAM_ID,
            presence: Vec::new(),
            data: ColumnData::U64(stream_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::STREAM_SEQUENCE,
            presence: Vec::new(),
            data: ColumnData::U64(stream_sequence),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::OCCURRED_AT,
            presence: Vec::new(),
            data: ColumnData::I64(occurred),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::INGESTED_AT,
            presence: Vec::new(),
            data: ColumnData::I64(ingested),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::SOURCE_ID,
            presence: Vec::new(),
            data: ColumnData::U64(source_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::EVENT_TYPE_ID,
            presence: Vec::new(),
            data: ColumnData::U64(event_type_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ENTITY_TYPE_ID,
            presence: Vec::new(),
            data: ColumnData::U64(entity_type_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ENTITY_ID_HASH_LOW,
            presence: Vec::new(),
            data: ColumnData::U64(entity_hash_low),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ENTITY_ID_HASH_HIGH,
            presence: Vec::new(),
            data: ColumnData::U64(entity_hash_high),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::PAYLOAD_REF,
            presence: Vec::new(),
            data: ColumnData::U64(payload_ref),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::FLAGS,
            presence: Vec::new(),
            data: ColumnData::U64(flags),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::SCHEMA_VERSION,
            presence: Vec::new(),
            data: ColumnData::U64(schema_version),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::EVENT_ID,
            presence: Vec::new(),
            data: ColumnData::U128(event_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ENTITY_ID,
            presence: Vec::new(),
            data: ColumnData::Strings(entity_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ACTOR_ID,
            presence: Vec::new(),
            data: ColumnData::Strings(actor_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ACCOUNT_ID,
            presence: Vec::new(),
            data: ColumnData::Strings(account_id),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ACTOR_ID_HASH_LOW,
            presence: Vec::new(),
            data: ColumnData::U64(actor_hash_low),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::ACCOUNT_ID_HASH_LOW,
            presence: Vec::new(),
            data: ColumnData::U64(account_hash_low),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::TRACE_ID_HASH_LOW,
            presence: Vec::new(),
            data: ColumnData::U64(trace_hash_low),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::DEDUPE_HASH_LOW,
            presence: Vec::new(),
            data: ColumnData::U64(dedupe_low),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::DEDUPE_HASH_HIGH,
            presence: Vec::new(),
            data: ColumnData::U64(dedupe_high),
            random_access: false,
        },
        PendingBlock {
            column_id: column_ids::PAYLOAD_FLAGS,
            presence: Vec::new(),
            data: ColumnData::U64(payload_flags),
            random_access: false,
        },
    ];
    let mut blocks = Vec::with_capacity(
        required_blocks.len()
            + promoted_values.len()
            + shred_plan.len()
            + freetext.len()
            + if carries_provenance {
                PROVENANCE_COLUMNS.len()
            } else {
                0
            }
            + if carries_relationships {
                relationship_columns.len()
            } else {
                0
            },
    );
    blocks.extend(required_blocks);
    for (index, (bitmap, values)) in promoted_values.into_iter().enumerate() {
        blocks.push(PendingBlock {
            column_id: column_ids::PROMOTED_BASE + index as u32,
            presence: bitmap,
            data: values.into_column_data()?,
            random_access: false,
        });
    }
    for (entry, (bitmap, values)) in shred_plan.iter().zip(shredded_values) {
        blocks.push(PendingBlock {
            column_id: entry.column_id,
            presence: bitmap,
            data: values.into_shredded_column_data()?,
            random_access: true,
        });
    }
    for (entry, (bitmap, values)) in freetext.iter().zip(freetext_values) {
        blocks.push(PendingBlock {
            column_id: entry.column_id,
            presence: bitmap,
            data: ColumnData::Strings(values),
            random_access: true,
        });
    }

    if carries_provenance {
        for (column_id, data) in [
            (column_ids::AUTHOR_PUBKEY, ColumnData::Strings(author_pubkey)),
            (column_ids::SIGNATURE, ColumnData::Strings(signature)),
            (column_ids::SIGNATURE_SCHEME, ColumnData::Strings(signature_scheme)),
            (column_ids::PROTOCOL_EVENT_ID, ColumnData::Strings(protocol_event_id)),
            (column_ids::PROTOCOL_KIND, ColumnData::I64(protocol_kind)),
            (column_ids::CLAIMED_AT, ColumnData::I64(claimed_at)),
        ] {
            blocks.push(PendingBlock {
                column_id,
                presence: provenance_presence.clone(),
                data,
                random_access: true,
            });
        }
    }

    if carries_relationships {
        for ((presence, values), column_id) in relationship_columns.into_iter().zip(RELATIONSHIP_COLUMN_IDS) {
            blocks.push(PendingBlock {
                column_id,
                presence,
                data: ColumnData::Strings(values),
                random_access: true,
            });
        }
    }

    let mut elided_entity_hashes = Vec::new();
    if elide_entity_hash {
        let mut kept = Vec::with_capacity(blocks.len());
        for block in blocks {
            match (block.column_id, block.data) {
                (column_ids::ENTITY_ID_HASH_LOW, ColumnData::U64(values)) if wants(column_ids::ENTITY_ID_HASH_LOW) => {
                    elided_entity_hashes = values;
                }
                (column_ids::ENTITY_ID_HASH_LOW | column_ids::ENTITY_ID_HASH_HIGH, _) => {}
                (column_id, data) => kept.push(PendingBlock {
                    column_id,
                    data,
                    ..block
                }),
            }
        }
        blocks = kept;
    }
    if wanted.is_some() {
        blocks.retain(|block| wants(block.column_id));
    }

    Ok(GranulePieces {
        entry,
        blocks,
        by_entity_type,
        by_event_type,
        by_source,
        dictionary_bytes,
        elided_entity_hashes,
        freetext_row_offsets,
        has_internal_late_event,
        offsets_bytes,
        residual_bytes,
    })
}

#[cfg(test)]
#[path = "test/build.rs"]
mod tests;
