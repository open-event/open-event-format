use super::api::{AuthChains, EventGraph, EventIds, SubjectState};
use super::cache::{BlockCache, BlockKey, BlockKind};
use super::constant::{
    GRAPH_FETCH_GAP_BYTES, GRAPH_FETCH_WINDOW_BYTES, GRAPH_HEDGE_MIN_SAMPLES, GRAPH_HEDGE_SAMPLES, HELD_RANGE_BUCKETS,
};
use super::footer::{Footer, StripeEntry};
use super::graph::{ChainLabel, ChainLink, EdgeKind, GranuleEdges};
use super::reader::{DECODED_CACHE_BUDGET_BYTES, HeaderCommitment, OeFile, OeFooter, VerifiedStripeRead};
use super::required_features;
use super::{LayoutClass, OeHeader};
use crate::artifacts::state_chunk::{self, StateChange, StateChunkStore, StateKey, StateRoot};
use crate::clock::Clock;
use crate::columns::column_ids;
use crate::error::{FormatError, StateChunkError};
use crate::events::TenantId;
use crate::file::bytes::slice;
use crate::file::{ByteRange, FileError, RangeSource};
use crate::lifecycle::OeFileEntry;
use crate::security::StateChunkKey;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};
use std::time::Duration;

#[derive(Debug)]
pub(super) enum FileBytes {
    Local(Arc<Vec<u8>>),
    Remote(Box<RemoteFile>),
}

impl FileBytes {
    pub(super) fn fresh(&self) -> Self {
        match self {
            FileBytes::Local(bytes) => FileBytes::Local(Arc::clone(bytes)),
            FileBytes::Remote(remote) => FileBytes::Remote(Box::new(remote.fresh())),
        }
    }
}

pub(super) struct RemoteFile {
    cache: Option<Arc<BlockCache>>,
    file_id: u128,
    footer: Arc<OeFooter>,
    held: HeldRanges,
    marks_extents: Arc<Vec<(u64, u64)>>,
    source: Arc<dyn RangeSource>,
    tail: Arc<[u8]>,
    tail_start: u64,
    tail_served_from: u64,
    tenant_id: TenantId,
}

impl std::fmt::Debug for RemoteFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteFile")
            .field("file_id", &self.file_id)
            .field("tail_start", &self.tail_start)
            .finish_non_exhaustive()
    }
}

impl OeFile {
    pub fn open_remote(
        source: Arc<dyn RangeSource>,
        cache: Option<Arc<BlockCache>>,
        entry: &OeFileEntry,
        header: &HeaderCommitment,
        footer_dek: Option<&[u8; 32]>,
    ) -> Result<Self, FormatError> {
        if header.file_id != entry.file_id {
            return Err(FormatError::Structural {
                rule: "header commitment names a different file than the manifest entry",
            });
        }
        let (tail_start, tail, opened) = open_tail(source.as_ref(), cache.as_deref(), entry, header, footer_dek)?;
        let footer = opened.footer().clone();
        if footer.profile != entry.profile {
            return Err(FormatError::Structural {
                rule: "manifest entry profile must match footer profile",
            });
        }
        let usable_optional_features = opened.usable_optional_features();
        let remote = RemoteFile {
            cache,
            file_id: entry.file_id,
            held: HeldRanges::new(),
            marks_extents: Arc::new(marks_extents(&footer)),
            source,
            tail,
            tail_served_from: data_area_end(&footer).max(tail_start),
            tail_start,
            tenant_id: entry.tenant_id,
            footer: Arc::new(opened),
        };
        Self::assemble(
            FileBytes::Remote(Box::new(remote)),
            remote_header(&footer, entry, header),
            footer,
            usable_optional_features,
            None,
            DECODED_CACHE_BUDGET_BYTES,
        )
    }
}

fn open_tail(
    source: &dyn RangeSource,
    cache: Option<&BlockCache>,
    entry: &OeFileEntry,
    header: &HeaderCommitment,
    footer_dek: Option<&[u8; 32]>,
) -> Result<(u64, Arc<[u8]>, OeFooter), FormatError> {
    let range = entry.tail_range();
    let open =
        |tail: &[u8]| OeFooter::open_authenticated_tail(tail, entry.size_bytes, &entry.file_seal, header, footer_dek);
    let key = |offset: u64, len: u64| BlockKey {
        file_id: entry.file_id,
        kind: BlockKind::Footer,
        len,
        offset,
        tenant_id: entry.tenant_id,
    };
    let suffix_key = key(range.start, range.len);
    let open_from = |suffix: Arc<[u8]>, cached: bool| -> Result<(u64, Arc<[u8]>, OeFooter), FormatError> {
        let mut start = range.start;
        let mut tail = Arc::clone(&suffix);
        let mut fetched_prefix = None;
        if !range.exact
            && let Some(exact_len) = OeFooter::tail_shortfall(&suffix)?
        {
            let exact_start = entry.size_bytes - exact_len.min(entry.size_bytes);
            if exact_start < range.start {
                let prefix_key = key(exact_start, range.start - exact_start);
                let prefix: Arc<[u8]> = match cache.filter(|_| cached).and_then(|cache| cache.get(&prefix_key)) {
                    Some(prefix) => prefix,
                    None => {
                        let prefix: Arc<[u8]> =
                            fetch_exact(source, entry.file_id, prefix_key.offset, prefix_key.len)?.into();
                        fetched_prefix = Some((prefix_key, Arc::clone(&prefix)));
                        prefix
                    }
                };
                start = exact_start;
                tail = [&prefix[..], &suffix[..]].concat().into();
            }
        }
        let footer = open(&tail)?;
        if let Some(cache) = cache {
            if !cached {
                cache.insert(suffix_key, suffix);
            }
            if let Some((prefix_key, prefix)) = fetched_prefix {
                cache.insert(prefix_key, prefix);
            }
        }
        Ok((start, tail, footer))
    };
    if let Some(cache) = cache
        && let Some(suffix) = cache.get(&suffix_key)
    {
        match open_from(suffix, true) {
            Ok(opened) => return Ok(opened),
            Err(_) => cache.remove(&suffix_key),
        }
    }
    open_from(
        fetch_exact(source, entry.file_id, range.start, range.len)?.into(),
        false,
    )
}

fn fetch_exact(source: &dyn RangeSource, object: u128, offset: u64, len: u64) -> Result<Vec<u8>, FormatError> {
    let bytes = source.read_range(object, offset, len).map_err(unavailable)?;
    if bytes.len() as u64 != len {
        return Err(FormatError::Unavailable {
            detail: format!("asked for {len} bytes at offset {offset}, got {}", bytes.len()),
        });
    }
    Ok(bytes)
}

fn unavailable(error: FileError) -> FormatError {
    FormatError::Unavailable {
        detail: error.to_string(),
    }
}

fn marks_extents(footer: &Footer) -> Vec<(u64, u64)> {
    footer
        .stripes
        .iter()
        .filter_map(|stripe| {
            let pages = footer
                .marks_page_offsets
                .iter()
                .filter(|entry| entry.stripe_id == stripe.stripe_id);
            let start = pages.clone().map(|entry| entry.page_offset).min()?;
            let end = pages
                .map(|entry| entry.page_offset.saturating_add(entry.page_len))
                .max()?;
            Some((
                stripe.file_offset.saturating_add(start),
                stripe.file_offset.saturating_add(end),
            ))
        })
        .collect()
}

fn data_area_end(footer: &Footer) -> u64 {
    let stripes = footer
        .stripes
        .iter()
        .map(|stripe| stripe.file_offset.saturating_add(stripe.byte_len));
    let gaps = footer
        .integrity_gaps
        .iter()
        .map(|gap| gap.file_offset.saturating_add(gap.byte_len));
    stripes.chain(gaps).max().unwrap_or(0)
}

fn remote_header(footer: &Footer, entry: &OeFileEntry, commitment: &HeaderCommitment) -> OeHeader {
    let granules = &footer.granules;
    OeHeader {
        created_at_physical: 0,
        feature_flags: commitment.feature_flags,
        file_id: commitment.file_id,
        footer_pointer_hint: 0,
        generation_id: 0,
        layout_class: LayoutClass::Compact,
        max_epoch: granules.last().map_or(0, |granule| granule.last_epoch),
        max_ingested_at_physical: granules
            .iter()
            .map(|granule| granule.max_ingested_at_physical)
            .max()
            .unwrap_or(0),
        max_occurred_at_physical: granules
            .iter()
            .map(|granule| granule.max_occurred_at_physical)
            .max()
            .unwrap_or(0),
        max_sequence: granules.last().map_or(0, |granule| granule.last_sequence),
        min_epoch: granules.first().map_or(0, |granule| granule.first_epoch),
        min_ingested_at_physical: granules
            .iter()
            .map(|granule| granule.min_ingested_at_physical)
            .min()
            .unwrap_or(0),
        min_occurred_at_physical: granules
            .iter()
            .map(|granule| granule.min_occurred_at_physical)
            .min()
            .unwrap_or(0),
        min_sequence: granules.first().map_or(0, |granule| granule.first_sequence),
        profile: footer.profile,
        projection_count: 1,
        row_count: footer.exact_counts.row_count,
        tenant_id: entry.tenant_id,
        version_major: footer.format_version.0,
        version_minor: footer.format_version.1,
    }
}

impl RemoteFile {
    fn fresh(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            file_id: self.file_id,
            footer: Arc::clone(&self.footer),
            held: HeldRanges::new(),
            marks_extents: Arc::clone(&self.marks_extents),
            source: Arc::clone(&self.source),
            tail: Arc::clone(&self.tail),
            tail_served_from: self.tail_served_from,
            tail_start: self.tail_start,
            tenant_id: self.tenant_id,
        }
    }

    pub(super) fn read(&self, position: u64, len: usize, what: &'static str) -> Result<&[u8], FormatError> {
        if len == 0 {
            return Ok(&[]);
        }
        let end = position
            .checked_add(len as u64)
            .ok_or(FormatError::RefOutOfRange { what })?;
        if let Some((start, bytes)) = self.held.find(position, end) {
            return Ok(slice(bytes, (position - start) as usize, len, what)?);
        }
        let Some(stripe) = self.stripe_holding(position, end) else {
            if position >= self.tail_served_from {
                return Ok(slice(&self.tail, (position - self.tail_start) as usize, len, what)?);
            }
            return Err(FormatError::RefOutOfRange {
                what: "remote read outside any one stripe",
            });
        };
        let stripe_end = stripe.file_offset + stripe.byte_len;
        let (want_start, want_end) = self
            .marks_extents
            .iter()
            .find(|(start, extent_end)| {
                *start <= position && end <= *extent_end && stripe.file_offset <= *start && *extent_end <= stripe_end
            })
            .copied()
            .unwrap_or((position, end));
        let (start, bytes) = self.fetch_stripe_range(stripe, want_start, want_end)?;
        let held = self.held.hold(start, bytes)?;
        Ok(slice(held, (position - start) as usize, len, what)?)
    }

    fn stripe_holding(&self, start: u64, end: u64) -> Option<&StripeEntry> {
        self.footer.footer().stripes.iter().find(|stripe| {
            stripe.file_offset <= start
                && stripe
                    .file_offset
                    .checked_add(stripe.byte_len)
                    .is_some_and(|stripe_end| end <= stripe_end)
        })
    }

    fn plan_prefetch(&self, ranges: &[(u64, u64)]) -> Result<Vec<VerifiedStripeRead>, FormatError> {
        let mut wanted: Vec<(u64, u64)> = ranges
            .iter()
            .filter(|(start, end)| start < end && self.held.find(*start, *end).is_none())
            .copied()
            .collect();
        wanted.sort_unstable();
        let mut pieces: Vec<VerifiedStripeRead> = Vec::new();
        for (start, end) in wanted {
            if self.held.find(start, end).is_some() {
                continue;
            }
            let stripe = self.stripe_holding(start, end).ok_or(FormatError::RefOutOfRange {
                what: "remote read outside any one stripe",
            })?;
            let plan =
                self.footer
                    .plan_verified_stripe_read(stripe.stripe_id, start - stripe.file_offset, end - start)?;
            if self.hold_cached(&plan)? {
                continue;
            }
            let plan_end = plan.file_offset + plan.length;
            if let Some(last) = pieces.last_mut()
                && last.stripe_id == plan.stripe_id
            {
                let last_end = last.file_offset + last.length;
                if plan_end <= last_end {
                    continue;
                }
                if plan.file_offset <= last_end.saturating_add(GRAPH_FETCH_GAP_BYTES)
                    && plan_end - last.file_offset <= GRAPH_FETCH_WINDOW_BYTES
                {
                    *last = self.footer.plan_verified_stripe_read(
                        stripe.stripe_id,
                        last.file_offset - stripe.file_offset,
                        plan_end - last.file_offset,
                    )?;
                    continue;
                }
            }
            pieces.push(plan);
        }
        let mut planned = Vec::with_capacity(pieces.len());
        for piece in pieces {
            if !self.hold_cached(&piece)? {
                planned.push(piece);
            }
        }
        Ok(planned)
    }

    fn hold_cached(&self, piece: &VerifiedStripeRead) -> Result<bool, FormatError> {
        let Some(cache) = &self.cache else {
            return Ok(false);
        };
        let key = self.graph_key(piece);
        let Some(bytes) = cache.get(&key) else {
            return Ok(false);
        };
        if self
            .footer
            .verify_stripe_range_in(piece, &bytes, piece.file_offset)
            .is_err()
        {
            cache.remove(&key);
            return Ok(false);
        }
        self.held.hold(piece.file_offset, bytes)?;
        Ok(true)
    }

    fn admit(&self, piece: &VerifiedStripeRead, bytes: &[u8]) -> Result<(), FormatError> {
        if self
            .footer
            .verify_stripe_range_in(piece, bytes, piece.file_offset)
            .is_err()
        {
            return Ok(());
        }
        let bytes: Arc<[u8]> = bytes.into();
        if let Some(cache) = &self.cache {
            cache.insert(self.graph_key(piece), Arc::clone(&bytes));
        }
        self.held.hold(piece.file_offset, bytes)?;
        Ok(())
    }

    fn graph_key(&self, piece: &VerifiedStripeRead) -> BlockKey {
        BlockKey {
            file_id: self.file_id,
            kind: BlockKind::GraphIndex,
            len: piece.length,
            offset: piece.file_offset,
            tenant_id: self.tenant_id,
        }
    }

    fn fetch_stripe_range(&self, stripe: &StripeEntry, start: u64, end: u64) -> Result<(u64, Arc<[u8]>), FormatError> {
        let planned =
            self.footer
                .plan_verified_stripe_read(stripe.stripe_id, start - stripe.file_offset, end - start)?;
        let read = self.footer.plan_verified_stripe_read(
            stripe.stripe_id,
            planned.file_offset - stripe.file_offset,
            planned.length,
        )?;
        if let Some(bytes) = self.fetch_verified(&read)? {
            return Ok((read.file_offset, bytes));
        }
        let whole = self
            .footer
            .plan_whole_stripe_read(stripe.stripe_id, 0, stripe.byte_len)?;
        if whole != read
            && let Some(bytes) = self.fetch_verified(&whole)?
        {
            return Ok((whole.file_offset, bytes));
        }
        Err(FormatError::Blake3Mismatch {
            scope: "remote stripe range",
        })
    }

    fn fetch_verified(&self, read: &VerifiedStripeRead) -> Result<Option<Arc<[u8]>>, FormatError> {
        let proves = |bytes: &[u8]| {
            self.footer
                .verify_stripe_range_in(read, bytes, read.file_offset)
                .is_ok()
        };
        let key = BlockKey {
            file_id: self.file_id,
            kind: BlockKind::Stripe,
            len: read.length,
            offset: read.file_offset,
            tenant_id: self.tenant_id,
        };
        if let Some(cache) = &self.cache
            && let Some(bytes) = cache.get(&key)
        {
            if proves(&bytes) {
                return Ok(Some(bytes));
            }
            cache.remove(&key);
        }
        let bytes: Arc<[u8]> = fetch_exact(self.source.as_ref(), self.file_id, read.file_offset, read.length)?.into();
        if !proves(&bytes) {
            return Ok(None);
        }
        if let Some(cache) = &self.cache {
            cache.insert(key, Arc::clone(&bytes));
        }
        Ok(Some(bytes))
    }
}

struct HeldRanges {
    by_start: RwLock<BTreeMap<u64, (u64, usize)>>,
    slots: HeldSlots,
}

impl HeldRanges {
    fn new() -> Self {
        Self {
            by_start: RwLock::new(BTreeMap::new()),
            slots: HeldSlots::new(),
        }
    }

    fn find(&self, start: u64, end: u64) -> Option<(u64, &[u8])> {
        let (held_start, slot) = {
            let index = self.by_start.read().unwrap_or_else(PoisonError::into_inner);
            index
                .range(..=start)
                .rev()
                .find(|(_, (held_end, _))| *held_end >= end)
                .map(|(held_start, (_, slot))| (*held_start, *slot))?
        };
        self.slots.get(slot).map(|bytes| (held_start, bytes))
    }

    fn hold(&self, start: u64, bytes: Arc<[u8]>) -> Result<&[u8], FormatError> {
        let end = start + bytes.len() as u64;
        let (slot, held) = self.slots.push(bytes)?;
        let mut index = self.by_start.write().unwrap_or_else(PoisonError::into_inner);
        if index.get(&start).is_none_or(|(held_end, _)| *held_end < end) {
            index.insert(start, (end, slot));
        }
        Ok(held)
    }
}

type HeldBucket = Box<[OnceLock<Arc<[u8]>>]>;

struct HeldSlots {
    buckets: [OnceLock<HeldBucket>; HELD_RANGE_BUCKETS],
    next: AtomicUsize,
}

impl HeldSlots {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| OnceLock::new()),
            next: AtomicUsize::new(0),
        }
    }

    fn position(slot: usize) -> (usize, usize) {
        let n = slot + 1;
        let bucket = (usize::BITS - 1 - n.leading_zeros()) as usize;
        (bucket, n - (1 << bucket))
    }

    fn push(&self, bytes: Arc<[u8]>) -> Result<(usize, &[u8]), FormatError> {
        let slot = self.next.fetch_add(1, Ordering::Relaxed);
        let (bucket, index) = Self::position(slot);
        let full = || FormatError::Structural {
            rule: "remote reader holds more fetched ranges than it can index",
        };
        let cells = self
            .buckets
            .get(bucket)
            .ok_or_else(full)?
            .get_or_init(|| (0..1usize << bucket).map(|_| OnceLock::new()).collect());
        let cell = cells.get(index).ok_or_else(full)?;
        let held = cell.get_or_init(|| bytes);
        Ok((slot, &**held))
    }

    fn get(&self, slot: usize) -> Option<&[u8]> {
        let (bucket, index) = Self::position(slot);
        self.buckets.get(bucket)?.get()?.get(index)?.get().map(|bytes| &**bytes)
    }
}

fn coalesce(pieces: &[VerifiedStripeRead]) -> Vec<(ByteRange, Range<usize>)> {
    let mut requests: Vec<(ByteRange, Range<usize>)> = Vec::new();
    for (index, piece) in pieces.iter().enumerate() {
        let piece_end = piece.file_offset + piece.length;
        if let Some((request, covered)) = requests.last_mut() {
            let request_end = request.offset + request.len;
            let joined_end = piece_end.max(request_end);
            if piece.file_offset <= request_end.saturating_add(GRAPH_FETCH_GAP_BYTES)
                && joined_end - request.offset <= GRAPH_FETCH_WINDOW_BYTES
            {
                request.len = joined_end - request.offset;
                covered.end = index + 1;
                continue;
            }
        }
        let request = ByteRange {
            len: piece.length,
            offset: piece.file_offset,
        };
        requests.push((request, index..index + 1));
    }
    requests
}

type BatchAnswer = Result<Vec<Vec<u8>>, FileError>;

fn spawn_batch(
    source: &Arc<dyn RangeSource>,
    object: u128,
    asked: &Arc<Vec<ByteRange>>,
    sender: mpsc::Sender<BatchAnswer>,
) -> Result<(), FormatError> {
    let source = Arc::clone(source);
    let asked = Arc::clone(asked);
    std::thread::Builder::new()
        .name("oe-graph-get".to_owned())
        .spawn(move || {
            sender.send(source.read_ranges(object, &asked)).ok();
        })
        .map(drop)
        .map_err(|error| FormatError::Unavailable {
            detail: error.to_string(),
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteGraphOptions {
    pub hedge_floor_nanos: u64,
    pub max_in_flight: usize,
}

pub struct RemoteGraph {
    clock: Arc<dyn Clock>,
    files: Vec<OeFile>,
    latencies: Mutex<VecDeque<u64>>,
    options: RemoteGraphOptions,
}

impl RemoteGraph {
    pub fn new(files: Vec<OeFile>, clock: Arc<dyn Clock>, options: RemoteGraphOptions) -> Result<Self, FormatError> {
        if options.max_in_flight == 0 {
            return Err(FormatError::Structural {
                rule: "a remote graph keeps at least one request in flight",
            });
        }
        let mut spans: Vec<(&[u8], u64, u64)> = Vec::with_capacity(files.len());
        for file in &files {
            let footer = file.footer();
            let graph = footer.graph.as_ref().ok_or(FormatError::Structural {
                rule: "a remote graph reads only graph-profile files that carry edges",
            })?;
            let end = graph.first_sn.saturating_add(footer.exact_counts.row_count);
            spans.push((&graph.subject, graph.first_sn, end));
        }
        spans.sort_unstable();
        if spans
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.0 == right.0 && right.1 < left.2))
        {
            return Err(FormatError::Structural {
                rule: "two graph files of one subject hold the same event",
            });
        }
        Ok(Self {
            clock,
            files,
            latencies: Mutex::new(VecDeque::with_capacity(GRAPH_HEDGE_SAMPLES)),
            options,
        })
    }

    pub fn state<'a>(&'a self, chunks: &'a dyn StateChunkStore, key: &'a StateChunkKey) -> RemoteState<'a> {
        RemoteState {
            chunks,
            graph: self,
            key,
        }
    }

    fn subject_files<'a>(&'a self, subject: &'a [u8]) -> impl Iterator<Item = (usize, &'a OeFile)> + 'a {
        self.files.iter().enumerate().filter(move |(_, file)| {
            file.footer()
                .graph
                .as_ref()
                .is_some_and(|graph| graph.subject == subject)
        })
    }

    fn locate(&self, subject: &[u8], sn: u64) -> Result<(usize, u32), FormatError> {
        self.subject_files(subject)
            .find_map(|(index, file)| Some((index, file.graph_granule_of(sn)?)))
            .ok_or(FormatError::RefOutOfRange {
                what: "no graph file holds the event",
            })
    }

    fn edges_of<T: Default>(
        &self,
        subject: &[u8],
        sns: &[u64],
        read: impl Fn(&GranuleEdges, u64) -> T,
    ) -> Result<Vec<T>, FormatError> {
        let located: Vec<(usize, u32)> = sns
            .iter()
            .map(|&sn| self.locate(subject, sn))
            .collect::<Result<_, _>>()?;
        let granules: BTreeSet<(usize, u32)> = located.iter().copied().collect();
        let mut ranges: BTreeMap<usize, Vec<(u64, u64)>> = BTreeMap::new();
        for &(index, granule_id) in &granules {
            let Some(file) = self.files.get(index) else {
                continue;
            };
            let (position, len) = file.graph_block_range(granule_id)?;
            let end = position.checked_add(len).ok_or(FormatError::RefOutOfRange {
                what: "graph edge block range",
            })?;
            ranges.entry(index).or_default().push((position, end));
        }
        for (index, ranges) in &ranges {
            if let Some(remote) = self.files.get(*index).and_then(OeFile::remote) {
                self.prefetch(remote, ranges)?;
            }
        }
        let mut decoded: BTreeMap<(usize, u32), Arc<GranuleEdges>> = BTreeMap::new();
        for &(index, granule_id) in &granules {
            if let Some(file) = self.files.get(index) {
                decoded.insert((index, granule_id), file.cached_granule_edges(granule_id)?);
            }
        }
        Ok(sns
            .iter()
            .zip(&located)
            .map(|(&sn, key)| decoded.get(key).map(|edges| read(edges, sn)).unwrap_or_default())
            .collect())
    }

    fn prefetch_external_ids(&self, file: &OeFile, remote: &RemoteFile, sns: &[u64]) -> Result<(), FormatError> {
        let footer = file.footer();
        let granules: BTreeSet<u32> = sns.iter().filter_map(|&sn| file.graph_granule_of(sn)).collect();
        let stripes: BTreeSet<u32> = footer
            .granules
            .iter()
            .filter(|granule| granules.contains(&granule.granule_id))
            .map(|granule| granule.stripe_id)
            .collect();
        let marks: Vec<(u64, u64)> = footer
            .stripes
            .iter()
            .filter(|stripe| stripes.contains(&stripe.stripe_id))
            .flat_map(|stripe| {
                let stripe_end = stripe.file_offset.saturating_add(stripe.byte_len);
                remote
                    .marks_extents
                    .iter()
                    .copied()
                    .filter(move |(start, end)| stripe.file_offset <= *start && *end <= stripe_end)
            })
            .collect();
        self.prefetch(remote, &marks)?;
        let columns: &[u32] = if footer.required_feature_flags & required_features::EXTERNAL_ID_EVENT_ID_ELISION != 0 {
            &[column_ids::EXTERNAL_ID, column_ids::EVENT_ID]
        } else {
            &[column_ids::EXTERNAL_ID]
        };
        let mut blocks = Vec::with_capacity(granules.len() * columns.len());
        for &granule_id in &granules {
            for &column_id in columns {
                let Some(mark) = file.mark(column_id, 0, granule_id)? else {
                    continue;
                };
                if mark.page_count > 1 || mark.compressed_size == 0 {
                    continue;
                }
                let start = file.file_position(granule_id, mark.compressed_offset)? as u64;
                blocks.push((start, start.saturating_add(mark.compressed_size)));
            }
        }
        self.prefetch(remote, &blocks)
    }

    fn prefetch(&self, remote: &RemoteFile, ranges: &[(u64, u64)]) -> Result<(), FormatError> {
        let pieces = remote.plan_prefetch(ranges)?;
        let requests = coalesce(&pieces);
        for batch in requests.chunks(self.options.max_in_flight) {
            let asked: Vec<ByteRange> = batch.iter().map(|(request, _)| *request).collect();
            let replies = self.fetch(&remote.source, remote.file_id, asked)?;
            if replies.len() != batch.len() {
                return Err(FormatError::Unavailable {
                    detail: format!("asked for {} ranges, got {}", batch.len(), replies.len()),
                });
            }
            for ((request, covered), reply) in batch.iter().zip(&replies) {
                if reply.len() as u64 != request.len {
                    return Err(FormatError::Unavailable {
                        detail: format!(
                            "asked for {} bytes at offset {}, got {}",
                            request.len,
                            request.offset,
                            reply.len()
                        ),
                    });
                }
                for piece in pieces.get(covered.clone()).unwrap_or_default() {
                    let start = (piece.file_offset - request.offset) as usize;
                    let bytes = slice(reply, start, piece.length as usize, "coalesced graph request")?;
                    remote.admit(piece, bytes)?;
                }
            }
        }
        Ok(())
    }

    fn fetch(
        &self,
        source: &Arc<dyn RangeSource>,
        object: u128,
        asked: Vec<ByteRange>,
    ) -> Result<Vec<Vec<u8>>, FormatError> {
        let started = self.clock.now_nanos();
        let answer = if asked.len().saturating_mul(2) > self.options.max_in_flight {
            source.read_ranges(object, &asked).map_err(unavailable)
        } else {
            self.fetch_hedged(source, object, &Arc::new(asked))
        };
        self.record(self.clock.now_nanos().saturating_sub(started));
        answer
    }

    fn fetch_hedged(
        &self,
        source: &Arc<dyn RangeSource>,
        object: u128,
        asked: &Arc<Vec<ByteRange>>,
    ) -> Result<Vec<Vec<u8>>, FormatError> {
        let no_answer = || FormatError::Unavailable {
            detail: "graph request thread ended without an answer".to_owned(),
        };
        let (sender, receiver) = mpsc::channel();
        spawn_batch(source, object, asked, sender.clone())?;
        let first = match receiver.recv_timeout(Duration::from_nanos(self.hedge_delay())) {
            Ok(answer) => return answer.map_err(unavailable),
            Err(RecvTimeoutError::Disconnected) => return Err(no_answer()),
            Err(RecvTimeoutError::Timeout) => {
                spawn_batch(source, object, asked, sender)?;
                receiver.recv().map_err(|_| no_answer())?
            }
        };
        match first {
            Ok(replies) => Ok(replies),
            Err(_) => receiver.recv().map_err(|_| no_answer())?.map_err(unavailable),
        }
    }

    fn hedge_delay(&self) -> u64 {
        let floor = self.options.hedge_floor_nanos;
        let mut buffer = [0u64; GRAPH_HEDGE_SAMPLES];
        let count = {
            let latencies = self.latencies.lock().unwrap_or_else(PoisonError::into_inner);
            if latencies.len() < GRAPH_HEDGE_MIN_SAMPLES {
                return floor;
            }
            for (slot, sample) in buffer.iter_mut().zip(latencies.iter()) {
                *slot = *sample;
            }
            latencies.len().min(GRAPH_HEDGE_SAMPLES)
        };
        let samples = buffer.get_mut(..count).unwrap_or_default();
        let rank = (count * 95).div_ceil(100).saturating_sub(1);
        (*samples.select_nth_unstable(rank).1).max(floor)
    }

    fn record(&self, nanos: i64) {
        let mut latencies = self.latencies.lock().unwrap_or_else(PoisonError::into_inner);
        if latencies.len() == GRAPH_HEDGE_SAMPLES {
            latencies.pop_front();
        }
        latencies.push_back(u64::try_from(nanos).unwrap_or(0));
    }
}

impl EventGraph for RemoteGraph {
    fn parents_of(&self, subject: &[u8], sns: &[u64], kind: EdgeKind) -> Result<Vec<Vec<u64>>, FormatError> {
        self.edges_of(subject, sns, |edges, sn| edges.parents(sn, kind).to_vec())
    }

    fn children_of(&self, subject: &[u8], sns: &[u64], kind: EdgeKind) -> Result<Vec<Vec<u64>>, FormatError> {
        self.edges_of(subject, sns, |edges, sn| edges.children(sn, kind))
    }
}

impl EventIds for RemoteGraph {
    fn resolve_ids(&self, subject: &[u8], ids: &[&[u8]]) -> Result<Vec<Option<u64>>, FormatError> {
        let mut found: Vec<Option<u64>> = vec![None; ids.len()];
        for (_, file) in self.subject_files(subject) {
            let Some(index) = file.footer().graph.as_ref().and_then(|graph| graph.event_ids.as_ref()) else {
                continue;
            };
            let candidates: Vec<(usize, u64)> = ids
                .iter()
                .enumerate()
                .filter(|(position, _)| found.get(*position).is_some_and(Option::is_none))
                .flat_map(|(position, id)| index.candidates(id).iter().map(move |&sn| (position, sn)))
                .collect();
            if candidates.is_empty() {
                continue;
            }
            if let Some(remote) = file.remote() {
                let sns: Vec<u64> = candidates.iter().map(|(_, sn)| *sn).collect();
                self.prefetch_external_ids(file, remote, &sns)?;
            }
            for (position, sn) in candidates {
                let (Some(slot), Some(id)) = (found.get_mut(position), ids.get(position)) else {
                    continue;
                };
                if slot.is_some() {
                    continue;
                }
                if let Some(row) = file.graph_row_of(sn)
                    && file.external_id(row)?.as_deref() == Some(*id)
                {
                    *slot = Some(sn);
                }
            }
        }
        Ok(found)
    }
}

impl AuthChains for RemoteGraph {
    fn chain_labels(&self, subject: &[u8], sns: &[u64]) -> Result<Vec<Option<ChainLabel>>, FormatError> {
        self.edges_of(subject, sns, |edges, sn| edges.label(sn))
    }

    fn chain_links(&self, subject: &[u8], chains: &[u32]) -> Result<Vec<ChainLink>, FormatError> {
        let mut links: Vec<ChainLink> = self
            .subject_files(subject)
            .flat_map(|(_, file)| file.graph_chain_links(chains))
            .collect();
        links.sort_unstable();
        links.dedup();
        Ok(links)
    }
}

pub struct RemoteState<'a> {
    chunks: &'a dyn StateChunkStore,
    graph: &'a RemoteGraph,
    key: &'a StateChunkKey,
}

impl SubjectState for RemoteState<'_> {
    fn state_root(&self, subject: &[u8], sn: u64) -> Result<Option<StateRoot>, FormatError> {
        let (index, _) = self.graph.locate(subject, sn)?;
        Ok(self.graph.files.get(index).and_then(|file| file.state_root(sn)))
    }

    fn state_get(&self, root: &StateRoot, key: StateKey) -> Result<Option<u64>, StateChunkError> {
        state_chunk::state_get(self.chunks, self.key, root, key)
    }

    fn state_diff(&self, before: &StateRoot, after: &StateRoot) -> Result<Vec<StateChange>, StateChunkError> {
        state_chunk::state_diff(self.chunks, self.key, before, after)
    }
}

#[cfg(test)]
#[path = "test/remote.rs"]
mod tests;
