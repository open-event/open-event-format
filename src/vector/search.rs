use super::constant::*;
use super::deletes::SegmentDeletes;
use super::error::VectorError;
use super::ivf::PartitionIndex;
use super::kernel::TopK;
use super::model::{EmbeddingModel, ScoredRow, SegmentManifest, VectorMetric};
use super::segment::{ColumnStats, SegmentReader, SegmentSource};
use crate::indexes::bitmap::{RoaringRangeBitmap, RowRange};
use std::collections::HashSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchFilter {
    pub room_ids: Option<Vec<String>>,
    pub senders: Option<Vec<String>>,
    pub time_range: Option<(i64, i64)>,
}

impl SearchFilter {
    pub(super) fn is_empty(&self) -> bool {
        self.room_ids.is_none() && self.senders.is_none() && self.time_range.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct VectorSearch<'a> {
    pub filter: SearchFilter,
    pub k: usize,
    pub model: &'a EmbeddingModel,
    /// Version 1–3 segments rerank `k × oversample` coarse candidates. Version 4 segments rerank by error bounds
    /// (decreasing upper bound until it falls below the k-th exact score) and use `oversample` as a patience: a
    /// segment stops reranking after `k × oversample` consecutive rows that did not enter its top k; 0 means no
    /// limit. Range searches ignore it.
    pub oversample: usize,
    pub query: &'a [f32],
}

/// Work done by one search, summed over segments.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchStats {
    /// Segments whose blocks were scanned.
    pub segments_searched: usize,
    /// Segments skipped because their score bound (or every partition bound) could not beat the running k-th score.
    pub segments_skipped: usize,
    /// Coarse rows estimated.
    pub rows_scanned: u64,
    /// Rows whose coarse upper bound survived the scan (bound-driven segments only).
    pub rows_candidate: u64,
    /// Rows scored with the fine representation.
    pub rows_reranked: u64,
    /// Partition-index partitions whose rows were scanned.
    pub partitions_probed: u64,
    /// Partition-index partitions never scanned because their bound could not beat the running k-th score.
    pub partitions_pruned: u64,
    /// Segments answered by scanning first and filtering the candidates (post-filter).
    pub segments_post_filtered: usize,
    /// Published partition indexes that failed to load or verify and were ignored.
    pub indexes_rejected: usize,
    /// A range search with no `k` matched more than its limit; the best `limit` hits were returned.
    pub range_truncated: bool,
}

/// Score bounds for a range search, in the metric's score order (higher is closer). For L2 the score is
/// `-distance²`, so a distance radius `r` is `min = -r²`; `ScoreRange::l2_radius` builds that.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ScoreRange {
    /// Hits score at least this.
    pub min: Option<f32>,
    /// Hits score at most this.
    pub max: Option<f32>,
}

impl ScoreRange {
    /// Rows within L2 distance `radius` of the query.
    pub fn l2_radius(radius: f32) -> Self {
        Self {
            min: Some(-(radius * radius)),
            max: None,
        }
    }

    /// Rows at L2 distance in `[inner, outer]` from the query.
    pub fn l2_annulus(inner: f32, outer: f32) -> Self {
        Self {
            min: Some(-(outer * outer)),
            max: Some(-(inner * inner)),
        }
    }

    pub fn contains(&self, score: f32) -> bool {
        self.min.is_none_or(|min| score >= min) && self.max.is_none_or(|max| score <= max)
    }

    fn floor(&self) -> f32 {
        self.min.unwrap_or(f32::NEG_INFINITY)
    }

    fn ceiling(&self) -> f32 {
        self.max.unwrap_or(f32::INFINITY)
    }

    fn is_bounded(&self) -> bool {
        self.min.is_some() || self.max.is_some()
    }
}

/// How a metadata filter is applied to one segment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FilterStrategy {
    /// Post-filter when the granule statistics estimate that the filter keeps at least
    /// `POST_FILTER_SELECTIVITY` of the rows, otherwise pre-filter.
    #[default]
    Adaptive,
    /// Decode the filter columns of every granule that may match and scan only matching rows.
    PreFilter,
    /// Scan without the filter, then filter the candidates, widening (and finally pre-filtering) if fewer than
    /// `k` survive.
    PostFilter,
}

/// Estimated kept fraction at or above which `FilterStrategy::Adaptive` post-filters.
pub const POST_FILTER_SELECTIVITY: f64 = 0.75;
/// Partitions probed first (closest centroids) when a segment has a partition index; the probe then widens to
/// every partition whose bound can still beat the k-th best score, so the result equals a full scan.
pub const DEFAULT_NPROBES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchOptions {
    pub filter_strategy: FilterStrategy,
    pub nprobes: usize,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            filter_strategy: FilterStrategy::Adaptive,
            nprobes: DEFAULT_NPROBES,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub event_id: String,
    pub row: u64,
    pub score: f32,
    pub segment: usize,
}

pub fn search_segments<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    search: &VectorSearch<'_>,
) -> Result<Vec<SearchHit>, VectorError> {
    search_segments_with_deletes(segments, &[], search)
}

pub fn search_segments_with_deletes<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    search: &VectorSearch<'_>,
) -> Result<Vec<SearchHit>, VectorError> {
    search_segments_with_stats(segments, deletes, search).map(|(hits, _)| hits)
}

/// `search_segments_with_deletes`, also reporting how much work the search did.
///
/// Segments are visited nearest centroid first. The running k-th best exact score is handed to each later segment,
/// which skips itself, or rows, whose score bound cannot reach it.
pub fn search_segments_with_stats<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    search: &VectorSearch<'_>,
) -> Result<(Vec<SearchHit>, SearchStats), VectorError> {
    search_segments_indexed(segments, deletes, &[], search, SearchOptions::default())
}

/// `search_segments_with_stats` with an optional partition index per segment (`indexes[i]` for `segments[i]`;
/// missing entries mean none). A segment with an index probes its partitions best bound first and stops once no
/// remaining partition can beat the k-th best score; one without is scanned. Both give the same result.
pub fn search_segments_indexed<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    indexes: &[Option<&PartitionIndex>],
    search: &VectorSearch<'_>,
    options: SearchOptions,
) -> Result<(Vec<SearchHit>, SearchStats), VectorError> {
    search_ranged(segments, deletes, indexes, search, ScoreRange::default(), options)
}

/// Range search: every live row (passing the filter) whose exact fine score lies in `range`, best first.
///
/// `limit` caps the hits like `k` (`search.k` is ignored); `None` means "all in range", capped at
/// `MAX_RANGE_RESULTS`, with `SearchStats::range_truncated` set if more matched. Segment, partition and row
/// bounds skip work that cannot fall in the range: a segment or partition whose upper bound is below `min` or
/// whose lower bound is above `max`, and coarse rows likewise. Results are exact with respect to the fine
/// representation (version 1–3 coarse codes have no bounds, so such segments are scored on the fine column).
pub fn search_segments_in_range<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    indexes: &[Option<&PartitionIndex>],
    search: &VectorSearch<'_>,
    range: ScoreRange,
    limit: Option<usize>,
    options: SearchOptions,
) -> Result<(Vec<SearchHit>, SearchStats), VectorError> {
    search_in_range_capped(
        segments,
        deletes,
        indexes,
        search,
        range,
        limit,
        options,
        MAX_RANGE_RESULTS,
    )
}

/// `search_segments_in_range` with the unlimited-result cap as a parameter (tests lower it).
#[allow(clippy::too_many_arguments)]
pub(super) fn search_in_range_capped<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    indexes: &[Option<&PartitionIndex>],
    search: &VectorSearch<'_>,
    range: ScoreRange,
    limit: Option<usize>,
    options: SearchOptions,
    max_results: usize,
) -> Result<(Vec<SearchHit>, SearchStats), VectorError> {
    if range.min.zip(range.max).is_some_and(|(min, max)| min > max) || range.min.is_some_and(f32::is_nan) {
        return Err(VectorError::InvalidInput {
            rule: "range minimum above its maximum".to_owned(),
        });
    }
    let cap = limit.unwrap_or(max_results);
    let wanted = if limit.is_none() { cap.saturating_add(1) } else { cap };
    let ranged = VectorSearch {
        k: wanted,
        ..search.clone()
    };
    let (mut hits, mut stats) = search_ranged(segments, deletes, indexes, &ranged, range, options)?;
    if hits.len() > cap {
        hits.truncate(cap);
        stats.range_truncated = true;
    }
    Ok((hits, stats))
}

fn search_ranged<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    indexes: &[Option<&PartitionIndex>],
    search: &VectorSearch<'_>,
    range: ScoreRange,
    options: SearchOptions,
) -> Result<(Vec<SearchHit>, SearchStats), VectorError> {
    let mut stats = SearchStats::default();
    let mut hits: Vec<(usize, ScoredRow)> = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        segment.manifest().model.require_same(search.model)?;
        if let Some(partitions) = indexes.get(index).copied().flatten() {
            partitions.check(segment)?;
        }
    }
    let allowed = AllowedValues::new(&search.filter);
    let order = nearest_segments(segments, search.query, search.model, segments.len())?;
    for index in order {
        let Some(segment) = segments.get(index) else {
            continue;
        };
        let deleted = deletes
            .get(index)
            .copied()
            .flatten()
            .filter(|d| d.deleted_count() > 0)
            .map(SegmentDeletes::bitmap);
        let threshold = if hits.len() >= search.k && search.k > 0 {
            hits.last().map_or(f32::NEG_INFINITY, |(_, row)| row.score)
        } else {
            f32::NEG_INFINITY
        }
        .max(range.floor());
        let scan = SegmentScan {
            options,
            range,
            partitions: indexes.get(index).copied().flatten(),
            search,
            segment,
        };
        let before = stats;
        let scored = if search.filter.is_empty() {
            let live = deleted.map(|deleted| RoaringRangeBitmap::from_ranges(all_rows(segment)).difference(&deleted));
            if live.as_ref().is_some_and(RoaringRangeBitmap::is_empty) {
                continue;
            }
            scan.run(live.as_ref(), search.k, threshold, &mut stats)?
        } else {
            let post = match options.filter_strategy {
                FilterStrategy::PreFilter => None,
                FilterStrategy::PostFilter => Some(()),
                FilterStrategy::Adaptive => {
                    (estimated_selectivity(segment, &search.filter) >= POST_FILTER_SELECTIVITY).then_some(())
                }
            };
            let post = match post {
                Some(()) => scan.post_filter(deleted.as_ref(), &allowed, threshold, &mut stats)?,
                None => None,
            };
            match post {
                Some(rows) => rows,
                None => {
                    let Some(mut filter) = row_filter(segment, &search.filter, &allowed)? else {
                        continue;
                    };
                    if let Some(deleted) = &deleted {
                        filter = filter.difference(deleted);
                    }
                    if filter.is_empty() {
                        continue;
                    }
                    scan.run(Some(&filter), search.k, threshold, &mut stats)?
                }
            }
        };
        stats.segments_skipped = before.segments_skipped;
        if stats.rows_scanned > before.rows_scanned {
            stats.segments_searched += 1;
        } else {
            stats.segments_skipped += 1;
        }
        hits.extend(scored.into_iter().map(|row| (index, row)));
        sort_hits(&mut hits);
        hits.truncate(search.k);
    }
    let mut by_segment: Vec<usize> = (0..hits.len()).collect();
    by_segment.sort_by_key(|position| hits.get(*position).map(|(index, row)| (*index, row.row)));
    let segment_of = |position: &usize| hits.get(*position).map(|(index, _)| *index);
    let mut event_ids = vec![String::new(); hits.len()];
    for positions in by_segment.chunk_by(|left, right| segment_of(left) == segment_of(right)) {
        let segment = positions
            .first()
            .and_then(segment_of)
            .and_then(|index| segments.get(index))
            .ok_or(VectorError::InvalidInput {
                rule: "segment index".to_owned(),
            })?;
        let rows: Vec<u64> = positions
            .iter()
            .filter_map(|position| hits.get(*position).map(|(_, row)| row.row))
            .collect();
        for (position, event_id) in positions.iter().zip(segment.event_ids(&rows)?) {
            if let Some(slot) = event_ids.get_mut(*position) {
                *slot = event_id;
            }
        }
    }
    let hits = hits
        .into_iter()
        .zip(event_ids)
        .map(|((index, row), event_id)| SearchHit {
            event_id,
            row: row.row,
            score: row.score,
            segment: index,
        })
        .collect();
    Ok((hits, stats))
}

fn all_rows<S: SegmentSource>(segment: &SegmentReader<S>) -> [RowRange; 1] {
    [RowRange {
        start: 0,
        end: segment.row_count(),
    }]
}

/// One segment's search: an optional partition index over the bounded two-stage scan.
struct SegmentScan<'a, S> {
    options: SearchOptions,
    range: ScoreRange,
    partitions: Option<&'a PartitionIndex>,
    search: &'a VectorSearch<'a>,
    segment: &'a SegmentReader<S>,
}

impl<S: SegmentSource> SegmentScan<'_, S> {
    /// The segment's best `k` rows among `filter` (all rows when `None`) scoring at least `threshold`.
    fn run(
        &self,
        filter: Option<&RoaringRangeBitmap>,
        k: usize,
        threshold: f32,
        stats: &mut SearchStats,
    ) -> Result<Vec<ScoredRow>, VectorError> {
        let representations = self.segment.representations(EMBEDDING_COLUMN_ID);
        let (Some(coarse), Some(fine)) = (representations.first(), representations.last()) else {
            return Ok(Vec::new());
        };
        let bounded = |filter: Option<&RoaringRangeBitmap>, threshold: f32, stats: &mut SearchStats| {
            self.segment.search_bounded(
                coarse.column_id,
                fine.column_id,
                super::segment::BoundedSearch {
                    cap: (self.search.oversample > 0 && !self.range.is_bounded()).then_some(self.search.oversample),
                    filter,
                    k,
                    model: self.search.model,
                    query: self.search.query,
                    threshold,
                    ceiling: self.range.ceiling(),
                    ranged: self.range.is_bounded(),
                },
                stats,
            )
        };
        let Some(partitions) = self.partitions else {
            let filter = filter.filter(|filter| filter.count() < self.segment.row_count());
            return bounded(filter, threshold, stats);
        };
        // Round one probes the `nprobes` partitions whose centroids score best; every later round probes, in one
        // scan, every unprobed partition whose bound still reaches the k-th best score, until none does.
        let ranked = partitions.ranked(self.search.query);
        let mut probed = vec![false; ranked.len()];
        let mut best = TopK::new(k);
        let ceiling = self.range.ceiling();
        let mut first_round = true;
        // Partitions entirely above a range's maximum never hold a hit.
        for (partition, _) in &ranked {
            if partitions.above(*partition, self.search.query, ceiling)
                && let Some(slot) = probed.get_mut(*partition)
            {
                *slot = true;
                stats.partitions_pruned += 1;
            }
        }
        loop {
            let bar = best.threshold().map_or(threshold, |kth| kth.max(threshold));
            let probe: Vec<usize> = if first_round {
                partitions
                    .closest(self.search.query)
                    .into_iter()
                    .filter(|partition| {
                        !probed.get(*partition).copied().unwrap_or(true)
                            && ranked.iter().any(|(p, upper)| p == partition && *upper >= bar)
                    })
                    .take(self.options.nprobes.max(1))
                    .collect()
            } else {
                ranked
                    .iter()
                    .filter(|(partition, upper)| *upper >= bar && !probed.get(*partition).copied().unwrap_or(true))
                    .map(|(partition, _)| *partition)
                    .collect()
            };
            first_round = false;
            if probe.is_empty() {
                break;
            }
            for partition in &probe {
                if let Some(slot) = probed.get_mut(*partition) {
                    *slot = true;
                }
            }
            stats.partitions_probed += probe.len() as u64;
            let mut rows = partitions.rows_of(&probe);
            if let Some(filter) = filter {
                rows = rows.intersect(filter);
            }
            if rows.is_empty() {
                continue;
            }
            for row in bounded(Some(&rows), bar, stats)? {
                best.offer(row);
            }
        }
        stats.partitions_pruned += probed.iter().filter(|probed| !**probed).count() as u64;
        Ok(best.into_sorted())
    }

    /// Scans for more than `k` rows without the metadata filter, then keeps those passing it. Widens once if
    /// fewer than `k` survive; returns `None` (pre-filter instead) if they still do not.
    fn post_filter(
        &self,
        deleted: Option<&RoaringRangeBitmap>,
        allowed: &AllowedValues<'_>,
        threshold: f32,
        stats: &mut SearchStats,
    ) -> Result<Option<Vec<ScoredRow>>, VectorError> {
        let k = self.search.k;
        let live = deleted.map(|deleted| RoaringRangeBitmap::from_ranges(all_rows(self.segment)).difference(deleted));
        for wanted in [k.saturating_mul(4).max(64), k.saturating_mul(32).max(512)] {
            let rows = self.run(live.as_ref(), wanted, threshold, stats)?;
            let exhausted = rows.len() < wanted;
            let mut sorted: Vec<u64> = rows.iter().map(|row| row.row).collect();
            sorted.sort_unstable();
            let keep = rows_matching(self.segment, &self.search.filter, allowed, &sorted)?;
            let mut survivors: Vec<ScoredRow> = rows.into_iter().filter(|row| keep.contains(row.row)).collect();
            if survivors.len() >= k || exhausted {
                survivors.truncate(k);
                stats.segments_post_filtered += 1;
                return Ok(Some(survivors));
            }
        }
        Ok(None)
    }
}

fn sort_hits(hits: &mut [(usize, ScoredRow)]) {
    hits.sort_by(|left, right| {
        right
            .1
            .score
            .total_cmp(&left.1.score)
            .then(left.0.cmp(&right.0))
            .then(left.1.row.cmp(&right.1.row))
    });
}

/// The filter's allowed string values as byte sets, built once per search.
pub(super) struct AllowedValues<'a> {
    rooms: Option<HashSet<&'a [u8]>>,
    senders: Option<HashSet<&'a [u8]>>,
}

impl<'a> AllowedValues<'a> {
    pub(super) fn new(filter: &'a SearchFilter) -> Self {
        let set = |values: &'a Option<Vec<String>>| {
            values
                .as_ref()
                .map(|values| values.iter().map(|value| value.as_bytes()).collect())
        };
        Self {
            rooms: set(&filter.room_ids),
            senders: set(&filter.senders),
        }
    }
}

/// Which local rows of `granule` pass `filter`.
fn granule_matches<S: SegmentSource>(
    segment: &SegmentReader<S>,
    granule: usize,
    filter: &SearchFilter,
    allowed: &AllowedValues<'_>,
) -> Result<Vec<bool>, VectorError> {
    let mut keep = vec![true; segment.granule_rows(granule)];
    for (name, set) in [(ROOM_ID_COLUMN, &allowed.rooms), (SENDER_COLUMN, &allowed.senders)] {
        let Some(set) = set else {
            continue;
        };
        if !keep.contains(&true) {
            break;
        }
        let mut seen = vec![false; keep.len()];
        match segment.for_each_string(name, granule, |local, value| {
            if set.contains(value)
                && let Some(slot) = seen.get_mut(local)
            {
                *slot = true;
            }
        }) {
            Ok(()) | Err(VectorError::MissingColumn { .. }) => {}
            Err(error) => return Err(error),
        }
        keep.iter_mut().zip(seen).for_each(|(kept, seen)| *kept &= seen);
    }
    if let Some((from, to)) = filter.time_range
        && keep.contains(&true)
    {
        let times = match segment.integers(TIMESTAMP_COLUMN, granule) {
            Err(VectorError::MissingColumn { .. }) => Vec::new(),
            other => other?,
        };
        keep.iter_mut()
            .enumerate()
            .for_each(|(local, kept)| *kept &= times.get(local).is_some_and(|time| (from..=to).contains(time)));
    }
    Ok(keep)
}

pub(super) fn row_filter<S: SegmentSource>(
    segment: &SegmentReader<S>,
    filter: &SearchFilter,
    allowed: &AllowedValues<'_>,
) -> Result<Option<RoaringRangeBitmap>, VectorError> {
    let mut ranges = Vec::new();
    let mut rows = Vec::new();
    for granule in 0..segment.granule_count() {
        let first = segment.granule_first_row(granule);
        let count = segment.granule_rows(granule);
        if count == 0 || !granule_may_match(segment, granule, filter) {
            continue;
        }
        if filter.is_empty() {
            ranges.push(RowRange {
                start: first,
                end: first + count as u64,
            });
            continue;
        }
        let keep = granule_matches(segment, granule, filter, allowed)?;
        rows.extend(
            keep.iter()
                .enumerate()
                .filter(|(_, kept)| **kept)
                .map(|(local, _)| first + local as u64),
        );
    }
    let bitmap = if filter.is_empty() {
        RoaringRangeBitmap::from_ranges(ranges)
    } else {
        RoaringRangeBitmap::from_rows(rows)
    };
    Ok((!bitmap.is_empty()).then_some(bitmap))
}

/// The rows of `sorted` that pass `filter`, decoding filter columns only for the granules those rows fall in.
fn rows_matching<S: SegmentSource>(
    segment: &SegmentReader<S>,
    filter: &SearchFilter,
    allowed: &AllowedValues<'_>,
    sorted: &[u64],
) -> Result<RoaringRangeBitmap, VectorError> {
    let mut kept = Vec::new();
    let mut remaining = sorted;
    for granule in 0..segment.granule_count() {
        if remaining.is_empty() {
            break;
        }
        let first = segment.granule_first_row(granule);
        let end = first + segment.granule_rows(granule) as u64;
        let split = remaining.partition_point(|row| *row < end);
        let (inside, rest) = remaining.split_at(split);
        remaining = rest;
        if inside.is_empty() || !granule_may_match(segment, granule, filter) {
            continue;
        }
        let keep = granule_matches(segment, granule, filter, allowed)?;
        kept.extend(
            inside
                .iter()
                .filter(|row| keep.get((**row - first) as usize).copied().unwrap_or(false)),
        );
    }
    Ok(RoaringRangeBitmap::from_rows(kept))
}

/// Fraction of the segment's rows in granules whose statistics say the filter may match.
pub(super) fn estimated_selectivity<S: SegmentSource>(segment: &SegmentReader<S>, filter: &SearchFilter) -> f64 {
    let total = segment.row_count();
    if total == 0 {
        return 0.0;
    }
    let may: u64 = (0..segment.granule_count())
        .filter(|granule| granule_may_match(segment, *granule, filter))
        .map(|granule| segment.granule_rows(granule) as u64)
        .sum();
    may as f64 / total as f64
}

/// Whether a granule may hold matching rows. Absent statistics mean "may match".
pub(super) fn granule_may_match<S: SegmentSource>(
    segment: &SegmentReader<S>,
    granule: usize,
    filter: &SearchFilter,
) -> bool {
    let strings_may_match = |name: &str, allowed: &Option<Vec<String>>| match allowed {
        None => true,
        Some(allowed) => match segment.column_stats(name, granule) {
            Some(ColumnStats::String { max, min }) => allowed.iter().any(|value| value >= min && value <= max),
            _ => true,
        },
    };
    let time_may_match = match filter.time_range {
        None => true,
        Some((from, to)) => match segment.column_stats(TIMESTAMP_COLUMN, granule) {
            Some(ColumnStats::Integer { max, min }) => *min <= to && *max >= from,
            _ => true,
        },
    };
    strings_may_match(ROOM_ID_COLUMN, &filter.room_ids)
        && strings_may_match(SENDER_COLUMN, &filter.senders)
        && time_may_match
}

pub fn nearest_segments<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    query: &[f32],
    model: &EmbeddingModel,
    n: usize,
) -> Result<Vec<usize>, VectorError> {
    let mut scored = Vec::with_capacity(segments.len());
    for (index, segment) in segments.iter().enumerate() {
        segment.manifest().model.require_same(model)?;
        let score = segment.centroid().map_or(f32::INFINITY, |centroid| {
            let pairs = centroid.iter().zip(query);
            match model.metric {
                VectorMetric::Cosine | VectorMetric::Dot => pairs.map(|(left, right)| left * right).sum(),
                VectorMetric::L2 => -pairs.map(|(left, right)| (left - right) * (left - right)).sum::<f32>(),
            }
        });
        scored.push((index, score));
    }
    scored.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    Ok(scored.into_iter().take(n).map(|(index, _)| index).collect())
}

pub fn segments_to_reembed<'a, K>(
    manifests: impl IntoIterator<Item = (K, &'a SegmentManifest)>,
    current: &EmbeddingModel,
) -> Vec<K> {
    manifests
        .into_iter()
        .filter(|(_, manifest)| manifest.model != *current)
        .map(|(key, _)| key)
        .collect()
}
