//! Multivector (late-interaction, ColBERT-style) documents and MaxSim search.
//!
//! A document is several token vectors. A multivector segment (version 5) stores one row per token, in document
//! order; each token row repeats its document's event id and metadata columns, so filters, deletes and row reads
//! work unchanged, and the footer records each document's first token row (`SegmentReader::document_offsets`).
//!
//! `score(doc) = Σ_query_tokens max_doc_tokens sim(q, t)`. Search follows WARP's shape: for every query token the
//! coarse representation gathers the best `token_candidates` token rows by upper bound; a document missing from a
//! query token's candidates is imputed that token's best dropped upper bound. The imputed sums are upper bounds on
//! each document's exact MaxSim, so documents are reranked exactly on the fine representation in decreasing bound
//! until the bound falls below the k-th exact score. If the bound of documents never seen still reaches it, those
//! are scored too, so the answer equals brute-force MaxSim on the fine representation (up to the coarse bound's
//! confidence, as for single-vector search). `max_rerank` caps the documents reranked per segment, trading that
//! guarantee for latency.

use super::codec::CodedQuery;
use super::constant::*;
use super::deletes::SegmentDeletes;
use super::error::VectorError;
use super::ivf::PartitionIndex;
use super::kernel::{TopK, for_each_row};
use super::model::{EmbeddingModel, ScoredRow, VectorMetric};
use super::seal::{FileKeys, WrappingKey};
use super::search::{AllowedValues, SearchFilter, SearchOptions, SearchStats, row_filter};
use super::segment::{SegmentInput, SegmentReader, SegmentSource, SegmentSpec, SubBlock, write_segment_inner};
use crate::indexes::bitmap::RoaringRangeBitmap;
use std::collections::{BTreeMap, HashMap};
use zeroize::Zeroizing;

/// Documents for a multivector segment: per document an event id, metadata values and its token vectors.
#[derive(Debug, Clone, Default)]
pub struct MultivectorInput {
    pub event_ids: Vec<String>,
    pub integer_columns: Vec<(String, Vec<i64>)>,
    pub string_columns: Vec<(String, Vec<String>)>,
    /// Token vectors per document; every document has at least one.
    pub documents: Vec<Vec<Vec<f32>>>,
}

/// Writes a version 5 segment holding one row per token.
pub fn write_multivector_segment(
    spec: &SegmentSpec,
    input: &MultivectorInput,
    wrapping: &WrappingKey,
) -> Result<Vec<u8>, VectorError> {
    let invalid = |rule: String| Err(VectorError::InvalidInput { rule });
    let documents = input.event_ids.len();
    if input.documents.len() != documents {
        return invalid(format!(
            "{} token lists for {documents} documents",
            input.documents.len()
        ));
    }
    let mut offsets = Vec::with_capacity(documents + 1);
    let mut rows = SegmentInput {
        integer_columns: input
            .integer_columns
            .iter()
            .map(|(name, _)| (name.clone(), Vec::new()))
            .collect(),
        string_columns: input
            .string_columns
            .iter()
            .map(|(name, _)| (name.clone(), Vec::new()))
            .collect(),
        ..SegmentInput::default()
    };
    for (document, tokens) in input.documents.iter().enumerate() {
        if tokens.is_empty() {
            return invalid(format!("document {document} has no token vectors"));
        }
        offsets.push(rows.vectors.len() as u64);
        let event_id = input.event_ids.get(document).cloned().unwrap_or_default();
        for token in tokens {
            rows.event_ids.push(event_id.clone());
            rows.vectors.push(token.clone());
            for ((name, into), (_, from)) in rows.string_columns.iter_mut().zip(&input.string_columns) {
                let Some(value) = from.get(document) else {
                    return invalid(format!(
                        "column {name} has {} values for {documents} documents",
                        from.len()
                    ));
                };
                into.push(value.clone());
            }
            for ((name, into), (_, from)) in rows.integer_columns.iter_mut().zip(&input.integer_columns) {
                let Some(value) = from.get(document) else {
                    return invalid(format!(
                        "column {name} has {} values for {documents} documents",
                        from.len()
                    ));
                };
                into.push(*value);
            }
        }
    }
    for (name, len) in input
        .string_columns
        .iter()
        .map(|(name, values)| (name, values.len()))
        .chain(input.integer_columns.iter().map(|(name, values)| (name, values.len())))
    {
        if len != documents {
            return invalid(format!("column {name} has {len} values for {documents} documents"));
        }
    }
    offsets.push(rows.vectors.len() as u64);
    write_segment_inner(spec, &rows, wrapping, &FileKeys::random_multivector()?, Some(offsets))
}

/// The live documents of a multivector segment: a document keeps its live token rows and disappears when none is
/// live (deleting a document's event id deletes all its token rows).
pub fn read_live_documents<S: SegmentSource>(
    segment: &SegmentReader<S>,
    deletes: Option<&SegmentDeletes>,
) -> Result<MultivectorInput, VectorError> {
    let offsets = segment.document_offsets().ok_or_else(|| VectorError::InvalidInput {
        rule: "not a multivector (version 5) segment".to_owned(),
    })?;
    let rows = segment.read_all()?;
    let mut out = MultivectorInput {
        integer_columns: rows
            .integer_columns
            .iter()
            .map(|(name, _)| (name.clone(), Vec::new()))
            .collect(),
        string_columns: rows
            .string_columns
            .iter()
            .map(|(name, _)| (name.clone(), Vec::new()))
            .collect(),
        ..MultivectorInput::default()
    };
    for bounds in offsets.windows(2) {
        let (Some(start), Some(end)) = (bounds.first(), bounds.get(1)) else {
            continue;
        };
        let live: Vec<usize> = (*start..*end)
            .filter(|row| deletes.is_none_or(|deletes| !deletes.is_deleted(*row)))
            .filter_map(|row| usize::try_from(row).ok())
            .collect();
        let Some(first) = live.first().copied() else {
            continue;
        };
        out.event_ids
            .push(rows.event_ids.get(first).cloned().unwrap_or_default());
        for ((_, into), (_, from)) in out.string_columns.iter_mut().zip(&rows.string_columns) {
            into.push(from.get(first).cloned().unwrap_or_default());
        }
        for ((_, into), (_, from)) in out.integer_columns.iter_mut().zip(&rows.integer_columns) {
            into.push(from.get(first).copied().unwrap_or_default());
        }
        out.documents
            .push(live.iter().filter_map(|row| rows.vectors.get(*row).cloned()).collect());
    }
    Ok(out)
}

/// Merges multivector segments into one version 5 segment, keeping each document's tokens together and dropping
/// documents with no live token. With `cluster`, documents are grouped by k-means partition of their mean token
/// vector (see `merge_segments_clustered`).
pub fn merge_multivector_segments<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    spec: &SegmentSpec,
    wrapping: &WrappingKey,
    cluster: Option<&super::ivf::PartitionParams>,
) -> Result<Vec<u8>, VectorError> {
    let mut merged: Option<MultivectorInput> = None;
    for (index, segment) in segments.iter().enumerate() {
        segment.manifest().model.require_same(&spec.model)?;
        let input = read_live_documents(segment, deletes.get(index).copied().flatten())?;
        match &mut merged {
            None => merged = Some(input),
            Some(merged) => {
                let names = |input: &MultivectorInput| {
                    (
                        input
                            .string_columns
                            .iter()
                            .map(|(name, _)| name.clone())
                            .collect::<Vec<_>>(),
                        input
                            .integer_columns
                            .iter()
                            .map(|(name, _)| name.clone())
                            .collect::<Vec<_>>(),
                    )
                };
                if names(merged) != names(&input) {
                    return Err(VectorError::InvalidInput {
                        rule: "merged segments hold the same columns".to_owned(),
                    });
                }
                merged.event_ids.extend(input.event_ids);
                merged.documents.extend(input.documents);
                for ((_, into), (_, from)) in merged.string_columns.iter_mut().zip(input.string_columns) {
                    into.extend(from);
                }
                for ((_, into), (_, from)) in merged.integer_columns.iter_mut().zip(input.integer_columns) {
                    into.extend(from);
                }
            }
        }
    }
    let mut merged = merged.unwrap_or_default();
    if let Some(params) = cluster {
        let dimension = spec.model.dimension as usize;
        let means: Vec<Vec<f32>> = merged
            .documents
            .iter()
            .map(|tokens| super::kernel::ball_of(dimension, tokens.iter().map(Vec::as_slice)).0)
            .collect();
        let order = super::segment::clustered_order(&means, dimension, params);
        fn pick<T: Clone>(values: &[T], order: &[usize]) -> Vec<T> {
            order.iter().filter_map(|row| values.get(*row).cloned()).collect()
        }
        merged = MultivectorInput {
            event_ids: pick(&merged.event_ids, &order),
            integer_columns: merged
                .integer_columns
                .iter()
                .map(|(name, values)| (name.clone(), pick(values, &order)))
                .collect(),
            string_columns: merged
                .string_columns
                .iter()
                .map(|(name, values)| (name.clone(), pick(values, &order)))
                .collect(),
            documents: pick(&merged.documents, &order),
        };
    }
    if merged.documents.is_empty() {
        return Err(VectorError::InvalidInput {
            rule: "a merged multivector segment needs a live document".to_owned(),
        });
    }
    write_multivector_segment(spec, &merged, wrapping)
}

/// Exact MaxSim of `document` for `query` under `metric` (L2 similarity is `-distance²`), for reference and tests.
pub fn max_sim(query: &[Vec<f32>], document: &[Vec<f32>], metric: VectorMetric) -> f32 {
    let similarity = |left: &[f32], right: &[f32]| -> f32 {
        let pairs = left.iter().zip(right);
        match metric {
            VectorMetric::Cosine | VectorMetric::Dot => pairs.map(|(a, b)| a * b).sum(),
            VectorMetric::L2 => -pairs.map(|(a, b)| (a - b) * (a - b)).sum::<f32>(),
        }
    };
    query
        .iter()
        .map(|q| {
            document
                .iter()
                .map(|t| similarity(q, t))
                .fold(f32::NEG_INFINITY, f32::max)
        })
        .sum()
}

#[derive(Debug, Clone)]
pub struct MultivectorSearch<'a> {
    pub filter: SearchFilter,
    pub k: usize,
    pub model: &'a EmbeddingModel,
    /// Query token vectors.
    pub query: &'a [Vec<f32>],
    /// Coarse token rows kept per query token (`DEFAULT_TOKEN_CANDIDATES`).
    pub token_candidates: usize,
    /// Documents reranked per segment at most; `None` keeps the exact guarantee.
    pub max_rerank: Option<usize>,
}

impl<'a> MultivectorSearch<'a> {
    pub fn new(model: &'a EmbeddingModel, query: &'a [Vec<f32>], k: usize) -> Self {
        Self {
            filter: SearchFilter::default(),
            k,
            model,
            query,
            token_candidates: DEFAULT_TOKEN_CANDIDATES,
            max_rerank: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MultivectorHit {
    pub event_id: String,
    /// Document index within its segment.
    pub document: u64,
    pub score: f32,
    pub segment: usize,
}

/// MaxSim top-k over multivector segments (`deletes[i]` marks deleted token rows of `segments[i]`; a document
/// with no live token is gone). Stats count token rows. See `search_multivector_segments_indexed`.
pub fn search_multivector_segments<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    search: &MultivectorSearch<'_>,
) -> Result<(Vec<MultivectorHit>, SearchStats), VectorError> {
    search_multivector_segments_indexed(segments, deletes, &[], search, SearchOptions::default())
}

/// `search_multivector_segments` with an optional partition index (built over the token rows) per segment.
///
/// Segments are visited best first: largest MaxSim bound (sum over query tokens of the segment score bound), then
/// nearest centroid (sum over query tokens of the centroid score; a segment's centroid alone is a poor guide for a
/// sum of per-token maxima). The running k-th score skips any later segment whose bound cannot reach it.
/// With an index, each query token's candidates come from its `nprobes` best partitions (more while they hold
/// fewer than twice `token_candidates` rows), and a token row in an unprobed partition is imputed that partition's
/// bound. If documents never seen could still reach the k-th score, the probe widens (×4 per round, finally the
/// whole segment), so the answer equals the unindexed search.
pub fn search_multivector_segments_indexed<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    indexes: &[Option<&PartitionIndex>],
    search: &MultivectorSearch<'_>,
    options: SearchOptions,
) -> Result<(Vec<MultivectorHit>, SearchStats), VectorError> {
    let dimension = search.model.dimension as usize;
    if search.query.is_empty() || search.query.iter().any(|token| token.len() != dimension) {
        return Err(VectorError::InvalidInput {
            rule: format!("a multivector query holds at least one token of width {dimension}"),
        });
    }
    for (index, segment) in segments.iter().enumerate() {
        segment.manifest().model.require_same(search.model)?;
        if segment.document_offsets().is_none() {
            return Err(VectorError::InvalidInput {
                rule: "multivector search needs version 5 (multivector) segments".to_owned(),
            });
        }
        if let Some(partitions) = indexes.get(index).copied().flatten() {
            partitions.check(segment)?;
        }
    }
    let mut order: Vec<(usize, (f32, f32))> = segments
        .iter()
        .enumerate()
        .map(|(index, segment)| {
            let bound: f32 = search.query.iter().map(|token| segment.segment_bounds(token).1).sum();
            let closeness = segment.centroid().map_or(f32::INFINITY, |centroid| {
                search
                    .query
                    .iter()
                    .map(|token| centroid_score(search.model.metric, centroid, token))
                    .sum()
            });
            (index, (bound, closeness))
        })
        .collect();
    order.sort_by(|left, right| {
        (right.1)
            .0
            .total_cmp(&(left.1).0)
            .then((right.1).1.total_cmp(&(left.1).1))
            .then(left.0.cmp(&right.0))
    });
    let mut stats = SearchStats::default();
    let mut hits: Vec<(usize, ScoredRow)> = Vec::new();
    for (index, _) in order {
        let Some(segment) = segments.get(index) else {
            continue;
        };
        let threshold = if search.k > 0 && hits.len() >= search.k {
            hits.last().map_or(f32::NEG_INFINITY, |(_, row)| row.score)
        } else {
            f32::NEG_INFINITY
        };
        let deleted = deletes
            .get(index)
            .copied()
            .flatten()
            .filter(|deletes| deletes.deleted_count() > 0)
            .map(SegmentDeletes::bitmap);
        let found = SegmentMaxSim {
            nprobes: options.nprobes.max(1),
            partitions: indexes.get(index).copied().flatten(),
            search,
            segment,
        }
        .run(deleted.as_ref(), threshold, &mut stats)?;
        hits.extend(found.into_iter().map(|row| (index, row)));
        hits.sort_by(|left, right| {
            right
                .1
                .score
                .total_cmp(&left.1.score)
                .then(left.0.cmp(&right.0))
                .then(left.1.row.cmp(&right.1.row))
        });
        hits.truncate(search.k);
    }
    let mut out = Vec::with_capacity(hits.len());
    for (index, row) in hits {
        let segment = segments.get(index).ok_or_else(|| VectorError::InvalidInput {
            rule: "segment index".to_owned(),
        })?;
        let first = segment
            .document_rows(row.row)
            .map(|rows| rows.start)
            .unwrap_or_default();
        let event_id = segment.event_ids(&[first])?.into_iter().next().unwrap_or_default();
        out.push(MultivectorHit {
            event_id,
            document: row.row,
            score: row.score,
            segment: index,
        });
    }
    Ok((out, stats))
}

fn centroid_score(metric: VectorMetric, centroid: &[f32], query: &[f32]) -> f32 {
    let pairs = centroid.iter().zip(query);
    match metric {
        VectorMetric::Cosine | VectorMetric::Dot => pairs.map(|(a, b)| a * b).sum(),
        VectorMetric::L2 => -pairs.map(|(a, b)| (a - b) * (a - b)).sum::<f32>(),
    }
}

struct SegmentMaxSim<'a, S> {
    nprobes: usize,
    partitions: Option<&'a PartitionIndex>,
    search: &'a MultivectorSearch<'a>,
    segment: &'a SegmentReader<S>,
}

impl<S: SegmentSource> SegmentMaxSim<'_, S> {
    /// The segment's best `k` documents (`ScoredRow::row` is the document index) scoring at least `threshold`.
    fn run(
        &self,
        deleted: Option<&RoaringRangeBitmap>,
        threshold: f32,
        stats: &mut SearchStats,
    ) -> Result<Vec<ScoredRow>, VectorError> {
        let segment = self.segment;
        let search = self.search;
        if search.k == 0 || segment.row_count() == 0 || segment.representations(EMBEDDING_COLUMN_ID).is_empty() {
            return Ok(Vec::new());
        }
        let mut live = if search.filter.is_empty() {
            None
        } else {
            match row_filter(segment, &search.filter, &AllowedValues::new(&search.filter))? {
                Some(rows) => Some(rows),
                None => return Ok(Vec::new()),
            }
        };
        if let Some(deleted) = deleted {
            let base = live.unwrap_or_else(|| RoaringRangeBitmap::from_rows(0..segment.row_count()));
            live = Some(base.difference(deleted));
        }
        if live.as_ref().is_some_and(RoaringRangeBitmap::is_empty) {
            return Ok(Vec::new());
        }
        let bound: f32 = search.query.iter().map(|token| segment.segment_bounds(token).1).sum();
        if bound < threshold {
            stats.segments_skipped += 1;
            return Ok(Vec::new());
        }
        stats.segments_searched += 1;
        let Some(partitions) = self.partitions else {
            return self
                .round(live.as_ref(), live.as_ref(), None, threshold, stats)
                .map(|(rows, _)| rows);
        };
        let mut nprobes = self.nprobes;
        loop {
            let everything = nprobes >= partitions.partitions.len();
            let probe = (!everything).then(|| self.probe(partitions, nprobes, live.as_ref()));
            let (scan, floors) = match &probe {
                Some((rows, floors, probed)) => {
                    stats.partitions_probed += *probed as u64;
                    (Some(rows), Some(floors.as_slice()))
                }
                None => {
                    stats.partitions_probed += partitions.partitions.len() as u64;
                    (live.as_ref(), None)
                }
            };
            let (rows, complete) = self.round(live.as_ref(), scan, floors, threshold, stats)?;
            if complete || everything {
                return Ok(rows);
            }
            nprobes = nprobes.saturating_mul(4);
        }
    }

    /// The token rows to scan (live rows of each query token's best partitions) and, per query token, the bound on
    /// any token row outside its probed partitions.
    fn probe(
        &self,
        partitions: &PartitionIndex,
        nprobes: usize,
        live: Option<&RoaringRangeBitmap>,
    ) -> (RoaringRangeBitmap, Vec<f32>, usize) {
        let wanted = self.search.token_candidates.max(1).saturating_mul(2);
        let mut probed = vec![false; partitions.partitions.len()];
        let mut floors = Vec::with_capacity(self.search.query.len());
        for token in self.search.query {
            let mut floor = f32::NEG_INFINITY;
            let (mut taken, mut rows) = (0usize, 0usize);
            for (partition, upper) in partitions.ranked(token) {
                if taken >= nprobes && rows >= wanted {
                    floor = upper;
                    break;
                }
                if let Some(slot) = probed.get_mut(partition) {
                    *slot = true;
                }
                taken += 1;
                rows += partitions.partitions.get(partition).map_or(0, |p| p.rows.len());
            }
            floors.push(floor);
        }
        let chosen: Vec<usize> = (0..probed.len())
            .filter(|index| probed.get(*index) == Some(&true))
            .collect();
        let mut rows = partitions.rows_of(&chosen);
        if let Some(live) = live {
            rows = rows.intersect(live);
        }
        (rows, floors, chosen.len())
    }

    /// One candidate-and-rerank pass over the token rows `scan`; `floors[i]` bounds query token `i`'s score on any
    /// row outside `scan`. Returns the documents and whether the answer is complete (no unseen document can reach
    /// the k-th score). Without floors the pass scans every live row and scores unseen documents when needed.
    fn round(
        &self,
        live: Option<&RoaringRangeBitmap>,
        scan: Option<&RoaringRangeBitmap>,
        floors: Option<&[f32]>,
        threshold: f32,
        stats: &mut SearchStats,
    ) -> Result<(Vec<ScoredRow>, bool), VectorError> {
        let segment = self.segment;
        let search = self.search;
        let k = search.k;
        let representations = segment.representations(EMBEDDING_COLUMN_ID);
        let (Some(coarse), Some(fine)) = (representations.first(), representations.last()) else {
            return Ok((Vec::new(), true));
        };
        // Candidate generation: the best `token_candidates` token rows per query token by coarse upper bound.
        let coarse_meta = segment.vector_meta(coarse.column_id, search.model)?;
        let coarse_queries: Vec<CodedQuery> = search
            .query
            .iter()
            .map(|token| segment.coded_query(coarse_meta.element, token))
            .collect();
        let keep = search.token_candidates.max(1);
        let mut candidates: Vec<Vec<(f32, u64)>> = vec![Vec::new(); search.query.len()];
        let mut missing = floors.map_or_else(|| vec![f32::NEG_INFINITY; search.query.len()], <[f32]>::to_vec);
        let width = coarse_meta.row_width();
        segment.scan_parts(coarse.column_id, scan, |first_row, plaintext| {
            for_each_row(width, plaintext, first_row, scan, |row, bytes| {
                stats.rows_scanned += 1;
                for ((query, kept), missing) in coarse_queries.iter().zip(&mut candidates).zip(&mut missing) {
                    let upper = query.estimate(bytes).upper;
                    if upper < *missing {
                        continue;
                    }
                    kept.push((upper, row));
                    if kept.len() >= keep.saturating_mul(2).max(64) {
                        trim(kept, keep, missing);
                    }
                }
            });
        })?;
        let mut documents: BTreeMap<u64, Vec<f32>> = BTreeMap::new();
        for (token, (kept, missing)) in candidates.iter_mut().zip(&mut missing).enumerate() {
            trim(kept, keep, missing);
            stats.rows_candidate += kept.len() as u64;
            for (upper, row) in kept.iter() {
                let Some(document) = segment.document_of(*row) else {
                    continue;
                };
                let bounds = documents
                    .entry(document)
                    .or_insert_with(|| vec![f32::NEG_INFINITY; search.query.len()]);
                if let Some(slot) = bounds.get_mut(token) {
                    *slot = slot.max(*upper);
                }
            }
        }
        // Imputation: a token row not kept for query token `i` scores at most `missing[i]`.
        let unseen: f32 = missing.iter().sum();
        let mut ranked: Vec<(u64, f32)> = documents
            .iter()
            .map(|(document, bounds)| {
                let bound = bounds
                    .iter()
                    .zip(&missing)
                    .map(|(found, missing)| found.max(*missing))
                    .sum();
                (*document, bound)
            })
            .collect();
        ranked.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));

        // Exact MaxSim on the fine representation, best bound first.
        let mut exact = Exact::new(segment, fine.column_id, search, live)?;
        let mut best = TopK::new(k);
        let bar = |best: &TopK| best.threshold().map_or(threshold, |kth| kth.max(threshold));
        let limit = search.max_rerank.unwrap_or(usize::MAX);
        for (reranked, (document, bound)) in ranked.iter().enumerate() {
            if reranked >= limit || *bound < bar(&best) {
                break;
            }
            if let Some(score) = exact.score(*document, stats)? {
                best.offer(ScoredRow { row: *document, score });
            }
        }
        if search.max_rerank.is_none() && unseen >= bar(&best) {
            if floors.is_some() {
                return Ok((Vec::new(), false));
            }
            for document in 0..segment.document_count() {
                if documents.contains_key(&document) {
                    continue;
                }
                if let Some(score) = exact.score(document, stats)? {
                    best.offer(ScoredRow { row: document, score });
                }
            }
        }
        Ok((
            best.into_sorted()
                .into_iter()
                .filter(|row| row.score >= threshold)
                .collect(),
            true,
        ))
    }
}

/// Keeps the `keep` best `(upper, row)` pairs, raising `missing` to the best upper bound dropped.
fn trim(kept: &mut Vec<(f32, u64)>, keep: usize, missing: &mut f32) {
    if kept.len() <= keep {
        return;
    }
    let order = |left: &(f32, u64), right: &(f32, u64)| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1));
    kept.select_nth_unstable_by(keep, order);
    for (upper, _) in kept.iter().skip(keep) {
        *missing = missing.max(*upper);
    }
    kept.truncate(keep);
}

/// Exact MaxSim scoring of whole documents, caching opened fine sub-blocks.
struct Exact<'a, S> {
    cache: HashMap<SubBlock, Zeroizing<Vec<u8>>>,
    column: u32,
    live: Option<&'a RoaringRangeBitmap>,
    queries: Vec<CodedQuery>,
    segment: &'a SegmentReader<S>,
    width: usize,
}

impl<'a, S: SegmentSource> Exact<'a, S> {
    fn new(
        segment: &'a SegmentReader<S>,
        column: u32,
        search: &MultivectorSearch<'_>,
        live: Option<&'a RoaringRangeBitmap>,
    ) -> Result<Self, VectorError> {
        let meta = segment.vector_meta(column, search.model)?;
        Ok(Self {
            cache: HashMap::new(),
            column,
            live,
            queries: search
                .query
                .iter()
                .map(|token| segment.coded_query(meta.element, token))
                .collect(),
            segment,
            width: meta.row_width(),
        })
    }

    /// The document's MaxSim over its live tokens (`None` when it has none).
    fn score(&mut self, document: u64, stats: &mut SearchStats) -> Result<Option<f32>, VectorError> {
        let Some(rows) = self.segment.document_rows(document) else {
            return Ok(None);
        };
        let column = self.segment.column_by_id(self.column)?;
        let mut best = vec![f32::NEG_INFINITY; self.queries.len()];
        let mut any = false;
        if self.cache.len() > 256 {
            self.cache.clear();
        }
        for row in rows {
            if self.live.is_some_and(|live| !live.contains(row)) {
                continue;
            }
            let (granule, local) = self.segment.locate(row)?;
            let at = (granule, local / column.part_rows);
            let local = local % column.part_rows;
            if let std::collections::hash_map::Entry::Vacant(slot) = self.cache.entry(at) {
                slot.insert(self.segment.open_part(column, at.0, at.1)?);
            }
            let bytes = self
                .cache
                .get(&at)
                .and_then(|plaintext| plaintext.get(local * self.width..(local + 1) * self.width))
                .ok_or(VectorError::Format(crate::error::FormatError::Truncated {
                    what: "vector segment",
                }))?;
            for (slot, query) in best.iter_mut().zip(&self.queries) {
                *slot = slot.max(query.score(bytes));
            }
            stats.rows_reranked += 1;
            any = true;
        }
        Ok(any.then(|| best.iter().sum()))
    }
}
