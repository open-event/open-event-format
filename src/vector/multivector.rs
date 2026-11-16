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
use super::kernel::{TopK, for_each_row};
use super::model::{EmbeddingModel, ScoredRow, VectorMetric};
use super::seal::{FileKeys, WrappingKey};
use super::search::{AllowedValues, SearchFilter, SearchStats, row_filter};
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
/// with no live token is gone). Segments are visited in order and the running k-th score skips any segment whose
/// bound (sum over query tokens of the segment score bound) cannot reach it. Stats count token rows.
pub fn search_multivector_segments<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&SegmentDeletes>],
    search: &MultivectorSearch<'_>,
) -> Result<(Vec<MultivectorHit>, SearchStats), VectorError> {
    let dimension = search.model.dimension as usize;
    if search.query.is_empty() || search.query.iter().any(|token| token.len() != dimension) {
        return Err(VectorError::InvalidInput {
            rule: format!("a multivector query holds at least one token of width {dimension}"),
        });
    }
    for segment in segments {
        segment.manifest().model.require_same(search.model)?;
        if segment.document_offsets().is_none() {
            return Err(VectorError::InvalidInput {
                rule: "multivector search needs version 5 (multivector) segments".to_owned(),
            });
        }
    }
    let mut stats = SearchStats::default();
    let mut hits: Vec<(usize, ScoredRow)> = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
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
        let found = SegmentMaxSim { search, segment }.run(deleted.as_ref(), threshold, &mut stats)?;
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

struct SegmentMaxSim<'a, S> {
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
        let k = search.k;
        let representations = segment.representations(EMBEDDING_COLUMN_ID);
        let (Some(coarse), Some(fine)) = (representations.first(), representations.last()) else {
            return Ok(Vec::new());
        };
        if k == 0 || segment.row_count() == 0 {
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

        // Candidate generation: the best `token_candidates` token rows per query token by coarse upper bound.
        let coarse_meta = segment.vector_meta(coarse.column_id, search.model)?;
        let coarse_queries: Vec<CodedQuery> = search
            .query
            .iter()
            .map(|token| segment.coded_query(coarse_meta.element, token))
            .collect();
        let keep = search.token_candidates.max(1);
        let mut candidates: Vec<Vec<(f32, u64)>> = vec![Vec::new(); search.query.len()];
        let mut missing = vec![f32::NEG_INFINITY; search.query.len()];
        let width = coarse_meta.row_width();
        segment.scan_parts(coarse.column_id, live.as_ref(), |first_row, plaintext| {
            for_each_row(width, plaintext, first_row, live.as_ref(), |row, bytes| {
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
        let mut exact = Exact::new(segment, fine.column_id, search, live.as_ref())?;
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
            for document in 0..segment.document_count() {
                if documents.contains_key(&document) {
                    continue;
                }
                if let Some(score) = exact.score(document, stats)? {
                    best.offer(ScoredRow { row: document, score });
                }
            }
        }
        Ok(best
            .into_sorted()
            .into_iter()
            .filter(|row| row.score >= threshold)
            .collect())
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
