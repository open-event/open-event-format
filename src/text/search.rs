//! BM25 top-k over one or more segment indexes with block-max WAND pruning.
//!
//! Corpus statistics (document count, total length, per-term document frequency) are summed over every segment
//! searched, so scores are comparable across segments and equal to scoring the union as one corpus. As in Lucene,
//! deleted documents still count in the statistics; they are only kept out of the results.
//!
//! `score(d) = Σ_t idf(t) · tf·(k1+1) / (tf + k1·(1 − b + b·|d|/avgdl))`, `idf(t) = ln(1 + (N − df + 0.5)/(df + 0.5))`,
//! over the distinct folded query tokens. Contributions are summed in query-token order, so pruned and exhaustive
//! search give bit-equal scores. Ties break by event id, then segment, then row.

use super::index::{BlockMeta, Bm25Index, NO_DOC, POSTING_BLOCK, TermPostings};
use crate::indexes::text_token::tokenize;
use crate::vector::SegmentDeletes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Upper bounds are inflated by this factor so float rounding can never make a bound fall below a real score.
const BOUND_SLACK: f64 = 1.0 + 1e-9;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Params {
    pub k1: f64,
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// One segment to search: its index and, optionally, its deletion vector (rows are doc ids).
#[derive(Debug, Clone, Copy)]
pub struct TextSegment<'a> {
    pub index: &'a Bm25Index,
    pub deletes: Option<&'a SegmentDeletes>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextHit {
    pub event_id: String,
    pub row: u64,
    pub score: f64,
    /// Position of the segment in the searched slice.
    pub segment: usize,
}

/// Distinct folded query tokens, in first-occurrence order.
pub fn query_terms(query: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for token in tokenize(query) {
        if !terms.iter().any(|term| term.as_str() == token.as_ref()) {
            terms.push(token.into_owned());
        }
    }
    terms
}

struct Scorer {
    k1: f64,
    b: f64,
    avgdl: f64,
}

impl Scorer {
    fn tf_part(&self, tf: u32, doc_length: u32) -> f64 {
        let tf = f64::from(tf);
        let norm = self.k1 * (1.0 - self.b + self.b * f64::from(doc_length) / self.avgdl);
        tf * (self.k1 + 1.0) / (tf + norm)
    }
}

/// Global statistics and per-term weights for a query over a set of segments.
struct Prepared {
    scorer: Scorer,
    terms: Vec<String>,
    idf: Vec<f64>,
}

fn prepare(segments: &[TextSegment<'_>], query: &str, params: Bm25Params) -> Option<Prepared> {
    if !(params.k1.is_finite() && params.k1 >= 0.0 && params.b.is_finite() && (0.0..=1.0).contains(&params.b)) {
        return None;
    }
    let terms = query_terms(query);
    let docs: u64 = segments
        .iter()
        .map(|segment| u64::from(segment.index.doc_count()))
        .sum();
    let total: u64 = segments.iter().map(|segment| segment.index.total_length()).sum();
    if terms.is_empty() || docs == 0 {
        return None;
    }
    let avgdl = match total {
        0 => 1.0,
        total => total as f64 / docs as f64,
    };
    let n = docs as f64;
    let idf = terms
        .iter()
        .map(|term| {
            let df: u64 = segments
                .iter()
                .map(|segment| u64::from(segment.index.doc_freq(term)))
                .sum();
            let df = df as f64;
            (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
        })
        .collect();
    Some(Prepared {
        scorer: Scorer {
            k1: params.k1,
            b: params.b,
            avgdl,
        },
        terms,
        idf,
    })
}

#[derive(Debug)]
struct Candidate {
    score: f64,
    event_id: String,
    segment: usize,
    row: u64,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    /// Greater means worse, so a max-heap keeps the current worst hit on top.
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.event_id.cmp(&other.event_id))
            .then(self.segment.cmp(&other.segment))
            .then(self.row.cmp(&other.row))
    }
}

struct TopK {
    limit: usize,
    heap: BinaryHeap<Candidate>,
}

impl TopK {
    fn threshold(&self) -> f64 {
        match self.heap.len() >= self.limit {
            true => self.heap.peek().map_or(f64::NEG_INFINITY, |worst| worst.score),
            false => f64::NEG_INFINITY,
        }
    }

    fn offer(&mut self, candidate: Candidate) {
        if self.heap.len() < self.limit {
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| candidate < *worst) {
            self.heap.pop();
            self.heap.push(candidate);
        }
    }

    fn into_hits(self) -> Vec<TextHit> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|candidate| TextHit {
                event_id: candidate.event_id,
                row: candidate.row,
                score: candidate.score,
                segment: candidate.segment,
            })
            .collect()
    }
}

struct Cursor<'a> {
    index: &'a Bm25Index,
    postings: &'a TermPostings,
    term: usize,
    weight: f64,
    upper_bound: f64,
    block: usize,
    docs: Vec<u32>,
    tfs: Vec<u32>,
    position: usize,
    doc: u32,
}

impl<'a> Cursor<'a> {
    fn new(index: &'a Bm25Index, postings: &'a TermPostings, term: usize, weight: f64, scorer: &Scorer) -> Self {
        let mut cursor = Cursor {
            index,
            postings,
            term,
            weight,
            upper_bound: weight * scorer.tf_part(postings.max_tf, postings.min_len) * BOUND_SLACK,
            block: 0,
            docs: Vec::with_capacity(POSTING_BLOCK),
            tfs: Vec::with_capacity(POSTING_BLOCK),
            position: 0,
            doc: NO_DOC,
        };
        cursor.load_block(0);
        cursor
    }

    fn load_block(&mut self, block: usize) {
        self.block = block;
        self.position = 0;
        let loaded = self
            .postings
            .blocks
            .get(block)
            .and_then(|meta| self.index.decode_block(meta, &mut self.docs, &mut self.tfs));
        self.doc = match loaded {
            Some(()) => self.docs.first().copied().unwrap_or(NO_DOC),
            None => NO_DOC,
        };
    }

    fn tf(&self) -> u32 {
        self.tfs.get(self.position).copied().unwrap_or(0)
    }

    /// Block containing the first posting `>= target` (without decoding it), from the current block on.
    fn shallow(&self, target: u32) -> Option<&'a BlockMeta> {
        self.postings
            .blocks
            .get(self.block..)?
            .iter()
            .find(|meta| meta.last_doc >= target)
    }

    /// Moves to the first posting `>= target`.
    fn advance_to(&mut self, target: u32) {
        if self.doc >= target {
            return;
        }
        let current_last = self.postings.blocks.get(self.block).map_or(0, |meta| meta.last_doc);
        if current_last < target {
            let skip = self
                .postings
                .blocks
                .get(self.block..)
                .and_then(|rest| rest.iter().position(|meta| meta.last_doc >= target));
            match skip {
                Some(offset) => self.load_block(self.block + offset),
                None => {
                    self.doc = NO_DOC;
                    return;
                }
            }
        }
        let rest = self.docs.get(self.position..).unwrap_or_default();
        self.position += rest.partition_point(|&doc| doc < target);
        self.doc = self.docs.get(self.position).copied().unwrap_or(NO_DOC);
    }
}

fn cursors<'a>(index: &'a Bm25Index, prepared: &Prepared) -> Vec<Cursor<'a>> {
    prepared
        .terms
        .iter()
        .zip(&prepared.idf)
        .enumerate()
        .filter_map(|(term, (text, &idf))| {
            index
                .term(text)
                .map(|postings| Cursor::new(index, postings, term, idf, &prepared.scorer))
        })
        .collect()
}

/// Scores the cursors positioned on `doc` (summing in query-term order), offers it, and steps them past it.
fn score_doc(
    cursors: &mut [Cursor<'_>],
    doc: u32,
    prepared: &Prepared,
    segment: usize,
    text: &TextSegment<'_>,
    top: &mut TopK,
) {
    let length = text.index.doc_length(doc);
    let mut parts: Vec<(usize, f64)> = cursors
        .iter()
        .filter(|cursor| cursor.doc == doc)
        .map(|cursor| {
            (
                cursor.term,
                cursor.weight * prepared.scorer.tf_part(cursor.tf(), length),
            )
        })
        .collect();
    parts.sort_by_key(|&(term, _)| term);
    let score = parts.iter().fold(0.0, |sum, &(_, part)| sum + part);
    let deleted = text.deletes.is_some_and(|deletes| deletes.is_deleted(u64::from(doc)));
    if !deleted && let Some(event_id) = text.index.event_id(doc) {
        top.offer(Candidate {
            score,
            event_id: event_id.to_owned(),
            segment,
            row: u64::from(doc),
        });
    }
    cursors
        .iter_mut()
        .filter(|cursor| cursor.doc == doc)
        .for_each(|cursor| cursor.advance_to(doc.saturating_add(1)));
}

fn search_segment_wand(segment: usize, text: &TextSegment<'_>, prepared: &Prepared, top: &mut TopK) {
    let mut cursors = cursors(text.index, prepared);
    loop {
        cursors.retain(|cursor| cursor.doc != NO_DOC);
        if cursors.is_empty() {
            return;
        }
        cursors.sort_by_key(|cursor| (cursor.doc, cursor.term));
        let threshold = top.threshold();
        let mut bound = 0.0;
        let Some(mut pivot) = cursors.iter().position(|cursor| {
            bound += cursor.upper_bound;
            bound >= threshold
        }) else {
            return;
        };
        let Some(pivot_doc) = cursors.get(pivot).map(|cursor| cursor.doc) else {
            return;
        };
        while cursors.get(pivot + 1).is_some_and(|cursor| cursor.doc == pivot_doc) {
            pivot += 1;
        }
        let mut next = cursors.get(pivot + 1).map_or(NO_DOC, |cursor| cursor.doc);
        let mut block_bound = 0.0;
        for cursor in cursors.iter().take(pivot + 1) {
            if let Some(meta) = cursor.shallow(pivot_doc) {
                block_bound += cursor.weight * prepared.scorer.tf_part(meta.max_tf, meta.min_len);
                next = next.min(meta.last_doc.saturating_add(1));
            }
        }
        if block_bound * BOUND_SLACK < threshold {
            // No document in [pivot_doc, next) can enter the top k: every cursor that could match one is in
            // 0..=pivot and sits in the block bounded above.
            let target = next.max(pivot_doc.saturating_add(1));
            cursors
                .iter_mut()
                .take(pivot + 1)
                .for_each(|cursor| cursor.advance_to(target));
            continue;
        }
        if cursors.first().is_some_and(|cursor| cursor.doc == pivot_doc) {
            score_doc(&mut cursors, pivot_doc, prepared, segment, text, top);
        } else {
            cursors
                .iter_mut()
                .take_while(|cursor| cursor.doc < pivot_doc)
                .for_each(|cursor| cursor.advance_to(pivot_doc));
        }
    }
}

fn search_segment_exhaustive(segment: usize, text: &TextSegment<'_>, prepared: &Prepared, top: &mut TopK) {
    let mut cursors = cursors(text.index, prepared);
    loop {
        let Some(doc) = cursors
            .iter()
            .map(|cursor| cursor.doc)
            .min()
            .filter(|&doc| doc != NO_DOC)
        else {
            return;
        };
        score_doc(&mut cursors, doc, prepared, segment, text, top);
    }
}

fn run(
    segments: &[TextSegment<'_>],
    query: &str,
    k: usize,
    params: Bm25Params,
    each: fn(usize, &TextSegment<'_>, &Prepared, &mut TopK),
) -> Vec<TextHit> {
    let Some(prepared) = prepare(segments, query, params).filter(|_| k > 0) else {
        return Vec::new();
    };
    let mut top = TopK {
        limit: k,
        heap: BinaryHeap::with_capacity(k.min(1 << 16) + 1),
    };
    for (position, segment) in segments.iter().enumerate() {
        each(position, segment, &prepared, &mut top);
    }
    top.into_hits()
}

/// Top `k` BM25 hits (best first) across `segments`, with block-max WAND pruning. Returns nothing for an empty
/// query, `k == 0` or invalid parameters (`k1 < 0`, `b` outside `[0, 1]`).
pub fn search_bm25(segments: &[TextSegment<'_>], query: &str, k: usize, params: Bm25Params) -> Vec<TextHit> {
    run(segments, query, k, params, search_segment_wand)
}

/// Same results as [`search_bm25`], scoring every matching document (no pruning).
pub fn search_bm25_exhaustive(segments: &[TextSegment<'_>], query: &str, k: usize, params: Bm25Params) -> Vec<TextHit> {
    run(segments, query, k, params, search_segment_exhaustive)
}
