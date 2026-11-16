//! Hybrid text + vector ranking by Reciprocal Rank Fusion.
//!
//! OE's text-token index (`indexes::text_token`) is a membership filter for pruning, not a ranked text search, so
//! the text side is any ranked list of event ids the caller produced (e.g. a full-text engine's results).
//! `score(e) = Σ_lists weight / (k + rank)` with 1-based ranks; an id absent from a list contributes nothing, and
//! only its first occurrence in a list counts. Ties break by best (smallest) rank in any list, then by the list
//! order (an id ranked by an earlier list first), then by event id, so the output is deterministic.

use super::search::SearchHit;

/// The usual RRF constant (Cormack et al.).
pub const DEFAULT_RRF_K: f64 = 60.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RrfParams {
    /// Rank offset `k`; larger values flatten the difference between top and lower ranks.
    pub k: f64,
    pub vector_weight: f64,
    pub text_weight: f64,
    /// Fused hits returned at most (`None` for all).
    pub limit: Option<usize>,
}

impl Default for RrfParams {
    fn default() -> Self {
        Self {
            k: DEFAULT_RRF_K,
            vector_weight: 1.0,
            text_weight: 1.0,
            limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FusedHit {
    pub event_id: String,
    pub score: f64,
    /// 1-based rank per input list (`None` when absent from it), in the order the lists were given.
    pub ranks: Vec<Option<usize>>,
}

/// Fuses ranked lists of event ids, each with a weight.
pub fn reciprocal_rank_fusion<T: AsRef<str>>(lists: &[(&[T], f64)], k: f64, limit: Option<usize>) -> Vec<FusedHit> {
    let mut fused: std::collections::HashMap<&str, FusedHit> = std::collections::HashMap::new();
    for (list_index, (list, weight)) in lists.iter().enumerate() {
        for (position, event_id) in list.iter().enumerate() {
            let event_id = event_id.as_ref();
            let hit = fused.entry(event_id).or_insert_with(|| FusedHit {
                event_id: event_id.to_owned(),
                score: 0.0,
                ranks: vec![None; lists.len()],
            });
            let Some(slot) = hit.ranks.get_mut(list_index) else {
                continue;
            };
            if slot.is_some() {
                continue;
            }
            let rank = position + 1;
            *slot = Some(rank);
            hit.score += weight / (k + rank as f64);
        }
    }
    let mut hits: Vec<FusedHit> = fused.into_values().collect();
    // Scores are summed in list order for every id, so equal inputs give bit-equal scores.
    let best_rank = |hit: &FusedHit| hit.ranks.iter().flatten().min().copied().unwrap_or(usize::MAX);
    let first_list = |hit: &FusedHit| hit.ranks.iter().position(Option::is_some).unwrap_or(usize::MAX);
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(best_rank(left).cmp(&best_rank(right)))
            .then(first_list(left).cmp(&first_list(right)))
            .then(left.event_id.cmp(&right.event_id))
    });
    if let Some(limit) = limit {
        hits.truncate(limit);
    }
    hits
}

/// Hybrid search: fuses vector hits (in their given order, best first) with a ranked text list. `ranks` in the
/// result are `[vector, text]`. A vector hit list spanning several segments may hold one event id more than
/// once; only its best-ranked occurrence counts.
pub fn hybrid_rrf<T: AsRef<str>>(vector: &[SearchHit], text: &[T], params: RrfParams) -> Vec<FusedHit> {
    let vector_ids: Vec<&str> = vector.iter().map(|hit| hit.event_id.as_str()).collect();
    let text_ids: Vec<&str> = text.iter().map(AsRef::as_ref).collect();
    reciprocal_rank_fusion(
        &[
            (vector_ids.as_slice(), params.vector_weight),
            (text_ids.as_slice(), params.text_weight),
        ],
        params.k,
        params.limit,
    )
}
