use super::Exactness;
use super::bool_pruning::{ExprQuery, FilterExpr};
use crate::layout::reader::query_scan::ScanOrder;
use hashbrown::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatVerdict {
    CouldMatch,
    ProvenAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterClause {
    pub column_id: u32,
    pub predicate: PredicateKind,
    pub value_hi: i128,
    pub value_lo: i128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateKind {
    Equality,
    Membership,
    Presence,
    Range,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    pub clauses: Vec<FilterClause>,
    pub memberships: Vec<InList>,
}

impl Filter {
    pub fn new(clauses: Vec<FilterClause>) -> Self {
        Filter {
            clauses,
            memberships: Vec::new(),
        }
    }

    pub fn with_in_list(mut self, column_id: u32, values: Vec<i128>) -> Self {
        let in_list = InList::new(column_id, MembershipList::new(values));
        self.clauses.push(in_list.clause);
        if in_list.clause.predicate != PredicateKind::Range {
            self.memberships.push(in_list);
        }
        self
    }

    pub fn membership_for(&self, clause: &FilterClause) -> Option<&MembershipList> {
        self.memberships
            .iter()
            .find(|in_list| in_list.clause == *clause)
            .map(|in_list| &in_list.list)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InList {
    pub clause: FilterClause,
    pub list: MembershipList,
}

impl InList {
    pub fn new(column_id: u32, list: MembershipList) -> Self {
        let clause = list.clause(column_id).unwrap_or(FilterClause {
            column_id,
            predicate: PredicateKind::Membership,
            value_hi: i128::MIN,
            value_lo: i128::MAX,
        });
        InList { clause, list }
    }

    pub fn matches_row(&self, clause_matches: &mut impl FnMut(&FilterClause) -> bool) -> bool {
        let values = self.list.values();
        let probe = |predicate, value_lo, value_hi| FilterClause {
            column_id: self.clause.column_id,
            predicate,
            value_hi,
            value_lo,
        };
        let (mut lo, mut hi) = (0, values.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let Some(&listed) = values.get(mid) else {
                break;
            };
            if clause_matches(&probe(PredicateKind::Range, i128::MIN, listed)) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        values
            .get(lo)
            .is_some_and(|&value| clause_matches(&probe(PredicateKind::Equality, value, value)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statistic {
    MinMaxBound {
        column_id: u32,
        max: i128,
        min: i128,
    },
    NullCount {
        column_id: u32,
        null_count: u64,
        row_count: u64,
    },
    PathPresence {
        column_id: u32,
        present: bool,
    },
    PointMembership {
        column_id: u32,
        members: Vec<i128>,
    },
    RangeEmptiness {
        column_id: u32,
        non_empty_spans: Vec<(i128, i128)>,
    },
    SequenceBounds {
        column_id: u32,
        max_sequence: i128,
        min_sequence: i128,
    },
    SingleMember {
        column_id: u32,
        member: Option<i128>,
    },
    SingleSpan {
        column_id: u32,
        span: Option<(i128, i128)>,
    },
    TextToken {
        column_id: u32,
        token_hashes: Vec<i128>,
    },
    TimeBounds {
        column_id: u32,
        max_time: i128,
        min_time: i128,
    },
}

impl Statistic {
    pub fn column_id(&self) -> u32 {
        match self {
            Statistic::MinMaxBound { column_id, .. }
            | Statistic::NullCount { column_id, .. }
            | Statistic::PathPresence { column_id, .. }
            | Statistic::PointMembership { column_id, .. }
            | Statistic::RangeEmptiness { column_id, .. }
            | Statistic::SequenceBounds { column_id, .. }
            | Statistic::SingleMember { column_id, .. }
            | Statistic::SingleSpan { column_id, .. }
            | Statistic::TextToken { column_id, .. }
            | Statistic::TimeBounds { column_id, .. } => *column_id,
        }
    }

    pub fn relevant_to(&self, clause: &FilterClause) -> bool {
        if self.column_id() != clause.column_id {
            return false;
        }
        match self {
            Statistic::MinMaxBound { .. } | Statistic::SequenceBounds { .. } | Statistic::TimeBounds { .. } => {
                matches!(
                    clause.predicate,
                    PredicateKind::Equality | PredicateKind::Membership | PredicateKind::Range
                )
            }
            Statistic::NullCount { .. } => matches!(
                clause.predicate,
                PredicateKind::Equality | PredicateKind::Membership | PredicateKind::Range
            ),
            Statistic::PointMembership { .. } | Statistic::SingleMember { .. } => {
                matches!(clause.predicate, PredicateKind::Equality | PredicateKind::Membership)
            }
            Statistic::RangeEmptiness { .. } | Statistic::SingleSpan { .. } => {
                matches!(clause.predicate, PredicateKind::Membership | PredicateKind::Range)
            }
            Statistic::PathPresence { .. } => {
                matches!(clause.predicate, PredicateKind::Presence)
            }
            Statistic::TextToken { .. } => {
                matches!(clause.predicate, PredicateKind::Equality | PredicateKind::Membership)
            }
        }
    }

    pub fn keep_confirms_match(&self) -> bool {
        match self {
            Statistic::MinMaxBound { .. }
            | Statistic::NullCount { .. }
            | Statistic::PathPresence { .. }
            | Statistic::PointMembership { .. }
            | Statistic::RangeEmptiness { .. }
            | Statistic::SequenceBounds { .. }
            | Statistic::SingleMember { .. }
            | Statistic::SingleSpan { .. }
            | Statistic::TextToken { .. }
            | Statistic::TimeBounds { .. } => false,
        }
    }

    pub fn judge_in(&self, clause: &FilterClause, list: Option<&MembershipList>) -> StatVerdict {
        let Some(list) = list else {
            return self.judge(clause);
        };
        let could_match = match self {
            Statistic::MinMaxBound { max, min, .. } => list.any_in(*min, *max),
            Statistic::SequenceBounds {
                max_sequence,
                min_sequence,
                ..
            } => list.any_in(*min_sequence, *max_sequence),
            Statistic::TimeBounds { max_time, min_time, .. } => list.any_in(*min_time, *max_time),
            Statistic::PointMembership { members, .. } => members.iter().any(|&m| list.contains(m)),
            Statistic::TextToken { token_hashes, .. } => token_hashes.iter().any(|&h| list.contains(h)),
            Statistic::SingleMember { member, .. } => member.is_some_and(|m| list.contains(m)),
            Statistic::RangeEmptiness { non_empty_spans, .. } => {
                non_empty_spans.iter().any(|&(lo, hi)| list.any_in(lo, hi))
            }
            Statistic::SingleSpan { span, .. } => span.is_some_and(|(lo, hi)| list.any_in(lo, hi)),
            Statistic::NullCount { .. } | Statistic::PathPresence { .. } => {
                return self.judge(clause);
            }
        };
        if could_match {
            StatVerdict::CouldMatch
        } else {
            StatVerdict::ProvenAbsent
        }
    }

    pub fn judge(&self, clause: &FilterClause) -> StatVerdict {
        match self {
            Statistic::MinMaxBound { max, min, .. } => disjoint(*min, *max, clause.value_lo, clause.value_hi),
            Statistic::SequenceBounds {
                max_sequence,
                min_sequence,
                ..
            } => disjoint(*min_sequence, *max_sequence, clause.value_lo, clause.value_hi),
            Statistic::TimeBounds { max_time, min_time, .. } => {
                disjoint(*min_time, *max_time, clause.value_lo, clause.value_hi)
            }
            Statistic::NullCount {
                null_count, row_count, ..
            } => {
                if *row_count > 0 && null_count >= row_count {
                    StatVerdict::ProvenAbsent
                } else {
                    StatVerdict::CouldMatch
                }
            }
            Statistic::PointMembership { members, .. } => {
                let any_in_range = members.iter().any(|&m| m >= clause.value_lo && m <= clause.value_hi);
                if any_in_range {
                    StatVerdict::CouldMatch
                } else {
                    StatVerdict::ProvenAbsent
                }
            }
            Statistic::SingleMember { member, .. } => {
                if member.is_some_and(|m| m >= clause.value_lo && m <= clause.value_hi) {
                    StatVerdict::CouldMatch
                } else {
                    StatVerdict::ProvenAbsent
                }
            }
            Statistic::SingleSpan { span, .. } => {
                if span.is_some_and(|(lo, hi)| !is_disjoint(lo, hi, clause.value_lo, clause.value_hi)) {
                    StatVerdict::CouldMatch
                } else {
                    StatVerdict::ProvenAbsent
                }
            }
            Statistic::RangeEmptiness { non_empty_spans, .. } => {
                let overlaps = non_empty_spans
                    .iter()
                    .any(|&(lo, hi)| !is_disjoint(lo, hi, clause.value_lo, clause.value_hi));
                if overlaps {
                    StatVerdict::CouldMatch
                } else {
                    StatVerdict::ProvenAbsent
                }
            }
            Statistic::PathPresence { present, .. } => {
                if *present {
                    StatVerdict::CouldMatch
                } else {
                    StatVerdict::ProvenAbsent
                }
            }
            Statistic::TextToken { token_hashes, .. } => {
                let any_match = token_hashes
                    .iter()
                    .any(|&h| h >= clause.value_lo && h <= clause.value_hi);
                if any_match {
                    StatVerdict::CouldMatch
                } else {
                    StatVerdict::ProvenAbsent
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruningTerm {
    pub clause: FilterClause,
    pub exactness: Exactness,
    pub statistic: Statistic,
}

impl PruningTerm {
    pub fn verdict(&self) -> StatVerdict {
        self.statistic.judge(&self.clause)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GranuleStatistics {
    pub statistics: Vec<Statistic>,
}

impl GranuleStatistics {
    fn matching(&self, template: &Statistic) -> Option<&Statistic> {
        self.statistics
            .iter()
            .find(|s| same_kind(s, template) && s.column_id() == template.column_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruningExpression {
    pub filter: Filter,
    pub terms: Vec<PruningTerm>,
}

impl PruningExpression {
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneOutcome {
    pub drop: bool,
    pub residual_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPlan {
    AggregateShortcut,
    Scan {
        filter: FilterExpr,
        limit: Option<usize>,
        order: ScanOrder,
        pruning: Option<PruningExpression>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub answerable_from_aggregate: bool,
    pub filter: Filter,
}

pub fn build_pruning_expression(filter: &Filter, declared: &GranuleStatistics) -> PruningExpression {
    let mut terms = Vec::new();
    for clause in &filter.clauses {
        for statistic in &declared.statistics {
            if !statistic.relevant_to(clause) {
                continue;
            }
            terms.push(PruningTerm {
                clause: *clause,
                exactness: exactness_of(statistic),
                statistic: statistic.clone(),
            });
        }
    }
    PruningExpression {
        filter: filter.clone(),
        terms,
    }
}

pub fn evaluate(expr: &PruningExpression, granule: &GranuleStatistics) -> PruneOutcome {
    let mut confirmed_clauses: Vec<FilterClause> = Vec::new();
    for term in &expr.terms {
        let Some(stat) = granule.matching(&term.statistic) else {
            continue;
        };
        match stat.judge_in(&term.clause, expr.filter.membership_for(&term.clause)) {
            StatVerdict::ProvenAbsent => {
                return PruneOutcome {
                    drop: true,
                    residual_required: false,
                };
            }
            StatVerdict::CouldMatch => {
                if !term.exactness.requires_residual_filter() && term.statistic.keep_confirms_match() {
                    confirmed_clauses.push(term.clause);
                }
            }
        }
    }
    PruneOutcome {
        drop: false,
        residual_required: !expr
            .filter
            .clauses
            .iter()
            .all(|clause| confirmed_clauses.contains(clause)),
    }
}

pub fn prune_pages_in_granule(
    filter: &Filter,
    page_statistics: &[(u32, GranuleStatistics)],
    total_page_count: u32,
) -> Vec<u32> {
    let no_stats = GranuleStatistics::default();
    let mut stats_by_page: HashMap<u32, &GranuleStatistics> = HashMap::with_capacity(page_statistics.len());
    for (page_idx, stats) in page_statistics {
        if *page_idx < total_page_count {
            stats_by_page.entry(*page_idx).or_insert(stats);
        }
    }
    (0..total_page_count)
        .filter(|page_idx| {
            let stats = stats_by_page.get(page_idx).copied().unwrap_or(&no_stats);
            page_could_match(filter, stats)
        })
        .collect()
}

pub fn plan_query(query: impl Into<ExprQuery>, declared: &GranuleStatistics) -> QueryPlan {
    let query = query.into();
    if query.answerable_from_aggregate {
        return QueryPlan::AggregateShortcut;
    }
    QueryPlan::Scan {
        pruning: query
            .filter
            .as_conjunction()
            .map(|filter| build_pruning_expression(&filter, declared)),
        filter: query.filter,
        limit: query.limit,
        order: query.order,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipList {
    values: Vec<i128>,
}

impl MembershipList {
    pub fn new(mut values: Vec<i128>) -> Self {
        values.sort_unstable();
        values.dedup();
        Self { values }
    }

    pub fn values(&self) -> &[i128] {
        &self.values
    }

    pub fn contains(&self, value: i128) -> bool {
        self.values.binary_search(&value).is_ok()
    }

    pub fn any_in(&self, lo: i128, hi: i128) -> bool {
        if lo > hi {
            return false;
        }
        let first_at_or_above = self.values.partition_point(|value| *value < lo);
        self.values.get(first_at_or_above).is_some_and(|value| *value <= hi)
    }

    pub fn judge_bounds(&self, min: i128, max: i128) -> StatVerdict {
        if self.any_in(min, max) {
            StatVerdict::CouldMatch
        } else {
            StatVerdict::ProvenAbsent
        }
    }

    pub fn clause(&self, column_id: u32) -> Option<FilterClause> {
        let (&value_lo, &value_hi) = (self.values.first()?, self.values.last()?);
        let contiguous = value_hi
            .checked_sub(value_lo)
            .and_then(|span| span.checked_add(1))
            .is_some_and(|span| span == self.values.len() as i128);
        Some(FilterClause {
            column_id,
            predicate: if contiguous {
                PredicateKind::Range
            } else {
                PredicateKind::Membership
            },
            value_hi,
            value_lo,
        })
    }
}

fn page_could_match(filter: &Filter, stats: &GranuleStatistics) -> bool {
    !filter
        .clauses
        .iter()
        .any(|clause| clause_proven_absent(clause, filter.membership_for(clause), stats))
}

pub(crate) fn clause_proven_absent(
    clause: &FilterClause,
    list: Option<&MembershipList>,
    granule: &GranuleStatistics,
) -> bool {
    granule
        .statistics
        .iter()
        .filter(|statistic| statistic.relevant_to(clause))
        .any(|statistic| {
            granule
                .matching(statistic)
                .is_some_and(|own| own.judge_in(clause, list) == StatVerdict::ProvenAbsent)
        })
}

fn exactness_of(statistic: &Statistic) -> Exactness {
    match statistic {
        Statistic::MinMaxBound { .. }
        | Statistic::NullCount { .. }
        | Statistic::SequenceBounds { .. }
        | Statistic::TimeBounds { .. } => Exactness::Exact,
        Statistic::PathPresence { .. }
        | Statistic::PointMembership { .. }
        | Statistic::RangeEmptiness { .. }
        | Statistic::SingleMember { .. }
        | Statistic::SingleSpan { .. }
        | Statistic::TextToken { .. } => Exactness::InexactNoFalseNegative,
    }
}

fn disjoint(a_lo: i128, a_hi: i128, b_lo: i128, b_hi: i128) -> StatVerdict {
    if is_disjoint(a_lo, a_hi, b_lo, b_hi) {
        StatVerdict::ProvenAbsent
    } else {
        StatVerdict::CouldMatch
    }
}

fn is_disjoint(a_lo: i128, a_hi: i128, b_lo: i128, b_hi: i128) -> bool {
    a_hi < b_lo || b_hi < a_lo
}

fn same_kind(a: &Statistic, b: &Statistic) -> bool {
    matches!(
        (a, b),
        (Statistic::MinMaxBound { .. }, Statistic::MinMaxBound { .. })
            | (Statistic::NullCount { .. }, Statistic::NullCount { .. })
            | (Statistic::PathPresence { .. }, Statistic::PathPresence { .. })
            | (Statistic::PointMembership { .. }, Statistic::PointMembership { .. })
            | (Statistic::RangeEmptiness { .. }, Statistic::RangeEmptiness { .. })
            | (Statistic::SequenceBounds { .. }, Statistic::SequenceBounds { .. })
            | (Statistic::SingleMember { .. }, Statistic::SingleMember { .. })
            | (Statistic::SingleSpan { .. }, Statistic::SingleSpan { .. })
            | (Statistic::TextToken { .. }, Statistic::TextToken { .. })
            | (Statistic::TimeBounds { .. }, Statistic::TimeBounds { .. })
    )
}

#[cfg(test)]
#[path = "test/pruning.rs"]
mod tests;
