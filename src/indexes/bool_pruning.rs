//! Boolean pruning across fields: filters that mix AND and OR over clauses on different columns.
//!
//! [`Filter`](super::pruning::Filter) is a flat conjunction, which cannot say `event_type = 3 OR amount > 100`. A
//! [`FilterExpr`] can: it is a tree of [`FilterClause`] leaves joined by `And` and `Or` nodes. Each granule is judged
//! with three-valued logic — a node is [`Truth::False`] when the granule's statistics prove no row can satisfy it,
//! [`Truth::True`] when every row is certain to (only the empty `And` today, since no granule statistic confirms a
//! match), and [`Truth::Unknown`] otherwise. A granule is dropped only when the root is proven `False`, so an `Or`
//! drops a granule only when *every* branch is proven absent.
//!
//! A kept granule still needs its rows filtered. [`residual`] returns the expression simplified for that granule:
//! `Or` branches proven absent there are removed, because no row in the granule satisfies them, so the residual
//! accepts exactly the rows the original accepts. [`FilterExpr::matches_row`] evaluates any expression over one row.

use super::pruning::{Filter, FilterClause, GranuleStatistics, InList, Query, clause_proven_absent};
use crate::layout::reader::query_scan::ScanOrder;

/// A boolean filter tree over clauses that may name different columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterExpr {
    /// All children must hold; an empty `And` is always true.
    And(Vec<FilterExpr>),
    Clause(FilterClause),
    /// `column IN (…)`, pruned and row-tested by binary search over the sorted list.
    In(InList),
    /// At least one child must hold; an empty `Or` is always false.
    Or(Vec<FilterExpr>),
}

/// Three-valued verdict of a filter node over one granule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    /// Proven: no row of the granule satisfies the node.
    False,
    /// Proven: every row of the granule satisfies the node.
    True,
    Unknown,
}

impl From<Filter> for FilterExpr {
    fn from(filter: Filter) -> Self {
        let Filter { clauses, memberships } = filter;
        let mut children: Vec<FilterExpr> = clauses
            .into_iter()
            .filter(|clause| !memberships.iter().any(|in_list| in_list.clause == *clause))
            .map(FilterExpr::Clause)
            .collect();
        children.extend(memberships.into_iter().map(FilterExpr::In));
        FilterExpr::And(children)
    }
}

impl FilterExpr {
    /// The flat conjunction this expression is, when it is one, so it can take the existing AND-only path.
    pub fn as_conjunction(&self) -> Option<Filter> {
        let mut filter = Filter::default();
        collect_conjunction(self, &mut filter).then_some(filter)
    }

    /// Whether one row satisfies the expression, given an exact per-clause row test.
    pub fn matches_row(&self, clause_matches: &mut impl FnMut(&FilterClause) -> bool) -> bool {
        match self {
            FilterExpr::Clause(clause) => clause_matches(clause),
            FilterExpr::In(in_list) => in_list.matches_row(clause_matches),
            FilterExpr::And(children) => children.iter().all(|child| child.matches_row(clause_matches)),
            FilterExpr::Or(children) => children.iter().any(|child| child.matches_row(clause_matches)),
        }
    }

    /// The three-valued verdict of this expression over the granule described by `granule`.
    pub fn truth(&self, granule: &GranuleStatistics) -> Truth {
        match self {
            FilterExpr::Clause(clause) => absent_or_unknown(clause_proven_absent(clause, None, granule)),
            FilterExpr::In(in_list) => {
                absent_or_unknown(clause_proven_absent(&in_list.clause, Some(&in_list.list), granule))
            }
            FilterExpr::And(children) => {
                let mut all_true = true;
                for child in children {
                    match child.truth(granule) {
                        Truth::False => return Truth::False,
                        Truth::Unknown => all_true = false,
                        Truth::True => {}
                    }
                }
                if all_true { Truth::True } else { Truth::Unknown }
            }
            FilterExpr::Or(children) => {
                let mut all_false = true;
                for child in children {
                    match child.truth(granule) {
                        Truth::True => return Truth::True,
                        Truth::Unknown => all_false = false,
                        Truth::False => {}
                    }
                }
                if all_false { Truth::False } else { Truth::Unknown }
            }
        }
    }
}

/// The keep/drop decision for one granule: `true` drops it, which happens only when the expression is proven false.
pub fn drops_granule(expr: &FilterExpr, granule: &GranuleStatistics) -> bool {
    expr.truth(granule) == Truth::False
}

/// The row filter a kept granule still needs, or `None` when the granule is dropped.
///
/// Children proven false are removed from `Or` nodes and children proven true from `And` nodes; the result accepts
/// exactly the granule's rows the original accepts. An expression proven true comes back as the empty `And`.
pub fn residual(expr: &FilterExpr, granule: &GranuleStatistics) -> Option<FilterExpr> {
    match expr.truth(granule) {
        Truth::False => None,
        Truth::True => Some(FilterExpr::And(Vec::new())),
        Truth::Unknown => Some(match expr {
            FilterExpr::Clause(_) | FilterExpr::In(_) => expr.clone(),
            FilterExpr::And(children) => FilterExpr::And(
                children
                    .iter()
                    .filter(|child| child.truth(granule) != Truth::True)
                    .filter_map(|child| residual(child, granule))
                    .collect(),
            ),
            FilterExpr::Or(children) => FilterExpr::Or(children.iter().filter_map(|c| residual(c, granule)).collect()),
        }),
    }
}

/// Indexes of the granules in `granules` the expression cannot rule out.
pub fn prune_granules(expr: &FilterExpr, granules: &[GranuleStatistics]) -> Vec<usize> {
    (0..granules.len())
        .filter(|&index| granules.get(index).is_some_and(|granule| !drops_granule(expr, granule)))
        .collect()
}

/// A query whose filter is a boolean tree, with an optional row limit and order: the general form of [`Query`] that
/// [`plan_query`](super::pruning::plan_query) plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprQuery {
    pub answerable_from_aggregate: bool,
    pub filter: FilterExpr,
    pub limit: Option<usize>,
    pub order: ScanOrder,
}

impl From<Query> for ExprQuery {
    fn from(query: Query) -> Self {
        ExprQuery {
            answerable_from_aggregate: query.answerable_from_aggregate,
            filter: query.filter.into(),
            limit: None,
            order: ScanOrder::Stored,
        }
    }
}

impl From<&Query> for ExprQuery {
    fn from(query: &Query) -> Self {
        query.clone().into()
    }
}

impl From<&ExprQuery> for ExprQuery {
    fn from(query: &ExprQuery) -> Self {
        query.clone()
    }
}

/// A leaf's verdict: `False` when the granule's statistics prove it absent, else `Unknown` (no granule statistic
/// confirms a match).
fn absent_or_unknown(absent: bool) -> Truth {
    if absent { Truth::False } else { Truth::Unknown }
}

fn collect_conjunction(expr: &FilterExpr, out: &mut Filter) -> bool {
    match expr {
        FilterExpr::Clause(clause) => {
            out.clauses.push(*clause);
            true
        }
        FilterExpr::In(in_list) => {
            out.clauses.push(in_list.clause);
            out.memberships.push(in_list.clone());
            true
        }
        FilterExpr::And(children) => children.iter().all(|child| collect_conjunction(child, out)),
        FilterExpr::Or(children) => match children.as_slice() {
            [only] => collect_conjunction(only, out),
            _ => false,
        },
    }
}

#[cfg(test)]
#[path = "test/bool_pruning.rs"]
mod tests;
