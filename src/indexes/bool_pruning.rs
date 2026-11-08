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

use super::pruning::{Filter, FilterClause, GranuleStatistics, Query, StatVerdict};
use crate::layout::reader::query_scan::ScanOrder;

/// A boolean filter tree over clauses that may name different columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterExpr {
    /// All children must hold; an empty `And` is always true.
    And(Vec<FilterExpr>),
    Clause(FilterClause),
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
        FilterExpr::And(filter.clauses.into_iter().map(FilterExpr::Clause).collect())
    }
}

impl FilterExpr {
    /// The flat conjunction this expression is, when it is one, so it can take the existing AND-only path.
    pub fn as_conjunction(&self) -> Option<Filter> {
        let mut clauses = Vec::new();
        if collect_conjunction(self, &mut clauses) {
            Some(Filter { clauses })
        } else {
            None
        }
    }

    /// Whether one row satisfies the expression, given an exact per-clause row test.
    pub fn matches_row(&self, clause_matches: &mut impl FnMut(&FilterClause) -> bool) -> bool {
        match self {
            FilterExpr::Clause(clause) => clause_matches(clause),
            FilterExpr::And(children) => children.iter().all(|child| child.matches_row(clause_matches)),
            FilterExpr::Or(children) => children.iter().any(|child| child.matches_row(clause_matches)),
        }
    }

    /// The three-valued verdict of this expression over the granule described by `granule`.
    pub fn truth(&self, granule: &GranuleStatistics) -> Truth {
        match self {
            FilterExpr::Clause(clause) => clause_truth(clause, granule),
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
            FilterExpr::Clause(_) => expr.clone(),
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

/// A query whose filter is a boolean tree, with an optional row limit and order: the [`Query`] the scan path plans
/// once a filter may hold OR.
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

/// The plan for an [`ExprQuery`]: the aggregate shortcut, or a scan that prunes granules with [`drops_granule`],
/// filters kept rows with [`residual`], and pushes the limit and order down to
/// [`OeFile::scan_selective`](crate::layout::reader::OeFile::scan_selective).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprQueryPlan {
    AggregateShortcut,
    Scan {
        filter: FilterExpr,
        limit: Option<usize>,
        order: ScanOrder,
    },
}

/// Plans an [`ExprQuery`], keeping the aggregate shortcut tier above granule pruning exactly as
/// [`plan_query`](super::pruning::plan_query) does. A pure conjunction prunes exactly as the AND-only expression would.
pub fn plan_expr_query(query: &ExprQuery) -> ExprQueryPlan {
    if query.answerable_from_aggregate {
        return ExprQueryPlan::AggregateShortcut;
    }
    ExprQueryPlan::Scan {
        filter: query.filter.clone(),
        limit: query.limit,
        order: query.order,
    }
}

/// A leaf's verdict: `False` when any relevant statistic the granule declares proves the clause absent.
fn clause_truth(clause: &FilterClause, granule: &GranuleStatistics) -> Truth {
    let absent = granule
        .statistics
        .iter()
        .any(|statistic| statistic.relevant_to(clause) && statistic.judge(clause) == StatVerdict::ProvenAbsent);
    if absent { Truth::False } else { Truth::Unknown }
}

fn collect_conjunction(expr: &FilterExpr, out: &mut Vec<FilterClause>) -> bool {
    match expr {
        FilterExpr::Clause(clause) => {
            out.push(*clause);
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
