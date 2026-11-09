use super::pruning::{Filter, FilterClause, GranuleStatistics, InList, Query, clause_proven_absent};
use crate::layout::reader::query_scan::ScanOrder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterExpr {
    And(Vec<FilterExpr>),
    Clause(FilterClause),
    In(InList),
    Or(Vec<FilterExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    False,
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
    pub fn as_conjunction(&self) -> Option<Filter> {
        let mut filter = Filter::default();
        collect_conjunction(self, &mut filter).then_some(filter)
    }

    pub fn matches_row(&self, clause_matches: &mut impl FnMut(&FilterClause) -> bool) -> bool {
        match self {
            FilterExpr::Clause(clause) => clause_matches(clause),
            FilterExpr::In(in_list) => in_list.matches_row(clause_matches),
            FilterExpr::And(children) => children.iter().all(|child| child.matches_row(clause_matches)),
            FilterExpr::Or(children) => children.iter().any(|child| child.matches_row(clause_matches)),
        }
    }

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

pub fn drops_granule(expr: &FilterExpr, granule: &GranuleStatistics) -> bool {
    expr.truth(granule) == Truth::False
}

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

pub fn prune_granules(expr: &FilterExpr, granules: &[GranuleStatistics]) -> Vec<usize> {
    (0..granules.len())
        .filter(|&index| granules.get(index).is_some_and(|granule| !drops_granule(expr, granule)))
        .collect()
}

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
