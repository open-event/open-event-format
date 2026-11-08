use super::*;
use crate::indexes::pruning::{PredicateKind, Statistic, build_pruning_expression, evaluate};

fn range(column_id: u32, lo: i128, hi: i128) -> FilterExpr {
    FilterExpr::Clause(FilterClause {
        column_id,
        predicate: PredicateKind::Range,
        value_hi: hi,
        value_lo: lo,
    })
}

/// A granule whose column 1 spans `[a_lo, a_hi]` and column 2 spans `[b_lo, b_hi]`, with the rows behind those bounds.
struct Granule {
    rows: Vec<(i128, i128)>,
    stats: GranuleStatistics,
}

fn granules() -> Vec<Granule> {
    (0..20)
        .map(|g: i128| {
            let rows: Vec<(i128, i128)> = (0..10).map(|r| (g * 10 + r, (g * 7 + r * 3) % 50)).collect();
            let bounds = |pick: fn(&(i128, i128)) -> i128| {
                let values = rows.iter().map(pick);
                (values.clone().min().unwrap_or(0), values.max().unwrap_or(0))
            };
            let (a_min, a_max) = bounds(|row| row.0);
            let (b_min, b_max) = bounds(|row| row.1);
            Granule {
                stats: GranuleStatistics {
                    statistics: vec![
                        Statistic::MinMaxBound {
                            column_id: 1,
                            max: a_max,
                            min: a_min,
                        },
                        Statistic::MinMaxBound {
                            column_id: 2,
                            max: b_max,
                            min: b_min,
                        },
                    ],
                },
                rows,
            }
        })
        .collect()
}

fn row_matches(expr: &FilterExpr, row: (i128, i128)) -> bool {
    expr.matches_row(&mut |clause| {
        let value = if clause.column_id == 1 { row.0 } else { row.1 };
        value >= clause.value_lo && value <= clause.value_hi
    })
}

/// Pruning plus residual filtering returns exactly what a naive full scan returns, for mixed AND/OR shapes.
#[test]
fn pruned_scan_with_residual_equals_naive_scan() {
    let exprs = vec![
        FilterExpr::Or(vec![range(1, 0, 15), range(1, 180, 199)]),
        FilterExpr::Or(vec![range(1, 50, 52), range(2, 1000, 2000)]),
        FilterExpr::And(vec![
            range(2, 0, 10),
            FilterExpr::Or(vec![range(1, 30, 60), range(1, 120, 125)]),
        ]),
        FilterExpr::Or(vec![
            FilterExpr::And(vec![range(1, 0, 40), range(2, 40, 49)]),
            range(1, 150, 151),
        ]),
        FilterExpr::Or(Vec::new()),
        FilterExpr::And(Vec::new()),
    ];
    let granules = granules();
    for expr in &exprs {
        let naive: Vec<(i128, i128)> = granules
            .iter()
            .flat_map(|granule| granule.rows.iter().copied())
            .filter(|&row| row_matches(expr, row))
            .collect();
        let mut pruned = Vec::new();
        for granule in &granules {
            let Some(residual) = residual(expr, &granule.stats) else {
                assert!(
                    granule.rows.iter().all(|&row| !row_matches(expr, row)),
                    "{expr:?} dropped a match"
                );
                continue;
            };
            pruned.extend(granule.rows.iter().copied().filter(|&row| row_matches(&residual, row)));
        }
        assert_eq!(pruned, naive, "{expr:?}");
    }
}

#[test]
fn or_drops_only_when_every_branch_is_absent() {
    let granules = granules();
    let expr = FilterExpr::Or(vec![range(1, 0, 15), range(1, 180, 199)]);
    let stats: Vec<GranuleStatistics> = granules.iter().map(|granule| granule.stats.clone()).collect();
    assert_eq!(prune_granules(&expr, &stats), vec![0, 1, 18, 19]);
    // One branch on another column no granule can rule out keeps every granule.
    let wide = FilterExpr::Or(vec![range(1, 0, 15), range(2, 0, 49)]);
    assert_eq!(prune_granules(&wide, &stats).len(), 20);
}

#[test]
fn residual_removes_absent_or_branches() {
    let granules = granules();
    let expr = FilterExpr::Or(vec![range(1, 0, 15), range(1, 180, 199)]);
    let residual = residual(&expr, &granules[0].stats).unwrap();
    assert_eq!(residual, FilterExpr::Or(vec![range(1, 0, 15)]));
}

/// A pure conjunction agrees with the existing AND-only evaluator.
#[test]
fn conjunction_matches_flat_filter() {
    let expr = FilterExpr::And(vec![range(1, 40, 60), range(2, 0, 20)]);
    let flat = expr.as_conjunction().unwrap();
    assert!(
        FilterExpr::Or(vec![range(1, 0, 1), range(1, 3, 4)])
            .as_conjunction()
            .is_none()
    );
    for granule in granules() {
        let expected = evaluate(&build_pruning_expression(&flat, &granule.stats), &granule.stats).drop;
        assert_eq!(drops_granule(&expr, &granule.stats), expected);
        assert_eq!(drops_granule(&FilterExpr::from(flat.clone()), &granule.stats), expected);
    }
}
