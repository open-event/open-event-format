use oe::indexes::pruning::*;

fn eq_clause(column_id: u32, value: i128) -> FilterClause {
    FilterClause {
        column_id,
        predicate: PredicateKind::Equality,
        value_hi: value,
        value_lo: value,
    }
}

fn range_clause(column_id: u32, lo: i128, hi: i128) -> FilterClause {
    FilterClause {
        column_id,
        predicate: PredicateKind::Range,
        value_hi: hi,
        value_lo: lo,
    }
}

fn presence_clause(column_id: u32) -> FilterClause {
    FilterClause {
        column_id,
        predicate: PredicateKind::Presence,
        value_hi: 0,
        value_lo: 0,
    }
}

#[test]
fn one_expression_spans_every_declared_statistic() {
    let declared = GranuleStatistics {
        statistics: vec![
            Statistic::MinMaxBound {
                column_id: 1,
                max: 100,
                min: 0,
            },
            Statistic::PointMembership {
                column_id: 2,
                members: vec![10, 20, 30],
            },
            Statistic::RangeEmptiness {
                column_id: 3,
                non_empty_spans: vec![(0, 50)],
            },
        ],
    };
    let filter = Filter {
        memberships: Vec::new(),
        clauses: vec![eq_clause(1, 5), eq_clause(2, 20), range_clause(3, 10, 40)],
    };

    let expr = build_pruning_expression(&filter, &declared);
    assert_eq!(expr.term_count(), 3);

    let outcome = evaluate(&expr, &declared);
    assert!(!outcome.drop);
}

#[test]
fn a_granule_with_no_matching_row_may_be_kept_but_one_with_a_match_is_never_dropped() {
    let cannot_prove = GranuleStatistics {
        statistics: vec![Statistic::MinMaxBound {
            column_id: 1,
            max: 10,
            min: 0,
        }],
    };
    let filter = Filter {
        memberships: Vec::new(),
        clauses: vec![eq_clause(1, 5)],
    };
    let expr = build_pruning_expression(&filter, &cannot_prove);
    assert!(!evaluate(&expr, &cannot_prove).drop);

    let has_match = GranuleStatistics {
        statistics: vec![Statistic::MinMaxBound {
            column_id: 1,
            max: 10,
            min: 0,
        }],
    };
    let range = Filter {
        memberships: Vec::new(),
        clauses: vec![range_clause(1, 2, 4)],
    };
    let expr_match = build_pruning_expression(&range, &has_match);
    assert!(!evaluate(&expr_match, &has_match).drop);

    let no_stat = GranuleStatistics { statistics: vec![] };
    let other = Filter {
        memberships: Vec::new(),
        clauses: vec![eq_clause(7, 42)],
    };
    let expr_missing = build_pruning_expression(&other, &no_stat);
    assert_eq!(expr_missing.term_count(), 0);
    assert!(!evaluate(&expr_missing, &no_stat).drop);

    let absent = Filter {
        memberships: Vec::new(),
        clauses: vec![eq_clause(1, 999)],
    };
    let expr_absent = build_pruning_expression(&absent, &cannot_prove);
    assert!(evaluate(&expr_absent, &cannot_prove).drop);
}

#[test]
fn a_new_statistic_adds_a_term_not_a_branch() {
    let filter = Filter {
        memberships: Vec::new(),
        clauses: vec![presence_clause(4)],
    };

    let absent = GranuleStatistics {
        statistics: vec![Statistic::PathPresence {
            column_id: 4,
            present: false,
        }],
    };
    let expr = build_pruning_expression(&filter, &absent);
    assert_eq!(expr.term_count(), 1);
    assert!(evaluate(&expr, &absent).drop);

    let present = GranuleStatistics {
        statistics: vec![Statistic::PathPresence {
            column_id: 4,
            present: true,
        }],
    };
    let expr_present = build_pruning_expression(&filter, &present);
    assert!(!evaluate(&expr_present, &present).drop);

    assert_eq!(
        Statistic::PathPresence {
            column_id: 4,
            present: false,
        }
        .judge(&presence_clause(4)),
        StatVerdict::ProvenAbsent
    );
}

#[test]
fn an_inexact_term_keeps_the_residual_filter() {
    let declared = GranuleStatistics {
        statistics: vec![Statistic::PointMembership {
            column_id: 1,
            members: vec![5, 6, 7],
        }],
    };
    let filter = Filter {
        memberships: Vec::new(),
        clauses: vec![eq_clause(1, 5)],
    };
    let expr = build_pruning_expression(&filter, &declared);
    let outcome = evaluate(&expr, &declared);

    assert!(!outcome.drop, "membership reports the value present");
    assert!(
        outcome.residual_required,
        "an inexact surviving term forces an exact filter above/inside the scan"
    );

    let range_declared = GranuleStatistics {
        statistics: vec![Statistic::RangeEmptiness {
            column_id: 2,
            non_empty_spans: vec![(0, 100)],
        }],
    };
    let range_filter = Filter {
        memberships: Vec::new(),
        clauses: vec![range_clause(2, 10, 20)],
    };
    let range_expr = build_pruning_expression(&range_filter, &range_declared);
    let range_outcome = evaluate(&range_expr, &range_declared);
    assert!(!range_outcome.drop);
    assert!(range_outcome.residual_required);
}

#[test]
fn aggregate_and_sketch_shortcuts_stay_above_pruning() {
    let declared = GranuleStatistics {
        statistics: vec![Statistic::MinMaxBound {
            column_id: 1,
            max: 10,
            min: 0,
        }],
    };

    let answerable = Query {
        answerable_from_aggregate: true,
        filter: Filter {
            memberships: Vec::new(),
            clauses: vec![eq_clause(1, 5)],
        },
    };
    assert_eq!(plan_query(&answerable, &declared), QueryPlan::AggregateShortcut);

    let not_answerable = Query {
        answerable_from_aggregate: false,
        filter: Filter {
            memberships: Vec::new(),
            clauses: vec![eq_clause(1, 5)],
        },
    };
    match plan_query(&not_answerable, &declared) {
        QueryPlan::Scan {
            pruning: Some(pruning), ..
        } => assert_eq!(pruning.term_count(), 1),
        QueryPlan::AggregateShortcut | QueryPlan::Scan { pruning: None, .. } => {
            panic!("expected a scan plan when no aggregate answers it")
        }
    }
}
