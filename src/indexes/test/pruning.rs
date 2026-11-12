use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::columns::{FreetextDeclaration, PromotedColumn, PromotionPlan, column_ids};
use crate::events::variant::VariantValue;
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::layout::LayoutTargets;
use crate::layout::footer::ColumnKind;
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, build_oe_file};

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

fn filter(clauses: Vec<FilterClause>) -> Filter {
    Filter::new(clauses)
}

fn stats(statistics: Vec<Statistic>) -> GranuleStatistics {
    GranuleStatistics { statistics }
}

#[test]
fn minmax_outside_range_proves_absent() {
    let stat = Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    };
    assert_eq!(stat.judge(&eq_clause(1, 100)), StatVerdict::ProvenAbsent);
    assert_eq!(stat.judge(&eq_clause(1, 5)), StatVerdict::CouldMatch);
}

#[test]
fn point_membership_never_answers_range() {
    let stat = Statistic::PointMembership {
        column_id: 1,
        members: vec![1, 2, 3],
    };
    assert!(stat.relevant_to(&eq_clause(1, 1)));
    assert!(!stat.relevant_to(&range_clause(1, 0, 100)));
}

#[test]
fn range_filter_answers_range_not_presence() {
    let stat = Statistic::RangeEmptiness {
        column_id: 1,
        non_empty_spans: vec![(0, 10), (50, 60)],
    };
    assert!(stat.relevant_to(&range_clause(1, 0, 5)));
    assert!(!stat.relevant_to(&presence_clause(1)));
    assert_eq!(stat.judge(&range_clause(1, 20, 30)), StatVerdict::ProvenAbsent);
    assert_eq!(stat.judge(&range_clause(1, 5, 55)), StatVerdict::CouldMatch);
}

#[test]
fn fully_null_column_proves_value_absent() {
    let stat = Statistic::NullCount {
        column_id: 1,
        null_count: 8,
        row_count: 8,
    };
    assert_eq!(stat.judge(&eq_clause(1, 5)), StatVerdict::ProvenAbsent);

    let partial = Statistic::NullCount {
        column_id: 1,
        null_count: 4,
        row_count: 8,
    };
    assert_eq!(partial.judge(&eq_clause(1, 5)), StatVerdict::CouldMatch);
}

#[test]
fn one_expression_holds_one_term_per_relevant_statistic() {
    let declared = stats(vec![
        Statistic::MinMaxBound {
            column_id: 1,
            max: 100,
            min: 0,
        },
        Statistic::PointMembership {
            column_id: 2,
            members: vec![7, 8, 9],
        },
        Statistic::RangeEmptiness {
            column_id: 3,
            non_empty_spans: vec![(0, 50)],
        },
        Statistic::NullCount {
            column_id: 9,
            null_count: 0,
            row_count: 4,
        },
    ]);
    let f = filter(vec![eq_clause(1, 5), eq_clause(2, 8), range_clause(3, 10, 20)]);
    let expr = build_pruning_expression(&f, &declared);
    assert_eq!(expr.term_count(), 3);

    let outcome = evaluate(&expr, &declared);
    assert!(!outcome.drop);
}

#[test]
fn missing_statistic_keeps_granule() {
    let declared = stats(vec![]);
    let f = filter(vec![eq_clause(1, 5)]);
    let expr = build_pruning_expression(&f, &declared);
    assert_eq!(expr.term_count(), 0);
    let outcome = evaluate(&expr, &declared);
    assert!(!outcome.drop);
    assert!(outcome.residual_required);
}

#[test]
fn conservative_keeps_non_matching_but_never_drops_matching() {
    let declared = stats(vec![Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    }]);
    let kept = build_pruning_expression(&filter(vec![eq_clause(1, 5)]), &declared);
    assert!(!evaluate(&kept, &declared).drop);

    let matching = build_pruning_expression(&filter(vec![range_clause(1, 0, 3)]), &declared);
    assert!(!evaluate(&matching, &declared).drop);

    let absent = build_pruning_expression(&filter(vec![eq_clause(1, 999)]), &declared);
    assert!(evaluate(&absent, &declared).drop);
}

#[test]
fn inexact_surviving_term_requires_residual() {
    let declared = stats(vec![Statistic::PointMembership {
        column_id: 1,
        members: vec![5, 6, 7],
    }]);
    let expr = build_pruning_expression(&filter(vec![eq_clause(1, 5)]), &declared);
    let outcome = evaluate(&expr, &declared);
    assert!(!outcome.drop);
    assert!(outcome.residual_required);
}

#[test]
fn bounds_only_surviving_term_requires_residual() {
    let declared = stats(vec![Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    }]);
    let expr = build_pruning_expression(&filter(vec![eq_clause(1, 5)]), &declared);
    let outcome = evaluate(&expr, &declared);
    assert!(!outcome.drop);
    assert!(outcome.residual_required);

    let range_expr = build_pruning_expression(&filter(vec![range_clause(1, 5, 100)]), &declared);
    let range_outcome = evaluate(&range_expr, &declared);
    assert!(!range_outcome.drop);
    assert!(range_outcome.residual_required);
}

#[test]
fn presence_keep_still_needs_the_row_level_filter() {
    let declared = stats(vec![Statistic::PathPresence {
        column_id: 4,
        present: true,
    }]);
    let expr = build_pruning_expression(&filter(vec![presence_clause(4)]), &declared);
    let outcome = evaluate(&expr, &declared);
    assert!(!outcome.drop);
    assert!(outcome.residual_required);
}

#[test]
fn a_clause_no_statistic_covers_requires_the_residual_filter_beside_a_confirming_one() {
    let declared = stats(vec![Statistic::PathPresence {
        column_id: 4,
        present: true,
    }]);
    let expr = build_pruning_expression(&filter(vec![presence_clause(4), eq_clause(1, 5)]), &declared);
    let outcome = evaluate(&expr, &declared);
    assert!(!outcome.drop);
    assert!(outcome.residual_required);
}

#[test]
fn dropped_granule_needs_no_residual() {
    let declared = stats(vec![
        Statistic::MinMaxBound {
            column_id: 1,
            max: 10,
            min: 0,
        },
        Statistic::PointMembership {
            column_id: 1,
            members: vec![5],
        },
    ]);
    let expr = build_pruning_expression(&filter(vec![eq_clause(1, 999)]), &declared);
    let outcome = evaluate(&expr, &declared);
    assert!(outcome.drop);
    assert!(!outcome.residual_required);
}

#[test]
fn new_statistic_kind_adds_a_term_in_the_same_loop() {
    let declared = stats(vec![Statistic::PathPresence {
        column_id: 4,
        present: false,
    }]);
    let f = filter(vec![presence_clause(4)]);
    let expr = build_pruning_expression(&f, &declared);
    assert_eq!(expr.term_count(), 1);
    assert!(evaluate(&expr, &declared).drop);

    let present = stats(vec![Statistic::PathPresence {
        column_id: 4,
        present: true,
    }]);
    let expr2 = build_pruning_expression(&f, &present);
    assert!(!evaluate(&expr2, &present).drop);
}

#[test]
fn evaluate_matches_per_statistic_decision() {
    let declared = stats(vec![
        Statistic::MinMaxBound {
            column_id: 1,
            max: 10,
            min: 0,
        },
        Statistic::RangeEmptiness {
            column_id: 2,
            non_empty_spans: vec![(100, 200)],
        },
    ]);
    let f = filter(vec![eq_clause(1, 5), range_clause(2, 0, 50)]);

    let manual_drop = declared.statistics.iter().any(|s| {
        f.clauses
            .iter()
            .any(|c| s.relevant_to(c) && s.judge(c) == StatVerdict::ProvenAbsent)
    });

    let expr = build_pruning_expression(&f, &declared);
    let outcome = evaluate(&expr, &declared);
    assert_eq!(outcome.drop, manual_drop);
    assert!(outcome.drop);
}

#[test]
fn aggregate_query_takes_shortcut_not_pruning() {
    let query = Query {
        answerable_from_aggregate: true,
        filter: filter(vec![eq_clause(1, 5)]),
    };
    let declared = stats(vec![Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    }]);
    assert_eq!(plan_query(&query, &declared), QueryPlan::AggregateShortcut);
}

#[test]
fn non_aggregate_query_falls_through_to_scan() {
    let query = Query {
        answerable_from_aggregate: false,
        filter: filter(vec![eq_clause(1, 5)]),
    };
    let declared = stats(vec![Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    }]);
    match plan_query(&query, &declared) {
        QueryPlan::Scan {
            pruning: Some(pruning), ..
        } => assert_eq!(pruning.term_count(), 1),
        QueryPlan::AggregateShortcut | QueryPlan::Scan { pruning: None, .. } => panic!("expected a scan plan"),
    }
}

#[test]
fn evaluate_uses_the_granule_under_test_not_the_template() {
    let template = stats(vec![Statistic::MinMaxBound {
        column_id: 1,
        max: 1000,
        min: 0,
    }]);
    let f = filter(vec![eq_clause(1, 500)]);
    let expr = build_pruning_expression(&f, &template);

    let other_granule = stats(vec![Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    }]);
    assert!(evaluate(&expr, &other_granule).drop);
}

fn reference_prune_pages(filter: &Filter, page_statistics: &[(u32, GranuleStatistics)], total: u32) -> Vec<u32> {
    let no_stats = GranuleStatistics::default();
    (0..total)
        .filter(|&page_idx| {
            let stats = page_statistics
                .iter()
                .find(|(pi, _)| *pi == page_idx)
                .map(|(_, s)| s)
                .unwrap_or(&no_stats);
            !evaluate(&build_pruning_expression(filter, stats), stats).drop
        })
        .collect()
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn random_below(state: &mut u64, bound: u64) -> u64 {
    next_random(state) % bound
}

fn random_value(state: &mut u64) -> i128 {
    random_below(state, 13) as i128 - 6
}

fn random_values(state: &mut u64) -> Vec<i128> {
    (0..random_below(state, 4)).map(|_| random_value(state)).collect()
}

fn random_clause(state: &mut u64) -> FilterClause {
    let predicate = match random_below(state, 4) {
        0 => PredicateKind::Equality,
        1 => PredicateKind::Membership,
        2 => PredicateKind::Presence,
        _ => PredicateKind::Range,
    };
    let a = random_value(state);
    let b = random_value(state);
    let (value_lo, value_hi) = match predicate {
        PredicateKind::Equality => (a, a),
        _ => (a.min(b), a.max(b)),
    };
    FilterClause {
        column_id: random_below(state, 3) as u32,
        predicate,
        value_hi,
        value_lo,
    }
}

fn random_statistic(state: &mut u64) -> Statistic {
    let column_id = random_below(state, 3) as u32;
    let a = random_value(state);
    let b = random_value(state);
    let (lo, hi) = (a.min(b), a.max(b));
    match random_below(state, 8) {
        0 => Statistic::MinMaxBound {
            column_id,
            max: hi,
            min: lo,
        },
        1 => {
            let row_count = random_below(state, 3);
            Statistic::NullCount {
                column_id,
                null_count: random_below(state, row_count + 2),
                row_count,
            }
        }
        2 => Statistic::PathPresence {
            column_id,
            present: random_below(state, 2) == 0,
        },
        3 => Statistic::PointMembership {
            column_id,
            members: random_values(state),
        },
        4 => Statistic::RangeEmptiness {
            column_id,
            non_empty_spans: (0..random_below(state, 3))
                .map(|_| {
                    let x = random_value(state);
                    let y = random_value(state);
                    (x.min(y), x.max(y))
                })
                .collect(),
        },
        5 => Statistic::SequenceBounds {
            column_id,
            max_sequence: hi,
            min_sequence: lo,
        },
        6 => Statistic::TextToken {
            column_id,
            token_hashes: random_values(state),
        },
        _ => Statistic::TimeBounds {
            column_id,
            max_time: hi,
            min_time: lo,
        },
    }
}

#[test]
fn page_pruning_matches_building_and_evaluating_an_expression_per_page() {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..20_000 {
        let f = filter(
            (0..random_below(&mut state, 4))
                .map(|_| random_clause(&mut state))
                .collect(),
        );
        let total = random_below(&mut state, 6) as u32;
        let page_statistics: Vec<(u32, GranuleStatistics)> = (0..random_below(&mut state, 8))
            .map(|_| {
                let page_idx = random_below(&mut state, 8) as u32;
                let page = stats(
                    (0..random_below(&mut state, 6))
                        .map(|_| random_statistic(&mut state))
                        .collect(),
                );
                (page_idx, page)
            })
            .collect();
        assert_eq!(
            prune_pages_in_granule(&f, &page_statistics, total),
            reference_prune_pages(&f, &page_statistics, total),
            "filter {f:?}, pages {page_statistics:?}, total {total}"
        );
    }
}

#[test]
fn page_pruning_judges_contradictory_duplicates_by_the_first_statistic_of_their_kind() {
    let wide = Statistic::MinMaxBound {
        column_id: 1,
        max: 1000,
        min: 0,
    };
    let narrow = Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    };
    let f = filter(vec![eq_clause(1, 500)]);
    let pages = vec![
        (0, stats(vec![wide.clone(), narrow.clone()])),
        (1, stats(vec![narrow, wide])),
        (1, stats(vec![])),
        (9, stats(vec![])),
    ];
    assert_eq!(prune_pages_in_granule(&f, &pages, 3), vec![0, 2]);
    assert_eq!(reference_prune_pages(&f, &pages, 3), vec![0, 2]);
    assert_eq!(prune_pages_in_granule(&filter(vec![]), &pages, 3), vec![0, 1, 2]);
}

fn indexed_row(i: u64) -> OeRow {
    let mut payload = std::collections::BTreeMap::new();
    payload.insert("amount".to_owned(), VariantValue::Int(1_000 + i as i64));
    OeRow {
        epoch: 1,
        sequence: i + 1,
        event: EventInput {
            envelope: EventEnvelope {
                event_id: EventId::new_test_id(0xBEEF_0000 + u128::from(i)),
                tenant_id: TenantId::new_test_id(7),
                stream_id: StreamId(2),
                stream_sequence: i,
                occurred_at: TimestampValue::from_physical_nanos(1_000_000 + i as i64 * 1000),
                ingested_at: TimestampValue::from_physical_nanos(2_000_000 + i as i64 * 1000),
                source: "crm".to_owned(),
                event_type: "deal.updated".to_owned(),
                entity_type: "opportunity".to_owned(),
                entity_id_hash_low: i,
                entity_id_hash_high: 1,
                entity_id: Some(format!("opp-{i}")),
                actor_id_hash_low: 3,
                actor_id: None,
                account_id_hash_low: 4,
                account_id: None,
                trace_id_hash_low: 5,
                dedupe_hash_low: 100 + i,
                dedupe_hash_high: 6,
                schema_version: 2,
                flags: EventFlags(0),
            },
            payload: PayloadInput::Variant(VariantValue::object(payload)),
            source_schema: None,
            source_delivery: None,
            connector_delivery_hash_low: i,
            connector_delivery_hash_high: 0,
            provenance: None,
            relationships: None,
        },
    }
}

fn indexed_config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 42,
        footer_dek: None,
        footer_encryption: crate::security::FooterEncryption::Plaintext,
        freetext: FreetextDeclaration { fields: Vec::new() },
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 5,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan {
            columns: vec![PromotedColumn {
                name: "amount_promoted".to_owned(),
                path: "amount".to_owned(),
                kind: ColumnKind::I64,
                since_schema_version: 1,
                substring_searchable: false,
            }],
        },
        targets: LayoutTargets {
            index_granularity: 64,
            index_granularity_bytes: 1 << 30,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(7),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

#[test]
fn hot_column_prunes_pages_cold_column_falls_back_to_granule_granularity() {
    let rows: Vec<OeRow> = (0..17).map(indexed_row).collect();
    let built = build_oe_file(rows.clone(), &indexed_config()).unwrap();
    let granule_id = built.footer.granules[0].granule_id;
    let hot_column_id = column_ids::PROMOTED_BASE;
    let cold_column_id = column_ids::SEQUENCE;

    let mut hot_pages: Vec<_> = built
        .footer
        .page_minmax
        .iter()
        .filter(|entry| entry.column_id == hot_column_id && entry.granule_id == granule_id)
        .collect();
    hot_pages.sort_by_key(|entry| entry.page_index);
    assert_eq!(hot_pages.len(), 4, "17 rows at 5 rows/page must split into 4 pages");

    let hot_page_statistics: Vec<(u32, GranuleStatistics)> = hot_pages
        .iter()
        .map(|entry| {
            (
                entry.page_index,
                GranuleStatistics {
                    statistics: vec![Statistic::MinMaxBound {
                        column_id: hot_column_id,
                        max: entry.max_i128.expect("hot page carries a bound"),
                        min: entry.min_i128.expect("hot page carries a bound"),
                    }],
                },
            )
        })
        .collect();

    let hot_filter = filter(vec![range_clause(hot_column_id, 1005, 1014)]);
    let surviving_hot_pages = prune_pages_in_granule(&hot_filter, &hot_page_statistics, hot_pages.len() as u32);
    assert_eq!(
        surviving_hot_pages,
        vec![1, 2],
        "page-level min/max must reject every page whose span cannot hold the queried range"
    );

    assert!(
        built
            .footer
            .page_minmax
            .iter()
            .all(|entry| entry.column_id != cold_column_id),
        "a column the promotion plan does not mention must carry no page-level minmax entries"
    );

    let cold_total_pages = built
        .footer
        .page_directory
        .iter()
        .filter(|entry| entry.column_id == cold_column_id && entry.granule_id == granule_id)
        .map(|entry| entry.page_index + 1)
        .max()
        .unwrap_or(0);
    assert_eq!(
        cold_total_pages,
        hot_pages.len() as u32,
        "both columns are split into the same page geometry"
    );

    let cold_filter = filter(vec![range_clause(cold_column_id, 5, 8)]);
    let surviving_cold_pages = prune_pages_in_granule(&cold_filter, &[], cold_total_pages);
    assert_eq!(
        surviving_cold_pages,
        (0..cold_total_pages).collect::<Vec<_>>(),
        "a cold column's pages must all be kept — absence of page-level stats never prunes a page"
    );

    let granule = built
        .footer
        .granules
        .iter()
        .find(|granule| granule.granule_id == granule_id)
        .expect("built granule");
    let granule_stats = GranuleStatistics {
        statistics: vec![Statistic::SequenceBounds {
            column_id: cold_column_id,
            max_sequence: granule.last_sequence as i128,
            min_sequence: granule.first_sequence as i128,
        }],
    };
    let kept = evaluate(&build_pruning_expression(&cold_filter, &granule_stats), &granule_stats);
    assert!(
        !kept.drop,
        "sequence range [5, 8] falls inside the granule's own span, so granule-level pruning must keep it"
    );
    let dropped = evaluate(
        &build_pruning_expression(
            &filter(vec![range_clause(cold_column_id, 10_000, 10_001)]),
            &granule_stats,
        ),
        &granule_stats,
    );
    assert!(
        dropped.drop,
        "a range wholly outside the granule's sequence span must still be dropped without any page-level stats"
    );
}

#[test]
fn page_pruning_first_duplicate_wins_with_unsorted_and_out_of_range_ids() {
    let bounds = |min: i128, max: i128| stats(vec![Statistic::MinMaxBound { column_id: 1, max, min }]);
    let page_statistics = vec![
        (2, bounds(100, 200)),
        (0, bounds(0, 10)),
        (2, bounds(0, 10)),
        (9, bounds(100, 200)),
    ];
    let kept = prune_pages_in_granule(&filter(vec![eq_clause(1, 5)]), &page_statistics, 3);
    assert_eq!(
        kept,
        vec![0, 1],
        "page 2's first entry rules it out; page 1 has no stats; page 9 is out of range"
    );
}

#[test]
fn inline_statistics_match_their_vector_forms() {
    let clauses = [
        eq_clause(1, 5),
        eq_clause(1, -3),
        eq_clause(2, 5),
        range_clause(1, 3, 8),
        range_clause(1, 8, 3),
        range_clause(1, i128::MIN, i128::MAX),
        presence_clause(1),
        FilterClause {
            column_id: 1,
            predicate: PredicateKind::Membership,
            value_hi: 9,
            value_lo: 4,
        },
    ];
    for clause in clauses {
        for member in [None, Some(clause.value_lo), Some(7), Some(-1)] {
            let inline = Statistic::SingleMember { column_id: 1, member };
            let vector = Statistic::PointMembership {
                column_id: 1,
                members: member.into_iter().collect(),
            };
            assert_eq!(inline.relevant_to(&clause), vector.relevant_to(&clause));
            assert_eq!(inline.judge(&clause), vector.judge(&clause), "{clause:?} {member:?}");
            assert_eq!(exactness_of(&inline), exactness_of(&vector));
            assert!(!inline.keep_confirms_match());
        }
        for span in [
            None,
            Some((clause.value_lo, clause.value_hi)),
            Some((6, 7)),
            Some((9, 2)),
        ] {
            let inline = Statistic::SingleSpan { column_id: 1, span };
            let vector = Statistic::RangeEmptiness {
                column_id: 1,
                non_empty_spans: span.into_iter().collect(),
            };
            assert_eq!(inline.relevant_to(&clause), vector.relevant_to(&clause));
            assert_eq!(inline.judge(&clause), vector.judge(&clause), "{clause:?} {span:?}");
            assert_eq!(exactness_of(&inline), exactness_of(&vector));
            assert!(!inline.keep_confirms_match());
        }
    }
}

#[test]
fn membership_lists_prune_by_binary_search_and_contiguous_lists_become_ranges() {
    let raw = vec![90, 10, 50, 10, 70, 30, 90];
    let list = MembershipList::new(raw.clone());
    assert_eq!(list.values(), &[10, 30, 50, 70, 90]);
    for lo in 0..100 {
        assert_eq!(list.contains(lo), raw.contains(&lo));
        for hi in lo - 3..lo + 25 {
            let linear = raw.iter().any(|value| (lo..=hi).contains(value));
            assert_eq!(list.any_in(lo, hi), linear, "[{lo}, {hi}]");
            let expected = if linear {
                StatVerdict::CouldMatch
            } else {
                StatVerdict::ProvenAbsent
            };
            assert_eq!(list.judge_bounds(lo, hi), expected);
        }
    }
    let envelope = list.clause(4).unwrap();
    assert_eq!(envelope.predicate, PredicateKind::Membership);
    assert_eq!((envelope.value_lo, envelope.value_hi), (10, 90));
    let gap = Statistic::MinMaxBound {
        column_id: 4,
        max: 49,
        min: 31,
    };
    assert_eq!(gap.judge(&envelope), StatVerdict::CouldMatch);
    assert_eq!(list.judge_bounds(31, 49), StatVerdict::ProvenAbsent);

    let contiguous = MembershipList::new((5..=20).rev().chain(7..9).collect())
        .clause(2)
        .unwrap();
    assert_eq!(contiguous, range_clause(2, 5, 20));
    assert_eq!(
        MembershipList::new(vec![i128::MIN, i128::MAX])
            .clause(1)
            .unwrap()
            .predicate,
        PredicateKind::Membership
    );
    assert_eq!(MembershipList::new(vec![]).clause(1), None);
}

#[test]
fn filter_in_list_prunes_gaps_against_naive_rows() {
    let values = vec![17, 205, 206, 640, 999];
    let filter = Filter::default().with_in_list(3, values.clone());
    assert_eq!(filter.memberships.len(), 1);
    let declared = stats(vec![Statistic::MinMaxBound {
        column_id: 3,
        max: 0,
        min: 0,
    }]);
    let expr = build_pruning_expression(&filter, &declared);
    let mut page_stats = Vec::new();
    for g in 0..100i128 {
        let rows: Vec<i128> = (0..10).map(|r| g * 10 + r).collect();
        let granule = stats(vec![Statistic::MinMaxBound {
            column_id: 3,
            max: g * 10 + 9,
            min: g * 10,
        }]);
        let holds_match = rows.iter().any(|row| values.contains(row));
        assert_eq!(evaluate(&expr, &granule).drop, !holds_match, "granule {g}");
        let envelope_only = build_pruning_expression(&Filter::new(filter.clauses.clone()), &declared);
        assert_eq!(evaluate(&envelope_only, &granule).drop, g < 1);
        page_stats.push((g as u32, granule));
    }
    assert_eq!(prune_pages_in_granule(&filter, &page_stats, 100), vec![1, 20, 64, 99]);

    let points = stats(vec![Statistic::PointMembership {
        column_id: 3,
        members: vec![18, 300, 998],
    }]);
    assert!(evaluate(&build_pruning_expression(&filter, &points), &points).drop);
    let range = Filter::default().with_in_list(3, vec![4, 5, 6]);
    assert!(range.memberships.is_empty());
    assert_eq!(range.clauses, vec![range_clause(3, 4, 6)]);
}

#[test]
fn plan_query_carries_boolean_filters_limit_and_order() {
    use crate::indexes::bool_pruning::ExprQuery;
    let declared = stats(vec![Statistic::MinMaxBound {
        column_id: 1,
        max: 10,
        min: 0,
    }]);
    let or = FilterExpr::Or(vec![
        FilterExpr::Clause(eq_clause(1, 5)),
        FilterExpr::Clause(eq_clause(1, 9)),
    ]);
    let plan = plan_query(
        ExprQuery {
            answerable_from_aggregate: false,
            filter: or.clone(),
            limit: Some(3),
            order: ScanOrder::TimeDescending,
        },
        &declared,
    );
    assert_eq!(
        plan,
        QueryPlan::Scan {
            filter: or,
            limit: Some(3),
            order: ScanOrder::TimeDescending,
            pruning: None,
        }
    );
    let flat = Query {
        answerable_from_aggregate: false,
        filter: filter(vec![eq_clause(1, 5)]),
    };
    match plan_query(&flat, &declared) {
        QueryPlan::Scan {
            filter,
            limit: None,
            order: ScanOrder::Stored,
            pruning: Some(pruning),
        } => {
            assert_eq!(filter, FilterExpr::from(flat.filter.clone()));
            assert_eq!(pruning.term_count(), 1);
        }
        other => panic!("unexpected plan {other:?}"),
    }
}
