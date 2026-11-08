use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::columns::{FreetextDeclaration, PromotionPlan};
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::layout::LayoutTargets;
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, build_oe_file};

/// One row with a free-text note on every row but the fifth, an integer, and a low-cardinality string, so the file
/// carries string columns of both the view-decodable and the materializing kind beside the numeric envelope.
fn row(i: u64) -> OeRow {
    let mut payload = std::collections::BTreeMap::new();
    if !i.is_multiple_of(5) {
        payload.insert(
            "note".to_owned(),
            VariantValue::String(format!(
                "row {i} carries a note of its own, {}",
                "x".repeat((i % 7) as usize)
            )),
        );
    }
    payload.insert("amount".to_owned(), VariantValue::Int(i as i64 * 37 - 500));
    payload.insert(
        "region".to_owned(),
        VariantValue::String(["north", "south", "east", "west"][(i % 4) as usize].to_owned()),
    );
    OeRow {
        epoch: 1,
        sequence: i + 1,
        event: EventInput {
            envelope: EventEnvelope {
                event_id: EventId::new_test_id(0x5CA7_0000 + u128::from(i)),
                tenant_id: TenantId::new_test_id(3),
                stream_id: StreamId(1),
                stream_sequence: i,
                occurred_at: TimestampValue::from_physical_nanos(
                    1_000 + ((i * 37) % 97) as i64 * 3 + (i / 97) as i64 * 400,
                ),
                ingested_at: TimestampValue::from_physical_nanos(2_000 + i as i64 * 3),
                source: "crm".to_owned(),
                event_type: if i.is_multiple_of(3) {
                    "deal.updated"
                } else {
                    "deal.created"
                }
                .to_owned(),
                entity_type: "opportunity".to_owned(),
                entity_id_hash_low: i * 7919,
                entity_id_hash_high: 1,
                entity_id: None,
                actor_id_hash_low: 3,
                actor_id: None,
                account_id_hash_low: 4,
                account_id: None,
                trace_id_hash_low: 5,
                dedupe_hash_low: i,
                dedupe_hash_high: 6,
                schema_version: 1,
                flags: EventFlags(0),
            },
            payload: PayloadInput::Variant(VariantValue::Object(payload)),
            source_schema: None,
            source_delivery: None,
            connector_delivery_hash_low: i,
            connector_delivery_hash_high: 0,
            provenance: None,
            relationships: None,
        },
    }
}

/// A file of `rows` rows cut into granules of `rows_per_granule`, opened for reading.
fn file(rows: u64, rows_per_granule: usize) -> OeFile {
    let config = OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 0,
        footer_dek: None,
        footer_encryption: crate::security::FooterEncryption::Plaintext,
        freetext: FreetextDeclaration {
            fields: vec!["note".to_owned()],
        },
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan::default(),
        targets: LayoutTargets {
            index_granularity: rows_per_granule,
            index_granularity_bytes: 64 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
    };
    let built = build_oe_file((0..rows).map(row).collect(), &config).unwrap();
    OeFile::open(built.bytes, Some(&built.file_seal)).unwrap()
}

fn all_granules(file: &OeFile) -> Vec<u32> {
    file.footer()
        .granules
        .iter()
        .map(|granule| granule.granule_id)
        .collect()
}

/// `(occurred_at, row ordinal)` of every row of the file, from a plain full scan.
fn naive_rows(file: &OeFile) -> Vec<(i64, u64)> {
    let mut rows = Vec::new();
    file.scan_projected(&[column_ids::OCCURRED_AT], &all_granules(file), |batch| {
        let first = file
            .footer()
            .granules
            .iter()
            .find(|g| g.granule_id == batch.granule_id)
            .map(|g| g.first_row_ordinal)
            .unwrap_or(0);
        rows.extend(
            occurred(&batch)
                .into_iter()
                .enumerate()
                .map(|(r, at)| (at, first + r as u64)),
        );
        Ok(())
    })
    .unwrap();
    rows
}

fn occurred(batch: &ScanBatch) -> Vec<i64> {
    match batch.columns.first() {
        Some(ScanColumn::Materialized(ColumnRead {
            data: ColumnData::I64(values),
            ..
        })) => values.clone(),
        _ => panic!("occurred_at is i64"),
    }
}

fn even(batch: &ScanBatch, row: u32) -> bool {
    occurred(batch)[row as usize] % 2 == 0
}

/// `(occurred_at, row ordinal)` of every row a selective scan returned.
fn run(file: &OeFile, request: &ScanRequest<'_>, filter: bool) -> (Vec<(i64, u64)>, SelectiveScanStats) {
    let mut out = Vec::new();
    let stats = file
        .scan_selective(
            &[column_ids::OCCURRED_AT],
            &all_granules(file),
            request,
            |batch, row| !filter || even(batch, row),
            |selected| {
                let values = occurred(&selected.batch);
                for row in selected.rows.iter() {
                    out.push((values[row as usize], selected.first_row_ordinal + u64::from(row)));
                }
                Ok(())
            },
        )
        .unwrap();
    (out, stats)
}

fn deletions() -> DeletionVector {
    let mut deletions = DeletionVector::new();
    for row in (0..400).step_by(7) {
        deletions.mark_deleted(row);
    }
    deletions
}

#[test]
fn stored_limit_stops_early_and_matches_naive() {
    let file = file(400, 32);
    let deletions = deletions();
    let naive: Vec<(i64, u64)> = naive_rows(&file)
        .into_iter()
        .filter(|&(at, row)| at % 2 == 0 && !deletions.is_deleted(row))
        .take(10)
        .collect();
    let request = ScanRequest {
        deletions: Some(&deletions),
        limit: Some(10),
        ..ScanRequest::default()
    };
    let (rows, stats) = run(&file, &request, true);
    assert_eq!(rows, naive);
    assert_eq!(stats.rows_returned, 10);
    assert!(stats.granules_read.len() < all_granules(&file).len());
}

#[test]
fn time_ordered_top_n_matches_naive_and_prunes_granules() {
    let file = file(400, 32);
    let deletions = deletions();
    for (order, n) in [
        (ScanOrder::TimeDescending, 5),
        (ScanOrder::TimeDescending, 40),
        (ScanOrder::TimeAscending, 7),
        (ScanOrder::TimeAscending, 1000),
    ] {
        let mut naive: Vec<(i64, u64)> = naive_rows(&file)
            .into_iter()
            .filter(|&(at, row)| at % 2 == 0 && !deletions.is_deleted(row))
            .collect();
        if order == ScanOrder::TimeDescending {
            naive.sort_by(|a, b| b.cmp(a));
        } else {
            naive.sort();
        }
        naive.truncate(n);
        naive.sort_by_key(|&(_, row)| row);
        let bound = TopNBound::new(order);
        let request = ScanRequest {
            bound: Some(&bound),
            deletions: Some(&deletions),
            limit: Some(n),
            order,
            ..ScanRequest::default()
        };
        let (mut rows, stats) = run(&file, &request, true);
        rows.sort_by_key(|&(_, row)| row);
        assert_eq!(rows, naive, "{order:?} {n}");
        if n == 5 {
            assert!(stats.pruned_by_bound > 0, "{stats:?}");
            assert!(bound.version() > 0);
        }
    }
}

#[test]
fn shared_bound_is_rechecked_before_each_granule() {
    let file = file(400, 32);
    let bound = TopNBound::new(ScanOrder::TimeDescending);
    // Another scan already holds its N rows at or above this time: only granules reaching it may be read.
    let cut = naive_rows(&file).iter().map(|&(at, _)| at).max().unwrap() - 10;
    assert!(bound.tighten(cut));
    assert!(!bound.tighten(cut - 1));
    let request = ScanRequest {
        bound: Some(&bound),
        limit: Some(3),
        order: ScanOrder::TimeDescending,
        ..ScanRequest::default()
    };
    let (_, stats) = run(&file, &request, false);
    for granule in &file.footer().granules {
        let read = stats.granules_read.contains(&granule.granule_id);
        assert_eq!(read, granule.max_occurred_at_physical >= cut, "{granule:?}");
    }
}

#[test]
fn selection_plans_point_reads_and_matches_naive() {
    let file = file(400, 32);
    let wanted: Vec<u64> = vec![3, 4, 5, 6, 40, 41, 200, 399, 1000];
    let selection = RoaringRangeBitmap::from_rows(wanted.iter().copied());
    let plan = plan_row_selection(&file.footer().granules, &selection);
    assert_eq!(plan.first().map(|s| s.rows.clone()), Some(RowSelection::Range(3..7)));
    let touched: Vec<u32> = plan.iter().map(|s| s.granule_id).collect();
    let deletions = deletions();
    let naive: Vec<(i64, u64)> = naive_rows(&file)
        .into_iter()
        .filter(|&(_, row)| wanted.contains(&row) && !deletions.is_deleted(row))
        .collect();
    let request = ScanRequest {
        deletions: Some(&deletions),
        selection: Some(&selection),
        ..ScanRequest::default()
    };
    let (rows, stats) = run(&file, &request, false);
    assert_eq!(rows, naive);
    // Row 399 is deleted, so its granule is planned but never read.
    let live: Vec<u32> = touched
        .iter()
        .copied()
        .filter(|&g| {
            plan.iter().any(|s| {
                s.granule_id == g
                    && s.rows.iter().any(|r| {
                        !deletions.is_deleted(file.footer().granules[g as usize].first_row_ordinal + u64::from(r))
                    })
            })
        })
        .collect();
    assert!(live.len() < touched.len());
    assert_eq!(stats.granules_read, live);
    for selected in &plan {
        let pages = file
            .pages_for_rows(column_ids::OCCURRED_AT, selected.granule_id, &selected.rows)
            .unwrap();
        assert!(!pages.is_empty());
    }
}

#[test]
fn contiguous_masks_become_slices() {
    assert_eq!(
        RowSelection::from_mask(&[false, true, true, true, false]),
        RowSelection::Range(1..4)
    );
    assert_eq!(RowSelection::from_mask(&[false, false]), RowSelection::Range(0..0));
    assert_eq!(
        RowSelection::from_mask(&[true, false, true]),
        RowSelection::Rows(vec![0, 2])
    );
    assert_eq!(RowSelection::from_sorted_rows(vec![4, 5, 6]), RowSelection::Range(4..7));
    let values: Vec<i64> = (0..10).collect();
    assert_eq!(RowSelection::Range(2..5).select(&values), vec![2, 3, 4]);
    assert_eq!(RowSelection::Rows(vec![1, 9]).select(&values), vec![1, 9]);
    // A time window over time-sorted rows is one run, and slicing it equals masking it.
    let times: Vec<i64> = (0..100).map(|t| t * 5).collect();
    let mask: Vec<bool> = times.iter().map(|&t| (100..=250).contains(&t)).collect();
    let selection = RowSelection::from_mask(&mask);
    assert!(matches!(selection, RowSelection::Range(_)));
    let masked: Vec<i64> = times.iter().zip(&mask).filter(|(_, k)| **k).map(|(t, _)| *t).collect();
    assert_eq!(selection.select(&times), masked);
}

#[test]
fn time_order_without_occurred_at_is_refused() {
    let file = file(40, 16);
    let request = ScanRequest {
        order: ScanOrder::TimeAscending,
        ..ScanRequest::default()
    };
    let result = file.scan_selective(
        &[column_ids::EVENT_ID],
        &all_granules(&file),
        &request,
        |_, _| true,
        |_| Ok(()),
    );
    assert!(result.is_err());
}

use crate::indexes::bool_pruning::ExprQuery;
use crate::indexes::pruning::{Filter, PredicateKind, Query, plan_query};

fn clause(column_id: u32, predicate: PredicateKind, lo: i128, hi: i128) -> FilterExpr {
    FilterExpr::Clause(FilterClause {
        column_id,
        predicate,
        value_hi: hi,
        value_lo: lo,
    })
}

/// Exact row test over the two projected columns: occurred_at (i64) first, entity hash (u64) second.
fn clause_holds(batch: &ScanBatch, row: u32, clause: &FilterClause) -> bool {
    let value = match (clause.column_id, &batch.columns[..]) {
        (column_ids::OCCURRED_AT, _) => i128::from(occurred(batch)[row as usize]),
        (
            column_ids::ENTITY_ID_HASH_LOW,
            [
                _,
                ScanColumn::Materialized(ColumnRead {
                    data: ColumnData::U64(values),
                    ..
                }),
            ],
        ) => i128::from(values[row as usize]),
        _ => panic!("unprojected clause column"),
    };
    clause.value_lo <= value && value <= clause.value_hi
}

/// `(occurred_at, entity hash, row ordinal)` of every row.
fn naive_full(file: &OeFile) -> Vec<(i64, u64, u64)> {
    (0..400u64)
        .zip(naive_rows(file))
        .map(|(i, (at, row))| {
            assert_eq!(i, row);
            (at, i * 7919, row)
        })
        .collect()
}

fn execute(file: &OeFile, plan: &QueryPlan, inputs: &QueryInputs<'_>) -> (Vec<u64>, SelectiveScanStats) {
    let mut rows = Vec::new();
    let stats = file
        .execute_query(
            plan,
            &[column_ids::OCCURRED_AT, column_ids::ENTITY_ID_HASH_LOW],
            &all_granules(file),
            inputs,
            clause_holds,
            |selected| {
                rows.extend(selected.rows.iter().map(|r| selected.first_row_ordinal + u64::from(r)));
                Ok(())
            },
        )
        .unwrap();
    rows.sort_unstable();
    (rows, stats)
}

#[test]
fn or_query_prunes_granules_and_matches_naive() {
    let file = file(400, 32);
    let times: Vec<i64> = naive_rows(&file).iter().map(|&(at, _)| at).collect();
    let (lo, hi) = (*times.iter().min().unwrap(), *times.iter().max().unwrap());
    let filter = FilterExpr::Or(vec![
        clause(
            column_ids::OCCURRED_AT,
            PredicateKind::Range,
            i128::from(lo),
            i128::from(lo + 40),
        ),
        clause(
            column_ids::OCCURRED_AT,
            PredicateKind::Range,
            i128::from(hi - 40),
            i128::from(hi),
        ),
    ]);
    let matches = |at: i64| (lo..=lo + 40).contains(&at) || (hi - 40..=hi).contains(&at);
    for (limit, order) in [
        (None, ScanOrder::Stored),
        (Some(6), ScanOrder::TimeDescending),
        (Some(9), ScanOrder::Stored),
    ] {
        let plan = plan_query(
            &ExprQuery {
                answerable_from_aggregate: false,
                filter: filter.clone(),
                limit,
                order,
            },
            &GranuleStatistics::default(),
        );
        let (rows, stats) = execute(&file, &plan, &QueryInputs::default());
        let mut naive: Vec<(i64, u64, u64)> = naive_full(&file).into_iter().filter(|r| matches(r.0)).collect();
        if order == ScanOrder::TimeDescending {
            naive.sort_by_key(|r| std::cmp::Reverse((r.0, r.2)));
        }
        naive.truncate(limit.unwrap_or(usize::MAX));
        let mut expected: Vec<u64> = naive.iter().map(|r| r.2).collect();
        expected.sort_unstable();
        assert_eq!(rows, expected, "{limit:?} {order:?}");
        assert!(stats.granules_read.len() < all_granules(&file).len(), "{stats:?}");
    }
}

#[test]
fn entity_and_external_id_hits_route_to_point_reads() {
    let file = file(400, 32);
    let target = 123u64 * 7919;
    let selection = file.selection_for_entity_hash(target);
    let plan = plan_query(
        &ExprQuery {
            answerable_from_aggregate: false,
            filter: clause(
                column_ids::ENTITY_ID_HASH_LOW,
                PredicateKind::Equality,
                i128::from(target),
                i128::from(target),
            ),
            limit: None,
            order: ScanOrder::Stored,
        },
        &GranuleStatistics::default(),
    );
    let inputs = QueryInputs {
        selection: Some(&selection),
        ..QueryInputs::default()
    };
    let (rows, _) = execute(&file, &plan, &inputs);
    assert_eq!(rows, vec![123]);

    let file_id = file.header().file_id;
    let locations: Vec<ExternalIdLocation> = [5u64, 77, 78, 301]
        .iter()
        .map(|&row_ordinal| ExternalIdLocation {
            file_id,
            generation: 1,
            row_ordinal,
        })
        .chain([ExternalIdLocation {
            file_id: file_id ^ 1,
            generation: 1,
            row_ordinal: 6,
        }])
        .collect();
    let selection = file.selection_for_external_ids(&locations);
    let everything = plan_query(
        &ExprQuery::from(Query {
            answerable_from_aggregate: false,
            filter: Filter::default(),
        }),
        &GranuleStatistics::default(),
    );
    let inputs = QueryInputs {
        selection: Some(&selection),
        ..QueryInputs::default()
    };
    let (rows, stats) = execute(&file, &everything, &inputs);
    assert_eq!(rows, vec![5, 77, 78, 301]);
    assert_eq!(stats.granules_read.len(), 3);
}

#[test]
fn aggregate_shortcut_is_not_scanned() {
    let file = file(40, 16);
    let plan = plan_query(
        &ExprQuery {
            answerable_from_aggregate: true,
            filter: FilterExpr::And(Vec::new()),
            limit: Some(1),
            order: ScanOrder::Stored,
        },
        &GranuleStatistics::default(),
    );
    assert_eq!(plan, QueryPlan::AggregateShortcut);
    let result = file.execute_query(&plan, &[], &[], &QueryInputs::default(), |_, _, _| true, |_| Ok(()));
    assert!(result.is_err());
}

/// An IN list over occurred-at, alone and ORed with an entity equality, runs through the one planner and
/// `execute_query`, skips granules that hold no listed value, and returns what a naive scan returns.
#[test]
fn in_list_query_prunes_gaps_and_matches_naive() {
    use crate::indexes::pruning::{InList, MembershipList};
    let file = file(400, 32);
    let times: Vec<i64> = naive_rows(&file).iter().map(|&(at, _)| at).collect();
    let listed: Vec<i64> = vec![times[3], times[200], times[201], times[399]];
    let list = FilterExpr::In(InList::new(
        column_ids::OCCURRED_AT,
        MembershipList::new(listed.iter().map(|&t| i128::from(t)).collect()),
    ));
    let target = 57u64 * 7919;
    let either = FilterExpr::Or(vec![
        list.clone(),
        clause(
            column_ids::ENTITY_ID_HASH_LOW,
            PredicateKind::Equality,
            i128::from(target),
            i128::from(target),
        ),
    ]);
    for (filter, extra) in [(list, None), (either, Some(target))] {
        let plan = plan_query(
            ExprQuery {
                answerable_from_aggregate: false,
                filter,
                limit: None,
                order: ScanOrder::Stored,
            },
            &GranuleStatistics::default(),
        );
        let (rows, stats) = execute(&file, &plan, &QueryInputs::default());
        let expected: Vec<u64> = naive_full(&file)
            .into_iter()
            .filter(|r| listed.contains(&r.0) || Some(r.1) == extra)
            .map(|r| r.2)
            .collect();
        assert_eq!(rows, expected);
        assert!(stats.granules_read.len() < all_granules(&file).len(), "{stats:?}");
        let holding: std::collections::BTreeSet<u32> = file
            .footer
            .granules
            .iter()
            .filter(|g| {
                (g.first_row_ordinal..g.first_row_ordinal + u64::from(g.row_count)).any(|row| expected.contains(&row))
            })
            .map(|g| g.granule_id)
            .collect();
        assert!(holding.iter().all(|g| stats.granules_read.contains(g)), "{stats:?}");
    }
    // The list's envelope keeps granules the gap check drops.
    let lo = i128::from(*listed.iter().min().unwrap());
    let hi = i128::from(*listed.iter().max().unwrap());
    let read = |filter| {
        let plan = plan_query(
            ExprQuery {
                answerable_from_aggregate: false,
                filter,
                limit: None,
                order: ScanOrder::Stored,
            },
            &GranuleStatistics::default(),
        );
        execute(&file, &plan, &QueryInputs::default()).1.granules_read.len()
    };
    let envelope = read(clause(column_ids::OCCURRED_AT, PredicateKind::Membership, lo, hi));
    let gaps = read(FilterExpr::In(InList::new(
        column_ids::OCCURRED_AT,
        MembershipList::new(listed.iter().map(|&t| i128::from(t)).collect()),
    )));
    assert!(gaps < envelope, "{gaps} vs {envelope}");
}
