use oe::indexes::model::{PredicateShape, RowRange, SkipIndex, choose_skip_index, pushdown_residual_required};
use oe::indexes::{Exactness, Granularity, SkipIndexKind};

#[test]
fn inexact_skip_index_keeps_a_filter_above() {
    let bloom = SkipIndex {
        block_ref: 0,
        column_id: 3,
        exactness: SkipIndexKind::SplitBlockBloomFilter.default_exactness(),
        fpr: Some(0.01),
        granularity: Granularity::Granule,
        index_id: 1,
        kind: SkipIndexKind::SplitBlockBloomFilter,
        projection_id: 0,
        row_range: RowRange {
            first_row_ordinal: 0,
            row_count: 4096,
        },
    };

    assert_eq!(bloom.exactness, Exactness::InexactNoFalseNegative);
    assert!(
        bloom.exactness.requires_residual_filter(),
        "an inexact skip index forces a residual filter"
    );
    assert!(
        pushdown_residual_required(bloom.kind),
        "pushdown with an inexact kind keeps a filter above the scan"
    );

    let minmax_exact = SkipIndexKind::MinMax;
    assert_eq!(minmax_exact.default_exactness(), Exactness::Exact);
    assert!(!pushdown_residual_required(minmax_exact));
}

#[test]
fn point_filter_does_not_answer_a_range_predicate() {
    let point_only = [
        SkipIndexKind::EntityHashFilter,
        SkipIndexKind::AccountHashFilter,
        SkipIndexKind::OpportunityHashFilter,
        SkipIndexKind::BinaryFuseFilter,
        SkipIndexKind::RibbonFilter,
        SkipIndexKind::SplitBlockBloomFilter,
    ];
    assert_eq!(
        choose_skip_index(PredicateShape::RangeBound, &point_only),
        None,
        "a point-membership filter must not answer a range predicate"
    );
    for kind in point_only {
        assert!(
            !kind.answers(PredicateShape::RangeBound),
            "{kind:?} must not answer a range predicate"
        );
    }

    let with_range_filter = [
        SkipIndexKind::EntityHashFilter,
        SkipIndexKind::RangeFilter,
        SkipIndexKind::SplitBlockBloomFilter,
    ];
    assert_eq!(
        choose_skip_index(PredicateShape::RangeBound, &with_range_filter),
        Some(SkipIndexKind::RangeFilter)
    );

    let with_minmax = [SkipIndexKind::BinaryFuseFilter, SkipIndexKind::MinMax];
    assert_eq!(
        choose_skip_index(PredicateShape::RangeBound, &with_minmax),
        Some(SkipIndexKind::MinMax)
    );
}
