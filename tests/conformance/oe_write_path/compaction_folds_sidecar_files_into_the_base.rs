use oe::events::TimestampValue;
use oe::writer::compaction::{DerivedColumnEntry, DerivedColumnsSidecar, fold_settled_derived_columns};

#[test]
fn settled_derived_columns_folded_in() {
    let sidecar = DerivedColumnsSidecar {
        columns: vec![
            DerivedColumnEntry {
                column_name: "cluster_id".to_owned(),
                settle_horizon: TimestampValue::from_physical_nanos(1_000),
            },
            DerivedColumnEntry {
                column_name: "revenue_anomaly_score".to_owned(),
                settle_horizon: TimestampValue::from_physical_nanos(5_000),
            },
        ],
        row_count: 64,
    };

    let compacted_range_time = TimestampValue::from_physical_nanos(2_000);
    let folded = fold_settled_derived_columns(&sidecar, compacted_range_time);

    assert_eq!(
        folded.embedded_columns.len(),
        1,
        "only the column past its settle horizon is embedded in the new base file"
    );
    assert_eq!(folded.embedded_columns[0].column_name, "cluster_id");

    let tail = folded
        .unsettled_sidecar
        .expect("a sibling is re-emitted for the still-unsettled tail");
    assert_eq!(tail.columns.len(), 1, "the sibling carries only the unsettled column");
    assert_eq!(tail.columns[0].column_name, "revenue_anomaly_score");
    assert_eq!(
        tail.row_count, sidecar.row_count,
        "row alignment is preserved in the re-emitted sibling"
    );
}
