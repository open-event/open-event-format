use oe::events::families::{ColumnFamily, Tier};

#[test]
fn anomaly_score_arrives_days_after_seal() {
    assert_eq!(ColumnFamily::RevenueAnomalyColumns.tier(), Tier::B);
    assert!(!ColumnFamily::RevenueAnomalyColumns.lives_in_base_file());
    assert_eq!(ColumnFamily::ClusterColumns.tier(), Tier::B);
    assert_eq!(ColumnFamily::DriverAndCauseColumns.tier(), Tier::B);
}

#[test]
fn per_event_embedding_computed_at_seal() {
    assert_eq!(ColumnFamily::EmbeddingColumnsInternal.tier(), Tier::A);
    assert!(ColumnFamily::EmbeddingColumnsInternal.lives_in_base_file());
    let tier_b: Vec<ColumnFamily> = ColumnFamily::ALL
        .into_iter()
        .filter(|family| family.tier() == Tier::B)
        .collect();
    assert_eq!(
        tier_b,
        vec![
            ColumnFamily::ClusterColumns,
            ColumnFamily::RevenueAnomalyColumns,
            ColumnFamily::DriverAndCauseColumns,
        ]
    );
}
