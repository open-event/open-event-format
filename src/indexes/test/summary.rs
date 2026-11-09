use super::*;
use crate::layout::footer::{ColumnDescriptor, ColumnKind, ExactCounts, FileDictionaries, Footer, GranuleEntry};

fn granule(id: u32, first_seq: u64, last_seq: u64, min_occ: i64, max_occ: i64) -> GranuleEntry {
    GranuleEntry {
        compressed_bytes_estimate: 0,
        first_epoch: 1,
        first_row_ordinal: 0,
        first_sequence: first_seq,
        granule_id: id,
        last_epoch: 1,
        last_sequence: last_seq,
        max_ingested_at_physical: max_occ,
        max_occurred_at_physical: max_occ,
        min_ingested_at_physical: min_occ,
        min_occurred_at_physical: min_occ,
        row_count: 4,
        stripe_id: 0,
    }
}

fn footer_with(granules: Vec<GranuleEntry>, row_count: u64) -> Footer {
    Footer {
        audit: None,
        clustering: Vec::new(),
        columns: vec![ColumnDescriptor {
            column_id: 0,
            internal_only: false,
            kind: ColumnKind::U64,
            name: "c".to_owned(),
            nullable: false,
        }],
        dictionaries: FileDictionaries::default(),
        embedding_row_offsets: Vec::new(),
        entity_hash_filters: Vec::new(),
        entity_summary: None,
        escape_hatches: Vec::new(),
        exact_counts: ExactCounts {
            row_count,
            ..ExactCounts::default()
        },
        external_ids: Vec::new(),
        format_version: (1, 0),
        freetext: Vec::new(),
        freetext_row_offsets: Vec::new(),
        granules,
        graph: None,
        inbox_recipients: Vec::new(),
        integrity_gaps: Vec::new(),
        io_alignment_bytes: 0,
        marks: Vec::new(),
        marks_directory: Vec::new(),
        marks_page_offsets: Vec::new(),
        marks_pages: Vec::new(),
        optional_feature_flags: 0,
        page_directory: Vec::new(),
        page_minmax: Vec::new(),
        page_stats: Vec::new(),
        payload_granules: Vec::new(),
        presence: Vec::new(),
        profile: crate::layout::Profile::Event,
        reference_filters: None,
        required_feature_flags: 0,
        schema_fingerprint: [7u8; 32],
        shared_dictionaries: Vec::new(),
        packed_shared_dictionaries: Vec::new(),
        shredded: Vec::new(),
        snapshot_keys: None,
        sparse_keys: Vec::new(),
        state_roots: Vec::new(),
        stripe_checksums: Vec::new(),
        stripe_proofs: Vec::new(),
        stripe_ndv: Vec::new(),
        stripes: Vec::new(),
        text_token_indexes: Vec::new(),
        text_token_offsets: Vec::new(),
        time_series_rollups: None,
        vector_columns: Vec::new(),
    }
}

#[test]
fn summary_encode_decode_round_trip() {
    let summary = ManifestSummary {
        granule_count: 3,
        has_deletion_vectors: true,
        has_late_events: false,
        max_occurred_at_physical: 500,
        max_sequence: 99,
        min_occurred_at_physical: 100,
        min_sequence: 1,
        row_count: 42,
        schema_fingerprint: [9u8; 32],
    };
    let bytes = summary.encode();
    let decoded = ManifestSummary::decode(&bytes).unwrap();
    assert_eq!(decoded, summary);
}

#[test]
fn summary_decode_rejects_truncated() {
    assert!(ManifestSummary::decode(&[0u8; 10]).is_err());
}

#[test]
fn summary_decode_rejects_inverted_coverage_with_non_zero_counts() {
    let forged = ManifestSummary {
        granule_count: 2,
        has_deletion_vectors: false,
        has_late_events: false,
        max_occurred_at_physical: 100,
        max_sequence: 20,
        min_occurred_at_physical: 500,
        min_sequence: 1,
        row_count: 8,
        schema_fingerprint: [0u8; 32],
    };
    assert!(ManifestSummary::decode(&forged.encode()).is_err());

    let forged_sequence = ManifestSummary {
        max_sequence: 5,
        min_occurred_at_physical: 100,
        min_sequence: 30,
        ..forged
    };
    assert!(ManifestSummary::decode(&forged_sequence.encode()).is_err());
}

#[test]
fn summary_decode_allows_inverted_coverage_when_empty() {
    let empty = summary_from_footer(&footer_with(Vec::new(), 0));
    assert_eq!(empty.granule_count, 0);
    assert_eq!(empty.row_count, 0);
    assert!(empty.min_occurred_at_physical > empty.max_occurred_at_physical);
    assert_eq!(ManifestSummary::decode(&empty.encode()).unwrap(), empty);
}

#[test]
fn summary_from_footer_aggregates_granule_ranges() {
    let footer = footer_with(
        vec![
            granule(0, 1, 10, 100, 200),
            granule(1, 11, 20, 50, 300),
            granule(2, 21, 30, 250, 250),
        ],
        12,
    );
    let summary = summary_from_footer(&footer);
    assert_eq!(summary.granule_count, 3);
    assert_eq!(summary.row_count, 12);
    assert_eq!(summary.min_sequence, 1);
    assert_eq!(summary.max_sequence, 30);
    assert_eq!(summary.min_occurred_at_physical, 50);
    assert_eq!(summary.max_occurred_at_physical, 300);
    assert_eq!(summary.schema_fingerprint, [7u8; 32]);
}

#[test]
fn summary_copies_the_deletion_vector_flag_from_the_footer() {
    let mut footer = footer_with(vec![granule(0, 1, 10, 100, 200)], 4);
    footer.optional_feature_flags = crate::layout::optional_features::OE_NATIVE_DELETION_VECTORS;
    assert!(summary_from_footer(&footer).has_deletion_vectors);

    footer.optional_feature_flags = 0;
    assert!(!summary_from_footer(&footer).has_deletion_vectors);
}

#[test]
fn summary_copies_the_late_events_flag_from_the_footer() {
    let mut footer = footer_with(vec![granule(0, 1, 10, 100, 200)], 4);
    footer.optional_feature_flags = crate::layout::optional_features::OE_LATE_EVENTS;
    assert!(summary_from_footer(&footer).has_late_events);

    footer.optional_feature_flags = 0;
    assert!(!summary_from_footer(&footer).has_late_events);
}

#[test]
fn summary_from_empty_footer_prunes_everything() {
    let footer = footer_with(Vec::new(), 0);
    let summary = summary_from_footer(&footer);
    assert!(can_prune_file(&summary, &FilePredicate::default()));
}

#[test]
fn prune_when_predicate_range_is_outside_coverage() {
    let summary = summary_from_footer(&footer_with(vec![granule(0, 10, 20, 1_000, 2_000)], 4));

    assert!(can_prune_file(
        &summary,
        &FilePredicate {
            min_occurred_at_physical: Some(3_000),
            ..FilePredicate::default()
        }
    ));
    assert!(can_prune_file(
        &summary,
        &FilePredicate {
            max_occurred_at_physical: Some(500),
            ..FilePredicate::default()
        }
    ));
    assert!(can_prune_file(
        &summary,
        &FilePredicate {
            min_sequence: Some(100),
            ..FilePredicate::default()
        }
    ));
    assert!(can_prune_file(
        &summary,
        &FilePredicate {
            max_sequence: Some(5),
            ..FilePredicate::default()
        }
    ));
}

#[test]
fn never_prune_when_coverage_overlaps() {
    let summary = summary_from_footer(&footer_with(vec![granule(0, 10, 20, 1_000, 2_000)], 4));

    assert!(!can_prune_file(
        &summary,
        &FilePredicate {
            min_occurred_at_physical: Some(1_500),
            max_occurred_at_physical: Some(2_500),
            ..FilePredicate::default()
        }
    ));
    assert!(!can_prune_file(
        &summary,
        &FilePredicate {
            min_occurred_at_physical: Some(2_000),
            ..FilePredicate::default()
        }
    ));
    assert!(!can_prune_file(
        &summary,
        &FilePredicate {
            max_sequence: Some(10),
            ..FilePredicate::default()
        }
    ));
    assert!(!can_prune_file(&summary, &FilePredicate::default()));
    assert!(can_prune_file(
        &summary,
        &FilePredicate {
            min_occurred_at_physical: Some(1_500),
            min_sequence: Some(1_000),
            ..FilePredicate::default()
        }
    ));
}

#[test]
fn planning_reads_granules_from_footer() {
    let footer = footer_with(vec![granule(3, 1, 5, 1, 2), granule(4, 6, 9, 3, 4)], 8);
    assert!(footer_is_authoritative_for_planning());
    assert_eq!(plan_granules_from_footer(&footer), vec![3, 4]);
}

#[test]
fn summary_decode_rejects_row_counts_that_disagree_with_granule_counts() {
    let base = summary_from_footer(&footer_with(vec![granule(0, 1, 4, 1, 2), granule(1, 5, 8, 3, 4)], 8));
    assert_eq!(ManifestSummary::decode(&base.encode()).unwrap(), base);
    for (granule_count, row_count) in [(0, 8), (2, 0), (3, 2)] {
        let forged = ManifestSummary {
            granule_count,
            row_count,
            ..base
        };
        assert!(
            ManifestSummary::decode(&forged.encode()).is_err(),
            "{granule_count} granules, {row_count} rows"
        );
    }
}

#[test]
fn granule_stats_and_summaries_are_validated_against_the_footer() {
    let footer = footer_with(vec![granule(0, 1, 4, 1, 2), granule(1, 5, 8, 3, 4)], 8);
    validate_granule_row_counts(&footer).unwrap();
    let summary = summary_from_footer(&footer);
    summary.validate_against_footer(&footer).unwrap();

    let overrun = footer_with(footer.granules.clone(), 7);
    assert!(validate_granule_row_counts(&overrun).is_err());
    assert!(summary.validate_against_footer(&overrun).is_err());

    let mut empty_granule = footer.clone();
    empty_granule.granules[1].row_count = 0;
    empty_granule.exact_counts.row_count = 4;
    assert!(validate_granule_row_counts(&empty_granule).is_err());

    let stale = ManifestSummary {
        granule_count: 1,
        row_count: 4,
        ..summary
    };
    assert!(stale.validate_against_footer(&footer).is_err());
    let miscounted = ManifestSummary {
        row_count: 9,
        ..summary
    };
    assert!(miscounted.validate_against_footer(&footer).is_err());
}
