use super::*;
use crate::dataset::generate;
use tempfile::tempdir;

fn spec() -> BenchmarkSpec {
    BenchmarkSpec {
        iterations: 1,
        mutation_percent: 5,
        range_percent: 10,
        row_count: 500,
        rows_per_group: 64,
        seed: 13,
    }
}

#[test]
fn the_projected_positions_name_the_columns_they_claim_to() {
    let schema = schema();
    let named = |index: usize| schema.fields().get(index).map(|field| field.name().as_str());

    assert_eq!(named(column::OCCURRED_AT), Some("occurred_at"));
    assert_eq!(named(column::ENTITY_ID), Some("entity_id"));
    assert_eq!(named(column::ENTITY_ID_HASH_LOW), Some("entity_id_hash_low"));
    assert_eq!(named(column::EVENT_TYPE), Some("event_type"));
    assert_eq!(named(column::NOTE), Some(FIELD_NOTE));
    assert_eq!(named(column::CLIENT), Some(FIELD_CLIENT));
}

#[test]
fn row_groups_hold_exactly_the_rows_the_spec_asked_for() {
    let dir = tempdir().unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = ParquetFormat::prepare(&dataset, &spec, dir.path()).unwrap();
    let metadata = ParquetFormat::metadata(&format.base).unwrap();
    let groups = metadata.metadata().row_groups();

    assert!(groups.len() > 1, "one row group would prune nothing");
    for group in groups.iter().take(groups.len() - 1) {
        assert_eq!(group.num_rows() as usize, spec.rows_per_group);
    }
}

#[test]
fn a_time_window_rules_out_most_row_groups() {
    let dir = tempdir().unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = ParquetFormat::prepare(&dataset, &spec, dir.path()).unwrap();
    let metadata = ParquetFormat::metadata(&format.base).unwrap();
    let total = metadata.metadata().num_row_groups();
    let kept = (0..total)
        .filter(|group| {
            overlaps_i64(
                metadata.metadata(),
                *group,
                column::OCCURRED_AT,
                dataset.workload.occurred_at_lo,
                dataset.workload.occurred_at_hi,
            )
        })
        .count();

    assert!(kept > 0 && kept < total, "kept {kept} of {total} row groups");
}

#[test]
fn a_delete_rewrites_the_file_without_the_dropped_rows() {
    let dir = tempdir().unwrap();
    let scratch = dir.path().join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = ParquetFormat::prepare(&dataset, &spec, dir.path()).unwrap();

    let measured = format.run(Operation::DeleteRows, &scratch).unwrap();

    assert_eq!(measured.outcome.matched_rows, spec.mutated_row_count() as u64);
    let rewritten = std::fs::metadata(scratch.join("deleted.parquet")).unwrap().len();
    assert!(rewritten > 0 && rewritten < format.file_bytes());
}
