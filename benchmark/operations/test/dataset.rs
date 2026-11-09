use super::*;

fn spec() -> BenchmarkSpec {
    BenchmarkSpec {
        iterations: 1,
        mutation_percent: 10,
        range_percent: 10,
        row_count: 200,
        rows_per_group: 32,
        seed: 7,
    }
}

#[test]
fn the_same_spec_generates_the_same_rows() {
    assert_eq!(generate(&spec()).rows, generate(&spec()).rows);
}

#[test]
fn a_row_does_not_depend_on_how_many_rows_surround_it() {
    let small = generate(&BenchmarkSpec {
        row_count: 100,
        ..spec()
    });
    let large = generate(&spec());
    assert_eq!(small.rows, large.rows[..100]);
}

#[test]
fn the_point_lookup_target_belongs_to_exactly_one_row() {
    let dataset = generate(&spec());
    let matches = dataset
        .rows
        .iter()
        .filter(|row| row.entity_id == dataset.workload.entity_id)
        .count();
    assert_eq!(matches, 1);
}

#[test]
fn the_time_window_keeps_some_rows_and_drops_others() {
    let dataset = generate(&spec());
    let inside = dataset
        .rows
        .iter()
        .filter(|row| {
            (dataset.workload.occurred_at_lo..=dataset.workload.occurred_at_hi).contains(&row.occurred_at_nanos)
        })
        .count();
    assert!(inside > 0 && inside < dataset.rows.len(), "kept {inside} of 200 rows");
}

#[test]
fn an_amendment_changes_exactly_the_rows_it_names() {
    let dataset = generate(&spec());
    let ordinals = &dataset.workload.diff_ordinals;
    let after = amended(&dataset.rows, ordinals);
    let changed: Vec<usize> = dataset
        .rows
        .iter()
        .zip(after.iter())
        .enumerate()
        .filter(|(_, (before, after))| before != after)
        .map(|(index, _)| index)
        .collect();
    assert_eq!(changed, ordinals.iter().map(|n| *n as usize).collect::<Vec<_>>());
}

#[test]
fn a_delete_keeps_every_row_it_was_not_asked_to_drop() {
    let dataset = generate(&spec());
    let ordinals = &dataset.workload.mutated_ordinals;
    let kept = without(&dataset.rows, ordinals);
    assert_eq!(kept.len(), dataset.rows.len() - ordinals.len());
    assert!(kept.iter().all(|row| !ordinals.contains(&(row.sequence - 1))));
}

#[test]
fn identity_hashes_tell_apart_values_that_share_their_whole_words() {
    let values = [
        "",
        "ab",
        "ab\0",
        "\0ab",
        "ent-0123456789abcde",
        "ent-0123456789abcdef",
        "ent-0123456789abcdee",
        "ent-0123456789abcdef\0",
    ];
    let hashes: std::collections::BTreeSet<u64> = values.iter().map(|value| identity_hash(value)).collect();
    assert_eq!(hashes.len(), values.len());
}

#[test]
fn the_free_text_needle_is_planted_in_some_rows_but_not_all() {
    let dataset = generate(&spec());
    let carrying = dataset
        .rows
        .iter()
        .filter(|row| row.note.contains(&dataset.workload.needle))
        .count();
    assert!(
        carrying > 0 && carrying < dataset.rows.len(),
        "{carrying} rows carry it"
    );
}
