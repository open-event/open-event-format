use oe::indexes::learned_position::{LearnedPositionIndex, plan_learned_seek};

#[test]
fn range_seek_via_the_learned_model() {
    let keys: Vec<i64> = (0..200).map(|i| i * 5).collect();
    let column_id = 1u32;
    let index = LearnedPositionIndex::build(column_id, 0, &keys, 4);

    let seek =
        plan_learned_seek(&index, true, column_id).expect("learned seek must be available when proof is present");

    let key_lo = 300i64;
    let true_position = 60u64;

    let window = seek.seek_range_start(key_lo);

    assert!(
        window.search_from <= true_position && true_position <= window.search_to,
        "window [{}, {}] must contain true row {} for key {}",
        window.search_from,
        window.search_to,
        true_position,
        key_lo,
    );

    assert!(
        window.search_to - window.search_from <= 2 * index.error_bound,
        "confirmation window must be bounded by 2 × error_bound rows, not a full scan"
    );

    let key_hi = 400i64;
    let window_hi = seek.seek_range_start(key_hi);
    let true_end = 80u64;
    assert!(
        window_hi.search_from <= true_end && true_end <= window_hi.search_to,
        "end-of-range window must also contain the true boundary"
    );
}

#[test]
fn no_proof_no_learned_seek() {
    let keys: Vec<i64> = (0..100).map(|i| i * 10).collect();
    let column_id = 2u32;
    let index = LearnedPositionIndex::build(column_id, 0, &keys, 5);

    let no_proof = plan_learned_seek(&index, false, column_id);
    assert!(
        no_proof.is_none(),
        "learned seek must not be used when no sortedness proof covers the column"
    );

    let wrong_column = plan_learned_seek(&index, true, column_id + 1);
    assert!(
        wrong_column.is_none(),
        "learned seek must not be used when the proof covers a different column"
    );
}
