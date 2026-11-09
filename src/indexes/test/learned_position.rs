use crate::file::bytes::Writer;
use crate::indexes::learned_position::{LEARNED_POSITION_MAGIC, LearnedPositionIndex, Segment, plan_learned_seek};
use crate::layout::footer::SortDirection;

fn uniform_keys(start: i64, step: i64, count: usize) -> Vec<i64> {
    (0..count as i64).map(|i| start + i * step).collect()
}

#[test]
fn estimate_lands_within_error_bound() {
    let keys = uniform_keys(0, 10, 100);
    let index = LearnedPositionIndex::build(1, 0, &keys, 4);
    let window = index.estimate_position(300);
    assert!(
        window.search_from <= 30 && 30 <= window.search_to,
        "window [{}, {}] must contain true position 30",
        window.search_from,
        window.search_to,
    );
    assert!(
        window.search_to - window.search_from <= 2 * index.error_bound,
        "window width must not exceed 2 * error_bound"
    );
}

#[test]
fn estimate_clamped_to_row_count() {
    let keys = uniform_keys(0, 1, 50);
    let index = LearnedPositionIndex::build(1, 0, &keys, 3);
    let window = index.estimate_position(9999);
    assert!(window.search_to < 50, "search_to must not exceed row_count - 1");
    let window2 = index.estimate_position(-100);
    assert_eq!(window2.search_from, 0);
}

#[test]
fn single_key_column() {
    let keys = vec![42i64];
    let index = LearnedPositionIndex::build(0, 0, &keys, 0);
    let window = index.estimate_position(42);
    assert_eq!(window.approximate_row, 0);
    assert_eq!(window.search_from, 0);
    assert_eq!(window.search_to, 0);
}

#[test]
fn duplicated_key_seek_finds_earliest_row() {
    let keys = vec![5i64; 10];
    let index = LearnedPositionIndex::build(1, 0, &keys, 2);
    let window = index.estimate_position(5);
    assert!(
        window.search_from == 0,
        "window must start at row 0 to cover the earliest occurrence of the duplicated key, got [{}, {}]",
        window.search_from,
        window.search_to,
    );
}

#[test]
fn duplicate_run_straddling_a_segment_break_brackets_the_true_row() {
    let keys = vec![0i64, 0, 0, 1000];
    let index = LearnedPositionIndex::build(1, 0, &keys, 1);
    let window = index.estimate_position(0);
    assert!(
        window.search_from == 0,
        "window [{}, {}] must contain the true boundary row 0 for the duplicated key 0",
        window.search_from,
        window.search_to,
    );
}

#[test]
fn duplicate_run_starting_inside_a_segment_brackets_its_first_row() {
    let keys = vec![0i64, 1, 1, 1, 1];
    let index = LearnedPositionIndex::build(1, 0, &keys, 1);
    let window = index.estimate_position(1);
    assert!(
        window.search_from <= 1 && 1 <= window.search_to,
        "window [{}, {}] must contain row 1, the first row holding key 1",
        window.search_from,
        window.search_to,
    );
}

#[test]
fn a_long_duplicate_run_round_trips_through_encode_and_decode() {
    let index = LearnedPositionIndex::build(1, 0, &[5i64; 10], 2);
    assert_eq!(LearnedPositionIndex::decode(&index.encode()).unwrap(), index);
}

#[test]
fn absent_key_between_fitted_keys_stays_inside_the_window() {
    let keys = vec![0i64, 1, 1_000_000];
    let index = LearnedPositionIndex::build(1, 0, &keys, 1);
    let window = index.estimate_position(2);
    assert!(
        window.search_from <= 2 && 2 <= window.search_to,
        "window [{}, {}] must contain the true boundary row 2 for absent key 2",
        window.search_from,
        window.search_to,
    );
}

#[test]
fn absent_key_in_an_inter_segment_gap_stays_inside_the_window() {
    let keys: Vec<i64> = (0..=10).chain((0..=100).map(|i| 1_000_000 + i)).collect();
    let index = LearnedPositionIndex::build(1, 0, &keys, 1);
    let window = index.estimate_position(500_000);
    assert!(
        window.search_from <= 11 && 11 <= window.search_to,
        "window [{}, {}] must contain the true boundary row 11 for a key in the inter-segment gap",
        window.search_from,
        window.search_to,
    );
}

#[test]
fn plan_returns_none_without_sortedness_proof() {
    let keys = uniform_keys(0, 5, 20);
    let index = LearnedPositionIndex::build(7, 0, &keys, 2);
    assert!(
        plan_learned_seek(&index, false, 7).is_none(),
        "learned seek must not be used without a sortedness proof"
    );
}

#[test]
fn plan_returns_none_for_wrong_column() {
    let keys = uniform_keys(0, 5, 20);
    let index = LearnedPositionIndex::build(7, 0, &keys, 2);
    assert!(
        plan_learned_seek(&index, true, 99).is_none(),
        "learned seek must not be used for a different column"
    );
}

#[test]
fn plan_returns_seek_when_proof_and_column_match() {
    let keys = uniform_keys(0, 5, 20);
    let index = LearnedPositionIndex::build(7, 0, &keys, 2);
    let seek = plan_learned_seek(&index, true, 7);
    assert!(
        seek.is_some(),
        "learned seek must be available with proof + matching column"
    );
    let window = seek.unwrap().seek_range_start(50);
    assert!(
        window.search_from <= 10 && 10 <= window.search_to,
        "window must contain the true position of key 50"
    );
}

#[test]
fn keys_spanning_the_whole_signed_domain_fit_and_look_up_without_overflow() {
    let keys = [i64::MIN, 0, i64::MAX];
    let index = LearnedPositionIndex::build(1, 0, &keys, 2);

    for (position, key) in keys.iter().enumerate() {
        let window = index.estimate_position(*key);
        assert!(
            window.search_from <= position as u64 && position as u64 <= window.search_to,
            "window must contain the true position of {key}"
        );
    }
    let _ = index.estimate_position(i64::MAX);
    let _ = index.estimate_position(i64::MIN);
}

#[test]
fn keys_built_at_given_rows_land_within_the_bound_of_those_rows() {
    let keys: Vec<i64> = (0..200).map(|i| i * i * 7).collect();
    let mut rows = Vec::new();
    let mut row = 0u64;
    for i in 0..200u64 {
        rows.push(row);
        row += 1 + (i % 5) * 40;
    }
    let index = LearnedPositionIndex::build_at_rows(1, 0, &keys, &rows, row + 100, 4);
    for (key, row) in keys.iter().zip(&rows) {
        let window = index.estimate_position(*key);
        assert!(
            window.search_from <= *row && *row <= window.search_to,
            "window [{}, {}] must contain row {row}",
            window.search_from,
            window.search_to,
        );
    }
    assert_eq!(LearnedPositionIndex::decode(&index.encode()).unwrap(), index);
}

fn encode_field_by_field(index: &LearnedPositionIndex) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(LEARNED_POSITION_MAGIC);
    out.put_u32(index.column_id);
    out.put_u32(index.projection_id);
    out.put_u8(match index.direction {
        SortDirection::Ascending => 0,
        SortDirection::Descending => 1,
    });
    out.put_u64(index.error_bound);
    out.put_u64(index.row_count);
    out.put_u32(index.segments.len() as u32);
    for segment in &index.segments {
        out.put_u64(segment.intercept.to_bits());
        out.put_i64(segment.key_lo);
        out.put_u64(segment.slope.to_bits());
    }
    out.into_bytes()
}

#[test]
fn encoding_matches_the_field_by_field_layout_and_its_predicted_length() {
    let cubes: Vec<i64> = (0..300).map(|i| i * i * i).collect();
    let many = LearnedPositionIndex::build(2, 0, &cubes, 0);
    assert!(many.segments.len() > 10);
    let mut descending = LearnedPositionIndex::build(9, 3, &uniform_keys(-50, 3, 40), 2);
    descending.direction = SortDirection::Descending;
    let mut odd_floats = LearnedPositionIndex::build(4, 1, &[7], 0);
    odd_floats.segments.push(Segment {
        intercept: -0.0,
        key_lo: i64::MAX - 1,
        slope: f64::from_bits(0x7FF8_0000_0000_0001),
    });
    odd_floats.segments.push(Segment {
        intercept: f64::INFINITY,
        key_lo: i64::MAX,
        slope: f64::NEG_INFINITY,
    });
    for index in [
        LearnedPositionIndex::build(1, 0, &[], 4),
        LearnedPositionIndex::build(1, 0, &[42], 4),
        many,
        descending,
        odd_floats,
    ] {
        let bytes = index.encode();
        assert_eq!(bytes, encode_field_by_field(&index));
        assert_eq!(index.encoded_len(), bytes.len());

        let mut framed = Writer::new();
        framed.put_slice(b"prefix");
        index.encode_into(&mut framed);
        assert_eq!(framed.bytes().get(..6), Some(&b"prefix"[..]));
        assert_eq!(framed.bytes().get(6..), Some(&bytes[..]));
    }
}
