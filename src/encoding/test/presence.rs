use super::*;

fn count_bits_one_at_a_time(bitmap: &[u8], len: usize) -> usize {
    (0..len)
        .filter(|bit| bitmap.get(bit / 8).is_some_and(|byte| byte & (1 << (bit % 8)) != 0))
        .count()
}

fn every_third_row(rows: usize) -> Vec<u8> {
    let mut bitmap = vec![0u8; rows.div_ceil(8)];
    for row in (0..rows).step_by(3) {
        bitmap[row / 8] |= 1 << (row % 8);
    }
    bitmap
}

#[test]
fn counting_by_word_matches_counting_bit_by_bit() {
    let rows = 300;
    let bitmap = every_third_row(rows);
    for len in 0..=rows {
        assert_eq!(
            count_set_bits(&bitmap, len),
            count_bits_one_at_a_time(&bitmap, len),
            "len {len}"
        );
    }
}

#[test]
fn bits_past_the_length_never_count() {
    let bitmap = vec![0xFFu8; 3];
    for len in 0..=24 {
        assert_eq!(count_set_bits(&bitmap, len), len, "len {len}");
    }
}

#[test]
fn a_short_bitmap_counts_its_missing_bits_as_unset() {
    let bitmap = vec![0xFFu8; 2];
    assert_eq!(count_set_bits(&bitmap, 1000), 16);
    assert_eq!(count_set_bits(&[], 64), 0);
    assert_eq!(count_set_bits(&bitmap, 0), 0);
}

#[test]
fn present_before_counts_the_prefix() {
    let rows = 200;
    let bitmap = every_third_row(rows);
    for row in 0..rows {
        assert_eq!(
            count_present_before(&bitmap, row),
            count_bits_one_at_a_time(&bitmap, row),
            "row {row}"
        );
    }
    let all = count_bits_one_at_a_time(&bitmap, bitmap.len() * 8);
    assert_eq!(count_present_before(&bitmap, rows * 2), all, "row past the bitmap");
}

fn bitmap_shapes(len: usize) -> Vec<Vec<u8>> {
    let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ len as u64;
    let random: Vec<u8> = (0..len)
        .map(|_| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 56) as u8
        })
        .collect();
    let mut sparse = vec![0u8; len];
    for index in (0..len).step_by(8) {
        sparse[index] = 0x80;
    }
    let mut clustered = vec![0u8; len];
    for byte in clustered.iter_mut().skip(6).take(4) {
        *byte = 0xFF;
    }
    vec![
        vec![0u8; len],
        vec![0xFFu8; len],
        vec![0x55u8; len],
        vec![0xAAu8; len],
        sparse,
        clustered,
        random,
    ]
}

#[test]
fn presence_rank_matches_a_bit_by_bit_count_at_every_row() {
    for len in 0..=17 {
        for bitmap in bitmap_shapes(len) {
            let rank = PresenceRank::new(&bitmap);
            assert_eq!(rank.present_before_word.len(), len.div_ceil(8), "len {len}");
            for row in 0..len * 8 + 70 {
                let present = bitmap.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0);
                let expected = present.then(|| count_bits_one_at_a_time(&bitmap, row));
                assert_eq!(
                    rank.position(&bitmap, row),
                    expected,
                    "len {len} row {row} bitmap {bitmap:?}"
                );
                assert_eq!(
                    present_position(&bitmap, row),
                    expected,
                    "len {len} row {row} bitmap {bitmap:?}"
                );
            }
        }
    }
}

#[test]
fn block_count_matches_counting_bit_by_bit_for_every_shape_and_length() {
    for bytes in 0..=100 {
        for bitmap in bitmap_shapes(bytes) {
            for len in 0..=bytes * 8 + 20 {
                assert_eq!(
                    count_set_bits(&bitmap, len),
                    count_bits_one_at_a_time(&bitmap, len),
                    "bytes {bytes} len {len} bitmap {bitmap:?}"
                );
            }
        }
    }
}
