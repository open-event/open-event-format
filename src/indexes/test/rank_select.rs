use super::super::bitmap::RoaringRangeBitmap;
use super::super::rank_select::{BitRankDirectory, RankSelect};

fn bitmap(rows: impl IntoIterator<Item = u64>) -> RoaringRangeBitmap {
    RoaringRangeBitmap::from_rows(rows)
}

fn naive_rank(rows: &[u64], position: u64) -> u64 {
    rows.iter().filter(|&&r| r < position).count() as u64
}

fn naive_select(rows: &[u64], k: u64) -> Option<u64> {
    rows.get(k as usize).copied()
}

#[test]
fn rank_matches_naive_for_sparse_bitmap() {
    let rows: Vec<u64> = vec![0, 2, 5, 10, 11, 20];
    let bm = bitmap(rows.iter().copied());
    let rs = RankSelect::from_bitmap(&bm);
    for pos in 0u64..=22 {
        assert_eq!(rs.rank(pos), naive_rank(&rows, pos), "rank({pos})");
    }
}

#[test]
fn rank_matches_naive_for_dense_bitmap() {
    let rows: Vec<u64> = (0u64..100).collect();
    let bm = bitmap(rows.iter().copied());
    let rs = RankSelect::from_bitmap(&bm);
    for pos in 0u64..=101 {
        assert_eq!(rs.rank(pos), naive_rank(&rows, pos), "rank({pos})");
    }
}

#[test]
fn rank_at_zero_is_always_zero() {
    let bm = bitmap([5, 10, 15]);
    let rs = RankSelect::from_bitmap(&bm);
    assert_eq!(rs.rank(0), 0);
}

#[test]
fn rank_on_empty_bitmap_is_zero() {
    let rs = RankSelect::from_bitmap(&RoaringRangeBitmap::default());
    assert_eq!(rs.rank(0), 0);
    assert_eq!(rs.rank(100), 0);
}

#[test]
fn select_matches_naive_for_sparse_bitmap() {
    let rows: Vec<u64> = vec![1, 3, 7, 8, 100, 200];
    let bm = bitmap(rows.iter().copied());
    let rs = RankSelect::from_bitmap(&bm);
    for k in 0u64..=(rows.len() as u64 + 1) {
        assert_eq!(rs.select(k), naive_select(&rows, k), "select({k})");
    }
}

#[test]
fn select_matches_naive_for_dense_bitmap() {
    let rows: Vec<u64> = (50u64..60).collect();
    let bm = bitmap(rows.iter().copied());
    let rs = RankSelect::from_bitmap(&bm);
    for k in 0u64..=10 {
        assert_eq!(rs.select(k), naive_select(&rows, k), "select({k})");
    }
}

#[test]
fn select_returns_none_out_of_range() {
    let bm = bitmap([1, 2, 3]);
    let rs = RankSelect::from_bitmap(&bm);
    assert_eq!(rs.total(), 3);
    assert!(rs.select(3).is_none());
    assert!(rs.select(100).is_none());
}

#[test]
fn select_on_empty_bitmap_is_none() {
    let rs = RankSelect::from_bitmap(&RoaringRangeBitmap::default());
    assert!(rs.select(0).is_none());
}

#[test]
fn rank_and_select_are_inverses() {
    let rows: Vec<u64> = vec![0, 1, 5, 6, 7, 20, 21, 50];
    let bm = bitmap(rows.iter().copied());
    let rs = RankSelect::from_bitmap(&bm);
    for k in 0..rows.len() as u64 {
        let pos = rs.select(k).unwrap();
        assert_eq!(rs.rank(pos), k, "rank(select({k})) should be {k}");
        assert_eq!(rs.rank(pos + 1), k + 1, "rank(select({k}) + 1) should be {}", k + 1);
    }
}

fn raw_path(bits: &[u8]) -> RankSelect {
    let rows = bits.iter().enumerate().flat_map(|(byte_index, &byte)| {
        (0..8u64).filter_map(move |bit| (byte & (1 << bit) != 0).then_some(byte_index as u64 * 8 + bit))
    });
    RankSelect::from_bitmap(&bitmap(rows))
}

fn assert_equivalent_to_raw_path(bits: &[u8]) {
    let compressed = RankSelect::from_packed_bits(bits);
    let raw = raw_path(bits);
    assert_eq!(compressed.total(), raw.total(), "total over {bits:02x?}");
    for position in 0..=(bits.len() as u64 * 8 + 2) {
        assert_eq!(
            compressed.rank(position),
            raw.rank(position),
            "rank({position}) over {bits:02x?}"
        );
    }
    for k in 0..=(raw.total() + 2) {
        assert_eq!(compressed.select(k), raw.select(k), "select({k}) over {bits:02x?}");
    }
}

#[test]
fn from_packed_bits_matches_raw_path_on_edge_patterns() {
    assert_equivalent_to_raw_path(&[]);
    assert_equivalent_to_raw_path(&[0x00]);
    assert_equivalent_to_raw_path(&[0xFF]);
    assert_equivalent_to_raw_path(&[0x00, 0x00, 0x00]);
    assert_equivalent_to_raw_path(&[0xFF, 0xFF, 0xFF]);
    assert_equivalent_to_raw_path(&[0x01]);
    assert_equivalent_to_raw_path(&[0x80]);
    assert_equivalent_to_raw_path(&[0xAA, 0x55]);
    assert_equivalent_to_raw_path(&[0xF0, 0x0F]);
    assert_equivalent_to_raw_path(&[0xFF, 0x01, 0x80, 0xFF]);
    assert_equivalent_to_raw_path(&[0x00, 0xFF, 0x00, 0xFF, 0x00]);
}

#[test]
fn from_packed_bits_matches_raw_path_on_generated_patterns() {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for len in [1usize, 2, 7, 8, 9, 32, 129] {
        let bits: Vec<u8> = (0..len).map(|_| next() as u8).collect();
        assert_equivalent_to_raw_path(&bits);
        let clustered: Vec<u8> = (0..len)
            .map(|i| match (i / 4) % 3 {
                0 => 0xFF,
                1 => 0x00,
                _ => next() as u8,
            })
            .collect();
        assert_equivalent_to_raw_path(&clustered);
    }
}

#[test]
fn total_matches_bitmap_count() {
    let rows: Vec<u64> = vec![3, 7, 11, 15];
    let bm = bitmap(rows.iter().copied());
    let rs = RankSelect::from_bitmap(&bm);
    assert_eq!(rs.total(), bm.count());
    assert_eq!(rs.total(), 4);
}

#[test]
fn gather_survivors_via_select() {
    let live = bitmap([10, 20, 30, 40]);
    let rs = RankSelect::from_bitmap(&live);
    let physical: Vec<u64> = (0..4).map(|k| rs.select(k).unwrap()).collect();
    assert_eq!(physical, vec![10, 20, 30, 40]);
    assert_eq!(rs.rank(20), 1);
    assert_eq!(rs.rank(21), 2);
}

#[test]
fn anti_join_via_difference_matches_select_gather() {
    let all = bitmap(0..10u64);
    let deleted = bitmap([2u64, 5, 8]);
    let live = all.difference(&deleted);
    let expected_live: Vec<u64> = (0..10u64).filter(|r| ![2, 5, 8].contains(r)).collect();
    let got_live: Vec<u64> = live.ranges().iter().flat_map(|r| r.start..r.end).collect();
    assert_eq!(got_live, expected_live);

    let rs = RankSelect::from_bitmap(&live);
    assert_eq!(rs.total(), expected_live.len() as u64);
    for (k, &physical) in expected_live.iter().enumerate() {
        assert_eq!(rs.select(k as u64), Some(physical));
        assert_eq!(rs.rank(physical), k as u64);
    }
}

#[test]
fn from_packed_bits_matches_from_bitmap() {
    let bits = [0b1010_0110u8, 0xFF, 0x00, 0x81];
    let rows: Vec<u64> = (0..32u64)
        .filter(|i| bits[(i / 8) as usize] & (1 << (i % 8)) != 0)
        .collect();
    let packed = RankSelect::from_packed_bits(&bits);
    let oracle = RankSelect::from_bitmap(&bitmap(rows.iter().copied()));
    assert_eq!(packed.total(), oracle.total());
    for p in 0..=33 {
        assert_eq!(packed.rank(p), oracle.rank(p));
        assert_eq!(packed.select(p), oracle.select(p));
    }
}

#[test]
fn bit_rank_directory_matches_naive_popcount() {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let bits: Vec<u8> = (0..1300)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let is_set = |row: usize| bits.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0);
    for len in [0usize, 1, 7, 511, 512, 513, 4096, 10_399, 10_400, 20_000] {
        let directory = BitRankDirectory::new(&bits, len);
        let covered = len.min(bits.len() * 8);
        assert_eq!(directory.len(), covered);
        for position in (0..=covered + 3).step_by(37).chain([covered, 512, 1024]) {
            let expected = (0..position.min(covered)).filter(|&row| is_set(row)).count() as u64;
            assert_eq!(directory.rank(position), expected, "len {len} position {position}");
        }
        assert_eq!(
            directory.count_range(100, 700),
            (100..700.min(covered)).filter(|&row| is_set(row)).count() as u64
        );
        assert_eq!(directory.count_range(700, 100), 0);
    }
}
