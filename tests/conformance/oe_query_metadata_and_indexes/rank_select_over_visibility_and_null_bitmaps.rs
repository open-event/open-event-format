use oe::indexes::bitmap::RoaringRangeBitmap;
use oe::indexes::rank_select::RankSelect;

#[test]
fn gather_survivors_without_expanding_the_bitmap() {
    let deleted: Vec<u64> = vec![1, 4, 7, 11, 15, 18];
    let all = RoaringRangeBitmap::from_rows(0u64..20);
    let del_bm = RoaringRangeBitmap::from_rows(deleted.iter().copied());
    let live = all.difference(&del_bm);

    let expected_live: Vec<u64> = (0u64..20).filter(|r| !deleted.contains(r)).collect();
    let got_live: Vec<u64> = live.ranges().iter().flat_map(|r| r.start..r.end).collect();
    assert_eq!(got_live, expected_live, "live bitmap must equal complement of deleted");

    let rs = RankSelect::from_bitmap(&live);
    assert_eq!(rs.total(), expected_live.len() as u64);

    for (logical, &physical) in expected_live.iter().enumerate() {
        let got = rs.select(logical as u64).expect("logical position must be in range");
        assert_eq!(got, physical, "select({logical}) must return physical row {physical}");
    }

    for (logical, &physical) in expected_live.iter().enumerate() {
        assert_eq!(
            rs.rank(physical),
            logical as u64,
            "rank({physical}) must return logical position {logical}"
        );
    }
}

#[test]
fn anti_join_as_bitmap_operations() {
    let deletion_vector = RoaringRangeBitmap::from_rows([0u64, 5, 10, 15, 20, 25, 30, 35, 40, 45]);
    let all_rows = RoaringRangeBitmap::from_rows(0u64..50);

    let live_bitmap = all_rows.difference(&deletion_vector);

    let deleted_set: std::collections::BTreeSet<u64> =
        deletion_vector.ranges().iter().flat_map(|r| r.start..r.end).collect();
    let expected_live: Vec<u64> = (0u64..50).filter(|r| !deleted_set.contains(r)).collect();

    let bitmap_live: Vec<u64> = live_bitmap.ranges().iter().flat_map(|r| r.start..r.end).collect();

    assert_eq!(
        bitmap_live, expected_live,
        "bitmap AND-NOT must equal row-by-row anti-join"
    );

    let rs = RankSelect::from_bitmap(&live_bitmap);
    assert_eq!(rs.total(), expected_live.len() as u64, "survivor count must match");

    for (k, &physical) in expected_live.iter().enumerate() {
        assert_eq!(
            rs.select(k as u64),
            Some(physical),
            "select({k}) must address physical row {physical}"
        );
    }
}
