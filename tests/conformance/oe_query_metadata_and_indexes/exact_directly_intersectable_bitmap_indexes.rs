use oe::indexes::Exactness;
use oe::indexes::bitmap::*;

#[test]
fn non_intersectable_bitmap_rejected() {
    let not_intersectable = BitmapBlockInfo {
        checksum_verified: true,
        directly_intersectable: false,
        feature_declared: true,
    };
    assert!(!can_use_bitmap(&not_intersectable));

    assert!(!can_use_bitmap(&BitmapBlockInfo {
        checksum_verified: true,
        directly_intersectable: true,
        feature_declared: false,
    }));
    assert!(!can_use_bitmap(&BitmapBlockInfo {
        checksum_verified: false,
        directly_intersectable: true,
        feature_declared: true,
    }));

    let mut index = BitmapIndex::new();
    index.insert(100, [0, 1, 2, 3, 4]);
    index.insert(200, [2, 3, 4, 5, 6]);

    let active = index.bitmap(100).unwrap();
    let us = index.bitmap(200).unwrap();
    let both = active.intersect(us);
    let rows: Vec<u64> = both.ranges().iter().flat_map(|r| r.start..r.end).collect();
    assert_eq!(rows, vec![2, 3, 4]);
    assert_eq!(index.intersect_values(&[100, 200]), both);
    assert_eq!(RoaringRangeBitmap::exactness(), Exactness::Exact);

    let block = active.encode();
    let checksum = bitmap_block_checksum(&block);
    let info = verify_bitmap_block(&block, &checksum, true, true);
    assert!(can_use_bitmap(&info));
}
