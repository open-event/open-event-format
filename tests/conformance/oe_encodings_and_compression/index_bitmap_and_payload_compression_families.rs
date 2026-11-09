use oe::indexes::Exactness;
use oe::indexes::bitmap::{BitmapIndex, RoaringRangeBitmap};

#[test]
fn bitmap_compression() {
    let mut index = BitmapIndex::new();
    index.insert(0, 0..4_000u64);
    index.insert(1, 4_000..9_000u64);
    index.insert(2, 9_000..10_000u64);
    assert_eq!(index.len(), 3);

    let active = index.bitmap(0).expect("value 0 indexed");
    assert_eq!(
        active.ranges().len(),
        1,
        "a dense run compresses to one range, not 4000 bits"
    );
    assert_eq!(active.count(), 4_000);
    let encoded = active.encode();
    assert!(
        encoded.len() < 4_000 / 8,
        "native compression stores runs, far below one bit per row"
    );
    assert_eq!(RoaringRangeBitmap::decode(&encoded).unwrap(), *active);

    assert_eq!(RoaringRangeBitmap::exactness(), Exactness::Exact);
    let window = RoaringRangeBitmap::from_rows(3_500..4_500);
    let both = active.intersect(&window);
    assert_eq!(both.count(), 500);
    assert!(both.contains(3_999) && !both.contains(4_000));
}
