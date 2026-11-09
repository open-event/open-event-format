use oe::encoding::{ColumnData, Transform, encode_block};

#[test]
fn reject_lz4_only_timestamp_storage() {
    let nanos: Vec<i64> = (0..8192).map(|i| 1_700_000_000_000_000_000 + i * 1_000).collect();
    let encoded = encode_block(&ColumnData::I64(nanos), false);
    let transform = encoded.pipeline.transform().unwrap();
    assert!(
        matches!(
            transform,
            Transform::ForBitpack | Transform::DeltaBitpack | Transform::Rle
        ),
        "timestamps must use an adaptive transform, got {transform:?}"
    );
}
