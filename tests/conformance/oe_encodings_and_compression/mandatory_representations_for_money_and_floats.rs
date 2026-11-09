use oe::encoding::{ColumnData, Transform, decode_block, encode_block};

#[test]
fn float_metric_outside_tolerance() {
    let pathological: Vec<f64> = (1..512).map(|i| (i as f64) * 1.0e300).collect();
    let encoded = encode_block(&ColumnData::F64(pathological.clone()), false);
    assert_ne!(encoded.pipeline.transform().unwrap(), Transform::Alp);
    let decoded = decode_block(encoded.pipeline, &encoded.bytes).unwrap();
    assert_eq!(decoded, ColumnData::F64(pathological));
    let friendly: Vec<f64> = (0..512).map(|i| (i as f64) * 0.5).collect();
    let encoded = encode_block(&ColumnData::F64(friendly), false);
    assert_eq!(encoded.pipeline.transform().unwrap(), Transform::Alp);
}
