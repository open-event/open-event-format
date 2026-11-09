use oe::compat::availability::PINNED_AVAILABILITY_WINDOWS;
use oe::compat::validate_pipeline_window;
use oe::encoding::{CascadeStrategy, Transform, decode_block, encode_occurred_residual, resolve_occurred_residual};

#[test]
fn occurred_relative_transform_refused_without_its_window() {
    let occurred: Vec<i64> = (0..2_000i64).map(|i| 1_000_000 + i * 977 + (i * i) % 131).collect();
    let ingested: Vec<i64> = occurred.iter().map(|value| value + 2).collect();
    let block = encode_occurred_residual(&ingested, &occurred, CascadeStrategy::DecodeOptimized).unwrap();
    assert_eq!(block.pipeline.transform().unwrap(), Transform::OccurredResidual);
    assert!(validate_pipeline_window(block.pipeline.0, (1, 0), &PINNED_AVAILABILITY_WINDOWS).is_ok());
    assert!(decode_block(block.pipeline, &block.bytes).is_err());
    let (plain, body) = resolve_occurred_residual(&block.bytes, &occurred).unwrap();
    assert_eq!(
        decode_block(plain, &body).unwrap(),
        oe::encoding::ColumnData::I64(ingested)
    );
    assert!(resolve_occurred_residual(&block.bytes, &occurred[1..]).is_err());
    let older: Vec<_> = PINNED_AVAILABILITY_WINDOWS
        .iter()
        .copied()
        .filter(|window| window.transform_id != 16)
        .collect();
    assert!(validate_pipeline_window(block.pipeline.0, (1, 0), &older).is_err());
}
