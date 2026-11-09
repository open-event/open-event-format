use oe::compat::availability::PINNED_AVAILABILITY_WINDOWS;
use oe::compat::validate_pipeline_window;
use oe::encoding::{ColumnData, StringColumn, Transform, decode_block, encode_block};

#[test]
fn unknown_transform_refused_cleanly() {
    let values: StringColumn = (0..600usize).map(|i| Some(format!("dev-{:06}", i * 13))).collect();
    let block = encode_block(&ColumnData::Strings(values), false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::FsstPackedString);
    assert!(validate_pipeline_window(block.pipeline.0, (1, 0), &PINNED_AVAILABILITY_WINDOWS).is_ok());
    assert!(decode_block(block.pipeline, &block.bytes).is_ok());
    let older: Vec<_> = PINNED_AVAILABILITY_WINDOWS
        .iter()
        .copied()
        .filter(|window| window.transform_id != 15)
        .collect();
    assert!(validate_pipeline_window(block.pipeline.0, (1, 0), &older).is_err());
}
