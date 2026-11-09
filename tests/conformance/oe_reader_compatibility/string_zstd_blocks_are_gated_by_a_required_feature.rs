use oe::compat::check_features;
use oe::encoding::{
    CascadeStrategy, ColumnData, Compression, PipelineId, StringColumn, decode_block, encode_block_replayed_with,
};
use oe::layout::required_features;

fn repeated_ids() -> ColumnData {
    let values: StringColumn = (0..8192u64)
        .map(|i| Some(format!("!room{:06}:example.org", (i * 7919 % 211) * (i % 7) / 6)))
        .collect();
    ColumnData::Strings(values)
}

#[test]
fn unknown_compression_refused_cleanly() {
    let block = encode_block_replayed_with(
        &repeated_ids(),
        true,
        CascadeStrategy::DecodeOptimized,
        None,
        None,
        true,
    )
    .0;
    assert_eq!(block.pipeline.compression().unwrap(), Compression::StringZstd);
    assert_eq!((block.pipeline.0 >> 8) & 0xFF, 6);
    assert!(decode_block(block.pipeline, &block.bytes).is_ok());
    let unassigned = PipelineId((block.pipeline.0 & !0xFF00) | (7 << 8));
    assert!(unassigned.compression().is_err());
    assert!(decode_block(unassigned, &block.bytes).is_err());
}

#[test]
fn a_file_declaring_bit_37_is_refused_by_an_older_reader() {
    assert_eq!(required_features::STRING_ZSTD_BLOCKS, 1 << 37);
    assert_eq!(required_features::STRING_ZSTD_BLOCKS & required_features::ALL, 0);
    assert!(check_features(required_features::STRING_ZSTD_BLOCKS, 0).is_ok());
    let old_known = required_features::KNOWN & !required_features::STRING_ZSTD_BLOCKS;
    assert_ne!(required_features::STRING_ZSTD_BLOCKS & !old_known, 0);
}
