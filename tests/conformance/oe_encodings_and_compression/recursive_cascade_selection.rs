use oe::encoding::decompressor::{Decompressor, SoftwareDecompressor, active_decompressor};
use oe::encoding::{
    CascadeStrategy, ColumnData, MAX_CASCADE_DEPTH, SideStream, Transform, cascade_depth, cascade_level_fits,
    decode_block, decode_block_range, encode_block, encode_block_with_strategy,
};

fn alp_column_with_exceptions(rows: usize, stride: usize) -> ColumnData {
    ColumnData::F64(
        (0..rows)
            .map(|i| {
                if i % stride == 0 {
                    std::f64::consts::PI * 1e19 * (i + 1) as f64
                } else {
                    i as f64 * 0.5
                }
            })
            .collect(),
    )
}

#[test]
fn cascade_an_encoding_s_exception_stream() {
    let data = alp_column_with_exceptions(20_000, 10);
    let block = encode_block(&data, false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Alp, "ALP selected");
    assert_eq!(
        block.pipeline.side_stream().unwrap(),
        SideStream::ForBitpack,
        "the exception index stream carries a recorded inner encoding"
    );
    assert!(cascade_depth(block.pipeline).unwrap() >= 2);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
}

#[test]
fn bounded_recursion_depth() {
    const { assert!(MAX_CASCADE_DEPTH <= 4, "the declared maximum stays small") };
    assert!(cascade_level_fits(MAX_CASCADE_DEPTH - 1));
    assert!(
        !cascade_level_fits(MAX_CASCADE_DEPTH),
        "a level beyond the declared maximum is never added"
    );

    let datasets = [
        alp_column_with_exceptions(20_000, 10),
        ColumnData::U64((0..4096u64).map(|i| i * 7).collect()),
        ColumnData::Strings((0..512).map(|i| Some(format!("value-{}", i % 3))).collect()),
    ];
    for data in &datasets {
        for strategy in [CascadeStrategy::DecodeOptimized, CascadeStrategy::SizeOptimized] {
            let block = encode_block_with_strategy(data, false, strategy);
            assert!(
                cascade_depth(block.pipeline).unwrap() <= MAX_CASCADE_DEPTH,
                "{strategy:?}: recorded cascade must stay within the declared maximum"
            );
        }
    }
}

#[test]
fn deeper_level_kept_only_when_sampling_wins() {
    let sparse = alp_column_with_exceptions(3_000, 250);
    let block = encode_block(&sparse, false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Alp);
    assert_eq!(
        block.pipeline.side_stream().unwrap(),
        SideStream::None,
        "a losing inner candidate is not applied"
    );
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), sparse);

    let dense = alp_column_with_exceptions(20_000, 10);
    let cascaded = encode_block(&dense, false);
    assert_eq!(cascaded.pipeline.side_stream().unwrap(), SideStream::ForBitpack);
}

#[test]
fn reader_decodes_the_recorded_cascade_deterministically() {
    let data = alp_column_with_exceptions(20_000, 10);
    let block = encode_block(&data, false);
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::ForBitpack);

    let recorded = oe::encoding::PipelineId(block.pipeline.0);
    let first = decode_block(recorded, &block.bytes).unwrap();
    let second = decode_block(recorded, &block.bytes).unwrap();
    assert_eq!(first, data);
    assert_eq!(first, second);

    let ColumnData::F64(all) = &data else { unreachable!() };
    let granule = decode_block_range(recorded, &block.bytes, 5_000, 5_064).unwrap();
    assert_eq!(granule, ColumnData::F64(all[5_000..5_064].to_vec()));
}

#[test]
fn software_parity_holds_at_every_cascade_level() {
    let data = alp_column_with_exceptions(20_000, 10);
    let block = encode_block_with_strategy(&data, false, CascadeStrategy::SizeOptimized);
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::ForBitpack);

    let compression = block.pipeline.compression().unwrap();
    let software = SoftwareDecompressor
        .decompress(compression, &block.bytes)
        .expect("software path always decodes");
    let active = active_decompressor()
        .decompress(compression, &block.bytes)
        .expect("active engine decodes the same bytes");
    assert_eq!(
        software, active,
        "trailing stage: software and active engines agree byte for byte"
    );

    let decoded = decode_block(block.pipeline, &block.bytes).unwrap();
    assert_eq!(decoded, data);
    let reencoded = encode_block_with_strategy(&decoded, false, CascadeStrategy::SizeOptimized);
    assert_eq!(reencoded.pipeline, block.pipeline);
    assert_eq!(reencoded.bytes, block.bytes);
}
