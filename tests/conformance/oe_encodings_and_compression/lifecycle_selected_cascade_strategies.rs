use oe::encoding::{CascadeStrategy, ColumnData, decode_block, encode_block, encode_block_with_strategy};

fn monotonic_u64(n: usize) -> ColumnData {
    ColumnData::U64((0..n as u64).collect())
}

#[test]
fn fresh_part_uses_the_decode_optimized_strategy() {
    assert_eq!(
        CascadeStrategy::for_fresh_publication(),
        CascadeStrategy::DecodeOptimized
    );

    let data = monotonic_u64(512);
    let block = encode_block(&data, false);
    let decoded = decode_block(block.pipeline, &block.bytes).unwrap();
    assert_eq!(decoded, data, "DecodeOptimized block decodes correctly");
}

#[test]
fn rewrite_uses_the_size_optimized_strategy() {
    assert_eq!(
        CascadeStrategy::for_rewrite_or_compaction(),
        CascadeStrategy::SizeOptimized
    );

    let data = monotonic_u64(512);
    let block = encode_block_with_strategy(&data, false, CascadeStrategy::SizeOptimized);
    let decoded = decode_block(block.pipeline, &block.bytes).unwrap();
    assert_eq!(decoded, data, "SizeOptimized block decodes correctly");
}

#[test]
fn no_operator_knob_selects_the_strategy() {
    let data = ColumnData::F64(vec![1.0, 2.0, 3.0]);
    let _ = encode_block(&data, false);
}

#[test]
fn strategy_selection_is_deterministic() {
    let data = ColumnData::F64(vec![1.5, 2.5, 3.5, 4.5, 5.5]);
    for strategy in [CascadeStrategy::DecodeOptimized, CascadeStrategy::SizeOptimized] {
        let a = encode_block_with_strategy(&data, false, strategy);
        let b = encode_block_with_strategy(&data, false, strategy);
        assert_eq!(a.pipeline, b.pipeline, "{strategy:?}: pipeline differs");
        assert_eq!(a.bytes, b.bytes, "{strategy:?}: bytes differ");
    }
}

#[test]
fn both_strategies_round_trip_correctly() {
    let datasets = [
        monotonic_u64(256),
        ColumnData::I64((-128..128_i64).collect()),
        ColumnData::F64((0..64).map(|i| i as f64 * 0.5).collect()),
        ColumnData::Decimal {
            values: (1..=32_i128).map(|i| i * 100).collect(),
            scale: 2,
        },
    ];
    for data in &datasets {
        for strategy in [CascadeStrategy::DecodeOptimized, CascadeStrategy::SizeOptimized] {
            let block = encode_block_with_strategy(data, false, strategy);
            let decoded = decode_block(block.pipeline, &block.bytes).unwrap();
            assert_eq!(&decoded, data, "{strategy:?}: round-trip failed");
        }
    }
}
