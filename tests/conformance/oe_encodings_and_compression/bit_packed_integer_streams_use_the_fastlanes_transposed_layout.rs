use oe::encoding::{ColumnData, StringColumn, Transform, decode_block, encode_block};

fn round_trip(data: ColumnData) {
    let encoded = encode_block(&data, false);
    let decoded = decode_block(encoded.pipeline, &encoded.bytes).unwrap();
    assert_eq!(decoded, data);
}

#[test]
fn integer_columns_round_trip_across_vector_boundaries() {
    let monotonic: Vec<u64> = (0..3000u64).map(|i| 1_700_000_000_000 + i * 17).collect();
    let encoded = encode_block(&ColumnData::U64(monotonic.clone()), false);
    assert!(
        matches!(
            encoded.pipeline.transform().unwrap(),
            Transform::ForBitpack | Transform::DeltaBitpack
        ),
        "monotonic data must select a FastLanes FOR/DELTA candidate"
    );
    let decoded = decode_block(encoded.pipeline, &encoded.bytes).unwrap();
    assert_eq!(decoded, ColumnData::U64(monotonic));

    let signed: Vec<i64> = (0..1025i64).map(|i| -5_000_000 + i * 13).collect();
    round_trip(ColumnData::I64(signed));
}

#[test]
fn alp_and_dictionary_streams_ride_the_same_transposed_layout() {
    let metric: Vec<f64> = (0..2049).map(|i| (i as f64) * 0.25 + 10.5).collect();
    let alp = encode_block(&ColumnData::F64(metric.clone()), false);
    assert_eq!(alp.pipeline.transform().unwrap(), Transform::Alp);
    assert_eq!(decode_block(alp.pipeline, &alp.bytes).unwrap(), ColumnData::F64(metric));

    let labels: StringColumn = (0..2500).map(|i| Some(["EUR", "USD", "NOK", "GBP"][i % 4])).collect();
    let dict = encode_block(&ColumnData::Strings(labels.clone()), false);
    assert_eq!(dict.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(
        decode_block(dict.pipeline, &dict.bytes).unwrap(),
        ColumnData::Strings(labels)
    );
}
