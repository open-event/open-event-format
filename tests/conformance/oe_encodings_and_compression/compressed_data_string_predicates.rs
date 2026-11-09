use oe::encoding::predicate::{
    ContainsMatcher, StringBound, StringPredicate, filter_string_block, filter_string_block_or_decode,
};
use oe::encoding::{ColumnData, PipelineId, SideStream, StringColumn, Transform, decode_block, encode_block};

fn low_cardinality(rows: usize) -> StringColumn {
    let palette = ["apple", "banana", "cherry", "date"];
    (0..rows)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(palette[i % palette.len()])
            }
        })
        .collect()
}

fn high_cardinality(rows: usize) -> StringColumn {
    (0..rows)
        .map(|i| if i % 9 == 0 { None } else { Some(format!("id-{i:05}")) })
        .collect()
}

fn long_high_cardinality(rows: usize) -> StringColumn {
    (0..rows)
        .map(|i| {
            if i % 9 == 0 {
                None
            } else {
                Some(format!("{i:05}-order-line-reference-{i:05}"))
            }
        })
        .collect()
}

fn decode_strings(pipeline: PipelineId, bytes: &[u8]) -> StringColumn {
    match decode_block(pipeline, bytes).unwrap() {
        ColumnData::Strings(values) => values,
        other => panic!("expected strings, got {other:?}"),
    }
}

fn bound(value: &str, inclusive: bool) -> StringBound {
    StringBound {
        inclusive,
        value: value.to_owned(),
    }
}

#[test]
fn dictionary_codes_preserve_value_order() {
    let values = low_cardinality(64);
    let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
    assert_eq!(encoded.pipeline.transform().unwrap(), Transform::DictionaryString);

    let predicate = StringPredicate::Range {
        lower: Some(bound("banana", true)),
        upper: Some(bound("date", false)),
    };
    let mask = filter_string_block(encoded.pipeline, &encoded.bytes, &predicate)
        .unwrap()
        .expect("dictionary range is answered from codes");
    assert_eq!(mask, predicate.filter_decoded(&values));
    for (index, value) in values.iter().enumerate() {
        let expected = matches!(value, Some("banana") | Some("cherry"));
        assert_eq!(mask[index], expected, "row {index} = {value:?}");
    }
}

#[test]
fn dictionary_equality_and_range_answered_from_codes() {
    let values = low_cardinality(80);
    let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
    let predicates = [
        StringPredicate::Equals("cherry".to_owned()),
        StringPredicate::InSet(vec!["apple".to_owned(), "date".to_owned()]),
        StringPredicate::Range {
            lower: Some(bound("banana", true)),
            upper: None,
        },
    ];
    for predicate in &predicates {
        let mask = filter_string_block(encoded.pipeline, &encoded.bytes, predicate)
            .unwrap()
            .expect("dictionary predicate answered from codes");
        assert_eq!(mask, predicate.filter_decoded(&values), "{predicate:?}");
    }
}

#[test]
fn fsst_equality_answered_from_compressed_bytes() {
    let values = high_cardinality(96);
    let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
    assert!(matches!(
        encoded.pipeline.transform().unwrap(),
        Transform::FsstString | Transform::FsstPackedString
    ));

    let predicates = [
        StringPredicate::Equals("id-00042".to_owned()),
        StringPredicate::Equals("id-99999".to_owned()),
        StringPredicate::NotEquals("id-00007".to_owned()),
        StringPredicate::InSet(vec!["id-00003".to_owned(), "id-00050".to_owned()]),
    ];
    for predicate in &predicates {
        let mask = filter_string_block(encoded.pipeline, &encoded.bytes, predicate)
            .unwrap()
            .expect("FSST equality-class answered from compressed bytes");
        assert_eq!(mask, predicate.filter_decoded(&values), "{predicate:?}");
    }
}

#[test]
fn fsst_range_falls_back_to_decode() {
    let values = high_cardinality(96);
    let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
    assert!(matches!(
        encoded.pipeline.transform().unwrap(),
        Transform::FsstString | Transform::FsstPackedString
    ));

    let predicate = StringPredicate::Range {
        lower: Some(bound("id-00010", true)),
        upper: Some(bound("id-00050", false)),
    };
    assert!(
        filter_string_block(encoded.pipeline, &encoded.bytes, &predicate)
            .unwrap()
            .is_none()
    );
    let answered = filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, &predicate).unwrap();
    assert_eq!(answered, predicate.filter_decoded(&values));
}

#[test]
fn fsst_range_answered_from_per_value_prefix_keys() {
    let values = long_high_cardinality(128);
    let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
    assert!(matches!(
        encoded.pipeline.transform().unwrap(),
        Transform::FsstString | Transform::FsstPackedString
    ));
    assert_eq!(encoded.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);

    let predicates = [
        StringPredicate::Range {
            lower: Some(bound("00010", true)),
            upper: Some(bound("00050", false)),
        },
        StringPredicate::Range {
            lower: Some(bound("0001", true)),
            upper: Some(bound("0002", false)),
        },
        StringPredicate::Range {
            lower: Some(bound("00007-order-line-reference-00007", true)),
            upper: Some(bound("00007-order-line-reference-00007", true)),
        },
    ];
    for predicate in &predicates {
        let mask = filter_string_block(encoded.pipeline, &encoded.bytes, predicate)
            .unwrap()
            .expect("FSST range answered from the block's prefix keys");
        assert_eq!(mask, predicate.filter_decoded(&values), "{predicate:?}");
    }
}

#[test]
fn fsst_substring_candidates_pruned_by_byte_fingerprints() {
    let values = long_high_cardinality(128);
    let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
    assert_eq!(encoded.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);

    for needle in ["order", "00042", "ZZZ", "~"] {
        let predicate = StringPredicate::Contains(ContainsMatcher::new(vec![needle.to_owned()], false));
        let mask = filter_string_block(encoded.pipeline, &encoded.bytes, &predicate)
            .unwrap()
            .expect("FSST substring answered over the block's own values");
        assert_eq!(mask, predicate.filter_decoded(&values), "{needle:?}");
    }
    let absent = StringPredicate::Contains(ContainsMatcher::new(vec!["~".to_owned()], false));
    assert!(
        filter_string_block(encoded.pipeline, &encoded.bytes, &absent)
            .unwrap()
            .expect("answered from the fingerprints")
            .iter()
            .all(|selected| !selected)
    );
}

#[test]
fn compressed_data_filter_equals_full_decode() {
    let predicates = [
        StringPredicate::Equals("banana".to_owned()),
        StringPredicate::NotEquals("banana".to_owned()),
        StringPredicate::InSet(vec!["apple".to_owned(), "id-00010".to_owned()]),
        StringPredicate::Range {
            lower: Some(bound("b", true)),
            upper: Some(bound("e", false)),
        },
    ];
    for values in [low_cardinality(70), high_cardinality(70)] {
        let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
        let decoded = decode_strings(encoded.pipeline, &encoded.bytes);
        for predicate in &predicates {
            let answered = filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, predicate).unwrap();
            assert_eq!(answered, predicate.filter_decoded(&decoded), "{predicate:?}");
            for (index, value) in decoded.iter().enumerate() {
                if value.is_none() {
                    assert!(!answered[index], "null row {index} matched {predicate:?}");
                }
            }
        }
    }
}
