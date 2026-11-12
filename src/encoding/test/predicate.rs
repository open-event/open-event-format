use super::*;
use crate::encoding::{
    CascadeStrategy, Compression, NullStream, NullStreamForm, bitpack, decode_delta_bitpack, encode_block,
    encode_block_forced_for_conformance,
};
use crate::file::bytes::Writer;
use std::collections::BTreeSet;

fn decoded(pipeline: super::PipelineId, bytes: &[u8]) -> StringColumn {
    match decode_block(pipeline, bytes).unwrap() {
        ColumnData::Strings(values) => values,
        other => panic!("expected strings, got {other:?}"),
    }
}

fn check(values: &[Option<String>], predicate: &StringPredicate, random_access: bool) -> bool {
    let column: StringColumn = values.to_vec().into();
    let encoded = encode_block(&ColumnData::Strings(column.clone()), random_access);
    let reference = predicate.filter_decoded(&column);
    assert_eq!(reference.len(), values.len(), "reference mask must cover every row");

    let combined = filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, predicate).unwrap();
    assert_eq!(combined, reference, "fallback wrapper disagreed");

    let round = predicate.filter_decoded(&decoded(encoded.pipeline, &encoded.bytes));
    assert_eq!(round, reference, "decode round-trip disagreed");

    if let Some(count) = count_string_block_shared(encoded.pipeline, &encoded.bytes, predicate, None).unwrap() {
        assert_eq!(
            count,
            reference.iter().filter(|matched| **matched).count(),
            "count-only kernel disagreed with the reference mask"
        );
    }

    match filter_string_block(encoded.pipeline, &encoded.bytes, predicate).unwrap() {
        Some(mask) => {
            assert_eq!(mask, reference, "fast kernel disagreed with reference");
            true
        }
        None => false,
    }
}

fn transform_of(values: &[Option<String>], random_access: bool) -> Transform {
    encode_block(&ColumnData::Strings(values.to_vec().into()), random_access)
        .pipeline
        .transform()
        .unwrap()
}

fn low_cardinality(rows: usize) -> Vec<Option<String>> {
    let palette = ["apple", "banana", "cherry", "date"];
    (0..rows)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(palette[i % palette.len()].to_owned())
            }
        })
        .collect()
}

fn high_cardinality(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| if i % 9 == 0 { None } else { Some(format!("id-{i:05}")) })
        .collect()
}

fn long_high_cardinality(rows: usize) -> Vec<Option<String>> {
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

fn long_shared_prefix(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| {
            if i % 9 == 0 {
                None
            } else {
                Some(format!("https://example.com/orders/{i:05}"))
            }
        })
        .collect()
}

fn long_opaque(rows: usize) -> Vec<Option<String>> {
    let filler = "x".repeat(200);
    (0..rows).map(|i| Some(format!("{filler}-{i}"))).collect()
}

fn equals(value: &str) -> StringPredicate {
    StringPredicate::Equals(value.to_owned())
}

fn contains(value: &str) -> StringPredicate {
    StringPredicate::Contains(ContainsMatcher::new(vec![value.to_owned()], false))
}

fn contains_all(values: &[&str], case_insensitive: bool) -> StringPredicate {
    StringPredicate::Contains(ContainsMatcher::new(
        values.iter().map(|value| (*value).to_owned()).collect(),
        case_insensitive,
    ))
}

fn bound(value: &str, inclusive: bool) -> StringBound {
    StringBound {
        inclusive,
        value: value.to_owned(),
    }
}

#[test]
fn low_cardinality_columns_encode_as_a_dictionary() {
    let values = low_cardinality(64);
    assert_eq!(transform_of(&values, true), Transform::DictionaryString);
}

#[test]
fn dictionary_equality_is_answered_from_codes() {
    let values = low_cardinality(64);
    for random_access in [true, false] {
        assert!(check(&values, &equals("banana"), random_access));
        assert!(check(&values, &equals("fig"), random_access));
        assert!(check(
            &values,
            &StringPredicate::NotEquals("cherry".to_owned()),
            random_access
        ));
        assert!(check(
            &values,
            &StringPredicate::InSet(vec!["apple".to_owned(), "date".to_owned(), "kiwi".to_owned()]),
            random_access
        ));
    }
}

#[test]
fn dictionary_ranges_are_answered_from_sorted_codes() {
    let values = low_cardinality(96);
    let cases = [
        StringPredicate::Range {
            lower: Some(bound("banana", true)),
            upper: Some(bound("cherry", true)),
        },
        StringPredicate::Range {
            lower: Some(bound("banana", false)),
            upper: Some(bound("date", false)),
        },
        StringPredicate::Range {
            lower: Some(bound("aaa", true)),
            upper: None,
        },
        StringPredicate::Range {
            lower: None,
            upper: Some(bound("cherry", false)),
        },
        StringPredicate::Range {
            lower: Some(bound("date", false)),
            upper: None,
        },
    ];
    for predicate in &cases {
        for random_access in [true, false] {
            assert!(
                check(&values, predicate, random_access),
                "dictionary range should be handled fast: {predicate:?}"
            );
        }
    }
}

#[test]
fn high_cardinality_columns_encode_as_fsst() {
    let values = high_cardinality(128);
    assert_eq!(transform_of(&values, true), Transform::FsstPackedString);
}

#[test]
fn fsst_equality_class_is_answered_from_compressed_bytes() {
    let values = high_cardinality(128);
    for random_access in [true, false] {
        assert!(check(&values, &equals("id-00042"), random_access));
        assert!(check(&values, &equals("id-99999"), random_access));
        assert!(check(
            &values,
            &StringPredicate::NotEquals("id-00007".to_owned()),
            random_access
        ));
        assert!(check(
            &values,
            &StringPredicate::InSet(vec!["id-00003".to_owned(), "id-00050".to_owned(), "missing".to_owned(),]),
            random_access
        ));
    }
}

#[test]
fn fsst_blocks_carry_per_value_keys_only_when_their_values_are_long_enough() {
    let long = encode_block(&ColumnData::Strings(long_high_cardinality(128).into()), true);
    assert_eq!(long.pipeline.transform().unwrap(), Transform::FsstString);
    assert_eq!(long.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);

    let short = encode_block(&ColumnData::Strings(high_cardinality(128).into()), true);
    assert_eq!(short.pipeline.transform().unwrap(), Transform::FsstPackedString);
    assert_eq!(short.pipeline.side_stream().unwrap(), SideStream::None);
}

#[test]
fn a_block_with_per_value_keys_still_decodes_unchanged() {
    let values: Vec<Option<String>> = long_high_cardinality(128);
    let column: StringColumn = values.clone().into();
    for random_access in [true, false] {
        let encoded = encode_block(&ColumnData::Strings(column.clone()), random_access);
        assert_eq!(encoded.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);
        assert_eq!(decoded(encoded.pipeline, &encoded.bytes), column);
        let ColumnData::Strings(window) =
            crate::encoding::decode_block_range(encoded.pipeline, &encoded.bytes, 10, 20).unwrap()
        else {
            panic!("expected strings");
        };
        assert_eq!(window, values[10..20].to_vec().into());
        let views = crate::encoding::decode_string_block_views(encoded.pipeline, &encoded.bytes)
            .unwrap()
            .expect("FSST blocks decode into string views");
        let seen: Vec<Option<String>> = views.iter().map(|value| value.map(str::to_owned)).collect();
        assert_eq!(seen, values);
    }
}

#[test]
fn fsst_range_predicates_are_answered_from_prefix_keys() {
    let cases = [
        ("separating prefixes", long_high_cardinality(128)),
        ("shared prefixes", long_shared_prefix(128)),
    ];
    let predicates = [
        StringPredicate::Range {
            lower: Some(bound("00010", true)),
            upper: Some(bound("00050", false)),
        },
        StringPredicate::Range {
            lower: Some(bound("https://example.com/orders/00010", true)),
            upper: Some(bound("https://example.com/orders/00050", false)),
        },
        StringPredicate::Range {
            lower: Some(bound("00100", false)),
            upper: None,
        },
        StringPredicate::Range {
            lower: Some(bound("https://example.com/orders/001", true)),
            upper: Some(bound("https://example.com/orders/002", false)),
        },
        StringPredicate::Range {
            lower: None,
            upper: Some(bound("zzzz", true)),
        },
        StringPredicate::Range {
            lower: Some(bound("zzzz", true)),
            upper: None,
        },
    ];
    for (name, values) in cases {
        for predicate in &predicates {
            for random_access in [true, false] {
                assert!(
                    check(&values, predicate, random_access),
                    "{name}: FSST range should be answered from the prefix keys: {predicate:?}"
                );
            }
        }
    }
}

#[test]
fn fsst_range_bounds_honour_inclusivity() {
    let values = long_high_cardinality(128);
    for inclusive in [true, false] {
        assert!(check(
            &values,
            &StringPredicate::Range {
                lower: Some(bound("00007-order-line-reference-00007", inclusive)),
                upper: Some(bound("00007-order-line-reference-00007", inclusive)),
            },
            true
        ));
    }
}

#[test]
fn fsst_substring_candidates_are_pruned_by_fingerprints() {
    let values = long_high_cardinality(128);
    let encoded = encode_block(&ColumnData::Strings(values.clone().into()), true);
    assert_eq!(encoded.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);
    for needle in ["order", "line-reference", "00042", "ZZZ", "~", "\u{0}"] {
        assert!(
            check(&values, &contains(needle), true),
            "fingerprint-pruned contains disagreed for {needle:?}"
        );
    }
    assert!(check(&values, &contains_all(&["ORDER", "REFERENCE"], true), true));
    assert!(check(&values, &contains_all(&["order", "nope"], false), true));
}

struct KeyScanRng(u64);

impl KeyScanRng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn text(&mut self, len: usize) -> String {
        (0..len).map(|_| ['a', 'b', '\0'][self.below(3)]).collect()
    }

    fn value(&mut self) -> Option<String> {
        match self.below(10) {
            0 => None,
            1 => Some(String::new()),
            2 | 3 => {
                let len = 1 + self.below(9);
                Some(self.text(len))
            }
            _ => {
                let len = 30 + self.below(30);
                Some(self.text(len))
            }
        }
    }

    fn bound(&mut self, values: &[Option<String>]) -> Option<StringBound> {
        let value = match self.below(4) {
            0 => return None,
            1 => values
                .get(self.below(values.len()))
                .cloned()
                .flatten()
                .unwrap_or_default(),
            2 => self.text(PREFIX_KEY_PREFIX_LEN),
            _ => {
                let len = self.below(12);
                self.text(len)
            }
        };
        Some(bound(&value, self.below(2) == 0))
    }
}

fn keyed_fsst_block(values: &[Option<String>]) -> (StringColumn, crate::encoding::EncodedBlock) {
    let column: StringColumn = values.to_vec().into();
    let block = encode_block_forced_for_conformance(
        &ColumnData::Strings(column.clone()),
        true,
        CascadeStrategy::NoTrailing,
        Transform::FsstString,
    )
    .unwrap();
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);
    (column, block)
}

fn key_scan_row_counts() -> [usize; 9] {
    [1, 7, 63, 64, 65, 127, 128, 129, 300]
}

#[test]
fn chunked_prefix_key_ranges_agree_with_the_decoded_filter() {
    let mut rng = KeyScanRng(0x9E37_79B9_7F4A_7C15);
    let mut blocks = 0;
    for rows in key_scan_row_counts() {
        for _ in 0..8 {
            let values: Vec<Option<String>> = (0..rows).map(|_| rng.value()).collect();
            if values.iter().flatten().map(String::len).sum::<usize>() < values.iter().flatten().count() * 24 {
                continue;
            }
            blocks += 1;
            let (column, block) = keyed_fsst_block(&values);
            for _ in 0..24 {
                let predicate = StringPredicate::Range {
                    lower: rng.bound(&values),
                    upper: rng.bound(&values),
                };
                let mask = filter_string_block(block.pipeline, &block.bytes, &predicate)
                    .unwrap()
                    .expect("a keyed FSST block answers a range");
                assert_eq!(mask, predicate.filter_decoded(&column), "{rows} rows, {predicate:?}");
            }
        }
    }
    assert!(blocks > 40, "too few keyed blocks were exercised: {blocks}");
}

#[test]
fn chunked_fingerprint_candidates_agree_with_the_decoded_search() {
    let mut rng = KeyScanRng(0xD1B5_4A32_D192_ED03);
    for rows in key_scan_row_counts() {
        let mut values: Vec<Option<String>> = (0..rows)
            .map(|_| {
                let len = 30 + rng.below(30);
                Some(rng.text(len))
            })
            .collect();
        for value in values.iter_mut().step_by(5) {
            *value = rng.value();
        }
        if let Some(Some(last)) = values.last_mut() {
            last.push_str("-tail-marker");
        }
        let (column, block) = keyed_fsst_block(&values);
        let mut predicates: Vec<StringPredicate> = ["a", "ab\0b", "aaaaaaaa", "z", "-tail-marker", ""]
            .into_iter()
            .map(contains)
            .collect();
        predicates.push(contains_all(&["A", "B"], true));
        for predicate in &predicates {
            let reference = predicate.filter_decoded(&column);
            let mask = filter_string_block(block.pipeline, &block.bytes, predicate)
                .unwrap()
                .expect("a keyed FSST block answers a substring test");
            assert_eq!(mask, reference, "{rows} rows, {predicate:?}");
            let count = count_string_block_shared(block.pipeline, &block.bytes, predicate, None)
                .unwrap()
                .expect("a keyed FSST block counts a substring test");
            assert_eq!(
                count,
                reference.iter().filter(|hit| **hit).count(),
                "{rows} rows, {predicate:?}"
            );
        }
    }
}

#[test]
fn a_forged_prefix_key_length_refuses_the_range_scan() {
    let values: Vec<Option<String>> = (0..100).map(|i| Some(format!("{i:05}-a-long-enough-value"))).collect();
    let (_, block) = keyed_fsst_block(&values);
    let mut forged = block.bytes.clone();
    let prefixes_start = forged.len() - values.len() * (PREFIX_KEY_BYTES + StringFingerprint::STORED_BYTES);
    forged[prefixes_start + 70 * PREFIX_KEY_BYTES + PREFIX_KEY_PREFIX_LEN] = PREFIX_KEY_PREFIX_LEN as u8 + 1;
    let predicate = StringPredicate::Range {
        lower: Some(bound("00010", true)),
        upper: None,
    };
    assert!(filter_string_block(block.pipeline, &block.bytes, &predicate).is_ok());
    assert!(filter_string_block(block.pipeline, &forged, &predicate).is_err());
}

#[test]
fn fsst_range_predicates_fall_back_to_decode() {
    let values = high_cardinality(128);
    let predicate = StringPredicate::Range {
        lower: Some(bound("id-00010", true)),
        upper: Some(bound("id-00020", false)),
    };
    assert!(
        filter_string_block(
            encode_block(&ColumnData::Strings(values.clone().into()), true).pipeline,
            &encode_block(&ColumnData::Strings(values.clone().into()), true).bytes,
            &predicate,
        )
        .unwrap()
        .is_none()
    );
    assert!(!check(&values, &predicate, true));
}

#[test]
fn a_forged_fsst_symbol_table_refuses_in_the_predicate_kernel() {
    let mut out = Writer::new();
    out.put_u32(1);
    out.put_u8(1);
    out.put_u16(300);
    for index in 0..300u64 {
        out.put_u64(index);
        out.put_u8(2);
    }
    out.put_u32(1);
    out.put_u32(0);
    out.put_u32(1);
    out.put_u8(0);
    let forged = out.into_bytes();

    let pipeline = PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String);
    assert!(filter_string_block(pipeline, &forged, &equals("x")).is_err());
}

#[test]
fn raw_string_columns_fall_back_to_decode() {
    let values = long_opaque(32);
    assert_eq!(transform_of(&values, true), Transform::RawString);
    assert!(!check(&values, &equals("anything"), true));
    assert!(!check(
        &values,
        &StringPredicate::Range {
            lower: Some(bound("a", true)),
            upper: None
        },
        true
    ));
}

#[test]
fn substring_predicates_are_answered_over_each_encodings_arena() {
    let cases: [(&str, Vec<Option<String>>); 3] = [
        ("dictionary", low_cardinality(96)),
        ("fsst", high_cardinality(128)),
        ("raw", long_opaque(32)),
    ];
    for (name, values) in cases {
        for random_access in [true, false] {
            for needle in ["an", "err", "-1", "zzz", "x"] {
                assert!(
                    check(&values, &contains(needle), random_access),
                    "{name} block should answer `contains {needle}` from its arena"
                );
            }
        }
    }
}

#[test]
fn a_substring_spanning_two_neighbouring_values_is_not_a_match() {
    let values: Vec<Option<String>> = ["hello", "world", "helloworld"]
        .iter()
        .map(|value| Some((*value).to_owned()))
        .collect();
    for random_access in [true, false] {
        assert!(check(&values, &contains("owor"), random_access));
        let mask = filter_string_block_or_decode(
            encode_block(&ColumnData::Strings(values.clone().into()), random_access).pipeline,
            &encode_block(&ColumnData::Strings(values.clone().into()), random_access).bytes,
            &contains("owor"),
        )
        .unwrap();
        assert_eq!(mask, vec![false, false, true], "only the joined value contains `owor`");
    }
}

#[test]
fn a_cross_boundary_hit_does_not_hide_an_overlapping_row_match() {
    let values = vec![Some("a".to_owned()), Some("aaa".to_owned())];
    for random_access in [true, false] {
        let encoded = encode_block(&ColumnData::Strings(values.clone().into()), random_access);
        let predicate = contains("aaa");
        let mask = filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, &predicate).unwrap();
        assert_eq!(mask, vec![false, true]);
        assert_eq!(
            count_string_block_shared(encoded.pipeline, &encoded.bytes, &predicate, None).unwrap(),
            Some(1)
        );
    }
}

#[test]
fn specialized_single_needle_shapes_agree_with_decoded_search() {
    let values = vec![
        None,
        Some(String::new()),
        Some("x".to_owned()),
        Some("café 🙂 alpha alpha".to_owned()),
        Some("aaaaaaaa".to_owned()),
        Some("a long needle lives in this one row".to_owned()),
        Some("dense dense dense".to_owned()),
        Some("dense again".to_owned()),
    ];
    for needle in ["", "x", "🙂", "aaa", "a long needle lives", "absent", "alpha", "dense"] {
        for random_access in [true, false] {
            assert!(check(&values, &contains(needle), random_access));
        }
    }
}

#[test]
fn a_multi_needle_substring_test_asks_for_every_needle() {
    let values: Vec<Option<String>> = ["alpha beta", "alpha", "beta", "betalpha"]
        .iter()
        .map(|value| Some((*value).to_owned()))
        .collect();
    for random_access in [true, false] {
        let encoded = encode_block(&ColumnData::Strings(values.clone().into()), random_access);
        let mask = filter_string_block_or_decode(
            encoded.pipeline,
            &encoded.bytes,
            &contains_all(&["alpha", "beta"], false),
        )
        .unwrap();
        assert_eq!(
            mask,
            vec![true, false, false, true],
            "only the values carrying both needles match"
        );
        let overlapping =
            filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, &contains_all(&["beta", "eta"], false))
                .unwrap();
        assert_eq!(
            overlapping,
            vec![true, false, true, true],
            "overlapping needles both count"
        );
    }
}

#[test]
fn a_case_insensitive_substring_test_matches_either_case() {
    let values: Vec<Option<String>> = ["Alpha Beta", "ALPHA", "gamma"]
        .iter()
        .map(|value| Some((*value).to_owned()))
        .collect();
    for random_access in [true, false] {
        let encoded = encode_block(&ColumnData::Strings(values.clone().into()), random_access);
        let folded =
            filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, &contains_all(&["alpha"], true)).unwrap();
        assert_eq!(folded, vec![true, true, false]);
        let exact =
            filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, &contains_all(&["alpha"], false)).unwrap();
        assert_eq!(exact, vec![false, false, false], "a case-sensitive needle stays exact");
    }
}

#[test]
fn the_empty_substring_matches_every_present_row() {
    for values in [low_cardinality(40), high_cardinality(40), long_opaque(8)] {
        assert!(check(&values, &contains(""), true));
    }
}

#[test]
fn null_rows_never_match_any_predicate() {
    let dict_values = low_cardinality(40);
    let fsst_values = high_cardinality(40);
    let predicates = [
        contains("an"),
        equals("banana"),
        StringPredicate::NotEquals("banana".to_owned()),
        StringPredicate::Range {
            lower: None,
            upper: None,
        },
    ];
    for predicate in &predicates {
        let dict_mask = filter_string_block_or_decode(
            encode_block(&ColumnData::Strings(dict_values.clone().into()), true).pipeline,
            &encode_block(&ColumnData::Strings(dict_values.clone().into()), true).bytes,
            predicate,
        )
        .unwrap();
        for (index, value) in dict_values.iter().enumerate() {
            if value.is_none() {
                assert!(!dict_mask[index], "null dict row {index} matched {predicate:?}");
            }
        }
        let fsst_mask = check_mask(&fsst_values, predicate);
        for (index, value) in fsst_values.iter().enumerate() {
            if value.is_none() {
                assert!(!fsst_mask[index], "null fsst row {index} matched {predicate:?}");
            }
        }
    }
}

fn check_mask(values: &[Option<String>], predicate: &StringPredicate) -> Vec<bool> {
    let encoded = encode_block(&ColumnData::Strings(values.to_vec().into()), true);
    filter_string_block_or_decode(encoded.pipeline, &encoded.bytes, predicate).unwrap()
}

#[test]
fn empty_and_all_null_columns_are_handled() {
    let empty: Vec<Option<String>> = Vec::new();
    assert!(check_mask(&empty, &equals("x")).is_empty());

    let all_null: Vec<Option<String>> = vec![None; 5];
    let mask = check_mask(&all_null, &equals("x"));
    assert_eq!(mask, vec![false; 5]);
}

#[test]
fn delta_encoded_u64_is_answered_over_packed_lanes() {
    let values: Vec<u64> = (0..300).map(|i| 5_000 + i * 3).collect();
    let block = encode_block(&ColumnData::U64(values.clone()), false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DeltaBitpack);

    let predicate = NumericPredicate::Range {
        lower: Some(NumericBound {
            inclusive: true,
            value: 5_300,
        }),
        upper: Some(NumericBound {
            inclusive: false,
            value: 5_600,
        }),
    };
    let mask = filter_numeric_block(block.pipeline, &block.bytes, &predicate)
        .unwrap()
        .expect("DELTA range answered from packed lanes");
    let expected: Vec<bool> = values.iter().map(|v| (5_300..5_600).contains(v)).collect();
    assert_eq!(mask, expected);
    assert_eq!(mask, predicate.filter_decoded(&ColumnData::U64(values)));
}

#[test]
fn delta_encoded_i64_unzigzags_before_comparing() {
    let values: Vec<i64> = (0..200).map(|i| -500 + i * 4).collect();
    let block = encode_block(&ColumnData::I64(values.clone()), false);
    assert!(matches!(
        block.pipeline.transform().unwrap(),
        Transform::ForBitpack | Transform::DeltaBitpack
    ));

    let predicate = NumericPredicate::Range {
        lower: Some(NumericBound {
            inclusive: true,
            value: -100,
        }),
        upper: Some(NumericBound {
            inclusive: false,
            value: 200,
        }),
    };
    let mask = filter_numeric_block(block.pipeline, &block.bytes, &predicate)
        .unwrap()
        .expect("I64 FOR/DELTA answered over packed lanes");
    let expected: Vec<bool> = values.iter().map(|v| (-100..200).contains(v)).collect();
    assert_eq!(mask, expected);
    assert_eq!(mask, predicate.filter_decoded(&ColumnData::I64(values)));
}

#[test]
fn delta_encoded_equals_not_equals_and_in_set_match_the_reference() {
    let unsigned: Vec<u64> = (0..300).map(|i| 5_000 + i * 3).collect();
    let block = encode_block_forced_for_conformance(
        &ColumnData::U64(unsigned.clone()),
        false,
        CascadeStrategy::SizeOptimized,
        Transform::DeltaBitpack,
    )
    .unwrap();
    let hit = unsigned[40];
    let predicates = [
        NumericPredicate::Equals(hit as i128),
        NumericPredicate::Equals(4),
        NumericPredicate::NotEquals(hit as i128),
        NumericPredicate::InSet(vec![hit as i128, unsigned[100] as i128, -9]),
        NumericPredicate::InSet(Vec::new()),
    ];
    for predicate in &predicates {
        let mask = filter_numeric_block(block.pipeline, &block.bytes, predicate)
            .unwrap()
            .expect("DELTA answered from the running prefix sum");
        assert_eq!(
            mask,
            predicate.filter_decoded(&ColumnData::U64(unsigned.clone())),
            "diverged for {predicate:?}"
        );
    }

    let signed: Vec<i64> = (0..200).map(|i| -500 + i * 4).collect();
    let block = encode_block_forced_for_conformance(
        &ColumnData::I64(signed.clone()),
        false,
        CascadeStrategy::SizeOptimized,
        Transform::DeltaBitpack,
    )
    .unwrap();
    let hit = signed[40];
    let predicates = [
        NumericPredicate::Equals(hit as i128),
        NumericPredicate::Equals(i128::from(i64::MAX)),
        NumericPredicate::NotEquals(hit as i128),
        NumericPredicate::InSet(vec![hit as i128, signed[100] as i128, i128::from(i64::MIN)]),
    ];
    for predicate in &predicates {
        let mask = filter_numeric_block(block.pipeline, &block.bytes, predicate)
            .unwrap()
            .expect("DELTA answered from the running prefix sum");
        assert_eq!(
            mask,
            predicate.filter_decoded(&ColumnData::I64(signed.clone())),
            "diverged for {predicate:?}"
        );
    }
}

fn delta_body(first: u64, codes: &[u64], width: u32, count: usize) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.put_u64(first);
    bitpack(codes, width, &mut writer);
    let mut body = writer.into_bytes();
    body[8..12].copy_from_slice(&(count as u32).to_le_bytes());
    body
}

#[test]
fn delta_filter_matches_decode_then_filter_across_vector_boundaries() {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    for width in [0u32, 1, 63, 64] {
        for count in [0usize, 1, 1023, 1024, 1025, 2048, 2049] {
            for first in [0u64, u64::MAX - 3, i64::MAX as u64 - 1] {
                let padded = count.div_ceil(1024).max(1) * 1024;
                let codes: Vec<u64> = (0..padded)
                    .map(|_| if width == 0 { 0 } else { next() & mask(width) | 1 })
                    .collect();
                let body = delta_body(first, &codes, width, count);
                let mut values = vec![first];
                for code in &codes[..count] {
                    let last = *values.last().unwrap();
                    values.push(last.wrapping_add(unzigzag(*code) as u64));
                }
                let hit = values[values.len() / 2];
                for (kind, column) in [
                    (ValueKind::U64, ColumnData::U64(values.clone())),
                    (
                        ValueKind::I64,
                        ColumnData::I64(values.iter().map(|value| unzigzag(*value)).collect()),
                    ),
                ] {
                    let signed_hit = i128::from(unzigzag(hit));
                    let hit = if kind == ValueKind::U64 {
                        i128::from(hit)
                    } else {
                        signed_hit
                    };
                    let predicates = [
                        NumericPredicate::Equals(hit),
                        NumericPredicate::NotEquals(hit),
                        NumericPredicate::InSet(vec![hit, -1, i128::from(u64::MAX)]),
                        NumericPredicate::InSet(Vec::new()),
                        NumericPredicate::Range {
                            lower: Some(NumericBound {
                                inclusive: false,
                                value: hit - 1000,
                            }),
                            upper: Some(NumericBound {
                                inclusive: true,
                                value: hit,
                            }),
                        },
                        NumericPredicate::Range {
                            lower: None,
                            upper: Some(NumericBound {
                                inclusive: false,
                                value: 0,
                            }),
                        },
                    ];
                    for predicate in &predicates {
                        assert_eq!(
                            filter_delta_block(&body, predicate, kind).unwrap(),
                            predicate.filter_decoded(&column),
                            "width {width}, count {count}, first {first}, {kind:?}, {predicate:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn delta_filter_matches_decode_then_filter_decoded() {
    let mut state: u64 = 0xD1B5_4A32_D192_ED03;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for width in [0u32, 3, 20, 64] {
        for count in [0usize, 1, 17, 1000, 1024, 3333] {
            let first = next();
            let codes: Vec<u64> = (0..count).map(|_| next() & mask(width)).collect();
            let mut writer = Writer::new();
            writer.put_u64(first);
            bitpack(&codes, width, &mut writer);
            let body = writer.into_bytes();
            let unsigned = decode_delta_bitpack::<u64>(&mut Reader::new(&body)).unwrap();
            let signed = decode_delta_bitpack::<i64>(&mut Reader::new(&body)).unwrap();
            let pivot = unsigned.get(unsigned.len() / 2).copied().unwrap_or(0);
            for (kind, column, hit) in [
                (ValueKind::U64, ColumnData::U64(unsigned.clone()), i128::from(pivot)),
                (
                    ValueKind::I64,
                    ColumnData::I64(signed.clone()),
                    i128::from(unzigzag(pivot)),
                ),
            ] {
                let predicates = [
                    NumericPredicate::Equals(hit),
                    NumericPredicate::NotEquals(hit),
                    NumericPredicate::InSet(vec![hit, 0, hit + 1]),
                    NumericPredicate::Range {
                        lower: Some(NumericBound {
                            inclusive: true,
                            value: hit,
                        }),
                        upper: None,
                    },
                ];
                for predicate in &predicates {
                    assert_eq!(
                        filter_delta_block(&body, predicate, kind).unwrap(),
                        predicate.filter_decoded(&column),
                        "width {width}, count {count}, {kind:?}, {predicate:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn delta_filter_keeps_its_stream_validation() {
    let predicate = NumericPredicate::NotEquals(0);
    let body = delta_body(7, &[2; 2048], 2, 1500);
    assert_eq!(
        filter_delta_block(&body, &predicate, ValueKind::U64).unwrap().len(),
        1501
    );

    let mut too_wide = body.clone();
    too_wide[12] = 65;
    assert!(filter_delta_block(&too_wide, &predicate, ValueKind::U64).is_err());
    assert!(filter_delta_block(&body[..10], &predicate, ValueKind::U64).is_err());
    let mut three_vectors = body.clone();
    three_vectors[8..12].copy_from_slice(&2100u32.to_le_bytes());
    assert!(filter_delta_block(&three_vectors, &predicate, ValueKind::U64).is_err());
    assert!(filter_delta_block(&body[..body.len() - 1], &predicate, ValueKind::U64).is_err());

    let too_many_zeros = delta_body(7, &[], 0, (1 << 24) + 1);
    assert!(filter_delta_block(&too_many_zeros, &predicate, ValueKind::U64).is_err());
    let zeros = delta_body(7, &[], 0, 3);
    assert_eq!(
        filter_delta_block(&zeros, &predicate, ValueKind::U64).unwrap(),
        vec![true; 4]
    );

    let mut trailing = body.clone();
    trailing.extend_from_slice(&[0xFF; 5]);
    assert_eq!(
        filter_delta_block(&trailing, &predicate, ValueKind::U64).unwrap(),
        filter_delta_block(&body, &predicate, ValueKind::U64).unwrap()
    );
}

#[test]
fn decimal128_predicate_kernel_matches_the_reference() {
    let values: Vec<i128> = vec![i128::MIN, -1_000, -1, 0, 1, 1_000, i128::MAX];
    let data = ColumnData::Decimal {
        scale: 2,
        values: values.clone(),
    };
    let block = encode_block(&data, false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Decimal128);

    let predicates = [
        NumericPredicate::Equals(0),
        NumericPredicate::Equals(i128::MIN),
        NumericPredicate::Equals(i128::MAX),
        NumericPredicate::NotEquals(-1),
        NumericPredicate::InSet(vec![i128::MIN, 1, 999]),
        NumericPredicate::InSet(Vec::new()),
        range(Some((i128::MIN, false)), None),
        range(Some((i128::MIN, true)), None),
        range(None, Some((i128::MAX, false))),
        range(None, Some((i128::MAX, true))),
        range(Some((-1_000, true)), Some((1_000, false))),
        range(Some((-1_000, false)), Some((-1_000, false))),
    ];
    for predicate in &predicates {
        let mask = filter_numeric_block(block.pipeline, &block.bytes, predicate)
            .unwrap()
            .expect("Decimal128 answered from the stored mantissa");
        assert_eq!(mask, predicate.filter_decoded(&data), "diverged for {predicate:?}");
    }
}

#[test]
fn u128_beyond_i128_max_behaves_like_positive_infinity() {
    let huge = i128::MAX as u128 + 1;
    let data = ColumnData::U128(vec![7, huge, u128::MAX, 3]);

    let lower_only = NumericPredicate::Range {
        lower: Some(NumericBound {
            inclusive: true,
            value: 5,
        }),
        upper: None,
    };
    assert_eq!(lower_only.filter_decoded(&data), vec![true, true, true, false]);

    let bounded = NumericPredicate::Range {
        lower: None,
        upper: Some(NumericBound {
            inclusive: true,
            value: i128::MAX,
        }),
    };
    assert_eq!(bounded.filter_decoded(&data), vec![true, false, false, true]);

    assert_eq!(
        NumericPredicate::Equals(i128::MAX).filter_decoded(&data),
        vec![false, false, false, false]
    );
    assert_eq!(
        NumericPredicate::NotEquals(i128::MAX).filter_decoded(&data),
        vec![true, true, true, true]
    );
}

#[test]
fn a_forged_for_block_that_overflows_the_base_is_rejected_like_the_full_decode() {
    let mut body = Writer::new();
    body.put_u64(u64::MAX);
    crate::encoding::bitpack(&[1u64], 1, &mut body);
    let bytes = body.into_bytes();

    let pipeline = PipelineId::new(Transform::ForBitpack, Compression::None, ValueKind::U64);
    assert!(filter_numeric_block(pipeline, &bytes, &NumericPredicate::Equals(0)).is_err());
    assert!(decode_block(pipeline, &bytes).is_err());
}

fn for_u64_block(values: &[u64]) -> (PipelineId, Vec<u8>) {
    let mut body = Writer::new();
    crate::encoding::encode_for_bitpack(values, &mut body);
    (
        PipelineId::new(Transform::ForBitpack, Compression::None, ValueKind::U64),
        body.into_bytes(),
    )
}

fn for_i64_block(values: &[i64]) -> (PipelineId, Vec<u8>) {
    let codes: Vec<u64> = values.iter().map(|v| crate::encoding::zigzag(*v)).collect();
    let mut body = Writer::new();
    crate::encoding::encode_for_bitpack(&codes, &mut body);
    (
        PipelineId::new(Transform::ForBitpack, Compression::None, ValueKind::I64),
        body.into_bytes(),
    )
}

fn range(lower: Option<(i128, bool)>, upper: Option<(i128, bool)>) -> NumericPredicate {
    NumericPredicate::Range {
        lower: lower.map(|(value, inclusive)| NumericBound { inclusive, value }),
        upper: upper.map(|(value, inclusive)| NumericBound { inclusive, value }),
    }
}

fn assert_for_block_matches_reference(pipeline: PipelineId, bytes: &[u8], predicates: &[NumericPredicate]) {
    let data = decode_block(pipeline, bytes).unwrap();
    for predicate in predicates {
        let mask = filter_numeric_block(pipeline, bytes, predicate)
            .unwrap()
            .expect("FOR blocks answer from packed lanes");
        assert_eq!(
            mask,
            predicate.filter_decoded(&data),
            "packed-domain answer diverged for {predicate:?}"
        );
    }
}

#[test]
fn for_encoded_u64_answers_every_predicate_shape_over_packed_lanes() {
    let mut state: u64 = 0xA076_1D64_78BD_642F;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let base = 5_000_000_000u64;
    let values: Vec<u64> = (0..2500).map(|_| base + (next() % 100_000)).collect();
    let hit = values[1701] as i128;
    let (pipeline, bytes) = for_u64_block(&values);
    let predicates = vec![
        NumericPredicate::Equals(hit),
        NumericPredicate::Equals(3),
        NumericPredicate::Equals(-7),
        NumericPredicate::Equals(i128::from(u64::MAX) + 5),
        NumericPredicate::NotEquals(hit),
        NumericPredicate::NotEquals(-1),
        NumericPredicate::InSet(vec![hit, hit, 3, -9, i128::from(u64::MAX) + 1, values[40] as i128]),
        NumericPredicate::InSet(Vec::new()),
        range(Some((hit, true)), Some((hit + 5_000, false))),
        range(Some((hit, false)), Some((hit + 5_000, true))),
        range(None, Some((hit, true))),
        range(Some((hit, true)), None),
        range(Some((-50, true)), Some((3, true))),
        range(Some((10, true)), Some((5, true))),
        range(Some((i128::MIN, false)), Some((i128::MAX, false))),
        range(None, None),
    ];
    assert_for_block_matches_reference(pipeline, &bytes, &predicates);
}

#[test]
fn for_encoded_i64_splits_signed_ranges_across_the_zigzag_parity() {
    let mut state: u64 = 0x8500_92C7_44FD_92B5;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let mut values: Vec<i64> = (0..2500).map(|_| (next() % 200_000) as i64 - 100_000).collect();
    values.extend_from_slice(&[i64::MIN, i64::MAX, -1, 0, 1]);
    let hit = i128::from(values[901]);
    let (pipeline, bytes) = for_i64_block(&values);
    let predicates = vec![
        NumericPredicate::Equals(hit),
        NumericPredicate::Equals(i128::from(i64::MIN)),
        NumericPredicate::Equals(i128::from(i64::MAX)),
        NumericPredicate::Equals(i128::from(i64::MAX) + 1),
        NumericPredicate::NotEquals(hit),
        NumericPredicate::NotEquals(i128::MIN),
        NumericPredicate::InSet(vec![hit, 0, -1, i128::from(i64::MIN), i128::from(u64::MAX) + 9]),
        range(Some((-500, true)), Some((500, true))),
        range(Some((-500, false)), Some((500, false))),
        range(Some((-99_000, true)), Some((-3, false))),
        range(Some((3, false)), Some((99_000, true))),
        range(None, Some((0, true))),
        range(Some((0, false)), None),
        range(Some((i128::from(i64::MIN), true)), Some((i128::from(i64::MIN), true))),
        range(Some((i128::MAX - 1, true)), None),
        range(None, None),
    ];
    assert_for_block_matches_reference(pipeline, &bytes, &predicates);
}

#[test]
fn a_zero_width_for_block_answers_from_the_base_alone() {
    let values = vec![42u64; 300];
    let (pipeline, bytes) = for_u64_block(&values);
    let predicates = vec![
        NumericPredicate::Equals(42),
        NumericPredicate::Equals(41),
        NumericPredicate::NotEquals(42),
        range(Some((42, false)), None),
        range(None, Some((42, true))),
    ];
    assert_for_block_matches_reference(pipeline, &bytes, &predicates);
}

#[test]
fn alp_filter_matches_decode_then_filter_including_exceptions_and_escapes() {
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let mut values: Vec<f64> = Vec::new();
    for i in 0..1024u64 {
        if i % 97 == 3 {
            values.push(std::f64::consts::PI + i as f64);
        } else {
            values.push(((next() % 400_000) as i64 - 200_000) as f64 / 100.0);
        }
    }
    for _ in 0..1024 {
        values.push(f64::from_bits(next()));
    }
    for _ in 0..500 {
        values.push(((next() % 400_000) as i64 - 200_000) as f64 / 100.0);
    }
    values[0] = 1_234.56;
    values[2070] = -0.0;
    values[2071] = 0.0;
    let block = encode_block(&ColumnData::F64(values.clone()), false);
    assert_eq!(
        block.pipeline.transform().unwrap(),
        Transform::Alp,
        "fixture must pick ALP"
    );
    let body = crate::encoding::remove_trailing(block.pipeline.compression().unwrap(), &block.bytes).unwrap();
    assert_eq!(
        body.first().copied(),
        Some(crate::encoding::constant::ALP_VECTOR_ESCAPE_SENTINEL),
        "the noise vector must escape, or this test loses its raw-payload coverage"
    );
    let data = decode_block(block.pipeline, &block.bytes).unwrap();

    let float_range = |lower: Option<(f64, bool)>, upper: Option<(f64, bool)>| FloatPredicate::Range {
        lower: lower.map(|(value, inclusive)| FloatBound { inclusive, value }),
        upper: upper.map(|(value, inclusive)| FloatBound { inclusive, value }),
    };
    let predicates = vec![
        FloatPredicate::Equals(1_234.56),
        FloatPredicate::Equals(1_234.567),
        FloatPredicate::Equals(values[314]),
        FloatPredicate::Equals(values[1500]),
        FloatPredicate::Equals(0.0),
        FloatPredicate::Equals(-0.0),
        FloatPredicate::Equals(f64::NAN),
        FloatPredicate::Equals(f64::INFINITY),
        FloatPredicate::NotEquals(1_234.56),
        FloatPredicate::NotEquals(f64::NAN),
        FloatPredicate::InSet(vec![1_234.56, values[1500], f64::NAN, -0.0]),
        FloatPredicate::InSet(Vec::new()),
        FloatPredicate::InSet(vec![
            values[2400],
            values[10],
            values[2400],
            0.0,
            -0.0,
            values[10],
            f64::INFINITY,
        ]),
        FloatPredicate::InSet(vec![0.01, 0.02, 0.03, 0.0, f64::NAN, 0.02]),
        FloatPredicate::InSet(values.iter().step_by(7).rev().copied().collect()),
        FloatPredicate::InSet(
            values
                .iter()
                .step_by(3)
                .chain(values.iter().step_by(5))
                .copied()
                .collect(),
        ),
        float_range(Some((-350.0, true)), Some((350.0, false))),
        float_range(Some((-350.0, false)), Some((350.0, true))),
        float_range(None, Some((0.0, true))),
        float_range(Some((0.0, false)), None),
        float_range(Some((f64::NAN, true)), None),
        float_range(None, Some((f64::NAN, false))),
        float_range(Some((f64::NEG_INFINITY, true)), Some((f64::INFINITY, true))),
        float_range(Some((1_000.0, true)), Some((1.0, true))),
        float_range(None, None),
    ];
    for predicate in &predicates {
        let mask = filter_float_block(block.pipeline, &block.bytes, predicate)
            .unwrap()
            .expect("ALP blocks answer in the integer domain");
        let reference = match &data {
            ColumnData::F64(decoded) => predicate.filter_decoded(decoded),
            other => panic!("expected floats, got {other:?}"),
        };
        assert_eq!(mask, reference, "integer-domain answer diverged for {predicate:?}");
    }
}

#[test]
fn alp_in_sets_translate_to_merged_intervals() {
    let power = ALP_POWERS[2];
    let test = AlpIntTest::translate(
        &FloatPredicate::InSet(vec![0.05, 0.01, 0.02, 0.01, f64::NAN, -0.03]),
        power,
    );
    assert!(!test.negate);
    assert_eq!(test.ranges, vec![(-3, -3), (1, 2), (5, 5)]);
}

fn framed_notes(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| {
            if i % 5 == 0 {
                None
            } else if i % 4_000 == 1 {
                Some(format!(
                    "row {i} escalated to the regulator-callback after a threshold breach"
                ))
            } else if i % 97 == 0 {
                Some(format!(
                    "row {i} raised on the escalation-path after a threshold breach"
                ))
            } else {
                Some(format!("row {i} settled without incident on the standard path"))
            }
        })
        .collect()
}

fn framed_short(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| (i % 7 != 0).then(|| format!("id-{i:06}{}", "-".repeat(i % 5))))
        .collect()
}

fn values_straddling_frames(bytes: &[u8]) -> usize {
    let table = super::super::seekable_zstd::seek_table(bytes).unwrap();
    let body = super::super::remove_trailing(Compression::SeekableZstd, bytes).unwrap();
    let mut reader = Reader::new(&body);
    let header = super::super::read_string_header(Transform::FsstString, &mut reader).unwrap();
    let offsets = super::super::read_offsets(&mut reader, header.present_count).unwrap();
    let data_start = header.offsets_start + (header.present_count + 1) * 4;
    (1..table.num_frames())
        .map(|frame| table.frame_start_decomp(frame).unwrap() as usize)
        .filter(|boundary| {
            offsets
                .windows(2)
                .any(|pair| data_start + pair[0] < *boundary && *boundary < data_start + pair[1])
        })
        .count()
}

#[test]
fn a_framed_fsst_block_counts_substrings_frame_by_frame_like_the_whole_block_kernel() {
    let cases = [
        (
            framed_notes(24_000),
            SideStream::FsstValueKeys,
            vec![
                "escalation-path",
                "regulator-callback",
                "quicksilver-ratchet",
                "",
                "row",
            ],
        ),
        (
            framed_short(40_000),
            SideStream::None,
            vec!["id-0001", "id-039999", "zz", "", "id-"],
        ),
    ];
    for (values, side, needles) in cases {
        let column: StringColumn = values.into();
        let mut block = encode_block_forced_for_conformance(
            &ColumnData::Strings(column.clone()),
            true,
            CascadeStrategy::DecodeOptimized,
            Transform::FsstString,
        )
        .unwrap();
        assert_eq!(block.pipeline.transform().unwrap(), Transform::FsstString);
        if block.pipeline.compression().unwrap() == Compression::None {
            block.bytes = super::super::seekable_zstd::compress(&block.bytes).unwrap();
            block.pipeline = PipelineId::new(Transform::FsstString, Compression::SeekableZstd, ValueKind::String)
                .with_side_stream(side);
        }
        assert_eq!(block.pipeline.compression().unwrap(), Compression::SeekableZstd);
        assert_eq!(block.pipeline.side_stream().unwrap(), side);
        let frames = super::super::seekable_zstd::seek_table(&block.bytes)
            .unwrap()
            .num_frames();
        assert!(
            frames >= 3,
            "{side:?}: the block must span several frames, not {frames}"
        );
        assert!(
            values_straddling_frames(&block.bytes) > 0,
            "{side:?}: some value's codes must cross a frame boundary"
        );
        let body = super::super::remove_trailing(Compression::SeekableZstd, &block.bytes).unwrap();
        let mut predicates: Vec<StringPredicate> = needles.iter().map(|needle| contains(needle)).collect();
        predicates.push(contains_all(&[needles[0], "threshold"], false));
        predicates.push(contains_all(&[needles[0].to_ascii_uppercase().as_str()], true));
        for predicate in &predicates {
            let StringPredicate::Contains(matcher) = predicate else {
                unreachable!("every predicate here is a substring test");
            };
            let reference = predicate.filter_decoded(&column).iter().filter(|hit| **hit).count();
            let framed = count_string_block_shared(block.pipeline, &block.bytes, predicate, None)
                .unwrap()
                .expect("the framed kernel answers a substring test");
            assert_eq!(framed, reference, "{side:?}: framed count for {predicate:?}");
            assert_eq!(
                count_fsst_contains(&body, side, matcher).unwrap(),
                reference,
                "{side:?}: whole-block count for {predicate:?}"
            );
        }
    }
}

#[test]
fn a_framed_contains_scan_leaves_the_prefix_key_frames_compressed() {
    let column: StringColumn = framed_notes(24_000).into();
    let block = encode_block(&ColumnData::Strings(column.clone()), true);
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);
    let table = super::super::seekable_zstd::seek_table(&block.bytes).unwrap();
    let body = super::super::remove_trailing(Compression::SeekableZstd, &block.bytes).unwrap();
    let mut reader = Reader::new(&body);
    let header = super::super::read_string_header(Transform::FsstString, &mut reader).unwrap();
    let offsets = super::super::read_offsets(&mut reader, header.present_count).unwrap();
    let data_start = header.offsets_start + (header.present_count + 1) * 4;
    let prefixes_start = data_start + offsets.last().copied().unwrap();
    let fingerprints_start = prefixes_start + header.present_count * PREFIX_KEY_BYTES;
    let frames_over = |start: usize, end: usize| -> BTreeSet<u32> {
        (table.frame_index_decomp(start as u64)..=table.frame_index_decomp(end as u64 - 1)).collect()
    };
    for run_start in [prefixes_start, fingerprints_start] {
        let frame = table.frame_index_decomp(run_start as u64);
        assert_eq!(
            table.frame_start_decomp(frame).unwrap(),
            run_start as u64,
            "a frame starts where the run at {run_start} does"
        );
    }
    let prefix_frames = frames_over(prefixes_start, fingerprints_start);
    let fingerprint_frames = frames_over(fingerprints_start, body.len());
    assert!(prefix_frames.is_disjoint(&fingerprint_frames));
    assert!(prefix_frames.len() >= 2, "the prefix keys span frames of their own");
    let last_hit = column
        .iter_present()
        .enumerate()
        .filter(|(_, value)| value.contains("escalation-path"))
        .map(|(index, _)| index)
        .last()
        .unwrap();
    let dense = frames_over(0, data_start + offsets[last_hit + 1])
        .union(&fingerprint_frames)
        .count();
    let pruned = frames_over(0, data_start).union(&fingerprint_frames).count();
    assert!(dense < table.num_frames() as usize && pruned < dense);
    for (needle, expected) in [("escalation-path", dense), ("zz", pruned)] {
        let predicate = contains(needle);
        let reference = predicate.filter_decoded(&column).iter().filter(|hit| **hit).count();
        super::super::seekable_zstd::take_frames_inflated();
        let count = count_string_block_shared(block.pipeline, &block.bytes, &predicate, None)
            .unwrap()
            .unwrap();
        assert_eq!(count, reference, "{needle}: count");
        assert_eq!(
            super::super::seekable_zstd::take_frames_inflated(),
            expected,
            "{needle}: frames inflated"
        );
    }
}

#[test]
fn a_damaged_framed_fsst_block_is_refused_by_the_frame_by_frame_count() {
    let block = encode_block(&ColumnData::Strings(framed_notes(24_000).into()), true);
    let frames_len = super::super::seekable_zstd::seek_table(&block.bytes)
        .unwrap()
        .size_comp() as usize;
    let predicate = contains("escalation-path");

    let mut zeroed = block.bytes.clone();
    zeroed[..frames_len].fill(0);
    assert!(count_string_block_shared(block.pipeline, &zeroed, &predicate, None).is_err());

    let mut cut = block.bytes[..frames_len - 10].to_vec();
    cut.extend_from_slice(&block.bytes[frames_len..]);
    assert!(count_string_block_shared(block.pipeline, &cut, &predicate, None).is_err());

    assert!(count_string_block_shared(block.pipeline, &block.bytes[..frames_len], &predicate, None).is_err());
}

#[test]
fn a_scan_fans_out_only_over_several_granules_worth_a_worker_each() {
    assert!(!scan_in_parallel(1, u64::MAX));
    assert!(!scan_in_parallel(8, PARALLEL_SCAN_MIN_GRANULE_BYTES - 1));
    assert!(scan_in_parallel(2, PARALLEL_SCAN_MIN_GRANULE_BYTES));
}

#[test]
fn multi_needle_matching_agrees_with_per_needle_checks_across_mark_sizes() {
    let contains_each = |needles: &[String], value: &str, fold: bool| {
        let value = if fold {
            value.to_ascii_lowercase()
        } else {
            value.to_owned()
        };
        needles.iter().all(|needle| {
            let needle = if fold {
                needle.to_ascii_lowercase()
            } else {
                needle.clone()
            };
            value.contains(&needle)
        })
    };
    let all: String = (0..200).map(|i| format!("<{i}>")).collect();
    let values = [
        String::new(),
        all.clone(),
        all.to_ascii_uppercase(),
        all.replace("<64>", ""),
        format!("\u{e9}\u{e9}{all}\u{1F600}"),
        "abcabc".to_owned(),
    ];
    for count in [1usize, 2, 63, 64, 65, 130] {
        let mut needles: Vec<String> = (0..count).map(|i| format!("<{}>", i % 66)).collect();
        if count > 1 {
            needles[count - 1] = String::new();
        }
        for case_insensitive in [false, true] {
            let matcher = ContainsMatcher::new(needles.clone(), case_insensitive);
            for value in &values {
                assert_eq!(
                    matcher.matches(value),
                    contains_each(&needles, value, case_insensitive),
                    "{count} needles, case-insensitive {case_insensitive}"
                );
            }
        }
    }
    let overlapping = ContainsMatcher::new(vec!["abca".to_owned(), "cab".to_owned(), "\u{e9}".to_owned()], true);
    assert!(!overlapping.matches("abcabc"));
    assert!(overlapping.matches("ABCABC\u{e9}"));
}

fn merge_intervals_by_copying<T: Copy + Ord>(mut intervals: Vec<(T, T)>, after: fn(T) -> T) -> Vec<(T, T)> {
    intervals.sort_unstable_by_key(|&(lo, _)| lo);
    let mut merged: Vec<(T, T)> = Vec::with_capacity(intervals.len());
    for (lo, hi) in intervals {
        match merged.last_mut() {
            Some((_, last_hi)) if lo <= after(*last_hi) => {
                if hi > *last_hi {
                    *last_hi = hi;
                }
            }
            _ => merged.push((lo, hi)),
        }
    }
    merged
}

fn interval_cases<T: Copy + Ord>(points: &[T]) -> Vec<Vec<(T, T)>> {
    let ordered = |a: T, b: T| if a <= b { (a, b) } else { (b, a) };
    let mut cases: Vec<Vec<(T, T)>> = vec![Vec::new()];
    for &a in points {
        cases.push(vec![(a, a)]);
        cases.push(vec![(a, a), (a, a)]);
        for &b in points {
            cases.push(vec![ordered(a, b)]);
            cases.push(vec![ordered(a, b), ordered(a, b)]);
            cases.push(vec![(a, a), ordered(a, b)]);
            cases.push(vec![ordered(b, a), (b, b), (a, a)]);
        }
    }
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for len in 0..40 {
        for _ in 0..25 {
            let case = (0..len)
                .map(|_| {
                    let a = points[(next() % points.len() as u64) as usize];
                    let b = points[(next() % points.len() as u64) as usize];
                    ordered(a, b)
                })
                .collect();
            cases.push(case);
        }
    }
    cases
}

#[test]
fn merging_intervals_in_place_matches_merging_by_copying() {
    let u64_points = [
        u64::MIN,
        1,
        2,
        3,
        4,
        5,
        7,
        8,
        10,
        11,
        100,
        u64::MAX / 2,
        u64::MAX - 2,
        u64::MAX - 1,
        u64::MAX,
    ];
    for case in interval_cases(&u64_points) {
        let expected = merge_intervals_by_copying(case.clone(), |hi: u64| hi.saturating_add(1));
        assert_eq!(merge_intervals(case.clone()), expected, "u64 input {case:?}");
    }

    let i128_points = [
        i128::MIN,
        i128::MIN + 1,
        i128::MIN + 2,
        -5,
        -2,
        -1,
        0,
        1,
        2,
        3,
        5,
        6,
        i128::MAX - 2,
        i128::MAX - 1,
        i128::MAX,
    ];
    for case in interval_cases(&i128_points) {
        let expected = merge_intervals_by_copying(case.clone(), |hi: i128| hi.saturating_add(1));
        assert_eq!(merge_intervals_i128(case.clone()), expected, "i128 input {case:?}");
    }
}

fn reference_weave_present(nulls: &NullStream, present: &[bool]) -> Result<Vec<bool>, FormatError> {
    let mut mask = Vec::with_capacity(nulls.row_count());
    let mut verdicts = present.iter();
    nulls.each_row(|set| {
        if set {
            let verdict = verdicts.next().ok_or(FormatError::Structural {
                rule: "null bitmap disagrees with present count",
            })?;
            mask.push(*verdict);
        } else {
            mask.push(false);
        }
        Ok(())
    })?;
    if verdicts.next().is_some() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    Ok(mask)
}

type NullShape = (&'static str, fn(usize) -> bool);

fn null_shapes() -> [NullShape; 4] {
    [
        ("all absent", |_| false),
        ("all present", |_| true),
        ("clustered", |i| (i / 100) % 3 != 1),
        ("scattered", |i| i % 2 == 0),
    ]
}

fn null_streams_of_every_form() -> Vec<(String, NullStream<'static>)> {
    let mut streams = vec![(
        "all present x0".to_owned(),
        NullStream {
            form: NullStreamForm::AllPresent,
            row_count: 0,
        },
    )];
    for rows in [1usize, 2, 1023, 1024, 1025] {
        for (shape, present) in null_shapes() {
            let values: StringColumn = (0..rows).map(|i| present(i).then(|| format!("v{i}"))).collect();
            streams.push((format!("{shape} x{rows}"), NullStream::from_values(&values)));
        }
    }
    streams
}

#[test]
fn weaving_owned_verdicts_matches_the_copying_weave_for_every_null_form() {
    let mut forms = BTreeSet::new();
    for (name, nulls) in null_streams_of_every_form() {
        forms.insert(match nulls.form {
            NullStreamForm::AllAbsent => "all absent",
            NullStreamForm::AllPresent => "all present",
            NullStreamForm::Raw(_) => "raw",
            NullStreamForm::Runs(_) => "runs",
        });
        let present_count = nulls.present_count();
        for len in [present_count.saturating_sub(1), present_count, present_count + 1] {
            let verdict_shapes: [fn(usize) -> bool; 3] = [|_| true, |_| false, |i| i % 3 == 0];
            for verdict in verdict_shapes {
                let verdicts: Vec<bool> = (0..len).map(verdict).collect();
                assert_eq!(
                    weave_present(&nulls, verdicts.clone()),
                    reference_weave_present(&nulls, &verdicts),
                    "{name}: {len} verdicts for {present_count} present rows"
                );
            }
        }
    }
    assert_eq!(forms.len(), 4, "every null form is exercised: {forms:?}");
}

#[test]
fn weaving_an_all_present_block_returns_the_verdict_vector_itself() {
    for rows in [1usize, 2, 1023, 1024, 1025] {
        let values: StringColumn = (0..rows).map(|i| Some(format!("v{i}"))).collect();
        let nulls = NullStream::from_values(&values);
        assert!(matches!(nulls.form, NullStreamForm::AllPresent), "x{rows}: all present");
        let mut verdicts = Vec::with_capacity(rows + 64);
        verdicts.extend((0..rows).map(|i| i % 3 == 0));
        let expected = verdicts.clone();
        let (pointer, capacity) = (verdicts.as_ptr(), verdicts.capacity());
        let mask = weave_present(&nulls, verdicts).unwrap();
        assert_eq!(mask.as_ptr(), pointer, "x{rows}: same allocation");
        assert_eq!(mask.capacity(), capacity, "x{rows}: same capacity");
        assert_eq!(mask, expected, "x{rows}: same verdicts");
        for len in [rows - 1, rows + 1] {
            assert_eq!(
                weave_present(&nulls, vec![true; len]),
                Err(FormatError::Structural {
                    rule: "null bitmap disagrees with present count",
                }),
                "x{rows}: {len} verdicts refused"
            );
        }
    }
}

#[test]
fn string_predicate_masks_agree_with_decoding_under_every_null_form() {
    let datasets: [fn(usize) -> String; 4] = [
        |i| ["apple", "banana", "", "d\u{e9}j\u{e0}"][i % 4].to_owned(),
        |i| format!("id-{i:05}"),
        |i| format!("{i:05}-order-line-reference-{i:05}"),
        |i| format!("{}-{i}", "x".repeat(80)),
    ];
    let predicates = [
        equals("banana"),
        equals("id-00007"),
        StringPredicate::NotEquals("apple".to_owned()),
        StringPredicate::InSet(vec!["apple".to_owned(), "id-00002".to_owned(), "apple".to_owned()]),
        contains("an"),
        contains("0"),
        contains(""),
        contains("\u{e9}"),
        StringPredicate::Range {
            lower: Some(bound("00100", true)),
            upper: Some(bound("id-00500", false)),
        },
    ];
    for rows in [1usize, 2, 1023, 1024, 1025] {
        for (_, present) in null_shapes() {
            for value in datasets {
                let values: Vec<Option<String>> = (0..rows).map(|i| present(i).then(|| value(i))).collect();
                for predicate in &predicates {
                    for random_access in [false, true] {
                        check(&values, predicate, random_access);
                    }
                }
            }
        }
    }
}

fn lcg(seed: u64) -> impl FnMut() -> u64 {
    let mut state = seed;
    move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 11
    }
}

fn assert_for_block_matches_i128(pipeline: PipelineId, bytes: &[u8], values: &[i128], predicates: &[NumericPredicate]) {
    for predicate in predicates {
        let mask = filter_numeric_block(pipeline, bytes, predicate)
            .unwrap()
            .expect("FOR blocks answer from packed lanes");
        let reference: Vec<bool> = values.iter().map(|v| predicate.matches_i128(*v)).collect();
        assert_eq!(mask, reference, "packed lane kernel diverged for {predicate:?}");
    }
}

#[test]
fn specialised_packed_lane_kernels_agree_with_the_reference() {
    let mut next = lcg(0x5851_F42D_4C95_7F2D);
    let base = 1_000_000u64;
    let values: Vec<u64> = (0..3077).map(|_| base + next() % 5_000).collect();
    let wide: Vec<i128> = values.iter().map(|v| i128::from(*v)).collect();
    let hit = wide[2900];
    let set = |len: i128| NumericPredicate::InSet((0..len).map(|i| wide[0] + i * 37 - 200).collect());
    let predicates = vec![
        NumericPredicate::Equals(hit),
        NumericPredicate::Equals(i128::from(base) - 1),
        NumericPredicate::Equals(i128::from(u64::MAX)),
        NumericPredicate::NotEquals(hit),
        NumericPredicate::NotEquals(i128::from(u64::MAX) + 1),
        range(Some((hit - 300, true)), Some((hit + 300, false))),
        range(Some((hit, true)), Some((hit, true))),
        range(Some((0, true)), Some((i128::from(u64::MAX), true))),
        range(Some((i128::from(u64::MAX), false)), None),
        range(Some((i128::MIN, false)), Some((i128::from(base) + 2, false))),
        range(Some((hit, true)), Some((hit - 1, true))),
        set(2),
        set(4),
        set(5),
        set(16),
        set(17),
        set(40),
        NumericPredicate::InSet(vec![
            hit,
            hit + 1,
            hit + 2,
            wide[7],
            wide[7] + 1,
            -5,
            i128::from(u64::MAX) + 3,
        ]),
        NumericPredicate::InSet(Vec::new()),
    ];
    let (pipeline, bytes) = for_u64_block(&values);
    assert_for_block_matches_i128(pipeline, &bytes, &wide, &predicates);

    let signed: Vec<i64> = (0..3077).map(|_| (next() % 6_000) as i64 - 3_000).collect();
    let wide: Vec<i128> = signed.iter().map(|v| i128::from(*v)).collect();
    let hit = wide[3070];
    let set = |len: i128| NumericPredicate::InSet((0..len).map(|i| i * 41 - 400).collect());
    let predicates = vec![
        NumericPredicate::Equals(hit),
        NumericPredicate::Equals(-hit - 1),
        NumericPredicate::NotEquals(hit),
        NumericPredicate::NotEquals(i128::from(i64::MIN) - 1),
        range(Some((-700, true)), Some((900, false))),
        range(Some((-700, false)), Some((-2, true))),
        range(Some((0, true)), Some((1_500, true))),
        range(Some((i128::from(i64::MIN), true)), None),
        range(None, Some((i128::from(i64::MAX), false))),
        range(Some((i128::MIN, true)), Some((i128::MIN, false))),
        range(Some((i128::MAX, false)), None),
        range(Some((5, true)), Some((5, true))),
        set(3),
        set(4),
        set(9),
        set(16),
        set(25),
    ];
    let (pipeline, bytes) = for_i64_block(&signed);
    assert_for_block_matches_i128(pipeline, &bytes, &wide, &predicates);
}

#[test]
fn mixed_delta_points_and_zigzag_ranges_fall_back_to_the_search() {
    let mut next = lcg(0x2127_599B_F432_5C37);
    let deltas: Vec<u64> = (0..1500).map(|_| next() % 512).collect();
    let mut body = Writer::new();
    bitpack(&deltas, 9, &mut body);
    let bytes = body.into_bytes();
    let stream = read_packed_stream(&mut Reader::new(&bytes)).unwrap();
    let base = 40;
    for negate in [false, true] {
        let test = PackedLaneTest {
            negate,
            ranges: MergedLaneRanges::from_ranges(vec![
                PackedLaneRange::Delta { hi: 7, lo: 7 },
                PackedLaneRange::Zigzag {
                    even: (100, 300),
                    odd: (151, 401),
                },
            ]),
        };
        let mut out = vec![false; stream.padded_count()];
        test.apply(&stream, base, &mut out);
        out.truncate(deltas.len());
        let expected: Vec<bool> = deltas.iter().map(|delta| test.matches(base, *delta)).collect();
        assert_eq!(out, expected);
    }
}

#[test]
fn decoded_integer_filters_agree_with_the_reference() {
    let mut next = lcg(0x9FB2_1C65_1E98_DF25);
    let mut unsigned: Vec<u64> = (0..3001).map(|_| next() % 4_000).collect();
    unsigned.extend_from_slice(&[0, 1, u64::MAX, u64::MAX - 1, 1 << 63]);
    let mut signed: Vec<i64> = (0..3001).map(|_| (next() % 4_000) as i64 - 2_000).collect();
    signed.extend_from_slice(&[i64::MIN, i64::MIN + 1, i64::MAX, i64::MAX - 1, -1, 0, 1]);
    let set = |len: i128, start: i128| NumericPredicate::InSet((0..len).map(|i| start + i * 53).collect());
    let extremes = [
        i128::MIN,
        i128::from(i64::MIN) - 1,
        i128::from(i64::MIN),
        -1,
        0,
        i128::from(i64::MAX),
        i128::from(u64::MAX),
        i128::from(u64::MAX) + 1,
        i128::MAX,
    ];
    let mut predicates = vec![
        NumericPredicate::InSet(Vec::new()),
        NumericPredicate::InSet(vec![-1, i128::from(u64::MAX) + 1, i128::MIN]),
        NumericPredicate::InSet(vec![
            5,
            6,
            7,
            8,
            -3,
            -2,
            100,
            i128::from(i64::MIN),
            i128::from(u64::MAX),
        ]),
        range(None, None),
        range(Some((10, true)), Some((9, true))),
        range(Some((-300, true)), Some((900, false))),
        range(Some((-300, false)), Some((-300, true))),
    ];
    for len in [2, 4, 5, 16, 17, 40] {
        predicates.push(set(len, -900));
        predicates.push(set(len, 100));
    }
    for &n in &extremes {
        predicates.push(NumericPredicate::Equals(n));
        predicates.push(NumericPredicate::NotEquals(n));
        for inclusive in [false, true] {
            predicates.push(range(Some((n, inclusive)), None));
            predicates.push(range(None, Some((n, inclusive))));
            predicates.push(range(Some((n, inclusive)), Some((1_000, true))));
            predicates.push(range(Some((-1_000, true)), Some((n, inclusive))));
        }
    }
    for value in [unsigned[17], unsigned[2999]] {
        predicates.push(NumericPredicate::Equals(i128::from(value)));
        predicates.push(NumericPredicate::NotEquals(i128::from(value)));
    }
    for value in [signed[17], signed[2999]] {
        predicates.push(NumericPredicate::Equals(i128::from(value)));
        predicates.push(NumericPredicate::NotEquals(i128::from(value)));
    }
    let unsigned_data = ColumnData::U64(unsigned.clone());
    let signed_data = ColumnData::I64(signed.clone());
    for predicate in &predicates {
        let expected: Vec<bool> = unsigned
            .iter()
            .map(|v| predicate.matches_i128(i128::from(*v)))
            .collect();
        assert_eq!(
            predicate.filter_decoded(&unsigned_data),
            expected,
            "u64 diverged for {predicate:?}"
        );
        let expected: Vec<bool> = signed.iter().map(|v| predicate.matches_i128(i128::from(*v))).collect();
        assert_eq!(
            predicate.filter_decoded(&signed_data),
            expected,
            "i64 diverged for {predicate:?}"
        );
    }
}

#[test]
fn decoded_float_filters_agree_with_the_reference() {
    let mut next = lcg(0x1405_7B7E_F767_814F);
    let specials = [
        f64::NAN,
        -f64::NAN,
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        f64::from_bits(1),
        -f64::from_bits(1),
        f64::MAX,
        f64::MIN,
        1.5,
        -2.25,
    ];
    let mut values: Vec<f64> = (0..3001).map(|_| (next() % 2_000) as f64 / 4.0 - 250.0).collect();
    values.extend((0..500).map(|_| f64::from_bits(next() << 11 ^ next())));
    values.extend_from_slice(&specials);
    let float_range = |lower: Option<(f64, bool)>, upper: Option<(f64, bool)>| FloatPredicate::Range {
        lower: lower.map(|(value, inclusive)| FloatBound { inclusive, value }),
        upper: upper.map(|(value, inclusive)| FloatBound { inclusive, value }),
    };
    let mut predicates = vec![
        FloatPredicate::InSet(Vec::new()),
        FloatPredicate::InSet(vec![f64::NAN]),
        FloatPredicate::InSet(vec![-0.0, 1.5, f64::NAN, values[9]]),
        FloatPredicate::InSet(vec![0.0, -2.25, f64::NAN, values[9], values[10]]),
        FloatPredicate::InSet(values.iter().step_by(150).copied().collect()),
        FloatPredicate::InSet(values.iter().step_by(40).copied().chain(specials).collect()),
        float_range(None, None),
        float_range(Some((-10.0, true)), Some((10.0, false))),
        float_range(Some((10.0, true)), Some((-10.0, true))),
    ];
    for &n in specials.iter().chain(&[values[3], values[3002]]) {
        predicates.push(FloatPredicate::Equals(n));
        predicates.push(FloatPredicate::NotEquals(n));
        for inclusive in [false, true] {
            predicates.push(float_range(Some((n, inclusive)), None));
            predicates.push(float_range(None, Some((n, inclusive))));
            predicates.push(float_range(Some((n, inclusive)), Some((100.0, true))));
            predicates.push(float_range(Some((-100.0, false)), Some((n, inclusive))));
        }
    }
    for predicate in &predicates {
        let expected: Vec<bool> = values.iter().map(|v| predicate.matches_f64(*v)).collect();
        assert_eq!(
            predicate.filter_decoded(&values),
            expected,
            "f64 diverged for {predicate:?}"
        );
    }
}

#[test]
fn large_string_in_lists_agree_with_the_per_value_reference() {
    let mut needles: Vec<String> = (0..300).rev().map(|i| format!("id-{:05}", i * 3)).collect();
    needles.extend(needles.clone());
    needles.push("missing".to_owned());
    needles.push("banana".to_owned());
    let predicate = StringPredicate::InSet(needles);
    for values in [high_cardinality(256), low_cardinality(256)] {
        let column: StringColumn = values.clone().into();
        let reference: Vec<bool> = column.iter().map(|value| predicate.matches_value(value)).collect();
        assert_eq!(predicate.filter_decoded(&column), reference);
        assert!(reference.iter().any(|matched| *matched) && reference.iter().any(|matched| !*matched));
        for random_access in [true, false] {
            check(&values, &predicate, random_access);
        }
    }
    assert!(
        check(&high_cardinality(256), &predicate, true),
        "FSST answers the set from compressed bytes"
    );
}

#[test]
fn a_contiguous_integer_in_list_becomes_one_range_check() {
    let set: Vec<i128> = (10..=60).rev().chain(20..30).collect();
    let test = NativeRangeTest::translate(&NumericPredicate::InSet(set.clone()));
    assert_eq!(test.ranges, vec![(10, 60)]);
    assert!(!test.negate);
    let gapped = NativeRangeTest::translate(&NumericPredicate::InSet(vec![1, 2, 3, 7, 8]));
    assert_eq!(gapped.ranges, vec![(1, 3), (7, 8)]);
    let values: Vec<u64> = (0..100).collect();
    let mask = NumericPredicate::InSet(set).filter_decoded(&ColumnData::U64(values.clone()));
    assert_eq!(mask, values.iter().map(|v| (10..=60).contains(v)).collect::<Vec<_>>());
}

fn mid_cardinality(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| (i % 11 != 5).then(|| format!("!room{:06}:example.org", (i * 7919) % (rows / 3))))
        .collect()
}

#[test]
fn mid_cardinality_strings_win_the_dictionary_trial_and_filter_on_codes() {
    let values = mid_cardinality(4096);
    assert_eq!(transform_of(&values, false), Transform::DictionaryString);
    let present = values.iter().flatten().next().unwrap().clone();
    assert!(check(&values, &equals(&present), false));
    assert!(check(&values, &equals("!room999999:example.org"), false));
    assert!(check(&values, &equals(""), false));
    assert_eq!(transform_of(&values, true), Transform::DictionaryString);
    assert!(check(&values, &equals(&present), true));
}

fn dictionary_body(entries: &[&str], codes: &[u64]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(codes.len() as u32);
    out.put_u8(0);
    out.put_u32(entries.len() as u32);
    let mut offset = 0u32;
    out.put_u32(offset);
    for entry in entries {
        offset += entry.len() as u32;
        out.put_u32(offset);
    }
    for entry in entries {
        out.put_slice(entry.as_bytes());
    }
    bitpack(codes, 2, &mut out);
    out.into_bytes()
}

#[test]
fn an_unsorted_or_repeating_dictionary_is_filtered_like_its_decoded_rows() {
    let pipeline = PipelineId::new(Transform::DictionaryString, Compression::None, ValueKind::String);
    let predicates = [
        equals("a"),
        equals("b"),
        equals("d"),
        StringPredicate::NotEquals("a".to_owned()),
        StringPredicate::NotEquals("c".to_owned()),
        StringPredicate::InSet(vec!["a".to_owned()]),
        StringPredicate::InSet(vec!["c".to_owned(), "a".to_owned(), "zz".to_owned()]),
        StringPredicate::Range {
            lower: Some(bound("a", true)),
            upper: None,
        },
        StringPredicate::Range {
            lower: Some(bound("a", false)),
            upper: Some(bound("c", false)),
        },
        StringPredicate::Range {
            lower: None,
            upper: Some(bound("b", true)),
        },
        contains("a"),
    ];
    for entries in [
        ["b", "a", "c"],
        ["c", "b", "a"],
        ["a", "b", "a"],
        ["a", "a", "a"],
        ["a", "b", "c"],
    ] {
        let body = dictionary_body(&entries, &[0, 1, 2, 2, 1, 0, 1]);
        let rows = decoded(pipeline, &body);
        for predicate in &predicates {
            let reference = predicate.filter_decoded(&rows);
            assert_eq!(
                filter_string_block(pipeline, &body, predicate).unwrap(),
                Some(reference.clone()),
                "{entries:?} {predicate:?}"
            );
            assert_eq!(
                count_string_block_shared(pipeline, &body, predicate, None).unwrap(),
                Some(reference.iter().filter(|matched| **matched).count()),
                "{entries:?} {predicate:?}"
            );
        }
    }
}

#[test]
fn the_dictionary_count_refuses_a_code_stream_its_block_cannot_hold() {
    let pipeline = PipelineId::new(Transform::DictionaryString, Compression::None, ValueKind::String);
    let past_the_dictionary = dictionary_body(&["a", "b"], &[0, 1, 2]);
    assert!(matches!(
        count_string_block_shared(pipeline, &past_the_dictionary, &equals("a"), None),
        Err(FormatError::RefOutOfRange { .. })
    ));
    let mut short = dictionary_body(&["a", "b"], &[0, 1, 1]);
    short[..4].copy_from_slice(&4u32.to_le_bytes());
    assert!(matches!(
        count_string_block_shared(pipeline, &short, &equals("a"), None),
        Err(FormatError::Structural { .. })
    ));
}

#[test]
fn a_forged_run_form_row_count_refuses_the_row_mask() {
    let mut out = Writer::new();
    out.put_u32(u32::MAX);
    out.put_u8(2);
    out.put_u32(1);
    out.put_u32(0);
    out.put_u32(1);
    out.put_u32(1);
    out.put_u32(0);
    out.put_u32(1);
    out.put_slice(b"a");
    out.put_u32(1);
    out.put_u8(0);
    let pipeline = PipelineId::new(Transform::DictionaryString, Compression::None, ValueKind::String);
    assert!(matches!(
        filter_string_block(pipeline, &out.into_bytes(), &equals("a")),
        Err(FormatError::Structural { .. })
    ));
}

#[test]
fn substring_kernels_agree_with_a_per_value_search_for_every_needle_shape() {
    let mut values: Vec<Option<String>> = vec![
        Some("a".to_owned()),
        Some("AAA".to_owned()),
        Some(String::new()),
        Some(String::new()),
        None,
        Some("Alpha Beta".to_owned()),
        Some("beta".to_owned()),
        Some("alp".to_owned()),
        Some("ha betalpha".to_owned()),
        Some(String::new()),
        Some("ALPHABETA alpha".to_owned()),
        Some("b".to_owned()),
        Some("eta".to_owned()),
    ];
    values.extend((0..400).map(|row| match row % 9 {
        0 => None,
        1 => Some(String::new()),
        2 => Some(format!("row {row} Alpha")),
        3 => Some(format!("beta row {row} alpha beta")),
        4 => Some("al".to_owned()),
        5 => Some("pha".to_owned()),
        _ => Some(format!("filler value number {row} with nothing of note")),
    }));
    let column: StringColumn = values.clone().into();
    let needle_sets: [(&[&str], bool); 14] = [
        (&["alpha"], true),
        (&["ALPHA"], true),
        (&["aaa"], true),
        (&["a"], true),
        (&["alpha", "beta"], false),
        (&["alpha", "beta"], true),
        (&["beta", "eta"], false),
        (&["beta", "eta", "a"], true),
        (&["alpha", "alpha"], true),
        (&["", "alpha"], false),
        (&["", "a"], true),
        (&["row", "alpha", "beta", "1"], true),
        (&["absent", "alpha"], false),
        (&["alphabeta", "habet", "a a"], true),
    ];
    for transform in [Transform::RawString, Transform::FsstString, Transform::DictionaryString] {
        let block = encode_block_forced_for_conformance(
            &ColumnData::Strings(column.clone()),
            false,
            CascadeStrategy::NoTrailing,
            transform,
        )
        .unwrap();
        assert_eq!(block.pipeline.transform().unwrap(), transform);
        for (needles, case_insensitive) in needle_sets {
            let predicate = contains_all(needles, case_insensitive);
            let StringPredicate::Contains(matcher) = &predicate else {
                panic!("a substring predicate");
            };
            let reference: Vec<bool> = values
                .iter()
                .map(|value| {
                    value.as_deref().is_some_and(|value| {
                        needles.iter().all(|needle| {
                            if case_insensitive {
                                value.to_ascii_lowercase().contains(&needle.to_ascii_lowercase())
                            } else {
                                value.contains(needle)
                            }
                        })
                    })
                })
                .collect();
            for (value, expected) in values.iter().zip(&reference) {
                if let Some(value) = value {
                    assert_eq!(matcher.matches(value), *expected, "{needles:?} in {value:?}");
                }
            }
            assert_eq!(
                filter_string_block(block.pipeline, &block.bytes, &predicate).unwrap(),
                Some(reference.clone()),
                "{transform:?} {needles:?} case-insensitive {case_insensitive}"
            );
            assert_eq!(
                count_string_block_shared(block.pipeline, &block.bytes, &predicate, None).unwrap(),
                Some(reference.iter().filter(|matched| **matched).count()),
                "{transform:?} {needles:?} case-insensitive {case_insensitive}"
            );
        }
    }
}
