use super::*;
use crate::encoding::predicate::{NumericBound, NumericPredicate, filter_numeric_block};
use crate::encoding::{
    CascadeStrategy, EncodedBlock, decode_block, decode_block_range, decode_block_shared,
    encode_block_forced_for_conformance, encode_block_with_shared_dictionary,
};

fn forced(data: &ColumnData, transform: Transform) -> EncodedBlock {
    let block = encode_block_forced_for_conformance(data, false, CascadeStrategy::NoTrailing, transform).unwrap();
    assert_eq!(block.pipeline.transform().unwrap(), transform);
    block
}

/// Rows with nulls sprinkled in, `distinct` distinct values, each value long enough for FSST to train on.
fn strings(rows: usize, distinct: usize) -> StringColumn {
    (0..rows)
        .map(|row| {
            (row % 7 != 3).then(|| {
                format!(
                    "tenant-{:04}/service-{}/path/{}",
                    (row * 31) % distinct,
                    row % 5,
                    row % distinct
                )
            })
        })
        .collect()
}

fn sample_rows(len: usize) -> Vec<usize> {
    let mut rows: Vec<usize> = (0..len).step_by(97).collect();
    rows.extend([len - 1, 0, len / 2, 3, 3]);
    rows
}

fn oracle_gather(full: &ColumnData, rows: &[usize]) -> ColumnData {
    gather_column(full, rows).unwrap()
}

#[test]
fn take_matches_full_decode_for_every_string_transform() {
    for (transform, distinct) in [
        (Transform::FsstString, 5000),
        (Transform::RawString, 5000),
        (Transform::DictionaryString, 8),
        (Transform::DictionaryString, 600),
    ] {
        let data = ColumnData::Strings(strings(5000, distinct));
        let block = forced(&data, transform);
        let full = decode_block(block.pipeline, &block.bytes).unwrap();
        assert_eq!(full, data);
        let rows = sample_rows(5000);
        let taken = take_block_rows(block.pipeline, &block.bytes, &rows).unwrap();
        assert_eq!(taken, oracle_gather(&full, &rows), "{transform:?} distinct {distinct}");
        assert!(take_block_rows(block.pipeline, &block.bytes, &[5000]).is_err());
    }
}

#[test]
fn take_matches_full_decode_with_trailing_compression() {
    let data = ColumnData::Strings(strings(4096, 4096));
    for strategy in [
        CascadeStrategy::for_fresh_publication(),
        CascadeStrategy::for_rewrite_or_compaction(),
    ] {
        for random_access in [false, true] {
            let block = crate::encoding::encode_block_with_strategy(&data, random_access, strategy);
            let rows = sample_rows(4096);
            let full = decode_block(block.pipeline, &block.bytes).unwrap();
            assert_eq!(
                take_block_rows(block.pipeline, &block.bytes, &rows).unwrap(),
                oracle_gather(&full, &rows)
            );
        }
    }
}

#[test]
fn take_and_range_match_full_decode_for_rle() {
    let u: Vec<u64> = (0..10_000u64).map(|row| row / 333).collect();
    let i: Vec<i64> = (0..10_000i64)
        .map(|row| (row / 250) * if row % 2000 < 1000 { -1 } else { 1 })
        .collect();
    for data in [ColumnData::U64(u), ColumnData::I64(i)] {
        let block = forced(&data, Transform::Rle);
        let rows = sample_rows(10_000);
        assert_eq!(
            take_block_rows(block.pipeline, &block.bytes, &rows).unwrap(),
            oracle_gather(&data, &rows)
        );
        assert!(take_block_rows(block.pipeline, &block.bytes, &[10_000]).is_err());
        for (start, end) in [(0, 1), (332, 334), (5000, 9999), (9990, 20_000), (0, 10_000)] {
            let expected = slice_oracle(&data, start, end);
            assert_eq!(
                decode_block_range(block.pipeline, &block.bytes, start, end).unwrap(),
                expected,
                "{start}..{end}"
            );
        }
    }
}

fn slice_oracle(data: &ColumnData, start: usize, end: usize) -> ColumnData {
    let rows: Vec<usize> = (start..end.min(data.row_count())).collect();
    gather_column(data, &rows).unwrap()
}

#[test]
fn rle_filter_by_selection_matches_decode_then_filter() {
    let data = ColumnData::U64((0..5_000u64).map(|row| row / 77).collect());
    let block = forced(&data, Transform::Rle);
    for pattern in [0u8, 0xFF, 0b1010_0110, 0x01] {
        let mut selection = vec![pattern; 5_000 / 8 + 1];
        if let Some(byte) = selection.get_mut(300) {
            *byte = 0;
        }
        let rows: Vec<usize> = (0..5_000)
            .filter(|&row| selection.get(row / 8).is_some_and(|byte| byte & (1 << (row % 8)) != 0))
            .collect();
        let expected = oracle_gather(&data, &rows);
        assert_eq!(
            filter_block_by_selection(block.pipeline, &block.bytes, None, &selection).unwrap(),
            expected
        );
        // A non-RLE block goes through the decode-then-filter fallback and agrees.
        let plain = forced(&data, Transform::PlainU64);
        assert_eq!(
            filter_block_by_selection(plain.pipeline, &plain.bytes, None, &selection).unwrap(),
            expected
        );
    }
    // A selection shorter than the block selects nothing past its end.
    assert_eq!(
        filter_block_by_selection(block.pipeline, &block.bytes, None, &[0xFF]).unwrap(),
        slice_oracle(&data, 0, 8)
    );
}

#[test]
fn rle_numeric_predicate_fast_path_matches_decoded_filter() {
    let data = ColumnData::I64((0..4_000i64).map(|row| row / 100 - 20).collect());
    let block = forced(&data, Transform::Rle);
    for predicate in [
        NumericPredicate::Equals(3),
        NumericPredicate::NotEquals(-20),
        NumericPredicate::InSet(vec![-5, 0, 7]),
        NumericPredicate::Range {
            lower: Some(NumericBound {
                inclusive: true,
                value: -3,
            }),
            upper: Some(NumericBound {
                inclusive: false,
                value: 9,
            }),
        },
    ] {
        let mask = filter_numeric_block(block.pipeline, &block.bytes, &predicate).unwrap();
        assert_eq!(mask, Some(predicate.filter_decoded(&data)), "{predicate:?}");
    }
}

#[test]
fn dictionary_codes_project_without_materialising_rows() {
    for distinct in [8, 600] {
        let column = strings(3000, distinct);
        let data = ColumnData::Strings(column.clone());
        let block = forced(&data, Transform::DictionaryString);
        let codes = decode_dictionary_codes(block.pipeline, &block.bytes, None)
            .unwrap()
            .unwrap();
        assert_eq!(codes.len(), 3000);
        assert_eq!(codes.to_strings(), column);
        let rows = sample_rows(3000);
        assert_eq!(
            ColumnData::Strings(codes.gather(&rows).unwrap()),
            oracle_gather(&data, &rows)
        );
        assert!(codes.gather(&[3000]).is_err());
        // Grouping from codes alone agrees with counting the decoded values.
        let counts = codes.code_counts();
        for (entry, count) in codes.dictionary().iter().zip(&counts) {
            let expected = column.iter().filter(|value| *value == Some(entry.as_str())).count() as u64;
            assert_eq!(*count, expected);
        }
        let needle = codes.dictionary().get(1).cloned().unwrap();
        let expected: Vec<bool> = column.iter().map(|value| value == Some(needle.as_str())).collect();
        assert_eq!(codes.equals_mask(&needle), expected);
        assert!(codes.equals_mask("absent").iter().all(|matched| !matched));
    }
    let fsst = forced(&ColumnData::Strings(strings(100, 100)), Transform::FsstString);
    assert!(
        decode_dictionary_codes(fsst.pipeline, &fsst.bytes, None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn dictionary_codes_and_take_with_shared_alphabet() {
    let column = strings(2000, 10);
    let mut alphabet: Vec<String> = column.iter().flatten().map(str::to_owned).collect();
    alphabet.extend(["aaa".to_owned(), "zzz".to_owned()]);
    alphabet.sort_unstable();
    alphabet.dedup();
    let data = ColumnData::Strings(column.clone());
    let block = encode_block_with_shared_dictionary(&data, false, CascadeStrategy::NoTrailing, Some(&alphabet));
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::FileScopeDictionary);
    let codes = decode_dictionary_codes(block.pipeline, &block.bytes, Some(&alphabet))
        .unwrap()
        .unwrap();
    assert_eq!(codes.to_strings(), column);
    assert!(decode_dictionary_codes(block.pipeline, &block.bytes, None).is_err());
    let rows = sample_rows(2000);
    let full = decode_block_shared(block.pipeline, &block.bytes, Some(&alphabet)).unwrap();
    assert_eq!(
        take_block_rows_shared(block.pipeline, &block.bytes, Some(&alphabet), &rows).unwrap(),
        oracle_gather(&full, &rows)
    );
}

#[test]
fn fsst_uncompressed_len_matches_decoded_bytes() {
    for column in [strings(5000, 5000), strings(1, 1), StringColumn::new()] {
        let data = ColumnData::Strings(column.clone());
        let block = forced(&data, Transform::FsstString);
        let expected: usize = column.iter().flatten().map(str::len).sum();
        assert_eq!(
            fsst_uncompressed_len(block.pipeline, &block.bytes).unwrap(),
            Some(expected)
        );
    }
    let raw = forced(&ColumnData::Strings(strings(10, 10)), Transform::RawString);
    assert_eq!(fsst_uncompressed_len(raw.pipeline, &raw.bytes).unwrap(), None);
}

#[test]
fn dictionary_count_from_codes_matches_decoded_filter() {
    use crate::encoding::predicate::{ContainsMatcher, StringPredicate, count_string_block_shared};
    for distinct in [8, 600] {
        let column = strings(3000, distinct);
        let block = forced(&ColumnData::Strings(column.clone()), Transform::DictionaryString);
        let codes = decode_dictionary_codes(block.pipeline, &block.bytes, None)
            .unwrap()
            .unwrap();
        let present = codes.dictionary().get(2).cloned().unwrap();
        for predicate in [
            StringPredicate::Equals(present.clone()),
            StringPredicate::Equals("absent".to_owned()),
            StringPredicate::NotEquals(present.clone()),
            StringPredicate::InSet(vec![present.clone(), "absent".to_owned()]),
            StringPredicate::Contains(ContainsMatcher::new(vec!["service-3".to_owned()], false)),
        ] {
            let expected = predicate
                .filter_decoded(&column)
                .into_iter()
                .filter(|matched| *matched)
                .count();
            assert_eq!(
                count_string_block_shared(block.pipeline, &block.bytes, &predicate, None).unwrap(),
                Some(expected),
                "{distinct} distinct"
            );
        }
    }
}

#[test]
fn take_rejects_truncated_fsst_body() {
    let block = forced(&ColumnData::Strings(strings(500, 500)), Transform::FsstString);
    let cut = block.bytes.get(..block.bytes.len() / 2).unwrap();
    assert!(take_block_rows(block.pipeline, cut, &[499]).is_err());
    assert!(fsst_uncompressed_len(block.pipeline, cut).is_err());
}
