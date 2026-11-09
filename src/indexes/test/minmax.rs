use super::*;

macro_rules! round_trip_test {
    ($name:ident, $decode:path, [$($zone:expr),+ $(,)?]) => {
        #[test]
        fn $name() {
            for zone in [$($zone),+] {
                assert_eq!($decode(&zone.encode()).unwrap(), zone);
            }
        }
    };
    ($name:ident, $decode:path, [$($zone:expr),+ $(,)?], check_exactness) => {
        #[test]
        fn $name() {
            for zone in [$($zone),+] {
                let decoded = $decode(&zone.encode()).unwrap();
                assert_eq!(decoded, zone);
                assert_eq!(decoded.exactness(), zone.exactness());
            }
        }
    };
}

#[test]
fn numeric_build_finds_extremes_and_counts_nulls() {
    let values = [Some(5), None, Some(-3), Some(11), None, Some(0)];
    let zone = NumericMinMax::build(&values);
    assert_eq!(zone.min, Some(-3));
    assert_eq!(zone.max, Some(11));
    assert_eq!(zone.null_count, 2);
    assert_eq!(zone.row_count, 6);
    assert_eq!(zone.exactness(), Exactness::Exact);
}

#[test]
fn numeric_build_all_null_has_no_bounds() {
    let values = [None, None, None];
    let zone = NumericMinMax::build(&values);
    assert_eq!(zone.min, None);
    assert_eq!(zone.max, None);
    assert_eq!(zone.null_count, 3);
}

#[test]
fn numeric_prune_outside_range() {
    let zone = NumericMinMax::build(&[Some(10), Some(20)]);
    assert!(zone.can_prune_range(0, 9));
    assert!(zone.can_prune_range(21, 30));
    assert!(!zone.can_prune_range(0, 10));
    assert!(!zone.can_prune_range(15, 25));
    assert!(!zone.can_prune_range(12, 18));
}

#[test]
fn numeric_empty_block_always_prunes() {
    let zone = NumericMinMax::build(&[None]);
    assert!(zone.can_prune_range(-1000, 1000));
}

#[test]
fn numeric_inverted_predicate_prunes() {
    let zone = NumericMinMax::build(&[Some(1), Some(2)]);
    assert!(zone.can_prune_range(10, 5));
}

round_trip_test!(
    numeric_round_trips,
    NumericMinMax::decode,
    [
        NumericMinMax::build(&[Some(i128::MIN), Some(i128::MAX), None]),
        NumericMinMax::build(&[None, None]),
        NumericMinMax::build(&[Some(0)]),
    ]
);

#[test]
fn numeric_decode_truncated_errors() {
    assert!(NumericMinMax::decode(&[0, 0, 0]).is_err());
}

#[test]
fn numeric_decode_rejects_a_bad_option_tag() {
    let mut bytes = NumericMinMax::build(&[Some(1), Some(9)]).encode();
    bytes[8] = 2;
    assert!(
        NumericMinMax::decode(&bytes).is_err(),
        "an option tag other than 0 or 1 must be rejected, not treated as an absent bound"
    );
}

#[test]
fn numeric_decode_rejects_null_count_above_row_count() {
    let mut out = Writer::new();
    out.put_u32(2);
    out.put_u32(5);
    put_opt_i128(&mut out, None);
    put_opt_i128(&mut out, None);
    assert!(NumericMinMax::decode(&out.into_bytes()).is_err());
}

#[test]
fn numeric_decode_rejects_unpaired_bounds() {
    let mut out = Writer::new();
    out.put_u32(1);
    out.put_u32(0);
    put_opt_i128(&mut out, Some(3));
    put_opt_i128(&mut out, None);
    assert!(NumericMinMax::decode(&out.into_bytes()).is_err());
}

#[test]
fn numeric_decode_rejects_min_above_max() {
    let mut out = Writer::new();
    out.put_u32(2);
    out.put_u32(0);
    put_opt_i128(&mut out, Some(10));
    put_opt_i128(&mut out, Some(3));
    assert!(NumericMinMax::decode(&out.into_bytes()).is_err());
}

#[test]
fn string_untruncated_holds_exact_bounds() {
    let values = [Some(b"apple".as_slice()), Some(b"cherry"), Some(b"banana")];
    let zone = StringMinMax::build(&values, 16);
    assert!(!zone.is_truncated);
    assert_eq!(zone.min, b"apple");
    assert_eq!(zone.max, Some(b"cherry".to_vec()));
    assert_eq!(zone.exactness(), Exactness::Exact);
}

#[test]
fn string_skips_nulls() {
    let values = [None, Some(b"m".as_slice()), None, Some(b"a"), Some(b"z")];
    let zone = StringMinMax::build(&values, 8);
    assert_eq!(zone.min, b"a");
    assert_eq!(zone.max, Some(b"z".to_vec()));
    assert!(!zone.is_truncated);
}

#[test]
fn string_all_null_is_empty_exact_range() {
    let zone = StringMinMax::build(&[None, None], 8);
    assert!(!zone.is_truncated);
    assert_eq!(zone.min, b"");
    assert_eq!(zone.max, Some(Vec::new()));
    assert_eq!(zone.exactness(), Exactness::Exact);
}

#[test]
fn string_exactly_at_budget_is_not_truncated() {
    let values = [Some(b"abcd".as_slice()), Some(b"wxyz")];
    let zone = StringMinMax::build(&values, 4);
    assert!(!zone.is_truncated);
    assert_eq!(zone.min, b"abcd");
    assert_eq!(zone.max, Some(b"wxyz".to_vec()));
}

#[test]
fn string_truncation_bounds_stored_size() {
    let long_min = vec![b'a'; 1000];
    let long_max = vec![b'q'; 1000];
    let values = [Some(long_min.as_slice()), Some(long_max.as_slice())];
    let zone = StringMinMax::build(&values, 8);
    assert!(zone.is_truncated);
    assert!(zone.min.len() <= 8, "min length {}", zone.min.len());
    assert!(zone.max.as_ref().map(Vec::len).unwrap_or(0) <= 8, "max length bounded");
    assert_eq!(zone.exactness(), Exactness::InexactNoFalseNegative);
}

#[test]
fn string_lower_bound_rounds_down() {
    let values = [Some(b"applesauce".as_slice()), Some(b"applet")];
    let zone = StringMinMax::build(&values, 5);
    assert!(zone.is_truncated);
    assert_eq!(zone.min, b"apple");
    assert!(zone.min.as_slice() <= b"applesauce".as_slice());
}

#[test]
fn string_upper_bound_rounds_up_by_incrementing_last_byte() {
    let values = [Some(b"a".as_slice()), Some(b"applet")];
    let zone = StringMinMax::build(&values, 5);
    assert!(zone.is_truncated);
    assert_eq!(zone.max, Some(b"applf".to_vec()));
    assert!(zone.max.as_deref().unwrap() >= b"applet".as_slice());
}

#[test]
fn string_upper_bound_drops_trailing_0xff_before_incrementing() {
    let mut true_max = vec![0x41u8, 0xFF, 0xFF, 0xFF];
    true_max.extend_from_slice(b"tail");
    let values = [Some(b"\x00".as_slice()), Some(true_max.as_slice())];
    let zone = StringMinMax::build(&values, 2);
    assert!(zone.is_truncated);
    assert_eq!(zone.max, Some(vec![0x42]));
    assert!(zone.max.as_deref().unwrap() >= true_max.as_slice());
}

#[test]
fn string_upper_bound_all_0xff_is_unbounded_above() {
    let true_max = vec![0xFFu8; 10];
    let values = [Some(b"\x00".as_slice()), Some(true_max.as_slice())];
    let zone = StringMinMax::build(&values, 4);
    assert!(zone.is_truncated);
    assert_eq!(zone.max, None);
}

#[test]
fn string_only_lower_bound_truncated_still_inexact() {
    let long_min = vec![b'a'; 100];
    let values = [Some(long_min.as_slice()), Some(b"zoo".as_slice())];
    let zone = StringMinMax::build(&values, 4);
    assert!(zone.is_truncated);
    assert_eq!(zone.min, vec![b'a'; 4]);
    assert_eq!(zone.max, Some(b"zoo".to_vec()));
}

#[test]
fn string_prune_outside_exact_range() {
    let zone = StringMinMax::build(&[Some(b"m".as_slice()), Some(b"t")], 8);
    assert!(zone.can_prune(b"a", b"l"));
    assert!(zone.can_prune(b"u", b"z"));
    assert!(!zone.can_prune(b"a", b"m"));
    assert!(!zone.can_prune(b"n", b"z"));
}

#[test]
fn string_prune_respects_unbounded_above() {
    let true_max = vec![0xFFu8; 10];
    let zone = StringMinMax::build(&[Some(b"\x00".as_slice()), Some(true_max.as_slice())], 4);
    assert_eq!(zone.max, None);
    assert!(zone.can_prune(&[], &[]) || !zone.can_prune(&[], &[]));
    assert!(!zone.can_prune(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF], &[0xFF; 16]));
}

#[test]
fn truncated_bounds_keep_pruning_sound_against_brute_force() {
    let raw: Vec<Vec<u8>> = vec![
        b"alpha-1234567890".to_vec(),
        b"bravo-abcdefghij".to_vec(),
        b"charlie-zzzzzzzz".to_vec(),
        b"delta-0000000000".to_vec(),
    ];
    let values: Vec<Option<&[u8]>> = raw.iter().map(|v| Some(v.as_slice())).collect();
    let zone = StringMinMax::build(&values, 4);
    assert!(zone.is_truncated);

    let probes: &[&[u8]] = &[
        b"", b"a", b"al", b"b", b"br", b"c", b"ch", b"d", b"de", b"e", b"z", b"zz",
    ];
    for lo in probes {
        for hi in probes {
            if lo > hi {
                continue;
            }
            if zone.can_prune(lo, hi) {
                for value in &raw {
                    let inside = value.as_slice() >= *lo && value.as_slice() <= *hi;
                    assert!(!inside, "pruned [{lo:?}, {hi:?}] but value {value:?} is inside it",);
                }
            }
        }
    }
}

#[test]
fn stored_lower_le_true_min_and_upper_ge_true_max() {
    let raw: Vec<Vec<u8>> = vec![
        b"mango-supercalifragilistic".to_vec(),
        b"apple-pie-with-a-long-name".to_vec(),
        b"zebra-stripes-everywhere!!".to_vec(),
    ];
    let values: Vec<Option<&[u8]>> = raw.iter().map(|v| Some(v.as_slice())).collect();
    let true_min = raw.iter().min().unwrap();
    let true_max = raw.iter().max().unwrap();
    let zone = StringMinMax::build(&values, 6);

    assert!(
        zone.min.as_slice() <= true_min.as_slice(),
        "stored lower {:?} must be <= true min {:?}",
        zone.min,
        true_min
    );
    if let Some(max) = &zone.max {
        assert!(
            max.as_slice() >= true_max.as_slice(),
            "stored upper {:?} must be >= true max {:?}",
            max,
            true_max
        );
    }
}

#[test]
fn string_build_is_deterministic() {
    let raw: Vec<Vec<u8>> = vec![
        b"the-quick-brown-fox".to_vec(),
        b"jumps-over-the-lazy".to_vec(),
        b"dog-0xFFFFFFFFFFFFFF".to_vec(),
    ];
    let values: Vec<Option<&[u8]>> = raw.iter().map(|v| Some(v.as_slice())).collect();
    let a = StringMinMax::build(&values, 5);
    let b = StringMinMax::build(&values, 5);
    assert_eq!(a.min, b.min);
    assert_eq!(a.max, b.max);
    assert_eq!(a.is_truncated, b.is_truncated);
    assert_eq!(a, b);
}

round_trip_test!(
    string_round_trips,
    StringMinMax::decode,
    [
        StringMinMax::build(&[Some(b"apple".as_slice()), Some(b"cherry")], 16),
        StringMinMax::build(&[Some(vec![b'a'; 50].as_slice()), Some(b"zoo")], 4),
        StringMinMax::build(&[Some(b"\x00".as_slice()), Some(vec![0xFFu8; 8].as_slice())], 4),
        StringMinMax::build(&[None], 8),
    ],
    check_exactness
);

#[test]
fn string_decode_truncated_errors() {
    assert!(StringMinMax::decode(&[0, 255, 255, 255, 255]).is_err());
}

#[test]
fn string_decode_rejects_min_above_max() {
    let forged = StringMinMax {
        is_truncated: false,
        max: Some(b"a".to_vec()),
        min: b"z".to_vec(),
    };
    assert!(StringMinMax::decode(&forged.encode()).is_err());
}

#[test]
fn string_decode_rejects_exact_entry_with_unbounded_max() {
    let forged = StringMinMax {
        is_truncated: false,
        max: None,
        min: b"a".to_vec(),
    };
    assert!(StringMinMax::decode(&forged.encode()).is_err());
}

#[test]
fn sequence_build_and_prune() {
    let positions = [(1u64, 50u64), (1, 10), (2, 5), (1, 90)];
    let range = SequenceRange::build(&positions).unwrap();
    assert_eq!(range.first_epoch, 1);
    assert_eq!(range.first_sequence, 10);
    assert_eq!(range.last_epoch, 2);
    assert_eq!(range.last_sequence, 5);
    assert_eq!(range.exactness(), Exactness::Exact);

    assert!(range.can_prune((0, 0), (1, 9)));
    assert!(range.can_prune((2, 6), (9, 9)));
    assert!(!range.can_prune((1, 0), (1, 20)));
}

#[test]
fn sequence_empty_is_none() {
    assert_eq!(SequenceRange::build(&[]), None);
}

round_trip_test!(
    sequence_round_trips,
    SequenceRange::decode,
    [SequenceRange::build(&[(3, 4), (9, 100)]).unwrap()]
);

#[test]
fn sequence_decode_rejects_first_beyond_last() {
    let forged = SequenceRange {
        first_epoch: 2,
        first_sequence: 0,
        last_epoch: 1,
        last_sequence: 100,
    };
    assert!(SequenceRange::decode(&forged.encode()).is_err());
}

#[test]
fn time_build_and_prune() {
    let range = TimeRange::build(&[100, -50, 0, 250]).unwrap();
    assert_eq!(range.min_physical, -50);
    assert_eq!(range.max_physical, 250);
    assert_eq!(range.exactness(), Exactness::Exact);

    assert!(range.can_prune(-1000, -51));
    assert!(range.can_prune(251, 1000));
    assert!(!range.can_prune(0, 100));
    assert!(!range.can_prune(-100, -50));
}

#[test]
fn time_empty_is_none() {
    assert_eq!(TimeRange::build(&[]), None);
}

round_trip_test!(
    time_round_trips,
    TimeRange::decode,
    [TimeRange::build(&[i64::MIN, i64::MAX]).unwrap()]
);

#[test]
fn time_decode_truncated_errors() {
    assert!(TimeRange::decode(&[0, 0, 0]).is_err());
}

#[test]
fn time_decode_rejects_min_above_max() {
    let forged = TimeRange {
        max_physical: -100,
        min_physical: 100,
    };
    assert!(TimeRange::decode(&forged.encode()).is_err());
}

#[test]
fn numeric_decode_rejects_absent_bounds_over_a_block_with_non_null_rows() {
    let forged = NumericMinMax {
        max: None,
        min: None,
        null_count: 1,
        row_count: 4,
    };
    assert!(NumericMinMax::decode(&forged.encode()).is_err());

    let all_null = NumericMinMax {
        max: None,
        min: None,
        null_count: 4,
        row_count: 4,
    };
    assert_eq!(NumericMinMax::decode(&all_null.encode()).unwrap(), all_null);
}

#[test]
fn numeric_decode_rejects_stats_that_disagree_with_their_row_counts() {
    let bounded_all_null = NumericMinMax {
        max: Some(9),
        min: Some(1),
        null_count: 4,
        row_count: 4,
    };
    assert!(NumericMinMax::decode(&bounded_all_null.encode()).is_err());

    let zone_map = NumericMinMax::build(&[Some(3), None, Some(-7), Some(12)]);
    assert_eq!(
        (zone_map.min, zone_map.max, zone_map.null_count),
        (Some(-7), Some(12), 1)
    );
    let bytes = zone_map.encode();
    assert_eq!(NumericMinMax::decode_for_granule(&bytes, 4).unwrap(), zone_map);
    assert!(
        NumericMinMax::decode_for_granule(&bytes, 3).is_err(),
        "overruns the granule"
    );
    assert!(
        NumericMinMax::decode_for_granule(&bytes, 5).is_err(),
        "misses granule rows"
    );
}

#[test]
fn numeric_build_matches_separate_min_and_max_folds() {
    let values = [Some(i128::MAX), None, Some(0), Some(i128::MIN), Some(5)];
    for len in 0..=values.len() {
        let slice = &values[..len];
        let zone_map = NumericMinMax::build(slice);
        assert_eq!(zone_map.min, slice.iter().flatten().copied().min());
        assert_eq!(zone_map.max, slice.iter().flatten().copied().max());
        assert_eq!(
            zone_map.null_count as usize,
            slice.iter().filter(|v| v.is_none()).count()
        );
    }
}
