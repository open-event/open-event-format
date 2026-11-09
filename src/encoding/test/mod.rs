use super::*;
use crate::benchmarks::{XorFloatMeasurement, xor_float_gate_passes};
use crate::file::bytes::{Reader, Writer};
use arrow_array::Array;
use std::collections::BTreeMap;

#[test]
fn a_block_that_encodes_larger_than_plain_is_stored_plain() {
    for values in [
        vec![7u64, 9],
        vec![1_000_000u64, 1_000_064, 1_000_128],
        vec![0u64],
        vec![u64::MAX, 3, 17, 9_000_000_000],
    ] {
        let data = ColumnData::U64(values.clone());
        let block = encode_block(&data, false);
        let plain = plain_form(&data, false, CascadeStrategy::DecodeOptimized, None);
        assert!(
            block.bytes.len() <= plain.bytes.len(),
            "{} values encoded to {} bytes, past the plain form's {}",
            values.len(),
            block.bytes.len(),
            plain.bytes.len()
        );
        assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
    }
}

#[test]
fn a_block_that_earns_its_encoding_keeps_it() {
    let data = ColumnData::U64((0..4096u64).map(|i| i / 64).collect());
    let block = encode_block(&data, false);
    let plain = plain_form(&data, false, CascadeStrategy::DecodeOptimized, None);
    assert!(
        block.bytes.len() * 10 < plain.bytes.len(),
        "a run-heavy block must still encode far under plain: {} vs {}",
        block.bytes.len(),
        plain.bytes.len()
    );
    assert_ne!(block.pipeline.transform().unwrap(), Transform::PlainU64);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
}

fn round_trip(data: ColumnData, random_access: bool) -> (Transform, Compression) {
    let encoded = encode_block(&data, random_access);
    let decoded = decode_block(encoded.pipeline, &encoded.bytes).unwrap();
    assert_eq!(decoded, data);
    (
        encoded.pipeline.transform().unwrap(),
        encoded.pipeline.compression().unwrap(),
    )
}

#[test]
fn monotonic_columns_choose_for_or_delta() {
    let values: Vec<u64> = (0..8192u64).map(|i| 1_700_000_000_000 + i * 17).collect();
    let (transform, _) = round_trip(ColumnData::U64(values), false);
    assert!(
        matches!(transform, Transform::ForBitpack | Transform::DeltaBitpack),
        "monotonic data must select a FastLanes-style candidate, got {transform:?}"
    );
}

#[test]
fn repeated_values_choose_rle() {
    let values: Vec<u64> = (0..64u64)
        .flat_map(|run| std::iter::repeat_n(run * 1_000_003, 64))
        .collect();
    let (transform, _) = round_trip(ColumnData::U64(values), false);
    assert_eq!(transform, Transform::Rle);
    round_trip(ColumnData::U64(vec![42; 4096]), false);
}

#[test]
fn transform_sampling_sees_past_a_misleading_prefix() {
    let mut values = vec![42u64; TRANSFORM_SAMPLE_SIZE];
    let mut state = 0x1234_5678_u64;
    for _ in 0..7 * 1024 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        values.push(state >> 32);
    }
    let encoded = encode_block(&ColumnData::U64(values.clone()), true);
    let transform = encoded.pipeline.transform().unwrap();
    assert_ne!(
        transform,
        Transform::Rle,
        "a misleading constant prefix must not pick RLE for a high-entropy block"
    );
    let mut rle = Writer::new();
    encode_rle(&values, &mut rle);
    assert!(
        encoded.bytes.len() * 2 < rle.into_bytes().len(),
        "the stratified pick must beat the prefix pick by a wide margin"
    );
    assert_eq!(
        decode_block(encoded.pipeline, &encoded.bytes).unwrap(),
        ColumnData::U64(values)
    );
}

#[test]
fn monotonic_block_still_chooses_delta_across_sample_run_boundaries() {
    let values: Vec<u64> = (0..8192u64).map(|i| 10_000 + i * 3).collect();
    let (transform, _) = round_trip(ColumnData::U64(values), true);
    assert!(
        matches!(transform, Transform::ForBitpack | Transform::DeltaBitpack),
        "monotonic data must keep a FastLanes-style candidate under stratified sampling, got {transform:?}"
    );
}

fn signed_selection_columns() -> Vec<Vec<i64>> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut random = |count: usize| -> Vec<i64> {
        (0..count)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                state as i64
            })
            .collect()
    };
    let mut columns = Vec::new();
    for count in [0usize, 1, 1023, 1024, 1025, 4096, 10_007] {
        columns.push(random(count));
        columns.push(vec![-7; count]);
        columns.push((0..count as i64).map(|i| i * 3 - 5_000).collect());
        columns.push((0..count as i64).map(|i| if i % 2 == 0 { i } else { -i }).collect());
        columns.push(
            (0..count)
                .map(|i| if i % 3 == 0 { i64::MIN } else { i64::MAX })
                .collect(),
        );
    }
    let mut prefixed = vec![42i64; TRANSFORM_SAMPLE_SIZE];
    prefixed.extend(random(7 * 1024));
    columns.push(prefixed);
    columns
}

#[test]
fn signed_sample_drawn_as_zigzag_codes_matches_the_mapped_signed_sample() {
    for values in signed_selection_columns() {
        let reference: Option<Vec<u64>> =
            transform_sample(&values).map(|sample| sample.iter().map(|value| zigzag(*value)).collect());
        assert_eq!(
            transform_sample_mapped(&values, zigzag),
            reference,
            "sample at {} values",
            values.len()
        );
        let reference_mapped = reference.unwrap_or_else(|| values.iter().map(|value| zigzag(*value)).collect());
        for strategy in [
            CascadeStrategy::DecodeOptimized,
            CascadeStrategy::NoTrailing,
            CascadeStrategy::SizeOptimized,
        ] {
            assert_eq!(
                choose_i64_transform(&values, strategy),
                score_u64_candidates(&reference_mapped, values.len(), strategy),
                "{strategy:?} at {} values",
                values.len()
            );
        }
    }
}

fn reference_float_fallback(values: &[f64]) -> Transform {
    let sample = transform_sample(values);
    let sample: &[f64] = sample.as_deref().unwrap_or(values);
    if sample.is_empty() {
        return Transform::PlainF64;
    }
    let plain_bytes: Vec<u8> = sample.iter().flat_map(|value| value.to_le_bytes()).collect();
    let mut bss_bytes = vec![0u8; sample.len() * 8];
    write_byte_stream_split(sample, &mut bss_bytes);
    let plain_compressed = deflate::compress(&plain_bytes).len();
    let bss_compressed = deflate::compress(&bss_bytes).len();
    let (_, _, alp_rd_estimate) = alp_rd_choose_split(sample);
    if alp_rd_estimate < plain_compressed.min(bss_compressed) {
        Transform::AlpRd
    } else if bss_compressed < plain_compressed {
        Transform::ByteStreamSplit
    } else {
        Transform::PlainF64
    }
}

fn float_fallback_columns() -> Vec<Vec<f64>> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let mut random_bits = |count: usize| -> Vec<f64> {
        (0..count)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                f64::from_bits(state)
            })
            .collect()
    };
    let specials = [
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0xFFF0_0000_0000_BEEF),
        f64::MIN_POSITIVE / 3.0,
        -f64::MIN_POSITIVE / 7.0,
    ];
    let mut columns = Vec::new();
    for count in [0usize, 1, 1023, 1024, 1025, 4096, 10_007] {
        columns.push(random_bits(count));
        columns.push((0..count).map(|i| 1.0 + (i as f64) * f64::EPSILON * 977.0).collect());
        columns.push((0..count).map(|i| specials[i % specials.len()]).collect());
        columns.push(
            (0..count)
                .map(|i| f64::from_bits((1023u64 << 52) | ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 12)))
                .collect(),
        );
    }
    columns
}

#[test]
fn float_fallback_trial_in_one_buffer_matches_separate_buffers() {
    for values in float_fallback_columns() {
        assert_eq!(
            choose_float_fallback(&values),
            reference_float_fallback(&values),
            "fallback at {} values",
            values.len()
        );
        let mut reused: Vec<u8> = values.iter().flat_map(|value| value.to_le_bytes()).collect();
        write_byte_stream_split(&values, &mut reused);
        let mut fresh = vec![0u8; values.len() * 8];
        write_byte_stream_split(&values, &mut fresh);
        assert_eq!(reused, fresh, "planes at {} values", values.len());
    }
}

#[test]
fn metric_f64_uses_alp_with_exact_round_trip() {
    let values: Vec<f64> = (0..2048).map(|i| (i as f64) * 0.25 + 10.5).collect();
    let (transform, _) = round_trip(ColumnData::F64(values), false);
    assert_eq!(transform, Transform::Alp);
    let weird: Vec<f64> = (0..256).map(|i| (i as f64).sqrt() * std::f64::consts::PI).collect();
    round_trip(ColumnData::F64(weird), false);
}

fn narrow_range_noisy_floats(count: usize) -> Vec<f64> {
    const FIXED_TOP_24_BITS: u64 = 0x3EE0_0000_0000_0000;
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..count)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            f64::from_bits(FIXED_TOP_24_BITS | (state & 0xFF_FFFF_FFFF))
        })
        .collect()
}

#[test]
fn noisy_float_column_alp_rejects_prefers_byte_stream_split_over_plain() {
    let values = narrow_range_noisy_floats(4096);
    let (transform, _) = round_trip(ColumnData::F64(values), false);
    assert_eq!(
        transform,
        Transform::ByteStreamSplit,
        "high-entropy floats in a narrow magnitude range should prefer byte-stream-split over raw plain storage"
    );
}

#[test]
fn byte_stream_split_deflate_page_supports_single_granule_range_decode() {
    let values = narrow_range_noisy_floats(4096);
    let block = encode_block(&ColumnData::F64(values.clone()), true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::ByteStreamSplit);
    assert_eq!(block.pipeline.compression().unwrap(), Compression::SeekableZstd);

    let start = 100;
    let end = 137;
    let decoded = decode_block_range(block.pipeline, &block.bytes, start, end).unwrap();
    assert_eq!(decoded, ColumnData::F64(values[start..end].to_vec()));
}

fn byte_stream_split_test_values(count: usize) -> Vec<f64> {
    let special = [
        f64::NAN.to_bits(),
        0x7FF0_0000_0000_0001,
        0xFFF8_0000_DEAD_BEEF,
        0.0f64.to_bits(),
        (-0.0f64).to_bits(),
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        1,
        0x800F_FFFF_FFFF_FFFF,
        f64::MAX.to_bits(),
        f64::MIN_POSITIVE.to_bits(),
    ];
    let mut state = 0x2545_F491_4F6C_DD1D;
    (0..count)
        .map(|i| {
            f64::from_bits(
                special
                    .get(i % 50)
                    .copied()
                    .unwrap_or_else(|| pseudo_random(&mut state)),
            )
        })
        .collect()
}

fn byte_stream_split_body(values: &[f64]) -> Vec<u8> {
    let mut out = Writer::new();
    encode_byte_stream_split(values, &mut out);
    out.into_bytes()
}

fn float_bits(values: &[f64]) -> Vec<u64> {
    values.iter().map(|value| value.to_bits()).collect()
}

fn frames_covering(stored: &[u8], ranges: &[(usize, usize)]) -> usize {
    let table = seekable_zstd::seek_table(stored).unwrap();
    let mut frames = std::collections::BTreeSet::new();
    for &(start, len) in ranges.iter().filter(|(_, len)| *len > 0) {
        frames.extend(table.frame_index_decomp(start as u64)..=table.frame_index_decomp((start + len - 1) as u64));
    }
    frames.len()
}

#[test]
fn byte_stream_split_full_decode_keeps_every_bit_and_refuses_a_truncated_plane() {
    for count in [0, 1, 300] {
        let values = byte_stream_split_test_values(count);
        let body = byte_stream_split_body(&values);
        let decoded = decode_byte_stream_split(&mut Reader::new(&body)).unwrap();
        assert_eq!(float_bits(&decoded), float_bits(&values), "{count} rows");

        let mut trailing = body.clone();
        trailing.extend_from_slice(&[0xAB; 3]);
        let decoded = decode_byte_stream_split(&mut Reader::new(&trailing)).unwrap();
        assert_eq!(
            float_bits(&decoded),
            float_bits(&values),
            "{count} rows and trailing bytes"
        );

        for plane in (0..8).filter(|_| count > 0) {
            let cut = &body[..4 + plane * count + count - 1];
            assert_eq!(
                decode_byte_stream_split(&mut Reader::new(cut)),
                Err(FormatError::Truncated {
                    what: "byte-stream-split plane"
                }),
                "{count} rows cut short in plane {plane}"
            );
        }
    }
    assert_eq!(
        decode_byte_stream_split(&mut Reader::new(&[1, 0])),
        Err(FormatError::Truncated {
            what: "byte-stream-split count"
        })
    );
}

fn byte_stream_split_reference(values: &[f64]) -> Vec<u8> {
    let mut planes = vec![0u8; values.len() * 8];
    for (row, value) in values.iter().enumerate() {
        for (plane, byte) in value.to_le_bytes().into_iter().enumerate() {
            planes[plane * values.len() + row] = byte;
        }
    }
    planes
}

#[test]
fn byte_stream_split_tiles_match_a_byte_at_a_time_transpose() {
    let tile = BYTE_STREAM_SPLIT_TILE;
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let random_lengths: Vec<usize> = (0..24).map(|_| (pseudo_random(&mut state) % 5000) as usize).collect();
    let lengths = [
        0,
        1,
        2,
        tile - 1,
        tile,
        tile + 1,
        2 * tile - 1,
        2 * tile + 1,
        3 * tile + 7,
        65_537,
    ];
    for count in lengths.into_iter().chain(random_lengths) {
        let values = byte_stream_split_test_values(count);
        let mut planes = vec![0u8; count * 8];
        write_byte_stream_split(&values, &mut planes);
        assert_eq!(planes, byte_stream_split_reference(&values), "{count} rows");
        let body = byte_stream_split_body(&values);
        assert_eq!(&body[4..], &planes[..], "{count} rows in a block body");
        let decoded = decode_byte_stream_split(&mut Reader::new(&body)).unwrap();
        assert_eq!(float_bits(&decoded), float_bits(&values), "{count} rows decoded");
    }
}

#[test]
fn byte_stream_split_framed_range_decode_matches_the_full_decode_and_inflates_each_frame_once() {
    let count = 300;
    let values = byte_stream_split_test_values(count);
    let body = byte_stream_split_body(&values);
    let pipeline = PipelineId::new(Transform::ByteStreamSplit, Compression::SeekableZstd, ValueKind::F64);
    let shared_frames: Vec<usize> = (1..4).map(|i| i * count * 5 / 2).collect();
    let frame_per_plane: Vec<usize> = (1..8).map(|plane| 4 + plane * count).collect();
    let small_frames: Vec<usize> = (1..body.len() / 37).map(|i| i * 37).collect();
    for breaks in [Vec::new(), shared_frames, frame_per_plane, small_frames] {
        let bytes = seekable_zstd::compress_with_breaks(&body, &breaks).expect("the body compresses");
        let ColumnData::F64(full) = decode_block(pipeline, &bytes).unwrap() else {
            panic!("a float block decodes to floats");
        };
        assert_eq!(float_bits(&full), float_bits(&values));
        for (start, end) in [
            (0, count),
            (0, 1),
            (36, 38),
            (100, 137),
            (count - 1, count),
            (250, count + 50),
            (count, count + 5),
            (count + 3, count + 10),
            (9, 9),
            (20, 10),
        ] {
            let effective_end = end.min(count);
            let mut ranges = Vec::new();
            if start < end {
                ranges.push((0, 4));
            }
            if start < effective_end {
                ranges.extend((0..8).map(|plane| (4 + plane * count + start, effective_end - start)));
            }
            seekable_zstd::take_frames_inflated();
            let ColumnData::F64(range) = decode_block_range(pipeline, &bytes, start, end).unwrap() else {
                panic!("a float block decodes to floats");
            };
            assert_eq!(
                seekable_zstd::take_frames_inflated(),
                frames_covering(&bytes, &ranges),
                "frames inflated for rows {start}..{end} under breaks {breaks:?}"
            );
            assert_eq!(
                float_bits(&range),
                float_bits(full.get(start..effective_end).unwrap_or(&[])),
                "rows {start}..{end} under breaks {breaks:?}"
            );
        }
    }
}

#[test]
fn byte_stream_split_framed_range_decode_refuses_a_forged_count_and_a_damaged_last_plane() {
    let count = 300;
    let mut body = byte_stream_split_body(&byte_stream_split_test_values(count));
    let pipeline = PipelineId::new(Transform::ByteStreamSplit, Compression::SeekableZstd, ValueKind::F64);
    let frame_per_plane: Vec<usize> = (1..8).map(|plane| 4 + plane * count).collect();
    let mut bytes = seekable_zstd::compress_with_breaks(&body, &frame_per_plane).expect("the body compresses");
    assert!(decode_block_range(pipeline, &bytes, 10, 20).is_ok());
    let table = seekable_zstd::seek_table(&bytes).unwrap();
    assert_eq!(table.num_frames(), 8);
    let last_plane = table.frame_start_comp(7).unwrap() as usize;
    bytes[last_plane] ^= 0xFF;
    assert!(decode_block_range(pipeline, &bytes, 10, 20).is_err());

    body[..4].copy_from_slice(&(count as u32 + 1).to_le_bytes());
    let forged = seekable_zstd::compress(&body).expect("the body compresses");
    assert!(decode_block_range(pipeline, &forged, 10, 20).is_err());
}

#[test]
fn plain_framed_range_decode_matches_the_full_decode_and_inflates_each_frame_once() {
    let values: Vec<u64> = (0..300u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).collect();
    let mut body = Writer::new();
    body.put_u32(values.len() as u32);
    for value in &values {
        body.put_u64(*value);
    }
    let body = body.into_bytes();
    let pipeline = PipelineId::new(Transform::PlainU64, Compression::SeekableZstd, ValueKind::U64);
    let small_frames: Vec<usize> = (1..body.len() / 37).map(|i| i * 37).collect();
    for breaks in [Vec::new(), vec![1_000], small_frames] {
        let bytes = seekable_zstd::compress_with_breaks(&body, &breaks).expect("the body compresses");
        assert_eq!(decode_block(pipeline, &bytes).unwrap(), ColumnData::U64(values.clone()));
        for (start, end) in [(0, 300), (0, 1), (4, 5), (120, 140), (299, 300), (250, 400), (300, 310)] {
            let effective_end = end.min(values.len());
            let mut ranges = vec![(0, 4)];
            if start < effective_end {
                ranges.push((4 + start * 8, (effective_end - start) * 8));
            }
            seekable_zstd::take_frames_inflated();
            let decoded = decode_block_range(pipeline, &bytes, start, end).unwrap();
            assert_eq!(
                seekable_zstd::take_frames_inflated(),
                frames_covering(&bytes, &ranges),
                "frames inflated for rows {start}..{end} under breaks {breaks:?}"
            );
            assert_eq!(
                decoded,
                ColumnData::U64(values[start..effective_end].to_vec()),
                "rows {start}..{end} under breaks {breaks:?}"
            );
        }
    }
}

#[test]
fn plain_deflate_page_range_decode_matches_full_decode() {
    let values: Vec<u64> = (0..2000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).collect();
    let mut body = (values.len() as u32).to_le_bytes().to_vec();
    for value in &values {
        body.extend_from_slice(&value.to_le_bytes());
    }
    let pipeline = PipelineId::new(Transform::PlainU64, Compression::Deflate, ValueKind::U64);
    let page = deflate::compress(&body);
    assert!(pipeline.supports_byte_range_extraction().unwrap());
    assert_eq!(decode_block(pipeline, &page).unwrap(), ColumnData::U64(values.clone()));
    for (start, end) in [(0, 1), (0, 600), (511, 513), (1000, 1600), (1990, 5000), (3000, 4000)] {
        let expected = values.get(start..end.min(values.len())).unwrap_or(&[]).to_vec();
        assert_eq!(
            decode_block_range(pipeline, &page, start, end).unwrap(),
            ColumnData::U64(expected),
            "rows {start}..{end}"
        );
    }

    let mut forged = body;
    forged[..4].copy_from_slice(&1u32.to_le_bytes());
    assert!(decode_block_range(pipeline, &deflate::compress(&forged), 0, 1).is_err());
}

fn forged_alp_block(exponent_index: u8, count: u32, width: u8) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u8(exponent_index);
    out.put_u32(0);
    out.put_u32(count);
    out.put_u8(width);
    out.into_bytes()
}

#[test]
fn alp_range_decode_rejects_forged_bit_width_and_amplified_count() {
    let pipeline = PipelineId::new(Transform::Alp, Compression::None, ValueKind::F64);

    let too_wide = forged_alp_block(0, 4, 65);
    assert!(decode_block_range(pipeline, &too_wide, 0, 4).is_err());

    let amplified = forged_alp_block(0, u32::MAX, 0);
    assert!(decode_block_range(pipeline, &amplified, 0, 2_000_000_000).is_err());

    let mut valid_bytes = forged_alp_block(0, 4, 1);
    for _ in 0..FASTLANES_LANES {
        valid_bytes.extend_from_slice(&0u64.to_le_bytes());
    }
    assert!(decode_block_range(pipeline, &valid_bytes, 0, 4).is_ok());
}

#[test]
fn money_is_decimal128_never_float() {
    let (transform, _) = round_trip(
        ColumnData::Decimal {
            values: vec![123_456_789_000_000_000_000_000i128, -5_00],
            scale: 2,
        },
        false,
    );
    assert_eq!(transform, Transform::Decimal128);
}

#[test]
fn low_cardinality_strings_choose_dictionary() {
    let values: Vec<Option<String>> = (0..2000)
        .map(|i| Some(["EUR", "USD", "NOK"][i % 3].to_owned()))
        .collect();
    let (transform, _) = round_trip(ColumnData::Strings(values.into()), false);
    assert_eq!(transform, Transform::DictionaryString);
}

#[test]
fn high_cardinality_short_strings_choose_fsst() {
    let values: Vec<Option<String>> = (0..2000)
        .map(|i| {
            if i % 17 == 0 {
                None
            } else {
                Some(format!("https://example.com/opportunity/{i}/stage"))
            }
        })
        .collect();
    let (transform, _) = round_trip(ColumnData::Strings(values.into()), false);
    assert_eq!(transform, Transform::FsstString);
}

#[test]
fn random_access_blocks_keep_only_the_compression_that_preserves_range_access() {
    let values: Vec<Option<String>> = (0..512).map(|i| Some(format!("text {i} {i}"))).collect();
    let encoded = encode_block(&ColumnData::Strings(values.clone().into()), true);
    assert!(
        matches!(
            encoded.pipeline.compression().unwrap(),
            Compression::None | Compression::SeekableZstd
        ),
        "a random-access block took {:?}",
        encoded.pipeline.compression().unwrap()
    );
    assert!(encoded.pipeline.supports_byte_range_extraction().unwrap());
    assert_eq!(
        decode_block(encoded.pipeline, &encoded.bytes).unwrap(),
        ColumnData::Strings(values.into())
    );

    let statuses = ["lost", "open", "pending", "won"];
    let low_cardinality: Vec<Option<String>> = (0..512).map(|i| Some(statuses[i % 4].to_owned())).collect();
    let dictionary = encode_block(&ColumnData::Strings(low_cardinality.into()), true);
    assert_eq!(dictionary.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(dictionary.pipeline.compression().unwrap(), Compression::None);
}

#[test]
fn pipeline_id_is_recorded_and_decodable() {
    let encoded = encode_block(&ColumnData::U64((0..100).collect()), false);
    let id = encoded.pipeline;
    assert_eq!(
        PipelineId::new(
            id.transform().unwrap(),
            id.compression().unwrap(),
            id.value_kind().unwrap()
        ),
        id
    );
}

#[test]
fn byte_range_extraction_capability_matches_decode_block_range_s_fast_paths() {
    assert!(Transform::Alp.is_per_value_addressable());
    assert!(Transform::FsstString.is_per_value_addressable());
    assert!(!Transform::PlainU64.is_per_value_addressable());
    assert!(!Transform::PlainF64.is_per_value_addressable());
    assert!(!Transform::ByteStreamSplit.is_per_value_addressable());
    assert!(Transform::DictionaryString.is_per_value_addressable());
    assert!(Transform::RawString.is_per_value_addressable());

    assert!(Transform::PlainU64.supports_framed_range());
    assert!(Transform::PlainF64.supports_framed_range());
    assert!(Transform::ByteStreamSplit.supports_framed_range());
    assert!(!Transform::Alp.supports_framed_range());
    assert!(!Transform::DictionaryString.supports_framed_range());

    assert!(Transform::FsstString.keeps_range_access_under_framing());
    assert!(Transform::RawString.keeps_range_access_under_framing());
    assert!(Transform::PlainU64.keeps_range_access_under_framing());
    assert!(!Transform::DictionaryString.keeps_range_access_under_framing());
    assert!(!Transform::Alp.keeps_range_access_under_framing());

    let raw_deflated = PipelineId::new(Transform::RawString, Compression::SeekableZstd, ValueKind::String);
    assert!(raw_deflated.supports_byte_range_extraction().unwrap());

    let plain_uncompressed = PipelineId::new(Transform::PlainU64, Compression::None, ValueKind::U64);
    assert!(!plain_uncompressed.supports_byte_range_extraction().unwrap());

    let plain_deflated = PipelineId::new(Transform::PlainU64, Compression::SeekableZstd, ValueKind::U64);
    assert!(plain_deflated.supports_byte_range_extraction().unwrap());

    let alp = PipelineId::new(Transform::Alp, Compression::None, ValueKind::F64);
    assert!(alp.supports_byte_range_extraction().unwrap());
}

#[test]
fn large_block_sampling_cascade_round_trips_exactly() {
    let body: Vec<u8> = (0..400_000u32)
        .flat_map(|i| (u64::from(i % 512)).to_le_bytes())
        .collect();
    assert!(
        body.len() > TRAILING_FULL_TRIAL_MAX_BYTES,
        "test body must exercise the sampling path"
    );
    let (compression, stored) = apply_trailing(body.clone(), CascadeStrategy::DecodeOptimized);
    assert_ne!(
        compression,
        Compression::None,
        "a large, repetitive body must still gain a trailing stage"
    );
    assert_eq!(
        remove_trailing(compression, &stored).unwrap(),
        body,
        "the sampled trailing stage must round-trip byte-for-byte"
    );
}

#[test]
fn large_incompressible_block_keeps_raw_bytes() {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let body: Vec<u8> = (0..400_000u32)
        .flat_map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state.to_le_bytes()
        })
        .collect();
    let (compression, stored) = apply_trailing(body.clone(), CascadeStrategy::SizeOptimized);
    assert_eq!(compression, Compression::None);
    assert_eq!(stored, body);
}

#[test]
fn the_no_trailing_strategy_stores_the_decode_optimized_body_uncompressed() {
    let strings = ColumnData::Strings(
        (0..4096)
            .map(|i| Some(format!("row {i} settled without incident")))
            .collect(),
    );
    let ints = ColumnData::U64((0..4096u64).map(|i| i % 512).collect());
    for (data, random_access) in [(&strings, false), (&strings, true), (&ints, false), (&ints, true)] {
        let optimized = encode_block_with_strategy(data, random_access, CascadeStrategy::DecodeOptimized);
        let control = encode_block_with_strategy(data, random_access, CascadeStrategy::NoTrailing);
        assert_eq!(
            control.pipeline.transform().unwrap(),
            optimized.pipeline.transform().unwrap()
        );
        assert_eq!(
            control.pipeline.side_stream().unwrap(),
            optimized.pipeline.side_stream().unwrap()
        );
        assert_eq!(control.pipeline.compression().unwrap(), Compression::None);
        assert_eq!(control.bytes.len() as u64, control.uncompressed_len);
        assert_eq!(control.uncompressed_len, optimized.uncompressed_len);
        assert_eq!(
            remove_trailing(optimized.pipeline.compression().unwrap(), &optimized.bytes).unwrap(),
            control.bytes,
            "the control stores exactly the body the optimized block compresses"
        );
        assert_eq!(decode_block(control.pipeline, &control.bytes).unwrap(), *data);
    }
    let optimized = encode_block_with_strategy(&strings, false, CascadeStrategy::DecodeOptimized);
    assert_ne!(optimized.pipeline.compression().unwrap(), Compression::None);
}

#[test]
fn fastlanes_bitpack_round_trips_across_widths_and_block_boundaries() {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let counts = [
        0usize, 1, 7, 8, 9, 16, 17, 31, 63, 64, 127, 128, 129, 511, 512, 1023, 1024, 1025, 2047, 2048, 2049, 3000,
    ];
    for width in 0u32..=64 {
        let limit = mask(width);
        for &count in &counts {
            let values: Vec<u64> = (0..count)
                .map(|_| if width == 0 { 0 } else { next() & limit })
                .collect();
            let mut writer = Writer::new();
            bitpack(&values, width, &mut writer);
            let bytes = writer.into_bytes();
            let mut reader = Reader::new(&bytes);
            let decoded = bitunpack(&mut reader).unwrap();
            assert_eq!(decoded, values, "round-trip failed at width {width}, count {count}");
        }
    }
}

#[test]
fn reading_one_fastlanes_value_matches_unpacking_its_whole_vector() {
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let count = 2 * 1024 + 300;
    for width in 0u32..=64 {
        let limit = mask(width);
        let values: Vec<u64> = (0..count)
            .map(|_| if width == 0 { 0 } else { next() & limit })
            .collect();
        let mut writer = Writer::new();
        bitpack(&values, width, &mut writer);
        let bytes = writer.into_bytes();
        let mut reader = Reader::new(&bytes);
        reader.u32("count").unwrap();
        reader.u8("width").unwrap();
        let packed = reader.take(reader.remaining(), "packed").unwrap();
        let probes = (0..1024)
            .chain((1024..2048).step_by(17))
            .chain(2048..count)
            .chain([count - 1]);
        for index in probes {
            assert_eq!(
                fastlanes_value_at(packed, width, index),
                values[index],
                "width {width}, index {index}"
            );
        }
    }
}

#[test]
fn fastlanes_layout_packs_whole_1024_value_vectors() {
    for &(count, width) in &[(1usize, 7u32), (1024, 11), (1025, 11), (5000, 13)] {
        let values: Vec<u64> = (0..count as u64).map(|i| i & mask(width)).collect();
        let mut writer = Writer::new();
        bitpack(&values, width, &mut writer);
        let vectors = count.div_ceil(1024);
        let expected = 5 + vectors * 16 * width as usize * 8;
        assert_eq!(writer.len(), expected, "count {count}, width {width}");
    }
}

fn reference_bitpack(values: &[u64], width: u32, out: &mut Writer) {
    out.put_u32(values.len() as u32);
    out.put_u8(width as u8);
    if width == 0 {
        return;
    }
    let words_per_vector = 16 * width as usize;
    let mut packed = vec![0u64; values.len().div_ceil(1024) * words_per_vector];
    for (packed_vector, in_vector) in packed.chunks_exact_mut(words_per_vector).zip(values.chunks(1024)) {
        let mut padded = [0u64; 1024];
        padded[..in_vector.len()].copy_from_slice(in_vector);
        bitpack_vector(&padded, packed_vector, width, mask(width));
    }
    out.put_u64_slice(&packed);
}

#[test]
fn bitpack_writes_the_same_bytes_as_the_staged_reference() {
    let mut state: u64 = 0x5851_F42D_4C95_7F2D;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let counts = [0usize, 1, 1023, 1024, 1025, 3 * 1024 + 7];
    for width in 0u32..=64 {
        let limit = mask(width);
        for &count in &counts {
            let random: Vec<u64> = (0..count).map(|_| next() & limit).collect();
            for values in [random, vec![0; count], vec![limit; count]] {
                for prefix in 0..8 {
                    let mut expected = Writer::new();
                    let mut sliced = Writer::new();
                    let mut called = Writer::new();
                    for writer in [&mut expected, &mut sliced, &mut called] {
                        writer.put_slice(&vec![0xAB; prefix]);
                    }
                    let calls = std::cell::Cell::new(0usize);
                    for _ in 0..2 {
                        reference_bitpack(&values, width, &mut expected);
                        bitpack(&values, width, &mut sliced);
                        bitpack_with(values.len(), width, &mut called, |i| {
                            calls.set(calls.get() + 1);
                            values[i]
                        });
                    }
                    let context = format!("width {width}, count {count}, prefix {prefix}");
                    assert_eq!(sliced.bytes(), expected.bytes(), "slice packer: {context}");
                    assert_eq!(called.bytes(), expected.bytes(), "callback packer: {context}");
                    let expected_calls = if width == 0 { 0 } else { 2 * count };
                    assert_eq!(calls.get(), expected_calls, "callback count: {context}");
                }
            }
        }
    }
}

fn string_values_with_nulls(row_count: usize, present: impl Fn(usize) -> bool) -> StringColumn {
    (0..row_count)
        .map(|i| present(i).then(|| format!("value-{i:04}")))
        .collect()
}

#[test]
fn null_stream_round_trips_every_form_with_identical_presence_answers() {
    let patterns: Vec<(&str, StringColumn)> = vec![
        ("all present", string_values_with_nulls(300, |_| true)),
        ("all absent", string_values_with_nulls(300, |_| false)),
        ("clustered", string_values_with_nulls(300, |i| !(100..250).contains(&i))),
        ("scattered", string_values_with_nulls(300, |i| i % 2 == 0)),
        ("single row", string_values_with_nulls(1, |_| true)),
        ("trailing null", string_values_with_nulls(9, |i| i < 8)),
    ];
    for (name, values) in patterns {
        let stream = NullStream::from_values(&values);
        let mut out = Writer::new();
        stream.write(&mut out);
        let bytes = out.into_bytes();
        let mut reader = Reader::new(&bytes);
        let decoded = NullStream::read(&mut reader).unwrap();
        assert_eq!(decoded, stream, "{name}: round trip");
        assert_eq!(decoded.row_count(), values.len(), "{name}: row count");
        assert_eq!(
            decoded.present_count(),
            values.iter().filter(|value| value.is_some()).count(),
            "{name}: present count"
        );
        let mut rank = 0usize;
        for (row, value) in values.iter().enumerate() {
            assert_eq!(decoded.is_present(row), value.is_some(), "{name}: is_present({row})");
            assert_eq!(decoded.present_before(row), rank, "{name}: present_before({row})");
            if value.is_some() {
                rank += 1;
            }
        }
        let mut walked: Vec<bool> = Vec::new();
        decoded
            .each_row(|set| {
                walked.push(set);
                Ok(())
            })
            .unwrap();
        let expected: Vec<bool> = values.iter().map(|value| value.is_some()).collect();
        assert_eq!(walked, expected, "{name}: each_row");
    }
}

#[test]
fn null_stream_picks_the_smallest_form() {
    let encoded_len = |values: &StringColumn| {
        let mut out = Writer::new();
        NullStream::from_values(values).write(&mut out);
        out.len()
    };
    assert_eq!(encoded_len(&string_values_with_nulls(4096, |_| true)), 5);
    assert_eq!(encoded_len(&string_values_with_nulls(4096, |_| false)), 5);
    assert_eq!(
        encoded_len(&string_values_with_nulls(4096, |i| !(1000..3000).contains(&i))),
        25
    );
    assert_eq!(encoded_len(&string_values_with_nulls(4096, |i| i % 2 == 0)), 517);
}

fn reference_null_stream(values: &StringColumn) -> NullStream<'static> {
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for (index, value) in values.iter().enumerate() {
        if value.is_some() {
            let index = index as u32;
            match runs.last_mut() {
                Some(run) if run.1 == index => run.1 = index + 1,
                _ => runs.push((index, index + 1)),
            }
        }
    }
    let row_count = values.len();
    let form = if runs.is_empty() {
        NullStreamForm::AllAbsent
    } else if runs == [(0, row_count as u32)] {
        NullStreamForm::AllPresent
    } else if 4 + runs.len() * 8 < row_count.div_ceil(8) {
        NullStreamForm::Runs(NullRuns::Pairs(runs))
    } else {
        let mut raw = vec![0u8; row_count.div_ceil(8)];
        for &(start, end) in &runs {
            for row in start as usize..end as usize {
                raw[row / 8] |= 1 << (row % 8);
            }
        }
        NullStreamForm::Raw(Cow::Owned(raw))
    };
    NullStream { form, row_count }
}

type RowShape = (&'static str, Box<dyn Fn(usize) -> bool>);

#[test]
fn null_stream_matches_the_reference_derivation() {
    let mut patterns: Vec<(String, StringColumn)> = vec![
        ("empty".into(), string_values_with_nulls(0, |_| true)),
        ("single present".into(), string_values_with_nulls(1, |_| true)),
        ("single absent".into(), string_values_with_nulls(1, |_| false)),
    ];
    for row_count in [7usize, 8, 9, 15, 16, 17, 63, 64, 65, 100, 255, 256, 257, 1000, 4096] {
        let shapes: [RowShape; 12] = [
            ("all present", Box::new(|_| true)),
            ("all absent", Box::new(|_| false)),
            ("alternating", Box::new(|i| i % 2 == 0)),
            ("alternating odd", Box::new(|i| i % 2 == 1)),
            ("sparse present", Box::new(|i| i % 97 == 3)),
            ("sparse absent", Box::new(|i| i % 89 != 5)),
            ("clusters", Box::new(|i| (i / 40) % 3 == 0)),
            ("leading nulls", Box::new(move |i| i >= row_count / 3)),
            ("trailing nulls", Box::new(move |i| i < row_count - row_count / 3)),
            ("byte-aligned blocks", Box::new(|i| (i / 8) % 2 == 0)),
            ("first row only", Box::new(|i| i == 0)),
            ("last row only", Box::new(move |i| i + 1 == row_count)),
        ];
        for (shape, present) in shapes {
            patterns.push((
                format!("{shape} x{row_count}"),
                string_values_with_nulls(row_count, present),
            ));
        }
    }
    for (row_count, run_count) in [
        (88usize, 1usize),
        (89, 1),
        (96, 1),
        (97, 1),
        (152, 2),
        (153, 2),
        (160, 2),
        (161, 2),
    ] {
        patterns.push((
            format!("tie {run_count} runs x{row_count}"),
            string_values_with_nulls(row_count, move |i| i < 10 || (run_count == 2 && (20..30).contains(&i))),
        ));
    }
    for (name, values) in patterns {
        let actual = NullStream::from_values(&values);
        let expected = reference_null_stream(&values);
        assert_eq!(actual, expected, "{name}: form");
        let mut actual_bytes = Writer::new();
        actual.write(&mut actual_bytes);
        let mut expected_bytes = Writer::new();
        expected.write(&mut expected_bytes);
        assert_eq!(actual_bytes.into_bytes(), expected_bytes.into_bytes(), "{name}: bytes");
    }
}

fn reference_string_transform(present: &[&str]) -> Transform {
    if present.is_empty() {
        return Transform::RawString;
    }
    let sample = transform_sample(present);
    let sample: &[&str] = sample.as_deref().unwrap_or(present);
    let mut distinct: Vec<&str> = sample.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    let average_len = sample.iter().map(|value| value.len()).sum::<usize>() / sample.len().max(1);
    if distinct.len() * DICTIONARY_MAX_CARDINALITY_DIVISOR <= sample.len() {
        Transform::DictionaryString
    } else if average_len <= FSST_MAX_AVERAGE_VALUE_LEN && sample.len() >= FSST_MIN_SAMPLE_COUNT {
        Transform::FsstString
    } else {
        Transform::RawString
    }
}

#[test]
fn string_transform_choice_matches_the_reference() {
    for size in [0usize, 1, 15, 16, 1023, 1024, 1025, 5000, 20_000] {
        for cardinality in [1usize, 3, 250, 256, 257, usize::MAX] {
            for pad in [0usize, 40, 60, 64, 65, 200] {
                let owned: Vec<String> = (0..size)
                    .map(|i| {
                        let key = if cardinality == usize::MAX {
                            i
                        } else {
                            (i * 7) % cardinality
                        };
                        format!("{key}{}", "x".repeat(pad + (i % 3)))
                    })
                    .collect();
                let present: Vec<&str> = owned.iter().map(String::as_str).collect();
                let before = present.clone();
                assert_eq!(
                    choose_string_transform(&present),
                    reference_string_transform(&present),
                    "size {size}, cardinality {cardinality}, pad {pad}"
                );
                assert_eq!(present, before, "caller's values must stay in their order");
            }
        }
    }
}

#[test]
fn string_transforms_round_trip_under_every_null_stream_form() {
    let presence_patterns: [(&str, fn(usize) -> bool); 4] = [
        ("all present", |_| true),
        ("all absent", |_| false),
        ("clustered", |i| !(200..900).contains(&i)),
        ("scattered", |i| i % 3 != 1),
    ];
    for (name, present) in presence_patterns {
        let dictionary: Vec<Option<String>> = (0..1200)
            .map(|i| present(i).then(|| format!("label-{}", i % 3)))
            .collect();
        let fsst: Vec<Option<String>> = (0..1200)
            .map(|i| present(i).then(|| format!("event-name-{i:05}")))
            .collect();
        let raw: Vec<Option<String>> = (0..1200)
            .map(|i| present(i).then(|| format!("{i:05}-").repeat(40)))
            .collect();
        for values in [dictionary, fsst, raw] {
            let encoded = encode_block(&ColumnData::Strings(values.clone().into()), false);
            let decoded = decode_block(encoded.pipeline, &encoded.bytes).unwrap();
            assert_eq!(
                decoded,
                ColumnData::Strings(values.clone().into()),
                "{name}: full decode"
            );
            if let Some(views) = decode_string_block_views(encoded.pipeline, &encoded.bytes).unwrap() {
                let view_nulls: Vec<bool> = (0..views.len()).map(|i| views.is_valid(i)).collect();
                let expected: Vec<bool> = values.iter().map(|value| value.is_some()).collect();
                assert_eq!(view_nulls, expected, "{name}: view nulls");
                let viewed: Vec<Option<String>> = (0..views.len())
                    .map(|row| views.is_valid(row).then(|| views.value(row).to_string()))
                    .collect();
                assert_eq!(viewed, values, "{name}: view values");
            }
        }
    }
}

#[test]
fn null_stream_refuses_malformed_input() {
    let read = |bytes: &[u8]| {
        let mut reader = Reader::new(bytes);
        NullStream::read(&mut reader).map(NullStream::into_owned)
    };
    let stream = |row_count: u32, form: u8, rest: &[u8]| {
        let mut out = Writer::new();
        out.put_u32(row_count);
        out.put_u8(form);
        out.put_slice(rest);
        out.into_bytes()
    };
    let run = |start: u32, end: u32| [start.to_le_bytes(), end.to_le_bytes()].concat();
    assert!(read(&stream(8, 9, &[])).is_err(), "unknown form tag");
    assert!(read(&stream(u32::MAX, 0, &[])).is_err(), "amplified all-present count");
    assert!(read(&stream(u32::MAX, 1, &[])).is_err(), "amplified all-absent count");
    let empty_run = [2u32.to_le_bytes().as_slice(), &run(3, 3)].concat();
    assert!(read(&stream(8, 2, &empty_run)).is_err(), "empty run");
    let beyond = [1u32.to_le_bytes().as_slice(), &run(0, 9)].concat();
    assert!(read(&stream(8, 2, &beyond)).is_err(), "run past the row count");
    let touching = [2u32.to_le_bytes().as_slice(), &run(0, 2), &run(2, 4)].concat();
    assert!(read(&stream(8, 2, &touching)).is_err(), "touching runs");
    let unsorted = [2u32.to_le_bytes().as_slice(), &run(4, 6), &run(0, 2)].concat();
    assert!(read(&stream(8, 2, &unsorted)).is_err(), "unsorted runs");
    assert!(read(&stream(64, 3, &[0xFF; 4])).is_err(), "truncated raw bitmap");
}

#[test]
fn raw_null_stream_borrows_the_input_bitmap_and_decodes_into_owned_output() {
    let derived = NullStream::from_values(&string_values_with_nulls(300, |i| i % 2 == 0));
    assert!(matches!(derived.form, NullStreamForm::Raw(Cow::Owned(_))));
    let mut out = Writer::new();
    derived.write(&mut out);
    let bytes = out.into_bytes();
    let read = NullStream::read(&mut Reader::new(&bytes)).unwrap();
    match &read.form {
        NullStreamForm::Raw(Cow::Borrowed(bits)) => {
            assert_eq!(bits.as_ptr(), bytes[5..].as_ptr());
            assert_eq!(bits.len(), 300usize.div_ceil(8));
        }
        other => panic!("expected a borrowed raw bitmap, got {other:?}"),
    }
    assert_eq!(read, derived);
    let mut rewritten = Writer::new();
    read.write(&mut rewritten);
    assert_eq!(rewritten.into_bytes(), bytes);

    let values: Vec<Option<String>> = (0..300).map(|i| (i % 2 == 0).then(|| format!("v{i}-\u{e9}"))).collect();
    let data = ColumnData::Strings(values.clone().into());
    for transform in [Transform::DictionaryString, Transform::FsstString, Transform::RawString] {
        let block =
            encode_block_forced_for_conformance(&data, false, CascadeStrategy::DecodeOptimized, transform).unwrap();
        let mut bytes = block.bytes.clone();
        let full = decode_block(block.pipeline, &bytes).unwrap();
        let range = decode_block_range(block.pipeline, &bytes, 3, 10).unwrap();
        let views = decode_string_block_views(block.pipeline, &bytes).unwrap().unwrap();
        bytes.fill(0);
        drop(bytes);
        assert_eq!(full, data, "{transform:?}: full decode");
        assert_eq!(
            range,
            ColumnData::Strings(values[3..10].to_vec().into()),
            "{transform:?}: range decode"
        );
        let viewed: Vec<Option<String>> = (0..views.len())
            .map(|row| views.is_valid(row).then(|| views.value(row).to_string()))
            .collect();
        assert_eq!(viewed, values, "{transform:?}: views");
    }
}

fn reference_null_stream_read<'a>(reader: &mut Reader<'a>) -> Result<NullStream<'a>, FormatError> {
    let row_count = reader.u32("string row count")? as usize;
    let form = match reader.u8("null stream form")? {
        0 | 1 if row_count > MAX_BLOCK_VALUES => {
            return Err(FormatError::Structural {
                rule: "null stream row count beyond the block bound",
            });
        }
        0 => NullStreamForm::AllPresent,
        1 => NullStreamForm::AllAbsent,
        2 => {
            let run_count = reader.u32("null stream run count")? as usize;
            let mut runs: Vec<(u32, u32)> = Vec::with_capacity(reader.capacity_hint(run_count, 8));
            for _ in 0..run_count {
                let start = reader.u32("null stream run start")?;
                let end = reader.u32("null stream run end")?;
                if end <= start || end as usize > row_count {
                    return Err(FormatError::Structural {
                        rule: "null stream run must be non-empty and within the row count",
                    });
                }
                if runs.last().is_some_and(|&(_, previous_end)| start <= previous_end) {
                    return Err(FormatError::Structural {
                        rule: "null stream runs must be sorted and non-adjacent",
                    });
                }
                runs.push((start, end));
            }
            NullStreamForm::Runs(NullRuns::Pairs(runs))
        }
        3 => NullStreamForm::Raw(Cow::Borrowed(reader.take(row_count.div_ceil(8), "null bitmap")?)),
        _ => {
            return Err(FormatError::Structural {
                rule: "unknown null stream form",
            });
        }
    };
    Ok(NullStream { form, row_count })
}

fn run_form_stream(row_count: u32, run_count: u32, runs: &[(u32, u32)], trailing: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(row_count);
    out.put_u8(2);
    out.put_u32(run_count);
    for &(start, end) in runs {
        out.put_u32(start);
        out.put_u32(end);
    }
    out.put_slice(trailing);
    out.into_bytes()
}

fn assert_same_presence_answers(actual: &NullStream, expected: &NullStream, name: &str) {
    let rows = expected.row_count();
    assert_eq!(actual.row_count(), rows, "{name}: row count");
    assert_eq!(
        actual.present_count(),
        expected.present_count(),
        "{name}: present count"
    );
    for row in 0..rows + 2 {
        assert_eq!(
            actual.is_present(row),
            expected.is_present(row),
            "{name}: is_present({row})"
        );
        assert_eq!(
            actual.present_before(row),
            expected.present_before(row),
            "{name}: present_before({row})"
        );
    }
    let walk = |stream: &NullStream| {
        let mut walked = Vec::new();
        stream
            .each_row(|set| {
                walked.push(set);
                Ok(())
            })
            .unwrap();
        walked
    };
    assert_eq!(walk(actual), walk(expected), "{name}: each_row");
    let range_walk = |stream: &NullStream, start: usize, end: usize| {
        let mut walked = Vec::new();
        stream
            .each_row_range(start, end, |set| {
                walked.push(set);
                Ok(())
            })
            .unwrap();
        walked
    };
    let bounds = [
        0,
        1,
        7,
        39,
        40,
        41,
        119,
        120,
        rows / 2,
        rows.saturating_sub(1),
        rows,
        rows + 5,
    ];
    for start in bounds {
        for end in bounds {
            assert_eq!(
                range_walk(actual, start, end),
                range_walk(expected, start, end),
                "{name}: each_row_range({start}, {end})"
            );
        }
    }
    let values = || (1..=expected.present_count() as u128).collect::<Vec<_>>().into_iter();
    assert_eq!(
        views_by_row(actual, values()).unwrap(),
        views_by_row(expected, values()).unwrap(),
        "{name}: views"
    );
    assert_eq!(null_buffer(actual), null_buffer(expected), "{name}: null buffer");
}

#[test]
fn run_null_stream_borrows_the_input_table_and_answers_like_the_owned_runs() {
    let patterns: Vec<(&str, StringColumn)> = vec![
        ("clusters x1000", string_values_with_nulls(1000, |i| (i / 40) % 3 == 0)),
        ("clusters x4096", string_values_with_nulls(4096, |i| (i / 40) % 3 == 0)),
        (
            "one null cluster",
            string_values_with_nulls(4096, |i| !(1000..3000).contains(&i)),
        ),
        ("leading nulls", string_values_with_nulls(1000, |i| i >= 600)),
        ("trailing nulls", string_values_with_nulls(1000, |i| i < 400)),
        ("first row only", string_values_with_nulls(1000, |i| i == 0)),
        ("last row only", string_values_with_nulls(1000, |i| i == 999)),
    ];
    for (name, values) in patterns {
        let derived = NullStream::from_values(&values);
        let NullStreamForm::Runs(NullRuns::Pairs(pairs)) = &derived.form else {
            panic!(
                "{name}: expected the encoder to build owned runs, got {:?}",
                derived.form
            );
        };
        let run_count = pairs.len();
        let mut out = Writer::new();
        derived.write(&mut out);
        let bytes = out.into_bytes();
        let mut reader = Reader::new(&bytes);
        let read = NullStream::read(&mut reader).unwrap();
        assert_eq!(reader.position(), bytes.len(), "{name}: consumed");
        match &read.form {
            NullStreamForm::Runs(NullRuns::Encoded(table)) => {
                assert_eq!(table.as_ptr(), bytes[9..].as_ptr(), "{name}: borrowed span start");
                assert_eq!(table.len(), run_count * 8, "{name}: borrowed span length");
            }
            other => panic!("{name}: expected a borrowed run table, got {other:?}"),
        }
        assert_eq!(read, derived, "{name}: equal to the owned runs");
        assert_same_presence_answers(&read, &derived, name);
        let mut rewritten = Writer::new();
        read.write(&mut rewritten);
        assert_eq!(rewritten.into_bytes(), bytes, "{name}: byte-identical write");
    }

    let bytes = run_form_stream(10, 0, &[], &[]);
    let read = NullStream::read(&mut Reader::new(&bytes)).unwrap();
    let owned = NullStream {
        form: NullStreamForm::Runs(NullRuns::Pairs(Vec::new())),
        row_count: 10,
    };
    assert_eq!(read, owned, "empty table");
    assert_same_presence_answers(&read, &owned, "empty table");
}

#[test]
fn run_null_stream_into_owned_outlives_the_input_bytes() {
    let derived = NullStream::from_values(&string_values_with_nulls(1000, |i| (i / 40) % 3 == 0));
    let mut out = Writer::new();
    derived.write(&mut out);
    let mut bytes = out.into_bytes();
    let owned = NullStream::read(&mut Reader::new(&bytes)).unwrap().into_owned();
    bytes.fill(0);
    drop(bytes);
    assert!(matches!(owned.form, NullStreamForm::Runs(NullRuns::Pairs(_))));
    assert_eq!(owned, derived);
    assert_same_presence_answers(&owned, &derived, "owned after drop");

    let NullStreamForm::Runs(NullRuns::Pairs(pairs)) = &derived.form else {
        panic!("expected owned runs, got {:?}", derived.form);
    };
    let pointer = pairs.as_ptr();
    let kept = derived.into_owned();
    let NullStreamForm::Runs(NullRuns::Pairs(kept_pairs)) = &kept.form else {
        panic!("expected owned runs, got {:?}", kept.form);
    };
    assert_eq!(kept_pairs.as_ptr(), pointer, "already-owned runs move without a copy");
}

#[test]
fn run_null_stream_parse_matches_the_copying_parse() {
    let mut cases: Vec<(&str, Vec<u8>)> = vec![
        ("truncated count", {
            let mut bytes = run_form_stream(8, 0, &[], &[]);
            bytes.truncate(7);
            bytes
        }),
        ("truncated start", run_form_stream(8, 1, &[], &[0, 0])),
        ("truncated end", run_form_stream(8, 1, &[], &[0, 0, 0, 0, 2, 0])),
        ("valid run then missing record", run_form_stream(8, 2, &[(0, 2)], &[])),
        (
            "bad first run then missing records",
            run_form_stream(8, 3, &[(3, 3)], &[]),
        ),
        (
            "out of bounds first run then truncated",
            run_form_stream(8, 3, &[(0, 9)], &[1]),
        ),
        ("overlap then truncated", run_form_stream(8, 3, &[(0, 4), (2, 6)], &[])),
        ("reversed", run_form_stream(8, 1, &[(5, 2)], &[])),
        ("empty", run_form_stream(8, 1, &[(3, 3)], &[])),
        ("out of bounds", run_form_stream(8, 1, &[(0, 9)], &[])),
        ("overlapping", run_form_stream(8, 2, &[(0, 4), (2, 6)], &[])),
        ("adjacent", run_form_stream(8, 2, &[(0, 2), (2, 4)], &[])),
        ("unsorted", run_form_stream(8, 2, &[(4, 6), (0, 2)], &[])),
        ("forged run count", run_form_stream(8, u32::MAX, &[(0, 2)], &[])),
        ("empty table", run_form_stream(8, 0, &[], &[])),
        ("empty table, zero rows", run_form_stream(0, 0, &[], &[])),
        ("run in zero rows", run_form_stream(0, 1, &[(0, 1)], &[])),
        ("valid", run_form_stream(64, 3, &[(0, 2), (5, 9), (40, 64)], &[])),
        (
            "valid with trailing bytes",
            run_form_stream(64, 2, &[(1, 2), (60, 64)], &[7, 7, 7]),
        ),
        ("whole u32 range", run_form_stream(u32::MAX, 1, &[(0, u32::MAX)], &[])),
        (
            "last u32 row",
            run_form_stream(u32::MAX, 2, &[(0, 1), (u32::MAX - 1, u32::MAX)], &[]),
        ),
        (
            "one past the u32 rows",
            run_form_stream(u32::MAX - 1, 1, &[(0, u32::MAX)], &[]),
        ),
    ];
    let valid = run_form_stream(64, 2, &[(1, 2), (60, 64)], &[]);
    for cut in 0..valid.len() {
        cases.push(("prefix", valid[..cut].to_vec()));
    }
    for (name, bytes) in &cases {
        let mut actual_reader = Reader::new(bytes);
        let actual = NullStream::read(&mut actual_reader);
        let mut expected_reader = Reader::new(bytes);
        let expected = reference_null_stream_read(&mut expected_reader);
        assert_eq!(actual, expected, "{name} ({} bytes)", bytes.len());
        if let Ok(stream) = &actual {
            assert_eq!(
                actual_reader.position(),
                expected_reader.position(),
                "{name}: stopping point"
            );
            if let NullStreamForm::Runs(runs) = &stream.form {
                assert!(matches!(runs, NullRuns::Encoded(_)), "{name}: borrowed");
            }
        }
    }
}

fn forged_fsst_block(symbol_lengths: &[u8], data: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(1);
    out.put_u8(0);
    out.put_u16(symbol_lengths.len() as u16);
    for (index, len) in symbol_lengths.iter().enumerate() {
        out.put_u64(u64::from_le_bytes([index as u8; 8]));
        out.put_u8(*len);
    }
    out.put_u32(1);
    out.put_u32(0);
    out.put_u32(data.len() as u32);
    out.put_slice(data);
    out.into_bytes()
}

fn forged_fsst_block_words(symbols: &[(u64, u8)], data: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(1);
    out.put_u8(0);
    out.put_u16(symbols.len() as u16);
    for (word, len) in symbols {
        out.put_u64(*word);
        out.put_u8(*len);
    }
    out.put_u32(1);
    out.put_u32(0);
    out.put_u32(data.len() as u32);
    out.put_slice(data);
    out.into_bytes()
}

#[test]
fn a_forged_fsst_symbol_table_is_rejected_not_aborted() {
    let pipeline = PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String);

    let too_many_symbols = forged_fsst_block(&vec![2u8; 300], &[0]);
    let zero_length = forged_fsst_block(&[0], &[0]);
    let over_length = forged_fsst_block(&[9], &[0]);
    let one_then_longer = forged_fsst_block(&[1, 3], &[0]);
    let decreasing = forged_fsst_block(&[3, 2], &[0]);
    for forged in [
        &too_many_symbols,
        &zero_length,
        &over_length,
        &one_then_longer,
        &decreasing,
    ] {
        assert!(decode_block(pipeline, forged).is_err());
        assert!(decode_string_block_views(pipeline, forged).is_err());
        assert!(decode_block_range(pipeline, forged, 0, 1).is_err());
    }
}

#[test]
fn a_forged_fsst_table_colliding_in_the_decoder_hash_is_rejected() {
    let pipeline = PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String);

    let colliding = forged_fsst_block_words(&[(0x00_0001, 3), (0x00_0801, 3)], &[0]);
    assert!(decode_block(pipeline, &colliding).is_err());
    assert!(decode_string_block_views(pipeline, &colliding).is_err());
    assert!(decode_block_range(pipeline, &colliding, 0, 1).is_err());

    let distinct = forged_fsst_block_words(&[(0x00_0001, 3), (0x00_0002, 3)], &[0]);
    assert!(decode_block(pipeline, &distinct).is_ok());
}

fn forged_fsst_block_pair(symbols: &[(u64, u8)], first: &[u8], second: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(2);
    out.put_u8(0);
    out.put_u16(symbols.len() as u16);
    for (word, len) in symbols {
        out.put_u64(*word);
        out.put_u8(*len);
    }
    out.put_u32(2);
    out.put_u32(0);
    out.put_u32(first.len() as u32);
    out.put_u32((first.len() + second.len()) as u32);
    out.put_slice(first);
    out.put_slice(second);
    out.into_bytes()
}

#[test]
fn a_value_boundary_cutting_a_character_in_half_is_rejected() {
    let pipeline = PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String);

    let split = forged_fsst_block_pair(&[(0xC3, 1), (0xA9, 1)], &[0], &[1]);
    assert!(decode_block(pipeline, &split).is_err());
    assert!(decode_string_block_views(pipeline, &split).is_err());
    assert!(decode_block_range(pipeline, &split, 0, 2).is_err());

    let whole = forged_fsst_block_pair(&[(0xA9C3, 2)], &[0], &[0]);
    assert_eq!(
        decode_block(pipeline, &whole).unwrap(),
        ColumnData::Strings(vec![Some("\u{e9}"), Some("\u{e9}")].into())
    );
}

fn forged_raw_string_block_pair(first: &[u8], second: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(2);
    out.put_u8(0);
    out.put_u32(2);
    out.put_u32(0);
    out.put_u32(first.len() as u32);
    out.put_u32((first.len() + second.len()) as u32);
    out.put_slice(first);
    out.put_slice(second);
    out.into_bytes()
}

fn forged_dictionary_block_pair(first: &[u8], second: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(2);
    out.put_u8(0);
    out.put_u32(2);
    out.put_u32(0);
    out.put_u32(first.len() as u32);
    out.put_u32((first.len() + second.len()) as u32);
    out.put_slice(first);
    out.put_slice(second);
    bitpack(&[0, 1], 1, &mut out);
    out.into_bytes()
}

#[test]
fn raw_and_dictionary_value_boundaries_cutting_a_character_in_half_are_rejected() {
    let raw = PipelineId::new(Transform::RawString, Compression::None, ValueKind::String);
    let split = forged_raw_string_block_pair(&[0xC3], &[0xA9]);
    assert!(decode_string_block_views(raw, &split).is_err());
    assert!(decode_block(raw, &split).is_err());
    let whole = forged_raw_string_block_pair("\u{e9}".as_bytes(), "\u{e9}".as_bytes());
    let views = decode_string_block_views(raw, &whole).unwrap().unwrap();
    assert_eq!((views.value(0), views.value(1)), ("\u{e9}", "\u{e9}"));

    let dictionary = PipelineId::new(Transform::DictionaryString, Compression::None, ValueKind::String);
    let split = forged_dictionary_block_pair(&[0xC3], &[0xA9]);
    assert!(decode_string_block_views(dictionary, &split).is_err());
    assert!(decode_block(dictionary, &split).is_err());
    let whole = forged_dictionary_block_pair("\u{e9}".as_bytes(), b"x");
    let views = decode_string_block_views(dictionary, &whole).unwrap().unwrap();
    assert_eq!((views.value(0), views.value(1)), ("\u{e9}", "x"));

    let mut short = Writer::new();
    short.put_u32(2);
    short.put_u8(0);
    short.put_u32(1);
    short.put_u32(0);
    short.put_u32(1);
    short.put_slice(b"a");
    let short = short.into_bytes();
    assert!(decode_string_block_views(raw, &short).is_err());
    assert!(decode_block(raw, &short).is_err());
}

#[test]
fn string_view_decode_weaves_every_null_stream_form() {
    let rows = 700usize;
    let value = |i: usize| match i % 3 {
        0 => format!("k-{}", i % 5),
        1 => format!("value-{i:05}-{}", "\u{e9}".repeat(i % 4)),
        _ => String::new(),
    };
    let patterns: [(&str, fn(usize) -> bool); 4] = [
        ("all present", |_| true),
        ("all absent", |_| false),
        ("clustered runs", |i| (i / 100) % 2 == 0),
        ("scattered bitmap", |i| i % 3 != 1),
    ];
    for (name, present) in patterns {
        let values: Vec<Option<String>> = (0..rows).map(|i| present(i).then(|| value(i))).collect();
        let data = ColumnData::Strings(values.clone().into());
        for transform in [Transform::DictionaryString, Transform::FsstString, Transform::RawString] {
            let block =
                encode_block_forced_for_conformance(&data, false, CascadeStrategy::DecodeOptimized, transform).unwrap();
            assert_eq!(
                decode_block(block.pipeline, &block.bytes).unwrap(),
                data,
                "{name} {transform:?}"
            );
            let views = decode_string_block_views(block.pipeline, &block.bytes)
                .unwrap()
                .expect("a string block decodes to views");
            let viewed: Vec<Option<String>> = (0..views.len())
                .map(|row| views.is_valid(row).then(|| views.value(row).to_string()))
                .collect();
            assert_eq!(viewed, values, "{name} {transform:?}");
        }
    }
}

#[test]
fn forged_fsst_code_bytes_are_rejected_before_the_unchecked_decode() {
    let pipeline = PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String);

    let out_of_range_code = forged_fsst_block(&[2], &[0x7F]);
    let dangling_escape = forged_fsst_block(&[2], &[255]);
    for forged in [&out_of_range_code, &dangling_escape] {
        assert!(decode_block(pipeline, forged).is_err());
        assert!(decode_string_block_views(pipeline, forged).is_err());
        assert!(decode_block_range(pipeline, forged, 0, 1).is_err());
    }

    let valid = forged_fsst_block(&[2], &[0]);
    assert!(decode_block(pipeline, &valid).is_ok());
}

#[test]
fn fsst_decoding_refuses_only_undefined_codes_and_dangling_escapes() {
    let full = fsst::Compressor::rebuild_from(
        (0..FSST_MAX_SYMBOL_COUNT as u8)
            .map(fsst::Symbol::from_u8)
            .collect::<Vec<_>>(),
        vec![1u8; FSST_MAX_SYMBOL_COUNT],
    );
    let two = fsst::Compressor::rebuild_from(
        [
            fsst::Symbol::from_slice(&[b'b', b'c', 0, 0, 0, 0, 0, 0]),
            fsst::Symbol::from_u8(b'a'),
        ],
        [2u8, 1],
    );
    let cases: [(&fsst::Compressor, &[u8], Option<&[u8]>); 19] = [
        (&full, &[], Some(&[])),
        (&full, &[0, 254], Some(&[0, 254])),
        (&full, &[255, 65], Some(b"A")),
        (&full, &[255, 255], Some(&[255])),
        (&full, &[7, 255, 255, 255, 255], Some(&[7, 255, 255])),
        (&full, &[255], None),
        (&full, &[7, 255], None),
        (&full, &[255, 255, 255], None),
        (&full, &[255, 65, 255], None),
        (&two, &[0, 1, 0], Some(b"bcabc")),
        (&two, &[255, 2], Some(&[2])),
        (&two, &[255, 255], Some(&[255])),
        (&two, &[255, 200, 1], Some(&[200, b'a'])),
        (&two, &[2], None),
        (&two, &[0, 200], None),
        (&two, &[255], None),
        (&two, &[1, 255], None),
        (&two, &[255, 255, 255], None),
        (&two, &[255, 2, 2], None),
    ];
    for (compressor, compressed, expected) in cases {
        let mut buffer = Vec::new();
        let decoded = fsst_decompress_value(&FsstDecodeTable::new(compressor), compressed, &mut buffer)
            .ok()
            .map(|()| buffer.as_slice());
        assert_eq!(
            decoded,
            expected,
            "{} symbols, codes {compressed:?}",
            compressor.symbol_lengths().len()
        );
    }
}

fn pseudo_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn forged_fsst_table(symbols: &[(u64, u8)]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u16(symbols.len() as u16);
    for (word, len) in symbols {
        out.put_u64(*word);
        out.put_u8(*len);
    }
    out.into_bytes()
}

fn fsst_table_is_well_formed(symbols: &[(u64, u8)]) -> bool {
    if symbols.len() > FSST_MAX_SYMBOL_COUNT {
        return false;
    }
    let lengths: Vec<u8> = symbols.iter().map(|(_, len)| *len).collect();
    if lengths.iter().any(|len| !(1..=8).contains(len)) {
        return false;
    }
    let first_one = lengths.iter().position(|&len| len == 1).unwrap_or(lengths.len());
    if lengths[..first_one].windows(2).any(|pair| pair[1] < pair[0]) || lengths[first_one..].iter().any(|&len| len != 1)
    {
        return false;
    }
    let mut slots: Vec<u64> = symbols
        .iter()
        .filter(|(_, len)| *len >= 3)
        .map(|(word, _)| fsst_lossy_pht_slot(*word))
        .collect();
    let long_symbols = slots.len();
    slots.sort_unstable();
    slots.dedup();
    slots.len() == long_symbols
}

fn forged_fsst_symbols(state: &mut u64, count: usize, shape: u64) -> Vec<(u64, u8)> {
    let mut symbols = Vec::with_capacity(count);
    let mut len = 2u8;
    for index in 0..count {
        let draw = pseudo_random(state);
        len = if shape == 0 {
            (draw % 10) as u8
        } else if len == 1 {
            1
        } else {
            match draw % 8 {
                0 => 1,
                1 | 2 => (len + 1).min(8),
                _ => len,
            }
        };
        let word = pseudo_random(state);
        let word = match shape {
            2 => word & 0x1FFF,
            3 => (word & !0xFF_FFFF) | ((index as u64 * 977) & 0x7FF),
            _ => word,
        };
        let word = if len == 0 || len >= 8 {
            word
        } else {
            word & ((1u64 << (8 * u64::from(len))) - 1)
        };
        symbols.push((word, len));
    }
    symbols
}

#[test]
fn the_direct_table_parser_and_the_compressor_parser_accept_the_same_tables() {
    let mut state = 0x5EED_u64;
    let mut tables: Vec<Vec<(u64, u8)>> = Vec::new();
    for round in 0..4000u64 {
        let draw = pseudo_random(&mut state);
        let count = match round % 4 {
            0 => (draw % 9) as usize,
            1 => 248 + (draw % 12) as usize,
            _ => (draw % 256) as usize,
        };
        tables.push(forged_fsst_symbols(&mut state, count, round % 4));
    }
    for corpus in [
        (0..300)
            .map(|i| format!("session-{i:07}@tenant-{:03}.example.com", i % 40))
            .collect::<Vec<_>>(),
        (0..300)
            .map(|i| format!("{i} \u{fc}n\u{ef}c\u{f6}d\u{e9} {}", "\u{2713}".repeat(i % 5)))
            .collect(),
        vec![String::from("a"); 20],
    ] {
        let corpus: Vec<&[u8]> = corpus.iter().map(String::as_bytes).collect();
        let compressor = fsst::Compressor::train(&corpus);
        let symbols = compressor.symbol_table();
        let lengths = compressor.symbol_lengths();
        tables.push(
            symbols
                .iter()
                .zip(lengths)
                .map(|(symbol, len)| (symbol.to_u64(), *len))
                .collect(),
        );
    }

    let (mut accepted, mut refused) = (0usize, 0usize);
    for (index, symbols) in tables.iter().enumerate() {
        let well_formed = fsst_table_is_well_formed(symbols);
        let bytes = forged_fsst_table(symbols);
        let cut = if index % 5 == 0 && !bytes.is_empty() {
            (pseudo_random(&mut state) % bytes.len() as u64) as usize
        } else {
            bytes.len()
        };
        let stored = &bytes[..cut];
        let mut direct_reader = Reader::new(stored);
        let mut compressor_reader = Reader::new(stored);
        let direct = FsstDecodeTable::read(&mut direct_reader);
        let via_compressor = read_fsst_compressor(&mut compressor_reader);
        assert_eq!(
            direct.as_ref().err(),
            via_compressor.as_ref().err(),
            "table {index}: {symbols:?} cut to {cut}"
        );
        assert_eq!(
            direct.is_ok(),
            well_formed && cut == bytes.len(),
            "table {index}: {symbols:?} cut to {cut}"
        );
        let (Ok(direct), Ok(compressor)) = (direct, via_compressor) else {
            refused += 1;
            continue;
        };
        accepted += 1;
        assert_eq!(direct_reader.position(), compressor_reader.position());
        assert_eq!(direct_reader.position(), bytes.len());
        let rebuilt = FsstDecodeTable::new(&compressor);
        assert_eq!(direct.symbol_count, symbols.len());
        assert_eq!(rebuilt.symbol_count, symbols.len());
        assert_eq!(direct.lengths, rebuilt.lengths);
        assert_eq!(direct.symbols, rebuilt.symbols);
        for (code, (word, len)) in symbols.iter().enumerate() {
            assert_eq!((direct.symbols[code], direct.lengths[code]), (*word, *len));
        }
        for code in symbols.len()..256 {
            assert_eq!((direct.symbols[code], direct.lengths[code]), (0, 0));
        }
    }
    assert!(
        accepted >= 500 && refused >= 500,
        "accepted {accepted}, refused {refused}"
    );
}

fn reference_fsst_decode(table: &FsstDecodeTable, codes: &[u8]) -> Result<Vec<u8>, FormatError> {
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(&code) = codes.get(at) {
        if code == fsst::ESCAPE_CODE {
            let literal = codes.get(at + 1).ok_or(FormatError::Truncated {
                what: "fsst escaped byte",
            })?;
            out.push(*literal);
            at += 2;
        } else {
            let len = table.lengths[usize::from(code)];
            if len == 0 {
                return Err(FormatError::RefOutOfRange { what: "fsst code" });
            }
            out.extend_from_slice(&table.symbols[usize::from(code)].to_le_bytes()[..usize::from(len)]);
            at += 1;
        }
    }
    Ok(out)
}

fn forged_fsst_codes(state: &mut u64, defined: usize, len: usize, escape: u64, undefined: u64) -> Vec<u8> {
    let mut codes = Vec::with_capacity(len + 1);
    while codes.len() < len {
        let draw = pseudo_random(state);
        let kind = draw % 16;
        if kind < escape {
            codes.push(fsst::ESCAPE_CODE);
            if draw >> 8 & 0x3 != 0 || codes.len() < len {
                codes.push((draw >> 16) as u8);
            }
        } else if kind < escape + undefined && defined < 255 {
            codes.push(defined as u8 + ((draw >> 16) % (255 - defined as u64)) as u8);
        } else if defined > 0 {
            codes.push(((draw >> 16) % defined as u64) as u8);
        } else {
            codes.push(fsst::ESCAPE_CODE);
            codes.push((draw >> 16) as u8);
        }
    }
    codes
}

#[test]
fn the_chunked_fsst_decoder_matches_the_reference_decoder_over_random_streams() {
    let mut state = 0xF55F_u64;
    let mut buffer = Vec::new();
    let (mut ok, mut undefined, mut dangling) = (0usize, 0usize, 0usize);
    for round in 0..6000u64 {
        let draw = pseudo_random(&mut state);
        let count = 1 + (draw % 255) as usize;
        let symbols = forged_fsst_symbols(&mut state, count, 3);
        assert!(fsst_table_is_well_formed(&symbols));
        let bytes = forged_fsst_table(&symbols);
        let table = FsstDecodeTable::read(&mut Reader::new(&bytes)).unwrap();
        let compressor = read_fsst_compressor(&mut Reader::new(&bytes)).unwrap();
        let len = match round % 8 {
            0 => 0,
            1..=4 => (draw >> 8) as usize % 9,
            5 | 6 => (draw >> 8) as usize % 64,
            _ => (draw >> 8) as usize % 3000,
        };
        let escape = match round % 5 {
            0 => 0,
            1 => 16,
            _ => (draw >> 20) % 5,
        };
        let undefined_rate = (draw >> 24) % 3;
        let codes = forged_fsst_codes(&mut state, count, len, escape, undefined_rate);
        let expected = reference_fsst_decode(&table, &codes);
        buffer.clear();
        buffer.extend_from_slice(b"kept");
        let decoded = fsst_decompress_value(&table, &codes, &mut buffer).map(|()| buffer[4..].to_vec());
        assert_eq!(decoded, expected, "table {symbols:?}, codes {codes:?}");
        match &expected {
            Ok(plain) => {
                ok += 1;
                assert_eq!(&buffer[..4], b"kept");
                assert_eq!(plain, &compressor.decompressor().decompress(&codes));
            }
            Err(FormatError::RefOutOfRange { .. }) => undefined += 1,
            Err(FormatError::Truncated { .. }) => dangling += 1,
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert!(
        ok >= 1000 && undefined >= 300 && dangling >= 100,
        "ok {ok}, undefined {undefined}, dangling {dangling}"
    );
}

#[test]
fn the_arena_fsst_decode_matches_decoding_the_values_one_by_one() {
    const WHAT: &str = "fsst value";
    const RULE: &str = "fsst offsets must be non-decreasing";
    let mut state = 0xA4E4_u64;
    let mut buffer = Vec::new();
    let (mut ok, mut backwards, mut past, mut bad_codes) = (0usize, 0usize, 0usize, 0usize);
    for round in 0..3000u64 {
        let draw = pseudo_random(&mut state);
        let count = 1 + (draw % 255) as usize;
        let symbols = forged_fsst_symbols(&mut state, count, 3);
        let table = FsstDecodeTable::read(&mut Reader::new(&forged_fsst_table(&symbols))).unwrap();
        let lead = (draw >> 8) as usize % 4;
        let mut data = vec![0xEE; lead];
        let mut offsets = vec![lead];
        let values = (draw >> 12) as usize % 40;
        for value in 0..values {
            let draw = pseudo_random(&mut state);
            let len = match draw % 4 {
                0 => 0,
                1 => draw as usize >> 8 & 0x7,
                _ => draw as usize >> 8 & 0x3F,
            };
            let escape = if round % 3 == 0 { 0 } else { draw >> 20 & 0x3 };
            let undefined = u64::from(value % 7 == 6 && draw >> 24 & 0x3 == 0);
            data.extend(forged_fsst_codes(&mut state, count, len, escape, undefined));
            offsets.push(data.len());
        }
        let fault = pseudo_random(&mut state);
        if values > 0 && fault.is_multiple_of(4) {
            let at = 1 + (fault >> 8) as usize % values;
            offsets[at] = offsets[at].saturating_sub(1 + (fault >> 16) as usize % 8);
        } else if fault % 4 == 1 {
            let at = (fault >> 8) as usize % offsets.len();
            offsets[at] = data.len() + 1 + (fault >> 16) as usize % 8;
        }

        let expected: Result<(Vec<usize>, Vec<u8>), FormatError> = (|| {
            let mut plain = b"kept".to_vec();
            let mut bounds = vec![plain.len()];
            if offsets[0] > data.len() {
                return Err(FormatError::Truncated { what: WHAT });
            }
            for pair in offsets.windows(2) {
                let (start, end) = (pair[0], pair[1]);
                if end < start {
                    return Err(FormatError::Structural { rule: RULE });
                }
                if end > data.len() {
                    return Err(FormatError::Truncated { what: WHAT });
                }
                plain.extend(reference_fsst_decode(&table, &data[start..end])?);
                bounds.push(plain.len());
            }
            Ok((bounds, plain))
        })();
        buffer.clear();
        buffer.extend_from_slice(b"kept");
        let decoded = fsst_decompress_values_with_table(&table, &data, &offsets, WHAT, RULE, &mut buffer)
            .map(|bounds| (bounds, buffer.clone()));
        assert_eq!(decoded, expected, "offsets {offsets:?}, data {data:?}");
        match &expected {
            Ok(_) => ok += 1,
            Err(FormatError::Structural { .. }) => backwards += 1,
            Err(FormatError::Truncated { what }) if *what == WHAT => past += 1,
            Err(_) => bad_codes += 1,
        }
    }
    assert!(
        ok >= 800 && backwards >= 150 && past >= 150 && bad_codes >= 100,
        "ok {ok}, backwards {backwards}, past {past}, bad codes {bad_codes}"
    );
}

#[test]
fn fsst_view_decode_matches_the_full_decode_over_empties_escapes_and_nulls() {
    let values: Vec<Option<String>> = (0..1400)
        .map(|i| match i % 7 {
            0 => None,
            1 => Some(String::new()),
            2 => Some(format!("id-{i}")),
            3 => Some(format!("session-{i:07}@tenant-{:03}.example.com", i % 400)),
            4 => Some(format!(
                "{i} \u{fc}n\u{ef}c\u{f6}d\u{e9} \u{2713} {}",
                "\u{1F600}".repeat(i % 3)
            )),
            5 => Some(char::from_u32(0x100 + (i as u32 % 0x300)).unwrap_or('?').to_string()),
            _ => Some((i % 5).to_string()),
        })
        .collect();
    let data = ColumnData::Strings(values.clone().into());
    for random_access in [false, true] {
        let block = encode_block_forced_for_conformance(
            &data,
            random_access,
            CascadeStrategy::DecodeOptimized,
            Transform::FsstString,
        )
        .unwrap();
        assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
        let views = decode_string_block_views(block.pipeline, &block.bytes)
            .unwrap()
            .expect("an FSST block decodes to views");
        let viewed: Vec<Option<String>> = (0..views.len())
            .map(|row| views.is_valid(row).then(|| views.value(row).to_string()))
            .collect();
        assert_eq!(viewed, values, "random access {random_access}");
    }

    let pipeline = PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String);
    let hello = u64::from_le_bytes(*b"hello\0\0\0");
    let escapes_only = forged_fsst_block_pair(&[(hello, 5)], &[255, b'a', 255, b'b'], &[0, 255, b'!', 0]);
    let views = decode_string_block_views(pipeline, &escapes_only).unwrap().unwrap();
    assert_eq!((views.value(0), views.value(1)), ("ab", "hello!hello"));
    let empty_then_full = forged_fsst_block_pair(&[(hello, 5)], &[], &[0]);
    let views = decode_string_block_views(pipeline, &empty_then_full).unwrap().unwrap();
    assert_eq!((views.value(0), views.value(1)), ("", "hello"));
    let full_table = vec![1u8; FSST_MAX_SYMBOL_COUNT];
    let views = decode_string_block_views(pipeline, &forged_fsst_block(&full_table, &[72, 105, 255, 33]))
        .unwrap()
        .unwrap();
    assert_eq!(views.value(0), "Hi!");
    assert!(decode_string_block_views(pipeline, &forged_fsst_block(&full_table, &[72, 255])).is_err());
    assert!(decode_block(pipeline, &forged_fsst_block(&full_table, &[72, 255])).is_err());
}

#[test]
fn damaged_fsst_blocks_are_refused_not_decoded_out_of_bounds() {
    let values: Vec<Option<String>> = (0..60)
        .map(|i| (i % 9 != 4).then(|| format!("ticket-{i:04} \u{fc}n\u{ef}code {}", i % 13)))
        .collect();
    let block = encode_block_forced_for_conformance(
        &ColumnData::Strings(values.into()),
        false,
        CascadeStrategy::DecodeOptimized,
        Transform::FsstString,
    )
    .unwrap();
    let body = remove_trailing(block.pipeline.compression().unwrap(), &block.bytes)
        .unwrap()
        .into_owned();
    let pipeline = PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String)
        .with_side_stream(block.pipeline.side_stream().unwrap());
    assert!(decode_string_block_views(pipeline, &body).unwrap().is_some());

    let exercise = |bytes: &[u8]| {
        let _ = decode_block(pipeline, bytes);
        let _ = decode_string_block_views(pipeline, bytes);
        let _ = decode_block_range(pipeline, bytes, 1, 3);
    };
    for len in 0..body.len() {
        exercise(&body[..len]);
    }
    let mut damaged = body.clone();
    for position in 0..body.len() {
        for forged in [0x00, 0x7F, 0x80, 0xFE, 0xFF, body[position].wrapping_add(1)] {
            damaged[position] = forged;
            exercise(&damaged);
        }
        damaged[position] = body[position];
    }
}

#[test]
fn i64_plain_framed_range_decode_matches_the_full_decode() {
    let values: Vec<i64> = (0..1500i64).map(|i| (i - 750) * 12_345).collect();
    let mut body = Writer::new();
    body.put_u32(values.len() as u32);
    for value in &values {
        body.put_u64(zigzag(*value));
    }
    let bytes = seekable_zstd::compress(&body.into_bytes()).expect("the body compresses");
    let pipeline = PipelineId::new(Transform::PlainU64, Compression::SeekableZstd, ValueKind::I64);
    assert!(pipeline.supports_byte_range_extraction().unwrap());

    assert_eq!(decode_block(pipeline, &bytes).unwrap(), ColumnData::I64(values.clone()));
    let (start, end) = (100, 1400);
    assert_eq!(
        decode_block_range(pipeline, &bytes, start, end).unwrap(),
        ColumnData::I64(values[start..end].to_vec())
    );
    assert_eq!(
        decode_block_range(pipeline, &bytes, 5, 5).unwrap(),
        ColumnData::I64(Vec::new())
    );
}

#[test]
fn an_uncompressed_block_is_not_copied_when_its_trailing_stage_is_removed() {
    let bytes = vec![7u8; 64];
    let body = remove_trailing(Compression::None, &bytes).unwrap();
    assert!(matches!(body, std::borrow::Cow::Borrowed(_)));
}

#[test]
fn the_buffer_reusing_trailing_stage_removal_matches_remove_trailing() {
    let bytes = vec![7u8; 64];
    let mut scratch = vec![1u8; 8];
    let body = remove_trailing_into(Compression::None, &bytes, &mut scratch).unwrap();
    assert_eq!(body.as_ptr(), bytes.as_ptr());
    assert_eq!(scratch, vec![1u8; 8]);
    let borrowed = with_trailing_removed(Compression::None, &bytes, |body| Ok(body.as_ptr())).unwrap();
    assert_eq!(borrowed, bytes.as_ptr());

    let raw: Vec<u8> = (0..4096u32).map(|i| (i % 13) as u8).collect();
    let stored = compress_zstd_framed(&raw, 1);
    let inflated = remove_trailing_into(Compression::Zstd1, &stored, &mut scratch)
        .unwrap()
        .to_vec();
    assert_eq!(inflated, raw);
    assert_eq!(scratch, raw);
    assert_eq!(
        remove_trailing(Compression::Zstd1, &stored).unwrap().as_ref(),
        raw.as_slice()
    );
    let consumed = with_trailing_removed(Compression::Zstd1, &stored, |body| Ok(body.to_vec())).unwrap();
    assert_eq!(consumed, raw);
}

#[test]
fn a_framed_page_disagreeing_with_its_row_count_header_is_rejected_by_the_range_decoder() {
    let count = 1500usize;
    let mut body = Writer::new();
    body.put_u32(count as u32 + 1);
    for i in 0..count {
        body.put_u64(i as u64);
    }
    let bytes = seekable_zstd::compress(&body.into_bytes()).expect("the body compresses");

    let pipeline = PipelineId::new(Transform::PlainU64, Compression::SeekableZstd, ValueKind::U64);
    assert!(decode_block_range(pipeline, &bytes, 600, 700).is_err());
}

#[test]
fn fastlanes_kernels_round_trip_across_vector_boundaries() {
    for &count in &[1025u64, 3000] {
        let ramp: Vec<u64> = (0..count).map(|i| 1_700_000_000_000 + i * 17).collect();
        round_trip(ColumnData::U64(ramp), false);
        let signed: Vec<i64> = (0..count as i64).map(|i| -1_000_000 + i * 13).collect();
        round_trip(ColumnData::I64(signed), false);
        let labels: Vec<Option<String>> = (0..count)
            .map(|i| Some(["EUR", "USD", "NOK", "GBP"][(i % 4) as usize].to_owned()))
            .collect();
        round_trip(ColumnData::Strings(labels.into()), false);
        let metric: Vec<f64> = (0..count).map(|i| (i as f64) * 0.25 + 10.5).collect();
        round_trip(ColumnData::F64(metric), false);
    }
}

#[test]
fn integer_transforms_round_trip_signed_and_unsigned_blocks() {
    let transforms = [
        Transform::DeltaBitpack,
        Transform::ForBitpack,
        Transform::PlainU64,
        Transform::Rle,
    ];
    for count in [1i64, 1023, 1024, 1025, 3000] {
        let signed: Vec<i64> = (0..count).map(|i| (i % 97 - 48) * 1_000_003 - i).collect();
        let unsigned: Vec<u64> = signed.iter().map(|value| value.unsigned_abs() + 7).collect();
        for transform in transforms {
            for data in [ColumnData::I64(signed.clone()), ColumnData::U64(unsigned.clone())] {
                let block =
                    encode_block_forced_for_conformance(&data, false, CascadeStrategy::SizeOptimized, transform)
                        .unwrap();
                assert_eq!(
                    decode_block(block.pipeline, &block.bytes).unwrap(),
                    data,
                    "{transform:?} at {count} values"
                );
            }
        }
    }
}

fn running_sum_reference<T: IntValue>(differences: &[u64], carry: u64) -> (Vec<T>, u64) {
    let mut total = carry;
    let values = differences
        .iter()
        .map(|difference| {
            total = total.wrapping_add(*difference);
            T::from_code(total)
        })
        .collect();
    (values, total)
}

#[test]
fn prefix_sum_matches_a_running_sum() {
    let mut state = 0x2545_F491_4F6C_DD1D;
    for len in [0usize, 1, 2, 15, 16, 17, 31, 1023, 1024, 1025, 3001] {
        for carry in [0u64, 1, u64::MAX - 5, i64::MAX as u64] {
            for range_mask in [u64::MAX, 0xFFFF, 0] {
                let differences: Vec<u64> = (0..len).map(|_| pseudo_random(&mut state) & range_mask).collect();

                let (expected, expected_total) = running_sum_reference::<u64>(&differences, carry);
                let mut values = differences.clone();
                assert_eq!(prefix_sum(&mut values, carry), expected_total, "u64 total, len {len}");
                assert_eq!(values, expected, "u64 values, len {len}, carry {carry}");

                let (expected, expected_total) = running_sum_reference::<i64>(&differences, carry);
                let mut values: Vec<i64> = differences.iter().map(|bits| i64::from_bits(*bits)).collect();
                assert_eq!(prefix_sum(&mut values, carry), expected_total, "i64 total, len {len}");
                assert_eq!(values, expected, "i64 values, len {len}, carry {carry}");
            }
        }
    }
}

#[test]
fn delta_decode_matches_a_running_sum() {
    let mut state = 0x9E37_79B9_7F4A_7C15;
    for width in [0u32, 1, 17, 64] {
        for count in [0usize, 1, 5, 1023, 1024, 1025, 2500] {
            for first in [0u64, 42, u64::MAX - 2] {
                let codes: Vec<u64> = (0..count).map(|_| pseudo_random(&mut state) & mask(width)).collect();
                let mut writer = Writer::new();
                writer.put_u64(first);
                bitpack(&codes, width, &mut writer);
                let body = writer.into_bytes();
                let differences: Vec<u64> = codes.iter().map(|code| unzigzag(*code) as u64).collect();

                let (tail, _) = running_sum_reference::<u64>(&differences, first);
                let expected: Vec<u64> = std::iter::once(first).chain(tail).collect();
                let decoded = decode_delta_bitpack::<u64>(&mut Reader::new(&body)).unwrap();
                assert_eq!(decoded, expected, "u64, width {width}, count {count}, first {first}");

                let (tail, _) = running_sum_reference::<i64>(&differences, first);
                let expected: Vec<i64> = std::iter::once(unzigzag(first)).chain(tail).collect();
                let decoded = decode_delta_bitpack::<i64>(&mut Reader::new(&body)).unwrap();
                assert_eq!(decoded, expected, "i64, width {width}, count {count}, first {first}");
            }
        }
    }
}

#[test]
fn fused_i64_bitpack_encoders_match_the_unfused_mapped_path() {
    for count in [1i64, 1023, 1024, 1025, 3000] {
        let signed: Vec<i64> = (0..count).map(|i| (i % 97 - 48) * 1_000_003 - i).collect();
        let mapped: Vec<u64> = signed.iter().map(|value| zigzag(*value)).collect();

        let mut fused = Writer::new();
        encode_for_bitpack_i64(&signed, &mut fused);
        let mut unfused = Writer::new();
        encode_for_bitpack(&mapped, &mut unfused);
        assert_eq!(fused.into_bytes(), unfused.into_bytes(), "ForBitpack at {count} values");

        let mut fused = Writer::new();
        encode_delta_bitpack_i64(&signed, &mut fused);
        let mut unfused = Writer::new();
        encode_delta_bitpack(&mapped, &mut unfused);
        assert_eq!(
            fused.into_bytes(),
            unfused.into_bytes(),
            "DeltaBitpack at {count} values"
        );
    }
}

#[test]
fn frame_of_reference_near_the_top_of_the_range_stays_exact() {
    let data = ColumnData::I64(vec![i64::MIN, -1, i64::MIN + 5, -3]);
    let block =
        encode_block_forced_for_conformance(&data, false, CascadeStrategy::SizeOptimized, Transform::ForBitpack)
            .unwrap();
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);

    let mut forged = Writer::new();
    forged.put_u64(u64::MAX);
    bitpack(&[1], 1, &mut forged);
    let pipeline = PipelineId::new(Transform::ForBitpack, Compression::None, ValueKind::U64);
    assert!(decode_block(pipeline, &forged.into_bytes()).is_err());
}

#[test]
fn u128_stats_straddling_i128_max_are_both_present_or_both_absent() {
    let straddling = stats_for(&ColumnData::U128(vec![1, u128::MAX]));
    assert_eq!(straddling.min_i128, None);
    assert_eq!(straddling.max_i128, None);

    let within_range = stats_for(&ColumnData::U128(vec![1, i128::MAX as u128]));
    assert_eq!(within_range.min_i128, Some(1));
    assert_eq!(within_range.max_i128, Some(i128::MAX));

    let both_past_max = stats_for(&ColumnData::U128(vec![i128::MAX as u128 + 1, u128::MAX]));
    assert_eq!(both_past_max.min_i128, None);
    assert_eq!(both_past_max.max_i128, None);
}

#[test]
fn shredded_kinds_round_trip_and_range_decode_through_the_adaptive_encoder() {
    let u64_data = ColumnData::U64((0..5000u64).map(|i| i * 3 + 7).collect());
    let block = encode_block(&u64_data, true);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), u64_data);
    assert_eq!(
        decode_block_range(block.pipeline, &block.bytes, 100, 132).unwrap(),
        ColumnData::U64((100..132).map(|i| i * 3 + 7).collect())
    );

    for data in [
        ColumnData::I64((-500..500i64).map(|i| i * 11).collect()),
        ColumnData::F64((0..1000).map(|i| i as f64 * 0.25).collect()),
        ColumnData::Decimal {
            values: (0..500i128).map(|i| i * 1000 - 250_000).collect(),
            scale: 2,
        },
        ColumnData::U128((0..64u128).map(|i| i << 64 | i).collect()),
    ] {
        let block = encode_block(&data, true);
        assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
    }
}

#[test]
fn shredded_low_cardinality_strings_round_trip_and_range_decode() {
    let statuses = ["lost", "open", "pending", "won"];
    let values: Vec<Option<String>> = (0..800)
        .map(|i| {
            if i % 13 == 0 {
                None
            } else {
                Some(statuses[i % statuses.len()].to_owned())
            }
        })
        .collect();
    let data = ColumnData::Strings(values.clone().into());
    let block = encode_block(&data, true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
    assert_eq!(
        decode_block_range(block.pipeline, &block.bytes, 26, 39).unwrap(),
        ColumnData::Strings(values[26..39].to_vec().into())
    );
}

#[test]
fn shredded_high_cardinality_strings_round_trip() {
    let values: Vec<Option<String>> = (0..300).map(|i| Some(format!("event-{i:06}"))).collect();
    let data = ColumnData::Strings(values.into());
    let block = encode_block(&data, true);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
}

#[test]
fn shredded_encoding_is_deterministic() {
    let data = ColumnData::U64((0..2048u64).collect());
    let a = encode_block(&data, true);
    let b = encode_block(&data, true);
    assert_eq!(a.bytes, b.bytes);
    assert_eq!(a.pipeline, b.pipeline);
}

#[test]
fn retired_vortex_transform_id_is_rejected() {
    let pipeline = PipelineId(11);
    assert!(pipeline.transform().is_err());
    assert!(decode_block(pipeline, &[0u8; 16]).is_err());
    assert!(decode_block_range(pipeline, &[0u8; 16], 0, 4).is_err());
}

#[test]
fn dictionary_range_decode_matches_whole_block_across_vector_boundaries() {
    let statuses = ["lost", "open", "pending", "won"];
    let values: Vec<Option<String>> = (0..3_000)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(statuses[i % statuses.len()].to_owned())
            }
        })
        .collect();
    let block = encode_block(&ColumnData::Strings(values.clone().into()), true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    for (start, end) in [(0, 40), (1_000, 1_100), (2_040, 2_060), (2_990, 3_000), (1_023, 1_025)] {
        assert_eq!(
            decode_block_range(block.pipeline, &block.bytes, start, end).unwrap(),
            ColumnData::Strings(values[start..end].to_vec().into()),
            "range {start}..{end}"
        );
    }
}

fn zero_width_dictionary_block(
    values: &StringColumn,
    entries: &[&str],
    side: SideStream,
    codes: usize,
) -> (PipelineId, Vec<u8>) {
    let mut out = Writer::new();
    NullStream::from_values(values).write(&mut out);
    match side {
        SideStream::FileScopeDictionary => {}
        SideStream::FsstDictionaryValues => {
            out.put_u32(entries.len() as u32);
            out.put_u16(0);
            write_offsets(entries.iter().map(|entry| entry.len() * 2), &mut out).unwrap();
            for byte in entries.iter().flat_map(|entry| entry.bytes()) {
                out.put_u8(255);
                out.put_u8(byte);
            }
        }
        _ => {
            out.put_u32(entries.len() as u32);
            write_offsets(entries.iter().map(|entry| entry.len()), &mut out).unwrap();
            for entry in entries {
                out.put_slice(entry.as_bytes());
            }
        }
    }
    out.put_u32(codes as u32);
    out.put_u8(0);
    let pipeline =
        PipelineId::new(Transform::DictionaryString, Compression::None, ValueKind::String).with_side_stream(side);
    (pipeline, out.into_bytes())
}

#[test]
fn zero_width_dictionary_range_decode_matches_the_whole_block() {
    let alphabet = vec!["only \u{e9}".to_owned(), "other".to_owned()];
    let rows = 2000usize;
    let patterns: [(&str, fn(usize) -> bool); 4] = [
        ("all present", |_| true),
        ("all absent", |_| false),
        ("leading and trailing nulls", |i| (5..1500).contains(&i)),
        ("scattered bitmap", |i| i % 3 != 1),
    ];
    let shared = Some(alphabet.as_slice());
    for (name, present) in patterns {
        let values: StringColumn = (0..rows).map(|i| present(i).then_some(alphabet[0].as_str())).collect();
        let count = values.iter().flatten().count();
        for side in [
            SideStream::None,
            SideStream::FsstDictionaryValues,
            SideStream::FileScopeDictionary,
        ] {
            let (pipeline, bytes) = zero_width_dictionary_block(&values, &[alphabet[0].as_str()], side, count);
            assert_eq!(
                decode_block_shared(pipeline, &bytes, shared).unwrap(),
                ColumnData::Strings(values.clone()),
                "{name} {side:?}: full decode"
            );
            for (start, end) in [
                (0, rows),
                (0, 1),
                (3, 9),
                (1023, 1025),
                (1990, rows + 50),
                (rows, rows + 5),
            ] {
                let expected: StringColumn = values
                    .iter()
                    .skip(start)
                    .take(end.min(rows).saturating_sub(start))
                    .collect();
                assert_eq!(
                    decode_block_range_shared(pipeline, &bytes, shared, start, end).unwrap(),
                    ColumnData::Strings(expected),
                    "{name} {side:?}: range {start}..{end}"
                );
            }
        }
    }

    let half: StringColumn = (0..10).map(|i| (i >= 5).then_some("x")).collect();
    let (pipeline, bytes) = zero_width_dictionary_block(&half, &[], SideStream::None, 5);
    assert_eq!(
        decode_block_range(pipeline, &bytes, 0, 5).unwrap(),
        ColumnData::Strings((0..5).map(|_| None::<&str>).collect()),
        "no present row selected, so the empty dictionary is never consulted"
    );
    for result in [
        decode_block_range(pipeline, &bytes, 3, 8),
        decode_block(pipeline, &bytes),
    ] {
        assert!(
            matches!(
                result,
                Err(FormatError::RefOutOfRange {
                    what: "dictionary code"
                })
            ),
            "{result:?}"
        );
    }
}

fn forged_dictionary_block(values: &StringColumn, entries: Option<&[&str]>, codes: &[u64]) -> (PipelineId, Vec<u8>) {
    let mut out = Writer::new();
    NullStream::from_values(values).write(&mut out);
    let side = match entries {
        Some(entries) => {
            out.put_u32(entries.len() as u32);
            write_offsets(entries.iter().map(|entry| entry.len()), &mut out).unwrap();
            for entry in entries {
                out.put_slice(entry.as_bytes());
            }
            SideStream::None
        }
        None => SideStream::FileScopeDictionary,
    };
    bitpack(codes, bits_needed(codes.iter().copied().max().unwrap_or(0)), &mut out);
    let pipeline =
        PipelineId::new(Transform::DictionaryString, Compression::None, ValueKind::String).with_side_stream(side);
    (pipeline, out.into_bytes())
}

#[test]
fn dictionary_whole_block_decode_checks_every_code_before_weaving_nulls() {
    let alphabet = vec!["a".to_owned(), "\u{e9}t\u{e9}".to_owned()];
    let entries = ["a", "\u{e9}t\u{e9}"];
    let values: StringColumn = (0..4).map(|i| (i != 1).then_some("x")).collect();
    let disagree = FormatError::Structural {
        rule: "null bitmap disagrees with present count",
    };
    let out_of_range = FormatError::RefOutOfRange {
        what: "dictionary code",
    };
    for local in [Some(entries.as_slice()), None] {
        let decode = |codes: &[u64]| {
            let (pipeline, bytes) = forged_dictionary_block(&values, local, codes);
            decode_block_shared(pipeline, &bytes, Some(&alphabet))
        };
        assert_eq!(
            decode(&[1, 0, 1]).unwrap(),
            ColumnData::Strings(vec![Some("\u{e9}t\u{e9}"), None, Some("a"), Some("\u{e9}t\u{e9}")].into())
        );
        assert_eq!(decode(&[0, 1]).unwrap_err(), disagree, "too few codes");
        assert_eq!(decode(&[0, 1, 0, 1]).unwrap_err(), disagree, "too many codes");
        assert_eq!(decode(&[0, 1, 0, 2]).unwrap_err(), out_of_range, "invalid surplus code");
        assert_eq!(decode(&[2, 1]).unwrap_err(), out_of_range, "invalid code with too few");
    }

    let absent: StringColumn = (0..3).map(|_| None::<&str>).collect();
    let (pipeline, bytes) = forged_dictionary_block(&absent, Some(&[]), &[]);
    assert_eq!(decode_block(pipeline, &bytes).unwrap(), ColumnData::Strings(absent));
    let (pipeline, bytes) = forged_dictionary_block(&values, Some(&[]), &[0, 0, 0]);
    assert_eq!(
        decode_block(pipeline, &bytes).unwrap_err(),
        out_of_range,
        "empty dictionary"
    );
}

#[test]
fn dictionary_whole_block_decode_checks_every_entry_before_the_codes() {
    let values: StringColumn = (0..3).map(|_| Some("x")).collect();
    let plain = |offsets: &[u32], data: &[u8], codes: &[u64]| {
        let mut out = Writer::new();
        NullStream::from_values(&values).write(&mut out);
        out.put_u32(offsets.len() as u32 - 1);
        for &offset in offsets {
            out.put_u32(offset);
        }
        out.put_slice(data);
        bitpack(codes, bits_needed(codes.iter().copied().max().unwrap_or(0)), &mut out);
        let pipeline = PipelineId::new(Transform::DictionaryString, Compression::None, ValueKind::String);
        decode_block(pipeline, &out.into_bytes())
    };
    let utf8 = FormatError::InvalidUtf8 {
        what: "dictionary entry",
    };
    assert_eq!(
        plain(&[0, 1, 3], "a\u{e9}".as_bytes(), &[1, 0, 1]).unwrap(),
        ColumnData::Strings(vec![Some("\u{e9}"), Some("a"), Some("\u{e9}")].into())
    );
    assert_eq!(
        plain(&[1, 2, 4], "za\u{e9}".as_bytes(), &[0, 0, 1]).unwrap(),
        ColumnData::Strings(vec![Some("a"), Some("a"), Some("\u{e9}")].into()),
        "a leading offset other than zero"
    );
    assert_eq!(
        plain(&[0, 1, 2], b"a\xFF", &[0, 0, 0]).unwrap_err(),
        utf8,
        "unused invalid entry"
    );
    assert_eq!(
        plain(&[0, 1, 2], b"a\xFF", &[0, 9]).unwrap_err(),
        utf8,
        "beats a bad code and count"
    );
    assert_eq!(
        plain(&[0, 1, 3], "\u{e9}b".as_bytes(), &[0, 0, 0]).unwrap_err(),
        utf8,
        "split character"
    );
    assert_eq!(
        plain(&[0, 2, 1, 3], b"abc", &[0, 0, 0]).unwrap_err(),
        FormatError::Structural {
            rule: "dictionary offsets must be non-decreasing",
        }
    );
    assert!(matches!(
        plain(&[0, 1, 9], b"ab", &[0, 0, 0]),
        Err(FormatError::Truncated { .. })
    ));

    let long = "y".repeat(DICTIONARY_FSST_MAX_VALUE_LEN + 1);
    let (pipeline, bytes) = zero_width_dictionary_block(&values, &[long.as_str()], SideStream::FsstDictionaryValues, 1);
    assert_eq!(
        decode_block(pipeline, &bytes).unwrap_err(),
        FormatError::Structural {
            rule: "dictionary entry exceeds the FSST value cap",
        },
        "the cap beats a count disagreement"
    );
}

fn long_free_text(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| {
            (i % 9 != 0).then(|| {
                format!(
                    "note {i:06}: the renewal call covered pricing, the pilot rollout, and the security review; \
                     the account team agreed to send a revised quote by {}/{} and to schedule the migration \
                     workshop once procurement signs off.",
                    i % 28 + 1,
                    i % 12 + 1
                )
            })
        })
        .collect()
}

#[test]
fn a_raw_string_block_holding_invalid_utf8_is_rejected() {
    let pipeline = PipelineId::new(Transform::RawString, Compression::None, ValueKind::String);
    let mut out = Writer::new();
    out.put_u32(1);
    out.put_u8(0);
    out.put_u32(1);
    out.put_u32(0);
    out.put_u32(1);
    out.put_slice(&[0xFF]);
    let forged = out.into_bytes();

    assert!(decode_block(pipeline, &forged).is_err());
    assert!(decode_string_block_views(pipeline, &forged).is_err());
    assert!(decode_block_range(pipeline, &forged, 0, 1).is_err());
}

fn forged_raw_string_block(values: &StringColumn, offsets: &[u32], data: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    NullStream::from_values(values).write(&mut out);
    out.put_u32(offsets.len().saturating_sub(1) as u32);
    for &offset in offsets {
        out.put_u32(offset);
    }
    out.put_slice(data);
    out.into_bytes()
}

#[test]
fn raw_string_whole_block_decode_validates_every_value_before_weaving_nulls() {
    let pipeline = PipelineId::new(Transform::RawString, Compression::None, ValueKind::String);
    let values: StringColumn = (0..4).map(|i| (i != 1).then_some("x")).collect();
    let decode =
        |offsets: &[u32], data: &[u8]| decode_block(pipeline, &forged_raw_string_block(&values, offsets, data));
    let disagree = FormatError::Structural {
        rule: "null bitmap disagrees with present count",
    };
    let utf8 = FormatError::InvalidUtf8 { what: "raw string" };

    assert_eq!(
        decode(&[0, 1, 1, 3], "a\u{e9}".as_bytes()).unwrap(),
        ColumnData::Strings(vec![Some("a"), None, Some(""), Some("\u{e9}")].into())
    );
    assert_eq!(decode(&[0, 1, 2], b"ab").unwrap_err(), disagree, "too few values");
    assert_eq!(
        decode(&[0, 1, 2, 3, 4], b"abcd").unwrap_err(),
        disagree,
        "too many values"
    );
    assert_eq!(
        decode(&[0, 1, 2, 3, 4], b"abc\xFF").unwrap_err(),
        utf8,
        "invalid surplus value"
    );
    assert_eq!(
        decode(&[0, 1, 2], b"\xFFb").unwrap_err(),
        utf8,
        "invalid value with too few"
    );
    assert_eq!(
        decode(&[0, 2, 1, 3], b"abc").unwrap_err(),
        FormatError::Structural {
            rule: "raw string offsets must be non-decreasing",
        }
    );
    assert!(matches!(
        decode(&[0, 1, 2, 9], b"abc"),
        Err(FormatError::Truncated { .. })
    ));
    assert_eq!(
        decode(&[2, 3, 4, 5], b"zzabc").unwrap(),
        ColumnData::Strings(vec![Some("a"), None, Some("b"), Some("c")].into()),
        "a leading offset other than zero"
    );
}

#[test]
fn raw_string_range_decode_matches_the_whole_block() {
    let values = long_free_text(2_000);
    let block = encode_block(&ColumnData::Strings(values.clone().into()), false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::RawString);
    assert!(block.pipeline.supports_byte_range_extraction().unwrap());
    for (start, end) in [(0, 2_000), (0, 1), (9, 10), (137, 138), (500, 640), (1_999, 2_000)] {
        assert_eq!(
            decode_block_range(block.pipeline, &block.bytes, start, end).unwrap(),
            ColumnData::Strings(values[start..end].to_vec().into()),
            "range {start}..{end}"
        );
    }
    assert_eq!(
        decode_block_range(block.pipeline, &block.bytes, 7, 7).unwrap(),
        ColumnData::Strings(StringColumn::new())
    );
    assert_eq!(
        decode_block_range(block.pipeline, &block.bytes, 1_995, 2_100).unwrap(),
        ColumnData::Strings(values[1_995..].to_vec().into())
    );
}

#[test]
fn string_arenas_range_decode_from_a_deflate_page() {
    let raw = long_free_text(2_000);
    let short: Vec<Option<String>> = (0..3_000)
        .map(|i| (i % 11 != 0).then(|| format!("session-{i:07}@tenant-{:03}.example.com", i % 400)))
        .collect();
    for (values, transform) in [(raw, Transform::RawString), (short, Transform::FsstString)] {
        let block = encode_block(&ColumnData::Strings(values.clone().into()), true);
        assert_eq!(block.pipeline.transform().unwrap(), transform);
        assert_eq!(
            block.pipeline.compression().unwrap(),
            Compression::SeekableZstd,
            "{transform:?} keeps a trailing stage that preserves range access"
        );
        assert!(block.pipeline.supports_byte_range_extraction().unwrap());
        assert_eq!(
            decode_block(block.pipeline, &block.bytes).unwrap(),
            ColumnData::Strings(values.clone().into())
        );
        let rows = values.len();
        for (start, end) in [(0, rows), (0, 1), (11, 12), (rows / 2, rows / 2 + 3), (rows - 1, rows)] {
            assert_eq!(
                decode_block_range(block.pipeline, &block.bytes, start, end).unwrap(),
                ColumnData::Strings(values[start..end].to_vec().into()),
                "{transform:?} range {start}..{end}"
            );
        }
    }
}

#[test]
fn string_framed_range_decode_matches_the_full_decode_and_inflates_each_frame_once() {
    let raw = long_free_text(2_000);
    let short: Vec<Option<String>> = (0..3_000)
        .map(|i| (i % 11 != 0).then(|| format!("session-{i:07}@tenant-{:03}.example.com", i % 400)))
        .collect();
    for (values, transform) in [(raw, Transform::RawString), (short, Transform::FsstString)] {
        let block = encode_block(&ColumnData::Strings(values.clone().into()), true);
        assert_eq!(block.pipeline.transform().unwrap(), transform);
        assert_eq!(block.pipeline.compression().unwrap(), Compression::SeekableZstd);
        let body = seekable_zstd::decompress_all(&block.bytes).unwrap();
        let breaks: Vec<usize> = (1..body.len() / 997).map(|i| i * 997).collect();
        let bytes = seekable_zstd::compress_with_breaks(&body, &breaks).expect("the body compresses");
        let table = seekable_zstd::seek_table(&bytes).unwrap();
        let rows = values.len();
        for (start, end) in [
            (0, rows),
            (0, 1),
            (11, 12),
            (rows / 2, rows / 2 + 3),
            (rows - 5, rows + 20),
            (rows, rows + 4),
        ] {
            let mut window = seekable_zstd::Window::open(&bytes).unwrap();
            seekable_zstd::take_frames_inflated();
            let header = read_windowed_string_header(transform, &mut window).unwrap();
            let header_frames = seekable_zstd::take_frames_inflated() as u32;
            let mut ranges = vec![(0, table.frame_end_decomp(header_frames - 1).unwrap() as usize)];
            let effective_end = end.min(rows);
            if start < effective_end {
                let present_start = header.nulls.present_before(start);
                let present_end = header.nulls.present_before(effective_end);
                let offsets_at = header.offsets_start + present_start * 4;
                let offsets_len = (present_end - present_start + 1) * 4;
                let offsets = decode_offsets(&window.read(offsets_at, offsets_len).unwrap());
                let (first, last) = (offsets[0], offsets[offsets.len() - 1]);
                ranges.push((offsets_at, offsets_len));
                ranges.push((
                    header.offsets_start + (header.present_count + 1) * 4 + first,
                    last - first,
                ));
            }
            drop(window);

            seekable_zstd::take_frames_inflated();
            let decoded = decode_block_range(block.pipeline, &bytes, start, end).unwrap();
            assert_eq!(
                seekable_zstd::take_frames_inflated(),
                frames_covering(&bytes, &ranges),
                "{transform:?} frames inflated for rows {start}..{end}"
            );
            assert_eq!(
                decoded,
                ColumnData::Strings(values[start..effective_end].to_vec().into()),
                "{transform:?} rows {start}..{end}"
            );
        }
    }
}

#[test]
fn a_frame_window_decompresses_only_the_frames_a_range_falls_in() {
    let frame = seekable_zstd::FRAME_BYTES as usize;
    let plain: Vec<u8> = (0..(frame * 6 + 40) as u32).map(|i| (i % 251) as u8).collect();
    let page = seekable_zstd::compress(&plain).expect("the arena compresses");

    let mut window = seekable_zstd::Window::open(&page).unwrap();
    assert_eq!(window.plain_len(), plain.len());
    assert_eq!(
        window.read(frame * 3 + 100, 40).unwrap(),
        plain[frame * 3 + 100..frame * 3 + 140]
    );
    assert_eq!(window.decompressed_frames(), 1);
    assert_eq!(window.read(frame - 3, 6).unwrap(), plain[frame - 3..frame + 3]);
    assert_eq!(window.decompressed_frames(), 3);

    assert_eq!(window.read(0, plain.len()).unwrap(), plain);
    assert_eq!(window.read(plain.len() - 5, 5).unwrap(), plain[plain.len() - 5..]);
    assert_eq!(window.read(7, 0).unwrap(), Vec::<u8>::new());
    assert!(window.read(plain.len() - 4, 5).is_err());
    assert_eq!(seekable_zstd::decompress_all(&page).unwrap(), plain);
}

#[test]
fn fsst_buffer_reuse_matches_per_value_compress() {
    let values: Vec<String> = (0..512)
        .map(|i| match i % 5 {
            0 => String::new(),
            1 => format!("id{i}"),
            2 => format!("account-{i}-renewal-opportunity-{}", i * 31),
            3 => "übergroße Snowman ☃ value ".repeat(i % 7 + 1),
            _ => format!("{:x}", i * 2_654_435_761usize).repeat(200),
        })
        .collect();
    let corpus: Vec<&[u8]> = values.iter().map(|value| value.as_bytes()).collect();
    let compressor = fsst::Compressor::train(&corpus);
    let mut scratch = Vec::new();
    for value in &corpus {
        compress_fsst_value(&compressor, value, &mut scratch);
        assert_eq!(scratch, compressor.compress(value));
    }
}

#[test]
fn fsst_buffer_reuse_survives_the_all_escape_worst_case() {
    let corpus: Vec<&[u8]> = vec![b"aaaaaaaaaaaaaaaa"];
    let compressor = fsst::Compressor::train(&corpus);
    let plaintext = vec![b'z'; 4096];
    let mut scratch = Vec::new();
    compress_fsst_value(&compressor, &plaintext, &mut scratch);
    assert_eq!(scratch, compressor.compress(&plaintext));
    assert_eq!(scratch.len(), plaintext.len() * 2);
}

#[test]
fn the_decode_cost_multiplier_breaks_marginal_ties_toward_the_cheaper_family() {
    let mut state = 0xACE1_u64;
    let mut values = Vec::with_capacity(4096);
    for _ in 0..1024 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let value = (state >> 32) & 0x1FF_FFFF;
        for _ in 0..4 {
            values.push(value);
        }
    }
    assert_eq!(
        choose_u64_transform(&values, CascadeStrategy::SizeOptimized),
        Transform::Rle,
        "rewrite keeps the smaller bytes"
    );
    assert_ne!(
        choose_u64_transform(&values, CascadeStrategy::DecodeOptimized),
        Transform::Rle,
        "fresh publication must not keep a marginal RLE win over the FastLanes kernels"
    );
    let decode = encode_block_with_strategy(&ColumnData::U64(values.clone()), true, CascadeStrategy::DecodeOptimized);
    assert_eq!(
        decode_block(decode.pipeline, &decode.bytes).unwrap(),
        ColumnData::U64(values.clone())
    );
    let size = encode_block_with_strategy(&ColumnData::U64(values.clone()), true, CascadeStrategy::SizeOptimized);
    assert_eq!(
        decode_block(size.pipeline, &size.bytes).unwrap(),
        ColumnData::U64(values)
    );
}

#[test]
fn replay_skips_selection_and_the_trip_wire_rearms_on_drift() {
    let head_values: Vec<u64> = (0..2048u64).map(|i| 10_000 + i * 3).collect();
    let head = encode_block(&ColumnData::U64(head_values.clone()), true);
    let capture = ReplayCapture::from_head(
        &ColumnData::U64(head_values),
        head.pipeline,
        head.uncompressed_len,
        None,
    )
    .unwrap();
    let next: Vec<u64> = (0..2048u64).map(|i| 500_000 + i * 3).collect();
    let (block, replayed) = encode_block_replayed(
        &ColumnData::U64(next.clone()),
        true,
        CascadeStrategy::DecodeOptimized,
        Some(&capture),
        None,
    );
    assert!(replayed, "a stable distribution keeps the replay");
    assert_eq!(block.pipeline.transform().unwrap(), capture.transform);
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::U64(next)
    );

    let constant_head = encode_block(&ColumnData::U64(vec![7; 2048]), true);
    let stale = ReplayCapture::from_head(
        &ColumnData::U64(vec![7; 2048]),
        constant_head.pipeline,
        constant_head.uncompressed_len,
        None,
    )
    .unwrap();
    let mut state = 0x5EED_u64;
    let noise: Vec<u64> = (0..2048)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state >> 16
        })
        .collect();
    let (block, replayed) = encode_block_replayed(
        &ColumnData::U64(noise.clone()),
        true,
        CascadeStrategy::DecodeOptimized,
        Some(&stale),
        None,
    );
    assert!(!replayed, "the trip-wire must re-arm full selection on drift");
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::U64(noise)
    );
}

fn stored_fsst_table(block: &EncodedBlock) -> Vec<(u64, u8)> {
    let body = remove_trailing(block.pipeline.compression().unwrap(), &block.bytes).unwrap();
    let mut reader = Reader::new(&body);
    NullStream::read(&mut reader).unwrap();
    fsst_table_entries(&FsstTable(read_fsst_compressor(&mut reader).unwrap()))
}

fn fsst_table_entries(table: &FsstTable) -> Vec<(u64, u8)> {
    table
        .0
        .symbol_table()
        .iter()
        .map(|symbol| symbol.to_u64())
        .zip(table.0.symbol_lengths().iter().copied())
        .collect()
}

fn fsst_user_value(i: u64) -> Option<String> {
    Some(format!("user-{}@example-{}.test", i * 7919 % 100_003, i % 13))
}

#[test]
fn a_replayed_fsst_block_stores_the_heads_table_instead_of_training_its_own() {
    let head_values: Vec<Option<String>> = (0..2048u64)
        .map(|i| {
            if i % 4 == 3 {
                Some(format!("order:{}/status=shipped", i * 104_729 % 99_991))
            } else {
                fsst_user_value(i)
            }
        })
        .collect();
    let head_data = ColumnData::Strings(head_values.into());
    let mut head = encode_block(&head_data, false);
    assert_eq!(head.pipeline.transform().unwrap(), Transform::FsstString);
    let head_table = head.fsst.take().expect("an FSST head hands out the table it trained");
    let capture = ReplayCapture::from_head(
        &head_data,
        head.pipeline,
        head.uncompressed_len,
        Some(head_table.clone()),
    )
    .unwrap();
    assert!(capture.fsst.is_some());

    let later = ColumnData::Strings((5_000..7_048u64).map(fsst_user_value).collect::<Vec<_>>().into());
    let (block, replayed) =
        encode_block_replayed(&later, false, CascadeStrategy::DecodeOptimized, Some(&capture), None);
    assert!(replayed, "the same distribution keeps the replay");
    assert!(block.fsst.is_none(), "a replayed block trains no table of its own");
    assert_eq!(stored_fsst_table(&block), fsst_table_entries(&head_table));
    let own = encode_block(&later, false);
    assert_ne!(
        stored_fsst_table(&own),
        fsst_table_entries(&head_table),
        "the block's own training would store a different table"
    );
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), later);

    let bare = ReplayCapture { fsst: None, ..capture };
    let (block, replayed) = encode_block_replayed(&later, false, CascadeStrategy::DecodeOptimized, Some(&bare), None);
    assert!(replayed);
    assert!(block.fsst.is_some(), "a capture without a table trains one");
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), later);
}

#[test]
fn a_shifted_distribution_retrains_its_fsst_table() {
    let head_data = ColumnData::Strings((0..2048u64).map(fsst_user_value).collect::<Vec<_>>().into());
    let mut head = encode_block(&head_data, false);
    let head_table = head.fsst.take().unwrap();
    let capture = ReplayCapture::from_head(
        &head_data,
        head.pipeline,
        head.uncompressed_len,
        Some(head_table.clone()),
    )
    .unwrap();
    let shifted = ColumnData::Strings(
        (0..2048u64)
            .map(|i| Some(format!("ORDER#{:X}#SHIPPED#{}", i * 2_654_435_761, i % 97)))
            .collect::<Vec<_>>()
            .into(),
    );
    let (block, replayed) =
        encode_block_replayed(&shifted, false, CascadeStrategy::DecodeOptimized, Some(&capture), None);
    assert!(!replayed, "a stale table must re-arm full selection");
    assert_eq!(block.pipeline.transform().unwrap(), Transform::FsstString);
    assert_ne!(stored_fsst_table(&block), fsst_table_entries(&head_table));
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), shifted);
}

#[test]
fn a_trailing_stage_cannot_hide_an_amplifying_replay() {
    let long: String = (0..2_000u64)
        .map(|k| char::from(b'!' + (k.wrapping_mul(2_654_435_761) ^ (k >> 3)).wrapping_rem(90) as u8))
        .collect();
    let mut head_values: Vec<Option<String>> = vec![Some("small".to_owned()); 7];
    head_values.insert(0, Some(long));
    let head_data = ColumnData::Strings(head_values.into());
    let head = encode_block(&head_data, true);
    assert_eq!(head.pipeline.transform().unwrap(), Transform::RawString);
    let capture = ReplayCapture::from_head(&head_data, head.pipeline, head.uncompressed_len, None).unwrap();

    let later = ColumnData::Strings(vec![Some("small".to_owned()); 64].into());
    let (block, replayed) = encode_block_replayed(&later, true, CascadeStrategy::DecodeOptimized, Some(&capture), None);
    assert!(
        !replayed,
        "a replay whose transform amplifies the block must re-arm full selection, however well its bytes compress"
    );
    let selected = encode_block(&later, true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(block.bytes.len(), selected.bytes.len());
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), later);
}

#[test]
fn a_replay_past_the_plain_form_falls_back_to_full_selection() {
    let mut values = Vec::new();
    let mut next = 0u64;
    'outer: loop {
        for run_len in [2usize, 1, 1, 2, 1] {
            for _ in 0..run_len {
                values.push(next);
                if values.len() == 8192 {
                    break 'outer;
                }
            }
            next += 1;
        }
    }
    let data = ColumnData::U64(values.clone());
    let head = ReplayCapture {
        compression: Compression::None,
        decoded_len: 90,
        fsst: None,
        raw_len: 100,
        transform: Transform::Rle,
    };
    let (block, replayed) = encode_block_replayed(&data, true, CascadeStrategy::DecodeOptimized, Some(&head), None);
    assert!(
        !replayed,
        "a replay past the plain form must fall back to full selection"
    );
    let plain = encode_block_inner(
        &data,
        true,
        CascadeStrategy::DecodeOptimized,
        Some(Transform::PlainU64),
        Some(Compression::None),
        None,
        None,
        false,
    );
    assert!(
        block.bytes.len() <= plain.bytes.len(),
        "the kept block ({}) must never store more than the plain form ({})",
        block.bytes.len(),
        plain.bytes.len()
    );
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::U64(values)
    );
}

#[test]
fn conformance_forcing_exercises_every_family_and_rejects_invalid_ones() {
    let ints: Vec<u64> = (0..512u64).map(|i| 40 + i % 7).collect();
    let strings: Vec<Option<String>> = (0..64).map(|i| Some(format!("value-{}", i % 9))).collect();
    let floats: Vec<f64> = (0..512).map(|i| (i as f64) * 0.25 + 1.5).collect();
    let cases: Vec<(Transform, ColumnData)> = vec![
        (Transform::PlainU64, ColumnData::U64(ints.clone())),
        (Transform::ForBitpack, ColumnData::U64(ints.clone())),
        (Transform::DeltaBitpack, ColumnData::U64(ints.clone())),
        (Transform::Rle, ColumnData::U64(ints.clone())),
        (Transform::DictionaryString, ColumnData::Strings(strings.clone().into())),
        (Transform::FsstString, ColumnData::Strings(strings.clone().into())),
        (Transform::RawString, ColumnData::Strings(strings.clone().into())),
        (Transform::Alp, ColumnData::F64(floats.clone())),
        (Transform::AlpRd, ColumnData::F64(floats.clone())),
        (Transform::ByteStreamSplit, ColumnData::F64(floats.clone())),
        (Transform::PlainF64, ColumnData::F64(floats)),
        (
            Transform::Decimal128,
            ColumnData::Decimal {
                scale: 2,
                values: vec![1_00, 2_50, 99_99],
            },
        ),
        (Transform::PlainU128, ColumnData::U128(vec![1, u128::MAX, 42])),
    ];
    for (transform, data) in cases {
        let block =
            encode_block_forced_for_conformance(&data, true, CascadeStrategy::DecodeOptimized, transform).unwrap();
        assert_eq!(
            block.pipeline.transform().unwrap(),
            transform,
            "forcing {transform:?} must record it"
        );
        assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
    }
    assert!(
        encode_block_forced_for_conformance(
            &ColumnData::Strings(strings.into()),
            true,
            CascadeStrategy::DecodeOptimized,
            Transform::Rle
        )
        .is_err()
    );
    assert!(
        encode_block_forced_for_conformance(
            &ColumnData::U64(ints),
            true,
            CascadeStrategy::DecodeOptimized,
            Transform::DictionaryString
        )
        .is_err()
    );
}

fn mix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn one_noisy_vector() -> Vec<f64> {
    (0..4096)
        .map(|i| {
            if (1024..2048).contains(&i) {
                f64::from_bits(0x3FF0_0000_0000_0000 | (mix(i as u64) & 0x000F_FFFF_FFFF_FFFF))
            } else {
                (i as f64) * 0.25
            }
        })
        .collect()
}

#[test]
fn alp_escapes_a_pathological_vector_instead_of_abandoning_the_block() {
    let values = one_noisy_vector();
    let block = encode_block(&ColumnData::F64(values.clone()), false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Alp);
    let body = remove_trailing(block.pipeline.compression().unwrap(), &block.bytes).unwrap();
    assert_eq!(body[0], ALP_VECTOR_ESCAPE_SENTINEL);
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::F64(values)
    );
}

#[test]
fn alp_escaped_vectors_survive_range_decode_and_predicate_filtering() {
    let values = one_noisy_vector();
    let block = encode_block(&ColumnData::F64(values.clone()), true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Alp);
    for (start, end) in [
        (0, 4096),
        (1000, 1100),
        (1500, 1600),
        (2000, 2100),
        (900, 2100),
        (4090, 4096),
    ] {
        let ColumnData::F64(ranged) = decode_block_range(block.pipeline, &block.bytes, start, end).unwrap() else {
            panic!("alp block must range-decode to floats");
        };
        assert_eq!(ranged, values[start..end], "range [{start}, {end})");
    }
    let filter = predicate::FloatPredicate::Range {
        lower: Some(predicate::FloatBound {
            inclusive: true,
            value: 1.0,
        }),
        upper: Some(predicate::FloatBound {
            inclusive: false,
            value: 600.0,
        }),
    };
    let mask = predicate::filter_float_block(block.pipeline, &block.bytes, &filter)
        .unwrap()
        .expect("alp blocks answer float predicates from compressed bytes");
    assert_eq!(mask, filter.filter_decoded(&values));
}

#[test]
fn well_behaved_alp_blocks_keep_the_unescaped_layout() {
    let values: Vec<f64> = (0..4096).map(|i| (i as f64) * 0.25).collect();
    let block = encode_block(&ColumnData::F64(values.clone()), true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Alp);
    let body = remove_trailing(block.pipeline.compression().unwrap(), &block.bytes).unwrap();
    assert_ne!(body[0], ALP_VECTOR_ESCAPE_SENTINEL);
}

#[test]
fn alp_rd_encodes_high_entropy_doubles_with_shared_top_bits() {
    let values: Vec<f64> = (0..2048)
        .map(|i| f64::from_bits(0x43F0_0000_0000_0000 | (mix(i) >> 12)))
        .collect();
    let block = encode_block(&ColumnData::F64(values.clone()), false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::AlpRd);
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::F64(values)
    );
}

#[test]
fn alp_rd_round_trips_arbitrary_floats_when_forced() {
    let values: Vec<f64> = (0..1500)
        .map(|i| match i % 5 {
            0 => -(i as f64) * 3.5,
            1 => f64::from_bits(0x40E0_0000_0000_0000 | (mix(i as u64) & 0x000F_FFFF_FFFF_FFFF)),
            2 => 0.0,
            3 => (i as f64).sqrt(),
            _ => f64::MAX / (i as f64 + 1.0),
        })
        .collect();
    let block = encode_block_forced_for_conformance(
        &ColumnData::F64(values.clone()),
        true,
        CascadeStrategy::DecodeOptimized,
        Transform::AlpRd,
    )
    .unwrap();
    assert_eq!(block.pipeline.transform().unwrap(), Transform::AlpRd);
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::F64(values.clone())
    );
    let ColumnData::F64(ranged) = decode_block_range(block.pipeline, &block.bytes, 700, 900).unwrap() else {
        panic!("alp-rd block must range-decode to floats");
    };
    assert_eq!(ranged, values[700..900]);
}

#[test]
fn dictionary_blocks_fsst_compress_a_bulky_alphabet_and_keep_code_pushdown() {
    let distinct: Vec<String> = (0..200)
        .map(|i| {
            format!(
                "https://events.example.com/api/v1/tenants/tenant-{i:04}/streams/ingest/checkpoints/segment-{i:04}/manifest.json"
            )
        })
        .collect();
    let values: Vec<Option<String>> = (0..2000)
        .map(|i| (i % 13 != 0).then(|| distinct[i % 200].clone()))
        .collect();
    let block = encode_block(&ColumnData::Strings(values.clone().into()), false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::FsstDictionaryValues);
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::Strings(values.clone().into())
    );
    let ColumnData::Strings(ranged) = decode_block_range(block.pipeline, &block.bytes, 100, 250).unwrap() else {
        panic!("dictionary block must range-decode to strings");
    };
    assert_eq!(ranged, values[100..250].to_vec().into());
    let views = decode_string_block_views(block.pipeline, &block.bytes)
        .unwrap()
        .expect("dictionary blocks decode to views");
    for (row, expected) in values.iter().enumerate() {
        match expected {
            Some(text) => assert_eq!(views.value(row), text.as_str()),
            None => assert!(views.is_null(row)),
        }
    }
    let descriptor::PageDescriptor::Dictionary { entries } =
        descriptor::extract_descriptor(block.pipeline, &block.bytes).unwrap()
    else {
        panic!("dictionary block must describe its entries");
    };
    let mut sorted = distinct.clone();
    sorted.sort();
    assert_eq!(entries, sorted);
    let filters = [
        predicate::StringPredicate::Equals(distinct[17].clone()),
        predicate::StringPredicate::NotEquals(distinct[17].clone()),
        predicate::StringPredicate::InSet(vec![distinct[3].clone(), distinct[150].clone(), "absent".to_owned()]),
        predicate::StringPredicate::Range {
            lower: Some(predicate::StringBound {
                inclusive: true,
                value: distinct[40].clone(),
            }),
            upper: Some(predicate::StringBound {
                inclusive: false,
                value: distinct[160].clone(),
            }),
        },
    ];
    for filter in filters {
        let mask = predicate::filter_string_block(block.pipeline, &block.bytes, &filter)
            .unwrap()
            .expect("dictionary blocks answer string predicates from codes");
        assert_eq!(mask, filter.filter_decoded(&values.clone().into()), "{filter:?}");
    }
}

#[test]
fn small_cheap_alphabets_keep_the_plain_dictionary_value_stream() {
    let values: Vec<Option<String>> = (0..600).map(|i| Some(format!("kind-{}", i % 8))).collect();
    let block = encode_block(&ColumnData::Strings(values.clone().into()), false);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::None);
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::Strings(values.into())
    );
}

#[test]
fn a_replayed_block_runs_only_its_captured_trailing_codec() {
    let values: Vec<u64> = (0..8192u64).map(|i| (i % 64) * 7).collect();
    let capture = |compression| ReplayCapture {
        compression,
        decoded_len: 8192 * 8,
        fsst: None,
        raw_len: 8192 * 8,
        transform: Transform::ForBitpack,
    };
    for captured in [Compression::Lz4, Compression::Zstd1, Compression::None] {
        let (block, replayed) = encode_block_replayed(
            &ColumnData::U64(values.clone()),
            false,
            CascadeStrategy::DecodeOptimized,
            Some(&capture(captured)),
            None,
        );
        assert!(replayed, "a generous capture head must keep the replay");
        assert_eq!(block.pipeline.compression().unwrap(), captured, "captured {captured:?}");
        assert_eq!(
            decode_block(block.pipeline, &block.bytes).unwrap(),
            ColumnData::U64(values.clone())
        );
    }
}

#[test]
fn shared_scope_dictionary_blocks_store_codes_only_and_translate_once() {
    let alphabet: Vec<String> = ["blue", "green", "red", "yellow"].map(str::to_owned).to_vec();
    let values: Vec<Option<String>> = (0..600)
        .map(|i| (i % 7 != 0).then(|| alphabet[i % 4].clone()))
        .collect();
    let shared = encode_block_with_shared_dictionary(
        &ColumnData::Strings(values.clone().into()),
        false,
        CascadeStrategy::DecodeOptimized,
        Some(&alphabet),
    );
    assert_eq!(shared.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(shared.pipeline.side_stream().unwrap(), SideStream::FileScopeDictionary);
    let local = encode_block(&ColumnData::Strings(values.clone().into()), false);
    assert!(
        shared.bytes.len() < local.bytes.len(),
        "the shared scope must drop the value stream ({} vs {})",
        shared.bytes.len(),
        local.bytes.len()
    );

    assert!(
        decode_block(shared.pipeline, &shared.bytes).is_err(),
        "a shared-scope block must refuse to decode without its alphabet"
    );
    assert_eq!(
        decode_block_shared(shared.pipeline, &shared.bytes, Some(&alphabet)).unwrap(),
        ColumnData::Strings(values.clone().into())
    );
    assert_eq!(
        decode_block_range_shared(shared.pipeline, &shared.bytes, Some(&alphabet), 100, 130).unwrap(),
        ColumnData::Strings(values[100..130].to_vec().into())
    );
    let views = decode_string_block_views_shared(shared.pipeline, &shared.bytes, Some(&alphabet))
        .unwrap()
        .expect("dictionary blocks decode to views");
    let prepared = prepare_shared_string_view_dictionary(&alphabet).unwrap();
    let prepared_views =
        decode_string_block_views_shared_prepared(shared.pipeline, &shared.bytes, Some(&alphabet), Some(&prepared))
            .unwrap()
            .expect("dictionary blocks decode through the prepared alphabet");
    for (row, expected) in values.iter().enumerate() {
        match expected {
            Some(text) => {
                assert_eq!(views.value(row), text.as_str(), "row {row}");
                assert_eq!(prepared_views.value(row), text.as_str(), "prepared row {row}");
            }
            None => {
                assert!(views.is_null(row), "row {row}");
                assert!(prepared_views.is_null(row), "prepared row {row}");
            }
        }
    }

    let filters = [
        predicate::StringPredicate::Equals("green".to_owned()),
        predicate::StringPredicate::NotEquals("red".to_owned()),
        predicate::StringPredicate::Range {
            lower: Some(predicate::StringBound {
                inclusive: true,
                value: "blue".to_owned(),
            }),
            upper: Some(predicate::StringBound {
                inclusive: false,
                value: "red".to_owned(),
            }),
        },
    ];
    for filter in filters {
        let translated = predicate::translate_for_shared_dictionary(&filter, &alphabet);
        let mask = predicate::filter_string_block_shared(shared.pipeline, &shared.bytes, &filter, Some(&translated))
            .unwrap()
            .expect("shared-scope blocks answer from codes with the once-per-file translation");
        assert_eq!(mask, filter.filter_decoded(&values.clone().into()), "{filter:?}");
        assert!(
            predicate::filter_string_block(shared.pipeline, &shared.bytes, &filter)
                .unwrap()
                .is_none(),
            "without the translation the fast path declines rather than guessing"
        );
    }

    let mut novel = values.clone();
    novel[3] = Some("purple".to_owned());
    let block = encode_block_with_shared_dictionary(
        &ColumnData::Strings(novel.clone().into()),
        false,
        CascadeStrategy::DecodeOptimized,
        Some(&alphabet),
    );
    assert_ne!(
        block.pipeline.side_stream().unwrap(),
        SideStream::FileScopeDictionary,
        "a value outside the alphabet keeps a block-local dictionary"
    );
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::Strings(novel.into())
    );

    let forged = PipelineId((shared.pipeline.0 & 0x00FF_FFFF) | (4 << 24));
    assert!(forged.side_stream().is_err(), "the external scope is reserved");
}

#[test]
fn prepared_shared_dictionary_matches_concatenated_views() {
    let mut many: Vec<String> = [0usize, 1, 11, 12, 13, 300]
        .iter()
        .map(|&len| "x".repeat(len))
        .collect();
    many.extend(
        [
            "",
            "dup",
            "dup",
            "naive cafe \u{e9}\u{1f600}",
            "\u{65e5}\u{672c}\u{8a9e}\u{30c6}\u{30ad}",
        ]
        .map(str::to_owned),
    );
    many.extend((0..500).map(|i| format!("value number {i}")));
    for alphabet in [Vec::new(), vec!["only one long entry here".to_owned()], many] {
        let entries: Vec<&str> = alphabet.iter().map(String::as_str).collect();
        let (arena, bounds) = concatenate(&entries);
        let expected = views_over(&arena, &bounds).unwrap();
        let prepared = prepare_shared_string_view_dictionary(&alphabet).unwrap();
        assert_eq!(
            prepared.arena.as_slice(),
            arena.as_slice(),
            "{} entries",
            alphabet.len()
        );
        assert_eq!(prepared.entry_views, expected, "{} entries", alphabet.len());
    }
}

#[test]
fn prepared_shared_dictionary_outlives_its_alphabet() {
    let alphabet: Vec<String> = ["a value long enough to sit in the arena", "blue", "red", "yellow"]
        .map(str::to_owned)
        .to_vec();
    let values: Vec<Option<String>> = (0..600)
        .map(|i| (i % 7 != 0).then(|| alphabet[i % 4].clone()))
        .collect();
    let block = encode_block_with_shared_dictionary(
        &ColumnData::Strings(values.clone().into()),
        false,
        CascadeStrategy::DecodeOptimized,
        Some(&alphabet),
    );
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::FileScopeDictionary);
    let prepared = prepare_shared_string_view_dictionary(&alphabet).unwrap();
    drop(alphabet);
    for _ in 0..2 {
        let bytes = block.bytes.clone();
        let views = decode_string_block_views_shared_prepared(block.pipeline, &bytes, None, Some(&prepared))
            .unwrap()
            .expect("dictionary blocks decode to views");
        drop(bytes);
        for (row, expected) in values.iter().enumerate() {
            assert_eq!(
                views.is_valid(row).then(|| views.value(row)),
                expected.as_deref(),
                "row {row}"
            );
        }
    }
}

#[test]
fn view_arena_len_is_checked_at_the_u32_limit() {
    let max = u32::MAX as usize;
    assert_eq!(view_arena_len([]).unwrap(), 0);
    assert_eq!(view_arena_len([max - 12, 12]).unwrap(), max);
    for lens in [vec![max - 12, 13], vec![max, 1], vec![usize::MAX, 1]] {
        assert!(matches!(
            view_arena_len(lens),
            Err(FormatError::Structural {
                rule: "string arena larger than a view offset can address"
            })
        ));
    }
}

#[test]
fn a_block_covering_a_corner_of_a_wide_alphabet_keeps_its_local_dictionary() {
    let alphabet: Vec<String> = (0..1024).map(|i| format!("v{i:04}")).collect();

    let corner: Vec<Option<String>> = (0..8192)
        .map(|i| Some(alphabet[if i % 2 == 0 { 0 } else { 1023 }].clone()))
        .collect();
    let block = encode_block_with_shared_dictionary(
        &ColumnData::Strings(corner.clone().into()),
        false,
        CascadeStrategy::DecodeOptimized,
        Some(&alphabet),
    );
    assert_ne!(
        block.pipeline.side_stream().unwrap(),
        SideStream::FileScopeDictionary,
        "wide-alphabet codes for a two-value block must lose the size trial"
    );
    assert_eq!(
        decode_block_shared(block.pipeline, &block.bytes, Some(&alphabet)).unwrap(),
        ColumnData::Strings(corner.into())
    );

    let narrow: Vec<String> = (0..64).map(|i| format!("v{i:04}")).collect();
    let spanning: Vec<Option<String>> = (0..8192).map(|i| Some(narrow[i % 64].clone())).collect();
    let block = encode_block_with_shared_dictionary(
        &ColumnData::Strings(spanning.clone().into()),
        false,
        CascadeStrategy::DecodeOptimized,
        Some(&narrow),
    );
    assert_eq!(
        block.pipeline.side_stream().unwrap(),
        SideStream::FileScopeDictionary,
        "a block spanning the alphabet drops its value stream and shares"
    );
    assert_eq!(
        decode_block_shared(block.pipeline, &block.bytes, Some(&narrow)).unwrap(),
        ColumnData::Strings(spanning.into())
    );
}

#[test]
fn a_block_shorter_than_one_fastlanes_vector_is_not_padded_to_one() {
    for count in [2usize, 3, 8, 64, 512] {
        let values: Vec<u64> = (0..count as u64).map(|i| i * 7 + 3).collect();
        let block = encode_block(&ColumnData::U64(values.clone()), true);
        let plain_bytes = 4 + count * 8;
        assert!(
            block.bytes.len() <= plain_bytes + 16,
            "a {count}-value block took {} bytes, more than the {plain_bytes} its values occupy written plainly: \
             the transform choice is paying for a whole padded vector",
            block.bytes.len()
        );
        assert_eq!(
            decode_block(block.pipeline, &block.bytes).unwrap(),
            ColumnData::U64(values),
            "a {count}-value block must round-trip whatever transform wins"
        );
    }
}

#[test]
fn a_trailing_codec_is_chosen_on_the_bytes_it_actually_stores() {
    let body = vec![7u8; 68];
    let lz4 = vec![0u8; 47];
    let zstd_framed = vec![0u8; 45 + ZSTD_LENGTH_PREFIX_BYTES];
    let (compression, stored) = pick_trailing(body.clone(), 60, lz4.clone(), zstd_framed);
    assert_eq!(
        compression,
        Compression::Lz4,
        "the codec storing fewer bytes must win, prefix included"
    );
    assert_eq!(stored.len(), lz4.len());

    let zstd_framed = vec![0u8; 30 + ZSTD_LENGTH_PREFIX_BYTES];
    let (compression, stored) = pick_trailing(body.clone(), 60, vec![0u8; 47], zstd_framed.clone());
    assert_eq!(compression, Compression::Zstd1);
    assert_eq!(
        stored.len(),
        zstd_framed.len(),
        "the stored form is exactly the already-framed candidate, unchanged"
    );

    let zstd_framed = vec![0u8; 58 + ZSTD_LENGTH_PREFIX_BYTES];
    let (compression, stored) = pick_trailing(body.clone(), 60, vec![0u8; 61], zstd_framed);
    assert_eq!(
        compression,
        Compression::None,
        "a stage that only clears the savings bar by ignoring its own framing must be dropped"
    );
    assert_eq!(stored, body);
}

#[test]
fn compress_zstd_framed_matches_compress_then_frame_and_round_trips() {
    use super::decompressor::{Decompressor, SoftwareDecompressor};

    let corpus: Vec<Vec<u8>> = vec![
        Vec::new(),
        b"tiny".to_vec(),
        vec![7u8; 5_000],
        (0..20_000u32).map(|i| (i % 251) as u8).collect(),
    ];
    for bytes in corpus {
        for level in [1, 3] {
            let framed = compress_zstd_framed(&bytes, level);

            let bare = compress_zstd(&bytes, level);
            let mut expected = Vec::with_capacity(ZSTD_LENGTH_PREFIX_BYTES + bare.len());
            expected.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            expected.extend_from_slice(&bare);
            assert_eq!(framed, expected, "level {level}, {} input bytes", bytes.len());

            let decompressed = SoftwareDecompressor.decompress(Compression::Zstd1, &framed).unwrap();
            assert_eq!(decompressed, bytes, "level {level} round trip");
        }
    }
}

#[test]
fn alp_probe_kernel_matches_the_scalar_reference() {
    let mut corpus: Vec<f64> = vec![
        0.0,
        -0.0,
        0.25,
        -0.25,
        0.5,
        -0.5,
        1.5,
        2.5,
        -2.5,
        0.049999999999999996,
        0.15,
        -0.15,
        f64::from_bits(0x3FDF_FFFF_FFFF_FFFF),
        1.0 / 3.0,
        6.02214076e23,
        -6.62607015e-34,
        123.456,
        -9_876.543_21,
        8.9e18,
        -8.9e18,
        9.1e18,
        4.5e15,
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        5e-324,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        (1u64 << 51) as f64,
        ((1u64 << 51) as f64) + 0.5,
        i64::MAX as f64,
    ];
    let mut state: u64 = 0x0DDB_1A5E_5BAD_5EED;
    for _ in 0..4096 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        corpus.push(f64::from_bits(state));
        corpus.push((state >> 40) as f64 / 100.0);
    }
    let mut zigzags = vec![0u64; corpus.len()];
    let mut encodable = vec![false; corpus.len()];
    for power in ALP_POWERS {
        alp_probe_chunk(&corpus, power, &mut zigzags, &mut encodable);
        for ((value, zig), ok) in corpus.iter().zip(&zigzags).zip(&encodable) {
            match alp_try(*value, power) {
                Some(int) => {
                    assert!(*ok, "kernel rejected {value:e} at power {power:e}, reference accepts");
                    assert_eq!(
                        *zig,
                        zigzag(int),
                        "kernel code diverges for {value:e} at power {power:e}"
                    );
                }
                None => {
                    assert!(!*ok, "kernel accepted {value:e} at power {power:e}, reference rejects");
                    assert_eq!(*zig, 0, "a rejected value must leave the masked zero code");
                }
            }
        }
    }
}

#[test]
fn alp_narrow_conversion_is_exact_across_the_width_bound() {
    let limit = mask(ALP_EXACT_CONVERT_MAX_WIDTH);
    let mut probes: Vec<u64> = vec![0, 1, 2, 3, limit, limit - 1, limit - 2, limit / 2, limit / 2 + 1];
    let mut state: u64 = 0xFEED_FACE_CAFE_BEEF;
    for _ in 0..4096 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        probes.push(state & limit);
    }
    for packed in probes {
        assert_eq!(
            alp_narrow_int_to_f64(packed),
            unzigzag(packed) as f64,
            "conversion diverges for packed {packed:#x} (int {})",
            unzigzag(packed)
        );
    }
}

#[test]
fn alp_blocks_wider_than_the_exact_conversion_bound_still_round_trip() {
    let values: Vec<f64> = (0..1500u64)
        .map(|i| ((i << 30) as i64 - (750u64 << 30) as i64) as f64 * 8_192.0)
        .collect();
    let mut out = Writer::new();
    let side = encode_alp(&values, CascadeStrategy::DecodeOptimized, &mut out).expect("exact multiples encode");
    let bytes = out.into_bytes();
    let mut reader = Reader::new(&bytes);
    let decoded = decode_alp(&mut reader, side).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn alp_rd_split_kernel_matches_a_linear_dictionary_search() {
    let corpus: Vec<f64> = (0..800)
        .map(|i| f64::from_bits(0x4037_0000_0000_0000u64 | (i as u64 * 0x0000_0421_8461_1077)))
        .collect();
    for right_width in [ALP_RD_MIN_RIGHT_WIDTH, 52, ALP_RD_MAX_RIGHT_WIDTH] {
        for dict_len in 0..=ALP_RD_MAX_DICT_LEN {
            let mut dict: Vec<u16> = corpus
                .iter()
                .step_by(90)
                .take(dict_len)
                .map(|value| (value.to_bits() >> right_width) as u16)
                .collect();
            dict.dedup();
            let mut padded = [0u16; ALP_RD_MAX_DICT_LEN];
            for (slot, entry) in padded.iter_mut().zip(&dict) {
                *slot = *entry;
            }
            let mut codes = vec![0u64; corpus.len()];
            let mut rights = vec![0u64; corpus.len()];
            alp_rd_split_chunk(&corpus, right_width, &padded, dict.len(), &mut codes, &mut rights);
            for ((value, code), right) in corpus.iter().zip(&codes).zip(&rights) {
                let bits = value.to_bits();
                assert_eq!(*right, bits & mask(right_width));
                let left = (bits >> right_width) as u16;
                match dict.iter().position(|entry| *entry == left) {
                    Some(expected) => assert_eq!(*code, expected as u64),
                    None => assert_eq!(*code, ALP_RD_CODE_MISS),
                }
            }
        }
    }
}

#[test]
fn alp_rd_choose_split_matches_the_map_based_reference() {
    fn reference(sample: &[f64]) -> (u32, Vec<u16>, usize) {
        let mut best: Option<(u32, Vec<u16>, usize)> = None;
        for right_width in ALP_RD_MIN_RIGHT_WIDTH..=ALP_RD_MAX_RIGHT_WIDTH {
            let mut counts: BTreeMap<u16, usize> = BTreeMap::new();
            for value in sample {
                *counts.entry((value.to_bits() >> right_width) as u16).or_default() += 1;
            }
            let mut ranked: Vec<(u16, usize)> = counts.into_iter().collect();
            ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            ranked.truncate(ALP_RD_MAX_DICT_LEN);
            let covered: usize = ranked.iter().map(|(_, count)| count).sum();
            let dict: Vec<u16> = ranked.into_iter().map(|(left, _)| left).collect();
            let exceptions = sample.len() - covered;
            let code_bits = bits_needed(dict.len().saturating_sub(1) as u64) as usize;
            let estimate = 2
                + 2 * dict.len()
                + 4
                + exceptions * 6
                + (sample.len() * (code_bits + right_width as usize)).div_ceil(8);
            if best.as_ref().is_none_or(|(_, _, current)| estimate < *current) {
                best = Some((right_width, dict, estimate));
            }
        }
        best.unwrap_or((ALP_RD_MAX_RIGHT_WIDTH, Vec::new(), usize::MAX))
    }
    let with_left = |left: u64, i: u64| f64::from_bits((left << 48) | (mix(i) >> 16));
    let boundary_key = |i: u64| {
        let j = i % 27;
        if j < 21 { j / 3 } else { 7 + (j - 21) / 2 }
    };
    let specials = [
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0x7FF0_0000_0000_0001),
        f64::from_bits(0xFFFF_FFFF_FFFF_FFFF),
        f64::from_bits(1),
        f64::from_bits(0x000F_FFFF_FFFF_FFFF),
        f64::from_bits(0x8000_0000_0000_0001),
        f64::MIN_POSITIVE,
        f64::MAX,
    ];
    let shapes: [&dyn Fn(u64) -> f64; 9] = [
        &|_| 1.5,
        &|i| with_left(0x4000 + i % 8, i),
        &|i| with_left(0x4000 + i % 9, i),
        &|i| with_left(0x4000 + boundary_key(i), i),
        &|i| with_left(0xFFFF - boundary_key(i), i),
        &|i| with_left(0x4000 + mix(i ^ 0xABCD) % 12, i),
        &|i| f64::from_bits(mix(i)),
        &|i| {
            if i % 2 == 0 {
                specials[(mix(i) % specials.len() as u64) as usize]
            } else {
                f64::from_bits(0x4000_0000_0000_0000 | (mix(i) >> 12))
            }
        },
        &|i| f64::from_bits(((0x3FF + mix(i) % 4) << 52) | (mix(i + 1) >> 12)),
    ];
    for size in [0u64, 1, 7, 8, 9, 16, 128, 1023, 1024, 4096] {
        for (shape_index, shape) in shapes.iter().enumerate() {
            let sample: Vec<f64> = (0..size).map(shape).collect();
            assert_eq!(
                alp_rd_choose_split(&sample),
                reference(&sample),
                "size {size}, shape {shape_index}"
            );
        }
    }
    assert_eq!(alp_rd_choose_split(&[]), (ALP_RD_MIN_RIGHT_WIDTH, Vec::new(), 6));
}

fn decode_alp_rd_reference(reader: &mut Reader<'_>) -> Result<Vec<f64>, FormatError> {
    let right_width = u32::from(reader.u8("alp-rd right width")?);
    if !(ALP_RD_MIN_RIGHT_WIDTH..=ALP_RD_MAX_RIGHT_WIDTH).contains(&right_width) {
        return Err(FormatError::Structural {
            rule: "alp-rd right width out of range",
        });
    }
    let dict_len = reader.u8("alp-rd dictionary length")? as usize;
    if dict_len > ALP_RD_MAX_DICT_LEN {
        return Err(FormatError::Structural {
            rule: "alp-rd dictionary too long",
        });
    }
    let left_limit = 1u64 << (64 - right_width);
    let mut dict = Vec::with_capacity(dict_len);
    for _ in 0..dict_len {
        let left = reader.u16("alp-rd dictionary entry")?;
        if u64::from(left) >= left_limit {
            return Err(FormatError::Structural {
                rule: "alp-rd left part exceeds its width",
            });
        }
        dict.push(left);
    }
    let exception_count = reader.u32("alp-rd exception count")? as usize;
    if exception_count > MAX_BLOCK_VALUES {
        return Err(FormatError::Structural {
            rule: "alp-rd exception count beyond the block bound",
        });
    }
    let mut exceptions = Vec::new();
    for _ in 0..exception_count {
        let index = reader.u32("alp-rd exception index")?;
        let left = reader.u16("alp-rd exception left part")?;
        if u64::from(left) >= left_limit {
            return Err(FormatError::Structural {
                rule: "alp-rd left part exceeds its width",
            });
        }
        exceptions.push((index, left));
    }
    let codes = bitunpack(reader)?;
    let rights = bitunpack(reader)?;
    if codes.len() != rights.len() {
        return Err(FormatError::Structural {
            rule: "alp-rd code and right-part streams disagree",
        });
    }
    let mut values = Vec::with_capacity(codes.len());
    for (code, right) in codes.iter().zip(rights.iter()) {
        if *right >> right_width != 0 {
            return Err(FormatError::Structural {
                rule: "alp-rd right part exceeds its width",
            });
        }
        let left = dict
            .get(*code as usize)
            .copied()
            .ok_or(FormatError::RefOutOfRange { what: "alp-rd code" })?;
        values.push(f64::from_bits((u64::from(left) << right_width) | *right));
    }
    for (index, left) in exceptions {
        let right = rights.get(index as usize).ok_or(FormatError::RefOutOfRange {
            what: "alp-rd exception index",
        })?;
        let slot = values.get_mut(index as usize).ok_or(FormatError::RefOutOfRange {
            what: "alp-rd exception index",
        })?;
        *slot = f64::from_bits((u64::from(left) << right_width) | *right);
    }
    Ok(values)
}

fn alp_rd_body(right_width: u8, dict: &[u16], exceptions: &[(u32, u16)], codes: &[u64], rights: &[u64]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u8(right_width);
    out.put_u8(dict.len() as u8);
    for left in dict {
        out.put_u16(*left);
    }
    out.put_u32(exceptions.len() as u32);
    for (index, left) in exceptions {
        out.put_u32(*index);
        out.put_u16(*left);
    }
    bitpack(codes, bits_needed(codes.iter().copied().max().unwrap_or(0)), &mut out);
    bitpack(rights, bits_needed(rights.iter().copied().max().unwrap_or(0)), &mut out);
    out.into_bytes()
}

fn assert_alp_rd_decode_matches_reference(body: &[u8]) {
    let mut reader = Reader::new(body);
    let mut reference_reader = Reader::new(body);
    let decoded = decode_alp_rd(&mut reader);
    let expected = decode_alp_rd_reference(&mut reference_reader);
    match (decoded, expected) {
        (Ok(decoded), Ok(expected)) => {
            let decoded: Vec<u64> = decoded.iter().map(|value| value.to_bits()).collect();
            let expected: Vec<u64> = expected.iter().map(|value| value.to_bits()).collect();
            assert_eq!(decoded, expected);
            assert_eq!(reader.remaining(), reference_reader.remaining());
        }
        (decoded, expected) => assert_eq!(decoded.err(), expected.err()),
    }
}

#[test]
fn alp_rd_in_place_decode_matches_the_reference_bit_for_bit() {
    let specials = [
        0.0f64.to_bits(),
        (-0.0f64).to_bits(),
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        0x7FF8_0000_0000_0001,
        0xFFF4_0000_DEAD_BEEF,
        1,
        0x800F_FFFF_FFFF_FFFF,
    ];
    for len in [0usize, 1, 7, 1023, 1024, 1025, 2049] {
        for spread in [1u64, 3, 9, 64] {
            let values: Vec<f64> = (0..len as u64)
                .map(|i| match i % 11 {
                    0 => f64::from_bits(specials[(i / 11) as usize % specials.len()]),
                    _ => f64::from_bits(0x4030_0000_0000_0000 ^ ((mix(i) % spread) << 52) ^ (mix(i + 7) >> 12)),
                })
                .collect();
            let mut out = Writer::new();
            encode_alp_rd(&values, &mut out);
            let body = out.into_bytes();
            assert_alp_rd_decode_matches_reference(&body);
            let decoded = decode_alp_rd(&mut Reader::new(&body)).unwrap();
            let decoded: Vec<u64> = decoded.iter().map(|value| value.to_bits()).collect();
            let original: Vec<u64> = values.iter().map(|value| value.to_bits()).collect();
            assert_eq!(decoded, original, "len {len}, spread {spread}");
        }
    }
    for right_width in [ALP_RD_MIN_RIGHT_WIDTH, 52, ALP_RD_MAX_RIGHT_WIDTH] {
        let left_mask = mask(64 - right_width);
        for dict_len in 0..=ALP_RD_MAX_DICT_LEN {
            let dict: Vec<u16> = (0..dict_len as u64).map(|i| (mix(i) & left_mask) as u16).collect();
            let len = 1500usize;
            let codes: Vec<u64> = (0..len as u64).map(|i| mix(i) % dict_len.max(1) as u64).collect();
            let rights: Vec<u64> = (0..len as u64).map(|i| mix(i + 99) & mask(right_width)).collect();
            let exceptions: Vec<(u32, u16)> = (0..len as u32)
                .step_by(if dict_len == 0 { 1 } else { 37 })
                .chain([5, 5, 1499])
                .map(|index| (index, (mix(u64::from(index) + 3) & left_mask) as u16))
                .collect();
            let body = alp_rd_body(right_width as u8, &dict, &exceptions, &codes, &rights);
            assert_alp_rd_decode_matches_reference(&body);
        }
    }
}

#[test]
fn alp_rd_in_place_decode_rejects_corrupt_bodies_like_the_reference() {
    let right_width = 52u8;
    let dict = [0x403u16, 0x404];
    let codes = vec![0u64, 1, 0, 1];
    let rights = vec![1u64, 2, 3, 4];
    let wide_right = 1u64 << right_width;
    let corrupt = [
        alp_rd_body(right_width, &dict, &[], &codes, &rights[..3]),
        alp_rd_body(right_width, &dict, &[], &[0, 2, 0, 1], &rights),
        alp_rd_body(right_width, &dict, &[(1, 0x405)], &[0, 2, 0, 1], &rights),
        alp_rd_body(right_width, &dict, &[], &[0, 9, 0, 1], &[1, wide_right, 3, 4]),
        alp_rd_body(right_width, &[0x1000], &[], &[0, 0, 0, 0], &rights),
        alp_rd_body(right_width, &dict, &[(0, 0x1000)], &codes, &rights),
        alp_rd_body(right_width, &dict, &[(4, 0x405)], &codes, &rights),
        alp_rd_body(ALP_RD_MIN_RIGHT_WIDTH as u8 - 1, &dict, &[], &codes, &rights),
        alp_rd_body(right_width, &[0x403; ALP_RD_MAX_DICT_LEN + 1], &[], &codes, &rights),
    ];
    for body in &corrupt {
        assert!(decode_alp_rd(&mut Reader::new(body)).is_err());
        assert_alp_rd_decode_matches_reference(body);
    }
    let mut out = Writer::new();
    out.put_u8(right_width);
    out.put_u8(0);
    out.put_u32(MAX_BLOCK_VALUES as u32 + 1);
    assert_alp_rd_decode_matches_reference(&out.into_bytes());
    let mut body = alp_rd_body(right_width, &dict, &[(2, 0x405)], &codes, &rights);
    for end in 0..body.len() {
        assert_alp_rd_decode_matches_reference(&body[..end]);
    }
    body.extend_from_slice(&[0xAB; 5]);
    assert_alp_rd_decode_matches_reference(&body);
}

#[test]
fn alp_rd_round_trips_randomized_columns() {
    let specials = [
        f64::NAN.to_bits(),
        0xFFF8_0000_DEAD_BEEF,
        (-0.0f64).to_bits(),
        0.0f64.to_bits(),
        1,
        0x800F_FFFF_FFFF_FFFF,
    ];
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    for _ in 0..40 {
        let len = (pseudo_random(&mut state) % 5000) as usize;
        let spread = 1 + pseudo_random(&mut state) % 12;
        let values: Vec<f64> = (0..len)
            .map(|i| {
                let noise = pseudo_random(&mut state);
                if i % 97 == 3 {
                    f64::from_bits(specials[(noise % specials.len() as u64) as usize])
                } else {
                    f64::from_bits(0x3FF0_0000_0000_0000 ^ ((noise % spread) << 52) ^ (noise >> 12))
                }
            })
            .collect();
        let mut out = Writer::new();
        encode_alp_rd(&values, &mut out);
        let body = out.into_bytes();
        assert_alp_rd_decode_matches_reference(&body);
        let decoded = decode_alp_rd(&mut Reader::new(&body)).unwrap();
        assert_eq!(float_bits(&decoded), float_bits(&values), "len {len}, spread {spread}");
    }
}

#[test]
fn alp_rd_decode_rejects_a_forged_out_of_range_code() {
    let right_width = 52u8;
    let dict = [0x403u16, 0x404, 0x3FF];
    let len = 3000usize;
    let rights: Vec<u64> = (0..len as u64).map(|i| mix(i) & mask(u32::from(right_width))).collect();
    let forged_codes = [3u64, 7, ALP_RD_MAX_DICT_LEN as u64, ALP_RD_MAX_DICT_LEN as u64 + 1, 255];
    for bad_code in forged_codes {
        for bad_row in [0usize, 1023, 1024, 2999] {
            let mut codes: Vec<u64> = (0..len as u64).map(|i| i % 3).collect();
            codes[bad_row] = bad_code;
            let body = alp_rd_body(right_width, &dict, &[], &codes, &rights);
            assert_eq!(
                decode_alp_rd(&mut Reader::new(&body)),
                Err(FormatError::RefOutOfRange { what: "alp-rd code" }),
                "code {bad_code} at row {bad_row}"
            );
            assert_alp_rd_decode_matches_reference(&body);
            let mut wide_rights = rights.clone();
            wide_rights[len - 1] = 1u64 << right_width;
            let body = alp_rd_body(right_width, &dict, &[], &codes, &wide_rights);
            assert_alp_rd_decode_matches_reference(&body);
        }
    }
}

fn encode_alp_rd_reference(values: &[f64], out: &mut Writer) {
    let sample = transform_sample(values);
    let sample: &[f64] = sample.as_deref().unwrap_or(values);
    let (right_width, dict, _) = alp_rd_choose_split(sample);
    let mut padded_dict = [0u16; ALP_RD_MAX_DICT_LEN];
    for (slot, entry) in padded_dict.iter_mut().zip(&dict) {
        *slot = *entry;
    }
    let mut codes = vec![0u64; values.len()];
    let mut rights = vec![0u64; values.len()];
    alp_rd_split_chunk(values, right_width, &padded_dict, dict.len(), &mut codes, &mut rights);
    let mut exceptions: Vec<(u32, u16)> = Vec::new();
    for (index, (code, value)) in codes.iter_mut().zip(values).enumerate() {
        if *code == ALP_RD_CODE_MISS {
            *code = 0;
            exceptions.push((index as u32, (value.to_bits() >> right_width) as u16));
        }
    }
    out.put_u8(right_width as u8);
    out.put_u8(dict.len() as u8);
    for left in &dict {
        out.put_u16(*left);
    }
    out.put_u32(exceptions.len() as u32);
    for (index, left) in &exceptions {
        out.put_u32(*index);
        out.put_u16(*left);
    }
    let max_code = codes.iter().copied().max().unwrap_or(0);
    bitpack(&codes, bits_needed(max_code), out);
    let max_right = rights.iter().copied().max().unwrap_or(0);
    bitpack(&rights, bits_needed(max_right), out);
}

#[test]
fn alp_rd_encode_matches_the_full_buffer_reference_byte_for_byte() {
    let specials = [
        0.0f64,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0xFFF4_0000_DEAD_BEEF),
        f64::from_bits(1),
        f64::from_bits(0x800F_FFFF_FFFF_FFFF),
    ];
    let generators: [fn(u64) -> f64; 5] = [
        |i| f64::from_bits(0x43F0_0000_0000_0000 | (mix(i) >> 12)),
        |i| f64::from_bits(0x4030_0000_0000_0000 ^ ((mix(i) % 12) << 52) ^ (mix(i + 7) >> 12)),
        |i| f64::from_bits(mix(i)),
        |i| f64::from_bits((mix(i) % 3) << 52),
        |i| f64::from_bits(0x4010_0000_0000_0000 | ((mix(i) % 2) << 63) | 0x000F_FFFF_FFFF_FFFF),
    ];
    for len in [0usize, 1, 2, 1023, 1024, 1025, 2048, 3001] {
        for generate in generators {
            let mut values: Vec<f64> = (0..len as u64).map(generate).collect();
            for (slot, special) in values.iter_mut().step_by(97).zip(specials.iter().cycle()) {
                *slot = *special;
            }
            let mut out = Writer::new();
            encode_alp_rd(&values, &mut out);
            let mut reference = Writer::new();
            encode_alp_rd_reference(&values, &mut reference);
            assert_eq!(out.bytes(), reference.bytes(), "len {len}");
            let decoded = decode_alp_rd(&mut Reader::new(out.bytes())).unwrap();
            let decoded: Vec<u64> = decoded.iter().map(|value| value.to_bits()).collect();
            let original: Vec<u64> = values.iter().map(|value| value.to_bits()).collect();
            assert_eq!(decoded, original, "len {len}");
        }
    }
}

fn alp_encode_columns() -> Vec<(String, Vec<f64>)> {
    let specials = [
        -0.0f64,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7FF8_0000_0000_0001),
        f64::from_bits(0xFFF4_0000_DEAD_BEEF),
        f64::from_bits(1),
        1.0e300,
    ];
    let noise = |i: usize| f64::from_bits(0x3FF0_0000_0000_0000 | (mix(i as u64) & 0x000F_FFFF_FFFF_FFFF));
    let mut columns: Vec<(String, Vec<f64>)> = vec![
        ("empty".to_owned(), Vec::new()),
        ("singleton".to_owned(), vec![0.25]),
        ("singleton nan".to_owned(), vec![f64::NAN]),
    ];
    for len in [1usize, 2, 1023, 1024, 1025, 2048, 3001, 5000] {
        columns.push((format!("quarters {len}"), (0..len).map(|i| (i as f64) * 0.25).collect()));
        columns.push((format!("zeros {len}"), vec![0.0; len]));
        columns.push((
            format!("wide {len}"),
            (0..len).map(|i| (mix(i as u64) >> 12) as f64 - 2.0e15).collect(),
        ));
        let mut sparse: Vec<f64> = (0..len).map(|i| -(i as f64) / 100.0).collect();
        for (slot, special) in sparse.iter_mut().step_by(97).zip(specials.iter().cycle()) {
            *slot = *special;
        }
        columns.push((format!("sparse specials {len}"), sparse));
        columns.push((
            format!("dense {len}"),
            (0..len)
                .map(|i| if i % 10 == 3 { noise(i) } else { (i as f64) * 0.5 })
                .collect(),
        ));
    }
    columns.push((
        "dense long".to_owned(),
        (0..65_536)
            .map(|i| if i % 10 == 3 { noise(i) } else { (i as f64) * 0.5 })
            .collect(),
    ));
    let per_vector_noise = [128usize, 129, 0, 1024, 1024];
    let mut mixed: Vec<f64> = Vec::new();
    for (vector_index, noisy) in per_vector_noise.iter().enumerate() {
        let len = if vector_index == 4 { 500 } else { FASTLANES_VECTOR };
        for offset in 0..len {
            let i = vector_index * FASTLANES_VECTOR + offset;
            mixed.push(if offset < *noisy { noise(i) } else { (i as f64) * 0.125 });
        }
    }
    columns.push(("mixed escapes".to_owned(), mixed.clone()));
    let escaped_specials: Vec<f64> = mixed
        .iter()
        .enumerate()
        .map(|(i, value)| {
            if value.to_bits() & 0x3FF0_0000_0000_0000 == 0x3FF0_0000_0000_0000 && i % 2 == 0 {
                specials.get(i % specials.len()).copied().unwrap_or(0.0)
            } else {
                *value
            }
        })
        .collect();
    columns.push(("mixed escaped specials".to_owned(), escaped_specials));
    columns.push(("one noisy vector".to_owned(), one_noisy_vector()));
    columns
}

fn decode_alp_exceptions_reference(reader: &mut Reader<'_>, side: SideStream) -> Result<Vec<(u32, u64)>, FormatError> {
    let exception_count = reader.u32("alp exception count")? as usize;
    let mut exceptions = Vec::with_capacity(reader.capacity_hint(exception_count, 12));
    match side {
        SideStream::FileScopeDictionary | SideStream::FsstDictionaryValues | SideStream::FsstValueKeys => {
            return Err(FormatError::Structural {
                rule: "alp blocks never carry a string block's side stream",
            });
        }
        SideStream::None => {
            for _ in 0..exception_count {
                let index = reader.u32("alp exception index")?;
                let bits = reader.u64("alp exception bits")?;
                exceptions.push((index, bits));
            }
        }
        SideStream::ForBitpack => {
            let indexes: Vec<u64> = decode_for_bitpack(reader)?;
            if indexes.len() != exception_count {
                return Err(FormatError::Structural {
                    rule: "alp exception index stream disagrees with the exception count",
                });
            }
            for index in indexes {
                let bits = reader.u64("alp exception bits")?;
                let index = u32::try_from(index).map_err(|_| FormatError::RefOutOfRange {
                    what: "alp exception index",
                })?;
                exceptions.push((index, bits));
            }
        }
    }
    Ok(exceptions)
}

fn decode_alp_reference(reader: &mut Reader<'_>, side: SideStream) -> Result<Vec<f64>, FormatError> {
    let mut exponent_index = reader.u8("alp exponent")? as usize;
    let mut escaped = Vec::new();
    if exponent_index == usize::from(ALP_VECTOR_ESCAPE_SENTINEL) {
        exponent_index = reader.u8("alp exponent")? as usize;
        escaped = read_alp_escaped_vectors(reader)?;
    }
    let power = ALP_POWERS.get(exponent_index).copied().ok_or(FormatError::Structural {
        rule: "alp exponent out of range",
    })?;
    let exceptions = decode_alp_exceptions_reference(reader, side)?;
    let stream = read_packed_stream(reader)?;
    let mut values = vec![0f64; stream.padded_count()];
    if stream.width <= ALP_EXACT_CONVERT_MAX_WIDTH {
        stream.unpack_into(&mut values, |packed| alp_narrow_int_to_f64(packed) / power);
    } else {
        stream.unpack_into(&mut values, |packed| (unzigzag(packed) as f64) / power);
    }
    values.truncate(stream.count);
    for (index, bits) in exceptions {
        let slot = values.get_mut(index as usize).ok_or(FormatError::RefOutOfRange {
            what: "alp exception index",
        })?;
        *slot = f64::from_bits(bits);
    }
    apply_alp_escapes(reader, &escaped, &mut values)?;
    Ok(values)
}

fn decode_alp_range_reference(
    reader: &mut Reader<'_>,
    side: SideStream,
    start: usize,
    end: usize,
) -> Result<Vec<f64>, FormatError> {
    let mut exponent_index = reader.u8("alp exponent")? as usize;
    let mut escaped = Vec::new();
    if exponent_index == usize::from(ALP_VECTOR_ESCAPE_SENTINEL) {
        exponent_index = reader.u8("alp exponent")? as usize;
        escaped = read_alp_escaped_vectors(reader)?;
    }
    let power = ALP_POWERS.get(exponent_index).copied().ok_or(FormatError::Structural {
        rule: "alp exponent out of range",
    })?;
    let exceptions: Vec<(usize, u64)> = decode_alp_exceptions_reference(reader, side)?
        .into_iter()
        .filter_map(|(index, bits)| {
            let index = index as usize;
            (index >= start && index < end).then(|| (index - start, bits))
        })
        .collect();
    let count = reader.u32("bitpack count")? as usize;
    let width = u32::from(reader.u8("bitpack width")?);
    if width > 64 {
        return Err(FormatError::Structural {
            rule: "bit width beyond 64",
        });
    }
    let effective_end = end.min(count);
    let effective_len = effective_end.saturating_sub(start);
    if effective_len == 0 {
        return Ok(Vec::new());
    }
    let mut consumed_words = 0usize;
    let unscale = |packed: u64| (unzigzag(packed) as f64) / power;
    let mut values: Vec<f64> = if width == 0 {
        if count > MAX_BLOCK_VALUES {
            return Err(FormatError::Structural {
                rule: "zero-width bitpack count beyond the block bound",
            });
        }
        vec![unscale(0); effective_len]
    } else {
        let bits_per_val = width as usize;
        let words_per_lane = fastlanes_words_per_lane(bits_per_val);
        let vector_start = start / FASTLANES_VECTOR;
        let vector_end = effective_end.div_ceil(FASTLANES_VECTOR);
        let words_per_vector = FASTLANES_LANES * words_per_lane;
        let skip_words = vector_start * words_per_vector;
        reader.take(skip_words.saturating_mul(8), "bitpack skip word")?;
        let needed_words = (vector_end - vector_start) * words_per_vector;
        let packed_bytes = reader.take(needed_words.saturating_mul(8), "bitpack word")?;
        consumed_words = skip_words + needed_words;
        with_unpack_buffer(|buffer| {
            unpack_fastlanes_range_into(packed_bytes, width, start, effective_end, buffer)
                .iter()
                .map(|packed| unscale(*packed))
                .collect()
        })
    };
    for (rel_idx, bits) in exceptions {
        if let Some(slot) = values.get_mut(rel_idx) {
            *slot = f64::from_bits(bits);
        }
    }
    if !escaped.is_empty() {
        let total_words = if width == 0 {
            0
        } else {
            count.div_ceil(FASTLANES_VECTOR) * FASTLANES_LANES * fastlanes_words_per_lane(width as usize)
        };
        reader.take(
            total_words.saturating_sub(consumed_words).saturating_mul(8),
            "bitpack skip word",
        )?;
        for vector_index in &escaped {
            let vector_start = *vector_index as usize * FASTLANES_VECTOR;
            if vector_start >= count {
                return Err(FormatError::RefOutOfRange {
                    what: "alp escaped vector index",
                });
            }
            let vector_end = (vector_start + FASTLANES_VECTOR).min(count);
            let raw = reader.take((vector_end - vector_start) * 8, "alp escaped value")?;
            for row in vector_start.max(start)..vector_end.min(effective_end) {
                let word = raw
                    .get((row - vector_start) * 8..(row - vector_start) * 8 + 8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                if let Some(slot) = values.get_mut(row - start) {
                    *slot = f64::from_bits(word);
                }
            }
        }
    }
    Ok(values)
}

fn handmade_alp_block(
    side: SideStream,
    exceptions: &[(u64, u64)],
    escaped: &[u32],
    codes: &[u64],
    raw: &[u64],
) -> Vec<u8> {
    let mut out = Writer::new();
    if escaped.is_empty() {
        out.put_u8(2);
    } else {
        out.put_u8(ALP_VECTOR_ESCAPE_SENTINEL);
        out.put_u8(2);
        out.put_u32(escaped.len() as u32);
        for vector_index in escaped {
            out.put_u32(*vector_index);
        }
    }
    out.put_u32(exceptions.len() as u32);
    if side == SideStream::ForBitpack {
        let indexes: Vec<u64> = exceptions.iter().map(|(index, _)| *index).collect();
        encode_for_bitpack(&indexes, &mut out);
        for (_, bits) in exceptions {
            out.put_u64(*bits);
        }
    } else {
        for (index, bits) in exceptions {
            out.put_u32(*index as u32);
            out.put_u64(*bits);
        }
    }
    bitpack(codes, bits_needed(codes.iter().copied().max().unwrap_or(0)), &mut out);
    for word in raw {
        out.put_u64(*word);
    }
    out.into_bytes()
}

fn alp_parity_ranges() -> [(usize, usize); 14] {
    [
        (0, 0),
        (5, 3),
        (0, 1),
        (0, 10),
        (3, 1500),
        (1000, 1100),
        (1023, 1025),
        (1024, 2048),
        (2000, 6000),
        (4095, 4096),
        (4500, 4600),
        (5000, 9000),
        (10_000, 20_000),
        (0, usize::MAX),
    ]
}

fn assert_alp_decoders_match_reference(body: &[u8], side: SideStream, ranges: &[(usize, usize)], label: &str) {
    let bits = |decoded: Result<Vec<f64>, FormatError>| {
        decoded.map(|values| values.iter().map(|value| value.to_bits()).collect::<Vec<u64>>())
    };
    assert_eq!(
        bits(decode_alp(&mut Reader::new(body), side)),
        bits(decode_alp_reference(&mut Reader::new(body), side)),
        "{label}: full decode"
    );
    for (start, end) in ranges {
        assert_eq!(
            bits(decode_alp_range(&mut Reader::new(body), side, *start, *end)),
            bits(decode_alp_range_reference(&mut Reader::new(body), side, *start, *end)),
            "{label}: range [{start}, {end})"
        );
    }
}

#[test]
fn alp_decode_with_borrowed_exceptions_matches_the_record_reference() {
    let mut sides_seen = Vec::new();
    for (name, values) in alp_encode_columns() {
        for strategy in [CascadeStrategy::DecodeOptimized, CascadeStrategy::SizeOptimized] {
            let mut out = Writer::new();
            let Some(side) = encode_alp(&values, strategy, &mut out) else {
                continue;
            };
            sides_seen.push(side);
            assert_alp_decoders_match_reference(out.bytes(), side, &alp_parity_ranges(), &name);
        }
    }
    assert!(sides_seen.contains(&SideStream::None));
    assert!(sides_seen.contains(&SideStream::ForBitpack));
}

#[test]
fn alp_decode_with_borrowed_exceptions_fails_on_truncation_like_the_record_reference() {
    let values: Vec<f64> = (0..1200)
        .map(|i| {
            if i >= 1024 {
                f64::from_bits(0x3FF0_0000_0000_0000 | (mix(i as u64) & 0x000F_FFFF_FFFF_FFFF))
            } else if i % 101 == 7 {
                f64::from_bits(0x7FF8_0000_0000_0000 | i as u64)
            } else {
                (i as f64) * 0.25
            }
        })
        .collect();
    let ranges = [(0, 1200), (0, 5), (1000, 1100), (1100, 1200), (3000, 4000)];
    let mut sides_seen = Vec::new();
    for strategy in [CascadeStrategy::DecodeOptimized, CascadeStrategy::SizeOptimized] {
        let mut out = Writer::new();
        let side = encode_alp(&values, strategy, &mut out).expect("a mostly regular block encodes as ALP");
        sides_seen.push(side);
        let body = out.into_bytes();
        for cut in 0..body.len() {
            let truncated = body.get(..cut).unwrap_or(&[]);
            assert_alp_decoders_match_reference(truncated, side, &ranges, &format!("{side:?} cut at {cut}"));
        }
    }
    let exceptions: Vec<(u64, u64)> = (0..40).map(|i| (i * 7 + 3, mix(i))).collect();
    let codes: Vec<u64> = (0..600).map(|i| zigzag(i - 300)).collect();
    for side in [SideStream::None, SideStream::ForBitpack] {
        sides_seen.push(side);
        let body = handmade_alp_block(side, &exceptions, &[], &codes, &[]);
        for cut in 0..body.len() {
            let truncated = body.get(..cut).unwrap_or(&[]);
            assert_alp_decoders_match_reference(truncated, side, &ranges, &format!("handmade {side:?} cut at {cut}"));
        }
    }
    assert!(sides_seen.contains(&SideStream::None));
    assert!(sides_seen.contains(&SideStream::ForBitpack));
}

#[test]
fn alp_decode_with_borrowed_exceptions_handles_malformed_lists_like_the_record_reference() {
    let codes: Vec<u64> = (0..1500).map(|i| zigzag(i % 97 - 48)).collect();
    let nan = 0x7FF8_0000_0000_00AB;
    let neg_zero = (-0.0f64).to_bits();
    let lists: [(&str, Vec<(u64, u64)>); 8] = [
        ("none", vec![]),
        ("one", vec![(4, nan)]),
        (
            "duplicates and unsorted",
            vec![(900, nan), (5, neg_zero), (900, f64::INFINITY.to_bits()), (2, 1)],
        ),
        ("dense", (0..1500).step_by(3).map(|i| (i, mix(i))).collect()),
        ("past the output", vec![(3, nan), (1500, nan)]),
        ("far past the output", vec![(u64::from(u32::MAX), nan)]),
        ("inside an escaped vector", vec![(1030, nan), (10, neg_zero)]),
        ("past u32", vec![(7, nan), (1 << 33, nan)]),
    ];
    let raw: Vec<u64> = (0..476).map(|i| f64::NEG_INFINITY.to_bits() ^ i).collect();
    for (name, exceptions) in &lists {
        for side in [SideStream::None, SideStream::ForBitpack] {
            for (escaped, raw) in [(&[][..], &[][..]), (&[1u32][..], &raw[..]), (&[1u32, 9][..], &raw[..])] {
                let body = handmade_alp_block(side, exceptions, escaped, &codes, raw);
                let label = format!("{name} {side:?} escaped {escaped:?}");
                assert_alp_decoders_match_reference(&body, side, &alp_parity_ranges(), &label);
                for other in [
                    SideStream::None,
                    SideStream::ForBitpack,
                    SideStream::FileScopeDictionary,
                    SideStream::FsstDictionaryValues,
                    SideStream::FsstValueKeys,
                ] {
                    assert_alp_decoders_match_reference(
                        &body,
                        other,
                        &alp_parity_ranges(),
                        &format!("{label} as {other:?}"),
                    );
                }
            }
        }
    }

    let body = handmade_alp_block(
        SideStream::None,
        &[(5, nan), (5, neg_zero), (1030, nan)],
        &[1],
        &codes,
        &raw,
    );
    let decoded = decode_alp(&mut Reader::new(&body), SideStream::None).unwrap();
    assert_eq!(decoded.get(5).map(|value| value.to_bits()), Some(neg_zero));
    assert_eq!(decoded.get(1030).map(|value| value.to_bits()), raw.get(6).copied());

    let mut forged = Writer::new();
    forged.put_u8(2);
    forged.put_u32(1);
    encode_for_bitpack(&[1 << 33], &mut forged);
    let forged = forged.into_bytes();
    assert_alp_decoders_match_reference(
        &forged,
        SideStream::ForBitpack,
        &alp_parity_ranges(),
        "truncated bits past u32",
    );
    assert_eq!(
        decode_alp(&mut Reader::new(&forged), SideStream::ForBitpack),
        Err(FormatError::Truncated {
            what: "alp exception bits"
        })
    );

    let mut mismatch = Writer::new();
    mismatch.put_u8(2);
    mismatch.put_u32(2);
    encode_for_bitpack(&[4], &mut mismatch);
    assert_alp_decoders_match_reference(
        mismatch.bytes(),
        SideStream::ForBitpack,
        &alp_parity_ranges(),
        "count mismatch",
    );

    let mut overflow = Writer::new();
    overflow.put_u8(2);
    overflow.put_u32(2);
    overflow.put_u64(u64::MAX - 1);
    bitpack(&[0, 5], 3, &mut overflow);
    overflow.put_u64(nan);
    overflow.put_u64(nan);
    bitpack(&codes, 7, &mut overflow);
    assert_alp_decoders_match_reference(
        overflow.bytes(),
        SideStream::ForBitpack,
        &alp_parity_ranges(),
        "base overflow",
    );

    for (width, count) in [(65u8, 4u32), (0, u32::MAX), (0, 1 << 25), (0, 3)] {
        for side in [SideStream::None, SideStream::ForBitpack] {
            let mut forged = Writer::new();
            forged.put_u8(2);
            forged.put_u32(2);
            if side == SideStream::ForBitpack {
                encode_for_bitpack(&[1, 9_000_000], &mut forged);
                forged.put_u64(nan);
                forged.put_u64(nan);
            } else {
                for index in [1u32, 9_000_000] {
                    forged.put_u32(index);
                    forged.put_u64(nan);
                }
            }
            forged.put_u32(count);
            forged.put_u8(width);
            assert_alp_decoders_match_reference(
                forged.bytes(),
                side,
                &alp_parity_ranges(),
                &format!("forged header width {width} count {count} {side:?}"),
            );
        }
    }

    for body in [
        &[][..],
        &[2u8][..],
        &[ALP_VECTOR_ESCAPE_SENTINEL][..],
        &[30u8, 0, 0, 0, 0][..],
    ] {
        for side in [SideStream::None, SideStream::FsstValueKeys] {
            assert_alp_decoders_match_reference(body, side, &alp_parity_ranges(), &format!("short body {body:?}"));
        }
    }
}

#[test]
fn presence_rank_and_present_position_agree_with_a_prefix_count() {
    let rows: usize = 200;
    let mut bitmap = vec![0u8; rows.div_ceil(8)];
    for row in (0..rows).step_by(3) {
        bitmap[row / 8] |= 1 << (row % 8);
    }
    let rank = PresenceRank::new(&bitmap);
    for row in 0..rows {
        let expected = (row % 3 == 0).then(|| count_present_before(&bitmap, row));
        assert_eq!(rank.position(&bitmap, row), expected, "row {row}");
        assert_eq!(present_position(&bitmap, row), expected, "row {row}");
    }
    assert_eq!(rank.position(&bitmap, rows), None, "row past the bitmap");
    assert_eq!(present_position(&bitmap, rows), None, "row past the bitmap");
}

fn memory_utilization_series(count: usize) -> Vec<f64> {
    let total = 16_000_000_000u64;
    let mut used = 6_000_000_000u64;
    (0..count as u64)
        .map(|i| {
            let draw = mix(i);
            if draw % 10 >= 5 {
                used = (used + (draw >> 8) % 513 * 4096).saturating_sub(256 * 4096);
            }
            used as f64 / total as f64
        })
        .collect()
}

fn best_existing_float_block(values: &[f64]) -> usize {
    let data = ColumnData::F64(values.to_vec());
    let strategy = CascadeStrategy::DecodeOptimized;
    [
        Transform::Alp,
        Transform::AlpRd,
        Transform::ByteStreamSplit,
        Transform::PlainF64,
    ]
    .into_iter()
    .map(|transform| {
        encode_block_forced_for_conformance(&data, true, strategy, transform)
            .unwrap()
            .bytes
            .len()
    })
    .chain([encode_block_with_strategy(&data, true, strategy).bytes.len()])
    .min()
    .unwrap()
}

#[test]
fn xor_float_blocks_round_trip_bit_for_bit_through_the_pipeline() {
    let mut values = vec![
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7FF8_0000_DEAD_BEEF),
        f64::from_bits(0xFFF0_0000_0000_0001),
        f64::MIN_POSITIVE,
        f64::MAX,
    ];
    values.extend(memory_utilization_series(3000));
    let block = encode_block_forced_for_conformance(
        &ColumnData::F64(values.clone()),
        true,
        CascadeStrategy::DecodeOptimized,
        Transform::Chimp,
    )
    .unwrap();
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Chimp);
    let bits = |data: ColumnData| match data {
        ColumnData::F64(values) => values.iter().map(|value| value.to_bits()).collect::<Vec<u64>>(),
        other => panic!("expected floats, got {other:?}"),
    };
    let expected: Vec<u64> = values.iter().map(|value| value.to_bits()).collect();
    assert_eq!(bits(decode_block(block.pipeline, &block.bytes).unwrap()), expected);
    assert_eq!(
        bits(decode_block_range(block.pipeline, &block.bytes, 3, 900).unwrap()),
        expected[3..900]
    );
    assert!(PipelineId(17).transform().is_err());
}

#[test]
fn xor_float_range_reads_equal_the_full_decode_sliced_with_and_without_trailing_compression() {
    let periodic: Vec<f64> = (0..4000).map(|i| [1.0, 2.5, 2.5, -7.0][i % 4]).collect();
    for (values, random_access) in [(memory_utilization_series(3000), true), (periodic, false)] {
        let block = encode_block_forced_for_conformance(
            &ColumnData::F64(values.clone()),
            random_access,
            CascadeStrategy::DecodeOptimized,
            Transform::Chimp,
        )
        .unwrap();
        assert_eq!(block.pipeline.transform().unwrap(), Transform::Chimp);
        assert_eq!(
            block.pipeline.compression().unwrap() == Compression::None,
            random_access
        );
        let full = decode_block(block.pipeline, &block.bytes).unwrap();
        let count = values.len();
        for (start, end) in [
            (0, 1),
            (count / 2, count / 2 + 1),
            (count - 1, count),
            (10, 250),
            (0, count),
            (count - 3, count + 50),
            (count, count + 1),
            (count + 10, count + 20),
            (5, 5),
            (9, 4),
        ] {
            assert_eq!(
                decode_block_range(block.pipeline, &block.bytes, start, end).unwrap(),
                slice_column_data(full.clone(), start, end),
                "rows {start}..{end}"
            );
        }
    }
}

#[test]
fn xor_floats_beat_every_existing_float_encoding_on_a_slowly_varying_metric() {
    let values = memory_utilization_series(8192);
    let data = ColumnData::F64(values.clone());
    let best_existing = best_existing_float_block(&values);
    let block = encode_block_with_xor_floats(&data, true, CascadeStrategy::DecodeOptimized);
    let adaptive = encode_block_with_strategy(&data, true, CascadeStrategy::DecodeOptimized);
    eprintln!(
        "memory utilization, 8192 values: chimp {:.3} bytes/value, best existing {:.3} bytes/value, adaptive \
         selection {:.3} bytes/value",
        block.bytes.len() as f64 / values.len() as f64,
        best_existing as f64 / values.len() as f64,
        adaptive.bytes.len() as f64 / values.len() as f64
    );
    assert_eq!(block.pipeline.transform().unwrap(), Transform::Chimp);
    assert!(xor_float_gate_passes(&XorFloatMeasurement {
        best_existing_bytes: best_existing as u64,
        chimp_bytes: block.bytes.len() as u64,
        values: values.len() as u64,
    }));
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
}

#[test]
fn xor_floats_are_kept_only_where_they_store_fewer_bytes() {
    let decimal: Vec<f64> = (0..8192u64)
        .map(|i| (2000 + (mix(i) % 50) as i64) as f64 / 100.0)
        .collect();
    let data = ColumnData::F64(decimal.clone());
    let selected = encode_block_with_strategy(&data, true, CascadeStrategy::DecodeOptimized);
    let time_series = encode_block_with_xor_floats(&data, true, CascadeStrategy::DecodeOptimized);
    let forced =
        encode_block_forced_for_conformance(&data, true, CascadeStrategy::DecodeOptimized, Transform::Chimp).unwrap();
    eprintln!(
        "two-decimal readings, 8192 values: chimp {:.3} bytes/value, adaptive selection {:.3} bytes/value",
        forced.bytes.len() as f64 / decimal.len() as f64,
        selected.bytes.len() as f64 / decimal.len() as f64
    );
    assert_eq!(time_series.pipeline, selected.pipeline);
    assert_eq!(time_series.bytes, selected.bytes);
    let ints = ColumnData::I64((0..4096).collect());
    assert_eq!(
        encode_block_with_xor_floats(&ints, false, CascadeStrategy::DecodeOptimized).bytes,
        encode_block_with_strategy(&ints, false, CascadeStrategy::DecodeOptimized).bytes
    );
}

fn reference_rle(values: &[u64]) -> Vec<u8> {
    let mut runs: Vec<(u64, u32)> = Vec::new();
    for value in values {
        match runs.last_mut() {
            Some((current, count)) if current == value => *count += 1,
            _ => runs.push((*value, 1)),
        }
    }
    let mut out = Writer::new();
    out.put_u32(runs.len() as u32);
    for (value, count) in runs {
        out.put_u64(value);
        out.put_u32(count);
    }
    out.into_bytes()
}

#[test]
fn rle_without_a_run_list_matches_the_collected_runs() {
    let alternating: Vec<u64> = (0..2_000).map(|i| i % 2).collect();
    let distinct: Vec<u64> = (0..2_000).collect();
    let cases: [Vec<u64>; 6] = [
        vec![],
        vec![7],
        vec![3; 9_000],
        vec![1, 1, 2, 2, 2, 1, u64::MAX, u64::MAX, 0],
        alternating,
        distinct,
    ];
    for values in cases {
        let mut out = Writer::new();
        encode_rle(&values, &mut out);
        assert_eq!(out.into_bytes(), reference_rle(&values));
    }
}

#[test]
fn signed_plain_and_rle_stream_zigzag_codes_identically_to_a_mapped_copy() {
    let mut mixed: Vec<i64> = (0..2_500).map(|i| if i % 3 == 0 { -i } else { i / 7 }).collect();
    mixed.extend([i64::MIN, i64::MAX, i64::MIN, 0]);
    let cases: [Vec<i64>; 5] = [vec![], vec![-1], vec![i64::MIN; 1_024], vec![5; 1_025], mixed];
    for values in cases {
        let mapped: Vec<u64> = values.iter().map(|value| zigzag(*value)).collect();
        let mut plain = Writer::new();
        encode_i64_with(Transform::PlainU64, &values, &mut plain);
        let mut expected_plain = Writer::new();
        encode_plain_u64(&mapped, &mut expected_plain);
        assert_eq!(plain.into_bytes(), expected_plain.into_bytes());
        let mut rle = Writer::new();
        encode_i64_with(Transform::Rle, &values, &mut rle);
        assert_eq!(rle.into_bytes(), reference_rle(&mapped));
    }
}

#[test]
fn alp_exception_indexes_pack_identically_to_a_collected_index_vector() {
    let exceptions: Vec<(u32, u64)> = (0..3_000u32).map(|i| (i * 3 + 1_000, u64::from(i))).collect();
    let indexes: Vec<u64> = exceptions.iter().map(|(index, _)| u64::from(*index)).collect();
    let mut expected = Writer::new();
    encode_for_bitpack(&indexes, &mut expected);
    let base = indexes.iter().copied().min().unwrap_or(0);
    let max_delta = indexes.iter().map(|index| index - base).max().unwrap_or(0);
    let mut actual = Writer::new();
    actual.put_u64(base);
    bitpack_with(exceptions.len(), bits_needed(max_delta), &mut actual, |i| {
        u64::from(exceptions[i].0) - base
    });
    assert_eq!(actual.into_bytes(), expected.into_bytes());
}

fn reference_fsst_block(values: &StringColumn, replay: Option<&FsstTable>) -> (Vec<u8>, SideStream, Vec<usize>) {
    let present = present_values(values);
    let mut out = Writer::new();
    NullStream::from_values(values).write(&mut out);
    let corpus: Vec<&[u8]> = present.iter().map(|value| value.as_bytes()).collect();
    let trained;
    let compressor = match replay {
        Some(table) => &table.0,
        None => {
            trained = fsst::Compressor::train(&corpus);
            &trained
        }
    };
    out.put_u16(compressor.symbol_table().len() as u16);
    for (symbol, length) in compressor.symbol_table().iter().zip(compressor.symbol_lengths().iter()) {
        out.put_u64(symbol.to_u64());
        out.put_u8(*length);
    }
    let mut scratch = Vec::new();
    let mut arena = Vec::new();
    let mut compressed_lens = Vec::new();
    for value in &corpus {
        compress_fsst_value(compressor, value, &mut scratch);
        compressed_lens.push(scratch.len());
        arena.extend_from_slice(&scratch);
    }
    out.put_u32(present.len() as u32);
    write_offsets(compressed_lens.into_iter(), &mut out).unwrap();
    out.put_slice(&arena);
    if !fsst_value_keys_pay(&present) {
        return (out.into_bytes(), SideStream::None, Vec::new());
    }
    let prefixes_start = out.len();
    for value in &present {
        out.put_slice(&PrefixKey::of(value).to_stored());
    }
    let fingerprints_start = out.len();
    for value in &present {
        out.put_u32(StringFingerprint::of(value.as_bytes()).to_bits());
    }
    (
        out.into_bytes(),
        SideStream::FsstValueKeys,
        vec![prefixes_start, fingerprints_start],
    )
}

fn fsst_layout_columns() -> Vec<(&'static str, StringColumn)> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let escape_heavy: StringColumn = (0..300)
        .map(|_| {
            let text: String = (0..24)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    char::from_u32(0x20 + (state >> 40) as u32 % 0x2_0000).unwrap_or('?')
                })
                .collect();
            Some(text)
        })
        .collect();
    vec![
        ("empty", StringColumn::new()),
        ("all null", (0..50).map(|_| None::<&str>).collect()),
        (
            "present empty strings",
            (0..60).map(|i| Some(if i % 3 == 0 { "" } else { "abc" })).collect(),
        ),
        (
            "alternating nulls",
            (0..400)
                .map(|i| (i % 2 == 0).then(|| format!("user-{i:05}@example.com")))
                .collect(),
        ),
        (
            "multibyte unicode",
            (0..200)
                .map(|i| {
                    Some(format!(
                        "Gr\u{fc}\u{df}e {i} \u{65e5}\u{672c}\u{8a9e} \u{1f680} {}",
                        i * 7
                    ))
                })
                .collect(),
        ),
        (
            "long strings",
            (0..120)
                .map(|i| (i % 5 != 0).then(|| format!("{i}:").repeat(40) + &"lorem ipsum dolor sit amet ".repeat(8)))
                .collect(),
        ),
        ("escape heavy", escape_heavy),
    ]
}

#[test]
fn fsst_trained_block_bytes_match_the_reference_layout() {
    for (name, values) in fsst_layout_columns() {
        let (expected, expected_side, expected_breaks) = reference_fsst_block(&values, None);
        let present = present_values(&values);
        let mut out = Writer::new();
        let (side, trained, breaks) = encode_fsst_string(&values, &present, None, &mut out).unwrap();
        assert_eq!(out.into_bytes(), expected, "{name}: bytes");
        assert_eq!(side, expected_side, "{name}: side stream");
        assert_eq!(breaks, expected_breaks, "{name}: frame breaks");
        if name == "long strings" {
            assert_eq!(
                side,
                SideStream::FsstValueKeys,
                "long values must exercise the per-value keys"
            );
        }
        let corpus: Vec<&[u8]> = present.iter().map(|value| value.as_bytes()).collect();
        assert_eq!(
            fsst_table_entries(&trained.expect("a block without a replay table trains one")),
            fsst_table_entries(&FsstTable(fsst::Compressor::train(&corpus))),
            "{name}: trained table"
        );
    }
}

#[test]
fn fsst_replayed_block_bytes_match_the_reference_layout() {
    let training: Vec<String> = (0..300).map(|i| format!("user-{i:05}@example.com")).collect();
    let corpus: Vec<&[u8]> = training.iter().map(|value| value.as_bytes()).collect();
    let table = FsstTable(fsst::Compressor::train(&corpus));
    for (name, values) in fsst_layout_columns() {
        let (expected, expected_side, expected_breaks) = reference_fsst_block(&values, Some(&table));
        let present = present_values(&values);
        let mut out = Writer::new();
        let (side, trained, breaks) = encode_fsst_string(&values, &present, Some(&table), &mut out).unwrap();
        assert_eq!(out.into_bytes(), expected, "{name}: bytes");
        assert_eq!(side, expected_side, "{name}: side stream");
        assert_eq!(breaks, expected_breaks, "{name}: frame breaks");
        assert!(trained.is_none(), "{name}: a replayed block trains no table");
    }
}

fn reference_encode_dictionary_string(
    values: &StringColumn,
    present: &[&str],
    strategy: CascadeStrategy,
    shared: Option<&[String]>,
    out: &mut Writer,
) -> Result<SideStream, FormatError> {
    NullStream::from_values(values).write(out);
    let mut dictionary: Vec<&str> = AHashSet::from_iter(present.iter().copied()).into_iter().collect();
    dictionary.sort_unstable();
    let code_of: AHashMap<&str, u64> = dictionary
        .iter()
        .enumerate()
        .map(|(index, value)| (*value, index as u64))
        .collect();
    let codes: Vec<u64> = present
        .iter()
        .map(|value| code_of.get(value).copied().unwrap_or(0))
        .collect();
    if let Some(alphabet) = shared
        && dictionary
            .iter()
            .all(|value| alphabet.binary_search_by(|entry| entry.as_str().cmp(value)).is_ok())
    {
        let shared_codes: Vec<u64> = codes
            .iter()
            .map(|&code| {
                dictionary
                    .get(code as usize)
                    .and_then(|value| alphabet.binary_search_by(|entry| entry.as_str().cmp(value)).ok())
                    .unwrap_or(0) as u64
            })
            .collect();
        let max = shared_codes.iter().copied().max().unwrap_or(0);
        let mut shared_out = Writer::new();
        bitpack(&shared_codes, bits_needed(max), &mut shared_out);
        let mut local_out = Writer::new();
        let local_side = reference_encode_local_dictionary(&dictionary, &codes, strategy, &mut local_out)?;
        if shared_out.len() < local_out.len() {
            out.put_slice(shared_out.bytes());
            return Ok(SideStream::FileScopeDictionary);
        }
        out.put_slice(local_out.bytes());
        return Ok(local_side);
    }
    reference_encode_local_dictionary(&dictionary, &codes, strategy, out)
}

fn reference_encode_local_dictionary(
    dictionary: &[&str],
    codes: &[u64],
    strategy: CascadeStrategy,
    out: &mut Writer,
) -> Result<SideStream, FormatError> {
    out.put_u32(dictionary.len() as u32);
    let fsst = (cascade_level_fits(1)
        && dictionary.len() >= FSST_MIN_SAMPLE_COUNT
        && dictionary
            .iter()
            .all(|value| value.len() <= DICTIONARY_FSST_MAX_VALUE_LEN))
    .then(|| {
        let corpus: Vec<&[u8]> = dictionary.iter().map(|value| value.as_bytes()).collect();
        let compressor = fsst::Compressor::train(&corpus);
        let mut scratch = Vec::new();
        let mut arena = Vec::new();
        let mut compressed_lens = Vec::new();
        for value in &corpus {
            compress_fsst_value(&compressor, value, &mut scratch);
            compressed_lens.push(scratch.len());
            arena.extend_from_slice(&scratch);
        }
        let plain = (dictionary.len() + 1) * 4 + corpus.iter().map(|value| value.len()).sum::<usize>();
        let compressed = 2 + 9 * compressor.symbol_table().len() + (dictionary.len() + 1) * 4 + arena.len();
        let threshold = match strategy {
            CascadeStrategy::DecodeOptimized | CascadeStrategy::NoTrailing => {
                plain * DECODE_OPTIMIZED_THRESHOLD_NUM / DECODE_OPTIMIZED_THRESHOLD_DEN
            }
            CascadeStrategy::SizeOptimized => plain - plain / SIZE_OPTIMIZED_SAVINGS_DIVISOR,
        };
        (compressed < threshold).then_some((compressor, compressed_lens, arena))
    })
    .flatten();
    let side = match fsst {
        Some((compressor, compressed_lens, arena)) => {
            out.put_u16(compressor.symbol_table().len() as u16);
            for (symbol, length) in compressor.symbol_table().iter().zip(compressor.symbol_lengths()) {
                out.put_u64(symbol.to_u64());
                out.put_u8(*length);
            }
            write_offsets(compressed_lens.into_iter(), out)?;
            out.put_slice(&arena);
            SideStream::FsstDictionaryValues
        }
        None => {
            write_offsets(dictionary.iter().map(|value| value.len()), out)?;
            for value in dictionary {
                out.put_slice(value.as_bytes());
            }
            SideStream::None
        }
    };
    let max = codes.iter().copied().max().unwrap_or(0);
    bitpack(codes, bits_needed(max), out);
    Ok(side)
}

#[test]
fn dictionary_string_encoding_matches_the_reference_encoder_byte_for_byte() {
    let urls: Vec<String> = (0..200)
        .map(|i| format!("https://events.example.com/api/v1/tenants/tenant-{i:04}/streams/manifest-{i:04}.json"))
        .collect();
    let unicode: Vec<String> = (0..40).map(|i| format!("caf\u{e9}-\u{65e5}\u{672c}-{i}")).collect();
    let wide_alphabet: Vec<String> = (0..1024).map(|i| format!("v{i:04}")).collect();
    let mut url_alphabet = urls.clone();
    url_alphabet.sort();
    let mut unicode_alphabet = unicode.clone();
    unicode_alphabet.sort();
    let pick = |pool: &[String], rows: usize, null_every: usize| -> Vec<Option<String>> {
        (0..rows)
            .map(|i| (null_every == 0 || i % null_every != 0).then(|| pool[i % pool.len()].clone()))
            .collect()
    };
    let columns: Vec<Vec<Option<String>>> = vec![
        Vec::new(),
        vec![None; 50],
        vec![Some("only".to_owned())],
        vec![Some(String::new()); 1025],
        pick(&["a".to_owned(), String::new(), "b".to_owned()], 1023, 5),
        pick(&urls, 1024, 0),
        pick(&urls, 2049, 13),
        pick(&unicode, 1025, 3),
        pick(&wide_alphabet[..2], 4096, 0),
        pick(&wide_alphabet[..64], 2048, 7),
        (0..3000)
            .map(|i| Some(if i % 2 == 0 { "v0000" } else { "v1023" }.to_owned()))
            .collect(),
    ];
    let alphabets: [Option<&[String]>; 4] = [None, Some(&wide_alphabet), Some(&url_alphabet), Some(&unicode_alphabet)];
    let strategies = [
        CascadeStrategy::DecodeOptimized,
        CascadeStrategy::NoTrailing,
        CascadeStrategy::SizeOptimized,
    ];
    let mut sides = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        let values: StringColumn = column.clone().into();
        let present = present_values(&values);
        for shared in alphabets {
            for strategy in strategies {
                let mut expected = Writer::new();
                expected.put_slice(b"prefix");
                let expected_side =
                    reference_encode_dictionary_string(&values, &present, strategy, shared, &mut expected).unwrap();
                let mut actual = Writer::new();
                actual.put_slice(b"prefix");
                let actual_side = encode_dictionary_string(&values, &present, strategy, shared, &mut actual).unwrap();
                let case = format!(
                    "column {index}, shared {}, {strategy:?}",
                    shared.map_or(0, <[String]>::len)
                );
                assert_eq!(actual_side, expected_side, "{case}");
                assert_eq!(actual.bytes(), expected.bytes(), "{case}");
                sides.push(actual_side);
            }
        }
    }
    for side in [
        SideStream::FileScopeDictionary,
        SideStream::FsstDictionaryValues,
        SideStream::None,
    ] {
        assert!(sides.contains(&side), "the cases must exercise {side:?}");
    }
}

#[test]
fn fsst_dictionary_values_written_in_place_match_the_reference_across_the_threshold() {
    let strategies = [
        CascadeStrategy::DecodeOptimized,
        CascadeStrategy::NoTrailing,
        CascadeStrategy::SizeOptimized,
    ];
    let mut expected = Writer::new();
    expected.put_slice(b"prefix");
    let mut actual = Writer::new();
    actual.put_slice(b"prefix");
    let mut sides = Vec::new();
    for prefix_len in 0..48 {
        let prefix = "\u{e9}x".repeat(prefix_len / 2) + &"p".repeat(prefix_len % 2);
        let owned: Vec<String> = (0..FSST_MIN_SAMPLE_COUNT + prefix_len)
            .map(|i| format!("{prefix}{i:03}"))
            .collect();
        let mut dictionary: Vec<&str> = owned.iter().map(String::as_str).collect();
        dictionary.sort_unstable();
        let codes: Vec<u64> = (0..2000).map(|row| (row % dictionary.len()) as u64).collect();
        for strategy in strategies {
            let expected_side =
                reference_encode_local_dictionary(&dictionary, &codes, strategy, &mut expected).unwrap();
            let actual_side =
                encode_local_dictionary(&dictionary, codes.len(), |row| codes[row], strategy, &mut actual).unwrap();
            assert_eq!(actual_side, expected_side, "prefix {prefix_len}, {strategy:?}");
            assert_eq!(actual.bytes(), expected.bytes(), "prefix {prefix_len}, {strategy:?}");
            sides.push(actual_side);
        }
    }
    assert!(
        sides.contains(&SideStream::FsstDictionaryValues),
        "the sweep must keep FSST"
    );
    assert!(sides.contains(&SideStream::None), "the sweep must roll FSST back");
}

#[test]
fn the_needle_encoder_matches_the_crate_compressor() {
    let mut state = 0x5EED_F55Au64;
    let mut tables: Vec<Vec<(u64, u8)>> = Vec::new();
    for round in 0..40u64 {
        let corpus: Vec<Vec<u8>> = (0..300u64)
            .map(|row| format!("room-{:05}-user{}-{}", (row * 7919 + round) % 997, row % 13, round).into_bytes())
            .collect();
        let compressor = fsst::Compressor::train(&corpus.iter().map(Vec::as_slice).collect());
        let symbols = compressor.symbol_table().iter().map(|symbol| symbol.to_u64());
        tables.push(symbols.zip(compressor.symbol_lengths().iter().copied()).collect());
    }
    while tables.len() < 400 {
        let count = 1 + (pseudo_random(&mut state) % 255) as usize;
        let shape = 1 + pseudo_random(&mut state) % 3;
        let symbols = forged_fsst_symbols(&mut state, count, shape);
        if fsst_table_is_well_formed(&symbols) {
            tables.push(symbols);
        }
    }
    for (index, symbols) in tables.iter().enumerate() {
        let table = FsstDecodeTable::read(&mut Reader::new(&forged_fsst_table(symbols))).unwrap();
        let compressor = table.compressor();
        let encoder = table.needle_encoder();
        for _ in 0..50 {
            let mut plaintext = Vec::new();
            for _ in 0..pseudo_random(&mut state) % 12 {
                let draw = pseudo_random(&mut state);
                match draw % 4 {
                    0 => plaintext.push((draw >> 8) as u8),
                    1 => plaintext.push(0),
                    _ => {
                        let (word, len) = symbols[(draw >> 8) as usize % symbols.len()];
                        let cut = 1 + (draw >> 20) as usize % usize::from(len);
                        plaintext.extend_from_slice(&word.to_le_bytes()[..cut]);
                    }
                }
            }
            assert_eq!(
                encoder.compress(&plaintext),
                compressor.compress(&plaintext),
                "table {index}: {plaintext:?}"
            );
        }
    }
}

#[test]
fn a_framed_fsst_block_decodes_whole_without_inflating_its_key_frames() {
    for (name, values) in fsst_layout_columns() {
        let present = present_values(&values);
        let mut out = Writer::new();
        let (side, _, breaks) = encode_fsst_string(&values, &present, None, &mut out).unwrap();
        let body = out.into_bytes();
        let framed = seekable_zstd::compress_with_breaks(&body, &breaks).unwrap();
        let pipeline =
            PipelineId::new(Transform::FsstString, Compression::SeekableZstd, ValueKind::String).with_side_stream(side);
        let plain_pipeline =
            PipelineId::new(Transform::FsstString, Compression::None, ValueKind::String).with_side_stream(side);
        let _ = seekable_zstd::take_frames_inflated();
        let decoded = decode_block(pipeline, &framed).unwrap();
        let inflated = seekable_zstd::take_frames_inflated();
        assert_eq!(decoded, ColumnData::Strings(values.clone()), "{name}: framed decode");
        assert_eq!(
            decode_block(plain_pipeline, &body).unwrap(),
            decoded,
            "{name}: framed and unframed decodes agree"
        );
        let table = seekable_zstd::seek_table(&framed).unwrap();
        if let Some(&keys_start) = breaks.first().filter(|_| !present.is_empty()) {
            let value_frames = table.frame_index_decomp(keys_start as u64 - 1) as usize + 1;
            assert!(
                inflated <= value_frames,
                "{name}: inflated {inflated} frames, the values span only {value_frames}"
            );
            assert!(
                (table.num_frames() as usize) > value_frames,
                "{name}: the keys sit in frames of their own"
            );
        }
    }
}

#[test]
fn a_dictionary_block_decodes_every_null_shape_and_refuses_a_code_past_its_entries() {
    let words = ["m.text", "m.image", "", "m.notice"];
    let shapes: Vec<StringColumn> = vec![
        (0..3000).map(|i| Some(words[i % words.len()])).collect(),
        (0..3000)
            .map(|i| (i % 3 != 0).then_some(words[i % words.len()]))
            .collect(),
        (0..3000)
            .map(|i| (i >= 1000).then_some(words[i % words.len()]))
            .collect(),
        (0..10).map(|_| None::<&str>).collect(),
    ];
    for values in shapes {
        let block = encode_block(&ColumnData::Strings(values.clone()), true);
        if block.pipeline.transform().unwrap() == Transform::DictionaryString {
            assert_eq!(
                decode_block(block.pipeline, &block.bytes).unwrap(),
                ColumnData::Strings(values)
            );
        }
    }
    let nulls = NullStream::from_values(&(0..4).map(|_| Some("x")).collect::<StringColumn>());
    assert!(matches!(
        merge_nulls_dictionary(&nulls, &[0, 1, 2, 3], &["a", "b", "c"]),
        Err(FormatError::RefOutOfRange { .. })
    ));
    assert!(matches!(
        merge_nulls_dictionary(&nulls, &[0, 1, 2], &["a", "b", "c"]),
        Err(FormatError::Structural { .. })
    ));
}

#[test]
fn a_wide_fsst_block_stores_its_value_frames_raw_and_its_key_frames_compressed() {
    const WORDS: [&str; 24] = [
        "the", "meeting", "moved", "to", "thursday", "please", "review", "draft", "before", "lunch", "shipping",
        "tonight", "thanks", "again", "for", "quick", "fix", "on", "staging", "build", "looks", "good", "merge", "now",
    ];
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move |below: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % below
    };
    let values: StringColumn = (0..6000)
        .map(|row| {
            (row % 11 != 0).then(|| {
                (0..6 + next(30))
                    .map(|_| WORDS[next(WORDS.len() as u64) as usize])
                    .collect::<Vec<_>>()
                    .join(" ")
            })
        })
        .collect();
    let block = encode_block(&ColumnData::Strings(values.clone()), true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::FsstString);
    assert_eq!(block.pipeline.side_stream().unwrap(), SideStream::FsstValueKeys);
    assert_eq!(block.pipeline.compression().unwrap(), Compression::SeekableZstd);
    let plain = remove_trailing(Compression::SeekableZstd, &block.bytes).unwrap();
    let keys_start = string_values_end(Transform::FsstString, &plain).unwrap();
    let table = seekable_zstd::seek_table(&block.bytes).unwrap();
    let (mut raw_value_frames, mut compressed_key_frames) = (0, 0);
    for frame in 0..table.num_frames() {
        let start = table.frame_start_decomp(frame).unwrap() as usize;
        let ratio = table.frame_size_comp(frame).unwrap() as f64 / table.frame_size_decomp(frame).unwrap() as f64;
        if start < keys_start {
            assert!(ratio > 0.5, "value frame {frame} keeps a codec that does not halve it");
            raw_value_frames += usize::from(ratio > 1.0);
        } else {
            assert!(ratio <= 1.0, "key frame {frame} stays compressed");
            compressed_key_frames += usize::from(ratio < 0.75);
        }
    }
    assert!(raw_value_frames > 0 && compressed_key_frames > 0);
    assert_eq!(
        decode_block(block.pipeline, &block.bytes).unwrap(),
        ColumnData::Strings(values.clone())
    );
    assert_eq!(
        decode_block_range(block.pipeline, &block.bytes, 2500, 2600).unwrap(),
        ColumnData::Strings(values.slice(2500, 2600))
    );
}

fn packed_fsst_sample() -> StringColumn {
    let mut values = StringColumn::new();
    for i in 0..3000u64 {
        if i % 11 == 0 {
            values.push(None);
        } else {
            values.push(Some(&format!("@user{}:example.org", i.wrapping_mul(7919) % 100_000)));
        }
    }
    values
}

#[test]
fn packed_fsst_round_trips_through_every_read_path() {
    let values = packed_fsst_sample();
    let data = ColumnData::Strings(values.clone());
    for random_access in [false, true] {
        let block = encode_block(&data, random_access);
        assert_eq!(block.pipeline.transform().unwrap(), Transform::FsstPackedString);
        let classic = encode_block_forced_for_conformance(
            &data,
            random_access,
            CascadeStrategy::DecodeOptimized,
            Transform::FsstString,
        )
        .unwrap();
        assert_eq!(classic.pipeline.transform().unwrap(), Transform::FsstString);
        assert!(block.uncompressed_len < classic.uncompressed_len);
        assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
        assert_eq!(
            decode_block_range(block.pipeline, &block.bytes, 17, 1234).unwrap(),
            decode_block_range(classic.pipeline, &classic.bytes, 17, 1234).unwrap()
        );
        let rows = [0usize, 1, 11, 500, 2999];
        assert_eq!(
            take_block_rows(block.pipeline, &block.bytes, &rows).unwrap(),
            take_block_rows(classic.pipeline, &classic.bytes, &rows).unwrap()
        );
        let views = decode_string_block_views(block.pipeline, &block.bytes)
            .unwrap()
            .unwrap();
        let classic_views = decode_string_block_views(classic.pipeline, &classic.bytes)
            .unwrap()
            .unwrap();
        assert_eq!(views.len(), 3000);
        assert!(views.is_null(0));
        for row in [1usize, 2, 500, 2999] {
            assert_eq!(views.value(row), classic_views.value(row));
        }
        for predicate in [
            predicate::StringPredicate::Equals("@user7919:example.org".to_owned()),
            predicate::StringPredicate::NotEquals("@user7919:example.org".to_owned()),
        ] {
            assert_eq!(
                predicate::filter_string_block(block.pipeline, &block.bytes, &predicate).unwrap(),
                predicate::filter_string_block(classic.pipeline, &classic.bytes, &predicate).unwrap()
            );
            assert_eq!(
                predicate::filter_string_block_or_decode(block.pipeline, &block.bytes, &predicate).unwrap(),
                predicate.filter_decoded(&values)
            );
        }
        assert_eq!(
            lazy::fsst_uncompressed_len(block.pipeline, &block.bytes).unwrap(),
            lazy::fsst_uncompressed_len(classic.pipeline, &classic.bytes).unwrap()
        );
    }
}

#[test]
fn packed_fsst_replays() {
    let data = ColumnData::Strings(packed_fsst_sample());
    let head = encode_block(&data, false);
    let capture = ReplayCapture::from_head(&data, head.pipeline, head.uncompressed_len, head.fsst.clone()).unwrap();
    let (block, kept) = encode_block_replayed(&data, false, CascadeStrategy::DecodeOptimized, Some(&capture), None);
    assert!(kept);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::FsstPackedString);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
}

#[test]
fn packed_fsst_rejects_inconsistent_lengths() {
    let data = ColumnData::Strings(packed_fsst_sample());
    let block = encode_block(&data, true);
    assert_eq!(block.pipeline.compression().unwrap(), Compression::None);
    let mut short = block.bytes.clone();
    short.pop();
    assert!(decode_block(block.pipeline, &short).is_err());
    let mut long = block.bytes.clone();
    long.push(0);
    assert!(decode_block(block.pipeline, &long).is_err());
    let wrong_kind = PipelineId::new(Transform::FsstPackedString, Compression::None, ValueKind::U64);
    assert!(decode_block(wrong_kind, &block.bytes).is_err());
}

#[test]
fn random_access_blocks_take_the_dictionary_trial() {
    let mut values = StringColumn::new();
    for i in 0..4096u64 {
        values.push(Some(&format!("!room{}:example.org", (i * 31) % 1500)));
    }
    let data = ColumnData::Strings(values);
    let block = encode_block(&data, true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
    let ColumnData::Strings(range) = decode_block_range(block.pipeline, &block.bytes, 100, 200).unwrap() else {
        panic!("string block decodes to strings");
    };
    assert_eq!(range.len(), 100);
}

#[test]
fn occurred_residual_round_trips_and_refuses_malformed_inner_blocks() {
    let occurred: Vec<i64> = (0..3_000i64)
        .map(|i| 1_700_000_000_000 + i * 1_009 + (i * 7_919) % 613)
        .collect();
    let block = encode_occurred_residual(&occurred, &occurred, CascadeStrategy::DecodeOptimized).unwrap();
    let direct = encode_block(&ColumnData::I64(occurred.clone()), false);
    assert!(block.bytes.len() * 20 < direct.bytes.len());
    assert_eq!(block.stats.min_i128, direct.stats.min_i128);
    assert_eq!(block.stats.max_i128, direct.stats.max_i128);
    let (plain, body) = resolve_occurred_residual(&block.bytes, &occurred).unwrap();
    assert_eq!(plain.transform().unwrap(), Transform::PlainU64);
    assert_eq!(decode_block(plain, &body).unwrap(), ColumnData::I64(occurred.clone()));
    let wild: Vec<i64> = occurred
        .iter()
        .enumerate()
        .map(|(i, v)| if i % 2 == 0 { i64::MIN } else { v ^ i64::MAX })
        .collect();
    let block = encode_occurred_residual(&wild, &occurred, CascadeStrategy::DecodeOptimized).unwrap();
    let (plain, body) = resolve_occurred_residual(&block.bytes, &occurred).unwrap();
    assert_eq!(decode_block(plain, &body).unwrap(), ColumnData::I64(wild));
    assert!(encode_occurred_residual(&[], &[], CascadeStrategy::DecodeOptimized).is_none());
    assert!(encode_occurred_residual(&[1, 2], &[1], CascadeStrategy::DecodeOptimized).is_none());
    for inner in [
        PipelineId::new(Transform::OccurredResidual, Compression::None, ValueKind::I64),
        PipelineId::new(Transform::PlainF64, Compression::None, ValueKind::F64),
        PipelineId::new(Transform::PlainU64, Compression::None, ValueKind::U64),
    ] {
        let mut body = inner.0.to_le_bytes().to_vec();
        body.extend_from_slice(&block.bytes[4..]);
        assert!(resolve_occurred_residual(&body, &occurred).is_err());
    }
}
