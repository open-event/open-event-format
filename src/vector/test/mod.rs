use super::*;
use crate::indexes::bitmap::RoaringRangeBitmap;
use arrow_array::Array;

fn model(dimension: u32, metric: VectorMetric) -> EmbeddingModel {
    EmbeddingModel {
        dimension,
        digest: [7u8; 32],
        id: "test-model".to_owned(),
        metric,
    }
}

fn unit(seed: u64, dimension: u32) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let raw: Vec<f32> = (0..dimension)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / (1u64 << 24) as f32) - 0.5
        })
        .collect();
    let norm = raw.iter().map(|value| value * value).sum::<f32>().sqrt();
    raw.into_iter().map(|value| value / norm).collect()
}

fn key(byte: u8) -> WrappingKey {
    WrappingKey::new([byte; 32])
}

fn input(rows: usize, dimension: u32) -> SegmentInput {
    SegmentInput {
        event_ids: (0..rows).map(|row| format!("$event{row}")).collect(),
        integer_columns: vec![(
            TIMESTAMP_COLUMN.to_owned(),
            (0..rows as i64).map(|row| 1_000 + row).collect(),
        )],
        string_columns: vec![
            (
                ROOM_ID_COLUMN.to_owned(),
                (0..rows)
                    .map(|row| format!("!room{}:secret.example", row % 2))
                    .collect(),
            ),
            (
                SENDER_COLUMN.to_owned(),
                (0..rows)
                    .map(|row| format!("@user{}:secret.example", row % 3))
                    .collect(),
            ),
        ],
        vectors: (0..rows).map(|row| unit(row as u64, dimension)).collect(),
    }
}

fn spec(model: EmbeddingModel, representations: Vec<VectorElement>) -> SegmentSpec {
    SegmentSpec {
        model,
        producer_device: "DEVICE".to_owned(),
        representations,
        room_id: Some("!room0:secret.example".to_owned()),
    }
}

const ALL: [VectorElement; 4] = [
    VectorElement::U1,
    VectorElement::I8,
    VectorElement::F16,
    VectorElement::F32,
];

#[test]
fn wrong_width_is_rejected() {
    let mut rows = input(3, 16);
    rows.vectors[1] = vec![0.25; 15];
    let result = write_segment(
        &spec(model(16, VectorMetric::Dot), vec![VectorElement::F32]),
        &rows,
        &key(1),
    );
    assert!(matches!(result, Err(VectorError::InvalidInput { .. })));
}

#[test]
fn unnormalized_vector_is_rejected_for_cosine() {
    let mut rows = input(3, 16);
    rows.vectors[2] = vec![1.0; 16];
    let result = write_segment(
        &spec(model(16, VectorMetric::Cosine), vec![VectorElement::F32]),
        &rows,
        &key(1),
    );
    assert!(matches!(result, Err(VectorError::InvalidInput { .. })));
    let accepted = write_segment(
        &spec(model(16, VectorMetric::Dot), vec![VectorElement::F32]),
        &rows,
        &key(1),
    );
    assert!(accepted.is_ok());
}

#[test]
fn empty_and_single_row_segments_are_written_and_read() {
    for rows in [0, 1] {
        let bytes = write_segment(
            &spec(model(8, VectorMetric::Cosine), ALL.to_vec()),
            &input(rows, 8),
            &key(1),
        )
        .unwrap();
        let reader = SegmentReader::open(bytes, &key(1)).unwrap();
        assert_eq!(reader.row_count(), rows as u64);
        assert_eq!(reader.granule_count(), 1);
        for meta in reader.vector_columns() {
            let granule = reader
                .vector_granule(meta.column_id, 0, &model(8, VectorMetric::Cosine))
                .unwrap();
            assert_eq!(granule.len(), rows);
        }
    }
}

#[test]
fn vector_blocks_start_on_64_byte_boundaries() {
    let bytes = write_segment(
        &spec(model(5, VectorMetric::Dot), ALL.to_vec()),
        &input(GRANULE_ROWS + 3, 5),
        &key(1),
    )
    .unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    for column in &reader.footer_columns_for_test() {
        for offset in column {
            assert_eq!(offset % 64, 0);
        }
    }
}

impl<S: SegmentSource> SegmentReader<S> {
    fn footer_columns_for_test(&self) -> Vec<Vec<u64>> {
        self.footer_block_offsets()
    }
}

#[test]
fn granules_and_single_rows_match_what_was_written_for_every_element_type() {
    let dimension = 12;
    let rows = input(GRANULE_ROWS + 10, dimension);
    let model = model(dimension, VectorMetric::Cosine);
    let bytes = write_segment(&spec(model.clone(), ALL.to_vec()), &rows, &key(1)).unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    for element in ALL {
        let column = representation_column_id(element);
        let mut expected = Vec::new();
        for vector in &rows.vectors {
            encode_row(element, vector, &mut expected);
        }
        let width = element.row_width(dimension);
        for row in [
            0u64,
            1,
            GRANULE_ROWS as u64 - 1,
            GRANULE_ROWS as u64,
            GRANULE_ROWS as u64 + 9,
        ] {
            let start = row as usize * width;
            assert_eq!(
                reader.vector_row(column, row, &model).unwrap(),
                expected[start..start + width]
            );
        }
        let granule = reader.vector_granule(column, 1, &model).unwrap();
        assert_eq!(granule.len(), 10);
        let values = granule.values().to_data();
        let stored = values.buffers()[0].as_slice();
        assert_eq!(stored, &expected[GRANULE_ROWS * width..]);
        if element == VectorElement::F32 {
            assert_eq!(decode_row(element, dimension, &expected[..width]), rows.vectors[0]);
        }
    }
}

#[test]
fn representation_lookup_returns_every_linked_column_coarsest_first() {
    let bytes = write_segment(
        &spec(
            model(8, VectorMetric::Cosine),
            vec![VectorElement::F32, VectorElement::U1, VectorElement::I8],
        ),
        &input(4, 8),
        &key(1),
    )
    .unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    let elements: Vec<VectorElement> = reader
        .representations(EMBEDDING_COLUMN_ID)
        .iter()
        .map(|meta| meta.element)
        .collect();
    assert_eq!(elements, vec![VectorElement::U1, VectorElement::I8, VectorElement::F32]);
}

#[test]
fn reading_or_scanning_with_a_different_model_or_metric_fails() {
    let stored = model(8, VectorMetric::Cosine);
    let bytes = write_segment(&spec(stored.clone(), vec![VectorElement::I8]), &input(4, 8), &key(1)).unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    let column = representation_column_id(VectorElement::I8);
    let mut other_id = stored.clone();
    other_id.id = "other".to_owned();
    let mut other_metric = stored.clone();
    other_metric.metric = VectorMetric::L2;
    for wrong in [other_id, other_metric] {
        assert!(matches!(
            reader.vector_row(column, 0, &wrong),
            Err(VectorError::ModelMismatch { .. })
        ));
        assert!(matches!(
            reader.top_k(column, &unit(1, 8), &wrong, None, 3),
            Err(VectorError::ModelMismatch { .. })
        ));
    }
}

fn brute_force(
    meta: &VectorColumnMeta,
    rows: &[Vec<f32>],
    query: &[f32],
    allowed: impl Fn(u64) -> bool,
    k: usize,
) -> Vec<ScoredRow> {
    let prepared = PreparedQuery::new(query, meta.model.metric);
    let mut scored: Vec<ScoredRow> = rows
        .iter()
        .enumerate()
        .filter(|(row, _)| allowed(*row as u64))
        .map(|(row, vector)| {
            let mut bytes = Vec::new();
            encode_row(meta.element, vector, &mut bytes);
            ScoredRow {
                row: row as u64,
                score: prepared.score(meta.element, &bytes),
            }
        })
        .collect();
    scored.sort_by(|left, right| right.score.total_cmp(&left.score).then(left.row.cmp(&right.row)));
    scored.truncate(k);
    scored
}

#[test]
fn top_k_equals_a_brute_force_reference_with_and_without_a_filter() {
    let dimension = 32;
    let rows = input(GRANULE_ROWS + 500, dimension);
    for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
        let model = model(dimension, metric);
        let bytes = write_segment(
            &spec(
                model.clone(),
                vec![VectorElement::U1, VectorElement::I8, VectorElement::F32],
            ),
            &rows,
            &key(1),
        )
        .unwrap();
        let reader = SegmentReader::open(bytes, &key(1)).unwrap();
        let filter = RoaringRangeBitmap::from_rows((0..rows.event_ids.len() as u64).filter(|row| row % 7 == 3));
        let query = unit(99_999, dimension);
        for meta in reader.vector_columns().to_vec() {
            let all = reader.top_k(meta.column_id, &query, &model, None, 20).unwrap();
            assert_eq!(all, brute_force(&meta, &rows.vectors, &query, |_| true, 20));
            let filtered = reader.top_k(meta.column_id, &query, &model, Some(&filter), 20).unwrap();
            assert_eq!(
                filtered,
                brute_force(&meta, &rows.vectors, &query, |row| filter.contains(row), 20)
            );
            assert!(filtered.iter().all(|row| row.row % 7 == 3));
        }
    }
}

#[test]
fn k_larger_than_the_matching_rows_returns_all_of_them() {
    let model = model(8, VectorMetric::Cosine);
    let bytes = write_segment(&spec(model.clone(), vec![VectorElement::U1]), &input(50, 8), &key(1)).unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    let filter = RoaringRangeBitmap::from_rows([3, 9, 27]);
    let column = representation_column_id(VectorElement::U1);
    let found = reader.top_k(column, &unit(5, 8), &model, Some(&filter), 100).unwrap();
    let mut rows: Vec<u64> = found.iter().map(|row| row.row).collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![3, 9, 27]);
}

#[test]
fn manifest_round_trips_and_content_hash_matches_file_bytes() {
    let model = model(8, VectorMetric::Cosine);
    let rows = input(10, 8);
    let bytes = write_segment(
        &spec(model.clone(), vec![VectorElement::U1, VectorElement::I8]),
        &rows,
        &key(1),
    )
    .unwrap();
    let footer_len = u64::from_le_bytes(bytes[bytes.len() - 12..bytes.len() - 4].try_into().unwrap()) as usize;
    let body = &bytes[128..bytes.len() - 20 - footer_len];
    let reader = SegmentReader::open(bytes.clone(), &key(1)).unwrap();
    let manifest = reader.manifest();
    assert_eq!(manifest.content_hash, *blake3::hash(body).as_bytes());
    assert_eq!(manifest.model, model);
    assert_eq!(manifest.producer_device, "DEVICE");
    assert_eq!(manifest.representations, vec![VectorElement::U1, VectorElement::I8]);
    assert_eq!(manifest.row_count, 10);
    assert_eq!(manifest.window.first_event, "$event0");
    assert_eq!(manifest.window.last_event, "$event9");
    assert_eq!(manifest.window.room_id.as_deref(), Some("!room0:secret.example"));
    reader.verify_content().unwrap();
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

#[test]
fn no_plaintext_value_room_id_or_footer_field_appears_in_the_file() {
    let model = model(8, VectorMetric::Cosine);
    let rows = input(20, 8);
    let bytes = write_segment(&spec(model, ALL.to_vec()), &rows, &key(1)).unwrap();
    for needle in [
        "secret.example",
        "$event1",
        "test-model",
        "DEVICE",
        "embedding_",
        "timestamp",
        "room_id",
    ] {
        assert!(!contains(&bytes, needle.as_bytes()), "{needle} leaked");
    }
    assert!(!contains(&bytes, &rows.vectors[0][0].to_le_bytes()));
    assert!(!contains(&bytes, &[7u8; 32]));
}

#[test]
fn a_ranged_read_decrypts_only_the_touched_blocks() {
    let model = model(8, VectorMetric::Cosine);
    let bytes = write_segment(&spec(model.clone(), ALL.to_vec()), &input(GRANULE_ROWS * 3, 8), &key(1)).unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    assert_eq!(reader.blocks_opened(), 0);
    reader
        .vector_row(
            representation_column_id(VectorElement::I8),
            GRANULE_ROWS as u64 + 1,
            &model,
        )
        .unwrap();
    assert_eq!(reader.blocks_opened(), 1);
    let filter = RoaringRangeBitmap::from_rows([5]);
    reader
        .top_k(
            representation_column_id(VectorElement::U1),
            &unit(1, 8),
            &model,
            Some(&filter),
            1,
        )
        .unwrap();
    assert_eq!(reader.blocks_opened(), 2);
}

#[test]
fn the_wrong_key_fails_to_open() {
    let bytes = write_segment(
        &spec(model(8, VectorMetric::Cosine), vec![VectorElement::U1]),
        &input(4, 8),
        &key(1),
    )
    .unwrap();
    assert!(matches!(
        SegmentReader::open(bytes, &key(2)),
        Err(VectorError::Authentication { .. })
    ));
}

#[test]
fn a_tampered_swapped_or_truncated_block_fails() {
    let model = model(8, VectorMetric::Cosine);
    let bytes = write_segment(
        &spec(model.clone(), vec![VectorElement::U1, VectorElement::I8]),
        &input(GRANULE_ROWS + 1, 8),
        &key(1),
    )
    .unwrap();
    let reader = SegmentReader::open(bytes.clone(), &key(1)).unwrap();
    let offsets = reader.footer_block_offsets();
    let column = representation_column_id(VectorElement::I8);

    let mut tampered = bytes.clone();
    tampered[offsets[0][0] as usize + 3] ^= 1;
    let reader = SegmentReader::open(tampered, &key(1)).unwrap();
    assert!(matches!(
        reader.strings(EVENT_ID_COLUMN, 0),
        Err(VectorError::Authentication { .. })
    ));

    let mut swapped = bytes.clone();
    let room_first = offsets[1][0] as usize;
    let room_second = offsets[1][1] as usize;
    let len = 64;
    let a = swapped[room_first..room_first + len].to_vec();
    let b = swapped[room_second..room_second + len].to_vec();
    swapped[room_first..room_first + len].copy_from_slice(&b);
    swapped[room_second..room_second + len].copy_from_slice(&a);
    let reader = SegmentReader::open(swapped, &key(1)).unwrap();
    assert!(reader.strings(ROOM_ID_COLUMN, 0).is_err());

    let truncated = bytes[..bytes.len() - 1].to_vec();
    assert!(SegmentReader::open(truncated, &key(1)).is_err());

    let mut footer_tampered = bytes.clone();
    let at = footer_tampered.len() - 30;
    footer_tampered[at] ^= 1;
    assert!(SegmentReader::open(footer_tampered, &key(1)).is_err());

    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    assert!(reader.vector_row(column, 0, &model).is_ok());
}

#[test]
fn rewrapping_keeps_the_content_and_locks_out_the_old_key() {
    let model = model(8, VectorMetric::Cosine);
    let bytes = write_segment(&spec(model.clone(), vec![VectorElement::I8]), &input(4, 8), &key(1)).unwrap();
    let rewrapped = rewrap_segment(&bytes, &key(1), &key(2)).unwrap();
    assert!(SegmentReader::open(rewrapped.clone(), &key(1)).is_err());
    let reader = SegmentReader::open(rewrapped, &key(2)).unwrap();
    reader.verify_content().unwrap();
    let original = SegmentReader::open(bytes, &key(1)).unwrap();
    assert_eq!(reader.manifest(), original.manifest());
}

#[test]
fn changing_the_model_lists_every_file_built_with_the_old_one() {
    let old = model(8, VectorMetric::Cosine);
    let mut new = old.clone();
    new.digest = [9u8; 32];
    let files: Vec<SegmentManifest> = [&old, &new, &old]
        .iter()
        .map(|model| {
            let bytes = write_segment(&spec((*model).clone(), vec![VectorElement::U1]), &input(2, 8), &key(1)).unwrap();
            SegmentReader::open(bytes, &key(1)).unwrap().manifest().clone()
        })
        .collect();
    let stale = segments_to_reembed(files.iter().enumerate(), &new);
    assert_eq!(stale, vec![0, 2]);
}

#[test]
fn multi_segment_search_prunes_filters_and_merges() {
    let dimension = 64;
    let model = model(dimension, VectorMetric::Cosine);
    let mut segments = Vec::new();
    let mut all = Vec::new();
    for segment in 0..4u64 {
        let mut rows = input(300, dimension);
        rows.event_ids = (0..300).map(|row| format!("$s{segment}r{row}")).collect();
        rows.vectors = (0..300).map(|row| unit(segment * 1000 + row, dimension)).collect();
        rows.string_columns[0].1 = vec![format!("!room{segment}"); 300];
        all.extend(
            rows.event_ids
                .iter()
                .cloned()
                .zip(rows.vectors.iter().cloned())
                .map(|(id, v)| (id, v, segment)),
        );
        let bytes = write_segment(
            &spec(
                model.clone(),
                vec![VectorElement::U1, VectorElement::I8, VectorElement::F32],
            ),
            &rows,
            &key(1),
        )
        .unwrap();
        segments.push(SegmentReader::open(bytes, &key(1)).unwrap());
    }
    let query = unit(2042, dimension);
    let exact = |allowed: &dyn Fn(u64) -> bool| {
        let mut scored: Vec<(f32, String)> = all
            .iter()
            .filter(|(_, _, segment)| allowed(*segment))
            .map(|(id, vector, _)| (vector.iter().zip(&query).map(|(a, b)| a * b).sum::<f32>(), id.clone()))
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        scored.into_iter().take(10).map(|(_, id)| id).collect::<Vec<_>>()
    };
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 10,
        model: &model,
        oversample: 20,
        query: &query,
    };
    let hits = search_segments(&segments, &search).unwrap();
    let truth = exact(&|_| true);
    let recall = hits.iter().filter(|hit| truth.contains(&hit.event_id)).count();
    assert!(recall >= 9, "recall {recall}/10");
    assert_eq!(hits[0].event_id, "$s2r42");

    let before: Vec<u64> = segments.iter().map(|segment| segment.blocks_opened()).collect();
    let filtered = VectorSearch {
        filter: SearchFilter {
            room_ids: Some(vec!["!room1".to_owned()]),
            ..SearchFilter::default()
        },
        ..search.clone()
    };
    let hits = search_segments(&segments, &filtered).unwrap();
    assert_eq!(hits.len(), 10);
    assert!(hits.iter().all(|hit| hit.segment == 1));
    for (index, segment) in segments.iter().enumerate() {
        if index != 1 {
            assert_eq!(
                segment.blocks_opened(),
                before[index],
                "pruned segment {index} was decrypted"
            );
        }
    }

    let timed = VectorSearch {
        filter: SearchFilter {
            senders: Some(vec!["@user0:secret.example".to_owned()]),
            time_range: Some((1_000, 1_009)),
            ..SearchFilter::default()
        },
        ..search
    };
    let hits = search_segments(&segments, &timed).unwrap();
    assert_eq!(hits.len(), 10);
    assert!(hits.iter().all(|hit| hit.row < 10 && hit.row % 3 == 0));
}

#[test]
fn u1_estimate_tracks_the_true_dot_product() {
    let dimension = 384;
    let query = unit(1, dimension);
    let mut error = 0.0f32;
    for seed in 2..200 {
        let vector = unit(seed, dimension);
        let mut row = Vec::new();
        encode_row(VectorElement::U1, &vector, &mut row);
        let truth: f32 = vector.iter().zip(&query).map(|(a, b)| a * b).sum();
        error += (u1_estimated_dot(&query, &row) - truth).abs();
    }
    assert!(error / 198.0 < 0.1);
}

#[test]
fn a_prefix_read_returns_the_leading_dimensions_and_refuses_quantized_columns() {
    let model = model(32, VectorMetric::Dot);
    let rows = input(5, 32);
    let bytes = write_segment(
        &spec(
            model.clone(),
            vec![VectorElement::I8, VectorElement::F16, VectorElement::F32],
        ),
        &rows,
        &key(1),
    )
    .unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    let f32_prefix = reader
        .vector_prefix(representation_column_id(VectorElement::F32), 3, 8, &model)
        .unwrap();
    assert_eq!(f32_prefix, rows.vectors[3][..8]);
    let f16_prefix = reader
        .vector_prefix(representation_column_id(VectorElement::F16), 3, 8, &model)
        .unwrap();
    assert_eq!(f16_prefix.len(), 8);
    assert!(
        f16_prefix
            .iter()
            .zip(&rows.vectors[3])
            .all(|(read, wrote)| (read - wrote).abs() < 1e-3)
    );
    assert!(
        reader
            .vector_prefix(representation_column_id(VectorElement::I8), 3, 8, &model)
            .is_err()
    );
    assert!(
        reader
            .vector_prefix(representation_column_id(VectorElement::F32), 3, 33, &model)
            .is_err()
    );
}

#[test]
fn the_centroid_is_the_normalized_mean_and_ranks_segments_by_closeness() {
    let model = model(16, VectorMetric::Cosine);
    let segment = |seeds: std::ops::Range<u64>| {
        let mut rows = input(seeds.clone().count(), 16);
        rows.vectors = seeds.map(|seed| unit(seed, 16)).collect();
        SegmentReader::open(
            write_segment(
                &spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]),
                &rows,
                &key(1),
            )
            .unwrap(),
            &key(1),
        )
        .unwrap()
    };
    let near = segment(100..101);
    let far = segment(200..203);
    let centroid = near.centroid().unwrap();
    assert!(
        centroid
            .iter()
            .zip(unit(100, 16))
            .all(|(read, wrote)| (read - wrote).abs() < 1e-5)
    );
    let norm = far
        .centroid()
        .unwrap()
        .iter()
        .map(|value| value * value)
        .sum::<f32>()
        .sqrt();
    assert!((norm - 1.0).abs() < 1e-5);

    let empty = SegmentReader::open(
        write_segment(&spec(model.clone(), vec![VectorElement::F32]), &input(0, 16), &key(1)).unwrap(),
        &key(1),
    )
    .unwrap();
    assert_eq!(empty.centroid(), None);

    let segments = [far, near, empty];
    let ranked = nearest_segments(&segments, &unit(100, 16), &model, 3).unwrap();
    assert_eq!(ranked, vec![2, 1, 0]);
    assert_eq!(
        nearest_segments(&segments, &unit(100, 16), &model, 2).unwrap(),
        vec![2, 1]
    );
    assert!(nearest_segments(&segments, &unit(100, 16), &self::model(16, VectorMetric::Dot), 1).is_err());
}

#[test]
fn merging_small_segments_keeps_every_row_in_order_and_searches_the_same() {
    let model = model(24, VectorMetric::Cosine);
    let representations = vec![VectorElement::U1, VectorElement::F32];
    let first = input(GRANULE_ROWS + 3, 24);
    let mut second = input(5, 24);
    second.event_ids = (0..5).map(|row| format!("$late{row}")).collect();
    second.vectors = (0..5).map(|row| unit(9_000 + row, 24)).collect();
    let readers: Vec<_> = [&first, &second]
        .into_iter()
        .map(|rows| {
            SegmentReader::open(
                write_segment(&spec(model.clone(), representations.clone()), rows, &key(1)).unwrap(),
                &key(1),
            )
            .unwrap()
        })
        .collect();
    let merged = SegmentReader::open(
        merge_segments(&readers, &spec(model.clone(), representations.clone()), &key(2)).unwrap(),
        &key(2),
    )
    .unwrap();
    assert_eq!(merged.row_count(), (GRANULE_ROWS + 8) as u64);
    assert_eq!(merged.manifest().window.first_event, "$event0");
    assert_eq!(merged.manifest().window.last_event, "$late4");
    let all = merged.read_all().unwrap();
    let expected: Vec<String> = first.event_ids.iter().chain(&second.event_ids).cloned().collect();
    assert_eq!(all.event_ids, expected);
    assert_eq!(all.vectors[GRANULE_ROWS + 4], second.vectors[1]);
    assert_eq!(all.string_columns, {
        let mut columns = first.string_columns.clone();
        columns
            .iter_mut()
            .zip(&second.string_columns)
            .for_each(|((_, into), (_, from))| into.extend(from.clone()));
        columns
    });

    let query = unit(9_002, 24);
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 3,
        model: &model,
        oversample: DEFAULT_OVERSAMPLE,
        query: &query,
    };
    let before: Vec<String> = search_segments(&readers, &search)
        .unwrap()
        .into_iter()
        .map(|hit| hit.event_id)
        .collect();
    let after: Vec<String> = search_segments(std::slice::from_ref(&merged), &search)
        .unwrap()
        .into_iter()
        .map(|hit| hit.event_id)
        .collect();
    assert_eq!(before, after);

    let mut other = input(2, 24);
    other.integer_columns.clear();
    let mismatched = SegmentReader::open(
        write_segment(&spec(model.clone(), representations.clone()), &other, &key(1)).unwrap(),
        &key(1),
    )
    .unwrap();
    let readers = [readers.into_iter().next().unwrap(), mismatched];
    assert!(merge_segments(&readers, &spec(model.clone(), representations.clone()), &key(2)).is_err());
    let dot = self::model(24, VectorMetric::Dot);
    assert!(merge_segments(&readers, &spec(dot, representations), &key(2)).is_err());
}

#[test]
fn segment_deletes_are_immutable_generations_that_round_trip() {
    let empty = SegmentDeletes::new();
    let first = empty.with_deleted([3, 4, 9]);
    let second = first.with_deleted([5]);
    assert_eq!((empty.generation(), first.generation(), second.generation()), (0, 1, 2));
    assert_eq!(empty.deleted_count(), 0);
    assert_eq!(first.deleted_count(), 3);
    assert!(!first.is_deleted(5));
    assert!(second.is_deleted(5) && second.is_deleted(3));
    let back = SegmentDeletes::decode(&second.encode()).unwrap();
    assert_eq!(back.generation(), 2);
    assert_eq!(back.deletion_vector().iter().collect::<Vec<_>>(), vec![3, 4, 5, 9]);
    assert!(SegmentDeletes::decode(&[1, 2]).is_err());
}

#[test]
fn search_skips_deleted_rows_and_compaction_drops_them() {
    let model = model(24, VectorMetric::Cosine);
    let representations = vec![VectorElement::U1, VectorElement::F32];
    let rows = input(GRANULE_ROWS + 10, 24);
    let bytes = write_segment(&spec(model.clone(), representations.clone()), &rows, &key(1)).unwrap();
    let readers = vec![SegmentReader::open(bytes.clone(), &key(1)).unwrap()];
    let target = GRANULE_ROWS as u64 + 2;
    let query = rows.vectors[target as usize].clone();
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 3,
        model: &model,
        oversample: DEFAULT_OVERSAMPLE,
        query: &query,
    };
    let before = search_segments(&readers, &search).unwrap();
    assert_eq!(before[0].row, target);

    let event = rows.event_ids[target as usize].clone();
    let deleted_rows = readers[0].rows_for_event_ids(&[event.as_str()]).unwrap();
    assert_eq!(deleted_rows, vec![target]);
    let deletes = SegmentDeletes::new().with_deleted(deleted_rows);
    let after = search_segments_with_deletes(&readers, &[Some(&deletes)], &search).unwrap();
    assert_eq!(after.len(), 3);
    assert!(after.iter().all(|hit| hit.row != target && hit.event_id != event));
    assert_eq!(after[0].row, before[1].row);
    assert_eq!(
        search_segments_with_deletes(&readers, &[Some(&SegmentDeletes::new())], &search).unwrap(),
        before
    );

    let filtered = VectorSearch {
        filter: SearchFilter {
            room_ids: Some(vec![rows.string_columns[0].1[target as usize].clone()]),
            ..SearchFilter::default()
        },
        ..search.clone()
    };
    assert!(
        search_segments_with_deletes(&readers, &[Some(&deletes)], &filtered)
            .unwrap()
            .iter()
            .all(|hit| hit.row != target)
    );
    let everything = deletes.with_deleted(0..readers[0].row_count());
    assert!(
        search_segments_with_deletes(&readers, &[Some(&everything)], &search)
            .unwrap()
            .is_empty()
    );

    let compacted = SegmentReader::open(
        merge_segments_with_deletes(
            &readers,
            &[Some(&deletes)],
            &spec(model.clone(), representations),
            &key(2),
        )
        .unwrap(),
        &key(2),
    )
    .unwrap();
    assert_eq!(compacted.row_count(), readers[0].row_count() - 1);
    let all = compacted.read_all().unwrap();
    let expected: Vec<String> = rows.event_ids.iter().filter(|id| **id != event).cloned().collect();
    assert_eq!(all.event_ids, expected);
    assert_eq!(all.string_columns[0].1.len(), expected.len());
    assert_eq!(all.integer_columns[0].1.len(), expected.len());
    assert!(!all.integer_columns[0].1.contains(&(1_000 + target as i64)));
    let hits = search_segments(std::slice::from_ref(&compacted), &search).unwrap();
    assert!(hits.iter().all(|hit| hit.event_id != event));
}

#[test]
fn a_catalogued_search_reads_each_segments_deletes_from_the_manifest() {
    use crate::lifecycle::ManifestGeneration;
    use crate::object_store::sim::SimObjectStore;
    let model = model(16, VectorMetric::Cosine);
    let representations = vec![VectorElement::U1, VectorElement::F32];
    let rows = input(40, 16);
    let bytes = write_segment(&spec(model.clone(), representations), &rows, &key(1)).unwrap();
    let readers = vec![SegmentReader::open(bytes, &key(1)).unwrap()];
    let target = 7u64;
    let query = rows.vectors[target as usize].clone();
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 3,
        model: &model,
        oversample: DEFAULT_OVERSAMPLE,
        query: &query,
    };
    let store = SimObjectStore::new();
    let mut manifest = ManifestGeneration::default();
    manifest.register_vector_segment(11);
    let before = search_catalogued_segments(&store, &manifest, &readers, &[11], &search).unwrap();
    assert_eq!(before[0].row, target);
    publish_segment_deletes(&store, &mut manifest, 11, &SegmentDeletes::new().with_deleted([target])).unwrap();
    let after = search_catalogued_segments(&store, &manifest, &readers, &[11], &search).unwrap();
    assert!(after.iter().all(|hit| hit.row != target));
    assert!(search_catalogued_segments(&store, &manifest, &readers, &[], &search).is_err());
}

#[test]
fn every_block_and_the_footer_is_sealed_on_its_own_nonce() {
    let keys = super::seal::FileKeys::random().unwrap();
    let indexes = [
        0,
        1,
        2,
        255,
        256,
        1 << 32,
        u64::MAX - 1,
        super::constant::FOOTER_BLOCK_INDEX,
    ];
    let nonces: std::collections::HashSet<[u8; 12]> = indexes.iter().map(|index| keys.nonce(*index)).collect();
    assert_eq!(nonces.len(), indexes.len());
    assert_eq!(keys.nonce(7)[..4], keys.nonce_prefix[..4]);
    assert_eq!(keys.nonce(7)[4..], 7u64.to_le_bytes());
}

#[test]
fn blocks_of_identical_plaintext_seal_differently_and_cannot_stand_in_for_each_other() {
    let dimension = 8;
    let model = model(dimension, VectorMetric::Dot);
    let mut rows = input(GRANULE_ROWS * 2, dimension);
    rows.vectors = vec![vec![0.25; dimension as usize]; GRANULE_ROWS * 2];
    let bytes = write_segment(&spec(model.clone(), vec![VectorElement::F32]), &rows, &key(1)).unwrap();
    let reader = SegmentReader::open(bytes.clone(), &key(1)).unwrap();
    let column = representation_column_id(VectorElement::F32);
    assert_eq!(
        reader.vector_row(column, 0, &model).unwrap(),
        reader.vector_row(column, GRANULE_ROWS as u64, &model).unwrap()
    );

    let offsets = reader.footer_block_offsets();
    let vector_blocks = offsets.last().unwrap();
    let plain_len = GRANULE_ROWS * dimension as usize * 4;
    let sealed_len = plain_len + 16;
    let first = vector_blocks[0] as usize;
    let second = vector_blocks[1] as usize;
    let first_block = bytes[first..first + sealed_len].to_vec();
    let second_block = bytes[second..second + sealed_len].to_vec();
    assert_ne!(first_block[..plain_len], second_block[..plain_len]);
    assert!(
        first_block
            .chunks(16)
            .zip(second_block.chunks(16))
            .all(|(left, right)| left != right)
    );

    let mut swapped = bytes.clone();
    swapped[first..first + sealed_len].copy_from_slice(&second_block);
    swapped[second..second + sealed_len].copy_from_slice(&first_block);
    let reader = SegmentReader::open(swapped, &key(1)).unwrap();
    for row in [0, GRANULE_ROWS as u64] {
        assert!(matches!(
            reader.vector_row(column, row, &model),
            Err(VectorError::Authentication { .. })
        ));
    }

    let other = write_segment(&spec(model.clone(), vec![VectorElement::F32]), &rows, &key(1)).unwrap();
    let mut transplanted = bytes.clone();
    transplanted[first..first + sealed_len].copy_from_slice(&other[first..first + sealed_len]);
    let reader = SegmentReader::open(transplanted, &key(1)).unwrap();
    assert!(matches!(
        reader.vector_row(column, 0, &model),
        Err(VectorError::Authentication { .. })
    ));
}

#[test]
fn a_first_version_segment_still_reads_and_is_never_written_again() {
    let model = model(8, VectorMetric::Cosine);
    let spec = spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]);
    let rows = input(GRANULE_ROWS + 5, 8);
    let version = |bytes: &[u8]| u16::from_le_bytes([bytes[4], bytes[5]]);
    let current = write_segment(&spec, &rows, &key(1)).unwrap();
    assert_eq!(version(&current), 2);

    let legacy_keys = super::seal::FileKeys::random_shared_nonce().unwrap();
    assert_eq!(legacy_keys.nonce(0), legacy_keys.nonce(1));
    assert_eq!(legacy_keys.nonce(0)[..], legacy_keys.nonce_prefix[..12]);
    let legacy = super::segment::write_segment_sealed_by(&spec, &rows, &key(1), &legacy_keys).unwrap();
    assert_eq!(version(&legacy), 1);
    let reader = SegmentReader::open(legacy.clone(), &key(1)).unwrap();
    reader.verify_content().unwrap();
    let read = reader.read_all().unwrap();
    assert_eq!(read.event_ids, rows.event_ids);
    assert_eq!(read.string_columns, rows.string_columns);
    assert_eq!(read.integer_columns, rows.integer_columns);

    for (bytes, relabelled) in [(&legacy, 2u8), (&current, 1u8)] {
        let mut bytes = bytes.clone();
        bytes[4] = relabelled;
        assert!(matches!(
            SegmentReader::open(bytes, &key(1)),
            Err(VectorError::Authentication { .. })
        ));
    }
    let mut unknown = legacy.clone();
    unknown[4] = 3;
    assert!(matches!(
        SegmentReader::open(unknown, &key(1)),
        Err(VectorError::Format(_))
    ));

    let rewrapped = rewrap_segment(&legacy, &key(1), &key(2)).unwrap();
    assert_eq!(version(&rewrapped), 1);
    let rewrapped = SegmentReader::open(rewrapped, &key(2)).unwrap();
    assert_eq!(rewrapped.read_all().unwrap().event_ids, rows.event_ids);
    let merged = merge_segments(std::slice::from_ref(&reader), &spec, &key(1)).unwrap();
    assert_eq!(version(&merged), 2);
    let merged = SegmentReader::open(merged, &key(1)).unwrap();
    assert_eq!(merged.read_all().unwrap().event_ids, rows.event_ids);
}

#[test]
fn a_row_scores_exactly_as_its_decoded_floats_do() {
    for dimension in [1u32, 7, 32, 100] {
        let query = unit(4_242, dimension);
        for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
            let prepared = PreparedQuery::new(&query, metric);
            for element in [VectorElement::I8, VectorElement::F16, VectorElement::F32] {
                if element == VectorElement::I8 && metric != VectorMetric::L2 {
                    continue;
                }
                for seed in 0..20 {
                    let mut row = Vec::new();
                    encode_row(element, &unit(seed, dimension), &mut row);
                    let decoded = decode_row(element, dimension, &row);
                    let pairs = decoded.iter().zip(&query);
                    let expected: f32 = match metric {
                        VectorMetric::L2 => -pairs
                            .map(|(stored, value)| (stored - value) * (stored - value))
                            .sum::<f32>(),
                        VectorMetric::Cosine | VectorMetric::Dot => pairs.map(|(stored, value)| stored * value).sum(),
                    };
                    assert_eq!(
                        prepared.score(element, &row).to_bits(),
                        expected.to_bits(),
                        "{element:?} {metric:?} dimension {dimension} seed {seed}"
                    );
                }
            }
        }
    }
}

#[test]
fn a_filter_of_many_scattered_runs_scans_exactly_its_rows_in_every_granule() {
    use crate::indexes::bitmap::RowRange;
    let dimension = 8;
    let model = model(dimension, VectorMetric::Dot);
    let rows = input(GRANULE_ROWS * 3 + 17, dimension);
    let total = rows.event_ids.len() as u64;
    let bytes = write_segment(&spec(model.clone(), vec![VectorElement::F32]), &rows, &key(1)).unwrap();
    let reader = SegmentReader::open(bytes.clone(), &key(1)).unwrap();
    let meta = reader.vector_columns()[0].clone();
    let query = unit(77, dimension);
    let granule = GRANULE_ROWS as u64;
    let filters = [
        RoaringRangeBitmap::from_rows((0..total).filter(|row| row % 2 == 0)),
        RoaringRangeBitmap::from_ranges([
            RowRange { start: 3, end: 9 },
            RowRange {
                start: granule - 2,
                end: granule + 2,
            },
            RowRange {
                start: granule * 3,
                end: granule * 3 + 1,
            },
            RowRange {
                start: total - 1,
                end: total + 50,
            },
        ]),
        RoaringRangeBitmap::from_rows([granule * 2 - 1]),
        RoaringRangeBitmap::from_rows([granule * 2]),
        RoaringRangeBitmap::from_rows([total + 5]),
    ];
    for filter in &filters {
        let k = total as usize;
        let found = reader.top_k(meta.column_id, &query, &model, Some(filter), k).unwrap();
        assert_eq!(found.len(), (0..total).filter(|row| filter.contains(*row)).count());
        assert_eq!(
            found,
            brute_force(&meta, &rows.vectors, &query, |row| filter.contains(row), k)
        );
    }
    for (filter, opened) in [(&filters[2], 1), (&filters[3], 1), (&filters[4], 0)] {
        let reader = SegmentReader::open(bytes.clone(), &key(1)).unwrap();
        reader.top_k(meta.column_id, &query, &model, Some(filter), 5).unwrap();
        assert_eq!(reader.blocks_opened(), opened);
    }
}

#[test]
fn search_hits_carry_their_own_event_ids_and_decrypt_each_id_granule_once() {
    let dimension = 16;
    let model = model(dimension, VectorMetric::Dot);
    let mut first = input(GRANULE_ROWS * 2 + 30, dimension);
    let mut second = input(GRANULE_ROWS + 30, dimension);
    first.event_ids = (0..first.event_ids.len()).map(|row| format!("$a{row}")).collect();
    second.event_ids = (0..second.event_ids.len()).map(|row| format!("$b{row}")).collect();
    second.vectors = (0..second.event_ids.len())
        .map(|row| unit(1_000_000 + row as u64, dimension))
        .collect();
    let segment_spec = spec(model.clone(), vec![VectorElement::F32]);
    let readers = vec![
        SegmentReader::open(write_segment(&segment_spec, &first, &key(1)).unwrap(), &key(1)).unwrap(),
        SegmentReader::open(write_segment(&segment_spec, &second, &key(1)).unwrap(), &key(1)).unwrap(),
    ];
    let query = unit(31_337, dimension);
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 400,
        model: &model,
        oversample: DEFAULT_OVERSAMPLE,
        query: &query,
    };
    let hits = search_segments(&readers, &search).unwrap();
    let opened: u64 = readers.iter().map(|reader| reader.blocks_opened()).sum();
    assert_eq!(hits.len(), 400);
    assert!(hits.iter().any(|hit| hit.segment == 0) && hits.iter().any(|hit| hit.segment == 1));
    for pair in hits.windows(2) {
        assert!(
            pair[0].score > pair[1].score
                || (pair[0].score == pair[1].score && (pair[0].segment, pair[0].row) < (pair[1].segment, pair[1].row))
        );
    }
    for hit in &hits {
        let prefix = if hit.segment == 0 { "$a" } else { "$b" };
        assert_eq!(hit.event_id, format!("{prefix}{}", hit.row));
    }
    assert!(opened <= 10, "{opened} blocks opened");
}

#[test]
fn rows_for_event_ids_finds_every_holder_once_in_row_order() {
    let model = model(8, VectorMetric::Cosine);
    let mut rows = input(GRANULE_ROWS + 20, 8);
    rows.event_ids[GRANULE_ROWS + 3] = rows.event_ids[5].clone();
    let bytes = write_segment(&spec(model, vec![VectorElement::I8]), &rows, &key(1)).unwrap();
    let reader = SegmentReader::open(bytes, &key(1)).unwrap();
    let last = format!("$event{}", GRANULE_ROWS + 19);
    let found = reader
        .rows_for_event_ids(&[last.as_str(), "$event5", "$missing", "$event5", "$event0"])
        .unwrap();
    assert_eq!(found, vec![0, 5, GRANULE_ROWS as u64 + 3, GRANULE_ROWS as u64 + 19]);
    assert!(reader.rows_for_event_ids(&[]).unwrap().is_empty());
}

struct RangedOnly(Vec<u8>);

impl SegmentSource for RangedOnly {
    fn byte_len(&self) -> Result<u64, VectorError> {
        Ok(self.0.len() as u64)
    }

    fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>, VectorError> {
        self.0.read_range(offset, len)
    }
}

#[test]
fn borrowed_and_ranged_sources_read_the_same_rows() {
    let model = model(8, VectorMetric::Cosine);
    let rows = input(GRANULE_ROWS + 9, 8);
    let bytes = write_segment(&spec(model.clone(), ALL.to_vec()), &rows, &key(1)).unwrap();
    assert_eq!(bytes.borrow_range(10, 4), Some(&bytes[10..14]));
    assert_eq!(bytes.borrow_range(bytes.len() as u64 - 1, 2), None);
    assert_eq!(bytes.borrow_range(u64::MAX, 2), None);
    let borrowed = SegmentReader::open(bytes.clone(), &key(1)).unwrap();
    let ranged = SegmentReader::open(RangedOnly(bytes), &key(1)).unwrap();
    let query = unit(3, 8);
    for meta in borrowed.vector_columns() {
        assert_eq!(
            borrowed.top_k(meta.column_id, &query, &model, None, 25).unwrap(),
            ranged.top_k(meta.column_id, &query, &model, None, 25).unwrap()
        );
    }
    assert_eq!(
        borrowed.read_all().unwrap().event_ids,
        ranged.read_all().unwrap().event_ids
    );
    assert_eq!(borrowed.blocks_opened(), ranged.blocks_opened());
}
