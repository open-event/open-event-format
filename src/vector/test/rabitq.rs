use super::super::rabitq::Rotation;
use super::*;

/// `clusters × members` unit rows: each a cluster centre plus noise, shuffled so every segment holds every cluster.
fn clustered(clusters: usize, members: usize, dimension: u32, noise: f32) -> Vec<Vec<f32>> {
    let mut rows = Vec::with_capacity(clusters * members);
    for cluster in 0..clusters {
        let centre = unit(1_000_000 + cluster as u64, dimension);
        for member in 0..members {
            rows.push(perturb(&centre, (cluster * members + member) as u64, noise));
        }
    }
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    for index in (1..rows.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        rows.swap(index, (state % (index as u64 + 1)) as usize);
    }
    rows
}

fn perturb(centre: &[f32], seed: u64, noise: f32) -> Vec<f32> {
    let direction = unit(seed.wrapping_add(77), centre.len() as u32);
    let mut row: Vec<f32> = centre.iter().zip(&direction).map(|(c, d)| c + noise * d).collect();
    let norm = row.iter().map(|value| value * value).sum::<f32>().sqrt();
    row.iter_mut().for_each(|value| *value /= norm);
    row
}

fn rows_input(vectors: &[Vec<f32>], prefix: &str) -> SegmentInput {
    SegmentInput {
        event_ids: (0..vectors.len()).map(|row| format!("${prefix}{row}")).collect(),
        vectors: vectors.to_vec(),
        ..SegmentInput::default()
    }
}

fn exact(metric: VectorMetric, left: &[f32], right: &[f32]) -> f32 {
    match metric {
        VectorMetric::L2 => -left.iter().zip(right).map(|(a, b)| (a - b) * (a - b)).sum::<f32>(),
        _ => left.iter().zip(right).map(|(a, b)| a * b).sum(),
    }
}

#[test]
fn the_rotation_is_orthonormal_and_inverts_for_every_dimension() {
    for dimension in [1usize, 2, 8, 12, 100, 128, 384, 768] {
        let rotation = Rotation::new(dimension, 42);
        let original = unit(dimension as u64, dimension as u32);
        let mut rotated = original.clone();
        rotation.apply(&mut rotated);
        let norm = rotated.iter().map(|value| value * value).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4, "dimension {dimension} norm {norm}");
        if dimension > 4 {
            assert!(rotated.iter().zip(&original).any(|(a, b)| (a - b).abs() > 1e-3));
        }
        rotation.apply_inverse(&mut rotated);
        for (a, b) in rotated.iter().zip(&original) {
            assert!((a - b).abs() < 1e-4, "dimension {dimension}");
        }
        let other = unit(dimension as u64 + 1, dimension as u32);
        let mut other_rotated = other.clone();
        rotation.apply(&mut other_rotated);
        let mut again = original.clone();
        rotation.apply(&mut again);
        let before = exact(VectorMetric::Dot, &original, &other);
        let after = exact(VectorMetric::Dot, &again, &other_rotated);
        assert!((before - after).abs() < 1e-4);
    }
}

#[test]
fn rabitq_and_segment_int8_estimates_stay_within_their_bounds() {
    let dimension = 128;
    let vectors = clustered(100, 20, dimension, 0.6);
    for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
        let model = model(dimension, metric);
        let bytes = write_segment(
            &spec(
                model.clone(),
                vec![VectorElement::U1, VectorElement::I8, VectorElement::F32],
            ),
            &rows_input(&vectors, "e"),
            &key(1),
        )
        .unwrap();
        let reader = SegmentReader::open(bytes, &key(1)).unwrap();
        assert_eq!(reader.version(), SEGMENT_VERSION);
        for element in [VectorElement::U1, VectorElement::I8] {
            let (mut misses, mut checked, mut width) = (0usize, 0usize, 0f64);
            for query_seed in 0..5u64 {
                let query = perturb(&unit(1_000_000 + query_seed * 7, dimension), 500 + query_seed, 0.6);
                let prepared = reader.codec().query(element, &query);
                for vector in &vectors {
                    let mut row = Vec::new();
                    reader.codec().encode_row(element, vector, &mut row);
                    let estimate = prepared.estimate(&row);
                    let truth = exact(metric, &query, vector);
                    checked += 1;
                    width += f64::from(estimate.upper - estimate.lower);
                    if truth < estimate.lower || truth > estimate.upper {
                        misses += 1;
                    }
                }
            }
            let rate = misses as f64 / checked as f64;
            match element {
                VectorElement::I8 => assert_eq!(misses, 0, "{metric:?} int8 bound violated"),
                _ => assert!(rate < 0.01, "{metric:?} rabitq bound missed {rate}"),
            }
            assert!(
                width / (checked as f64) < 1.0,
                "{metric:?} {element:?} interval too wide"
            );
        }
    }
}

#[test]
fn rabitq_rows_decode_near_the_original() {
    let dimension = 64;
    let model = model(dimension, VectorMetric::L2);
    let vectors: Vec<Vec<f32>> = (0..64).map(|row| unit(row, dimension)).collect();
    let reader = SegmentReader::open(
        write_segment(
            &spec(model.clone(), vec![VectorElement::U1]),
            &rows_input(&vectors, "e"),
            &key(1),
        )
        .unwrap(),
        &key(1),
    )
    .unwrap();
    let column = representation_column_id(VectorElement::U1);
    let read = reader.read_all().unwrap();
    for (row, vector) in vectors.iter().enumerate() {
        let stored = reader.vector_row(column, row as u64, &model).unwrap();
        let decoded = reader.decode_vector_row(VectorElement::U1, &stored);
        assert_eq!(decoded, read.vectors[row]);
        let similarity =
            exact(VectorMetric::Dot, &decoded, vector) / decoded.iter().map(|value| value * value).sum::<f32>().sqrt();
        assert!(similarity > 0.6, "row {row} decodes to cosine {similarity}");
    }
}

#[test]
fn equal_input_gives_byte_identical_vector_blocks_and_codec_parameters() {
    let dimension = 96;
    let vectors = clustered(30, 10, dimension, 0.5);
    let representations = vec![
        VectorElement::U1,
        VectorElement::I8,
        VectorElement::F16,
        VectorElement::F32,
    ];
    for metric in [VectorMetric::Cosine, VectorMetric::L2] {
        let model = model(dimension, metric);
        let segment_spec = spec(model.clone(), representations.clone());
        let readers: Vec<_> = [1u8, 2]
            .into_iter()
            .map(|byte| {
                let bytes = write_segment(&segment_spec, &rows_input(&vectors, "e"), &key(byte)).unwrap();
                SegmentReader::open(bytes, &key(byte)).unwrap()
            })
            .collect();
        assert_eq!(readers[0].codec().params(), readers[1].codec().params());
        let params = readers[0].codec().params().unwrap();
        assert_eq!(params.seed, super::super::codec::rotation_seed(&model));
        assert_eq!(params.centre.len(), dimension as usize);
        assert_eq!(params.i8_dimensions.len(), dimension as usize);
        for element in &representations {
            let column = representation_column_id(*element);
            let left = readers[0].vector_granule(column, 0, &model).unwrap().to_data();
            let right = readers[1].vector_granule(column, 0, &model).unwrap().to_data();
            assert_eq!(
                left.child_data()[0].buffers()[0].as_slice(),
                right.child_data()[0].buffers()[0].as_slice(),
                "{element:?}"
            );
        }
        let mut other_model = model.clone();
        other_model.id = "other-model".to_owned();
        assert_ne!(
            super::super::codec::rotation_seed(&other_model),
            super::super::codec::rotation_seed(&model)
        );
    }
}

fn brute_force_top(vectors: &[&[Vec<f32>]], query: &[f32], metric: VectorMetric, k: usize) -> Vec<(usize, u64)> {
    let mut all: Vec<(f32, usize, u64)> = vectors
        .iter()
        .enumerate()
        .flat_map(|(segment, rows)| {
            rows.iter()
                .enumerate()
                .map(move |(row, vector)| (exact(metric, query, vector), segment, row as u64))
        })
        .collect();
    all.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    all.into_iter()
        .take(k)
        .map(|(_, segment, row)| (segment, row))
        .collect()
}

/// 20 000 × 128 clustered unit rows in four segments: bound-driven RaBitQ search against brute force.
#[test]
fn rabitq_search_matches_brute_force_recall_with_a_small_rerank_set() {
    let dimension = 128;
    let k = 10;
    let vectors = clustered(500, 40, dimension, 0.6);
    for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
        let model = model(dimension, metric);
        let segment_spec = spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]);
        let parts: Vec<&[Vec<f32>]> = vectors.chunks(5000).collect();
        let readers: Vec<_> = parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let bytes = write_segment(&segment_spec, &rows_input(part, &format!("s{index}-")), &key(1)).unwrap();
                SegmentReader::open(bytes, &key(1)).unwrap()
            })
            .collect();
        let queries = 50;
        let (mut found, mut reranked, mut candidates) = (0usize, 0u64, 0u64);
        for query_index in 0..queries {
            let cluster = (query_index * 37 % 500) as u64;
            let query = perturb(&unit(1_000_000 + cluster, dimension), 900_000 + query_index as u64, 0.6);
            let search = VectorSearch {
                filter: SearchFilter::default(),
                k,
                model: &model,
                oversample: DEFAULT_OVERSAMPLE,
                query: &query,
            };
            let (hits, stats) = search_segments_with_stats(&readers, &[], &search).unwrap();
            let truth = brute_force_top(&parts, &query, metric, k);
            found += hits
                .iter()
                .filter(|hit| truth.contains(&(hit.segment, hit.row)))
                .count();
            reranked += stats.rows_reranked;
            candidates += stats.rows_candidate;
            assert_eq!(stats.rows_scanned, vectors.len() as u64);
        }
        let recall = found as f64 / (queries * k) as f64;
        let mean_reranked = reranked as f64 / queries as f64;
        println!(
            "{metric:?}: recall@{k} {recall:.4}, mean reranked {mean_reranked:.1}, mean candidates {:.1}",
            candidates as f64 / queries as f64
        );
        assert!(recall >= 0.95, "{metric:?} recall {recall}");
        assert!(
            // The position-scaled patience reranks a little past the last improvement (~22 rows per segment).
            mean_reranked / (readers.len() as f64) < (k * DEFAULT_OVERSAMPLE) as f64 / 3.0,
            "{metric:?} reranked {mean_reranked} per query over {} segments",
            readers.len()
        );
    }
}

#[test]
fn a_far_segment_is_skipped_once_nearer_segments_fill_the_result() {
    let dimension = 64;
    let model = model(dimension, VectorMetric::Cosine);
    let segment_spec = spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]);
    let near_centre = unit(1, dimension);
    let far_centre: Vec<f32> = near_centre.iter().map(|value| -value).collect();
    let near: Vec<Vec<f32>> = (0..500).map(|row| perturb(&near_centre, row, 0.3)).collect();
    let far: Vec<Vec<f32>> = (0..500).map(|row| perturb(&far_centre, 10_000 + row, 0.3)).collect();
    let readers: Vec<_> = [&far, &near]
        .iter()
        .map(|rows| {
            SegmentReader::open(
                write_segment(&segment_spec, &rows_input(rows, "e"), &key(1)).unwrap(),
                &key(1),
            )
            .unwrap()
        })
        .collect();
    let query = perturb(&near_centre, 99_999, 0.3);
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 5,
        model: &model,
        oversample: 0,
        query: &query,
    };
    let (hits, stats) = search_segments_with_stats(&readers, &[], &search).unwrap();
    assert_eq!(stats.segments_searched, 1);
    assert_eq!(stats.segments_skipped, 1);
    assert_eq!(stats.rows_scanned, 500);
    let truth = brute_force_top(&[&far, &near], &query, VectorMetric::Cosine, 5);
    assert_eq!(hits.iter().map(|hit| (hit.segment, hit.row)).collect::<Vec<_>>(), truth);
}

#[test]
fn a_third_version_segment_keeps_hamming_and_per_row_int8_and_merges_into_version_4() {
    let dimension = 16;
    let model = model(dimension, VectorMetric::Dot);
    let representations = vec![VectorElement::U1, VectorElement::I8, VectorElement::F32];
    let segment_spec = spec(model.clone(), representations.clone());
    let rows = input(GRANULE_ROWS + 300, dimension);
    let keys = super::super::seal::FileKeys::random_subblock_vectors().unwrap();
    let legacy = super::super::segment::write_segment_sealed_by(&segment_spec, &rows, &key(1), &keys).unwrap();
    let reader = SegmentReader::open(legacy, &key(1)).unwrap();
    assert_eq!(reader.version(), 3);
    reader.verify_content().unwrap();
    assert!(reader.codec().params().is_none());
    let query = unit(5, dimension);
    for element in [VectorElement::U1, VectorElement::I8] {
        let column = representation_column_id(element);
        let mut expected = Vec::new();
        encode_row(element, &rows.vectors[GRANULE_ROWS + 7], &mut expected);
        let stored = reader.vector_row(column, GRANULE_ROWS as u64 + 7, &model).unwrap();
        assert_eq!(stored, expected);
        assert_eq!(
            reader.decode_vector_row(element, &stored),
            decode_row(element, dimension, &expected)
        );
        let prepared = PreparedQuery::new(&query, VectorMetric::Dot);
        let top = reader.top_k(column, &query, &model, None, 5).unwrap();
        let mut reference: Vec<ScoredRow> = rows
            .vectors
            .iter()
            .enumerate()
            .map(|(row, vector)| {
                let mut bytes = Vec::new();
                encode_row(element, vector, &mut bytes);
                ScoredRow {
                    row: row as u64,
                    score: prepared.score(element, &bytes),
                }
            })
            .collect();
        reference.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.row.cmp(&b.row)));
        reference.truncate(5);
        assert_eq!(top, reference);
    }
    if let VectorElement::U1 = representations[0] {
        let hamming = reader
            .top_k(representation_column_id(VectorElement::U1), &query, &model, None, 1)
            .unwrap();
        assert_eq!(hamming[0].score.fract(), 0.0);
    }
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 5,
        model: &model,
        oversample: DEFAULT_OVERSAMPLE,
        query: &query,
    };
    let (_, stats) = search_segments_with_stats(std::slice::from_ref(&reader), &[], &search).unwrap();
    assert_eq!(stats.rows_reranked, (5 * DEFAULT_OVERSAMPLE) as u64);
    assert_eq!(stats.rows_candidate, 0);

    let merged = SegmentReader::open(
        merge_segments(std::slice::from_ref(&reader), &segment_spec, &key(1)).unwrap(),
        &key(1),
    )
    .unwrap();
    assert_eq!(merged.version(), SEGMENT_VERSION);
    assert!(merged.codec().params().is_some());
    assert_eq!(merged.read_all().unwrap().vectors, reader.read_all().unwrap().vectors);
    let exact_search = VectorSearch {
        oversample: 0,
        ..search
    };
    let before = search_segments(
        std::slice::from_ref(&reader),
        &VectorSearch {
            oversample: GRANULE_ROWS,
            ..exact_search.clone()
        },
    )
    .unwrap();
    let after = search_segments(std::slice::from_ref(&merged), &exact_search).unwrap();
    assert_eq!(
        before.iter().map(|hit| &hit.event_id).collect::<Vec<_>>(),
        after.iter().map(|hit| &hit.event_id).collect::<Vec<_>>()
    );
}
