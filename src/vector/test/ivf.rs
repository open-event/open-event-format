use super::super::search::{estimated_selectivity, granule_may_match};
use super::*;
use crate::lifecycle::ManifestGeneration;
use crate::object_store::sim::SimObjectStore;

/// `clusters × members` unit rows written cluster by cluster, so a cluster's rows share sub-blocks.
fn clustered_in_order(clusters: usize, members: usize, dimension: u32, noise: f32) -> Vec<Vec<f32>> {
    let mut rows = Vec::with_capacity(clusters * members);
    for cluster in 0..clusters {
        let centre = unit(5_000_000 + cluster as u64, dimension);
        for member in 0..members {
            let direction = unit((cluster * members + member) as u64 + 99, dimension);
            let mut row: Vec<f32> = centre.iter().zip(&direction).map(|(c, d)| c + noise * d).collect();
            let norm = row.iter().map(|v| v * v).sum::<f32>().sqrt();
            row.iter_mut().for_each(|v| *v /= norm);
            rows.push(row);
        }
    }
    rows
}

fn score(metric: VectorMetric, left: &[f32], right: &[f32]) -> f32 {
    match metric {
        VectorMetric::L2 => -left.iter().zip(right).map(|(a, b)| (a - b) * (a - b)).sum::<f32>(),
        _ => left.iter().zip(right).map(|(a, b)| a * b).sum(),
    }
}

fn exact_top(vectors: &[Vec<f32>], query: &[f32], metric: VectorMetric, k: usize) -> Vec<u64> {
    let mut all: Vec<(f32, u64)> = vectors
        .iter()
        .enumerate()
        .map(|(row, vector)| (score(metric, vector, query), row as u64))
        .collect();
    all.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    all.into_iter().take(k).map(|(_, row)| row).collect()
}

fn vectors_input(vectors: &[Vec<f32>]) -> SegmentInput {
    let mut rows = input(vectors.len(), vectors.first().map_or(1, |v| v.len() as u32));
    rows.vectors = vectors.to_vec();
    rows
}

#[test]
fn an_indexed_search_matches_the_scan_and_opens_far_fewer_blocks() {
    let dimension = 64;
    let k = 10;
    let vectors = clustered_in_order(64, 125, dimension, 0.5);
    for metric in [VectorMetric::Cosine, VectorMetric::L2] {
        let model = model(dimension, metric);
        let bytes = write_segment(
            &spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]),
            &vectors_input(&vectors),
            &key(1),
        )
        .unwrap();
        let segments = vec![SegmentReader::open(bytes, &key(1)).unwrap()];
        let index = PartitionIndex::build(&segments[0], &PartitionParams::default()).unwrap();
        assert!(
            (64..=90).contains(&index.partitions.len()),
            "⌈√8000⌉ = 90 partitions, empty ones dropped"
        );
        let queries = 30;
        let (mut scan_found, mut index_found) = (0usize, 0usize);
        let (mut scan_blocks, mut index_blocks, mut probed) = (0u64, 0u64, 0u64);
        for query_index in 0..queries {
            let cluster = (query_index * 13 % 64) as u64;
            let centre = unit(5_000_000 + cluster, dimension);
            let noise = unit(777_000 + query_index as u64, dimension);
            let mut query: Vec<f32> = centre.iter().zip(&noise).map(|(c, d)| c + 0.4 * d).collect();
            let norm = query.iter().map(|v| v * v).sum::<f32>().sqrt();
            query.iter_mut().for_each(|v| *v /= norm);
            let search = VectorSearch {
                filter: SearchFilter::default(),
                k,
                model: &model,
                oversample: 0,
                query: &query,
            };
            let truth = exact_top(&vectors, &query, metric, k);
            let before = segments[0].blocks_opened();
            let (scanned, _) = search_segments_with_stats(&segments, &[], &search).unwrap();
            let middle = segments[0].blocks_opened();
            let (indexed, stats) =
                search_segments_indexed(&segments, &[], &[Some(&index)], &search, SearchOptions::default()).unwrap();
            let after = segments[0].blocks_opened();
            scan_blocks += middle - before;
            index_blocks += after - middle;
            probed += stats.partitions_probed;
            scan_found += scanned.iter().filter(|hit| truth.contains(&hit.row)).count();
            index_found += indexed.iter().filter(|hit| truth.contains(&hit.row)).count();
            assert_eq!(
                indexed.iter().map(|hit| hit.row).collect::<Vec<_>>(),
                scanned.iter().map(|hit| hit.row).collect::<Vec<_>>(),
                "{metric:?} query {query_index}: the index changes the answer"
            );
        }
        let total = (queries * k) as f64;
        println!(
            "{metric:?}: recall@{k} scan {:.3} indexed {:.3}; blocks opened per query scan {:.1} indexed {:.1}; \
             partitions probed {:.1}/{}",
            scan_found as f64 / total,
            index_found as f64 / total,
            scan_blocks as f64 / queries as f64,
            index_blocks as f64 / queries as f64,
            probed as f64 / queries as f64,
            index.partitions.len(),
        );
        assert!(index_found as f64 / total >= 0.99, "{metric:?} indexed recall");
        assert!(
            index_blocks * 3 < scan_blocks,
            "{metric:?}: indexed opened {index_blocks} blocks vs scan {scan_blocks}"
        );
    }
}

#[test]
fn the_index_is_deterministic_round_trips_and_seals_with_the_wrapping_key() {
    let dimension = 16;
    let model = model(dimension, VectorMetric::Cosine);
    let vectors = clustered_in_order(10, 30, dimension, 0.4);
    let bytes = write_segment(
        &spec(model, vec![VectorElement::F32]),
        &vectors_input(&vectors),
        &key(1),
    )
    .unwrap();
    let segment = SegmentReader::open(bytes, &key(1)).unwrap();
    let params = PartitionParams {
        partitions: Some(7),
        ..PartitionParams::default()
    };
    let index = PartitionIndex::build(&segment, &params).unwrap();
    assert_eq!(index, PartitionIndex::build(&segment, &params).unwrap());
    assert_eq!(
        index.partitions.iter().map(|p| p.rows.len()).sum::<usize>(),
        vectors.len()
    );
    for partition in &index.partitions {
        assert!(partition.rows.windows(2).all(|pair| pair[0] < pair[1]));
        for row in &partition.rows {
            let distance = score(VectorMetric::L2, &vectors[*row as usize], &partition.centroid);
            assert!((-distance).sqrt() <= partition.radius);
        }
    }

    let plain = index.encode(None).unwrap();
    assert_eq!(index.encode(None).unwrap(), plain);
    assert_eq!(PartitionIndex::decode(&plain, None).unwrap(), index);

    let sealed = index.encode(Some(&key(1))).unwrap();
    assert_eq!(PartitionIndex::decode(&sealed, Some(&key(1))).unwrap(), index);
    assert!(PartitionIndex::decode(&sealed, None).is_err());
    assert!(PartitionIndex::decode(&sealed, Some(&key(2))).is_err());
    let centroid_bytes = index.partitions[0].centroid[0].to_le_bytes();
    assert!(plain.windows(4).any(|w| w == centroid_bytes));
    assert!(
        !sealed.windows(4).any(|w| w == centroid_bytes),
        "sealed centroids must not appear"
    );
    let mut tampered = sealed.clone();
    let last = tampered.len() - 1;
    tampered[last] ^= 1;
    assert!(PartitionIndex::decode(&tampered, Some(&key(1))).is_err());
    let mut truncated = plain.clone();
    truncated.truncate(plain.len() - 1);
    assert!(PartitionIndex::decode(&truncated, None).is_err());
}

#[test]
fn a_published_index_loads_rejects_mismatches_and_dropping_it_still_answers() {
    let dimension = 32;
    let model = model(dimension, VectorMetric::Dot);
    let vectors = clustered_in_order(20, 50, dimension, 0.5);
    let segment_spec = spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]);
    let segment = SegmentReader::open(
        write_segment(&segment_spec, &vectors_input(&vectors), &key(1)).unwrap(),
        &key(1),
    )
    .unwrap();
    let other = SegmentReader::open(
        write_segment(&segment_spec, &vectors_input(&vectors[..500]), &key(1)).unwrap(),
        &key(1),
    )
    .unwrap();
    let index = PartitionIndex::build(&segment, &PartitionParams::default()).unwrap();
    let store = SimObjectStore::new();
    let mut manifest = ManifestGeneration::default();
    let reference = publish_segment_index(&store, &mut manifest, 9, &index, Some(&key(1))).unwrap();
    assert!(reference.sealed);
    assert_eq!(reference.object_key, segment_index_object_key(9, &reference.blake3));
    assert_eq!(manifest.vector_segment(9).unwrap().index, Some(reference.clone()));
    let loaded = load_segment_index(&store, &manifest, 9, &segment, Some(&key(1))).unwrap();
    assert_eq!(loaded.as_ref(), Some(&index));
    assert!(
        load_segment_index(&store, &manifest, 9, &segment, None).is_err(),
        "sealed needs the key"
    );
    assert!(
        load_segment_index(&store, &manifest, 9, &other, Some(&key(1))).is_err(),
        "other content"
    );
    assert!(
        search_segments_indexed(
            std::slice::from_ref(&other),
            &[],
            &[Some(&index)],
            &VectorSearch {
                filter: SearchFilter::default(),
                k: 5,
                model: &model,
                oversample: 0,
                query: &vectors[0],
            },
            SearchOptions::default()
        )
        .is_err(),
        "a search refuses an index of other content"
    );

    let mut bad_digest = manifest.clone();
    bad_digest.vector_segments[0].index.as_mut().unwrap().blake3[0] ^= 1;
    assert!(matches!(
        load_segment_index(&store, &bad_digest, 9, &segment, Some(&key(1))),
        Err(VectorError::Authentication { .. })
    ));
    let mut bad_model = manifest.clone();
    bad_model.vector_segments[0].index.as_mut().unwrap().model_digest[0] ^= 1;
    assert!(matches!(
        load_segment_index(&store, &bad_model, 9, &segment, Some(&key(1))),
        Err(VectorError::ModelMismatch { .. })
    ));
    let mut wrong_model = index.clone();
    wrong_model.model.id = "other-model".to_owned();
    assert!(wrong_model.check(&segment).is_err());

    let query = vectors[123].clone();
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 10,
        model: &model,
        oversample: DEFAULT_OVERSAMPLE,
        query: &query,
    };
    let segments = std::slice::from_ref(&segment);
    let plain = search_segments(segments, &search).unwrap();
    let (indexed, stats) = search_catalogued_segments_indexed(
        &store,
        &manifest,
        segments,
        &[9],
        &search,
        Some(&key(1)),
        SearchOptions::default(),
    )
    .unwrap();
    assert_eq!(indexed, plain);
    assert!(stats.partitions_probed > 0 && stats.indexes_rejected == 0);
    let (rejected, stats) = search_catalogued_segments_indexed(
        &store,
        &bad_digest,
        segments,
        &[9],
        &search,
        Some(&key(1)),
        SearchOptions::default(),
    )
    .unwrap();
    assert_eq!(rejected, plain);
    assert_eq!((stats.indexes_rejected, stats.partitions_probed), (1, 0));

    let dropped = drop_segment_index(&mut manifest, 9);
    assert_eq!(dropped, Some(reference));
    assert_eq!(manifest.vector_segment(9).unwrap().index, None);
    let (after, stats) = search_catalogued_segments_indexed(
        &store,
        &manifest,
        segments,
        &[9],
        &search,
        Some(&key(1)),
        SearchOptions::default(),
    )
    .unwrap();
    assert_eq!(after, plain);
    assert_eq!((stats.indexes_rejected, stats.partitions_probed), (0, 0));

    let legacy: VectorSegmentEntry = serde_json::from_str(r#"{"deletes":null,"segment_id":9}"#).unwrap();
    assert_eq!(legacy.index, None);
}

#[test]
fn an_indexed_search_honours_filters_and_deletes() {
    let dimension = 32;
    let model = model(dimension, VectorMetric::Cosine);
    let vectors = clustered_in_order(30, 100, dimension, 0.5);
    let segment = SegmentReader::open(
        write_segment(
            &spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]),
            &vectors_input(&vectors),
            &key(1),
        )
        .unwrap(),
        &key(1),
    )
    .unwrap();
    let index = PartitionIndex::build(&segment, &PartitionParams::default()).unwrap();
    let segments = std::slice::from_ref(&segment);
    let deletes = SegmentDeletes::new().with_deleted((0..3000).step_by(7));
    for (query_row, filter) in [
        (40usize, SearchFilter::default()),
        (
            1500,
            SearchFilter {
                room_ids: Some(vec!["!room1:secret.example".to_owned()]),
                ..SearchFilter::default()
            },
        ),
        (
            2900,
            SearchFilter {
                senders: Some(vec!["@user2:secret.example".to_owned()]),
                time_range: Some((1_000, 3_500)),
                ..SearchFilter::default()
            },
        ),
    ] {
        let search = VectorSearch {
            filter,
            k: 10,
            model: &model,
            oversample: 0,
            query: &vectors[query_row],
        };
        let plain = search_segments_with_deletes(segments, &[Some(&deletes)], &search).unwrap();
        for strategy in [
            FilterStrategy::PreFilter,
            FilterStrategy::PostFilter,
            FilterStrategy::Adaptive,
        ] {
            let options = SearchOptions {
                filter_strategy: strategy,
                ..SearchOptions::default()
            };
            let (indexed, _) =
                search_segments_indexed(segments, &[Some(&deletes)], &[Some(&index)], &search, options).unwrap();
            assert_eq!(indexed, plain, "{strategy:?} {:?}", search.filter);
        }
        assert_eq!(plain.len(), 10);
        assert!(plain.iter().all(|hit| !deletes.is_deleted(hit.row)));
    }
}

#[test]
fn granules_without_statistics_may_match() {
    let dimension = 8;
    let model = model(dimension, VectorMetric::Dot);
    let mut rows = input(50, dimension);
    rows.integer_columns.clear();
    rows.string_columns.retain(|(name, _)| name == ROOM_ID_COLUMN);
    let segment = SegmentReader::open(
        write_segment(&spec(model, vec![VectorElement::F32]), &rows, &key(1)).unwrap(),
        &key(1),
    )
    .unwrap();
    let timed = SearchFilter {
        time_range: Some((0, 10)),
        ..SearchFilter::default()
    };
    let senders = SearchFilter {
        senders: Some(vec!["@user0:secret.example".to_owned()]),
        ..SearchFilter::default()
    };
    assert!(segment.column_stats(TIMESTAMP_COLUMN, 0).is_none());
    assert!(
        granule_may_match(&segment, 0, &timed),
        "absent stats must mean may match"
    );
    assert!(granule_may_match(&segment, 0, &senders));
    assert_eq!(estimated_selectivity(&segment, &timed), 1.0);
    let rooms = SearchFilter {
        room_ids: Some(vec!["!zzz".to_owned()]),
        ..SearchFilter::default()
    };
    assert!(!granule_may_match(&segment, 0, &rooms), "present stats still prune");
    assert_eq!(estimated_selectivity(&segment, &rooms), 0.0);
}

#[test]
fn pre_and_post_filtering_return_the_same_hits() {
    let dimension = 32;
    let rows_count = 10_000;
    for metric in [VectorMetric::Cosine, VectorMetric::L2] {
        let model = model(dimension, metric);
        let mut rows = input(rows_count, dimension);
        rows.string_columns[1].1 = (0..rows_count)
            .map(|row| format!("@user{}:secret.example", if row % 50 == 0 { 9 } else { row % 3 }))
            .collect();
        let segments: Vec<_> = (0..2)
            .map(|part| {
                let mut part_rows = rows.clone();
                part_rows.vectors = (0..rows_count)
                    .map(|row| unit((part * rows_count + row) as u64, dimension))
                    .collect();
                SegmentReader::open(
                    write_segment(
                        &spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]),
                        &part_rows,
                        &key(1),
                    )
                    .unwrap(),
                    &key(1),
                )
                .unwrap()
            })
            .collect();
        let deletes = SegmentDeletes::new().with_deleted((0..rows_count as u64).step_by(5));
        let filters = [
            SearchFilter {
                room_ids: Some(vec!["!room0:secret.example".to_owned()]),
                ..SearchFilter::default()
            },
            SearchFilter {
                senders: Some(vec![
                    "@user0:secret.example".to_owned(),
                    "@user1:secret.example".to_owned(),
                ]),
                time_range: Some((1_000, 9_000)),
                ..SearchFilter::default()
            },
            // Rare: 1 row in 50 survives, so the post-filter widens and finally pre-filters.
            SearchFilter {
                senders: Some(vec!["@user9:secret.example".to_owned()]),
                ..SearchFilter::default()
            },
            SearchFilter {
                senders: Some(vec!["@nobody".to_owned()]),
                ..SearchFilter::default()
            },
        ];
        for (index, filter) in filters.into_iter().enumerate() {
            let query = unit(424_242 + index as u64, dimension);
            let search = VectorSearch {
                filter,
                k: 10,
                model: &model,
                oversample: 0,
                query: &query,
            };
            let run = |strategy| {
                search_segments_indexed(
                    &segments,
                    &[Some(&deletes), None],
                    &[],
                    &search,
                    SearchOptions {
                        filter_strategy: strategy,
                        ..SearchOptions::default()
                    },
                )
                .unwrap()
            };
            let (pre, pre_stats) = run(FilterStrategy::PreFilter);
            let (post, post_stats) = run(FilterStrategy::PostFilter);
            let (adaptive, adaptive_stats) = run(FilterStrategy::Adaptive);
            assert_eq!(post, pre, "{metric:?} filter {index}");
            assert_eq!(adaptive, pre, "{metric:?} filter {index}");
            assert_eq!(pre_stats.segments_post_filtered, 0);
            assert!(pre.iter().all(|hit| hit.segment != 0 || !deletes.is_deleted(hit.row)));
            match index {
                0 | 1 => {
                    assert_eq!(pre.len(), 10);
                    assert_eq!(post_stats.segments_post_filtered, 2);
                    assert_eq!(adaptive_stats.segments_post_filtered, 2, "stats keep every granule");
                }
                2 => {
                    assert_eq!(pre.len(), 10);
                    assert!(pre.iter().all(|hit| hit.row % 50 == 0));
                }
                _ => assert!(pre.is_empty()),
            }
        }
    }
}

/// Default settings (`DEFAULT_OVERSAMPLE` as the rerank patience) on tight clusters written in row order, where every
/// member of the query's cluster has a similar RaBitQ upper bound: recall@10 stays at least 0.95 and the rerank set
/// stays near the cluster size.
#[test]
fn default_settings_keep_recall_on_tight_clusters() {
    let k = 10;
    for (dimension, noise) in [(64u32, 0.5f32), (64, 0.2), (32, 0.3), (128, 0.2)] {
        let vectors = clustered_in_order(64, 125, dimension, noise);
        for metric in [VectorMetric::Cosine, VectorMetric::L2] {
            let model = model(dimension, metric);
            for coarse in [VectorElement::U1, VectorElement::I8] {
                let bytes = write_segment(
                    &spec(model.clone(), vec![coarse, VectorElement::F32]),
                    &vectors_input(&vectors),
                    &key(1),
                )
                .unwrap();
                let segments = vec![SegmentReader::open(bytes, &key(1)).unwrap()];
                let queries = 30;
                let (mut found, mut reranked) = (0usize, 0u64);
                for query_index in 0..queries {
                    let centre = unit(5_000_000 + (query_index * 13 % 64) as u64, dimension);
                    let direction = unit(777_000 + query_index as u64, dimension);
                    let mut query: Vec<f32> = centre.iter().zip(&direction).map(|(c, d)| c + noise * d).collect();
                    let norm = query.iter().map(|v| v * v).sum::<f32>().sqrt();
                    query.iter_mut().for_each(|v| *v /= norm);
                    let search = VectorSearch {
                        filter: SearchFilter::default(),
                        k,
                        model: &model,
                        oversample: DEFAULT_OVERSAMPLE,
                        query: &query,
                    };
                    let truth = exact_top(&vectors, &query, metric, k);
                    let (hits, stats) = search_segments_with_stats(&segments, &[], &search).unwrap();
                    found += hits.iter().filter(|hit| truth.contains(&hit.row)).count();
                    reranked += stats.rows_reranked;
                }
                let recall = found as f64 / (queries * k) as f64;
                let mean = reranked as f64 / queries as f64;
                println!(
                    "d{dimension} noise {noise} {metric:?} {coarse:?}: recall@{k} {recall:.3}, reranked {mean:.1}"
                );
                assert!(recall >= 0.95, "d{dimension} {metric:?} {coarse:?} recall {recall}");
                assert!(
                    mean <= 200.0,
                    "d{dimension} {metric:?} {coarse:?} reranked {mean} of 8000"
                );
            }
        }
    }
}

/// Small dimensions: the writer default picks I8 coarse codes (RaBitQ's bound is too loose there), which keep
/// recall and a small rerank set; an explicit U1 coarse code still works, reranking more.
#[test]
fn small_dimensions_default_to_int8_coarse_codes() {
    let k = 10;
    let dimension = 16;
    assert_eq!(
        default_representations(&model(dimension, VectorMetric::Cosine)),
        vec![VectorElement::I8, VectorElement::F32]
    );
    assert_eq!(
        default_representations(&model(RABITQ_MIN_DIMENSION, VectorMetric::Cosine)),
        vec![VectorElement::U1, VectorElement::F32]
    );
    let vectors = clustered_in_order(64, 125, dimension, 0.3);
    for metric in [VectorMetric::Cosine, VectorMetric::L2] {
        let model = model(dimension, metric);
        let mut measured = Vec::new();
        for segment_spec in [
            SegmentSpec::new(model.clone(), "DEVICE", None),
            spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]),
        ] {
            let segments = vec![
                SegmentReader::open(
                    write_segment(&segment_spec, &vectors_input(&vectors), &key(1)).unwrap(),
                    &key(1),
                )
                .unwrap(),
            ];
            let queries = 30;
            let (mut found, mut reranked) = (0usize, 0u64);
            for query_index in 0..queries {
                let centre = unit(5_000_000 + (query_index * 13 % 64) as u64, dimension);
                let direction = unit(777_000 + query_index as u64, dimension);
                let mut query: Vec<f32> = centre.iter().zip(&direction).map(|(c, d)| c + 0.3 * d).collect();
                let norm = query.iter().map(|v| v * v).sum::<f32>().sqrt();
                query.iter_mut().for_each(|v| *v /= norm);
                let search = VectorSearch {
                    filter: SearchFilter::default(),
                    k,
                    model: &model,
                    oversample: DEFAULT_OVERSAMPLE,
                    query: &query,
                };
                let truth = exact_top(&vectors, &query, metric, k);
                let (hits, stats) = search_segments_with_stats(&segments, &[], &search).unwrap();
                found += hits.iter().filter(|hit| truth.contains(&hit.row)).count();
                reranked += stats.rows_reranked;
            }
            let recall = found as f64 / (queries * k) as f64;
            let mean = reranked as f64 / queries as f64;
            println!(
                "d{dimension} {metric:?} {:?}: recall@{k} {recall:.3}, reranked {mean:.1}",
                segment_spec.representations[0]
            );
            measured.push((recall, mean));
        }
        let (default, rabitq) = (measured[0], measured[1]);
        assert_eq!(default.0, 1.0, "{metric:?} default recall");
        assert!(rabitq.0 >= 0.95, "{metric:?} U1 recall {}", rabitq.0);
        assert!(
            default.1 * 5.0 < rabitq.1,
            "{metric:?}: I8 reranks {} vs U1 {}",
            default.1,
            rabitq.1
        );
    }
}

fn shuffled(mut rows: Vec<Vec<f32>>, seed: u64) -> Vec<Vec<f32>> {
    let mut state = seed | 1;
    for index in (1..rows.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        rows.swap(index, (state % (index as u64 + 1)) as usize);
    }
    rows
}

/// Blocks opened, partitions probed and recall for scan and indexed search over one segment.
struct Measured {
    scan_blocks: f64,
    index_blocks: f64,
    probed: f64,
    recall: f64,
    partitions: usize,
    skipped: f64,
}

fn measure(vectors: &[Vec<f32>], segment: &SegmentReader<Vec<u8>>, clusters: usize, metric: VectorMetric) -> Measured {
    let dimension = segment.manifest().model.dimension;
    let model = segment.manifest().model.clone();
    let index = PartitionIndex::build(segment, &PartitionParams::default()).unwrap();
    let all = segment.read_all().unwrap().vectors;
    let segments = std::slice::from_ref(segment);
    let queries = 30;
    let (mut scan_blocks, mut index_blocks, mut probed, mut found, mut skipped) = (0u64, 0u64, 0u64, 0usize, 0u64);
    for query_index in 0..queries {
        let centre = unit(5_000_000 + (query_index * 13 % clusters) as u64, dimension);
        let direction = unit(777_000 + query_index as u64, dimension);
        let mut query: Vec<f32> = centre.iter().zip(&direction).map(|(c, d)| c + 0.4 * d).collect();
        let norm = query.iter().map(|v| v * v).sum::<f32>().sqrt();
        query.iter_mut().for_each(|v| *v /= norm);
        let search = VectorSearch {
            filter: SearchFilter::default(),
            k: 10,
            model: &model,
            oversample: 0,
            query: &query,
        };
        let before = segment.blocks_opened();
        let (scanned, scan_stats) = search_segments_with_stats(segments, &[], &search).unwrap();
        skipped += scan_stats.subblocks_skipped;
        let middle = segment.blocks_opened();
        let (indexed, stats) =
            search_segments_indexed(segments, &[], &[Some(&index)], &search, SearchOptions::default()).unwrap();
        let after = segment.blocks_opened();
        assert_eq!(
            indexed.iter().map(|hit| hit.row).collect::<Vec<_>>(),
            scanned.iter().map(|hit| hit.row).collect::<Vec<_>>(),
            "query {query_index}: the index changes the answer"
        );
        scan_blocks += middle - before;
        index_blocks += after - middle;
        probed += stats.partitions_probed;
        let truth: Vec<f32> = {
            let mut scores: Vec<f32> = vectors.iter().map(|row| score(metric, row, &query)).collect();
            scores.sort_by(|a, b| b.total_cmp(a));
            scores.truncate(10);
            scores
        };
        found += indexed
            .iter()
            .filter(|hit| score(metric, &all[hit.row as usize], &query) >= truth[9] - 1e-5)
            .count();
    }
    let n = queries as f64;
    Measured {
        scan_blocks: scan_blocks as f64 / n,
        index_blocks: index_blocks as f64 / n,
        probed: probed as f64 / n,
        recall: found as f64 / (n * 10.0),
        partitions: index.partitions.len(),
        skipped: skipped as f64 / n,
    }
}

/// 200 natural clusters with ⌈√8000⌉ = 90 partitions, rows in cluster order and shuffled (time-ordered): the
/// indexed search never opens more blocks than the scan and gives the same answer.
#[test]
fn many_natural_clusters_probe_incrementally() {
    let dimension = 64;
    let ordered = clustered_in_order(200, 40, dimension, 0.5);
    for metric in [VectorMetric::Cosine, VectorMetric::L2] {
        let model = model(dimension, metric);
        for (layout, vectors) in [
            ("cluster order", ordered.clone()),
            ("time order", shuffled(ordered.clone(), 11)),
        ] {
            let segment = SegmentReader::open(
                write_segment(
                    &spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]),
                    &vectors_input(&vectors),
                    &key(1),
                )
                .unwrap(),
                &key(1),
            )
            .unwrap();
            let m = measure(&vectors, &segment, 200, metric);
            println!(
                "{metric:?} {layout}: blocks per query scan {:.1} indexed {:.1}; probed {:.1}/{}; recall {:.3}",
                m.scan_blocks, m.index_blocks, m.probed, m.partitions, m.recall
            );
            assert_eq!(m.recall, 1.0);
            assert!(
                m.index_blocks <= m.scan_blocks,
                "{metric:?} {layout}: indexed opens more blocks"
            );
        }
    }
}

/// Time-ordered rows (every sub-block mixes topics) merged with `merge_segments_clustered`: the plain scan skips
/// most sub-blocks by their ball bounds and the indexed search opens a fraction of the blocks, with the same
/// answer by event id as the time-ordered segment.
#[test]
fn a_clustered_merge_cuts_blocks_opened_on_time_ordered_segments() {
    let dimension = 64;
    let vectors = shuffled(clustered_in_order(50, 160, dimension, 0.5), 11);
    for metric in [VectorMetric::Cosine, VectorMetric::L2] {
        let model = model(dimension, metric);
        let segment_spec = spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]);
        let halves: Vec<SegmentReader<Vec<u8>>> = vectors
            .chunks(4000)
            .enumerate()
            .map(|(half, rows)| {
                let mut input = vectors_input(rows);
                input.event_ids = (0..rows.len()).map(|row| format!("$h{half}r{row}")).collect();
                SegmentReader::open(write_segment(&segment_spec, &input, &key(1)).unwrap(), &key(1)).unwrap()
            })
            .collect();
        let time = SegmentReader::open(merge_segments(&halves, &segment_spec, &key(1)).unwrap(), &key(1)).unwrap();
        let clustered = SegmentReader::open(
            merge_segments_clustered(&halves, &[], &segment_spec, &key(1), &PartitionParams::default()).unwrap(),
            &key(1),
        )
        .unwrap();
        assert_eq!(clustered.row_count(), 8000);
        let (before, after) = (time.read_all().unwrap(), clustered.read_all().unwrap());
        let by_id: std::collections::HashMap<&String, &Vec<f32>> =
            before.event_ids.iter().zip(&before.vectors).collect();
        assert!(
            after
                .event_ids
                .iter()
                .zip(&after.vectors)
                .all(|(id, vector)| by_id[id] == vector)
        );
        assert_ne!(after.event_ids, before.event_ids);

        let time_measured = measure(&vectors, &time, 50, metric);
        let clustered_measured = measure(&vectors, &clustered, 50, metric);
        for (layout, m) in [("time order", &time_measured), ("clustered merge", &clustered_measured)] {
            println!(
                "{metric:?} {layout}: blocks per query scan {:.1} (sub-blocks skipped {:.1}) indexed {:.1}; \
                 probed {:.1}/{}; recall {:.3}",
                m.scan_blocks, m.skipped, m.index_blocks, m.probed, m.partitions, m.recall
            );
            assert_eq!(m.recall, 1.0, "{metric:?} {layout}");
            assert!(m.index_blocks <= m.scan_blocks, "{metric:?} {layout}");
        }
        assert!(
            clustered_measured.index_blocks * 3.0 < time_measured.index_blocks,
            "{metric:?} indexed"
        );
        assert!(
            clustered_measured.scan_blocks * 2.0 < time_measured.scan_blocks,
            "{metric:?} scan"
        );

        // The same queries answer with the same event ids on both layouts.
        let query = vectors[17].clone();
        let search = VectorSearch {
            filter: SearchFilter::default(),
            k: 10,
            model: &model,
            oversample: DEFAULT_OVERSAMPLE,
            query: &query,
        };
        let ids = |segment: &SegmentReader<Vec<u8>>| {
            search_segments(std::slice::from_ref(segment), &search)
                .unwrap()
                .into_iter()
                .map(|hit| hit.event_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&clustered), ids(&time));
    }
}

/// Segments written before the sub-block ball footer tail still open and answer identically, only without skipping.
#[test]
fn segments_without_sub_block_balls_still_read_and_answer_the_same() {
    let dimension = 64;
    let vectors = clustered_in_order(50, 160, dimension, 0.5);
    let model = model(dimension, VectorMetric::Cosine);
    let bytes = write_segment(
        &spec(model.clone(), vec![VectorElement::U1, VectorElement::F32]),
        &vectors_input(&vectors),
        &key(1),
    )
    .unwrap();
    let stripped = super::super::segment::strip_subblock_balls(&bytes, &key(1)).unwrap();
    assert!(stripped.len() < bytes.len());
    let with = SegmentReader::open(bytes, &key(1)).unwrap();
    let without = SegmentReader::open(stripped, &key(1)).unwrap();
    assert!(with.has_subblock_balls() && !without.has_subblock_balls());
    without.verify_content().unwrap();
    for query_index in 0..10u64 {
        let query = vectors[(query_index * 797 % 8000) as usize].clone();
        let search = VectorSearch {
            filter: SearchFilter::default(),
            k: 10,
            model: &model,
            oversample: DEFAULT_OVERSAMPLE,
            query: &query,
        };
        let (a, with_stats) = search_segments_with_stats(std::slice::from_ref(&with), &[], &search).unwrap();
        let (b, without_stats) = search_segments_with_stats(std::slice::from_ref(&without), &[], &search).unwrap();
        assert_eq!(a, b);
        assert!(with_stats.subblocks_skipped > 0);
        assert_eq!(without_stats.subblocks_skipped, 0);
    }
}
