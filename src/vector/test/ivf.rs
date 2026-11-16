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
