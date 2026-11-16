//! Range search, multivector MaxSim and hybrid RRF.
use super::*;

fn clustered(clusters: usize, members: usize, dimension: u32, noise: f32, seed: u64) -> Vec<Vec<f32>> {
    let mut rows = Vec::with_capacity(clusters * members);
    for cluster in 0..clusters {
        let centre = unit(seed + cluster as u64, dimension);
        for member in 0..members {
            let direction = unit(seed * 7 + (cluster * members + member) as u64 + 99, dimension);
            let mut row: Vec<f32> = centre.iter().zip(&direction).map(|(c, d)| c + noise * d).collect();
            let norm = row.iter().map(|v| v * v).sum::<f32>().sqrt();
            row.iter_mut().for_each(|v| *v /= norm);
            rows.push(row);
        }
    }
    rows
}

fn similarity(metric: VectorMetric, left: &[f32], right: &[f32]) -> f32 {
    match metric {
        VectorMetric::L2 => -left.iter().zip(right).map(|(a, b)| (a - b) * (a - b)).sum::<f32>(),
        _ => left.iter().zip(right).map(|(a, b)| a * b).sum(),
    }
}

fn segment_of(
    vectors: &[Vec<f32>],
    prefix: &str,
    model: &EmbeddingModel,
    coarse: VectorElement,
) -> SegmentReader<Vec<u8>> {
    let mut rows = input(vectors.len(), model.dimension);
    rows.vectors = vectors.to_vec();
    rows.event_ids = (0..vectors.len()).map(|row| format!("{prefix}{row}")).collect();
    let bytes = write_segment(&spec(model.clone(), vec![coarse, VectorElement::F32]), &rows, &key(1)).unwrap();
    SegmentReader::open(bytes, &key(1)).unwrap()
}

#[test]
fn range_search_returns_exactly_the_rows_in_range() {
    let dimension = 128;
    let parts = [
        clustered(20, 60, dimension, 0.6, 1_000),
        clustered(20, 60, dimension, 0.6, 9_000),
    ];
    for metric in [VectorMetric::Dot, VectorMetric::L2] {
        let model = model(dimension, metric);
        for coarse in [VectorElement::I8, VectorElement::U1] {
            let segments: Vec<_> = parts
                .iter()
                .enumerate()
                .map(|(index, vectors)| segment_of(vectors, &format!("$s{index}r"), &model, coarse))
                .collect();
            let indexes: Vec<PartitionIndex> = segments
                .iter()
                .map(|segment| PartitionIndex::build(segment, &PartitionParams::default()).unwrap())
                .collect();
            let (mut expected_total, mut found_total, mut scanned, mut reranked) = (0usize, 0usize, 0u64, 0u64);
            for query_index in 0..12u64 {
                let query = parts[(query_index % 2) as usize][(query_index * 97 % 1200) as usize].clone();
                let mut scores: Vec<f32> = parts
                    .iter()
                    .flat_map(|vectors| vectors.iter().map(|row| similarity(metric, row, &query)))
                    .collect();
                scores.sort_by(|a, b| b.total_cmp(a));
                // A band from the 5th to the 60th best score (excluding the query row itself on top).
                let (low, high) = (scores[60], scores[5]);
                let range = ScoreRange {
                    min: Some(low),
                    max: Some(high),
                };
                let search = VectorSearch {
                    filter: SearchFilter::default(),
                    k: 10,
                    model: &model,
                    oversample: 0,
                    query: &query,
                };
                let truth: std::collections::HashSet<String> = parts
                    .iter()
                    .enumerate()
                    .flat_map(|(segment, vectors)| {
                        let query = &query;
                        vectors.iter().enumerate().filter_map(move |(row, vector)| {
                            let score = similarity(metric, vector, query);
                            // Skip rows on the boundary, where summation order decides.
                            let clear = (score - low).abs() > 1e-4 && (score - high).abs() > 1e-4;
                            (clear && range.contains(score)).then(|| format!("$s{segment}r{row}"))
                        })
                    })
                    .collect();
                let (hits, stats) =
                    search_segments_in_range(&segments, &[], &[], &search, range, None, SearchOptions::default())
                        .unwrap();
                assert!(!stats.range_truncated);
                assert!(hits.iter().all(|hit| range.contains(hit.score)), "a hit out of range");
                assert!(hits.windows(2).all(|pair| pair[0].score >= pair[1].score));
                let ids: std::collections::HashSet<String> = hits.iter().map(|hit| hit.event_id.clone()).collect();
                expected_total += truth.len();
                found_total += truth.iter().filter(|id| ids.contains(*id)).count();
                if coarse == VectorElement::I8 {
                    assert!(
                        truth.iter().all(|id| ids.contains(id)),
                        "{metric:?}: an in-range row was missed"
                    );
                }
                scanned += stats.rows_scanned;
                reranked += stats.rows_reranked;

                let indexed: Vec<&PartitionIndex> = indexes.iter().collect();
                let indexed: Vec<Option<&PartitionIndex>> = indexed.into_iter().map(Some).collect();
                let (with_index, _) =
                    search_segments_in_range(&segments, &[], &indexed, &search, range, None, SearchOptions::default())
                        .unwrap();
                if coarse == VectorElement::I8 {
                    assert_eq!(
                        with_index.iter().map(|hit| &hit.event_id).collect::<Vec<_>>(),
                        hits.iter().map(|hit| &hit.event_id).collect::<Vec<_>>(),
                        "{metric:?}: the partition index changes a range answer"
                    );
                }

                let (limited, _) =
                    search_segments_in_range(&segments, &[], &[], &search, range, Some(7), SearchOptions::default())
                        .unwrap();
                assert_eq!(limited.len(), 7);
                assert_eq!(
                    limited.iter().map(|hit| hit.score).collect::<Vec<_>>(),
                    hits.iter().take(7).map(|hit| hit.score).collect::<Vec<_>>()
                );
            }
            let recall = found_total as f64 / expected_total as f64;
            println!(
                "{metric:?} {coarse:?}: range recall {recall:.4} over {expected_total} rows; scanned {scanned}, \
                 reranked {reranked} over 12 queries of 2400 rows"
            );
            assert!(recall >= 0.99, "{metric:?} {coarse:?}: range recall {recall}");
            if coarse == VectorElement::I8 {
                assert!(
                    reranked < scanned / 4,
                    "{metric:?} {coarse:?}: bounds do not cut the rerank"
                );
            }
        }
    }
}

#[test]
fn an_l2_radius_skips_far_segments_and_rejects_inverted_ranges() {
    let dimension = 32;
    let model = model(dimension, VectorMetric::L2);
    let near = clustered(1, 300, dimension, 0.2, 50);
    let far: Vec<Vec<f32>> = near.iter().map(|row| row.iter().map(|v| -v).collect()).collect();
    let segments = vec![
        segment_of(&near, "$near", &model, VectorElement::U1),
        segment_of(&far, "$far", &model, VectorElement::U1),
    ];
    let query = near[0].clone();
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 10,
        model: &model,
        oversample: 0,
        query: &query,
    };
    let radius = 0.25;
    let (hits, stats) = search_segments_in_range(
        &segments,
        &[],
        &[],
        &search,
        ScoreRange::l2_radius(radius),
        None,
        SearchOptions::default(),
    )
    .unwrap();
    let truth = near
        .iter()
        .filter(|row| -similarity(VectorMetric::L2, row, &query) <= radius * radius - 1e-4)
        .count();
    assert!(
        hits.len() >= truth && truth > 1,
        "{} hits, {truth} in radius",
        hits.len()
    );
    assert!(hits.iter().all(|hit| hit.segment == 0 && -hit.score <= radius * radius));
    assert_eq!(stats.segments_skipped, 1, "{stats:?}");

    let inverted = ScoreRange {
        min: Some(1.0),
        max: Some(0.0),
    };
    assert!(search_segments_in_range(&segments, &[], &[], &search, inverted, None, SearchOptions::default()).is_err());
    // Unit rows are at most 2 apart, so an annulus beyond 2.1 is empty; bounds leave few rows to rerank.
    let (none, stats) = search_segments_in_range(
        &segments,
        &[],
        &[],
        &search,
        ScoreRange::l2_annulus(2.1, 3.0),
        None,
        SearchOptions::default(),
    )
    .unwrap();
    assert!(none.is_empty());
    assert!(stats.rows_reranked < 30, "{stats:?}");
}

fn documents(count: usize, dimension: u32, seed: u64) -> Vec<Vec<Vec<f32>>> {
    let topics = clustered(16, 1, dimension, 0.0, seed);
    (0..count)
        .map(|document| {
            let tokens = 2 + (document * 7 + seed as usize) % 9;
            (0..tokens)
                .map(|token| {
                    let topic = &topics[(document * 3 + token * 5) % topics.len()];
                    let noise = unit(seed * 1_000 + (document * 16 + token) as u64, dimension);
                    let mut row: Vec<f32> = topic.iter().zip(&noise).map(|(t, n)| t + 0.7 * n).collect();
                    let norm = row.iter().map(|v| v * v).sum::<f32>().sqrt();
                    row.iter_mut().for_each(|v| *v /= norm);
                    row
                })
                .collect()
        })
        .collect()
}

fn multivector_segment(
    docs: &[Vec<Vec<f32>>],
    prefix: &str,
    model: &EmbeddingModel,
    coarse: VectorElement,
) -> SegmentReader<Vec<u8>> {
    let input = MultivectorInput {
        event_ids: (0..docs.len()).map(|doc| format!("{prefix}{doc}")).collect(),
        integer_columns: vec![(TIMESTAMP_COLUMN.to_owned(), (0..docs.len() as i64).collect())],
        string_columns: vec![(
            ROOM_ID_COLUMN.to_owned(),
            (0..docs.len()).map(|doc| format!("!room{}", doc % 2)).collect(),
        )],
        documents: docs.to_vec(),
    };
    let bytes =
        write_multivector_segment(&spec(model.clone(), vec![coarse, VectorElement::F32]), &input, &key(1)).unwrap();
    SegmentReader::open(bytes, &key(1)).unwrap()
}

fn brute_max_sim(
    parts: &[Vec<Vec<Vec<f32>>>],
    query: &[Vec<f32>],
    metric: VectorMetric,
    keep: impl Fn(usize, usize) -> bool,
) -> Vec<(String, f32)> {
    let mut all: Vec<(String, f32)> = parts
        .iter()
        .enumerate()
        .flat_map(|(segment, docs)| {
            let keep = &keep;
            docs.iter()
                .enumerate()
                .filter(move |(doc, _)| keep(segment, *doc))
                .map(move |(doc, tokens)| (format!("$m{segment}d{doc}"), max_sim(query, tokens, metric)))
        })
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    all
}

#[test]
fn multivector_search_matches_brute_force_max_sim() {
    let dimension = 128;
    let parts = [documents(400, dimension, 3), documents(300, dimension, 11)];
    for metric in [VectorMetric::Dot, VectorMetric::L2] {
        let model = model(dimension, metric);
        for coarse in [VectorElement::U1, VectorElement::I8] {
            let segments: Vec<_> = parts
                .iter()
                .enumerate()
                .map(|(index, docs)| multivector_segment(docs, &format!("$m{index}d"), &model, coarse))
                .collect();
            let (mut exact_matches, mut reranked, mut tokens) = (0usize, 0u64, 0u64);
            for query_index in 0..10usize {
                // A query near one document: a few of its tokens, slightly perturbed, plus one random token.
                let source = &parts[query_index % 2][query_index * 37 % 300];
                let mut query: Vec<Vec<f32>> = source.iter().take(3).cloned().collect();
                query.push(unit(4_242 + query_index as u64, dimension));
                for candidates in [DEFAULT_TOKEN_CANDIDATES, 3] {
                    let search = MultivectorSearch {
                        token_candidates: candidates,
                        ..MultivectorSearch::new(&model, &query, 10)
                    };
                    let (hits, stats) = search_multivector_segments(&segments, &[], &search).unwrap();
                    let truth = brute_max_sim(&parts, &query, metric, |_, _| true);
                    assert_eq!(hits.len(), 10);
                    for (hit, (id, score)) in hits.iter().zip(&truth) {
                        assert!(
                            (hit.score - score).abs() < 1e-3,
                            "{metric:?} {coarse:?}: {hit:?} vs {id} {score}"
                        );
                    }
                    let same = hits
                        .iter()
                        .zip(&truth)
                        .filter(|(hit, (id, _))| hit.event_id == *id)
                        .count();
                    exact_matches += same;
                    if candidates == DEFAULT_TOKEN_CANDIDATES {
                        reranked += stats.rows_reranked;
                        tokens += stats.rows_scanned;
                    }
                }
            }
            println!(
                "{metric:?} {coarse:?}: {exact_matches}/200 ranks identical to brute force; fine tokens scored \
                 {reranked} of {tokens} scanned"
            );
            assert!(exact_matches >= 196, "{metric:?} {coarse:?}: {exact_matches}/200");
            if coarse == VectorElement::I8 {
                assert!(reranked * 4 < tokens, "{metric:?} {coarse:?}: rerank is not pruned");
            }
        }
    }
}

#[test]
fn multivector_segments_are_version_5_and_honour_filters_and_deletes() {
    let dimension = 16;
    let model = model(dimension, VectorMetric::Cosine);
    let docs = documents(50, dimension, 5);
    let segment = multivector_segment(&docs, "$m0d", &model, VectorElement::U1);
    assert_eq!(segment.version(), SEGMENT_VERSION_MULTIVECTOR);
    assert_eq!(segment.document_count(), 50);
    let total: usize = docs.iter().map(Vec::len).sum();
    assert_eq!(segment.row_count(), total as u64);
    let rows = segment.document_rows(7).unwrap();
    assert_eq!(rows.end - rows.start, docs[7].len() as u64);
    assert_eq!(segment.document_of(rows.start), Some(7));
    assert_eq!(segment.document_of(rows.end - 1), Some(7));
    assert_eq!(
        segment.event_ids(&[rows.start, rows.end - 1]).unwrap(),
        vec!["$m0d7", "$m0d7"]
    );
    assert_eq!(segment.document_of(total as u64), None);

    // A plain segment stays version 4, has no documents, and is refused by multivector search.
    let plain = segment_of(&docs[0], "$p", &model, VectorElement::U1);
    assert_eq!(plain.version(), SEGMENT_VERSION);
    assert!(plain.document_offsets().is_none());
    let query = docs[7].clone();
    let search = MultivectorSearch::new(&model, &query, 5);
    assert!(search_multivector_segments(std::slice::from_ref(&plain), &[], &search).is_err());
    assert!(
        merge_segments(
            std::slice::from_ref(&segment),
            &spec(model.clone(), vec![VectorElement::F32]),
            &key(1)
        )
        .is_err()
    );

    let readers = vec![segment];
    let (hits, _) = search_multivector_segments(&readers, &[], &search).unwrap();
    assert_eq!(hits[0].event_id, "$m0d7");
    assert_eq!(hits[0].document, 7);

    // Deleting a document's token rows removes it.
    let deletes = SegmentDeletes::new().with_deleted(rows.clone());
    let (hits, _) = search_multivector_segments(&readers, &[Some(&deletes)], &search).unwrap();
    assert!(hits.iter().all(|hit| hit.document != 7));
    let truth = brute_max_sim(std::slice::from_ref(&docs), &query, VectorMetric::Cosine, |_, doc| {
        doc != 7
    });
    assert_eq!(hits[0].event_id, truth[0].0);

    // Filters apply to the documents' metadata.
    let filtered = MultivectorSearch {
        filter: SearchFilter {
            room_ids: Some(vec!["!room0".to_owned()]),
            ..SearchFilter::default()
        },
        ..search.clone()
    };
    let (hits, _) = search_multivector_segments(&readers, &[], &filtered).unwrap();
    let truth = brute_max_sim(std::slice::from_ref(&docs), &query, VectorMetric::Cosine, |_, doc| {
        doc % 2 == 0
    });
    assert_eq!(
        hits.iter().map(|hit| hit.event_id.clone()).collect::<Vec<_>>(),
        truth.iter().take(5).map(|(id, _)| id.clone()).collect::<Vec<_>>()
    );

    // A rerank cap still answers (approximately) with k hits.
    let capped = MultivectorSearch {
        max_rerank: Some(5),
        token_candidates: 4,
        ..search.clone()
    };
    assert_eq!(search_multivector_segments(&readers, &[], &capped).unwrap().0.len(), 5);

    // Empty documents and bad queries are rejected.
    let bad = MultivectorInput {
        event_ids: vec!["$e".to_owned()],
        documents: vec![Vec::new()],
        ..MultivectorInput::default()
    };
    assert!(write_multivector_segment(&spec(model.clone(), vec![VectorElement::F32]), &bad, &key(1)).is_err());
    let wrong: Vec<Vec<f32>> = vec![vec![0.0; 3]];
    assert!(search_multivector_segments(&readers, &[], &MultivectorSearch::new(&model, &wrong, 5)).is_err());
}

#[test]
fn rrf_fuses_ranks_with_weights_and_deterministic_ties() {
    let hit = |id: &str, score: f32| SearchHit {
        event_id: id.to_owned(),
        row: 0,
        score,
        segment: 0,
    };
    let vector = vec![hit("$a", 0.9), hit("$b", 0.8), hit("$c", 0.7)];
    let text = ["$c", "$d", "$a"];
    let fused = hybrid_rrf(&vector, &text, RrfParams::default());
    let ids: Vec<&str> = fused.iter().map(|hit| hit.event_id.as_str()).collect();
    // $a: 1/61 + 1/63; $c: 1/63 + 1/61 (tie, broken by best rank 1 then list order: $a is first in vector list).
    assert_eq!(ids, ["$a", "$c", "$b", "$d"]);
    assert!((fused[0].score - (1.0 / 61.0 + 1.0 / 63.0)).abs() < 1e-12);
    assert_eq!(fused[0].score, fused[1].score);
    assert_eq!(fused[0].ranks, vec![Some(1), Some(3)]);
    assert_eq!(fused[3].ranks, vec![None, Some(2)]);

    // $b vs $d tie exactly at rank 2 in different lists: the vector list comes first.
    assert_eq!(fused[2].score, fused[3].score);

    let text_heavy = hybrid_rrf(
        &vector,
        &text,
        RrfParams {
            text_weight: 3.0,
            limit: Some(2),
            ..RrfParams::default()
        },
    );
    assert_eq!(
        text_heavy.iter().map(|hit| hit.event_id.as_str()).collect::<Vec<_>>(),
        ["$c", "$a"]
    );

    // Duplicates count once at their best rank, and the result does not depend on hash order.
    let duplicated = vec![hit("$x", 0.9), hit("$x", 0.8), hit("$y", 0.7)];
    let fused = hybrid_rrf(
        &duplicated,
        &["$y"],
        RrfParams {
            k: 1.0,
            ..RrfParams::default()
        },
    );
    assert_eq!(fused[0].event_id, "$y");
    assert!((fused[0].score - (1.0 / 4.0 + 1.0 / 2.0)).abs() < 1e-12);
    assert!((fused[1].score - 0.5).abs() < 1e-12);
    for _ in 0..5 {
        assert_eq!(
            hybrid_rrf(&duplicated, &["$y"], RrfParams::default()),
            hybrid_rrf(&duplicated, &["$y"], RrfParams::default())
        );
    }
    let three: Vec<&[&str]> = vec![&["$p", "$q"], &["$q", "$p"], &["$r"]];
    let lists: Vec<(&[&str], f64)> = three.iter().map(|list| (*list, 1.0)).collect();
    let fused = reciprocal_rank_fusion(&lists, DEFAULT_RRF_K, None);
    assert_eq!(
        fused.iter().map(|hit| hit.event_id.as_str()).collect::<Vec<_>>(),
        ["$p", "$q", "$r"]
    );
}
