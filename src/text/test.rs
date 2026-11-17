use super::*;
use crate::indexes::text_token::tokenize;
use crate::object_store::ObjectStore;
use crate::object_store::sim::SimObjectStore;
use crate::vector::{RrfParams, SearchHit, SegmentDeletes, WrappingKey, hybrid_search};
use std::collections::HashMap;

const WORDS: &[&str] = &[
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "mu", "nu", "xi",
    "omicron", "pi", "rho", "sigma", "tau", "upsilon",
];

fn corpus(seed: u64, docs: usize, prefix: &str) -> Vec<(String, String)> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    (0..docs)
        .map(|doc| {
            let length = 1 + (next() % 30) as usize;
            let text: Vec<&str> = (0..length)
                .map(|_| {
                    // Skewed so early words are common and late ones rare.
                    let pick = (next() % 400) as usize;
                    let index = (pick * pick / 8000).min(WORDS.len() - 1);
                    WORDS[index]
                })
                .collect();
            (
                format!("{prefix}-{doc:05}"),
                text.join(if doc % 3 == 0 { ", " } else { " " }),
            )
        })
        .collect()
}

fn build(segment_id: u128, docs: &[(String, String)]) -> Bm25Index {
    let input: Vec<TextDocument<'_>> = docs
        .iter()
        .map(|(event_id, text)| TextDocument { event_id, text })
        .collect();
    Bm25Index::build(segment_id, [segment_id as u8; 32], &input).unwrap()
}

/// Straightforward BM25 over raw texts, independent of the index.
fn naive(
    segments: &[(&[(String, String)], Option<&SegmentDeletes>)],
    query: &str,
    params: Bm25Params,
) -> Vec<(String, f64)> {
    let tokenised: Vec<Vec<Vec<String>>> = segments
        .iter()
        .map(|(docs, _)| {
            docs.iter()
                .map(|(_, text)| tokenize(text).map(|token| token.into_owned()).collect())
                .collect()
        })
        .collect();
    let all: Vec<&Vec<String>> = tokenised.iter().flatten().collect();
    let n = all.len() as f64;
    let avgdl = all.iter().map(|doc| doc.len()).sum::<usize>() as f64 / n;
    let terms = query_terms(query);
    let mut out = Vec::new();
    for ((docs, deletes), tokens) in segments.iter().zip(&tokenised) {
        for (row, ((event_id, _), doc)) in docs.iter().zip(tokens).enumerate() {
            if deletes.is_some_and(|deletes| deletes.is_deleted(row as u64)) {
                continue;
            }
            let mut score = 0.0;
            let mut matched = false;
            for term in &terms {
                let tf = doc.iter().filter(|token| *token == term).count() as f64;
                if tf == 0.0 {
                    continue;
                }
                matched = true;
                let df = all.iter().filter(|doc| doc.contains(term)).count() as f64;
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                let dl = doc.len() as f64;
                score += idf * tf * (params.k1 + 1.0) / (tf + params.k1 * (1.0 - params.b + params.b * dl / avgdl));
            }
            if matched {
                out.push((event_id.clone(), score));
            }
        }
    }
    out.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    out
}

fn assert_matches_naive(hits: &[TextHit], expected: &[(String, f64)], k: usize) {
    assert_eq!(hits.len(), expected.len().min(k));
    for (hit, (event_id, score)) in hits.iter().zip(expected) {
        assert!(
            (hit.score - score).abs() < 1e-9,
            "{} {} vs {} {}",
            hit.event_id,
            hit.score,
            event_id,
            score
        );
    }
    // Float ties may order differently, so check each returned id has its reference score.
    let reference: HashMap<&str, f64> = expected.iter().map(|(id, score)| (id.as_str(), *score)).collect();
    for hit in hits {
        let score = reference.get(hit.event_id.as_str()).copied().unwrap_or(f64::NAN);
        assert!((hit.score - score).abs() < 1e-9, "{} not in reference", hit.event_id);
    }
}

#[test]
fn bm25_scores_match_naive_reference() {
    let docs = corpus(1, 700, "a");
    let index = build(1, &docs);
    for params in [
        Bm25Params::default(),
        Bm25Params { k1: 2.0, b: 0.3 },
        Bm25Params { k1: 0.9, b: 1.0 },
    ] {
        for query in [
            "alpha",
            "theta kappa",
            "upsilon sigma beta",
            "Gamma, DELTA!",
            "missing",
            "alpha alpha mu",
        ] {
            let segments = [TextSegment {
                index: &index,
                deletes: None,
            }];
            let expected = naive(&[(&docs, None)], query, params);
            for k in [1, 10, 1000] {
                assert_matches_naive(&search_bm25(&segments, query, k, params), &expected, k);
                assert_matches_naive(&search_bm25_exhaustive(&segments, query, k, params), &expected, k);
            }
        }
    }
}

#[test]
fn wand_equals_exhaustive() {
    let first = corpus(2, 1500, "a");
    let second = corpus(3, 900, "b");
    let (a, b) = (build(1, &first), build(2, &second));
    let deletes = SegmentDeletes::new().with_deleted((0..900).step_by(7));
    let segments = [
        TextSegment {
            index: &a,
            deletes: None,
        },
        TextSegment {
            index: &b,
            deletes: Some(&deletes),
        },
    ];
    for query in [
        "alpha",
        "alpha beta",
        "kappa lambda mu nu",
        "upsilon tau alpha",
        "omicron pi rho sigma",
    ] {
        for k in [1, 3, 10, 50, 5000] {
            let params = Bm25Params::default();
            assert_eq!(
                search_bm25(&segments, query, k, params),
                search_bm25_exhaustive(&segments, query, k, params),
                "{query} k={k}"
            );
        }
    }
}

#[test]
fn multi_segment_stats_are_global() {
    let first = corpus(4, 300, "a");
    let second = corpus(5, 500, "b");
    let (a, b) = (build(1, &first), build(2, &second));
    let segments = [
        TextSegment {
            index: &a,
            deletes: None,
        },
        TextSegment {
            index: &b,
            deletes: None,
        },
    ];
    let query = "iota theta beta";
    let expected = naive(&[(&first, None), (&second, None)], query, Bm25Params::default());
    let hits = search_bm25(&segments, query, 25, Bm25Params::default());
    assert_matches_naive(&hits, &expected, 25);
    // The same corpus as one segment ranks identically.
    let merged: Vec<(String, String)> = first.iter().chain(&second).cloned().collect();
    let single = build(3, &merged);
    let one = search_bm25(
        &[TextSegment {
            index: &single,
            deletes: None,
        }],
        query,
        25,
        Bm25Params::default(),
    );
    let ids = |hits: &[TextHit]| {
        hits.iter()
            .map(|hit| (hit.event_id.clone(), hit.score))
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&hits), ids(&one));
}

#[test]
fn deletes_are_honoured() {
    let docs = corpus(6, 400, "a");
    let index = build(1, &docs);
    let query = "zeta eta";
    let before = search_bm25(
        &[TextSegment {
            index: &index,
            deletes: None,
        }],
        query,
        5,
        Bm25Params::default(),
    );
    let deletes = SegmentDeletes::new().with_deleted(before.iter().map(|hit| hit.row));
    let segments = [TextSegment {
        index: &index,
        deletes: Some(&deletes),
    }];
    let after = search_bm25(&segments, query, 400, Bm25Params::default());
    assert!(after.iter().all(|hit| !deletes.is_deleted(hit.row)));
    assert!(
        before
            .iter()
            .all(|gone| after.iter().all(|hit| hit.event_id != gone.event_id))
    );
    assert_matches_naive(
        &after,
        &naive(&[(&docs, Some(&deletes))], query, Bm25Params::default()),
        400,
    );
}

#[test]
fn encoding_is_deterministic_and_round_trips() {
    let docs = corpus(7, 600, "a");
    let index = build(9, &docs);
    let key = WrappingKey::new([5u8; 32]);
    for key in [None, Some(&key)] {
        let bytes = index.encode(key).unwrap();
        assert_eq!(bytes, build(9, &docs).encode(key).unwrap());
        assert_eq!(Bm25Index::decode(&bytes, key).unwrap(), index);
    }
    let sealed = index.encode(Some(&key)).unwrap();
    assert!(!sealed.windows(5).any(|window| window == b"alpha"));
    assert!(index.encode(None).unwrap().windows(5).any(|window| window == b"alpha"));
}

#[test]
fn tampering_and_wrong_keys_are_rejected() {
    let docs = corpus(8, 200, "a");
    let index = build(4, &docs);
    let key = WrappingKey::new([1u8; 32]);
    for key in [None, Some(&key)] {
        let bytes = index.encode(key).unwrap();
        for position in [0, 10, 60, bytes.len() / 2, bytes.len() - 1] {
            let mut tampered = bytes.clone();
            tampered[position] ^= 0x40;
            assert!(Bm25Index::decode(&tampered, key).is_err(), "byte {position}");
        }
        assert!(Bm25Index::decode(&bytes[..bytes.len() - 1], key).is_err());
    }
    let sealed = index.encode(Some(&key)).unwrap();
    assert!(Bm25Index::decode(&sealed, None).is_err());
    assert!(Bm25Index::decode(&sealed, Some(&WrappingKey::new([2u8; 32]))).is_err());
}

#[test]
fn catalogue_publishes_and_binds_to_content() {
    let store = SimObjectStore::new();
    let docs = corpus(9, 150, "a");
    let index = build(7, &docs);
    let key = WrappingKey::new([3u8; 32]);
    let reference = publish_text_index(&store, &index, Some(&key)).unwrap();
    assert_eq!(publish_text_index(&store, &index, Some(&key)).unwrap(), reference);
    assert!(reference.sealed);
    let loaded = load_text_index(&store, &reference, 7, &index.content_hash, Some(&key)).unwrap();
    assert_eq!(loaded, index);
    // Other segment content is refused.
    assert!(matches!(
        load_text_index(&store, &reference, 7, &[0u8; 32], Some(&key)),
        Err(TextIndexError::Mismatch { .. })
    ));
    assert!(load_text_index(&store, &reference, 8, &index.content_hash, Some(&key)).is_err());
    // A forged reference that lies about content is caught by the artifact's own binding.
    let mut forged = reference.clone();
    forged.content_hash = [0u8; 32];
    assert!(load_text_index(&store, &forged, 7, &[0u8; 32], Some(&key)).is_err());
    // Corrupted object bytes are caught by the BLAKE3 in the reference.
    let mut bytes = store.get(&reference.object_key).unwrap().unwrap().bytes.to_vec();
    bytes[70] ^= 1;
    store.delete(&reference.object_key).unwrap();
    store.put_if_absent(&reference.object_key, &bytes).unwrap();
    assert!(matches!(
        load_text_index(&store, &reference, 7, &index.content_hash, Some(&key)),
        Err(TextIndexError::Authentication { .. })
    ));
}

#[test]
fn postings_span_many_blocks() {
    let docs: Vec<(String, String)> = (0..2000)
        .map(|doc| {
            let text = match doc % 5 {
                0 => "common rare".to_owned(),
                _ => format!("common {}", "filler ".repeat(doc % 11)),
            };
            (format!("e{doc}"), text)
        })
        .collect();
    let index = build(1, &docs);
    assert_eq!(index.doc_freq("common"), 2000);
    let decoded = Bm25Index::decode(&index.encode(None).unwrap(), None).unwrap();
    let segments = [TextSegment {
        index: &decoded,
        deletes: None,
    }];
    let expected = naive(&[(&docs, None)], "rare common", Bm25Params::default());
    let hits = search_bm25(&segments, "rare common", 20, Bm25Params::default());
    assert_matches_naive(&hits, &expected, 20);
    assert_eq!(
        hits,
        search_bm25_exhaustive(&segments, "rare common", 20, Bm25Params::default())
    );
}

#[test]
fn degenerate_queries_return_nothing() {
    let docs = corpus(10, 20, "a");
    let index = build(1, &docs);
    let segments = [TextSegment {
        index: &index,
        deletes: None,
    }];
    assert!(search_bm25(&segments, "", 10, Bm25Params::default()).is_empty());
    assert!(search_bm25(&segments, "alpha", 0, Bm25Params::default()).is_empty());
    assert!(search_bm25(&segments, "alpha", 10, Bm25Params { k1: -1.0, b: 0.5 }).is_empty());
    assert!(search_bm25(&[], "alpha", 10, Bm25Params::default()).is_empty());
    let empty = build(2, &[]);
    let decoded = Bm25Index::decode(&empty.encode(None).unwrap(), None).unwrap();
    assert_eq!(decoded.doc_count(), 0);
}

#[test]
fn hybrid_search_fuses_vector_and_bm25() {
    let docs: Vec<(String, String)> = vec![
        ("$a".to_owned(), "the quarterly budget review meeting".to_owned()),
        ("$b".to_owned(), "lunch plans for friday".to_owned()),
        ("$c".to_owned(), "budget budget budget approval".to_owned()),
        ("$d".to_owned(), "weekend hiking trip".to_owned()),
    ];
    let index = build(1, &docs);
    let segments = [TextSegment {
        index: &index,
        deletes: None,
    }];
    let vector = [
        SearchHit {
            event_id: "$a".to_owned(),
            row: 0,
            score: 0.9,
            segment: 0,
        },
        SearchHit {
            event_id: "$d".to_owned(),
            row: 3,
            score: 0.5,
            segment: 0,
        },
    ];
    let fused = hybrid_search(
        &vector,
        &segments,
        "budget",
        10,
        Bm25Params::default(),
        RrfParams::default(),
    );
    let ids: Vec<&str> = fused.iter().map(|hit| hit.event_id.as_str()).collect();
    assert_eq!(ids, ["$a", "$c", "$d"]);
    assert_eq!(fused[0].ranks, [Some(1), Some(2)]);
    assert_eq!(fused[1].ranks, [None, Some(1)]);
}
