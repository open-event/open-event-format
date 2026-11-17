#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

use criterion::{Criterion, criterion_group, criterion_main};
use oe::text::*;
use std::hint::black_box;

const DOCS: usize = 100_000;
const VOCABULARY: u64 = 20_000;

fn corpus() -> Vec<(String, String)> {
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    (0..DOCS)
        .map(|doc| {
            let length = 5 + next() % 40;
            let words: Vec<String> = (0..length)
                .map(|_| {
                    // Zipf-ish: squaring a uniform draw favours small word ids.
                    let uniform = (next() % VOCABULARY) as f64 / VOCABULARY as f64;
                    format!("w{}", (uniform * uniform * VOCABULARY as f64) as u64)
                })
                .collect();
            (format!("$event{doc}"), words.join(" "))
        })
        .collect()
}

fn bm25(c: &mut Criterion) {
    let docs = corpus();
    let input: Vec<TextDocument<'_>> = docs
        .iter()
        .map(|(event_id, text)| TextDocument { event_id, text })
        .collect();
    c.bench_function("bm25_build_100k", |b| {
        b.iter(|| Bm25Index::build(1, [0; 32], black_box(&input)).unwrap())
    });
    let index = Bm25Index::build(1, [0; 32], &input).unwrap();
    let bytes = index.encode(None).unwrap();
    c.bench_function("bm25_decode_100k", |b| {
        b.iter(|| Bm25Index::decode(black_box(&bytes), None).unwrap())
    });
    let segments = [TextSegment {
        index: &index,
        deletes: None,
    }];
    let params = Bm25Params::default();
    for (name, query) in [("common", "w0 w1 w2"), ("mixed", "w0 w150 w4000"), ("rare", "w9000 w12000")] {
        c.bench_function(&format!("bm25_wand_top10_{name}"), |b| {
            b.iter(|| search_bm25(&segments, black_box(query), 10, params))
        });
        c.bench_function(&format!("bm25_exhaustive_top10_{name}"), |b| {
            b.iter(|| search_bm25_exhaustive(&segments, black_box(query), 10, params))
        });
    }
}

criterion_group!(benches, bm25);
criterion_main!(benches);
