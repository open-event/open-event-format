#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

use criterion::{Criterion, criterion_group, criterion_main};
use oe::indexes::bitmap::RoaringRangeBitmap;
use oe::vector::*;
use std::hint::black_box;

const ROWS: usize = 100_000;
const DIMENSION: u32 = 384;

fn unit(state: &mut u64) -> Vec<f32> {
    let raw: Vec<f32> = (0..DIMENSION)
        .map(|_| {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            ((*state >> 40) as f32 / (1u64 << 24) as f32) - 0.5
        })
        .collect();
    let norm = raw.iter().map(|value| value * value).sum::<f32>().sqrt();
    raw.into_iter().map(|value| value / norm).collect()
}

fn scans(c: &mut Criterion) {
    let mut state = 0x2545_F491_4F6C_DD1D;
    let model = EmbeddingModel {
        dimension: DIMENSION,
        digest: [0; 32],
        id: "bench".to_owned(),
        metric: VectorMetric::Cosine,
    };
    let input = SegmentInput {
        event_ids: (0..ROWS).map(|row| format!("$e{row}")).collect(),
        integer_columns: Vec::new(),
        string_columns: vec![(
            ROOM_ID_COLUMN.to_owned(),
            (0..ROWS).map(|row| format!("!r{}", row % 10)).collect(),
        )],
        vectors: (0..ROWS).map(|_| unit(&mut state)).collect(),
    };
    let spec = SegmentSpec {
        model: model.clone(),
        producer_device: "bench".to_owned(),
        representations: vec![VectorElement::U1, VectorElement::I8],
        room_id: None,
    };
    let key = WrappingKey::new([1; 32]);
    let reader = SegmentReader::open(write_segment(&spec, &input, &key).unwrap(), &key).unwrap();
    let query = unit(&mut state);
    let room = RoaringRangeBitmap::from_rows((0..ROWS as u64).filter(|row| row % 10 == 3));
    let mut group = c.benchmark_group("vector_scan_100k_x384");
    for element in [VectorElement::U1, VectorElement::I8] {
        let column = representation_column_id(element);
        group.bench_function(format!("{element:?}/all"), |b| {
            b.iter(|| black_box(reader.top_k(column, &query, &model, None, 100).unwrap()))
        });
        group.bench_function(format!("{element:?}/one_room"), |b| {
            b.iter(|| black_box(reader.top_k(column, &query, &model, Some(&room), 100).unwrap()))
        });
    }
    group.finish();
}

/// Per-row kernels (`PreparedQuery::score`): per-dimension I8 and F32, dot and L2, over 10,000 encoded rows at
/// d = 768; and the RaBitQ estimator end to end (a version 4 U1 + F32 segment: bound-driven coarse scan + rerank),
/// scanned and through its partition index.
fn kernels(c: &mut Criterion) {
    const KERNEL_ROWS: usize = 10_000;
    const KERNEL_DIMENSION: u32 = 768;
    let mut state = 0x9E37_79B9_7F4A_7C15;
    let unit768 = |state: &mut u64| -> Vec<f32> {
        let raw: Vec<f32> = (0..KERNEL_DIMENSION)
            .map(|_| {
                *state ^= *state << 13;
                *state ^= *state >> 7;
                *state ^= *state << 17;
                ((*state >> 40) as f32 / (1u64 << 24) as f32) - 0.5
            })
            .collect();
        let norm = raw.iter().map(|value| value * value).sum::<f32>().sqrt();
        raw.into_iter().map(|value| value / norm).collect()
    };
    let vectors: Vec<Vec<f32>> = (0..KERNEL_ROWS).map(|_| unit768(&mut state)).collect();
    let query = unit768(&mut state);
    let mut group = c.benchmark_group("vector_kernel_10k_x768");
    for element in [VectorElement::I8, VectorElement::F32] {
        let rows: Vec<Vec<u8>> = vectors
            .iter()
            .map(|vector| {
                let mut out = Vec::new();
                encode_row(element, vector, &mut out);
                out
            })
            .collect();
        for metric in [VectorMetric::Dot, VectorMetric::L2] {
            let prepared = PreparedQuery::new(&query, metric);
            group.bench_function(format!("{element:?}/{metric:?}"), |b| {
                b.iter(|| black_box(rows.iter().map(|row| prepared.score(element, row)).sum::<f32>()))
            });
        }
    }
    group.finish();

    let model = EmbeddingModel {
        dimension: KERNEL_DIMENSION,
        digest: [0; 32],
        id: "bench".to_owned(),
        metric: VectorMetric::Cosine,
    };
    let input = SegmentInput {
        event_ids: (0..KERNEL_ROWS).map(|row| format!("$e{row}")).collect(),
        vectors,
        ..SegmentInput::default()
    };
    let spec = SegmentSpec::new(model.clone(), "bench", None);
    let key = WrappingKey::new([1; 32]);
    let segments = [SegmentReader::open(write_segment(&spec, &input, &key).unwrap(), &key).unwrap()];
    let index = PartitionIndex::build(&segments[0], &PartitionParams::default()).unwrap();
    let search = VectorSearch {
        filter: SearchFilter::default(),
        k: 10,
        model: &model,
        oversample: DEFAULT_OVERSAMPLE,
        query: &query,
    };
    let mut group = c.benchmark_group("vector_rabitq_10k_x768");
    group.bench_function("scan_top10", |b| {
        b.iter(|| black_box(search_segments_with_stats(&segments, &[], &search).unwrap()))
    });
    group.bench_function("indexed_top10", |b| {
        b.iter(|| {
            black_box(
                search_segments_indexed(&segments, &[], &[Some(&index)], &search, SearchOptions::default()).unwrap(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, scans, kernels);
criterion_main!(benches);
