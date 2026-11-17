//! Vector search recall and latency: deterministic clustered datasets, the search configurations OE offers, and
//! exact brute force as ground truth.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use anyhow::Result;
use oe::vector::*;
use std::collections::HashSet;
use std::time::Instant;

pub const K: usize = 10;
/// Rooms in every dataset; rows pick one uniformly, so one room keeps ~1% of rows and half the rooms ~50%.
pub const ROOMS: usize = 100;
/// Range queries return every row scoring at least halfway between the query's `RANGE_RANK`-th and next exact score.
pub const RANGE_RANK: usize = 30;
/// Noise norm around a cluster centre (before normalising); 0.5 gives tight but overlapping topics.
const SPREAD: f32 = 0.5;

/// Rerank patience passed as `VectorSearch::oversample` (`--oversample`; the library default unless set).
pub static OVERSAMPLE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(DEFAULT_OVERSAMPLE);

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn uniform(&mut self) -> f32 {
        ((self.next_u64() >> 40) as f32 + 0.5) / (1u64 << 24) as f32
    }

    /// Standard normal (Box–Muller).
    fn gaussian(&mut self) -> f32 {
        let (a, b) = (self.uniform(), self.uniform());
        (-2.0 * a.ln()).sqrt() * (std::f32::consts::TAU * b).cos()
    }
}

pub fn normalised(mut vector: Vec<f32>) -> Vec<f32> {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    vector.iter_mut().for_each(|value| *value /= norm);
    vector
}

fn random_unit(rng: &mut Rng, dimension: usize) -> Vec<f32> {
    normalised((0..dimension).map(|_| rng.gaussian()).collect())
}

fn around(rng: &mut Rng, centre: &[f32]) -> Vec<f32> {
    let scale = SPREAD / (centre.len() as f32).sqrt();
    normalised(centre.iter().map(|value| value + scale * rng.gaussian()).collect())
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// Rows arrive in time order: every topic is mixed into every sub-block.
    Time,
    /// Rows are grouped by topic.
    Cluster,
}

impl Order {
    pub fn name(self) -> &'static str {
        match self {
            Order::Time => "time-ordered",
            Order::Cluster => "cluster-ordered",
        }
    }
}

pub struct Dataset {
    pub dimension: usize,
    pub order: Order,
    pub vectors: Vec<Vec<f32>>,
    pub rooms: Vec<usize>,
    pub queries: Vec<Vec<f32>>,
}

/// `rows` unit vectors around `⌈√rows / 2⌉` random centres (equal seed, equal data whatever the order).
pub fn dataset(rows: usize, dimension: usize, order: Order, queries: usize, seed: u64) -> Dataset {
    let mut rng = Rng::new(seed ^ (rows as u64) << 20 ^ dimension as u64);
    let clusters = (((rows as f64).sqrt() / 2.0).ceil() as usize).max(4);
    let centres: Vec<Vec<f32>> = (0..clusters).map(|_| random_unit(&mut rng, dimension)).collect();
    let mut assigned: Vec<usize> = (0..rows).map(|_| rng.below(clusters)).collect();
    if order == Order::Cluster {
        assigned.sort_unstable();
    }
    let vectors = assigned
        .iter()
        .map(|cluster| around(&mut rng, &centres[*cluster]))
        .collect();
    let rooms = (0..rows).map(|_| rng.below(ROOMS)).collect();
    let queries = (0..queries)
        .map(|_| {
            let cluster = rng.below(clusters);
            around(&mut rng, &centres[cluster])
        })
        .collect();
    Dataset {
        dimension,
        order,
        vectors,
        rooms,
        queries,
    }
}

pub fn model(dimension: usize) -> EmbeddingModel {
    EmbeddingModel {
        dimension: dimension as u32,
        digest: [7; 32],
        id: "bench-vector".to_owned(),
        metric: VectorMetric::Cosine,
    }
}

pub fn room_name(room: usize) -> String {
    format!("!r{room}")
}

fn event_row(id: &str) -> u64 {
    id.trim_start_matches("$e").trim_start_matches("$d").parse().unwrap()
}

pub fn segment_input(data: &Dataset) -> SegmentInput {
    SegmentInput {
        event_ids: (0..data.vectors.len()).map(|row| format!("$e{row}")).collect(),
        integer_columns: vec![(TIMESTAMP_COLUMN.to_owned(), (0..data.vectors.len() as i64).collect())],
        string_columns: vec![(
            ROOM_ID_COLUMN.to_owned(),
            data.rooms.iter().map(|room| room_name(*room)).collect(),
        )],
        vectors: data.vectors.clone(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    TopK,
    /// One room of `ROOMS` (~1% of rows).
    RoomOnePercent,
    /// Half the rooms (~50% of rows).
    RoomHalf,
    /// Every row above a per-query score threshold (~`RANGE_RANK` rows).
    Range,
}

impl Workload {
    pub const ALL: [Workload; 4] = [
        Workload::TopK,
        Workload::RoomOnePercent,
        Workload::RoomHalf,
        Workload::Range,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Workload::TopK => "top-10",
            Workload::RoomOnePercent => "room filter 1%",
            Workload::RoomHalf => "room filter 50%",
            Workload::Range => "range",
        }
    }

    fn rooms(self) -> Option<Vec<usize>> {
        match self {
            Workload::RoomOnePercent => Some(vec![7]),
            Workload::RoomHalf => Some((0..ROOMS / 2).collect()),
            _ => None,
        }
    }

    fn filter(self) -> SearchFilter {
        SearchFilter {
            room_ids: self.rooms().map(|rooms| rooms.into_iter().map(room_name).collect()),
            ..SearchFilter::default()
        }
    }
}

/// Exact answers for one query.
pub struct Truth {
    pub rows: Vec<u64>,
    /// Range threshold (score minimum), for `Workload::Range`.
    pub threshold: f32,
}

/// Brute force over the f32 vectors: the k best rows (passing the filter), or for a range every row above the
/// threshold.
pub fn brute_force(data: &Dataset, query: &[f32], workload: Workload) -> Truth {
    let allowed: Option<HashSet<usize>> = workload.rooms().map(|rooms| rooms.into_iter().collect());
    let mut scored: Vec<(f32, u64)> = data
        .vectors
        .iter()
        .enumerate()
        .filter(|(row, _)| allowed.as_ref().is_none_or(|rooms| rooms.contains(&data.rooms[*row])))
        .map(|(row, vector)| (dot(vector, query), row as u64))
        .collect();
    let wanted = if workload == Workload::Range { RANGE_RANK + 1 } else { K };
    let wanted = wanted.min(scored.len());
    if wanted < scored.len() {
        scored.select_nth_unstable_by(wanted, |a, b| b.0.total_cmp(&a.0));
        scored.truncate(wanted);
    }
    scored.sort_by(|a, b| b.0.total_cmp(&a.0));
    if workload == Workload::Range {
        let threshold = (scored[RANGE_RANK - 1].0 + scored[RANGE_RANK].0) / 2.0;
        return Truth {
            rows: scored[..RANGE_RANK].iter().map(|(_, row)| *row).collect(),
            threshold,
        };
    }
    Truth {
        rows: scored.into_iter().map(|(_, row)| row).collect(),
        threshold: f32::NEG_INFINITY,
    }
}

/// A searchable configuration: a segment, maybe its partition index.
pub struct Config {
    pub name: String,
    pub reader: SegmentReader<Vec<u8>>,
    pub index: Option<PartitionIndex>,
    pub segment_ms: f64,
    pub index_ms: f64,
    pub segment_bytes: usize,
    pub index_bytes: usize,
}

pub fn key() -> WrappingKey {
    WrappingKey::new([0x5A; 32])
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e3
}

pub fn write(data: &Dataset) -> Result<(Vec<u8>, f64)> {
    let spec = SegmentSpec::new(model(data.dimension), "bench", None);
    let input = segment_input(data);
    let start = Instant::now();
    let bytes = write_segment(&spec, &input, &key())?;
    Ok((bytes, ms(start)))
}

pub fn config(name: &str, bytes: Vec<u8>, segment_ms: f64, indexed: bool) -> Result<Config> {
    let segment_bytes = bytes.len();
    let reader = SegmentReader::open(bytes, &key())?;
    let (index, index_ms, index_bytes) = if indexed {
        let start = Instant::now();
        let index = PartitionIndex::build(&reader, &PartitionParams::default())?;
        let took = ms(start);
        let size = index.encode(None)?.len();
        (Some(index), took, size)
    } else {
        (None, 0.0, 0)
    };
    Ok(Config {
        name: name.to_owned(),
        reader,
        index,
        segment_ms,
        index_ms,
        segment_bytes,
        index_bytes,
    })
}

/// Clustered merge of a segment (the compaction a time-ordered store runs), timed as its segment build.
pub fn clustered_merge(source: &[u8], dimension: usize) -> Result<(Vec<u8>, f64)> {
    let reader = SegmentReader::open(source.to_vec(), &key())?;
    let spec = SegmentSpec::new(model(dimension), "bench", None);
    let start = Instant::now();
    let bytes = merge_segments_clustered(&[reader], &[], &spec, &key(), &PartitionParams::default())?;
    Ok((bytes, ms(start)))
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Measurement {
    pub dataset: String,
    pub config: String,
    pub workload: String,
    pub queries: usize,
    pub recall: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub blocks_opened: f64,
    pub rows_reranked: f64,
    pub bytes_per_vector: f64,
    pub index_bytes_per_vector: f64,
    pub segment_build_ms: f64,
    pub index_build_ms: f64,
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let at = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[at]
}

fn summary(
    dataset: &str,
    config: &str,
    workload: Workload,
    mut latencies: Vec<f64>,
    recall: (usize, usize),
    blocks: u64,
    reranked: u64,
) -> Measurement {
    latencies.sort_by(f64::total_cmp);
    let queries = latencies.len().max(1) as f64;
    Measurement {
        dataset: dataset.to_owned(),
        config: config.to_owned(),
        workload: workload.name().to_owned(),
        queries: latencies.len(),
        recall: if recall.1 == 0 {
            1.0
        } else {
            recall.0 as f64 / recall.1 as f64
        },
        p50_ms: percentile(&latencies, 0.5),
        p99_ms: percentile(&latencies, 0.99),
        blocks_opened: blocks as f64 / queries,
        rows_reranked: reranked as f64 / queries,
        bytes_per_vector: 0.0,
        index_bytes_per_vector: 0.0,
        segment_build_ms: 0.0,
        index_build_ms: 0.0,
    }
}

/// Exact brute force timed over the in-memory f32 vectors, and the truth for every query.
pub fn measure_exact(data: &Dataset, label: &str, workload: Workload) -> (Measurement, Vec<Truth>) {
    let mut latencies = Vec::new();
    let mut truths = Vec::new();
    for query in &data.queries {
        let start = Instant::now();
        truths.push(brute_force(data, query, workload));
        latencies.push(ms(start));
    }
    let mut measured = summary(label, "exact (f32 brute force)", workload, latencies, (1, 1), 0, 0);
    measured.bytes_per_vector = (data.dimension * 4) as f64;
    (measured, truths)
}

pub fn measure(
    data: &Dataset,
    label: &str,
    config: &Config,
    workload: Workload,
    truths: &[Truth],
) -> Result<Measurement> {
    let model = model(data.dimension);
    let segments = std::slice::from_ref(&config.reader);
    let indexes = [config.index.as_ref()];
    let (mut latencies, mut found, mut total, mut blocks, mut reranked) = (Vec::new(), 0, 0, 0, 0);
    for (query, truth) in data.queries.iter().zip(truths) {
        let search = VectorSearch {
            filter: workload.filter(),
            k: K,
            model: &model,
            oversample: OVERSAMPLE.load(std::sync::atomic::Ordering::Relaxed),
            query,
        };
        let before = config.reader.blocks_opened();
        let start = Instant::now();
        let (hits, stats) = if workload == Workload::Range {
            let range = ScoreRange {
                min: Some(truth.threshold),
                max: None,
            };
            search_segments_in_range(segments, &[], &indexes, &search, range, None, SearchOptions::default())?
        } else {
            search_segments_indexed(segments, &[], &indexes, &search, SearchOptions::default())?
        };
        latencies.push(ms(start));
        blocks += config.reader.blocks_opened() - before;
        reranked += stats.rows_reranked;
        let got: HashSet<u64> = hits.iter().map(|hit| event_row(&hit.event_id)).collect();
        found += truth.rows.iter().filter(|row| got.contains(row)).count();
        total += truth.rows.len();
    }
    let mut measured = summary(
        label,
        &config.name,
        workload,
        latencies,
        (found, total),
        blocks,
        reranked,
    );
    let rows = data.vectors.len() as f64;
    measured.bytes_per_vector = config.segment_bytes as f64 / rows;
    measured.index_bytes_per_vector = config.index_bytes as f64 / rows;
    measured.segment_build_ms = config.segment_ms;
    measured.index_build_ms = config.index_ms;
    Ok(measured)
}

/// The configurations for one dataset: scan and indexed on the segment as written, plus for time-ordered data the
/// clustered merge (scanned and indexed).
pub fn configs(data: &Dataset) -> Result<Vec<Config>> {
    let (bytes, segment_ms) = write(data)?;
    let mut out = Vec::new();
    if data.order == Order::Time {
        let (merged, merge_ms) = clustered_merge(&bytes, data.dimension)?;
        out.push(config("scan", bytes.clone(), segment_ms, false)?);
        out.push(config("indexed (IVF)", bytes, segment_ms, true)?);
        out.push(config(
            "clustered merge + scan",
            merged.clone(),
            segment_ms + merge_ms,
            false,
        )?);
        out.push(config(
            "clustered merge + indexed",
            merged,
            segment_ms + merge_ms,
            true,
        )?);
    } else {
        out.push(config("scan", bytes.clone(), segment_ms, false)?);
        out.push(config("indexed (IVF)", bytes, segment_ms, true)?);
    }
    Ok(out)
}

/// Every configuration and workload over one dataset.
pub fn run_dataset(
    data: &Dataset,
    workloads: &[Workload],
    mut log: impl FnMut(&Measurement),
) -> Result<Vec<Measurement>> {
    let label = format!("{} × {} {}", data.vectors.len(), data.dimension, data.order.name());
    let configs = configs(data)?;
    let mut out = Vec::new();
    for workload in workloads {
        let (exact, truths) = measure_exact(data, &label, *workload);
        log(&exact);
        out.push(exact);
        for config in &configs {
            let measured = measure(data, &label, config, *workload, &truths)?;
            log(&measured);
            out.push(measured);
        }
    }
    Ok(out)
}

/// A small multivector (MaxSim) workload: `documents` documents of 4–12 tokens drawn around 1–3 of the dataset's
/// topics, 4-token queries.
pub fn run_multivector(documents: usize, dimension: usize, queries: usize, seed: u64) -> Result<Vec<Measurement>> {
    let mut rng = Rng::new(seed ^ 0x4D41_5853_494D);
    let topics = (((documents as f64).sqrt()).ceil() as usize).max(4);
    let centres: Vec<Vec<f32>> = (0..topics).map(|_| random_unit(&mut rng, dimension)).collect();
    let mut docs: Vec<Vec<Vec<f32>>> = Vec::with_capacity(documents);
    for _ in 0..documents {
        let own: Vec<usize> = (0..1 + rng.below(3)).map(|_| rng.below(topics)).collect();
        let tokens = 4 + rng.below(9);
        docs.push(
            (0..tokens)
                .map(|_| {
                    let topic = own[rng.below(own.len())];
                    around(&mut rng, &centres[topic])
                })
                .collect(),
        );
    }
    let query_sets: Vec<Vec<Vec<f32>>> = (0..queries)
        .map(|_| {
            let topic = rng.below(topics);
            (0..4).map(|_| around(&mut rng, &centres[topic])).collect()
        })
        .collect();
    let model = model(dimension);
    let tokens: usize = docs.iter().map(Vec::len).sum();
    let label = format!("multivector {documents} docs / {tokens} tokens × {dimension}");
    let input = MultivectorInput {
        event_ids: (0..documents).map(|doc| format!("$d{doc}")).collect(),
        integer_columns: Vec::new(),
        string_columns: Vec::new(),
        documents: docs.clone(),
    };
    let spec = SegmentSpec::new(model.clone(), "bench", None);
    let start = Instant::now();
    let bytes = write_multivector_segment(&spec, &input, &key())?;
    let segment_ms = ms(start);

    let mut out = Vec::new();
    let mut latencies = Vec::new();
    let mut truths = Vec::new();
    for query in &query_sets {
        let start = Instant::now();
        let mut scored: Vec<(f32, u64)> = docs
            .iter()
            .enumerate()
            .map(|(doc, tokens)| (max_sim(query, tokens, VectorMetric::Cosine), doc as u64))
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        latencies.push(ms(start));
        truths.push(scored.into_iter().take(K).map(|(_, doc)| doc).collect::<Vec<_>>());
    }
    let mut exact = summary(&label, "exact (f32 MaxSim)", Workload::TopK, latencies, (1, 1), 0, 0);
    exact.workload = "MaxSim top-10".to_owned();
    exact.bytes_per_vector = (dimension * 4) as f64;
    out.push(exact);

    for indexed in [false, true] {
        let config = config(
            if indexed { "indexed (IVF)" } else { "scan" },
            bytes.clone(),
            segment_ms,
            indexed,
        )?;
        let indexes = [config.index.as_ref()];
        let (mut latencies, mut found, mut total, mut blocks, mut reranked) = (Vec::new(), 0, 0, 0, 0);
        for (query, truth) in query_sets.iter().zip(&truths) {
            let search = MultivectorSearch::new(&model, query, K);
            let before = config.reader.blocks_opened();
            let start = Instant::now();
            let (hits, stats) = search_multivector_segments_indexed(
                std::slice::from_ref(&config.reader),
                &[],
                &indexes,
                &search,
                SearchOptions::default(),
            )?;
            latencies.push(ms(start));
            blocks += config.reader.blocks_opened() - before;
            reranked += stats.rows_reranked;
            let got: HashSet<u64> = hits.iter().map(|hit| event_row(&hit.event_id)).collect();
            found += truth.iter().filter(|doc| got.contains(doc)).count();
            total += truth.len();
        }
        let mut measured = summary(
            &label,
            &config.name,
            Workload::TopK,
            latencies,
            (found, total),
            blocks,
            reranked,
        );
        measured.workload = "MaxSim top-10".to_owned();
        measured.bytes_per_vector = config.segment_bytes as f64 / tokens as f64;
        measured.index_bytes_per_vector = config.index_bytes as f64 / tokens as f64;
        measured.segment_build_ms = config.segment_ms;
        measured.index_build_ms = config.index_ms;
        out.push(measured);
    }
    Ok(out)
}

pub fn markdown(measurements: &[Measurement]) -> String {
    let mut out = String::from(
        "| Dataset | Workload | Configuration | Recall@10 | p50 ms | p99 ms | Blocks opened | Rows reranked | Bytes/vector (+ index) | Build ms (segment + index) |\n\
         |---|---|---|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for m in measurements {
        let exact = m.config.starts_with("exact");
        out.push_str(&format!(
            "| {} | {} | {} | {:.3} | {:.3} | {:.3} | {} | {} | {:.0}{} | {} |\n",
            m.dataset,
            m.workload,
            m.config,
            m.recall,
            m.p50_ms,
            m.p99_ms,
            if exact {
                "–".to_owned()
            } else {
                format!("{:.1}", m.blocks_opened)
            },
            if exact {
                "–".to_owned()
            } else {
                format!("{:.1}", m.rows_reranked)
            },
            m.bytes_per_vector,
            if m.index_bytes_per_vector > 0.0 {
                format!(" + {:.0}", m.index_bytes_per_vector)
            } else {
                String::new()
            },
            if exact {
                "–".to_owned()
            } else {
                format!("{:.0} + {:.0}", m.segment_build_ms, m.index_build_ms)
            },
        ));
    }
    out
}
