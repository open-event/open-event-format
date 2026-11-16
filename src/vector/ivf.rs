//! IVF partition index: an external, droppable artifact that groups a segment's rows into k-means partitions.
//!
//! The index stores, per partition, its centroid, its radius (the largest distance from the centroid to one of its
//! rows) and its sorted row ids (delta-varint). A query ranks partitions by the score bound `centroid score +
//! radius` and probes the best ones first; a partition whose bound cannot beat the running k-th score is never
//! opened. The index is bound to the segment's `content_hash` and embedding model, and can be sealed with the
//! segment's wrapping key because centroids and row lists leak information about the data.

use super::error::VectorError;
use super::model::{EmbeddingModel, VectorMetric};
use super::seal::WrappingKey;
use super::segment::{SegmentReader, SegmentSource};
use crate::file::bytes::{Reader, Writer};
use crate::indexes::bitmap::RoaringRangeBitmap;
use crate::security::AeadScheme;
use zeroize::Zeroizing;

pub const PARTITION_INDEX_MAGIC: &[u8; 4] = b"OEVI";
pub const PARTITION_INDEX_VERSION: u16 = 1;
const FLAG_SEALED: u16 = 1;
const INDEX_KEY_CONTEXT: &str = "oe 2026 vector partition index key v1";
const INDEX_NONCE_LEN: usize = 24;
const HEADER_BYTES: usize = 4 + 2 + 2 + 32;

/// How a partition index is built. Equal parameters over equal segment content give byte-identical plaintext.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionParams {
    /// Number of partitions; `None` uses `⌈√rows⌉`.
    pub partitions: Option<usize>,
    /// Lloyd iterations after seeding.
    pub iterations: usize,
    /// Seed for the initial centroid choice.
    pub seed: u64,
}

impl Default for PartitionParams {
    fn default() -> Self {
        Self {
            partitions: None,
            iterations: 10,
            seed: 0x0E5E_ED1F_0000_0001,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Partition {
    pub centroid: Vec<f32>,
    /// Largest `|x − centroid|` over the partition's rows, slightly widened for float rounding.
    pub radius: f32,
    /// Sorted row ids.
    pub rows: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionIndex {
    pub content_hash: [u8; 32],
    pub iterations: u32,
    pub model: EmbeddingModel,
    pub partitions: Vec<Partition>,
    pub row_count: u64,
    pub seed: u64,
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn squared_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| (a - b) * (a - b)).sum()
}

fn nearest(centroids: &[Vec<f32>], row: &[f32]) -> usize {
    let mut best = (0usize, f32::INFINITY);
    for (index, centroid) in centroids.iter().enumerate() {
        let distance = squared_distance(centroid, row);
        if distance < best.1 {
            best = (index, distance);
        }
    }
    best.0
}

/// Deterministic Lloyd k-means: seeded distinct initial rows, a fixed number of iterations, L2 assignment.
fn kmeans(vectors: &[Vec<f32>], dimension: usize, partitions: usize, params: &PartitionParams) -> Vec<usize> {
    let n = vectors.len();
    let mut order: Vec<usize> = (0..n).collect();
    let mut state = params.seed;
    for slot in 0..partitions.min(n) {
        let pick = slot + (splitmix(&mut state) % (n - slot) as u64) as usize;
        order.swap(slot, pick);
    }
    let mut centroids: Vec<Vec<f32>> = order
        .iter()
        .take(partitions)
        .filter_map(|row| vectors.get(*row).cloned())
        .collect();
    let mut assignment = vec![0usize; n];
    for iteration in 0..=params.iterations {
        for (slot, row) in assignment.iter_mut().zip(vectors) {
            *slot = nearest(&centroids, row);
        }
        if iteration == params.iterations {
            break;
        }
        let mut sums = vec![vec![0f64; dimension]; centroids.len()];
        let mut counts = vec![0usize; centroids.len()];
        for (slot, row) in assignment.iter().zip(vectors) {
            if let (Some(sum), Some(count)) = (sums.get_mut(*slot), counts.get_mut(*slot)) {
                sum.iter_mut()
                    .zip(row)
                    .for_each(|(total, value)| *total += f64::from(*value));
                *count += 1;
            }
        }
        for ((centroid, sum), count) in centroids.iter_mut().zip(&sums).zip(&counts) {
            if *count > 0 {
                centroid
                    .iter_mut()
                    .zip(sum)
                    .for_each(|(value, total)| *value = (*total / *count as f64) as f32);
            }
        }
    }
    assignment
}

impl PartitionIndex {
    /// Builds the index over the segment's finest stored representation (the vectors search scores exactly).
    pub fn build<S: SegmentSource>(segment: &SegmentReader<S>, params: &PartitionParams) -> Result<Self, VectorError> {
        let manifest = segment.manifest();
        let model = manifest.model.clone();
        let dimension = model.dimension as usize;
        let vectors = segment.read_all()?.vectors;
        let n = vectors.len();
        let wanted = params
            .partitions
            .unwrap_or_else(|| (n as f64).sqrt().ceil() as usize)
            .clamp(1, n.max(1));
        let partitions = if n == 0 {
            Vec::new()
        } else {
            let assignment = kmeans(&vectors, dimension, wanted, params);
            let mut groups: Vec<Vec<u64>> = vec![Vec::new(); wanted];
            for (row, slot) in assignment.iter().enumerate() {
                if let Some(group) = groups.get_mut(*slot) {
                    group.push(row as u64);
                }
            }
            groups
                .into_iter()
                .filter(|rows| !rows.is_empty())
                .map(|rows| {
                    let mut sum = vec![0f64; dimension];
                    for row in &rows {
                        if let Some(vector) = vectors.get(*row as usize) {
                            sum.iter_mut().zip(vector).for_each(|(t, v)| *t += f64::from(*v));
                        }
                    }
                    let centroid: Vec<f32> = sum.iter().map(|t| (*t / rows.len() as f64) as f32).collect();
                    let radius = rows
                        .iter()
                        .filter_map(|row| vectors.get(*row as usize))
                        .map(|vector| {
                            vector
                                .iter()
                                .zip(&centroid)
                                .map(|(a, b)| {
                                    let d = f64::from(*a) - f64::from(*b);
                                    d * d
                                })
                                .sum::<f64>()
                                .sqrt()
                        })
                        .fold(0f64, f64::max);
                    Partition {
                        centroid,
                        radius: (radius * (1.0 + 1e-4) + 1e-5) as f32,
                        rows,
                    }
                })
                .collect()
        };
        Ok(Self {
            content_hash: manifest.content_hash,
            iterations: params.iterations as u32,
            model,
            partitions,
            row_count: segment.row_count(),
            seed: params.seed,
        })
    }

    /// Refuses an index built for different segment content, another model or another row count.
    pub fn check<S: SegmentSource>(&self, segment: &SegmentReader<S>) -> Result<(), VectorError> {
        let manifest = segment.manifest();
        if self.content_hash != manifest.content_hash || self.row_count != manifest.row_count {
            return Err(VectorError::Authentication {
                what: "partition index is bound to other segment content",
            });
        }
        self.model.require_same(&manifest.model)
    }

    /// An upper bound on the score of any row of `partition` for `query` (scores as the kernels compute them).
    pub fn upper_bound(&self, partition: &Partition, query: &[f32]) -> f32 {
        let bound = match self.model.metric {
            VectorMetric::Cosine | VectorMetric::Dot => {
                let dot: f32 = partition.centroid.iter().zip(query).map(|(a, b)| a * b).sum();
                let norm = query.iter().map(|v| v * v).sum::<f32>().sqrt();
                dot + norm * partition.radius
            }
            VectorMetric::L2 => {
                let distance = squared_distance(&partition.centroid, query).sqrt();
                let gap = (distance - partition.radius).max(0.0);
                -(gap * gap)
            }
        };
        bound + 1e-4 * (1.0 + bound.abs())
    }

    /// Partitions ordered by decreasing upper bound, with that bound.
    pub(crate) fn ranked(&self, query: &[f32]) -> Vec<(usize, f32)> {
        let mut ranked: Vec<(usize, f32)> = self
            .partitions
            .iter()
            .enumerate()
            .map(|(index, partition)| (index, self.upper_bound(partition, query)))
            .collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        ranked
    }

    /// Partitions ordered by decreasing centroid score (the likeliest to hold the best rows first).
    pub(crate) fn closest(&self, query: &[f32]) -> Vec<usize> {
        let mut scored: Vec<(usize, f32)> = self
            .partitions
            .iter()
            .enumerate()
            .map(|(index, partition)| {
                let score = match self.model.metric {
                    VectorMetric::Cosine | VectorMetric::Dot => {
                        partition.centroid.iter().zip(query).map(|(a, b)| a * b).sum()
                    }
                    VectorMetric::L2 => -squared_distance(&partition.centroid, query),
                };
                (index, score)
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        scored.into_iter().map(|(index, _)| index).collect()
    }

    pub(crate) fn rows_of(&self, partitions: &[usize]) -> RoaringRangeBitmap {
        let mut rows: Vec<u64> = partitions
            .iter()
            .filter_map(|index| self.partitions.get(*index))
            .flat_map(|partition| partition.rows.iter().copied())
            .collect();
        rows.sort_unstable();
        RoaringRangeBitmap::from_rows(rows)
    }

    fn encode_payload(&self) -> Vec<u8> {
        let mut out = Writer::new();
        self.model.encode_into(&mut out);
        out.put_u64(self.row_count);
        out.put_u32(self.iterations);
        out.put_u64(self.seed);
        out.put_u32(self.partitions.len() as u32);
        for partition in &self.partitions {
            partition.centroid.iter().for_each(|v| out.put_slice(&v.to_le_bytes()));
            out.put_slice(&partition.radius.to_le_bytes());
            out.put_u32(partition.rows.len() as u32);
            let mut deltas = Vec::new();
            let mut previous = 0u64;
            for (position, row) in partition.rows.iter().enumerate() {
                let delta = if position == 0 { *row } else { row - previous };
                put_varint(&mut deltas, delta);
                previous = *row;
            }
            out.put_u32(deltas.len() as u32);
            out.put_slice(&deltas);
        }
        out.into_bytes()
    }

    fn decode_payload(content_hash: [u8; 32], payload: &[u8]) -> Result<Self, VectorError> {
        let mut reader = Reader::new(payload);
        let model = EmbeddingModel::decode_from(&mut reader)?;
        let row_count = reader.u64("partition index rows")?;
        let iterations = reader.u32("partition index iterations")?;
        let seed = reader.u64("partition index seed")?;
        let count = reader.u32("partition count")? as usize;
        let dimension = model.dimension as usize;
        let mut partitions = Vec::with_capacity(reader.capacity_hint(count, 4 * dimension + 12));
        let mut seen = 0u64;
        for _ in 0..count {
            let centroid = reader
                .take(4 * dimension, "partition centroid")?
                .chunks_exact(4)
                .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])))
                .collect();
            let radius = f32::from_le_bytes(
                reader
                    .take(4, "partition radius")?
                    .try_into()
                    .map_err(|_| malformed("partition radius"))?,
            );
            let rows_len = reader.u32("partition row count")? as usize;
            let bytes_len = reader.u32("partition row bytes")? as usize;
            let mut deltas = reader.take(bytes_len, "partition rows")?;
            let mut rows = Vec::with_capacity(rows_len.min(bytes_len));
            let mut previous = 0u64;
            for position in 0..rows_len {
                let delta = take_varint(&mut deltas)?;
                let row = if position == 0 {
                    delta
                } else {
                    previous
                        .checked_add(delta)
                        .filter(|_| delta > 0)
                        .ok_or_else(|| malformed("partition rows not strictly increasing"))?
                };
                if row >= row_count {
                    return Err(malformed("partition row past the segment"));
                }
                rows.push(row);
                previous = row;
            }
            if !deltas.is_empty() || !radius.is_finite() {
                return Err(malformed("partition entry"));
            }
            seen += rows.len() as u64;
            partitions.push(Partition { centroid, radius, rows });
        }
        if reader.remaining() != 0 || seen != row_count {
            return Err(malformed("partition index does not cover every row once"));
        }
        Ok(Self {
            content_hash,
            iterations,
            model,
            partitions,
            row_count,
            seed,
        })
    }

    /// Serialises the index, sealing the payload when `key` is given (use the segment's wrapping key whenever its
    /// data is sensitive: centroids and row lists describe the vectors).
    pub fn encode(&self, key: Option<&WrappingKey>) -> Result<Vec<u8>, VectorError> {
        let mut header = Vec::with_capacity(HEADER_BYTES);
        header.extend_from_slice(PARTITION_INDEX_MAGIC);
        header.extend_from_slice(&PARTITION_INDEX_VERSION.to_le_bytes());
        header.extend_from_slice(&(if key.is_some() { FLAG_SEALED } else { 0 }).to_le_bytes());
        header.extend_from_slice(&self.content_hash);
        let payload = self.encode_payload();
        let Some(key) = key else {
            header.extend_from_slice(&payload);
            return Ok(header);
        };
        let mut nonce = [0u8; INDEX_NONCE_LEN];
        crate::security::fill_random(&mut nonce).ok_or(VectorError::Random)?;
        let sealed = AeadScheme::XChaCha20Poly1305
            .seal(&index_key(key), &nonce, &header, &payload)
            .ok_or(VectorError::Authentication {
                what: "seal partition index",
            })?;
        let mut out = header;
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    pub fn decode(bytes: &[u8], key: Option<&WrappingKey>) -> Result<Self, VectorError> {
        let header = bytes
            .get(..HEADER_BYTES)
            .ok_or_else(|| malformed("partition index header"))?;
        if header.get(..4) != Some(PARTITION_INDEX_MAGIC.as_slice()) {
            return Err(VectorError::Format(crate::error::FormatError::BadMagic {
                expected: "OEVI",
            }));
        }
        let mut reader = Reader::new(header);
        reader.take(4, "partition index magic")?;
        let version = reader.u16("partition index version")?;
        if version != PARTITION_INDEX_VERSION {
            return Err(VectorError::Format(crate::error::FormatError::UnsupportedVersion {
                field: "partition index version",
                found: u32::from(version),
            }));
        }
        let flags = reader.u16("partition index flags")?;
        let mut content_hash = [0u8; 32];
        content_hash.copy_from_slice(reader.take(32, "partition index content hash")?);
        let body = bytes.get(HEADER_BYTES..).unwrap_or_default();
        if flags & FLAG_SEALED == 0 {
            return Self::decode_payload(content_hash, body);
        }
        let key = key.ok_or(VectorError::Authentication {
            what: "sealed partition index needs the segment's wrapping key",
        })?;
        let nonce = body
            .get(..INDEX_NONCE_LEN)
            .ok_or_else(|| malformed("partition index nonce"))?;
        let opened = AeadScheme::XChaCha20Poly1305
            .open(
                &index_key(key),
                nonce,
                header,
                body.get(INDEX_NONCE_LEN..).unwrap_or_default(),
            )
            .map(Zeroizing::new)
            .ok_or(VectorError::Authentication {
                what: "partition index",
            })?;
        Self::decode_payload(content_hash, &opened)
    }
}

fn index_key(key: &WrappingKey) -> Zeroizing<[u8; 32]> {
    Zeroizing::new(blake3::derive_key(INDEX_KEY_CONTEXT, key.bytes()))
}

fn malformed(rule: &'static str) -> VectorError {
    VectorError::Format(crate::error::FormatError::Structural { rule })
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn take_varint(bytes: &mut &[u8]) -> Result<u64, VectorError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first().ok_or_else(|| malformed("partition row varint"))?;
        *bytes = rest;
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(malformed("partition row varint too long"))
}
