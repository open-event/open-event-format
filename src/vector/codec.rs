//! How a segment's vector columns are encoded and scored.
//!
//! Versions 1–3 (`legacy`): U1 rows are plain sign bits scored by Hamming similarity, I8 rows carry their own
//! min/max scale and offset. Version 4: U1 rows are RaBitQ codes (`rabitq`) and I8 rows use one scale and offset
//! per dimension for the whole segment, recorded in the footer; their trailer holds the reconstruction's squared
//! norm and its distance to the original row, so L2 is `|q|² + |x̂|² − 2 q·x̂` and both quantised codes give a
//! bound on the exact score. F16 and F32 are unchanged.

use super::constant::*;
use super::kernel::{PreparedQuery, dot_i8};
use super::model::{EmbeddingModel, VectorElement, VectorMetric};
use super::quantize::{decode_row, encode_row, trailer_pair};
use super::rabitq::{self, Estimate, RabitQuery, Rotation};

/// Version 4 footer parameters shared by every row of the segment.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CodecParams {
    /// RaBitQ centre: the mean row (empty when the segment has no rows).
    pub(crate) centre: Vec<f32>,
    /// Per-dimension int8 `(scale, offset)`; empty when the segment has no I8 column.
    pub(crate) i8_dimensions: Vec<(f32, f32)>,
    /// Largest `|o − centre|` over the segment's rows, for a whole-segment score bound.
    pub(crate) radius: f32,
    /// Seed of the RaBitQ rotation.
    pub(crate) seed: u64,
}

impl CodecParams {
    pub(crate) fn build(model: &EmbeddingModel, vectors: &[Vec<f32>], representations: &[VectorElement]) -> Self {
        let dimension = model.dimension as usize;
        let mut centre = vec![0f32; dimension];
        for vector in vectors {
            centre.iter_mut().zip(vector).for_each(|(slot, value)| *slot += value);
        }
        let count = vectors.len().max(1) as f32;
        centre.iter_mut().for_each(|value| *value /= count);
        if vectors.is_empty() {
            centre.clear();
        }
        let radius = vectors
            .iter()
            .map(|vector| {
                vector
                    .iter()
                    .zip(&centre)
                    .map(|(value, centre)| (value - centre).powi(2))
                    .sum::<f32>()
                    .sqrt()
            })
            .fold(0f32, f32::max);
        let i8_dimensions = if representations.contains(&VectorElement::I8) {
            (0..dimension)
                .map(|index| {
                    let values = vectors.iter().filter_map(|vector| vector.get(index).copied());
                    let (min, max) = values.fold((f32::INFINITY, f32::NEG_INFINITY), |(min, max), value| {
                        (min.min(value), max.max(value))
                    });
                    if !min.is_finite() || max <= min {
                        (1.0, if min.is_finite() { min } else { 0.0 })
                    } else {
                        let scale = (max - min) / 255.0;
                        (scale, min + 128.0 * scale)
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        CodecParams {
            centre,
            i8_dimensions,
            radius,
            seed: rotation_seed(model),
        }
    }
}

/// The rotation seed: a function of the model alone, so equal input gives equal bytes.
pub(crate) fn rotation_seed(model: &EmbeddingModel) -> u64 {
    let mut hasher = blake3::Hasher::new_derive_key(RABITQ_SEED_CONTEXT);
    hasher.update(&model.digest);
    hasher.update(model.id.as_bytes());
    hasher.update(&model.dimension.to_le_bytes());
    hasher.update(&[model.metric.code()]);
    let digest = hasher.finalize();
    let mut seed = [0u8; 8];
    seed.iter_mut()
        .zip(digest.as_bytes())
        .for_each(|(slot, byte)| *slot = *byte);
    u64::from_le_bytes(seed)
}

#[derive(Debug, Clone)]
pub(crate) struct SegmentCodec {
    dimension: usize,
    metric: VectorMetric,
    v4: Option<(Rotation, CodecParams)>,
}

impl SegmentCodec {
    pub(crate) fn legacy(model: &EmbeddingModel) -> Self {
        SegmentCodec {
            dimension: model.dimension as usize,
            metric: model.metric,
            v4: None,
        }
    }

    pub(crate) fn v4(model: &EmbeddingModel, params: CodecParams) -> Self {
        SegmentCodec {
            dimension: model.dimension as usize,
            metric: model.metric,
            v4: Some((Rotation::new(model.dimension as usize, params.seed), params)),
        }
    }

    #[cfg(test)]
    pub(crate) fn params(&self) -> Option<&CodecParams> {
        self.v4.as_ref().map(|(_, params)| params)
    }

    pub(crate) fn encode_row(&self, element: VectorElement, vector: &[f32], out: &mut Vec<u8>) {
        match (element, &self.v4) {
            (VectorElement::U1, Some((rotation, params))) => rabitq::encode(rotation, &params.centre, vector, out),
            (VectorElement::I8, Some((_, params))) => {
                let mut squared = 0f32;
                let mut error = 0f32;
                for (value, (scale, offset)) in vector.iter().zip(&params.i8_dimensions) {
                    let code = ((value - offset) / scale).round().clamp(-128.0, 127.0) as i8;
                    let restored = f32::from(code) * scale + offset;
                    squared += restored * restored;
                    error += (value - restored).powi(2);
                    out.push(code as u8);
                }
                out.extend_from_slice(&squared.to_le_bytes());
                out.extend_from_slice(&error.sqrt().to_le_bytes());
            }
            _ => encode_row(element, vector, out),
        }
    }

    pub(crate) fn decode_row(&self, element: VectorElement, row: &[u8]) -> Vec<f32> {
        match (element, &self.v4) {
            (VectorElement::U1, Some((rotation, params))) => {
                rabitq::decode(rotation, &params.centre, self.dimension, row)
            }
            (VectorElement::I8, Some((_, params))) => row
                .iter()
                .take(self.dimension)
                .zip(&params.i8_dimensions)
                .map(|(code, (scale, offset))| f32::from(*code as i8) * scale + offset)
                .collect(),
            _ => decode_row(element, self.dimension as u32, row),
        }
    }

    pub(crate) fn query(&self, element: VectorElement, query: &[f32]) -> CodedQuery {
        let l2 = self.metric == VectorMetric::L2;
        match (element, &self.v4) {
            (VectorElement::U1, Some((rotation, params))) => {
                CodedQuery::RaBitQ(RabitQuery::new(rotation, &params.centre, query, l2))
            }
            (VectorElement::I8, Some((_, params))) => CodedQuery::I8(I8Query::new(&params.i8_dimensions, query, l2)),
            (VectorElement::F16 | VectorElement::F32, _) => {
                CodedQuery::Exact(PreparedQuery::new(query, self.metric), element)
            }
            _ => CodedQuery::Legacy(PreparedQuery::new(query, self.metric), element),
        }
    }

    /// Lower and upper bounds on any row's exact score in this segment (`±∞` without version 4 parameters).
    pub(crate) fn segment_bounds(&self, query: &[f32]) -> (f32, f32) {
        let Some((_, params)) = &self.v4 else {
            return (f32::NEG_INFINITY, f32::INFINITY);
        };
        let centre = |index: usize| params.centre.get(index).copied().unwrap_or(0.0);
        let slack = 1e-4;
        let (lower, upper) = match self.metric {
            VectorMetric::L2 => {
                let distance = query
                    .iter()
                    .enumerate()
                    .map(|(index, value)| (value - centre(index)).powi(2))
                    .sum::<f32>()
                    .sqrt();
                (
                    -(distance + params.radius).powi(2),
                    -(distance - params.radius).max(0.0).powi(2),
                )
            }
            VectorMetric::Cosine | VectorMetric::Dot => {
                let norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
                let at_centre: f32 = query
                    .iter()
                    .enumerate()
                    .map(|(index, value)| value * centre(index))
                    .sum();
                (at_centre - norm * params.radius, at_centre + norm * params.radius)
            }
        };
        (lower - slack * (1.0 + lower.abs()), upper + slack)
    }
}

/// A query prepared for one column of one segment.
#[derive(Debug, Clone)]
pub(crate) enum CodedQuery {
    /// F16 and F32: the exact score.
    Exact(PreparedQuery, VectorElement),
    /// Version 1–3 U1 and I8: a score with no bound.
    Legacy(PreparedQuery, VectorElement),
    RaBitQ(RabitQuery),
    I8(I8Query),
}

impl CodedQuery {
    pub(crate) fn estimate(&self, row: &[u8]) -> Estimate {
        match self {
            CodedQuery::Exact(query, element) => Estimate::exact(query.score(*element, row)),
            CodedQuery::Legacy(query, element) => Estimate::unbounded(query.score(*element, row)),
            CodedQuery::RaBitQ(query) => query.estimate(row),
            CodedQuery::I8(query) => query.estimate(row),
        }
    }

    pub(crate) fn score(&self, row: &[u8]) -> f32 {
        self.estimate(row).score
    }

    /// Whether `estimate` carries a finite interval, so a search can rerank by bounds.
    pub(crate) fn bounded(&self) -> bool {
        !matches!(self, CodedQuery::Legacy(..))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct I8Query {
    /// Dot and cosine: `Σ offset_j q_j`.
    constant: f32,
    dimension: usize,
    l2: bool,
    norm: f32,
    /// `scale_j · q_j`.
    scaled: Vec<f32>,
    squared_norm: f32,
}

impl I8Query {
    fn new(dimensions: &[(f32, f32)], query: &[f32], l2: bool) -> Self {
        let squared_norm: f32 = query.iter().map(|value| value * value).sum();
        I8Query {
            constant: query
                .iter()
                .zip(dimensions)
                .map(|(value, (_, offset))| value * offset)
                .sum(),
            dimension: query.len(),
            l2,
            norm: squared_norm.sqrt(),
            scaled: query
                .iter()
                .zip(dimensions)
                .map(|(value, (scale, _))| value * scale)
                .collect(),
            squared_norm,
        }
    }

    fn estimate(&self, row: &[u8]) -> Estimate {
        let (codes, trailer) = row.split_at(self.dimension.min(row.len()));
        let (restored_squared, error) = trailer_pair(trailer);
        let dot = dot_i8(codes, &self.scaled) + self.constant;
        let slack = 1e-5 * (1.0 + dot.abs());
        if self.l2 {
            let distance = (self.squared_norm + restored_squared - 2.0 * dot).max(0.0).sqrt();
            Estimate {
                lower: -(distance + error).powi(2) - slack,
                score: -distance * distance,
                upper: -(distance - error).max(0.0).powi(2) + slack,
            }
        } else {
            let error = self.norm * error + slack;
            Estimate {
                lower: dot - error,
                score: dot,
                upper: dot + error,
            }
        }
    }
}
