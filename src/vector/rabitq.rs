//! RaBitQ-style one-bit codes (segment version 4).
//!
//! A row `o` is centred on the segment's RaBitQ centre `c`, rotated by a deterministic random orthogonal matrix `P`
//! and reduced to the signs of `P(o − c)`. Each row keeps two correction factors: `norm = |o − c|` and
//! `alignment = <ō, u>`, where `u = P(o − c) / norm` and `ō = sign(u) / √d`. For a query vector `v` the estimator
//! `<o − c, v> ≈ norm · <ō, P v> / alignment` is unbiased, and with high probability its error is below
//! `norm · |v| · ε₀ · √(1 − alignment²) / (alignment · √(d − 1))` (Gao & Long, SIGMOD 2024). The per-row error
//! bound is therefore a pure function of the stored factors and needs no extra bytes.
//!
//! The query side is scalar-quantised to four bits per dimension and stored as four bit planes, so the inner
//! product with a row's sign bits is five popcounts per 64 dimensions.

use super::constant::{RABITQ_EPSILON, RABITQ_ROTATION_ROUNDS};
use super::kernel::plane_counts;
use super::quantize::trailer_pair;

/// A deterministic random orthogonal rotation: rounds of random sign flips and normalised Walsh–Hadamard
/// transforms. When the dimension is not a power of two the transform alternates between the leading and the
/// trailing power-of-two window, which keeps the dimension and stays orthogonal.
#[derive(Debug, Clone)]
pub(crate) struct Rotation {
    dimension: usize,
    signs: Vec<f32>,
    window: usize,
}

impl Rotation {
    pub(crate) fn new(dimension: usize, seed: u64) -> Self {
        let mut state = seed;
        let mut next = move || {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        let count = RABITQ_ROTATION_ROUNDS * 2 * dimension;
        let mut signs = Vec::with_capacity(count);
        while signs.len() < count {
            let word = next();
            signs.extend(
                (0..64)
                    .take(count - signs.len())
                    .map(|bit| if word >> bit & 1 == 1 { -1.0 } else { 1.0 }),
            );
        }
        let window = if dimension == 0 {
            0
        } else {
            1usize << (usize::BITS - 1 - dimension.leading_zeros())
        };
        Rotation {
            dimension,
            signs,
            window,
        }
    }

    pub(crate) fn apply(&self, values: &mut [f32]) {
        for round in self.signs.chunks_exact(2 * self.dimension.max(1)) {
            let (head_signs, tail_signs) = round.split_at(self.dimension);
            flip(values, head_signs);
            fwht(values.get_mut(..self.window).unwrap_or_default());
            flip(values, tail_signs);
            fwht(
                values
                    .get_mut(self.dimension - self.window..self.dimension)
                    .unwrap_or_default(),
            );
        }
    }

    pub(crate) fn apply_inverse(&self, values: &mut [f32]) {
        for round in self.signs.chunks_exact(2 * self.dimension.max(1)).rev() {
            let (head_signs, tail_signs) = round.split_at(self.dimension);
            fwht(
                values
                    .get_mut(self.dimension - self.window..self.dimension)
                    .unwrap_or_default(),
            );
            flip(values, tail_signs);
            fwht(values.get_mut(..self.window).unwrap_or_default());
            flip(values, head_signs);
        }
    }
}

fn flip(values: &mut [f32], signs: &[f32]) {
    values.iter_mut().zip(signs).for_each(|(value, sign)| *value *= sign);
}

/// In-place normalised fast Walsh–Hadamard transform; `values.len()` is a power of two.
fn fwht(values: &mut [f32]) {
    let len = values.len();
    let mut half = 1;
    while half < len {
        for pair in values.chunks_exact_mut(2 * half) {
            let (left, right) = pair.split_at_mut(half);
            for (a, b) in left.iter_mut().zip(right.iter_mut()) {
                let (sum, difference) = (*a + *b, *a - *b);
                *a = sum;
                *b = difference;
            }
        }
        half *= 2;
    }
    if len > 1 {
        let scale = 1.0 / (len as f32).sqrt();
        values.iter_mut().for_each(|value| *value *= scale);
    }
}

/// Appends a RaBitQ row: `ceil(d / 8)` sign bytes, then `norm` and `alignment` as little-endian `f32`.
pub(crate) fn encode(rotation: &Rotation, centre: &[f32], vector: &[f32], out: &mut Vec<u8>) {
    let mut residual: Vec<f32> = vector
        .iter()
        .enumerate()
        .map(|(index, value)| value - centre.get(index).copied().unwrap_or(0.0))
        .collect();
    let norm = residual.iter().map(|value| value * value).sum::<f32>().sqrt();
    rotation.apply(&mut residual);
    out.extend(residual.chunks(8).map(|chunk| {
        chunk.iter().enumerate().fold(
            0u8,
            |byte, (bit, value)| if *value > 0.0 { byte | (1 << bit) } else { byte },
        )
    }));
    let alignment = if norm > 0.0 {
        (residual.iter().map(|value| value.abs()).sum::<f32>() / (norm * (vector.len() as f32).sqrt())).min(1.0)
    } else {
        1.0
    };
    out.extend_from_slice(&norm.to_le_bytes());
    out.extend_from_slice(&alignment.to_le_bytes());
}

/// The reconstruction `c + norm · alignment · P⁻¹ ō`, the projection of the row onto its code.
pub(crate) fn decode(rotation: &Rotation, centre: &[f32], dimension: usize, row: &[u8]) -> Vec<f32> {
    let (bits, trailer) = row.split_at(dimension.div_ceil(8).min(row.len()));
    let (norm, alignment) = trailer_pair(trailer);
    let magnitude = norm * alignment / (dimension.max(1) as f32).sqrt();
    let mut values: Vec<f32> = (0..dimension)
        .map(|index| {
            let set = bits.get(index / 8).is_some_and(|byte| byte & (1 << (index % 8)) != 0);
            if set { magnitude } else { -magnitude }
        })
        .collect();
    rotation.apply_inverse(&mut values);
    values
        .iter_mut()
        .zip(centre.iter().chain(std::iter::repeat(&0.0)))
        .for_each(|(value, centre)| *value += centre);
    values
}

/// An estimate of a row's score with a high-probability interval around it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Estimate {
    pub(crate) lower: f32,
    pub(crate) score: f32,
    pub(crate) upper: f32,
}

impl Estimate {
    pub(crate) fn exact(score: f32) -> Self {
        Estimate {
            lower: score,
            score,
            upper: score,
        }
    }

    pub(crate) fn unbounded(score: f32) -> Self {
        Estimate {
            lower: f32::NEG_INFINITY,
            score,
            upper: f32::INFINITY,
        }
    }
}

/// A query prepared against one segment's RaBitQ column.
#[derive(Debug, Clone)]
pub(crate) struct RabitQuery {
    bit_bytes: usize,
    /// Dot and cosine: `q · c`. L2: `|q − c|²`.
    constant: f32,
    delta: f32,
    epsilon: f32,
    l2: bool,
    low: f32,
    planes: Vec<[u64; 4]>,
    quantisation_error: f32,
    root_dimension: f32,
    sum: f32,
    vector_norm: f32,
}

impl RabitQuery {
    pub(crate) fn new(rotation: &Rotation, centre: &[f32], query: &[f32], l2: bool) -> Self {
        let dimension = query.len();
        let centred = |index: usize, value: f32| value - centre.get(index).copied().unwrap_or(0.0);
        let constant = if l2 {
            query
                .iter()
                .enumerate()
                .map(|(index, value)| centred(index, *value).powi(2))
                .sum()
        } else {
            query.iter().zip(centre).map(|(value, centre)| value * centre).sum()
        };
        let mut rotated: Vec<f32> = if l2 {
            query
                .iter()
                .enumerate()
                .map(|(index, value)| centred(index, *value))
                .collect()
        } else {
            query.to_vec()
        };
        let vector_norm = rotated.iter().map(|value| value * value).sum::<f32>().sqrt();
        rotation.apply(&mut rotated);
        let low = rotated.iter().copied().fold(f32::INFINITY, f32::min);
        let high = rotated.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let delta = if high > low { (high - low) / 15.0 } else { 1.0 };
        let low = if low.is_finite() { low } else { 0.0 };
        let mut planes = vec![[0u64; 4]; dimension.div_ceil(64)];
        let mut sum = 0.0;
        let mut squared_error = 0.0;
        for (index, value) in rotated.iter().enumerate() {
            let level = ((value - low) / delta).round().clamp(0.0, 15.0);
            let quantised = low + delta * level;
            sum += quantised;
            squared_error += (value - quantised).powi(2);
            let level = level as u64;
            if let Some(word) = planes.get_mut(index / 64) {
                word.iter_mut()
                    .enumerate()
                    .for_each(|(plane, bits)| *bits |= (level >> plane & 1) << (index % 64));
            }
        }
        RabitQuery {
            bit_bytes: dimension.div_ceil(8),
            constant,
            delta,
            epsilon: RABITQ_EPSILON / ((dimension.max(2) - 1) as f32).sqrt(),
            l2,
            low,
            planes,
            quantisation_error: squared_error.sqrt(),
            root_dimension: (dimension.max(1) as f32).sqrt(),
            sum,
            vector_norm,
        }
    }

    pub(crate) fn estimate(&self, row: &[u8]) -> Estimate {
        let (bits, trailer) = row.split_at(self.bit_bytes.min(row.len()));
        let (norm, alignment) = trailer_pair(trailer);
        let (inner, error) = if norm > 0.0 && alignment > 0.0 {
            let (ones, weighted) = plane_counts(bits, &self.planes);
            let selected = self.low * ones as f32 + self.delta * weighted as f32;
            let code_dot = (2.0 * selected - self.sum) / self.root_dimension;
            let spread = (1.0 - alignment * alignment).max(0.0).sqrt();
            let error = norm / alignment
                * (self.vector_norm * self.epsilon * spread
                    + RABITQ_EPSILON * self.quantisation_error / self.root_dimension);
            (norm * code_dot / alignment, error)
        } else {
            (0.0, 0.0)
        };
        let slack = 1e-5 * (1.0 + self.constant.abs());
        if self.l2 {
            let score = -(self.constant + norm * norm - 2.0 * inner);
            let error = 2.0 * error + slack;
            Estimate {
                lower: score - error,
                score,
                upper: score + error,
            }
        } else {
            let score = self.constant + inner;
            let error = error + slack;
            Estimate {
                lower: score - error,
                score,
                upper: score + error,
            }
        }
    }
}
