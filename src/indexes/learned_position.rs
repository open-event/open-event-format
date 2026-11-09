use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::layout::footer::SortDirection;

const LEARNED_POSITION_MAGIC: u32 = 0x4C50_5831;

const LEARNED_POSITION_HEADER_BYTES: usize = 33;

const LEARNED_POSITION_SEGMENT_BYTES: usize = 24;

#[derive(Clone, Debug, PartialEq)]
struct Segment {
    intercept: f64,
    key_lo: i64,
    slope: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LearnedPositionIndex {
    pub column_id: u32,
    pub direction: SortDirection,
    pub error_bound: u64,
    pub projection_id: u32,
    row_count: u64,
    segments: Vec<Segment>,
}

impl LearnedPositionIndex {
    pub fn build(column_id: u32, projection_id: u32, keys: &[i64], error_bound: u64) -> Self {
        let rows: Vec<u64> = (0..keys.len() as u64).collect();
        Self::build_at_rows(column_id, projection_id, keys, &rows, keys.len() as u64, error_bound)
    }

    pub fn build_at_rows(
        column_id: u32,
        projection_id: u32,
        keys: &[i64],
        rows: &[u64],
        row_count: u64,
        error_bound: u64,
    ) -> Self {
        let segments = fit_segments(keys, rows, error_bound);
        Self {
            column_id,
            direction: SortDirection::Ascending,
            error_bound: error_bound.saturating_add(1),
            projection_id,
            row_count,
            segments,
        }
    }

    pub fn estimate_position(&self, key: i64) -> PositionWindow {
        let approx = self.model_eval(key);
        let search_from = approx.saturating_sub(self.error_bound);
        let search_to = approx
            .saturating_add(self.error_bound)
            .min(self.row_count.saturating_sub(1));
        PositionWindow {
            approximate_row: approx,
            search_from,
            search_to,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(self.encoded_len());
        self.encode_into(&mut out);
        out.into_bytes()
    }

    pub(crate) fn encoded_len(&self) -> usize {
        LEARNED_POSITION_HEADER_BYTES + self.segments.len() * LEARNED_POSITION_SEGMENT_BYTES
    }

    pub(crate) fn encode_into(&self, out: &mut Writer) {
        out.put_u32(LEARNED_POSITION_MAGIC);
        out.put_u32(self.column_id);
        out.put_u32(self.projection_id);
        out.put_u8(match self.direction {
            SortDirection::Ascending => 0,
            SortDirection::Descending => 1,
        });
        out.put_u64(self.error_bound);
        out.put_u64(self.row_count);
        out.put_u32(self.segments.len() as u32);
        for segment in &self.segments {
            out.put_u64(segment.intercept.to_bits());
            out.put_i64(segment.key_lo);
            out.put_u64(segment.slope.to_bits());
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        if reader.u32("learned position magic")? != LEARNED_POSITION_MAGIC {
            return Err(FormatError::Structural {
                rule: "learned position index bad magic",
            });
        }
        let column_id = reader.u32("learned position column")?;
        let projection_id = reader.u32("learned position projection")?;
        let direction = match reader.u8("learned position direction")? {
            0 => SortDirection::Ascending,
            1 => SortDirection::Descending,
            _ => {
                return Err(FormatError::Structural {
                    rule: "learned position unknown sort direction",
                });
            }
        };
        let error_bound = reader.u64("learned position error bound")?;
        let row_count = reader.u64("learned position row count")?;
        let segment_count = reader.u32("learned position segment count")? as usize;
        let mut segments = Vec::with_capacity(reader.capacity_hint(segment_count, 24));
        for _ in 0..segment_count {
            let intercept = f64::from_bits(reader.u64("learned position intercept")?);
            let key_lo = reader.i64("learned position key")?;
            let slope = f64::from_bits(reader.u64("learned position slope")?);
            if segments
                .last()
                .is_some_and(|previous: &Segment| key_lo <= previous.key_lo)
            {
                return Err(FormatError::Structural {
                    rule: "learned position segment keys must be strictly ascending",
                });
            }
            segments.push(Segment {
                intercept,
                key_lo,
                slope,
            });
        }
        Ok(Self {
            column_id,
            direction,
            error_bound,
            projection_id,
            row_count,
            segments,
        })
    }

    fn model_eval(&self, key: i64) -> u64 {
        let i = self.segments.partition_point(|s| s.key_lo < key);
        let seg_index = if self.segments.get(i).is_some_and(|seg| seg.key_lo == key) {
            i
        } else if i == 0 {
            return self.segments.first().map_or(0, |seg| seg.intercept as u64);
        } else {
            i - 1
        };
        let Some(seg) = self.segments.get(seg_index) else {
            return 0;
        };
        let mut pos = seg.intercept + seg.slope * (key as i128 - seg.key_lo as i128) as f64;
        if let Some(next) = self.segments.get(seg_index + 1) {
            pos = pos.min(next.intercept);
        }
        (pos.round() as i64).max(0).min(self.row_count.saturating_sub(1) as i64) as u64
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PositionWindow {
    pub approximate_row: u64,
    pub search_from: u64,
    pub search_to: u64,
}

pub fn plan_learned_seek(index: &LearnedPositionIndex, proof_present: bool, column_id: u32) -> Option<LearnedSeek<'_>> {
    if !proof_present || index.column_id != column_id {
        return None;
    }
    Some(LearnedSeek { index })
}

pub struct LearnedSeek<'a> {
    index: &'a LearnedPositionIndex,
}

impl<'a> LearnedSeek<'a> {
    pub fn seek_range_start(&self, key_lo: i64) -> PositionWindow {
        self.index.estimate_position(key_lo)
    }
}

fn fit_segments(keys: &[i64], rows: &[u64], error_bound: u64) -> Vec<Segment> {
    let mut points: Vec<(i64, u64)> = Vec::new();
    for (&key, &row) in keys.iter().zip(rows) {
        match points.last().map(|&(last, _)| last) {
            Some(last) if last == key => continue,
            Some(last) if (last as i128) + 1 < key as i128 => points.push((last + 1, row)),
            _ => {}
        }
        points.push((key, row));
    }

    let error = error_bound as f64;
    let mut segments: Vec<Segment> = Vec::new();
    let mut seg_start = 0usize;

    while let Some(&(key0, row0)) = points.get(seg_start) {
        let pos0 = row0 as f64;

        let mut slope_lo = f64::NEG_INFINITY;
        let mut slope_hi = f64::INFINITY;
        let mut chosen_slope = 0.0f64;
        let mut seg_end = seg_start;

        for (i, &(key, row)) in points.iter().enumerate().skip(seg_start + 1) {
            let dk = (key as i128 - key0 as i128) as f64;

            let s_lo = (row as f64 - error - pos0) / dk;
            let s_hi = (row as f64 + error - pos0) / dk;
            let new_lo = slope_lo.max(s_lo);
            let new_hi = slope_hi.min(s_hi);

            if new_lo > new_hi {
                break;
            }

            slope_lo = new_lo;
            slope_hi = new_hi;
            chosen_slope = (slope_lo + slope_hi) / 2.0;
            seg_end = i;
        }

        segments.push(Segment {
            intercept: pos0,
            key_lo: key0,
            slope: chosen_slope,
        });
        seg_start = seg_end + 1;
    }

    segments
}

#[cfg(test)]
#[path = "test/learned_position.rs"]
mod tests;
