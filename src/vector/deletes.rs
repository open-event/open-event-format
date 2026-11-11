use super::error::VectorError;
use super::segment::{SegmentReader, SegmentSource};
use crate::deletes::{DeletionVector, decode_deletion_vector, encode_deletion_vector};
use crate::file::bytes::{Reader, Writer};
use crate::indexes::bitmap::RoaringRangeBitmap;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct SegmentDeletes {
    generation: u64,
    rows: DeletionVector,
}

impl SegmentDeletes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_deleted(&self, rows: impl IntoIterator<Item = u64>) -> Self {
        let mut next = self.rows.clone();
        rows.into_iter().for_each(|row| next.mark_deleted(row));
        Self {
            generation: self.generation + 1,
            rows: next,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn is_deleted(&self, row: u64) -> bool {
        self.rows.is_deleted(row)
    }

    pub fn deleted_count(&self) -> u64 {
        self.rows.deleted_count()
    }

    pub fn deletion_vector(&self) -> &DeletionVector {
        &self.rows
    }

    pub(crate) fn bitmap(&self) -> RoaringRangeBitmap {
        RoaringRangeBitmap::from_rows(self.rows.iter())
    }

    pub fn encode(&self) -> Vec<u8> {
        let (_, vector) = encode_deletion_vector(&self.rows);
        let mut out = Writer::with_capacity(8 + vector.len());
        out.put_u64(self.generation);
        let mut bytes = out.into_bytes();
        bytes.extend_from_slice(&vector);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, VectorError> {
        let mut reader = Reader::new(bytes);
        let generation = reader.u64("segment deletes generation")?;
        let rows = decode_deletion_vector(reader.take(reader.remaining(), "segment deletion vector")?)?;
        Ok(Self { generation, rows })
    }
}

impl<S: SegmentSource> SegmentReader<S> {
    pub fn rows_for_event_ids(&self, event_ids: &[&str]) -> Result<Vec<u64>, VectorError> {
        let wanted: HashSet<&str> = event_ids.iter().copied().collect();
        let mut rows = Vec::new();
        for granule in 0..self.granule_count() {
            let first = self.granule_first_row(granule);
            let ids = self.strings(super::EVENT_ID_COLUMN, granule)?;
            rows.extend(
                ids.iter()
                    .enumerate()
                    .filter(|(_, id)| wanted.contains(id.as_str()))
                    .map(|(local, _)| first + local as u64),
            );
        }
        Ok(rows)
    }
}
