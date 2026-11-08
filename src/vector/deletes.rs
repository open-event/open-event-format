//! Removes embeddings from a vector segment without rewriting it: a segment's deleted rows are recorded beside it as
//! a [`SegmentDeletes`], the same row-position deletion vector OE files use (see [`crate::deletes`]), published as a
//! new immutable generation each time rows are added. A search skips the rows it marks
//! ([`super::search_segments_with_deletes`]); compaction ([`super::merge_segments_with_deletes`]) drops them for good,
//! after which the merged segment starts with no deletes.

use super::error::VectorError;
use super::segment::{SegmentReader, SegmentSource};
use crate::deletes::{DeletionVector, decode_deletion_vector, encode_deletion_vector};
use crate::file::bytes::{Reader, Writer};
use crate::indexes::bitmap::RoaringRangeBitmap;

/// The rows deleted from one vector segment, in its file-row ordinals, at one generation.
///
/// Immutable once built: [`Self::with_deleted`] returns the next generation and leaves this one as it was, so a
/// search that picked a generation keeps a stable view while a newer one is published.
///
/// See: oe-vector-columns/spec.md
#[derive(Debug, Clone, Default)]
pub struct SegmentDeletes {
    generation: u64,
    rows: DeletionVector,
}

impl SegmentDeletes {
    /// Generation 0: nothing deleted.
    pub fn new() -> Self {
        Self::default()
    }

    /// The next generation: every row this one deletes plus `rows`.
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

    /// The underlying deletion vector, in file-row ordinals.
    pub fn deletion_vector(&self) -> &DeletionVector {
        &self.rows
    }

    /// The deleted rows as a bitmap.
    pub(crate) fn bitmap(&self) -> RoaringRangeBitmap {
        RoaringRangeBitmap::from_rows(self.rows.iter())
    }

    /// The stored form: the generation, then the deletion vector in its OE wire form.
    pub fn encode(&self) -> Vec<u8> {
        let (_, vector) = encode_deletion_vector(&self.rows);
        let mut out = Writer::with_capacity(8 + vector.len());
        out.put_u64(self.generation);
        let mut bytes = out.into_bytes();
        bytes.extend_from_slice(&vector);
        bytes
    }

    /// Reads back what [`Self::encode`] wrote; malformed bytes fail rather than decode to a smaller set.
    pub fn decode(bytes: &[u8]) -> Result<Self, VectorError> {
        let mut reader = Reader::new(bytes);
        let generation = reader.u64("segment deletes generation")?;
        let rows = decode_deletion_vector(reader.take(reader.remaining(), "segment deletion vector")?)?;
        Ok(Self { generation, rows })
    }
}

impl<S: SegmentSource> SegmentReader<S> {
    /// The file rows holding `event_ids`, ascending, so a caller can delete embeddings by event id. Ids the segment
    /// does not hold are skipped.
    pub fn rows_for_event_ids(&self, event_ids: &[&str]) -> Result<Vec<u64>, VectorError> {
        let mut rows = Vec::new();
        for granule in 0..self.granule_count() {
            let first = self.granule_first_row(granule);
            let ids = self.strings(super::EVENT_ID_COLUMN, granule)?;
            rows.extend(
                ids.iter()
                    .enumerate()
                    .filter(|(_, id)| event_ids.contains(&id.as_str()))
                    .map(|(local, _)| first + local as u64),
            );
        }
        Ok(rows)
    }
}
