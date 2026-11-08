//! Saved field-level deletion vectors: the fields of a published file's rows hidden at query time, stored beside its
//! manifest entry the same way saved row-level deletion vectors are (see [`crate::deletion_store`]).
//!
//! A multi-subject event survives the erasure of one of its subjects: that subject's fields are crypto-shredded (see
//! [`crate::security::seal_subject_fields`]) and a field-level deletion vector naming those fields and the affected
//! rows is published alongside, so a query hides them without opening any key. Each distinct set of redacted columns
//! is one immutable, generationed [`StoredFieldDeletionVector`]; recording more rows for the same columns publishes its
//! next generation (the union) and the entry's [`OeFileEntry::field_deletion_vectors`] moves to it in the next
//! manifest generation.
//!
//! See: oe-deletes-and-corrections/spec.md

use crate::deletes::{
    DeletionVector, FieldDeletionVector, FieldDeletionVectorRef, decode_deletion_vector, encode_deletion_vector,
};
use crate::error::FormatError;
use crate::events::SequenceRange;
use crate::lifecycle::{ManifestGeneration, OeFileEntry};

/// Domain-separation context for a stored field deletion vector's BLAKE3 digest.
const STORED_FIELD_DELETION_VECTOR_CONTEXT: &str = "oe 2026 stored field deletion vector v1";

/// One published generation of a file's field-level deletion vector for one set of redacted columns. Immutable once
/// published.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredFieldDeletionVector {
    /// Domain-separated BLAKE3 over the identity fields, the columns, and `encoded`; checked on every load.
    pub blake3: [u8; 32],
    /// Number of affected rows in this generation.
    pub deleted_count: u64,
    /// Starts at 1 and rises by exactly one per published generation of this column set.
    pub deletion_vector_generation: u64,
    /// The affected rows in [`encode_deletion_vector`]'s wire form.
    pub encoded: Vec<u8>,
    /// The redacted field paths, sorted and unique.
    pub redacted_columns: Vec<String>,
    pub target_file_id: u128,
    pub target_projection_id: Option<u64>,
    pub target_sequence_range: SequenceRange,
}

fn normalized(columns: &[String]) -> Vec<String> {
    let mut columns = columns.to_vec();
    columns.sort();
    columns.dedup();
    columns
}

impl StoredFieldDeletionVector {
    /// Publishes the next generation for `additions`' column set in the file `entry` describes: the union of the
    /// entry's current vector for those columns (verified first) and `additions`. The first generation is 1.
    pub fn next_generation(entry: &OeFileEntry, additions: &FieldDeletionVector) -> Result<Self, FormatError> {
        let columns = normalized(additions.redacted_columns());
        let (mut rows, generation) = match entry
            .field_deletion_vectors
            .iter()
            .find(|stored| stored.redacted_columns == columns)
        {
            Some(prior) => (prior.load_rows(entry.file_id)?, prior.deletion_vector_generation + 1),
            None => (DeletionVector::new(), 1),
        };
        additions.affected_ordinals().for_each(|row| rows.mark_deleted(row));
        let (_, encoded) = encode_deletion_vector(&rows);
        let mut stored = Self {
            blake3: [0; 32],
            deleted_count: rows.deleted_count(),
            deletion_vector_generation: generation,
            encoded,
            redacted_columns: columns,
            target_file_id: entry.file_id,
            target_projection_id: None,
            target_sequence_range: entry.coverage,
        };
        stored.blake3 = stored.digest();
        Ok(stored)
    }

    /// Verifies the digest and identity, then decodes the vector. Refuses one naming another file, one whose bytes,
    /// columns, or identity fields were altered, or one whose decoded count disagrees with `deleted_count`.
    pub fn load(&self, expected_file_id: u128) -> Result<FieldDeletionVector, FormatError> {
        let rows = self.load_rows(expected_file_id)?;
        let mut vector = FieldDeletionVector::new(self.redacted_columns.clone());
        rows.iter().for_each(|row| vector.mark_affected(row));
        Ok(vector)
    }

    fn load_rows(&self, expected_file_id: u128) -> Result<DeletionVector, FormatError> {
        if self.target_file_id != expected_file_id {
            return Err(FormatError::Structural {
                rule: "stored field deletion vector names a different target file",
            });
        }
        if self.deletion_vector_generation == 0 {
            return Err(FormatError::Structural {
                rule: "stored field deletion vector generation must start at 1",
            });
        }
        if self.digest() != self.blake3 {
            return Err(FormatError::Structural {
                rule: "stored field deletion vector BLAKE3 mismatch",
            });
        }
        let rows = decode_deletion_vector(&self.encoded)?;
        if rows.deleted_count() != self.deleted_count {
            return Err(FormatError::Structural {
                rule: "stored field deletion vector deleted_count disagrees with its rows",
            });
        }
        Ok(rows)
    }

    /// The spec's identity record for this vector.
    pub fn to_ref(&self) -> FieldDeletionVectorRef {
        FieldDeletionVectorRef {
            blake3: self.blake3,
            deleted_count: self.deleted_count,
            deletion_vector_generation: self.deletion_vector_generation,
            redacted_columns: self.redacted_columns.clone(),
            target_file_id: self.target_file_id,
            target_projection_id: self.target_projection_id,
            target_sequence_range: self.target_sequence_range,
        }
    }

    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new_derive_key(STORED_FIELD_DELETION_VECTOR_CONTEXT);
        hasher.update(&self.target_file_id.to_le_bytes());
        match self.target_projection_id {
            Some(id) => hasher.update(&[1]).update(&id.to_le_bytes()),
            None => hasher.update(&[0]),
        };
        let range = self.target_sequence_range;
        hasher.update(&range.epoch.to_le_bytes());
        hasher.update(&range.first_sequence.to_le_bytes());
        hasher.update(&range.last_sequence.to_le_bytes());
        hasher.update(&self.deletion_vector_generation.to_le_bytes());
        hasher.update(&self.deleted_count.to_le_bytes());
        hasher.update(&(self.redacted_columns.len() as u64).to_le_bytes());
        for column in &self.redacted_columns {
            hasher.update(&(column.len() as u64).to_le_bytes());
            hasher.update(column.as_bytes());
        }
        hasher.update(&(self.encoded.len() as u64).to_le_bytes());
        hasher.update(&self.encoded);
        *hasher.finalize().as_bytes()
    }
}

/// Every field-level deletion vector of one file, loaded and verified, for query-time hiding.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldRedactions {
    pub vectors: Vec<FieldDeletionVector>,
}

impl FieldRedactions {
    /// True when field `column` of row `ordinal` must be hidden.
    pub fn column_redacted(&self, column: &str, ordinal: u64) -> bool {
        self.vectors
            .iter()
            .any(|vector| vector.column_redacted(column, ordinal))
    }

    /// True when row `ordinal`'s raw payload must be withheld because some field of it is redacted.
    pub fn withholds_raw_payload(&self, ordinal: u64) -> bool {
        self.vectors.iter().any(|vector| vector.withholds_raw_payload(ordinal))
    }
}

impl OeFileEntry {
    /// Loads and verifies every saved field-level deletion vector of this file (empty when no field was redacted).
    pub fn load_field_deletion_vectors(&self) -> Result<FieldRedactions, FormatError> {
        Ok(FieldRedactions {
            vectors: self
                .field_deletion_vectors
                .iter()
                .map(|stored| stored.load(self.file_id))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl ManifestGeneration {
    /// Records `additions` in file `file_id` of this (unpublished, next) generation by publishing the next generation
    /// of the file's field deletion vector for `additions`' column set. Returns that vector's new generation, or `None`
    /// when no entry has that id. An `additions` with no affected row publishes nothing and returns the current
    /// generation for its columns (0 when there is none).
    pub fn publish_field_deletions(
        &mut self,
        file_id: u128,
        additions: &FieldDeletionVector,
    ) -> Result<Option<u64>, FormatError> {
        let Some(entry) = self.files.iter_mut().find(|entry| entry.file_id == file_id) else {
            return Ok(None);
        };
        let columns = normalized(additions.redacted_columns());
        let current = entry
            .field_deletion_vectors
            .iter()
            .find(|stored| stored.redacted_columns == columns)
            .map(|stored| stored.deletion_vector_generation);
        if additions.affected_count() == 0 || columns.is_empty() {
            return Ok(Some(current.unwrap_or(0)));
        }
        let stored = StoredFieldDeletionVector::next_generation(entry, additions)?;
        let generation = stored.deletion_vector_generation;
        entry
            .field_deletion_vectors
            .retain(|prior| prior.redacted_columns != columns);
        entry.field_deletion_vectors.push(stored);
        Ok(Some(generation))
    }
}

#[cfg(test)]
#[path = "test/field_deletion_store.rs"]
mod tests;
