use crate::columns::column_ids;
use crate::deletes::{
    DeletionVector, FieldDeletionVector, FieldDeletionVectorRef, decode_deletion_vector, encode_deletion_vector,
};
use crate::error::FormatError;
use crate::events::SequenceRange;
use crate::events::variant::VariantValue;
use crate::layout::reader::{OeFile, PayloadRead};
use crate::lifecycle::{ManifestGeneration, OeFileEntry};

const STORED_FIELD_DELETION_VECTOR_CONTEXT: &str = "oe 2026 stored field deletion vector v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredFieldDeletionVector {
    pub blake3: [u8; 32],
    pub deleted_count: u64,
    pub deletion_vector_generation: u64,
    pub encoded: Vec<u8>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldRedactions {
    pub vectors: Vec<FieldDeletionVector>,
}

impl FieldRedactions {
    pub fn column_redacted(&self, column: &str, ordinal: u64) -> bool {
        self.vectors
            .iter()
            .any(|vector| vector.column_redacted(column, ordinal))
    }

    pub fn withholds_raw_payload(&self, ordinal: u64) -> bool {
        self.vectors.iter().any(|vector| vector.withholds_raw_payload(ordinal))
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.iter().all(|vector| vector.affected_count() == 0)
    }

    pub fn redacted_columns_at(&self, ordinal: u64) -> Vec<&str> {
        let mut columns: Vec<&str> = self
            .vectors
            .iter()
            .filter(|vector| vector.withholds_raw_payload(ordinal))
            .flat_map(|vector| vector.redacted_columns().iter().map(String::as_str))
            .collect();
        columns.sort_unstable();
        columns.dedup();
        columns
    }

    pub fn redact_value(&self, ordinal: u64, value: &mut VariantValue) {
        for path in self.redacted_columns_at(ordinal) {
            if let Some(slot) = field_mut(value, path) {
                *slot = VariantValue::Null;
            }
        }
    }

    pub fn redact_payload(&self, ordinal: u64, payload: PayloadRead) -> PayloadRead {
        if !self.withholds_raw_payload(ordinal) {
            return payload;
        }
        match payload {
            PayloadRead::Value(mut value) => {
                self.redact_value(ordinal, &mut value);
                PayloadRead::Value(value)
            }
            PayloadRead::External(_) | PayloadRead::None => PayloadRead::None,
        }
    }

    pub fn redacted_column_ids(&self, file: &OeFile, ordinal: u64) -> Vec<u32> {
        let paths = self.redacted_columns_at(ordinal);
        if paths.is_empty() {
            return Vec::new();
        }
        let footer = file.footer();
        let mut ids: Vec<u32> = footer
            .shredded
            .iter()
            .chain(&footer.sparse_keys)
            .filter(|entry| paths.contains(&entry.path.as_str()))
            .map(|entry| entry.column_id)
            .chain(
                footer
                    .columns
                    .iter()
                    .filter(|column| paths.contains(&column.name.as_str()))
                    .map(|column| column.column_id),
            )
            .chain(std::iter::once(column_ids::RAW_PAYLOAD))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }
}

fn field_mut<'a>(value: &'a mut VariantValue, path: &str) -> Option<&'a mut VariantValue> {
    let mut current = value;
    for segment in path.split('.') {
        let VariantValue::Object(fields) = current else {
            return None;
        };
        current = fields.get_mut(segment)?;
    }
    Some(current)
}

impl OeFile {
    pub fn payload_redacted(&self, row_ordinal: u64, redactions: &FieldRedactions) -> Result<PayloadRead, FormatError> {
        Ok(redactions.redact_payload(row_ordinal, self.payload(row_ordinal)?))
    }

    pub fn raw_payload_redacted(
        &self,
        row_ordinal: u64,
        redactions: &FieldRedactions,
    ) -> Result<Option<std::borrow::Cow<'_, [u8]>>, FormatError> {
        if redactions.withholds_raw_payload(row_ordinal) {
            return Ok(None);
        }
        self.raw_payload(row_ordinal)
    }
}

impl OeFileEntry {
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
