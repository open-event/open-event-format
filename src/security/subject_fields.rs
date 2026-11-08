//! Per-field erasure for events that mention several data subjects: selected fields of one payload are each sealed
//! under the content key of the subject they belong to, so erasing one subject tombstones only that subject's fields
//! while the event, its envelope, its unlabelled fields, and every other subject's fields still read.
//!
//! Which field belongs to which subject is the caller's choice, normally driven by the data-classification PII labels
//! of the event's schema ([`FieldSubjects`] maps a field path to a subject). A sealed field's value is replaced in the
//! payload by a binary value holding a fixed marker, the subject id in the clear (so a reader knows which key to ask
//! for, and an erasure job knows which fields to hide without any key), and the sealed blob. The seal binds the event id
//! and the field path, so a blob copied onto another event or another field fails authentication instead of opening.
//!
//! Field paths are dot-separated names into nested objects (`"sender.email"`). A path the payload does not hold seals
//! nothing. Like a sealed whole payload, a sealed field is opaque bytes, so it is not shredded, promoted, or indexed as
//! its original type.
//!
//! See: oe-deletes-and-corrections/spec.md ("Per-subject erasure via crypto-shredding"),
//! oe-security-and-isolation/spec.md

use super::SubjectKeyStore;
use super::constant::{SEALED_FIELD_BLOCK_CONTEXT, SEALED_FIELD_MAGIC};
use super::subject_payload::{decode_plaintext, row_event_id};
use crate::deletes::{FieldDeletionVector, RebuiltPayload, SubjectId, rebuild_payload};
use crate::error::{FormatError, SubjectPayloadError};
use crate::events::EventId;
use crate::events::variant::{KeyDictionary, VariantValue, encode_value};
use crate::file::bytes::{Reader, Writer};
use crate::layout::reader::{OeFile, PayloadRead};
use std::collections::{BTreeMap, BTreeSet};

/// Which payload fields belong to which data subject: field path (dot-separated) to subject.
pub type FieldSubjects = BTreeMap<String, SubjectId>;

/// One field of a payload read through [`open_subject_fields`] whose subject has been erased.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactedField {
    pub path: String,
    pub subject: SubjectId,
}

/// A payload read back with its subject-sealed fields opened where their subjects' keys are live.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldPayloadRead {
    /// Paths of sealed fields whose blob failed authentication under a live key (tampered or moved); each reads as
    /// [`VariantValue::Null`].
    pub rejected: Vec<String>,
    /// Sealed fields whose subject has been erased; each is tombstoned to [`VariantValue::Null`] in `value`.
    pub redacted: Vec<RedactedField>,
    /// The payload with every live subject's fields restored and every erased subject's fields tombstoned.
    pub value: VariantValue,
}

/// Seals each field `fields` names in `payload` under its subject's key from `keys`, as fields of event `event_id`.
/// Returns the payload with those fields replaced by their sealed form; every other field is left as it was.
///
/// Fails with [`SubjectPayloadError::SubjectErased`] when a named subject has been erased (nothing new is ever written
/// for an erased subject), and with a format error when `fields` is non-empty but `payload` is not an object.
pub fn seal_subject_fields(
    event_id: EventId,
    payload: &VariantValue,
    fields: &FieldSubjects,
    keys: &dyn SubjectKeyStore,
) -> Result<VariantValue, SubjectPayloadError> {
    if fields.is_empty() {
        return Ok(payload.clone());
    }
    if !matches!(payload, VariantValue::Object(_)) {
        return Err(SubjectPayloadError::Format(FormatError::Structural {
            rule: "per-field subject sealing needs an object payload",
        }));
    }
    let mut sealed = payload.clone();
    for (path, subject) in fields {
        let Some(slot) = field_mut(&mut sealed, path) else {
            continue;
        };
        if split_sealed_field(slot).is_some() {
            continue;
        }
        let stored = seal_field(*subject, event_id, path, slot, keys)?;
        *slot = VariantValue::Binary(stored);
    }
    Ok(sealed)
}

/// Opens every subject-sealed field in `payload` (a payload of event `event_id`) with the subjects' keys from `keys`.
/// A field whose subject has been erased is tombstoned to `Null` and listed in [`FieldPayloadRead::redacted`]; the
/// rest of the payload reads as written.
pub fn open_subject_fields(
    event_id: EventId,
    payload: &VariantValue,
    keys: &dyn SubjectKeyStore,
) -> Result<FieldPayloadRead, SubjectPayloadError> {
    let mut read = FieldPayloadRead {
        rejected: Vec::new(),
        redacted: Vec::new(),
        value: VariantValue::Null,
    };
    let mut value = payload.clone();
    open_in_place(
        &mut value,
        &mut String::new(),
        event_id.uuid().as_u128(),
        keys,
        &mut read,
    )?;
    read.value = value;
    Ok(read)
}

/// Reads row `row_ordinal`'s payload from `file` with its subject-sealed fields opened (see [`open_subject_fields`]).
/// Returns `None` when the row's payload is absent or external; a payload with no sealed field reads unchanged.
pub fn read_subject_fields(
    file: &OeFile,
    row_ordinal: u64,
    keys: &dyn SubjectKeyStore,
) -> Result<Option<FieldPayloadRead>, SubjectPayloadError> {
    let PayloadRead::Value(value) = file.payload(row_ordinal).map_err(SubjectPayloadError::Format)? else {
        return Ok(None);
    };
    let event_id = row_event_id(file, row_ordinal).map_err(SubjectPayloadError::Format)?;
    open_subject_fields(EventId::from_uuid(uuid::Uuid::from_u128(event_id)), &value, keys).map(Some)
}

/// The subjects whose sealed fields `payload` holds, by field path. Needs no key: the subject id is stored in the
/// clear beside each sealed blob.
pub fn sealed_field_subjects(payload: &VariantValue) -> FieldSubjects {
    let mut out = FieldSubjects::new();
    collect_sealed(payload, &mut String::new(), &mut out);
    out
}

/// Builds the field-level deletion vector that hides `erased`'s fields in `file` at query time: every row holding a
/// field sealed for `erased` is marked affected, and every such field path is redacted. Rows only other subjects'
/// fields touch are not marked. Returns `None` when no row of the file holds a field sealed for `erased`.
pub fn field_deletion_vector_for_subject(
    file: &OeFile,
    erased: SubjectId,
) -> Result<Option<FieldDeletionVector>, FormatError> {
    let mut columns = BTreeSet::new();
    let mut ordinals = Vec::new();
    for granule in &file.footer().granules {
        for row in granule.first_row_ordinal..granule.first_row_ordinal + u64::from(granule.row_count) {
            let PayloadRead::Value(value) = file.payload(row)? else {
                continue;
            };
            let paths: Vec<String> = sealed_field_subjects(&value)
                .into_iter()
                .filter(|(_, subject)| *subject == erased)
                .map(|(path, _)| path)
                .collect();
            if !paths.is_empty() {
                ordinals.push(row);
                columns.extend(paths);
            }
        }
    }
    if ordinals.is_empty() {
        return Ok(None);
    }
    let mut vector = FieldDeletionVector::new(columns.into_iter().collect());
    ordinals.into_iter().for_each(|row| vector.mark_affected(row));
    Ok(Some(vector))
}

fn field_block_id(event_id: u128, path: &str) -> u64 {
    let mut hasher = blake3::Hasher::new_derive_key(SEALED_FIELD_BLOCK_CONTEXT);
    hasher.update(&event_id.to_le_bytes());
    hasher.update(path.as_bytes());
    let digest = hasher.finalize();
    let mut low = [0u8; 8];
    low.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(low)
}

fn seal_field(
    subject: SubjectId,
    event_id: EventId,
    path: &str,
    value: &VariantValue,
    keys: &dyn SubjectKeyStore,
) -> Result<Vec<u8>, SubjectPayloadError> {
    let mut key = keys
        .sealing_key(subject)
        .map_err(SubjectPayloadError::KeyStore)?
        .ok_or(SubjectPayloadError::SubjectErased)?;
    if key.is_destroyed() {
        return Err(SubjectPayloadError::SubjectErased);
    }
    let mut names = BTreeSet::new();
    value.collect_keys(&mut names);
    let dictionary = KeyDictionary::build(names.into_iter().map(str::to_owned));
    let bytes = encode_value(value, &dictionary).map_err(SubjectPayloadError::Format)?;
    let dictionary = crate::artifacts::batch::encode_variant_dictionary(&dictionary);
    let dictionary_len = u32::try_from(dictionary.len()).map_err(|_| {
        SubjectPayloadError::Format(FormatError::Structural {
            rule: "a sealed field's key dictionary fits in u32 bytes",
        })
    })?;
    let mut plaintext = Writer::with_capacity(4 + dictionary.len() + bytes.len());
    plaintext.put_u32(dictionary_len);
    plaintext.put_slice(&dictionary);
    plaintext.put_slice(&bytes);
    let event_id = event_id.uuid().as_u128();
    let sealed = key
        .encrypt(field_block_id(event_id, path), event_id, plaintext.bytes())
        .ok_or(SubjectPayloadError::SealRefused)?;
    let mut stored = Writer::with_capacity(SEALED_FIELD_MAGIC.len() + 16 + sealed.len());
    stored.put_slice(SEALED_FIELD_MAGIC);
    stored.put_u128(subject.uuid().as_u128());
    stored.put_slice(&sealed);
    Ok(stored.into_bytes())
}

/// The subject and blob of a sealed field value, or `None` for any other value.
fn split_sealed_field(value: &VariantValue) -> Option<(SubjectId, &[u8])> {
    let VariantValue::Binary(stored) = value else {
        return None;
    };
    let rest = stored.strip_prefix(SEALED_FIELD_MAGIC.as_slice())?;
    let mut reader = Reader::new(rest);
    let subject = reader.u128("sealed field subject").ok()?;
    let sealed = reader.take(reader.remaining(), "sealed field blob").ok()?;
    Some((SubjectId::from_uuid(uuid::Uuid::from_u128(subject)), sealed))
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

fn push_segment(path: &mut String, name: &str) -> usize {
    let len = path.len();
    if !path.is_empty() {
        path.push('.');
    }
    path.push_str(name);
    len
}

fn collect_sealed(value: &VariantValue, path: &mut String, out: &mut FieldSubjects) {
    if let Some((subject, _)) = split_sealed_field(value) {
        out.insert(path.clone(), subject);
        return;
    }
    if let VariantValue::Object(fields) = value {
        for (name, field) in fields {
            let len = push_segment(path, name);
            collect_sealed(field, path, out);
            path.truncate(len);
        }
    }
}

fn open_in_place(
    value: &mut VariantValue,
    path: &mut String,
    event_id: u128,
    keys: &dyn SubjectKeyStore,
    read: &mut FieldPayloadRead,
) -> Result<(), SubjectPayloadError> {
    if let Some((subject, sealed)) = split_sealed_field(value) {
        let opened = match keys.opening_key(subject).map_err(SubjectPayloadError::KeyStore)? {
            None => RebuiltPayload::Tombstone,
            Some(key) => rebuild_payload(&key, field_block_id(event_id, path), event_id, sealed),
        };
        *value = match opened {
            RebuiltPayload::Payload(plaintext) => decode_plaintext(&plaintext).map_err(SubjectPayloadError::Format)?,
            RebuiltPayload::Rejected => {
                read.rejected.push(path.clone());
                VariantValue::Null
            }
            RebuiltPayload::Tombstone => {
                read.redacted.push(RedactedField {
                    path: path.clone(),
                    subject,
                });
                VariantValue::Null
            }
        };
        return Ok(());
    }
    if let VariantValue::Object(fields) = value {
        for (name, field) in fields.iter_mut() {
            let len = push_segment(path, name);
            open_in_place(field, path, event_id, keys, read)?;
            path.truncate(len);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "test/subject_fields.rs"]
mod tests;
