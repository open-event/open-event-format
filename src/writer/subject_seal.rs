//! Seals the payloads of rows that name a data subject before they are built into a file, so each such payload is
//! stored only as ciphertext under that subject's content key and destroying the key erases it.
//!
//! See: oe-security-and-isolation/spec.md

use crate::artifacts::batch::EncodedPayload;
use crate::error::SubjectPayloadError;
use crate::events::variant::VariantRef;
use crate::events::variant::{KeyDictionary, VariantValue, encode_object_fields_into, encode_value};
use crate::file::bytes::Writer;
use crate::security::{FieldSubjects, SubjectKeyStore, seal_subject_fields, seal_subject_payload};
use crate::writer::build::{BuildPayload, BuildRow, FieldName};
use std::collections::BTreeSet;

/// Seals the payload of every row that names a subject under that subject's key from `keys`, then clears the row's
/// subject. Call it on the rows before building them: the build refuses a row that still names a subject, so a payload
/// meant to be sealed is never written in the clear.
///
/// A sealed row's payload becomes opaque bytes, so none of its fields are shredded, promoted, or indexed; read it back
/// with [`read_subject_payload`](crate::security::read_subject_payload). A row with no payload has nothing to seal. A
/// row whose payload is an external reference keeps the reference as it is: the body it points at must be sealed
/// where it is stored.
///
/// Fails without building anything when a subject has been erased or the key store cannot answer; rows already sealed
/// stay sealed and the failing row keeps its payload and subject.
pub fn seal_subject_rows(rows: &mut [BuildRow], keys: &dyn SubjectKeyStore) -> Result<(), SubjectPayloadError> {
    for row in rows.iter_mut() {
        let Some(subject) = row.subject else {
            continue;
        };
        let encoded_here;
        let encoded = match &row.payload {
            BuildPayload::Encoded(encoded) => encoded,
            BuildPayload::ExternalRef(_) | BuildPayload::None => {
                row.subject = None;
                continue;
            }
            BuildPayload::Object(fields) => {
                encoded_here = encode_object(fields)?;
                &encoded_here
            }
            BuildPayload::Whole(value) => {
                encoded_here = encode(value)?;
                &encoded_here
            }
        };
        let sealed = seal_subject_payload(subject, row.envelope.event_id, encoded, keys)?;
        row.payload = BuildPayload::Whole(VariantValue::Binary(sealed));
        row.subject = None;
    }
    Ok(())
}

/// Seals, in each row, the payload fields its [`FieldSubjects`] names under each field's subject's key from `keys`
/// (per-field erasure for events that mention several subjects; see
/// [`seal_subject_fields`](crate::security::seal_subject_fields)). `field_subjects(i, row)` gives row `i`'s map; a row
/// with an empty map, no payload, or an external payload is left as it is. Only the named fields become opaque; the
/// rest of the payload is still shredded, promoted and indexed. Read rows back with
/// [`read_subject_fields`](crate::security::read_subject_fields).
///
/// Fails, leaving the failing row as it was, when a subject has been erased or the key store cannot answer.
pub fn seal_subject_field_rows(
    rows: &mut [BuildRow],
    mut field_subjects: impl FnMut(usize, &BuildRow) -> FieldSubjects,
    keys: &dyn SubjectKeyStore,
) -> Result<(), SubjectPayloadError> {
    for (index, row) in rows.iter_mut().enumerate() {
        let fields = field_subjects(index, row);
        if fields.is_empty() {
            continue;
        }
        let value = match &row.payload {
            BuildPayload::ExternalRef(_) | BuildPayload::None => continue,
            BuildPayload::Object(fields) => object_value(fields),
            BuildPayload::Whole(value) => value.clone(),
            BuildPayload::Encoded(encoded) => VariantRef::new(&encoded.bytes)
                .decode(&encoded.dictionary)
                .map_err(SubjectPayloadError::Format)?,
        };
        let sealed = seal_subject_fields(row.envelope.event_id, &value, &fields, keys)?;
        let VariantValue::Object(sealed) = sealed else {
            unreachable!("seal_subject_fields keeps an object an object")
        };
        row.payload = BuildPayload::Object(
            sealed
                .into_iter()
                .map(|(name, value)| (FieldName::from(name.as_str()), value))
                .collect(),
        );
    }
    Ok(())
}

fn object_value(fields: &[(FieldName, VariantValue)]) -> VariantValue {
    VariantValue::Object(
        fields
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect(),
    )
}

/// Encodes the object made of `fields`, giving the same bytes and dictionary as encoding the equivalent
/// [`VariantValue::Object`]. Fields already sorted and unique are encoded where they lie; otherwise they are normalized
/// the way an object map would hold them (sorted by name, the last of a repeated name wins).
fn encode_object(fields: &[(FieldName, VariantValue)]) -> Result<EncodedPayload, SubjectPayloadError> {
    if !fields.iter().zip(fields.iter().skip(1)).all(|(a, b)| *a.0 < *b.0) {
        return encode(&object_value(fields));
    }
    let mut keys = BTreeSet::new();
    let borrowed: Vec<(&str, &VariantValue)> = fields
        .iter()
        .map(|(name, value)| {
            keys.insert(&**name);
            value.collect_keys(&mut keys);
            (&**name, value)
        })
        .collect();
    let dictionary = KeyDictionary::build(keys.into_iter().map(str::to_owned));
    let mut out = Writer::new();
    encode_object_fields_into(&borrowed, &dictionary, &mut out).map_err(SubjectPayloadError::Format)?;
    Ok(EncodedPayload {
        bytes: out.into_bytes(),
        dictionary,
    })
}

/// Encodes `value` against a key dictionary of exactly the keys it uses.
fn encode(value: &VariantValue) -> Result<EncodedPayload, SubjectPayloadError> {
    let mut keys = BTreeSet::new();
    value.collect_keys(&mut keys);
    let dictionary = KeyDictionary::build(keys.into_iter().map(str::to_owned));
    let bytes = encode_value(value, &dictionary).map_err(SubjectPayloadError::Format)?;
    Ok(EncodedPayload { bytes, dictionary })
}

#[cfg(test)]
#[path = "test/subject_seal.rs"]
mod tests;
