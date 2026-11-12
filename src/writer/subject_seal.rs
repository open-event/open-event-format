use crate::artifacts::batch::EncodedPayload;
use crate::error::SubjectPayloadError;
use crate::events::variant::VariantRef;
use crate::events::variant::{KeyDictionary, VariantValue, encode_object_fields_into, encode_value};
use crate::file::bytes::Writer;
use crate::security::{FieldSubjects, SubjectKeyStore, seal_subject_fields, seal_subject_payload};
use crate::writer::build::{BuildPayload, BuildRow, FieldName};
use std::collections::BTreeSet;

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
        row.payload = BuildPayload::Object(sealed.into_iter().collect());
    }
    Ok(())
}

fn object_value(fields: &[(FieldName, VariantValue)]) -> VariantValue {
    VariantValue::Object(
        fields
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    )
}

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
