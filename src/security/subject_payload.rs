use super::constant::SEALED_PAYLOAD_MAGIC;
use crate::artifacts::batch::{EncodedPayload, decode_variant_dictionary, encode_variant_dictionary};
use crate::columns::column_ids;
use crate::deletes::{RebuiltPayload, SubjectContentKey, SubjectId, rebuild_payload};
use crate::encoding::ColumnData;
use crate::error::{FormatError, StorageError, SubjectPayloadError};
use crate::events::EventId;
use crate::events::variant::{VariantRef, VariantValue};
use crate::file::bytes::{Reader, Writer};
use crate::layout::reader::{OeFile, PayloadRead};

pub trait SubjectKeyStore {
    fn opening_key(&self, subject: SubjectId) -> Result<Option<SubjectContentKey>, StorageError>;

    fn sealing_key(&self, subject: SubjectId) -> Result<Option<SubjectContentKey>, StorageError>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubjectPayloadRead {
    Opened(VariantValue),
    Plain(PayloadRead),
    Rejected,
    Tombstone,
}

pub fn seal_subject_payload(
    subject: SubjectId,
    event_id: EventId,
    payload: &EncodedPayload,
    keys: &dyn SubjectKeyStore,
) -> Result<Vec<u8>, SubjectPayloadError> {
    let mut key = keys
        .sealing_key(subject)
        .map_err(SubjectPayloadError::KeyStore)?
        .ok_or(SubjectPayloadError::SubjectErased)?;
    if key.is_destroyed() {
        return Err(SubjectPayloadError::SubjectErased);
    }
    let dictionary = encode_variant_dictionary(&payload.dictionary);
    let dictionary_len = u32::try_from(dictionary.len()).map_err(|_| {
        SubjectPayloadError::Format(FormatError::Structural {
            rule: "a sealed payload's key dictionary fits in u32 bytes",
        })
    })?;
    let mut plaintext = Writer::with_capacity(4 + dictionary.len() + payload.bytes.len());
    plaintext.put_u32(dictionary_len);
    plaintext.put_slice(&dictionary);
    plaintext.put_slice(&payload.bytes);
    let event_id = event_id.uuid().as_u128();
    let sealed = key
        .encrypt(seal_block_id(event_id), event_id, plaintext.bytes())
        .ok_or(SubjectPayloadError::SealRefused)?;
    let mut stored = Writer::with_capacity(SEALED_PAYLOAD_MAGIC.len() + 16 + sealed.len());
    stored.put_slice(SEALED_PAYLOAD_MAGIC);
    stored.put_u128(subject.uuid().as_u128());
    stored.put_slice(&sealed);
    Ok(stored.into_bytes())
}

pub fn read_subject_payload(
    file: &OeFile,
    row_ordinal: u64,
    keys: &dyn SubjectKeyStore,
) -> Result<SubjectPayloadRead, SubjectPayloadError> {
    let payload = file.payload(row_ordinal).map_err(SubjectPayloadError::Format)?;
    if let PayloadRead::Value(VariantValue::Binary(stored)) = &payload
        && let Some((subject, sealed)) = split_sealed(stored)
    {
        let event_id = row_event_id(file, row_ordinal).map_err(SubjectPayloadError::Format)?;
        return open_sealed(subject, event_id, sealed, keys);
    }
    Ok(SubjectPayloadRead::Plain(payload))
}

fn seal_block_id(event_id: u128) -> u64 {
    event_id as u64
}

fn split_sealed(stored: &[u8]) -> Option<(SubjectId, &[u8])> {
    let rest = stored.strip_prefix(SEALED_PAYLOAD_MAGIC.as_slice())?;
    let mut reader = Reader::new(rest);
    let subject = reader.u128("sealed payload subject").ok()?;
    let sealed = reader.take(reader.remaining(), "sealed payload blob").ok()?;
    Some((SubjectId::from_uuid(uuid::Uuid::from_u128(subject)), sealed))
}

pub(crate) fn row_event_id(file: &OeFile, row_ordinal: u64) -> Result<u128, FormatError> {
    let granules = &file.footer().granules;
    let index = granules.partition_point(|granule| granule.first_row_ordinal <= row_ordinal);
    let granule = index
        .checked_sub(1)
        .and_then(|index| granules.get(index))
        .ok_or(FormatError::RefOutOfRange {
            what: "row ordinal beyond granule directory",
        })?;
    let event_ids = granule_event_ids(file, granule.granule_id)?;
    row_ordinal
        .checked_sub(granule.first_row_ordinal)
        .and_then(|row| usize::try_from(row).ok())
        .and_then(|row| event_ids.get(row))
        .copied()
        .ok_or(FormatError::RefOutOfRange {
            what: "row ordinal beyond its granule's event ids",
        })
}

fn open_sealed(
    subject: SubjectId,
    event_id: u128,
    sealed: &[u8],
    keys: &dyn SubjectKeyStore,
) -> Result<SubjectPayloadRead, SubjectPayloadError> {
    let Some(key) = keys.opening_key(subject).map_err(SubjectPayloadError::KeyStore)? else {
        return Ok(SubjectPayloadRead::Tombstone);
    };
    match rebuild_payload(&key, seal_block_id(event_id), event_id, sealed) {
        RebuiltPayload::Payload(plaintext) => decode_plaintext(&plaintext)
            .map(SubjectPayloadRead::Opened)
            .map_err(SubjectPayloadError::Format),
        RebuiltPayload::Rejected => Ok(SubjectPayloadRead::Rejected),
        RebuiltPayload::Tombstone => Ok(SubjectPayloadRead::Tombstone),
    }
}

pub(crate) fn decode_plaintext(plaintext: &[u8]) -> Result<VariantValue, FormatError> {
    let mut reader = Reader::new(plaintext);
    let dictionary_len = reader.u32("sealed payload dictionary length")? as usize;
    let dictionary = decode_variant_dictionary(reader.take(dictionary_len, "sealed payload dictionary")?)?;
    let value = reader.take(reader.remaining(), "sealed payload value")?;
    VariantRef::new(value).decode(&dictionary)
}

#[cfg(test)]
#[path = "test/subject_payload.rs"]
mod tests;

/// The EVENT_ID column of `granule_id`, decoded once and reused while consecutive rows stay in the same granule.
fn granule_event_ids(file: &OeFile, granule_id: u32) -> Result<std::sync::Arc<[u128]>, FormatError> {
    if let Some((cached_id, ids)) = file
        .event_ids
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        && *cached_id == granule_id
    {
        return Ok(ids.clone());
    }
    let read = file.read_column(column_ids::EVENT_ID, granule_id)?;
    let ColumnData::U128(event_ids) = &read.data else {
        return Err(FormatError::Structural {
            rule: "the event id column holds 128-bit ids",
        });
    };
    let ids: std::sync::Arc<[u128]> = event_ids.as_slice().into();
    *file.event_ids.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some((granule_id, ids.clone()));
    Ok(ids)
}
