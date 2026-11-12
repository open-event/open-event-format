use super::constant::{AUDIT_ROW_HASHER, ROW_DIGEST_INPUT_CAPACITY};
use super::event::read_u64s;
use super::footer::AuditTrail;
use super::reader::{OeFile, PayloadRead};
use crate::artifacts::batch::encode_variant_dictionary_into;
use crate::columns::column_ids;
use crate::error::{AuditChainError, FormatError};
use crate::events::EventEnvelope;
use crate::events::variant::{KeyDictionary, VariantValue, encode_object_fields_into, encode_value_into};
use crate::file::bytes::Writer;
use std::cell::RefCell;
use std::collections::BTreeSet;

pub fn verify_audit_files(chain_start: [u8; 32], files: &[OeFile]) -> Result<[u8; 32], AuditChainError> {
    let mut expected = chain_start;
    for (file_index, file) in files.iter().enumerate() {
        let Some(trail) = file.footer().audit else {
            return Err(AuditChainError::NotAudit { file_index });
        };
        if trail.chain_start != expected {
            return Err(AuditChainError::ChainBreak { file_index });
        }
        let recomputed =
            recomputed_chain_end(file, &trail).map_err(|error| AuditChainError::Format { error, file_index })?;
        if recomputed != trail.chain_end {
            return Err(AuditChainError::RowsAltered { file_index });
        }
        expected = trail.chain_end;
    }
    Ok(expected)
}

pub(crate) enum DigestPayload<'a> {
    External(&'a str),
    None,
    Object(&'a [(&'a str, &'a VariantValue)]),
    Value(&'a VariantValue),
}

impl<'a> From<&'a PayloadRead> for DigestPayload<'a> {
    fn from(payload: &'a PayloadRead) -> Self {
        match payload {
            PayloadRead::External(reference) => DigestPayload::External(reference),
            PayloadRead::None => DigestPayload::None,
            PayloadRead::Value(value) => DigestPayload::Value(value),
        }
    }
}

pub(crate) struct DigestEnvelope<'a> {
    pub(crate) account_id: Option<&'a str>,
    pub(crate) account_id_hash_low: u64,
    pub(crate) actor_id: Option<&'a str>,
    pub(crate) actor_id_hash_low: u64,
    pub(crate) dedupe_hash_high: u64,
    pub(crate) dedupe_hash_low: u64,
    pub(crate) entity_id: Option<&'a str>,
    pub(crate) entity_id_hash_high: u64,
    pub(crate) entity_id_hash_low: u64,
    pub(crate) entity_type: &'a str,
    pub(crate) event_id: u128,
    pub(crate) event_type: &'a str,
    pub(crate) flags: u32,
    pub(crate) ingested_at: i64,
    pub(crate) occurred_at: i64,
    pub(crate) schema_version: u32,
    pub(crate) source: &'a str,
    pub(crate) stream_id: u64,
    pub(crate) stream_sequence: u64,
    pub(crate) tenant_id: u128,
    pub(crate) trace_id_hash_low: u64,
}

impl<'a> From<&'a EventEnvelope> for DigestEnvelope<'a> {
    fn from(envelope: &'a EventEnvelope) -> Self {
        DigestEnvelope {
            account_id: envelope.account_id.as_deref(),
            account_id_hash_low: envelope.account_id_hash_low,
            actor_id: envelope.actor_id.as_deref(),
            actor_id_hash_low: envelope.actor_id_hash_low,
            dedupe_hash_high: envelope.dedupe_hash_high,
            dedupe_hash_low: envelope.dedupe_hash_low,
            entity_id: envelope.entity_id.as_deref(),
            entity_id_hash_high: envelope.entity_id_hash_high,
            entity_id_hash_low: envelope.entity_id_hash_low,
            entity_type: &envelope.entity_type,
            event_id: envelope.event_id.uuid().as_u128(),
            event_type: &envelope.event_type,
            flags: envelope.flags.0,
            ingested_at: envelope.ingested_at.physical_nanos(),
            occurred_at: envelope.occurred_at.physical_nanos(),
            schema_version: envelope.schema_version,
            source: &envelope.source,
            stream_id: envelope.stream_id.0,
            stream_sequence: envelope.stream_sequence,
            tenant_id: envelope.tenant_id.uuid().as_u128(),
            trace_id_hash_low: envelope.trace_id_hash_low,
        }
    }
}

pub(crate) fn row_digest(
    out: &mut Writer,
    epoch: u64,
    sequence: u64,
    envelope: &EventEnvelope,
    payload: DigestPayload<'_>,
) -> Result<[u8; 32], FormatError> {
    envelope_row_digest(out, epoch, sequence, &envelope.into(), payload)
}

pub(crate) fn envelope_row_digest(
    out: &mut Writer,
    epoch: u64,
    sequence: u64,
    envelope: &DigestEnvelope<'_>,
    payload: DigestPayload<'_>,
) -> Result<[u8; 32], FormatError> {
    out.clear();
    out.put_u64(epoch);
    out.put_u64(sequence);
    put_optional_text(out, envelope.account_id);
    out.put_u64(envelope.account_id_hash_low);
    put_optional_text(out, envelope.actor_id);
    out.put_u64(envelope.actor_id_hash_low);
    out.put_u64(envelope.dedupe_hash_high);
    out.put_u64(envelope.dedupe_hash_low);
    put_optional_text(out, envelope.entity_id);
    out.put_u64(envelope.entity_id_hash_high);
    out.put_u64(envelope.entity_id_hash_low);
    put_text(out, envelope.entity_type);
    out.put_u128(envelope.event_id);
    put_text(out, envelope.event_type);
    out.put_u32(envelope.flags);
    out.put_i64(envelope.ingested_at);
    out.put_i64(envelope.occurred_at);
    out.put_u32(envelope.schema_version);
    put_text(out, envelope.source);
    out.put_u64(envelope.stream_id);
    out.put_u64(envelope.stream_sequence);
    out.put_u128(envelope.tenant_id);
    out.put_u64(envelope.trace_id_hash_low);
    match payload {
        DigestPayload::None => out.put_u8(0),
        DigestPayload::External(reference) => {
            out.put_u8(1);
            put_text(out, reference);
        }
        DigestPayload::Object(fields) => {
            out.put_u8(2);
            let mut keys = BTreeSet::new();
            for (name, value) in fields {
                keys.insert(*name);
                value.collect_keys(&mut keys);
            }
            with_dictionary(out, keys, |dictionary, out| {
                encode_object_fields_into(fields, dictionary, out)
            })?;
        }
        DigestPayload::Value(value) => {
            out.put_u8(2);
            let mut keys = BTreeSet::new();
            value.collect_keys(&mut keys);
            with_dictionary(out, keys, |dictionary, out| encode_value_into(value, dictionary, out))?;
        }
    }
    Ok(*blake3::hash(out.bytes()).as_bytes())
}

thread_local! {
    static ROW_DICTIONARY: RefCell<KeyDictionary> = RefCell::new(KeyDictionary::default());
}

fn with_dictionary(
    out: &mut Writer,
    keys: BTreeSet<&str>,
    encode: impl FnOnce(&KeyDictionary, &mut Writer) -> Result<(), FormatError>,
) -> Result<(), FormatError> {
    ROW_DICTIONARY.with_borrow_mut(|dictionary| {
        if !keys.iter().copied().eq(dictionary.keys()) {
            *dictionary = KeyDictionary::from_sorted_unique(keys.into_iter().map(str::to_owned).collect())?;
        }
        encode_variant_dictionary_into(dictionary, out);
        encode(dictionary, out)
    })
}

pub(crate) fn row_hash(prev_row_hash: &[u8; 32], row_digest: &[u8; 32]) -> [u8; 32] {
    let mut hasher = AUDIT_ROW_HASHER.clone();
    hasher.update(prev_row_hash);
    hasher.update(row_digest);
    *hasher.finalize().as_bytes()
}

fn recomputed_chain_end(file: &OeFile, trail: &AuditTrail) -> Result<[u8; 32], FormatError> {
    let mut hash = trail.chain_start;
    let mut out = Writer::with_capacity(ROW_DIGEST_INPUT_CAPACITY);
    for granule in &file.footer().granules {
        let rows = granule.row_count as usize;
        let envelopes = file.granule_envelopes(granule.granule_id)?;
        let epochs = read_u64s(file, column_ids::EPOCH, granule.granule_id, rows)?;
        let sequences = read_u64s(file, column_ids::SEQUENCE, granule.granule_id, rows)?;
        for (ordinal, ((envelope, epoch), sequence)) in
            (granule.first_row_ordinal..).zip(envelopes.iter().zip(epochs).zip(sequences))
        {
            let payload = file.payload(ordinal)?;
            hash = row_hash(
                &hash,
                &row_digest(&mut out, epoch, sequence, envelope, (&payload).into())?,
            );
        }
    }
    Ok(hash)
}

fn put_text(out: &mut Writer, text: &str) {
    out.put_u32(text.len() as u32);
    out.put_slice(text.as_bytes());
}

fn put_optional_text(out: &mut Writer, text: Option<&str>) {
    match text {
        Some(text) => {
            out.put_u8(1);
            put_text(out, text);
        }
        None => out.put_u8(0),
    }
}

#[cfg(test)]
#[path = "test/audit.rs"]
mod tests;
