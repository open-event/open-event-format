use super::error::QueueError;
use crate::artifacts::batch::{EncodedPayload, EventInput, PayloadInput};
use crate::error::FormatError;
use crate::events::provenance::{SignatureScheme, SignedEventProvenance};
use crate::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef};
use crate::events::variant::{EncodeScratch, KeyDictionary, VariantRef, encode_value_with_scratch};
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::file::bytes::{Reader, Writer};
use uuid::Uuid;

pub const RECORD_ALIGN: u64 = 64;

const PENDING_RECORD_CAPACITY_HINT: usize = 256;

const MIN_QUEUE_CAPACITY_BYTES: u64 = 1024;

const PROTOCOL_ID_BYTES: usize = 32;

const PROTOCOL_SIGNATURE_BYTES: usize = 64;

const RELATIONSHIP_REF_MIN_BYTES: usize = 8;

#[derive(Debug, Clone, PartialEq)]
pub struct PendingRecord {
    pub epoch: u64,
    pub event: EventInput,
    pub tenant_id: TenantId,
}

fn put_optional_string(out: &mut Writer, value: Option<&str>) {
    match value {
        None => out.put_u32(u32::MAX),
        Some(value) => {
            out.put_u32(value.len() as u32);
            out.put_slice(value.as_bytes());
        }
    }
}

fn read_optional_string(reader: &mut Reader<'_>) -> Result<Option<String>, FormatError> {
    let len = reader.u32("optional string length")?;
    if len == u32::MAX {
        return Ok(None);
    }
    let bytes = reader.take(len as usize, "optional string")?;
    let text = simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 {
        what: "pending record string",
    })?;
    Ok(Some(text.to_owned()))
}

fn read_required_string(reader: &mut Reader<'_>) -> Result<String, FormatError> {
    read_optional_string(reader)?.ok_or(FormatError::Structural {
        rule: "required string absent in pending record",
    })
}

fn put_encoded_payload(out: &mut Writer, dictionary: &KeyDictionary, bytes: &[u8]) {
    out.put_u32(dictionary.key_count());
    for key in dictionary.keys() {
        put_optional_string(out, Some(key));
    }
    out.put_u32(bytes.len() as u32);
    out.put_slice(bytes);
}

pub fn encode_pending(record: &PendingRecord) -> Result<Vec<u8>, FormatError> {
    let mut out = Writer::with_capacity(PENDING_RECORD_CAPACITY_HINT);
    encode_pending_into(record, &mut EncodeScratch::new(), &mut out)?;
    Ok(out.into_bytes())
}

fn encode_pending_into(
    record: &PendingRecord,
    scratch: &mut EncodeScratch,
    out: &mut Writer,
) -> Result<(), FormatError> {
    out.put_u128(record.tenant_id.uuid().as_u128());
    out.put_u64(record.epoch);
    let envelope = &record.event.envelope;
    out.put_u128(envelope.event_id.uuid().as_u128());
    out.put_u64(envelope.stream_id.0);
    out.put_u64(envelope.stream_sequence);
    out.put_i64(envelope.occurred_at.physical_nanos());
    out.put_i64(envelope.ingested_at.physical_nanos());
    out.put_u64(envelope.entity_id_hash_low);
    out.put_u64(envelope.entity_id_hash_high);
    out.put_u64(envelope.actor_id_hash_low);
    out.put_u64(envelope.account_id_hash_low);
    out.put_u64(envelope.trace_id_hash_low);
    out.put_u64(envelope.dedupe_hash_low);
    out.put_u64(envelope.dedupe_hash_high);
    out.put_u32(envelope.schema_version);
    out.put_u32(envelope.flags.0);
    out.put_u64(record.event.connector_delivery_hash_low);
    out.put_u64(record.event.connector_delivery_hash_high);
    put_optional_string(out, Some(&envelope.source));
    put_optional_string(out, Some(&envelope.event_type));
    put_optional_string(out, Some(&envelope.entity_type));
    put_optional_string(out, envelope.entity_id.as_deref());
    put_optional_string(out, envelope.actor_id.as_deref());
    put_optional_string(out, envelope.account_id.as_deref());
    put_optional_string(out, record.event.source_schema.as_deref());
    put_optional_string(out, record.event.source_delivery.as_deref());
    match &record.event.payload {
        PayloadInput::Encoded(payload) => {
            out.put_u8(1);
            put_encoded_payload(out, &payload.dictionary, &payload.bytes);
        }
        PayloadInput::None => out.put_u8(0),
        PayloadInput::Variant(value) => {
            out.put_u8(1);
            let mut keys = std::collections::BTreeSet::new();
            value.collect_keys(&mut keys);
            let dictionary = KeyDictionary::build(keys.into_iter().map(str::to_owned));
            let encoded = encode_value_with_scratch(value, &dictionary, scratch)?;
            put_encoded_payload(out, &dictionary, &encoded);
        }
        PayloadInput::ExternalRef(reference) => {
            out.put_u8(2);
            put_optional_string(out, Some(reference));
        }
    }
    match &record.event.provenance {
        None => out.put_u8(0),
        Some(provenance) => {
            out.put_u8(1);
            out.put_slice(&provenance.author_pubkey);
            out.put_slice(&provenance.protocol_event_id);
            out.put_slice(&provenance.signature);
            out.put_i64(provenance.claimed_at.physical_nanos());
            out.put_u32(provenance.protocol_kind);
            put_optional_string(out, Some(provenance.scheme.as_str()));
        }
    }
    match &record.event.relationships {
        None => out.put_u8(0),
        Some(relationships) => {
            out.put_u8(1);
            out.put_u32(relationships.refs().len() as u32);
            let mut scratch = String::new();
            for reference in relationships.refs() {
                put_optional_string(out, Some(reference.kind.as_str()));
                scratch.clear();
                reference.column_value_into(&mut scratch);
                put_optional_string(out, Some(&scratch));
            }
        }
    }
    Ok(())
}

fn peek_tenant_epoch(bytes: &[u8]) -> Option<(TenantId, u64)> {
    let tenant_id = TenantId::from_uuid(Uuid::from_u128(u128::from_le_bytes(bytes.get(0..16)?.try_into().ok()?)));
    let epoch = u64::from_le_bytes(bytes.get(16..24)?.try_into().ok()?);
    Some((tenant_id, epoch))
}

pub fn decode_pending(bytes: &[u8]) -> Result<PendingRecord, FormatError> {
    let mut reader = Reader::new(bytes);
    let tenant_id = TenantId::from_uuid(Uuid::from_u128(reader.u128("tenant_id")?));
    let epoch = reader.u64("epoch")?;
    let event_id = EventId::from_uuid(Uuid::from_u128(reader.u128("event_id")?));
    let stream_id = StreamId(reader.u64("stream_id")?);
    let stream_sequence = reader.u64("stream_sequence")?;
    let occurred_at = TimestampValue::from_physical_nanos(reader.i64("occurred_at")?);
    let ingested_at = TimestampValue::from_physical_nanos(reader.i64("ingested_at")?);
    let entity_id_hash_low = reader.u64("entity_id_hash_low")?;
    let entity_id_hash_high = reader.u64("entity_id_hash_high")?;
    let actor_id_hash_low = reader.u64("actor_id_hash_low")?;
    let account_id_hash_low = reader.u64("account_id_hash_low")?;
    let trace_id_hash_low = reader.u64("trace_id_hash_low")?;
    let dedupe_hash_low = reader.u64("dedupe_hash_low")?;
    let dedupe_hash_high = reader.u64("dedupe_hash_high")?;
    let schema_version = reader.u32("schema_version")?;
    let flags = EventFlags(reader.u32("flags")?);
    let connector_delivery_hash_low = reader.u64("connector_delivery_hash_low")?;
    let connector_delivery_hash_high = reader.u64("connector_delivery_hash_high")?;
    let source = read_required_string(&mut reader)?;
    let event_type = read_required_string(&mut reader)?;
    let entity_type = read_required_string(&mut reader)?;
    let entity_id = read_optional_string(&mut reader)?;
    let actor_id = read_optional_string(&mut reader)?;
    let account_id = read_optional_string(&mut reader)?;
    let source_schema = read_optional_string(&mut reader)?;
    let source_delivery = read_optional_string(&mut reader)?;
    let payload = match reader.u8("payload kind")? {
        0 => PayloadInput::None,
        1 => {
            let key_count = reader.u32("dictionary key count")?;
            let mut keys = Vec::with_capacity(key_count as usize);
            for _ in 0..key_count {
                keys.push(read_required_string(&mut reader)?);
            }
            let dictionary = KeyDictionary::from_sorted_unique(keys)?;
            let len = reader.u32("payload length")? as usize;
            let bytes = reader.take(len, "payload value")?;
            VariantRef::new(bytes).validate(&dictionary)?;
            PayloadInput::Encoded(EncodedPayload {
                bytes: bytes.to_vec(),
                dictionary,
            })
        }
        2 => PayloadInput::ExternalRef(read_required_string(&mut reader)?),
        _ => {
            return Err(FormatError::Structural {
                rule: "unknown pending payload kind",
            });
        }
    };
    let provenance = match reader.u8("provenance presence")? {
        0 => None,
        1 => {
            let author_pubkey = reader
                .take(PROTOCOL_ID_BYTES, "author_pubkey")?
                .try_into()
                .map_err(|_| FormatError::Truncated { what: "author_pubkey" })?;
            let protocol_event_id = reader
                .take(PROTOCOL_ID_BYTES, "protocol_event_id")?
                .try_into()
                .map_err(|_| FormatError::Truncated {
                    what: "protocol_event_id",
                })?;
            let signature = reader
                .take(PROTOCOL_SIGNATURE_BYTES, "signature")?
                .try_into()
                .map_err(|_| FormatError::Truncated { what: "signature" })?;
            let claimed_at = TimestampValue::from_physical_nanos(reader.i64("claimed_at")?);
            let protocol_kind = reader.u32("protocol_kind")?;
            let tag = read_required_string(&mut reader)?;
            let scheme = SignatureScheme::from_str(&tag).ok_or(FormatError::Structural {
                rule: "unknown signature scheme in a pending record",
            })?;
            Some(SignedEventProvenance {
                author_pubkey,
                claimed_at,
                protocol_event_id,
                protocol_kind,
                scheme,
                signature,
            })
        }
        _ => {
            return Err(FormatError::Structural {
                rule: "unknown pending provenance presence byte",
            });
        }
    };
    let relationships = match reader.u8("relationships presence")? {
        0 => None,
        1 => {
            let count = reader.u32("relationship count")? as usize;
            let mut refs = Vec::with_capacity(reader.capacity_hint(count, RELATIONSHIP_REF_MIN_BYTES));
            for _ in 0..count {
                let kind_tag = read_required_string(&mut reader)?;
                let kind = RelationshipKind::from_str(&kind_tag).ok_or(FormatError::Structural {
                    rule: "unknown relationship kind in a pending record",
                })?;
                let value = read_required_string(&mut reader)?;
                refs.push(
                    RelationshipRef::parse_column_value(kind, &value).map_err(|_| FormatError::Structural {
                        rule: "malformed relationship reference in a pending record",
                    })?,
                );
            }
            Some(EventRelationships::new(refs).map_err(|_| FormatError::Structural {
                rule: "an event may declare at most one parent and one root reference",
            })?)
        }
        _ => {
            return Err(FormatError::Structural {
                rule: "unknown pending relationships presence byte",
            });
        }
    };
    Ok(PendingRecord {
        tenant_id,
        epoch,
        event: EventInput {
            envelope: EventEnvelope {
                event_id,
                tenant_id,
                stream_id,
                stream_sequence,
                occurred_at,
                ingested_at,
                source,
                event_type,
                entity_type,
                entity_id_hash_low,
                entity_id_hash_high,
                entity_id,
                actor_id_hash_low,
                actor_id,
                account_id_hash_low,
                account_id,
                trace_id_hash_low,
                dedupe_hash_low,
                dedupe_hash_high,
                schema_version,
                flags,
            },
            payload,
            source_schema,
            source_delivery,
            connector_delivery_hash_low,
            connector_delivery_hash_high,
            provenance,
            relationships,
        },
    })
}

#[derive(Debug)]
pub struct QueueAdmission {
    bytes: Vec<u8>,
    spans: Vec<(usize, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueRegion {
    pub end: u64,
    pub start: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct RawRecords<'a> {
    cursor: u64,
    end: u64,
    queue: &'a CommitQueue,
}

impl<'a> Iterator for RawRecords<'a> {
    type Item = Result<&'a [u8], QueueError>;

    fn next(&mut self) -> Option<Self::Item> {
        let queue = self.queue;
        while self.cursor < self.end {
            let len = queue.read_u64_at(self.cursor);
            if len == SKIP_MARKER {
                self.cursor += queue.capacity - (self.cursor % queue.capacity);
                continue;
            }
            let offset = queue.offset_of(self.cursor + LEN_HEADER);
            let Some(bytes) = queue.buffer.get(offset..offset + len as usize) else {
                self.cursor = self.end;
                return Some(Err(QueueError::Codec));
            };
            self.cursor += (LEN_HEADER + len).next_multiple_of(RECORD_ALIGN);
            return Some(Ok(bytes));
        }
        None
    }
}

#[derive(Debug)]
pub struct CommitQueue {
    buffer: Vec<u8>,
    capacity: u64,
    clean_cursor: u64,
    dirty_cursor: u64,
    hardened_cursor: u64,
    hardened_pending: Vec<QueueRegion>,
    head: u64,
    in_flight: Vec<QueueRegion>,
    push_encode_scratch: EncodeScratch,
    push_scratch: Vec<u8>,
}

const LEN_HEADER: u64 = 8;
const SKIP_MARKER: u64 = u64::MAX;

impl CommitQueue {
    pub fn new(capacity: usize) -> Self {
        let capacity = (capacity as u64)
            .next_multiple_of(RECORD_ALIGN)
            .max(MIN_QUEUE_CAPACITY_BYTES);
        Self {
            buffer: vec![0; capacity as usize],
            capacity,
            head: 0,
            dirty_cursor: 0,
            clean_cursor: 0,
            hardened_cursor: 0,
            hardened_pending: Vec::new(),
            in_flight: Vec::new(),
            push_encode_scratch: EncodeScratch::new(),
            push_scratch: Vec::with_capacity(PENDING_RECORD_CAPACITY_HINT),
        }
    }

    fn offset_of(&self, cursor: u64) -> usize {
        (cursor % self.capacity) as usize
    }

    fn write_at(&mut self, cursor: u64, bytes: &[u8]) {
        let offset = self.offset_of(cursor);
        if let Some(target) = self.buffer.get_mut(offset..offset + bytes.len()) {
            target.copy_from_slice(bytes);
        }
    }

    fn read_u64_at(&self, cursor: u64) -> u64 {
        let offset = self.offset_of(cursor);
        self.buffer
            .get(offset..offset + 8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])))
            .unwrap_or(0)
    }

    pub fn pending_bytes(&self) -> u64 {
        self.dirty_cursor - self.clean_cursor
    }

    pub fn dirty_cursor(&self) -> u64 {
        self.dirty_cursor
    }

    pub fn clean_cursor(&self) -> u64 {
        self.clean_cursor
    }

    pub fn hardened_cursor(&self) -> u64 {
        self.hardened_cursor
    }

    fn placement(&self, cursor: u64, payload_len: usize) -> Option<(u64, u64)> {
        let record_len = (LEN_HEADER + payload_len as u64).next_multiple_of(RECORD_ALIGN);
        if record_len > self.capacity {
            return None;
        }
        let to_wrap = self.capacity - (cursor % self.capacity);
        let start = if record_len > to_wrap { cursor + to_wrap } else { cursor };
        if start + record_len - self.head > self.capacity {
            return None;
        }
        Some((start, start + record_len))
    }

    fn push_encoded(&mut self, payload: &[u8]) -> Result<(), QueueError> {
        self.advance_head();
        let cursor = self.dirty_cursor;
        let Some((start, end)) = self.placement(cursor, payload.len()) else {
            return Err(QueueError::Full);
        };
        if start != cursor {
            self.write_at(cursor, &SKIP_MARKER.to_le_bytes());
        }
        self.write_at(start, &(payload.len() as u64).to_le_bytes());
        self.write_at(start + LEN_HEADER, payload);
        self.dirty_cursor = end;
        Ok(())
    }

    pub fn push(&mut self, record: &PendingRecord) -> Result<(), QueueError> {
        let mut scratch = std::mem::take(&mut self.push_scratch);
        scratch.clear();
        let mut out = Writer::from_vec(scratch);
        let encoded =
            encode_pending_into(record, &mut self.push_encode_scratch, &mut out).map_err(|_| QueueError::Codec);
        let payload = out.into_bytes();
        let result = encoded.and_then(|()| self.push_encoded(&payload));
        self.push_scratch = payload;
        result
    }

    pub fn copy_pending(&self) -> Result<(QueueRegion, Vec<PendingRecord>), QueueError> {
        let start = self.clean_cursor;
        let end = self.dirty_cursor;
        let mut records = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let len = self.read_u64_at(cursor);
            if len == SKIP_MARKER {
                cursor += self.capacity - (cursor % self.capacity);
                continue;
            }
            let offset = self.offset_of(cursor + LEN_HEADER);
            let bytes = self
                .buffer
                .get(offset..offset + len as usize)
                .ok_or(QueueError::Codec)?;
            records.push(decode_pending(bytes).map_err(|_| QueueError::Codec)?);
            cursor += (LEN_HEADER + len).next_multiple_of(RECORD_ALIGN);
        }
        Ok((QueueRegion { start, end }, records))
    }

    pub fn copy_pending_raw(&self) -> Result<(QueueRegion, Vec<&[u8]>), QueueError> {
        let (region, records) = self.pending_raw();
        Ok((region, records.collect::<Result<_, _>>()?))
    }

    pub(crate) fn pending_raw(&self) -> (QueueRegion, RawRecords<'_>) {
        let region = QueueRegion {
            end: self.dirty_cursor,
            start: self.clean_cursor,
        };
        let records = RawRecords {
            cursor: region.start,
            end: region.end,
            queue: self,
        };
        (region, records)
    }

    pub fn copy_matching_prefix(
        &self,
        tenant_id: TenantId,
        epoch: u64,
        limit_bytes: u64,
    ) -> Result<(QueueRegion, Vec<PendingRecord>), QueueError> {
        let start = self.clean_cursor;
        let end = self.dirty_cursor;
        let mut records = Vec::new();
        let mut cursor = start;
        while cursor < end && cursor - start < limit_bytes {
            let len = self.read_u64_at(cursor);
            if len == SKIP_MARKER {
                cursor += self.capacity - (cursor % self.capacity);
                continue;
            }
            let offset = self.offset_of(cursor + LEN_HEADER);
            let bytes = self
                .buffer
                .get(offset..offset + len as usize)
                .ok_or(QueueError::Codec)?;
            let record = decode_pending(bytes).map_err(|_| QueueError::Codec)?;
            if record.tenant_id != tenant_id || record.epoch != epoch {
                break;
            }
            records.push(record);
            cursor += (LEN_HEADER + len).next_multiple_of(RECORD_ALIGN);
        }
        Ok((QueueRegion { start, end: cursor }, records))
    }

    pub fn commit_claim(&mut self, region: QueueRegion) -> bool {
        if self.clean_cursor != region.start {
            return false;
        }
        self.clean_cursor = region.end;
        true
    }

    pub fn claim_for_flush(&mut self, region: QueueRegion) -> bool {
        if self.commit_claim(region) {
            self.in_flight.push(region);
            true
        } else {
            false
        }
    }

    pub fn admit(&self, records: &[PendingRecord]) -> Result<Option<QueueAdmission>, QueueError> {
        let mut scratch = EncodeScratch::new();
        self.admit_placed(
            records.len(),
            records.len() * PENDING_RECORD_CAPACITY_HINT,
            |index, out| {
                let record = records.get(index).ok_or(QueueError::Codec)?;
                encode_pending_into(record, &mut scratch, out).map_err(|_| QueueError::Codec)
            },
        )
    }

    pub fn admit_encoded(&self, records: &[&[u8]]) -> Result<Option<QueueAdmission>, QueueError> {
        let Some(total_len) = self.fits_whole(records.iter().map(|record| Ok(record.len())))? else {
            return Ok(None);
        };
        self.admit_placed(records.len(), total_len, |index, out| {
            out.put_slice(records.get(index).ok_or(QueueError::Codec)?);
            Ok(())
        })
    }

    pub(crate) fn admit_raw(&self, records: RawRecords<'_>) -> Result<Option<(QueueAdmission, usize)>, QueueError> {
        let mut count = 0;
        let lengths = records.clone().map(|record| {
            count += 1;
            record.map(<[u8]>::len)
        });
        let Some(total_len) = self.fits_whole(lengths)? else {
            return Ok(None);
        };
        let mut replay = records;
        let admission = self.admit_placed(count, total_len, |_, out| {
            out.put_slice(replay.next().ok_or(QueueError::Codec)??);
            Ok(())
        })?;
        Ok(admission.map(|admission| (admission, count)))
    }

    fn fits_whole(
        &self,
        lengths: impl IntoIterator<Item = Result<usize, QueueError>>,
    ) -> Result<Option<usize>, QueueError> {
        let mut cursor = self.dirty_cursor;
        let mut total_len = 0usize;
        for len in lengths {
            let len = len?;
            let Some((_, next)) = self.placement(cursor, len) else {
                return Ok(None);
            };
            cursor = next;
            total_len = total_len.checked_add(len).ok_or(QueueError::Codec)?;
        }
        Ok(Some(total_len))
    }

    fn admit_placed(
        &self,
        count: usize,
        capacity_hint: usize,
        mut write_one: impl FnMut(usize, &mut Writer) -> Result<(), QueueError>,
    ) -> Result<Option<QueueAdmission>, QueueError> {
        let mut bytes = Writer::with_capacity(capacity_hint);
        let mut spans = Vec::with_capacity(count);
        let mut cursor = self.dirty_cursor;
        for index in 0..count {
            let start = bytes.len();
            write_one(index, &mut bytes)?;
            let end = bytes.len();
            let Some((_, next)) = self.placement(cursor, end - start) else {
                return Ok(None);
            };
            cursor = next;
            spans.push((start, end));
        }
        Ok(Some(QueueAdmission {
            bytes: bytes.into_bytes(),
            spans,
        }))
    }

    pub fn push_admitted(&mut self, admission: &QueueAdmission) -> Result<(), QueueError> {
        for (start, end) in &admission.spans {
            let payload = admission.bytes.get(*start..*end).ok_or(QueueError::Codec)?;
            self.push_encoded(payload)?;
        }
        Ok(())
    }

    pub fn can_admit(&self, records: &[PendingRecord]) -> Result<bool, QueueError> {
        Ok(self.admit(records)?.is_some())
    }

    pub fn mark_hardened(&mut self, region: QueueRegion) {
        self.in_flight.retain(|pending| *pending != region);
        self.hardened_pending.push(region);
        self.drain_hardened();
    }

    pub fn release_hardened(&mut self) {
        self.advance_head();
    }

    pub fn abort_flush(&mut self, region: QueueRegion) {
        self.in_flight.retain(|pending| *pending != region);
        self.drain_hardened();
        self.advance_head();
    }

    fn drain_hardened(&mut self) {
        let limit = self
            .in_flight
            .iter()
            .map(|region| region.start)
            .min()
            .unwrap_or(self.clean_cursor);
        self.hardened_cursor = self.hardened_cursor.max(limit);
        self.hardened_pending.retain(|region| region.end > self.hardened_cursor);
    }

    fn advance_head(&mut self) {
        let clean = self.clean_cursor;
        let barrier = self
            .in_flight
            .iter()
            .map(|region| region.start)
            .filter(|&start| start >= self.head)
            .min()
            .unwrap_or(clean);
        self.head = self.head.max(barrier.min(clean));
    }
}

pub fn peek_steal(
    target: &CommitQueue,
    frame_tenant: TenantId,
    frame_epoch: u64,
    same_steal_group: bool,
) -> Option<(QueueRegion, Vec<&[u8]>)> {
    let (region, records) = peek_steal_raw(target, frame_tenant, frame_epoch, same_steal_group)?;
    Some((region, records.collect::<Result<_, _>>().ok()?))
}

pub(crate) fn peek_steal_raw(
    target: &CommitQueue,
    frame_tenant: TenantId,
    frame_epoch: u64,
    same_steal_group: bool,
) -> Option<(QueueRegion, RawRecords<'_>)> {
    if !same_steal_group {
        return None;
    }
    let (region, records) = target.pending_raw();
    if region.start == region.end {
        return None;
    }
    for record in records.clone() {
        if peek_tenant_epoch(record.ok()?) != Some((frame_tenant, frame_epoch)) {
            return None;
        }
    }
    Some((region, records))
}

#[cfg(test)]
#[path = "test/queue.rs"]
mod tests;
