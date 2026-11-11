use super::{
    BLAKE3_LEN, FRAME_ALIGN, FRAME_HEADER_LEN, MAGIC_LEN, MAX_LARGE_FRAME, MAX_NORMAL_FRAME, NORMAL_FRAME_SIZES,
    PAYLOAD_ENCODING_COMPACT_BATCH_V1,
};
use crate::error::FormatError;
use crate::events::TenantId;
use crate::file::bytes::{Reader, Writer, slice};
use crate::file::integrity::PARALLEL_HASH_MIN_LEN;
use uuid::Uuid;

pub const FLAG_LARGE_EVENT_FRAME: u32 = 1;
pub const FLAG_VOID_RECORD: u32 = 2;

const RESERVED_FLAG_MASK: u32 = !(FLAG_LARGE_EVENT_FRAME | FLAG_VOID_RECORD);

const HEADER_CRC64_OFFSET: usize = 128;

const RESERVED_TAIL_LEN: usize = 24;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OejFrameHeaderV1 {
    pub committed_at_physical: i64,
    pub created_at_physical: i64,
    pub dictionary_generation_hint: u64,
    pub durable_batch_id: u64,
    pub epoch: u64,
    pub event_count: u32,
    pub first_sequence: u64,
    pub flags: u32,
    pub frame_blake3: [u8; BLAKE3_LEN],
    pub frame_len: u32,
    pub header_crc64: u64,
    pub last_sequence: u64,
    pub page_count: u32,
    pub payload_encoding: u32,
    pub payload_len: u32,
    pub schema_generation: u64,
    pub tenant_id: TenantId,
    pub writer_id: u32,
    pub writer_local_batch_id: u64,
}

impl OejFrameHeaderV1 {
    pub fn is_large_event_frame(&self) -> bool {
        self.flags & FLAG_LARGE_EVENT_FRAME != 0
    }

    pub fn is_void_record(&self) -> bool {
        self.flags & FLAG_VOID_RECORD != 0
    }
}

#[derive(Debug, Clone)]
pub struct FrameBuildInput {
    pub committed_at_physical: i64,
    pub created_at_physical: i64,
    pub dictionary_generation_hint: u64,
    pub durable_batch_id: u64,
    pub epoch: u64,
    pub event_count: u32,
    pub first_sequence: u64,
    pub flags: u32,
    pub last_sequence: u64,
    pub schema_generation: u64,
    pub tenant_id: TenantId,
    pub writer_id: u32,
    pub writer_local_batch_id: u64,
}

fn encode_header_without_checks(header: &OejFrameHeaderV1, out: &mut Writer) {
    out.put_slice(b"OEJ1");
    out.put_u16(1);
    out.put_u16(FRAME_HEADER_LEN as u16);
    out.put_u32(header.frame_len);
    out.put_u32(header.payload_len);
    out.put_u32(header.page_count);
    out.put_u32(header.flags);
    out.put_u128(header.tenant_id.uuid().as_u128());
    out.put_u32(header.writer_id);
    out.put_u32(0);
    out.put_u64(header.epoch);
    out.put_u64(header.first_sequence);
    out.put_u64(header.last_sequence);
    out.put_u32(header.event_count);
    out.put_u32(header.payload_encoding);
    out.put_u64(header.durable_batch_id);
    out.put_u64(header.writer_local_batch_id);
    out.put_u64(header.schema_generation);
    out.put_u64(header.dictionary_generation_hint);
    out.put_i64(header.created_at_physical);
    out.put_i64(header.committed_at_physical);
    out.put_u64(0);
    out.put_slice(&[0u8; BLAKE3_LEN]);
    out.put_slice(&[0u8; RESERVED_TAIL_LEN]);
}

pub fn frame_len_for_payload(payload_len: u32, large: bool) -> Result<u32, FormatError> {
    let needed = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(FormatError::Structural {
            rule: "payload too large",
        })?;
    if large {
        if needed > MAX_LARGE_FRAME {
            return Err(FormatError::Structural {
                rule: "large-event frame exceeds 1 MiB",
            });
        }
        Ok(needed.div_ceil(FRAME_ALIGN) * FRAME_ALIGN)
    } else {
        NORMAL_FRAME_SIZES
            .iter()
            .copied()
            .find(|size| *size >= needed)
            .ok_or(FormatError::Structural {
                rule: "payload exceeds the 64 KiB normal-frame bound",
            })
    }
}

pub fn build_frame(input: &FrameBuildInput, payload: &[u8]) -> Result<Vec<u8>, FormatError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| FormatError::Structural {
        rule: "payload too large",
    })?;
    let large = input.flags & FLAG_LARGE_EVENT_FRAME != 0;
    let frame_len = frame_len_for_payload(payload_len, large)?;
    let header = OejFrameHeaderV1 {
        frame_len,
        payload_len,
        page_count: frame_len / FRAME_ALIGN,
        flags: input.flags,
        tenant_id: input.tenant_id,
        writer_id: input.writer_id,
        epoch: input.epoch,
        first_sequence: input.first_sequence,
        last_sequence: input.last_sequence,
        event_count: input.event_count,
        payload_encoding: PAYLOAD_ENCODING_COMPACT_BATCH_V1,
        durable_batch_id: input.durable_batch_id,
        writer_local_batch_id: input.writer_local_batch_id,
        schema_generation: input.schema_generation,
        dictionary_generation_hint: input.dictionary_generation_hint,
        created_at_physical: input.created_at_physical,
        committed_at_physical: input.committed_at_physical,
        header_crc64: 0,
        frame_blake3: [0; BLAKE3_LEN],
    };
    let mut out = Writer::with_capacity(frame_len as usize);
    encode_header_without_checks(&header, &mut out);
    out.put_slice(payload);
    let mut frame = out.into_bytes();
    frame.resize(frame_len as usize, 0);

    let crc = crate::file::integrity::crc64_nvme(frame.get(..FRAME_HEADER_LEN as usize).unwrap_or_default());
    splice_u64(&mut frame, HEADER_CRC64_OFFSET, crc);
    let digest = crate::file::integrity::hash_tree(&frame);
    if let Some(target) = frame.get_mut(136..168) {
        target.copy_from_slice(digest.as_bytes());
    }
    Ok(frame)
}

fn splice_u64(frame: &mut [u8], offset: usize, value: u64) {
    if let Some(target) = frame.get_mut(offset..offset + 8) {
        target.copy_from_slice(&value.to_le_bytes());
    }
}

pub fn precheck_header(frame: &[u8]) -> Result<OejFrameHeaderV1, FormatError> {
    let header_bytes = slice(frame, 0, FRAME_HEADER_LEN as usize, "frame header")?;
    let mut reader = Reader::new(header_bytes);
    let magic = reader.take(MAGIC_LEN, "magic")?;
    if magic != b"OEJ1" {
        return Err(FormatError::BadMagic { expected: "OEJ1" });
    }
    let version = reader.u16("version")?;
    if version != 1 {
        return Err(FormatError::UnsupportedVersion {
            field: "OEJFrameHeaderV1.version",
            found: u32::from(version),
        });
    }
    let header_len = reader.u16("header_len")?;
    if u32::from(header_len) != FRAME_HEADER_LEN {
        return Err(FormatError::Structural {
            rule: "header_len must be 192",
        });
    }
    let frame_len = reader.u32("frame_len")?;
    let payload_len = reader.u32("payload_len")?;
    let page_count = reader.u32("page_count")?;
    let flags = reader.u32("flags")?;
    let tenant_id = TenantId::from_uuid(Uuid::from_u128(reader.u128("tenant_id")?));
    let writer_id = reader.u32("writer_id")?;
    let reserved_zero_0 = reader.u32("reserved_zero_0")?;
    let epoch = reader.u64("epoch")?;
    let first_sequence = reader.u64("first_sequence")?;
    let last_sequence = reader.u64("last_sequence")?;
    let event_count = reader.u32("event_count")?;
    let payload_encoding = reader.u32("payload_encoding")?;
    let durable_batch_id = reader.u64("durable_batch_id")?;
    let writer_local_batch_id = reader.u64("writer_local_batch_id")?;
    let schema_generation = reader.u64("schema_generation")?;
    let dictionary_generation_hint = reader.u64("dictionary_generation_hint")?;
    let created_at_physical = reader.i64("created_at_physical")?;
    let committed_at_physical = reader.i64("committed_at_physical")?;
    let header_crc64 = reader.u64("header_crc64")?;
    let blake3_bytes = reader.take(BLAKE3_LEN, "frame_blake3")?;
    let reserved_tail = reader.take(RESERVED_TAIL_LEN, "reserved_zero_1")?;

    if reserved_zero_0 != 0 || reserved_tail.iter().any(|b| *b != 0) {
        return Err(FormatError::ReservedNotZero {
            field: "OEJFrameHeaderV1 reserved",
        });
    }
    if flags & RESERVED_FLAG_MASK != 0 {
        return Err(FormatError::ReservedNotZero {
            field: "OEJFrameHeaderV1.flags",
        });
    }

    let mut crc = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme);
    crc.update(slice(header_bytes, 0, HEADER_CRC64_OFFSET, "frame header")?);
    crc.update(&[0u8; 40]);
    crc.update(slice(header_bytes, 168, 24, "frame header")?);
    if crc.finalize() != header_crc64 {
        return Err(FormatError::HeaderCrcMismatch);
    }

    let mut frame_blake3 = [0u8; BLAKE3_LEN];
    frame_blake3.copy_from_slice(blake3_bytes);
    Ok(OejFrameHeaderV1 {
        frame_len,
        payload_len,
        page_count,
        flags,
        tenant_id,
        writer_id,
        epoch,
        first_sequence,
        last_sequence,
        event_count,
        payload_encoding,
        durable_batch_id,
        writer_local_batch_id,
        schema_generation,
        dictionary_generation_hint,
        created_at_physical,
        committed_at_physical,
        header_crc64,
        frame_blake3,
    })
}

pub fn decode_frame(frame: &[u8]) -> Result<(OejFrameHeaderV1, &[u8]), FormatError> {
    let header = precheck_header(frame)?;

    if header.frame_len as usize != frame.len() {
        return Err(FormatError::Structural {
            rule: "frame_len must equal the aligned frame length",
        });
    }
    if header.frame_len % FRAME_ALIGN != 0 {
        return Err(FormatError::Structural {
            rule: "frame_len must be a multiple of 4096",
        });
    }
    if header.is_large_event_frame() {
        if header.frame_len > MAX_LARGE_FRAME {
            return Err(FormatError::Structural {
                rule: "large-event frame exceeds 1 MiB",
            });
        }
    } else if !NORMAL_FRAME_SIZES.contains(&header.frame_len) {
        return Err(FormatError::Structural {
            rule: "normal frame length must be exactly 4/8/16/32/64 KiB",
        });
    }
    if header.frame_len > MAX_NORMAL_FRAME && !header.is_large_event_frame() {
        return Err(FormatError::Structural {
            rule: "normal frames above 64 KiB are invalid",
        });
    }
    if header.payload_len > header.frame_len - FRAME_HEADER_LEN {
        return Err(FormatError::Structural {
            rule: "payload_len must fit inside the frame after the header",
        });
    }
    if header.page_count != header.frame_len / FRAME_ALIGN {
        return Err(FormatError::Structural {
            rule: "page_count must equal frame_len / 4096",
        });
    }
    if header.payload_encoding != PAYLOAD_ENCODING_COMPACT_BATCH_V1 {
        return Err(FormatError::Structural {
            rule: "payload_encoding must be oe_oej_compact_batch_v1 (1)",
        });
    }
    if header.is_void_record() {
        if header.event_count != 0 || header.payload_len != 0 {
            return Err(FormatError::Structural {
                rule: "void records carry zero events and no payload",
            });
        }
        if header.last_sequence < header.first_sequence {
            return Err(FormatError::Structural {
                rule: "void range must be non-empty and ordered",
            });
        }
    } else {
        if header.event_count == 0 {
            return Err(FormatError::Structural {
                rule: "event frames must carry events",
            });
        }
        let expected_last = header
            .first_sequence
            .checked_add(u64::from(header.event_count) - 1)
            .ok_or(FormatError::Structural {
                rule: "sequence range overflow",
            })?;
        if header.last_sequence != expected_last {
            return Err(FormatError::Structural {
                rule: "last_sequence must equal first_sequence + event_count - 1",
            });
        }
    }

    let mut hasher = blake3::Hasher::new();
    let (head, tail) = frame
        .get(..136)
        .zip(frame.get(168..))
        .ok_or(FormatError::Truncated { what: "frame header" })?;
    hasher.update(head);
    hasher.update(&[0u8; BLAKE3_LEN]);
    if tail.len() < PARALLEL_HASH_MIN_LEN {
        hasher.update(tail);
    } else {
        hasher.update_rayon(tail);
    }
    if hasher.finalize().as_bytes() != &header.frame_blake3 {
        return Err(FormatError::Blake3Mismatch { scope: "frame" });
    }

    let payload_end = FRAME_HEADER_LEN as usize + header.payload_len as usize;
    let padding = slice(frame, payload_end, frame.len() - payload_end, "frame padding")?;
    if !super::is_all_zero(padding) {
        return Err(FormatError::ReservedNotZero { field: "frame padding" });
    }

    let payload = slice(
        frame,
        FRAME_HEADER_LEN as usize,
        header.payload_len as usize,
        "frame payload",
    )?;
    Ok((header, payload))
}

#[cfg(test)]
#[path = "test/frame.rs"]
mod tests;
