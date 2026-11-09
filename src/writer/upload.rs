use super::build::BuiltOe;
use super::publish::{PublishFailure, oe_upload_segments};
use super::state_gc::StateChunkLedger;
use crate::artifacts::state_chunk::SealedStateChunk;
use crate::clock::Clock;
use crate::error::StateChunkError;
use crate::events::TenantId;
use crate::object_store::{MultipartUpload, ObjectStat, ObjectStore, PutOutcome, UploadedPart, state_chunk_object_key};
use std::ops::Range;

pub(crate) fn upload_oe(
    objects: &dyn ObjectStore,
    key: &str,
    built: &BuiltOe,
    min_part_bytes: u64,
) -> Result<(), PublishFailure> {
    if let Some(stored) = objects.stat(key).map_err(PublishFailure::Storage)? {
        return matches_build(stored, built);
    }
    let parts = upload_parts(&oe_upload_segments(built), min_part_bytes);
    if parts.len() > 1 {
        upload_multipart(objects, key, &built.bytes, &parts)?;
    } else {
        objects
            .put_if_absent(key, &built.bytes)
            .map_err(PublishFailure::Storage)?;
    }
    match objects.stat(key).map_err(PublishFailure::Storage)? {
        Some(stored) => matches_build(stored, built),
        None => Err(PublishFailure::Verification(
            "uploaded object is missing from the store",
        )),
    }
}

pub fn upload_state_chunks<'a>(
    objects: &dyn ObjectStore,
    ledger: &dyn StateChunkLedger,
    tenant_id: TenantId,
    chunks: impl IntoIterator<Item = &'a SealedStateChunk>,
    clock: &dyn Clock,
) -> Result<usize, StateChunkError> {
    let now = clock.now_nanos();
    let mut written = 0;
    for chunk in chunks {
        if !ledger.touch(&chunk.address, now)? {
            return Err(StateChunkError::BeingDeleted {
                address: hex_simd::encode_to_string(chunk.address, hex_simd::AsciiCase::Lower),
            });
        }
        let key = state_chunk_object_key(tenant_id, &chunk.address);
        if let PutOutcome::Written { .. } = objects.put_if_absent(&key, &chunk.bytes)? {
            written += 1;
        }
    }
    Ok(written)
}

pub(crate) fn upload_parts(segments: &[u64], min_part_bytes: u64) -> Vec<Range<u64>> {
    let mut parts = Vec::new();
    let mut start = 0u64;
    let mut end = 0u64;
    for &len in segments {
        end += len;
        if end - start >= min_part_bytes {
            parts.push(start..end);
            start = end;
        }
    }
    if end > start {
        parts.push(start..end);
    }
    parts
}

fn matches_build(stored: ObjectStat, built: &BuiltOe) -> Result<(), PublishFailure> {
    if stored.size_bytes == built.bytes.len() as u64 && stored.crc64_nvme == built.file_crc64_nvme {
        Ok(())
    } else {
        Err(PublishFailure::Verification(
            "stored object does not match the built file",
        ))
    }
}

fn upload_multipart(
    objects: &dyn ObjectStore,
    key: &str,
    bytes: &[u8],
    parts: &[Range<u64>],
) -> Result<(), PublishFailure> {
    let upload = objects.begin_multipart(key).map_err(PublishFailure::Storage)?;
    let sent = send_parts(objects, &upload, bytes, parts);
    if sent.is_err() {
        let _ = objects.abort_multipart(&upload);
    }
    sent
}

fn send_parts(
    objects: &dyn ObjectStore,
    upload: &MultipartUpload,
    bytes: &[u8],
    parts: &[Range<u64>],
) -> Result<(), PublishFailure> {
    let mut uploaded = Vec::with_capacity(parts.len());
    for (range, part_number) in parts.iter().zip(1u32..) {
        let part = usize::try_from(range.start)
            .ok()
            .zip(usize::try_from(range.end).ok())
            .and_then(|(start, end)| bytes.get(start..end))
            .ok_or(PublishFailure::Verification("upload part lies outside the built file"))?;
        let etag = objects
            .upload_part(upload, part_number, part)
            .map_err(PublishFailure::Storage)?;
        uploaded.push(UploadedPart { etag, part_number });
    }
    match objects
        .complete_multipart(upload, &uploaded)
        .map_err(PublishFailure::Storage)?
    {
        PutOutcome::Written { .. } => {}
        PutOutcome::PreconditionFailed => {
            let _ = objects.abort_multipart(upload);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "test/upload.rs"]
mod tests;
