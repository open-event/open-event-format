use super::model::{ETag, MultipartUpload, ObjectStat, PutOutcome, StoredObject, UploadedPart};
use crate::error::StorageError;
use crate::events::TenantId;

pub trait ObjectStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>, StorageError>;

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<PutOutcome, StorageError>;

    fn put_if_match(&self, key: &str, bytes: &[u8], etag: &ETag) -> Result<PutOutcome, StorageError>;

    fn stat(&self, key: &str) -> Result<Option<ObjectStat>, StorageError>;

    fn begin_multipart(&self, key: &str) -> Result<MultipartUpload, StorageError>;

    fn upload_part(&self, upload: &MultipartUpload, part_number: u32, bytes: &[u8]) -> Result<ETag, StorageError>;

    fn complete_multipart(&self, upload: &MultipartUpload, parts: &[UploadedPart]) -> Result<PutOutcome, StorageError>;

    fn abort_multipart(&self, upload: &MultipartUpload) -> Result<(), StorageError>;

    fn delete(&self, key: &str) -> Result<(), StorageError>;
}

pub fn oe_object_key(tenant_id: TenantId, file_id: u128) -> String {
    format!("oe/{tenant_id}/{file_id:032x}.oe")
}

pub fn state_chunk_object_key(tenant_id: TenantId, address: &[u8; 32]) -> String {
    format!(
        "state/{tenant_id}/{}.hsc",
        hex_simd::encode_to_string(address, hex_simd::AsciiCase::Lower)
    )
}
