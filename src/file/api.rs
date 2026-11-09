use super::error::FileError;
use super::model::{Atomicity, BlockTarget, ByteRange};

pub trait BlockStore {
    fn append(&mut self, target: BlockTarget, frame: &[u8]) -> Result<u64, FileError>;

    fn append_and_sync(&mut self, target: BlockTarget, frame: &[u8]) -> Result<u64, FileError> {
        let offset = self.append(target, frame)?;
        self.sync(target)?;
        Ok(offset)
    }

    fn sync(&mut self, target: BlockTarget) -> Result<(), FileError>;

    fn truncate(&mut self, target: BlockTarget, len: u64) -> Result<(), FileError>;

    fn read(&self, target: BlockTarget, offset: u64, len: u32) -> Result<Vec<u8>, FileError>;

    fn extent(&self, target: BlockTarget) -> Result<u64, FileError>;

    fn atomicity(&self, target: BlockTarget) -> Result<Atomicity, FileError>;
}

pub trait RangeSource: Send + Sync {
    fn read_range(&self, object: u128, offset: u64, len: u64) -> Result<Vec<u8>, FileError>;

    fn read_ranges(&self, object: u128, ranges: &[ByteRange]) -> Result<Vec<Vec<u8>>, FileError> {
        ranges
            .iter()
            .map(|range| self.read_range(object, range.offset, range.len))
            .collect()
    }
}

pub fn verify_checksum(
    bytes: &[u8],
    expected_blake3: &[u8; blake3::OUT_LEN],
    expected_size: u64,
) -> Result<(), FileError> {
    if bytes.len() as u64 != expected_size {
        return Err(FileError::ChecksumMismatch);
    }
    if blake3::hash(bytes) != blake3::Hash::from_bytes(*expected_blake3) {
        return Err(FileError::ChecksumMismatch);
    }
    Ok(())
}

pub fn slice_range(bytes: &[u8], offset: u64, length: Option<u64>) -> Result<Vec<u8>, FileError> {
    let size = bytes.len() as u64;
    if offset > size {
        return Err(FileError::InvalidRange { offset, size });
    }
    let end = match length {
        Some(len) => offset.saturating_add(len).min(size),
        None => size,
    };
    Ok(bytes.get(offset as usize..end as usize).unwrap_or_default().to_vec())
}

#[cfg(test)]
#[path = "test/api.rs"]
mod tests;
