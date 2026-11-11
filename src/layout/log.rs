use super::Profile;
use super::constant::{
    LOG_FORMAT_VERSION, LOG_HEADER_LEN, LOG_MAGIC, LOG_RECORD_FRAME_LEN, LOG_TRAILER_LEN, LOG_TRAILER_SENTINEL,
};
use crate::error::{FormatError, LogReadError};
use crate::file::bytes::{Reader, Writer};
use crate::file::constant::{CHUNK_GROUP_BYTES, TREE_TRAILER_MAGIC};
use crate::file::integrity::{
    BuiltOutboard, build_outboard_tree_and_root, crc64_matches, crc64_nvme, parse_outboard_object, verify_object_range,
};

#[derive(Debug)]
pub struct LogObjectWriter {
    bytes: Writer,
    record_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedLogObject {
    pub bytes: Vec<u8>,
    pub root: [u8; blake3::OUT_LEN],
}

#[derive(Debug, Clone)]
pub struct LogRecords<'a> {
    content: &'a [u8],
    finished: bool,
    object: &'a [u8],
    position: usize,
    record_count: u32,
    root: Option<[u8; blake3::OUT_LEN]>,
    verified_len: usize,
}

impl LogObjectWriter {
    pub fn new() -> Self {
        let mut bytes = Writer::new();
        bytes.put_slice(&header());
        Self { bytes, record_count: 0 }
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<(), FormatError> {
        let len = u32::try_from(payload.len())
            .ok()
            .filter(|len| *len != LOG_TRAILER_SENTINEL)
            .ok_or(FormatError::Structural {
                rule: "a log record payload must be shorter than u32::MAX bytes",
            })?;
        let record_count = self.record_count.checked_add(1).ok_or(FormatError::Structural {
            rule: "a log object holds at most u32::MAX records",
        })?;
        let start = self.bytes.len();
        self.bytes.put_u32(len);
        self.bytes.put_slice(payload);
        let crc = crc64_nvme(self.bytes.bytes().get(start..).unwrap_or_default());
        self.bytes.put_u64(crc);
        self.record_count = record_count;
        Ok(())
    }

    pub fn finish(mut self) -> SealedLogObject {
        self.bytes.put_u32(LOG_TRAILER_SENTINEL);
        self.bytes.put_u32(self.record_count);
        self.bytes.put_slice(&LOG_MAGIC);
        let mut bytes = self.bytes.into_bytes();
        let BuiltOutboard { root, tree } = build_outboard_tree_and_root(&bytes, CHUNK_GROUP_BYTES);
        if let Some(tree) = tree {
            bytes.extend_from_slice(&tree);
            bytes.extend_from_slice(&(tree.len() as u64).to_le_bytes());
            bytes.extend_from_slice(&TREE_TRAILER_MAGIC);
        }
        SealedLogObject { bytes, root }
    }
}

impl Default for LogObjectWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> LogRecords<'a> {
    pub fn open(object: &'a [u8]) -> Result<Self, LogReadError> {
        Self::start(object, None)
    }

    pub fn open_verified(object: &'a [u8], root: &[u8; blake3::OUT_LEN]) -> Result<Self, LogReadError> {
        Self::start(object, Some(*root))
    }

    pub fn verified_len(&self) -> u64 {
        self.verified_len as u64
    }

    fn start(object: &'a [u8], root: Option<[u8; blake3::OUT_LEN]>) -> Result<Self, LogReadError> {
        let content = parse_outboard_object(object, &LOG_MAGIC).map_or(object, |parsed| parsed.content);
        let mut records = Self {
            content,
            finished: false,
            object,
            position: LOG_HEADER_LEN,
            record_count: 0,
            root,
            verified_len: 0,
        };
        records.verify_through(LOG_HEADER_LEN)?;
        if content.len() < LOG_HEADER_LEN && header().starts_with(content) {
            return Err(LogReadError::TornTail {
                intact_len: 0,
                intact_records: 0,
            });
        }
        let mut reader = Reader::new(content);
        if reader.take(LOG_MAGIC.len(), "log magic").map_err(FormatError::from)? != LOG_MAGIC {
            return Err(FormatError::BadMagic { expected: "OEFL" }.into());
        }
        let version = reader.u8("log format version").map_err(FormatError::from)?;
        if version != LOG_FORMAT_VERSION {
            return Err(FormatError::UnsupportedVersion {
                field: "log format version",
                found: u32::from(version),
            }
            .into());
        }
        if Profile::from_u8(reader.u8("log profile").map_err(FormatError::from)?)? != Profile::Log {
            return Err(FormatError::Structural {
                rule: "a log object must declare the log profile",
            }
            .into());
        }
        Ok(records)
    }

    fn next_record(&mut self) -> Result<Option<&'a [u8]>, LogReadError> {
        let rest = self.content.get(self.position..).unwrap_or_default();
        let mut reader = Reader::new(rest);
        let Ok(len) = reader.u32("log record length") else {
            return Err(self.torn());
        };
        if len == LOG_TRAILER_SENTINEL {
            let (Ok(count), Ok(magic)) = (
                reader.u32("log record count"),
                reader.take(LOG_MAGIC.len(), "log trailer magic"),
            ) else {
                return Err(self.torn());
            };
            self.verify_through(self.position.saturating_add(LOG_TRAILER_LEN))?;
            if magic != LOG_MAGIC || reader.remaining() != 0 {
                return Err(FormatError::Structural {
                    rule: "a log trailer must end the object",
                }
                .into());
            }
            if count != self.record_count {
                return Err(FormatError::Structural {
                    rule: "a log trailer's record count must match the records read",
                }
                .into());
            }
            return Ok(None);
        }
        let (Ok(payload), Ok(crc)) = (
            reader.take(len as usize, "log record payload"),
            reader.u64("log record crc"),
        ) else {
            return Err(self.torn());
        };
        let frame_len = LOG_RECORD_FRAME_LEN.saturating_add(payload.len());
        self.verify_through(self.position.saturating_add(frame_len))?;
        let covered = rest
            .get(..frame_len.saturating_sub(size_of::<u64>()))
            .unwrap_or_default();
        if !crc64_matches(covered, crc) {
            return Err(FormatError::Structural {
                rule: "a log record's CRC-64/NVME must match its length and payload",
            }
            .into());
        }
        self.position = self.position.saturating_add(frame_len);
        self.record_count = self.record_count.saturating_add(1);
        Ok(Some(payload))
    }

    fn torn(&mut self) -> LogReadError {
        if let Err(error) = self.verify_through(self.content.len()) {
            return error.into();
        }
        LogReadError::TornTail {
            intact_len: self.position as u64,
            intact_records: u64::from(self.record_count),
        }
    }

    fn verify_through(&mut self, end: usize) -> Result<(), FormatError> {
        let Some(root) = &self.root else {
            return Ok(());
        };
        let end = end.min(self.content.len());
        if self.verified_len > 0 && self.verified_len >= end {
            return Ok(());
        }
        let window_end = end
            .div_ceil(CHUNK_GROUP_BYTES)
            .saturating_mul(CHUNK_GROUP_BYTES)
            .min(self.content.len());
        verify_object_range(
            self.object,
            root,
            self.verified_len as u64,
            window_end.saturating_sub(self.verified_len) as u64,
            CHUNK_GROUP_BYTES,
            &LOG_MAGIC,
        )?;
        self.verified_len = window_end;
        Ok(())
    }
}

impl<'a> Iterator for LogRecords<'a> {
    type Item = Result<&'a [u8], LogReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next = self.next_record().transpose();
        self.finished = !matches!(next, Some(Ok(_)));
        next
    }
}

fn header() -> [u8; LOG_HEADER_LEN] {
    let [m0, m1, m2, m3] = LOG_MAGIC;
    [m0, m1, m2, m3, LOG_FORMAT_VERSION, Profile::Log.code()]
}

#[cfg(test)]
#[path = "test/log.rs"]
mod tests;
