//! The production local-disk version of the write side's safe-retry store: [`FileSafeRetryStore`] keeps every
//! receipt in an append-only, CRC-checked log, so the replay guard keeps working across a restart.
//!
//! See: oe-write-path/spec.md

use super::constant::{
    RETRY_LOG_COMPACT_FILE, RETRY_LOG_COMPACT_MIN_GARBAGE, RETRY_LOG_COMPACT_RATIO, RETRY_LOG_FILE,
    RETRY_LOG_HEADER_LEN, RETRY_LOG_MAGIC, RETRY_LOG_MAX_PAYLOAD, RETRY_LOG_TAG_EVICT, RETRY_LOG_TAG_RECORD,
    RETRY_LOG_VERSION,
};
use super::retry::{RetryReceipt, SafeRetryStore, StatusClass};
use super::sim::SimSafeRetryStore;
use crate::error::StorageError;
use crate::events::{SequencePoint, TenantId};
use crate::invariants::ShardId;
use crate::invariants::disk::sync_dir;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Bytes of framing around each log record's payload: a little-endian `u32` length before it and a little-endian
/// CRC-64/NVME of length and payload after it.
const FRAME_OVERHEAD: usize = 4 + 8;

/// One entry of the receipt log.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LogEntry {
    /// Every row whose expiry is at or before this instant was evicted.
    Evict(i64),
    /// A receipt was recorded (or overwritten).
    Record(RetryReceipt),
}

fn status_code(status: StatusClass) -> u8 {
    match status {
        StatusClass::Acknowledged => 0,
        StatusClass::Indeterminate => 1,
        StatusClass::Rejected => 2,
    }
}

fn status_from_code(code: u8) -> Option<StatusClass> {
    match code {
        0 => Some(StatusClass::Acknowledged),
        1 => Some(StatusClass::Indeterminate),
        2 => Some(StatusClass::Rejected),
        _ => None,
    }
}

impl LogEntry {
    fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(96);
        match self {
            Self::Evict(now) => {
                payload.push(RETRY_LOG_TAG_EVICT);
                payload.extend_from_slice(&now.to_le_bytes());
            }
            Self::Record(receipt) => {
                payload.push(RETRY_LOG_TAG_RECORD);
                payload.extend_from_slice(&receipt.tenant_id.uuid().as_u128().to_le_bytes());
                for word in [
                    receipt.delivery_identity.0,
                    receipt.delivery_identity.1,
                    receipt.dedupe.0,
                    receipt.dedupe.1,
                    receipt.commit.epoch,
                    receipt.commit.sequence,
                    receipt.frame_offset,
                ] {
                    payload.extend_from_slice(&word.to_le_bytes());
                }
                payload.extend_from_slice(&receipt.expiry_physical_nanos.to_le_bytes());
                payload.extend_from_slice(&receipt.shard.0.to_le_bytes());
                payload.push(status_code(receipt.status_class));
            }
        }
        let len = payload.len() as u32;
        let mut frame = Vec::with_capacity(payload.len() + FRAME_OVERHEAD);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&payload);
        let crc = crc_fast::crc64_nvme(&frame);
        frame.extend_from_slice(&crc.to_le_bytes());
        frame
    }

    fn decode(payload: &[u8]) -> Option<Self> {
        let mut reader = Reader(payload);
        let entry = match reader.u8()? {
            RETRY_LOG_TAG_EVICT => Self::Evict(i64::from_le_bytes(reader.array()?)),
            RETRY_LOG_TAG_RECORD => {
                let tenant = u128::from_le_bytes(reader.array()?);
                let delivery_identity = (reader.u64()?, reader.u64()?);
                let dedupe = (reader.u64()?, reader.u64()?);
                let commit = SequencePoint {
                    epoch: reader.u64()?,
                    sequence: reader.u64()?,
                };
                let frame_offset = reader.u64()?;
                let expiry_physical_nanos = i64::from_le_bytes(reader.array()?);
                let shard = ShardId(u32::from_le_bytes(reader.array()?));
                let status_class = status_from_code(reader.u8()?)?;
                Self::Record(RetryReceipt {
                    commit,
                    dedupe,
                    delivery_identity,
                    expiry_physical_nanos,
                    frame_offset,
                    shard,
                    status_class,
                    tenant_id: TenantId::from_uuid(Uuid::from_u128(tenant)),
                })
            }
            _ => return None,
        };
        reader.0.is_empty().then_some(entry)
    }
}

/// A bounds-checked little-endian cursor over one record payload.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|[byte]| byte)
    }

    fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }
}

fn header() -> [u8; RETRY_LOG_HEADER_LEN as usize] {
    let mut bytes = [0; RETRY_LOG_HEADER_LEN as usize];
    let (magic, version) = bytes.split_at_mut(RETRY_LOG_MAGIC.len());
    magic.copy_from_slice(&RETRY_LOG_MAGIC);
    version.copy_from_slice(&RETRY_LOG_VERSION.to_le_bytes());
    bytes
}

/// Parses the log body after the header: every whole, CRC-valid entry in order, and the byte length (header included)
/// they end at. Parsing stops at the first short, oversized, corrupt or undecodable record — a torn tail.
fn parse_entries(body: &[u8]) -> (Vec<LogEntry>, u64) {
    let mut entries = Vec::new();
    let mut rest = body;
    let mut good = RETRY_LOG_HEADER_LEN;
    while let Some((len_bytes, after_len)) = rest.split_first_chunk::<4>() {
        let len = u32::from_le_bytes(*len_bytes);
        if len == 0 || len > RETRY_LOG_MAX_PAYLOAD {
            break;
        }
        let framed = 4 + len as usize;
        let Some((payload, after_payload)) = after_len.split_at_checked(len as usize) else {
            break;
        };
        let Some((crc_bytes, next)) = after_payload.split_first_chunk::<8>() else {
            break;
        };
        let Some(covered) = rest.get(..framed) else {
            break;
        };
        if crc_fast::crc64_nvme(covered) != u64::from_le_bytes(*crc_bytes) {
            break;
        }
        let Some(entry) = LogEntry::decode(payload) else {
            break;
        };
        entries.push(entry);
        good += (framed + 8) as u64;
        rest = next;
    }
    (entries, good)
}

fn io_error(op: &'static str) -> impl FnOnce(std::io::Error) -> StorageError {
    move |error| StorageError::Io {
        op,
        detail: error.to_string(),
    }
}

/// A durable safe-retry store on local disk: the production [`SafeRetryStore`], a drop-in for the in-memory
/// [`SimSafeRetryStore`] that keeps its receipts across a restart.
///
/// Every `record`, and every eviction that drops at least one row, appends one entry to `receipts.log` in the store's
/// directory: a little-endian length, a fixed-layout payload, and a CRC-64/NVME over both. By default each append is
/// `fdatasync`ed before the call returns, so a receipt the engine relied on survives a crash
/// ([`FileSafeRetryStore::with_sync_each_record`] turns that off for callers that batch with
/// [`FileSafeRetryStore::sync`]). Opening the store replays the log into the same in-memory index the simulation store
/// uses, so lookups, tenant isolation, expiry eviction and the replay-guard check behave identically. A torn or
/// corrupt tail — a crash mid-append, or bit rot — ends the replay at the last whole record and is cut off durably;
/// rows lost that way are pointers, not data, and `reconstruct_from_replay` rebuilds them from the journal.
///
/// When the log holds many more entries than live rows it is compacted: the live rows are written to a fresh file,
/// synced, renamed over the log, and the directory is synced, so a crash leaves either the old log or the new one.
///
/// The trait's methods cannot fail, so an I/O failure while appending keeps the in-memory row (the engine keeps
/// working, as with the simulation store) and is held until [`FileSafeRetryStore::sync`] or
/// [`FileSafeRetryStore::take_failure`] reports it; the log is cut back to its last whole record so later appends stay
/// readable.
///
/// One directory must have one open store at a time.
///
/// See: oe-write-path/spec.md
#[derive(Debug)]
pub struct FileSafeRetryStore {
    dir: PathBuf,
    failure: Option<StorageError>,
    file: File,
    index: SimSafeRetryStore,
    /// Entries in the log, live or superseded.
    log_entries: u64,
    /// Bytes of whole records in the log, header included: where the next append goes.
    log_len: u64,
    sync_each_record: bool,
}

impl FileSafeRetryStore {
    /// The store kept in `dir`, created when missing, with every receipt on its log replayed. A torn tail is cut off;
    /// a file that is not a receipt log at all is an error.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let dir = dir.into();
        if !dir.is_dir() {
            std::fs::create_dir_all(&dir).map_err(io_error("retry store create"))?;
            if let Some(parent) = dir.parent().filter(|parent| !parent.as_os_str().is_empty()) {
                sync_dir(parent).map_err(io_error("retry store directory sync"))?;
            }
        }
        // A compaction a crash interrupted before its rename never became the log.
        let _ = std::fs::remove_file(dir.join(RETRY_LOG_COMPACT_FILE));
        let path = dir.join(RETRY_LOG_FILE);
        let created = !path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(io_error("retry log open"))?;
        if created {
            sync_dir(&dir).map_err(io_error("retry store directory sync"))?;
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io_error("retry log read"))?;
        let mut store = Self {
            dir,
            failure: None,
            file,
            index: SimSafeRetryStore::new(),
            log_entries: 0,
            log_len: RETRY_LOG_HEADER_LEN,
            sync_each_record: true,
        };
        let expected = header();
        match bytes.split_first_chunk::<{ RETRY_LOG_HEADER_LEN as usize }>() {
            Some((found, body)) if *found == expected => {
                let (entries, good) = parse_entries(body);
                store.log_entries = entries.len() as u64;
                for entry in entries {
                    store.apply(entry);
                }
                store.log_len = good;
                if good != bytes.len() as u64 {
                    store.file.set_len(good).map_err(io_error("retry log truncate"))?;
                    store.file.sync_all().map_err(io_error("retry log sync"))?;
                }
            }
            Some(_) => {
                return Err(StorageError::Io {
                    op: "retry log open",
                    detail: "not a safe-retry receipt log (bad magic or version)".to_owned(),
                });
            }
            None if expected.starts_with(&bytes) => {
                // Empty, or a crash tore the header itself: nothing was ever recorded.
                store.file.set_len(0).map_err(io_error("retry log truncate"))?;
                store
                    .file
                    .seek(SeekFrom::Start(0))
                    .map_err(io_error("retry log seek"))?;
                store.file.write_all(&expected).map_err(io_error("retry log write"))?;
                store.file.sync_all().map_err(io_error("retry log sync"))?;
            }
            None => {
                return Err(StorageError::Io {
                    op: "retry log open",
                    detail: "not a safe-retry receipt log (bad header)".to_owned(),
                });
            }
        }
        Ok(store)
    }

    /// Whether each appended entry is `fdatasync`ed before the call returns (the default). With `false`, appends reach
    /// the OS only, and the caller makes them durable with [`FileSafeRetryStore::sync`].
    pub fn with_sync_each_record(mut self, sync_each_record: bool) -> Self {
        self.sync_each_record = sync_each_record;
        self
    }

    /// The directory the log lives in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// How many delivery rows are on record.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// True when no delivery rows are on record.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Entries in the log, superseded ones included, since it was last compacted.
    pub fn log_entries(&self) -> u64 {
        self.log_entries
    }

    /// Makes every appended entry durable, first reporting any append failure held since the last report.
    pub fn sync(&mut self) -> Result<(), StorageError> {
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        self.file.sync_data().map_err(io_error("retry log sync"))
    }

    /// The first append failure since the last report, if any. Rows whose append failed are still answered from
    /// memory but would be missing after a restart.
    pub fn take_failure(&mut self) -> Option<StorageError> {
        self.failure.take()
    }

    /// Rewrites the log as exactly the live rows, durably replacing the old one.
    pub fn compact(&mut self) -> Result<(), StorageError> {
        let staged_path = self.dir.join(RETRY_LOG_COMPACT_FILE);
        let mut bytes = header().to_vec();
        let mut entries = 0;
        for receipt in self.index.receipts() {
            bytes.extend_from_slice(&LogEntry::Record(receipt.clone()).encode());
            entries += 1;
        }
        let mut staged = File::create(&staged_path).map_err(io_error("retry log compact"))?;
        staged.write_all(&bytes).map_err(io_error("retry log compact"))?;
        staged.sync_all().map_err(io_error("retry log compact sync"))?;
        drop(staged);
        std::fs::rename(&staged_path, self.dir.join(RETRY_LOG_FILE)).map_err(io_error("retry log compact rename"))?;
        sync_dir(&self.dir).map_err(io_error("retry store directory sync"))?;
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.dir.join(RETRY_LOG_FILE))
            .map_err(io_error("retry log open"))?;
        self.log_entries = entries;
        self.log_len = bytes.len() as u64;
        Ok(())
    }

    fn apply(&mut self, entry: LogEntry) {
        match entry {
            LogEntry::Evict(now) => self.index.evict_expired(now),
            LogEntry::Record(receipt) => self.index.record(receipt),
        }
    }

    /// Appends one entry at the end of the last whole record, holding any failure for the caller to collect.
    fn append(&mut self, entry: &LogEntry) {
        if let Err(failure) = self.try_append(entry) {
            // Cut a partial write back so the next append lands after the last whole record.
            let _ = self.file.set_len(self.log_len);
            self.failure.get_or_insert(failure);
            return;
        }
        let live = self.index.len() as u64;
        let garbage = self.log_entries.saturating_sub(live);
        if garbage >= RETRY_LOG_COMPACT_MIN_GARBAGE
            && self.log_entries >= live.saturating_mul(RETRY_LOG_COMPACT_RATIO)
            && let Err(failure) = self.compact()
        {
            self.failure.get_or_insert(failure);
        }
    }

    fn try_append(&mut self, entry: &LogEntry) -> Result<(), StorageError> {
        let frame = entry.encode();
        self.file
            .seek(SeekFrom::Start(self.log_len))
            .map_err(io_error("retry log seek"))?;
        self.file.write_all(&frame).map_err(io_error("retry log append"))?;
        if self.sync_each_record {
            self.file.sync_data().map_err(io_error("retry log sync"))?;
        }
        self.log_len += frame.len() as u64;
        self.log_entries += 1;
        Ok(())
    }
}

impl SafeRetryStore for FileSafeRetryStore {
    fn record(&mut self, receipt: RetryReceipt) {
        let entry = LogEntry::Record(receipt.clone());
        self.index.record(receipt);
        self.append(&entry);
    }

    fn evict_expired(&mut self, now_physical_nanos: i64) {
        let before = self.index.len();
        self.index.evict_expired(now_physical_nanos);
        if self.index.len() != before {
            self.append(&LogEntry::Evict(now_physical_nanos));
        }
    }

    fn lookup(&self, tenant_id: TenantId, delivery_identity: (u64, u64)) -> Option<RetryReceipt> {
        self.index.lookup(tenant_id, delivery_identity)
    }

    fn acknowledged_within_guard(
        &mut self,
        tenant_id: TenantId,
        dedupe: (u64, u64),
        now_physical_nanos: i64,
    ) -> Option<RetryReceipt> {
        self.evict_expired(now_physical_nanos);
        self.index
            .acknowledged_within_guard(tenant_id, dedupe, now_physical_nanos)
    }
}

#[cfg(test)]
#[path = "test/disk.rs"]
mod tests;

#[cfg(test)]
impl FileSafeRetryStore {
    /// The log file's path, for tests that tear its tail.
    fn log_path(&self) -> PathBuf {
        self.dir.join(RETRY_LOG_FILE)
    }
}
