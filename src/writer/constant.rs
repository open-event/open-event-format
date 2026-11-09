use crate::error::FormatError;

pub const REFERENCE_FILTER_BITS_PER_KEY: u32 = 10;

pub(crate) const AUDIT_REWRITE_REFUSED: FormatError = FormatError::Structural {
    rule: "an audit file is never rewritten, spliced or compacted",
};

pub(crate) const AUDIT_DIGEST_BATCH_ROWS: usize = 4096;

pub(crate) const AUDIT_COMPACTION_REFUSED: &str = "an audit file is never compacted";

pub const DEFAULT_TIME_SERIES_BUCKET_NANOS: i64 = 60_000_000_000;

pub const DEFAULT_GRAPH_SEAL_EVENTS: u64 = 4096;

pub const DEFAULT_GRAPH_SEAL_AGE_NANOS: i64 = 300 * 1_000_000_000;

pub const STATE_CHUNK_GRACE_NANOS: i64 = 24 * 60 * 60 * 1_000_000_000;

pub(crate) const LINEAR_LOOKUP_MAX_ENTRIES: usize = 16;

pub const STATE_CHUNK_SWEEP_PAGE_RECORDS: usize = 1024;

pub const RETRY_LOG_FILE: &str = "receipts.log";

pub const RETRY_LOG_COMPACT_FILE: &str = "receipts.log.compact";

pub const RETRY_LOG_MAGIC: [u8; 4] = *b"OERS";
pub const RETRY_LOG_VERSION: u32 = 1;

pub const RETRY_LOG_HEADER_LEN: u64 = 8;

pub const RETRY_LOG_TAG_RECORD: u8 = 1;
pub const RETRY_LOG_TAG_EVICT: u8 = 2;

pub const RETRY_LOG_MAX_PAYLOAD: u32 = 256;

pub const RETRY_LOG_COMPACT_MIN_GARBAGE: u64 = 4096;

pub const RETRY_LOG_COMPACT_RATIO: u64 = 2;
