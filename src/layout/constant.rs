use std::sync::LazyLock;

pub const FOOTER_TAIL_TRAILER_LEN: usize = 12;

pub const OE_MAGIC: [u8; 4] = *b"OEF1";

pub const SCAN_WINDOW_BYTES: u64 = 32 * 1024 * 1024;

pub const HELD_RANGE_BUCKETS: usize = 48;

pub const AUDIT_GENESIS: [u8; 32] = [0; 32];

pub const AUDIT_ROW_HASH_CONTEXT: &str = "open-event OE v1 audit row hash";

pub static AUDIT_ROW_HASHER: LazyLock<blake3::Hasher> =
    LazyLock::new(|| blake3::Hasher::new_derive_key(AUDIT_ROW_HASH_CONTEXT));

pub const ROW_DIGEST_INPUT_CAPACITY: usize = 512;

pub const LOG_MAGIC: [u8; 4] = *b"OEFL";

pub const LOG_FORMAT_VERSION: u8 = 1;

pub const LOG_HEADER_LEN: usize = 6;

pub const LOG_RECORD_FRAME_LEN: usize = 12;

pub const LOG_TRAILER_SENTINEL: u32 = u32::MAX;

pub const LOG_TRAILER_LEN: usize = 12;

pub const GRAPH_EDGE_KINDS: usize = 2;

pub const MAX_GRAPH_SUBJECT_BYTES: usize = 255;

pub const UNRESOLVED_ENTRY_MIN_BYTES: usize = 11;

pub const CHAIN_RANGE_BYTES: usize = 12;

pub const GRAPH_FETCH_GAP_BYTES: u64 = 1024 * 1024;

pub const GRAPH_FETCH_WINDOW_BYTES: u64 = 8 * 1024 * 1024;

pub const GRAPH_HEDGE_SAMPLES: usize = 128;

pub const GRAPH_HEDGE_MIN_SAMPLES: usize = 20;

pub const SNAPSHOT_KEY_ERROR_BOUND: u64 = 32;

pub const SNAPSHOT_KEY_INDEX_FRAMING_BYTES: usize = 20;

pub const SNAPSHOT_KEY_LENGTH_BYTES: usize = 4;
