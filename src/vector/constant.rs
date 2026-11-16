pub const GRANULE_ROWS: usize = 4096;
/// Rows per sealed vector sub-block from `SEGMENT_VERSION` 3: a rescore or row read opens only these.
pub const VECTOR_SUBBLOCK_ROWS: usize = 128;
pub const BLOCK_ALIGNMENT: usize = 64;
pub const SEGMENT_MAGIC: &[u8; 4] = b"OEFV";
/// Version 4: U1 columns hold RaBitQ codes and I8 columns per-dimension segment-level int8 (see `codec`).
pub const SEGMENT_VERSION: u16 = 4;
pub const SEGMENT_VERSION_SUBBLOCKS: u16 = 3;
pub const SEGMENT_VERSION_GRANULE_VECTORS: u16 = 2;
pub const SEGMENT_VERSION_SHARED_NONCE: u16 = 1;
pub const BLOCK_NONCE_LEN: usize = 12;
pub const NONCE_RANDOM_LEN: usize = BLOCK_NONCE_LEN - 8;
pub const KEY_LEN: usize = 32;
pub const NONCE_PREFIX_LEN: usize = 16;
pub const WRAP_NONCE_LEN: usize = 24;
pub const TAG_LEN: usize = 16;
pub const HEADER_USED_LEN: usize = 4 + 2 + 2 + NONCE_PREFIX_LEN + WRAP_NONCE_LEN + KEY_LEN + TAG_LEN;
pub const HEADER_LEN: usize = 128;
pub const TRAILER_LEN: usize = 8 + 8 + 4;
pub const FOOTER_BLOCK_INDEX: u64 = u64::MAX;
pub const I8_TRAILER_BYTES: usize = 8;
pub const U1_TRAILER_BYTES: usize = 8;
pub const NORMALIZATION_TOLERANCE: f32 = 1e-3;
pub const DEFAULT_OVERSAMPLE: usize = 8;
pub const EMBEDDING_COLUMN_ID: u32 = crate::columns::column_ids::EMBEDDING_BASE;
pub const EVENT_ID_COLUMN: &str = "event_id";
pub const ROOM_ID_COLUMN: &str = "room_id";
pub const SENDER_COLUMN: &str = "sender";
pub const TIMESTAMP_COLUMN: &str = "timestamp";
/// RaBitQ confidence multiplier ε₀ for the per-row error bound (the paper uses 1.9; 2.5 keeps upper-bound misses well under 1%).
pub const RABITQ_EPSILON: f32 = 2.5;
/// Sign-flip + Hadamard rounds in the RaBitQ rotation.
pub const RABITQ_ROTATION_ROUNDS: usize = 3;
pub(crate) const RABITQ_SEED_CONTEXT: &str = "oe 2026 vector rabitq rotation seed v1";
