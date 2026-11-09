pub const STATE_CHUNK_MAGIC: [u8; 4] = *b"HSC1";

pub const STATE_CHUNK_HEADER_LEN: usize = 12;

pub const STATE_CHUNK_CRC_LEN: usize = 8;

pub const STATE_LEAF_ENTRY_LEN: usize = 16;

pub const STATE_INTERIOR_ENTRY_LEN: usize = 40;

pub const STATE_CHUNK_MIN_BYTES: usize = 4 * 1024;

pub const STATE_CHUNK_MAX_BYTES: usize = 16 * 1024;

pub const STATE_CHUNK_BOUNDARY_SPAN_BYTES: usize = 4 * 1024;
