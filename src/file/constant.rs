pub const FRAME_ALIGNMENT: usize = 4096;

pub const MAX_LARGE_FRAME: usize = 1 << 20;

pub const CHUNK_GROUP_BYTES: usize = 1 << 20;

pub const TREE_TRAILER_MAGIC: [u8; 4] = *b"OEFT";

pub const TREE_TRAILER_LEN: usize = 12;
