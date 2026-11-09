pub mod batch;
pub mod constant;
pub mod external;
pub mod frame;
pub mod overlay;
pub mod segment;
pub mod state_chunk;
pub mod watermark;

pub const FRAME_ALIGN: u32 = 4096;

pub const NORMAL_FRAME_SIZES: [u32; 5] = [4096, 8192, 16384, 32768, 65536];

pub const DEFAULT_FLUSH_TARGET: u32 = 16384;

pub const FORCE_COMMIT_TARGET: u32 = 4096;

pub const MAX_NORMAL_FRAME: u32 = 65536;

pub const MAX_LARGE_FRAME: u32 = 1_048_576;

pub const PAYLOAD_ENCODING_COMPACT_BATCH_V1: u32 = 1;

pub const FRAME_HEADER_LEN: u32 = 192;

pub const BATCH_HEADER_LEN: u32 = 128;

pub const FIXED_RECORD_LEN: u32 = 128;

pub const VARIABLE_RECORD_LEN: u32 = 64;

pub const PROVENANCE_RECORD_LEN: u32 = 144;

pub const REF_ABSENT: u32 = 0xFFFF_FFFF;

pub const BLAKE3_LEN: usize = 32;

pub const MAGIC_LEN: usize = 4;

pub(crate) fn is_all_zero(bytes: &[u8]) -> bool {
    let mut chunks = bytes.chunks_exact(8);
    chunks.all(|chunk| u64::from_ne_bytes(chunk.try_into().unwrap_or_default()) == 0)
        && chunks.remainder().iter().all(|&byte| byte == 0)
}
