pub const ALP_CONVERT_MAGIC: f64 = 6_755_399_441_055_744.0;

pub const ALP_EXACT_CONVERT_MAX_WIDTH: u32 = 52;

pub const ALP_MAX_EXCEPTION_SAMPLE_DIVISOR: usize = 8;

pub const ALP_MAX_SCALED_MAGNITUDE: f64 = 9.0e18;

pub const ALP_RD_CODE_MISS: u64 = u64::MAX;

pub const ALP_RD_MAX_DICT_LEN: usize = 8;

pub const ALP_RD_MAX_RIGHT_WIDTH: u32 = 63;

pub const ALP_RD_MIN_RIGHT_WIDTH: u32 = 48;

pub const ALP_VECTOR_ESCAPE_SENTINEL: u8 = 255;

pub const BYTE_STREAM_SPLIT_TILE: usize = 32;

pub const BRANCH_FREE_SET_NARROW: usize = 4;

pub const BRANCH_FREE_SET_WIDE: usize = 16;

pub const CHIMP_LEADING_ZEROS: [u32; 8] = [0, 8, 12, 16, 18, 20, 22, 24];

pub const CHIMP_TRAILING_ZERO_THRESHOLD: u32 = 6;

pub const DECODE_OPTIMIZED_THRESHOLD_DEN: usize = 4;

pub const DICTIONARY_FSST_MAX_VALUE_LEN: usize = 4096;

pub const DECODE_OPTIMIZED_THRESHOLD_NUM: usize = 3;

pub const DICTIONARY_MAX_CARDINALITY_DIVISOR: usize = 4;

pub const DICTIONARY_TRIAL_MAX_DISTINCT_NUM: usize = 3;

pub const DICTIONARY_TRIAL_MAX_DISTINCT_DEN: usize = 4;

pub const FSST_LOSSY_PHT_HASH_MULTIPLIER: u64 = 2_971_215_073;

pub const FSST_LOSSY_PHT_HASH_SHIFT: u32 = 15;

pub const FSST_MAX_AVERAGE_VALUE_LEN: usize = 160;

pub const FSST_MAX_SYMBOL_COUNT: usize = 255;

pub const FSST_MIN_SAMPLE_COUNT: usize = 16;

pub const FSST_VALUE_KEYS_MAX_PLAINTEXT_DIVISOR: usize = 2;

pub const MAX_RETAINED_SCRATCH_VALUES: usize = 1 << 20;

pub const MAX_RETAINED_ARENA_BYTES: usize = 4 << 20;

pub const MAX_RETAINED_INFLATE_BYTES: usize = 4 << 20;

pub const FSST_MAX_SYMBOL_BYTES: usize = 8;

pub const FSST_KEY_SCAN_CHUNK: usize = u64::BITS as usize;

pub const PREFIX_KEY_BYTES: usize = PREFIX_KEY_PREFIX_LEN + 1;

pub const PREFIX_KEY_PREFIX_LEN: usize = 7;

pub const PREFIX_SUM_CHUNK: usize = 16;

pub const MAX_RLE_EXPANSION_VALUES: usize = 64 * 1024 * 1024;

pub const PARALLEL_SCAN_MIN_GRANULE_BYTES: u64 = 64 * 1024;

pub const PARALLEL_SCAN_MIN_GRANULES: usize = 2;

pub const REPLAY_TRIP_DEN: u64 = 4;

pub const REPLAY_TRIP_NUM: u64 = 5;

pub const SIZE_OPTIMIZED_SAVINGS_DIVISOR: usize = 20;

pub const TRANSFORM_SAMPLE_RUNS: usize = 8;

pub const TRANSFORM_SAMPLE_SIZE: usize = 1024;

pub const ZSTD_LENGTH_PREFIX_BYTES: usize = 4;

pub const FASTLANES_SPARSE_MAX_VALUES: usize = 64;

pub const FSST_TRAILING_MAX_RATIO_NUM: usize = 1;

pub const FSST_TRAILING_MAX_RATIO_DEN: usize = 2;

pub const FSST_PER_FRAME_BAR_MIN_BLOCK_BYTES: usize = 4 * 1024;

pub const STRING_ZSTD_MIN_SAVINGS_NUM: usize = 9;

pub const STRING_ZSTD_MIN_SAVINGS_DEN: usize = 10;
