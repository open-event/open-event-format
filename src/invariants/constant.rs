//! Shared constants for the clock interfaces: the xorshift jitter shifts both `io::SystemClock` and `sim::SimClock`
//! draw from, and the fixed wall-clock start `sim::SimClock` boots at.

/// xorshift64 shift triple (Marsaglia's classic 13/7/17 variant).
pub const JITTER_XORSHIFT_SHIFT_1: u32 = 13;
pub const JITTER_XORSHIFT_SHIFT_2: u32 = 7;
pub const JITTER_XORSHIFT_SHIFT_3: u32 = 17;

/// `SimClock`'s fixed starting wall-clock time (2023-11-14T22:13:20Z) — arbitrary but reproducible.
pub const SIM_CLOCK_START_NANOS: i64 = 1_700_000_000_000_000_000;

/// File extension of one journal shard file kept by `disk::FileJournalStorage` (`{shard as 8 hex digits}.oej`).
pub const JOURNAL_SHARD_EXTENSION: &str = "oej";

/// Subdirectory of a `disk::FilePublishedSet` holding its create-only generation files.
pub const PUBLISHED_GENERATIONS_DIR: &str = "generations";

/// File extension of one published generation file (`{id as 20 decimal digits}.json`).
pub const PUBLISHED_GENERATION_EXTENSION: &str = "json";

/// Name of the head pointer file of a `disk::FilePublishedSet`.
pub const PUBLISHED_HEAD_FILE: &str = "head";

/// Extension of the temporary files a `disk::FilePublishedSet` stages before a create-only link or an atomic rename.
pub const PUBLISHED_STAGING_EXTENSION: &str = "tmp";

/// How many generations behind the head a `disk::FilePublishedSet` keeps by default before pruning older ones: enough
/// that a publisher rebasing after a lost race, or a reader that just read an older head, still finds its generation.
pub const PUBLISHED_GENERATIONS_KEPT: u64 = 8;
