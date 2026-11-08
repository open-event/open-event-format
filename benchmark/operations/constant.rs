//! The fixed shape of the comparison: how many distinct values the generated dataset draws from, and the encoder
//! settings each format is given.
//!
//! These are pinned rather than configurable because they are what makes the two sides comparable. Changing one
//! changes every format's file, so a number moved here invalidates every committed report.

/// Payload field carrying a per-row integer. Promoted to a typed column on the OE side and a typed Arrow column on
/// the Parquet and Vortex sides, so every format filters it without touching the rest of the payload.
pub const FIELD_CHAR_COUNT: &str = "char_count";

/// Payload field carrying a low-cardinality label.
pub const FIELD_KIND: &str = "kind";

/// Payload field declared free-text: the one the substring search reads.
pub const FIELD_NOTE: &str = "note";

/// Payload field carrying a mid-cardinality label, read by the single-field payload probe.
pub const FIELD_CLIENT: &str = "client";

/// Payload field missing from some rows, so every format is measured over a column with real gaps rather than a
/// fully dense one. Integer rather than boolean because OE only shreds a path whose values a typed column reproduces
/// byte-identically, and boolean is not one of those kinds.
pub const FIELD_REACTIONS: &str = "reactions";

/// One row in this many omits [`FIELD_REACTIONS`]. Kept below half so the field stays dense enough for OE's
/// statistics-driven shredding to lift it into a column of its own — otherwise it would sit in the residual payload,
/// which Parquet has no counterpart for, and the two files would no longer hold the same columns.
pub const REACTIONS_ABSENT_EVERY: u64 = 3;

/// Distinct `entity_type` values in the generated dataset.
pub const ENTITY_TYPES: &[&str] = &["room", "thread", "direct"];

/// Distinct `event_type` values. The search benchmarks target the last one, which is the rarest.
pub const EVENT_TYPES: &[&str] = &["message.sent", "message.edited", "reaction.added", "member.joined"];

/// Distinct `kind` payload values.
pub const KINDS: &[&str] = &["text", "image", "file", "notice"];

/// Distinct `client` payload values.
pub const CLIENTS: &[&str] = &["web", "ios", "android", "desktop"];

/// Distinct `source` connector names.
pub const SOURCES: &[&str] = &["gateway", "bot", "import"];

/// Nanoseconds between one row's `occurred_at` and the next. One row per millisecond keeps the time range wide enough
/// that a window predicate prunes most granules and row groups rather than none.
pub const OCCURRED_AT_STEP_NANOS: i64 = 1_000_000;

/// Where the generated timeline starts, in nanoseconds since the Unix epoch (2024-01-01T00:00:00Z).
pub const OCCURRED_AT_ORIGIN_NANOS: i64 = 1_704_067_200_000_000_000;

/// How long after it occurred a row was ingested, in nanoseconds.
pub const INGEST_LAG_NANOS: i64 = 250_000_000;

/// Substring the free-text search looks for. Planted in one note in this many rows.
pub const NEEDLE: &str = "release-checklist";

/// Substring the rare free-text search looks for, planted in one note in [`RARE_NEEDLE_EVERY`] rows — few enough that
/// most blocks hold no match, which is what a text index is for.
pub const RARE_NEEDLE: &str = "offsite-agenda";

/// One note in this many contains [`RARE_NEEDLE`], counted from [`RARE_NEEDLE_FIRST_ROW`].
pub const RARE_NEEDLE_EVERY: u64 = 30_000;

/// The first row carrying [`RARE_NEEDLE`]. Offset by one from the dense needle's rows so no row carries both and each
/// needle's row count is exactly what its spacing says, at every dataset size.
pub const RARE_NEEDLE_FIRST_ROW: u64 = 1;

/// Substring the absent free-text search looks for. Nothing in the generated corpus contains it, so the search finds
/// nothing and measures only what it costs a format to establish that.
pub const ABSENT_NEEDLE: &str = "quicksilver-ratchet";

/// One note in this many contains [`NEEDLE`].
pub const NEEDLE_EVERY: u64 = 997;

/// How many rows the single-field payload probe reads, spread evenly across the file. Enough that per-row lookup cost
/// dominates the loop overhead, small enough that a whole-column decode is not automatically the cheaper plan.
pub const PAYLOAD_PROBE_ROWS: usize = 256;

/// Zstd level Parquet compresses with. OE's adaptive cascade picks between LZ4, Deflate, and Zstd-3 per block, so
/// Parquet is given the matching Zstd level rather than its own default.
pub const ZSTD_LEVEL: i32 = 3;

/// The tenant every generated row belongs to. A OE file carries exactly one tenant.
pub const TENANT_SEED: u128 = 1;

/// The stream every generated row belongs to.
pub const STREAM_ID: u64 = 1;

/// The `(epoch, sequence)` epoch every generated row carries.
pub const EPOCH: u64 = 1;
