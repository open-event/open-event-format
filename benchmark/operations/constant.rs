
pub const FIELD_CHAR_COUNT: &str = "char_count";

pub const FIELD_KIND: &str = "kind";

pub const FIELD_NOTE: &str = "note";

pub const FIELD_CLIENT: &str = "client";

pub const FIELD_REACTIONS: &str = "reactions";

pub const REACTIONS_ABSENT_EVERY: u64 = 3;

pub const ENTITY_TYPES: &[&str] = &["room", "thread", "direct"];

pub const EVENT_TYPES: &[&str] = &["message.sent", "message.edited", "reaction.added", "member.joined"];

pub const KINDS: &[&str] = &["text", "image", "file", "notice"];

pub const CLIENTS: &[&str] = &["web", "ios", "android", "desktop"];

pub const SOURCES: &[&str] = &["gateway", "bot", "import"];

pub const OCCURRED_AT_STEP_NANOS: i64 = 1_000_000;

pub const OCCURRED_AT_ORIGIN_NANOS: i64 = 1_704_067_200_000_000_000;

pub const INGEST_LAG_NANOS: i64 = 250_000_000;

pub const NEEDLE: &str = "release-checklist";

pub const RARE_NEEDLE: &str = "offsite-agenda";

pub const RARE_NEEDLE_EVERY: u64 = 30_000;

pub const RARE_NEEDLE_FIRST_ROW: u64 = 1;

pub const ABSENT_NEEDLE: &str = "quicksilver-ratchet";

pub const NEEDLE_EVERY: u64 = 997;

pub const PAYLOAD_PROBE_ROWS: usize = 256;

pub const ZSTD_LEVEL: i32 = 3;

pub const TENANT_SEED: u128 = 1;

pub const STREAM_ID: u64 = 1;

pub const EPOCH: u64 = 1;
