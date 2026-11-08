//! Fixed values the security module stamps into stored bytes.
//!
//! See: oe-security-and-isolation/spec.md

/// The leading bytes of a payload sealed under a data subject's content key. A row's whole payload is stored as a
/// binary value that starts with these 16 bytes, then the subject id (16 bytes, little-endian), then the sealed blob;
/// a reader that sees them asks the key store for that subject's key before handing the payload out.
pub(crate) const SEALED_PAYLOAD_MAGIC: &[u8; 16] = b"open-event/seal1";

/// BLAKE3 `derive_key` context for the key that turns a state chunk's plaintext into its address. Changing it changes
/// every address, so the format fixes it.
pub(crate) const STATE_CHUNK_ADDRESS_CONTEXT: &str = "open-event 2026-10-11 state chunk address key v1";

/// BLAKE3 `derive_key` context for the key that seals a state chunk's plaintext.
pub(crate) const STATE_CHUNK_SEAL_CONTEXT: &str = "open-event 2026-10-11 state chunk seal key v1";

/// The leading bytes of one payload field sealed under a data subject's content key (per-field erasure for events that
/// mention several subjects). The field's value is stored as a binary value that starts with these 16 bytes, then the
/// subject id (16 bytes, little-endian), then the sealed blob.
pub(crate) const SEALED_FIELD_MAGIC: &[u8; 16] = b"open-event/fsea1";

/// BLAKE3 `derive_key` context for the block id a sealed field binds: derived from the event id and the field path, so
/// every field of every event seals under its own subkey and a blob moved to another field fails authentication.
pub(crate) const SEALED_FIELD_BLOCK_CONTEXT: &str = "open-event 2026-11-08 sealed field block id v1";
