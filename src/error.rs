use crate::file::error::{CodecError, FileError};
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FormatError {
    #[error("bad magic, expected {expected}")]
    BadMagic { expected: &'static str },
    #[error("blake3 mismatch over {scope}")]
    Blake3Mismatch { scope: &'static str },
    #[error("header crc64 mismatch")]
    HeaderCrcMismatch,
    #[error("invalid utf-8 in {what}")]
    InvalidUtf8 { what: &'static str },
    #[error("reference out of range: {what}")]
    RefOutOfRange { what: &'static str },
    #[error("reserved field not zero: {field}")]
    ReservedNotZero { field: &'static str },
    #[error("structural rule violated: {rule}")]
    Structural { rule: &'static str },
    #[error("truncated input while reading {what}")]
    Truncated { what: &'static str },
    #[error("file bytes unavailable: {detail}")]
    Unavailable { detail: String },
    #[error("unknown required feature flags {bits:#x}: refusing")]
    UnknownRequiredFeature { bits: u64 },
    #[error("unsupported version {found} in {field}")]
    UnsupportedVersion { field: &'static str, found: u32 },
}

impl From<CodecError> for FormatError {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::BadMagic { expected } => FormatError::BadMagic { expected },
            CodecError::Blake3Mismatch { scope } => FormatError::Blake3Mismatch { scope },
            CodecError::HeaderCrcMismatch => FormatError::HeaderCrcMismatch,
            CodecError::Structural { rule } => FormatError::Structural { rule },
            CodecError::Truncated { what } => FormatError::Truncated { what },
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AuditChainError {
    #[error("audit file {file_index} does not continue the chain: a file before it is missing, extra or out of order")]
    ChainBreak { file_index: usize },
    #[error("audit file {file_index} could not be read: {error}")]
    Format { error: FormatError, file_index: usize },
    #[error("file {file_index} is not an audit file")]
    NotAudit { file_index: usize },
    #[error(
        "audit file {file_index} rows do not hash to its recorded chain end: a row was altered, dropped or reordered"
    )]
    RowsAltered { file_index: usize },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum LogReadError {
    #[error("log object corrupt: {0}")]
    Corrupt(#[from] FormatError),
    #[error("log object ends in a torn tail after {intact_records} intact records ({intact_len} bytes)")]
    TornTail { intact_len: u64, intact_records: u64 },
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum IntrospectionError {
    #[error("unauthorized")]
    Unauthorized,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ProvenanceError {
    #[error("recomputed content hash does not match the hash the event carries")]
    ContentHashMismatch,
    #[error("recomputed protocol event id does not match the stored one")]
    EventIdMismatch,
    #[error("stored event cannot be rebuilt into the protocol's canonical form: {rule}")]
    MalformedEvent { rule: &'static str },
    #[error("hex field is not lowercase hex of the expected length")]
    MalformedHex,
    #[error("author public key is not a valid signing key")]
    MalformedKey,
    #[error("signature bytes are not a valid signature")]
    MalformedSignature,
    #[error("stored signer signature is not `<scheme> <signer> <key id> <key hex> <signature hex>`")]
    MalformedSignerSignature,
    #[error("a multi-signer event must carry at least one signature")]
    NoSignatures,
    #[error("payload does not carry `{field}` in the shape the protocol serializes")]
    PayloadShape { field: &'static str },
    #[error("signature does not verify against the author public key")]
    SignatureRejected,
    #[error("protocol version is not one this engine can re-verify")]
    UnknownProtocolVersion,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum RelationshipError {
    #[error("stored relationship column value is not `<space>:<lowercase hex>` of the space's width")]
    MalformedColumnValue,
    #[error("an event may declare at most one parent reference")]
    MoreThanOneParent,
    #[error("an event may declare at most one root reference")]
    MoreThanOneRoot,
    #[error("target identifier space tag is not in the registry")]
    UnknownSpace,
    #[error("target length does not match its identifier space")]
    WrongTargetLength,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ExternalIdIndexError {
    #[error("reading a file's external ids failed: {0}")]
    Format(#[from] FormatError),
    #[error("external-id store failed: {0}")]
    Storage(#[from] StorageError),
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("injected fault: {kind}")]
    InjectedFault { kind: &'static str },
    #[error("storage i/o failure during {op}: {detail}")]
    Io { op: &'static str, detail: String },
    #[error("read beyond written extent")]
    OutOfBounds,
    #[error("append not 4096-byte aligned")]
    Unaligned,
    #[error("unknown journal shard")]
    UnknownShard,
}

impl From<FileError> for StorageError {
    fn from(error: FileError) -> Self {
        match error {
            FileError::InjectedFault { kind } => StorageError::InjectedFault { kind },
            FileError::Io { detail, op } => StorageError::Io { op, detail },
            FileError::OutOfBounds | FileError::InvalidRange { .. } => StorageError::OutOfBounds,
            FileError::Unaligned => StorageError::Unaligned,
            FileError::UnknownTarget => StorageError::UnknownShard,
            other => StorageError::Io {
                op: "journal",
                detail: other.to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum PublishError {
    #[error("cas lost; current head generation {current_generation}")]
    CasLost { current_generation: u64 },
    #[error("generation object already exists")]
    GenerationExists,
    #[error("publish backend failure: {detail}")]
    Io { detail: String },
    #[error("expected generation unknown")]
    UnknownGeneration,
    #[error("publish verification failed: {rule}")]
    VerificationFailed { rule: &'static str },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SubjectPayloadError {
    #[error(transparent)]
    Format(FormatError),
    #[error(transparent)]
    KeyStore(StorageError),
    #[error("the subject's sealing key has no epochs left in its reserved range")]
    SealRefused,
    #[error("the subject has been erased; refusing to seal a new payload for it")]
    SubjectErased,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum EntityScanError {
    #[error("correction chain loops back to the event at epoch {epoch} sequence {sequence}")]
    CorrectionCycle { epoch: u64, sequence: u64 },
    #[error("correcting event at epoch {epoch} sequence {sequence} is in none of the scanned files or the overlay")]
    CorrectionNotFound { epoch: u64, sequence: u64 },
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error("deletes and corrections store failed: {detail}")]
    Store { detail: String },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum InboxError {
    #[error(transparent)]
    Deletes(#[from] EntityScanError),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error("acknowledgement watermark store failed: {detail}")]
    Watermarks { detail: String },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum StateChunkError {
    #[error("state chunk {address} is being deleted by a sweep; retry the upload")]
    BeingDeleted { address: String },
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error("state chunk {address} is missing from the store")]
    Missing { address: String },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("state chunk {address} failed authentication")]
    Unauthentic { address: String },
}
