use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("bad magic, expected {expected}")]
    BadMagic { expected: &'static str },
    #[error("blake3 mismatch over {scope}")]
    Blake3Mismatch { scope: &'static str },
    #[error("header crc64 mismatch")]
    HeaderCrcMismatch,
    #[error("structural rule violated: {rule}")]
    Structural { rule: &'static str },
    #[error("truncated input while reading {what}")]
    Truncated { what: &'static str },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FileError {
    #[error("{what} already exists with differing content")]
    AlreadyExists { what: &'static str },
    #[error("bytes did not match the expected blake3 checksum")]
    ChecksumMismatch,
    #[error("injected fault: {kind}")]
    InjectedFault { kind: &'static str },
    #[error("range offset {offset} past end {size}")]
    InvalidRange { offset: u64, size: u64 },
    #[error("file i/o failure during {op}: {detail}")]
    Io { detail: String, op: &'static str },
    #[error("{what} not found")]
    NotFound { what: &'static str },
    #[error("read beyond written extent")]
    OutOfBounds,
    #[error("required NVMe device unavailable: {detail}")]
    RequiredDeviceMissing { detail: String },
    #[error("append not 4096-byte aligned")]
    Unaligned,
    #[error("unknown append target")]
    UnknownTarget,
}

impl From<CodecError> for FileError {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::Blake3Mismatch { .. } | CodecError::HeaderCrcMismatch => FileError::ChecksumMismatch,
            CodecError::BadMagic { expected } => FileError::Io {
                op: "decode",
                detail: format!("bad magic, expected {expected}"),
            },
            CodecError::Structural { rule } => FileError::Io {
                op: "decode",
                detail: rule.to_owned(),
            },
            CodecError::Truncated { what } => FileError::Io {
                op: "decode",
                detail: format!("truncated input while reading {what}"),
            },
        }
    }
}
