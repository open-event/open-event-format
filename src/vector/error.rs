use crate::error::FormatError;
use crate::file::error::CodecError;

#[derive(Debug, thiserror::Error)]
pub enum VectorError {
    #[error("vector segment failed authentication: {what}")]
    Authentication { what: &'static str },
    #[error("vector segment is malformed: {0}")]
    Format(#[from] FormatError),
    #[error("invalid vector segment input: {rule}")]
    InvalidInput { rule: String },
    #[error("vector model or metric mismatch: {detail}")]
    ModelMismatch { detail: String },
    #[error("vector segment has no column {name}")]
    MissingColumn { name: String },
    #[error("random number source failed")]
    Random,
    #[error("vector segment bytes unavailable: {detail}")]
    Unavailable { detail: String },
}

impl From<CodecError> for VectorError {
    fn from(error: CodecError) -> Self {
        VectorError::Format(error.into())
    }
}
