use crate::error::{FormatError, StorageError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlushError {
    #[error(transparent)]
    Format(FormatError),
    #[error(transparent)]
    Lease(LeaseError),
    #[error("frame contains events for more than one tenant or epoch")]
    MixedTenantOrEpoch,
    #[error("nothing to flush")]
    NothingToFlush,
    #[error(transparent)]
    Queue(QueueError),
    #[error(transparent)]
    Storage(StorageError),
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum LeaseError {
    #[error("lease expired; re-reserve a fresh range and retry")]
    Expired,
    #[error("unknown lease id")]
    Unknown,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum QueueError {
    #[error("record codec failure")]
    Codec,
    #[error("bounded buffer is full; wait for the head to advance")]
    Full,
    #[error("event tenant does not match this worker's tenant")]
    TenantMismatch,
}
