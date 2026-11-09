pub mod constant;
pub mod disk;
pub mod io;
pub mod sim;

use super::error::{PublishError, StorageError};
use super::lifecycle::ManifestGeneration;
pub use crate::clock::Clock;
pub use crate::file::model::{Atomicity as ShardAtomicity, DurabilityMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub u32);

pub trait MonotonicClock: Clock {
    fn monotonic_nanos(&self) -> u64;
    fn jitter(&self, bound: u64) -> u64;
}

pub trait JournalStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, StorageError>;
    fn sync(&mut self, shard: ShardId) -> Result<(), StorageError>;
    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), StorageError>;
    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, StorageError>;
    fn extent(&self, shard: ShardId) -> Result<u64, StorageError>;
    fn atomicity(&self, shard: ShardId) -> Result<ShardAtomicity, StorageError>;
}

pub trait EncodeExecutor: Sync {
    fn parallelism(&self) -> usize {
        1
    }

    fn run_jobs(&self, job_count: usize, job: &(dyn Fn(usize) + Sync));
}

pub trait PublishedSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError>;
    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError>;
    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError>;
    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError>;
}
