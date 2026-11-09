use super::constant::{
    JITTER_XORSHIFT_SHIFT_1, JITTER_XORSHIFT_SHIFT_2, JITTER_XORSHIFT_SHIFT_3, SIM_CLOCK_START_NANOS,
};
use super::{Clock, JournalStorage, MonotonicClock, PublishedSet, ShardAtomicity, ShardId};
use crate::clock::FixedClock;
use crate::error::{PublishError, StorageError};
use crate::file::api::BlockStore;
use crate::file::model::BlockTarget;
use crate::file::sim::{Fault as BlockFault, SimBlockStore};
use crate::lifecycle::ManifestGeneration;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct SimClock {
    auto_advance_nanos: AtomicU64,
    monotonic_nanos: AtomicU64,
    now: FixedClock,
    rng_state: AtomicU64,
}

impl SimClock {
    pub fn new(seed: u64) -> Self {
        Self {
            auto_advance_nanos: AtomicU64::new(0),
            monotonic_nanos: AtomicU64::new(0),
            now: FixedClock::at(SIM_CLOCK_START_NANOS),
            rng_state: AtomicU64::new(seed | 1),
        }
    }

    pub fn advance(&self, nanos: u64) {
        self.now.advance(nanos as i64);
        let _ = self
            .monotonic_nanos
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |now| {
                Some(now.saturating_add(nanos))
            });
    }

    pub fn auto_advance(&self, nanos: u64) {
        self.auto_advance_nanos.store(nanos, Ordering::Relaxed);
    }
}

impl Clock for SimClock {
    fn now_nanos(&self) -> i64 {
        let now = self.now.now_nanos();
        self.advance(self.auto_advance_nanos.load(Ordering::Relaxed));
        now
    }
}

impl MonotonicClock for SimClock {
    fn monotonic_nanos(&self) -> u64 {
        self.monotonic_nanos.load(Ordering::Relaxed)
    }

    fn jitter(&self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        let mut x = self.rng_state.load(Ordering::Relaxed);
        x ^= x << JITTER_XORSHIFT_SHIFT_1;
        x ^= x >> JITTER_XORSHIFT_SHIFT_2;
        x ^= x << JITTER_XORSHIFT_SHIFT_3;
        self.rng_state.store(x, Ordering::Relaxed);
        x % bound
    }
}

#[derive(Debug, Clone)]
pub enum Fault {
    FailAppend { shard: ShardId },
    FailSync { shard: ShardId },
    TornTail { keep_bytes: u32, shard: ShardId },
}

fn target_of(shard: ShardId) -> BlockTarget {
    BlockTarget(u64::from(shard.0))
}

impl Fault {
    fn into_block(self) -> BlockFault {
        match self {
            Fault::FailAppend { shard } => BlockFault::FailAppend {
                target: target_of(shard),
            },
            Fault::FailSync { shard } => BlockFault::FailSync {
                target: target_of(shard),
            },
            Fault::TornTail { keep_bytes, shard } => BlockFault::TornTail {
                keep_bytes,
                target: target_of(shard),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct SimJournalStorage {
    inner: SimBlockStore,
}

impl SimJournalStorage {
    pub fn new() -> Self {
        Self {
            inner: SimBlockStore::new(),
        }
    }

    pub fn with_atomicity(self, atomicity: ShardAtomicity) -> Self {
        Self {
            inner: self.inner.with_atomicity(atomicity),
        }
    }

    pub fn inject(&mut self, fault: Fault) {
        self.inner.inject(fault.into_block());
    }

    pub fn crash(&mut self) {
        self.inner.crash();
    }

    pub fn crash_with_surviving_pending(&mut self, shard: ShardId, survivors: &BTreeSet<usize>) {
        self.inner.crash_with_surviving_pending(target_of(shard), survivors);
    }

    pub fn corrupt_durable_byte(&mut self, shard: ShardId, offset: u64) {
        self.inner.corrupt_durable_byte(target_of(shard), offset);
    }
}

impl JournalStorage for SimJournalStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, StorageError> {
        Ok(self.inner.append(target_of(shard), frame)?)
    }

    fn sync(&mut self, shard: ShardId) -> Result<(), StorageError> {
        Ok(self.inner.sync(target_of(shard))?)
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), StorageError> {
        Ok(self.inner.truncate(target_of(shard), len)?)
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, StorageError> {
        Ok(self.inner.read(target_of(shard), offset, len)?)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, StorageError> {
        Ok(self.inner.extent(target_of(shard))?)
    }

    fn atomicity(&self, shard: ShardId) -> Result<ShardAtomicity, StorageError> {
        Ok(self.inner.atomicity(target_of(shard))?)
    }
}

#[derive(Debug, Default)]
pub struct SimulatedPublishedSet {
    generations: BTreeMap<u64, ManifestGeneration>,
    head: u64,
}

impl SimulatedPublishedSet {
    pub fn new() -> Self {
        let mut generations = BTreeMap::new();
        generations.insert(0, ManifestGeneration::default());
        Self { generations, head: 0 }
    }
}

impl PublishedSet for SimulatedPublishedSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError> {
        let generation = self
            .generations
            .get(&self.head)
            .cloned()
            .ok_or(PublishError::UnknownGeneration)?;
        Ok((self.head, generation))
    }

    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError> {
        if self.generations.contains_key(&generation.generation) {
            return Err(PublishError::GenerationExists);
        }
        self.generations.insert(generation.generation, generation);
        Ok(())
    }

    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError> {
        if !self.generations.contains_key(&next) {
            return Err(PublishError::UnknownGeneration);
        }
        if self.head != expected {
            return Err(PublishError::CasLost {
                current_generation: self.head,
            });
        }
        self.head = next;
        Ok(())
    }

    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError> {
        self.generations
            .get(&id)
            .cloned()
            .ok_or(PublishError::UnknownGeneration)
    }
}

#[derive(Debug, Default)]
pub struct SerialEncodeExecutor;

impl super::EncodeExecutor for SerialEncodeExecutor {
    fn parallelism(&self) -> usize {
        1
    }

    fn run_jobs(&self, job_count: usize, job: &(dyn Fn(usize) + Sync)) {
        for index in 0..job_count {
            job(index);
        }
    }
}
