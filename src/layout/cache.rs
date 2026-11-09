use crate::events::TenantId;
use crate::file::FileError;
use hashbrown::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockKind {
    Footer,
    GraphIndex,
    Stripe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub file_id: u128,
    pub kind: BlockKind,
    pub len: u64,
    pub offset: u64,
    pub tenant_id: TenantId,
}

pub trait CacheStore: Send + Sync {
    fn get(&self, key: &BlockKey) -> Result<Option<Vec<u8>>, FileError>;

    fn put(&self, key: &BlockKey, bytes: &[u8]) -> Result<(), FileError>;

    fn remove(&self, key: &BlockKey) -> Result<(), FileError>;
}

pub struct BlockCache {
    disk: Option<Arc<dyn CacheStore>>,
    memory: Mutex<MemoryTier>,
}

#[derive(Debug)]
struct MemoryTier {
    budget_bytes: u64,
    entries: HashMap<BlockKey, usize>,
    free_slots: Vec<usize>,
    held_bytes: u64,
    least_recent: Option<usize>,
    most_recent: Option<usize>,
    slots: Vec<MemorySlot>,
}

#[derive(Debug)]
struct MemorySlot {
    bytes: Option<Arc<[u8]>>,
    key: BlockKey,
    newer: Option<usize>,
    older: Option<usize>,
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("disk", &self.disk.is_some())
            .field("memory_bytes", &self.memory_bytes())
            .finish()
    }
}

impl BlockCache {
    pub fn new(memory_budget_bytes: u64, disk: Option<Arc<dyn CacheStore>>) -> Self {
        Self {
            disk,
            memory: Mutex::new(MemoryTier {
                budget_bytes: memory_budget_bytes,
                entries: HashMap::new(),
                free_slots: Vec::new(),
                held_bytes: 0,
                least_recent: None,
                most_recent: None,
                slots: Vec::new(),
            }),
        }
    }

    pub fn memory_bytes(&self) -> u64 {
        self.memory.lock().unwrap_or_else(PoisonError::into_inner).held_bytes
    }

    pub(crate) fn get(&self, key: &BlockKey) -> Option<Arc<[u8]>> {
        {
            let mut memory = self.memory.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(bytes) = memory.get(key) {
                return Some(bytes);
            }
        }
        let bytes: Arc<[u8]> = self.disk.as_ref()?.get(key).ok().flatten()?.into();
        self.memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(*key, Arc::clone(&bytes));
        Some(bytes)
    }

    pub(crate) fn insert(&self, key: BlockKey, bytes: Arc<[u8]>) {
        if let Some(disk) = &self.disk {
            disk.put(&key, &bytes).ok();
        }
        self.memory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, bytes);
    }

    pub(crate) fn remove(&self, key: &BlockKey) {
        {
            let mut memory = self.memory.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(slot) = memory.entries.remove(key) {
                memory.release(slot);
            }
        }
        if let Some(disk) = &self.disk {
            disk.remove(key).ok();
        }
    }
}

impl MemoryTier {
    fn get(&mut self, key: &BlockKey) -> Option<Arc<[u8]>> {
        let slot = *self.entries.get(key)?;
        self.unlink(slot);
        self.push_most_recent(slot);
        self.slots.get(slot)?.bytes.clone()
    }

    fn insert(&mut self, key: BlockKey, bytes: Arc<[u8]>) {
        let len = bytes.len() as u64;
        if len > self.budget_bytes {
            return;
        }
        let slot = match self.entries.get(&key) {
            Some(&slot) => {
                self.unlink(slot);
                if let Some(replaced) = self.slots.get_mut(slot).and_then(|entry| entry.bytes.replace(bytes)) {
                    self.held_bytes -= replaced.len() as u64;
                }
                slot
            }
            None => {
                let fresh = MemorySlot {
                    bytes: Some(bytes),
                    key,
                    newer: None,
                    older: None,
                };
                let reused = self
                    .free_slots
                    .pop()
                    .and_then(|slot| Some((slot, self.slots.get_mut(slot)?)));
                let slot = match reused {
                    Some((slot, entry)) => {
                        *entry = fresh;
                        slot
                    }
                    None => {
                        self.slots.push(fresh);
                        self.slots.len() - 1
                    }
                };
                self.entries.insert(key, slot);
                slot
            }
        };
        self.push_most_recent(slot);
        self.held_bytes += len;
        while self.held_bytes > self.budget_bytes {
            let Some(oldest) = self.least_recent.filter(|&oldest| oldest != slot) else {
                break;
            };
            if let Some(entry) = self.slots.get(oldest) {
                let oldest_key = entry.key;
                self.entries.remove(&oldest_key);
            }
            self.release(oldest);
        }
    }

    fn release(&mut self, slot: usize) {
        self.unlink(slot);
        if let Some(bytes) = self.slots.get_mut(slot).and_then(|entry| entry.bytes.take()) {
            self.held_bytes -= bytes.len() as u64;
        }
        self.free_slots.push(slot);
    }

    fn unlink(&mut self, slot: usize) {
        let Some(entry) = self.slots.get_mut(slot) else {
            return;
        };
        let (newer, older) = (entry.newer.take(), entry.older.take());
        match newer.and_then(|newer| self.slots.get_mut(newer)) {
            Some(entry) => entry.older = older,
            None => self.most_recent = older,
        }
        match older.and_then(|older| self.slots.get_mut(older)) {
            Some(entry) => entry.newer = newer,
            None => self.least_recent = newer,
        }
    }

    fn push_most_recent(&mut self, slot: usize) {
        let most_recent = self.most_recent;
        if let Some(entry) = self.slots.get_mut(slot) {
            entry.older = most_recent;
        }
        match most_recent.and_then(|previous| self.slots.get_mut(previous)) {
            Some(entry) => entry.newer = Some(slot),
            None => self.least_recent = Some(slot),
        }
        self.most_recent = Some(slot);
    }
}

#[cfg(test)]
#[path = "test/cache.rs"]
mod tests;
