use super::constant::{
    STATE_CHUNK_BOUNDARY_SPAN_BYTES, STATE_CHUNK_CRC_LEN, STATE_CHUNK_HEADER_LEN, STATE_CHUNK_MAGIC,
    STATE_CHUNK_MAX_BYTES, STATE_CHUNK_MIN_BYTES, STATE_INTERIOR_ENTRY_LEN, STATE_LEAF_ENTRY_LEN,
};
use crate::error::{FormatError, StateChunkError, StorageError};
use crate::events::TenantId;
use crate::file::bytes::{Reader, Writer};
use crate::file::integrity::crc64_nvme;
use crate::indexes::stable_hash;
use crate::object_store::{ObjectStore, state_chunk_object_key};
use crate::security::StateChunkKey;
use dashmap::DashMap;
use hashbrown::{HashMap, HashSet};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{self, AtomicU64};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct StateKey {
    pub state_key_id: u32,
    pub type_id: u32,
}

impl Ord for StateKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.type_id, self.state_key_id).cmp(&(other.type_id, other.state_key_id))
    }
}

impl PartialOrd for StateKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StateRoot {
    pub address: [u8; 32],
    pub entry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedStateChunk {
    pub address: [u8; 32],
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateTree {
    pub chunks: Vec<SealedStateChunk>,
    pub root: StateRoot,
}

impl StateTree {
    pub fn new_chunks(&self, previous: &StateTree) -> Vec<SealedStateChunk> {
        self.new_chunks_iter(previous).cloned().collect()
    }

    pub fn new_chunks_iter<'a>(&'a self, previous: &'a StateTree) -> impl Iterator<Item = &'a SealedStateChunk> {
        let stored: HashSet<&[u8; 32]> = previous.chunks.iter().map(|chunk| &chunk.address).collect();
        self.chunks.iter().filter(move |chunk| !stored.contains(&chunk.address))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateTreeUpdate {
    pub new_chunks: Vec<SealedStateChunk>,
    pub root: StateRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateChange {
    pub after: Option<u64>,
    pub before: Option<u64>,
    pub key: StateKey,
}

pub trait StateChunkStore {
    fn get(&self, address: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError>;
}

pub struct ObjectStoreStateChunks<'a> {
    pub objects: &'a dyn ObjectStore,
    pub tenant_id: TenantId,
}

impl StateChunkStore for ObjectStoreStateChunks<'_> {
    fn get(&self, address: &[u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .objects
            .get(&state_chunk_object_key(self.tenant_id, address))?
            .map(|stored| stored.bytes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StateChunk {
    Interior {
        children: Vec<(StateKey, [u8; 32])>,
        level: u8,
    },
    Leaf(Vec<(StateKey, u64)>),
}

impl StateChunk {
    fn level(&self) -> u8 {
        match self {
            StateChunk::Interior { level, .. } => *level,
            StateChunk::Leaf(_) => 0,
        }
    }

    fn decoded_bytes(&self) -> u64 {
        match self {
            StateChunk::Interior { children, .. } => (children.len() * size_of::<(StateKey, [u8; 32])>()) as u64,
            StateChunk::Leaf(entries) => (entries.len() * size_of::<(StateKey, u64)>()) as u64,
        }
    }
}

pub struct StateChunkCache {
    budget_bytes: u64,
    chunks: DashMap<[u8; 32], CachedStateChunk, ahash::RandomState>,
    key: StateChunkKey,
    kept_bytes: AtomicU64,
    reads: AtomicU64,
}

struct CachedStateChunk {
    bytes: u64,
    chunk: Arc<StateChunk>,
    last_used: u64,
}

impl StateChunkCache {
    pub fn new(key: StateChunkKey, budget_bytes: u64) -> Self {
        Self {
            budget_bytes,
            chunks: DashMap::default(),
            key,
            kept_bytes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
        }
    }

    pub fn state_get(
        &self,
        store: &dyn StateChunkStore,
        root: &StateRoot,
        state_key: StateKey,
    ) -> Result<Option<u64>, StateChunkError> {
        lookup(root, state_key, |address, level| self.chunk(store, address, level))
    }

    fn chunk(
        &self,
        store: &dyn StateChunkStore,
        address: &[u8; 32],
        level: Option<u8>,
    ) -> Result<Arc<StateChunk>, StateChunkError> {
        let tick = self.reads.fetch_add(1, atomic::Ordering::Relaxed);
        let kept = self.chunks.get_mut(address).map(|mut slot| {
            slot.last_used = tick;
            Arc::clone(&slot.chunk)
        });
        let chunk = match kept {
            Some(chunk) => chunk,
            None => self.keep(*address, read_state_chunk(store, &self.key, address, None)?, tick),
        };
        check_level(&chunk, level)?;
        Ok(chunk)
    }

    fn keep(&self, address: [u8; 32], chunk: StateChunk, tick: u64) -> Arc<StateChunk> {
        let decoded = Arc::new(chunk);
        let bytes = decoded.decoded_bytes();
        let kept = {
            let slot = self.chunks.entry(address).or_insert(CachedStateChunk {
                bytes,
                chunk: Arc::clone(&decoded),
                last_used: tick,
            });
            if Arc::ptr_eq(&slot.chunk, &decoded) {
                self.kept_bytes.fetch_add(bytes, atomic::Ordering::Relaxed);
            }
            Arc::clone(&slot.chunk)
        };
        while self.kept_bytes.load(atomic::Ordering::Relaxed) > self.budget_bytes && self.chunks.len() > 1 {
            let Some(evict) = self
                .chunks
                .iter()
                .filter(|slot| *slot.key() != address)
                .min_by_key(|slot| slot.last_used)
                .map(|slot| *slot.key())
            else {
                break;
            };
            if let Some((_, evicted)) = self.chunks.remove(&evict) {
                self.kept_bytes.fetch_sub(evicted.bytes, atomic::Ordering::Relaxed);
            }
        }
        kept
    }
}

pub fn build_state_tree(key: &StateChunkKey, entries: &BTreeMap<StateKey, u64>) -> Result<StateTree, FormatError> {
    let entry_count = u32::try_from(entries.len()).map_err(|_| FormatError::Structural {
        rule: "a state map holds at most u32::MAX keys",
    })?;
    let mut chunks = Vec::new();
    let mut parents = Vec::new();
    let mut open = Vec::with_capacity(STATE_CHUNK_MAX_BYTES / STATE_LEAF_ENTRY_LEN);
    for (entry_key, sn) in entries {
        push_entry(
            key,
            0,
            (*entry_key, sn.to_le_bytes()),
            &mut open,
            &mut parents,
            &mut chunks,
        )?;
    }
    if !open.is_empty() || parents.is_empty() {
        parents.push(seal_chunk(key, 0, &open, &mut chunks)?);
    }
    let mut level = 0u8;
    while parents.len() > 1 {
        level += 1;
        parents = seal_level(key, level, &parents, &mut chunks)?;
    }
    let (_, address) = parents.first().ok_or(FormatError::Structural {
        rule: "a state tree has a root",
    })?;
    Ok(StateTree {
        chunks,
        root: StateRoot {
            address: *address,
            entry_count,
        },
    })
}

fn seal_level<const N: usize>(
    key: &StateChunkKey,
    level: u8,
    entries: &[(StateKey, [u8; N])],
    chunks: &mut Vec<SealedStateChunk>,
) -> Result<Vec<(StateKey, [u8; 32])>, FormatError> {
    let entry_len = 8 + N;
    let mut parents = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    for (end, (entry_key, _)) in (1..).zip(entries) {
        bytes += entry_len;
        if end == entries.len() || ends_chunk(level, *entry_key, entry_len, bytes) {
            parents.push(seal_chunk(
                key,
                level,
                entries.get(start..end).unwrap_or_default(),
                chunks,
            )?);
            start = end;
            bytes = 0;
        }
    }
    if entries.is_empty() {
        parents.push(seal_chunk(key, level, entries, chunks)?);
    }
    Ok(parents)
}

fn ends_chunk(level: u8, entry_key: StateKey, entry_len: usize, bytes: usize) -> bool {
    bytes + entry_len > STATE_CHUNK_MAX_BYTES
        || (bytes >= STATE_CHUNK_MIN_BYTES && is_boundary(level, entry_key, entry_len))
}

fn is_boundary(level: u8, entry_key: StateKey, entry_len: usize) -> bool {
    let [t0, t1, t2, t3] = entry_key.type_id.to_le_bytes();
    let [s0, s1, s2, s3] = entry_key.state_key_id.to_le_bytes();
    stable_hash(&[level, t0, t1, t2, t3, s0, s1, s2, s3])
        .is_multiple_of((STATE_CHUNK_BOUNDARY_SPAN_BYTES / entry_len) as u64)
}

fn seal_chunk<const N: usize>(
    key: &StateChunkKey,
    level: u8,
    entries: &[(StateKey, [u8; N])],
    chunks: &mut Vec<SealedStateChunk>,
) -> Result<(StateKey, [u8; 32]), FormatError> {
    let mut out = Writer::with_capacity(STATE_CHUNK_HEADER_LEN + entries.len() * (8 + N) + STATE_CHUNK_CRC_LEN);
    out.put_slice(&STATE_CHUNK_MAGIC);
    out.put_u8(level);
    out.put_slice(&[0; 3]);
    out.put_u32(entries.len() as u32);
    for (entry_key, value) in entries {
        out.put_u32(entry_key.type_id);
        out.put_u32(entry_key.state_key_id);
        out.put_slice(value);
    }
    out.put_u64(crc64_nvme(out.bytes()));
    let (address, bytes) = key.seal(out.bytes()).ok_or(FormatError::Structural {
        rule: "a state chunk is too large to seal",
    })?;
    chunks.push(SealedStateChunk { address, bytes });
    let first = entries.first().map(|(entry_key, _)| *entry_key).unwrap_or_default();
    Ok((first, address))
}

pub fn update_state_tree(
    store: &dyn StateChunkStore,
    key: &StateChunkKey,
    root: &StateRoot,
    changes: &BTreeMap<StateKey, Option<u64>>,
) -> Result<StateTreeUpdate, StateChunkError> {
    let (levels, interior) = read_levels(store, key, root)?;
    let old: HashSet<&[u8; 32]> = levels.iter().flatten().map(|(_, address)| address).collect();
    let leaf_changes: BTreeMap<StateKey, Option<[u8; 8]>> = changes
        .iter()
        .map(|(state_key, sn)| (*state_key, sn.map(u64::to_le_bytes)))
        .collect();
    let read_leaf = |address: &[u8; 32]| match read_state_chunk(store, key, address, Some(0))? {
        StateChunk::Leaf(entries) => Ok(entries
            .into_iter()
            .map(|(entry_key, sn)| (entry_key, sn.to_le_bytes()))
            .collect()),
        StateChunk::Interior { .. } => Err(StateChunkError::Format(FormatError::Structural {
            rule: "a state chunk sits one level below its parent",
        })),
    };
    let mut sealed = Vec::new();
    let (mut chunks, added) = update_level(
        key,
        0,
        levels.first().map(Vec::as_slice).unwrap_or_default(),
        read_leaf,
        &leaf_changes,
        &mut sealed,
    )?;
    let entry_count = u32::try_from(i64::from(root.entry_count) + added).map_err(|_| FormatError::Structural {
        rule: "a state map holds at most u32::MAX keys",
    })?;
    if entry_count == 0 {
        sealed = build_state_tree(key, &BTreeMap::new())?.chunks;
        chunks = sealed
            .iter()
            .map(|chunk| (StateKey::default(), chunk.address))
            .collect();
    }
    let mut level = 0u8;
    while chunks.len() > 1 {
        let below = levels.get(usize::from(level));
        level += 1;
        chunks = match (below, levels.get(usize::from(level))) {
            (Some(below), Some(above)) => {
                let read_interior = |address: &[u8; 32]| {
                    interior.get(address).cloned().ok_or_else(|| StateChunkError::Missing {
                        address: hex_simd::encode_to_string(address, hex_simd::AsciiCase::Lower),
                    })
                };
                let changes = changed_children(below, &chunks);
                update_level(key, level, above, read_interior, &changes, &mut sealed)?.0
            }
            _ => seal_level(key, level, &chunks, &mut sealed)?,
        };
    }
    let (_, address) = chunks.first().ok_or(FormatError::Structural {
        rule: "a state tree has a root",
    })?;
    let root = StateRoot {
        address: *address,
        entry_count,
    };
    sealed.retain(|chunk| !old.contains(&chunk.address));
    Ok(StateTreeUpdate {
        new_chunks: sealed,
        root,
    })
}

type ChunkList = Vec<(StateKey, [u8; 32])>;

type InteriorChunks = HashMap<[u8; 32], ChunkList>;

fn read_levels(
    store: &dyn StateChunkStore,
    key: &StateChunkKey,
    root: &StateRoot,
) -> Result<(Vec<ChunkList>, InteriorChunks), StateChunkError> {
    let mut interior = HashMap::new();
    let mut top_down = vec![vec![(StateKey::default(), root.address)]];
    let mut level = None;
    while level != Some(0) {
        let mut below = Vec::new();
        let mut below_level = None;
        for (_, address) in top_down.last().map(Vec::as_slice).unwrap_or_default() {
            if let StateChunk::Interior {
                children,
                level: parent,
            } = read_state_chunk(store, key, address, level)?
            {
                below_level = Some(parent - 1);
                below.extend_from_slice(&children);
                interior.insert(*address, children);
            }
        }
        if below.is_empty() {
            break;
        }
        top_down.push(below);
        level = below_level;
    }
    top_down.reverse();
    Ok((top_down, interior))
}

fn update_level<const N: usize>(
    key: &StateChunkKey,
    level: u8,
    old: &[(StateKey, [u8; 32])],
    mut entries_of: impl FnMut(&[u8; 32]) -> Result<Vec<(StateKey, [u8; N])>, StateChunkError>,
    changes: &BTreeMap<StateKey, Option<[u8; N]>>,
    sealed: &mut Vec<SealedStateChunk>,
) -> Result<(ChunkList, i64), StateChunkError> {
    let mut chunks = Vec::with_capacity(old.len());
    let mut added = 0i64;
    let mut pending = changes.iter().peekable();
    let mut next = 0;
    while let Some(&(&first_change, _)) = pending.peek() {
        let start = old
            .partition_point(|(first, _)| *first <= first_change)
            .saturating_sub(1)
            .max(next);
        chunks.extend_from_slice(old.get(next..start).unwrap_or_default());
        next = start;
        let mut open: Vec<(StateKey, [u8; N])> = Vec::new();
        loop {
            let bound = old.get(next + 1).map(|(first, _)| *first);
            let stored = match old.get(next) {
                Some((_, address)) => entries_of(address)?,
                None => Vec::new(),
            };
            next += 1;
            let mut stored = stored.into_iter().peekable();
            while let Some((&change_key, &value)) =
                pending.next_if(|(change_key, _)| bound.is_none_or(|bound| **change_key < bound))
            {
                while let Some(entry) = stored.next_if(|(stored_key, _)| *stored_key < change_key) {
                    push_entry(key, level, entry, &mut open, &mut chunks, sealed)?;
                }
                let replaced = stored.next_if(|(stored_key, _)| *stored_key == change_key).is_some();
                added += i64::from(value.is_some()) - i64::from(replaced);
                if let Some(value) = value {
                    push_entry(key, level, (change_key, value), &mut open, &mut chunks, sealed)?;
                }
            }
            for entry in stored {
                push_entry(key, level, entry, &mut open, &mut chunks, sealed)?;
            }
            if next >= old.len() {
                if !open.is_empty() {
                    chunks.push(seal_chunk(key, level, &open, sealed)?);
                }
                break;
            }
            if open.is_empty() {
                break;
            }
        }
    }
    chunks.extend_from_slice(old.get(next..).unwrap_or_default());
    Ok((chunks, added))
}

fn push_entry<const N: usize>(
    key: &StateChunkKey,
    level: u8,
    entry: (StateKey, [u8; N]),
    open: &mut Vec<(StateKey, [u8; N])>,
    chunks: &mut ChunkList,
    sealed: &mut Vec<SealedStateChunk>,
) -> Result<(), FormatError> {
    let entry_len = 8 + N;
    let (entry_key, _) = entry;
    open.push(entry);
    if ends_chunk(level, entry_key, entry_len, open.len() * entry_len) {
        chunks.push(seal_chunk(key, level, open, sealed)?);
        open.clear();
    }
    Ok(())
}

fn changed_children(
    old: &[(StateKey, [u8; 32])],
    new: &[(StateKey, [u8; 32])],
) -> BTreeMap<StateKey, Option<[u8; 32]>> {
    let mut changes = BTreeMap::new();
    let mut old = old.iter().peekable();
    let mut new = new.iter().peekable();
    loop {
        match (old.peek().copied(), new.peek().copied()) {
            (None, None) => return changes,
            (Some((old_first, _)), None) => {
                changes.insert(*old_first, None);
                old.next();
            }
            (None, Some((new_first, new_address))) => {
                changes.insert(*new_first, Some(*new_address));
                new.next();
            }
            (Some((old_first, old_address)), Some((new_first, new_address))) => match old_first.cmp(new_first) {
                Ordering::Less => {
                    changes.insert(*old_first, None);
                    old.next();
                }
                Ordering::Greater => {
                    changes.insert(*new_first, Some(*new_address));
                    new.next();
                }
                Ordering::Equal => {
                    if old_address != new_address {
                        changes.insert(*new_first, Some(*new_address));
                    }
                    old.next();
                    new.next();
                }
            },
        }
    }
}

pub(crate) fn decode_state_chunk(bytes: &[u8]) -> Result<StateChunk, FormatError> {
    let (body, crc) = bytes
        .split_at_checked(bytes.len().saturating_sub(STATE_CHUNK_CRC_LEN))
        .filter(|(_, crc)| crc.len() == STATE_CHUNK_CRC_LEN)
        .ok_or(FormatError::Truncated { what: "state chunk" })?;
    if crc != crc64_nvme(body).to_le_bytes() {
        return Err(FormatError::Structural {
            rule: "state chunk CRC-64/NVME mismatch",
        });
    }
    let mut reader = Reader::new(body);
    if reader.take(4, "state chunk magic")? != STATE_CHUNK_MAGIC {
        return Err(FormatError::BadMagic { expected: "HSC1" });
    }
    let level = reader.u8("state chunk level")?;
    if reader.take(3, "state chunk reserved bytes")? != [0; 3] {
        return Err(FormatError::ReservedNotZero {
            field: "state chunk reserved bytes",
        });
    }
    let count = reader.u32("state chunk entry count")? as usize;
    let entry_len = if level == 0 {
        STATE_LEAF_ENTRY_LEN
    } else {
        STATE_INTERIOR_ENTRY_LEN
    };
    if count.checked_mul(entry_len) != Some(reader.remaining()) || (level > 0 && count == 0) {
        return Err(FormatError::Structural {
            rule: "state chunk entries fill it exactly, and an interior chunk names a child",
        });
    }
    let mut previous: Option<StateKey> = None;
    let mut leaf = Vec::new();
    let mut children = Vec::new();
    for _ in 0..count {
        let entry_key = StateKey {
            type_id: reader.u32("state entry type id")?,
            state_key_id: reader.u32("state entry state key id")?,
        };
        if previous.is_some_and(|previous| previous >= entry_key) {
            return Err(FormatError::Structural {
                rule: "state chunk keys must be strictly ascending",
            });
        }
        previous = Some(entry_key);
        if level == 0 {
            leaf.push((entry_key, reader.u64("state entry event sequence number")?));
        } else {
            let address = reader.take(32, "state entry child address")?;
            children.push((entry_key, address.try_into().unwrap_or([0; 32])));
        }
    }
    Ok(if level == 0 {
        StateChunk::Leaf(leaf)
    } else {
        StateChunk::Interior { children, level }
    })
}

pub(crate) fn read_state_chunk(
    store: &dyn StateChunkStore,
    key: &StateChunkKey,
    address: &[u8; 32],
    level: Option<u8>,
) -> Result<StateChunk, StateChunkError> {
    let hex = || hex_simd::encode_to_string(address, hex_simd::AsciiCase::Lower);
    let sealed = store
        .get(address)?
        .ok_or_else(|| StateChunkError::Missing { address: hex() })?;
    let plaintext = key
        .open(address, &sealed)
        .ok_or_else(|| StateChunkError::Unauthentic { address: hex() })?;
    let chunk = decode_state_chunk(&plaintext)?;
    check_level(&chunk, level)?;
    Ok(chunk)
}

fn check_level(chunk: &StateChunk, level: Option<u8>) -> Result<(), StateChunkError> {
    if level.is_some_and(|level| level != chunk.level()) {
        return Err(StateChunkError::Format(FormatError::Structural {
            rule: "a state chunk sits one level below its parent",
        }));
    }
    Ok(())
}

pub fn state_get(
    store: &dyn StateChunkStore,
    key: &StateChunkKey,
    root: &StateRoot,
    state_key: StateKey,
) -> Result<Option<u64>, StateChunkError> {
    lookup(root, state_key, |address, level| {
        read_state_chunk(store, key, address, level)
    })
}

fn lookup<C: Borrow<StateChunk>>(
    root: &StateRoot,
    state_key: StateKey,
    mut read: impl FnMut(&[u8; 32], Option<u8>) -> Result<C, StateChunkError>,
) -> Result<Option<u64>, StateChunkError> {
    let mut address = root.address;
    let mut level = None;
    loop {
        let chunk = read(&address, level)?;
        match chunk.borrow() {
            StateChunk::Leaf(entries) => {
                let found = entries
                    .binary_search_by(|(entry_key, _)| entry_key.cmp(&state_key))
                    .ok();
                return Ok(found.and_then(|index| entries.get(index)).map(|(_, sn)| *sn));
            }
            StateChunk::Interior {
                children,
                level: parent,
            } => {
                let after = children.partition_point(|(first, _)| *first <= state_key);
                let Some((_, child)) = after.checked_sub(1).and_then(|index| children.get(index)) else {
                    return Ok(None);
                };
                address = *child;
                level = Some(parent - 1);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Pending {
    Entry(StateKey, u64),
    Subtree {
        address: [u8; 32],
        first: StateKey,
        level: u8,
    },
}

pub fn state_diff(
    store: &dyn StateChunkStore,
    key: &StateChunkKey,
    before: &StateRoot,
    after: &StateRoot,
) -> Result<Vec<StateChange>, StateChunkError> {
    let mut changes = Vec::new();
    if before.address == after.address {
        return Ok(changes);
    }
    let mut old = VecDeque::new();
    let mut new = VecDeque::new();
    expand(store, key, &before.address, None, &mut old)?;
    expand(store, key, &after.address, None, &mut new)?;
    loop {
        let (expand_old, popped_old, popped_new) = match (old.front().copied(), new.front().copied()) {
            (None, None) => return Ok(changes),
            (Some(Pending::Subtree { address: a, .. }), Some(Pending::Subtree { address: b, .. })) if a == b => {
                (None, true, true)
            }
            (Some(Pending::Entry(old_key, old_sn)), Some(Pending::Entry(new_key, new_sn))) => {
                match old_key.cmp(&new_key) {
                    Ordering::Less => {
                        changes.push(change(old_key, Some(old_sn), None));
                        (None, true, false)
                    }
                    Ordering::Greater => {
                        changes.push(change(new_key, None, Some(new_sn)));
                        (None, false, true)
                    }
                    Ordering::Equal => {
                        if old_sn != new_sn {
                            changes.push(change(old_key, Some(old_sn), Some(new_sn)));
                        }
                        (None, true, true)
                    }
                }
            }
            (Some(Pending::Entry(old_key, old_sn)), Some(Pending::Subtree { first, .. })) if old_key < first => {
                changes.push(change(old_key, Some(old_sn), None));
                (None, true, false)
            }
            (Some(Pending::Subtree { first, .. }), Some(Pending::Entry(new_key, new_sn))) if new_key < first => {
                changes.push(change(new_key, None, Some(new_sn)));
                (None, false, true)
            }
            (
                Some(Pending::Subtree {
                    first: a, level: la, ..
                }),
                Some(Pending::Subtree {
                    first: b, level: lb, ..
                }),
            ) => (Some(a < b || (a == b && la >= lb)), false, false),
            (Some(Pending::Subtree { .. }), _) => (Some(true), false, false),
            (_, Some(Pending::Subtree { .. })) => (Some(false), false, false),
            (Some(Pending::Entry(old_key, old_sn)), None) => {
                changes.push(change(old_key, Some(old_sn), None));
                (None, true, false)
            }
            (None, Some(Pending::Entry(new_key, new_sn))) => {
                changes.push(change(new_key, None, Some(new_sn)));
                (None, false, true)
            }
        };
        if popped_old {
            old.pop_front();
        }
        if popped_new {
            new.pop_front();
        }
        if let Some(expand_old) = expand_old {
            let side = if expand_old { &mut old } else { &mut new };
            if let Some(Pending::Subtree { address, level, .. }) = side.pop_front() {
                expand(store, key, &address, Some(level), side)?;
            }
        }
    }
}

fn change(key: StateKey, before: Option<u64>, after: Option<u64>) -> StateChange {
    StateChange { after, before, key }
}

fn expand(
    store: &dyn StateChunkStore,
    key: &StateChunkKey,
    address: &[u8; 32],
    level: Option<u8>,
    side: &mut VecDeque<Pending>,
) -> Result<(), StateChunkError> {
    match read_state_chunk(store, key, address, level)? {
        StateChunk::Leaf(entries) => {
            for (entry_key, sn) in entries.into_iter().rev() {
                side.push_front(Pending::Entry(entry_key, sn));
            }
        }
        StateChunk::Interior { children, level } => {
            for (first, address) in children.into_iter().rev() {
                side.push_front(Pending::Subtree {
                    address,
                    first,
                    level: level - 1,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "test/state_chunk.rs"]
mod tests;
