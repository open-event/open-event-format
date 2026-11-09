use super::api::BlockStore;
use super::constant::{FRAME_ALIGNMENT, MAX_LARGE_FRAME};
use super::error::FileError;
use super::model::{Atomicity, BlockTarget, DurabilityMode};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone)]
pub enum Fault {
    FailAppend { target: BlockTarget },
    FailSync { target: BlockTarget },
    TornTail { keep_bytes: u32, target: BlockTarget },
}

#[derive(Debug, Default)]
struct SimTarget {
    durable: Vec<u8>,
    pending: Vec<(u64, Vec<u8>)>,
}

#[derive(Debug)]
pub struct SimBlockStore {
    atomicity: Atomicity,
    faults: VecDeque<Fault>,
    targets: BTreeMap<BlockTarget, SimTarget>,
}

impl Default for SimBlockStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SimBlockStore {
    pub fn new() -> Self {
        Self {
            atomicity: Atomicity {
                atomic_frame_multiple: FRAME_ALIGNMENT as u32,
                awupf_bytes: 0,
                durability_mode: DurabilityMode::Buffered,
                untorn_write_bytes: 0,
            },
            faults: VecDeque::new(),
            targets: BTreeMap::new(),
        }
    }

    pub fn with_atomicity(mut self, atomicity: Atomicity) -> Self {
        self.atomicity = atomicity;
        self
    }

    pub fn inject(&mut self, fault: Fault) {
        self.faults.push_back(fault);
    }

    pub fn crash(&mut self) {
        let torn: Vec<Fault> = {
            let mut kept = VecDeque::new();
            let mut torn = Vec::new();
            while let Some(fault) = self.faults.pop_front() {
                if matches!(fault, Fault::TornTail { .. }) {
                    torn.push(fault);
                } else {
                    kept.push_back(fault);
                }
            }
            self.faults = kept;
            torn
        };
        for (target_id, target) in &mut self.targets {
            let torn_keep = torn.iter().find_map(|fault| match fault {
                Fault::TornTail { keep_bytes, target } if target == target_id => Some(*keep_bytes),
                _ => None,
            });
            if let Some(keep_bytes) = torn_keep
                && let Some((offset, bytes)) = target.pending.last()
            {
                let keep = (keep_bytes as usize).min(bytes.len());
                let end = *offset as usize + keep;
                if target.durable.len() < end {
                    target.durable.resize(end, 0);
                }
                let prefix = bytes.get(..keep).unwrap_or_default();
                if let Some(slot) = target.durable.get_mut(*offset as usize..end) {
                    slot.copy_from_slice(prefix);
                }
            }
            target.pending.clear();
        }
    }

    pub fn crash_with_surviving_pending(&mut self, target: BlockTarget, survivors: &BTreeSet<usize>) {
        if let Some(state) = self.targets.get_mut(&target) {
            let pending = std::mem::take(&mut state.pending);
            for (index, (offset, bytes)) in pending.into_iter().enumerate() {
                if survivors.contains(&index) {
                    write_durable(&mut state.durable, offset, &bytes);
                }
            }
        }
    }

    pub fn corrupt_durable_byte(&mut self, target: BlockTarget, offset: u64) {
        if let Some(state) = self.targets.get_mut(&target)
            && let Some(byte) = state.durable.get_mut(offset as usize)
        {
            *byte ^= 0xFF;
        }
    }

    fn take_fault(&mut self, matches: impl Fn(&Fault) -> bool) -> Option<Fault> {
        let position = self.faults.iter().position(matches)?;
        self.faults.remove(position)
    }
}

fn write_durable(durable: &mut Vec<u8>, offset: u64, bytes: &[u8]) {
    let end = offset as usize + bytes.len();
    if durable.len() < end {
        durable.resize(end, 0);
    }
    if let Some(slot) = durable.get_mut(offset as usize..end) {
        slot.copy_from_slice(bytes);
    }
}

impl BlockStore for SimBlockStore {
    fn append(&mut self, target: BlockTarget, frame: &[u8]) -> Result<u64, FileError> {
        if frame.is_empty() || !frame.len().is_multiple_of(FRAME_ALIGNMENT) || frame.len() > MAX_LARGE_FRAME {
            return Err(FileError::Unaligned);
        }
        if self
            .take_fault(|fault| matches!(fault, Fault::FailAppend { target: t } if *t == target))
            .is_some()
        {
            return Err(FileError::InjectedFault { kind: "fail-append" });
        }
        let state = self.targets.entry(target).or_default();
        let pending_len: usize = state.pending.iter().map(|(_, bytes)| bytes.len()).sum();
        let offset = state.durable.len() as u64 + pending_len as u64;
        state.pending.push((offset, frame.to_vec()));
        Ok(offset)
    }

    fn sync(&mut self, target: BlockTarget) -> Result<(), FileError> {
        if self
            .take_fault(|fault| matches!(fault, Fault::FailSync { target: t } if *t == target))
            .is_some()
        {
            return Err(FileError::InjectedFault { kind: "fail-sync" });
        }
        let state = self.targets.entry(target).or_default();
        for (offset, bytes) in std::mem::take(&mut state.pending) {
            write_durable(&mut state.durable, offset, &bytes);
        }
        Ok(())
    }

    fn truncate(&mut self, target: BlockTarget, len: u64) -> Result<(), FileError> {
        if !len.is_multiple_of(FRAME_ALIGNMENT as u64) {
            return Err(FileError::Unaligned);
        }
        if len > self.extent(target)? {
            return Err(FileError::OutOfBounds);
        }
        let state = self.targets.entry(target).or_default();
        for (offset, bytes) in std::mem::take(&mut state.pending) {
            write_durable(&mut state.durable, offset, &bytes);
        }
        state.durable.truncate(len as usize);
        Ok(())
    }

    fn read(&self, target: BlockTarget, offset: u64, len: u32) -> Result<Vec<u8>, FileError> {
        let state = self.targets.get(&target).ok_or(FileError::UnknownTarget)?;
        let start = offset as usize;
        let end = start.checked_add(len as usize).ok_or(FileError::OutOfBounds)?;
        let durable_len = state.durable.len();
        if end <= durable_len {
            return state
                .durable
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or(FileError::OutOfBounds);
        }
        let pending: Vec<u8> = state
            .pending
            .iter()
            .flat_map(|(_, bytes)| bytes.iter().copied())
            .collect();
        if end > durable_len + pending.len() {
            return Err(FileError::OutOfBounds);
        }
        let mut out = Vec::with_capacity(len as usize);
        if start < durable_len {
            out.extend_from_slice(state.durable.get(start..durable_len).ok_or(FileError::OutOfBounds)?);
        }
        let pending_start = start.saturating_sub(durable_len);
        out.extend_from_slice(
            pending
                .get(pending_start..end - durable_len)
                .ok_or(FileError::OutOfBounds)?,
        );
        Ok(out)
    }

    fn extent(&self, target: BlockTarget) -> Result<u64, FileError> {
        Ok(self
            .targets
            .get(&target)
            .map(|state| {
                let pending_len: usize = state.pending.iter().map(|(_, bytes)| bytes.len()).sum();
                state.durable.len() as u64 + pending_len as u64
            })
            .unwrap_or(0))
    }

    fn atomicity(&self, _target: BlockTarget) -> Result<Atomicity, FileError> {
        Ok(self.atomicity)
    }
}

#[cfg(test)]
#[path = "test/sim.rs"]
mod tests;
