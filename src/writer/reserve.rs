use super::error::LeaseError;
use crate::artifacts::watermark::FIRST_SEQUENCE;
use crate::events::SequenceRange;
use hashbrown::HashSet;
use std::{collections::BTreeMap, ops::Bound};

pub const FORCE_COMMIT_IDLE_NANOS: u64 = 50_000;

pub const LEASE_INTERVAL_MULTIPLE: u64 = 64;

pub const LEASE_NANOS: u64 = FORCE_COMMIT_IDLE_NANOS * LEASE_INTERVAL_MULTIPLE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationLease {
    pub deadline_monotonic_nanos: u64,
    pub lease_id: u64,
    pub range: SequenceRange,
}

#[derive(Debug)]
pub struct SequenceAllocator {
    epoch: u64,
    next_lease_id: u64,
    next_sequence: u64,
    outstanding: BTreeMap<u64, ReservationLease>,
}

impl SequenceAllocator {
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            next_sequence: FIRST_SEQUENCE,
            next_lease_id: 1,
            outstanding: BTreeMap::new(),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn reserve(&mut self, count: u64, now_monotonic_nanos: u64) -> ReservationLease {
        let count = count.max(1);
        let first_sequence = self.next_sequence;
        self.next_sequence += count;
        let lease = ReservationLease {
            lease_id: self.next_lease_id,
            range: SequenceRange {
                epoch: self.epoch,
                first_sequence,
                last_sequence: first_sequence + count - 1,
            },
            deadline_monotonic_nanos: now_monotonic_nanos + LEASE_NANOS,
        };
        self.next_lease_id += 1;
        self.outstanding.insert(lease.lease_id, lease.clone());
        lease
    }

    pub fn harden(&mut self, lease_id: u64, now_monotonic_nanos: u64) -> Result<SequenceRange, LeaseError> {
        let lease = self.outstanding.get(&lease_id).ok_or(LeaseError::Unknown)?;
        if now_monotonic_nanos > lease.deadline_monotonic_nanos {
            return Err(LeaseError::Expired);
        }
        let lease = self.outstanding.remove(&lease_id).ok_or(LeaseError::Unknown)?;
        Ok(lease.range)
    }

    pub fn accept_late(&mut self, lease_id: u64) -> Result<SequenceRange, LeaseError> {
        self.outstanding
            .remove(&lease_id)
            .map(|lease| lease.range)
            .ok_or(LeaseError::Unknown)
    }

    pub fn expired_leases(&self, now_monotonic_nanos: u64) -> Vec<(u64, SequenceRange)> {
        self.outstanding
            .iter()
            .filter(|(_, lease)| now_monotonic_nanos > lease.deadline_monotonic_nanos)
            .map(|(id, lease)| (*id, lease.range))
            .collect()
    }

    pub(crate) fn next_expired_after(
        &self,
        now_monotonic_nanos: u64,
        after: Option<u64>,
    ) -> Option<(u64, SequenceRange)> {
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        self.outstanding
            .range((start, Bound::Unbounded))
            .find(|(_, lease)| now_monotonic_nanos > lease.deadline_monotonic_nanos)
            .map(|(id, lease)| (*id, lease.range))
    }

    pub fn close_voided(&mut self, lease_id: u64) -> Option<SequenceRange> {
        self.outstanding.remove(&lease_id).map(|lease| lease.range)
    }

    pub fn expire(&mut self, now_monotonic_nanos: u64) -> Vec<SequenceRange> {
        let expired: Vec<u64> = self
            .outstanding
            .iter()
            .filter(|(_, lease)| now_monotonic_nanos > lease.deadline_monotonic_nanos)
            .map(|(id, _)| *id)
            .collect();
        expired
            .into_iter()
            .filter_map(|id| self.outstanding.remove(&id))
            .map(|lease| lease.range)
            .collect()
    }

    pub fn outstanding_leases(&self) -> usize {
        self.outstanding.len()
    }
}

pub fn arbitrate_coverage(ranges: &[SequenceRange]) -> HashSet<usize> {
    let mut covered: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    let mut rejected = HashSet::default();
    for (index, range) in ranges.iter().enumerate() {
        let key = (range.epoch, range.first_sequence);
        let overlaps_earlier = covered
            .range(..=key)
            .next_back()
            .is_some_and(|((epoch, _), last)| *epoch == range.epoch && *last >= range.first_sequence);
        let overlaps_later = covered
            .range(key..)
            .next()
            .is_some_and(|((epoch, first), _)| *epoch == range.epoch && *first <= range.last_sequence);
        if overlaps_earlier || overlaps_later {
            rejected.insert(index);
        } else {
            covered.insert(key, range.last_sequence);
        }
    }
    rejected
}

#[cfg(test)]
#[path = "test/reserve.rs"]
mod tests;
