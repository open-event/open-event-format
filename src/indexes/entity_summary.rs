//! Which entities - rooms, accounts, devices, tenants' deals - one stored file holds, summarized small enough to ride
//! in the file's manifest entry, so a planner looking for one entity's events skips every file that cannot hold any
//! without opening it, and an embedder no longer keeps its own entity-to-file map.
//!
//! The writer computes the summary from the rows it builds a file from ([`EntitySummary::from_keys`]). A file holding
//! few distinct entities records them exactly, sorted, so the answer is exact in both directions; a file holding many
//! records a split-block Bloom filter over them instead, whose "no" is certain and whose "maybe" is wrong for about
//! one entity in a hundred.

use super::probabilistic::SplitBlockBloomFilter;
use crate::events::EventEnvelope;
use crate::events::TenantId;
use crate::lifecycle::{ManifestGeneration, OeFileEntry};

/// Most distinct entities a file records exactly; a file holding more records a Bloom filter instead. At 16 bytes a
/// key the exact form costs at most 4 KiB, about what the filter costs for the same count.
pub const EXACT_ENTITY_SUMMARY_LIMIT: usize = 256;

/// Bloom filter bits per distinct entity: about a 1% false-positive rate.
pub const ENTITY_SUMMARY_BITS_PER_KEY: u32 = 10;

/// One entity's grouping key: the 128-bit hash of its id, as the event envelope carries it
/// (`entity_id_hash_high`, `entity_id_hash_low`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct EntityKey {
    pub high: u64,
    pub low: u64,
}

impl EntityKey {
    pub fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    /// The key of the entity `envelope` belongs to.
    pub fn of_envelope(envelope: &EventEnvelope) -> Self {
        Self::new(envelope.entity_id_hash_high, envelope.entity_id_hash_low)
    }

    /// The 64-bit probe the Bloom form stores: both halves folded so keys sharing either half still spread.
    fn probe(self) -> u64 {
        let mixed = self.low ^ self.high.rotate_left(29).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        mixed ^ (mixed >> 31)
    }
}

/// The set of entities one file holds.
///
/// See: oe-manifest-integration/spec.md
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EntitySummary {
    /// Every distinct entity, ascending and without duplicates. Exact in both directions.
    Exact { keys: Vec<EntityKey> },
    /// A split-block Bloom filter over the file's `entity_count` distinct entities, in its own encoding
    /// ([`SplitBlockBloomFilter::encode`]). "No" is certain; "maybe" may be wrong.
    Bloom { entity_count: u64, filter: Vec<u8> },
}

impl EntitySummary {
    /// The summary of a file whose rows belong to `keys` (in any order, with repeats).
    pub fn from_keys(keys: impl IntoIterator<Item = EntityKey>) -> Self {
        let mut keys: Vec<EntityKey> = keys.into_iter().collect();
        keys.sort_unstable();
        keys.dedup();
        if keys.len() <= EXACT_ENTITY_SUMMARY_LIMIT {
            return Self::Exact { keys };
        }
        let probes: Vec<u64> = keys.iter().map(|key| key.probe()).collect();
        Self::Bloom {
            entity_count: keys.len() as u64,
            filter: SplitBlockBloomFilter::build(&probes, ENTITY_SUMMARY_BITS_PER_KEY).encode(),
        }
    }

    /// Whether the file may hold events of `key`. `false` is certain; `true` is certain only for the exact form. A
    /// filter whose bytes do not decode answers `true`, since dropping a file that holds the entity is the one unsafe
    /// answer.
    pub fn may_hold(&self, key: EntityKey) -> bool {
        match self {
            Self::Exact { keys } => keys.binary_search(&key).is_ok(),
            Self::Bloom { filter, .. } => SplitBlockBloomFilter::contains_encoded(filter, key.probe()).unwrap_or(true),
        }
    }

    /// Whether the answers of [`Self::may_hold`] are exact.
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact { .. })
    }

    /// How many distinct entities the file holds.
    pub fn entity_count(&self) -> u64 {
        match self {
            Self::Exact { keys } => keys.len() as u64,
            Self::Bloom { entity_count, .. } => *entity_count,
        }
    }

    /// The entities, when the summary records them exactly.
    pub fn exact_keys(&self) -> Option<&[EntityKey]> {
        match self {
            Self::Exact { keys } => Some(keys),
            Self::Bloom { .. } => None,
        }
    }
}

impl OeFileEntry {
    /// Whether this file may hold events of the entity `key`, from the entry alone. `false` is certain and lets a
    /// planner skip the file; `true` may be wrong. Always `true` for an entry without an entity summary (written before
    /// summaries existed, or by a builder that records none).
    pub fn may_hold_entity(&self, key: EntityKey) -> bool {
        self.entity_summary.as_ref().is_none_or(|summary| summary.may_hold(key))
    }
}

impl ManifestGeneration {
    /// The files a new query snapshot may select (see [`Self::snapshot_files`]) that belong to `tenant_id` and may
    /// hold events of the entity `key`, in manifest order. A superset: callers still filter rows by entity.
    pub fn candidate_files_for_entity(
        &self,
        tenant_id: TenantId,
        key: EntityKey,
    ) -> impl Iterator<Item = &OeFileEntry> {
        self.snapshot_files()
            .filter(move |entry| entry.tenant_id == tenant_id && entry.may_hold_entity(key))
    }
}

#[cfg(test)]
#[path = "test/entity_summary.rs"]
mod tests;
