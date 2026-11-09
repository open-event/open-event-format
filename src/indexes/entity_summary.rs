use super::probabilistic::SplitBlockBloomFilter;
use crate::events::EventEnvelope;
use crate::events::TenantId;
use crate::lifecycle::{ManifestGeneration, OeFileEntry};

pub const EXACT_ENTITY_SUMMARY_LIMIT: usize = 256;

pub const ENTITY_SUMMARY_BITS_PER_KEY: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct EntityKey {
    pub high: u64,
    pub low: u64,
}

impl EntityKey {
    pub fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    pub fn of_envelope(envelope: &EventEnvelope) -> Self {
        Self::new(envelope.entity_id_hash_high, envelope.entity_id_hash_low)
    }

    fn probe(self) -> u64 {
        let mixed = self.low ^ self.high.rotate_left(29).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        mixed ^ (mixed >> 31)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EntitySummary {
    Exact { keys: Vec<EntityKey> },
    Bloom { entity_count: u64, filter: Vec<u8> },
}

impl EntitySummary {
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

    pub fn may_hold(&self, key: EntityKey) -> bool {
        match self {
            Self::Exact { keys } => keys.binary_search(&key).is_ok(),
            Self::Bloom { filter, .. } => SplitBlockBloomFilter::contains_encoded(filter, key.probe()).unwrap_or(true),
        }
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact { .. })
    }

    pub fn entity_count(&self) -> u64 {
        match self {
            Self::Exact { keys } => keys.len() as u64,
            Self::Bloom { entity_count, .. } => *entity_count,
        }
    }

    pub fn exact_keys(&self) -> Option<&[EntityKey]> {
        match self {
            Self::Exact { keys } => Some(keys),
            Self::Bloom { .. } => None,
        }
    }
}

impl EntitySummary {
    pub(crate) fn encode_into(&self, out: &mut crate::file::bytes::Writer) {
        match self {
            Self::Exact { keys } => {
                out.put_u8(0);
                out.put_u32(keys.len() as u32);
                for key in keys {
                    out.put_u64(key.high);
                    out.put_u64(key.low);
                }
            }
            Self::Bloom { entity_count, filter } => {
                out.put_u8(1);
                out.put_u64(*entity_count);
                out.put_u32(filter.len() as u32);
                out.put_slice(filter);
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, crate::error::FormatError> {
        let malformed = crate::error::FormatError::Structural {
            rule: "malformed entity summary",
        };
        let mut reader = crate::file::bytes::Reader::new(bytes);
        let summary = match reader.u8("entity summary form")? {
            0 => {
                let count = reader.u32("entity summary key count")? as usize;
                if count > reader.remaining() / 16 {
                    return Err(malformed);
                }
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    let high = reader.u64("entity summary key")?;
                    let low = reader.u64("entity summary key")?;
                    keys.push(EntityKey::new(high, low));
                }
                if keys.windows(2).any(|pair| pair.first() >= pair.get(1)) {
                    return Err(malformed);
                }
                Self::Exact { keys }
            }
            1 => {
                let entity_count = reader.u64("entity summary entity count")?;
                let len = reader.u32("entity summary filter length")? as usize;
                let filter = reader.take(len, "entity summary filter")?.to_vec();
                Self::Bloom { entity_count, filter }
            }
            _ => return Err(malformed),
        };
        if reader.remaining() != 0 {
            return Err(malformed);
        }
        Ok(summary)
    }
}

impl crate::layout::reader::OeFile {
    pub fn entity_summary(&self) -> Option<&EntitySummary> {
        self.footer().entity_summary.as_ref()
    }
}

impl OeFileEntry {
    pub fn may_hold_entity(&self, key: EntityKey) -> bool {
        self.entity_summary.as_ref().is_none_or(|summary| summary.may_hold(key))
    }
}

impl ManifestGeneration {
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
