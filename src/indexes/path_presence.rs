use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::indexes::probabilistic::SplitBlockBloomFilter;
use crate::indexes::{Exactness, stable_hash};

const PATH_PRESENCE_MAGIC: u32 = 0x5050_5831;

const BITS_PER_PATH: u32 = 10;

const PATH_PRESENCE_HEADER_BYTES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathPresenceIndex {
    filter: SplitBlockBloomFilter,
}

impl PathPresenceIndex {
    pub fn build(path_hashes: &[u64]) -> Self {
        Self::build_from_hashes(path_hashes.iter().copied(), path_hashes.len())
    }

    pub(crate) fn build_from_hashes(path_hashes: impl IntoIterator<Item = u64>, hash_count: usize) -> Self {
        PathPresenceIndex {
            filter: SplitBlockBloomFilter::build_from_iter(path_hashes, hash_count, BITS_PER_PATH),
        }
    }

    pub fn build_from_paths(paths: &[&str]) -> Self {
        Self::build_from_hashes(paths.iter().map(|&p| hash_path(p)), paths.len())
    }

    pub fn might_contain_path(&self, path: &str) -> bool {
        self.filter.contains(hash_path(path))
    }

    pub fn might_contain_hash(&self, path_hash: u64) -> bool {
        self.filter.contains(path_hash)
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::InexactNoFalseNegative
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(PATH_PRESENCE_HEADER_BYTES + self.filter.encoded_len());
        out.put_u32(PATH_PRESENCE_MAGIC);
        self.filter.encode_into(&mut out);
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        if reader.u32("path presence magic")? != PATH_PRESENCE_MAGIC {
            return Err(FormatError::Structural {
                rule: "path presence index bad magic",
            });
        }
        let remaining = reader.remaining();
        let rest = reader.take(remaining, "path presence filter bytes")?;
        let filter = SplitBlockBloomFilter::decode(rest)?;
        Ok(PathPresenceIndex { filter })
    }
}

pub fn hash_path(path: &str) -> u64 {
    stable_hash(path.as_bytes())
}

#[cfg(test)]
#[path = "test/path_presence.rs"]
mod tests;
