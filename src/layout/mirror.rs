use super::cache::{BlockCache, BlockKey, BlockKind};
use super::reader::{OeFooter, TailRange};
use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::lifecycle::OeFileEntry;
use hashbrown::HashMap;
use std::sync::Arc;

const MIRROR_SECTION_FIXED_OVERHEAD_BYTES: usize = 56;

#[derive(Debug)]
struct MirrorSection {
    generation: u64,
    tail_bytes: Arc<[u8]>,
}

#[derive(Debug, Default)]
pub struct FooterMirror {
    sections: HashMap<u128, MirrorSection>,
}

impl FooterMirror {
    pub fn build(generation: u64, files: &[(u128, Vec<u8>)]) -> Vec<u8> {
        let mut plain = Writer::with_capacity(
            files
                .iter()
                .map(|(_, tail)| tail.len() + MIRROR_SECTION_FIXED_OVERHEAD_BYTES)
                .sum(),
        );
        plain.put_u64(files.len() as u64);
        for (file_id, tail_bytes) in files {
            plain.put_u128(*file_id);
            plain.put_u64(generation);
            plain.put_slice(&section_checksum(*file_id, generation, tail_bytes));
            plain.put_u64(tail_bytes.len() as u64);
            plain.put_slice(tail_bytes);
        }
        crate::encoding::deflate::compress(plain.bytes())
    }

    pub fn open(bytes: &[u8]) -> Result<Self, FormatError> {
        let plain = crate::encoding::deflate::decompress(bytes)?;
        let mut reader = Reader::new(&plain);
        let count = reader.u64("footer mirror section count")? as usize;
        let mut sections = HashMap::with_capacity(reader.capacity_hint(count, MIRROR_SECTION_FIXED_OVERHEAD_BYTES));
        for _ in 0..count {
            let file_id = reader.u128("footer mirror section file_id")?;
            let generation = reader.u64("footer mirror section generation")?;
            let checksum = reader.take(32, "footer mirror section blake3")?;
            let tail_len = reader.u64("footer mirror section tail length")? as usize;
            let tail_bytes = reader.take(tail_len, "footer mirror section tail bytes")?;
            if section_checksum(file_id, generation, tail_bytes) == checksum {
                sections.insert(
                    file_id,
                    MirrorSection {
                        generation,
                        tail_bytes: Arc::from(tail_bytes),
                    },
                );
            }
        }
        Ok(Self { sections })
    }

    pub fn seed_cache(&self, cache: &BlockCache, generation: u64, entries: &[OeFileEntry]) {
        for entry in entries {
            let range = entry.tail_range();
            if let Some(tail_bytes) = self.shared_tail(entry.file_id, generation)
                && range.exact
                && tail_bytes.len() as u64 == range.len
            {
                let key = BlockKey {
                    file_id: entry.file_id,
                    kind: BlockKind::Footer,
                    len: range.len,
                    offset: range.start,
                    tenant_id: entry.tenant_id,
                };
                cache.insert(key, Arc::clone(tail_bytes));
            }
        }
    }

    fn tail_bytes(&self, file_id: u128, generation: u64) -> Option<&[u8]> {
        self.shared_tail(file_id, generation).map(|tail| &tail[..])
    }

    fn shared_tail(&self, file_id: u128, generation: u64) -> Option<&Arc<[u8]>> {
        self.sections
            .get(&file_id)
            .filter(|section| section.generation == generation)
            .map(|section| &section.tail_bytes)
    }
}

fn section_checksum(file_id: u128, generation: u64, tail_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&file_id.to_le_bytes());
    hasher.update(&generation.to_le_bytes());
    hasher.update_rayon(tail_bytes);
    *hasher.finalize().as_bytes()
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the footer variant is the common outcome and is moved out at once; boxing it would cost an allocation per open"
)]
pub enum FooterSource {
    Mirror(OeFooter),
    PerFileTail(TailRange),
}

pub fn open_footer(mirror: Option<&FooterMirror>, entry: &OeFileEntry, generation: u64) -> FooterSource {
    if let Some(tail_bytes) = mirror.and_then(|mirror| mirror.tail_bytes(entry.file_id, generation))
        && let Ok(footer) = OeFooter::open(tail_bytes)
    {
        return FooterSource::Mirror(footer);
    }
    FooterSource::PerFileTail(entry.tail_range())
}

#[cfg(test)]
#[path = "test/mirror.rs"]
mod tests;
