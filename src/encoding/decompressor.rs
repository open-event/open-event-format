use super::{Compression, constant::MAX_RETAINED_INFLATE_BYTES};
use crate::error::FormatError;
use crate::file::bytes::Reader;
use std::cell::RefCell;
use std::sync::{Arc, OnceLock, RwLock};
use zstd::bulk::Decompressor as ZstdContext;

thread_local! {
    static INFLATE_BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };

    static ZSTD_CONTEXT: RefCell<ZstdContext<'static>> = RefCell::new(ZstdContext::default());
}

pub(crate) fn with_inflate_buffer<R>(decode: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    let mut buffer = INFLATE_BUFFER.with(|slot| slot.borrow_mut().take()).unwrap_or_default();
    let decoded = decode(&mut buffer);
    if buffer.capacity() <= MAX_RETAINED_INFLATE_BYTES {
        INFLATE_BUFFER.with(|slot| *slot.borrow_mut() = Some(buffer));
    }
    decoded
}

pub(crate) fn zstd_decompress_into(body: &[u8], expected: usize, plain: &mut Vec<u8>) -> Result<(), FormatError> {
    plain.clear();
    plain.reserve_exact(expected);
    let written = ZSTD_CONTEXT
        .with(|slot| slot.borrow_mut().decompress_to_buffer(body, plain))
        .map_err(|_| FormatError::Structural {
            rule: "zstd stage failed to decompress",
        })?;
    if written != expected {
        return Err(FormatError::Structural {
            rule: "zstd stage decompressed to a length other than its length prefix",
        });
    }
    Ok(())
}

const MAX_ZSTD_UNCOMPRESSED_BYTES: usize = 1 << 28;

const MAX_LZ4_UNCOMPRESSED_BYTES: usize = 1 << 28;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecompressorBackend {
    Qatzip,
    Software,
}

impl DecompressorBackend {
    pub fn label(self) -> &'static str {
        match self {
            DecompressorBackend::Qatzip => "intel_qat_qatzip",
            DecompressorBackend::Software => "software",
        }
    }
}

pub trait Decompressor: Send + Sync {
    fn decompress(&self, compression: Compression, bytes: &[u8]) -> Result<Vec<u8>, FormatError>;

    fn decompress_into(&self, compression: Compression, bytes: &[u8], plain: &mut Vec<u8>) -> Result<(), FormatError> {
        *plain = self.decompress(compression, bytes)?;
        Ok(())
    }

    fn backend(&self) -> DecompressorBackend;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SoftwareDecompressor;

impl Decompressor for SoftwareDecompressor {
    fn decompress(&self, compression: Compression, bytes: &[u8]) -> Result<Vec<u8>, FormatError> {
        let mut plain = Vec::new();
        self.decompress_into(compression, bytes, &mut plain)?;
        Ok(plain)
    }

    fn decompress_into(&self, compression: Compression, bytes: &[u8], plain: &mut Vec<u8>) -> Result<(), FormatError> {
        match compression {
            Compression::None => {
                plain.clear();
                plain.extend_from_slice(bytes);
            }
            Compression::Deflate => super::deflate::decompress_into(bytes, plain)?,
            Compression::SeekableZstd => super::seekable_zstd::decompress_all_into(bytes, plain)?,
            Compression::StringZstd => super::string_zstd::decompress_into(bytes, plain, self)?,
            Compression::Lz4 => {
                let mut reader = Reader::new(bytes);
                let uncompressed_len = reader.u32("lz4 uncompressed length")? as usize;
                if uncompressed_len > MAX_LZ4_UNCOMPRESSED_BYTES {
                    return Err(FormatError::Structural {
                        rule: "lz4 uncompressed length exceeds the per-block ceiling",
                    });
                }
                let body = reader.take(reader.remaining(), "lz4 body")?;
                plain.clear();
                plain.resize(uncompressed_len, 0);
                let written = lz4_flex::decompress_into(body, plain).map_err(|_| FormatError::Structural {
                    rule: "lz4 stage failed to decompress",
                })?;
                if written != uncompressed_len {
                    return Err(FormatError::Structural {
                        rule: "lz4 stage decompressed to a length other than its size prefix",
                    });
                }
            }
            Compression::Zstd1 | Compression::Zstd3 => {
                let mut reader = Reader::new(bytes);
                let uncompressed_len = reader.u32("zstd uncompressed length")? as usize;
                if uncompressed_len > MAX_ZSTD_UNCOMPRESSED_BYTES {
                    return Err(FormatError::Structural {
                        rule: "zstd uncompressed length exceeds the per-block ceiling",
                    });
                }
                let body = reader.take(reader.remaining(), "zstd body")?;
                zstd_decompress_into(body, uncompressed_len, plain)?;
            }
        }
        Ok(())
    }

    fn backend(&self) -> DecompressorBackend {
        DecompressorBackend::Software
    }
}

fn slot() -> &'static RwLock<Arc<dyn Decompressor>> {
    static ACTIVE: OnceLock<RwLock<Arc<dyn Decompressor>>> = OnceLock::new();
    ACTIVE.get_or_init(|| RwLock::new(Arc::new(SoftwareDecompressor)))
}

pub fn active_decompressor() -> Arc<dyn Decompressor> {
    slot()
        .read()
        .map(|guard| Arc::clone(&guard))
        .unwrap_or_else(|_| Arc::new(SoftwareDecompressor))
}

pub fn install_decompressor(decompressor: Arc<dyn Decompressor>) {
    if let Ok(mut guard) = slot().write() {
        *guard = decompressor;
    }
}

#[cfg(test)]
#[path = "test/decompressor.rs"]
mod tests;
