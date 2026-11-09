use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};

const MAX_EXTERNAL_URI_LEN: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalPayloadKind {
    DedicatedObject,
    ExternalUri,
    PackedSidecar,
}

impl ExternalPayloadKind {
    fn tag(self) -> u8 {
        match self {
            ExternalPayloadKind::DedicatedObject => 1,
            ExternalPayloadKind::ExternalUri => 2,
            ExternalPayloadKind::PackedSidecar => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalPayloadRef {
    pub blake3: [u8; 32],
    pub kind: ExternalPayloadKind,
    pub position: u64,
    pub size: u64,
    pub uri: Option<String>,
}

impl ExternalPayloadRef {
    pub fn encode(&self) -> Vec<u8> {
        let uri = self.uri.as_deref().unwrap_or_default();
        let mut out = Writer::with_capacity(1 + 8 + 8 + 32 + 4 + uri.len());
        out.put_u8(self.kind.tag());
        out.put_u64(self.position);
        out.put_u64(self.size);
        out.put_slice(&self.blake3);
        if self.kind == ExternalPayloadKind::ExternalUri {
            out.put_u32(uri.len() as u32);
            out.put_slice(uri.as_bytes());
        }
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        let kind = match reader.u8("external payload kind")? {
            1 => ExternalPayloadKind::DedicatedObject,
            2 => ExternalPayloadKind::ExternalUri,
            3 => ExternalPayloadKind::PackedSidecar,
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown external payload placement kind",
                });
            }
        };
        let position = reader.u64("external payload position")?;
        let size = reader.u64("external payload size")?;
        let mut blake3 = [0u8; 32];
        blake3.copy_from_slice(reader.take(32, "external payload blake3")?);
        let uri = if kind == ExternalPayloadKind::ExternalUri {
            let len = reader.u32("external payload uri length")? as usize;
            if len > MAX_EXTERNAL_URI_LEN {
                return Err(FormatError::Structural {
                    rule: "external payload uri exceeds its length bound",
                });
            }
            let raw = reader.take(len, "external payload uri")?;
            Some(
                simdutf8::basic::from_utf8(raw)
                    .map_err(|_| FormatError::Structural {
                        rule: "external payload uri is not UTF-8",
                    })?
                    .to_owned(),
            )
        } else {
            None
        };
        Ok(Self {
            blake3,
            kind,
            position,
            size,
            uri,
        })
    }

    pub fn verify(&self, fetched: &[u8]) -> Result<(), FormatError> {
        if fetched.len() as u64 != self.size {
            return Err(FormatError::Structural {
                rule: "external payload bytes differ from the declared size",
            });
        }
        if crate::file::integrity::hash_tree(fetched).as_bytes() != &self.blake3 {
            return Err(FormatError::Structural {
                rule: "external payload bytes do not match the declared blake3",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "test/external.rs"]
mod tests;
