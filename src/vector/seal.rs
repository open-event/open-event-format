use super::constant::*;
use super::error::VectorError;
use crate::security::AeadScheme;
use zeroize::Zeroizing;

#[derive(Clone)]
pub struct WrappingKey(Zeroizing<[u8; KEY_LEN]>);

impl WrappingKey {
    pub fn new(bytes: [u8; KEY_LEN]) -> Self {
        WrappingKey(Zeroizing::new(bytes))
    }

    pub(crate) fn bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

impl std::fmt::Debug for WrappingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WrappingKey(..)")
    }
}

pub(crate) struct FileKeys {
    pub(crate) data_key: Zeroizing<[u8; KEY_LEN]>,
    pub(crate) nonce_prefix: [u8; NONCE_PREFIX_LEN],
    pub(crate) version: u16,
}

impl FileKeys {
    pub(crate) fn random() -> Result<Self, VectorError> {
        let mut data_key = Zeroizing::new([0u8; KEY_LEN]);
        let mut nonce_prefix = [0u8; NONCE_PREFIX_LEN];
        getrandom::fill(data_key.as_mut()).map_err(|_| VectorError::Random)?;
        getrandom::fill(&mut nonce_prefix).map_err(|_| VectorError::Random)?;
        Ok(FileKeys {
            data_key,
            nonce_prefix,
            version: SEGMENT_VERSION,
        })
    }

    #[cfg(test)]
    pub(crate) fn random_shared_nonce() -> Result<Self, VectorError> {
        Ok(FileKeys {
            version: SEGMENT_VERSION_SHARED_NONCE,
            ..Self::random()?
        })
    }

    pub(crate) fn seal(&self, index: u64, plaintext: &[u8]) -> Result<Vec<u8>, VectorError> {
        AeadScheme::AesGcm256
            .seal(&self.data_key, &self.nonce(index), &self.aad(index), plaintext)
            .ok_or(VectorError::Authentication { what: "seal block" })
    }

    pub(crate) fn open(&self, index: u64, sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>, VectorError> {
        AeadScheme::AesGcm256
            .open(&self.data_key, &self.nonce(index), &self.aad(index), sealed)
            .map(Zeroizing::new)
            .ok_or(VectorError::Authentication { what: "block" })
    }

    pub(crate) fn nonce(&self, index: u64) -> [u8; BLOCK_NONCE_LEN] {
        let mut nonce = [0u8; BLOCK_NONCE_LEN];
        let index = index.to_le_bytes();
        let random = match self.version {
            SEGMENT_VERSION_SHARED_NONCE => BLOCK_NONCE_LEN,
            _ => NONCE_RANDOM_LEN,
        };
        nonce
            .iter_mut()
            .zip(self.nonce_prefix.iter().take(random).chain(index.iter()))
            .for_each(|(slot, byte)| {
                *slot = *byte;
            });
        nonce
    }

    fn aad(&self, index: u64) -> Vec<u8> {
        let mut aad = Vec::with_capacity(4 + 2 + NONCE_PREFIX_LEN + 8);
        aad.extend_from_slice(SEGMENT_MAGIC);
        aad.extend_from_slice(&self.version.to_le_bytes());
        aad.extend_from_slice(&self.nonce_prefix);
        aad.extend_from_slice(&index.to_le_bytes());
        aad
    }

    pub(crate) fn header(&self, wrapping: &WrappingKey) -> Result<Vec<u8>, VectorError> {
        let mut wrap_nonce = [0u8; WRAP_NONCE_LEN];
        getrandom::fill(&mut wrap_nonce).map_err(|_| VectorError::Random)?;
        let wrapped = AeadScheme::XChaCha20Poly1305
            .seal(wrapping.bytes(), &wrap_nonce, &self.wrap_aad(), self.data_key.as_ref())
            .ok_or(VectorError::Authentication { what: "wrap data key" })?;
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(SEGMENT_MAGIC);
        header.extend_from_slice(&self.version.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes());
        header.extend_from_slice(&self.nonce_prefix);
        header.extend_from_slice(&wrap_nonce);
        header.extend_from_slice(&wrapped);
        header.resize(HEADER_LEN, 0);
        Ok(header)
    }

    pub(crate) fn from_header(header: &[u8], wrapping: &WrappingKey) -> Result<Self, VectorError> {
        if header.len() < HEADER_USED_LEN || header.get(..4) != Some(SEGMENT_MAGIC.as_slice()) {
            return Err(VectorError::Format(crate::error::FormatError::BadMagic {
                expected: "OEFV",
            }));
        }
        let version = u16::from_le_bytes([header.get(4).copied().unwrap_or(0), header.get(5).copied().unwrap_or(0)]);
        if version != SEGMENT_VERSION && version != SEGMENT_VERSION_SHARED_NONCE {
            return Err(VectorError::Format(crate::error::FormatError::UnsupportedVersion {
                field: "vector segment version",
                found: u32::from(version),
            }));
        }
        let mut nonce_prefix = [0u8; NONCE_PREFIX_LEN];
        nonce_prefix.copy_from_slice(header.get(8..8 + NONCE_PREFIX_LEN).unwrap_or(&[0u8; NONCE_PREFIX_LEN]));
        let wrap_start = 8 + NONCE_PREFIX_LEN;
        let wrap_nonce = header.get(wrap_start..wrap_start + WRAP_NONCE_LEN).unwrap_or_default();
        let wrapped = header
            .get(wrap_start + WRAP_NONCE_LEN..HEADER_USED_LEN)
            .unwrap_or_default();
        let mut keys = FileKeys {
            data_key: Zeroizing::new([0u8; KEY_LEN]),
            nonce_prefix,
            version,
        };
        let opened = AeadScheme::XChaCha20Poly1305
            .open(wrapping.bytes(), wrap_nonce, &keys.wrap_aad(), wrapped)
            .map(Zeroizing::new)
            .ok_or(VectorError::Authentication { what: "data key" })?;
        keys.data_key
            .copy_from_slice(opened.get(..KEY_LEN).unwrap_or(&[0u8; KEY_LEN]));
        Ok(keys)
    }

    fn wrap_aad(&self) -> Vec<u8> {
        let mut aad = Vec::with_capacity(4 + 2 + NONCE_PREFIX_LEN + 8);
        aad.extend_from_slice(SEGMENT_MAGIC);
        aad.extend_from_slice(&self.version.to_le_bytes());
        aad.extend_from_slice(&self.nonce_prefix);
        aad.extend_from_slice(b"datakey\0");
        aad
    }
}
