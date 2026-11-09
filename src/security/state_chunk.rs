use super::constant::{STATE_CHUNK_ADDRESS_CONTEXT, STATE_CHUNK_SEAL_CONTEXT};
use super::{AeadScheme, KEY_LEN, TenantDek};
use std::fmt;
use zeroize::Zeroizing;

#[derive(Clone)]
pub struct StateChunkKey {
    address: Zeroizing<[u8; KEY_LEN]>,
    seal: Zeroizing<[u8; KEY_LEN]>,
}

impl StateChunkKey {
    pub fn derive(tenant_dek: &TenantDek) -> Self {
        Self {
            address: Zeroizing::new(blake3::derive_key(STATE_CHUNK_ADDRESS_CONTEXT, &*tenant_dek.0)),
            seal: Zeroizing::new(blake3::derive_key(STATE_CHUNK_SEAL_CONTEXT, &*tenant_dek.0)),
        }
    }

    pub fn address(&self, plaintext: &[u8]) -> [u8; 32] {
        *blake3::keyed_hash(&self.address, plaintext).as_bytes()
    }

    pub fn seal(&self, plaintext: &[u8]) -> Option<([u8; 32], Vec<u8>)> {
        let address = self.address(plaintext);
        let nonce = address.first_chunk::<24>()?;
        let sealed = AeadScheme::XChaCha20Poly1305.seal(&self.seal, nonce, &address, plaintext)?;
        Some((address, sealed))
    }

    pub fn open(&self, address: &[u8; 32], sealed: &[u8]) -> Option<Vec<u8>> {
        let nonce = address.first_chunk::<24>()?;
        let plaintext = AeadScheme::XChaCha20Poly1305.open(&self.seal, nonce, address, sealed)?;
        (self.address(&plaintext) == *address).then_some(plaintext)
    }
}

impl fmt::Debug for StateChunkKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StateChunkKey(<redacted>)")
    }
}

#[cfg(test)]
#[path = "test/state_chunk.rs"]
mod tests;
