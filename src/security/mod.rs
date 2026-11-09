use crate::events::TenantId;
#[cfg(target_arch = "wasm32")]
use aes_gcm::{Aes256Gcm, Nonce as AesNonce};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key as XChaChaKey, XChaCha20Poly1305, XNonce};
#[cfg(not(target_arch = "wasm32"))]
use hardware_rust_crypto::aes_gcm::HardwareAes256Gcm;
use hashbrown::{HashMap, HashSet};
use hkdf::Hkdf;
use sha2::Sha256;
use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex, PoisonError};

pub use state_chunk::*;
pub use subject_fields::*;
pub use subject_keys::*;
pub use subject_payload::*;

mod constant;
mod state_chunk;
mod subject_fields;
mod subject_keys;
mod subject_payload;

const MASTER_KEY_TENANT_DEK_INFO: &[u8] = b"open-event/master-key/tenant-dek/aes-256-gcm/v1";

const TENANT_DEK_CONTENT_KEY_INFO: &[u8] = b"open-event/tenant-dek/content-key/aes-256-gcm/v1";

const CONTENT_KEY_BLOCK_SEAL_INFO: &[u8] = b"open-event/content-key/block-seal/v1";

const KEY_LEN: usize = 32;

fn derive_wrapping_key(input_key_material: &[u8; KEY_LEN], info: &[u8]) -> Option<zeroize::Zeroizing<[u8; KEY_LEN]>> {
    let hkdf = Hkdf::<Sha256>::new(None, input_key_material);
    let mut key = zeroize::Zeroizing::new([0u8; KEY_LEN]);
    hkdf.expand(info, &mut *key).ok()?;
    Some(key)
}

fn derive_block_subkey(
    content_key: &[u8; KEY_LEN],
    block_id: u64,
    key_epoch: u64,
) -> Option<zeroize::Zeroizing<[u8; KEY_LEN]>> {
    let mut info = Vec::with_capacity(CONTENT_KEY_BLOCK_SEAL_INFO.len() + size_of::<u64>() * 2);
    info.extend_from_slice(CONTENT_KEY_BLOCK_SEAL_INFO);
    info.extend_from_slice(&block_id.to_le_bytes());
    info.extend_from_slice(&key_epoch.to_le_bytes());
    derive_wrapping_key(content_key, &info)
}

fn split_nonce(sealed: &[u8], scheme: AeadScheme) -> Option<(&[u8], &[u8])> {
    if sealed.len() < scheme.nonce_len() {
        return None;
    }
    Some(sealed.split_at(scheme.nonce_len()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AeadScheme {
    #[default]
    AesGcm256,
    XChaCha20Poly1305,
}

impl AeadScheme {
    pub const fn nonce_len(self) -> usize {
        match self {
            AeadScheme::AesGcm256 => 12,
            AeadScheme::XChaCha20Poly1305 => 24,
        }
    }

    pub const fn crate_name(self) -> &'static str {
        match self {
            AeadScheme::AesGcm256 => "hardware-rust-crypto",
            AeadScheme::XChaCha20Poly1305 => "chacha20poly1305",
        }
    }

    pub(crate) fn seal(self, key: &[u8; KEY_LEN], nonce: &[u8], aad: &[u8], plaintext: &[u8]) -> Option<Vec<u8>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            AeadScheme::AesGcm256 => HardwareAes256Gcm::new(key)
                .ok()?
                .encrypt_with_nonce(nonce, aad, plaintext)
                .ok(),
            #[cfg(target_arch = "wasm32")]
            AeadScheme::AesGcm256 => {
                let nonce: [u8; 12] = nonce.try_into().ok()?;
                let payload = Payload { msg: plaintext, aad };
                Aes256Gcm::new(&(*key).into())
                    .encrypt(&AesNonce::from(nonce), payload)
                    .ok()
            }
            AeadScheme::XChaCha20Poly1305 => {
                let nonce: [u8; 24] = nonce.try_into().ok()?;
                let key: XChaChaKey = (*key).into();
                let payload = Payload { msg: plaintext, aad };
                XChaCha20Poly1305::new(&key).encrypt(&XNonce::from(nonce), payload).ok()
            }
        }
    }

    pub(crate) fn open(self, key: &[u8; KEY_LEN], nonce: &[u8], aad: &[u8], ciphertext: &[u8]) -> Option<Vec<u8>> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            AeadScheme::AesGcm256 => HardwareAes256Gcm::new(key)
                .ok()?
                .decrypt_with_nonce(nonce, aad, ciphertext)
                .ok(),
            #[cfg(target_arch = "wasm32")]
            AeadScheme::AesGcm256 => {
                let nonce: [u8; 12] = nonce.try_into().ok()?;
                let payload = Payload { msg: ciphertext, aad };
                Aes256Gcm::new(&(*key).into())
                    .decrypt(&AesNonce::from(nonce), payload)
                    .ok()
            }
            AeadScheme::XChaCha20Poly1305 => {
                let nonce: [u8; 24] = nonce.try_into().ok()?;
                let key: XChaChaKey = (*key).into();
                let payload = Payload { msg: ciphertext, aad };
                XChaCha20Poly1305::new(&key).decrypt(&XNonce::from(nonce), payload).ok()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockNonce {
    pub bytes: Vec<u8>,
    pub scheme: AeadScheme,
}

impl BlockNonce {
    pub fn derive(scheme: AeadScheme, block_id: u64, key_epoch: u64) -> Self {
        let mut bytes = Vec::with_capacity(scheme.nonce_len().max(size_of::<u64>() * 2));
        bytes.extend_from_slice(&block_id.to_le_bytes());
        bytes.extend_from_slice(&key_epoch.to_le_bytes());
        bytes.resize(scheme.nonce_len(), 0);
        BlockNonce { bytes, scheme }
    }
}

#[derive(Debug, Clone)]
pub struct KeyEpochOdometer {
    epoch: u64,
    messages_per_epoch: u64,
    sealed_this_epoch: u64,
}

impl KeyEpochOdometer {
    pub fn new(messages_per_epoch: u64) -> Self {
        Self {
            epoch: 0,
            messages_per_epoch: messages_per_epoch.max(1),
            sealed_this_epoch: 0,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn sealed_this_epoch(&self) -> u64 {
        self.sealed_this_epoch
    }

    pub fn reserve(&mut self) -> u64 {
        if self.sealed_this_epoch >= self.messages_per_epoch {
            self.epoch += 1;
            self.sealed_this_epoch = 0;
        }
        self.sealed_this_epoch += 1;
        self.epoch
    }
}

#[derive(Clone)]
pub struct MasterKey(zeroize::Zeroizing<[u8; KEY_LEN]>);

impl MasterKey {
    pub fn new(key_material: [u8; KEY_LEN]) -> Self {
        Self(zeroize::Zeroizing::new(key_material))
    }

    pub fn wrap_tenant_dek(&self, dek: &TenantDek, tenant: TenantId, scheme: AeadScheme) -> Option<Vec<u8>> {
        let wrapping_key = derive_wrapping_key(&self.0, MASTER_KEY_TENANT_DEK_INFO)?;
        wrap_key_material(&wrapping_key, &dek.0, &tenant.uuid().to_bytes_le(), scheme)
    }

    pub fn unwrap_tenant_dek(&self, wrapped: &[u8], tenant: TenantId, scheme: AeadScheme) -> Option<TenantDek> {
        let wrapping_key = derive_wrapping_key(&self.0, MASTER_KEY_TENANT_DEK_INFO)?;
        let material = unwrap_key_material(&wrapping_key, wrapped, &tenant.uuid().to_bytes_le(), scheme)?;
        Some(TenantDek(material))
    }
}

impl fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MasterKey(<redacted>)")
    }
}

#[derive(Clone)]
pub struct TenantDek(zeroize::Zeroizing<[u8; KEY_LEN]>);

impl TenantDek {
    pub fn new(key_material: [u8; KEY_LEN]) -> Self {
        Self(zeroize::Zeroizing::new(key_material))
    }

    pub fn wrap_content_key(&self, content_key: &ContentKey, id: ContentKeyId, scheme: AeadScheme) -> Option<Vec<u8>> {
        let wrapping_key = derive_wrapping_key(&self.0, TENANT_DEK_CONTENT_KEY_INFO)?;
        wrap_key_material(&wrapping_key, &content_key.0, &id.uuid().to_bytes_le(), scheme)
    }

    pub fn unwrap_content_key(&self, wrapped: &[u8], id: ContentKeyId, scheme: AeadScheme) -> Option<ContentKey> {
        let wrapping_key = derive_wrapping_key(&self.0, TENANT_DEK_CONTENT_KEY_INFO)?;
        let material = unwrap_key_material(&wrapping_key, wrapped, &id.uuid().to_bytes_le(), scheme)?;
        Some(ContentKey(material))
    }
}

impl fmt::Debug for TenantDek {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TenantDek(<redacted>)")
    }
}

#[derive(Clone)]
pub struct ContentKey(zeroize::Zeroizing<[u8; KEY_LEN]>);

impl zeroize::ZeroizeOnDrop for ContentKey {}

impl ContentKey {
    pub fn new(key_material: [u8; KEY_LEN]) -> Self {
        Self(zeroize::Zeroizing::new(key_material))
    }

    pub(crate) fn seal(
        &self,
        block_id: u64,
        file_id: u128,
        key_epoch: u64,
        plaintext: &[u8],
        scheme: AeadScheme,
    ) -> Option<Vec<u8>> {
        let subkey = derive_block_subkey(&self.0, block_id, key_epoch)?;
        let nonce = BlockNonce::derive(scheme, block_id, key_epoch);
        let ciphertext = scheme.seal(&subkey, &nonce.bytes, &block_seal_aad(block_id, file_id), plaintext)?;
        let mut sealed = nonce.bytes;
        sealed.extend_from_slice(&ciphertext);
        sealed.extend_from_slice(&key_epoch.to_le_bytes());
        Some(sealed)
    }

    pub fn open(&self, block_id: u64, file_id: u128, sealed: &[u8], scheme: AeadScheme) -> Option<Vec<u8>> {
        let body_len = sealed.len().checked_sub(8)?;
        let (body, epoch_bytes) = sealed.split_at(body_len);
        let key_epoch = u64::from_le_bytes(epoch_bytes.try_into().ok()?);
        let subkey = derive_block_subkey(&self.0, block_id, key_epoch)?;
        let (nonce, ciphertext) = split_nonce(body, scheme)?;
        scheme.open(&subkey, nonce, &block_seal_aad(block_id, file_id), ciphertext)
    }
}

impl fmt::Debug for ContentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ContentKey(<redacted>)")
    }
}

const BLOCK_SEAL_AAD_LEN: usize = size_of::<u64>() + size_of::<u128>();

fn block_seal_aad(block_id: u64, file_id: u128) -> [u8; BLOCK_SEAL_AAD_LEN] {
    let mut aad = [0u8; BLOCK_SEAL_AAD_LEN];
    let (block_bytes, file_bytes) = aad.split_at_mut(size_of::<u64>());
    block_bytes.copy_from_slice(&block_id.to_le_bytes());
    file_bytes.copy_from_slice(&file_id.to_le_bytes());
    aad
}

fn wrap_key_material(
    wrapping_key: &[u8; KEY_LEN],
    material: &[u8; KEY_LEN],
    aad: &[u8],
    scheme: AeadScheme,
) -> Option<Vec<u8>> {
    let mut nonce = vec![0u8; scheme.nonce_len()];
    getrandom::fill(&mut nonce).ok()?;
    let ciphertext = scheme.seal(wrapping_key, &nonce, aad, material)?;
    let mut wrapped = nonce;
    wrapped.extend_from_slice(&ciphertext);
    Some(wrapped)
}

fn unwrap_key_material(
    wrapping_key: &[u8; KEY_LEN],
    wrapped: &[u8],
    aad: &[u8],
    scheme: AeadScheme,
) -> Option<zeroize::Zeroizing<[u8; KEY_LEN]>> {
    let (nonce, ciphertext) = split_nonce(wrapped, scheme)?;
    let material = zeroize::Zeroizing::new(scheme.open(wrapping_key, nonce, aad, ciphertext)?);
    let key: [u8; KEY_LEN] = material.as_slice().try_into().ok()?;
    Some(zeroize::Zeroizing::new(key))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FooterEncryption {
    Encrypted,
    #[default]
    Plaintext,
}

crate::typed_id::define_typed_id!(ContentKeyId, ContentKeyIdTag, "content_key");

#[derive(Clone)]
pub struct SealedContentKey {
    content_key: ContentKey,
    epoch_limit: u64,
    scheme: AeadScheme,
    state: Arc<Mutex<SealState>>,
}

impl zeroize::ZeroizeOnDrop for SealedContentKey {}

struct SealState {
    key_epoch: u64,
    sealed_nonces: HashSet<Vec<u8>>,
}

impl SealedContentKey {
    pub fn new(content_key: ContentKey, key_epoch: u64, scheme: AeadScheme) -> Self {
        Self::unchecked_epoch_range(content_key, key_epoch, u64::MAX, scheme)
    }

    pub fn with_epoch_range(
        content_key: ContentKey,
        key_epoch: u64,
        epoch_limit: u64,
        scheme: AeadScheme,
    ) -> Option<Self> {
        if key_epoch >= epoch_limit {
            return None;
        }
        Some(Self::unchecked_epoch_range(content_key, key_epoch, epoch_limit, scheme))
    }

    fn unchecked_epoch_range(content_key: ContentKey, key_epoch: u64, epoch_limit: u64, scheme: AeadScheme) -> Self {
        Self {
            content_key,
            epoch_limit,
            scheme,
            state: Arc::new(Mutex::new(SealState {
                key_epoch,
                sealed_nonces: HashSet::new(),
            })),
        }
    }

    pub fn key_epoch(&self) -> u64 {
        self.lock_state().key_epoch
    }

    pub fn scheme(&self) -> AeadScheme {
        self.scheme
    }

    pub fn encrypt(&mut self, block_id: u64, file_id: u128, plaintext: &[u8]) -> Option<Vec<u8>> {
        let mut state = self.lock_state();
        if state.key_epoch >= self.epoch_limit {
            return None;
        }
        let mut nonce = BlockNonce::derive(self.scheme, block_id, state.key_epoch);
        while state.sealed_nonces.contains(&nonce.bytes) {
            let next_epoch = state.key_epoch.checked_add(1).filter(|next| *next < self.epoch_limit)?;
            state.key_epoch = next_epoch;
            nonce = BlockNonce::derive(self.scheme, block_id, state.key_epoch);
        }
        let sealed = self
            .content_key
            .seal(block_id, file_id, state.key_epoch, plaintext, self.scheme)?;
        state.sealed_nonces.insert(nonce.bytes);
        Some(sealed)
    }

    pub fn decrypt(&self, block_id: u64, file_id: u128, sealed: &[u8]) -> Option<Vec<u8>> {
        self.content_key.open(block_id, file_id, sealed, self.scheme)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SealState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for SealedContentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealedContentKey")
            .field("key_epoch", &self.key_epoch())
            .field("scheme", &self.scheme)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DecryptOutcome {
    Plaintext(Vec<u8>),
    Rejected,
    Tombstone,
}

enum ContentKeyEntry {
    Active { wrapped_key: Vec<u8> },
    Destroyed,
}

pub struct ContentKeyStore {
    keys: HashMap<ContentKeyId, ContentKeyEntry>,
    scheme: AeadScheme,
    tenant_dek: TenantDek,
}

impl ContentKeyStore {
    pub fn new(tenant_dek: TenantDek) -> Self {
        Self::with_scheme(tenant_dek, AeadScheme::default())
    }

    pub fn with_scheme(tenant_dek: TenantDek, scheme: AeadScheme) -> Self {
        Self {
            keys: HashMap::new(),
            scheme,
            tenant_dek,
        }
    }

    pub fn register(&mut self, id: ContentKeyId, wrapped_key: Vec<u8>) {
        if matches!(self.keys.get(&id), Some(ContentKeyEntry::Destroyed)) {
            return;
        }
        self.keys.insert(id, ContentKeyEntry::Active { wrapped_key });
    }

    pub fn destroy(&mut self, id: ContentKeyId) {
        self.keys.insert(id, ContentKeyEntry::Destroyed);
    }

    pub fn is_destroyed(&self, id: ContentKeyId) -> bool {
        matches!(self.keys.get(&id), Some(ContentKeyEntry::Destroyed))
    }

    pub fn rekey(&mut self, id: ContentKeyId, new_wrapped_key: Vec<u8>) -> bool {
        match self.keys.get_mut(&id) {
            Some(ContentKeyEntry::Active { wrapped_key }) => {
                *wrapped_key = new_wrapped_key;
                true
            }
            _ => false,
        }
    }

    pub fn decrypt_or_tombstone(
        &self,
        id: ContentKeyId,
        block_id: u64,
        file_id: u128,
        sealed_block: &[u8],
    ) -> DecryptOutcome {
        match self.keys.get(&id) {
            Some(ContentKeyEntry::Active { wrapped_key }) => {
                match self.tenant_dek.unwrap_content_key(wrapped_key, id, self.scheme) {
                    Some(content_key) => {
                        let sealed_key = SealedContentKey::new(content_key, 0, self.scheme);
                        match sealed_key.decrypt(block_id, file_id, sealed_block) {
                            Some(plaintext) => DecryptOutcome::Plaintext(plaintext),
                            None => DecryptOutcome::Rejected,
                        }
                    }
                    None => DecryptOutcome::Rejected,
                }
            }
            Some(ContentKeyEntry::Destroyed) | None => DecryptOutcome::Tombstone,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyScope {
    None,
    SubjectKeys,
    TenantDek,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct KeyResidencyRegion(pub String);

#[derive(Debug, Clone)]
pub struct KeyLease {
    pub residency: KeyResidencyRegion,
    pub scope: KeyScope,
    pub tenant_id: TenantId,
}

pub fn is_eligible_for_job(
    tenant_id: TenantId,
    required_scope: KeyScope,
    required_region: &KeyResidencyRegion,
    node_leases: &[KeyLease],
) -> bool {
    let holds_in_region = |scope: KeyScope| {
        node_leases
            .iter()
            .any(|lease| lease.tenant_id == tenant_id && lease.scope == scope && lease.residency == *required_region)
    };
    match required_scope {
        KeyScope::None => true,
        KeyScope::SubjectKeys => holds_in_region(KeyScope::SubjectKeys) && holds_in_region(KeyScope::TenantDek),
        KeyScope::TenantDek => holds_in_region(KeyScope::TenantDek),
    }
}

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
