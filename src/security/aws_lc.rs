//! AWS-LC backend for every native cryptographic primitive: AES-256-GCM, XChaCha20-Poly1305, HKDF-SHA-256,
//! SHA-256, Ed25519 verification and the OS random source.
#![allow(
    unsafe_code,
    reason = "XChaCha20-Poly1305 is reached through the aws-lc-sys EVP_AEAD interface"
)]

use super::{AEAD_TAG_LEN, KEY_LEN};
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_sys::{
    EVP_AEAD_CTX, EVP_AEAD_CTX_free, EVP_AEAD_CTX_new, EVP_AEAD_CTX_open_gather, EVP_AEAD_CTX_seal_scatter,
    EVP_aead_xchacha20_poly1305,
};
use std::ptr::NonNull;

pub(crate) fn hkdf_sha256_expand(ikm: &[u8; KEY_LEN], info: &[u8], out: &mut [u8; KEY_LEN]) -> Option<()> {
    struct Len;
    impl aws_lc_rs::hkdf::KeyType for Len {
        fn len(&self) -> usize {
            KEY_LEN
        }
    }
    let prk = aws_lc_rs::hkdf::Salt::new(aws_lc_rs::hkdf::HKDF_SHA256, &[]).extract(ikm);
    let info = [info];
    prk.expand(&info, Len).ok()?.fill(out).ok()
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

pub(crate) fn ed25519_verify(public_key: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> bool {
    aws_lc_rs::signature::UnparsedPublicKey::new(&aws_lc_rs::signature::ED25519, public_key)
        .verify(message, signature)
        .is_ok()
}

pub(crate) fn fill_random(out: &mut [u8]) -> Option<()> {
    aws_lc_rs::rand::fill(out).ok()
}

fn aes_key(key: &[u8; KEY_LEN]) -> Option<LessSafeKey> {
    Some(LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).ok()?))
}

pub(crate) fn aes_gcm_seal_in_place(
    key: &[u8; KEY_LEN],
    nonce: &[u8],
    aad: &[u8],
    data: &mut [u8],
) -> Option<[u8; AEAD_TAG_LEN]> {
    let nonce = Nonce::try_assume_unique_for_key(nonce).ok()?;
    let tag = aes_key(key)?
        .seal_in_place_separate_tag(nonce, Aad::from(aad), data)
        .ok()?;
    tag.as_ref().try_into().ok()
}

pub(crate) fn aes_gcm_open(key: &[u8; KEY_LEN], nonce: &[u8], aad: &[u8], ciphertext: &[u8]) -> Option<Vec<u8>> {
    let nonce = Nonce::try_assume_unique_for_key(nonce).ok()?;
    let mut buf = ciphertext.to_vec();
    let len = aes_key(key)?.open_in_place(nonce, Aad::from(aad), &mut buf).ok()?.len();
    buf.truncate(len);
    Some(buf)
}

/// Owned `EVP_AEAD_CTX` keyed for XChaCha20-Poly1305.
struct XChaChaCtx(NonNull<EVP_AEAD_CTX>);

impl XChaChaCtx {
    fn new(key: &[u8; KEY_LEN]) -> Option<Self> {
        // SAFETY: returns a static, immutable algorithm descriptor.
        let aead = unsafe { EVP_aead_xchacha20_poly1305() };
        // SAFETY: `key` is valid for `KEY_LEN` bytes; the context copies the key and owns its own allocation.
        let ctx = unsafe { EVP_AEAD_CTX_new(aead, key.as_ptr(), KEY_LEN, AEAD_TAG_LEN) };
        NonNull::new(ctx).map(Self)
    }
}

impl Drop for XChaChaCtx {
    fn drop(&mut self) {
        // SAFETY: the pointer came from `EVP_AEAD_CTX_new` and is freed exactly once; this also zeroes the key.
        unsafe { EVP_AEAD_CTX_free(self.0.as_ptr()) }
    }
}

pub(crate) fn xchacha_seal_in_place(
    key: &[u8; KEY_LEN],
    nonce: &[u8],
    aad: &[u8],
    data: &mut [u8],
) -> Option<[u8; AEAD_TAG_LEN]> {
    if nonce.len() != 24 {
        return None;
    }
    let ctx = XChaChaCtx::new(key)?;
    let mut tag = [0u8; AEAD_TAG_LEN];
    let mut tag_len = 0usize;
    let ptr = data.as_mut_ptr();
    // SAFETY: every pointer is valid for its stated length; in-place sealing (`out == in`) is permitted by
    // AWS-LC, `tag` holds `AEAD_TAG_LEN` bytes and no extra input is passed.
    let ok = unsafe {
        EVP_AEAD_CTX_seal_scatter(
            ctx.0.as_ptr(),
            ptr,
            tag.as_mut_ptr(),
            &mut tag_len,
            AEAD_TAG_LEN,
            nonce.as_ptr(),
            nonce.len(),
            ptr,
            data.len(),
            std::ptr::null(),
            0,
            aad.as_ptr(),
            aad.len(),
        )
    };
    (ok == 1 && tag_len == AEAD_TAG_LEN).then_some(tag)
}

pub(crate) fn xchacha_open(key: &[u8; KEY_LEN], nonce: &[u8], aad: &[u8], ciphertext: &[u8]) -> Option<Vec<u8>> {
    if nonce.len() != 24 {
        return None;
    }
    let body_len = ciphertext.len().checked_sub(AEAD_TAG_LEN)?;
    let (body, tag) = ciphertext.split_at(body_len);
    let ctx = XChaChaCtx::new(key)?;
    let mut out = vec![0u8; body_len];
    // SAFETY: every pointer is valid for its stated length and `out` does not overlap the input.
    let ok = unsafe {
        EVP_AEAD_CTX_open_gather(
            ctx.0.as_ptr(),
            out.as_mut_ptr(),
            nonce.as_ptr(),
            nonce.len(),
            body.as_ptr(),
            body.len(),
            tag.as_ptr(),
            tag.len(),
            aad.as_ptr(),
            aad.len(),
        )
    };
    (ok == 1).then_some(out)
}
