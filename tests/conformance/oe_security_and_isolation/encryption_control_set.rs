use oe::deletes::{RebuiltPayload, SubjectContentKey, rebuild_payload};
use oe::events::families::{Caller, ColumnFamily, column_allowed};
use oe::security::{AeadScheme, BlockNonce, FooterEncryption};

const FILE_ID: u128 = 0xF11E_D00D;

#[test]
fn scan_path_uses_file_dek_columns_only() {
    assert!(
        ColumnFamily::EmbeddingColumnsInternal.internal_only(),
        "embedding columns are encrypted under subject content keys and must be internal_only"
    );
    for embedding_col in ["embedding_text", "embedding_semantic", "embedding_quantized"] {
        assert!(
            !column_allowed(embedding_col, Caller::Public),
            "{embedding_col} is subject-content-key encrypted and must be blocked at the scan boundary"
        );
    }

    for analytical_col in ["occurred_at", "event_type_id", "source_id", "entity_type_id"] {
        assert!(
            column_allowed(analytical_col, Caller::Public),
            "{analytical_col} is a file-DEK column and must be readable at the scan boundary"
        );
    }
}

#[test]
fn sensitive_schema_footer() {
    assert_eq!(
        FooterEncryption::default(),
        FooterEncryption::Plaintext,
        "footer encryption must default to Plaintext — it is opt-in only"
    );
    assert_ne!(
        FooterEncryption::Encrypted,
        FooterEncryption::Plaintext,
        "Encrypted is a distinct value from Plaintext so a tenant can opt in"
    );
    for policy in [FooterEncryption::Plaintext, FooterEncryption::Encrypted] {
        let _: FooterEncryption = policy;
    }
}

#[test]
fn payload_aead_uses_the_pinned_cipher() {
    assert_eq!(
        AeadScheme::default(),
        AeadScheme::AesGcm256,
        "default AEAD must be AES-256-GCM (hardware-only `hardware-rust-crypto`)"
    );

    for scheme in [AeadScheme::AesGcm256, AeadScheme::XChaCha20Poly1305] {
        match scheme {
            AeadScheme::AesGcm256 => {
                assert_eq!(scheme.nonce_len(), 12, "AES-256-GCM nonce must be 96 bits (12 bytes)");
                assert_eq!(
                    scheme.crate_name(),
                    "hardware-rust-crypto",
                    "must name the hardware-rust-crypto crate"
                );
            }
            AeadScheme::XChaCha20Poly1305 => {
                assert_eq!(
                    scheme.nonce_len(),
                    24,
                    "XChaCha20-Poly1305 nonce must be 192 bits (24 bytes)"
                );
                assert_eq!(
                    scheme.crate_name(),
                    "chacha20poly1305",
                    "must name the RustCrypto chacha20poly1305 crate"
                );
            }
        }
    }

    let nonce_block1_inv0 = BlockNonce::derive(AeadScheme::AesGcm256, 1, 0);
    let nonce_block2_inv0 = BlockNonce::derive(AeadScheme::AesGcm256, 2, 0);
    let nonce_block1_inv1 = BlockNonce::derive(AeadScheme::AesGcm256, 1, 1);
    assert_ne!(
        nonce_block1_inv0.bytes, nonce_block2_inv0.bytes,
        "different block IDs must produce different nonces under the same key"
    );
    assert_ne!(
        nonce_block1_inv0.bytes, nonce_block1_inv1.bytes,
        "different invocation counters on the same block must produce different nonces"
    );

    assert_eq!(nonce_block1_inv0.bytes.len(), AeadScheme::AesGcm256.nonce_len());
    let xchacha_nonce = BlockNonce::derive(AeadScheme::XChaCha20Poly1305, 99, 0);
    assert_eq!(xchacha_nonce.bytes.len(), AeadScheme::XChaCha20Poly1305.nonce_len());

    let nonce_bytes = BlockNonce::derive(AeadScheme::AesGcm256, 42, 0).bytes;
    let block_id_in_nonce = u64::from_le_bytes(nonce_bytes[..8].try_into().unwrap());
    assert_eq!(
        block_id_in_nonce, 42,
        "block_id must be encoded in the high bytes of the nonce"
    );
}

#[test]
fn pinned_aead_seals_and_rejects_tamper() {
    for scheme in [AeadScheme::AesGcm256, AeadScheme::XChaCha20Poly1305] {
        let mut key = SubjectContentKey::generate_with([5u8; 32], 0, scheme);
        let plaintext = b"single-subject pii".to_vec();
        let sealed = key.encrypt(3, FILE_ID, &plaintext).expect("a live key seals the block");

        assert_eq!(
            rebuild_payload(&key, 3, FILE_ID, &sealed),
            RebuiltPayload::Payload(plaintext.clone()),
            "an intact block round-trips to the exact plaintext under the pinned AEAD"
        );

        let mut tampered = sealed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        assert_eq!(
            rebuild_payload(&key, 3, FILE_ID, &tampered),
            RebuiltPayload::Rejected,
            "a flipped ciphertext byte is rejected on decrypt, never returned as altered plaintext"
        );

        assert_eq!(
            rebuild_payload(&key, 4, FILE_ID, &sealed),
            RebuiltPayload::Rejected,
            "an intact block opened under a different block id is rejected — ciphertext cannot be relocated"
        );

        assert_eq!(
            rebuild_payload(&key, 3, FILE_ID ^ 1, &sealed),
            RebuiltPayload::Rejected,
            "an intact block opened as part of a different file is rejected — ciphertext cannot be relocated across files"
        );

        let mut tampered_nonce = sealed.clone();
        tampered_nonce[0] ^= 0x01;
        assert_eq!(
            rebuild_payload(&key, 3, FILE_ID, &tampered_nonce),
            RebuiltPayload::Rejected,
            "altering the nonce is rejected on decrypt"
        );
    }
}

#[test]
fn deterministic_seal_is_reproducible_across_schemes() {
    for scheme in [AeadScheme::AesGcm256, AeadScheme::XChaCha20Poly1305] {
        let mut key_a = SubjectContentKey::generate_with([6u8; 32], 2, scheme);
        let mut key_b = SubjectContentKey::generate_with([6u8; 32], 2, scheme);
        let plaintext = b"reproducible payload".to_vec();
        let sealed_a = key_a.encrypt(9, FILE_ID, &plaintext).expect("seal under key a");
        let sealed_b = key_b.encrypt(9, FILE_ID, &plaintext).expect("seal under key b");

        assert_eq!(
            sealed_a, sealed_b,
            "the deterministic seal reproduces identical bytes for identical inputs"
        );
        assert_eq!(
            rebuild_payload(&key_a, 9, FILE_ID, &sealed_b),
            RebuiltPayload::Payload(plaintext),
            "either key opens the other's block to the same plaintext"
        );
    }
}
