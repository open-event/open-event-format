use super::constant::{NANOS_PER_SECOND, PROTOCOL_ID_BYTES, SIGNATURE_BYTES};
use super::envelope::TimestampValue;
use super::variant::VariantValue;
use crate::error::ProvenanceError;
use hex_simd::AsOut;
use k256::schnorr::signature::hazmat::PrehashVerifier;
use k256::schnorr::{Signature, VerifyingKey};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fmt::Write as _;

pub const CONTENT_FIELD: &str = "content";

pub const TAGS_FIELD: &str = "tags";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignatureScheme {
    Bip340SchnorrSecp256k1,
    Ed25519,
}

impl SignatureScheme {
    pub const fn as_str(self) -> &'static str {
        match self {
            SignatureScheme::Bip340SchnorrSecp256k1 => "bip340-schnorr-secp256k1",
            SignatureScheme::Ed25519 => "ed25519",
        }
    }

    #[allow(
        clippy::should_implement_trait,
        reason = "an unknown tag is `None`, not a parse error"
    )]
    pub fn from_str(tag: &str) -> Option<Self> {
        match tag {
            "bip340-schnorr-secp256k1" => Some(SignatureScheme::Bip340SchnorrSecp256k1),
            "ed25519" => Some(SignatureScheme::Ed25519),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedEventProvenance {
    pub author_pubkey: [u8; PROTOCOL_ID_BYTES],
    pub claimed_at: TimestampValue,
    pub protocol_event_id: [u8; PROTOCOL_ID_BYTES],
    pub protocol_kind: u32,
    pub scheme: SignatureScheme,
    pub signature: [u8; SIGNATURE_BYTES],
}

impl SignedEventProvenance {
    pub fn claimed_at_seconds(&self) -> i64 {
        self.claimed_at.physical_nanos().div_euclid(NANOS_PER_SECOND)
    }

    pub fn canonical_bytes(&self, payload: &VariantValue) -> Result<Vec<u8>, ProvenanceError> {
        let content = payload_content(payload)?;
        let tags = payload_tags(payload)?;
        let mut out = String::with_capacity(content.len() + 128);
        out.push_str("[0,\"");
        hex_simd::encode_append(self.author_pubkey, &mut out, hex_simd::AsciiCase::Lower);
        out.push_str("\",");
        #[allow(clippy::expect_used)]
        write!(out, "{}", self.claimed_at_seconds()).expect("String writes never fail");
        out.push(',');
        #[allow(clippy::expect_used)]
        write!(out, "{}", self.protocol_kind).expect("String writes never fail");
        out.push(',');
        push_tags(&mut out, tags);
        out.push(',');
        push_json_string(&mut out, content);
        out.push(']');
        Ok(out.into_bytes())
    }

    pub fn recompute_protocol_event_id(canonical: &[u8]) -> [u8; PROTOCOL_ID_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(canonical);
        hasher.finalize().into()
    }

    pub fn verify(&self, payload: &VariantValue) -> Result<(), ProvenanceError> {
        let canonical = self.canonical_bytes(payload)?;
        let recomputed = Self::recompute_protocol_event_id(&canonical);
        if recomputed != self.protocol_event_id {
            return Err(ProvenanceError::EventIdMismatch);
        }
        match self.scheme {
            SignatureScheme::Bip340SchnorrSecp256k1 => {
                verify_bip340_prehash(&self.author_pubkey, &recomputed, &self.signature)
            }
            SignatureScheme::Ed25519 => verify_ed25519(&self.author_pubkey, &canonical, &self.signature),
        }
    }
}

pub fn verify_bip340_prehash(
    author_pubkey: &[u8; PROTOCOL_ID_BYTES],
    prehash: &[u8; PROTOCOL_ID_BYTES],
    signature: &[u8; SIGNATURE_BYTES],
) -> Result<(), ProvenanceError> {
    let key = VerifyingKey::from_slice(author_pubkey).map_err(|_| ProvenanceError::MalformedKey)?;
    let signature = Signature::try_from(signature.as_slice()).map_err(|_| ProvenanceError::MalformedSignature)?;
    key.verify_prehash(prehash, &signature)
        .map_err(|_| ProvenanceError::SignatureRejected)
}

pub fn verify_ed25519(
    public_key: &[u8; PROTOCOL_ID_BYTES],
    message: &[u8],
    signature: &[u8; SIGNATURE_BYTES],
) -> Result<(), ProvenanceError> {
    let key = ed25519_dalek::VerifyingKey::from_bytes(public_key).map_err(|_| ProvenanceError::MalformedKey)?;
    let signature = ed25519_dalek::Signature::from_bytes(signature);
    key.verify_strict(message, &signature)
        .map_err(|_| ProvenanceError::SignatureRejected)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignerSignature {
    pub key_id: String,
    pub public_key: [u8; PROTOCOL_ID_BYTES],
    pub scheme: SignatureScheme,
    pub signature: [u8; SIGNATURE_BYTES],
    pub signer: String,
}

impl SignerSignature {
    pub fn new(
        signer: &str,
        key_id: &str,
        scheme: SignatureScheme,
        public_key: [u8; PROTOCOL_ID_BYTES],
        signature: [u8; SIGNATURE_BYTES],
    ) -> Result<Self, ProvenanceError> {
        if !is_token(signer) || !is_token(key_id) {
            return Err(ProvenanceError::MalformedSignerSignature);
        }
        Ok(Self {
            key_id: key_id.to_owned(),
            public_key,
            scheme,
            signature,
            signer: signer.to_owned(),
        })
    }

    pub fn verify(&self, message: &[u8]) -> Result<(), ProvenanceError> {
        match self.scheme {
            SignatureScheme::Bip340SchnorrSecp256k1 => {
                verify_bip340_prehash(&self.public_key, &sha256_digest(message), &self.signature)
            }
            SignatureScheme::Ed25519 => verify_ed25519(&self.public_key, message, &self.signature),
        }
    }

    pub fn parse(text: &str) -> Result<Self, ProvenanceError> {
        let mut parts = text.split(' ');
        let (Some(scheme), Some(signer), Some(key_id), Some(public_key), Some(signature), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(ProvenanceError::MalformedSignerSignature);
        };
        let scheme = SignatureScheme::from_str(scheme).ok_or(ProvenanceError::MalformedSignerSignature)?;
        Self::new(signer, key_id, scheme, hex_bytes(public_key)?, hex_bytes(signature)?)
    }

    pub fn join(signatures: &[SignerSignature]) -> String {
        let mut out = String::new();
        Self::join_into(signatures, &mut out);
        out
    }

    pub(crate) fn join_into(signatures: &[SignerSignature], out: &mut String) {
        let length = signatures
            .iter()
            .try_fold(signatures.len().saturating_sub(1), |total, entry| {
                entry
                    .scheme
                    .as_str()
                    .len()
                    .checked_add(entry.signer.len())?
                    .checked_add(entry.key_id.len())?
                    .checked_add(4 + 2 * (PROTOCOL_ID_BYTES + SIGNATURE_BYTES))?
                    .checked_add(total)
            });
        if let Some(length) = length {
            out.reserve(length);
        }
        for (index, entry) in signatures.iter().enumerate() {
            if index > 0 {
                out.push('\n');
            }
            out.push_str(entry.scheme.as_str());
            out.push(' ');
            out.push_str(&entry.signer);
            out.push(' ');
            out.push_str(&entry.key_id);
            out.push(' ');
            hex_lower_into(&entry.public_key, out);
            out.push(' ');
            hex_lower_into(&entry.signature, out);
        }
    }

    pub fn split(text: &str) -> Result<Vec<SignerSignature>, ProvenanceError> {
        text.split('\n').map(SignerSignature::parse).collect()
    }
}

impl fmt::Display for SignerSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut public_key = [0u8; 2 * PROTOCOL_ID_BYTES];
        let mut signature = [0u8; 2 * SIGNATURE_BYTES];
        write!(
            f,
            "{} {} {} {} {}",
            self.scheme.as_str(),
            self.signer,
            self.key_id,
            hex_simd::encode_as_str(
                &self.public_key,
                public_key.as_mut_slice().as_out(),
                hex_simd::AsciiCase::Lower
            ),
            hex_simd::encode_as_str(
                &self.signature,
                signature.as_mut_slice().as_out(),
                hex_simd::AsciiCase::Lower
            )
        )
    }
}

fn is_token(text: &str) -> bool {
    !text.is_empty() && !text.chars().any(char::is_whitespace)
}

pub fn sha256_digest(preimage: &[u8]) -> [u8; PROTOCOL_ID_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(preimage);
    hasher.finalize().into()
}

pub fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::new();
    hex_lower_into(bytes, &mut out);
    out
}

pub fn hex_lower_into(bytes: &[u8], out: &mut String) {
    hex_simd::encode_append(bytes, out, hex_simd::AsciiCase::Lower);
}

pub fn hex_bytes<const N: usize>(text: &str) -> Result<[u8; N], ProvenanceError> {
    if text.len() != N * 2 || text.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(ProvenanceError::MalformedHex);
    }
    let mut bytes = [0u8; N];
    hex_simd::decode(text.as_bytes(), bytes.as_mut_slice().as_out()).map_err(|_| ProvenanceError::MalformedHex)?;
    Ok(bytes)
}

fn payload_content(payload: &VariantValue) -> Result<&str, ProvenanceError> {
    match payload {
        VariantValue::Object(fields) => match fields.get(CONTENT_FIELD) {
            Some(VariantValue::String(content)) => Ok(content),
            _ => Err(ProvenanceError::PayloadShape { field: CONTENT_FIELD }),
        },
        _ => Err(ProvenanceError::PayloadShape { field: CONTENT_FIELD }),
    }
}

fn payload_tags(payload: &VariantValue) -> Result<&[VariantValue], ProvenanceError> {
    let VariantValue::Object(fields) = payload else {
        return Err(ProvenanceError::PayloadShape { field: TAGS_FIELD });
    };
    let Some(VariantValue::Array(tags)) = fields.get(TAGS_FIELD) else {
        return Err(ProvenanceError::PayloadShape { field: TAGS_FIELD });
    };
    let well_formed = tags.iter().all(|tag| {
        matches!(tag, VariantValue::Array(items) if items.iter().all(|item| matches!(item, VariantValue::String(_))))
    });
    if !well_formed {
        return Err(ProvenanceError::PayloadShape { field: TAGS_FIELD });
    }
    Ok(tags)
}

fn push_tags(out: &mut String, tags: &[VariantValue]) {
    out.push('[');
    for (index, tag) in tags.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('[');
        if let VariantValue::Array(items) = tag {
            let strings = items.iter().filter_map(|item| match item {
                VariantValue::String(value) => Some(value),
                _ => None,
            });
            for (item_index, item) in strings.enumerate() {
                if item_index > 0 {
                    out.push(',');
                }
                push_json_string(out, item);
            }
        }
        out.push(']');
    }
    out.push(']');
}

pub(crate) fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    let mut start = 0usize;
    for (index, character) in value.char_indices() {
        let escape = match character {
            '\n' => "\\n",
            '"' => "\\\"",
            '\\' => "\\\\",
            '\r' => "\\r",
            '\t' => "\\t",
            '\u{8}' => "\\b",
            '\u{c}' => "\\f",
            character if (character as u32) < 0x20 => {
                out.push_str(&value[start..index]);
                #[allow(clippy::expect_used)]
                write!(out, "\\u{:04x}", character as u32).expect("String writes never fail");
                start = index + character.len_utf8();
                continue;
            }
            _ => continue,
        };
        out.push_str(&value[start..index]);
        out.push_str(escape);
        start = index + character.len_utf8();
    }
    out.push_str(&value[start..]);
    out.push('"');
}

#[cfg(test)]
#[path = "test/provenance.rs"]
mod tests;
