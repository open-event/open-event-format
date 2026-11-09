use super::constant::{NANOS_PER_SECOND, PROTOCOL_ID_BYTES, SIGNATURE_BYTES};
use super::envelope::TimestampValue;
use super::provenance::{SignatureScheme, SignedEventProvenance};
use super::variant::VariantValue;
use crate::error::ProvenanceError;
use k256::schnorr::SigningKey;
use k256::schnorr::signature::hazmat::PrehashSigner;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct SimulatedEventAuthor {
    signing: SigningKey,
}

impl SimulatedEventAuthor {
    pub fn from_seed(seed: u8) -> Self {
        Self {
            #[allow(clippy::expect_used)]
            signing: SigningKey::from_bytes(&[seed.max(1); PROTOCOL_ID_BYTES].into())
                .expect("a repeated small byte is a valid key"),
        }
    }

    pub fn author_pubkey(&self) -> [u8; PROTOCOL_ID_BYTES] {
        self.signing.verifying_key().to_bytes().into()
    }

    pub fn sign(
        &self,
        kind: u32,
        claimed_at_seconds: i64,
        payload: &VariantValue,
    ) -> Result<SignedEventProvenance, ProvenanceError> {
        let mut provenance = SignedEventProvenance {
            author_pubkey: self.author_pubkey(),
            claimed_at: TimestampValue::from_physical_nanos(claimed_at_seconds * NANOS_PER_SECOND),
            protocol_event_id: [0u8; PROTOCOL_ID_BYTES],
            protocol_kind: kind,
            scheme: SignatureScheme::Bip340SchnorrSecp256k1,
            signature: [0u8; SIGNATURE_BYTES],
        };
        let canonical = provenance.canonical_bytes(payload)?;
        provenance.protocol_event_id = SignedEventProvenance::recompute_protocol_event_id(&canonical);
        let signature: k256::schnorr::Signature = self
            .signing
            .sign_prehash(&provenance.protocol_event_id)
            .map_err(|_| ProvenanceError::MalformedSignature)?;
        provenance.signature = signature.to_bytes();
        Ok(provenance)
    }

    pub fn sign_prehash(&self, prehash: &[u8; PROTOCOL_ID_BYTES]) -> Result<[u8; SIGNATURE_BYTES], ProvenanceError> {
        let signature: k256::schnorr::Signature = self
            .signing
            .sign_prehash(prehash)
            .map_err(|_| ProvenanceError::MalformedSignature)?;
        Ok(signature.to_bytes())
    }
}

pub fn signed_event_payload(content: &str, tags: &[&[&str]]) -> VariantValue {
    let mut fields = BTreeMap::new();
    fields.insert(
        super::provenance::CONTENT_FIELD.to_owned(),
        VariantValue::String(content.to_owned()),
    );
    fields.insert(
        super::provenance::TAGS_FIELD.to_owned(),
        VariantValue::Array(
            tags.iter()
                .map(|tag| {
                    VariantValue::Array(
                        tag.iter()
                            .map(|item| VariantValue::String((*item).to_owned()))
                            .collect(),
                    )
                })
                .collect(),
        ),
    );
    VariantValue::Object(fields)
}
