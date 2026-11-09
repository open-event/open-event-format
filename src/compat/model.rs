use crate::error::FormatError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscapeHatch {
    pub min_reader_version: u32,
    pub optional_feature_bit: u64,
    pub portable_decoder_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionalBlockPlan {
    Native,
    PortableDecode,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionalBlockOutcome {
    Decoded(Vec<u8>),
    Skip,
}

pub trait PortableDecoder {
    fn is_conformant_at(&self, min_reader_version: u32) -> bool;

    fn decode(&self, block: &[u8]) -> Result<Vec<u8>, FormatError>;
}

pub trait PortableDecoderFleet {
    fn resolve(&self, decoder_ref: &str) -> Option<&dyn PortableDecoder>;
}
