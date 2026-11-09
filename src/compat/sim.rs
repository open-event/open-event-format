use super::model::{PortableDecoder, PortableDecoderFleet};
use crate::error::FormatError;
use hashbrown::HashMap;
use std::mem::size_of;

#[derive(Debug, Default)]
pub struct SimulatedDecoderFleet {
    decoders: HashMap<String, SimulatedDecoder>,
}

impl SimulatedDecoderFleet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_decoder(mut self, decoder_ref: impl Into<String>, decoder: SimulatedDecoder) -> Self {
        self.decoders.insert(decoder_ref.into(), decoder);
        self
    }
}

impl PortableDecoderFleet for SimulatedDecoderFleet {
    fn resolve(&self, decoder_ref: &str) -> Option<&dyn PortableDecoder> {
        self.decoders
            .get(decoder_ref)
            .map(|decoder| decoder as &dyn PortableDecoder)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SimulatedDecoder {
    conformant: bool,
    provides_version: u32,
}

impl SimulatedDecoder {
    pub fn conformant(provides_version: u32) -> Self {
        Self {
            conformant: true,
            provides_version,
        }
    }

    pub fn uncertified(provides_version: u32) -> Self {
        Self {
            conformant: false,
            provides_version,
        }
    }
}

impl PortableDecoder for SimulatedDecoder {
    fn is_conformant_at(&self, min_reader_version: u32) -> bool {
        self.conformant && self.provides_version >= min_reader_version
    }

    fn decode(&self, block: &[u8]) -> Result<Vec<u8>, FormatError> {
        transcode_be_to_le(block)
    }
}

pub fn encode_demo_block(values: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        out.extend_from_slice(&value.to_be_bytes());
    }
    out
}

pub fn native_reference_decode(block: &[u8]) -> Result<Vec<u8>, FormatError> {
    transcode_be_to_le(block)
}

fn transcode_be_to_le(block: &[u8]) -> Result<Vec<u8>, FormatError> {
    if !block.len().is_multiple_of(size_of::<u64>()) {
        return Err(FormatError::Structural {
            rule: "optional block length must be a multiple of 8",
        });
    }
    let mut out = Vec::with_capacity(block.len());
    for chunk in block.chunks_exact(size_of::<u64>()) {
        let value = u64::from_be_bytes(chunk.try_into().unwrap_or([0; size_of::<u64>()]));
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

#[cfg(test)]
#[path = "test/sim.rs"]
mod tests;
