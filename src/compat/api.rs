use super::model::{EscapeHatch, OptionalBlockOutcome, OptionalBlockPlan, PortableDecoder, PortableDecoderFleet};
use crate::error::FormatError;
use crate::layout::optional_features;

pub fn plan_optional_block(
    feature_bit: u64,
    reader_version: u32,
    hatch: Option<&EscapeHatch>,
    fleet: &dyn PortableDecoderFleet,
) -> OptionalBlockPlan {
    if optional_features::KNOWN & feature_bit != 0 {
        return OptionalBlockPlan::Native;
    }
    match hatch.and_then(|hatch| usable_decoder(feature_bit, reader_version, hatch, fleet)) {
        Some(_) => OptionalBlockPlan::PortableDecode,
        None => OptionalBlockPlan::Skip,
    }
}

pub fn read_optional_block_via_escape_hatch(
    block: &[u8],
    expected_checksum: &[u8; blake3::OUT_LEN],
    feature_bit: u64,
    reader_version: u32,
    hatch: Option<&EscapeHatch>,
    fleet: &dyn PortableDecoderFleet,
) -> Result<OptionalBlockOutcome, FormatError> {
    let Some(decoder) = hatch.and_then(|hatch| usable_decoder(feature_bit, reader_version, hatch, fleet)) else {
        return Ok(OptionalBlockOutcome::Skip);
    };
    if crate::file::integrity::hash_tree(block).as_bytes() != expected_checksum {
        return Err(FormatError::Blake3Mismatch {
            scope: "optional block",
        });
    }
    Ok(OptionalBlockOutcome::Decoded(decoder.decode(block)?))
}

fn usable_decoder<'fleet>(
    feature_bit: u64,
    reader_version: u32,
    hatch: &EscapeHatch,
    fleet: &'fleet dyn PortableDecoderFleet,
) -> Option<&'fleet dyn PortableDecoder> {
    if hatch.optional_feature_bit != feature_bit {
        return None;
    }
    if reader_version < hatch.min_reader_version {
        return None;
    }
    let decoder = fleet.resolve(&hatch.portable_decoder_ref)?;
    decoder.is_conformant_at(hatch.min_reader_version).then_some(decoder)
}

#[cfg(test)]
#[path = "test/api.rs"]
mod tests;
