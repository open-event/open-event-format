pub mod api;
pub mod availability;
pub mod fixture_corpus;
pub mod model;
pub mod sim;

use super::error::FormatError;
use super::layout::{optional_features, required_features};
pub use api::{plan_optional_block, read_optional_block_via_escape_hatch};
pub use availability::{AvailabilityWindow, validate_pipeline_window, validate_pipeline_window_pinned};
pub use model::{EscapeHatch, OptionalBlockOutcome, OptionalBlockPlan, PortableDecoder, PortableDecoderFleet};
pub use sim::{SimulatedDecoder, SimulatedDecoderFleet, encode_demo_block, native_reference_decode};

pub const SUPPORTED_FORMAT_MAJOR: u16 = 1;

pub const SUPPORTED_READER_VERSION: u32 = 1;

pub fn check_features(required: u64, optional: u64) -> Result<u64, FormatError> {
    let unknown_required = required & !required_features::KNOWN;
    if unknown_required != 0 {
        return Err(FormatError::UnknownRequiredFeature { bits: unknown_required });
    }
    Ok(optional & optional_features::KNOWN)
}

pub fn check_format_version(major: u16, minor: u16) -> Result<(), FormatError> {
    if major != SUPPORTED_FORMAT_MAJOR {
        return Err(FormatError::UnsupportedVersion {
            field: "footer.format_version.major",
            found: u32::from(major) << 16 | u32::from(minor),
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
