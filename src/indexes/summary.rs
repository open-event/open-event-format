use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::layout::footer::Footer;
use crate::layout::optional_features;

const SCHEMA_FINGERPRINT_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestSummary {
    pub granule_count: u32,
    pub has_deletion_vectors: bool,
    pub has_late_events: bool,
    pub max_occurred_at_physical: i64,
    pub max_sequence: u64,
    pub min_occurred_at_physical: i64,
    pub min_sequence: u64,
    pub row_count: u64,
    pub schema_fingerprint: [u8; SCHEMA_FINGERPRINT_BYTES],
}

impl ManifestSummary {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(SCHEMA_FINGERPRINT_BYTES + 4 + 2 + 8 * 5);
        out.put_slice(&self.schema_fingerprint);
        out.put_u32(self.granule_count);
        out.put_u8(u8::from(self.has_deletion_vectors));
        out.put_u8(u8::from(self.has_late_events));
        out.put_i64(self.max_occurred_at_physical);
        out.put_u64(self.max_sequence);
        out.put_i64(self.min_occurred_at_physical);
        out.put_u64(self.min_sequence);
        out.put_u64(self.row_count);
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        let fingerprint_bytes = reader.take(SCHEMA_FINGERPRINT_BYTES, "summary schema_fingerprint")?;
        let mut schema_fingerprint = [0u8; SCHEMA_FINGERPRINT_BYTES];
        schema_fingerprint.copy_from_slice(fingerprint_bytes);
        let summary = Self {
            granule_count: reader.u32("summary granule_count")?,
            has_deletion_vectors: reader.u8("summary has_deletion_vectors")? != 0,
            has_late_events: reader.u8("summary has_late_events")? != 0,
            max_occurred_at_physical: reader.i64("summary max_occurred_at_physical")?,
            max_sequence: reader.u64("summary max_sequence")?,
            min_occurred_at_physical: reader.i64("summary min_occurred_at_physical")?,
            min_sequence: reader.u64("summary min_sequence")?,
            row_count: reader.u64("summary row_count")?,
            schema_fingerprint,
        };
        let occurred_inverted = summary.min_occurred_at_physical > summary.max_occurred_at_physical;
        let sequence_inverted = summary.min_sequence > summary.max_sequence;
        if (occurred_inverted || sequence_inverted) && (summary.granule_count != 0 || summary.row_count != 0) {
            return Err(FormatError::Structural {
                rule: "summary coverage inverted with non-zero counts",
            });
        }
        if (summary.granule_count == 0) != (summary.row_count == 0)
            || summary.row_count < u64::from(summary.granule_count)
        {
            return Err(FormatError::Structural {
                rule: "summary row count disagrees with its granule count",
            });
        }
        Ok(summary)
    }

    pub fn validate_against_footer(&self, footer: &Footer) -> Result<(), FormatError> {
        validate_granule_row_counts(footer)?;
        if usize::try_from(self.granule_count).ok() != Some(footer.granules.len())
            || self.row_count != footer.exact_counts.row_count
        {
            return Err(FormatError::Structural {
                rule: "summary counts disagree with the footer",
            });
        }
        Ok(())
    }
}

pub fn validate_granule_row_counts(footer: &Footer) -> Result<(), FormatError> {
    let mut total: u64 = 0;
    for granule in &footer.granules {
        if granule.row_count == 0 {
            return Err(FormatError::Structural {
                rule: "granule statistics cover no rows",
            });
        }
        total = total
            .checked_add(u64::from(granule.row_count))
            .ok_or(FormatError::Structural {
                rule: "granule row counts overflow",
            })?;
    }
    if total != footer.exact_counts.row_count {
        return Err(FormatError::Structural {
            rule: "granule row counts disagree with the file row count",
        });
    }
    Ok(())
}

pub fn summary_from_footer(footer: &Footer) -> ManifestSummary {
    let mut min_occurred = i64::MAX;
    let mut max_occurred = i64::MIN;
    let mut min_sequence = u64::MAX;
    let mut max_sequence = u64::MIN;
    for granule in &footer.granules {
        min_occurred = min_occurred.min(granule.min_occurred_at_physical);
        max_occurred = max_occurred.max(granule.max_occurred_at_physical);
        min_sequence = min_sequence.min(granule.first_sequence);
        max_sequence = max_sequence.max(granule.last_sequence);
    }
    if footer.granules.is_empty() {
        min_occurred = i64::MAX;
        max_occurred = i64::MIN;
        min_sequence = u64::MAX;
        max_sequence = u64::MIN;
    }
    ManifestSummary {
        granule_count: footer.granules.len() as u32,
        has_deletion_vectors: footer.optional_feature_flags & optional_features::OE_NATIVE_DELETION_VECTORS != 0,
        has_late_events: footer.optional_feature_flags & optional_features::OE_LATE_EVENTS != 0,
        max_occurred_at_physical: max_occurred,
        max_sequence,
        min_occurred_at_physical: min_occurred,
        min_sequence,
        row_count: footer.exact_counts.row_count,
        schema_fingerprint: footer.schema_fingerprint,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FilePredicate {
    pub max_occurred_at_physical: Option<i64>,
    pub max_sequence: Option<u64>,
    pub min_occurred_at_physical: Option<i64>,
    pub min_sequence: Option<u64>,
}

pub fn can_prune_file(summary: &ManifestSummary, predicate: &FilePredicate) -> bool {
    let occurred_empty = summary.min_occurred_at_physical > summary.max_occurred_at_physical;
    let sequence_empty = summary.min_sequence > summary.max_sequence;
    if occurred_empty || sequence_empty {
        return true;
    }

    let occurred_above = predicate
        .min_occurred_at_physical
        .is_some_and(|lo| lo > summary.max_occurred_at_physical);
    let occurred_below = predicate
        .max_occurred_at_physical
        .is_some_and(|hi| hi < summary.min_occurred_at_physical);
    let sequence_above = predicate.min_sequence.is_some_and(|lo| lo > summary.max_sequence);
    let sequence_below = predicate.max_sequence.is_some_and(|hi| hi < summary.min_sequence);

    occurred_above || occurred_below || sequence_above || sequence_below
}

pub fn footer_is_authoritative_for_planning() -> bool {
    true
}

pub fn plan_granules_from_footer(footer: &Footer) -> Vec<u32> {
    footer.granules.iter().map(|g| g.granule_id).collect()
}

#[cfg(test)]
#[path = "test/summary.rs"]
mod tests;
