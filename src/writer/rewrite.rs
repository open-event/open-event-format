use super::compaction::{RewritePlan, StripeDisposition};
use crate::layout::footer::StripeEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteSegment {
    CopyFromSource { len: u64, source_offset: u64 },
    Fresh { len: u64 },
}

pub fn rewrite_segments(
    plan: &RewritePlan,
    replacement_stripes: &[StripeEntry],
    file_len: u64,
    min_part_bytes: u64,
) -> Vec<RewriteSegment> {
    let mut segments = Vec::new();
    let mut cursor = 0u64;
    for (disposition, stripe) in plan.dispositions.iter().zip(replacement_stripes) {
        if stripe.file_offset > cursor {
            segments.push(RewriteSegment::Fresh {
                len: stripe.file_offset - cursor,
            });
            cursor = stripe.file_offset;
        }
        match disposition {
            StripeDisposition::Reuse {
                source_offset,
                byte_len,
            } if *byte_len >= min_part_bytes => {
                segments.push(RewriteSegment::CopyFromSource {
                    len: *byte_len,
                    source_offset: *source_offset,
                });
            }
            _ => segments.push(RewriteSegment::Fresh { len: stripe.byte_len }),
        }
        cursor += stripe.byte_len;
    }
    if cursor < file_len {
        segments.push(RewriteSegment::Fresh { len: file_len - cursor });
    }
    segments
}

#[cfg(test)]
#[path = "test/rewrite.rs"]
mod tests;
