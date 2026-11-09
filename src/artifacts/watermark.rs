use crate::events::{SequencePoint, SequenceRange};

pub const FIRST_SEQUENCE: u64 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeSet {
    ranges: Vec<(u64, u64)>,
}

impl RangeSet {
    pub fn insert(&mut self, first: u64, last: u64) {
        if last < first {
            return;
        }
        match self.ranges.last_mut() {
            None => {
                self.ranges.push((first, last));
                return;
            }
            Some((last_start, last_end)) if first >= *last_start && first <= last_end.saturating_add(1) => {
                *last_end = (*last_end).max(last);
                return;
            }
            Some((_, last_end)) if first > last_end.saturating_add(1) => {
                self.ranges.push((first, last));
                return;
            }
            Some(_) => {}
        }
        let position = self.ranges.partition_point(|(_, end)| end.saturating_add(1) < first);
        let mut merged_first = first;
        let mut merged_last = last;
        let mut absorbed_end = position;
        while let Some(&(start, end)) = self.ranges.get(absorbed_end) {
            if start > merged_last.saturating_add(1) {
                break;
            }
            merged_first = merged_first.min(start);
            merged_last = merged_last.max(end);
            absorbed_end += 1;
        }
        match self.ranges.get_mut(position) {
            Some(slot) if absorbed_end > position => {
                *slot = (merged_first, merged_last);
                self.ranges.drain(position + 1..absorbed_end);
            }
            _ => self.ranges.insert(position, (first, last)),
        }
    }

    pub fn contiguous_prefix_end(&self, from: u64) -> Option<u64> {
        let (start, end) = self.ranges.first()?;
        if *start > from {
            return None;
        }
        Some(*end)
    }

    pub fn covers(&self, first: u64, last: u64) -> bool {
        self.ranges.iter().any(|(start, end)| *start <= first && last <= *end)
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn max_covered(&self) -> Option<u64> {
        self.ranges.last().map(|(_, end)| *end)
    }

    fn gaps(&self, from: u64, to: u64) -> Vec<(u64, u64)> {
        let mut holes = Vec::new();
        let mut cursor = from;
        for (start, end) in &self.ranges {
            if *end < from {
                continue;
            }
            if *start > to {
                break;
            }
            if *start > cursor {
                holes.push((cursor, start - 1));
            }
            cursor = end.saturating_add(1);
            if cursor > to {
                break;
            }
        }
        if cursor <= to {
            holes.push((cursor, to));
        }
        holes
    }
}

#[derive(Debug, Clone, Default)]
struct EpochCoverage {
    durable: RangeSet,
    sealed: bool,
    visible: RangeSet,
}

#[derive(Debug, Default)]
pub struct WatermarkTracker {
    epochs: std::collections::BTreeMap<u64, EpochCoverage>,
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_durable(&mut self, range: SequenceRange) {
        self.epochs
            .entry(range.epoch)
            .or_default()
            .durable
            .insert(range.first_sequence, range.last_sequence);
    }

    pub fn record_visible(&mut self, range: SequenceRange) {
        self.epochs
            .entry(range.epoch)
            .or_default()
            .visible
            .insert(range.first_sequence, range.last_sequence);
    }

    pub fn seal_epoch(&mut self, epoch: u64) {
        if let Some(coverage) = self.epochs.get_mut(&epoch) {
            coverage.sealed = true;
            let Some(max) = coverage.durable.max_covered() else {
                return;
            };
            let voids = coverage.durable.gaps(FIRST_SEQUENCE, max);
            coverage.durable.insert(FIRST_SEQUENCE, max);
            for (first, last) in voids {
                coverage.visible.insert(first, last);
            }
        }
    }

    fn watermark_over(&self, pick: impl Fn(&EpochCoverage) -> &RangeSet) -> Option<SequencePoint> {
        self.epochs.iter().rev().find_map(|(epoch, coverage)| {
            let end = pick(coverage).contiguous_prefix_end(FIRST_SEQUENCE)?;
            Some(SequencePoint {
                epoch: *epoch,
                sequence: end,
            })
        })
    }

    pub fn commit_watermark(&self) -> Option<SequencePoint> {
        self.watermark_over(|coverage| &coverage.durable)
    }

    pub fn visibility_watermark(&self) -> Option<SequencePoint> {
        self.watermark_over(|coverage| &coverage.visible)
    }

    pub fn capture_snapshot_watermark(&self) -> Option<SequencePoint> {
        self.visibility_watermark()
    }

    pub fn durable_journal_cursor(&self) -> Option<SequencePoint> {
        self.commit_watermark()
    }

    pub fn live_queryable_cursor(&self) -> Option<SequencePoint> {
        self.visibility_watermark()
    }

    pub fn durably_covers(&self, range: &SequenceRange) -> bool {
        self.epochs
            .get(&range.epoch)
            .is_some_and(|coverage| coverage.durable.covers(range.first_sequence, range.last_sequence))
    }

    pub fn visibly_covers(&self, point: SequencePoint) -> bool {
        self.epochs
            .get(&point.epoch)
            .is_some_and(|coverage| coverage.visible.covers(FIRST_SEQUENCE, point.sequence))
    }
}

#[cfg(test)]
#[path = "test/watermark.rs"]
mod tests;
