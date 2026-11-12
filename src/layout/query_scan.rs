use super::*;
use crate::deletes::DeletionVector;
use crate::field_deletion_store::FieldRedactions;
use crate::indexes::bitmap::{RoaringRangeBitmap, RowRange};
use crate::indexes::bool_pruning::{FilterExpr, residual};
use crate::indexes::external_id::ExternalIdLocation;
use crate::indexes::pruning::{FilterClause, GranuleStatistics, QueryPlan, Statistic};
use std::sync::atomic::AtomicI64;

const PARALLEL_CHUNK: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanOrder {
    #[default]
    Stored,
    TimeAscending,
    TimeDescending,
}

#[derive(Debug)]
pub struct TopNBound {
    descending: bool,
    threshold: AtomicI64,
    version: AtomicU64,
}

impl TopNBound {
    pub fn new(order: ScanOrder) -> Self {
        let descending = order != ScanOrder::TimeAscending;
        TopNBound {
            descending,
            threshold: AtomicI64::new(if descending { i64::MIN } else { i64::MAX }),
            version: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> (u64, i64) {
        let version = self.version.load(Ordering::Acquire);
        (version, self.threshold.load(Ordering::Acquire))
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn tighten(&self, candidate: i64) -> bool {
        let previous = if self.descending {
            self.threshold.fetch_max(candidate, Ordering::AcqRel)
        } else {
            self.threshold.fetch_min(candidate, Ordering::AcqRel)
        };
        let moved = if self.descending {
            candidate > previous
        } else {
            candidate < previous
        };
        if moved {
            self.version.fetch_add(1, Ordering::AcqRel);
        }
        moved
    }

    pub fn admits(&self, min: i64, max: i64) -> bool {
        let threshold = self.threshold.load(Ordering::Acquire);
        if self.descending {
            max >= threshold
        } else {
            min <= threshold
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSelection {
    Range(Range<u32>),
    Rows(Vec<u32>),
}

impl RowSelection {
    pub fn all(row_count: u32) -> Self {
        RowSelection::Range(0..row_count)
    }

    pub fn from_mask(mask: &[bool]) -> Self {
        let first = mask.iter().position(|&keep| keep);
        let Some(first) = first else {
            return RowSelection::Range(0..0);
        };
        let last = mask.iter().rposition(|&keep| keep).unwrap_or(first);
        if mask.get(first..=last).is_some_and(|run| run.iter().all(|&keep| keep)) {
            return RowSelection::Range(first as u32..last as u32 + 1);
        }
        RowSelection::Rows(
            mask.iter()
                .enumerate()
                .filter(|(_, keep)| **keep)
                .map(|(row, _)| row as u32)
                .collect(),
        )
    }

    pub fn from_sorted_rows(rows: Vec<u32>) -> Self {
        match (rows.first(), rows.last()) {
            (None, _) | (_, None) => RowSelection::Range(0..0),
            (Some(&first), Some(&last)) if (last - first) as usize + 1 == rows.len() => {
                RowSelection::Range(first..last + 1)
            }
            _ => RowSelection::Rows(rows),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RowSelection::Range(range) => range.len(),
            RowSelection::Rows(rows) => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        match self {
            RowSelection::Range(range) => Box::new(range.clone()),
            RowSelection::Rows(rows) => Box::new(rows.iter().copied()),
        }
    }

    pub fn select<T: Clone>(&self, values: &[T]) -> Vec<T> {
        match self {
            RowSelection::Range(range) => values
                .get(range.start as usize..(range.end as usize).min(values.len()))
                .map(<[T]>::to_vec)
                .unwrap_or_default(),
            RowSelection::Rows(rows) => rows
                .iter()
                .filter_map(|&row| values.get(row as usize).cloned())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GranuleSelection {
    pub granule_id: u32,
    pub rows: RowSelection,
}

pub fn plan_row_selection(granules: &[GranuleEntry], rows: &RoaringRangeBitmap) -> Vec<GranuleSelection> {
    let mut ordered: Vec<&GranuleEntry> = granules.iter().collect();
    ordered.sort_by_key(|granule| granule.first_row_ordinal);
    let mut plan = Vec::new();
    for granule in ordered {
        let start = granule.first_row_ordinal;
        let end = start + u64::from(granule.row_count);
        let ranges = rows.ranges();
        let first = ranges.partition_point(|run| run.end <= start);
        let mut local = Vec::new();
        for run in ranges.get(first..).unwrap_or_default() {
            if run.start >= end {
                break;
            }
            let lo = (run.start.max(start) - start) as u32;
            let hi = (run.end.min(end) - start) as u32;
            local.push(lo..hi);
        }
        let selection = match local.as_slice() {
            [] => continue,
            [only] => RowSelection::Range(only.clone()),
            _ => RowSelection::Rows(local.into_iter().flatten().collect()),
        };
        plan.push(GranuleSelection {
            granule_id: granule.granule_id,
            rows: selection,
        });
    }
    plan
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanRequest<'a> {
    pub bound: Option<&'a TopNBound>,
    pub deletions: Option<&'a DeletionVector>,
    pub limit: Option<usize>,
    pub order: ScanOrder,
    pub redactions: Option<&'a FieldRedactions>,
    pub selection: Option<&'a RoaringRangeBitmap>,
}

#[derive(Debug)]
pub struct SelectedBatch {
    pub batch: ScanBatch,
    pub first_row_ordinal: u64,
    pub rows: RowSelection,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectiveScanStats {
    pub granules_read: Vec<u32>,
    pub pruned_by_bound: usize,
    pub rows_returned: usize,
}

impl OeFile {
    pub fn scan_selective(
        &self,
        column_ids: &[u32],
        granule_ids: &[u32],
        request: &ScanRequest<'_>,
        mut row_filter: impl FnMut(&ScanBatch, u32) -> bool,
        mut sink: impl FnMut(SelectedBatch) -> Result<(), FormatError>,
    ) -> Result<SelectiveScanStats, FormatError> {
        let timed = request.order != ScanOrder::Stored;
        let occurred_column = column_ids.iter().position(|&id| id == column_ids::OCCURRED_AT);
        if timed && occurred_column.is_none() {
            return Err(FormatError::Structural {
                rule: "a time-ordered scan projects occurred_at",
            });
        }
        let mut entries = Vec::with_capacity(granule_ids.len());
        for &granule_id in granule_ids {
            let entry = self.granule_entry(granule_id).ok_or(FormatError::RefOutOfRange {
                what: "scanned granule missing from the granule directory",
            })?;
            entries.push(*entry);
        }
        match request.order {
            ScanOrder::Stored => {}
            ScanOrder::TimeAscending => entries.sort_by_key(|g| (g.min_occurred_at_physical, g.first_row_ordinal)),
            ScanOrder::TimeDescending => entries.sort_by_key(|g| {
                (
                    std::cmp::Reverse(g.max_occurred_at_physical),
                    std::cmp::Reverse(g.first_row_ordinal),
                )
            }),
        }
        let planned: Option<hashbrown::HashMap<u32, RowSelection>> = request.selection.map(|rows| {
            plan_row_selection(&entries, rows)
                .into_iter()
                .map(|selected| (selected.granule_id, selected.rows))
                .collect()
        });
        let own_bound = TopNBound::new(request.order);
        let bound = match (timed, request.limit) {
            (true, Some(_)) => Some(request.bound.unwrap_or(&own_bound)),
            _ => request.bound,
        };
        let mut stats = SelectiveScanStats::default();
        let mut held: Vec<Option<SelectedBatch>> = Vec::new();
        let mut best: Vec<(i64, u64, usize, u32)> = Vec::new();
        let mut live: Vec<usize> = Vec::new();
        let chunk_len = if bound.is_some() { 1 } else { PARALLEL_CHUNK };
        let stored_limit_hit = |returned: usize| request.limit.is_some_and(|limit| !timed && returned >= limit);
        let mut cursor = 0;
        while cursor < entries.len() && !stored_limit_hit(stats.rows_returned) {
            let mut chunk: Vec<(&GranuleEntry, RowSelection)> = Vec::with_capacity(chunk_len);
            while chunk.len() < chunk_len {
                let Some(entry) = entries.get(cursor) else { break };
                cursor += 1;
                let candidates = match &planned {
                    Some(plan) => match plan.get(&entry.granule_id) {
                        Some(rows) => rows.clone(),
                        None => continue,
                    },
                    None => RowSelection::all(entry.row_count),
                };
                if candidates.is_empty() || all_deleted(request.deletions, entry, &candidates) {
                    continue;
                }
                if let Some(bound) = bound
                    && !bound.admits(entry.min_occurred_at_physical, entry.max_occurred_at_physical)
                {
                    stats.pruned_by_bound += 1;
                    continue;
                }
                chunk.push((entry, candidates));
            }
            if chunk.is_empty() {
                continue;
            }
            let ids: Vec<u32> = chunk.iter().map(|(entry, _)| entry.granule_id).collect();
            stats.granules_read.extend_from_slice(&ids);
            let mut batches: Vec<ScanBatch> = Vec::with_capacity(ids.len());
            let push = |batch| {
                batches.push(batch);
                Ok(())
            };
            if ids.len() > 1 {
                self.scan_projected(column_ids, &ids, push)?;
            } else {
                self.scan_projected_serial(column_ids, &ids, push)?;
            }
            if batches.len() != chunk.len() {
                return Err(FormatError::Structural {
                    rule: "a scanned granule yields one batch",
                });
            }
            for ((entry, candidates), mut batch) in chunk.into_iter().zip(batches) {
                if stored_limit_hit(stats.rows_returned) {
                    break;
                }
                self.mask_redacted(&mut batch, column_ids, request.redactions)?;
                let first = entry.first_row_ordinal;
                let mut keep: Vec<u32> = candidates
                    .iter()
                    .filter(|&row| !request.deletions.is_some_and(|d| d.is_deleted(first + u64::from(row))))
                    .filter(|&row| row_filter(&batch, row))
                    .collect();
                if !timed {
                    if let Some(limit) = request.limit {
                        keep.truncate(limit - stats.rows_returned);
                    }
                    let rows = RowSelection::from_sorted_rows(keep);
                    if rows.is_empty() {
                        continue;
                    }
                    stats.rows_returned += rows.len();
                    sink(SelectedBatch {
                        batch,
                        first_row_ordinal: first,
                        rows,
                    })?;
                    continue;
                }
                let occurred = occurred_column
                    .and_then(|index| batch.columns.get(index))
                    .and_then(|column| match column {
                        ScanColumn::Materialized(ColumnRead {
                            data: ColumnData::I64(values),
                            ..
                        }) => Some(values.as_slice()),
                        _ => None,
                    })
                    .ok_or(FormatError::Structural {
                        rule: "occurred_at holds one i64 per row",
                    })?;
                let index = held.len();
                let before = best.len();
                for &row in &keep {
                    let at = *occurred.get(row as usize).ok_or(FormatError::Structural {
                        rule: "occurred_at holds one i64 per row",
                    })?;
                    best.push((at, first + u64::from(row), index, row));
                }
                let Some(limit) = request.limit else {
                    best.truncate(before);
                    let rows = RowSelection::from_sorted_rows(keep);
                    if rows.is_empty() {
                        continue;
                    }
                    stats.rows_returned += rows.len();
                    sink(SelectedBatch {
                        batch,
                        first_row_ordinal: first,
                        rows,
                    })?;
                    continue;
                };
                live.push(keep.len());
                held.push((!keep.is_empty()).then_some(SelectedBatch {
                    batch,
                    first_row_ordinal: first,
                    rows: RowSelection::Range(0..0),
                }));
                sort_best(&mut best, request.order);
                for &(_, _, dropped, _) in best.get(limit..).unwrap_or_default() {
                    if let Some(count) = live.get_mut(dropped) {
                        *count = count.saturating_sub(1);
                        if *count == 0
                            && let Some(slot) = held.get_mut(dropped)
                        {
                            *slot = None;
                        }
                    }
                }
                best.truncate(limit);
                if best.len() == limit
                    && let (Some(bound), Some(&(nth, ..))) = (bound, best.last())
                {
                    bound.tighten(nth);
                }
            }
        }
        if timed {
            sort_best(&mut best, request.order);
            if let Some(limit) = request.limit {
                best.truncate(limit);
            }
            let mut rows_by_batch: Vec<Vec<u32>> = vec![Vec::new(); held.len()];
            for &(_, _, index, row) in &best {
                if let Some(rows) = rows_by_batch.get_mut(index) {
                    rows.push(row);
                }
            }
            for (slot, mut rows) in held.iter_mut().zip(rows_by_batch) {
                if rows.is_empty() {
                    continue;
                }
                rows.sort_unstable();
                if let Some(mut selected) = slot.take() {
                    selected.rows = RowSelection::from_sorted_rows(rows);
                    stats.rows_returned += selected.rows.len();
                    sink(selected)?;
                }
            }
        }
        Ok(stats)
    }

    pub fn pages_for_rows(
        &self,
        column_id: u32,
        granule_id: u32,
        rows: &RowSelection,
    ) -> Result<Vec<u32>, FormatError> {
        let pages = self.page_directory_for(column_id, 0, granule_id)?;
        if pages.len() <= 1 {
            return Ok(if rows.is_empty() { Vec::new() } else { vec![0] });
        }
        let granule_first = self
            .granule_entry(granule_id)
            .map(|granule| granule.first_row_ordinal)
            .ok_or(FormatError::RefOutOfRange { what: "granule id" })?;
        let starts: Vec<u64> = pages
            .iter()
            .map(|page| page.first_row_ordinal.saturating_sub(granule_first))
            .collect();
        let mut out: Vec<u32> = Vec::new();
        for row in rows.iter() {
            let page = starts
                .partition_point(|&start| start <= u64::from(row))
                .saturating_sub(1) as u32;
            if out.last() != Some(&page) {
                out.push(page);
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryInputs<'a> {
    pub bound: Option<&'a TopNBound>,
    pub deletions: Option<&'a DeletionVector>,
    pub redactions: Option<&'a FieldRedactions>,
    pub selection: Option<&'a RoaringRangeBitmap>,
}

impl OeFile {
    pub fn granule_statistics(&self, entry: &GranuleEntry) -> GranuleStatistics {
        fold_page_stats(
            entry,
            self.footer
                .page_stats
                .iter()
                .filter(|stats| stats.granule_id == entry.granule_id),
        )
    }

    pub fn execute_query(
        &self,
        plan: &QueryPlan,
        column_ids: &[u32],
        granule_ids: &[u32],
        inputs: &QueryInputs<'_>,
        mut clause_matches: impl FnMut(&ScanBatch, u32, &FilterClause) -> bool,
        sink: impl FnMut(SelectedBatch) -> Result<(), FormatError>,
    ) -> Result<SelectiveScanStats, FormatError> {
        let QueryPlan::Scan {
            filter, limit, order, ..
        } = plan
        else {
            return Err(FormatError::Structural {
                rule: "an aggregate-shortcut plan is answered by the aggregate tier",
            });
        };
        let mut page_stats: HashMap<u32, Vec<&PageStats>> =
            granule_ids.iter().map(|&granule_id| (granule_id, Vec::new())).collect();
        for stats in &self.footer.page_stats {
            if let Some(pages) = page_stats.get_mut(&stats.granule_id) {
                pages.push(stats);
            }
        }
        let mut residuals: HashMap<u32, FilterExpr> = HashMap::with_capacity(granule_ids.len());
        let mut kept = Vec::with_capacity(granule_ids.len());
        for &granule_id in granule_ids {
            let entry = self.granule_entry(granule_id).ok_or(FormatError::RefOutOfRange {
                what: "scanned granule missing from the granule directory",
            })?;
            let pages = page_stats.get(&granule_id).map(Vec::as_slice).unwrap_or_default();
            if let Some(rest) = residual(filter, &fold_page_stats(entry, pages.iter().copied())) {
                residuals.insert(granule_id, rest);
                kept.push(granule_id);
            }
        }
        let request = ScanRequest {
            bound: inputs.bound,
            deletions: inputs.deletions,
            limit: *limit,
            order: *order,
            redactions: inputs.redactions,
            selection: inputs.selection,
        };
        self.scan_selective(
            column_ids,
            &kept,
            &request,
            |batch, row| {
                residuals
                    .get(&batch.granule_id)
                    .is_some_and(|rest| rest.matches_row(&mut |clause| clause_matches(batch, row, clause)))
            },
            sink,
        )
    }

    pub fn selection_for_external_ids(&self, locations: &[ExternalIdLocation]) -> RoaringRangeBitmap {
        let file_id = self.header().file_id;
        RoaringRangeBitmap::from_rows(
            locations
                .iter()
                .filter(|location| location.file_id == file_id)
                .map(|location| location.row_ordinal),
        )
    }

    pub fn selection_for_entity_hash(&self, entity_id_hash_low: u64) -> RoaringRangeBitmap {
        RoaringRangeBitmap::from_ranges(self.granules_for_entity_hash(entity_id_hash_low).into_iter().map(|g| {
            RowRange {
                end: g.first_row_ordinal + u64::from(g.row_count),
                start: g.first_row_ordinal,
            }
        }))
    }

    pub fn selection_for_thread(
        &self,
        space: TargetIdSpace,
        root_target: &[u8],
    ) -> Result<RoaringRangeBitmap, FormatError> {
        Ok(RoaringRangeBitmap::from_rows(self.thread_rows(space, root_target)?))
    }
}

fn fold_page_stats<'a>(entry: &GranuleEntry, pages: impl Iterator<Item = &'a PageStats>) -> GranuleStatistics {
    let mut statistics = vec![Statistic::TimeBounds {
        column_id: column_ids::OCCURRED_AT,
        max_time: i128::from(entry.max_occurred_at_physical),
        min_time: i128::from(entry.min_occurred_at_physical),
    }];
    let mut columns: std::collections::BTreeMap<u32, FoldedPageStats> = Default::default();
    for stats in pages {
        let page_span = stats.min_i128.zip(stats.max_i128);
        let folded = columns.entry(stats.column_id).or_insert((page_span, 0, 0));
        folded.0 = match (folded.0, page_span) {
            (Some((lo, hi)), Some((min, max))) => Some((lo.min(min), hi.max(max))),
            _ => None,
        };
        folded.1 += u64::from(stats.null_count);
        folded.2 += u64::from(stats.row_count);
    }
    for (column_id, (span, null_count, row_count)) in columns {
        if row_count != u64::from(entry.row_count) {
            continue;
        }
        if let Some((min, max)) = span {
            statistics.push(Statistic::MinMaxBound { column_id, max, min });
        }
        statistics.push(Statistic::NullCount {
            column_id,
            null_count,
            row_count,
        });
    }
    GranuleStatistics { statistics }
}

type FoldedPageStats = (Option<(i128, i128)>, u64, u64);

fn sort_best(best: &mut [(i64, u64, usize, u32)], order: ScanOrder) {
    match order {
        ScanOrder::TimeDescending => best.sort_unstable_by_key(|b| std::cmp::Reverse((b.0, b.1))),
        _ => best.sort_unstable_by_key(|candidate| (candidate.0, candidate.1)),
    }
}

fn all_deleted(deletions: Option<&DeletionVector>, entry: &GranuleEntry, rows: &RowSelection) -> bool {
    deletions.is_some_and(|deletions| {
        rows.iter()
            .all(|row| deletions.is_deleted(entry.first_row_ordinal + u64::from(row)))
    })
}

#[cfg(test)]
#[path = "test/query_scan.rs"]
mod tests;
