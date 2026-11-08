//! Selective scans over [`OeFile::scan_projected`]: a row limit with an order, a shared top-N bound that prunes
//! granules as it tightens, scans planned from a known row selection, and contiguous selections handed back as slices.
//!
//! - **Limit pushdown.** [`ScanRequest::limit`] stops the scan once that many rows have survived the selection, the
//!   deletion vector and the caller's row filter. In stored order the first survivors win. In time order the scan
//!   walks granules by their occurred-at bounds (newest first for [`ScanOrder::TimeDescending`]) and keeps the best
//!   `limit` rows by `(occurred_at, row ordinal)`.
//! - **Dynamic top-N filter.** A [`TopNBound`] holds the occurred-at value a row must reach to still enter the top N,
//!   behind a version counter. Before each granule is dispatched the scan re-reads the bound and skips the granule if
//!   its occurred-at range cannot reach it. Several scans (over several files, say) may share one bound.
//! - **Planned from rows.** [`plan_row_selection`] maps a row bitmap — external-id or entity index hits — onto the
//!   granules that hold those rows and each granule's local rows, so only those granules are read and no per-row
//!   membership test runs over the rest; [`OeFile::pages_for_rows`] narrows a block further to its pages.
//! - **Slices.** [`RowSelection`] keeps a single contiguous run as a range, so a time window over time-sorted rows is
//!   sliced, not gathered row by row.

use super::*;
use crate::deletes::DeletionVector;
use crate::indexes::bitmap::{RoaringRangeBitmap, RowRange};
use crate::indexes::bool_pruning::{ExprQueryPlan, FilterExpr, residual};
use crate::indexes::external_id::ExternalIdLocation;
use crate::indexes::pruning::{FilterClause, GranuleStatistics, Statistic};
use std::sync::atomic::AtomicI64;

/// Granules a limited scan without a bound decodes side by side per dispatch.
const PARALLEL_CHUNK: usize = 4;

/// The order in which a scan visits granules and, with a limit, which rows win.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanOrder {
    /// The caller's granule order; the first `limit` survivors win.
    #[default]
    Stored,
    /// Oldest occurred-at first; the `limit` earliest rows win.
    TimeAscending,
    /// Newest occurred-at first; the `limit` latest rows win ("latest N").
    TimeDescending,
}

/// A monotone occurred-at bound shared between a top-N scan and its granule pruner.
///
/// For [`ScanOrder::TimeDescending`] it is a lower bound that only rises; for [`ScanOrder::TimeAscending`] an upper
/// bound that only falls. A granule whose occurred-at range lies strictly beyond it cannot place a row in the top N.
/// Every tightening bumps [`version`](Self::version), so a pruner can tell a fresh bound from the one it last saw.
#[derive(Debug)]
pub struct TopNBound {
    descending: bool,
    threshold: AtomicI64,
    version: AtomicU64,
}

impl TopNBound {
    /// An open bound for `order`; a stored-order bound never prunes.
    pub fn new(order: ScanOrder) -> Self {
        let descending = order != ScanOrder::TimeAscending;
        TopNBound {
            descending,
            threshold: AtomicI64::new(if descending { i64::MIN } else { i64::MAX }),
            version: AtomicU64::new(0),
        }
    }

    /// The current threshold and its version.
    pub fn snapshot(&self) -> (u64, i64) {
        let version = self.version.load(Ordering::Acquire);
        (version, self.threshold.load(Ordering::Acquire))
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Moves the bound to `candidate` when that is tighter, returning whether it moved.
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

    /// Whether a granule spanning `[min, max]` occurred-at could still hold a top-N row. Ties are kept: a row equal
    /// to the bound can still win on its row ordinal.
    pub fn admits(&self, min: i64, max: i64) -> bool {
        let threshold = self.threshold.load(Ordering::Acquire);
        if self.descending {
            max >= threshold
        } else {
            min <= threshold
        }
    }
}

/// The rows of one granule a scan keeps, as granule-local row indexes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSelection {
    /// One contiguous run: consumers slice instead of gathering.
    Range(Range<u32>),
    /// Sorted, distinct, not one contiguous run.
    Rows(Vec<u32>),
}

impl RowSelection {
    /// Every row of a granule of `row_count` rows.
    pub fn all(row_count: u32) -> Self {
        RowSelection::Range(0..row_count)
    }

    /// The selection a keep-mask describes: a range when the kept rows form one run (or none), else the row list.
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

    /// The selection from sorted, distinct row indexes, collapsed to a range when they are contiguous.
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

    /// The selected rows in ascending order.
    pub fn iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        match self {
            RowSelection::Range(range) => Box::new(range.clone()),
            RowSelection::Rows(rows) => Box::new(rows.iter().copied()),
        }
    }

    /// The selected values of a one-value-per-row column: a single copy of a slice for a range, a gather otherwise.
    /// Rows past the end of `values` are skipped.
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

/// One granule a row bitmap touches, with the granule-local rows it selects there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GranuleSelection {
    pub granule_id: u32,
    pub rows: RowSelection,
}

/// Maps a bitmap of file row ordinals onto the granules that hold them, in row order, intersecting the bitmap with
/// each granule's row range directly. Granules the bitmap does not touch are absent; rows past the file are ignored.
pub fn plan_row_selection(granules: &[GranuleEntry], rows: &RoaringRangeBitmap) -> Vec<GranuleSelection> {
    let mut ordered: Vec<&GranuleEntry> = granules.iter().collect();
    ordered.sort_by_key(|granule| granule.first_row_ordinal);
    let mut plan = Vec::new();
    for granule in ordered {
        let start = granule.first_row_ordinal;
        let end = start + u64::from(granule.row_count);
        let ranges = rows.ranges();
        // First run that ends after the granule starts.
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

/// What a selective scan reads and keeps.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScanRequest<'a> {
    /// A shared bound to re-check before every granule; time orders with a limit create their own when this is
    /// `None`, and tighten it either way.
    pub bound: Option<&'a TopNBound>,
    /// Rows already deleted, by file row ordinal.
    pub deletions: Option<&'a DeletionVector>,
    /// Stop once this many rows survive.
    pub limit: Option<usize>,
    pub order: ScanOrder,
    /// Only these file row ordinals are candidates; granules holding none are not read.
    pub selection: Option<&'a RoaringRangeBitmap>,
}

/// One granule's batch with the granule-local rows that survived.
#[derive(Debug)]
pub struct SelectedBatch {
    pub batch: ScanBatch,
    pub first_row_ordinal: u64,
    pub rows: RowSelection,
}

/// What a selective scan did, for callers and tests that check pruning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectiveScanStats {
    /// Granules decoded, in dispatch order.
    pub granules_read: Vec<u32>,
    /// Granules skipped by the top-N bound just before dispatch.
    pub pruned_by_bound: usize,
    pub rows_returned: usize,
}

impl OeFile {
    /// [`Self::scan_projected`] with a limit, an order, a selection, deletions and a row filter; see the module
    /// documentation. `row_filter(batch, row)` decides each candidate row of a batch and must be exact. `sink` gets
    /// one [`SelectedBatch`] per granule that keeps a row: in visit order for [`ScanOrder::Stored`], and for a time
    /// order after the scan, in visit order, holding exactly the top-`limit` rows. A time order needs
    /// [`column_ids::OCCURRED_AT`] in `column_ids`.
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
            let entry = self
                .footer
                .granules
                .iter()
                .find(|granule| granule.granule_id == granule_id)
                .ok_or(FormatError::RefOutOfRange {
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
        // Time order: candidate batches and the best rows so far, as (occurred_at, row ordinal, batch index, row).
        let mut held: Vec<Option<SelectedBatch>> = Vec::new();
        let mut best: Vec<(i64, u64, usize, u32)> = Vec::new();
        // Without a bound to re-check, granules go out in windows that decode side by side; a bound is re-read
        // before every granule, so it dispatches them one at a time.
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
            for ((entry, candidates), batch) in chunk.into_iter().zip(batches) {
                if stored_limit_hit(stats.rows_returned) {
                    break;
                }
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
                        }) => Some(values.clone()),
                        _ => None,
                    })
                    .ok_or(FormatError::Structural {
                        rule: "occurred_at holds one i64 per row",
                    })?;
                let index = held.len();
                for &row in &keep {
                    let at = *occurred.get(row as usize).ok_or(FormatError::Structural {
                        rule: "occurred_at holds one i64 per row",
                    })?;
                    best.push((at, first + u64::from(row), index, row));
                }
                if let Some(limit) = request.limit {
                    sort_best(&mut best, request.order);
                    best.truncate(limit);
                    if best.len() == limit
                        && let (Some(bound), Some(&(nth, ..))) = (bound, best.last())
                    {
                        bound.tighten(nth);
                    }
                }
                held.push(Some(SelectedBatch {
                    batch,
                    first_row_ordinal: first,
                    rows: RowSelection::Range(0..0),
                }));
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

    /// The page indexes of `column_id`'s block in `granule_id` that hold any of `rows` (granule-local), for point
    /// reads through [`Self::read_page`]. A block without a page directory is one page, page 0.
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
            .footer
            .granules
            .iter()
            .find(|granule| granule.granule_id == granule_id)
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

/// Where a planned scan gets its rows besides the filter: every granule, or an index's row hits.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryInputs<'a> {
    pub bound: Option<&'a TopNBound>,
    pub deletions: Option<&'a DeletionVector>,
    /// Index hits (see [`OeFile::selection_for_external_ids`], [`OeFile::selection_for_entity_hash`] and
    /// [`OeFile::selection_for_thread`]); only these rows are candidates.
    pub selection: Option<&'a RoaringRangeBitmap>,
}

impl OeFile {
    /// What the footer says about a granule, as pruning statistics: its occurred-at bounds and every column's
    /// min/max and null count from the page statistics.
    pub fn granule_statistics(&self, entry: &GranuleEntry) -> GranuleStatistics {
        let mut statistics = vec![Statistic::TimeBounds {
            column_id: column_ids::OCCURRED_AT,
            max_time: i128::from(entry.max_occurred_at_physical),
            min_time: i128::from(entry.min_occurred_at_physical),
        }];
        // A granule may list several pages per column: fold them into one span and one null count per column, and
        // drop the span when any page lacks one.
        let mut columns: std::collections::BTreeMap<u32, FoldedPageStats> = Default::default();
        for stats in self
            .footer
            .page_stats
            .iter()
            .filter(|s| s.granule_id == entry.granule_id)
        {
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
            // Only page statistics covering every row of the granule may rule it out.
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

    /// Runs a planned query over `granule_ids`: granules the boolean filter proves empty are not read, each kept
    /// granule's rows are tested against its own residual filter through `clause_matches(batch, row, clause)` (which
    /// must be exact), and the plan's limit and order reach [`Self::scan_selective`]. The aggregate shortcut is
    /// answered by another tier, so it is refused here.
    pub fn execute_query(
        &self,
        plan: &ExprQueryPlan,
        column_ids: &[u32],
        granule_ids: &[u32],
        inputs: &QueryInputs<'_>,
        mut clause_matches: impl FnMut(&ScanBatch, u32, &FilterClause) -> bool,
        sink: impl FnMut(SelectedBatch) -> Result<(), FormatError>,
    ) -> Result<SelectiveScanStats, FormatError> {
        let ExprQueryPlan::Scan { filter, limit, order } = plan else {
            return Err(FormatError::Structural {
                rule: "an aggregate-shortcut plan is answered by the aggregate tier",
            });
        };
        let mut residuals: HashMap<u32, FilterExpr> = HashMap::with_capacity(granule_ids.len());
        let mut kept = Vec::with_capacity(granule_ids.len());
        for &granule_id in granule_ids {
            let entry =
                self.footer
                    .granules
                    .iter()
                    .find(|g| g.granule_id == granule_id)
                    .ok_or(FormatError::RefOutOfRange {
                        what: "scanned granule missing from the granule directory",
                    })?;
            if let Some(rest) = residual(filter, &self.granule_statistics(entry)) {
                residuals.insert(granule_id, rest);
                kept.push(granule_id);
            }
        }
        let request = ScanRequest {
            bound: inputs.bound,
            deletions: inputs.deletions,
            limit: *limit,
            order: *order,
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

    /// The rows of this file among external-id index hits; locations in other files are ignored.
    pub fn selection_for_external_ids(&self, locations: &[ExternalIdLocation]) -> RoaringRangeBitmap {
        let file_id = self.header().file_id;
        RoaringRangeBitmap::from_rows(
            locations
                .iter()
                .filter(|location| location.file_id == file_id)
                .map(|location| location.row_ordinal),
        )
    }

    /// The rows of the granules the entity-hash filters admit for `entity_id_hash_low`, as whole-granule runs. A
    /// superset: the caller's filter still compares the hash.
    pub fn selection_for_entity_hash(&self, entity_id_hash_low: u64) -> RoaringRangeBitmap {
        RoaringRangeBitmap::from_ranges(self.granules_for_entity_hash(entity_id_hash_low).into_iter().map(|g| {
            RowRange {
                end: g.first_row_ordinal + u64::from(g.row_count),
                start: g.first_row_ordinal,
            }
        }))
    }

    /// The rows of the thread rooted at `root_target`, from the thread index.
    pub fn selection_for_thread(
        &self,
        space: TargetIdSpace,
        root_target: &[u8],
    ) -> Result<RoaringRangeBitmap, FormatError> {
        Ok(RoaringRangeBitmap::from_rows(self.thread_rows(space, root_target)?))
    }
}

/// One column's page statistics folded over a granule: the value span, null count and row count.
type FoldedPageStats = (Option<(i128, i128)>, u64, u64);

/// Orders top-N candidates best first: by occurred-at, then row ordinal, both in the scan's direction.
fn sort_best(best: &mut [(i64, u64, usize, u32)], order: ScanOrder) {
    match order {
        ScanOrder::TimeDescending => best.sort_unstable_by_key(|b| std::cmp::Reverse((b.0, b.1))),
        _ => best.sort_unstable_by_key(|candidate| (candidate.0, candidate.1)),
    }
}

/// Whether the deletion vector removes every candidate row of the granule, so it need not be read.
fn all_deleted(deletions: Option<&DeletionVector>, entry: &GranuleEntry, rows: &RowSelection) -> bool {
    deletions.is_some_and(|deletions| {
        rows.iter()
            .all(|row| deletions.is_deleted(entry.first_row_ordinal + u64::from(row)))
    })
}

#[cfg(test)]
#[path = "test/query_scan.rs"]
mod tests;
