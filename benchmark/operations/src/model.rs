//! What one comparison run is told to do, what a single generated row looks like, and what the run reports back.
//!
//! Everything here is plain data. A run is pinned by a [`BenchmarkSpec`] and answers with a [`BenchmarkReport`], so
//! the same spec replayed on another host produces a report that can be diffed against the first line by line.

use anyhow::{Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// One file format the runner can prepare and measure.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkFormat {
    Oe,
    Parquet,
    Vortex,
}

impl BenchmarkFormat {
    pub fn label(self) -> &'static str {
        match self {
            Self::Oe => "oe",
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
        }
    }
}

impl FromStr for BenchmarkFormat {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "oe" => Ok(Self::Oe),
            "parquet" => Ok(Self::Parquet),
            "vortex" => Ok(Self::Vortex),
            _ => Err(anyhow!("unknown format `{value}` (expected oe, parquet, or vortex)")),
        }
    }
}

/// One operation the comparison measures on every format.
///
/// Every variant is answered by every format over the same dataset and is checked to return the same number of rows, so
/// a timing difference is a difference in how the format does the work rather than in how much work it did.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Operation {
    /// Encode the whole dataset and write it to a new file.
    CreateWrite,
    /// Remove a slice of rows and leave a readable result behind.
    DeleteRows,
    /// Report which rows differ between two versions of the same dataset.
    DiffVersions,
    /// Find the rows whose free-text field contains a substring carried by roughly one row in a thousand.
    FullTextSearch,
    /// Search the free-text field for a substring no row carries — the shape of a search that has to establish a file
    /// holds nothing, where a text index either skips the column's bytes or does not.
    FullTextSearchAbsent,
    /// Search the free-text field for a substring only a handful of rows carry, so most blocks hold no match and a
    /// text index has something to prune.
    FullTextSearchRare,
    /// Open a file cold and read nothing but its metadata.
    OpenMetadata,
    /// Fetch the single row carrying one entity id.
    PointLookup,
    /// Read every column of every row.
    ReadAllColumns,
    /// Read one column of every row.
    ReadOneColumn,
    /// Read one payload field for a scattered sample of rows.
    ReadPayloadField,
    /// Read the declared free-text column of every row.
    ReadTextColumn,
    /// Count the rows whose timestamp falls inside a window.
    RangeSearch,
    /// Change a slice of rows and leave a readable result behind.
    UpdateRows,
    /// Count the rows whose event type equals one value.
    ValueSearch,
}

impl Operation {
    /// The name this operation appears under in a report.
    pub fn label(self) -> &'static str {
        match self {
            Operation::CreateWrite => "create_write",
            Operation::DeleteRows => "delete_rows",
            Operation::DiffVersions => "diff_versions",
            Operation::FullTextSearch => "full_text_search",
            Operation::FullTextSearchAbsent => "full_text_search_absent",
            Operation::FullTextSearchRare => "full_text_search_rare",
            Operation::OpenMetadata => "open_metadata",
            Operation::PointLookup => "point_lookup",
            Operation::ReadAllColumns => "read_all_columns",
            Operation::ReadOneColumn => "read_one_column",
            Operation::ReadPayloadField => "read_payload_field",
            Operation::ReadTextColumn => "read_text_column",
            Operation::RangeSearch => "range_search",
            Operation::UpdateRows => "update_rows",
            Operation::ValueSearch => "value_search",
        }
    }
}

impl FromStr for Operation {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        OPERATIONS
            .iter()
            .copied()
            .find(|operation| operation.label() == value)
            .ok_or_else(|| anyhow!("unknown operation `{value}`"))
    }
}

/// Every operation, in the order a run measures them: create first (nothing can be read before it exists), then the
/// read and search family, then the two mutations, then the diff.
pub const OPERATIONS: &[Operation] = &[
    Operation::CreateWrite,
    Operation::OpenMetadata,
    Operation::ReadAllColumns,
    Operation::ReadOneColumn,
    Operation::ReadTextColumn,
    Operation::ReadPayloadField,
    Operation::PointLookup,
    Operation::RangeSearch,
    Operation::ValueSearch,
    Operation::FullTextSearch,
    Operation::FullTextSearchRare,
    Operation::FullTextSearchAbsent,
    Operation::UpdateRows,
    Operation::DeleteRows,
    Operation::DiffVersions,
];

/// Everything that pins one run. The same spec generates byte-identical files on any host, so two reports differ only
/// in their timings.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BenchmarkSpec {
    /// How many times each operation is repeated. The report keeps the fastest and the median of these.
    pub iterations: usize,
    /// Share of rows an update or a delete touches, in percent.
    pub mutation_percent: usize,
    /// Share of the timeline the range search covers, in percent, centred on the middle of it. This is the knob that
    /// decides how much of the file either format's pruning can rule out before it decodes anything.
    pub range_percent: usize,
    pub row_count: usize,
    /// Rows per OE granule, per Parquet row group, and per Vortex chunk. One value feeds all three so each format
    /// prunes at the same
    /// resolution and none of them wins on block size alone.
    pub rows_per_group: usize,
    /// Seeds the generated rows. Two runs of the same spec encode the identical dataset.
    pub seed: u64,
}

impl Default for BenchmarkSpec {
    /// Large enough that the files span many granules and row groups — where pruning, per-block statistics, and
    /// dictionary encoding all start to matter — and small enough to finish in under a minute on a laptop.
    fn default() -> Self {
        Self {
            iterations: 5,
            mutation_percent: 1,
            range_percent: 10,
            row_count: 100_000,
            rows_per_group: 8_192,
            seed: 0x5eed_0f0f_0000_0001,
        }
    }
}

impl BenchmarkSpec {
    /// Checks the spec describes a runnable comparison, so a bad hand-written TOML file is reported as a configuration
    /// error rather than failing part-way into a run.
    pub fn validate(&self) -> Result<()> {
        ensure!(self.iterations > 0, "iterations must be at least 1");
        ensure!(
            self.range_percent > 0 && self.range_percent <= 100,
            "range_percent must be between 1 and 100"
        );
        ensure!(self.row_count > 0, "row_count must be at least 1");
        ensure!(self.rows_per_group > 0, "rows_per_group must be at least 1");
        ensure!(
            self.mutation_percent > 0 && self.mutation_percent <= 100,
            "mutation_percent must be between 1 and 100"
        );
        ensure!(
            self.mutation_percent * self.row_count >= 100,
            "mutation_percent of row_count must select at least one row"
        );
        Ok(())
    }

    /// How many rows an update or a delete touches.
    pub fn mutated_row_count(&self) -> usize {
        self.row_count * self.mutation_percent / 100
    }
}

/// One generated row, in the shape every format encodes from.
///
/// This is the single source of truth for the dataset: the OE side turns it into an event envelope plus a payload
/// variant, the Parquet side turns it into a slot in an Arrow record batch, and neither adds a field the other does
/// not carry.
#[derive(Clone, Debug, PartialEq)]
pub struct BenchmarkRow {
    pub actor_id: Option<String>,
    pub char_count: i64,
    pub client: String,
    pub entity_id: String,
    pub entity_type: String,
    /// Public event identity, as the 128-bit value every format stores.
    pub event_id: u128,
    pub event_type: String,
    pub ingested_at_nanos: i64,
    pub kind: String,
    pub note: String,
    pub occurred_at_nanos: i64,
    /// Absent on one row in [`REACTIONS_ABSENT_EVERY`](crate::constant::REACTIONS_ABSENT_EVERY).
    pub reactions: Option<i64>,
    /// The row's `(epoch, sequence)` position within the file, and its row ordinal.
    pub sequence: u64,
    pub source: String,
    pub stream_sequence: u64,
}

/// What one operation cost on one format.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct FormatMeasurement {
    /// Allocation and operation-scoped peak-live memory, present only in a build with `alloc-counting` enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocations: Option<AllocationMeasurement>,
    /// File bytes the operation read or wrote. For a read this is what the format's own directory says it had to
    /// touch, so a format that prunes better reports a smaller number even at equal wall-clock.
    pub bytes: u64,
    /// Rows the operation produced or changed. Every format must report the same value; a run fails if they disagree.
    pub matched_rows: u64,
    pub median_nanos: u64,
    /// The median of the same iterations with the harness's input conversion counted in, present only where the
    /// operation converts the generated rows before the format can start (OE create/write and update). Earlier
    /// baselines reported this figure as `median_nanos`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_with_input_conversion_nanos: Option<u64>,
    pub min_nanos: u64,
    /// Every iteration in execution order. Summaries are derived from these values and never replace them.
    #[serde(default)]
    pub samples_nanos: Vec<u64>,
    /// Every iteration with input conversion counted in, in execution order; empty wherever
    /// `median_with_input_conversion_nanos` is absent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub samples_with_input_conversion_nanos: Vec<u64>,
    /// OE writer phases, present only when phase reporting was requested for an operation that writes OE.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phases: Vec<PhaseMeasurement>,
}

/// One operation iteration's allocator counters.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AllocationSample {
    pub allocated_bytes: u64,
    pub allocation_count: u64,
    pub largest_allocation_bytes: u64,
    pub peak_live_bytes: u64,
    pub reallocation_count: u64,
}

/// Median allocation counters plus every raw iteration, in execution order.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AllocationMeasurement {
    pub allocated_bytes: u64,
    pub allocation_count: u64,
    pub largest_allocation_bytes: u64,
    pub peak_live_bytes: u64,
    pub reallocation_count: u64,
    pub samples: Vec<AllocationSample>,
}

/// A stable phase boundary on the OE writer path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterPhase {
    InputConversion,
    SourceDecoding,
    Normalization,
    DictionaryConstruction,
    PathStatistics,
    GranuleConstruction,
    BlockEncoding,
    Layout,
    FooterConstruction,
    Integrity,
    FileAssembly,
    RowDestruction,
    DurableWrite,
}

/// Raw and summarized samples for one OE writer phase.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PhaseMeasurement {
    pub median_nanos: u64,
    pub min_nanos: u64,
    pub phase: WriterPhase,
    pub samples_nanos: Vec<u64>,
}

/// One format's measurement for one operation, next to how it compares with the operation's baseline.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FormatResult {
    /// The baseline's bytes divided by this format's. Above 1 means this format touched less of its file.
    pub byte_ratio: f64,
    pub format: String,
    pub measurement: FormatMeasurement,
    /// The baseline's median divided by this format's. Above 1 means this format was faster by that factor.
    pub speedup: f64,
}

/// One operation measured on every format.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OperationReport {
    /// The format every ratio in this operation is measured against: the slowest median of the operation, so the
    /// baseline is chosen per operation rather than being one format throughout.
    pub baseline: String,
    pub operation: Operation,
    /// One entry per format, in the order the runner measured them.
    pub results: Vec<FormatResult>,
}

/// One format's file, as written for the run.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FormatFile {
    pub bytes: u64,
    pub format: String,
}

/// The host and build a report was measured on. Absent details are ones the host could not answer; the process-level
/// ones are always known.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Environment {
    /// Whether the binary carries the counting allocator, and so whether its allocation figures are measurements.
    pub alloc_counting: bool,
    pub arch: String,
    /// The git commit the binary was built from, `-dirty` appended when the tree had uncommitted changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// Every `OE_BENCH_*` control in effect, as `NAME=value`, so a report says which control path it measured.
    #[serde(default)]
    pub controls: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kernel: Option<String>,
    pub os: String,
}

/// One report stamped with the run that produced it, so an appended history line is self-identifying without relying
/// on file order.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PersistedRun {
    pub report: BenchmarkReport,
    /// A fresh v4 UUID per run, so repeated runs of the same spec on the same host stay distinguishable.
    pub run_id: String,
    /// RFC 3339, so entries also sort lexically in append order.
    pub timestamp: String,
}

/// The finished comparison.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BenchmarkReport {
    /// Absent from reports written before it was recorded.
    #[serde(default)]
    pub environment: Environment,
    /// One entry per format, in the same order as each operation's results.
    pub files: Vec<FormatFile>,
    pub operations: Vec<OperationReport>,
    /// Process-wide resident-set high-water mark. Omitted on targets without a stable OS API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_high_water_bytes: Option<u64>,
    pub spec: BenchmarkSpec,
}

/// Turns raw per-iteration timings into the pair of figures a report keeps: the fastest run, and the median.
///
/// The fastest run is the least noisy estimate of the work itself; the median is what a caller would typically see.
/// Both come from the samples the run actually made — neither is interpolated.
pub fn summarize(mut samples: Vec<u64>, bytes: u64, matched_rows: u64) -> FormatMeasurement {
    let raw_samples = samples.clone();
    samples.sort_unstable();
    let median = samples.get(samples.len() / 2).copied().unwrap_or_default();
    FormatMeasurement {
        allocations: None,
        bytes,
        matched_rows,
        median_nanos: median,
        median_with_input_conversion_nanos: None,
        min_nanos: samples.first().copied().unwrap_or_default(),
        samples_nanos: raw_samples,
        samples_with_input_conversion_nanos: Vec::new(),
        phases: Vec::new(),
    }
}

/// Summarizes each allocation counter independently with the observed upper median, retaining raw samples for audit.
pub fn summarize_allocations(samples: Vec<AllocationSample>) -> AllocationMeasurement {
    fn median(samples: &[AllocationSample], value: impl Fn(&AllocationSample) -> u64) -> u64 {
        let mut values: Vec<u64> = samples.iter().map(value).collect();
        values.sort_unstable();
        values.get(values.len() / 2).copied().unwrap_or_default()
    }

    AllocationMeasurement {
        allocated_bytes: median(&samples, |sample| sample.allocated_bytes),
        allocation_count: median(&samples, |sample| sample.allocation_count),
        largest_allocation_bytes: median(&samples, |sample| sample.largest_allocation_bytes),
        peak_live_bytes: median(&samples, |sample| sample.peak_live_bytes),
        reallocation_count: median(&samples, |sample| sample.reallocation_count),
        samples,
    }
}

/// Summarizes raw phase samples without discarding their execution order.
pub fn summarize_phase(phase: WriterPhase, mut samples: Vec<u64>) -> PhaseMeasurement {
    let raw_samples = samples.clone();
    samples.sort_unstable();
    PhaseMeasurement {
        median_nanos: samples.get(samples.len() / 2).copied().unwrap_or_default(),
        min_nanos: samples.first().copied().unwrap_or_default(),
        phase,
        samples_nanos: raw_samples,
    }
}

/// How many times faster the first measurement is than the second, or `0.0` when the first took no measurable time.
pub fn ratio(slower: u64, faster: u64) -> f64 {
    if faster == 0 {
        return 0.0;
    }
    slower as f64 / faster as f64
}

#[cfg(test)]
#[path = "test/model.rs"]
mod tests;
