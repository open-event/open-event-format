
use anyhow::{Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Operation {
    CreateWrite,
    DeleteRows,
    DiffVersions,
    FullTextSearch,
    FullTextSearchAbsent,
    FullTextSearchRare,
    OpenMetadata,
    PointLookup,
    ReadAllColumns,
    ReadOneColumn,
    ReadPayloadField,
    ReadTextColumn,
    RangeSearch,
    UpdateRows,
    ValueSearch,
}

impl Operation {
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BenchmarkSpec {
    pub iterations: usize,
    pub mutation_percent: usize,
    pub range_percent: usize,
    pub row_count: usize,
    pub rows_per_group: usize,
    pub seed: u64,
}

impl Default for BenchmarkSpec {
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

    pub fn mutated_row_count(&self) -> usize {
        self.row_count * self.mutation_percent / 100
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BenchmarkRow {
    pub actor_id: Option<String>,
    pub char_count: i64,
    pub client: String,
    pub entity_id: String,
    pub entity_type: String,
    pub event_id: u128,
    pub event_type: String,
    pub ingested_at_nanos: i64,
    pub kind: String,
    pub note: String,
    pub occurred_at_nanos: i64,
    pub reactions: Option<i64>,
    pub sequence: u64,
    pub source: String,
    pub stream_sequence: u64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct FormatMeasurement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocations: Option<AllocationMeasurement>,
    pub bytes: u64,
    pub matched_rows: u64,
    pub median_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_with_input_conversion_nanos: Option<u64>,
    pub min_nanos: u64,
    #[serde(default)]
    pub samples_nanos: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub samples_with_input_conversion_nanos: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phases: Vec<PhaseMeasurement>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AllocationSample {
    pub allocated_bytes: u64,
    pub allocation_count: u64,
    pub largest_allocation_bytes: u64,
    pub peak_live_bytes: u64,
    pub reallocation_count: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AllocationMeasurement {
    pub allocated_bytes: u64,
    pub allocation_count: u64,
    pub largest_allocation_bytes: u64,
    pub peak_live_bytes: u64,
    pub reallocation_count: u64,
    pub samples: Vec<AllocationSample>,
}

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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PhaseMeasurement {
    pub median_nanos: u64,
    pub min_nanos: u64,
    pub phase: WriterPhase,
    pub samples_nanos: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FormatResult {
    pub byte_ratio: f64,
    pub format: String,
    pub measurement: FormatMeasurement,
    pub speedup: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OperationReport {
    pub baseline: String,
    pub operation: Operation,
    pub results: Vec<FormatResult>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FormatFile {
    pub bytes: u64,
    pub format: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Environment {
    pub alloc_counting: bool,
    pub arch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PersistedRun {
    pub report: BenchmarkReport,
    pub run_id: String,
    pub timestamp: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BenchmarkReport {
    #[serde(default)]
    pub environment: Environment,
    pub files: Vec<FormatFile>,
    pub operations: Vec<OperationReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_high_water_bytes: Option<u64>,
    pub spec: BenchmarkSpec,
}

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

pub fn ratio(slower: u64, faster: u64) -> f64 {
    if faster == 0 {
        return 0.0;
    }
    slower as f64 / faster as f64
}

#[cfg(test)]
#[path = "test/model.rs"]
mod tests;
