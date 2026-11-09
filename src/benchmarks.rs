use hashbrown::{HashMap, HashSet};

pub const NEGATIVE_CONTROL_FLUSH_BYTES: u64 = 128 * 1024;

pub fn flush_unit_valid_as_default(flush_bytes: u64) -> bool {
    flush_bytes < NEGATIVE_CONTROL_FLUSH_BYTES
}

#[derive(Debug, Clone)]
pub struct FlushTargetMeasurements {
    pub four_kib_benchmarked: bool,
    pub sixteen_kib_benchmarked: bool,
}

pub fn flush_default_selectable(m: &FlushTargetMeasurements) -> bool {
    m.four_kib_benchmarked && m.sixteen_kib_benchmarked
}

#[derive(Debug, Clone)]
pub struct QueryClassMeasurement {
    pub cold_cache_passes: bool,
    pub explicitly_excluded: bool,
    pub warm_cache_passes: bool,
}

pub fn query_class_is_ready(m: &QueryClassMeasurement) -> bool {
    m.explicitly_excluded || (m.warm_cache_passes && m.cold_cache_passes)
}

#[derive(Debug, Clone)]
pub struct AcceleratorConditions {
    pub batch_exceeds_threshold: bool,
    pub benefit_is_positive: bool,
    pub encoding_compatible: bool,
    pub fallback_available: bool,
    pub parity_passed: bool,
    pub provider_detected: bool,
    pub self_test_passed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceleratorPath {
    Accelerated,
    Software,
}

pub fn accelerator_path(conditions: &AcceleratorConditions) -> AcceleratorPath {
    if conditions.batch_exceeds_threshold
        && conditions.benefit_is_positive
        && conditions.encoding_compatible
        && conditions.fallback_available
        && conditions.parity_passed
        && conditions.provider_detected
        && conditions.self_test_passed
    {
        AcceleratorPath::Accelerated
    } else {
        AcceleratorPath::Software
    }
}

#[derive(Debug, Default)]
pub struct SnapshotSafetyChecker {
    oe_ids: HashSet<u128>,
}

impl SnapshotSafetyChecker {
    pub fn record_oe(&mut self, event_id: u128) {
        self.oe_ids.insert(event_id);
    }

    pub fn is_duplicate(&self, event_id: u128) -> bool {
        self.oe_ids.contains(&event_id)
    }
}

pub fn min_blackout_seconds(corpus_bytes: u64, throughput_bytes_per_sec: u64) -> u64 {
    if throughput_bytes_per_sec == 0 {
        return u64::MAX;
    }
    corpus_bytes.div_ceil(throughput_bytes_per_sec)
}

pub fn maintenance_gate_passes(live_p99_micros: u64, live_p99_bound_micros: u64) -> bool {
    live_p99_micros <= live_p99_bound_micros
}

#[derive(Debug, Clone)]
pub struct CommittedBaseline {
    pub benchmark_id: String,
    pub equivalence_verified: bool,
    pub measured_value: f64,
    pub tolerance_fraction: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceleratedBaselineError {
    EquivalenceNotVerified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadinessError {
    NoBenchmark { capability: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegressionReason {
    pub baseline_value: f64,
    pub benchmark_id: String,
    pub measured_value: f64,
}

#[derive(Debug, Default)]
pub struct BenchGateRegistry {
    baselines: HashMap<String, CommittedBaseline>,
    benchmarks_for: HashMap<String, Vec<String>>,
    ready: HashSet<String>,
}

impl BenchGateRegistry {
    pub fn register_benchmark(&mut self, capability: impl Into<String>, benchmark_id: impl Into<String>) {
        self.benchmarks_for
            .entry(capability.into())
            .or_default()
            .push(benchmark_id.into());
    }

    pub fn commit_baseline(&mut self, baseline: CommittedBaseline) {
        self.baselines.insert(baseline.benchmark_id.clone(), baseline);
    }

    pub fn commit_accelerated_baseline(&mut self, baseline: CommittedBaseline) -> Result<(), AcceleratedBaselineError> {
        if !baseline.equivalence_verified {
            return Err(AcceleratedBaselineError::EquivalenceNotVerified);
        }
        self.baselines.insert(baseline.benchmark_id.clone(), baseline);
        Ok(())
    }

    pub fn mark_ready(&mut self, capability: impl Into<String>) -> Result<(), ReadinessError> {
        let capability = capability.into();
        if !self.benchmarks_for.contains_key(&capability) {
            return Err(ReadinessError::NoBenchmark { capability });
        }
        self.ready.insert(capability);
        Ok(())
    }

    pub fn detect_regression(
        &mut self,
        capability: &str,
        benchmark_id: &str,
        measured_value: f64,
    ) -> Option<RegressionReason> {
        let baseline = self.baselines.get(benchmark_id)?;
        let threshold = baseline.measured_value * (1.0 + baseline.tolerance_fraction);
        if measured_value > threshold {
            self.ready.remove(capability);
            Some(RegressionReason {
                baseline_value: baseline.measured_value,
                benchmark_id: benchmark_id.to_owned(),
                measured_value,
            })
        } else {
            None
        }
    }

    pub fn is_ready(&self, capability: &str) -> bool {
        self.ready.contains(capability)
    }
}

pub const REQUESTS_PER_COLD_OPEN_TARGET: u32 = 1;

#[derive(Debug, Clone)]
pub struct ColdOpenMeasurement {
    pub justification_recorded: bool,
    pub request_count: u32,
}

pub fn cold_open_gate_passes(m: &ColdOpenMeasurement) -> bool {
    m.request_count <= REQUESTS_PER_COLD_OPEN_TARGET || m.justification_recorded
}

#[derive(Debug, Clone)]
pub struct PrunedFileMetadataMeasurement {
    pub full_granule_marks_share_bytes: u64,
    pub planner_metadata_bytes_read: u64,
}

pub fn pruned_file_metadata_gate_passes(m: &PrunedFileMetadataMeasurement) -> bool {
    m.planner_metadata_bytes_read < m.full_granule_marks_share_bytes
}

pub const S3_DURABLE_LOOKUP_ROUND_TRIP_TARGET: u32 = 2;

#[derive(Debug, Clone)]
pub struct S3DurableLookupMeasurement {
    pub dependent_round_trips: u32,
    pub elapsed_millis: f64,
    pub per_request_millis_budget: f64,
}

pub fn s3_durable_lookup_gate_passes(m: &S3DurableLookupMeasurement) -> bool {
    m.dependent_round_trips <= S3_DURABLE_LOOKUP_ROUND_TRIP_TARGET
        && m.elapsed_millis <= m.dependent_round_trips as f64 * m.per_request_millis_budget
}

#[derive(Debug, Clone)]
pub struct WafMeasurement {
    pub ceiling: f64,
    pub measured_waf: f64,
}

pub fn waf_gate_passes(m: &WafMeasurement) -> bool {
    m.measured_waf <= m.ceiling
}

pub const LOG_OBJECT_FRAMING_CEILING_BYTES: u64 = 32;

pub const LOG_STREAM_FIRST_RECORD_CEILING_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone)]
pub struct LogObjectMeasurement {
    pub first_record_verified_bytes: u64,
    pub framing_bytes: u64,
    pub object_bytes: u64,
    pub verified_bytes: u64,
}

pub fn log_object_gate_passes(m: &LogObjectMeasurement) -> bool {
    m.framing_bytes <= LOG_OBJECT_FRAMING_CEILING_BYTES
        && m.first_record_verified_bytes <= LOG_STREAM_FIRST_RECORD_CEILING_BYTES
        && m.verified_bytes <= m.object_bytes
}

pub const GRAPH_EDGE_BYTES_TARGET: f64 = 6.0;

#[derive(Debug, Clone)]
pub struct GraphEdgeBytesMeasurement {
    pub edges: u64,
    pub stored_bytes: u64,
}

pub fn graph_edge_bytes_gate_passes(m: &GraphEdgeBytesMeasurement) -> bool {
    m.stored_bytes as f64 <= m.edges as f64 * GRAPH_EDGE_BYTES_TARGET
}

pub const GRAPH_EVENT_ID_BYTES_TARGET: f64 = 12.0;

pub const GRAPH_EVENT_ID_FALSE_POSITIVE_TARGET: f64 = 0.02;

#[derive(Debug, Clone)]
pub struct GraphEventIdIndexMeasurement {
    pub absent_admitted: u64,
    pub absent_probes: u64,
    pub events: u64,
    pub section_bytes: u64,
}

pub fn graph_event_id_index_gate_passes(m: &GraphEventIdIndexMeasurement) -> bool {
    m.section_bytes as f64 <= m.events as f64 * GRAPH_EVENT_ID_BYTES_TARGET
        && m.absent_admitted as f64 <= m.absent_probes as f64 * GRAPH_EVENT_ID_FALSE_POSITIVE_TARGET
}

pub const GRAPH_CHAIN_LABEL_BYTES_TARGET: f64 = 4.0;

pub const GRAPH_CHAIN_LINK_BYTES_TARGET: f64 = 8.0;

#[derive(Debug, Clone)]
pub struct GraphChainCoverMeasurement {
    pub label_bytes: u64,
    pub labels: u64,
    pub link_bytes: u64,
    pub links: u64,
}

pub fn graph_chain_cover_gate_passes(m: &GraphChainCoverMeasurement) -> bool {
    m.label_bytes as f64 <= m.labels as f64 * GRAPH_CHAIN_LABEL_BYTES_TARGET
        && m.link_bytes as f64 <= m.links as f64 * GRAPH_CHAIN_LINK_BYTES_TARGET
}

pub const STATE_CHUNK_BYTES_PER_EVENT_TARGET: f64 = 16_384.0;

#[derive(Debug, Clone)]
pub struct StateChunkWriteMeasurement {
    pub bytes_written: u64,
    pub events: u64,
}

pub fn state_chunk_write_gate_passes(m: &StateChunkWriteMeasurement) -> bool {
    m.bytes_written as f64 <= m.events as f64 * STATE_CHUNK_BYTES_PER_EVENT_TARGET
}

pub const STATE_LOOKUP_LOCAL_P99_MICROS_TARGET: f64 = 1_000.0;

#[derive(Debug, Clone)]
pub struct StateLookupMeasurement {
    pub cold_fetches_max: u64,
    pub cold_p99_micros: f64,
    pub round_trip_micros: f64,
    pub tree_levels: u64,
    pub warm_p99_micros: f64,
}

pub fn state_lookup_gate_passes(m: &StateLookupMeasurement) -> bool {
    m.cold_fetches_max <= m.tree_levels
        && m.cold_p99_micros <= m.tree_levels as f64 * m.round_trip_micros + STATE_LOOKUP_LOCAL_P99_MICROS_TARGET
        && m.warm_p99_micros <= STATE_LOOKUP_LOCAL_P99_MICROS_TARGET
}

#[derive(Debug, Clone)]
pub struct RoomReplayMeasurement {
    pub events: u64,
    pub graph_bytes: u64,
    pub state_chunk_bytes: u64,
    pub wall_millis: f64,
}

pub fn room_replay_gate_passes(m: &RoomReplayMeasurement) -> bool {
    m.state_chunk_bytes as f64 <= m.events as f64 * STATE_CHUNK_BYTES_PER_EVENT_TARGET
}

pub const XOR_FLOAT_BYTES_PER_VALUE_TARGET: f64 = 3.5;

#[derive(Debug, Clone)]
pub struct XorFloatMeasurement {
    pub best_existing_bytes: u64,
    pub chimp_bytes: u64,
    pub values: u64,
}

pub fn xor_float_gate_passes(m: &XorFloatMeasurement) -> bool {
    m.chimp_bytes < m.best_existing_bytes && m.chimp_bytes as f64 <= m.values as f64 * XOR_FLOAT_BYTES_PER_VALUE_TARGET
}

pub const TIME_SERIES_ROLLUP_EDGE_BLOCK_READS_TARGET: u64 = 8;

#[derive(Debug, Clone)]
pub struct TimeSeriesRollupMeasurement {
    pub aligned_block_reads: u64,
    pub cut_block_reads: u64,
    pub scan_block_reads: u64,
}

pub fn time_series_rollup_gate_passes(m: &TimeSeriesRollupMeasurement) -> bool {
    m.aligned_block_reads == 0
        && m.cut_block_reads <= TIME_SERIES_ROLLUP_EDGE_BLOCK_READS_TARGET
        && m.cut_block_reads < m.scan_block_reads
}

pub const AUDIT_FILE_OVERHEAD_CEILING_BYTES: u64 = 192;

#[derive(Debug, Clone)]
pub struct AuditFileOverheadMeasurement {
    pub added_bytes_at_few_rows: u64,
    pub added_bytes_at_many_rows: u64,
}

pub fn audit_file_overhead_gate_passes(m: &AuditFileOverheadMeasurement) -> bool {
    m.added_bytes_at_few_rows <= AUDIT_FILE_OVERHEAD_CEILING_BYTES
        && m.added_bytes_at_many_rows == m.added_bytes_at_few_rows
}

pub const SNAPSHOT_LOOKUP_REQUEST_TARGET: u32 = 2;

#[derive(Debug, Clone)]
pub struct SnapshotLookupMeasurement {
    pub bytes_fetched: u64,
    pub requests: u32,
    pub stored_bytes: u64,
}

pub fn snapshot_lookup_gate_passes(m: &SnapshotLookupMeasurement) -> bool {
    m.requests <= SNAPSHOT_LOOKUP_REQUEST_TARGET && m.bytes_fetched < m.stored_bytes
}

pub const INBOX_DIRECTORY_BYTES_PER_RECIPIENT: u64 = 48;

pub const INBOX_DIRECTORY_FRAMING_CEILING_BYTES: u64 = 64;

#[derive(Debug, Clone)]
pub struct InboxReadMeasurement {
    pub covering_granules: u64,
    pub directory_bytes: u64,
    pub granules_read: u64,
    pub recipients: u64,
}

pub fn inbox_read_gate_passes(m: &InboxReadMeasurement) -> bool {
    m.directory_bytes <= m.recipients * INBOX_DIRECTORY_BYTES_PER_RECIPIENT + INBOX_DIRECTORY_FRAMING_CEILING_BYTES
        && m.granules_read <= m.covering_granules
}

pub const PROFILE_OVERHEAD_CEILING_BYTES: u64 = 64;

#[derive(Debug, Clone)]
pub struct ProfileOverheadMeasurement {
    pub event_file_bytes: u64,
    pub event_header_block_bytes: u64,
    pub profiled_file_bytes: u64,
    pub profiled_header_block_bytes: u64,
}

pub fn profile_overhead_gate_passes(m: &ProfileOverheadMeasurement) -> bool {
    m.profiled_header_block_bytes == m.event_header_block_bytes
        && m.profiled_file_bytes.saturating_sub(m.event_file_bytes) <= PROFILE_OVERHEAD_CEILING_BYTES
}

pub const GRAPH_WALK_REQUESTS_PER_FILE_PER_LEVEL: u64 = 1;

#[derive(Debug, Clone)]
pub struct GraphWalkMeasurement {
    pub bytes_fetched: u64,
    pub files: u64,
    pub levels: u64,
    pub requests: u64,
}

pub fn graph_walk_gate_passes(m: &GraphWalkMeasurement) -> bool {
    m.requests
        <= m.levels
            .saturating_mul(m.files)
            .saturating_mul(GRAPH_WALK_REQUESTS_PER_FILE_PER_LEVEL)
}
