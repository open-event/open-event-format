use super::events::{SequenceRange, TenantId};
use super::layout::Profile;
use super::layout::footer::Footer;
use super::layout::graph::{ChainRange, DepthRange, event_id_filter_may_hold};
use hashbrown::HashMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum FileType {
    DerivedColumns,
    EntityProjection,
    GenerationPart,
    #[default]
    OeFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum PartState {
    Active,
    DeleteOnDestroy,
    Deleted,
    OpenTmp,
    Outdated,
    Sealed,
}

impl PartState {
    fn rank(self) -> u8 {
        match self {
            PartState::OpenTmp => 0,
            PartState::Sealed => 1,
            PartState::Active => 2,
            PartState::Outdated => 3,
            PartState::DeleteOnDestroy => 4,
            PartState::Deleted => 5,
        }
    }

    pub fn can_transition_to(self, next: PartState) -> bool {
        matches!(
            (self, next),
            (PartState::OpenTmp, PartState::Sealed)
                | (PartState::Sealed, PartState::Active)
                | (PartState::Active, PartState::Outdated)
                | (PartState::Outdated, PartState::DeleteOnDestroy)
                | (PartState::DeleteOnDestroy, PartState::Deleted)
        )
    }

    pub fn selectable_for_new_snapshots(self) -> bool {
        matches!(self, PartState::Active)
    }
}

impl Ord for PartState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl PartialOrd for PartState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OeFileEntry {
    pub coverage: SequenceRange,
    #[serde(default)]
    pub deletion_vector: Option<crate::deletion_store::StoredDeletionVector>,
    #[serde(default)]
    pub entity_summary: Option<crate::indexes::entity_summary::EntitySummary>,
    pub feature_metadata: Option<Vec<u8>>,
    #[serde(default)]
    pub field_deletion_vectors: Vec<crate::field_deletion_store::StoredFieldDeletionVector>,
    pub file_seal: [u8; 32],
    pub file_id: u128,
    pub file_type: FileType,
    pub footer_len: Option<u64>,
    #[serde(default)]
    pub graph_event_id_filter: Option<Vec<u8>>,
    #[serde(default)]
    pub graph_summary: Option<GraphFileSummary>,
    pub optional_feature_flags: u64,
    pub part_index: u32,
    pub part_state: PartState,
    #[serde(default)]
    pub profile: Profile,
    pub required_feature_flags: u64,
    pub size_bytes: u64,
    pub tenant_id: TenantId,
    pub tree_len: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GraphFileSummary {
    pub chains: Vec<ChainRange>,
    pub depth: Option<DepthRange>,
    pub max_event_sn: u64,
    pub min_event_sn: u64,
    pub subject: Vec<u8>,
}

impl GraphFileSummary {
    pub(crate) fn of(footer: &Footer) -> Option<Self> {
        let graph = footer.graph.as_ref()?;
        Some(Self {
            chains: graph
                .chain_cover
                .as_ref()
                .map(|cover| cover.chains.clone())
                .unwrap_or_default(),
            depth: graph.depth,
            max_event_sn: graph
                .first_sn
                .saturating_add(footer.exact_counts.row_count.saturating_sub(1)),
            min_event_sn: graph.first_sn,
            subject: graph.subject.clone(),
        })
    }
}

impl OeFileEntry {
    pub fn tail_range(&self) -> crate::layout::reader::TailRange {
        crate::layout::reader::tail_range(self.size_bytes, self.footer_len, self.tree_len)
    }

    pub fn may_hold_graph_event(&self, event_id: &[u8]) -> bool {
        self.graph_event_id_filter
            .as_deref()
            .is_none_or(|filter| event_id_filter_may_hold(filter, event_id))
    }

    pub fn feature_metadata_placement_valid(&self) -> bool {
        match self.feature_metadata {
            None => true,
            Some(_) => matches!(self.file_type, FileType::GenerationPart) && self.part_index == 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionPolicy {
    pub max_jobs_per_cycle: usize,
    pub max_merge_width: usize,
    pub min_output_over_largest: u64,
    pub roll_target_bytes: u64,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            max_jobs_per_cycle: 4,
            max_merge_width: 8,
            min_output_over_largest: 2,
            roll_target_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionJob {
    pub expected_output_bytes: u64,
    pub file_ids: Vec<u128>,
}

pub fn plan_compaction_cycle(
    policy: &CompactionPolicy,
    generation: &ManifestGeneration,
    tenant_id: TenantId,
) -> Vec<CompactionJob> {
    if policy.max_merge_width < 2 || policy.max_jobs_per_cycle == 0 {
        return Vec::new();
    }
    let mut candidates: Vec<&OeFileEntry> = generation
        .files
        .iter()
        .filter(|entry| {
            entry.tenant_id == tenant_id
                && entry.part_state == PartState::Active
                && entry.file_type == FileType::OeFile
                && !matches!(entry.profile, Profile::Audit | Profile::Graph)
        })
        .collect();
    candidates.sort_by_key(|entry| (entry.coverage.epoch, entry.coverage.first_sequence));
    let follows_on = |last: &OeFileEntry, next: &OeFileEntry| {
        last.coverage.epoch == next.coverage.epoch
            && last.profile == next.profile
            && last.coverage.last_sequence.checked_add(1) == Some(next.coverage.first_sequence)
    };

    let mut jobs = Vec::new();
    let mut run_start = 0usize;
    for boundary in 0..=candidates.len() {
        let small = candidates
            .get(boundary)
            .copied()
            .filter(|entry| entry.size_bytes < policy.roll_target_bytes);
        let previous = boundary.checked_sub(1).filter(|&previous| previous >= run_start);
        if let Some(next) = small
            && previous
                .and_then(|previous| candidates.get(previous))
                .is_none_or(|last| follows_on(last, next))
        {
            continue;
        }
        let run = candidates.get(run_start..boundary).unwrap_or_default();
        let mut start = 0usize;
        while start + 2 <= run.len() {
            let window = run
                .get(start..run.len().min(start + policy.max_merge_width))
                .unwrap_or_default();
            let total: u64 = window.iter().map(|entry| entry.size_bytes).sum();
            let largest = window.iter().map(|entry| entry.size_bytes).max().unwrap_or(0);
            let qualifies =
                total >= largest.saturating_mul(policy.min_output_over_largest) || total >= policy.roll_target_bytes;
            if qualifies {
                jobs.push(CompactionJob {
                    expected_output_bytes: total,
                    file_ids: window.iter().map(|entry| entry.file_id).collect(),
                });
                if jobs.len() == policy.max_jobs_per_cycle {
                    return jobs;
                }
                start += window.len();
            } else {
                start += 1;
            }
        }
        run_start = if small.is_some() { boundary } else { boundary + 1 };
    }
    jobs
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphCompactionPolicy {
    pub finished_bytes: u64,
    pub max_files: usize,
    pub max_output_bytes: u64,
    pub small_total_bytes: u64,
}

impl Default for GraphCompactionPolicy {
    fn default() -> Self {
        Self {
            finished_bytes: 64 * 1024 * 1024,
            max_files: 8,
            max_output_bytes: 256 * 1024 * 1024,
            small_total_bytes: 8 * 1024 * 1024,
        }
    }
}

pub fn plan_graph_compaction_cycle(
    policy: &GraphCompactionPolicy,
    generation: &ManifestGeneration,
    tenant_id: TenantId,
) -> Vec<CompactionJob> {
    let mut candidates: Vec<(&OeFileEntry, &GraphFileSummary)> = generation
        .files
        .iter()
        .filter(|entry| {
            entry.tenant_id == tenant_id
                && entry.part_state == PartState::Active
                && entry.file_type == FileType::OeFile
                && entry.size_bytes < policy.finished_bytes
        })
        .filter_map(|entry| Some((entry, entry.graph_summary.as_ref()?)))
        .collect();
    candidates.sort_by(|(_, a), (_, b)| (&a.subject, a.min_event_sn).cmp(&(&b.subject, b.min_event_sn)));

    let mut jobs = Vec::new();
    let follows_on = |(_, a): &(&OeFileEntry, &GraphFileSummary), (_, b): &(&OeFileEntry, &GraphFileSummary)| {
        a.subject == b.subject && a.max_event_sn.checked_add(1) == Some(b.min_event_sn)
    };
    for run in candidates.chunk_by(follows_on) {
        let total: u64 = run.iter().map(|(entry, _)| entry.size_bytes).sum();
        if run.len() < 2 || (run.len() <= policy.max_files && total >= policy.small_total_bytes) {
            continue;
        }
        let mut job = CompactionJob {
            expected_output_bytes: 0,
            file_ids: Vec::new(),
        };
        for (entry, _) in run {
            if !job.file_ids.is_empty() && job.expected_output_bytes + entry.size_bytes > policy.max_output_bytes {
                jobs.push(job);
                job = CompactionJob {
                    expected_output_bytes: 0,
                    file_ids: Vec::new(),
                };
            }
            job.expected_output_bytes += entry.size_bytes;
            job.file_ids.push(entry.file_id);
        }
        jobs.push(job);
    }
    jobs.retain(|job| job.file_ids.len() >= 2);
    jobs
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct ManifestGeneration {
    pub files: Vec<OeFileEntry>,
    pub footer_mirror: Option<FooterMirrorObject>,
    pub generation: u64,
    pub index_artifacts: Vec<IndexArtifactRef>,
    pub retirements: Vec<Retirement>,
    #[serde(default)]
    pub vector_segments: Vec<crate::vector::VectorSegmentEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Retirement {
    pub file_id: u128,
    pub generation: u64,
    pub since_nanos: i64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FooterMirrorObject {
    pub blake3: [u8; 32],
    pub path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexArtifactRef {
    pub artifact_blake3: [u8; 32],
    pub column_id: u32,
    pub covered_file_id: u128,
    pub kind: crate::indexes::artifact::ArtifactKind,
    pub object_key: String,
}

pub fn retired_artifact_keys(previous: &ManifestGeneration, next: &ManifestGeneration) -> Vec<String> {
    let kept: hashbrown::HashSet<&str> = next
        .index_artifacts
        .iter()
        .map(|kept| kept.object_key.as_str())
        .collect();
    previous
        .index_artifacts
        .iter()
        .filter(|artifact| !kept.contains(artifact.object_key.as_str()))
        .map(|artifact| artifact.object_key.clone())
        .collect()
}

impl ManifestGeneration {
    pub fn snapshot_files(&self) -> impl Iterator<Item = &OeFileEntry> {
        self.files
            .iter()
            .filter(|entry| entry.part_state.selectable_for_new_snapshots())
    }

    pub fn covers(&self, range: &SequenceRange, tenant_id: TenantId) -> bool {
        self.files.iter().any(|entry| {
            matches!(
                entry.part_state,
                PartState::Active | PartState::Outdated | PartState::DeleteOnDestroy
            ) && entry.tenant_id == tenant_id
                && entry.coverage.contains(range)
        })
    }

    pub fn coverage_index(&self) -> CoverageIndex {
        let mut grouped: HashMap<(TenantId, u64), Vec<(u64, u64)>> = HashMap::default();
        for entry in &self.files {
            if matches!(
                entry.part_state,
                PartState::Active | PartState::Outdated | PartState::DeleteOnDestroy
            ) {
                grouped
                    .entry((entry.tenant_id, entry.coverage.epoch))
                    .or_default()
                    .push((entry.coverage.first_sequence, entry.coverage.last_sequence));
            }
        }
        let by_tenant_epoch = grouped
            .into_iter()
            .map(|(key, mut ranges)| {
                ranges.sort_unstable_by_key(|&(first, _)| first);
                let mut firsts = Vec::with_capacity(ranges.len());
                let mut running_max_last = Vec::with_capacity(ranges.len());
                let mut max_last = 0u64;
                for (first, last) in ranges {
                    firsts.push(first);
                    max_last = max_last.max(last);
                    running_max_last.push(max_last);
                }
                (key, (firsts, running_max_last))
            })
            .collect();
        CoverageIndex { by_tenant_epoch }
    }
}

#[derive(Debug, Default)]
pub struct CoverageIndex {
    by_tenant_epoch: HashMap<(TenantId, u64), (Vec<u64>, Vec<u64>)>,
}

impl CoverageIndex {
    pub fn covers(&self, range: &SequenceRange, tenant_id: TenantId) -> bool {
        let Some((firsts, running_max_last)) = self.by_tenant_epoch.get(&(tenant_id, range.epoch)) else {
            return false;
        };
        let idx = firsts.partition_point(|&first| first <= range.first_sequence);
        idx > 0
            && running_max_last
                .get(idx - 1)
                .is_some_and(|&max_last| max_last >= range.last_sequence)
    }
}

#[cfg(test)]
#[path = "test/lifecycle.rs"]
mod tests;
