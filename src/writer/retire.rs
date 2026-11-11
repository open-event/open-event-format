use super::build::{BuiltOe, OeBuildConfig};
use super::constant::{AUDIT_COMPACTION_REFUSED, LINEAR_LOOKUP_MAX_ENTRIES};
use super::publish::{MAX_REBASE_ATTEMPTS, OePublisher, PeerNotices, PublishFailure, Published};
use super::upload::upload_oe;
use crate::clock::Clock;
use crate::deletion_store::StoredDeletionVector;
use crate::error::PublishError;
use crate::events::{SequenceRange, TenantId};
use crate::field_deletion_store::StoredFieldDeletionVector;
use crate::invariants::PublishedSet;
use crate::layout::{OeHeader, Profile, decode_header};
use crate::lifecycle::{FileType, GraphFileSummary, ManifestGeneration, OeFileEntry, PartState, Retirement};
use crate::object_store::{ObjectStore, constant::MIN_MULTIPART_PART_BYTES, oe_object_key};
use hashbrown::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepPolicy {
    pub in_flight_query_horizon_nanos: u64,
    pub safety_window_nanos: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub deleted: Vec<u128>,
    pub generation: Option<u64>,
    pub unreferenced: Vec<u128>,
}

impl OePublisher {
    #[expect(
        clippy::too_many_arguments,
        reason = "the compaction publish wires every interface exactly once"
    )]
    pub fn publish_compaction(
        &mut self,
        built: BuiltOe,
        config: &OeBuildConfig,
        inputs: &[u128],
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        notices: &mut dyn PeerNotices,
        clock: &dyn Clock,
    ) -> Result<Published, PublishFailure> {
        if built.footer.profile == Profile::Audit {
            return Err(PublishFailure::Verification(AUDIT_COMPACTION_REFUSED));
        }
        let (_, head) = published.head().map_err(PublishFailure::Publish)?;
        let coverage = merged_coverage(&head, inputs, config.tenant_id)?;
        self.verify_staged(&built, config, &coverage)?;
        let merged = inputs
            .iter()
            .map(|input| MergedInput::fetch(&head, *input, config.tenant_id, objects))
            .collect::<Result<Vec<_>, _>>()?;
        self.publish_merged_onto(built, config, &head, &merged, published, objects, notices, clock)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the compaction publish wires every interface exactly once"
    )]
    pub(super) fn publish_merged(
        &mut self,
        built: BuiltOe,
        config: &OeBuildConfig,
        inputs: &[MergedInput],
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        notices: &mut dyn PeerNotices,
        clock: &dyn Clock,
    ) -> Result<Published, PublishFailure> {
        let (_, head) = published.head().map_err(PublishFailure::Publish)?;
        self.publish_merged_onto(built, config, &head, inputs, published, objects, notices, clock)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the compaction publish wires every interface exactly once"
    )]
    fn publish_merged_onto(
        &mut self,
        built: BuiltOe,
        config: &OeBuildConfig,
        head: &ManifestGeneration,
        inputs: &[MergedInput],
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        notices: &mut dyn PeerNotices,
        clock: &dyn Clock,
    ) -> Result<Published, PublishFailure> {
        if built.footer.profile == Profile::Audit {
            return Err(PublishFailure::Verification(AUDIT_COMPACTION_REFUSED));
        }
        let file_ids: Vec<u128> = inputs.iter().map(|input| input.file_id).collect();
        let coverage = merged_coverage(head, &file_ids, config.tenant_id)?;
        self.verify_staged(&built, config, &coverage)?;
        verify_rows_preserved(&built, inputs, config.tenant_id)?;
        let tenant_id = config.tenant_id;
        publish_in_place_of(
            built,
            config,
            coverage,
            |head| {
                deletions_unchanged(head, inputs, tenant_id)?;
                Ok(file_ids.clone())
            },
            published,
            objects,
            notices,
            clock,
        )
    }

    pub fn publish_snapshot(
        &mut self,
        built: BuiltOe,
        config: &OeBuildConfig,
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        notices: &mut dyn PeerNotices,
        clock: &dyn Clock,
    ) -> Result<Published, PublishFailure> {
        if built.footer.profile != Profile::Snapshot {
            return Err(PublishFailure::Verification("not a snapshot file"));
        }
        let coverage = SequenceRange {
            epoch: built.header.min_epoch,
            first_sequence: built.header.min_sequence,
            last_sequence: built.header.max_sequence,
        };
        self.verify_staged(&built, config, &coverage)?;
        let tenant_id = config.tenant_id;
        let replaced = |head: &ManifestGeneration| {
            if head
                .files
                .iter()
                .any(|entry| entry.profile != Profile::Snapshot || entry.tenant_id != tenant_id)
            {
                return Err(PublishFailure::Verification(
                    "a snapshot catalogue holds only that snapshot's files",
                ));
            }
            let active: Vec<&OeFileEntry> = head
                .files
                .iter()
                .filter(|entry| entry.part_state == PartState::Active)
                .collect();
            if active.iter().any(|entry| entry.coverage.epoch >= coverage.epoch) {
                return Err(PublishFailure::Verification(
                    "a snapshot replaces only an earlier epoch",
                ));
            }
            Ok(active.iter().map(|entry| entry.file_id).collect())
        };
        publish_in_place_of(built, config, coverage, replaced, published, objects, notices, clock)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the replacing publish wires every interface exactly once"
)]
fn publish_in_place_of(
    built: BuiltOe,
    config: &OeBuildConfig,
    coverage: SequenceRange,
    replaced: impl Fn(&ManifestGeneration) -> Result<Vec<u128>, PublishFailure>,
    published: &mut dyn PublishedSet,
    objects: &dyn ObjectStore,
    notices: &mut dyn PeerNotices,
    clock: &dyn Clock,
) -> Result<Published, PublishFailure> {
    upload_oe(
        objects,
        &oe_object_key(config.tenant_id, built.file_id),
        &built,
        MIN_MULTIPART_PART_BYTES,
    )?;
    let output = output_entry(&built, config, coverage);

    let mut head_id = 0;
    for _ in 0..=MAX_REBASE_ATTEMPTS {
        let (current_id, head) = published.head().map_err(PublishFailure::Publish)?;
        head_id = current_id;
        if head.files.contains(&output) {
            return Ok(Published {
                entry: output,
                file_bytes: built.bytes,
                generation: head_id,
            });
        }
        let inputs = replaced(&head)?;
        let next_id = head_id + 1;
        let now = clock.now_nanos();
        let mut next = ManifestGeneration {
            footer_mirror: None,
            generation: next_id,
            ..head
        };
        for input in &inputs {
            let entry = next
                .files
                .iter_mut()
                .find(|entry| entry.file_id == *input && entry.part_state == PartState::Active)
                .ok_or(PublishFailure::Verification(
                    "a compaction input is no longer an active file",
                ))?;
            debug_assert!(entry.part_state.can_transition_to(PartState::Outdated));
            entry.part_state = PartState::Outdated;
            next.retirements.push(Retirement {
                file_id: *input,
                generation: next_id,
                since_nanos: now,
            });
        }
        next.files.push(output.clone());
        if commit(published, head_id, next)? {
            notices.oe_published(&output);
            return Ok(Published {
                entry: output,
                file_bytes: built.bytes,
                generation: next_id,
            });
        }
    }
    Err(PublishFailure::Publish(PublishError::CasLost {
        current_generation: head_id,
    }))
}

fn output_entry(built: &BuiltOe, config: &OeBuildConfig, coverage: SequenceRange) -> OeFileEntry {
    OeFileEntry {
        coverage,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: built.file_seal,
        file_id: built.file_id,
        file_type: FileType::OeFile,
        footer_len: Some(built.footer_len),
        entity_summary: Some(built.entity_summary.clone()),
        graph_event_id_filter: built
            .footer
            .graph
            .as_ref()
            .and_then(|graph| graph.event_ids.as_ref())
            .map(|index| index.filter.clone()),
        graph_summary: GraphFileSummary::of(&built.footer),
        deletion_vector: None,
        optional_feature_flags: built.footer.optional_feature_flags,
        part_index: 0,
        part_state: PartState::Active,
        profile: built.footer.profile,
        required_feature_flags: built.footer.required_feature_flags,
        size_bytes: built.bytes.len() as u64,
        tenant_id: config.tenant_id,
        tree_len: built.tree_len,
    }
}

pub struct GraphRewriteOutput {
    pub built: BuiltOe,
    pub inputs: Vec<u128>,
}

pub(super) struct MergedGraphOutput {
    pub built: BuiltOe,
    pub inputs: Vec<MergedInput>,
}

impl OePublisher {
    pub fn publish_graph_rewrite(
        &mut self,
        outputs: Vec<GraphRewriteOutput>,
        config: &OeBuildConfig,
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        notices: &mut dyn PeerNotices,
        clock: &dyn Clock,
    ) -> Result<Vec<Published>, PublishFailure> {
        if outputs.is_empty() {
            return Err(PublishFailure::Verification(
                "a graph rewrite needs at least one output",
            ));
        }
        let (_, head) = published.head().map_err(PublishFailure::Publish)?;
        let mut merged = Vec::with_capacity(outputs.len());
        for output in outputs {
            if output.built.footer.profile != Profile::Graph {
                return Err(PublishFailure::Verification(
                    "a graph rewrite publishes graph files only",
                ));
            }
            let coverage = merged_coverage(&head, &output.inputs, config.tenant_id)?;
            self.verify_staged(&output.built, config, &coverage)?;
            let inputs = output
                .inputs
                .iter()
                .map(|input| MergedInput::fetch(&head, *input, config.tenant_id, objects))
                .collect::<Result<Vec<_>, _>>()?;
            verify_rows_preserved(&output.built, &inputs, config.tenant_id)?;
            merged.push(MergedGraphOutput {
                built: output.built,
                inputs,
            });
        }
        self.publish_graph_rewrite_onto(merged, config, &head, published, objects, notices, clock)
    }

    pub(super) fn publish_merged_graph_rewrite(
        &mut self,
        outputs: Vec<MergedGraphOutput>,
        config: &OeBuildConfig,
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        notices: &mut dyn PeerNotices,
        clock: &dyn Clock,
    ) -> Result<Vec<Published>, PublishFailure> {
        let (_, head) = published.head().map_err(PublishFailure::Publish)?;
        self.publish_graph_rewrite_onto(outputs, config, &head, published, objects, notices, clock)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the graph rewrite publish wires every interface exactly once"
    )]
    fn publish_graph_rewrite_onto(
        &mut self,
        outputs: Vec<MergedGraphOutput>,
        config: &OeBuildConfig,
        head: &ManifestGeneration,
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        notices: &mut dyn PeerNotices,
        clock: &dyn Clock,
    ) -> Result<Vec<Published>, PublishFailure> {
        if outputs.is_empty() {
            return Err(PublishFailure::Verification(
                "a graph rewrite needs at least one output",
            ));
        }
        let mut staged = Vec::with_capacity(outputs.len());
        for output in outputs {
            if output.built.footer.profile != Profile::Graph {
                return Err(PublishFailure::Verification(
                    "a graph rewrite publishes graph files only",
                ));
            }
            let file_ids: Vec<u128> = output.inputs.iter().map(|input| input.file_id).collect();
            let coverage = merged_coverage(head, &file_ids, config.tenant_id)?;
            self.verify_staged(&output.built, config, &coverage)?;
            verify_rows_preserved(&output.built, &output.inputs, config.tenant_id)?;
            staged.push((output, coverage));
        }
        let mut entries = Vec::with_capacity(staged.len());
        for (output, coverage) in &staged {
            upload_oe(
                objects,
                &oe_object_key(config.tenant_id, output.built.file_id),
                &output.built,
                MIN_MULTIPART_PART_BYTES,
            )?;
            entries.push(output_entry(&output.built, config, *coverage));
        }
        let finish = |generation: u64, staged: Vec<(MergedGraphOutput, SequenceRange)>, entries: Vec<OeFileEntry>| {
            staged
                .into_iter()
                .zip(entries)
                .map(|((output, _), entry)| Published {
                    entry,
                    file_bytes: output.built.bytes,
                    generation,
                })
                .collect()
        };
        let mut head_id = 0;
        for _ in 0..=MAX_REBASE_ATTEMPTS {
            let (current_id, head) = published.head().map_err(PublishFailure::Publish)?;
            head_id = current_id;
            if entries.iter().all(|entry| head.files.contains(entry)) {
                return Ok(finish(head_id, staged, entries));
            }
            for (output, _) in &staged {
                deletions_unchanged(&head, &output.inputs, config.tenant_id)?;
            }
            let next_id = head_id + 1;
            let now = clock.now_nanos();
            let mut next = ManifestGeneration {
                footer_mirror: None,
                generation: next_id,
                ..head
            };
            for input in staged.iter().flat_map(|(output, _)| &output.inputs) {
                let entry = next
                    .files
                    .iter_mut()
                    .find(|entry| entry.file_id == input.file_id && entry.part_state == PartState::Active)
                    .ok_or(PublishFailure::Verification(
                        "a compaction input is no longer an active file",
                    ))?;
                debug_assert!(entry.part_state.can_transition_to(PartState::Outdated));
                entry.part_state = PartState::Outdated;
                next.retirements.push(Retirement {
                    file_id: input.file_id,
                    generation: next_id,
                    since_nanos: now,
                });
            }
            next.files.extend(entries.iter().cloned());
            if commit(published, head_id, next)? {
                for entry in &entries {
                    notices.oe_published(entry);
                }
                return Ok(finish(next_id, staged, entries));
            }
        }
        Err(PublishFailure::Publish(PublishError::CasLost {
            current_generation: head_id,
        }))
    }
}

pub fn retire_acked_inbox_files(
    published: &mut dyn PublishedSet,
    file_ids: &[u128],
    clock: &dyn Clock,
) -> Result<Option<u64>, PublishFailure> {
    let mut head_id = 0;
    for _ in 0..=MAX_REBASE_ATTEMPTS {
        let (current_id, head) = published.head().map_err(PublishFailure::Publish)?;
        head_id = current_id;
        let next_id = head_id + 1;
        let now = clock.now_nanos();
        let mut next = ManifestGeneration {
            footer_mirror: None,
            generation: next_id,
            ..head
        };
        let mut retired = Vec::new();
        for entry in next.files.iter_mut().filter(|entry| file_ids.contains(&entry.file_id)) {
            if entry.profile != Profile::Inbox {
                return Err(PublishFailure::Verification(
                    "only inbox files retire by acknowledgement",
                ));
            }
            if entry.part_state != PartState::Active {
                continue;
            }
            debug_assert!(entry.part_state.can_transition_to(PartState::Outdated));
            entry.part_state = PartState::Outdated;
            retired.push(Retirement {
                file_id: entry.file_id,
                generation: next_id,
                since_nanos: now,
            });
        }
        if retired.is_empty() {
            return Ok(None);
        }
        next.retirements.extend(retired);
        if commit(published, head_id, next)? {
            return Ok(Some(next_id));
        }
    }
    Err(PublishFailure::Publish(PublishError::CasLost {
        current_generation: head_id,
    }))
}

pub fn sweep_retired_files(
    published: &mut dyn PublishedSet,
    objects: &dyn ObjectStore,
    policy: &SweepPolicy,
    oldest_live_snapshot: u64,
    clock: &dyn Clock,
) -> Result<SweepReport, PublishFailure> {
    let safety_window = policy.safety_window_nanos.max(policy.in_flight_query_horizon_nanos);
    let mut head_id = 0;
    for _ in 0..=MAX_REBASE_ATTEMPTS {
        let (current_id, head) = published.head().map_err(PublishFailure::Publish)?;
        head_id = current_id;
        let next_id = head_id + 1;
        let now = clock.now_nanos();
        let mut next = ManifestGeneration {
            footer_mirror: None,
            generation: next_id,
            ..head
        };
        let mut report = SweepReport::default();
        let mut started_clocks = Vec::new();
        let mut retirement_positions: Option<HashMap<u128, usize>> = None;
        for entry in &mut next.files {
            if !matches!(entry.part_state, PartState::Outdated | PartState::DeleteOnDestroy) {
                continue;
            }
            let position = if next.retirements.len() <= LINEAR_LOOKUP_MAX_ENTRIES {
                next.retirements
                    .iter()
                    .position(|retirement| retirement.file_id == entry.file_id)
            } else {
                retirement_positions
                    .get_or_insert_with(|| {
                        let mut positions = HashMap::with_capacity(next.retirements.len());
                        for (position, retirement) in next.retirements.iter().enumerate() {
                            positions.entry(retirement.file_id).or_insert(position);
                        }
                        positions
                    })
                    .get(&entry.file_id)
                    .copied()
            };
            let Some(retirement) = position.and_then(|position| next.retirements.get_mut(position)) else {
                started_clocks.push(Retirement {
                    file_id: entry.file_id,
                    generation: next_id,
                    since_nanos: now,
                });
                continue;
            };
            let waited = u64::try_from(now.saturating_sub(retirement.since_nanos)).unwrap_or(0);
            match entry.part_state {
                PartState::Outdated
                    if retirement.generation <= oldest_live_snapshot
                        && waited >= policy.in_flight_query_horizon_nanos =>
                {
                    debug_assert!(entry.part_state.can_transition_to(PartState::DeleteOnDestroy));
                    entry.part_state = PartState::DeleteOnDestroy;
                    *retirement = Retirement {
                        file_id: entry.file_id,
                        generation: next_id,
                        since_nanos: now,
                    };
                    report.unreferenced.push(entry.file_id);
                }
                PartState::DeleteOnDestroy if waited >= safety_window => report.deleted.push(entry.file_id),
                _ => {}
            }
        }
        if started_clocks.is_empty() && report.unreferenced.is_empty() && report.deleted.is_empty() {
            return Ok(report);
        }
        next.retirements.extend(started_clocks);

        let still_readable: HashSet<u128> = next
            .files
            .iter()
            .filter(|entry| matches!(entry.part_state, PartState::Active | PartState::Outdated))
            .map(|entry| entry.file_id)
            .collect();
        report.deleted.retain(|file_id| !still_readable.contains(file_id));
        if !report.deleted.is_empty() {
            let deleted_set: Option<HashSet<u128>> =
                (report.deleted.len() > LINEAR_LOOKUP_MAX_ENTRIES).then(|| report.deleted.iter().copied().collect());
            let is_deleted = |file_id: &u128| match &deleted_set {
                Some(set) => set.contains(file_id),
                None => report.deleted.contains(file_id),
            };
            for entry in next.files.iter().filter(|entry| is_deleted(&entry.file_id)) {
                debug_assert!(entry.part_state.can_transition_to(PartState::Deleted));
                objects
                    .delete(&oe_object_key(entry.tenant_id, entry.file_id))
                    .map_err(PublishFailure::Storage)?;
            }
            next.files.retain(|entry| !is_deleted(&entry.file_id));
            next.retirements.retain(|retirement| !is_deleted(&retirement.file_id));
        }

        if commit(published, head_id, next)? {
            report.generation = Some(next_id);
            return Ok(report);
        }
    }
    Err(PublishFailure::Publish(PublishError::CasLost {
        current_generation: head_id,
    }))
}

fn merged_coverage(
    head: &ManifestGeneration,
    inputs: &[u128],
    tenant_id: TenantId,
) -> Result<SequenceRange, PublishFailure> {
    let mut ranges = inputs
        .iter()
        .map(|input| {
            head.files
                .iter()
                .find(|entry| {
                    entry.file_id == *input && entry.tenant_id == tenant_id && entry.part_state == PartState::Active
                })
                .ok_or(PublishFailure::Verification(
                    "a compaction input is not an active file of this tenant",
                ))
                .and_then(|entry| match entry.profile {
                    Profile::Audit => Err(PublishFailure::Verification(AUDIT_COMPACTION_REFUSED)),
                    _ => Ok(entry.coverage),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    ranges.sort();
    let (first, rest) = ranges
        .split_first()
        .ok_or(PublishFailure::Verification("a compaction needs at least one input"))?;
    let mut merged = *first;
    for range in rest {
        if range.epoch != merged.epoch || merged.last_sequence.checked_add(1) != Some(range.first_sequence) {
            return Err(PublishFailure::Verification(
                "compaction inputs do not cover one contiguous range",
            ));
        }
        merged.last_sequence = range.last_sequence;
    }
    Ok(merged)
}

#[derive(Debug, Clone)]
pub(super) struct MergedInput {
    pub deletion_vector: Option<StoredDeletionVector>,
    pub field_deletion_vectors: Vec<StoredFieldDeletionVector>,
    pub file_id: u128,
    pub header: OeHeader,
}

impl MergedInput {
    pub(super) fn read(entry: &OeFileEntry, header: &OeHeader) -> Self {
        Self {
            deletion_vector: entry.deletion_vector.clone(),
            field_deletion_vectors: entry.field_deletion_vectors.clone(),
            file_id: entry.file_id,
            header: header.clone(),
        }
    }

    fn fetch(
        head: &ManifestGeneration,
        file_id: u128,
        tenant_id: TenantId,
        objects: &dyn ObjectStore,
    ) -> Result<Self, PublishFailure> {
        let stored = objects
            .get(&oe_object_key(tenant_id, file_id))
            .map_err(PublishFailure::Storage)?
            .ok_or(PublishFailure::Verification(
                "a compaction input's stored file is missing",
            ))?;
        let header = decode_header(&stored.bytes).map_err(PublishFailure::Format)?;
        let entry = head
            .files
            .iter()
            .find(|entry| entry.file_id == file_id && entry.tenant_id == tenant_id);
        Ok(Self {
            deletion_vector: entry.and_then(|entry| entry.deletion_vector.clone()),
            field_deletion_vectors: entry.map_or_else(Vec::new, |entry| entry.field_deletion_vectors.clone()),
            file_id,
            header,
        })
    }
}

fn deletions_unchanged(
    head: &ManifestGeneration,
    inputs: &[MergedInput],
    tenant_id: TenantId,
) -> Result<(), PublishFailure> {
    for input in inputs {
        let entry = head
            .files
            .iter()
            .find(|entry| entry.file_id == input.file_id && entry.tenant_id == tenant_id)
            .ok_or(PublishFailure::Verification(
                "a compaction input is no longer an active file",
            ))?;
        if entry.deletion_vector != input.deletion_vector
            || entry.field_deletion_vectors != input.field_deletion_vectors
        {
            return Err(PublishFailure::Verification(
                "a compaction input's deleted rows or redacted fields changed after the merge read them",
            ));
        }
    }
    Ok(())
}

fn verify_rows_preserved(built: &BuiltOe, inputs: &[MergedInput], tenant_id: TenantId) -> Result<(), PublishFailure> {
    let mut row_count = 0u64;
    let mut any_deleted = false;
    let mut bounds: Option<((u64, u64), (u64, u64))> = None;
    for input in inputs {
        let header = &input.header;
        if header.file_id != input.file_id || header.tenant_id != tenant_id {
            return Err(PublishFailure::Verification(
                "a compaction input's stored file is a different file",
            ));
        }
        let first = (header.min_epoch, header.min_sequence);
        let last = (header.max_epoch, header.max_sequence);
        row_count = row_count.saturating_add(header.row_count);
        let deleted = input.deletion_vector.as_ref().map_or(0, |vector| vector.deleted_count);
        row_count = row_count.saturating_sub(deleted);
        any_deleted |= deleted > 0;
        bounds = Some(bounds.map_or((first, last), |(low, high)| (low.min(first), high.max(last))));
    }
    let output_bounds = (
        (built.header.min_epoch, built.header.min_sequence),
        (built.header.max_epoch, built.header.max_sequence),
    );
    let bounds_hold = match bounds {
        Some(bounds) if any_deleted => {
            built.header.row_count == 0 || (output_bounds.0 >= bounds.0 && output_bounds.1 <= bounds.1)
        }
        bounds => bounds == Some(output_bounds),
    };
    if built.header.row_count != row_count || !bounds_hold {
        return Err(PublishFailure::Verification(
            "the compaction output does not hold every row of its inputs",
        ));
    }
    Ok(())
}

fn commit(published: &mut dyn PublishedSet, head_id: u64, next: ManifestGeneration) -> Result<bool, PublishFailure> {
    let next_id = next.generation;
    match published.put_generation(next) {
        Ok(()) => {}
        Err(PublishError::GenerationExists) => {
            return match published.advance_head(head_id, next_id) {
                Ok(()) | Err(PublishError::CasLost { .. }) => Ok(false),
                Err(error) => Err(PublishFailure::Publish(error)),
            };
        }
        Err(error) => return Err(PublishFailure::Publish(error)),
    }
    match published.advance_head(head_id, next_id) {
        Ok(()) => Ok(true),
        Err(PublishError::CasLost { .. }) => Ok(false),
        Err(error) => Err(PublishFailure::Publish(error)),
    }
}

#[cfg(test)]
#[path = "test/retire.rs"]
mod tests;
