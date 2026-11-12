use super::build::{BuiltOe, OeBuildConfig, OeRow, build_oe_file_with_executor};
use super::reserve::arbitrate_coverage;
use super::upload::upload_oe;
use crate::artifacts::batch::{decode_batch, envelope_of};
use crate::artifacts::frame::decode_frame;
use crate::artifacts::segment::{ReplayOutcome, ReplayedFrame, SegmentDescriptor, replay_segment, verify_chain_anchor};
use crate::error::{FormatError, PublishError, StorageError};
use crate::events::{SequenceRange, TenantId};
use crate::invariants::{EncodeExecutor, JournalStorage, MonotonicClock, PublishedSet, ShardId};
use crate::layout::{MAX_PAGE_BYTES, required_features};
use crate::lifecycle::{FileType, GraphFileSummary, ManifestGeneration, OeFileEntry, PartState};
use crate::object_store::{ObjectStore, constant::MIN_MULTIPART_PART_BYTES, oe_object_key};

const BLAKE3_HASH_BYTES: usize = 32;

const DEFAULT_ROLL_BYTE_TARGET: u64 = 1 << 30;

const DEFAULT_ROLL_TIME_WINDOW_NANOS: u64 = 300 * 1_000_000_000;

pub(super) const MAX_REBASE_ATTEMPTS: u32 = 16;

pub fn oe_upload_segments(built: &BuiltOe) -> Vec<u64> {
    let total = built.bytes.len() as u64;
    let mut cuts: Vec<u64> = built
        .footer
        .stripes
        .iter()
        .map(|stripe| stripe.file_offset)
        .filter(|&offset| offset > 0 && offset < total)
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    cuts.push(total);

    let mut segments = Vec::with_capacity(cuts.len());
    let mut previous = 0u64;
    for cut in cuts {
        if cut > previous {
            segments.push(cut - previous);
            previous = cut;
        }
    }
    segments
}

#[derive(Debug, Clone, Copy)]
pub struct RollPolicy {
    pub byte_target: u64,
    pub max_open_nanos: u64,
}

impl Default for RollPolicy {
    fn default() -> Self {
        Self {
            byte_target: DEFAULT_ROLL_BYTE_TARGET,
            max_open_nanos: DEFAULT_ROLL_TIME_WINDOW_NANOS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollTrigger {
    ByteTarget,
    TimeWindow,
}

#[derive(Debug, Clone, Copy)]
pub struct OpenFileState {
    pub compressed_bytes: u64,
    pub opened_at_monotonic_nanos: u64,
}

pub fn should_roll(state: &OpenFileState, policy: &RollPolicy, clock: &dyn MonotonicClock) -> Option<RollTrigger> {
    if state.compressed_bytes >= policy.byte_target {
        return Some(RollTrigger::ByteTarget);
    }
    if clock.monotonic_nanos().saturating_sub(state.opened_at_monotonic_nanos) >= policy.max_open_nanos {
        return Some(RollTrigger::TimeWindow);
    }
    None
}

pub trait PeerNotices {
    fn oe_published(&mut self, entry: &OeFileEntry);
}

#[derive(Debug, Default)]
pub struct NoopPeerNotices {
    pub published: Vec<u128>,
}

impl PeerNotices for NoopPeerNotices {
    fn oe_published(&mut self, entry: &OeFileEntry) {
        self.published.push(entry.file_id);
    }
}

pub trait PublishObserver {
    fn stage(&mut self, attempt_id: u64);
    fn promote(&mut self, attempt_id: u64);
    fn rollback(&mut self, attempt_id: u64);
}

#[derive(Debug, Default)]
pub struct RecordingObserver {
    pub promoted: Vec<u64>,
    pub rolled_back: Vec<u64>,
    pub staged: Vec<u64>,
}

impl PublishObserver for RecordingObserver {
    fn stage(&mut self, attempt_id: u64) {
        self.staged.push(attempt_id);
    }
    fn promote(&mut self, attempt_id: u64) {
        self.promoted.push(attempt_id);
    }
    fn rollback(&mut self, attempt_id: u64) {
        self.rolled_back.push(attempt_id);
    }
}

#[derive(Debug)]
pub enum PublishFailure {
    Format(FormatError),
    /// An input's deletion or redaction state changed after the replacement was built from it;
    /// the replacement must be rebuilt from the current inputs.
    InputsChanged,
    Publish(PublishError),
    RangeNotDurable,
    Storage(StorageError),
    Verification(&'static str),
}

#[derive(Debug, Default)]
pub struct OePublisher {
    attempt_counter: u64,
    pub quota_bytes: Option<u64>,
}

#[derive(Debug)]
pub struct Published {
    pub entry: OeFileEntry,
    pub file_bytes: Vec<u8>,
    pub generation: u64,
}

impl OePublisher {
    pub fn new() -> Self {
        Self::default()
    }

    fn rows_for_range(
        &self,
        replay: &ReplayOutcome,
        tenant_id: TenantId,
        range: &SequenceRange,
    ) -> Result<Vec<OeRow>, PublishFailure> {
        let frames = durable_event_frames(replay, tenant_id, range)?;
        let capacity: usize = frames.iter().map(|frame| frame.header.event_count as usize).sum();
        let mut rows = Vec::with_capacity(capacity);
        for frame in frames {
            let header = &frame.header;
            let (_, payload) = decode_frame(&frame.frame_bytes).map_err(PublishFailure::Format)?;
            let mut batch = decode_batch(payload, header.event_count).map_err(PublishFailure::Format)?;
            for (row_index, event) in batch.events.iter_mut().enumerate() {
                let sequence = header.first_sequence + row_index as u64;
                if sequence < range.first_sequence || sequence > range.last_sequence {
                    continue;
                }
                let envelope = envelope_of(event, header.tenant_id);
                let payload_input = match event.payload {
                    None => crate::artifacts::batch::PayloadInput::None,
                    Some(bytes)
                        if event.variable.payload_flags & crate::artifacts::batch::PAYLOAD_FLAG_EXTERNAL_REF != 0 =>
                    {
                        let reference = simdutf8::basic::from_utf8(bytes).map_err(|_| {
                            PublishFailure::Format(FormatError::InvalidUtf8 {
                                what: "external payload reference",
                            })
                        })?;
                        crate::artifacts::batch::PayloadInput::ExternalRef(reference.to_owned())
                    }
                    // Kept encoded: the builder reads it against the batch dictionary,
                    // which clones as a pointer, so no owned value tree is built here.
                    Some(bytes) => {
                        crate::artifacts::batch::PayloadInput::Encoded(crate::artifacts::batch::EncodedPayload {
                            bytes: bytes.to_vec(),
                            dictionary: batch.dictionary.clone(),
                        })
                    }
                };
                rows.push(OeRow {
                    epoch: header.epoch,
                    sequence,
                    event: crate::artifacts::batch::EventInput {
                        envelope,
                        payload: payload_input,
                        source_schema: event.source_schema.map(str::to_owned),
                        source_delivery: event.source_delivery.map(str::to_owned),
                        connector_delivery_hash_low: event.variable.connector_delivery_hash_low,
                        connector_delivery_hash_high: event.variable.connector_delivery_hash_high,
                        provenance: event.provenance.take(),
                        relationships: event.relationships.take(),
                    },
                });
            }
        }
        Ok(rows)
    }

    pub(super) fn verify_staged(
        &self,
        built: &BuiltOe,
        config: &OeBuildConfig,
        range: &SequenceRange,
    ) -> Result<(), PublishFailure> {
        if let Some(quota) = self.quota_bytes
            && built.bytes.len() as u64 > quota
        {
            return Err(PublishFailure::Verification("tenant quota exceeded"));
        }
        if built.footer.schema_fingerprint == [0u8; BLAKE3_HASH_BYTES] {
            return Err(PublishFailure::Verification("schema fingerprint missing"));
        }
        if !crate::file::integrity::crc64_matches(&built.bytes, built.file_crc64_nvme) {
            return Err(PublishFailure::Verification("file checksum mismatch"));
        }
        if built.header.tenant_id != config.tenant_id {
            return Err(PublishFailure::Verification("tenant mismatch"));
        }
        if built.header.min_epoch != range.epoch
            || built.header.min_sequence < range.first_sequence
            || built.header.max_sequence > range.last_sequence
        {
            return Err(PublishFailure::Verification("range mismatch"));
        }
        if built.footer.required_feature_flags & required_features::ALL != required_features::ALL {
            return Err(PublishFailure::Verification("feature directory incomplete"));
        }
        let mark_oversize = built
            .footer
            .marks
            .iter()
            .any(|mark| mark.page_count <= 1 && mark.compressed_size > MAX_PAGE_BYTES);
        let page_oversize = built
            .footer
            .page_directory
            .iter()
            .any(|entry| entry.compressed_len > MAX_PAGE_BYTES);
        if mark_oversize || page_oversize {
            return Err(PublishFailure::Verification(
                "column block exceeds the maximum page size",
            ));
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the publish boundary wires every interface exactly once"
    )]
    pub fn publish_range(
        &mut self,
        storage: &dyn JournalStorage,
        shard: ShardId,
        segment_id: u64,
        segment_generation: u64,
        expected_chain_anchor: Option<[u8; BLAKE3_HASH_BYTES]>,
        range: SequenceRange,
        config: &OeBuildConfig,
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        observer: &mut dyn PublishObserver,
        notices: &mut dyn PeerNotices,
        _clock: &dyn MonotonicClock,
        encode: &dyn EncodeExecutor,
    ) -> Result<Published, PublishFailure> {
        self.attempt_counter += 1;
        let attempt_id = self.attempt_counter;

        let replay = replay_segment(storage, shard, segment_id, segment_generation).map_err(PublishFailure::Storage)?;
        if let Some(expected) = expected_chain_anchor {
            let descriptor = SegmentDescriptor {
                segment_chain_blake3: Some(expected),
                ..SegmentDescriptor::new(segment_id, segment_generation)
            };
            if !verify_chain_anchor(&descriptor, replay.segment_chain_blake3.as_ref()) {
                return Err(PublishFailure::Verification(
                    "replayed segment chain anchor does not match the recorded anchor",
                ));
            }
        }
        let rows = self.rows_for_range(&replay, config.tenant_id, &range)?;
        if rows.is_empty() {
            return Err(PublishFailure::Verification("range contains no events (all void)"));
        }

        let built = build_oe_file_with_executor(rows, config, encode).map_err(PublishFailure::Format)?;
        let mut part_state = PartState::OpenTmp;
        debug_assert!(part_state.can_transition_to(PartState::Sealed));
        part_state = PartState::Sealed;

        observer.stage(attempt_id);
        let staged_entry = OeFileEntry {
            coverage: range,
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
            part_state,
            profile: built.footer.profile,
            required_feature_flags: built.footer.required_feature_flags,
            size_bytes: built.bytes.len() as u64,
            tenant_id: config.tenant_id,
            tree_len: built.tree_len,
        };

        if let Err(failure) = self.verify_staged(&built, config, &range) {
            observer.rollback(attempt_id);
            return Err(failure);
        }
        let object_key = oe_object_key(config.tenant_id, built.file_id);
        if let Err(failure) = upload_oe(objects, &object_key, &built, MIN_MULTIPART_PART_BYTES) {
            observer.rollback(attempt_id);
            return Err(failure);
        }

        let mut rebase_attempts = 0u32;
        let mut staged_generation: Option<u64> = None;
        let generation = loop {
            let (head_id, head) = match published.head() {
                Ok(head) => head,
                Err(error) => {
                    observer.rollback(attempt_id);
                    return Err(PublishFailure::Publish(error));
                }
            };
            if let Some(existing) = head.files.iter().find(|entry| {
                entry.coverage.contains(&range)
                    && entry.tenant_id == config.tenant_id
                    && matches!(entry.part_state, PartState::Active | PartState::Outdated)
            }) {
                if existing.file_id != built.file_id || existing.file_seal != built.file_seal {
                    observer.rollback(attempt_id);
                    return Err(PublishFailure::Verification(
                        "range already covered by a file with different content than the rebuilt bytes",
                    ));
                }
                if staged_generation.is_some() {
                    observer.promote(attempt_id);
                    notices.oe_published(existing);
                } else {
                    observer.rollback(attempt_id);
                }
                return Ok(Published {
                    entry: existing.clone(),
                    generation: head_id,
                    file_bytes: std::sync::Arc::unwrap_or_clone(built.bytes),
                });
            }
            let mut entry = staged_entry.clone();
            debug_assert!(entry.part_state.can_transition_to(PartState::Active));
            entry.part_state = PartState::Active;
            let mut next = ManifestGeneration {
                footer_mirror: None,
                generation: head_id + 1,
                ..head.clone()
            };
            next.files.push(entry.clone());
            let next_id = next.generation;
            let put = published.put_generation(next);
            if put.is_ok() {
                staged_generation = Some(next_id);
            }
            match put.and_then(|()| published.advance_head(head_id, next_id)) {
                Ok(()) => {
                    observer.promote(attempt_id);
                    notices.oe_published(&entry);
                    break next_id;
                }
                Err(PublishError::CasLost { .. }) => {
                    rebase_attempts += 1;
                    if rebase_attempts > MAX_REBASE_ATTEMPTS {
                        observer.rollback(attempt_id);
                        return Err(PublishFailure::Publish(PublishError::CasLost {
                            current_generation: head_id,
                        }));
                    }
                    continue;
                }
                Err(PublishError::GenerationExists) => {
                    let existing = match published.generation(next_id) {
                        Ok(g) => g,
                        Err(PublishError::UnknownGeneration) => {
                            rebase_attempts += 1;
                            if rebase_attempts > MAX_REBASE_ATTEMPTS {
                                observer.rollback(attempt_id);
                                return Err(PublishFailure::Publish(PublishError::CasLost {
                                    current_generation: head_id,
                                }));
                            }
                            continue;
                        }
                        Err(error) => {
                            observer.rollback(attempt_id);
                            return Err(PublishFailure::Publish(error));
                        }
                    };
                    if let Some(covering) = existing.files.iter().find(|e| {
                        e.coverage.contains(&range)
                            && e.tenant_id == config.tenant_id
                            && matches!(e.part_state, PartState::Active | PartState::Outdated)
                    }) {
                        if covering.file_id != built.file_id || covering.file_seal != built.file_seal {
                            observer.rollback(attempt_id);
                            return Err(PublishFailure::Verification(
                                "range already covered by a file with different content than the rebuilt bytes",
                            ));
                        }
                        match published.advance_head(head_id, next_id) {
                            Ok(()) => {
                                observer.promote(attempt_id);
                                notices.oe_published(&entry);
                                break next_id;
                            }
                            Err(PublishError::CasLost { .. }) => {
                                rebase_attempts += 1;
                                if rebase_attempts > MAX_REBASE_ATTEMPTS {
                                    observer.rollback(attempt_id);
                                    return Err(PublishFailure::Publish(PublishError::CasLost {
                                        current_generation: head_id,
                                    }));
                                }
                                continue;
                            }
                            Err(error) => {
                                observer.rollback(attempt_id);
                                return Err(PublishFailure::Publish(error));
                            }
                        }
                    } else {
                        rebase_attempts += 1;
                        if rebase_attempts > MAX_REBASE_ATTEMPTS {
                            observer.rollback(attempt_id);
                            return Err(PublishFailure::Publish(PublishError::CasLost {
                                current_generation: head_id,
                            }));
                        }
                        match published.advance_head(head_id, next_id) {
                            Ok(()) => {
                                for introduced in existing.files.iter().filter(|entry| {
                                    entry.part_state == PartState::Active
                                        && !head.files.iter().any(|prior| prior.file_id == entry.file_id)
                                }) {
                                    notices.oe_published(introduced);
                                }
                                continue;
                            }
                            Err(PublishError::CasLost { .. }) => continue,
                            Err(error) => {
                                observer.rollback(attempt_id);
                                return Err(PublishFailure::Publish(error));
                            }
                        }
                    }
                }
                Err(error) => {
                    observer.rollback(attempt_id);
                    return Err(PublishFailure::Publish(error));
                }
            }
        };

        let mut entry = staged_entry;
        entry.part_state = PartState::Active;
        Ok(Published {
            entry,
            generation,
            file_bytes: std::sync::Arc::unwrap_or_clone(built.bytes),
        })
    }
}

fn durable_event_frames<'r>(
    replay: &'r ReplayOutcome,
    tenant_id: TenantId,
    range: &SequenceRange,
) -> Result<Vec<&'r ReplayedFrame>, PublishFailure> {
    let in_range: Vec<_> = replay
        .frames
        .iter()
        .filter(|frame| {
            frame.header.tenant_id == tenant_id
                && frame.header.epoch == range.epoch
                && frame.header.last_sequence >= range.first_sequence
                && frame.header.first_sequence <= range.last_sequence
        })
        .collect();
    let frame_ranges: Vec<SequenceRange> = in_range
        .iter()
        .map(|frame| SequenceRange {
            epoch: frame.header.epoch,
            first_sequence: frame.header.first_sequence,
            last_sequence: frame.header.last_sequence,
        })
        .collect();
    let rejected = arbitrate_coverage(&frame_ranges);

    let mut accepted: Vec<&ReplayedFrame> = in_range
        .iter()
        .enumerate()
        .filter(|(index, _)| !rejected.contains(index))
        .map(|(_, frame)| *frame)
        .collect();
    accepted.sort_by_key(|frame| frame.header.first_sequence);

    let mut event_frames = Vec::new();
    let mut covered_to: Option<u64> = None;
    for frame in accepted {
        let header = &frame.header;
        let expected = covered_to.map_or(range.first_sequence, |c| c + 1);
        if header.first_sequence > expected {
            return Err(PublishFailure::RangeNotDurable);
        }
        covered_to = Some(header.last_sequence.min(range.last_sequence));
        if header.is_void_record() {
            continue;
        }
        event_frames.push(frame);
    }
    if covered_to.is_none_or(|c| c < range.last_sequence) {
        return Err(PublishFailure::RangeNotDurable);
    }
    Ok(event_frames)
}

pub fn range_holds_no_events(
    replay: &ReplayOutcome,
    tenant_id: TenantId,
    range: &SequenceRange,
) -> Result<bool, PublishFailure> {
    Ok(durable_event_frames(replay, tenant_id, range)?.is_empty())
}

pub fn journal_retention_can_advance(
    published: &ManifestGeneration,
    range: &SequenceRange,
    tenant_id: TenantId,
    range_is_all_void: bool,
    safety_window_elapsed: bool,
) -> bool {
    safety_window_elapsed && (range_is_all_void || published.covers(range, tenant_id))
}

#[cfg(test)]
#[path = "test/publish.rs"]
mod tests;
