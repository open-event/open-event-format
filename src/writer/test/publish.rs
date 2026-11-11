use super::super::build::BuildLifecycle;
use super::super::pipeline::{FlushReason, RouteDependency, WorkerCommitPipeline};
use super::super::reserve::SequenceAllocator;
use super::super::sim::SimSafeRetryStore;
use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::artifacts::overlay::LiveOverlayStore;
use crate::artifacts::watermark::WatermarkTracker;
use crate::columns::{FreetextDeclaration, PromotionPlan};
use crate::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef, TargetIdSpace};
use crate::events::variant::VariantValue;
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::invariants::sim::{SerialEncodeExecutor, SimClock, SimJournalStorage, SimulatedPublishedSet};
use crate::layout::LayoutTargets;
use crate::layout::reader::OeFile;
use crate::object_store::sim::{ObjectFault, SimObjectStore};
use crate::typed_id::TypedIdTestExt;

fn event(i: u64) -> EventInput {
    EventInput {
        envelope: EventEnvelope {
            event_id: EventId::new_test_id(0xCAFE + u128::from(i)),
            tenant_id: TenantId::new_test_id(9),
            stream_id: StreamId(1),
            stream_sequence: i,
            occurred_at: TimestampValue::from_physical_nanos(100 + i as i64),
            ingested_at: TimestampValue::from_physical_nanos(200 + i as i64),
            source: "crm".into(),
            event_type: "deal.updated".into(),
            entity_type: "opportunity".into(),
            entity_id_hash_low: i,
            entity_id_hash_high: 0,
            entity_id: Some(format!("opp-{i}")),
            actor_id_hash_low: 0,
            actor_id: None,
            account_id_hash_low: 0,
            account_id: None,
            trace_id_hash_low: 0,
            dedupe_hash_low: 1000 + i,
            dedupe_hash_high: 7,
            schema_version: 1,
            flags: EventFlags(0),
        },
        payload: PayloadInput::Variant(VariantValue::Int(i as i64)),
        source_schema: None,
        source_delivery: None,
        connector_delivery_hash_low: 5000 + i,
        connector_delivery_hash_high: 1,
        provenance: None,
        relationships: None,
    }
}

fn build_config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 1,
        footer_dek: None,
        footer_encryption: crate::security::FooterEncryption::Plaintext,
        freetext: FreetextDeclaration::default(),
        freetext_row_offset_index: false,
        generation_id: 0,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan::default(),
        targets: LayoutTargets::default(),
        tenant_id: TenantId::new_test_id(9),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

struct World {
    allocator: SequenceAllocator,
    clock: SimClock,
    overlay: LiveOverlayStore,
    retry: SimSafeRetryStore,
    storage: SimJournalStorage,
    watermarks: WatermarkTracker,
    worker: WorkerCommitPipeline,
}

fn ingest(count: u64) -> (World, SequenceRange) {
    let clock = SimClock::new(99);
    let mut storage = SimJournalStorage::new();
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();
    let mut retry = SimSafeRetryStore::new();
    let mut overlay = LiveOverlayStore::new();
    let mut worker = WorkerCommitPipeline::new(
        1,
        ShardId(0),
        TenantId::new_test_id(9),
        RouteDependency::AppendOnly,
        1 << 20,
    );
    for i in 0..count {
        worker.submit(event(i), 1, &mut retry, &clock).unwrap();
    }
    let result = worker
        .flush(
            FlushReason::Target,
            &mut allocator,
            &mut storage,
            &mut watermarks,
            &mut retry,
            &mut overlay,
            &clock,
        )
        .unwrap();
    let range = result.range;
    (
        World {
            clock,
            storage,
            allocator,
            watermarks,
            overlay,
            retry,
            worker,
        },
        range,
    )
}

#[test]
fn publish_boundary_end_to_end() {
    let (world, range) = ingest(5);
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    assert_eq!(published.entry.part_state, PartState::Active);
    assert_eq!(observer.promoted, vec![1]);
    assert!(observer.rolled_back.is_empty());
    assert_eq!(notices.published.len(), 1);
    let (generation_id, head) = published_set.head().unwrap();
    assert_eq!(generation_id, published.generation);
    assert!(head.covers(&range, TenantId::new_test_id(9)));
    let file = OeFile::open(published.file_bytes.clone(), Some(&published.entry.file_seal)).unwrap();
    assert_eq!(file.header().row_count, 5);
    let mut overlay = LiveOverlayStore::new();
    let replay = crate::artifacts::segment::replay_segment(&world.storage, ShardId(0), 1, 1).unwrap();
    overlay.rebuild_from_replay(&replay, 1, 1, |_, _| false).unwrap();
    assert_eq!(overlay.segment_count(), 1);
    let evicted = overlay.evict_covered(&head);
    assert_eq!(evicted, 1);
    assert!(!journal_retention_can_advance(
        &head,
        &range,
        TenantId::new_test_id(9),
        false,
        false
    ));
    assert!(journal_retention_can_advance(
        &head,
        &range,
        TenantId::new_test_id(9),
        false,
        true
    ));
}

#[test]
fn republish_same_range_is_idempotent_with_an_encrypted_footer() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let mut config = build_config();
    config.footer_encryption = crate::security::FooterEncryption::Encrypted;
    config.footer_dek = Some([7u8; 32]);
    let publish = |publisher: &mut OePublisher,
                   published_set: &mut SimulatedPublishedSet,
                   observer: &mut RecordingObserver,
                   notices: &mut NoopPeerNotices| {
        publisher.publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &config,
            published_set,
            &SimObjectStore::new(),
            observer,
            notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
    };
    let first = publish(&mut publisher, &mut published_set, &mut observer, &mut notices).unwrap();
    let second = publish(&mut publisher, &mut published_set, &mut observer, &mut notices)
        .expect("a retry of an encrypted-footer publication rebuilds byte-identical content");
    assert_eq!(first.entry.file_id, second.entry.file_id);
    assert_eq!(first.entry.file_seal, second.entry.file_seal);
    assert_eq!(first.file_bytes, second.file_bytes);
    assert_eq!(published_set.head().unwrap().0, first.generation);
    assert_eq!(notices.published.len(), 1);
}

#[test]
fn republish_same_range_is_idempotent() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let config = build_config();
    let first = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &config,
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    let second = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &config,
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    assert_eq!(first.entry.file_id, second.entry.file_id);
    assert_eq!(published_set.head().unwrap().0, first.generation);
    assert_eq!(notices.published.len(), 1);
    assert_eq!(
        published_set.head().unwrap().1.files.len(),
        1,
        "re-publication must not add a second covering file"
    );
    OeFile::open(second.file_bytes, Some(&second.entry.file_seal)).unwrap();
}

#[test]
fn republish_with_changed_build_config_conflicts_instead_of_pairing_mismatched_bytes() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let first_config = build_config();
    publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &first_config,
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    let mut second_config = build_config();
    second_config.created_at_physical = 2;
    let result = publisher.publish_range(
        &world.storage,
        ShardId(0),
        1,
        1,
        None,
        range,
        &second_config,
        &mut published_set,
        &SimObjectStore::new(),
        &mut observer,
        &mut notices,
        &world.clock,
        &SerialEncodeExecutor,
    );
    assert!(matches!(result, Err(PublishFailure::Verification(_))));
    assert_eq!(
        published_set.head().unwrap().1.files.len(),
        1,
        "the conflicting rebuild must not be published as a second file"
    );
}

fn covering_entry(range: SequenceRange, tenant: TenantId, part_state: PartState) -> OeFileEntry {
    OeFileEntry {
        coverage: range,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0u8; 32],
        file_id: 0,
        file_type: FileType::OeFile,
        footer_len: None,
        entity_summary: None,
        graph_event_id_filter: None,
        graph_summary: None,
        deletion_vector: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state,
        profile: crate::layout::Profile::Event,
        required_feature_flags: 0,
        size_bytes: 0,
        tenant_id: tenant,
        tree_len: None,
    }
}

#[test]
fn a_covering_entry_of_another_tenant_or_a_retired_entry_does_not_short_circuit() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();

    let seeded = ManifestGeneration {
        files: vec![
            covering_entry(range, TenantId::new_test_id(999), PartState::Active),
            covering_entry(range, TenantId::new_test_id(9), PartState::DeleteOnDestroy),
        ],
        generation: 1,
        ..Default::default()
    };
    published_set.put_generation(seeded).unwrap();
    published_set.advance_head(0, 1).unwrap();

    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();

    assert_eq!(published.entry.tenant_id, TenantId::new_test_id(9));
    assert_eq!(published.entry.part_state, PartState::Active);
    assert_ne!(
        published.entry.file_id, 0,
        "the published entry is the freshly built file, not the seeded stub"
    );
    assert_eq!(
        notices.published.len(),
        1,
        "short-circuiting on a foreign/retired entry would emit no notice"
    );
    let head = published_set.head().unwrap().1;
    assert!(
        head.files.iter().any(|e| {
            e.tenant_id == TenantId::new_test_id(9)
                && e.part_state == PartState::Active
                && e.file_id == published.entry.file_id
        }),
        "the newly published Active file must be in the manifest"
    );
}

struct RacingSet {
    inner: SimulatedPublishedSet,
    race_armed: bool,
}

impl PublishedSet for RacingSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError> {
        self.inner.head()
    }
    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError> {
        if self.race_armed {
            self.race_armed = false;
            let (head_id, head) = self.inner.head()?;
            let rival = ManifestGeneration {
                generation: head_id + 1,
                files: head.files,
                ..Default::default()
            };
            self.inner.put_generation(rival)?;
            self.inner.advance_head(head_id, head_id + 1)?;
        }
        self.inner.put_generation(generation)
    }
    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError> {
        self.inner.advance_head(expected, next)
    }
    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError> {
        self.inner.generation(id)
    }
}

#[test]
fn lost_cas_rebases_and_retries_never_overwrites() {
    let (world, range) = ingest(3);
    let mut publisher = OePublisher::new();
    let mut racing = RacingSet {
        inner: SimulatedPublishedSet::new(),
        race_armed: true,
    };
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut racing,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    assert_eq!(published.generation, 2);
    let rival = racing.inner.generation(1).unwrap();
    assert!(rival.files.is_empty());
    let winner = racing.inner.generation(2).unwrap();
    assert_eq!(winner.files.len(), 1);
}

#[test]
fn failed_publish_leaks_nothing() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    publisher.quota_bytes = Some(16);
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let error = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap_err();
    assert!(matches!(error, PublishFailure::Verification(_)));
    assert!(notices.published.is_empty());
    assert_eq!(observer.rolled_back, vec![1]);
    assert!(observer.promoted.is_empty());
    let (generation_id, head) = published_set.head().unwrap();
    assert_eq!(generation_id, 0);
    assert!(head.files.is_empty());
}

#[test]
fn dual_roll_trigger_bytes_or_time() {
    let clock = SimClock::new(5);
    let policy = RollPolicy::default();
    let state = OpenFileState {
        opened_at_monotonic_nanos: clock.monotonic_nanos(),
        compressed_bytes: policy.byte_target,
    };
    assert_eq!(should_roll(&state, &policy, &clock), Some(RollTrigger::ByteTarget));
    let state = OpenFileState {
        opened_at_monotonic_nanos: clock.monotonic_nanos(),
        compressed_bytes: 1024,
    };
    assert_eq!(should_roll(&state, &policy, &clock), None);
    clock.advance(policy.max_open_nanos + 1);
    assert_eq!(should_roll(&state, &policy, &clock), Some(RollTrigger::TimeWindow));
}

#[test]
fn unpublished_file_is_not_visible() {
    let (world, range) = ingest(3);
    let head = SimulatedPublishedSet::new().head().unwrap().1;
    assert!(!head.covers(&range, TenantId::new_test_id(9)));
    assert_eq!(head.snapshot_files().count(), 0);
    let mut overlay = LiveOverlayStore::new();
    let replay = crate::artifacts::segment::replay_segment(&world.storage, ShardId(0), 1, 1).unwrap();
    overlay
        .rebuild_from_replay(&replay, 1, 1, |t, r| head.covers(r, t))
        .unwrap();
    assert_eq!(overlay.segment_count(), 1, "range still served from LiveOverlay");
    assert_eq!(overlay.evict_covered(&head), 0, "premature eviction prevented");
}

#[test]
fn publish_skips_voids_but_requires_contiguity() {
    let (mut world, range) = ingest(2);
    let lease = world.allocator.reserve(3, world.clock.monotonic_nanos());
    assert_eq!(lease.range.first_sequence, range.last_sequence + 1);
    world.clock.advance(super::super::reserve::LEASE_NANOS + 1);
    world
        .worker
        .commit_voids(
            &mut world.allocator,
            &mut world.storage,
            &mut world.watermarks,
            &mut world.overlay,
            &world.clock,
        )
        .unwrap();
    world
        .worker
        .submit(event(10), 1, &mut world.retry, &world.clock)
        .unwrap();
    let tail = world
        .worker
        .flush(
            FlushReason::Target,
            &mut world.allocator,
            &mut world.storage,
            &mut world.watermarks,
            &mut world.retry,
            &mut world.overlay,
            &world.clock,
        )
        .unwrap();
    let full = SequenceRange {
        epoch: 1,
        first_sequence: range.first_sequence,
        last_sequence: tail.range.last_sequence,
    };
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            full,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    let file = OeFile::open(published.file_bytes, None).unwrap();
    assert_eq!(file.header().row_count, 3);
    assert!(published_set.head().unwrap().1.covers(&full, TenantId::new_test_id(9)));
}

#[test]
fn retention_advances_over_a_range_that_holds_only_voids() {
    let (mut world, range) = ingest(2);
    let lease = world.allocator.reserve(3, world.clock.monotonic_nanos());
    let void_range = lease.range;
    assert_eq!(void_range.first_sequence, range.last_sequence + 1);
    world.clock.advance(super::super::reserve::LEASE_NANOS + 1);
    world
        .worker
        .commit_voids(
            &mut world.allocator,
            &mut world.storage,
            &mut world.watermarks,
            &mut world.overlay,
            &world.clock,
        )
        .unwrap();

    let replay = crate::artifacts::segment::replay_segment(&world.storage, ShardId(0), 1, 1).unwrap();
    assert!(range_holds_no_events(&replay, TenantId::new_test_id(9), &void_range).unwrap());
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let failure = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            void_range,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut RecordingObserver::default(),
            &mut NoopPeerNotices::default(),
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap_err();
    assert!(matches!(failure, PublishFailure::Verification(_)));

    let head = published_set.head().unwrap().1;
    assert!(!head.covers(&void_range, TenantId::new_test_id(9)));
    assert!(!journal_retention_can_advance(
        &head,
        &void_range,
        TenantId::new_test_id(9),
        true,
        false
    ));
    assert!(journal_retention_can_advance(
        &head,
        &void_range,
        TenantId::new_test_id(9),
        true,
        true
    ));

    assert!(!range_holds_no_events(&replay, TenantId::new_test_id(9), &range).unwrap());
}

fn multi_stripe_config() -> OeBuildConfig {
    OeBuildConfig {
        targets: LayoutTargets {
            index_granularity: 8,
            index_granularity_bytes: 1 << 20,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
            stripe_target_bytes: 512,
        },
        ..build_config()
    }
}

#[test]
fn oe_upload_segments_cut_the_file_at_stripe_boundaries() {
    let rows: Vec<OeRow> = (0..40)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: event(i),
        })
        .collect();
    let built = crate::writer::build::build_oe_file(rows, &multi_stripe_config()).unwrap();
    assert!(built.footer.stripes.len() >= 2, "test needs a multi-stripe file");

    let segments = oe_upload_segments(&built);
    assert_eq!(segments.iter().sum::<u64>(), built.bytes.len() as u64);

    let mut boundaries = vec![0u64];
    let mut running = 0u64;
    for len in &segments {
        running += len;
        boundaries.push(running);
    }
    for stripe in &built.footer.stripes {
        assert!(
            boundaries.contains(&stripe.file_offset),
            "stripe offset {} falls on a segment boundary",
            stripe.file_offset
        );
    }
}

#[test]
fn publish_rejects_replayed_chain_that_does_not_match_recorded_anchor() {
    let (world, range) = ingest(4);
    let replay = crate::artifacts::segment::replay_segment(&world.storage, ShardId(0), 1, 1).unwrap();
    let recorded = replay.segment_chain_blake3.unwrap();

    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            Some(recorded),
            range,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    assert_eq!(published_set.head().unwrap().1.files.len(), 1);

    let mut wrong = recorded;
    wrong[0] ^= 0xFF;
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let result = publisher.publish_range(
        &world.storage,
        ShardId(0),
        1,
        1,
        Some(wrong),
        range,
        &build_config(),
        &mut published_set,
        &SimObjectStore::new(),
        &mut observer,
        &mut notices,
        &world.clock,
        &SerialEncodeExecutor,
    );
    assert!(matches!(result, Err(PublishFailure::Verification(_))));
    assert_eq!(published_set.head().unwrap().0, 0);
    assert!(observer.staged.is_empty());
    assert!(observer.rolled_back.is_empty());
    assert!(notices.published.is_empty());
}

#[test]
fn publish_is_not_short_circuited_by_another_tenants_covering_entry() {
    let (world, range) = ingest(4);
    let mut published_set = SimulatedPublishedSet::new();
    let other_tenant_entry = OeFileEntry {
        coverage: range,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [7u8; 32],
        file_id: 999,
        file_type: FileType::OeFile,
        footer_len: None,
        entity_summary: None,
        graph_event_id_filter: None,
        graph_summary: None,
        deletion_vector: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state: PartState::Active,
        profile: crate::layout::Profile::Event,
        required_feature_flags: 0,
        size_bytes: 1,
        tenant_id: TenantId::new_test_id(42),
        tree_len: None,
    };
    published_set
        .put_generation(ManifestGeneration {
            files: vec![other_tenant_entry],
            generation: 1,
            ..Default::default()
        })
        .unwrap();
    published_set.advance_head(0, 1).unwrap();

    let mut publisher = OePublisher::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    assert_eq!(published.entry.tenant_id, TenantId::new_test_id(9));
    let (_, head) = published_set.head().unwrap();
    assert_eq!(
        head.files.len(),
        2,
        "our file is published alongside the other tenant's entry"
    );
    assert_eq!(
        head.files
            .iter()
            .filter(|e| e.tenant_id == TenantId::new_test_id(9))
            .count(),
        1
    );
}

fn manifest_covering(range: SequenceRange, tenant_id: TenantId) -> ManifestGeneration {
    ManifestGeneration {
        files: vec![OeFileEntry {
            coverage: range,
            feature_metadata: None,
            field_deletion_vectors: Vec::new(),
            file_seal: [0u8; 32],
            file_id: 1,
            file_type: FileType::OeFile,
            footer_len: None,
            entity_summary: None,
            graph_event_id_filter: None,
            graph_summary: None,
            deletion_vector: None,
            optional_feature_flags: 0,
            part_index: 0,
            part_state: PartState::Active,
            profile: crate::layout::Profile::Event,
            required_feature_flags: 0,
            size_bytes: 1,
            tenant_id,
            tree_len: None,
        }],
        generation: 1,
        ..Default::default()
    }
}

#[test]
fn interrupted_publish_with_changed_content_is_refused_not_adopted() {
    let (world, range) = ingest(4);
    let mut published_set = SimulatedPublishedSet::new();
    published_set
        .put_generation(manifest_covering(range, TenantId::new_test_id(9)))
        .unwrap();

    let mut publisher = OePublisher::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let result = publisher.publish_range(
        &world.storage,
        ShardId(0),
        1,
        1,
        None,
        range,
        &build_config(),
        &mut published_set,
        &SimObjectStore::new(),
        &mut observer,
        &mut notices,
        &world.clock,
        &SerialEncodeExecutor,
    );
    assert!(matches!(result, Err(PublishFailure::Verification(_))));
    assert_eq!(
        published_set.head().unwrap().0,
        0,
        "the head must not advance over mismatched content"
    );
    assert!(notices.published.is_empty());
    assert_eq!(observer.rolled_back, vec![1]);
}

#[test]
fn interrupted_publish_of_identical_content_completes_by_advancing_the_head() {
    let (world, range) = ingest(4);
    let config = build_config();
    let mut scratch = SimulatedPublishedSet::new();
    let mut publisher = OePublisher::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let first = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &config,
            &mut scratch,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();

    let mut published_set = SimulatedPublishedSet::new();
    published_set
        .put_generation(ManifestGeneration {
            files: vec![first.entry.clone()],
            generation: 1,
            ..Default::default()
        })
        .unwrap();

    let republished = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &config,
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    assert_eq!(republished.entry.file_id, first.entry.file_id);
    assert_eq!(republished.generation, 1);
    assert_eq!(
        published_set.head().unwrap().0,
        1,
        "the head catches up to the already-written generation"
    );
}

#[test]
fn a_collided_generation_holding_only_a_retired_entry_is_not_adopted() {
    let (world, range) = ingest(4);
    let config = build_config();
    let mut scratch = SimulatedPublishedSet::new();
    let mut publisher = OePublisher::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let first = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &config,
            &mut scratch,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();

    let mut retired = first.entry.clone();
    retired.part_state = PartState::DeleteOnDestroy;
    let mut published_set = SimulatedPublishedSet::new();
    published_set
        .put_generation(ManifestGeneration {
            files: vec![retired],
            generation: 1,
            ..Default::default()
        })
        .unwrap();

    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let republished = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &config,
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();

    assert!(republished.generation > 1);
    let (head_id, head) = published_set.head().unwrap();
    assert_eq!(head_id, republished.generation);
    assert!(
        head.files
            .iter()
            .any(|entry| entry.part_state == PartState::Active && entry.coverage.contains(&range)),
        "the range ends up covered by a live entry, not a retired one"
    );
}

#[test]
fn orphaned_generation_for_another_range_does_not_wedge_the_publish() {
    let (world, range) = ingest(4);
    let orphan_range = SequenceRange {
        epoch: 1,
        first_sequence: 900,
        last_sequence: 950,
    };
    let mut published_set = SimulatedPublishedSet::new();
    published_set
        .put_generation(manifest_covering(orphan_range, TenantId::new_test_id(9)))
        .unwrap();

    let mut publisher = OePublisher::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut published_set,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .expect("an orphaned foreign generation must not block other ranges");

    assert_eq!(published.generation, 2);
    let (head_id, head) = published_set.head().unwrap();
    assert_eq!(head_id, 2);
    assert!(head.covers(&range, TenantId::new_test_id(9)));
    assert!(
        head.covers(&orphan_range, TenantId::new_test_id(9)),
        "helping over the stalled generation publishes its entry too"
    );
    assert_eq!(
        notices.published.len(),
        2,
        "both the rescued orphan's entry and this publisher's own entry are notified"
    );
}

struct HeadStolenSet {
    inner: SimulatedPublishedSet,
    steal_armed: bool,
}

impl PublishedSet for HeadStolenSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError> {
        self.inner.head()
    }
    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError> {
        self.inner.put_generation(generation)
    }
    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError> {
        if self.steal_armed {
            self.steal_armed = false;
            self.inner.advance_head(expected, next)?;
            return Err(PublishError::CasLost {
                current_generation: next,
            });
        }
        self.inner.advance_head(expected, next)
    }
    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError> {
        self.inner.generation(id)
    }
}

#[test]
fn a_winning_generation_the_head_was_advanced_over_is_completed_not_rolled_back() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    let mut stolen = HeadStolenSet {
        inner: SimulatedPublishedSet::new(),
        steal_armed: true,
    };
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut stolen,
            &SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();

    assert_eq!(published.generation, 1);
    assert_eq!(
        observer.promoted,
        vec![1],
        "the won publication promotes its staged side effects"
    );
    assert!(
        observer.rolled_back.is_empty(),
        "a live, query-visible file must not have had its side effects rolled back"
    );
    assert_eq!(notices.published.len(), 1, "peers are notified of the live file");
    let (_, head) = stolen.head().unwrap();
    assert!(head.covers(&range, TenantId::new_test_id(9)));
    assert!(
        head.files
            .iter()
            .any(|entry| entry.part_state == PartState::Active && entry.file_id == published.entry.file_id),
        "the attempt's own file is the live covering entry"
    );
}

#[test]
fn covers_and_retention_are_tenant_scoped() {
    let range = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 10,
    };
    let manifest = manifest_covering(range, TenantId::new_test_id(2));
    assert!(!manifest.covers(&range, TenantId::new_test_id(1)));
    assert!(manifest.covers(&range, TenantId::new_test_id(2)));
    assert!(!journal_retention_can_advance(
        &manifest,
        &range,
        TenantId::new_test_id(1),
        false,
        true
    ));
    assert!(journal_retention_can_advance(
        &manifest,
        &range,
        TenantId::new_test_id(2),
        false,
        true
    ));
}

#[test]
fn evict_covered_does_not_cross_tenants() {
    let (world, range) = ingest(3);
    let mut overlay = LiveOverlayStore::new();
    let replay = crate::artifacts::segment::replay_segment(&world.storage, ShardId(0), 1, 1).unwrap();
    overlay.rebuild_from_replay(&replay, 1, 1, |_, _| false).unwrap();
    assert_eq!(overlay.segment_count(), 1);

    let other_tenant = manifest_covering(range, TenantId::new_test_id(42));
    assert_eq!(overlay.evict_covered(&other_tenant), 0);
    assert_eq!(overlay.segment_count(), 1);

    let own_tenant = manifest_covering(range, TenantId::new_test_id(9));
    assert_eq!(overlay.evict_covered(&own_tenant), 1);
    assert_eq!(overlay.segment_count(), 0);
}

fn frame_for(tenant: u128, first_sequence: u64, count: u64) -> Vec<u8> {
    let events: Vec<EventInput> = (0..count)
        .map(|i| {
            let mut event = event(first_sequence + i);
            event.envelope.tenant_id = TenantId::new_test_id(tenant);
            event
        })
        .collect();
    let payload = crate::artifacts::batch::build_batch(&events, 1, 0).unwrap();
    crate::artifacts::frame::build_frame(
        &crate::artifacts::frame::FrameBuildInput {
            flags: 0,
            tenant_id: TenantId::new_test_id(tenant),
            writer_id: 1,
            epoch: 1,
            first_sequence,
            last_sequence: first_sequence + count - 1,
            event_count: count as u32,
            durable_batch_id: first_sequence,
            writer_local_batch_id: first_sequence,
            schema_generation: 1,
            dictionary_generation_hint: 0,
            created_at_physical: 1,
            committed_at_physical: 2,
        },
        &payload,
    )
    .unwrap()
}

fn publish_frames(frames: &[Vec<u8>], range: SequenceRange) -> Result<Published, PublishFailure> {
    let clock = SimClock::new(99);
    let mut storage = SimJournalStorage::new();
    for frame in frames {
        storage.append(ShardId(0), frame).unwrap();
    }
    storage.sync(ShardId(0)).unwrap();
    OePublisher::new().publish_range(
        &storage,
        ShardId(0),
        1,
        1,
        None,
        range,
        &build_config(),
        &mut SimulatedPublishedSet::new(),
        &SimObjectStore::new(),
        &mut RecordingObserver::default(),
        &mut NoopPeerNotices::default(),
        &clock,
        &SerialEncodeExecutor,
    )
}

#[test]
fn frames_made_durable_out_of_sequence_order_still_publish() {
    let range = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 6,
    };
    let published = publish_frames(&[frame_for(9, 4, 3), frame_for(9, 1, 3)], range).expect("the range is durable");
    let file = OeFile::open(published.file_bytes.clone(), Some(&published.entry.file_seal)).unwrap();
    assert_eq!(file.header().row_count, 6, "every row of the range is published");
}

#[test]
fn another_tenants_overlapping_frame_neither_blocks_publication_nor_joins_its_coverage() {
    let range = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 3,
    };
    let published =
        publish_frames(&[frame_for(42, 1, 3), frame_for(9, 1, 3)], range).expect("our own frame covers the range");
    let file = OeFile::open(published.file_bytes.clone(), Some(&published.entry.file_seal)).unwrap();
    assert_eq!(file.header().row_count, 3, "only this tenant's rows are published");

    assert!(matches!(
        publish_frames(&[frame_for(42, 1, 3)], range),
        Err(PublishFailure::RangeNotDurable)
    ));
}

#[test]
fn a_failed_upload_never_reaches_put_generation() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let objects = SimObjectStore::new();
    objects.inject(ObjectFault::FailPutIfAbsent);
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();

    let result = publisher.publish_range(
        &world.storage,
        ShardId(0),
        1,
        1,
        None,
        range,
        &build_config(),
        &mut published_set,
        &objects,
        &mut observer,
        &mut notices,
        &world.clock,
        &SerialEncodeExecutor,
    );

    assert!(matches!(result, Err(PublishFailure::Storage(_))));
    assert_eq!(
        published_set.generation(1),
        Err(PublishError::UnknownGeneration),
        "no generation was written"
    );
    assert_eq!(published_set.head().unwrap().0, 0);
    assert_eq!(observer.rolled_back, vec![1]);
    assert!(notices.published.is_empty());
    assert!(objects.keys().is_empty());
}

#[test]
fn a_published_file_is_uploaded_under_its_object_key() {
    let (world, range) = ingest(4);
    let mut publisher = OePublisher::new();
    let mut published_set = SimulatedPublishedSet::new();
    let objects = SimObjectStore::new();
    let published = publisher
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut published_set,
            &objects,
            &mut RecordingObserver::default(),
            &mut NoopPeerNotices::default(),
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();

    let key = oe_object_key(TenantId::new_test_id(9), published.entry.file_id);
    assert_eq!(objects.object(&key).unwrap(), published.file_bytes);
}

struct RivalCoversSet {
    inner: SimulatedPublishedSet,
    rival: Option<ManifestGeneration>,
}

impl PublishedSet for RivalCoversSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError> {
        self.inner.head()
    }
    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError> {
        if let Some(rival) = self.rival.take() {
            let rival_id = rival.generation;
            self.inner.put_generation(rival)?;
            self.inner.advance_head(rival_id - 1, rival_id)?;
        }
        self.inner.put_generation(generation)
    }
    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError> {
        self.inner.advance_head(expected, next)
    }
    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError> {
        self.inner.generation(id)
    }
}

#[test]
fn a_lost_race_after_upload_leaves_the_object_unreferenced() {
    let (world, range) = ingest(4);
    let tenant = TenantId::new_test_id(9);
    let mut publisher = OePublisher::new();
    let mut racing = RivalCoversSet {
        inner: SimulatedPublishedSet::new(),
        rival: Some(manifest_covering(range, tenant)),
    };
    let objects = SimObjectStore::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();

    let result = publisher.publish_range(
        &world.storage,
        ShardId(0),
        1,
        1,
        None,
        range,
        &build_config(),
        &mut racing,
        &objects,
        &mut observer,
        &mut notices,
        &world.clock,
        &SerialEncodeExecutor,
    );

    assert!(matches!(result, Err(PublishFailure::Verification(_))));
    let keys = objects.keys();
    assert_eq!(keys.len(), 1, "the uploaded object is kept, not deleted");
    let (_, head) = racing.inner.head().unwrap();
    assert_eq!(head.files.len(), 1);
    assert_ne!(keys[0], oe_object_key(tenant, head.files[0].file_id));
    assert_eq!(observer.rolled_back, vec![1]);
    assert!(notices.published.is_empty());
}

#[test]
fn rows_for_range_carry_declared_relationships_after_the_replay_is_dropped() {
    let declared = |i: u64| -> Option<EventRelationships> {
        let target = |len: usize, salt: u8| (0..len).map(|b| b as u8 ^ salt ^ i as u8).collect::<Vec<u8>>();
        match i % 4 {
            0 => None,
            1 => Some(EventRelationships::new(Vec::new()).unwrap()),
            2 => Some(
                EventRelationships::new(vec![
                    RelationshipRef::new(RelationshipKind::Prev, TargetIdSpace::ProtocolEventId, target(32, 1))
                        .unwrap(),
                    RelationshipRef::new(RelationshipKind::Auth, TargetIdSpace::ProtocolEventId, target(32, 2))
                        .unwrap(),
                    RelationshipRef::new(RelationshipKind::Link, TargetIdSpace::ExternalId, target(7, 3)).unwrap(),
                ])
                .unwrap(),
            ),
            _ => Some(
                EventRelationships::new(vec![
                    RelationshipRef::new(RelationshipKind::Link, TargetIdSpace::ExternalId, target(1, 4)).unwrap(),
                    RelationshipRef::new(RelationshipKind::Parent, TargetIdSpace::EventId, target(16, 5)).unwrap(),
                ])
                .unwrap(),
            ),
        }
    };
    let frame = |first_sequence: u64| {
        let events: Vec<EventInput> = (first_sequence..first_sequence + 8)
            .map(|i| {
                let mut event = event(i);
                event.relationships = declared(i);
                event
            })
            .collect();
        let payload = crate::artifacts::batch::build_batch(&events, 1, 0).unwrap();
        crate::artifacts::frame::build_frame(
            &crate::artifacts::frame::FrameBuildInput {
                flags: 0,
                tenant_id: TenantId::new_test_id(9),
                writer_id: 1,
                epoch: 1,
                first_sequence,
                last_sequence: first_sequence + 7,
                event_count: 8,
                durable_batch_id: first_sequence,
                writer_local_batch_id: first_sequence,
                schema_generation: 1,
                dictionary_generation_hint: 0,
                created_at_physical: 1,
                committed_at_physical: 2,
            },
            &payload,
        )
        .unwrap()
    };
    let mut storage = SimJournalStorage::new();
    storage.append(ShardId(0), &frame(1)).unwrap();
    storage.append(ShardId(0), &frame(9)).unwrap();
    storage.sync(ShardId(0)).unwrap();
    let replay = replay_segment(&storage, ShardId(0), 1, 1).unwrap();
    let range = SequenceRange {
        epoch: 1,
        first_sequence: 4,
        last_sequence: 13,
    };

    let rows = OePublisher::new()
        .rows_for_range(&replay, TenantId::new_test_id(9), &range)
        .unwrap();
    drop(replay);
    drop(storage);

    assert_eq!(
        rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
        (4..=13).collect::<Vec<_>>()
    );
    for row in &rows {
        assert_eq!(
            row.event.relationships,
            declared(row.sequence),
            "sequence {}",
            row.sequence
        );
    }
}

#[test]
fn a_publication_carries_the_heads_index_artifacts_and_vector_segments_into_the_next_generation() {
    let (world, range) = ingest(3);
    let artifact = crate::lifecycle::IndexArtifactRef {
        artifact_blake3: [7; 32],
        column_id: 1,
        covered_file_id: 0xA,
        kind: crate::indexes::artifact::ArtifactKind::BinaryFuse,
        object_key: "idx/a".to_owned(),
    };
    let retirement = crate::lifecycle::Retirement {
        file_id: 0xA,
        generation: 1,
        since_nanos: 5,
    };
    let segment = crate::vector::VectorSegmentEntry {
        deletes: None,
        segment_id: 0xB,
    };
    let mut published = SimulatedPublishedSet::new();
    published
        .put_generation(ManifestGeneration {
            footer_mirror: Some(crate::lifecycle::FooterMirrorObject {
                blake3: [0; 32],
                path: "mirror/1".to_owned(),
                size_bytes: 1,
            }),
            generation: 1,
            index_artifacts: vec![artifact.clone()],
            retirements: vec![retirement],
            vector_segments: vec![segment.clone()],
            ..Default::default()
        })
        .unwrap();
    published.advance_head(0, 1).unwrap();

    let result = OePublisher::new()
        .publish_range(
            &world.storage,
            ShardId(0),
            1,
            1,
            None,
            range,
            &build_config(),
            &mut published,
            &SimObjectStore::new(),
            &mut RecordingObserver::default(),
            &mut NoopPeerNotices::default(),
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();

    let (head_id, head) = published.head().unwrap();
    assert_eq!(head_id, 2);
    assert_eq!(head.files, vec![result.entry]);
    assert_eq!(head.index_artifacts, vec![artifact]);
    assert_eq!(head.vector_segments, vec![segment]);
    assert_eq!(head.retirements, vec![retirement]);
    assert_eq!(
        head.footer_mirror, None,
        "the file set changed, so the mirror is dropped"
    );
}
