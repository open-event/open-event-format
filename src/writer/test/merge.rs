use super::*;
use crate::clock::FixedClock;
use crate::columns::{FreetextDeclaration, PromotionPlan};
use crate::events::variant::VariantValue;
use crate::events::{SequenceRange, TenantId};
use crate::invariants::sim::SimulatedPublishedSet;
use crate::layout::LayoutTargets;
use crate::lifecycle::{
    CompactionPolicy, FileType, GraphFileSummary, ManifestGeneration, OeFileEntry, plan_compaction_cycle,
};
use crate::object_store::sim::SimObjectStore;
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::BuiltOe;
use crate::writer::publish::NoopPeerNotices;
use crate::writer::retire::{SweepPolicy, sweep_retired_files};
use std::collections::BTreeMap;

pub(crate) const START_NANOS: i64 = 1_700_000_000_000_000_000;

pub(crate) fn tenant() -> TenantId {
    TenantId::new_test_id(42)
}

pub(crate) fn payload_of(i: u64) -> VariantValue {
    let mut fields = BTreeMap::new();
    fields.insert("body".to_owned(), VariantValue::String(format!("message {i}")));
    fields.insert("n".to_owned(), VariantValue::Int(i as i64));
    VariantValue::Object(fields)
}

pub(crate) fn event(i: u64) -> EventInput {
    EventInput {
        envelope: EventEnvelope {
            event_id: EventId::new_test_id(0xBEEF_0000 + u128::from(i)),
            tenant_id: tenant(),
            stream_id: StreamId(3),
            stream_sequence: i,
            occurred_at: TimestampValue::from_physical_nanos(1_000 + i as i64),
            ingested_at: TimestampValue::from_physical_nanos(2_000 + i as i64),
            source: if i.is_multiple_of(2) {
                "chat".into()
            } else {
                "mail".into()
            },
            event_type: "message.sent".into(),
            entity_type: "room".into(),
            entity_id_hash_low: i,
            entity_id_hash_high: 1,
            entity_id: Some(format!("room-{}", i % 3)),
            actor_id_hash_low: 7,
            actor_id: (i % 2 == 1).then(|| format!("actor-{i}")),
            account_id_hash_low: 0,
            account_id: None,
            trace_id_hash_low: 9,
            dedupe_hash_low: 100 + i,
            dedupe_hash_high: 4,
            schema_version: 2,
            flags: EventFlags(0),
        },
        payload: PayloadInput::Variant(payload_of(i)),
        source_schema: None,
        source_delivery: None,
        connector_delivery_hash_low: 0,
        connector_delivery_hash_high: 0,
        provenance: None,
        relationships: None,
    }
}

pub(crate) fn build_config() -> OeBuildConfig {
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
        tenant_id: tenant(),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

pub(crate) fn rows_of(sequences: impl IntoIterator<Item = u64>) -> Vec<OeRow> {
    sequences
        .into_iter()
        .map(|sequence| OeRow {
            epoch: 1,
            sequence,
            event: event(sequence),
        })
        .collect()
}

pub(crate) fn active_entry(built: &BuiltOe, first: u64, last: u64) -> OeFileEntry {
    OeFileEntry {
        coverage: SequenceRange {
            epoch: 1,
            first_sequence: first,
            last_sequence: last,
        },
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: built.file_seal,
        file_id: built.file_id,
        file_type: FileType::OeFile,
        footer_len: Some(built.footer_len),
        graph_event_id_filter: None,
        graph_summary: GraphFileSummary::of(&built.footer),
        deletion_vector: None,
        entity_summary: Some(built.entity_summary.clone()),
        optional_feature_flags: built.footer.optional_feature_flags,
        part_index: 0,
        part_state: PartState::Active,
        profile: built.footer.profile,
        required_feature_flags: built.footer.required_feature_flags,
        size_bytes: built.bytes.len() as u64,
        tenant_id: tenant(),
        tree_len: built.tree_len,
    }
}

pub(crate) fn publish_built(
    built: &BuiltOe,
    first: u64,
    last: u64,
    published: &mut SimulatedPublishedSet,
    objects: &SimObjectStore,
) {
    objects
        .put_if_absent(&oe_object_key(tenant(), built.file_id), &built.bytes)
        .unwrap();
    let (head_id, head) = published.head().unwrap();
    let mut next = ManifestGeneration {
        generation: head_id + 1,
        footer_mirror: None,
        ..head
    };
    next.files.push(active_entry(built, first, last));
    published.put_generation(next).unwrap();
    published.advance_head(head_id, head_id + 1).unwrap();
}

#[test]
fn read_event_rows_returns_what_was_built() {
    let rows = rows_of(1..=5);
    let built = build_oe_file(rows.clone(), &build_config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_eq!(read_event_rows(&file).unwrap(), rows);
}

#[test]
fn compaction_merges_small_files_retires_inputs_and_the_sweeper_deletes_them() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let clock = FixedClock::at(START_NANOS);
    let inputs = [
        build_oe_file(rows_of(1..=3), &build_config()).unwrap(),
        build_oe_file(rows_of(4..=5), &build_config()).unwrap(),
        build_oe_file(rows_of(6..=8), &build_config()).unwrap(),
    ];
    publish_built(&inputs[0], 1, 3, &mut published, &objects);
    publish_built(&inputs[1], 4, 5, &mut published, &objects);
    publish_built(&inputs[2], 6, 8, &mut published, &objects);
    let input_ids: Vec<u128> = inputs.iter().map(|built| built.file_id).collect();

    let (before_id, head) = published.head().unwrap();
    let jobs = plan_compaction_cycle(&CompactionPolicy::default(), &head, tenant());
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].file_ids, input_ids);

    let mut publisher = OePublisher::default();
    let mut notices = NoopPeerNotices::default();
    let output = execute_compaction(
        &mut publisher,
        &jobs[0],
        &build_config(),
        &mut published,
        &objects,
        &mut notices,
        &clock,
    )
    .unwrap();

    assert_eq!(output.generation, before_id + 1);
    let (_, head) = published.head().unwrap();
    let active: Vec<u128> = head.snapshot_files().map(|entry| entry.file_id).collect();
    assert_eq!(active, vec![output.entry.file_id]);
    for input in &input_ids {
        let entry = head.files.iter().find(|entry| entry.file_id == *input).unwrap();
        assert_eq!(entry.part_state, PartState::Outdated);
        assert!(
            head.retirements
                .iter()
                .any(|retirement| retirement.file_id == *input && retirement.generation == output.generation)
        );
    }
    let merged = OeFile::open(output.file_bytes.clone(), Some(&output.entry.file_seal)).unwrap();
    assert_eq!(read_event_rows(&merged).unwrap(), rows_of(1..=8));
    assert_eq!(output.entry.coverage.first_sequence, 1);
    assert_eq!(output.entry.coverage.last_sequence, 8);

    let policy = SweepPolicy {
        in_flight_query_horizon_nanos: 1_000,
        safety_window_nanos: 5_000,
    };
    clock.advance(1_000_000);
    let report = sweep_retired_files(&mut published, &objects, &policy, before_id, &clock).unwrap();
    assert!(report.unreferenced.is_empty() && report.deleted.is_empty());
    for input in &input_ids {
        assert!(objects.object(&oe_object_key(tenant(), *input)).is_some());
    }
    let (head_id, _) = published.head().unwrap();
    let report = sweep_retired_files(&mut published, &objects, &policy, head_id, &clock).unwrap();
    assert_eq!(report.unreferenced.len(), 3);
    assert!(report.deleted.is_empty());
    clock.advance(4_000);
    let (head_id, _) = published.head().unwrap();
    let report = sweep_retired_files(&mut published, &objects, &policy, head_id, &clock).unwrap();
    assert!(report.deleted.is_empty(), "still inside the safety window");
    clock.advance(1_000);
    let (head_id, _) = published.head().unwrap();
    let report = sweep_retired_files(&mut published, &objects, &policy, head_id, &clock).unwrap();
    assert_eq!(report.deleted.len(), 3);
    for input in &input_ids {
        assert!(objects.object(&oe_object_key(tenant(), *input)).is_none());
    }
    let (_, head) = published.head().unwrap();
    assert_eq!(head.files.len(), 1);
    assert!(objects.object(&oe_object_key(tenant(), output.entry.file_id)).is_some());
}

#[test]
fn compaction_refuses_an_input_that_is_no_longer_active() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let a = build_oe_file(rows_of(1..=2), &build_config()).unwrap();
    publish_built(&a, 1, 2, &mut published, &objects);
    let job = CompactionJob {
        expected_output_bytes: 0,
        file_ids: vec![a.file_id, 77],
    };
    let (before, _) = published.head().unwrap();
    let result = execute_compaction(
        &mut OePublisher::default(),
        &job,
        &build_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &FixedClock::at(START_NANOS),
    );
    assert!(matches!(result, Err(PublishFailure::Verification(_))));
    assert_eq!(published.head().unwrap().0, before);
}

#[test]
fn compaction_drops_rows_removed_by_saved_deletion_vectors() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let clock = FixedClock::at(START_NANOS);
    let first = build_oe_file(rows_of(1..=3), &build_config()).unwrap();
    let second = build_oe_file(rows_of(4..=5), &build_config()).unwrap();
    publish_built(&first, 1, 3, &mut published, &objects);
    publish_built(&second, 4, 5, &mut published, &objects);

    let (head_id, mut head) = published.head().unwrap();
    head.generation = head_id + 1;
    head.footer_mirror = None;
    assert!(
        head.publish_deletions(
            first.file_id,
            &{
                let mut vector = crate::deletes::DeletionVector::new();
                vector.mark_deleted(1);
                vector
            },
            None
        )
        .unwrap()
        .is_some()
    );
    published.put_generation(head).unwrap();
    published.advance_head(head_id, head_id + 1).unwrap();

    let job = CompactionJob {
        expected_output_bytes: (first.bytes.len() + second.bytes.len()) as u64,
        file_ids: vec![first.file_id, second.file_id],
    };
    let output = execute_compaction(
        &mut OePublisher::default(),
        &job,
        &build_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &clock,
    )
    .unwrap();
    let merged = OeFile::open(output.file_bytes.clone(), Some(&output.entry.file_seal)).unwrap();
    assert_eq!(read_event_rows(&merged).unwrap(), rows_of([1, 3, 4, 5]));
}

use crate::events::provenance::{SignatureScheme, SignedEventProvenance};
use crate::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef};

pub(crate) fn rich_row(i: u64) -> OeRow {
    let mut row = rows_of([i]).remove(0);
    if i.is_multiple_of(2) {
        row.event.provenance = Some(SignedEventProvenance {
            author_pubkey: [i as u8; 32],
            claimed_at: TimestampValue::from_physical_nanos(5_000 + i as i64),
            protocol_event_id: [0xA0 ^ i as u8; 32],
            protocol_kind: 1 + i as u32,
            scheme: if i.is_multiple_of(4) {
                SignatureScheme::Ed25519
            } else {
                SignatureScheme::Bip340SchnorrSecp256k1
            },
            signature: [0x5A ^ i as u8; 64],
        });
    }
    if i.is_multiple_of(3) {
        let refs = RelationshipKind::ALL
            .into_iter()
            .flat_map(|kind| {
                let repeat = if matches!(kind, RelationshipKind::Parent | RelationshipKind::Root) {
                    1
                } else {
                    2
                };
                (0..repeat).map(move |n| RelationshipRef::to_event(kind, 0x1000 + u128::from(i) * 10 + n))
            })
            .collect();
        row.event.relationships = Some(EventRelationships::new(refs).unwrap());
    }
    if !i.is_multiple_of(4) {
        row.event.connector_delivery_hash_high = 0xFEED_0000 + i;
        row.event.connector_delivery_hash_low = u64::MAX - i;
        row.event.source_schema = (i % 2 == 1).then(|| format!("protobuf:sha256:{i:04x}"));
        row.event.source_delivery = Some(format!("delivery-{i}"));
    } else {
        row.event.connector_delivery_hash_low = 0;
        row.event.connector_delivery_hash_high = 0;
    }
    row
}

fn lineage_config() -> OeBuildConfig {
    OeBuildConfig {
        delivery_lineage: true,
        targets: LayoutTargets {
            index_granularity: 3,
            ..LayoutTargets::default()
        },
        ..build_config()
    }
}

fn open_built(built: &BuiltOe) -> OeFile {
    OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap()
}

#[test]
fn every_event_column_kind_round_trips_through_read_event_rows_and_a_rebuild() {
    let rows: Vec<OeRow> = (1..=13).map(rich_row).collect();
    let built = build_oe_file(rows.clone(), &lineage_config()).unwrap();
    assert!(built.footer.granules.len() > 1, "rows span several granules");
    assert_ne!(
        built.footer.optional_feature_flags & crate::layout::optional_features::DELIVERY_LINEAGE,
        0
    );
    let read = read_event_rows(&open_built(&built)).unwrap();
    assert_eq!(read, rows);

    let rebuilt = build_oe_file(read, &lineage_config()).unwrap();
    assert_eq!(read_event_rows(&open_built(&rebuilt)).unwrap(), rows);
    assert_eq!(rebuilt.footer.columns, built.footer.columns);
}

#[test]
fn delivery_lineage_is_stored_only_when_the_build_opts_in() {
    let rows: Vec<OeRow> = (1..=5).map(rich_row).collect();
    let built = build_oe_file(rows.clone(), &build_config()).unwrap();
    assert_eq!(
        built.footer.optional_feature_flags & crate::layout::optional_features::DELIVERY_LINEAGE,
        0
    );
    assert!(
        !built
            .footer
            .columns
            .iter()
            .any(|column| { crate::writer::source_form::is_lineage_column(column.column_id) })
    );
    let read = read_event_rows(&open_built(&built)).unwrap();
    for (read, original) in read.iter().zip(&rows) {
        assert_eq!(read.event.connector_delivery_hash_low, 0);
        assert_eq!(read.event.source_delivery, None);
        assert_eq!(read.event.provenance, original.event.provenance);
        assert_eq!(read.event.relationships, original.event.relationships);
    }
}

#[test]
fn source_form_round_trips_through_read_build_rows_and_is_refused_as_event_rows() {
    use crate::writer::build::{BuildInput, BuildRow};
    let mut rows: Vec<BuildRow> = OeRow::into_build_rows((1..=7).map(rich_row).collect());
    for (index, row) in rows.iter_mut().enumerate() {
        if index % 2 == 0 {
            row.external_id = Some(format!("${index}:example.org").into_bytes());
        }
        if index % 3 == 0 {
            row.raw_payload = Some(format!("{{\"n\":{index}}}").into_bytes());
        }
    }
    let built = build_oe_file(rows.clone(), &lineage_config()).unwrap();
    let file = open_built(&built);
    let read = read_build_rows(&file).unwrap();
    assert_eq!(read, rows);
    assert!(matches!(read_event_rows(&file), Err(FormatError::Structural { .. })));

    let rebuilt = build_oe_file(read, &lineage_config()).unwrap();
    assert_eq!(read_build_rows(&open_built(&rebuilt)).unwrap(), rows);
    assert_eq!(rebuilt.footer.external_ids, built.footer.external_ids);
}

#[test]
fn compaction_carries_provenance_relationships_and_lineage_over() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let first = build_oe_file((1..=4).map(rich_row).collect::<Vec<_>>(), &lineage_config()).unwrap();
    let second = build_oe_file((5..=9).map(rich_row).collect::<Vec<_>>(), &lineage_config()).unwrap();
    publish_built(&first, 1, 4, &mut published, &objects);
    publish_built(&second, 5, 9, &mut published, &objects);
    let job = CompactionJob {
        expected_output_bytes: 0,
        file_ids: vec![first.file_id, second.file_id],
    };
    let output = execute_compaction(
        &mut OePublisher::default(),
        &job,
        &lineage_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &FixedClock::at(START_NANOS),
    )
    .unwrap();
    let merged = OeFile::open(output.file_bytes, Some(&output.entry.file_seal)).unwrap();
    assert_eq!(
        read_event_rows(&merged).unwrap(),
        (1..=9).map(rich_row).collect::<Vec<_>>()
    );
}

fn body_redaction(ordinals: &[u64]) -> crate::deletes::FieldDeletionVector {
    let mut vector = crate::deletes::FieldDeletionVector::new(vec!["body".to_owned()]);
    ordinals.iter().for_each(|&ordinal| vector.mark_affected(ordinal));
    vector
}

#[test]
fn read_build_rows_redacted_nulls_redacted_fields_and_drops_the_raw_payload() {
    use crate::field_deletion_store::FieldRedactions;
    use crate::writer::build::{BuildInput, BuildRow};
    let mut rows: Vec<BuildRow> = OeRow::into_build_rows((1..=3).map(rich_row).collect());
    for (index, row) in rows.iter_mut().enumerate() {
        row.raw_payload = Some(format!("{{\"n\":{index}}}").into_bytes());
    }
    let file = open_built(&build_oe_file(rows, &lineage_config()).unwrap());
    let redactions = FieldRedactions {
        vectors: vec![body_redaction(&[1])],
    };
    let plain = read_build_rows(&file).unwrap();
    let read = read_build_rows_redacted(&file, &redactions).unwrap();
    assert_eq!(read[0], plain[0]);
    assert_eq!(read[2], plain[2]);
    assert!(read[1].raw_payload.is_none() && plain[1].raw_payload.is_some());
    assert_eq!(file.raw_payload_redacted(1, &redactions).unwrap(), None);
    assert!(file.raw_payload_redacted(0, &redactions).unwrap().is_some());
    let PayloadRead::Value(VariantValue::Object(fields)) = file.payload_redacted(1, &redactions).unwrap() else {
        panic!("an object payload");
    };
    if let Some(body) = fields.get("body") {
        assert_eq!(body, &VariantValue::Null);
    }
    assert_ne!(file.payload_redacted(0, &redactions).unwrap(), PayloadRead::None);
}

#[test]
fn compaction_applies_field_redactions_and_external_deletion_vectors() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let clock = FixedClock::at(START_NANOS);
    let first = build_oe_file(rows_of(1..=3), &build_config()).unwrap();
    let second = build_oe_file(rows_of(4..=5), &build_config()).unwrap();
    publish_built(&first, 1, 3, &mut published, &objects);
    publish_built(&second, 4, 5, &mut published, &objects);

    let (head_id, mut head) = published.head().unwrap();
    head.generation = head_id + 1;
    head.footer_mirror = None;
    let mut deleted = crate::deletes::DeletionVector::new();
    deleted.mark_deleted(1);
    head.publish_deletions_with_limit(first.file_id, &deleted, None, &objects, 0)
        .unwrap();
    assert!(head.files[0].deletion_vector.as_ref().unwrap().is_external());
    head.publish_field_deletions(first.file_id, &body_redaction(&[2]))
        .unwrap();
    head.publish_field_deletions(second.file_id, &body_redaction(&[0]))
        .unwrap();
    published.put_generation(head.clone()).unwrap();
    published.advance_head(head_id, head_id + 1).unwrap();

    let resolver = crate::corrections::CorrectionResolver::new([]);
    let manifest_deletes = crate::deletion_store::ManifestDeletes {
        corrections: &resolver,
        manifest: &head,
        objects: Some(&objects),
    };
    let first_file = open_built(&first);
    let scan = |low: u64, file: &OeFile, deletes: &dyn crate::layout::entity_scan::DeletesAndCorrections| {
        crate::layout::entity_scan::EntityScan {
            direction: crate::layout::footer::SortDirection::Ascending,
            entity_id_hash_high: 1,
            entity_id_hash_low: low,
            limit: None,
            sequence_range: crate::events::SequencePoint { epoch: 0, sequence: 0 }..=crate::events::SequencePoint {
                epoch: u64::MAX,
                sequence: u64::MAX,
            },
            tenant_id: tenant(),
        }
        .run(&[file], &crate::artifacts::overlay::LiveOverlayStore::new(), deletes)
        .unwrap()
    };
    let redacted = scan(3, &first_file, &manifest_deletes);
    assert_eq!(redacted.len(), 1);
    let PayloadRead::Value(VariantValue::Object(fields)) = &redacted[0].payload else {
        panic!("an object payload");
    };
    assert_eq!(fields.get("body"), Some(&VariantValue::Null));
    assert_eq!(fields.get("n"), Some(&VariantValue::Int(3)));
    assert!(
        scan(2, &first_file, &manifest_deletes).is_empty(),
        "the external deletion vector drops the row"
    );
    let untouched = scan(1, &first_file, &manifest_deletes);
    assert_eq!(untouched[0].payload, PayloadRead::Value(payload_of(1)));

    let job = CompactionJob {
        expected_output_bytes: (first.bytes.len() + second.bytes.len()) as u64,
        file_ids: vec![first.file_id, second.file_id],
    };
    let output = execute_compaction(
        &mut OePublisher::default(),
        &job,
        &build_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &clock,
    )
    .unwrap();
    assert!(output.entry.deletion_vector.is_none());
    assert!(output.entry.field_deletion_vectors.is_empty());
    let merged = OeFile::open(output.file_bytes.clone(), Some(&output.entry.file_seal)).unwrap();
    let rows = read_event_rows(&merged).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
        vec![1, 3, 4, 5]
    );
    for row in &rows {
        let PayloadInput::Variant(VariantValue::Object(fields)) = &row.event.payload else {
            panic!("an object payload");
        };
        let expected_body = if matches!(row.sequence, 3 | 4) {
            VariantValue::Null
        } else {
            VariantValue::String(format!("message {}", row.sequence))
        };
        assert_eq!(fields.get("body"), Some(&expected_body), "sequence {}", row.sequence);
    }
}

use crate::layout::graph::{EdgeKind, GraphInput, RowEdges};
use crate::writer::build::BuildInput;

const GRAPH_SUBJECT: &[u8] = b"!room:example.org";

fn graph_id(old_sn: u64) -> Vec<u8> {
    format!("${old_sn}:example.org").into_bytes()
}

fn graph_file(first: u64, end: u64) -> BuiltOe {
    let mut rows = OeRow::into_build_rows(rows_of(first + 1..=end));
    let mut graph = GraphInput {
        first_sn: first,
        subject: GRAPH_SUBJECT.to_vec(),
        ..GraphInput::default()
    };
    for (sn, row) in (first..).zip(&mut rows) {
        row.external_id = Some(graph_id(sn));
        let mut refs = Vec::new();
        let mut edges = RowEdges::default();
        if sn > 0 {
            refs.push(RelationshipRef::to_subject_sequence(RelationshipKind::Auth, 0));
            refs.push(RelationshipRef::to_subject_sequence(RelationshipKind::Prev, sn - 1));
            edges.auth.push(0);
            edges.prev.push(sn - 1);
        }
        row.relationships = (!refs.is_empty()).then(|| EventRelationships::new(refs).unwrap());
        graph.rows.push(edges);
    }
    let config = OeBuildConfig {
        graph: Some(graph),
        profile: Profile::Graph,
        ..build_config()
    };
    build_oe_file(rows, &config).unwrap()
}

fn delete_rows(published: &mut SimulatedPublishedSet, file_id: u128, ordinals: &[u64]) {
    let (head_id, mut head) = published.head().unwrap();
    head.generation = head_id + 1;
    head.footer_mirror = None;
    let mut vector = crate::deletes::DeletionVector::new();
    for &ordinal in ordinals {
        vector.mark_deleted(ordinal);
    }
    assert!(head.publish_deletions(file_id, &vector, None).unwrap().is_some());
    published.put_generation(head).unwrap();
    published.advance_head(head_id, head_id + 1).unwrap();
}

fn compact(published: &mut SimulatedPublishedSet, objects: &SimObjectStore, file_ids: Vec<u128>) -> Published {
    execute_compaction(
        &mut OePublisher::default(),
        &CompactionJob {
            expected_output_bytes: 0,
            file_ids,
        },
        &build_config(),
        published,
        objects,
        &mut NoopPeerNotices::default(),
        &FixedClock::at(START_NANOS),
    )
    .unwrap()
}

fn active_graph_files(published: &SimulatedPublishedSet, objects: &SimObjectStore) -> Vec<(OeFileEntry, OeFile)> {
    let (_, head) = published.head().unwrap();
    let mut entries: Vec<OeFileEntry> = head
        .files
        .iter()
        .filter(|entry| entry.part_state == PartState::Active && entry.graph_summary.is_some())
        .cloned()
        .collect();
    entries.sort_by_key(|entry| entry.graph_summary.as_ref().unwrap().min_event_sn);
    entries
        .into_iter()
        .map(|entry| {
            let stored = objects.get(&oe_object_key(tenant(), entry.file_id)).unwrap().unwrap();
            let file = OeFile::open(stored.bytes, Some(&entry.file_seal)).unwrap();
            (entry, file)
        })
        .collect()
}

fn edges_of(files: &[(OeFileEntry, OeFile)], sn: u64, kind: EdgeKind) -> (Vec<u64>, Vec<u64>) {
    let (_, file) = files
        .iter()
        .find(|(entry, _)| {
            let summary = entry.graph_summary.as_ref().unwrap();
            (summary.min_event_sn..=summary.max_event_sn).contains(&sn)
        })
        .unwrap();
    let edges = file.granule_edges(file.graph_granule_of(sn).unwrap()).unwrap();
    (edges.parents(sn, kind).to_vec(), edges.children(sn, kind))
}

fn assert_chain(files: &[(OeFileEntry, OeFile)], old_of: &[u64]) {
    let count = old_of.len() as u64;
    for (new, &old) in (0..count).zip(old_of) {
        let holder = files
            .iter()
            .find(|(entry, _)| {
                let summary = entry.graph_summary.as_ref().unwrap();
                (summary.min_event_sn..=summary.max_event_sn).contains(&new)
            })
            .unwrap();
        assert_eq!(
            holder.1.graph_sn_of(&graph_id(old)).unwrap(),
            Some(new),
            "id of old {old}"
        );
        let (prev, prev_children) = edges_of(files, new, EdgeKind::Prev);
        let expected_prev: Vec<u64> = new.checked_sub(1).into_iter().collect();
        assert_eq!(prev, expected_prev, "prev of {new}");
        let expected_children: Vec<u64> = (new + 1 < count).then_some(new + 1).into_iter().collect();
        assert_eq!(prev_children, expected_children, "prev children of {new}");
        let (auth, auth_children) = edges_of(files, new, EdgeKind::Auth);
        assert_eq!(auth, if new == 0 { vec![] } else { vec![0] }, "auth of {new}");
        if new == 0 {
            assert_eq!(auth_children, (1..count).collect::<Vec<_>>());
        }
        let row_ordinal = holder.1.graph_row_of(new).unwrap();
        let row = read_build_rows(&holder.1).unwrap().remove(row_ordinal as usize);
        let refs: Vec<RelationshipRef> = row.relationships.map(|r| r.refs().to_vec()).unwrap_or_default();
        let mut expected = Vec::new();
        if new > 0 {
            expected.push(RelationshipRef::to_subject_sequence(RelationshipKind::Auth, 0));
            expected.push(RelationshipRef::to_subject_sequence(RelationshipKind::Prev, new - 1));
        }
        assert_eq!(refs, expected, "relationships of {new}");
    }
}

#[test]
fn graph_compaction_with_a_deleted_mid_chain_event_renumbers_the_merged_range_and_later_files() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let (a, b, c) = (graph_file(0, 4), graph_file(4, 9), graph_file(9, 12));
    publish_built(&a, 1, 4, &mut published, &objects);
    publish_built(&b, 5, 9, &mut published, &objects);
    publish_built(&c, 10, 12, &mut published, &objects);
    delete_rows(&mut published, b.file_id, &[1]);

    let output = compact(&mut published, &objects, vec![a.file_id, b.file_id]);
    let summary = output.entry.graph_summary.clone().unwrap();
    assert_eq!((summary.min_event_sn, summary.max_event_sn), (0, 7));
    assert!(output.entry.deletion_vector.is_none());

    let (_, head) = published.head().unwrap();
    for id in [a.file_id, b.file_id, c.file_id] {
        let entry = head.files.iter().find(|entry| entry.file_id == id).unwrap();
        assert_eq!(entry.part_state, PartState::Outdated);
        assert!(
            head.retirements
                .iter()
                .any(|r| r.file_id == id && r.generation == output.generation)
        );
    }
    let files = active_graph_files(&published, &objects);
    assert_eq!(files.len(), 2);
    let later = files[1].0.graph_summary.clone().unwrap();
    assert_eq!((later.min_event_sn, later.max_event_sn), (8, 10));
    assert_eq!(files[1].0.coverage.first_sequence, 10);
    assert_eq!(files[0].1.graph_sn_of(&graph_id(5)).unwrap(), None);
    assert_chain(&files, &[0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11]);
}

#[test]
fn graph_renumbering_rewrites_an_earlier_file_whose_inbound_edges_name_renumbered_children() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let (a, b, c) = (graph_file(0, 4), graph_file(4, 9), graph_file(9, 12));
    publish_built(&a, 1, 4, &mut published, &objects);
    publish_built(&b, 5, 9, &mut published, &objects);
    publish_built(&c, 10, 12, &mut published, &objects);
    let ab = compact(&mut published, &objects, vec![a.file_id, b.file_id]);
    delete_rows(&mut published, c.file_id, &[0]);
    let output = compact(&mut published, &objects, vec![c.file_id]);
    let summary = output.entry.graph_summary.clone().unwrap();
    assert_eq!((summary.min_event_sn, summary.max_event_sn), (9, 10));

    let (_, head) = published.head().unwrap();
    let ab_entry = head
        .files
        .iter()
        .find(|entry| entry.file_id == ab.entry.file_id)
        .unwrap();
    assert_eq!(
        ab_entry.part_state,
        PartState::Outdated,
        "the earlier file was rewritten"
    );
    let files = active_graph_files(&published, &objects);
    assert_eq!(files.len(), 2);
    assert_chain(&files, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11]);
}

#[test]
fn graph_renumbering_refuses_an_output_with_no_surviving_event() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let (a, b) = (graph_file(0, 4), graph_file(4, 6));
    publish_built(&a, 1, 4, &mut published, &objects);
    publish_built(&b, 5, 6, &mut published, &objects);
    delete_rows(&mut published, b.file_id, &[0, 1]);
    let result = execute_compaction(
        &mut OePublisher::default(),
        &CompactionJob {
            expected_output_bytes: 0,
            file_ids: vec![b.file_id],
        },
        &build_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &FixedClock::at(START_NANOS),
    );
    assert!(matches!(result, Err(PublishFailure::Format(_))), "{result:?}");
}

#[test]
fn elided_external_ids_round_trip_through_read_build_rows() {
    use crate::layout::required_features;
    use crate::writer::build::BuildRow;
    let mut rows: Vec<BuildRow> = OeRow::into_build_rows((1..=8).map(rich_row).collect());
    for (index, row) in rows.iter_mut().enumerate() {
        let event_id = row.envelope.event_id.uuid().as_u128();
        row.external_id = match index % 3 {
            0 => Some(event_id.to_be_bytes().to_vec()),
            1 => Some(format!("{event_id:032x}").into_bytes()),
            _ => Some(format!("${index}:example.org").into_bytes()),
        };
    }
    let built = build_oe_file(rows.clone(), &lineage_config()).unwrap();
    assert_ne!(
        built.footer.required_feature_flags & required_features::EXTERNAL_ID_EVENT_ID_ELISION,
        0
    );
    let read = read_build_rows(&open_built(&built)).unwrap();
    assert_eq!(read, rows, "merge reads expand every sentinel");

    let rebuilt = build_oe_file(read, &lineage_config()).unwrap();
    assert_eq!(read_build_rows(&open_built(&rebuilt)).unwrap(), rows);
    assert_eq!(rebuilt.footer.external_ids, built.footer.external_ids);
    assert_ne!(
        rebuilt.footer.required_feature_flags & required_features::EXTERNAL_ID_EVENT_ID_ELISION,
        0,
        "the rewrite elides the same ids again"
    );
}

#[test]
fn elided_entity_hashes_round_trip_through_read_build_rows() {
    use crate::layout::required_features;
    use crate::writer::build::BuildRow;
    let mut rows: Vec<BuildRow> = OeRow::into_build_rows((1..=8).map(rich_row).collect());
    for row in &mut rows {
        let (low, high) = crate::events::derived_entity_id_hash(row.envelope.entity_id.as_deref());
        row.envelope.entity_id_hash_low = low;
        row.envelope.entity_id_hash_high = high;
    }
    let built = build_oe_file(rows.clone(), &lineage_config()).unwrap();
    assert_ne!(
        built.footer.required_feature_flags & required_features::ENTITY_HASH_COLUMNS_ELIDED,
        0
    );
    let read = read_build_rows(&open_built(&built)).unwrap();
    assert_eq!(read, rows, "merge reads derive every entity hash");
    let rebuilt = build_oe_file(read, &lineage_config()).unwrap();
    assert_ne!(
        rebuilt.footer.required_feature_flags & required_features::ENTITY_HASH_COLUMNS_ELIDED,
        0
    );
    assert_eq!(read_build_rows(&open_built(&rebuilt)).unwrap(), rows);
}
