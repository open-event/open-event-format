use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput, build_batch, decode_batch};
use crate::artifacts::frame::{FrameBuildInput, build_frame, decode_frame};
use crate::artifacts::overlay::convert_frame;
use crate::columns::{FreetextDeclaration, PromotionPlan};
use crate::deletes::latest_corrected_deletion_vector;
use crate::events::{EventFlags, StreamId, TimestampValue};
use crate::layout::LayoutTargets;
use crate::security::FooterEncryption;
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, build_oe_file};
use std::collections::BTreeMap;

const ROOM_A: u64 = 100;
const ROOM_B: u64 = 200;
const ROOM_HASH_HIGH: u64 = 9;

fn tenant() -> TenantId {
    TenantId::new_test_id(3)
}

fn room_of(sequence: u64) -> u64 {
    if sequence.is_multiple_of(2) { ROOM_A } else { ROOM_B }
}

fn event_id(sequence: u64) -> EventId {
    EventId::new_test_id(0xE000 + u128::from(sequence))
}

fn payload(sequence: u64) -> BTreeMap<String, VariantValue> {
    BTreeMap::from([
        ("n".to_owned(), VariantValue::Int(sequence as i64)),
        ("text".to_owned(), VariantValue::String(format!("message {sequence}"))),
    ])
}

fn event(sequence: u64, payload: BTreeMap<String, VariantValue>) -> EventInput {
    EventInput {
        connector_delivery_hash_high: 0,
        connector_delivery_hash_low: sequence,
        envelope: EventEnvelope {
            account_id: None,
            account_id_hash_low: 4,
            actor_id: None,
            actor_id_hash_low: 3,
            dedupe_hash_high: 6,
            dedupe_hash_low: sequence,
            entity_id: Some(format!("room-{}", room_of(sequence))),
            entity_id_hash_high: ROOM_HASH_HIGH,
            entity_id_hash_low: room_of(sequence),
            entity_type: "room".to_owned(),
            event_id: event_id(sequence),
            event_type: "message.sent".to_owned(),
            flags: EventFlags(0),
            ingested_at: TimestampValue::from_physical_nanos(2_000 + sequence as i64),
            occurred_at: TimestampValue::from_physical_nanos(1_000 + sequence as i64),
            schema_version: 1,
            source: "chat".to_owned(),
            stream_id: StreamId(1),
            stream_sequence: sequence,
            tenant_id: tenant(),
            trace_id_hash_low: 5,
        },
        payload: PayloadInput::Variant(VariantValue::Object(payload)),
        provenance: None,
        relationships: None,
        source_delivery: None,
        source_schema: None,
    }
}

fn config(generation_id: u64) -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 0,
        footer_dek: None,
        footer_encryption: FooterEncryption::Plaintext,
        freetext: FreetextDeclaration { fields: Vec::new() },
        freetext_row_offset_index: false,
        generation_id,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan::default(),
        targets: LayoutTargets {
            index_granularity: 4,
            index_granularity_bytes: 1 << 20,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
            stripe_target_bytes: 4096,
        },
        tenant_id: tenant(),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

fn file(
    first: u64,
    last: u64,
    generation_id: u64,
    payload_of: impl Fn(u64) -> BTreeMap<String, VariantValue>,
) -> OeFile {
    let rows: Vec<OeRow> = (first..=last)
        .map(|sequence| OeRow {
            epoch: 1,
            event: event(sequence, payload_of(sequence)),
            sequence,
        })
        .collect();
    let built = build_oe_file(rows, &config(generation_id)).unwrap();
    OeFile::open(built.bytes, Some(&built.file_seal)).unwrap()
}

fn overlay(first: u64, last: u64) -> LiveOverlayStore {
    let events: Vec<EventInput> = (first..=last)
        .map(|sequence| event(sequence, payload(sequence)))
        .collect();
    let batch = build_batch(&events, 1, 0).unwrap();
    let frame = build_frame(
        &FrameBuildInput {
            committed_at_physical: 2,
            created_at_physical: 1,
            dictionary_generation_hint: 0,
            durable_batch_id: 1,
            epoch: 1,
            event_count: events.len() as u32,
            first_sequence: first,
            flags: 0,
            last_sequence: last,
            schema_generation: 1,
            tenant_id: tenant(),
            writer_id: 1,
            writer_local_batch_id: 1,
        },
        &batch,
    )
    .unwrap();
    let (header, body) = decode_frame(&frame).unwrap();
    let decoded = decode_batch(body, header.event_count).unwrap();
    let segment = convert_frame(&header, &decoded, 1, 1, None, |_, _| false)
        .unwrap()
        .unwrap();
    let mut store = LiveOverlayStore::new();
    store.publish(segment);
    store
}

fn scan(direction: SortDirection, limit: Option<usize>) -> EntityScan {
    EntityScan {
        direction,
        entity_id_hash_high: ROOM_HASH_HIGH,
        entity_id_hash_low: ROOM_A,
        limit,
        sequence_range: SequencePoint { epoch: 1, sequence: 0 }..=SequencePoint {
            epoch: 1,
            sequence: u64::MAX,
        },
        tenant_id: tenant(),
    }
}

fn sequences(events: &[EntityEvent]) -> Vec<u64> {
    events.iter().map(|event| event.point.sequence).collect()
}

#[derive(Default)]
struct RecordedDeletes {
    corrections: Vec<CorrectionMetadata>,
    deleted: HashMap<u128, DeletionVector>,
}

impl DeletesAndCorrections for RecordedDeletes {
    fn deletion_vector(
        &self,
        _tenant_id: TenantId,
        file_id: u128,
    ) -> Result<Option<std::sync::Arc<DeletionVector>>, EntityScanError> {
        Ok(self.deleted.get(&file_id).cloned().map(std::sync::Arc::new))
    }

    fn is_correction(&self, _tenant_id: TenantId, point: SequencePoint) -> Result<bool, EntityScanError> {
        Ok(self.corrections.iter().any(|correction| {
            correction.correction_epoch == point.epoch && correction.correction_sequence == point.sequence
        }))
    }

    fn latest_correction(
        &self,
        _tenant_id: TenantId,
        event_id: EventId,
    ) -> Result<Option<CorrectionMetadata>, EntityScanError> {
        Ok(self
            .corrections
            .iter()
            .filter(|correction| correction.corrects_event_id == event_id)
            .max_by_key(|correction| correction.correction_generation)
            .cloned())
    }
}

fn correction(original: u64, correcting: u64, correction_type: CorrectionType) -> CorrectionMetadata {
    CorrectionMetadata {
        correction_epoch: 1,
        correction_generation: 1,
        correction_sequence: correcting,
        correction_type,
        corrects_event_id: event_id(original),
    }
}

#[test]
fn a_scan_spans_two_files_and_the_overlay() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let fresh = overlay(41, 50);

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older, &newer], &fresh, &RecordedDeletes::default())
        .unwrap();

    assert_eq!(sequences(&events), (2..=50).step_by(2).collect::<Vec<_>>());
    for event in &events {
        let sequence = event.point.sequence;
        assert_eq!(event.envelope, self::event(sequence, payload(sequence)).envelope);
        assert_eq!(
            event.payload,
            PayloadRead::Value(VariantValue::Object(payload(sequence)))
        );
    }
}

#[test]
fn the_newest_generation_wins_a_repeated_point() {
    let ingest = file(1, 20, 1, payload);
    let rewritten = file(1, 20, 2, |sequence| {
        BTreeMap::from([("n".to_owned(), VariantValue::Int(1_000 + sequence as i64))])
    });

    let events = scan(SortDirection::Ascending, None)
        .run(
            &[&rewritten, &ingest],
            &LiveOverlayStore::new(),
            &RecordedDeletes::default(),
        )
        .unwrap();

    assert_eq!(sequences(&events), (2..=20).step_by(2).collect::<Vec<_>>());
    for event in &events {
        let rewritten_payload =
            BTreeMap::from([("n".to_owned(), VariantValue::Int(1_000 + event.point.sequence as i64))]);
        assert_eq!(
            event.payload,
            PayloadRead::Value(VariantValue::Object(rewritten_payload))
        );
    }
}

#[test]
fn a_backwards_scan_with_a_limit_returns_the_newest_events() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let fresh = overlay(41, 50);

    let events = scan(SortDirection::Descending, Some(7))
        .run(&[&older, &newer], &fresh, &RecordedDeletes::default())
        .unwrap();

    assert_eq!(sequences(&events), vec![50, 48, 46, 44, 42, 40, 38]);
}

#[test]
fn a_deleted_row_is_skipped() {
    let older = file(1, 20, 1, payload);
    let mut deletes = RecordedDeletes::default();
    let mut vector = DeletionVector::new();
    vector.mark_deleted(3);
    deletes.deleted.insert(older.header().file_id, vector);

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), vec![2, 6, 8, 10, 12, 14, 16, 18, 20]);
}

#[test]
fn a_replacement_correction_is_returned_instead_of_the_original() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let deletes = RecordedDeletes {
        corrections: vec![correction(6, 30, CorrectionType::Replacement)],
        ..RecordedDeletes::default()
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older, &newer], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    let expected: Vec<u64> = (2..=40).step_by(2).filter(|sequence| *sequence != 30).collect();
    assert_eq!(sequences(&events), expected);
    let replaced = events.iter().find(|event| event.point.sequence == 6).unwrap();
    assert_eq!(replaced.envelope.event_id, event_id(30));
    assert_eq!(replaced.payload, PayloadRead::Value(VariantValue::Object(payload(30))));
    assert!(events.iter().all(|event| event.envelope.event_id != event_id(6)));
}

#[test]
fn an_amendment_lays_its_fields_over_the_original() {
    let older = file(1, 20, 1, |sequence| {
        if sequence == 8 {
            BTreeMap::from([("text".to_owned(), VariantValue::String("edited".to_owned()))])
        } else {
            payload(sequence)
        }
    });
    let deletes = RecordedDeletes {
        corrections: vec![correction(4, 8, CorrectionType::Amendment)],
        ..RecordedDeletes::default()
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), vec![2, 4, 6, 10, 12, 14, 16, 18, 20]);
    let amended = events.iter().find(|event| event.point.sequence == 4).unwrap();
    assert_eq!(amended.envelope.event_id, event_id(4));
    let mut expected = payload(4);
    expected.insert("text".to_owned(), VariantValue::String("edited".to_owned()));
    assert_eq!(amended.payload, PayloadRead::Value(VariantValue::Object(expected)));
}

#[test]
fn a_retraction_drops_the_event() {
    let older = file(1, 20, 1, payload);
    let deletes = RecordedDeletes {
        corrections: vec![correction(4, 9, CorrectionType::Retraction)],
        ..RecordedDeletes::default()
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), vec![2, 6, 8, 10, 12, 14, 16, 18, 20]);
}

#[test]
fn a_correction_outside_the_scanned_sources_is_refused() {
    let older = file(1, 20, 1, payload);
    let deletes = RecordedDeletes {
        corrections: vec![correction(4, 99, CorrectionType::Replacement)],
        ..RecordedDeletes::default()
    };

    let refused = scan(SortDirection::Ascending, None).run(&[&older], &LiveOverlayStore::new(), &deletes);

    assert_eq!(
        refused,
        Err(EntityScanError::CorrectionNotFound { epoch: 1, sequence: 99 })
    );
}

fn room_a_without(left_out: &[u64]) -> Vec<u64> {
    (2..=40)
        .step_by(2)
        .filter(|sequence| !left_out.contains(sequence))
        .collect()
}

#[test]
fn a_deleted_replacement_drops_the_event() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let mut vector = DeletionVector::new();
    vector.mark_deleted(9);
    let deletes = RecordedDeletes {
        corrections: vec![correction(6, 30, CorrectionType::Replacement)],
        deleted: [(newer.header().file_id, vector)].into_iter().collect(),
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older, &newer], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), room_a_without(&[6, 30]));
}

#[test]
fn a_chain_of_replacements_serves_the_last() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let deletes = RecordedDeletes {
        corrections: vec![
            correction(6, 30, CorrectionType::Replacement),
            correction(30, 34, CorrectionType::Replacement),
        ],
        ..RecordedDeletes::default()
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older, &newer], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), room_a_without(&[30, 34]));
    let replaced = events.iter().find(|event| event.point.sequence == 6).unwrap();
    assert_eq!(replaced.envelope.event_id, event_id(34));
    assert_eq!(replaced.payload, PayloadRead::Value(VariantValue::Object(payload(34))));
}

#[test]
fn a_chain_of_amendments_applies_in_order() {
    let older = file(1, 20, 1, |sequence| match sequence {
        8 => BTreeMap::from([
            ("tag".to_owned(), VariantValue::String("first".to_owned())),
            ("text".to_owned(), VariantValue::String("first".to_owned())),
        ]),
        12 => BTreeMap::from([("text".to_owned(), VariantValue::String("second".to_owned()))]),
        _ => payload(sequence),
    });
    let deletes = RecordedDeletes {
        corrections: vec![
            correction(4, 8, CorrectionType::Amendment),
            correction(8, 12, CorrectionType::Amendment),
        ],
        ..RecordedDeletes::default()
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), vec![2, 4, 6, 10, 14, 16, 18, 20]);
    let amended = events.iter().find(|event| event.point.sequence == 4).unwrap();
    assert_eq!(amended.envelope.event_id, event_id(4));
    let mut expected = payload(4);
    expected.insert("tag".to_owned(), VariantValue::String("first".to_owned()));
    expected.insert("text".to_owned(), VariantValue::String("second".to_owned()));
    assert_eq!(amended.payload, PayloadRead::Value(VariantValue::Object(expected)));
}

#[test]
fn a_retracted_replacement_drops_the_event() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let deletes = RecordedDeletes {
        corrections: vec![
            correction(6, 30, CorrectionType::Replacement),
            correction(30, 31, CorrectionType::Retraction),
        ],
        ..RecordedDeletes::default()
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older, &newer], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), room_a_without(&[6, 30]));
}

#[test]
fn a_correction_cycle_is_refused() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let deletes = RecordedDeletes {
        corrections: vec![
            correction(6, 30, CorrectionType::Replacement),
            correction(30, 34, CorrectionType::Replacement),
            correction(34, 30, CorrectionType::Replacement),
        ],
        ..RecordedDeletes::default()
    };

    let refused = scan(SortDirection::Ascending, None).run(&[&older, &newer], &LiveOverlayStore::new(), &deletes);

    assert_eq!(
        refused,
        Err(EntityScanError::CorrectionCycle { epoch: 1, sequence: 30 })
    );
}

#[test]
fn a_latest_corrected_deletion_vector_still_serves_the_replacement_once() {
    let older = file(1, 20, 1, payload);
    let newer = file(21, 40, 2, payload);
    let corrections = vec![correction(6, 30, CorrectionType::Replacement)];
    let events_in_older: Vec<(u64, EventId)> = (1..=20).map(|sequence| (sequence - 1, event_id(sequence))).collect();
    let vector = latest_corrected_deletion_vector(&events_in_older, &corrections);
    assert!(vector.is_deleted(5));
    let deletes = RecordedDeletes {
        corrections,
        deleted: [(older.header().file_id, vector)].into_iter().collect(),
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older, &newer], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), room_a_without(&[30]));
    let served: Vec<&EntityEvent> = events
        .iter()
        .filter(|event| event.envelope.event_id == event_id(30))
        .collect();
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].point.sequence, 6);
    assert!(events.iter().all(|event| event.envelope.event_id != event_id(6)));
}

#[test]
fn a_deleted_amended_original_drops_the_event() {
    let older = file(1, 20, 1, payload);
    let mut vector = DeletionVector::new();
    vector.mark_deleted(3);
    let deletes = RecordedDeletes {
        corrections: vec![correction(4, 8, CorrectionType::Amendment)],
        deleted: [(older.header().file_id, vector)].into_iter().collect(),
    };

    let events = scan(SortDirection::Ascending, None)
        .run(&[&older], &LiveOverlayStore::new(), &deletes)
        .unwrap();

    assert_eq!(sequences(&events), vec![2, 6, 10, 12, 14, 16, 18, 20]);
}

#[test]
fn a_file_without_stored_entity_hash_columns_scans_the_same_rows() {
    let derived = |sequence: u64| {
        let mut event = event(sequence, payload(sequence));
        let (low, high) = crate::events::derived_entity_id_hash(event.envelope.entity_id.as_deref());
        event.envelope.entity_id_hash_low = low;
        event.envelope.entity_id_hash_high = high;
        if sequence.is_multiple_of(5) {
            event.envelope.entity_id = None;
            event.envelope.entity_id_hash_low = 0;
            event.envelope.entity_id_hash_high = 0;
        }
        event
    };
    let rows = |first: u64, last: u64| -> Vec<OeRow> {
        (first..=last)
            .map(|sequence| OeRow {
                epoch: 1,
                event: derived(sequence),
                sequence,
            })
            .collect()
    };
    let built = build_oe_file(rows(1, 30), &config(1)).unwrap();
    let elided = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_ne!(
        elided.footer().required_feature_flags & crate::layout::required_features::ENTITY_HASH_COLUMNS_ELIDED,
        0
    );
    assert!(
        elided
            .footer()
            .columns
            .iter()
            .all(|column| column.column_id != column_ids::ENTITY_ID_HASH_LOW
                && column.column_id != column_ids::ENTITY_ID_HASH_HIGH)
    );
    assert!(elided.mark(column_ids::ENTITY_ID_HASH_LOW, 0, 0).unwrap().is_none());
    assert!(!elided.footer().entity_hash_filters.is_empty());

    for (ordinal, sequence) in (1..=30u64).enumerate() {
        assert_eq!(elided.envelope(ordinal as u64).unwrap(), derived(sequence).envelope);
    }

    let mut stored_rows = rows(1, 30);
    if let Some(row) = stored_rows.first_mut() {
        row.event.envelope.entity_id_hash_high = 1;
    }
    let built = build_oe_file(stored_rows, &config(1)).unwrap();
    let stored = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_eq!(
        stored.footer().required_feature_flags & crate::layout::required_features::ENTITY_HASH_COLUMNS_ELIDED,
        0
    );
    assert!(stored.mark(column_ids::ENTITY_ID_HASH_LOW, 0, 0).unwrap().is_some());

    let (low, high) = crate::events::derived_entity_id_hash(Some("room-100"));
    let room_scan = EntityScan {
        entity_id_hash_high: high,
        entity_id_hash_low: low,
        ..scan(SortDirection::Ascending, None)
    };
    let expected: Vec<u64> = (2..=30).step_by(2).filter(|sequence| sequence % 5 != 0).collect();
    for file in [&elided, &stored] {
        let events = room_scan
            .run(&[file], &LiveOverlayStore::new(), &RecordedDeletes::default())
            .unwrap();
        assert_eq!(sequences(&events), expected);
        for event in &events {
            assert_eq!(event.envelope, derived(event.point.sequence).envelope);
        }
    }
    let candidates = elided.granules_for_entity_hash(low);
    assert!(!candidates.is_empty());
    assert!(candidates.len() <= elided.footer().granules.len());
}
