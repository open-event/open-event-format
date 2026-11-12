use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput, build_batch, decode_batch};
use crate::artifacts::frame::{FrameBuildInput, build_frame, decode_frame};
use crate::artifacts::overlay::convert_frame;
use crate::columns::{FreetextDeclaration, PromotionPlan};
use crate::events::TenantId;
use crate::events::variant::VariantValue;
use crate::layout::LayoutTargets;
use crate::security::FooterEncryption;
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, build_oe_file};
use std::collections::BTreeMap;

fn tenant() -> TenantId {
    TenantId::new_test_id(3)
}

fn payload(sequence: u64) -> VariantValue {
    VariantValue::Object(BTreeMap::from([
        ("n".to_owned(), VariantValue::Int(sequence as i64)),
        ("text".to_owned(), VariantValue::String(format!("message {sequence}"))),
    ]))
}

fn event(sequence: u64) -> EventInput {
    EventInput {
        connector_delivery_hash_high: 0,
        connector_delivery_hash_low: sequence,
        envelope: EventEnvelope {
            account_id: sequence.is_multiple_of(3).then(|| format!("account-{sequence}")),
            account_id_hash_low: 40 + sequence,
            actor_id: sequence.is_multiple_of(2).then(|| format!("actor-{sequence}")),
            actor_id_hash_low: 30 + sequence,
            dedupe_hash_high: 6,
            dedupe_hash_low: sequence,
            entity_id: Some(format!("room-{}", sequence % 4)),
            entity_id_hash_high: 9,
            entity_id_hash_low: 100 + sequence % 4,
            entity_type: "room".to_owned(),
            event_id: EventId::new_test_id(0xE000 + u128::from(sequence)),
            event_type: if sequence.is_multiple_of(2) {
                "message.sent"
            } else {
                "message.edited"
            }
            .to_owned(),
            flags: EventFlags(0),
            ingested_at: TimestampValue::from_physical_nanos(2_000 + sequence as i64),
            occurred_at: TimestampValue::from_physical_nanos(1_000 + sequence as i64),
            schema_version: 2,
            source: if sequence.is_multiple_of(5) { "mobile" } else { "web" }.to_owned(),
            stream_id: StreamId(1),
            stream_sequence: sequence,
            tenant_id: tenant(),
            trace_id_hash_low: 5,
        },
        payload: PayloadInput::Variant(payload(sequence)),
        provenance: None,
        relationships: None,
        source_delivery: None,
        source_schema: None,
    }
}

fn config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 0,
        footer_dek: None,
        footer_encryption: FooterEncryption::Plaintext,
        freetext: FreetextDeclaration { fields: Vec::new() },
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan::default(),
        targets: LayoutTargets {
            index_granularity: 8,
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

#[test]
fn a_stored_row_reads_back_as_the_event_that_was_written() {
    let rows: Vec<OeRow> = (1..=30)
        .map(|sequence| OeRow {
            epoch: 1,
            event: event(sequence),
            sequence,
        })
        .collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert!(file.footer().granules.len() > 1, "the rows span several granules");

    for (ordinal, row) in rows.iter().enumerate() {
        let ordinal = ordinal as u64;
        assert_eq!(file.envelope(ordinal).unwrap(), row.event.envelope, "row {ordinal}");
        let (envelope, read) = file.event(ordinal).unwrap();
        assert_eq!(envelope, row.event.envelope);
        assert_eq!(read, PayloadRead::Value(payload(row.sequence)));
    }
    assert!(
        file.envelope(rows.len() as u64).is_err(),
        "a row past the end is refused"
    );
}

#[test]
fn an_overlay_row_reads_back_as_the_event_that_was_written() {
    let events: Vec<EventInput> = (41..=45).map(event).collect();
    let batch = build_batch(&events, 1, 0).unwrap();
    let frame = build_frame(
        &FrameBuildInput {
            committed_at_physical: 2,
            created_at_physical: 1,
            dictionary_generation_hint: 0,
            durable_batch_id: 1,
            epoch: 1,
            event_count: events.len() as u32,
            first_sequence: 41,
            flags: 0,
            last_sequence: 45,
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

    for (row, input) in events.iter().enumerate() {
        let (envelope, read) = segment.event(row).unwrap();
        assert_eq!(envelope, input.envelope, "row {row}");
        assert_eq!(read, PayloadRead::Value(payload(41 + row as u64)));
    }
    assert!(segment.event(events.len()).is_err(), "a row past the end is refused");
}

#[test]
fn a_single_row_envelope_matches_its_granule_envelopes() {
    let rows: Vec<OeRow> = (1..=30)
        .map(|sequence| OeRow {
            epoch: 1,
            event: event(sequence),
            sequence,
        })
        .collect();
    let built = build_oe_file(rows, &config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    for granule in &file.footer().granules {
        let bulk = file.granule_envelopes(granule.granule_id).unwrap();
        assert_eq!(bulk.len(), granule.row_count as usize);
        for (index, envelope) in bulk.iter().enumerate() {
            let ordinal = granule.first_row_ordinal + index as u64;
            assert_eq!(&file.envelope(ordinal).unwrap(), envelope, "row {ordinal}");
        }
    }
}

#[test]
fn string_at_maps_dense_and_presence_mapped_values() {
    let mut dense = StringColumn::new();
    for value in [Some("a"), None, Some("c")] {
        dense.push(value);
    }
    let dense = IdColumn::new((vec![0b101], dense), 3);
    assert_eq!(dense.at(0).as_deref(), Some("a"));
    assert_eq!(dense.at(1), None);
    assert_eq!(dense.at(2).as_deref(), Some("c"));
    let mut sparse = StringColumn::new();
    for value in [Some("b"), Some("d")] {
        sparse.push(value);
    }
    let sparse = IdColumn::new((vec![0b1010], sparse), 4);
    let read: Vec<_> = (0..5).map(|row| sparse.at(row)).collect();
    assert_eq!(read, [None, Some("b".to_owned()), None, Some("d".to_owned()), None]);
}

#[test]
fn per_row_envelope_and_event_id_reads_decode_each_granule_once() {
    let rows: Vec<OeRow> = (1..=30)
        .map(|sequence| OeRow {
            epoch: 1,
            event: event(sequence),
            sequence,
        })
        .collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let granule = file.footer().granules[0];
    let first = granule.first_row_ordinal;
    let count = u64::from(granule.row_count);
    assert!(count > 1, "the first granule holds several rows");

    assert_eq!(file.envelope(first).unwrap(), rows[first as usize].event.envelope);
    let reads = file.column_block_reads();
    for ordinal in first..first + count {
        assert_eq!(file.envelope(ordinal).unwrap(), rows[ordinal as usize].event.envelope);
    }
    assert_eq!(
        file.column_block_reads(),
        reads,
        "envelope columns decoded once per granule"
    );

    let expected = rows[first as usize].event.envelope.event_id.uuid().as_u128();
    assert_eq!(crate::security::row_event_id(&file, first).unwrap(), expected);
    let reads = file.column_block_reads();
    for ordinal in first..first + count {
        let expected = rows[ordinal as usize].event.envelope.event_id.uuid().as_u128();
        assert_eq!(crate::security::row_event_id(&file, ordinal).unwrap(), expected);
    }
    assert_eq!(
        file.column_block_reads(),
        reads,
        "the event id column decoded once per granule"
    );
}
