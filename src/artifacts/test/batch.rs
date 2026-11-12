use super::super::frame::{self, FrameBuildInput};
use super::*;
use crate::events::provenance::sha256_digest;
use crate::events::sim::{SimulatedEventAuthor, signed_event_payload};
use crate::events::variant::encode_value;
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::typed_id::TypedIdTestExt;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn sample_events(count: usize) -> Vec<EventInput> {
    (0..count)
        .map(|i| {
            let mut payload_fields = BTreeMap::new();
            payload_fields.insert("kind".to_owned(), VariantValue::String(format!("k{i}")));
            payload_fields.insert("amount".to_owned(), VariantValue::Int(1000 + i as i64));
            EventInput {
                envelope: EventEnvelope {
                    event_id: EventId::new_test_id(0x1000 + i as u128),
                    tenant_id: TenantId::new_test_id(7),
                    stream_id: StreamId(42),
                    stream_sequence: 100 + i as u64,
                    occurred_at: TimestampValue::from_physical_nanos(1_000_000 + i as i64),
                    ingested_at: TimestampValue::from_physical_nanos(2_000_000 + i as i64),
                    source: "crm".to_owned(),
                    event_type: "deal.updated".to_owned(),
                    entity_type: "opportunity".to_owned(),
                    entity_id_hash_low: 11 + i as u64,
                    entity_id_hash_high: 12,
                    entity_id: (i % 2 == 0).then(|| format!("opp-{i}")),
                    actor_id_hash_low: 13,
                    actor_id: None,
                    account_id_hash_low: 14,
                    account_id: Some("acct-1".to_owned()),
                    trace_id_hash_low: 15,
                    dedupe_hash_low: 16 + i as u64,
                    dedupe_hash_high: 17,
                    schema_version: 3,
                    flags: EventFlags(0),
                },
                payload: PayloadInput::Variant(VariantValue::object(payload_fields)),
                source_schema: None,
                source_delivery: Some(format!("delivery-{i}")),
                connector_delivery_hash_low: 21,
                connector_delivery_hash_high: 22,
                provenance: None,
                relationships: None,
            }
        })
        .collect()
}

pub(crate) fn sample_frame(count: usize, epoch: u64, first_sequence: u64) -> Vec<u8> {
    let events = sample_events(count);
    let payload = build_batch(&events, 1, 0).unwrap();
    frame::build_frame(
        &FrameBuildInput {
            flags: 0,
            tenant_id: TenantId::new_test_id(7),
            writer_id: 3,
            epoch,
            first_sequence,
            last_sequence: first_sequence + count as u64 - 1,
            event_count: count as u32,
            durable_batch_id: 900,
            writer_local_batch_id: 1,
            schema_generation: 1,
            dictionary_generation_hint: 0,
            created_at_physical: 1,
            committed_at_physical: 2,
        },
        &payload,
    )
    .unwrap()
}

#[test]
fn documented_sizes_and_magics() {
    let frame_bytes = sample_frame(3, 5, 101);
    assert_eq!(&frame_bytes[0..4], b"OEJ1");
    assert_eq!(u16::from_le_bytes([frame_bytes[4], frame_bytes[5]]), 1);
    assert_eq!(u16::from_le_bytes([frame_bytes[6], frame_bytes[7]]), 192);
    let frame_len = u32::from_le_bytes(frame_bytes[8..12].try_into().unwrap());
    assert!(super::super::NORMAL_FRAME_SIZES.contains(&frame_len));
    assert_eq!(frame_bytes.len() as u32, frame_len);
    assert_eq!(u64::from_le_bytes(frame_bytes[48..56].try_into().unwrap()), 5);
    assert_eq!(u64::from_le_bytes(frame_bytes[56..64].try_into().unwrap()), 101);
    assert_eq!(u64::from_le_bytes(frame_bytes[64..72].try_into().unwrap()), 103);
    assert_eq!(u32::from_le_bytes(frame_bytes[72..76].try_into().unwrap()), 3);
    assert_eq!(u32::from_le_bytes(frame_bytes[76..80].try_into().unwrap()), 1);

    let payload = &frame_bytes[192..];
    assert_eq!(&payload[0..4], b"HCB1");
    assert_eq!(u16::from_le_bytes(payload[6..8].try_into().unwrap()), 128);
    assert_eq!(u32::from_le_bytes(payload[44..48].try_into().unwrap()), 128);
    assert_eq!(u32::from_le_bytes(payload[48..52].try_into().unwrap()), 64);
}

#[test]
fn batch_round_trip_preserves_envelopes_and_payloads() {
    let events = sample_events(4);
    let payload = build_batch(&events, 9, 0).unwrap();
    let decoded = decode_batch(&payload, 4).unwrap();
    assert_eq!(decoded.header.batch_schema_generation, 9);
    for (input, output) in events.iter().zip(decoded.events.iter()) {
        assert_eq!(envelope_of(output, TenantId::new_test_id(7)), input.envelope);
        let PayloadInput::Variant(expected) = &input.payload else {
            panic!("sample uses variant payloads");
        };
        let bytes = output.payload.expect("payload present");
        let value = VariantRef::new(bytes).decode(&decoded.dictionary).unwrap();
        assert_eq!(&value, expected);
    }
}

fn encoded_payload_events(count: usize) -> Vec<EventInput> {
    sample_events(count)
        .into_iter()
        .map(|mut event| {
            let PayloadInput::Variant(value) = &event.payload else {
                panic!("sample uses variant payloads");
            };
            let mut keys = BTreeSet::new();
            value.collect_keys(&mut keys);
            let dictionary = KeyDictionary::build(keys.into_iter().map(str::to_owned));
            event.payload = PayloadInput::Encoded(EncodedPayload {
                bytes: encode_value(value, &dictionary).unwrap(),
                dictionary,
            });
            event
        })
        .collect()
}

#[test]
fn an_already_encoded_payload_builds_the_same_batch_as_its_value_tree() {
    let from_values = build_batch(&sample_events(4), 9, 0).unwrap();
    let from_encoded = build_batch(&encoded_payload_events(4), 9, 0).unwrap();
    assert_eq!(from_encoded, from_values);
}

#[test]
fn a_nested_already_encoded_payload_transcodes_to_the_same_bytes() {
    let mut nested = BTreeMap::new();
    nested.insert("zeta".to_owned().into(), VariantValue::String("z".repeat(300)));
    nested.insert(
        "alpha".to_owned().into(),
        VariantValue::Array(vec![
            VariantValue::Int(-9_000_000_000),
            VariantValue::Double(1.5),
            VariantValue::object(BTreeMap::from([(
                "deep".to_owned(),
                VariantValue::Binary(vec![1, 2, 3]),
            )])),
        ]),
    );
    let payload = VariantValue::Object(nested);

    let mut with_value = sample_events(2);
    with_value[1].payload = PayloadInput::Variant(payload.clone());
    let mut with_encoded = encoded_payload_events(2);
    let mut keys = BTreeSet::new();
    payload.collect_keys(&mut keys);
    let dictionary = KeyDictionary::build(keys.into_iter().map(str::to_owned));
    with_encoded[1].payload = PayloadInput::Encoded(EncodedPayload {
        bytes: encode_value(&payload, &dictionary).unwrap(),
        dictionary,
    });

    assert_eq!(
        build_batch(&with_encoded, 9, 0).unwrap(),
        build_batch(&with_value, 9, 0).unwrap()
    );
}

#[test]
fn forged_event_count_that_overflows_the_length_math_is_rejected() {
    let events = sample_events(1);
    let mut payload = build_batch(&events, 9, 0).unwrap();
    let forged: u32 = 0x0400_0000;
    payload[8..12].copy_from_slice(&forged.to_le_bytes());
    assert!(decode_batch(&payload, forged).is_err());
}

#[test]
fn build_batch_rejects_a_payload_nested_beyond_the_decoder_depth() {
    let mut events = sample_events(1);
    let mut value = VariantValue::Int(1);
    for _ in 0..200 {
        value = VariantValue::Array(vec![value]);
    }
    events[0].payload = PayloadInput::Variant(value);
    assert!(build_batch(&events, 1, 0).is_err());
}

#[test]
fn build_batch_rejects_an_empty_external_ref() {
    let mut events = sample_events(1);
    events[0].payload = PayloadInput::ExternalRef(String::new());
    assert!(build_batch(&events, 1, 0).is_err());

    events[0].payload = PayloadInput::ExternalRef("oej://tenant/7/blob/1".to_owned());
    let payload = build_batch(&events, 1, 0).unwrap();
    let decoded = decode_batch(&payload, 1).unwrap();
    assert_eq!(decoded.events[0].payload, Some("oej://tenant/7/blob/1".as_bytes()));
    assert_eq!(decoded.events[0].variable.payload_flags, PAYLOAD_FLAG_EXTERNAL_REF);
}

#[test]
fn frame_decode_validates_and_round_trips() {
    let frame_bytes = sample_frame(2, 1, 1);
    let (header, payload) = frame::decode_frame(&frame_bytes).unwrap();
    assert_eq!(header.event_count, 2);
    let decoded = decode_batch(payload, header.event_count).unwrap();
    assert_eq!(decoded.events.len(), 2);

    let mut corrupted = frame_bytes.clone();
    *corrupted.last_mut().unwrap() ^= 0xFF;
    assert!(frame::decode_frame(&corrupted).is_err());

    let mut wrong_encoding = frame_bytes.clone();
    wrong_encoding[76] = 2;
    assert!(frame::decode_frame(&wrong_encoding).is_err());
}

#[test]
fn live_overlay_conversion_is_deterministic_and_ordered() {
    use super::super::overlay;
    let frame_bytes = sample_frame(3, 2, 50);
    let (header, payload) = frame::decode_frame(&frame_bytes).unwrap();
    let decoded = decode_batch(payload, header.event_count).unwrap();
    let segment = overlay::convert_frame(&header, &decoded, 1, 1, None, |_, _| false)
        .unwrap()
        .expect("not covered");
    assert_eq!(segment.batch.num_rows(), 3);
    let sequences = segment
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap();
    assert_eq!(sequences.values(), &[50, 51, 52]);
    let refs = segment
        .batch
        .column(21)
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap();
    for (row, value) in refs.values().iter().enumerate() {
        let variable = &decoded.events[row].variable;
        assert_eq!(value >> 32, u64::from(variable.payload_len));
        assert_eq!(value & 0xFFFF_FFFF, u64::from(variable.payload_offset));
    }
    let skipped = overlay::convert_frame(&header, &decoded, 1, 1, None, |_, _| true).unwrap();
    assert!(skipped.is_none());
    let again = overlay::convert_frame(&header, &decoded, 1, 1, None, |_, _| false)
        .unwrap()
        .unwrap();
    assert_eq!(format!("{:?}", segment.batch), format!("{:?}", again.batch));
}

#[test]
fn header_timestamp_bounds_must_match_the_decoded_events() {
    let events = sample_events(4);
    let mut payload = build_batch(&events, 1, 0).unwrap();
    payload[56..64].copy_from_slice(&i64::MIN.to_le_bytes());
    let err = decode_batch(&payload, 4).expect_err("mismatched header timestamp bounds must be rejected");
    assert_eq!(
        err,
        FormatError::Structural {
            rule: "batch header timestamp bounds must match the decoded events"
        }
    );
}

#[test]
fn a_signed_event_survives_the_batch_round_trip_byte_exactly() {
    let author = SimulatedEventAuthor::from_seed(4);
    let payload = signed_event_payload("hello", &[&["e", "root"]]);
    let provenance = author.sign(1, 1_700_000_000, &payload).expect("well-shaped payload");
    let mut events = sample_events(2);
    if let Some(event) = events.get_mut(1) {
        event.payload = PayloadInput::Variant(payload.clone());
        event.provenance = Some(provenance.clone());
    }

    let encoded = build_batch(&events, 1, 0).expect("batch builds");
    let decoded = decode_batch(&encoded, 2).expect("batch decodes");
    assert_eq!(decoded.events.first().and_then(|e| e.provenance.clone()), None);
    assert_eq!(
        decoded.events.get(1).and_then(|e| e.provenance.clone()),
        Some(provenance)
    );

    let stored = decoded.events.get(1).expect("two events");
    let stored_payload = VariantRef::new(stored.payload.expect("a payload"))
        .decode(&decoded.dictionary)
        .expect("payload decodes");
    stored
        .provenance
        .as_ref()
        .expect("provenance survived")
        .verify(&stored_payload)
        .expect("the stored event re-verifies");
}

#[test]
fn an_unsigned_batch_carries_no_provenance_bytes_at_all() {
    let encoded = build_batch(&sample_events(3), 1, 0).expect("batch builds");
    let decoded = decode_batch(&encoded, 3).expect("batch decodes");
    assert_eq!(decoded.header.flags & BATCH_FLAG_SIGNED_PROVENANCE, 0);
    assert_eq!(decoded.header.provenance_table_len, 0);
    assert_eq!(decoded.header.provenance_table_offset, 0);
    assert!(decoded.events.iter().all(|event| event.provenance.is_none()));
}

#[test]
fn prev_and_auth_references_to_external_ids_survive_the_journal() {
    let mut refs: Vec<RelationshipRef> = (0..20)
        .map(|i| RelationshipRef::to_external(RelationshipKind::Prev, format!("$prev-{i}").as_bytes()).unwrap())
        .collect();
    refs.extend(
        (0..10).map(|i| RelationshipRef::to_external(RelationshipKind::Auth, format!("$auth-{i}").as_bytes()).unwrap()),
    );
    refs.push(RelationshipRef::to_external(RelationshipKind::Prev, b"$abc:example.org").unwrap());
    refs.push(RelationshipRef::to_event(RelationshipKind::Parent, 7));
    let relationships = EventRelationships::new(refs).unwrap();
    let mut events = sample_events(2);
    events[1].relationships = Some(relationships.clone());
    let encoded = build_batch(&events, 1, 0).expect("batch builds");
    let decoded = decode_batch(&encoded, 2).expect("batch decodes");
    assert_eq!(
        decoded.events.get(1).and_then(|event| event.relationships.clone()),
        Some(relationships)
    );
}

#[test]
fn a_dictionary_encoded_after_a_prefix_matches_its_standalone_bytes() {
    let dictionaries = [
        Vec::new(),
        vec![String::new()],
        vec!["a".to_owned()],
        vec![
            "amount".to_owned(),
            "kind".to_owned(),
            "z".repeat(5000),
            "\u{e9}t\u{e9}".to_owned(),
        ],
    ];
    for keys in dictionaries {
        let dictionary = KeyDictionary::from_sorted_unique(keys.clone()).unwrap();
        let standalone = encode_variant_dictionary(&dictionary);
        let mut expected = Vec::new();
        expected.extend_from_slice(&(keys.len() as u32).to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes());
        let mut offset = 0u32;
        for key in &keys {
            expected.extend_from_slice(&offset.to_le_bytes());
            offset += key.len() as u32;
        }
        expected.extend_from_slice(&offset.to_le_bytes());
        for key in &keys {
            expected.extend_from_slice(key.as_bytes());
        }
        assert_eq!(standalone, expected);

        let mut out = Writer::new();
        out.put_slice(b"prefix");
        encode_variant_dictionary_into(&dictionary, &mut out);
        assert_eq!(out.bytes().get(..6), Some(&b"prefix"[..]));
        assert_eq!(out.bytes().get(6..), Some(standalone.as_slice()));
    }
}

fn batch_digest(events: &[EventInput]) -> String {
    let encoded = build_batch(events, 1, 0).expect("batch builds");
    sha256_digest(&encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn metadata_string_events() -> Vec<EventInput> {
    let mut events = sample_events(4);
    if let [first, second, third, fourth] = events.as_mut_slice() {
        first.source_schema = Some("crm".to_owned());
        first.envelope.actor_id = Some("acct-1".to_owned());
        second.envelope.actor_id = Some(String::new());
        second.source_delivery = None;
        third.envelope.entity_id = Some("\u{e9}v\u{e9}nement-\u{5b9f}\u{4f53}-\u{1f600}".to_owned());
        third.envelope.account_id = Some("long-".repeat(2000));
        third.source_schema = Some(String::new());
        fourth.envelope.event_type = "opportunity".to_owned();
        fourth.source_delivery = Some("delivery-0".to_owned());
        fourth.envelope.account_id = None;
    }
    events
}

fn provenance_record(seed: u8, scheme: SignatureScheme) -> SignedEventProvenance {
    SignedEventProvenance {
        author_pubkey: [seed; 32],
        claimed_at: TimestampValue::from_physical_nanos(1_700_000_000_000_000_000 + i64::from(seed)),
        protocol_event_id: [seed.wrapping_add(1); 32],
        protocol_kind: u32::from(seed),
        scheme,
        signature: [seed.wrapping_add(2); 64],
    }
}

fn relationship_set(seed: u8) -> EventRelationships {
    EventRelationships::new(vec![
        RelationshipRef::to_event(RelationshipKind::Parent, 0x2000 + u128::from(seed)),
        RelationshipRef::to_external(RelationshipKind::Prev, format!("$prev-{seed}").as_bytes()).unwrap(),
        RelationshipRef::to_protocol(RelationshipKind::Root, [seed; 32]),
        RelationshipRef::to_external(RelationshipKind::Auth, b"$a").unwrap(),
        RelationshipRef::to_event(RelationshipKind::Related, 9),
        RelationshipRef::to_protocol(RelationshipKind::Link, [seed.wrapping_add(3); 32]),
    ])
    .unwrap()
}

fn side_table_events(pattern: &[(Option<SignatureScheme>, Option<Option<u8>>)]) -> Vec<EventInput> {
    let mut events = sample_events(pattern.len());
    for (index, (event, (scheme, relationships))) in events.iter_mut().zip(pattern).enumerate() {
        event.provenance = scheme.map(|scheme| provenance_record(index as u8, scheme));
        event.relationships = relationships.map(|seed| match seed {
            Some(seed) => relationship_set(seed),
            None => EventRelationships::new(Vec::new()).unwrap(),
        });
    }
    events
}

fn side_table_patterns() -> Vec<Vec<(Option<SignatureScheme>, Option<Option<u8>>)>> {
    let bip = Some(SignatureScheme::Bip340SchnorrSecp256k1);
    let ed = Some(SignatureScheme::Ed25519);
    vec![
        vec![(None, None), (None, None), (None, None)],
        vec![(bip, None), (None, None), (None, Some(Some(1)))],
        vec![(None, Some(Some(2))), (None, None), (ed, None)],
        vec![
            (bip, None),
            (None, Some(None)),
            (ed, Some(Some(3))),
            (None, None),
            (bip, Some(Some(4))),
        ],
        vec![(ed, Some(Some(5))), (bip, Some(None)), (ed, Some(Some(6)))],
    ]
}

#[test]
fn interned_metadata_strings_build_the_same_bytes_as_before() {
    assert_eq!(
        batch_digest(&metadata_string_events()),
        "bf4eba2460e8fe9df6b2fe65c5bd642e44017150881ed421975bc1c6a10a3506"
    );
}

#[test]
fn interned_strings_share_refs_by_content_and_outlive_the_input() {
    let events = metadata_string_events();
    let encoded = build_batch(&events, 1, 0).expect("batch builds");
    drop(events);
    let decoded = decode_batch(&encoded, 4).expect("batch decodes after the input is dropped");
    let [first, second, third, fourth] = decoded.events.as_slice() else {
        panic!("four events");
    };
    assert_eq!(first.fixed.source_string_ref, fourth.fixed.source_string_ref);
    assert_eq!(first.variable.source_schema_ref, first.fixed.source_string_ref);
    assert_eq!(first.variable.actor_id_string_ref, first.variable.account_id_string_ref);
    assert_eq!(fourth.fixed.event_type_string_ref, fourth.fixed.entity_type_string_ref);
    assert_eq!(fourth.variable.source_delivery_ref, first.variable.source_delivery_ref);
    assert_eq!(second.actor_id, Some(""));
    assert_eq!(third.source_schema, Some(""));
    assert_eq!(second.variable.actor_id_string_ref, third.variable.source_schema_ref);
    assert_eq!(second.variable.source_delivery_ref, REF_ABSENT);
    assert_eq!(fourth.account_id, None);
    assert_eq!(third.entity_id, Some("\u{e9}v\u{e9}nement-\u{5b9f}\u{4f53}-\u{1f600}"));
    assert_eq!(third.account_id.map(str::len), Some(5 * 2000));
}

#[test]
fn side_tables_build_the_same_bytes_as_before() {
    let digests: Vec<String> = side_table_patterns()
        .iter()
        .map(|pattern| batch_digest(&side_table_events(pattern)))
        .collect();
    assert_eq!(
        digests,
        vec![
            "3e0caf944ccb378c46e834500cdce1047a519ca29213df07a66d4efc2597c9a2",
            "97fe8341d7ec7b218d35b4b703b8599fd0776575569a74997b438782fd8a1874",
            "bf750df961a354cb857d636eec9a31bf8c1f4a501a747d3dab5b9e962bd2ef9a",
            "3549227b245b4f598bc520c6ef9562f3ddcb8dcb73136cfdd11766c42a9e04e9",
            "55b2b295a54af95cd457cfaab2e686f9f01fde1316ec24c29c7ac3706ae4e8e3",
        ]
    );
}

#[test]
fn side_table_refs_are_zero_without_a_section_and_absent_for_events_without_entries() {
    let patterns = side_table_patterns();
    let decode = |pattern: &[(Option<SignatureScheme>, Option<Option<u8>>)]| {
        let encoded = build_batch(&side_table_events(pattern), 1, 0).expect("batch builds");
        let decoded = decode_batch(&encoded, pattern.len() as u32).expect("batch decodes");
        let refs: Vec<(u32, u32)> = decoded
            .events
            .iter()
            .map(|event| (event.variable.provenance_ref, event.variable.relationships_ref))
            .collect();
        (decoded.header.flags, refs)
    };

    let (flags, refs) = decode(&patterns[0]);
    assert_eq!(flags, 0);
    assert_eq!(refs, vec![(REF_ABSENT, 0); 3]);

    let (flags, refs) = decode(&patterns[1]);
    assert_eq!(flags, BATCH_FLAG_SIGNED_PROVENANCE | BATCH_FLAG_RELATIONSHIPS);
    assert_eq!(refs, vec![(0, REF_ABSENT), (REF_ABSENT, REF_ABSENT), (REF_ABSENT, 0)]);

    let (_, refs) = decode(&patterns[3]);
    let first_set_len = 4 + 6 * 8 + 16 + 4 + "$prev-3".len() + 32 + 4 + "$a".len() + 16 + 32;
    assert_eq!(
        refs,
        vec![
            (0, REF_ABSENT),
            (REF_ABSENT, 0),
            (1, 4),
            (REF_ABSENT, REF_ABSENT),
            (2, 4 + first_set_len as u32),
        ]
    );
}

#[test]
fn a_built_frame_converts_from_its_events_exactly_as_from_its_decoded_batch() {
    use super::super::overlay;
    let mut events = sample_events(4);
    events[1].payload = PayloadInput::ExternalRef("oej://tenant/7/blob/1".to_owned());
    events[2].payload = PayloadInput::None;
    let frame_bytes = frame::build_frame(
        &FrameBuildInput {
            flags: 0,
            tenant_id: TenantId::new_test_id(7),
            writer_id: 3,
            epoch: 1,
            first_sequence: 10,
            last_sequence: 13,
            event_count: 4,
            durable_batch_id: 900,
            writer_local_batch_id: 1,
            schema_generation: 1,
            dictionary_generation_hint: 0,
            created_at_physical: 1,
            committed_at_physical: 2,
        },
        &build_batch(&events, 1, 0).unwrap(),
    )
    .unwrap();
    let (header, payload) = frame::decode_frame(&frame_bytes).unwrap();
    let decoded = decode_batch(payload, header.event_count).unwrap();
    let via_decode = overlay::convert_frame(&header, &decoded, 1, 0, None, |_, _| false)
        .unwrap()
        .unwrap();
    let via_events = overlay::convert_built_frame(&header, &events, payload, 1, 0, None, |_, _| false)
        .unwrap()
        .unwrap();
    assert_eq!(format!("{:?}", via_decode.batch), format!("{:?}", via_events.batch));
    assert_eq!(via_decode.meta.payload_arena, via_events.meta.payload_arena);
    assert_eq!(via_decode.meta.variant_dictionary, via_events.meta.variant_dictionary);
    for row in 0..4 {
        assert_eq!(via_decode.event(row).unwrap(), via_events.event(row).unwrap());
    }
    assert!(overlay::convert_built_frame(&header, &events[..3], payload, 1, 0, None, |_, _| false).is_err());
}
