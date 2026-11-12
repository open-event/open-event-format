use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::columns::{FreetextDeclaration, PromotionPlan};
use crate::deletes::{SubjectContentKey, SubjectId};
use crate::error::{FormatError, StorageError};
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::layout::LayoutTargets;
use crate::layout::reader::{OeFile, PayloadRead};
use crate::security::{FooterEncryption, SubjectPayloadRead, read_subject_payload};
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, SharedStrings, build_oe_file};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
struct MemorySubjectKeys {
    keys: RefCell<HashMap<SubjectId, SubjectContentKey>>,
}

impl MemorySubjectKeys {
    fn destroy(&self, subject: SubjectId) {
        if let Some(key) = self.keys.borrow_mut().get_mut(&subject) {
            key.destroy();
        }
    }
}

impl SubjectKeyStore for MemorySubjectKeys {
    fn opening_key(&self, subject: SubjectId) -> Result<Option<SubjectContentKey>, StorageError> {
        Ok(self
            .keys
            .borrow()
            .get(&subject)
            .filter(|key| !key.is_destroyed())
            .cloned())
    }

    fn sealing_key(&self, subject: SubjectId) -> Result<Option<SubjectContentKey>, StorageError> {
        let mut keys = self.keys.borrow_mut();
        let key = keys
            .entry(subject)
            .or_insert_with(|| SubjectContentKey::generate([subject.uuid().as_u128() as u8; 32]));
        Ok((!key.is_destroyed()).then(|| key.clone()))
    }
}

fn tenant() -> TenantId {
    TenantId::new_test_id(7)
}

fn config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 42,
        footer_dek: None,
        footer_encryption: FooterEncryption::Plaintext,
        freetext: FreetextDeclaration::default(),
        freetext_row_offset_index: false,
        generation_id: 1,
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

fn payload(i: u64) -> VariantValue {
    let mut fields = BTreeMap::new();
    fields.insert("body".to_owned(), VariantValue::String(format!("message {i}")));
    fields.insert("n".to_owned(), VariantValue::Int(i as i64));
    VariantValue::Object(fields)
}

fn row(i: u64, subject: Option<SubjectId>) -> BuildRow {
    let mut row = SharedStrings::default().build_row(OeRow {
        epoch: 1,
        event: EventInput {
            connector_delivery_hash_high: 0,
            connector_delivery_hash_low: i,
            envelope: EventEnvelope {
                account_id: None,
                account_id_hash_low: 0,
                actor_id: None,
                actor_id_hash_low: 0,
                dedupe_hash_high: 0,
                dedupe_hash_low: i,
                entity_id: Some(format!("room-{i}")),
                entity_id_hash_high: 1,
                entity_id_hash_low: i,
                entity_type: "room".to_owned(),
                event_id: EventId::new_test_id(0xBEEF_0000 + u128::from(i)),
                event_type: "m.room.message".to_owned(),
                flags: EventFlags(0),
                ingested_at: TimestampValue::from_physical_nanos(2_000_000 + i as i64),
                occurred_at: TimestampValue::from_physical_nanos(1_000_000 + i as i64),
                schema_version: 1,
                source: "messaging".to_owned(),
                stream_id: StreamId(1),
                stream_sequence: i,
                tenant_id: tenant(),
                trace_id_hash_low: 0,
            },
            payload: PayloadInput::Variant(payload(i)),
            provenance: None,
            relationships: None,
            source_delivery: None,
            source_schema: None,
        },
        sequence: i + 1,
    });
    row.subject = subject;
    row
}

#[test]
fn destroying_a_subject_key_tombstones_its_rows_while_other_rows_in_the_granule_still_read() {
    let alice = SubjectId::new_test_id(1);
    let bob = SubjectId::new_test_id(2);
    let subjects = [Some(alice), Some(bob), Some(alice), Some(bob), Some(alice), None];
    let mut rows: Vec<BuildRow> = (0..subjects.len() as u64)
        .zip(subjects)
        .map(|(i, subject)| row(i, subject))
        .collect();
    let keys = MemorySubjectKeys::default();
    seal_subject_rows(&mut rows, &keys).unwrap();
    assert!(rows.iter().all(|row| row.subject.is_none()));

    let built = build_oe_file(rows, &config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_eq!(file.footer().granules.len(), 1, "every row shares one granule");

    let read = |i: u64| read_subject_payload(&file, i, &keys).unwrap();
    for (i, subject) in (0..).zip(subjects) {
        let expected = match subject {
            Some(_) => SubjectPayloadRead::Opened(payload(i)),
            None => SubjectPayloadRead::Plain(PayloadRead::Value(payload(i))),
        };
        assert_eq!(read(i), expected, "row {i} reads while every key is live");
    }

    keys.destroy(alice);

    for (i, subject) in (0..).zip(subjects) {
        let expected = match subject {
            Some(subject) if subject == alice => SubjectPayloadRead::Tombstone,
            Some(_) => SubjectPayloadRead::Opened(payload(i)),
            None => SubjectPayloadRead::Plain(PayloadRead::Value(payload(i))),
        };
        assert_eq!(read(i), expected, "row {i} after alice's key is destroyed");
    }
}

#[test]
fn a_sealed_payload_is_not_stored_in_the_clear() {
    let mut rows = vec![row(0, Some(SubjectId::new_test_id(1)))];
    seal_subject_rows(&mut rows, &MemorySubjectKeys::default()).unwrap();

    let built = build_oe_file(rows, &config()).unwrap();
    assert!(!built.bytes.windows(9).any(|window| window == b"message 0"));
}

#[test]
fn the_build_refuses_a_row_that_still_names_a_subject() {
    let rows = vec![row(0, Some(SubjectId::new_test_id(1)))];

    assert_eq!(
        build_oe_file(rows, &config()).err(),
        Some(FormatError::Structural {
            rule: "a row naming a subject is sealed with seal_subject_rows before the build",
        })
    );
}

#[test]
fn sealing_for_an_erased_subject_fails_and_leaves_the_row_untouched() {
    let alice = SubjectId::new_test_id(1);
    let keys = MemorySubjectKeys::default();
    seal_subject_rows(&mut [row(0, Some(alice))], &keys).unwrap();
    keys.destroy(alice);

    let mut rows = vec![row(1, Some(alice))];
    let before = rows.clone();

    assert_eq!(
        seal_subject_rows(&mut rows, &keys),
        Err(SubjectPayloadError::SubjectErased)
    );
    assert_eq!(rows, before);
}

fn nested_fields() -> Vec<(FieldName, VariantValue)> {
    let mut inner = BTreeMap::new();
    inner.insert("city".to_owned(), VariantValue::String("Oslo".to_owned()));
    inner.insert("zip".to_owned(), VariantValue::Int(150));
    vec![
        (FieldName::from("address"), VariantValue::Object(inner)),
        (
            FieldName::from("tags"),
            VariantValue::Array(vec![VariantValue::Object(BTreeMap::from([(
                "label".to_owned(),
                VariantValue::Bool(true),
            )]))]),
        ),
        (FieldName::from("zeta"), VariantValue::Null),
    ]
}

fn object_of(fields: &[(FieldName, VariantValue)]) -> VariantValue {
    VariantValue::Object(
        fields
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect(),
    )
}

#[test]
fn sorted_object_fields_encode_like_the_equivalent_object_value() {
    let fields = nested_fields();
    assert_eq!(encode_object(&fields).unwrap(), encode(&object_of(&fields)).unwrap());
}

#[test]
fn unsorted_and_repeated_object_fields_encode_like_a_normalized_object_with_the_last_repeat_winning() {
    let mut fields = nested_fields();
    fields.reverse();
    fields.push((FieldName::from("zeta"), VariantValue::Int(9)));
    let mut expected = nested_fields();
    expected[2].1 = VariantValue::Int(9);
    assert_eq!(encode_object(&fields).unwrap(), encode(&object_of(&expected)).unwrap());
}

#[test]
fn erasing_one_subject_of_a_multi_subject_event_redacts_only_its_fields() {
    let alice = SubjectId::new_test_id(1);
    let bob = SubjectId::new_test_id(2);
    let mut rows: Vec<BuildRow> = (0..3).map(|i| row(i, None)).collect();
    let map = |i: usize, _: &BuildRow| match i {
        0 => crate::security::FieldSubjects::from([("body".to_owned(), alice), ("n".to_owned(), bob)]),
        1 => crate::security::FieldSubjects::from([("body".to_owned(), bob)]),
        _ => crate::security::FieldSubjects::new(),
    };
    let dir = tempfile::tempdir().unwrap();
    let keys =
        crate::security::FileSubjectKeyStore::open(dir.path(), crate::security::TenantDek::new([3; 32])).unwrap();
    seal_subject_field_rows(&mut rows, map, &keys).unwrap();

    let built = build_oe_file(rows, &config()).unwrap();
    assert!(!built.bytes.windows(9).any(|window| window == b"message 0"));
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let read = |i: u64| crate::security::read_subject_fields(&file, i, &keys).unwrap().unwrap();
    for i in 0..3 {
        assert_eq!(read(i).value, payload(i), "row {i} reads while both keys live");
    }

    keys.erase_subject_under_profile(alice, Some(1), &crate::clock::FixedClock::at(9))
        .unwrap();

    let after = read(0);
    let VariantValue::Object(fields) = &after.value else {
        panic!()
    };
    assert_eq!(fields["body"], VariantValue::Null, "alice's field is tombstoned");
    assert_eq!(
        fields["n"],
        VariantValue::Int(0),
        "bob's field of the same event survives"
    );
    assert_eq!(after.redacted.len(), 1);
    assert_eq!(read(1).value, payload(1));
    assert_eq!(read(2).value, payload(2));

    let vectors = crate::security::field_deletion_vectors_for_subject(&file, alice).unwrap();
    assert_eq!(vectors.len(), 1);
    let vector = &vectors[0];
    assert!(vector.column_redacted("body", 0));
    assert!(!vector.column_redacted("body", 1));
    assert!(!vector.column_redacted("n", 0));
    assert!(
        crate::security::field_deletion_vectors_for_subject(&file, SubjectId::new_test_id(9))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn subject_field_vectors_never_redact_another_subjects_field_in_a_different_row() {
    let alice = SubjectId::new_test_id(1);
    let bob = SubjectId::new_test_id(2);
    let mut rows: Vec<BuildRow> = (0..2).map(|i| row(i, None)).collect();
    let map = |i: usize, _: &BuildRow| match i {
        0 => crate::security::FieldSubjects::from([("body".to_owned(), alice), ("n".to_owned(), bob)]),
        _ => crate::security::FieldSubjects::from([("body".to_owned(), bob), ("n".to_owned(), alice)]),
    };
    let dir = tempfile::tempdir().unwrap();
    let keys =
        crate::security::FileSubjectKeyStore::open(dir.path(), crate::security::TenantDek::new([3; 32])).unwrap();
    seal_subject_field_rows(&mut rows, map, &keys).unwrap();
    let built = build_oe_file(rows, &config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();

    let vectors = crate::security::field_deletion_vectors_for_subject(&file, alice).unwrap();
    assert_eq!(vectors.len(), 2);
    let redacted = |path: &str, row: u64| vectors.iter().any(|vector| vector.column_redacted(path, row));
    assert!(redacted("body", 0));
    assert!(redacted("n", 1));
    assert!(!redacted("n", 0), "bob owns n in row 0");
    assert!(!redacted("body", 1), "bob owns body in row 1");
}
