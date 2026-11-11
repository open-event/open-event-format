use super::*;
use crate::clock::FixedClock;
use crate::security::{FileSubjectKeyStore, TenantDek};
use crate::typed_id::TypedIdTestExt;

fn store(dir: &std::path::Path) -> FileSubjectKeyStore {
    FileSubjectKeyStore::open(dir, TenantDek::new([7; 32])).unwrap()
}

fn object(fields: &[(&str, VariantValue)]) -> VariantValue {
    VariantValue::Object(fields.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect())
}

fn transfer() -> VariantValue {
    object(&[
        ("amount", VariantValue::Int(120)),
        (
            "payer",
            object(&[("email", VariantValue::String("alice@example.com".into()))]),
        ),
        ("payee_name", VariantValue::String("Bob Builder".into())),
    ])
}

fn subjects(alice: SubjectId, bob: SubjectId) -> FieldSubjects {
    FieldSubjects::from([("payer.email".to_owned(), alice), ("payee_name".to_owned(), bob)])
}

#[test]
fn fields_sealed_for_two_subjects_open_back_while_both_keys_live() {
    let dir = tempfile::tempdir().unwrap();
    let keys = store(dir.path());
    let (alice, bob) = (SubjectId::new_test_id(1), SubjectId::new_test_id(2));
    let event = EventId::new_test_id(0xE1);
    let sealed = seal_subject_fields(event, &transfer(), &subjects(alice, bob), &keys).unwrap();

    assert_ne!(sealed, transfer());
    assert_eq!(sealed_field_subjects(&sealed), subjects(alice, bob));
    let VariantValue::Object(fields) = &sealed else {
        panic!()
    };
    assert_eq!(
        fields["amount"],
        VariantValue::Int(120),
        "unlabelled fields stay in the clear"
    );

    let read = open_subject_fields(event, &sealed, &keys).unwrap();
    assert_eq!(read.value, transfer());
    assert!(read.redacted.is_empty() && read.rejected.is_empty());
}

#[test]
fn erasing_one_subject_tombstones_only_its_fields() {
    let dir = tempfile::tempdir().unwrap();
    let keys = store(dir.path());
    let (alice, bob) = (SubjectId::new_test_id(1), SubjectId::new_test_id(2));
    let event = EventId::new_test_id(0xE1);
    let sealed = seal_subject_fields(event, &transfer(), &subjects(alice, bob), &keys).unwrap();

    keys.erase_subject(alice, &FixedClock::at(5)).unwrap();

    let read = open_subject_fields(event, &sealed, &keys).unwrap();
    assert_eq!(
        read.value,
        object(&[
            ("amount", VariantValue::Int(120)),
            ("payer", object(&[("email", VariantValue::Null)])),
            ("payee_name", VariantValue::String("Bob Builder".into())),
        ])
    );
    assert_eq!(
        read.redacted,
        vec![RedactedField {
            path: "payer.email".into(),
            subject: alice
        }]
    );
    assert_eq!(
        seal_subject_fields(EventId::new_test_id(0xE2), &transfer(), &subjects(alice, bob), &keys),
        Err(SubjectPayloadError::SubjectErased)
    );
}

#[test]
fn a_sealed_field_moved_to_another_field_or_event_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let keys = store(dir.path());
    let alice = SubjectId::new_test_id(1);
    let event = EventId::new_test_id(0xE1);
    let payload = object(&[
        ("a", VariantValue::String("x".into())),
        ("b", VariantValue::String("y".into())),
    ]);
    let map = FieldSubjects::from([("a".to_owned(), alice), ("b".to_owned(), alice)]);
    let sealed = seal_subject_fields(event, &payload, &map, &keys).unwrap();
    let VariantValue::Object(mut fields) = sealed.clone() else {
        panic!()
    };
    let a = fields["a"].clone();
    fields.insert("b".into(), a);
    let read = open_subject_fields(event, &VariantValue::Object(fields), &keys).unwrap();
    assert_eq!(read.rejected, vec!["b".to_owned()]);

    let read = open_subject_fields(EventId::new_test_id(0xE2), &sealed, &keys).unwrap();
    assert_eq!(read.rejected.len(), 2);
}

#[test]
fn a_missing_path_seals_nothing_and_a_non_object_payload_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let keys = store(dir.path());
    let alice = SubjectId::new_test_id(1);
    let map = FieldSubjects::from([("nope.deeper".to_owned(), alice)]);
    let event = EventId::new_test_id(0xE1);
    assert_eq!(
        seal_subject_fields(event, &transfer(), &map, &keys).unwrap(),
        transfer()
    );
    assert!(matches!(
        seal_subject_fields(event, &VariantValue::Int(1), &map, &keys),
        Err(SubjectPayloadError::Format(_))
    ));
}

#[test]
fn subject_keys_scrub_their_material_on_drop() {
    fn zeroized_on_drop<T: zeroize::ZeroizeOnDrop>() {}
    zeroized_on_drop::<crate::security::ContentKey>();
    zeroized_on_drop::<crate::security::SealedContentKey>();
    zeroized_on_drop::<crate::deletes::SubjectContentKey>();
}

#[test]
fn an_erased_subject_redacts_only_its_own_field_of_each_row() {
    let dir = tempfile::tempdir().unwrap();
    let keys = store(dir.path());
    let (erased, kept) = (SubjectId::new_test_id(1), SubjectId::new_test_id(2));
    let message = object(&[
        (
            "sender",
            object(&[("email", VariantValue::String("s@example.com".into()))]),
        ),
        (
            "recipient",
            object(&[("email", VariantValue::String("r@example.com".into()))]),
        ),
    ]);
    let seal = |event: u128, fields: FieldSubjects| {
        seal_subject_fields(EventId::new_test_id(event), &message, &fields, &keys).unwrap()
    };
    let rows = [
        seal(1, FieldSubjects::from([("sender.email".to_owned(), erased)])),
        seal(
            2,
            FieldSubjects::from([
                ("recipient.email".to_owned(), erased),
                ("sender.email".to_owned(), kept),
            ]),
        ),
        seal(3, FieldSubjects::from([("sender.email".to_owned(), erased)])),
        seal(4, FieldSubjects::from([("sender.email".to_owned(), kept)])),
    ];

    let mut grouped = BTreeMap::new();
    for (row, payload) in rows.iter().enumerate() {
        group_erased_fields(&mut grouped, row as u64, payload, erased);
    }
    let vectors = erased_field_vectors(grouped);
    let redacted = |column: &str, row: u64| vectors.iter().any(|vector| vector.column_redacted(column, row));

    assert_eq!(vectors.len(), 2, "one vector per distinct set of erased fields");
    assert!(redacted("sender.email", 0) && redacted("sender.email", 2));
    assert!(redacted("recipient.email", 1));
    assert!(
        !redacted("sender.email", 1),
        "another subject's field of a shared row survives"
    );
    assert!(!redacted("recipient.email", 0) && !redacted("recipient.email", 2));
    assert!(!redacted("sender.email", 3) && !redacted("recipient.email", 3));
}
