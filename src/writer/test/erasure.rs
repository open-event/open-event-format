use super::*;
use crate::clock::FixedClock;
use crate::events::variant::VariantValue;
use crate::invariants::sim::SimulatedPublishedSet;
use crate::layout::reader::OeFile;
use crate::lifecycle::CompactionJob;
use crate::object_store::oe_object_key;
use crate::object_store::sim::SimObjectStore;
use crate::security::{FileSubjectKeyStore, SubjectPayloadRead, TenantDek, read_subject_payload};
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::BuildRow;
use crate::writer::build::{BuildInput, OeRow};
use crate::writer::merge::execute_compaction;
use crate::writer::merge::tests::{START_NANOS, build_config, payload_of, publish_built, rows_of};
use crate::writer::publish::NoopPeerNotices;

fn subject(n: u128) -> SubjectId {
    SubjectId::new_test_id(n)
}

fn subject_rows(first: u64, last: u64) -> Vec<BuildRow> {
    let mut rows = OeRow::into_build_rows(rows_of(first..=last));
    for row in &mut rows {
        row.subject = Some(subject(u128::from(row.sequence % 3) + 1));
    }
    rows
}

fn reads(file: &OeFile, keys: &FileSubjectKeyStore) -> Vec<SubjectPayloadRead> {
    (0..file.header().row_count)
        .map(|row| read_subject_payload(file, row, keys).unwrap())
        .collect()
}

#[test]
fn erasing_one_subject_tombstones_only_its_rows_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let clock = FixedClock::at(START_NANOS);
    let keys = FileSubjectKeyStore::open(dir.path(), TenantDek::new([3; 32])).unwrap();
    let mut rows = subject_rows(1, 6);
    seal_subject_rows(&mut rows, &keys).unwrap();
    let built = build_oe_file(rows, &build_config()).unwrap();
    let bytes = built.bytes.clone();
    let file = OeFile::open(bytes.clone(), Some(&built.file_seal)).unwrap();

    for (row, read) in reads(&file, &keys).into_iter().enumerate() {
        assert_eq!(read, SubjectPayloadRead::Opened(payload_of(row as u64 + 1)));
    }

    let erased = subject(3);
    keys.erase_subject(erased, &clock).unwrap();
    let check = |keys: &FileSubjectKeyStore| {
        for (row, read) in reads(&file, keys).into_iter().enumerate() {
            let sequence = row as u64 + 1;
            if sequence % 3 == 2 {
                assert_eq!(read, SubjectPayloadRead::Tombstone, "sequence {sequence} erased");
            } else {
                assert_eq!(
                    read,
                    SubjectPayloadRead::Opened(payload_of(sequence)),
                    "sequence {sequence} kept"
                );
            }
        }
    };
    check(&keys);
    let crate::layout::reader::PayloadRead::Value(VariantValue::Binary(raw)) = file.payload(1).unwrap() else {
        panic!("a sealed payload is stored as opaque bytes");
    };
    assert!(!raw.windows(9).any(|window| window == b"message 2"));

    drop(keys);
    let keys = FileSubjectKeyStore::open(dir.path(), TenantDek::new([3; 32])).unwrap();
    check(&keys);
    assert!(matches!(
        seal_subject_rows(&mut subject_rows(2, 2), &keys),
        Err(SubjectPayloadError::SubjectErased)
    ));

    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let mut other = subject_rows(7, 7);
    seal_subject_rows(&mut other, &keys).unwrap();
    let second = build_oe_file(other, &build_config()).unwrap();
    publish_built(&built, 1, 6, &mut published, &objects);
    publish_built(&second, 7, 7, &mut published, &objects);
    let output = execute_compaction(
        &mut OePublisher::default(),
        &CompactionJob {
            expected_output_bytes: 0,
            file_ids: vec![built.file_id, second.file_id],
        },
        &build_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &clock,
    )
    .unwrap();
    let merged = OeFile::open(output.file_bytes, Some(&output.entry.file_seal)).unwrap();
    for (row, read) in reads(&merged, &keys).into_iter().enumerate() {
        let sequence = row as u64 + 1;
        let expected = if sequence % 3 == 2 {
            SubjectPayloadRead::Tombstone
        } else {
            SubjectPayloadRead::Opened(payload_of(sequence))
        };
        assert_eq!(read, expected, "sequence {sequence} after compaction");
    }
}

#[test]
fn the_backlog_rekeys_tenant_key_payloads_once_their_subject_is_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let clock = FixedClock::at(START_NANOS);
    let keys = FileSubjectKeyStore::open(dir.path().join("keys"), TenantDek::new([4; 32])).unwrap();
    let backlog = FileErasureBacklog::open(dir.path().join("backlog.log"));
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();

    let rows = rows_of(1..=4);
    let built = build_oe_file(rows.clone(), &build_config()).unwrap();
    publish_built(&built, 1, 4, &mut published, &objects);
    for row in &rows {
        backlog
            .record_unresolved(built.file_id, row.event.envelope.event_id)
            .unwrap();
    }
    let (alice, bob) = (subject(10), subject(11));
    backlog.resolve(rows[0].event.envelope.event_id, alice).unwrap();
    backlog.resolve(rows[1].event.envelope.event_id, bob).unwrap();
    keys.erase_subject(bob, &clock).unwrap();

    let report = rekey_backlog_file(
        &mut OePublisher::default(),
        built.file_id,
        &backlog,
        &keys,
        &build_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &clock,
    )
    .unwrap();
    assert_eq!(report.sealed, vec![rows[0].event.envelope.event_id]);
    assert_eq!(report.dropped, vec![rows[1].event.envelope.event_id]);

    let (_, head) = published.head().unwrap();
    let output_id = report.output_file_id.unwrap();
    assert_eq!(
        head.snapshot_files().map(|entry| entry.file_id).collect::<Vec<_>>(),
        vec![output_id]
    );
    let entry = head.files.iter().find(|entry| entry.file_id == output_id).unwrap();
    let stored = objects.object(&oe_object_key(entry.tenant_id, output_id)).unwrap();
    let file = OeFile::open(stored, Some(&entry.file_seal)).unwrap();
    let read = reads(&file, &keys);
    assert_eq!(read[0], SubjectPayloadRead::Opened(payload_of(1)));
    assert_eq!(
        read[1],
        SubjectPayloadRead::Plain(crate::layout::reader::PayloadRead::None)
    );
    assert_eq!(
        read[2],
        SubjectPayloadRead::Plain(crate::layout::reader::PayloadRead::Value(payload_of(3)))
    );

    assert_eq!(backlog.pending().unwrap().len(), 2);
    keys.erase_subject(alice, &clock).unwrap();
    assert_eq!(
        read_subject_payload(&file, 0, &keys).unwrap(),
        SubjectPayloadRead::Tombstone
    );
}

#[test]
fn rekeying_a_file_keeps_its_deleted_rows_and_redacted_fields_out_of_the_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let clock = FixedClock::at(START_NANOS);
    let keys = FileSubjectKeyStore::open(dir.path().join("keys"), TenantDek::new([4; 32])).unwrap();
    let backlog = FileErasureBacklog::open(dir.path().join("backlog.log"));
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();

    let rows = rows_of(1..=4);
    let built = build_oe_file(rows.clone(), &build_config()).unwrap();
    publish_built(&built, 1, 4, &mut published, &objects);
    for row in &rows {
        backlog
            .record_unresolved(built.file_id, row.event.envelope.event_id)
            .unwrap();
    }
    let (head_id, mut next) = published.head().unwrap();
    next.generation = head_id + 1;
    let mut deleted = crate::deletes::DeletionVector::new();
    deleted.mark_deleted(2);
    assert!(next.publish_deletions(built.file_id, &deleted, None).unwrap().is_some());
    let mut redacted = crate::deletes::FieldDeletionVector::new(vec!["body".to_owned()]);
    redacted.mark_affected(1);
    assert!(
        next.publish_field_deletions(built.file_id, &redacted)
            .unwrap()
            .is_some()
    );
    published.put_generation(next).unwrap();
    published.advance_head(head_id, head_id + 1).unwrap();
    backlog.resolve(rows[0].event.envelope.event_id, subject(10)).unwrap();
    backlog.resolve(rows[2].event.envelope.event_id, subject(11)).unwrap();

    let report = rekey_backlog_file(
        &mut OePublisher::default(),
        built.file_id,
        &backlog,
        &keys,
        &build_config(),
        &mut published,
        &objects,
        &mut NoopPeerNotices::default(),
        &clock,
    )
    .unwrap();
    assert_eq!(report.sealed, vec![rows[0].event.envelope.event_id]);
    assert!(report.dropped.is_empty());

    let (_, head) = published.head().unwrap();
    let output_id = report.output_file_id.unwrap();
    let entry = head.files.iter().find(|entry| entry.file_id == output_id).unwrap();
    assert_eq!(entry.deletion_vector, None);
    assert!(entry.field_deletion_vectors.is_empty());
    let stored = objects.object(&oe_object_key(entry.tenant_id, output_id)).unwrap();
    let file = OeFile::open(stored, Some(&entry.file_seal)).unwrap();
    assert_eq!(file.header().row_count, 3, "the deleted row is physically gone");
    let read = reads(&file, &keys);
    assert_eq!(read[0], SubjectPayloadRead::Opened(payload_of(1)));
    let SubjectPayloadRead::Plain(crate::layout::reader::PayloadRead::Value(VariantValue::Object(fields))) = &read[1]
    else {
        panic!("an object payload");
    };
    assert_eq!(
        fields.get("body"),
        Some(&VariantValue::Null),
        "the redacted field stays redacted"
    );
    assert_eq!(fields.get("n"), Some(&VariantValue::Int(2)));
    assert_eq!(
        read[2],
        SubjectPayloadRead::Plain(crate::layout::reader::PayloadRead::Value(payload_of(4)))
    );
}
