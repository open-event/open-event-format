use super::*;
use crate::clock::FixedClock;
use crate::typed_id::TypedIdTestExt;

fn dek() -> TenantDek {
    TenantDek::new([7; 32])
}

#[test]
fn a_sealing_key_survives_reopen_and_resumes_past_its_epochs() {
    let dir = tempfile::tempdir().unwrap();
    let subject = SubjectId::new_test_id(1);
    let first_epoch = {
        let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
        let mut key = store.sealing_key(subject).unwrap().unwrap();
        let sealed = key.encrypt(5, 9, b"hello").unwrap();
        let opening = store.opening_key(subject).unwrap().unwrap();
        assert_eq!(
            crate::deletes::rebuild_payload(&opening, 5, 9, &sealed),
            crate::deletes::RebuiltPayload::Payload(b"hello".to_vec())
        );
        key.key_epoch()
    };
    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    let key = store.sealing_key(subject).unwrap().unwrap();
    assert!(
        key.key_epoch() > first_epoch,
        "a restarted checkout never reuses an earlier epoch"
    );
    // The key is the same key: it still opens what was sealed before the restart.
    assert!(store.opening_key(subject).unwrap().is_some());
}

#[test]
fn the_key_file_holds_only_wrapped_material_and_fails_under_another_dek() {
    let dir = tempfile::tempdir().unwrap();
    let subject = SubjectId::new_test_id(2);
    FileSubjectKeyStore::open(dir.path(), dek())
        .unwrap()
        .sealing_key(subject)
        .unwrap();
    let other = FileSubjectKeyStore::open(dir.path(), TenantDek::new([8; 32])).unwrap();
    assert!(other.opening_key(subject).is_err());
}

#[test]
fn erasure_is_logged_overwrites_the_key_and_is_permanent() {
    let dir = tempfile::tempdir().unwrap();
    let clock = FixedClock::at(1_000);
    let erased = SubjectId::new_test_id(3);
    let kept = SubjectId::new_test_id(4);
    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    store.sealing_key(erased).unwrap().unwrap();
    store.sealing_key(kept).unwrap().unwrap();
    let path = store.key_path(erased);
    let live = std::fs::read(&path).unwrap();

    let erasure = store.erase_subject(erased, &clock).unwrap();
    assert_eq!(
        erasure,
        SubjectErasure {
            erased_at_nanos: 1_000,
            jurisdiction_profile_version: None,
            subject: erased
        }
    );
    let after = std::fs::read(&path).unwrap();
    assert!(after.len() < live.len(), "no wrapped key bytes remain");
    assert!(store.opening_key(erased).unwrap().is_none());
    assert!(
        store.sealing_key(erased).unwrap().is_none(),
        "an erased subject never gets a fresh key"
    );
    assert!(store.opening_key(kept).unwrap().is_some());

    clock.advance(5);
    assert_eq!(
        store.erase_subject(erased, &clock).unwrap(),
        erasure,
        "erasing twice keeps the first record"
    );
    drop(store);
    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    assert!(store.is_erased(erased).unwrap());
    assert!(store.sealing_key(erased).unwrap().is_none());
    assert_eq!(store.erasures().unwrap(), vec![erasure]);
}

#[test]
fn reopening_finishes_an_erasure_that_crashed_after_logging() {
    let dir = tempfile::tempdir().unwrap();
    let subject = SubjectId::new_test_id(5);
    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    store.sealing_key(subject).unwrap().unwrap();
    // Simulate a crash right after the erasure log append.
    let mut record = [0u8; ERASURE_RECORD_LEN];
    record[..16].copy_from_slice(&subject.uuid().as_u128().to_le_bytes());
    append_synced(&store.erasure_log(), &record).unwrap();
    drop(store);
    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    assert!(store.opening_key(subject).unwrap().is_none());
}

#[test]
fn a_zero_filled_key_file_reads_as_destroyed() {
    let dir = tempfile::tempdir().unwrap();
    let subject = SubjectId::new_test_id(6);
    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    store.sealing_key(subject).unwrap().unwrap();
    let len = std::fs::read(store.key_path(subject)).unwrap().len();
    std::fs::write(store.key_path(subject), vec![0u8; len]).unwrap();
    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    assert!(store.opening_key(subject).unwrap().is_none());
    assert!(store.sealing_key(subject).unwrap().is_none());
}

#[test]
fn the_backlog_tracks_unresolved_resolved_and_rekeyed_events() {
    let dir = tempfile::tempdir().unwrap();
    let backlog = FileErasureBacklog::open(dir.path().join("backlog.log"));
    let (a, b) = (EventId::new_test_id(1), EventId::new_test_id(2));
    let subject = SubjectId::new_test_id(9);
    backlog.record_unresolved(10, a).unwrap();
    backlog.record_unresolved(10, b).unwrap();
    backlog.resolve(b, subject).unwrap();
    let reopened = FileErasureBacklog::open(dir.path().join("backlog.log"));
    assert_eq!(
        reopened.pending().unwrap(),
        vec![
            BacklogEntry {
                event_id: a,
                file_id: 10,
                subject: None
            },
            BacklogEntry {
                event_id: b,
                file_id: 10,
                subject: Some(subject)
            },
        ]
    );
    reopened.mark_rekeyed(b).unwrap();
    assert_eq!(backlog.pending().unwrap().len(), 1);
}

#[test]
fn the_erasure_log_pins_the_jurisdiction_profile_version_and_reads_legacy_records() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = SubjectId::new_test_id(10);
    let pinned = SubjectId::new_test_id(11);
    // A record of the original, unversioned log format.
    let mut record = [0u8; ERASURE_RECORD_LEN];
    record[..16].copy_from_slice(&legacy.uuid().as_u128().to_le_bytes());
    record[16..].copy_from_slice(&77u64.to_le_bytes());
    std::fs::write(dir.path().join("erasures.log"), record).unwrap();

    let store = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    assert!(store.is_erased(legacy).unwrap(), "a legacy erasure is finished on open");
    store.sealing_key(pinned).unwrap().unwrap();
    let erasure = store
        .erase_subject_under_profile(pinned, Some(3), &FixedClock::at(500))
        .unwrap();
    assert_eq!(erasure.jurisdiction_profile_version, Some(3));

    let reopened = FileSubjectKeyStore::open(dir.path(), dek()).unwrap();
    assert_eq!(
        reopened.erasures().unwrap(),
        vec![
            SubjectErasure {
                erased_at_nanos: 77,
                jurisdiction_profile_version: None,
                subject: legacy
            },
            SubjectErasure {
                erased_at_nanos: 500,
                jurisdiction_profile_version: Some(3),
                subject: pinned
            },
        ]
    );
    assert!(reopened.opening_key(pinned).unwrap().is_none());
}
