use super::*;
use crate::clock::FixedClock;
use crate::indexes::artifact::ArtifactKind;
use crate::invariants::sim::SimulatedPublishedSet;
use crate::lifecycle::{FooterMirrorObject, IndexArtifactRef};
use crate::object_store::sim::SimObjectStore;
use crate::writer::build::build_oe_file;
use crate::writer::merge::tests::{START_NANOS, active_entry, build_config, rows_of};

fn artifact(key: &str) -> IndexArtifactRef {
    IndexArtifactRef {
        artifact_blake3: [0; 32],
        column_id: 1,
        covered_file_id: 1,
        kind: ArtifactKind::BinaryFuse,
        object_key: key.to_owned(),
    }
}

fn mirror(key: &str) -> FooterMirrorObject {
    FooterMirrorObject {
        blake3: [0; 32],
        path: key.to_owned(),
        size_bytes: 1,
    }
}

fn push(published: &mut SimulatedPublishedSet, edit: impl FnOnce(&mut ManifestGeneration)) -> u64 {
    let (head_id, mut next) = published.head().unwrap();
    next.generation = head_id + 1;
    edit(&mut next);
    published.put_generation(next).unwrap();
    published.advance_head(head_id, head_id + 1).unwrap();
    head_id + 1
}

fn dv_key(entry: &OeFileEntry) -> Vec<String> {
    vec![format!("dv/{:032x}", entry.file_id)]
}

#[test]
fn dropped_mirrors_artifacts_and_deletion_vectors_are_deleted_only_after_the_horizon_and_safety_window() {
    let objects = SimObjectStore::new();
    for key in ["mirror/1", "mirror/2", "idx/a", "idx/b"] {
        objects.put_if_absent(key, b"x").unwrap();
    }
    let file = build_oe_file(rows_of(1..=2), &build_config()).unwrap();
    let entry = active_entry(&file, 1, 2);
    objects.put_if_absent(&dv_key(&entry)[0], b"dv").unwrap();

    let mut published = SimulatedPublishedSet::new();
    let first = push(&mut published, |next| {
        next.footer_mirror = Some(mirror("mirror/1"));
        next.index_artifacts = vec![artifact("idx/a"), artifact("idx/b")];
        next.files.push(entry.clone());
    });
    // The next generation drops the first mirror, index artifact `a`, and the file (whose sweep is over).
    let second = push(&mut published, |next| {
        next.footer_mirror = Some(mirror("mirror/2"));
        next.index_artifacts = vec![artifact("idx/b")];
        next.files.clear();
    });

    let clock = FixedClock::at(START_NANOS);
    let policy = SweepPolicy {
        in_flight_query_horizon_nanos: 1_000,
        safety_window_nanos: 2_000,
    };
    let mut ledger = SimDroppedObjectLedger::default();

    // A query still reads the first generation: nothing it may fetch is even noted.
    let report =
        sweep_dropped_objects(&published, &objects, &mut ledger, &policy, first, Some(&dv_key), &clock).unwrap();
    assert!(report.deleted.is_empty());
    assert!(
        report
            .noted
            .iter()
            .all(|key| !["mirror/1", "idx/a"].contains(&key.as_str()))
    );

    // No query reads it any more: the dropped objects are noted, but wait out the safety window.
    let report = sweep_dropped_objects(
        &published,
        &objects,
        &mut ledger,
        &policy,
        second,
        Some(&dv_key),
        &clock,
    )
    .unwrap();
    let mut noted = report.noted.clone();
    noted.sort();
    assert_eq!(
        noted,
        vec![dv_key(&entry)[0].clone(), "idx/a".to_owned(), "mirror/1".to_owned()]
    );
    assert!(report.deleted.is_empty());
    clock.advance(1_999);
    let report = sweep_dropped_objects(
        &published,
        &objects,
        &mut ledger,
        &policy,
        second,
        Some(&dv_key),
        &clock,
    )
    .unwrap();
    assert!(report.deleted.is_empty());
    assert!(objects.object("mirror/1").is_some());

    clock.advance(1);
    let report = sweep_dropped_objects(
        &published,
        &objects,
        &mut ledger,
        &policy,
        second,
        Some(&dv_key),
        &clock,
    )
    .unwrap();
    assert_eq!(report.deleted.len(), 3);
    for key in ["mirror/1", "idx/a", dv_key(&entry)[0].as_str()] {
        assert!(objects.object(key).is_none(), "{key} deleted");
    }
    for key in ["mirror/2", "idx/b"] {
        assert!(objects.object(key).is_some(), "{key} still named, kept");
    }
    assert!(ledger.dropped().unwrap().is_empty());
}

#[test]
fn an_object_named_again_is_forgotten_not_deleted() {
    let objects = SimObjectStore::new();
    objects.put_if_absent("idx/a", b"x").unwrap();
    let mut published = SimulatedPublishedSet::new();
    push(&mut published, |next| next.index_artifacts = vec![artifact("idx/a")]);
    push(&mut published, |next| next.index_artifacts.clear());
    let back = push(&mut published, |next| next.index_artifacts = vec![artifact("idx/a")]);
    let clock = FixedClock::at(START_NANOS);
    let policy = SweepPolicy {
        in_flight_query_horizon_nanos: 0,
        safety_window_nanos: 0,
    };
    let mut ledger = SimDroppedObjectLedger::default();
    let report = sweep_dropped_objects(&published, &objects, &mut ledger, &policy, back, None, &clock).unwrap();
    assert_eq!(report.noted, vec!["idx/a".to_owned()]);
    assert!(report.deleted.is_empty());
    assert!(objects.object("idx/a").is_some());
    assert!(ledger.dropped().unwrap().is_empty());
}

#[test]
fn external_deletion_vector_objects_are_swept_without_a_sidecar_closure() {
    let objects = SimObjectStore::new();
    let file = build_oe_file(rows_of(1..=2), &build_config()).unwrap();
    let entry = active_entry(&file, 1, 2);
    let file_id = entry.file_id;
    let mut published = SimulatedPublishedSet::new();
    let mut vector = crate::deletes::DeletionVector::new();
    vector.mark_deleted(1);
    let first = push(&mut published, |next| {
        next.files.push(entry.clone());
        next.publish_deletions_with_limit(file_id, &vector, None, &objects, 0)
            .unwrap();
    });
    let first_key = deletion_vector_object_keys(&published.generation(first).unwrap())[0].clone();
    vector.mark_deleted(0);
    // The next vector generation replaces the first object.
    let second = push(&mut published, |next| {
        next.publish_deletions_with_limit(file_id, &vector, None, &objects, 0)
            .unwrap();
    });
    let second_key = deletion_vector_object_keys(&published.generation(second).unwrap())[0].clone();
    assert_ne!(first_key, second_key);
    // Then the file leaves the catalogue.
    let third = push(&mut published, |next| next.files.clear());

    let clock = FixedClock::at(START_NANOS);
    let policy = SweepPolicy {
        in_flight_query_horizon_nanos: 0,
        safety_window_nanos: 0,
    };
    let mut ledger = SimDroppedObjectLedger::default();
    let report = sweep_dropped_objects(&published, &objects, &mut ledger, &policy, second, None, &clock).unwrap();
    assert_eq!(report.deleted, vec![first_key.clone()]);
    assert!(
        objects.object(&second_key).is_some(),
        "still named by the live generation"
    );
    let report = sweep_dropped_objects(&published, &objects, &mut ledger, &policy, third, None, &clock).unwrap();
    assert_eq!(report.deleted, vec![second_key.clone()]);
    assert!(objects.object(&first_key).is_none() && objects.object(&second_key).is_none());
}
