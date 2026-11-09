use super::*;
use crate::corrections::CorrectionRecord;
use crate::deletes::CorrectionType;
use crate::layout::Profile;
use crate::lifecycle::{FileType, PartState};
use crate::typed_id::TypedIdTestExt;
use std::collections::BTreeMap;

const FILE_ID: u128 = 0xF11E;

fn entry() -> OeFileEntry {
    OeFileEntry {
        coverage: SequenceRange {
            epoch: 1,
            first_sequence: 0,
            last_sequence: 99,
        },
        deletion_vector: None,
        entity_summary: None,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0; 32],
        file_id: FILE_ID,
        file_type: FileType::OeFile,
        footer_len: None,
        graph_event_id_filter: None,
        graph_summary: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state: PartState::Active,
        profile: Profile::Event,
        required_feature_flags: 0,
        size_bytes: 100,
        tenant_id: TenantId::new_test_id(7),
        tree_len: None,
    }
}

fn vector(ordinals: &[u64]) -> DeletionVector {
    let mut vector = DeletionVector::new();
    for &ordinal in ordinals {
        vector.mark_deleted(ordinal);
    }
    vector
}

#[test]
fn each_publish_is_a_new_generation_holding_the_union_of_every_earlier_one() {
    let mut manifest = ManifestGeneration {
        files: vec![entry()],
        ..ManifestGeneration::default()
    };
    assert_eq!(
        manifest.publish_deletions(FILE_ID, &vector(&[3, 4]), None).unwrap(),
        Some(1)
    );
    let first = manifest.files[0].deletion_vector.clone().unwrap();
    assert_eq!(
        manifest.publish_deletions(FILE_ID, &vector(&[4, 9]), Some(-5)).unwrap(),
        Some(2)
    );
    let loaded = manifest.files[0].load_deletion_vector().unwrap().unwrap();
    assert_eq!(loaded.iter().collect::<Vec<_>>(), vec![3, 4, 9]);
    assert_eq!(first.load(FILE_ID).unwrap().iter().collect::<Vec<_>>(), vec![3, 4]);
    let stored = manifest.files[0].deletion_vector.as_ref().unwrap();
    let identity = stored.to_ref();
    assert_eq!(identity.deleted_count, 3);
    assert_eq!(identity.deletion_vector_generation, 2);
    assert_eq!(identity.row_position_domain, RowPositionDomain::PrimaryRowsetOrdinal);
    assert_eq!(stored.aggregate_delta().subtract_count(10), 7);
    assert_eq!(manifest.publish_deletions(0xDEAD, &vector(&[1]), None).unwrap(), None);
}

#[test]
fn a_tampered_or_misdirected_vector_refuses_to_load() {
    let stored = StoredDeletionVector::next_generation(&entry(), &vector(&[1, 2]), None).unwrap();
    assert!(stored.load(FILE_ID + 1).is_err());
    let mut forged = stored.clone();
    forged.deleted_count = 1;
    assert!(forged.load(FILE_ID).is_err());
    let mut forged = stored.clone();
    forged.encoded = encode_deletion_vector(&vector(&[1])).1;
    assert!(forged.load(FILE_ID).is_err());
    let mut forged = stored;
    forged.target_sequence_range.last_sequence = 5;
    assert!(forged.load(FILE_ID).is_err());
}

#[test]
fn a_manifest_entry_written_before_saved_vectors_still_deserializes() {
    let mut json = serde_json::to_value(entry()).unwrap();
    json.as_object_mut().unwrap().remove("deletion_vector");
    let old: OeFileEntry = serde_json::from_value(json).unwrap();
    assert_eq!(old.deletion_vector, None);
    assert!(old.load_deletion_vector().unwrap().is_none());

    let mut current = entry();
    current.deletion_vector = Some(StoredDeletionVector::next_generation(&current, &vector(&[7]), None).unwrap());
    let round: OeFileEntry = serde_json::from_str(&serde_json::to_string(&current).unwrap()).unwrap();
    assert!(round.load_deletion_vector().unwrap().unwrap().is_deleted(7));
}

#[test]
fn manifest_deletes_serves_saved_vectors_and_corrections_to_a_scan() {
    let mut manifest = ManifestGeneration {
        files: vec![entry()],
        ..ManifestGeneration::default()
    };
    manifest.publish_deletions(FILE_ID, &vector(&[2]), None).unwrap();
    let original = EventId::new_test_id(1);
    let resolver = CorrectionResolver::new([CorrectionRecord {
        correcting_event_id: EventId::new_test_id(2),
        field_changes: BTreeMap::new(),
        metadata: CorrectionMetadata {
            correction_epoch: 1,
            correction_generation: 1,
            correction_sequence: 50,
            correction_type: CorrectionType::Replacement,
            corrects_event_id: original,
        },
    }]);
    let deletes = ManifestDeletes {
        corrections: &resolver,
        manifest: &manifest,
        objects: None,
    };
    let tenant = TenantId::new_test_id(7);
    assert!(deletes.deletion_vector(tenant, FILE_ID).unwrap().unwrap().is_deleted(2));
    assert!(
        deletes
            .deletion_vector(TenantId::new_test_id(8), FILE_ID)
            .unwrap()
            .is_none()
    );
    assert!(
        deletes
            .is_correction(tenant, SequencePoint { epoch: 1, sequence: 50 })
            .unwrap()
    );
    assert_eq!(
        deletes
            .latest_correction(tenant, original)
            .unwrap()
            .unwrap()
            .correction_sequence,
        50
    );
}

#[test]
fn a_large_vector_is_stored_as_a_verified_external_object_and_loads_through_the_store() {
    use crate::object_store::sim::SimObjectStore;
    let objects = SimObjectStore::new();
    let mut manifest = ManifestGeneration {
        files: vec![entry()],
        ..ManifestGeneration::default()
    };
    manifest
        .publish_deletions_in(FILE_ID, &vector(&[1]), None, &objects)
        .unwrap();
    assert!(!manifest.files[0].deletion_vector.as_ref().unwrap().is_external());
    assert!(objects.keys().is_empty());

    let big: Vec<u64> = (0..200_000).step_by(3).collect();
    assert_eq!(
        manifest
            .publish_deletions_in(FILE_ID, &vector(&big), None, &objects)
            .unwrap(),
        Some(2)
    );
    let stored = manifest.files[0].deletion_vector.clone().unwrap();
    assert!(stored.is_external());
    assert!(stored.encoded.is_empty());
    let key = deletion_vector_object_key(TenantId::new_test_id(7), FILE_ID, 2);
    assert_eq!(manifest.files[0].deletion_vector_object_key(), Some(key.as_str()));
    assert_eq!(deletion_vector_object_keys(&manifest), vec![key.clone()]);
    assert_eq!(stored.to_ref().encoding, DeletionVectorEncoding::NativeBlock);
    assert!(manifest.files[0].load_deletion_vector().is_err());
    let loaded = manifest.files[0].load_deletion_vector_from(&objects).unwrap().unwrap();
    assert!(loaded.is_deleted(1) && loaded.is_deleted(3) && !loaded.is_deleted(4));
    let round: OeFileEntry = serde_json::from_str(&serde_json::to_string(&manifest.files[0]).unwrap()).unwrap();
    assert_eq!(round.deletion_vector, Some(stored.clone()));

    manifest
        .publish_deletions_in(FILE_ID, &vector(&[4]), None, &objects)
        .unwrap();
    let loaded = manifest.files[0].load_deletion_vector_from(&objects).unwrap().unwrap();
    assert!(loaded.is_deleted(4) && loaded.is_deleted(1));
    assert!(objects.object(&key).is_some(), "the earlier object is immutable");

    let mut bytes = objects.object(&key).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    objects.delete(&key).unwrap();
    objects.put_if_absent(&key, &bytes).unwrap();
    assert!(stored.load_with(FILE_ID, Some(&objects)).is_err());
    objects.delete(&key).unwrap();
    assert!(stored.load_with(FILE_ID, Some(&objects)).is_err());
    assert!(stored.load_with(FILE_ID, None).is_err());
}

#[test]
fn a_retried_external_publish_reuses_its_own_object_but_refuses_foreign_bytes() {
    use crate::object_store::sim::SimObjectStore;
    let objects = SimObjectStore::new();
    let first =
        StoredDeletionVector::next_generation_with_limit(&entry(), &vector(&[5]), None, Some(&objects), 0).unwrap();
    let again =
        StoredDeletionVector::next_generation_with_limit(&entry(), &vector(&[5]), None, Some(&objects), 0).unwrap();
    assert_eq!(first, again);
    assert!(
        StoredDeletionVector::next_generation_with_limit(&entry(), &vector(&[6]), None, Some(&objects), 0).is_err()
    );
}
