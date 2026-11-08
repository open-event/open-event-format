use super::*;
use crate::object_store::sim::SimObjectStore;

#[test]
fn published_deletes_load_back_from_the_manifest_generation_by_generation() {
    let store = SimObjectStore::new();
    let mut manifest = ManifestGeneration::default();
    assert_eq!(load_segment_deletes(&store, &manifest, 7).unwrap().generation(), 0);
    manifest.register_vector_segment(7);
    assert_eq!(manifest.vector_segment(7).unwrap().deletes, None);

    let first = SegmentDeletes::new().with_deleted([1, 4]);
    let reference = publish_segment_deletes(&store, &mut manifest, 7, &first).unwrap();
    assert_eq!(reference.generation, 1);
    assert_eq!(reference.object_key, segment_deletes_object_key(7, 1));
    let snapshot = manifest.clone();

    let second = first.with_deleted([9]);
    publish_segment_deletes(&store, &mut manifest, 7, &second).unwrap();
    let loaded = load_segment_deletes(&store, &manifest, 7).unwrap();
    assert_eq!(loaded.generation(), 2);
    assert!(loaded.is_deleted(1) && loaded.is_deleted(9) && !loaded.is_deleted(2));

    let older = load_segment_deletes(&store, &snapshot, 7).unwrap();
    assert_eq!(older.generation(), 1, "an older snapshot keeps its own generation");
    assert!(!older.is_deleted(9));
    assert_eq!(manifest.vector_segments.len(), 1);
}

#[test]
fn a_stale_generation_or_a_conflicting_object_is_refused_and_a_retry_succeeds() {
    let store = SimObjectStore::new();
    let mut manifest = ManifestGeneration::default();
    let first = SegmentDeletes::new().with_deleted([1]);
    publish_segment_deletes(&store, &mut manifest, 3, &first).unwrap();
    assert!(publish_segment_deletes(&store, &mut manifest, 3, &first).is_err());

    // A retry of the same publication against the previous manifest finds the identical object and succeeds.
    let mut retry = ManifestGeneration::default();
    publish_segment_deletes(&store, &mut retry, 3, &first).unwrap();
    // Different rows under the same generation key are refused.
    let mut other = ManifestGeneration::default();
    assert!(publish_segment_deletes(&store, &mut other, 3, &SegmentDeletes::new().with_deleted([2])).is_err());
}

#[test]
fn a_tampered_object_fails_to_load_and_old_manifests_deserialize() {
    let store = SimObjectStore::new();
    let mut manifest = ManifestGeneration::default();
    publish_segment_deletes(&store, &mut manifest, 5, &SegmentDeletes::new().with_deleted([1])).unwrap();
    manifest.vector_segments[0].deletes.as_mut().unwrap().blake3 = [0; 32];
    assert!(matches!(
        load_segment_deletes(&store, &manifest, 5),
        Err(VectorError::Authentication { .. })
    ));

    let mut value = serde_json::to_value(ManifestGeneration::default()).unwrap();
    value.as_object_mut().unwrap().remove("vector_segments");
    let back: ManifestGeneration = serde_json::from_value(value).unwrap();
    assert!(back.vector_segments.is_empty());
}

#[test]
fn a_stored_generation_delta_carries_the_vector_segment_catalogue() {
    let store = SimObjectStore::new();
    let checkpoint = ManifestGeneration::default();
    let mut next = checkpoint.clone();
    next.generation = 1;
    publish_segment_deletes(&store, &mut next, 5, &SegmentDeletes::new().with_deleted([2])).unwrap();
    let delta = crate::object_store::model::GenerationDelta::between(&checkpoint, &next);
    assert_eq!(delta.apply(checkpoint), next);
}
