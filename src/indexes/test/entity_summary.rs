use super::*;
use crate::events::SequenceRange;
use crate::layout::Profile;
use crate::lifecycle::{FileType, PartState};
use crate::typed_id::TypedIdTestExt;

fn entry(file_id: u128, tenant: u128, summary: Option<EntitySummary>, part_state: PartState) -> OeFileEntry {
    OeFileEntry {
        coverage: SequenceRange {
            epoch: 1,
            first_sequence: file_id as u64 * 100,
            last_sequence: file_id as u64 * 100 + 99,
        },
        entity_summary: summary,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0u8; 32],
        file_id,
        file_type: FileType::OeFile,
        footer_len: None,
        graph_event_id_filter: None,
        graph_summary: None,
        deletion_vector: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state,
        profile: Profile::Event,
        required_feature_flags: 0,
        size_bytes: 1,
        tenant_id: TenantId::new_test_id(tenant),
        tree_len: None,
    }
}

fn keys(range: std::ops::Range<u64>) -> Vec<EntityKey> {
    range.map(|i| EntityKey::new(i * 3, i)).collect()
}

#[test]
fn a_small_key_set_is_recorded_exactly_sorted_and_deduplicated() {
    let mut input = keys(0..10);
    input.reverse();
    input.extend(keys(0..10));
    let summary = EntitySummary::from_keys(input);
    assert_eq!(summary.exact_keys(), Some(keys(0..10).as_slice()));
    assert_eq!(summary.entity_count(), 10);
    assert!(keys(0..10).into_iter().all(|key| summary.may_hold(key)));
    assert!(!summary.may_hold(EntityKey::new(30, 10)));
    assert!(EntitySummary::from_keys([]).exact_keys().is_some_and(<[_]>::is_empty));
}

#[test]
fn a_large_key_set_becomes_a_filter_without_false_negatives() {
    let count = EXACT_ENTITY_SUMMARY_LIMIT as u64 * 8;
    let summary = EntitySummary::from_keys(keys(0..count));
    assert!(!summary.is_exact());
    assert_eq!(summary.entity_count(), count);
    assert!(keys(0..count).into_iter().all(|key| summary.may_hold(key)));
    let false_positives = keys(count..count + 20_000)
        .into_iter()
        .filter(|key| summary.may_hold(*key))
        .count();
    assert!(false_positives < 600, "{false_positives}");
    assert!(EntitySummary::from_keys(keys(0..EXACT_ENTITY_SUMMARY_LIMIT as u64)).is_exact());
}

#[test]
fn undecodable_filter_bytes_never_rule_a_file_out() {
    let summary = EntitySummary::Bloom {
        entity_count: 3,
        filter: vec![1, 2, 3],
    };
    assert!(summary.may_hold(EntityKey::new(1, 2)));
}

#[test]
fn entries_without_a_summary_deserialize_and_may_hold_anything() {
    let with = entry(1, 7, Some(EntitySummary::from_keys(keys(0..3))), PartState::Active);
    let mut json = serde_json::to_value(&with).unwrap();
    let back: OeFileEntry = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(back, with);
    json.as_object_mut().unwrap().remove("entity_summary");
    let old: OeFileEntry = serde_json::from_value(json).unwrap();
    assert_eq!(old.entity_summary, None);
    assert!(old.may_hold_entity(EntityKey::new(99, 99)));
    assert!(!with.may_hold_entity(EntityKey::new(99, 99)));
}

#[test]
fn candidate_files_are_the_active_files_of_the_tenant_that_may_hold_the_entity() {
    let wanted = keys(5..6)[0];
    let generation = ManifestGeneration {
        files: vec![
            entry(1, 7, Some(EntitySummary::from_keys(keys(0..10))), PartState::Active),
            entry(2, 7, Some(EntitySummary::from_keys(keys(10..20))), PartState::Active),
            entry(3, 7, None, PartState::Active),
            entry(4, 8, Some(EntitySummary::from_keys(keys(0..10))), PartState::Active),
            entry(5, 7, Some(EntitySummary::from_keys(keys(0..10))), PartState::Outdated),
            entry(6, 7, Some(EntitySummary::from_keys(keys(0..2000))), PartState::Active),
        ],
        ..ManifestGeneration::default()
    };
    let ids: Vec<u128> = generation
        .candidate_files_for_entity(TenantId::new_test_id(7), wanted)
        .map(|entry| entry.file_id)
        .collect();
    assert_eq!(ids, vec![1, 3, 6]);
}
