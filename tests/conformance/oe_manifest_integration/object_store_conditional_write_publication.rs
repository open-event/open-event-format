use oe::error::PublishError;
use oe::events::{SequenceRange, TenantId};
use oe::invariants::PublishedSet;
use oe::invariants::sim::SimulatedPublishedSet;
use oe::lifecycle::{FileType, ManifestGeneration, OeFileEntry, PartState};
use oe::typed_id::TypedIdTestExt;

fn tenant() -> TenantId {
    TenantId::new_test_id(13)
}

fn make_entry(file_id: u128, range: SequenceRange) -> OeFileEntry {
    OeFileEntry {
        coverage: range,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0u8; 32],
        file_id,
        file_type: FileType::OeFile,
        footer_len: None,
        entity_summary: None,
        graph_event_id_filter: None,
        graph_summary: None,
        deletion_vector: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state: PartState::Active,
        profile: oe::layout::Profile::Event,
        required_feature_flags: 0,
        size_bytes: 1024,
        tenant_id: tenant(),
        tree_len: None,
    }
}

#[test]
fn concurrent_publishers_race_on_the_pointer() {
    let range_a = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 10,
    };
    let range_b = SequenceRange {
        epoch: 1,
        first_sequence: 11,
        last_sequence: 20,
    };
    let entry_a = make_entry(0xAA, range_a);
    let entry_b = make_entry(0xBB, range_b);

    let mut store = SimulatedPublishedSet::new();
    let (head, _) = store.head().unwrap();

    let gen1 = ManifestGeneration {
        generation: head + 1,
        files: vec![entry_b.clone()],
        ..Default::default()
    };
    store.put_generation(gen1).unwrap();
    store.advance_head(head, head + 1).unwrap();

    let gen1_loser = ManifestGeneration {
        generation: 1,
        files: vec![entry_a.clone()],
        ..Default::default()
    };
    let err = store.put_generation(gen1_loser).unwrap_err();
    assert!(
        matches!(err, PublishError::GenerationExists),
        "create-only put must fail with GenerationExists when the id is taken"
    );

    let (new_head, latest) = store.head().unwrap();
    assert_eq!(new_head, 1, "head must have advanced to the winner's generation");
    let mut gen2_files = latest.files.clone();
    gen2_files.push(entry_a);
    let gen2 = ManifestGeneration {
        generation: new_head + 1,
        files: gen2_files,
        ..Default::default()
    };
    store.put_generation(gen2).unwrap();
    store.advance_head(new_head, new_head + 1).unwrap();

    let winner_gen = store.generation(1).unwrap();
    assert_eq!(winner_gen.files.len(), 1);
    assert_eq!(
        winner_gen.files[0].file_id, 0xBB,
        "winner's generation must not be overwritten"
    );

    let final_gen = store.generation(2).unwrap();
    assert_eq!(final_gen.files.len(), 2, "rebased generation must include both entries");
    assert_eq!(store.head().unwrap().0, 2);
}
