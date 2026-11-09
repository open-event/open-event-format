use oe::events::{SequenceRange, TenantId};
use oe::lifecycle::{FileType, ManifestGeneration, OeFileEntry, PartState};
use oe::typed_id::TypedIdTestExt;

fn tenant() -> TenantId {
    TenantId::new_test_id(14)
}

#[test]
fn large_rollup_not_in_manifest() {
    let range = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 1_000,
    };
    let entry = OeFileEntry {
        coverage: range,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0u8; 32],
        file_id: 0xAA,
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
        size_bytes: 1 << 30,
        tenant_id: tenant(),
        tree_len: None,
    };

    assert!(
        entry.feature_metadata.is_none(),
        "committed OE entry must not embed a rollup payload"
    );
    assert!(entry.feature_metadata_placement_valid());

    let manifest = ManifestGeneration {
        generation: 1,
        files: vec![entry],
        ..Default::default()
    };
    assert_eq!(manifest.files.len(), 1);
    assert!(
        manifest.files[0].feature_metadata.is_none(),
        "manifest entry must not carry a large rollup regardless of file size"
    );
}
