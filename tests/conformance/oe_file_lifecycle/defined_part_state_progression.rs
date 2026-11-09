use crate::support;
use oe::events::SequenceRange;
use oe::lifecycle::{ManifestGeneration, OeFileEntry, PartState};

#[test]
fn sealed_file_not_yet_visible() {
    assert!(PartState::OpenTmp.can_transition_to(PartState::Sealed));
    assert!(PartState::Sealed.can_transition_to(PartState::Active));
    assert!(!PartState::OpenTmp.can_transition_to(PartState::Active));
    assert!(!PartState::Sealed.can_transition_to(PartState::OpenTmp));
    let sealed_entry = OeFileEntry {
        coverage: SequenceRange {
            epoch: 1,
            first_sequence: 1,
            last_sequence: 4,
        },
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0; 32],
        file_id: 1,
        file_type: oe::lifecycle::FileType::OeFile,
        footer_len: None,
        entity_summary: None,
        graph_event_id_filter: None,
        graph_summary: None,
        deletion_vector: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state: PartState::Sealed,
        profile: oe::layout::Profile::Event,
        required_feature_flags: 0,
        size_bytes: 4096,
        tenant_id: support::tenant(),
        tree_len: None,
    };
    let generation = ManifestGeneration {
        generation: 1,
        files: vec![sealed_entry],
        ..Default::default()
    };
    assert_eq!(generation.snapshot_files().count(), 0, "Sealed is not queryable");
}
