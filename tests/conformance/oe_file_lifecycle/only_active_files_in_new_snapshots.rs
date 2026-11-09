use crate::support;
use oe::events::SequenceRange;
use oe::lifecycle::{ManifestGeneration, OeFileEntry, PartState};

#[test]
fn outdated_file_after_replacement() {
    let coverage = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 8,
    };
    let outdated = OeFileEntry {
        coverage,
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
        part_state: PartState::Outdated,
        profile: oe::layout::Profile::Event,
        required_feature_flags: 0,
        size_bytes: 4096,
        tenant_id: support::tenant(),
        tree_len: None,
    };
    let replacement = OeFileEntry {
        file_id: 2,
        part_state: PartState::Active,
        ..outdated.clone()
    };
    let generation = ManifestGeneration {
        generation: 2,
        files: vec![outdated, replacement],
        ..Default::default()
    };
    let selected: Vec<u128> = generation.snapshot_files().map(|entry| entry.file_id).collect();
    assert_eq!(selected, vec![2], "new snapshots select only Active files");
    assert!(
        generation.files.iter().any(|entry| entry.file_id == 1),
        "the Outdated file stays referenced for in-flight snapshots"
    );
    assert!(PartState::Active.can_transition_to(PartState::Outdated));
    assert!(PartState::Outdated.can_transition_to(PartState::DeleteOnDestroy));
}
