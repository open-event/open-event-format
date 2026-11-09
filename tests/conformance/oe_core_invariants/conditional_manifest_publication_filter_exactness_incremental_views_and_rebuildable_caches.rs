use oe::lifecycle::ManifestGeneration;

use crate::support;
use oe::invariants::PublishedSet;
use oe::invariants::sim::SimulatedPublishedSet;
#[test]
fn lost_manifest_cas_rebases_instead_of_overwriting() {
    let mut set = SimulatedPublishedSet::new();
    let (head, base) = set.head().unwrap();
    let winner = ManifestGeneration {
        generation: head + 1,
        files: base.files.clone(),
        ..Default::default()
    };
    set.put_generation(winner).unwrap();
    set.advance_head(head, head + 1).unwrap();
    let lost = set.advance_head(head, head + 1);
    assert!(lost.is_err());
    let (new_head, latest) = set.head().unwrap();
    assert_eq!(new_head, 1);
    let mut rebased = ManifestGeneration {
        generation: new_head + 1,
        files: latest.files,
        ..Default::default()
    };
    rebased.files.push(oe::lifecycle::OeFileEntry {
        coverage: oe::events::SequenceRange {
            epoch: 1,
            first_sequence: 1,
            last_sequence: 2,
        },
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0; 32],
        file_id: 7,
        file_type: oe::lifecycle::FileType::OeFile,
        footer_len: None,
        entity_summary: None,
        graph_event_id_filter: None,
        graph_summary: None,
        deletion_vector: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state: oe::lifecycle::PartState::Active,
        profile: oe::layout::Profile::Event,
        required_feature_flags: 0,
        size_bytes: 1,
        tenant_id: support::tenant(),
        tree_len: None,
    });
    set.put_generation(rebased).unwrap();
    set.advance_head(new_head, new_head + 1).unwrap();
    assert_eq!(set.head().unwrap().0, 2);
    assert!(set.generation(1).unwrap().files.is_empty(), "winner not overwritten");
}
