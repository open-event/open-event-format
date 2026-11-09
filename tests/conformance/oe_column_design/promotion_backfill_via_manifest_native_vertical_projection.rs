use crate::support;
use oe::events::SequenceRange;
use oe::lifecycle::{FileType, ManifestGeneration, OeFileEntry, PartState};

#[test]
fn historical_backfill_without_payload_rewrite() {
    let base = support::built_file(8);
    let base_bytes_snapshot = base.bytes.clone();

    let coverage = SequenceRange {
        epoch: base.header.min_epoch,
        first_sequence: base.header.min_sequence,
        last_sequence: base.header.max_sequence,
    };

    let base_entry = OeFileEntry {
        coverage,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: base.file_seal,
        file_id: base.file_id,
        file_type: FileType::OeFile,
        footer_len: None,
        entity_summary: None,
        graph_event_id_filter: None,
        graph_summary: None,
        deletion_vector: None,
        optional_feature_flags: base.footer.optional_feature_flags,
        part_index: 0,
        part_state: PartState::Active,
        profile: oe::layout::Profile::Event,
        required_feature_flags: base.footer.required_feature_flags,
        size_bytes: base.bytes.len() as u64,
        tenant_id: support::tenant(),
        tree_len: None,
    };

    let sidecar_entry = OeFileEntry {
        coverage,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0xab; 32],
        file_id: base.file_id + 1,
        file_type: FileType::DerivedColumns,
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
        size_bytes: 512,
        tenant_id: support::tenant(),
        tree_len: None,
    };

    let manifest = ManifestGeneration {
        files: vec![base_entry, sidecar_entry],
        generation: 1,
        ..Default::default()
    };

    assert_eq!(manifest.snapshot_files().count(), 2);

    assert_eq!(manifest.files[0].coverage, manifest.files[1].coverage);
    assert_eq!(manifest.files[1].file_type, FileType::DerivedColumns);

    assert_eq!(
        base.bytes, base_bytes_snapshot,
        "base file bytes must not change during backfill"
    );
}

#[test]
fn compaction_folds_the_sidecar() {
    let coverage = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 8,
    };

    let base_entry = OeFileEntry {
        coverage,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0x11; 32],
        file_id: 101,
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
        tenant_id: support::tenant(),
        tree_len: None,
    };
    let sidecar_entry = OeFileEntry {
        coverage,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0x22; 32],
        file_id: 102,
        file_type: FileType::DerivedColumns,
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
        size_bytes: 256,
        tenant_id: support::tenant(),
        tree_len: None,
    };
    let before_compaction = ManifestGeneration {
        files: vec![base_entry.clone(), sidecar_entry.clone()],
        generation: 1,
        ..Default::default()
    };

    assert_eq!(before_compaction.snapshot_files().count(), 2);

    assert!(PartState::Active.can_transition_to(PartState::Outdated));

    let consolidated = OeFileEntry {
        coverage,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0x33; 32],
        file_id: 103,
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
        size_bytes: 2048,
        tenant_id: support::tenant(),
        tree_len: None,
    };
    let after_compaction = ManifestGeneration {
        files: vec![
            OeFileEntry {
                part_state: PartState::Outdated,
                ..base_entry
            },
            OeFileEntry {
                part_state: PartState::Outdated,
                ..sidecar_entry
            },
            consolidated,
        ],
        generation: 2,
        ..Default::default()
    };

    let snapshot: Vec<_> = after_compaction.snapshot_files().collect();
    assert_eq!(
        snapshot.len(),
        1,
        "only the consolidated base file should be active after compaction"
    );
    assert_eq!(snapshot[0].file_type, FileType::OeFile);
    assert_eq!(snapshot[0].file_id, 103);
    assert_eq!(snapshot[0].size_bytes, 2048);
}
