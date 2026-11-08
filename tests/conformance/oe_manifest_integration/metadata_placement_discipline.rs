//! Conformance tests for metadata placement discipline.
//!
//! The manifest holds only small summaries needed to decide whether to open a file. Large rollups belong in OE
//! aggregate blocks or PreparedView outputs, never embedded in the manifest itself.

use oe::events::{SequenceRange, TenantId};
use oe::lifecycle::{FileType, ManifestGeneration, OeFileEntry, PartState};
use oe::typed_id::TypedIdTestExt;

fn tenant() -> TenantId {
    TenantId::new_test_id(14)
}

/// conformance: oe-manifest-integration/metadata-placement-discipline/large-rollup-not-in-manifest
#[test]
fn large_rollup_not_in_manifest() {
    // A committed OE entry in the manifest carries only small fixed-size summaries: coverage, file identity, feature
    // flags, part state, and size. It never carries an embedded rollup payload. The feature_metadata field is None for
    // a regular OE entry; rollups live in OE aggregate blocks or PreparedView outputs.
    let range = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 1_000,
    };
    let entry = OeFileEntry {
        coverage: range,
        feature_metadata: None,
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
        size_bytes: 1 << 30, // The file is 1 GiB on storage.
        tenant_id: tenant(),
        tree_len: None,
    };

    // The entry carries no embedded rollup; only small fixed-size metadata.
    assert!(
        entry.feature_metadata.is_none(),
        "committed OE entry must not embed a rollup payload"
    );
    assert!(entry.feature_metadata_placement_valid());

    // A ManifestGeneration holds only the entry list — there is no global rollup blob at the generation level either.
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
