use oe::events::{SequenceRange, TenantId};
use oe::invariants::PublishedSet;
use oe::invariants::sim::SimulatedPublishedSet;
use oe::lifecycle::{FileType, ManifestGeneration, OeFileEntry, PartState};
use oe::typed_id::TypedIdTestExt;

fn tenant() -> TenantId {
    TenantId::new_test_id(12)
}
const FILE_A: u128 = 0xA1A1;

fn entry_for(range: SequenceRange) -> OeFileEntry {
    OeFileEntry {
        coverage: range,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [1u8; 32],
        file_id: FILE_A,
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
        size_bytes: 4096,
        tenant_id: tenant(),
        tree_len: None,
    }
}

#[test]
fn no_watermark_file_skew() {
    let range = SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 50,
    };
    let mut store = SimulatedPublishedSet::new();

    let (_, head) = store.head().unwrap();
    assert!(
        !head.covers(&range, tenant()),
        "unpublished range must not be covered before publication"
    );
    assert_eq!(head.files.len(), 0, "no files before publication");

    let entry = entry_for(range);
    let gen1 = ManifestGeneration {
        generation: 1,
        files: vec![entry],
        ..Default::default()
    };
    store.put_generation(gen1).unwrap();
    store.advance_head(0, 1).unwrap();

    let (_, published) = store.head().unwrap();
    assert!(
        published.covers(&range, tenant()),
        "coverage must be visible after publication"
    );
    let entry = &published.files[0];
    assert_eq!(entry.coverage, range, "entry coverage must match the published range");
    assert_ne!(
        entry.file_seal, [0u8; 32],
        "file_seal must be set alongside coverage — no partial entry observed"
    );
}
