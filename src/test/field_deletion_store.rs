//! Unit tests for saved field-level deletion vectors.

use super::*;
use crate::events::TenantId;
use crate::layout::Profile;
use crate::lifecycle::{FileType, PartState};
use crate::typed_id::TypedIdTestExt;

const FILE_ID: u128 = 0xF11E;

fn entry() -> OeFileEntry {
    OeFileEntry {
        coverage: SequenceRange {
            epoch: 1,
            first_sequence: 0,
            last_sequence: 99,
        },
        deletion_vector: None,
        entity_summary: None,
        feature_metadata: None,
        field_deletion_vectors: Vec::new(),
        file_seal: [0; 32],
        file_id: FILE_ID,
        file_type: FileType::OeFile,
        footer_len: None,
        graph_event_id_filter: None,
        graph_summary: None,
        optional_feature_flags: 0,
        part_index: 0,
        part_state: PartState::Active,
        profile: Profile::Event,
        required_feature_flags: 0,
        size_bytes: 100,
        tenant_id: TenantId::new_test_id(7),
        tree_len: None,
    }
}

fn redact(columns: &[&str], rows: &[u64]) -> FieldDeletionVector {
    let mut vector = FieldDeletionVector::new(columns.iter().map(|c| (*c).to_owned()).collect());
    rows.iter().for_each(|row| vector.mark_affected(*row));
    vector
}

#[test]
fn publishing_merges_rows_per_column_set_and_hides_only_those_fields() {
    let mut manifest = ManifestGeneration {
        files: vec![entry()],
        ..ManifestGeneration::default()
    };
    assert_eq!(
        manifest
            .publish_field_deletions(FILE_ID, &redact(&["payer.email"], &[2]))
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        manifest
            .publish_field_deletions(FILE_ID, &redact(&["payer.email"], &[5]))
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        manifest
            .publish_field_deletions(FILE_ID, &redact(&["payee_name"], &[2]))
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        manifest
            .publish_field_deletions(FILE_ID, &redact(&["payee_name"], &[]))
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        manifest.publish_field_deletions(0xDEAD, &redact(&["x"], &[1])).unwrap(),
        None
    );

    let entry = &manifest.files[0];
    assert_eq!(entry.field_deletion_vectors.len(), 2);
    let redactions = entry.load_field_deletion_vectors().unwrap();
    assert!(redactions.column_redacted("payer.email", 2));
    assert!(redactions.column_redacted("payer.email", 5));
    assert!(!redactions.column_redacted("payee_name", 5));
    assert!(!redactions.column_redacted("amount", 2));
    assert!(redactions.withholds_raw_payload(5) && !redactions.withholds_raw_payload(3));
    let reference = entry.field_deletion_vectors[0].to_ref();
    assert_eq!(reference.deleted_count, 2);
    assert_eq!(reference.redacted_columns, vec!["payer.email".to_owned()]);
}

#[test]
fn a_tampered_or_misplaced_vector_is_refused() {
    let mut manifest = ManifestGeneration {
        files: vec![entry()],
        ..ManifestGeneration::default()
    };
    manifest
        .publish_field_deletions(FILE_ID, &redact(&["a"], &[1]))
        .unwrap();
    let mut stored = manifest.files[0].field_deletion_vectors[0].clone();
    assert!(stored.load(FILE_ID + 1).is_err());
    stored.redacted_columns = vec!["b".to_owned()];
    assert!(stored.load(FILE_ID).is_err());
}

#[test]
fn entries_written_before_field_vectors_still_deserialize() {
    let mut value = serde_json::to_value(entry()).unwrap();
    value.as_object_mut().unwrap().remove("field_deletion_vectors");
    let back: OeFileEntry = serde_json::from_value(value).unwrap();
    assert!(back.field_deletion_vectors.is_empty());
}
