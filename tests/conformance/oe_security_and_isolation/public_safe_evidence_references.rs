use oe::events::families::{Caller, column_allowed};

#[test]
fn evidence_ref_hides_storage_identity() {
    let storage_identity_columns = [
        "object_store_path",
        "local_cache_path",
        "payload_ref",
        "payload_bytes",
        "row_offset",
        "sequence",
        "sequence_key",
        "epoch",
    ];

    for col in storage_identity_columns {
        assert!(
            !column_allowed(col, Caller::Public),
            "{col} must be blocked for public callers — evidence refs may not expose storage identity"
        );
        assert!(
            column_allowed(col, Caller::Internal),
            "{col} must remain readable by internal callers that build the opaque cursor"
        );
    }

    for hash_col in [
        "entity_id_hash",
        "entity_id_hash_low",
        "entity_id_hash_high",
        "actor_id_hash_low",
        "account_id_hash_low",
        "trace_id_hash_low",
        "dedupe_hash_low",
        "dedupe_hash_high",
    ] {
        assert!(
            !column_allowed(hash_col, Caller::Public),
            "{hash_col} must be blocked for public callers"
        );
    }
}
