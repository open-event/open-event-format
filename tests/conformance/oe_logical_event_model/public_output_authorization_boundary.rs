use oe::events::families::{Caller, column_allowed};

#[test]
fn internal_identity_requested_by_public_caller() {
    for column in ["tenant_id", "epoch", "sequence", "payload_ref", "object_store_path"] {
        assert!(
            !column_allowed(column, Caller::Public),
            "{column} must be blocked for public callers"
        );
        assert!(
            column_allowed(column, Caller::Internal),
            "{column} must be allowed for internal callers"
        );
    }
    for column in ["entity_id_hash_low", "dedupe_hash_low", "local_cache_path"] {
        assert!(!column_allowed(column, Caller::Public), "{column} must be blocked");
    }
}
