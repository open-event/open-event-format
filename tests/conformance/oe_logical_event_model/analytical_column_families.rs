use oe::events::families::{Caller, authorize_columns};

#[test]
fn internal_embedding_column_requested_publicly() {
    let (allowed, dropped) = authorize_columns(&["event_type_id", "embedding_v1", "internal_score"], Caller::Public);
    assert_eq!(allowed, vec!["event_type_id"]);
    assert_eq!(dropped, vec!["embedding_v1", "internal_score"]);
}
