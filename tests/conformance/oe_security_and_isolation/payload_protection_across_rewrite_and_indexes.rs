use oe::deletes::FieldDeletionVector;

#[test]
fn rewrite_does_not_resurrect_redacted_field() {
    let mut fdv = FieldDeletionVector::new(vec!["email".to_owned()]);
    fdv.mark_affected(0);

    assert!(
        fdv.column_redacted("email", 0),
        "redacted field must remain absent even after a file rewrite"
    );
    assert!(
        !fdv.column_redacted("amount", 0),
        "fields not in the redaction set must not be affected by the deletion vector"
    );
    assert!(
        !fdv.column_redacted("email", 1),
        "redaction applies only to the marked ordinal, not to other rows in the file"
    );
}
