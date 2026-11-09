use oe::indexes::constant_flags::{
    ColumnFlags, MetadataAnswer, answer_equality_from_flags, answer_presence_from_flags,
};

#[test]
fn equality_on_a_constant_column_answered_from_metadata() {
    let flags = ColumnFlags::constant(1);

    let answer = answer_equality_from_flags(&flags, 1);
    assert_eq!(
        answer,
        MetadataAnswer::AllRowsMatch,
        "a constant-flag page whose value matches the filter must answer AllRowsMatch without decode"
    );

    let answer_no = answer_equality_from_flags(&flags, 2);
    assert_eq!(
        answer_no,
        MetadataAnswer::NoRowsMatch,
        "a constant-flag page whose value differs from the filter must answer NoRowsMatch without decode"
    );

    assert_ne!(
        answer_equality_from_flags(&flags, 1),
        MetadataAnswer::MustDecode,
        "a constant-flag page must never fall through to MustDecode"
    );
}

#[test]
fn predicate_on_an_all_null_column_rejects_without_decode() {
    let flags = ColumnFlags::all_null();

    let answer = answer_presence_from_flags(&flags);
    assert_eq!(
        answer,
        MetadataAnswer::NoRowsMatch,
        "an all-null page must answer NoRowsMatch for any value predicate without decode"
    );

    let eq_answer = answer_equality_from_flags(&flags, 42);
    assert_eq!(
        eq_answer,
        MetadataAnswer::NoRowsMatch,
        "an all-null page must also answer equality predicates with NoRowsMatch, no decode"
    );

    assert_ne!(
        answer_presence_from_flags(&flags),
        MetadataAnswer::MustDecode,
        "an all-null page must never fall through to MustDecode"
    );
}
