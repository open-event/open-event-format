use oe::indexes::probabilistic::{MembershipFilterChoice, choose_membership_filter, should_build_probabilistic_filter};

#[test]
fn low_cardinality_field_avoids_probabilistic_filter() {
    let low_cardinality_distinct = 12;
    let many_rows = 5_000_000;
    assert!(
        !should_build_probabilistic_filter(low_cardinality_distinct, many_rows),
        "a low-cardinality field should not get a probabilistic filter"
    );

    let high_cardinality_distinct = 4_500_000;
    assert!(
        should_build_probabilistic_filter(high_cardinality_distinct, many_rows),
        "a high-cardinality field should get a probabilistic filter"
    );

    assert_eq!(
        choose_membership_filter(true),
        MembershipFilterChoice::BinaryFuse,
        "immutable build-time-known key sets prefer the binary fuse filter"
    );
}
