use super::spec_text;
use oe::compat::check_features;
use oe::error::FormatError;
use oe::layout::required_features;

#[test]
fn unknown_required_feature() {
    assert!(matches!(
        check_features(required_features::ALL | (1 << 62), 0),
        Err(FormatError::UnknownRequiredFeature { .. })
    ));
}

#[test]
fn missing_optional_acceleration() {
    let usable = check_features(required_features::ALL, 0).unwrap();
    assert_eq!(usable, 0, "no optional features declared, file still valid");

    let spec = spec_text();
    assert!(
        spec.contains(
            "The planner SHALL use an optional feature only when the footer declares it and its block checksum \
             verifies; absence of an optional feature SHALL leave the file valid with queries falling back to \
             another valid plan."
        ),
        "spec must commit the planner to falling back to another valid plan when an optional acceleration is absent"
    );
    assert!(
        spec.contains("the planner falls back to another valid plan or an authorized column scan"),
        "spec scenario must state the missing-optional-acceleration outcome explicitly"
    );
}
