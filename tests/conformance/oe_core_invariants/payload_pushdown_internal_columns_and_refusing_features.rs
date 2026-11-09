use crate::support;
use oe::compat::check_features;
use oe::error::FormatError;
use oe::events::variant::VariantValue;
use oe::layout::reader::OeFile;
use oe::layout::required_features;

#[test]
fn unknown_required_feature_refuses() {
    let unknown = required_features::ALL | (1 << 59);
    assert!(matches!(
        check_features(unknown, 0),
        Err(FormatError::UnknownRequiredFeature { .. })
    ));
}

#[test]
fn single_payload_path_extracted_without_sibling_decode() {
    let built = support::built_file(24);
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_eq!(file.payload_path(3, "rare").unwrap(), Some(VariantValue::Bool(true)));
    assert_eq!(file.payload_path(4, "rare").unwrap(), None);
    assert!(file.footer().shredded.iter().any(|entry| entry.path == "amount"));
    assert_eq!(file.payload_path(5, "amount").unwrap(), Some(VariantValue::Int(1_005)));
}
