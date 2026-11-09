use oe::compat::check_features;
use oe::layout::required_features;

#[test]
fn a_file_declaring_bit_38_is_refused_by_an_older_reader() {
    assert_eq!(required_features::PACKED_SHARED_DICTIONARIES, 1 << 38);
    assert_eq!(required_features::KNOWN, (1 << 40) - 1);
    assert_eq!(
        required_features::PACKED_SHARED_DICTIONARIES & required_features::ALL,
        0
    );
    assert!(check_features(required_features::PACKED_SHARED_DICTIONARIES, 0).is_ok());
    let old_known = required_features::KNOWN & !required_features::PACKED_SHARED_DICTIONARIES;
    assert_ne!(required_features::PACKED_SHARED_DICTIONARIES & !old_known, 0);
    assert!(check_features(1 << 40, 0).is_err());
}
