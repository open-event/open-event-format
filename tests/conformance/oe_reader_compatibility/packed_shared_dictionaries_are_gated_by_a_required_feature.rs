//! Checks that a file carrying a compressed union alphabet (footer section 45) declares required feature bit 38, so a
//! reader that would skip the section refuses the file instead of failing on its first shared-scope block.
use oe::compat::check_features;
use oe::layout::required_features;

/// conformance:
/// oe-reader-compatibility/packed-shared-dictionaries-are-gated-by-a-required-feature/a-file-declaring-bit-38-is-refused-by-an-older-reader
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
    // Bit 40 is not yet assigned, so a file declaring it is refused.
    assert!(check_features(1 << 40, 0).is_err());
}
