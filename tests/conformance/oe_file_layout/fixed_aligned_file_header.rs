use crate::support;
use oe::layout::{HEADER_BLOCK_LEN, decode_header};

#[test]
fn quick_rejection_from_header() {
    let built = support::built_file(8);
    let header_block = &built.bytes[..HEADER_BLOCK_LEN];
    let header = decode_header(header_block).unwrap();
    assert!(header.may_contain_sequence(1, 1, 4));
    assert!(!header.may_contain_sequence(2, 1, 4), "wrong epoch rejected");
    assert!(!header.may_contain_sequence(1, 100, 200), "outside range rejected");
    assert!(!header.may_contain_occurred(i64::MIN, 0), "before coverage rejected");
}
