use crate::support;
use oe::layout::LayoutClass;

#[test]
fn small_file_uses_compact() {
    let built = support::built_file(16);
    assert!(built.bytes.len() < 10 * 1024 * 1024);
    assert_eq!(built.header.layout_class, LayoutClass::Compact);
    oe::layout::reader::OeFile::open(built.bytes, None).unwrap();
}
