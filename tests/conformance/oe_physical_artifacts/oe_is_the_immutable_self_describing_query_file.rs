use crate::support;
use oe::layout::footer::encode_footer;
use oe::layout::optional_features;
use oe::layout::reader::OeFile;

fn with_footer(bytes: &[u8], blob: &[u8]) -> Vec<u8> {
    let original_len = {
        let tail = &bytes[bytes.len() - 12..bytes.len() - 4];
        u64::from_le_bytes(tail.try_into().unwrap()) as usize
    };
    let data_end = bytes.len() - 12 - original_len;
    let mut out = bytes[..data_end].to_vec();
    out.extend_from_slice(blob);
    out.extend_from_slice(&(blob.len() as u64).to_le_bytes());
    out.extend_from_slice(b"OEF1");
    out
}

#[test]
fn planner_inspects_feature_directory() {
    let built = support::built_file(20);

    let mut without_per_page = built.footer.clone();
    without_per_page.optional_feature_flags &= !optional_features::PER_PAGE_MARKS;
    let opened = OeFile::open(with_footer(&built.bytes, &encode_footer(&without_per_page)), None).unwrap();
    assert_eq!(opened.usable_optional_features() & optional_features::PER_PAGE_MARKS, 0);

    let mut with_per_page = built.footer.clone();
    with_per_page.optional_feature_flags |= optional_features::PER_PAGE_MARKS;
    let opened = OeFile::open(with_footer(&built.bytes, &encode_footer(&with_per_page)), None).unwrap();
    assert_ne!(opened.usable_optional_features() & optional_features::PER_PAGE_MARKS, 0);
}
