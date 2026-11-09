use crate::support;
use oe::layout::footer::{decode_footer, encode_footer};
use oe::layout::reader::OeFile;

fn retail(bytes: &[u8], blob: &[u8]) -> Vec<u8> {
    let original_blob_len = {
        let tail = &bytes[bytes.len() - 12..bytes.len() - 4];
        u64::from_le_bytes(tail.try_into().unwrap()) as usize
    };
    let data_end = bytes.len() - 12 - original_blob_len;
    let mut out = bytes[..data_end].to_vec();
    out.extend_from_slice(blob);
    out.extend_from_slice(&(blob.len() as u64).to_le_bytes());
    out.extend_from_slice(b"OEF1");
    out
}

#[test]
fn older_reader_newer_optional_block() {
    let built = support::built_file(8);
    let mut footer = built.footer.clone();
    footer.optional_feature_flags |= 1 << 63;
    let blob = encode_footer(&footer);
    let file = OeFile::open(retail(&built.bytes, &blob), None).unwrap();
    assert_eq!(
        file.usable_optional_features() & (1 << 63),
        0,
        "the unknown optional feature is ignored, not used"
    );
    assert_eq!(file.header().row_count, 8);
    decode_footer(&blob).unwrap();
}
