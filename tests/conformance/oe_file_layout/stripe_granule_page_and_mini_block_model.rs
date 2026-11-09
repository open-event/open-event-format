use crate::support;
use oe::error::FormatError;
use oe::layout::footer::{Footer, decode_footer, encode_footer};
use oe::layout::reader::OeFile;
use oe::layout::{MAX_PAGE_BYTES, required_features};

#[test]
fn stripe_exceeds_maximum() {
    let built = support::built_file(64);
    assert!(built.footer.stripes.len() > 1, "stripe target splits stripes");
    for stripe in &built.footer.stripes {
        assert!(stripe.byte_len < 512 * 1024 * 1024);
    }
    let mut previous_end: Option<u64> = None;
    for granule in &built.footer.granules {
        if let Some(end) = previous_end {
            assert_eq!(granule.first_sequence, end + 1, "contiguous sequence order");
        }
        previous_end = Some(granule.last_sequence);
    }
}

#[test]
fn oversized_page_on_read() {
    let built = support::built_file(8);
    let target = built
        .footer
        .marks
        .iter()
        .position(|mark| mark.page_count <= 1 && mark.compressed_size > 0)
        .expect("a single-page block that stores bytes");
    let (column_id, granule_id) = (
        built.footer.marks[target].column_id,
        built.footer.marks[target].granule_id,
    );
    let mut row_oriented = built.footer.clone();
    row_oriented.required_feature_flags &= !(required_features::COLUMNAR_MARKS | required_features::STRIPE_MARKS_PAGES);
    row_oriented.marks_page_offsets.clear();
    let mut columnar = built.footer.clone();
    columnar.required_feature_flags &= !required_features::STRIPE_MARKS_PAGES;
    columnar.marks_page_offsets.clear();

    for (footer, checked_at_open) in [(row_oriented, true), (columnar, false)] {
        OeFile::open(with_footer(&built.bytes, &footer), None)
            .and_then(|file| file.read_column(column_id, granule_id))
            .expect("the untampered re-encoded file opens and reads");
        let mut tampered = footer.clone();
        tampered.marks[target].compressed_size = MAX_PAGE_BYTES + 1;
        decode_footer(&encode_footer(&tampered)).unwrap();
        let opened = OeFile::open(with_footer(&built.bytes, &tampered), None);
        if checked_at_open {
            assert!(
                opened.is_err(),
                "row-oriented marks are checked before any column is read"
            );
        }
        assert!(matches!(
            opened.and_then(|file| file.read_column(column_id, granule_id)),
            Err(FormatError::Structural {
                rule: "page/chunk exceeds the maximum size"
            })
        ));
    }
}

fn with_footer(bytes: &[u8], footer: &Footer) -> Vec<u8> {
    let blob = encode_footer(footer);
    let original_blob_len = {
        let tail = &bytes[bytes.len() - 12..bytes.len() - 4];
        u64::from_le_bytes(tail.try_into().unwrap()) as usize
    };
    let data_end = bytes.len() - 12 - original_blob_len;
    let mut out = bytes[..data_end].to_vec();
    out.extend_from_slice(&blob);
    out.extend_from_slice(&(blob.len() as u64).to_le_bytes());
    out.extend_from_slice(b"OEF1");
    out
}
