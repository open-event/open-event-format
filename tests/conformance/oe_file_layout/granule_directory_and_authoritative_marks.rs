use crate::support;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::layout::reader::OeFile;

#[test]
fn random_access_via_marks() {
    let built = support::built_file(48);
    let file = OeFile::open(built.bytes, None).unwrap();
    let granule = file.footer().granules.last().copied().unwrap();
    let mark = file
        .mark(column_ids::SEQUENCE, 0, granule.granule_id)
        .unwrap()
        .expect("mark exists for (column, projection, granule)");
    let stripe = file
        .footer()
        .stripes
        .iter()
        .find(|s| s.stripe_id == granule.stripe_id)
        .expect("stripe exists for granule");
    let absolute = stripe.file_offset + mark.compressed_offset;
    assert!(absolute >= 4096, "resolved block starts past the header");
    assert!(
        mark.compressed_offset + mark.compressed_size <= stripe.byte_len,
        "a stripe-relative mark stays inside its stripe, so relocating the stripe rewrites one base-offset entry",
    );
    assert_eq!(mark.row_count, granule.row_count);
    let read = file.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
    let ColumnData::U64(values) = read.data else {
        panic!("u64 column")
    };
    assert_eq!(values.len(), granule.row_count as usize);
    assert_eq!(values[0], granule.first_sequence);
}
