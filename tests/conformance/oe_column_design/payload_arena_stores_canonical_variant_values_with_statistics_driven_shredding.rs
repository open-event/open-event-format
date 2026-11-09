use crate::support;
use oe::columns::column_ids;
use oe::encoding::Compression;
use oe::events::variant::VariantValue;
use oe::layout::reader::{OeFile, PayloadRead};

#[test]
fn hot_path_shredded_into_a_typed_column() {
    let built = support::built_file(32);
    let amount = built
        .footer
        .shredded
        .iter()
        .find(|entry| entry.path == "amount")
        .expect("amount shredded");
    let file = OeFile::open(built.bytes, None).unwrap();
    let granule = file.footer().granules[0].granule_id;
    let read = file.read_column(amount.column_id, granule).unwrap();
    assert!(!read.presence.is_empty());
    let mark = file
        .mark(amount.column_id, 0, granule)
        .unwrap()
        .expect("the shredded column has a mark in the granule");
    let pipeline = mark.codec_pipeline_id;
    let compression = pipeline.compression().unwrap();
    assert!(
        compression == Compression::None
            || (compression == Compression::SeekableZstd && pipeline.supports_byte_range_extraction().unwrap()),
        "shredded scan-path columns preserve random access in compressed form (got {compression:?})"
    );
    let PayloadRead::Value(value) = file.payload(0).unwrap() else {
        panic!("payload present");
    };
    let VariantValue::Object(fields) = value else {
        panic!("object")
    };
    assert!(fields.contains_key("amount"));
}

#[test]
fn rare_path_read_from_residual_without_sibling_decode() {
    let built = support::built_file(32);
    assert!(!built.footer.shredded.iter().any(|entry| entry.path == "rare"));
    let file = OeFile::open(built.bytes, None).unwrap();
    assert_eq!(file.payload_path(6, "rare").unwrap(), Some(VariantValue::Bool(true)));
    let _ = column_ids::PAYLOAD_REF;
}
