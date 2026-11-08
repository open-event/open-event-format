//! Checks the packed-lengths FSST layout: a keyless FSST block drops its four-byte offset table for bitpacked value
//! lengths, reads back exactly like the classic layout, and a file holding one declares a required feature so an old
//! reader refuses it at open.
use crate::support;
use oe::columns::column_ids;
use oe::compat::check_features;
use oe::encoding::predicate::{StringPredicate, filter_string_block};
use oe::encoding::{
    CascadeStrategy, ColumnData, StringColumn, Transform, decode_block, decode_block_range, encode_block,
    encode_block_forced_for_conformance,
};
use oe::layout::reader::OeFile;
use oe::layout::required_features;

fn short_ids(rows: usize) -> StringColumn {
    (0..rows)
        .map(|i| {
            if i % 9 == 0 {
                None
            } else {
                Some(format!("@user{i:05}:example.org"))
            }
        })
        .collect()
}

/// conformance:
/// oe-encodings-and-compression/fsst-blocks-may-store-bitpacked-value-lengths/short-high-cardinality-strings-drop-the-offset-table
#[test]
fn short_high_cardinality_strings_drop_the_offset_table() {
    let values = short_ids(2000);
    let data = ColumnData::Strings(values.clone());
    let packed = encode_block(&data, true);
    assert_eq!(packed.pipeline.transform().unwrap(), Transform::FsstPackedString);
    let classic =
        encode_block_forced_for_conformance(&data, true, CascadeStrategy::DecodeOptimized, Transform::FsstString)
            .unwrap();
    assert_eq!(classic.pipeline.transform().unwrap(), Transform::FsstString);
    assert!(packed.bytes.len() < classic.bytes.len());
    assert_eq!(decode_block(packed.pipeline, &packed.bytes).unwrap(), data);
    assert_eq!(
        decode_block_range(packed.pipeline, &packed.bytes, 10, 700).unwrap(),
        decode_block_range(classic.pipeline, &classic.bytes, 10, 700).unwrap()
    );
    let predicate = StringPredicate::Equals("@user00042:example.org".to_owned());
    let mask = filter_string_block(packed.pipeline, &packed.bytes, &predicate)
        .unwrap()
        .expect("equality answered from the compressed bytes");
    assert_eq!(mask, predicate.filter_decoded(&values));
}

/// conformance: oe-encodings-and-compression/fsst-blocks-may-store-bitpacked-value-lengths/an-old-reader-refuses-the-file
#[test]
fn an_old_reader_refuses_the_file() {
    let built = support::built_file(48);
    let file = OeFile::open(built.bytes, None).unwrap();
    let declared = file.footer().required_feature_flags;
    let granule = file.footer().granules[0].granule_id;
    let mark = file.mark(column_ids::ENTITY_ID, 0, granule).unwrap().unwrap();
    let stores_packed = mark.codec_pipeline_id.transform().unwrap() == Transform::FsstPackedString;
    assert_eq!(declared & required_features::PACKED_STRING_OFFSETS != 0, stores_packed);
    // A reader whose known set predates bit 33 refuses any file that declares it.
    let old_known = required_features::KNOWN & !required_features::PACKED_STRING_OFFSETS;
    let unknown = required_features::PACKED_STRING_OFFSETS & !old_known;
    assert_ne!(unknown, 0);
    assert!(check_features(required_features::PACKED_STRING_OFFSETS, 0).is_ok());
}
