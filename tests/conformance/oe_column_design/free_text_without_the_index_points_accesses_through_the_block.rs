use crate::support;
use oe::columns::FreetextDeclaration;
use oe::events::variant::VariantValue;
use oe::layout::optional_features;
use oe::layout::reader::OeFile;
use oe::writer::build::{BuiltOe, OeRow, build_oe_file};

fn body(i: u64) -> String {
    format!("note {i}: the account escalated after a threshold breach")
}

fn rows() -> Vec<OeRow> {
    (0..48)
        .map(|i| {
            let mut event = support::event(i);
            let oe::artifacts::batch::PayloadInput::Variant(VariantValue::Object(ref mut fields)) = event.payload
            else {
                unreachable!("the sample event carries a variant object payload");
            };
            fields.insert("note".to_owned().into(), VariantValue::String(body(i)));
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect()
}

fn build(freetext_row_offset_index: bool) -> BuiltOe {
    let mut config = support::build_config();
    config.freetext = FreetextDeclaration {
        fields: vec!["note".to_owned()],
    };
    config.freetext_row_offset_index = freetext_row_offset_index;
    build_oe_file(rows(), &config).unwrap()
}

#[test]
fn free_text_written_without_the_index_still_points_accesses() {
    let plain = build(false);

    assert_eq!(
        plain.footer.optional_feature_flags & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0,
        "a default build declares no per-row byte-offset index"
    );
    assert!(
        plain.footer.freetext_row_offsets.is_empty(),
        "a default build stores no per-row byte-offset index"
    );

    let file = OeFile::open(plain.bytes.clone(), None).unwrap();
    let column = plain
        .footer
        .freetext
        .iter()
        .find(|entry| entry.declared_field == "note")
        .expect("the writer gave the declared free-text field a column")
        .column_id;
    let granule = plain.footer.granules.first().expect("the file holds granules");
    let mark = file
        .mark(column, 0, granule.granule_id)
        .unwrap()
        .expect("the free-text column has a mark in every granule");
    assert!(
        mark.codec_pipeline_id.supports_byte_range_extraction().unwrap(),
        "the free-text block's encoding must be per-value addressable for the block to carry point access"
    );

    assert_eq!(
        file.decoded_cache_bytes(),
        0,
        "nothing is decoded before the first read"
    );
    let row = granule.first_row_ordinal + 1;
    assert_eq!(
        file.payload_path(row, "note").unwrap(),
        Some(VariantValue::String(body(row))),
        "row {row} read from the block's own per-value offsets"
    );
    assert_eq!(
        file.decoded_cache_bytes(),
        0,
        "a point read through the block's own offsets decodes no whole granule block"
    );

    let indexed = build(true);
    assert_ne!(
        indexed.footer.optional_feature_flags & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0,
        "the opted-in build carries the index this compares against"
    );
    let indexed_file = OeFile::open(indexed.bytes, None).unwrap();
    for ordinal in 0..48u64 {
        assert_eq!(
            file.payload_path(ordinal, "note").unwrap(),
            indexed_file.payload_path(ordinal, "note").unwrap(),
            "row {ordinal} must read identically with and without the index"
        );
    }
}
