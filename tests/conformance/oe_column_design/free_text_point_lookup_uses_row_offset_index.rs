use crate::support;
use oe::columns::FreetextDeclaration;
use oe::events::variant::VariantValue;
use oe::layout::optional_features;
use oe::layout::reader::{OeFile, PayloadRead};

#[test]
fn free_text_point_lookup_skips_the_whole_granule_decode() {
    let mut config = support::build_config();
    config.freetext = FreetextDeclaration {
        fields: vec!["note".to_owned()],
    };
    config.freetext_row_offset_index = true;
    let rows: Vec<oe::writer::build::OeRow> = (0..24)
        .map(|i| oe::writer::build::OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let built = oe::writer::build::build_oe_file(rows, &config).unwrap();

    assert_ne!(
        built.footer.optional_feature_flags & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0,
        "a build that opted into the per-row offset index must carry it"
    );
    assert!(!built.footer.freetext_row_offsets.is_empty());

    let file = OeFile::open(built.bytes, None).unwrap();

    for i in 0..24u64 {
        let PayloadRead::Value(VariantValue::Object(fields)) = file.payload(i).unwrap() else {
            panic!("payload present for row {i}");
        };
        assert_eq!(
            fields.get("note"),
            Some(&VariantValue::String(format!("free text body number {i}"))),
            "row {i} free-text point lookup"
        );
    }
}
