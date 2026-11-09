use crate::support;
use oe::columns::{FreetextDeclaration, column_ids};
use oe::encoding::ColumnData;
use oe::layout::reader::OeFile;
use oe::writer::build::build_oe_file;

#[test]
fn migration_re_extraction_uses_bulk_egress() {
    let mut config = support::build_config();
    config.freetext = FreetextDeclaration {
        fields: vec!["note".to_owned()],
    };

    let rows: Vec<oe::writer::build::OeRow> = (0..32)
        .map(|i| oe::writer::build::OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let built = build_oe_file(rows, &config).unwrap();
    let file = OeFile::open(built.bytes, None).unwrap();

    let bulk = file.bulk_read_family(column_ids::FREETEXT_BASE).unwrap();

    assert!(!bulk.is_empty(), "bulk read must cover at least one granule");

    let mut total_values = 0usize;
    for (_granule_id, read) in &bulk {
        let ColumnData::Strings(values) = &read.data else {
            panic!("free-text family must decode to strings");
        };
        for value in values.iter().flatten() {
            assert!(
                value.starts_with("free text body"),
                "unexpected free-text value: {value}"
            );
            total_values += 1;
        }
    }

    assert_eq!(total_values, 32, "bulk read must cover all rows");

    let granule_ids: Vec<u32> = bulk.iter().map(|(id, _)| *id).collect();
    assert!(
        granule_ids.windows(2).all(|w| w[0] < w[1]),
        "bulk read must iterate granules in ascending order"
    );
}
