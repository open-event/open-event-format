use crate::support;
use oe::columns::{PromotedColumn, PromotionPlan, column_ids};
use oe::layout::footer::ColumnKind;
use oe::layout::reader::{OeFile, PayloadRead};
use oe::writer::build::{OeRow, build_oe_file};

fn promoted_kind_plan(since: u32) -> PromotionPlan {
    PromotionPlan {
        columns: vec![PromotedColumn {
            name: "kind".to_owned(),
            path: "kind".to_owned(),
            kind: ColumnKind::String,
            since_schema_version: since,
            substring_searchable: false,
        }],
    }
}

#[test]
fn granule_predates_promotion() {
    let mut rows: Vec<OeRow> = (0..8)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    for row in rows.iter_mut().take(4) {
        row.event.envelope.schema_version = 1;
    }
    let mut config = support::build_config();
    config.promotion = promoted_kind_plan(2);
    let built = build_oe_file(rows, &config).unwrap();
    assert!(
        built
            .footer
            .presence
            .iter()
            .any(|entry| { entry.column_id == column_ids::PROMOTED_BASE && entry.since_schema_version == 2 })
    );
    let file = OeFile::open(built.bytes, None).unwrap();
    let granule = file.footer().granules[0].granule_id;
    let read = file.read_column(column_ids::PROMOTED_BASE, granule).unwrap();
    for index in 0..4usize {
        assert_eq!(read.presence[index / 8] & (1 << (index % 8)), 0);
        assert!(matches!(file.payload(index as u64).unwrap(), PayloadRead::Value(_)));
    }
    for index in 4..8usize {
        assert_ne!(read.presence[index / 8] & (1 << (index % 8)), 0);
    }
}

#[test]
fn cross_stream_sparsity_reads_as_null() {
    let mut rows: Vec<OeRow> = (0..6)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    use oe::artifacts::batch::PayloadInput;
    use oe::events::variant::VariantValue;
    for row in rows.iter_mut().take(3) {
        if let PayloadInput::Variant(VariantValue::Object(fields)) = &mut row.event.payload {
            fields.remove("kind");
        }
    }
    let mut config = support::build_config();
    config.promotion = promoted_kind_plan(1);
    let built = build_oe_file(rows, &config).unwrap();
    let file = OeFile::open(built.bytes, None).unwrap();
    let granule = file.footer().granules[0].granule_id;
    let read = file.read_column(column_ids::PROMOTED_BASE, granule).unwrap();
    for index in 0..3usize {
        assert_eq!(
            read.presence[index / 8] & (1 << (index % 8)),
            0,
            "genuine NULL for rows that never had the field"
        );
    }
    for index in 3..6usize {
        assert_ne!(read.presence[index / 8] & (1 << (index % 8)), 0);
    }
}
