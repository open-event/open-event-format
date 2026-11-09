use crate::support;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::events::families::{Caller, column_allowed};
use oe::layout::footer::ColumnKind;
use oe::layout::reader::OeFile;
use oe::writer::build::{AnalyticalColumn, OeRow, build_oe_file};

#[test]
fn evidence_card_from_context_columns() {
    for col in [
        "context_title",
        "context_summary",
        "context_entity_label",
        "context_source_label",
        "context_metric_label",
        "context_status_label",
        "context_amount_display",
        "context_period_ref",
        "context_snippet_ref",
        "context_lineage_ref",
    ] {
        assert!(
            !column_allowed(col, Caller::Public),
            "{col} is public-safe only when authorized, so an unauthorized public caller must not receive it"
        );
        assert!(
            column_allowed(col, Caller::Internal),
            "{col} must be readable by the owner service that assembles the evidence card"
        );
    }

    let rows: Vec<OeRow> = (0..8)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let context_titles: Vec<Option<String>> = rows
        .iter()
        .map(|row| Some(format!("Event #{}", row.sequence)))
        .collect();
    let mut config = support::build_config();
    config.analytical_columns = vec![AnalyticalColumn {
        column_id: column_ids::CONTEXT_BASE,
        data: ColumnData::Strings(context_titles.into()),
        internal_only: false,
        kind: ColumnKind::String,
        name: "context_title".to_owned(),
        substring_searchable: false,
    }];
    let built = build_oe_file(rows, &config).unwrap();

    let context_col = built
        .footer
        .columns
        .iter()
        .find(|c| c.name == "context_title")
        .expect("context_title column must be in the footer");
    assert!(!context_col.internal_only, "context columns are public-safe");
    assert_eq!(context_col.column_id, column_ids::CONTEXT_BASE);

    let file = OeFile::open(built.bytes, None).unwrap();
    let granule_id = file.footer().granules[0].granule_id;
    let read = file.read_column(column_ids::CONTEXT_BASE, granule_id).unwrap();
    let ColumnData::Strings(values) = read.data else {
        panic!("context_title must be a String column");
    };
    assert_eq!(values.len(), 8);
    assert_eq!(values.get(0), Some(Some("Event #1")));
    assert_eq!(values.get(7), Some(Some("Event #8")));
}
