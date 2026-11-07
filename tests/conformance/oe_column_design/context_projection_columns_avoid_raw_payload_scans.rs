//! Checks that the small set of "context" fields an investigation needs can be served from their own dedicated columns,
//! so building an evidence card never has to read back through the full raw event body.
use crate::support;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::events::families::{Caller, column_allowed};
use oe::layout::footer::ColumnKind;
use oe::layout::reader::OeFile;
use oe::writer::build::{AnalyticalColumn, OeRow, build_oe_file};

/// conformance: oe-column-design/context-projection-columns-avoid-raw-payload-scans/evidence-card-from-context-columns
#[test]
fn evidence_card_from_context_columns() {
    // Context projection columns (context_title, context_summary, etc.) carry data-class labels and are public-safe
    // only when the caller is authorized. The scan boundary allow-lists public output, so a bare public caller does not
    // receive them by default; the owner service that assembles the evidence card reads them below the API boundary,
    // where the data-class authorization is applied.
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

    // Build a file that includes a context_title column derived from the event envelope. An evidence card can be
    // assembled from this column alone, without touching the payload arena.
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

    // The context column is present in the file's column directory and is explicitly not internal-only, so it stays
    // eligible for authorized public output.
    let context_col = built
        .footer
        .columns
        .iter()
        .find(|c| c.name == "context_title")
        .expect("context_title column must be in the footer");
    assert!(!context_col.internal_only, "context columns are public-safe");
    assert_eq!(context_col.column_id, column_ids::CONTEXT_BASE);

    // The column can be read back without accessing the payload arena: the evidence card is assembled from the context
    // column block alone.
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
