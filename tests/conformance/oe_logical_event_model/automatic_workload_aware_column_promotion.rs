use crate::support;
use oe::columns::{PathStatistics, PromotedColumn, PromotionPlan, column_ids};
use oe::events::variant::VariantValue;
use oe::layout::footer::ColumnKind;
use oe::writer::build::{OeBuildConfig, OeRow, build_oe_file};
use std::collections::BTreeMap;

#[test]
fn frequently_filtered_field_is_promoted() {
    let mut stats = PathStatistics::new();
    let mut payload = BTreeMap::new();
    payload.insert("score".to_owned(), VariantValue::Int(42));
    let value = VariantValue::Object(payload);
    for _ in 0..200 {
        stats.observe(Some(&value));
    }
    let candidates = stats.shred_candidates(&[]);
    assert!(
        candidates.iter().any(|(path, _)| path == "score"),
        "field observed in every row must be selected as a promotion candidate"
    );

    let rows: Vec<OeRow> = (0..8)
        .map(|i| OeRow {
            epoch: 1,
            event: support::event(i),
            sequence: i + 1,
        })
        .collect();
    let config = OeBuildConfig {
        promotion: PromotionPlan {
            columns: vec![PromotedColumn {
                kind: ColumnKind::String,
                name: "kind_promoted".to_owned(),
                path: "kind".to_owned(),
                since_schema_version: 2,
                substring_searchable: false,
            }],
        },
        ..support::build_config()
    };
    let built = build_oe_file(rows, &config).unwrap();
    let promoted_cols: Vec<_> = built
        .footer
        .columns
        .iter()
        .filter(|c| c.column_id >= column_ids::PROMOTED_BASE && c.column_id < column_ids::SHREDDED_BASE)
        .collect();
    assert!(
        !promoted_cols.is_empty(),
        "write path must materialize the workload-driven field into a typed column in the footer"
    );
    assert!(
        promoted_cols.iter().any(|c| c.name == "kind_promoted"),
        "the promoted column must appear under its declared name"
    );
    assert!(
        !built.footer.presence.is_empty(),
        "the footer presence map must carry the schema-version-keyed entry for the promoted column"
    );
}
