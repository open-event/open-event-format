use crate::support;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::events::families::{Caller, ColumnFamily, authorize_columns, column_allowed};
use oe::layout::footer::ColumnKind;
use oe::layout::reader::OeFile;
use oe::writer::build::{AnalyticalColumn, OeRow, build_oe_file};

fn file_with_embedding_column() -> oe::writer::build::BuiltOe {
    let rows: Vec<OeRow> = (0..8)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let mut config = support::build_config();
    config.analytical_columns = vec![AnalyticalColumn {
        column_id: column_ids::EMBEDDING_BASE,
        data: ColumnData::F64((0..8).map(|i| i as f64 * 0.1).collect()),
        internal_only: true,
        kind: ColumnKind::F64,
        name: "embedding_text".to_owned(),
        substring_searchable: false,
    }];
    build_oe_file(rows, &config).unwrap()
}

#[test]
fn export_excludes_embeddings() {
    assert!(ColumnFamily::EmbeddingColumnsInternal.internal_only());

    for col in [
        "embedding_text",
        "embedding_semantic",
        "embedding_quantized",
        "embedding_v1",
    ] {
        assert!(
            !column_allowed(col, Caller::Public),
            "{col} must be blocked for public callers"
        );
        assert!(
            column_allowed(col, Caller::Internal),
            "{col} must remain readable by internal callers"
        );
    }

    let (allowed, dropped) = authorize_columns(
        &["occurred_at", "event_type_id", "embedding_text", "embedding_semantic"],
        Caller::Public,
    );
    assert_eq!(allowed, vec!["occurred_at", "event_type_id"]);
    assert_eq!(dropped, vec!["embedding_text", "embedding_semantic"]);

    let built = file_with_embedding_column();
    let emb_col = built
        .footer
        .columns
        .iter()
        .find(|c| c.name == "embedding_text")
        .expect("embedding_text must be in the footer");
    assert!(
        emb_col.internal_only,
        "embedding columns must be internal_only in the column directory"
    );

    let file = OeFile::open(built.bytes, None).unwrap();
    let granule_id = file.footer().granules[0].granule_id;
    let read = file.read_column(column_ids::EMBEDDING_BASE, granule_id).unwrap();
    let ColumnData::F64(values) = read.data else {
        panic!("embedding_text must be an F64 column");
    };
    assert_eq!(values.len(), 8);

    let col_names: Vec<&str> = built.footer.columns.iter().map(|c| c.name.as_str()).collect();
    let embedding_names: Vec<&str> = col_names
        .iter()
        .copied()
        .filter(|n| n.starts_with("embedding_"))
        .collect();
    let (public_allowed, public_dropped) = authorize_columns(&col_names, Caller::Public);
    assert!(
        public_dropped.iter().any(|n| n.starts_with("embedding_")),
        "authorize_columns must drop embedding columns from public projections"
    );
    assert!(
        !public_allowed.iter().any(|n| n.starts_with("embedding_")),
        "no embedding column may survive the public scan boundary"
    );
    drop(embedding_names);
}

#[test]
fn crypto_shredding_destroys_subject_vectors() {
    assert!(ColumnFamily::EmbeddingColumnsInternal.internal_only());

    assert!(!column_allowed("embedding_v1", Caller::Public));
    assert!(column_allowed("embedding_v1", Caller::Internal));

    let built = file_with_embedding_column();
    let emb_col = built
        .footer
        .columns
        .iter()
        .find(|c| c.name == "embedding_text")
        .expect("embedding_text must be in the footer");
    assert!(
        emb_col.internal_only,
        "embedding column must be internal_only so the scan boundary enforces key-destruction semantics"
    );
    assert_eq!(emb_col.column_id, column_ids::EMBEDDING_BASE);
}
