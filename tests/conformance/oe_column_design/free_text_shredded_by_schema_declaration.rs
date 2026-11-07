//! Checks that a field the schema declares as free text gets stored in its own block of text, separate from the mixed
//! "everything else" payload. That way a job that only needs the text (for example, re-running extraction over it)
//! reads just those text blocks and nothing else.
use crate::support;
use oe::columns::{FreetextDeclaration, column_ids};
use oe::encoding::ColumnData;
use oe::layout::optional_features;
use oe::layout::reader::OeFile;

/// conformance: oe-column-design/free-text-shredded-by-schema-declaration/re-extraction-reads-only-free-text-blocks
#[test]
fn re_extraction_reads_only_free_text_blocks() {
    // The declared free-text field is shredded by declaration (not by stats) into its own columnar family: a bulk
    // single-field read range-reads only those blocks, never the residual variant arena.
    let mut config = support::build_config();
    config.freetext = FreetextDeclaration {
        fields: vec!["note".to_owned()],
    };
    let rows: Vec<oe::writer::build::OeRow> = (0..24)
        .map(|i| oe::writer::build::OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let built = oe::writer::build::build_oe_file(rows, &config).unwrap();
    assert_eq!(built.footer.freetext.len(), 1);
    assert_ne!(
        built.footer.optional_feature_flags & optional_features::FREETEXT_COLUMNS,
        0
    );
    let file = OeFile::open(built.bytes, None).unwrap();
    // The whole free-text corpus is reconstructable from the free-text column blocks alone (one constant-time mark read
    // per granule).
    let mut bodies = Vec::new();
    for granule in &file.footer().granules {
        let read = file.read_column(column_ids::FREETEXT_BASE, granule.granule_id).unwrap();
        let ColumnData::Strings(values) = read.data else {
            panic!("text family")
        };
        bodies.extend(values.iter_present().map(str::to_owned));
    }
    assert_eq!(bodies.len(), 24);
    assert!(bodies.iter().all(|body| body.starts_with("free text body")));
}
