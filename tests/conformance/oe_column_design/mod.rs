use crate::support;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::file::integrity::hash_tree;
use oe::layout::footer::{ColumnKind, Footer, IntegrityGapEntry};
use oe::writer::build::{AnalyticalColumn, BuiltOe, OeRow, build_oe_file};

mod context_projection_columns_avoid_raw_payload_scans;
mod exact_single_vector_fetch_avoids_full_block_decode;
mod free_text_point_lookup_uses_row_offset_index;
mod free_text_shredded_by_schema_declaration;
mod free_text_without_the_index_points_accesses_through_the_block;
mod internal_embedding_vector_columns_isolated_from_public_output;
mod payload_arena_stores_canonical_variant_values_with_statistics_driven_shredding;
mod promotion_backfill_via_manifest_native_vertical_projection;
mod reader_without_index_falls_back_identically;
mod required_physical_columns;
mod schema_version_keyed_presence_map_for_promoted_columns;
mod strongly_typed_promoted_columns;

pub fn file_with_stored_vectors(vectors: &[String]) -> BuiltOe {
    let rows: Vec<OeRow> = (0..vectors.len() as u64)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let mut config = support::build_config();
    config.analytical_columns = vec![AnalyticalColumn {
        column_id: column_ids::EMBEDDING_BASE,
        data: ColumnData::Strings(vectors.iter().cloned().map(Some).collect()),
        internal_only: true,
        kind: ColumnKind::String,
        name: "embedding_vec".to_owned(),
        substring_searchable: false,
    }];
    let built = build_oe_file(rows, &config).unwrap();
    assert_eq!(built.footer.granules.len(), 1, "test needs every row in one granule");
    built
}

pub fn commit_trailing_gap(footer: &mut Footer, bytes_before_footer: &[u8]) {
    let tail_start = footer
        .stripes
        .iter()
        .map(|stripe| stripe.file_offset + stripe.byte_len)
        .max()
        .expect("a built file has stripes");
    footer.integrity_gaps.retain(|gap| gap.file_offset < tail_start);
    footer.integrity_gaps.push(IntegrityGapEntry {
        blake3: *hash_tree(&bytes_before_footer[tail_start as usize..]).as_bytes(),
        byte_len: bytes_before_footer.len() as u64 - tail_start,
        file_offset: tail_start,
    });
}
