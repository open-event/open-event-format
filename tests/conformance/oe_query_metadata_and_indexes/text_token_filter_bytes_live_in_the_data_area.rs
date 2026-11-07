//! Checks that text-token filter bytes live in the data area — the footer records only their byte ranges, a cold open
//! never fetches them, the reader resolves a filter lazily with unchanged pruning behaviour, and inline filters from
//! files written before the relocation still resolve.

use crate::support;
use oe::columns::FreetextDeclaration;
use oe::layout::footer::{TextTokenEntry, encode_footer};
use oe::layout::optional_features;
use oe::layout::reader::OeFile;
use oe::writer::build::{BuiltOe, OeBuildConfig, OeRow, build_oe_file};

fn filtered_file(rows: u64) -> BuiltOe {
    let rows: Vec<OeRow> = (0..rows)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let config = OeBuildConfig {
        freetext: FreetextDeclaration {
            fields: vec!["note".to_owned()],
        },
        freetext_row_offset_index: false,
        ..support::build_config()
    };
    build_oe_file(rows, &config).unwrap()
}

fn note_column(built: &BuiltOe) -> u32 {
    built
        .footer
        .freetext
        .iter()
        .find(|entry| entry.declared_field == "note")
        .expect("the note field is declared free text")
        .column_id
}

/// conformance: oe-query-metadata-and-indexes/text-token-filter-bytes-live-in-the-data-area/cold-open-fetches-no-filter-bytes
#[test]
fn cold_open_fetches_no_filter_bytes() {
    let built = filtered_file(48);
    let note = note_column(&built);
    assert!(
        built.footer.text_token_indexes.is_empty(),
        "no filter bytes ride the footer"
    );
    assert!(
        built
            .footer
            .text_token_offsets
            .iter()
            .any(|entry| entry.column_id == note),
        "the footer records the relocated filters' byte ranges"
    );
    assert!(
        built.footer.optional_feature_flags & optional_features::TEXT_TOKEN_FILTER_OFFSETS != 0,
        "the relocation is declared as an optional feature"
    );
}

/// conformance: oe-query-metadata-and-indexes/text-token-filter-bytes-live-in-the-data-area/lazily-resolved-filter-prunes-identically
#[test]
fn lazily_resolved_filter_prunes_identically() {
    let built = filtered_file(48);
    let note = note_column(&built);
    let file = OeFile::open(built.bytes, None).unwrap();
    let granule = file.footer().granules[0].granule_id;
    let index = file
        .text_token_filter(note, granule, 0)
        .unwrap()
        .expect("the note column's filter resolves from the data area");
    // Every note in the file carries these tokens; a token stored nowhere is provably absent.
    assert!(index.might_contain_token("free"));
    assert!(index.might_contain_token("body"));
    assert!(!index.might_contain_token("nowhere-in-any-note"));
}

/// conformance: oe-query-metadata-and-indexes/text-token-filter-bytes-live-in-the-data-area/inline-filters-from-older-files-still-resolve
#[test]
fn inline_filters_from_older_files_still_resolve() {
    // Rebuild the file's footer the way a pre-relocation writer wrote it: the filter bytes inline in the footer, no
    // offsets section, no declared feature. The reader must resolve the inline filter with identical answers.
    let built = filtered_file(48);
    let note = note_column(&built);
    let file = OeFile::open(built.bytes.clone(), None).unwrap();
    let granule = file.footer().granules[0].granule_id;
    let relocated = file
        .text_token_filter(note, granule, 0)
        .unwrap()
        .expect("relocated filter resolves");

    let mut legacy_footer = built.footer.clone();
    legacy_footer.text_token_indexes = legacy_footer
        .text_token_offsets
        .iter()
        .map(|entry| {
            let resolved = file
                .text_token_filter(entry.column_id, entry.granule_id, entry.page_index)
                .unwrap()
                .expect("every recorded filter resolves");
            TextTokenEntry {
                column_id: entry.column_id,
                granule_id: entry.granule_id,
                index_bytes: resolved.encode(),
                page_index: entry.page_index,
            }
        })
        .collect();
    legacy_footer.text_token_offsets = Vec::new();
    legacy_footer.optional_feature_flags &= !optional_features::TEXT_TOKEN_FILTER_OFFSETS;

    // Reassemble the file with the legacy footer: same header and data area, inline-filter tail.
    let head_len = built.bytes.len() - built.footer_len as usize;
    let mut legacy_bytes = built.bytes[..head_len].to_vec();
    let region = encode_footer(&legacy_footer);
    legacy_bytes.extend_from_slice(&region);
    legacy_bytes.extend_from_slice(&(region.len() as u64).to_le_bytes());
    legacy_bytes.extend_from_slice(b"OEF1");

    let legacy_file = OeFile::open(legacy_bytes, None).unwrap();
    let inline = legacy_file
        .text_token_filter(note, granule, 0)
        .unwrap()
        .expect("the inline filter resolves from the footer");
    for probe in ["free", "body", "number", "nowhere-in-any-note", "billing"] {
        assert_eq!(
            inline.might_contain_token(probe),
            relocated.might_contain_token(probe),
            "probe {probe} must answer identically inline and relocated"
        );
    }
}
