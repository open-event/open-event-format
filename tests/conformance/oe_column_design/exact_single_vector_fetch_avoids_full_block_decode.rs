use super::{commit_trailing_gap, file_with_stored_vectors};
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::layout::footer::{EmbeddingRowOffsets, encode_footer};
use oe::layout::optional_features;
use oe::layout::reader::OeFile;
use oe::writer::build::BuiltOe;

fn with_embedding_row_index(built: &BuiltOe, row_bytes: &[Vec<u8>]) -> Vec<u8> {
    let granule = built.footer.granules[0];
    let stripe = built
        .footer
        .stripes
        .iter()
        .find(|stripe| stripe.stripe_id == granule.stripe_id)
        .expect("granule's stripe is in the stripe directory");

    let mut offsets_block = Vec::with_capacity(row_bytes.len() * 8);
    let mut bytes_block = Vec::new();
    for row in row_bytes {
        offsets_block.extend_from_slice(&(bytes_block.len() as u32).to_le_bytes());
        offsets_block.extend_from_slice(&(row.len() as u32).to_le_bytes());
        bytes_block.extend_from_slice(row);
    }

    let old_footer_len = u64::from_le_bytes(
        built.bytes[built.bytes.len() - 12..built.bytes.len() - 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let stripe_region_end = built.bytes.len() - (old_footer_len + 12);
    let extra_offset = stripe_region_end as u64;

    let mut footer = built.footer.clone();
    footer.optional_feature_flags |= optional_features::TYPED_COLUMN_ROW_OFFSETS;
    footer.embedding_row_offsets = vec![EmbeddingRowOffsets {
        bytes_len: bytes_block.len() as u64,
        bytes_offset: extra_offset + offsets_block.len() as u64 - stripe.file_offset,
        column_id: column_ids::EMBEDDING_BASE,
        granule_id: granule.granule_id,
        offsets_len: offsets_block.len() as u64,
        offsets_offset: extra_offset - stripe.file_offset,
    }];

    let mut spliced = built.bytes[..stripe_region_end].to_vec();
    spliced.extend_from_slice(&offsets_block);
    spliced.extend_from_slice(&bytes_block);
    commit_trailing_gap(&mut footer, &spliced);
    let new_footer_blob = encode_footer(&footer);
    spliced.extend_from_slice(&new_footer_blob);
    spliced.extend_from_slice(&(new_footer_blob.len() as u64).to_le_bytes());
    spliced.extend_from_slice(b"OEF1");
    spliced
}

#[test]
fn single_vector_fetched_by_row_ordinal_via_index() {
    let stored_vectors: Vec<String> = (0..6).map(|i| format!("stored-vector-{i}")).collect();
    let built = file_with_stored_vectors(&stored_vectors);
    let mirrored = with_embedding_row_index(
        &built,
        &stored_vectors
            .iter()
            .map(|v| v.clone().into_bytes())
            .collect::<Vec<_>>(),
    );
    let file = OeFile::open(mirrored, None).unwrap();
    assert_ne!(
        file.usable_optional_features() & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0,
        "the spliced file must carry the per-row offset index"
    );

    for (i, expected) in stored_vectors.iter().enumerate() {
        assert_eq!(
            file.embedding_value_for_row(column_ids::EMBEDDING_BASE, i as u64)
                .unwrap(),
            Some(expected.clone().into_bytes()),
            "row {i} exact fetch by ordinal"
        );
    }
}

#[test]
fn exact_single_vector_fetch_avoids_full_block_decode() {
    let stored_vectors: Vec<String> = (0..6).map(|i| format!("stored-vector-{i}")).collect();
    let built = file_with_stored_vectors(&stored_vectors);
    let granule_id = built.footer.granules[0].granule_id;

    let plain = OeFile::open(built.bytes.clone(), None).unwrap();
    let whole_block = plain.read_column(column_ids::EMBEDDING_BASE, granule_id).unwrap();
    let ColumnData::Strings(whole_block_strings) = &whole_block.data else {
        panic!("embedding column decodes as strings");
    };
    let whole_block_values: Vec<Vec<u8>> = whole_block_strings
        .iter()
        .map(|v| v.expect("every row carries a vector").as_bytes().to_vec())
        .collect();
    assert_eq!(
        whole_block_values,
        stored_vectors
            .iter()
            .map(|vector| vector.as_bytes().to_vec())
            .collect::<Vec<_>>(),
        "whole-block decode returns the vectors the writer stored"
    );

    let mirrored = with_embedding_row_index(&built, &whole_block_values);
    let file = OeFile::open(mirrored, None).unwrap();
    for (i, expected) in whole_block_values.iter().enumerate() {
        let via_index = file
            .embedding_value_for_row(column_ids::EMBEDDING_BASE, i as u64)
            .unwrap();
        assert_eq!(
            via_index.as_ref(),
            Some(expected),
            "row {i} exact fetch must match the whole-block decode"
        );
    }

    let after = file.read_column(column_ids::EMBEDDING_BASE, granule_id).unwrap();
    assert_eq!(
        after.data, whole_block.data,
        "the vector block an approximate ANN scan would read is unchanged by the index"
    );

    let mut divergent = whole_block_values.clone();
    divergent[2] = b"exact-fetch-only-row-2".to_vec();
    let spliced = with_embedding_row_index(&built, &divergent);
    let file = OeFile::open(spliced, None).unwrap();
    assert_eq!(
        file.embedding_value_for_row(column_ids::EMBEDDING_BASE, 2).unwrap(),
        Some(b"exact-fetch-only-row-2".to_vec()),
        "row 2 exact fetch must read the index's bytes, not decode the granule's whole vector block"
    );
}
