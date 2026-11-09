use oe::encoding::deflate::{self, GRANULE_BYTES, granule_byte_range};
use oe::encoding::{
    ColumnData, Compression, PipelineId, Transform, decode_block, decode_block_range, encode_block, remove_trailing,
};

const CYCLE: [u64; 8] = [
    0x9E37_79B9_7F4A_7C15,
    0x2545_F491_4F6C_DD1D,
    0xBF58_476D_1CE4_E5B9,
    0x94D0_49BB_1331_11EB,
    0xD6E8_FEB8_6659_FD93,
    0xA5A5_5A5A_5A5A_5A5A,
    0x1234_5678_9ABC_DEF0,
    0x0F0E_0D0C_0B0A_0908,
];

fn repetitive_column(rows: usize) -> Vec<u64> {
    (0..rows).map(|i| CYCLE[i % CYCLE.len()]).collect()
}

fn incompressible_column(rows: usize) -> Vec<u64> {
    let mut state: u64 = 0x243F_6A88_85A3_08D3;
    (0..rows)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state
        })
        .collect()
}

fn deflate_page(values: &[u64]) -> (PipelineId, Vec<u8>) {
    let block = encode_block(&ColumnData::U64(values.to_vec()), true);
    let body = remove_trailing(block.pipeline.compression().unwrap(), &block.bytes).unwrap();
    let pipeline = PipelineId::new(
        block.pipeline.transform().unwrap(),
        Compression::Deflate,
        block.pipeline.value_kind().unwrap(),
    );
    (pipeline, deflate::compress(&body))
}

#[test]
fn deflate_chosen_only_by_the_sampler_never_by_config() {
    let compressible = encode_block(&ColumnData::U64(repetitive_column(4096)), true);
    assert_eq!(
        compressible.pipeline.compression().unwrap(),
        Compression::SeekableZstd,
        "a highly repetitive random-access column keeps the trailing family that wins the sampler's size comparison"
    );
    let body = remove_trailing(Compression::SeekableZstd, &compressible.bytes).unwrap();
    assert!(
        deflate::compress(&body).len() > compressible.bytes.len(),
        "deflate is left out only because it loses the size comparison on the same body"
    );

    let random = encode_block(&ColumnData::U64(incompressible_column(4096)), true);
    assert_eq!(
        random.pipeline.compression().unwrap(),
        Compression::None,
        "incompressible data must not be forced through deflate"
    );
}

#[test]
fn deflate_page_decodes_in_software_without_an_accelerator() {
    let values = repetitive_column(4096);
    let (pipeline, page) = deflate_page(&values);

    let decoded = decode_block(pipeline, &page).unwrap();
    assert_eq!(decoded, ColumnData::U64(values));
}

#[test]
fn single_granule_random_access_on_a_deflate_page() {
    let rows_per_granule = GRANULE_BYTES / 8;
    let values = repetitive_column(rows_per_granule * 4);
    let (pipeline, page) = deflate_page(&values);
    assert_eq!(pipeline.transform().unwrap(), Transform::PlainU64);

    let start = rows_per_granule * 2 + 3;
    let end = start + 5;
    let target_granule = (4 + start * 8) / GRANULE_BYTES;
    assert_eq!(
        (4 + (end - 1) * 8) / GRANULE_BYTES,
        target_granule,
        "the range must stay inside one granule"
    );

    let (body_start, _) = granule_byte_range(&page, 0).unwrap();
    let (granule_start, granule_len) = granule_byte_range(&page, target_granule).unwrap();
    let mut corrupted = page.clone();
    for (index, byte) in corrupted.iter_mut().enumerate().skip(body_start) {
        if index < granule_start || index >= granule_start + granule_len {
            *byte = 0xFF;
        }
    }

    let decoded = decode_block_range(pipeline, &corrupted, start, end).unwrap();
    assert_eq!(decoded, ColumnData::U64(values[start..end].to_vec()));
}
