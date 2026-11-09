use crate::support;
use oe::encoding::{ColumnData, Compression, PipelineId, StringColumn, decode_block, decode_block_range, encode_block};
use oe::layout::reader::OeFile;

#[test]
fn shredded_cascade_uses_the_adaptive_encoder() {
    let statuses = ["lost", "open", "pending", "won"];
    let data = ColumnData::Strings(
        (0..600)
            .map(|i| Some(statuses[i % statuses.len()].to_owned()))
            .collect(),
    );
    let block = encode_block(&data, true);
    assert!(block.pipeline.transform().is_ok(), "an adaptive transform is recorded");
    assert_ne!(
        block.pipeline.0 & 0xFF,
        11,
        "the retired Vortex transform id is never written"
    );
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);

    let built = support::built_file(32);
    let shredded = built.footer.shredded.first().expect("a shredded column exists");
    let mark = built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == shredded.column_id)
        .expect("shredded column has a mark");
    assert!(mark.codec_pipeline_id.transform().is_ok());
    assert_ne!(mark.codec_pipeline_id.0 & 0xFF, 11);

    let file = OeFile::open(built.bytes, None).unwrap();
    let granule = file.footer().granules[0].granule_id;
    let read = file.read_column(shredded.column_id, granule).unwrap();
    assert!(
        read.data.row_count() > 0,
        "shredded rows read back through the normal path"
    );
}

#[test]
fn random_access_preserved_in_compressed_form() {
    let values: StringColumn = (0..2_000).map(|i| Some(["eu", "us", "apac"][i % 3])).collect();
    let data = ColumnData::Strings(values.clone());
    let block = encode_block(&data, true);
    assert_ne!(block.pipeline.compression().unwrap(), Compression::Lz4);
    assert_ne!(block.pipeline.compression().unwrap(), Compression::Zstd1);
    assert_ne!(block.pipeline.compression().unwrap(), Compression::Zstd3);
    let granule = decode_block_range(block.pipeline, &block.bytes, 700, 764).unwrap();
    assert_eq!(granule, ColumnData::Strings(values.slice(700, 764)));
}

#[test]
fn serialization_round_trips_under_conformance() {
    let datasets = [
        ColumnData::U64((0..3_000u64).map(|i| i * 37 + 5).collect()),
        ColumnData::I64((-750..750i64).map(|i| i * 13).collect()),
        ColumnData::F64((0..1_500).map(|i| i as f64 * 0.75 - 200.0).collect()),
        ColumnData::Decimal {
            values: (0..400i128).map(|i| i * 250 - 50_000).collect(),
            scale: 2,
        },
        ColumnData::U128((0..64u128).map(|i| i << 64 | i).collect()),
        ColumnData::Strings(
            (0..900)
                .map(|i| {
                    if i % 11 == 0 {
                        None
                    } else {
                        Some(format!("kind-{}", i % 6))
                    }
                })
                .collect(),
        ),
    ];
    for data in &datasets {
        let block = encode_block(data, true);
        assert_eq!(&decode_block(block.pipeline, &block.bytes).unwrap(), data);
    }
}

#[test]
fn shredded_bytes_are_deterministic() {
    let data = ColumnData::Strings((0..1_024).map(|i| Some(format!("event-{:05}", i % 40))).collect());
    let a = encode_block(&data, true);
    let b = encode_block(&data, true);
    assert_eq!(a.bytes, b.bytes);
    assert_eq!(a.pipeline, b.pipeline);
}

#[test]
fn retired_transform_id_is_rejected() {
    let pipeline = PipelineId(11);
    assert!(pipeline.transform().is_err());
    assert!(decode_block(pipeline, &[0u8; 32]).is_err());
    assert!(decode_block_range(pipeline, &[0u8; 32], 0, 8).is_err());
}
