use crate::support;
use oe::columns::column_ids;
use oe::compat::check_features;
use oe::encoding::{ColumnData, Transform, encode_block};
use oe::events::TimestampValue;
use oe::invariants::sim::SerialEncodeExecutor;
use oe::layout::reader::OeFile;
use oe::layout::required_features;
use oe::writer::build::{OeRow, build_oe_file, update_oe_file_with_executor};

fn jitter(i: u64) -> i64 {
    (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as i64
}

fn rows(count: u64, lag: impl Fn(u64) -> i64) -> Vec<OeRow> {
    (0..count)
        .map(|i| {
            let mut event = support::event(i);
            let occurred = 1_700_000_000_000_000_000 + i as i64 * 1_000_000 + jitter(i);
            event.envelope.occurred_at = TimestampValue::from_physical_nanos(occurred);
            event.envelope.ingested_at = TimestampValue::from_physical_nanos(occurred + lag(i));
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect()
}

fn ingested(file: &OeFile, granule: u32) -> Vec<i64> {
    match file.read_column(column_ids::INGESTED_AT, granule).unwrap().data {
        ColumnData::I64(values) => values,
        other => panic!("ingested_at decodes to i64, got {other:?}"),
    }
}

#[test]
fn ingested_at_equal_to_occurred_at_costs_almost_nothing() {
    for lag in [0i64, 3] {
        let input = rows(3_000, |i| if lag == 0 { 0 } else { (i % 4) as i64 * lag });
        let expected: Vec<i64> = input
            .iter()
            .map(|row| row.event.envelope.ingested_at.physical_nanos())
            .collect();
        let built = build_oe_file(input, &support::build_config()).unwrap();
        let file = OeFile::open(built.bytes, None).unwrap();
        assert_ne!(
            file.footer().required_feature_flags & required_features::OCCURRED_RELATIVE_INGESTED_AT,
            0
        );
        let mut read = Vec::new();
        for granule in file.footer().granules.clone() {
            let mark = file
                .mark(column_ids::INGESTED_AT, 0, granule.granule_id)
                .unwrap()
                .unwrap();
            assert_eq!(mark.codec_pipeline_id.transform().unwrap(), Transform::OccurredResidual);
            let values = ingested(&file, granule.granule_id);
            let direct = encode_block(&ColumnData::I64(values.clone()), false);
            assert!(mark.compressed_size < direct.bytes.len() as u64);
            assert_eq!(granule.min_ingested_at_physical, *values.iter().min().unwrap());
            assert_eq!(granule.max_ingested_at_physical, *values.iter().max().unwrap());
            let raw = file
                .read_column_raw(column_ids::INGESTED_AT, granule.granule_id)
                .unwrap();
            assert_eq!(raw.pipeline.transform().unwrap(), Transform::PlainU64);
            read.extend(values);
        }
        assert_eq!(read, expected);
    }
}

#[test]
fn a_rewritten_occurred_at_re_encodes_ingested_at() {
    let config = support::build_config();
    let before = rows(3_000, |_| 0);
    let source = build_oe_file(before.clone(), &config).unwrap();
    let mut after = before.clone();
    let shifted = &mut after[17].event.envelope;
    shifted.occurred_at = TimestampValue::from_physical_nanos(shifted.occurred_at.physical_nanos() - 5_000);
    let expected: Vec<i64> = after
        .iter()
        .map(|row| row.event.envelope.ingested_at.physical_nanos())
        .collect();
    let updated = update_oe_file_with_executor(&source, &before, after, &config, &SerialEncodeExecutor).unwrap();
    let file = OeFile::open(updated.bytes, None).unwrap();
    let read: Vec<i64> = file
        .footer()
        .granules
        .clone()
        .iter()
        .flat_map(|granule| ingested(&file, granule.granule_id))
        .collect();
    assert_eq!(read, expected);
}

#[test]
fn an_old_reader_refuses_an_occurred_relative_file() {
    let declared = OeFile::open(
        build_oe_file(rows(1_000, |_| 0), &support::build_config())
            .unwrap()
            .bytes,
        None,
    )
    .unwrap()
    .footer()
    .required_feature_flags;
    assert_ne!(declared & required_features::OCCURRED_RELATIVE_INGESTED_AT, 0);
    assert_eq!(
        required_features::OCCURRED_RELATIVE_INGESTED_AT & required_features::ALL,
        0
    );
    let old_known = required_features::KNOWN & !required_features::OCCURRED_RELATIVE_INGESTED_AT;
    assert_ne!(declared & !old_known, 0);
    assert!(check_features(declared, 0).is_ok());
}
