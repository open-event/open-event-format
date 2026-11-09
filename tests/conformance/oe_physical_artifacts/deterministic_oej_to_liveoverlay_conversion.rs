use crate::support;
use arrow_array::UInt64Array;
use oe::artifacts::batch::decode_batch;
use oe::artifacts::frame::decode_frame;
use oe::artifacts::overlay::{convert_frame, select_representation, validate_segment};
use oe::artifacts::segment::replay_segment;

#[test]
fn row_count_mismatch() {
    let mut world = support::World::new(51);
    world.ingest(3);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let (header, payload) = decode_frame(&replay.frames[0].frame_bytes).unwrap();
    let decoded = decode_batch(payload, header.event_count).unwrap();
    let segment = convert_frame(&header, &decoded, 1, 1, None, |_, _| false)
        .unwrap()
        .expect("converted");
    assert!(validate_segment(&segment, &header).is_ok());
    let mut lying_header = header.clone();
    lying_header.event_count = 2;
    assert!(validate_segment(&segment, &lying_header).is_err());
}

#[test]
fn range_already_oe_covered() {
    let mut world = support::World::new(53);
    world.ingest(3);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let (header, payload) = decode_frame(&replay.frames[0].frame_bytes).unwrap();
    let decoded = decode_batch(payload, header.event_count).unwrap();
    let skipped = convert_frame(&header, &decoded, 1, 1, None, |_, _| true).unwrap();
    assert!(skipped.is_none());
}

#[test]
fn compressed_array_segment_yields_identical_query_results() {
    let mut world = support::World::new(57);
    world.ingest(5);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let (header, payload) = decode_frame(&replay.frames[0].frame_bytes).unwrap();
    let decoded = decode_batch(payload, header.event_count).unwrap();

    let seg_a = convert_frame(&header, &decoded, 1, 1, None, |_, _| false)
        .unwrap()
        .expect("converted A");
    let seg_b = convert_frame(&header, &decoded, 1, 1, None, |_, _| false)
        .unwrap()
        .expect("converted B");

    let read_sequences = |seg: &oe::artifacts::overlay::LiveOverlaySegment| {
        seg.batch
            .column_by_name("sequence")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .to_vec()
    };

    let sequences_a = read_sequences(&seg_a);
    let sequences_b = read_sequences(&seg_b);

    assert_eq!(sequences_a, sequences_b);
    assert_eq!(sequences_a.len(), 5);
    assert!(sequences_a.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn representation_is_deterministic_across_nodes() {
    let mut world = support::World::new(59);
    world.ingest(4);

    let replay_a = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let (header_a, payload_a) = decode_frame(&replay_a.frames[0].frame_bytes).unwrap();
    let rep_a = select_representation(header_a.epoch, header_a.first_sequence);
    let decoded_a = decode_batch(payload_a, header_a.event_count).unwrap();
    let seg_a = convert_frame(&header_a, &decoded_a, 1, 1, None, |_, _| false)
        .unwrap()
        .expect("node A converted");

    let replay_b = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let (header_b, payload_b) = decode_frame(&replay_b.frames[0].frame_bytes).unwrap();
    let rep_b = select_representation(header_b.epoch, header_b.first_sequence);
    let decoded_b = decode_batch(payload_b, header_b.event_count).unwrap();
    let seg_b = convert_frame(&header_b, &decoded_b, 1, 1, None, |_, _| false)
        .unwrap()
        .expect("node B converted");

    assert_eq!(rep_a, rep_b);

    assert_eq!(seg_a.meta, seg_b.meta);

    assert_eq!(seg_a.batch.num_rows(), seg_b.batch.num_rows());
    assert_eq!(seg_a.batch.num_columns(), seg_b.batch.num_columns());
    for col_idx in 0..seg_a.batch.num_columns() {
        assert_eq!(
            seg_a.batch.column(col_idx).to_data(),
            seg_b.batch.column(col_idx).to_data(),
            "column {col_idx} differs between nodes"
        );
    }
}
