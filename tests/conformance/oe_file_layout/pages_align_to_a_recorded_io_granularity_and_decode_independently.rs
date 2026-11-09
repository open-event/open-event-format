use crate::support;
use oe::columns::column_ids;
use oe::encoding::decode_block;
use oe::file::bytes::Reader;
use oe::layout::reader::OeFile;
use oe::layout::{decode_presence_frame, optional_features, required_features};
use oe::writer::build::{OeBuildConfig, OeRow, build_oe_file};

#[test]
fn aligned_fetch_of_surviving_pages() {
    const ALIGNMENT: u32 = 4096;

    let config = OeBuildConfig {
        io_alignment_bytes: ALIGNMENT,
        ..support::build_config()
    };
    let rows: Vec<OeRow> = (0..96)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    let built = build_oe_file(rows, &config).unwrap();

    assert_ne!(
        built.footer.optional_feature_flags & optional_features::PAGE_IO_ALIGNMENT,
        0,
        "PAGE_IO_ALIGNMENT must be declared in optional_feature_flags",
    );

    assert_eq!(built.footer.io_alignment_bytes, ALIGNMENT);

    for mark in &built.footer.marks {
        assert_eq!(
            mark.compressed_offset % u64::from(ALIGNMENT),
            0,
            "column {} granule {} offset {} is not aligned to {} bytes",
            mark.column_id,
            mark.granule_id,
            mark.compressed_offset,
            ALIGNMENT,
        );
    }

    let sequence_marks: Vec<_> = built
        .footer
        .marks
        .iter()
        .filter(|m| m.column_id == column_ids::SEQUENCE)
        .collect();
    assert!(
        sequence_marks.len() >= 3,
        "need at least 3 granules for non-adjacent pages"
    );

    let stride = (sequence_marks.len() - 1) / 2;
    let survivors = [
        sequence_marks[0],
        sequence_marks[stride],
        sequence_marks[sequence_marks.len() - 1],
    ];

    let compressed_presence = built.footer.required_feature_flags & required_features::COMPRESSED_PRESENCE != 0;

    let reader = OeFile::open(built.bytes.clone(), None).unwrap();

    for mark in survivors {
        let stripe_id = built
            .footer
            .granules
            .iter()
            .find(|g| g.granule_id == mark.granule_id)
            .expect("granule for mark")
            .stripe_id;
        let stripe_base = built
            .footer
            .stripes
            .iter()
            .find(|s| s.stripe_id == stripe_id)
            .expect("stripe for granule")
            .file_offset as usize;
        let start = stripe_base + mark.compressed_offset as usize;
        let end = start + mark.compressed_size as usize;
        let page_bytes = &built.bytes[start..end];

        let mut frame = Reader::new(page_bytes);
        decode_presence_frame(&mut frame, mark.row_count, compressed_presence).unwrap();
        let body = frame.take(frame.remaining(), "block body").unwrap();
        let isolated = decode_block(mark.codec_pipeline_id, body).unwrap();

        let full = reader.read_column(mark.column_id, mark.granule_id).unwrap();
        assert_eq!(
            isolated, full.data,
            "independent page decode must match full read for column {} granule {}",
            mark.column_id, mark.granule_id,
        );
    }
}
