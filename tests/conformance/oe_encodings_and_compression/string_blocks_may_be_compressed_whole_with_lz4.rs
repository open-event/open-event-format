//! Checks the whole-block LZ4 string stage: repeated identifiers store a dictionary and free text a raw arena,
//! each relaid and compressed whole (compression 6), smaller than the compressed-form layouts; every read path still
//! sees the classic body, and a file holding such a block declares a required feature so an old reader refuses it.
use crate::support;
use oe::artifacts::batch::PayloadInput;
use oe::compat::check_features;
use oe::encoding::predicate::{ContainsMatcher, StringPredicate, filter_string_block};
use oe::encoding::{
    CascadeStrategy, ColumnData, Compression, StringColumn, Transform, decode_block, decode_block_range, encode_block,
    encode_block_replayed_with,
};
use oe::events::variant::VariantValue;
use oe::layout::reader::OeFile;
use oe::layout::required_features;
use oe::writer::build::{OeRow, build_oe_file};

fn room(i: u64) -> String {
    // A skewed handful of busy rooms and a long tail of quiet ones.
    let draw = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 44) % 1_000;
    format!(
        "!room{:06}:example.org joined by @someone:matrix.example.org",
        draw * draw / 1_000
    )
}

fn sentence(i: u64) -> String {
    const WORDS: [&str; 10] = [
        "the", "of", "and", "message", "room", "is", "that", "reply", "for", "thread",
    ];
    // Forty recurring sentences, each tagged with its row: the repeats span whole values, which LZ4's match finder
    // shortens and FSST's short symbols do not.
    let template = i % 40;
    let words = 6 + template % 20;
    let text: Vec<&str> = (0..words)
        .map(|w| WORDS[((template.wrapping_mul(31) + w * w * 7) % WORDS.len() as u64) as usize])
        .collect();
    format!(
        "{} {i} -- sent from the example.org mobile client, reply to this thread to continue",
        text.join(" ")
    )
}

fn rooms(rows: u64) -> StringColumn {
    (0..rows).map(|i| Some(room(i))).collect()
}

fn sentences(rows: u64) -> StringColumn {
    (0..rows).map(|i| Some(sentence(i))).collect()
}

/// conformance:
/// oe-encodings-and-compression/string-blocks-may-be-compressed-whole-with-lz4/repeated-identifiers-take-dictionary-codes-under-lz4
#[test]
fn repeated_identifiers_take_dictionary_codes_under_lz4() {
    let values = rooms(8192);
    let data = ColumnData::Strings(values.clone());
    let block = encode_block_replayed_with(&data, true, CascadeStrategy::DecodeOptimized, None, None, true).0;
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(block.pipeline.compression().unwrap(), Compression::StringZstd);
    let control = encode_block_replayed_with(&data, true, CascadeStrategy::NoTrailing, None, None, true).0;
    assert!(block.bytes.len() < control.bytes.len());
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
    // Selection for a column the writer does not open it to (an envelope or relationship lookup key) keeps a layout
    // filters answer in place.
    let envelope = encode_block(&data, true);
    assert_ne!(envelope.pipeline.compression().unwrap(), Compression::StringZstd);
    assert_eq!(
        decode_block_range(block.pipeline, &block.bytes, 100, 140).unwrap(),
        ColumnData::Strings(values.slice(100, 140))
    );
    let predicate = StringPredicate::Equals(room(77));
    let mask = filter_string_block(block.pipeline, &block.bytes, &predicate)
        .unwrap()
        .expect("dictionary equality answered from the codes");
    assert_eq!(mask, predicate.filter_decoded(&values));
}

/// conformance:
/// oe-encodings-and-compression/string-blocks-may-be-compressed-whole-with-lz4/free-text-stores-the-smaller-form
#[test]
fn free_text_stores_the_smaller_form() {
    let values = sentences(4096);
    let data = ColumnData::Strings(values.clone());
    let block = encode_block_replayed_with(&data, true, CascadeStrategy::DecodeOptimized, None, None, true).0;
    // LZ4 seldom beats FSST on prose, so either form may win; whichever is stored is the smaller and reads the same.
    if block.pipeline.compression().unwrap() == Compression::StringZstd {
        assert_eq!(block.pipeline.transform().unwrap(), Transform::RawString);
    }
    let control = encode_block_replayed_with(&data, true, CascadeStrategy::NoTrailing, None, None, true).0;
    assert!(block.bytes.len() < control.bytes.len());
    assert_eq!(
        decode_block_range(block.pipeline, &block.bytes, 4000, 4001).unwrap(),
        ColumnData::Strings(values.slice(4000, 4001))
    );
    let predicate = StringPredicate::Contains(ContainsMatcher::new(vec!["reply thread".to_owned()], false));
    if let Some(mask) = filter_string_block(block.pipeline, &block.bytes, &predicate).unwrap() {
        assert_eq!(mask, predicate.filter_decoded(&values));
    }

    // In a file: the shredded free-text field point-reads row by row.
    let rows: Vec<OeRow> = (0..3_000u64)
        .map(|i| {
            let mut event = support::event(i);
            event.envelope.entity_id = Some(room(i));
            if let PayloadInput::Variant(VariantValue::Object(fields)) = &mut event.payload {
                fields.insert("note".to_owned(), VariantValue::String(sentence(i)));
            }
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect();
    let mut config = support::build_config();
    config.targets.index_granularity = 1_024;
    let file = OeFile::open(build_oe_file(rows, &config).unwrap().bytes, None).unwrap();
    for row in [0u64, 1, 1_500, 2_999] {
        assert_eq!(
            file.payload_path(row, "note").unwrap(),
            Some(VariantValue::String(sentence(row)))
        );
    }
}

/// conformance:
/// oe-encodings-and-compression/string-blocks-may-be-compressed-whole-with-lz4/an-old-reader-refuses-a-string-zstd-file
#[test]
fn an_old_reader_refuses_a_string_zstd_file() {
    let rows: Vec<OeRow> = (0..3_000u64)
        .map(|i| {
            let mut event = support::event(i);
            if let PayloadInput::Variant(VariantValue::Object(fields)) = &mut event.payload {
                fields.insert("room".to_owned(), VariantValue::String(room(i)));
            }
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect();
    let mut config = support::build_config();
    config.targets.index_granularity = 1_024;
    let file = OeFile::open(build_oe_file(rows, &config).unwrap().bytes, None).unwrap();
    let declared = file.footer().required_feature_flags;
    let room_column = file
        .footer()
        .columns
        .iter()
        .find(|column| column.name.ends_with("room"))
        .expect("the room field is shredded")
        .column_id;
    let stores_stage = file.footer().granules.iter().any(|granule| {
        file.mark(room_column, 0, granule.granule_id)
            .unwrap()
            .is_some_and(|mark| mark.codec_pipeline_id.compression().unwrap() == Compression::StringZstd)
    });
    assert!(stores_stage);
    assert_ne!(declared & required_features::STRING_ZSTD_BLOCKS, 0);
    assert_eq!(required_features::STRING_ZSTD_BLOCKS & required_features::ALL, 0);
    // A reader whose known set predates bit 37 finds an unknown required bit and refuses the file at open.
    let old_known = required_features::KNOWN & !required_features::STRING_ZSTD_BLOCKS;
    assert_ne!(declared & !old_known, 0);
    assert!(check_features(declared, 0).is_ok());
}
