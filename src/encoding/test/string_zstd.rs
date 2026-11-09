use super::*;
use crate::encoding::decompressor::SoftwareDecompressor;
use crate::encoding::{
    CascadeStrategy, ColumnData, PipelineId, StringColumn, ValueKind, decode_block, decode_block_range,
    encode_block_forced_for_conformance, encode_block_replayed_with, encode_block_with_strategy, remove_trailing,
};
use crate::file::bytes::Writer;

/// Room-like identifiers: a skewed handful of values repeated across a granule's rows.
fn rooms(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|row| {
            let room = (row * 7919 % 97) * (row * 31 % 13) / 12;
            (row % 29 != 0).then(|| format!("!room{room:06}:example.org"))
        })
        .collect()
}

/// Free text: sentences over a small vocabulary, every row distinct.
/// Repeated room ids sharing a long common suffix: what LZ4's match finder shortens well and FSST's symbol table
/// does not, so the relaid LZ4 stage wins the selection.
fn lz4_friendly(rows: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|row| {
            (row % 11 != 0).then(|| {
                format!(
                    "!room{}:example.org joined by @someone:matrix.example.org",
                    row * 37 % 300
                )
            })
        })
        .collect()
}

fn sentences(rows: usize) -> Vec<Option<String>> {
    const WORDS: [&str; 12] = [
        "the", "of", "and", "message", "room", "is", "that", "it", "for", "was", "reply", "thread",
    ];
    (0..rows)
        .map(|row| {
            let words = 6 + row % 23;
            let text: Vec<&str> = (0..words)
                .map(|word| {
                    WORDS
                        .get((row * 13 + word * word * 7) % WORDS.len())
                        .copied()
                        .unwrap_or("x")
                })
                .collect();
            Some(format!("{} #{row}", text.join(" ")))
        })
        .collect()
}

fn column(values: &[Option<String>]) -> ColumnData {
    ColumnData::Strings(values.iter().map(|value| value.as_deref()).collect::<StringColumn>())
}

/// The classic body of `transform` over `data`: the raw arena as conformance forcing writes it, or the local
/// dictionary with plain entries the whole-block candidate relays.
fn classic(transform: Transform, data: &ColumnData) -> (Vec<u8>, SideStream) {
    if transform == Transform::DictionaryString {
        let ColumnData::Strings(values) = data else {
            unreachable!("string fixtures")
        };
        let present = crate::encoding::present_values(values);
        let mut out = Writer::new();
        let side = crate::encoding::encode_dictionary_string_with(
            values,
            &present,
            CascadeStrategy::DecodeOptimized,
            None,
            false,
            &mut out,
        )
        .unwrap();
        return (out.into_bytes(), side);
    }
    let block = encode_block_forced_for_conformance(data, false, CascadeStrategy::NoTrailing, transform).unwrap();
    (block.bytes, block.pipeline.side_stream().unwrap())
}

/// Selection as the writer runs it for a shredded payload column.
fn select(data: &ColumnData, strategy: CascadeStrategy) -> crate::encoding::EncodedBlock {
    encode_block_replayed_with(data, true, strategy, None, None, true).0
}

/// The classic path's whole-block decode and view decode of a stored block: the stage undone into the classic body,
/// then that body parsed. `None` wherever it refuses.
fn classic_decodes(pipeline: PipelineId, stored: &[u8]) -> (Option<ColumnData>, Option<Vec<Option<String>>>) {
    let mut body = Vec::new();
    if decompress_into(stored, &mut body, &SoftwareDecompressor).is_err() {
        return (None, None);
    }
    let column = crate::encoding::decode_block_shared_with_body(pipeline, &body, None).ok();
    let views = crate::encoding::decode_string_block_views_with_body(
        pipeline,
        pipeline.transform().unwrap(),
        &body,
        None,
        None,
    )
    .ok()
    .flatten()
    .map(|array| array.iter().map(|value| value.map(str::to_owned)).collect());
    (column, views)
}

/// The direct decodes of a stored block equal the classic path's, refusals included.
fn assert_direct_matches_classic(pipeline: PipelineId, stored: &[u8]) {
    let (column, views) = classic_decodes(pipeline, stored);
    assert_eq!(decode_block(pipeline, stored).ok(), column);
    let direct_views = crate::encoding::decode_string_block_views(pipeline, stored)
        .ok()
        .flatten()
        .map(|array| array.iter().map(|value| value.map(str::to_owned)).collect::<Vec<_>>());
    assert_eq!(direct_views, views);
}

/// Stores `relaid` as a block of `tag` claiming a `classic_len` classic body, without checking it restores.
fn store_relaid(tag: u8, relaid: &[u8], classic_len: u32) -> Vec<u8> {
    let mut out = classic_len.to_le_bytes().to_vec();
    out.push(tag);
    out.extend_from_slice(&crate::encoding::compress_zstd_framed(relaid, 3));
    out
}

#[test]
fn direct_decode_matches_the_classic_path_on_malformed_relaid_bodies() {
    let cases = [
        (
            Transform::RawString,
            column(&[Some("ab".into()), None, Some(String::new()), Some("é".into())]),
        ),
        (Transform::DictionaryString, column(&rooms(1500))),
    ];
    for (transform, data) in cases {
        let (classic_bytes, side) = classic(transform, &data);
        let (tag, relaid) = relay(transform, side, &classic_bytes).unwrap();
        let pipeline = PipelineId::new(transform, Compression::StringZstd, ValueKind::String).with_side_stream(side);
        let classic_len = classic_bytes.len() as u32;
        assert_direct_matches_classic(pipeline, &store_relaid(tag, &relaid, classic_len));
        // Every truncation, with the classic length kept or adjusted by the bytes cut.
        for cut in 0..relaid.len() {
            let head = relaid.get(..cut).unwrap();
            for claim in [classic_len, classic_len - (relaid.len() - cut) as u32] {
                assert_direct_matches_classic(pipeline, &store_relaid(tag, head, claim));
            }
        }
        // Every single-byte change: a zero, a stray byte, invalid UTF-8.
        for position in 0..relaid.len() {
            for byte in [0u8, b'x', 0xff] {
                let mut mutated = relaid.clone();
                *mutated.get_mut(position).unwrap() = byte;
                assert_direct_matches_classic(pipeline, &store_relaid(tag, &mutated, classic_len));
            }
        }
        // Trailing bytes, and the other tag.
        let mut longer = relaid.clone();
        longer.push(0);
        assert_direct_matches_classic(pipeline, &store_relaid(tag, &longer, classic_len + 1));
        assert_direct_matches_classic(pipeline, &store_relaid(tag ^ 1, &relaid, classic_len));
    }
}

fn round_trip_stage(transform: Transform, data: &ColumnData) {
    let (classic_bytes, side) = classic(transform, data);
    let stored = compress(transform, side, &classic_bytes).expect("a relayout for this body");
    let mut plain = Vec::new();
    decompress_into(&stored, &mut plain, &SoftwareDecompressor).unwrap();
    assert_eq!(plain, classic_bytes, "the stage restores the exact classic body");
    let pipeline = PipelineId::new(transform, Compression::StringZstd, ValueKind::String).with_side_stream(side);
    assert_eq!(
        remove_trailing(Compression::StringZstd, &stored).unwrap().as_ref(),
        classic_bytes
    );
    assert_eq!(&decode_block(pipeline, &stored).unwrap(), data);
    assert_direct_matches_classic(pipeline, &stored);
    assert!(
        with_relaid(transform, side, &stored, |_| Ok(())).unwrap().is_some(),
        "the direct path is taken"
    );
    let ColumnData::Strings(values) = data else {
        unreachable!("string fixtures")
    };
    let range = decode_block_range(pipeline, &stored, 5, 9).unwrap();
    assert_eq!(range, ColumnData::Strings(values.slice(5, 9)));
}

#[test]
fn raw_and_dictionary_bodies_round_trip_through_the_relaid_stage() {
    round_trip_stage(Transform::RawString, &column(&rooms(3000)));
    round_trip_stage(Transform::RawString, &column(&sentences(2000)));
    round_trip_stage(Transform::DictionaryString, &column(&rooms(5000)));
    // Empty values and an all-null block relay too.
    round_trip_stage(
        Transform::RawString,
        &column(&[Some(String::new()), None, Some("a".into())]),
    );
    round_trip_stage(Transform::RawString, &column(&[None, None]));
}

#[test]
fn a_value_holding_a_zero_byte_is_not_relaid() {
    let data = column(&[Some("a\0b".into()), Some("c".into())]);
    let classic =
        encode_block_forced_for_conformance(&data, false, CascadeStrategy::NoTrailing, Transform::RawString).unwrap();
    assert!(compress(Transform::RawString, SideStream::None, &classic.bytes).is_none());
    // Selection still stores such a block correctly, under whichever stage it picks.
    let values: Vec<Option<String>> = (0..4000).map(|row| Some(format!("v\0{}", row % 50))).collect();
    let data = column(&values);
    let block = select(&data, CascadeStrategy::DecodeOptimized);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
}

#[test]
fn selection_takes_the_relaid_stage_for_repeated_ids_and_free_text() {
    {
        let values = lz4_friendly(8192);
        let random_access = true;
        let data = column(&values);
        // The plain entry points never weigh the candidate; only the writer's payload-column selection does.
        let envelope = encode_block_with_strategy(&data, random_access, CascadeStrategy::DecodeOptimized);
        assert_ne!(envelope.pipeline.compression().unwrap(), Compression::StringZstd);
        let block = select(&data, CascadeStrategy::DecodeOptimized);
        assert_eq!(block.pipeline.compression().unwrap(), Compression::StringZstd);
        assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
        // The measurement control never stores a trailing stage.
        let control = select(&data, CascadeStrategy::NoTrailing);
        assert_eq!(control.pipeline.compression().unwrap(), Compression::None);
        assert!(block.bytes.len() < control.bytes.len());
    }
}

#[test]
fn malformed_stages_are_refused_not_panicked_on() {
    let data = column(&rooms(2000));
    let (classic_bytes, side) = classic(Transform::DictionaryString, &data);
    let stored = compress(Transform::DictionaryString, side, &classic_bytes).unwrap();
    let mut plain = Vec::new();
    // An unknown tag, a wrong classic length, a forged huge length, and every truncation are refused.
    let mut bad_tag = stored.clone();
    if let Some(tag) = bad_tag.get_mut(4) {
        *tag = 9;
    }
    assert!(decompress_into(&bad_tag, &mut plain, &SoftwareDecompressor).is_err());
    let mut bad_len = stored.clone();
    if let Some(byte) = bad_len.get_mut(0) {
        *byte ^= 1;
    }
    assert!(decompress_into(&bad_len, &mut plain, &SoftwareDecompressor).is_err());
    let mut huge = stored.clone();
    if let Some(bytes) = huge.get_mut(..4) {
        bytes.copy_from_slice(&u32::MAX.to_le_bytes());
    }
    assert!(decompress_into(&huge, &mut plain, &SoftwareDecompressor).is_err());
    for cut in 0..stored.len() {
        assert!(decompress_into(stored.get(..cut).unwrap_or_default(), &mut plain, &SoftwareDecompressor).is_err());
    }
}
