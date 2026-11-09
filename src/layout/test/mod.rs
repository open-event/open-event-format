use super::*;
use crate::events::TenantId;
use crate::file::bytes::Writer;
use crate::typed_id::TypedIdTestExt;

fn spanning_header(min_epoch: u64, max_epoch: u64, min_sequence: u64, max_sequence: u64) -> OeHeader {
    OeHeader {
        created_at_physical: 0,
        feature_flags: 0,
        file_id: 0,
        footer_pointer_hint: 0,
        generation_id: 0,
        layout_class: LayoutClass::Compact,
        max_epoch,
        max_ingested_at_physical: 0,
        max_occurred_at_physical: 0,
        max_sequence,
        min_epoch,
        min_ingested_at_physical: 0,
        min_occurred_at_physical: 0,
        min_sequence,
        profile: crate::layout::Profile::Event,
        projection_count: 1,
        row_count: 1,
        tenant_id: TenantId::new_test_id(1),
        version_major: 1,
        version_minor: 0,
    }
}

#[test]
fn may_contain_sequence_is_epoch_aware_for_a_file_spanning_epochs() {
    let header = spanning_header(3, 5, 100, 50);

    assert!(header.may_contain_sequence(4, 1, 1));
    assert!(header.may_contain_sequence(4, 100, 100));
    assert!(header.may_contain_sequence(4, 999, 1000));

    assert!(header.may_contain_sequence(3, 200, 300));
    assert!(header.may_contain_sequence(3, 90, 100));
    assert!(
        !header.may_contain_sequence(3, 1, 50),
        "below the file's first-epoch start is rejected"
    );

    assert!(header.may_contain_sequence(5, 10, 20));
    assert!(header.may_contain_sequence(5, 50, 80));
    assert!(
        !header.may_contain_sequence(5, 60, 70),
        "above the file's last-epoch end is rejected"
    );

    assert!(!header.may_contain_sequence(2, 1, u64::MAX));
    assert!(!header.may_contain_sequence(6, 1, u64::MAX));
}

#[test]
fn may_contain_sequence_within_a_single_epoch_is_the_flat_interval() {
    let header = spanning_header(7, 7, 100, 200);
    assert!(header.may_contain_sequence(7, 150, 160));
    assert!(header.may_contain_sequence(7, 50, 100));
    assert!(header.may_contain_sequence(7, 200, 250));
    assert!(!header.may_contain_sequence(7, 1, 99), "below the interval is rejected");
    assert!(
        !header.may_contain_sequence(7, 201, 300),
        "above the interval is rejected"
    );
    assert!(!header.may_contain_sequence(8, 150, 160), "wrong epoch is rejected");
}

#[test]
fn presence_side_stream_forms_round_trip_and_zero_byte_forms_store_nothing() {
    use crate::file::bytes::{Reader, Writer};

    let round_trip = |presence: &[u8], rows: u32| {
        let mut out = Writer::new();
        super::encode_presence(presence, rows, &mut out);
        let bytes = out.into_bytes();
        let decoded = super::decode_presence(&mut Reader::new(&bytes), rows)
            .unwrap()
            .into_owned();
        (bytes, decoded)
    };

    let (bytes, decoded) = round_trip(&[], 100);
    assert_eq!(bytes, vec![super::presence_forms::EMPTY]);
    assert!(decoded.is_empty());

    let all_set = vec![0xFF, 0x03];
    let (bytes, decoded) = round_trip(&all_set, 10);
    assert_eq!(bytes, vec![super::presence_forms::ALL_PRESENT]);
    assert_eq!(decoded, all_set);

    let (bytes, decoded) = round_trip(&[0u8; 2], 10);
    assert_eq!(bytes, vec![super::presence_forms::ALL_ABSENT]);
    assert_eq!(decoded, vec![0u8; 2]);

    let rows = 4096u32;
    let mut clustered = vec![0u8; 512];
    for row in 100..140 {
        clustered[row / 8] |= 1 << (row % 8);
    }
    let (bytes, decoded) = round_trip(&clustered, rows);
    assert_eq!(bytes[0], super::presence_forms::SET_RUNS);
    assert_eq!(bytes.len(), 1 + 4 + 8, "one run costs one (start, len) pair");
    assert_eq!(decoded, clustered);

    let mut scattered = vec![0u8; 512];
    for row in [3usize, 900, 2001, 3777] {
        scattered[row / 8] |= 1 << (row % 8);
    }
    let (bytes, decoded) = round_trip(&scattered, rows);
    assert_eq!(bytes[0], super::presence_forms::SET_POSITIONS);
    assert_eq!(decoded, scattered);

    let noisy: Vec<u8> = (0..512).map(|i| (i as u8).wrapping_mul(37) | 1).collect();
    let (bytes, decoded) = round_trip(&noisy, rows);
    assert_eq!(bytes[0], super::presence_forms::RAW);
    assert_eq!(decoded, noisy);

    assert!(super::decode_presence(&mut Reader::new(&[9u8]), 10).is_err());
}

#[test]
fn presence_stored_as_a_bitmap_is_borrowed_from_the_block_bytes() {
    use crate::file::bytes::{Reader, Writer};
    use std::borrow::Cow;

    let noisy: Vec<u8> = (0..512).map(|i| (i as u8).wrapping_mul(37) | 1).collect();
    let mut out = Writer::new();
    super::encode_presence(&noisy, 4096, &mut out);
    let bytes = out.into_bytes();
    assert_eq!(bytes[0], super::presence_forms::RAW);
    let decoded = super::decode_presence_frame(&mut Reader::new(&bytes), 4096, true).unwrap();
    assert!(matches!(decoded, Cow::Borrowed(_)), "the raw form must not be copied");
    assert_eq!(decoded, noisy);

    let mut legacy = Writer::new();
    legacy.put_u32(noisy.len() as u32);
    legacy.put_slice(&noisy);
    let legacy_bytes = legacy.into_bytes();
    let decoded = super::decode_presence_frame(&mut Reader::new(&legacy_bytes), 4096, false).unwrap();
    assert!(
        matches!(decoded, Cow::Borrowed(_)),
        "the legacy frame must not be copied"
    );
    assert_eq!(decoded, noisy);

    let mut runs = Writer::new();
    let mut clustered = vec![0u8; 512];
    for row in 100..140 {
        clustered[row / 8] |= 1 << (row % 8);
    }
    super::encode_presence(&clustered, 4096, &mut runs);
    let runs_bytes = runs.into_bytes();
    assert_eq!(runs_bytes[0], super::presence_forms::SET_RUNS);
    let decoded = super::decode_presence_frame(&mut Reader::new(&runs_bytes), 4096, true).unwrap();
    assert!(matches!(decoded, Cow::Owned(_)));
    assert_eq!(decoded, clustered);
}

#[test]
fn presence_frame_decoding_follows_the_declared_feature() {
    use crate::file::bytes::{Reader, Writer};

    let bitmap = vec![0b0000_0101u8, 0b0000_0010];
    let mut legacy = Writer::new();
    legacy.put_u32(bitmap.len() as u32);
    legacy.put_slice(&bitmap);
    let legacy_bytes = legacy.into_bytes();

    let decoded = super::decode_presence_frame(&mut Reader::new(&legacy_bytes), 10, false).unwrap();
    assert_eq!(decoded, bitmap, "a legacy frame must decode to its exact bitmap");

    let misread = super::decode_presence_frame(&mut Reader::new(&legacy_bytes), 10, true).unwrap();
    assert_ne!(misread, bitmap);

    let mut compressed = Writer::new();
    super::encode_presence(&bitmap, 10, &mut compressed);
    let compressed_bytes = compressed.into_bytes();
    let decoded = super::decode_presence_frame(&mut Reader::new(&compressed_bytes), 10, true).unwrap();
    assert_eq!(decoded, bitmap);
}

const PROFILES: [Profile; 7] = [
    Profile::Audit,
    Profile::Event,
    Profile::Graph,
    Profile::Inbox,
    Profile::Log,
    Profile::Snapshot,
    Profile::TimeSeries,
];

fn header_len_word(block: &[u8]) -> usize {
    u32::from_le_bytes(block[8..12].try_into().unwrap()) as usize
}

#[test]
fn every_profile_round_trips_through_the_header() {
    for profile in PROFILES {
        let mut header = spanning_header(1, 1, 1, 10);
        header.profile = profile;
        assert_eq!(decode_header(&encode_header(&header)).unwrap(), header);
        assert_eq!(Profile::from_u8(profile.code()).unwrap(), profile);
    }
}

#[test]
fn profile_codes_are_explicit_and_distinct() {
    let codes: Vec<u8> = PROFILES.iter().map(|profile| profile.code()).collect();
    assert_eq!(codes, vec![3, 0, 1, 5, 4, 2, 6]);
}

#[test]
fn an_event_header_keeps_the_original_layout_and_other_profiles_do_not() {
    let event = encode_header(&spanning_header(1, 1, 1, 10));
    assert_eq!(header_len_word(&event), HEADER_FIXED_LEN);
    let mut graph = spanning_header(1, 1, 1, 10);
    graph.profile = Profile::Graph;
    assert_eq!(header_len_word(&encode_header(&graph)), PROFILED_HEADER_FIXED_LEN);
}

#[test]
fn an_unknown_header_profile_code_is_a_structural_error() {
    let mut header = spanning_header(1, 1, 1, 10);
    header.profile = Profile::Graph;
    let mut block = encode_header(&header);
    let profile_at = PROFILED_HEADER_FIXED_LEN - 8 - 32 - 1;
    assert_eq!(block[profile_at], Profile::Graph.code());
    block[profile_at] = 0xEE;
    let crc = crate::file::integrity::crc64_nvme(&block[..profile_at + 1]);
    block[profile_at + 1..profile_at + 9].copy_from_slice(&crc.to_le_bytes());
    let digest = blake3::hash(&block[..profile_at + 9]);
    block[profile_at + 9..profile_at + 41].copy_from_slice(digest.as_bytes());
    assert_eq!(
        decode_header(&block).unwrap_err(),
        FormatError::Structural {
            rule: "unknown profile"
        }
    );
    assert!(Profile::from_u8(7).is_err());
}

#[test]
fn graph_bits_are_known_but_not_declared_by_every_file() {
    assert_eq!(required_features::GRAPH & required_features::ALL, 0);
    assert_eq!(required_features::GRAPH & !required_features::KNOWN, 0);
    assert_eq!(required_features::GRAPH, ((1 << 29) - 1) & !((1 << 23) - 1));
}

#[test]
fn the_xor_float_bit_is_known_but_declared_only_by_files_that_use_it() {
    assert_eq!(required_features::XOR_FLOAT_BLOCKS, 1 << 30);
    assert_eq!(required_features::XOR_FLOAT_BLOCKS & required_features::ALL, 0);
    assert_eq!(required_features::XOR_FLOAT_BLOCKS & !required_features::KNOWN, 0);
    assert_eq!(optional_features::TIME_SERIES_ROLLUPS, 1 << 13);
    assert_eq!(optional_features::TIME_SERIES_ROLLUPS & !optional_features::KNOWN, 0);
}

#[test]
fn presence_run_and_position_forms_encode_exact_bytes() {
    let encode = |presence: &[u8], rows: u32| {
        let mut out = Writer::new();
        super::encode_presence(presence, rows, &mut out);
        out.into_bytes()
    };
    let words = |tag: u8, values: &[u32]| {
        let mut out = Writer::new();
        out.put_u8(tag);
        for &value in values {
            out.put_u32(value);
        }
        out.into_bytes()
    };

    let mut clustered = vec![0u8; 512];
    for row in (6..10).chain(4090..4096) {
        clustered[row / 8] |= 1 << (row % 8);
    }
    assert_eq!(
        encode(&clustered, 4093),
        words(super::presence_forms::SET_RUNS, &[2, 6, 4, 4090, 3])
    );

    let mut scattered = vec![0u8; 400];
    for row in [3usize, 900, 2001, 3199] {
        scattered[row / 8] |= 1 << (row % 8);
    }
    assert_eq!(
        encode(&scattered, 4096),
        words(super::presence_forms::SET_POSITIONS, &[4, 3, 900, 2001, 3199])
    );

    assert_eq!(encode(&[0u8], 0), vec![super::presence_forms::ALL_PRESENT]);
}

#[test]
fn the_packed_string_offsets_bit_is_known_but_declared_only_by_files_that_use_it() {
    assert_eq!(required_features::PACKED_STRING_OFFSETS, 1 << 33);
    assert_eq!(required_features::PACKED_STRING_OFFSETS & required_features::ALL, 0);
    assert_eq!(required_features::PACKED_STRING_OFFSETS & !required_features::KNOWN, 0);
    assert_eq!((1u64 << 40) & required_features::KNOWN, 0);
    assert!(crate::compat::check_features(1 << 40, 0).is_err());
    assert!(crate::compat::check_features(required_features::PACKED_STRING_OFFSETS, 0).is_ok());
}

#[test]
fn the_string_zstd_blocks_bit_is_known_but_declared_only_by_files_that_use_it() {
    assert_eq!(required_features::STRING_ZSTD_BLOCKS, 1 << 37);
    assert_eq!(required_features::STRING_ZSTD_BLOCKS & required_features::ALL, 0);
    assert_eq!(required_features::STRING_ZSTD_BLOCKS & !required_features::KNOWN, 0);
    assert!(crate::compat::check_features(required_features::STRING_ZSTD_BLOCKS, 0).is_ok());
    let old_known = required_features::KNOWN & !required_features::STRING_ZSTD_BLOCKS;
    assert_ne!(required_features::STRING_ZSTD_BLOCKS & !old_known, 0);
}

#[test]
fn the_string_lz4_blocks_bit_is_known_but_declared_only_by_files_that_use_it() {
    assert_eq!(required_features::STRING_LZ4_BLOCKS, 1 << 39);
    assert_eq!(required_features::STRING_LZ4_BLOCKS & required_features::ALL, 0);
    assert_eq!(required_features::STRING_LZ4_BLOCKS & !required_features::KNOWN, 0);
    assert!(crate::compat::check_features(required_features::STRING_LZ4_BLOCKS, 0).is_ok());
    let old_known = required_features::KNOWN & !required_features::STRING_LZ4_BLOCKS;
    assert_ne!(required_features::STRING_LZ4_BLOCKS & !old_known, 0);
}

#[test]
fn the_packed_shared_dictionaries_bit_is_known_but_declared_only_by_files_that_use_it() {
    assert_eq!(required_features::PACKED_SHARED_DICTIONARIES, 1 << 38);
    assert_eq!(required_features::KNOWN, (1 << 40) - 1);
    assert_eq!(
        required_features::PACKED_SHARED_DICTIONARIES & required_features::ALL,
        0
    );
    assert!(crate::compat::check_features(required_features::PACKED_SHARED_DICTIONARIES, 0).is_ok());
    let old_known = required_features::KNOWN & !required_features::PACKED_SHARED_DICTIONARIES;
    assert_ne!(required_features::PACKED_SHARED_DICTIONARIES & !old_known, 0);
}

#[test]
fn the_occurred_relative_ingested_at_bit_is_known_but_declared_only_by_files_that_use_it() {
    assert_eq!(required_features::OCCURRED_RELATIVE_INGESTED_AT, 1 << 34);
    assert_eq!(
        required_features::OCCURRED_RELATIVE_INGESTED_AT & required_features::ALL,
        0
    );
    assert_eq!(
        required_features::OCCURRED_RELATIVE_INGESTED_AT & !required_features::KNOWN,
        0
    );
    assert!(crate::compat::check_features(required_features::OCCURRED_RELATIVE_INGESTED_AT, 0).is_ok());
}

#[test]
fn the_entity_hash_columns_elided_bit_is_known_but_declared_only_by_files_that_use_it() {
    assert_eq!(required_features::ENTITY_HASH_COLUMNS_ELIDED, 1 << 35);
    assert_eq!(
        required_features::ENTITY_HASH_COLUMNS_ELIDED & required_features::ALL,
        0
    );
    assert_eq!(
        required_features::ENTITY_HASH_COLUMNS_ELIDED & !required_features::KNOWN,
        0
    );
    assert!(crate::compat::check_features(required_features::ENTITY_HASH_COLUMNS_ELIDED, 0).is_ok());
    assert_eq!(required_features::ENTITY_HASH_COLUMNS_ELIDED & ((1 << 35) - 1), 0);
}
