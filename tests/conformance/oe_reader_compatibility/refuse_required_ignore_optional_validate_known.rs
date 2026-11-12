use crate::support;
use oe::compat::{
    EscapeHatch, OptionalBlockOutcome, OptionalBlockPlan, SimulatedDecoder, SimulatedDecoderFleet, encode_demo_block,
    native_reference_decode, read_optional_block_via_escape_hatch,
};
use oe::layout::footer::encode_footer;
use oe::layout::reader::OeFile;
fn retail(bytes: &[u8], blob: &[u8]) -> Vec<u8> {
    let original_blob_len = {
        let tail = &bytes[bytes.len() - 12..bytes.len() - 4];
        u64::from_le_bytes(tail.try_into().unwrap()) as usize
    };
    let data_end = bytes.len() - 12 - original_blob_len;
    let mut out = bytes[..data_end].to_vec();
    out.extend_from_slice(blob);
    out.extend_from_slice(&(blob.len() as u64).to_le_bytes());
    out.extend_from_slice(b"OEF1");
    out
}

#[test]
fn unknown_required_feature_fails_file() {
    let built = support::built_file(8);
    let mut footer = built.footer.clone();
    footer.required_feature_flags |= 1 << 62;
    let blob = encode_footer(&footer);
    assert!(OeFile::open(retail(&built.bytes, &blob), None).is_err());
}

#[test]
fn checksum_validated_before_use() {
    let built = support::built_file(8);
    let mut corrupted = built.bytes.to_vec();
    let offset = built.footer.stripes[0].file_offset as usize;
    corrupted[offset] ^= 0x01;
    assert!(OeFile::open(corrupted, None).is_err());
}

#[test]
fn unknown_optional_block_ignored_or_read_via_portable_decoder() {
    let unknown_optional = 1 << 60;

    let built = support::built_file(8);
    let mut footer = built.footer.clone();
    footer.optional_feature_flags |= unknown_optional;
    let blob = encode_footer(&footer);
    let file = OeFile::open(retail(&built.bytes, &blob), None).unwrap();
    let empty = SimulatedDecoderFleet::new();
    assert_eq!(file.usable_optional_features() & unknown_optional, 0);
    assert_eq!(
        file.optional_feature_plan(unknown_optional, 1, &empty),
        OptionalBlockPlan::Skip
    );
    assert_eq!(file.header().row_count, 8);

    let mut footer = built.footer.clone();
    footer.optional_feature_flags |= unknown_optional;
    footer.escape_hatches.push(EscapeHatch {
        min_reader_version: 1,
        optional_feature_bit: unknown_optional,
        portable_decoder_ref: "portable".to_owned(),
    });
    let blob = encode_footer(&footer);
    let file = OeFile::open(retail(&built.bytes, &blob), None).unwrap();
    let fleet = SimulatedDecoderFleet::new().with_decoder("portable", SimulatedDecoder::conformant(1));
    assert_eq!(
        file.optional_feature_plan(unknown_optional, 1, &fleet),
        OptionalBlockPlan::PortableDecode
    );

    let block = encode_demo_block(&[5, 6, 7]);
    let checksum = *blake3::hash(&block).as_bytes();
    let hatch = &file.footer().escape_hatches[0];
    let outcome =
        read_optional_block_via_escape_hatch(&block, &checksum, unknown_optional, 1, Some(hatch), &fleet).unwrap();
    assert_eq!(
        outcome,
        OptionalBlockOutcome::Decoded(native_reference_decode(&block).unwrap())
    );
}
