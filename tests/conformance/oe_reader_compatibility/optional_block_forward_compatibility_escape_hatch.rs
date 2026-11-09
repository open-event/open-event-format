use oe::compat::{
    EscapeHatch, OptionalBlockOutcome, OptionalBlockPlan, SimulatedDecoder, SimulatedDecoderFleet, encode_demo_block,
    native_reference_decode, read_optional_block_via_escape_hatch,
};
use oe::layout::footer::encode_footer;
use oe::layout::reader::OeFile;

use crate::support;
use oe::columns::REQUIRED_COLUMNS;
const FUTURE_OPTIONAL: u64 = 1 << 60;

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
fn old_reader_reads_a_new_optional_encoding_via_a_portable_decoder() {
    let block = encode_demo_block(&[100, 200, 300]);
    let checksum = *blake3::hash(&block).as_bytes();
    let hatch = EscapeHatch {
        min_reader_version: 2,
        optional_feature_bit: FUTURE_OPTIONAL,
        portable_decoder_ref: "fancy-codec-v2".to_owned(),
    };
    let fleet = SimulatedDecoderFleet::new().with_decoder("fancy-codec-v2", SimulatedDecoder::conformant(2));

    let outcome =
        read_optional_block_via_escape_hatch(&block, &checksum, FUTURE_OPTIONAL, 2, Some(&hatch), &fleet).unwrap();
    assert_eq!(
        outcome,
        OptionalBlockOutcome::Decoded(native_reference_decode(&block).unwrap()),
        "decoded rows are byte-for-byte identical to a native decode"
    );

    let mut corrupted = block.clone();
    corrupted[0] ^= 0x01;
    assert!(
        read_optional_block_via_escape_hatch(&corrupted, &checksum, FUTURE_OPTIONAL, 2, Some(&hatch), &fleet).is_err()
    );
}

#[test]
fn old_reader_skips_the_optional_block_to_a_correct_scan() {
    let built = support::built_file(8);
    let mut footer = built.footer.clone();
    footer.optional_feature_flags |= FUTURE_OPTIONAL;
    footer.escape_hatches.push(EscapeHatch {
        min_reader_version: 9,
        optional_feature_bit: FUTURE_OPTIONAL,
        portable_decoder_ref: "future-codec".to_owned(),
    });
    let blob = encode_footer(&footer);
    let file = OeFile::open(retail(&built.bytes, &blob), None).unwrap();

    let fleet = SimulatedDecoderFleet::new();
    assert_eq!(
        file.optional_feature_plan(FUTURE_OPTIONAL, 1, &fleet),
        OptionalBlockPlan::Skip
    );
    assert_eq!(file.usable_optional_features() & FUTURE_OPTIONAL, 0);
    assert_eq!(file.header().row_count, 8);
    let granule = file.footer().granules[0].granule_id;
    for spec in REQUIRED_COLUMNS {
        file.read_column(spec.column_id, granule).unwrap();
    }
}

#[test]
fn escape_hatch_never_touches_required_features() {
    let built = support::built_file(8);

    let mut footer = built.footer.clone();
    footer.required_feature_flags |= 1 << 62;
    let blob = encode_footer(&footer);
    assert!(OeFile::open(retail(&built.bytes, &blob), None).is_err());

    let mut footer = built.footer.clone();
    footer.required_feature_flags |= 1 << 62;
    footer.escape_hatches.push(EscapeHatch {
        min_reader_version: 1,
        optional_feature_bit: FUTURE_OPTIONAL,
        portable_decoder_ref: "irrelevant".to_owned(),
    });
    let blob = encode_footer(&footer);
    assert!(OeFile::open(retail(&built.bytes, &blob), None).is_err());
}

#[test]
fn unconformant_portable_decoder_is_not_trusted() {
    let block = encode_demo_block(&[1, 2, 3]);
    let checksum = *blake3::hash(&block).as_bytes();
    let hatch = EscapeHatch {
        min_reader_version: 1,
        optional_feature_bit: FUTURE_OPTIONAL,
        portable_decoder_ref: "unvetted".to_owned(),
    };
    let fleet = SimulatedDecoderFleet::new().with_decoder("unvetted", SimulatedDecoder::uncertified(1));

    let outcome =
        read_optional_block_via_escape_hatch(&block, &checksum, FUTURE_OPTIONAL, 1, Some(&hatch), &fleet).unwrap();
    assert_eq!(
        outcome,
        OptionalBlockOutcome::Skip,
        "an uncertified decoder is declined, not used"
    );

    let built = support::built_file(8);
    let mut footer = built.footer.clone();
    footer.optional_feature_flags |= FUTURE_OPTIONAL;
    footer.escape_hatches.push(hatch);
    let blob = encode_footer(&footer);
    let file = OeFile::open(retail(&built.bytes, &blob), None).unwrap();
    assert_eq!(
        file.optional_feature_plan(FUTURE_OPTIONAL, 1, &fleet),
        OptionalBlockPlan::Skip
    );
}
