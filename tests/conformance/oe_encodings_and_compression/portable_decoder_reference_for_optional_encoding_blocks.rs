use oe::compat::{
    EscapeHatch, OptionalBlockOutcome, OptionalBlockPlan, SimulatedDecoder, SimulatedDecoderFleet, encode_demo_block,
    native_reference_decode, plan_optional_block, read_optional_block_via_escape_hatch,
};

const FUTURE_OPTIONAL: u64 = 1 << 60;

#[test]
fn new_shredded_encoding_ships_ahead_of_native_readers() {
    let block = encode_demo_block(&[7, 11, 13]);
    let checksum = *blake3::hash(&block).as_bytes();
    let hatch = EscapeHatch {
        min_reader_version: 2,
        optional_feature_bit: FUTURE_OPTIONAL,
        portable_decoder_ref: "shredded-codec-v2".to_owned(),
    };

    let fleet = SimulatedDecoderFleet::new().with_decoder("shredded-codec-v2", SimulatedDecoder::conformant(2));
    assert_eq!(
        plan_optional_block(FUTURE_OPTIONAL, 3, Some(&hatch), &fleet),
        OptionalBlockPlan::PortableDecode
    );
    let outcome =
        read_optional_block_via_escape_hatch(&block, &checksum, FUTURE_OPTIONAL, 3, Some(&hatch), &fleet).unwrap();
    assert_eq!(
        outcome,
        OptionalBlockOutcome::Decoded(native_reference_decode(&block).unwrap())
    );

    let empty_fleet = SimulatedDecoderFleet::new();
    assert_eq!(
        plan_optional_block(FUTURE_OPTIONAL, 3, Some(&hatch), &empty_fleet),
        OptionalBlockPlan::Skip
    );
    let skipped =
        read_optional_block_via_escape_hatch(&block, &checksum, FUTURE_OPTIONAL, 3, Some(&hatch), &empty_fleet)
            .unwrap();
    assert_eq!(skipped, OptionalBlockOutcome::Skip);
}

#[test]
fn portable_decoder_reference_does_not_weaken_software_parity() {
    let block = encode_demo_block(&[100, 200, 300, 400]);
    let checksum = *blake3::hash(&block).as_bytes();
    let hatch = EscapeHatch {
        min_reader_version: 2,
        optional_feature_bit: FUTURE_OPTIONAL,
        portable_decoder_ref: "shredded-codec-v2".to_owned(),
    };

    let fleet = SimulatedDecoderFleet::new().with_decoder("shredded-codec-v2", SimulatedDecoder::conformant(2));
    let outcome =
        read_optional_block_via_escape_hatch(&block, &checksum, FUTURE_OPTIONAL, 3, Some(&hatch), &fleet).unwrap();
    let native = native_reference_decode(&block).unwrap();
    assert_eq!(outcome, OptionalBlockOutcome::Decoded(native));

    let uncertified = SimulatedDecoderFleet::new().with_decoder("shredded-codec-v2", SimulatedDecoder::uncertified(2));
    assert_eq!(
        plan_optional_block(FUTURE_OPTIONAL, 3, Some(&hatch), &uncertified),
        OptionalBlockPlan::Skip
    );

    let known_bit = 1;
    assert_eq!(
        plan_optional_block(known_bit, 3, Some(&hatch), &fleet),
        OptionalBlockPlan::Native
    );
    assert_eq!(
        plan_optional_block(known_bit, 3, None, &SimulatedDecoderFleet::new()),
        OptionalBlockPlan::Native
    );
}
