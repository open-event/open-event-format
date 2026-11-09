use oe::encoding::{ColumnData, decode_block, encode_block};
use oe::events::{DurationValue, TimestampValue};

#[test]
fn timestamp_stored_as_nanosecond_delta() {
    let logical = TimestampValue::from_physical_nanos(1_700_000_000_123_456_789);
    let duration = DurationValue::from_physical_nanos(42_000);
    assert_eq!(duration.physical_nanos(), 42_000);
    let block = ColumnData::I64(vec![logical.physical_nanos(); 128]);
    let encoded = encode_block(&block, false);
    let decoded = decode_block(encoded.pipeline, &encoded.bytes).unwrap();
    assert_eq!(decoded, block);
}
