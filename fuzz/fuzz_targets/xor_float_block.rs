#![no_main]
use libfuzzer_sys::fuzz_target;

use oe::encoding::{Compression, PipelineId, Transform, ValueKind, decode_block};
use oe::layout::time_series::TimeSeriesRollups;

fuzz_target!(|data: &[u8]| {
    let _ = decode_block(PipelineId::new(Transform::Chimp, Compression::None, ValueKind::F64), data);
    let _ = TimeSeriesRollups::decode(data);
});
