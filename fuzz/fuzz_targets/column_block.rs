#![no_main]
use libfuzzer_sys::fuzz_target;

use oe::encoding::{
    ColumnData, PipelineId, decode_block, decode_string_block_views, occurred_residual_inner, resolve_occurred_residual,
};

fuzz_target!(|data: &[u8]| {
    if data.len() >= 4 {
        let (id_bytes, body) = data.split_at(4);
        let id = u32::from_le_bytes(id_bytes.try_into().unwrap_or([0; 4]));
        let _ = decode_block(PipelineId(id), body);
        let _ = decode_string_block_views(PipelineId(id), body);
        for transform in [8u32, 10] {
            let _ = decode_block(PipelineId(transform | (6 << 8) | (5 << 16)), body);
        }
        let count = occurred_residual_inner(data)
            .ok()
            .and_then(|(inner, rest)| match decode_block(inner, rest) {
                Ok(ColumnData::I64(values)) => Some(values.len()),
                _ => None,
            })
            .unwrap_or(0);
        let occurred: Vec<i64> = (0..count as i64).map(|i| i.wrapping_mul(0x5DEE_CE66)).collect();
        let _ = resolve_occurred_residual(data, &occurred);
        let _ = resolve_occurred_residual(data, occurred.get(1..).unwrap_or_default());
    }
});
