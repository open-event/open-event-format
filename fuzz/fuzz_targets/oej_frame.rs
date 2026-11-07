#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = oe::artifacts::frame::precheck_header(data);
    let _ = oe::artifacts::frame::decode_frame(data);
});
