#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = oe::layout::decode_header(data);
    let _ = oe::layout::reader::OeFile::open(data.to_vec(), None);
});
