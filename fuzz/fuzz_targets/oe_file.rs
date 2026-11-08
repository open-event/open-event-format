#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = oe::layout::decode_header(data);
    if let Ok(file) = oe::layout::reader::OeFile::open(data.to_vec(), None) {
        // Envelope reads reach the entity hash derivation of a file that omits the hash columns.
        let granules: Vec<u32> = file.footer().granules.iter().take(4).map(|granule| granule.granule_id).collect();
        for granule_id in granules {
            let _ = file.granule_envelopes(granule_id);
        }
    }
});
