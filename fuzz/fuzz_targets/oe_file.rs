#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = oe::layout::decode_header(data);
    if let Ok(file) = oe::layout::reader::OeFile::open(data.to_vec(), None) {
        let granules: Vec<u32> = file.footer().granules.iter().take(4).map(|granule| granule.granule_id).collect();
        for granule_id in granules {
            let _ = file.granule_envelopes(granule_id);
        }
        let packed: Vec<u32> = file.footer().packed_shared_dictionaries.iter().map(|entry| entry.column_id).collect();
        for column_id in packed {
            let equals = oe::encoding::predicate::StringPredicate::Equals("x".to_owned());
            let _ = file.shared_dictionary_predicate(column_id, &equals);
            let _ = file.shared_alphabet(column_id);
        }
    }
});
