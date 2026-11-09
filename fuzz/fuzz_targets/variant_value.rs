#![no_main]
use libfuzzer_sys::fuzz_target;

use oe::artifacts::batch::decode_variant_dictionary;
use oe::events::variant::VariantRef;

fuzz_target!(|data: &[u8]| {
    let mid = data.len() / 2;
    let (dict_bytes, value_bytes) = data.split_at(mid);
    if let Ok(dictionary) = decode_variant_dictionary(dict_bytes) {
        let _ = VariantRef::new(value_bytes).validate(&dictionary);
    }
});
