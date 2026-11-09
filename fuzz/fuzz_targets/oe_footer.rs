#![no_main]
use libfuzzer_sys::fuzz_target;

use oe::file::integrity::hash_tree;
use oe::layout::footer::decode_footer;

fn expand_alphabets(data: &[u8]) {
    if let Ok(footer) = decode_footer(data) {
        for entry in &footer.packed_shared_dictionaries {
            let _ = entry.values();
        }
    }
}

fuzz_target!(|data: &[u8]| {
    expand_alphabets(data);
    let Some(count) = data
        .get(52..56)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])) as usize)
    else {
        return;
    };
    let Some(area) = data.get(56 + count * 52..) else {
        return;
    };
    let mut resealed = data.to_vec();
    for (index, entry) in data[56..56 + count * 52].chunks_exact(52).enumerate() {
        let number = |at: usize| u64::from_le_bytes(entry[at..at + 8].try_into().unwrap_or([0; 8])) as usize;
        let (offset, len) = (number(4), number(12));
        if let Some(section) = offset.checked_add(len).and_then(|end| area.get(offset..end)) {
            let at = 56 + index * 52 + 20;
            resealed[at..at + 32].copy_from_slice(hash_tree(section).as_bytes());
        }
    }
    expand_alphabets(&resealed);
});
