use multiversion::multiversion;

pub(crate) fn count_set_bits(bitmap: &[u8], len: usize) -> usize {
    let full_bytes = (len / 8).min(bitmap.len());
    let mut count = count_ones(bitmap.get(..full_bytes).unwrap_or(bitmap));
    let tail_bits = len % 8;
    if tail_bits > 0
        && let Some(&byte) = bitmap.get(full_bytes)
    {
        let mask = (1u8 << tail_bits) - 1;
        count += (byte & mask).count_ones() as usize;
    }
    count
}

#[multiversion(targets(
    "x86_64+avx512f+avx512bw+avx512dq+avx512vl+avx512vpopcntdq",
    "x86_64+avx512f+avx512bw+avx512dq+avx512vl",
    "x86_64+avx2",
    "aarch64+neon"
))]
fn count_ones(bytes: &[u8]) -> usize {
    let mut blocks = bytes.chunks_exact(32);
    let mut lanes = [0u64; 4];
    for block in blocks.by_ref() {
        for (lane, word) in lanes.iter_mut().zip(block.chunks_exact(8)) {
            *lane += u64::from(u64::from_le_bytes(word.try_into().unwrap_or([0; 8])).count_ones());
        }
    }
    let mut words = blocks.remainder().chunks_exact(8);
    let mut count: u64 = lanes.iter().sum();
    count += words
        .by_ref()
        .map(|word| u64::from(u64::from_le_bytes(word.try_into().unwrap_or([0; 8])).count_ones()))
        .sum::<u64>();
    count += words
        .remainder()
        .iter()
        .map(|byte| u64::from(byte.count_ones()))
        .sum::<u64>();
    count as usize
}

pub(crate) fn count_present_before(bitmap: &[u8], row_idx: usize) -> usize {
    count_set_bits(bitmap, row_idx)
}

pub(crate) fn present_position(bitmap: &[u8], row: usize) -> Option<usize> {
    let byte = *bitmap.get(row / 8)?;
    let bit = 1u8 << (row % 8);
    (byte & bit != 0).then(|| count_present_before(bitmap, row))
}

#[derive(Debug)]
pub(crate) struct PresenceRank {
    present_before_word: Vec<u32>,
}

impl PresenceRank {
    pub(crate) fn new(bitmap: &[u8]) -> Self {
        Self {
            present_before_word: present_before_each_word(bitmap),
        }
    }

    pub(crate) fn position(&self, bitmap: &[u8], row: usize) -> Option<usize> {
        let byte = *bitmap.get(row / 8)?;
        if byte & (1u8 << (row % 8)) == 0 {
            return None;
        }
        let word_index = row / 64;
        let before_word = *self.present_before_word.get(word_index)? as usize;
        let start = word_index * 8;
        let word = load_word(bitmap.get(start..(start + 8).min(bitmap.len()))?);
        let below_row = (1u64 << (row % 64)) - 1;
        Some(before_word + (word & below_row).count_ones() as usize)
    }
}

#[multiversion(targets(
    "x86_64+avx512f+avx512bw+avx512dq+avx512vl+avx512vpopcntdq",
    "x86_64+avx512f+avx512bw+avx512dq+avx512vl",
    "x86_64+avx2",
    "aarch64+neon"
))]
fn present_before_each_word(bitmap: &[u8]) -> Vec<u32> {
    let mut words = bitmap.chunks_exact(8);
    let mut running = 0u32;
    let mut present_before_word: Vec<u32> = words
        .by_ref()
        .map(|word| {
            let before = running;
            running += u64::from_le_bytes(word.try_into().unwrap_or([0; 8])).count_ones();
            before
        })
        .collect();
    if !words.remainder().is_empty() {
        present_before_word.push(running);
    }
    present_before_word
}

fn load_word(bytes: &[u8]) -> u64 {
    let mut word = [0u8; 8];
    for (slot, byte) in word.iter_mut().zip(bytes) {
        *slot = *byte;
    }
    u64::from_le_bytes(word)
}

#[cfg(test)]
#[path = "test/presence.rs"]
mod tests;
