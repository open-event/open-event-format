use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer};
use crate::indexes::probabilistic::SplitBlockBloomFilter;
use crate::indexes::{Exactness, stable_hash, stable_hash_ascii_lowercase};
use hashbrown::HashSet;
use std::borrow::Cow;
use std::cell::RefCell;

const TEXT_TOKEN_MAGIC: u32 = 0x5454_4b31;

const BITS_PER_TOKEN: u32 = 8;

const TEXT_TOKEN_HEADER_BYTES: usize = 8;

const NGRAM_LEN: usize = 3;

const TRIGRAM_CODE_BITS: u32 = 21;

const TRIGRAM_CODE_MASK: usize = (1 << TRIGRAM_CODE_BITS) - 1;

const TRIGRAM_BITMAP_WORDS: usize = 1 << (TRIGRAM_CODE_BITS - 6);

const TRIGRAM_SUMMARY_WORDS: usize = TRIGRAM_BITMAP_WORDS >> 6;

const MAX_RETAINED_TOKEN_HASHES: usize = 1 << 20;

thread_local! {
    static BUILD_SCRATCH: RefCell<Option<BuildScratch>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextTokenIndex {
    filter: SplitBlockBloomFilter,
    has_ngrams: bool,
}

impl TextTokenIndex {
    pub fn build(token_hashes: &[u64], has_ngrams: bool) -> Self {
        TextTokenIndex {
            filter: SplitBlockBloomFilter::build(token_hashes, BITS_PER_TOKEN),
            has_ngrams,
        }
    }

    pub fn build_from_values(values: &[&str], include_ngrams: bool) -> Self {
        Self::build_from_iter(values.iter().copied(), include_ngrams)
    }

    pub fn build_from_iter<'a>(values: impl IntoIterator<Item = &'a str>, include_ngrams: bool) -> Self {
        let mut scratch = BUILD_SCRATCH
            .with(|slot| slot.borrow_mut().take())
            .unwrap_or_else(BuildScratch::new);
        for text in values {
            scratch.insert_value(text, include_ngrams);
        }
        let filter = scratch.build_filter();
        if scratch.other_hashes.capacity() <= MAX_RETAINED_TOKEN_HASHES {
            BUILD_SCRATCH.with(|slot| *slot.borrow_mut() = Some(scratch));
        }
        TextTokenIndex {
            filter,
            has_ngrams: include_ngrams,
        }
    }

    pub fn might_contain_token(&self, token: &str) -> bool {
        self.filter.contains(hash_folded_token(token))
    }

    pub fn might_contain_token_hash(&self, token_hash: u64) -> bool {
        self.filter.contains(token_hash)
    }

    pub fn might_contain_substring(&self, needle: &str) -> bool {
        if !self.has_ngrams {
            return true;
        }
        if needle.is_ascii() {
            return needle
                .as_bytes()
                .windows(NGRAM_LEN)
                .all(|ngram| self.filter.contains(stable_hash_ascii_lowercase(ngram)));
        }
        let needle = fold_for_ngrams(needle);
        for ngram in ngrams(&needle, NGRAM_LEN) {
            if !self.filter.contains(hash_token(ngram)) {
                return false;
            }
        }
        true
    }

    pub fn has_ngrams(&self) -> bool {
        self.has_ngrams
    }

    pub fn exactness(&self) -> Exactness {
        Exactness::InexactNoFalseNegative
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::with_capacity(TEXT_TOKEN_HEADER_BYTES + self.filter.encoded_len());
        out.put_u32(TEXT_TOKEN_MAGIC);
        out.put_u32(if self.has_ngrams { 1 } else { 0 });
        self.filter.encode_into(&mut out);
        out.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        if reader.u32("text token magic")? != TEXT_TOKEN_MAGIC {
            return Err(FormatError::Structural {
                rule: "text token index bad magic",
            });
        }
        let flags = reader.u32("text token flags")?;
        if flags & !1 != 0 {
            return Err(FormatError::ReservedNotZero {
                field: "TextTokenIndex.flags",
            });
        }
        let has_ngrams = flags & 1 != 0;
        let remaining = reader.remaining();
        let rest = reader.take(remaining, "text token filter bytes")?;
        let filter = SplitBlockBloomFilter::decode(rest)?;
        Ok(TextTokenIndex { filter, has_ngrams })
    }
}

pub fn tokenize(text: &str) -> impl Iterator<Item = Cow<'_, str>> + '_ {
    split_tokens(text).map(|token| match is_already_folded(token) {
        true => Cow::Borrowed(token),
        false => Cow::Owned(token.to_lowercase()),
    })
}

fn split_tokens(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| c.is_whitespace() || (c.is_ascii_punctuation() && c != '\''))
        .filter(|s| !s.is_empty())
}

fn split_ascii_tokens(text: &[u8]) -> impl Iterator<Item = &[u8]> {
    text.split(|&byte| is_ascii_token_separator(byte))
        .filter(|token| !token.is_empty())
}

fn is_ascii_token_separator(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | 0x0B | 0x0C | b'\r' | b' ') || (byte.is_ascii_punctuation() && byte != b'\'')
}

struct BuildScratch {
    occupied_words: [u64; TRIGRAM_SUMMARY_WORDS],
    other_hashes: HashSet<u64>,
    trigram_count: usize,
    trigrams: Vec<u64>,
}

impl BuildScratch {
    fn new() -> Self {
        BuildScratch {
            occupied_words: [0; TRIGRAM_SUMMARY_WORDS],
            other_hashes: HashSet::new(),
            trigram_count: 0,
            trigrams: vec![0; TRIGRAM_BITMAP_WORDS],
        }
    }

    fn insert_value(&mut self, text: &str, include_ngrams: bool) {
        if text.is_ascii() {
            for token in split_ascii_tokens(text.as_bytes()) {
                self.insert_ascii_token(token);
            }
            if include_ngrams {
                self.mark_ascii_trigrams(text.as_bytes());
            }
            return;
        }
        for token in split_tokens(text) {
            if token.is_ascii() {
                self.insert_ascii_token(token.as_bytes());
            } else {
                self.insert_folded_key(token.to_lowercase().as_bytes());
            }
        }
        if include_ngrams {
            let folded = fold_for_ngrams(text);
            for ngram in ngrams(&folded, NGRAM_LEN) {
                self.insert_folded_key(ngram.as_bytes());
            }
        }
    }

    fn insert_ascii_token(&mut self, token: &[u8]) {
        if let &[a, b, c] = token {
            self.mark_trigram(trigram_code([a, b, c]));
        } else {
            self.other_hashes.insert(stable_hash_ascii_lowercase(token));
        }
    }

    fn insert_folded_key(&mut self, key: &[u8]) {
        if let &[a, b, c] = key
            && key.is_ascii()
        {
            self.mark_trigram(trigram_code([a, b, c]));
        } else {
            self.other_hashes.insert(stable_hash(key));
        }
    }

    fn mark_ascii_trigrams(&mut self, text: &[u8]) {
        let Some((&[first, second], rest)) = text.split_first_chunk::<2>() else {
            return;
        };
        let mut code = (usize::from(first.to_ascii_lowercase()) << 7) | usize::from(second.to_ascii_lowercase());
        for &byte in rest {
            code = ((code << 7) | usize::from(byte.to_ascii_lowercase())) & TRIGRAM_CODE_MASK;
            self.mark_trigram(code);
        }
    }

    fn mark_trigram(&mut self, code: usize) {
        let word_index = code >> 6;
        if let Some(word) = self.trigrams.get_mut(word_index) {
            let bit = 1u64 << (code & 63);
            if *word & bit == 0 {
                *word |= bit;
                self.trigram_count += 1;
                if let Some(summary) = self.occupied_words.get_mut(word_index >> 6) {
                    *summary |= 1u64 << (word_index & 63);
                }
            }
        }
    }

    fn build_filter(&mut self) -> SplitBlockBloomFilter {
        let key_count = self.other_hashes.len() + self.trigram_count;
        let trigrams = &mut self.trigrams;
        let occupied_word_indexes = self
            .occupied_words
            .iter_mut()
            .enumerate()
            .flat_map(|(summary_index, summary)| {
                let mut occupied = std::mem::take(summary);
                std::iter::from_fn(move || {
                    if occupied == 0 {
                        return None;
                    }
                    let word_index = (summary_index << 6) | occupied.trailing_zeros() as usize;
                    occupied &= occupied - 1;
                    Some(word_index)
                })
            });
        let trigram_hashes = occupied_word_indexes.flat_map(|word_index| {
            let mut bits = trigrams.get_mut(word_index).map(std::mem::take).unwrap_or(0);
            std::iter::from_fn(move || {
                if bits == 0 {
                    return None;
                }
                let code = (word_index << 6) | bits.trailing_zeros() as usize;
                bits &= bits - 1;
                Some(trigram_hash(code))
            })
        });
        let filter = SplitBlockBloomFilter::build_from_iter(
            self.other_hashes.iter().copied().chain(trigram_hashes),
            key_count,
            BITS_PER_TOKEN,
        );
        self.other_hashes.clear();
        self.trigram_count = 0;
        filter
    }
}

fn trigram_code(bytes: [u8; 3]) -> usize {
    bytes
        .iter()
        .fold(0, |code, byte| (code << 7) | usize::from(byte.to_ascii_lowercase()))
}

fn trigram_hash(code: usize) -> u64 {
    let bytes = [(code >> 14) as u8, (code >> 7 & 0x7f) as u8, (code & 0x7f) as u8];
    stable_hash_ascii_lowercase(&bytes)
}

fn is_already_folded(text: &str) -> bool {
    text.bytes().all(|byte| byte.is_ascii() && !byte.is_ascii_uppercase())
}

pub fn hash_folded_token(token: &str) -> u64 {
    if token.is_ascii() {
        stable_hash_ascii_lowercase(token.as_bytes())
    } else {
        hash_token(&token.to_lowercase())
    }
}

fn fold_for_ngrams(text: &str) -> Cow<'_, str> {
    if is_already_folded(text) {
        return Cow::Borrowed(text);
    }
    Cow::Owned(
        text.chars()
            .flat_map(char::to_lowercase)
            .map(|c| if c == 'ς' { 'σ' } else { c })
            .collect(),
    )
}

pub fn ngrams(text: &str, n: usize) -> impl Iterator<Item = &str> {
    char_boundaries(text)
        .zip(char_boundaries(text).skip(n))
        .map_while(move |(start, stop)| text.get(start..stop))
}

fn char_boundaries(text: &str) -> impl Iterator<Item = usize> {
    text.char_indices()
        .map(|(pos, _)| pos)
        .chain(std::iter::once(text.len()))
}

pub fn hash_token(token: &str) -> u64 {
    stable_hash(token.as_bytes())
}

#[cfg(test)]
#[path = "test/text_token.rs"]
mod tests;
