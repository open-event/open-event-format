//! BM25 inverted index artifact: one per OE file/segment over a text column.
//!
//! Layout (all integers little endian / LEB128 varints):
//!
//! ```text
//! header   magic "OEBM" | version u16 | flags u16 (bit 0 = sealed) | segment_id u128 | content_hash [32]
//! body     doc_count u32 | total_length u64 | term_count u32
//!          doc lengths      varint × doc_count
//!          event ids        (varint len, utf-8) × doc_count
//!          term dictionary  front coded in runs of 16: (varint shared, varint suffix_len, suffix)
//!          term postings    per term: varint df, varint max_tf, varint min_len, varint block_count,
//!                           per block: varint count, varint last_doc delta, varint max_tf, varint min_len,
//!                           varint payload_len
//!          payload          varint len | blocks: width u8, width u8, bitpacked doc deltas, bitpacked tf-1
//! trailer  BLAKE3 (keyed by derive_key context) over header ‖ body-or-sealed-body
//! ```
//!
//! A sealed artifact stores `nonce[24] ‖ XChaCha20-Poly1305(body)` with the header as AAD, under a key derived from
//! the wrapping key and the header (so it is bound to the segment and its content). The nonce is a keyed hash of
//! the plaintext, so the same input under the same key gives the same bytes (deterministic, SIV-style).
//! Postings are 128-doc blocks; per-block `max_tf` and `min_len` give a block-max BM25 upper bound for pruning.

use crate::error::FormatError;
use crate::indexes::text_token::tokenize;
use crate::security::AeadScheme;
use crate::vector::WrappingKey;
use std::collections::BTreeMap;

pub const BM25_MAGIC: &[u8; 4] = b"OEBM";
pub const BM25_VERSION: u16 = 1;
/// Documents per posting block.
pub const POSTING_BLOCK: usize = 128;
const FRONT_CODING_RUN: usize = 16;
const HEADER_LEN: usize = 4 + 2 + 2 + 16 + 32;
const TRAILER_LEN: usize = 32;
const NONCE_LEN: usize = 24;
const FLAG_SEALED: u16 = 1;
const TRAILER_CONTEXT: &str = "oe 2026 bm25 text index trailer v1";
const KEY_CONTEXT: &str = "oe 2026 bm25 text index key v1";
const NONCE_CONTEXT: &str = "oe 2026 bm25 text index nonce v1";
/// Doc id reserved as the exhausted-cursor sentinel; segments hold fewer documents.
pub(crate) const NO_DOC: u32 = u32::MAX;

#[derive(Debug, thiserror::Error)]
pub enum TextIndexError {
    #[error("text index failed authentication: {what}")]
    Authentication { what: &'static str },
    #[error("text index is malformed: {0}")]
    Format(#[from] FormatError),
    #[error("invalid text index input: {rule}")]
    InvalidInput { rule: String },
    #[error("text index does not match: {detail}")]
    Mismatch { detail: String },
    #[error("text index bytes unavailable: {detail}")]
    Unavailable { detail: String },
}

fn malformed(rule: &'static str) -> TextIndexError {
    TextIndexError::Format(FormatError::Structural { rule })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockMeta {
    pub(crate) count: u32,
    /// Last doc id of the previous block (0 for the first block, whose first delta is absolute).
    pub(crate) base: u32,
    pub(crate) last_doc: u32,
    pub(crate) max_tf: u32,
    pub(crate) min_len: u32,
    pub(crate) offset: usize,
    pub(crate) len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TermPostings {
    pub(crate) doc_freq: u32,
    pub(crate) max_tf: u32,
    pub(crate) min_len: u32,
    pub(crate) blocks: Vec<BlockMeta>,
}

/// A decoded BM25 index for one segment. Doc ids are the segment's row ordinals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bm25Index {
    pub segment_id: u128,
    /// Hash of the segment content the index was built from; loading against other content is refused.
    pub content_hash: [u8; 32],
    pub(crate) event_ids: Vec<String>,
    pub(crate) doc_lengths: Vec<u32>,
    pub(crate) total_length: u64,
    pub(crate) terms: Vec<String>,
    pub(crate) postings: Vec<TermPostings>,
    pub(crate) data: Vec<u8>,
}

/// One document to index: its event id and payload text. Row `i` of the input is doc id `i`.
#[derive(Debug, Clone, Copy)]
pub struct TextDocument<'a> {
    pub event_id: &'a str,
    pub text: &'a str,
}

impl Bm25Index {
    /// Builds the index with the same tokenisation as the text-token pruning index (`text_token::tokenize`).
    pub fn build(segment_id: u128, content_hash: [u8; 32], docs: &[TextDocument<'_>]) -> Result<Self, TextIndexError> {
        if docs.len() >= NO_DOC as usize {
            return Err(TextIndexError::InvalidInput {
                rule: "too many documents for one text index".to_owned(),
            });
        }
        let mut inverted: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
        let mut doc_lengths = Vec::with_capacity(docs.len());
        let mut total_length = 0u64;
        let mut counts: BTreeMap<String, u32> = BTreeMap::new();
        for (doc, document) in (0u32..).zip(docs) {
            counts.clear();
            let mut length = 0u32;
            for token in tokenize(document.text) {
                length = length.saturating_add(1);
                let slot = counts.entry(token.into_owned()).or_insert(0);
                *slot = slot.saturating_add(1);
            }
            for (term, tf) in std::mem::take(&mut counts) {
                inverted.entry(term).or_default().push((doc, tf));
            }
            doc_lengths.push(length);
            total_length += u64::from(length);
        }
        let mut terms = Vec::with_capacity(inverted.len());
        let mut postings = Vec::with_capacity(inverted.len());
        let mut data = Vec::new();
        for (term, list) in inverted {
            let mut blocks = Vec::with_capacity(list.len().div_ceil(POSTING_BLOCK));
            let mut base = 0u32;
            for chunk in list.chunks(POSTING_BLOCK) {
                let offset = data.len();
                let mut previous = base;
                let deltas: Vec<u32> = chunk
                    .iter()
                    .map(|&(doc, _)| {
                        let delta = doc - previous;
                        previous = doc;
                        delta
                    })
                    .collect();
                let tfs: Vec<u32> = chunk.iter().map(|&(_, tf)| tf - 1).collect();
                let lens = chunk
                    .iter()
                    .map(|&(doc, _)| doc_lengths.get(doc as usize).copied().unwrap_or(0));
                let delta_width = bit_width(deltas.iter().copied().max().unwrap_or(0));
                let tf_width = bit_width(tfs.iter().copied().max().unwrap_or(0));
                data.push(delta_width);
                data.push(tf_width);
                pack(&deltas, delta_width, &mut data);
                pack(&tfs, tf_width, &mut data);
                blocks.push(BlockMeta {
                    count: chunk.len() as u32,
                    base,
                    last_doc: previous,
                    max_tf: chunk.iter().map(|&(_, tf)| tf).max().unwrap_or(1),
                    min_len: lens.min().unwrap_or(0),
                    offset,
                    len: data.len() - offset,
                });
                base = previous;
            }
            postings.push(TermPostings {
                doc_freq: list.len() as u32,
                max_tf: blocks.iter().map(|block| block.max_tf).max().unwrap_or(1),
                min_len: blocks.iter().map(|block| block.min_len).min().unwrap_or(0),
                blocks,
            });
            terms.push(term);
        }
        Ok(Bm25Index {
            segment_id,
            content_hash,
            event_ids: docs.iter().map(|doc| doc.event_id.to_owned()).collect(),
            doc_lengths,
            total_length,
            terms,
            postings,
            data,
        })
    }

    pub fn doc_count(&self) -> u32 {
        self.doc_lengths.len() as u32
    }

    pub fn total_length(&self) -> u64 {
        self.total_length
    }

    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    pub fn event_id(&self, doc: u32) -> Option<&str> {
        self.event_ids.get(doc as usize).map(String::as_str)
    }

    pub(crate) fn term(&self, term: &str) -> Option<&TermPostings> {
        let position = self.terms.binary_search_by(|probe| probe.as_str().cmp(term)).ok()?;
        self.postings.get(position)
    }

    /// Document frequency of an already folded term.
    pub fn doc_freq(&self, term: &str) -> u32 {
        self.term(term).map_or(0, |postings| postings.doc_freq)
    }

    pub(crate) fn doc_length(&self, doc: u32) -> u32 {
        self.doc_lengths.get(doc as usize).copied().unwrap_or(0)
    }

    /// Decodes a block's doc ids and term frequencies into `docs`/`tfs`.
    pub(crate) fn decode_block(&self, block: &BlockMeta, docs: &mut Vec<u32>, tfs: &mut Vec<u32>) -> Option<()> {
        docs.clear();
        tfs.clear();
        let payload = self.data.get(block.offset..block.offset.checked_add(block.len)?)?;
        let (&delta_width, rest) = payload.split_first()?;
        let (&tf_width, rest) = rest.split_first()?;
        let count = block.count as usize;
        let used = unpack(rest, count, delta_width, docs)?;
        unpack(rest.get(used..)?, count, tf_width, tfs)?;
        let mut current = block.base;
        for doc in docs.iter_mut() {
            current = current.checked_add(*doc)?;
            *doc = current;
        }
        for tf in tfs.iter_mut() {
            *tf = tf.checked_add(1)?;
        }
        Some(())
    }

    /// Serialises the index; with `key` the body is sealed. Equal inputs give equal bytes.
    pub fn encode(&self, key: Option<&WrappingKey>) -> Result<Vec<u8>, TextIndexError> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.data.len() + 64);
        out.extend_from_slice(BM25_MAGIC);
        out.extend_from_slice(&BM25_VERSION.to_le_bytes());
        out.extend_from_slice(&(if key.is_some() { FLAG_SEALED } else { 0 }).to_le_bytes());
        out.extend_from_slice(&self.segment_id.to_le_bytes());
        out.extend_from_slice(&self.content_hash);
        let body = self.encode_body();
        match key {
            None => out.extend_from_slice(&body),
            Some(key) => {
                let (data_key, nonce) = seal_parameters(key, &out, &body);
                let sealed = AeadScheme::XChaCha20Poly1305
                    .seal(&data_key, &nonce, &out, &body)
                    .ok_or(TextIndexError::Authentication { what: "seal body" })?;
                out.extend_from_slice(&nonce);
                out.extend_from_slice(&sealed);
            }
        }
        let trailer = trailer_digest(&out);
        out.extend_from_slice(&trailer);
        Ok(out)
    }

    fn encode_body(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.data.len() + self.event_ids.len() * 16 + self.terms.len() * 16);
        out.extend_from_slice(&self.doc_count().to_le_bytes());
        out.extend_from_slice(&self.total_length.to_le_bytes());
        out.extend_from_slice(&(self.terms.len() as u32).to_le_bytes());
        for length in &self.doc_lengths {
            put_varint(&mut out, u64::from(*length));
        }
        for event_id in &self.event_ids {
            put_varint(&mut out, event_id.len() as u64);
            out.extend_from_slice(event_id.as_bytes());
        }
        let mut previous: &[u8] = &[];
        for (position, term) in self.terms.iter().enumerate() {
            let term = term.as_bytes();
            let shared = match position % FRONT_CODING_RUN {
                0 => 0,
                _ => previous
                    .iter()
                    .zip(term)
                    .take_while(|(left, right)| left == right)
                    .count(),
            };
            let suffix = term.get(shared..).unwrap_or_default();
            put_varint(&mut out, shared as u64);
            put_varint(&mut out, suffix.len() as u64);
            out.extend_from_slice(suffix);
            previous = term;
        }
        for postings in &self.postings {
            put_varint(&mut out, u64::from(postings.doc_freq));
            put_varint(&mut out, u64::from(postings.max_tf));
            put_varint(&mut out, u64::from(postings.min_len));
            put_varint(&mut out, postings.blocks.len() as u64);
            for block in &postings.blocks {
                put_varint(&mut out, u64::from(block.count));
                put_varint(&mut out, u64::from(block.last_doc - block.base));
                put_varint(&mut out, u64::from(block.max_tf));
                put_varint(&mut out, u64::from(block.min_len));
                put_varint(&mut out, block.len as u64);
            }
        }
        put_varint(&mut out, self.data.len() as u64);
        out.extend_from_slice(&self.data);
        out
    }

    /// Decodes and fully validates an index. A sealed index needs `key`.
    pub fn decode(bytes: &[u8], key: Option<&WrappingKey>) -> Result<Self, TextIndexError> {
        let split = bytes
            .len()
            .checked_sub(TRAILER_LEN)
            .filter(|&split| split >= HEADER_LEN)
            .ok_or(malformed("bm25 index truncated"))?;
        let (content, trailer) = bytes.split_at(split);
        if trailer_digest(content).as_slice() != trailer {
            return Err(TextIndexError::Authentication {
                what: "bm25 index trailer",
            });
        }
        let (header, rest) = content.split_at(HEADER_LEN);
        if header.get(..4) != Some(BM25_MAGIC.as_slice()) {
            return Err(TextIndexError::Format(FormatError::BadMagic { expected: "OEBM" }));
        }
        let mut cursor = ByteCursor::new(header.get(4..).unwrap_or_default());
        let version = u16::from_le_bytes(cursor.array()?);
        if version != BM25_VERSION {
            return Err(TextIndexError::Format(FormatError::UnsupportedVersion {
                field: "bm25 index version",
                found: u32::from(version),
            }));
        }
        let flags = u16::from_le_bytes(cursor.array()?);
        if flags & !FLAG_SEALED != 0 {
            return Err(TextIndexError::Format(FormatError::ReservedNotZero {
                field: "Bm25Index.flags",
            }));
        }
        let segment_id = u128::from_le_bytes(cursor.array()?);
        let content_hash: [u8; 32] = cursor.array()?;
        let opened;
        let body = if flags & FLAG_SEALED != 0 {
            let key = key.ok_or(TextIndexError::InvalidInput {
                rule: "bm25 index is sealed but no key was given".to_owned(),
            })?;
            let (nonce, sealed) = rest
                .split_at_checked(NONCE_LEN)
                .ok_or(malformed("bm25 sealed body truncated"))?;
            let data_key = derive_data_key(key, header);
            opened = zeroize::Zeroizing::new(
                AeadScheme::XChaCha20Poly1305
                    .open(&data_key, nonce, header, sealed)
                    .ok_or(TextIndexError::Authentication {
                        what: "bm25 sealed body",
                    })?,
            );
            opened.as_slice()
        } else {
            rest
        };
        Self::decode_body(segment_id, content_hash, body)
    }

    fn decode_body(segment_id: u128, content_hash: [u8; 32], body: &[u8]) -> Result<Self, TextIndexError> {
        let mut cursor = ByteCursor::new(body);
        let doc_count = u32::from_le_bytes(cursor.array()?);
        let total_length = u64::from_le_bytes(cursor.array()?);
        let term_count = u32::from_le_bytes(cursor.array()?);
        if doc_count == NO_DOC || doc_count as usize > body.len() {
            return Err(malformed("bm25 doc count out of range"));
        }
        let mut doc_lengths = Vec::with_capacity(doc_count as usize);
        for _ in 0..doc_count {
            doc_lengths.push(u32::try_from(cursor.varint()?).map_err(|_| malformed("bm25 doc length overflow"))?);
        }
        if doc_lengths.iter().map(|&length| u64::from(length)).sum::<u64>() != total_length {
            return Err(malformed("bm25 total length mismatch"));
        }
        let mut event_ids = Vec::with_capacity(doc_count as usize);
        for _ in 0..doc_count {
            let len = cursor.length()?;
            let bytes = cursor.take(len)?;
            let text = std::str::from_utf8(bytes).map_err(|_| malformed("bm25 event id not utf-8"))?;
            event_ids.push(text.to_owned());
        }
        if term_count as usize > cursor.remaining() {
            return Err(malformed("bm25 term count out of range"));
        }
        let mut terms: Vec<String> = Vec::with_capacity(term_count as usize);
        let mut previous: Vec<u8> = Vec::new();
        for position in 0..term_count as usize {
            let shared = cursor.length()?;
            let suffix_len = cursor.length()?;
            if (position % FRONT_CODING_RUN == 0 && shared != 0) || shared > previous.len() {
                return Err(malformed("bm25 term front coding"));
            }
            let mut term = previous.get(..shared).unwrap_or_default().to_vec();
            term.extend_from_slice(cursor.take(suffix_len)?);
            let text = String::from_utf8(term).map_err(|_| malformed("bm25 term not utf-8"))?;
            if terms.last().is_some_and(|last| last.as_str() >= text.as_str()) || text.is_empty() {
                return Err(malformed("bm25 terms not strictly sorted"));
            }
            previous = text.as_bytes().to_vec();
            terms.push(text);
        }
        let mut postings = Vec::with_capacity(terms.len());
        let mut offset = 0usize;
        for _ in 0..term_count {
            let doc_freq = cursor.u32()?;
            let max_tf = cursor.u32()?;
            let min_len = cursor.u32()?;
            let block_count = cursor.length()?;
            if block_count == 0 || block_count > cursor.remaining() {
                return Err(malformed("bm25 block count out of range"));
            }
            let mut blocks = Vec::with_capacity(block_count);
            let mut base = 0u32;
            for _ in 0..block_count {
                let count = cursor.u32()?;
                let span = cursor.u32()?;
                let last_doc = base.checked_add(span).ok_or(malformed("bm25 block doc overflow"))?;
                let block = BlockMeta {
                    count,
                    base,
                    last_doc,
                    max_tf: cursor.u32()?,
                    min_len: cursor.u32()?,
                    offset,
                    len: cursor.length()?,
                };
                offset = offset
                    .checked_add(block.len)
                    .ok_or(malformed("bm25 payload overflow"))?;
                blocks.push(block);
                base = last_doc;
            }
            postings.push(TermPostings {
                doc_freq,
                max_tf,
                min_len,
                blocks,
            });
        }
        let data_len = cursor.length()?;
        let data = cursor.take(data_len)?.to_vec();
        if cursor.remaining() != 0 || data.len() != offset {
            return Err(malformed("bm25 payload length mismatch"));
        }
        let index = Bm25Index {
            segment_id,
            content_hash,
            event_ids,
            doc_lengths,
            total_length,
            terms,
            postings,
            data,
        };
        index.validate_postings()?;
        Ok(index)
    }

    /// Checks every block decodes to strictly increasing in-range doc ids matching its metadata, so search never
    /// meets a malformed block.
    fn validate_postings(&self) -> Result<(), TextIndexError> {
        let mut docs = Vec::with_capacity(POSTING_BLOCK);
        let mut tfs = Vec::with_capacity(POSTING_BLOCK);
        for postings in &self.postings {
            let mut total = 0u64;
            let mut previous: Option<u32> = None;
            let mut max_tf = 0u32;
            let mut min_len = u32::MAX;
            for block in &postings.blocks {
                if block.count == 0 || block.count as usize > POSTING_BLOCK {
                    return Err(malformed("bm25 block size"));
                }
                self.decode_block(block, &mut docs, &mut tfs)
                    .ok_or(malformed("bm25 block payload"))?;
                let mut block_tf = 0u32;
                let mut block_len = u32::MAX;
                for (&doc, &tf) in docs.iter().zip(&tfs) {
                    if previous.is_some_and(|previous| doc <= previous) || doc >= self.doc_count() {
                        return Err(malformed("bm25 posting doc ids"));
                    }
                    previous = Some(doc);
                    block_tf = block_tf.max(tf);
                    block_len = block_len.min(self.doc_length(doc));
                }
                if docs.last() != Some(&block.last_doc) || block_tf != block.max_tf || block_len != block.min_len {
                    return Err(malformed("bm25 block metadata"));
                }
                max_tf = max_tf.max(block_tf);
                min_len = min_len.min(block_len);
                total += u64::from(block.count);
            }
            if total != u64::from(postings.doc_freq) || max_tf != postings.max_tf || min_len != postings.min_len {
                return Err(malformed("bm25 term metadata"));
            }
        }
        Ok(())
    }
}

fn derive_data_key(key: &WrappingKey, header: &[u8]) -> zeroize::Zeroizing<[u8; 32]> {
    let mut hasher = blake3::Hasher::new_derive_key(KEY_CONTEXT);
    hasher.update(key.bytes());
    hasher.update(header);
    zeroize::Zeroizing::new(*hasher.finalize().as_bytes())
}

fn seal_parameters(key: &WrappingKey, header: &[u8], body: &[u8]) -> (zeroize::Zeroizing<[u8; 32]>, [u8; NONCE_LEN]) {
    let data_key = derive_data_key(key, header);
    let nonce_key = blake3::derive_key(NONCE_CONTEXT, data_key.as_slice());
    let digest = blake3::keyed_hash(&nonce_key, body);
    let mut nonce = [0u8; NONCE_LEN];
    nonce
        .iter_mut()
        .zip(digest.as_bytes())
        .for_each(|(slot, byte)| *slot = *byte);
    (data_key, nonce)
}

fn trailer_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(TRAILER_CONTEXT);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn bit_width(max: u32) -> u8 {
    (32 - max.leading_zeros()) as u8
}

fn pack(values: &[u32], width: u8, out: &mut Vec<u8>) {
    if width == 0 {
        return;
    }
    let mut acc = 0u64;
    let mut bits = 0u32;
    for &value in values {
        acc |= u64::from(value) << bits;
        bits += u32::from(width);
        while bits >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            bits -= 8;
        }
    }
    if bits > 0 {
        out.push(acc as u8);
    }
}

/// Unpacks `count` values of `width` bits; returns the bytes consumed.
fn unpack(bytes: &[u8], count: usize, width: u8, out: &mut Vec<u32>) -> Option<usize> {
    if width > 32 {
        return None;
    }
    if width == 0 {
        out.extend(std::iter::repeat_n(0, count));
        return Some(0);
    }
    let used = count.checked_mul(usize::from(width))?.div_ceil(8);
    let mut source = bytes.get(..used)?.iter();
    let mask = (1u64 << width) - 1;
    let mut acc = 0u64;
    let mut bits = 0u32;
    for _ in 0..count {
        while bits < u32::from(width) {
            acc |= u64::from(*source.next()?) << bits;
            bits += 8;
        }
        out.push((acc & mask) as u32);
        acc >>= width;
        bits -= u32::from(width);
    }
    Some(used)
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn remaining(&self) -> usize {
        self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], TextIndexError> {
        let (head, tail) = self
            .bytes
            .split_at_checked(len)
            .ok_or(malformed("bm25 index truncated"))?;
        self.bytes = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], TextIndexError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn varint(&mut self) -> Result<u64, TextIndexError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let (&byte, rest) = self.bytes.split_first().ok_or(malformed("bm25 varint truncated"))?;
            self.bytes = rest;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(malformed("bm25 varint too long"))
    }

    fn u32(&mut self) -> Result<u32, TextIndexError> {
        u32::try_from(self.varint()?).map_err(|_| malformed("bm25 varint exceeds u32"))
    }

    fn length(&mut self) -> Result<usize, TextIndexError> {
        let value = usize::try_from(self.varint()?).map_err(|_| malformed("bm25 length overflow"))?;
        if value > self.bytes.len().saturating_mul(8).saturating_add(64) {
            return Err(malformed("bm25 length out of range"));
        }
        Ok(value)
    }
}
