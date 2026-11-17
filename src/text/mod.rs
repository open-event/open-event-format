//! Full-text ranking: BM25 over per-segment inverted index artifacts.
//!
//! Like the vector partition index, a text index is a heavy, external, manifest-referenced artifact that can be
//! dropped and rebuilt; it is never needed to read the events themselves. Tokenisation is the one the text-token
//! pruning index uses (`indexes::text_token::tokenize`), so pruning and ranking agree on what a token is.

mod catalogue;
mod index;
mod search;

pub use catalogue::{TextIndexRef, load_text_index, publish_text_index, text_index_digest, text_index_object_key};
pub use index::{BM25_MAGIC, BM25_VERSION, Bm25Index, POSTING_BLOCK, TextDocument, TextIndexError};
pub use search::{Bm25Params, TextHit, TextSegment, query_terms, search_bm25, search_bm25_exhaustive};

#[cfg(test)]
#[path = "test.rs"]
mod tests;
