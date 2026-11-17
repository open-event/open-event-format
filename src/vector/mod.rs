mod catalogue;
mod codec;
mod constant;
mod deletes;
mod error;
mod fusion;
mod ivf;
mod kernel;
mod model;
mod multivector;
mod quantize;
mod rabitq;
mod seal;
mod search;
mod segment;

pub use catalogue::{
    SegmentDeletesRef, SegmentIndexRef, VectorSegmentEntry, drop_segment_index, drop_text_index,
    dropped_vector_object_keys, load_segment_deletes, load_segment_index, publish_segment_deletes,
    publish_segment_index, register_text_index, search_catalogued_segments, search_catalogued_segments_indexed,
    segment_deletes_object_key, segment_index_object_key, vector_object_keys,
};
pub use constant::{
    DEFAULT_OVERSAMPLE, DEFAULT_TOKEN_CANDIDATES, EMBEDDING_COLUMN_ID, EVENT_ID_COLUMN, GRANULE_ROWS,
    MAX_RANGE_RESULTS, RABITQ_EPSILON, RABITQ_MIN_DIMENSION, ROOM_ID_COLUMN, SEGMENT_VERSION,
    SEGMENT_VERSION_MULTIVECTOR, SENDER_COLUMN, TIMESTAMP_COLUMN, VECTOR_SUBBLOCK_ROWS,
};
pub use deletes::SegmentDeletes;
pub use error::*;
pub use fusion::{DEFAULT_RRF_K, FusedHit, RrfParams, hybrid_rrf, hybrid_search, reciprocal_rank_fusion};
pub use ivf::{PARTITION_INDEX_MAGIC, PARTITION_INDEX_VERSION, Partition, PartitionIndex, PartitionParams};
pub use kernel::{PreparedQuery, TopK, top_k};
pub use model::*;
pub use multivector::{
    MultivectorHit, MultivectorInput, MultivectorSearch, max_sim, merge_multivector_segments, read_live_documents,
    search_multivector_segments, search_multivector_segments_indexed, write_multivector_segment,
};
pub use quantize::{decode_row, encode_row, u1_estimated_dot};
pub use seal::WrappingKey;
pub use search::*;
pub use segment::*;

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
