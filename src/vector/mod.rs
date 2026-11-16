mod catalogue;
mod codec;
mod constant;
mod deletes;
mod error;
mod ivf;
mod kernel;
mod model;
mod quantize;
mod rabitq;
mod seal;
mod search;
mod segment;

pub use catalogue::{
    SegmentDeletesRef, SegmentIndexRef, VectorSegmentEntry, drop_segment_index, load_segment_deletes,
    load_segment_index, publish_segment_deletes, publish_segment_index, search_catalogued_segments,
    search_catalogued_segments_indexed, segment_deletes_object_key, segment_index_object_key,
};
pub use constant::{
    DEFAULT_OVERSAMPLE, EMBEDDING_COLUMN_ID, EVENT_ID_COLUMN, GRANULE_ROWS, RABITQ_EPSILON, ROOM_ID_COLUMN,
    SEGMENT_VERSION, SENDER_COLUMN, TIMESTAMP_COLUMN, VECTOR_SUBBLOCK_ROWS,
};
pub use deletes::SegmentDeletes;
pub use error::*;
pub use ivf::{PARTITION_INDEX_MAGIC, PARTITION_INDEX_VERSION, Partition, PartitionIndex, PartitionParams};
pub use kernel::{PreparedQuery, TopK, top_k};
pub use model::*;
pub use quantize::{decode_row, encode_row, u1_estimated_dot};
pub use seal::WrappingKey;
pub use search::*;
pub use segment::*;

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
