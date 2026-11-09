mod catalogue;
mod constant;
mod deletes;
mod error;
mod kernel;
mod model;
mod quantize;
mod seal;
mod search;
mod segment;

pub use catalogue::{
    SegmentDeletesRef, VectorSegmentEntry, load_segment_deletes, publish_segment_deletes, search_catalogued_segments,
    segment_deletes_object_key,
};
pub use constant::{
    DEFAULT_OVERSAMPLE, EMBEDDING_COLUMN_ID, EVENT_ID_COLUMN, GRANULE_ROWS, ROOM_ID_COLUMN, SENDER_COLUMN,
    TIMESTAMP_COLUMN,
};
pub use deletes::SegmentDeletes;
pub use error::*;
pub use kernel::{PreparedQuery, TopK, top_k};
pub use model::*;
pub use quantize::{decode_row, encode_row, u1_estimated_dot};
pub use seal::WrappingKey;
pub use search::*;
pub use segment::*;

#[cfg(test)]
#[path = "test/mod.rs"]
mod tests;
