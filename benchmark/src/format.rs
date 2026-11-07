//! What every format under test provides: writing the dataset to bytes, opening those bytes, and answering queries.

use crate::dataset::Dataset;
use crate::query::{Partial, Query};
use anyhow::Result;

pub trait Format {
    fn name(&self) -> &'static str;

    /// Encodes the whole dataset into the format's file bytes.
    fn write(&self, dataset: &Dataset) -> Result<Vec<u8>>;

    /// Parses the file's metadata (footer, schema) so queries can run. Takes ownership so formats that read from a
    /// buffer need no copy during queries.
    fn open(&self, dataset: &Dataset, bytes: Vec<u8>) -> Result<Box<dyn Opened>>;
}

pub trait Opened {
    fn query(&self, query: &Query) -> Result<Partial>;
}
