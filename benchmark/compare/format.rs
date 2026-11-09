
use crate::dataset::Dataset;
use crate::query::{Partial, Query};
use anyhow::Result;

pub trait Format {
    fn name(&self) -> &'static str;

    fn write(&self, dataset: &Dataset) -> Result<Vec<u8>>;

    fn open(&self, dataset: &Dataset, bytes: Vec<u8>) -> Result<Box<dyn Opened>>;
}

pub trait Opened {
    fn query(&self, query: &Query) -> Result<Partial>;

    fn redact(&self, _dataset: &Dataset, _rows: &[usize]) -> Result<Option<u64>> {
        Ok(None)
    }
}
