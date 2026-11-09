
use crate::dataset::{Column, Dataset};
use crate::format::{Format, Opened};
use crate::query::{Col, Partial, Predicate, Query, fold_chunk, needed};
use anyhow::Result;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use bytes::Bytes;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;
use std::sync::Arc;

pub const ROW_GROUP_ROWS: usize = 131_072;

pub struct Parquet;

pub fn arrow_batch(dataset: &Dataset) -> RecordBatch {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (name, column) in &dataset.columns {
        match column {
            Column::F64(values) => {
                fields.push(Field::new(name, DataType::Float64, false));
                arrays.push(Arc::new(Float64Array::from(values.clone())));
            }
            Column::I64(values) => {
                fields.push(Field::new(name, DataType::Int64, false));
                arrays.push(Arc::new(Int64Array::from(values.clone())));
            }
            Column::Str(values) => {
                fields.push(Field::new(name, DataType::Utf8, false));
                arrays.push(Arc::new(StringArray::from_iter_values(values.iter())));
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("columns share a length")
}

impl Format for Parquet {
    fn name(&self) -> &'static str {
        "parquet"
    }

    fn write(&self, dataset: &Dataset) -> Result<Vec<u8>> {
        let batch = arrow_batch(dataset);
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(out)
    }

    fn open(&self, _dataset: &Dataset, bytes: Vec<u8>) -> Result<Box<dyn Opened>> {
        let bytes = Bytes::from(bytes);
        let metadata = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::new())?;
        Ok(Box::new(ParquetFile { bytes, metadata }))
    }
}

struct ParquetFile {
    bytes: Bytes,
    metadata: ArrowReaderMetadata,
}

impl ParquetFile {
    fn column_index(&self, name: &str) -> usize {
        self.metadata
            .schema()
            .index_of(name)
            .unwrap_or_else(|_| panic!("no column {name}"))
    }

    fn may_match(&self, group: usize, predicates: &[Predicate]) -> bool {
        let row_group = self.metadata.metadata().row_group(group);
        predicates.iter().all(|predicate| {
            let Some(stats) = row_group.column(self.column_index(predicate.column())).statistics() else {
                return true;
            };
            match (predicate, stats) {
                (Predicate::RangeI64 { low, high, .. }, Statistics::Int64(s)) => match (s.min_opt(), s.max_opt()) {
                    (Some(min), Some(max)) => *max >= *low && *min < *high,
                    _ => true,
                },
                (Predicate::GreaterI64 { value, .. }, Statistics::Int64(s)) => s.max_opt().is_none_or(|max| *max > *value),
                (Predicate::EqStr { value, .. }, Statistics::ByteArray(s)) => match (s.min_opt(), s.max_opt()) {
                    (Some(min), Some(max)) => min.data() <= value.as_bytes() && value.as_bytes() <= max.data(),
                    _ => true,
                },
                _ => true,
            }
        })
    }
}

impl Opened for ParquetFile {
    fn query(&self, query: &Query) -> Result<Partial> {
        let names = needed(query);
        let indexes: Vec<usize> = names.iter().map(|n| self.column_index(n)).collect();
        let mut total = Partial::default();
        for group in 0..self.metadata.metadata().num_row_groups() {
            if !self.may_match(group, &query.predicates) {
                continue;
            }
            let mask = ProjectionMask::roots(self.metadata.parquet_schema(), indexes.iter().copied());
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(self.bytes.clone(), self.metadata.clone())
                .with_row_groups(vec![group])
                .with_projection(mask)
                .with_batch_size(ROW_GROUP_ROWS)
                .build()?;
            for batch in reader {
                let batch = batch?;
                let schema = batch.schema();
                let columns: Vec<(&str, Col)> = names
                    .iter()
                    .map(|name| {
                        let array = batch.column(schema.index_of(name).expect("projected"));
                        let col = match array.data_type() {
                            DataType::Float64 => Col::F64(array.as_any().downcast_ref::<Float64Array>().expect("f64").values()),
                            DataType::Int64 => Col::I64(array.as_any().downcast_ref::<Int64Array>().expect("i64").values()),
                            _ => {
                                let strings = array.as_any().downcast_ref::<StringArray>().expect("utf8");
                                Col::Str((0..strings.len()).map(|i| strings.value(i)).collect())
                            }
                        };
                        (*name, col)
                    })
                    .collect();
                total.add(&fold_chunk(&columns, batch.num_rows(), query), query.last_n);
            }
        }
        Ok(total)
    }
}
