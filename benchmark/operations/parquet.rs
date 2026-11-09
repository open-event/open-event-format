
use crate::api::{FormatUnderTest, Measured, OperationOutcome};
use crate::constant::*;
use crate::dataset::{Dataset, amended, identity_hash, without};
use crate::model::{BenchmarkRow, BenchmarkSpec, Operation};
use anyhow::{Context, Result, anyhow, bail};
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use memchr::memmem;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::statistics::Statistics;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

mod column {
    pub const OCCURRED_AT: usize = 4;
    pub const EVENT_TYPE: usize = 7;
    pub const ENTITY_ID_HASH_LOW: usize = 9;
    pub const ENTITY_ID: usize = 11;
    pub const NOTE: usize = 20;
    pub const CLIENT: usize = 21;
}

pub struct ParquetFormat<'a> {
    base: Bytes,
    base_path: PathBuf,
    dataset: &'a Dataset,
    next: Bytes,
    schema: Arc<Schema>,
    spec: BenchmarkSpec,
}

impl<'a> ParquetFormat<'a> {
    pub fn prepare(dataset: &'a Dataset, spec: &BenchmarkSpec, dir: &Path) -> Result<Self> {
        let schema = Arc::new(schema());
        let base_path = dir.join("base.parquet");
        let base = encode(&dataset.rows, &schema, spec)?;
        write_file(&base_path, &base)?;
        let next = encode(&amended(&dataset.rows, &dataset.workload.diff_ordinals), &schema, spec)?;
        Ok(Self {
            base: Bytes::from(base),
            base_path,
            dataset,
            next: Bytes::from(next),
            schema,
            spec: *spec,
        })
    }

    fn metadata(bytes: &Bytes) -> Result<ArrowReaderMetadata> {
        ArrowReaderMetadata::load(bytes, ArrowReaderOptions::default())
            .map_err(|error| anyhow!("reading the Parquet metadata: {error}"))
    }

    fn builder(&self, metadata: ArrowReaderMetadata) -> ParquetRecordBatchReaderBuilder<Bytes> {
        ParquetRecordBatchReaderBuilder::new_with_metadata(self.base.clone(), metadata)
    }

    fn scan(
        &self,
        metadata: &ArrowReaderMetadata,
        row_groups: &[usize],
        columns: &[usize],
    ) -> Result<(Vec<RecordBatch>, u64)> {
        let mask = ProjectionMask::roots(metadata.parquet_schema(), columns.iter().copied());
        let reader = self
            .builder(metadata.clone())
            .with_row_groups(row_groups.to_vec())
            .with_projection(mask)
            .build()
            .map_err(|error| anyhow!("building the Parquet reader: {error}"))?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|error| anyhow!("reading a Parquet batch: {error}"))?);
        }
        Ok((batches, chunk_bytes(metadata.metadata(), row_groups, columns)))
    }

    fn create_write(&self, scratch: &Path) -> Result<Measured> {
        let target = scratch.join("created.parquet");
        let started = Instant::now();
        let bytes = encode(&self.dataset.rows, &self.schema, &self.spec)?;
        write_file(&target, &bytes)?;
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: bytes.len() as u64,
                matched_rows: self.dataset.rows.len() as u64,
            },
        })
    }

    fn open_metadata(&self) -> Result<Measured> {
        let started = Instant::now();
        let mut file = File::open(&self.base_path).with_context(|| format!("opening {}", self.base_path.display()))?;
        let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default())
            .map_err(|error| anyhow!("reading the Parquet metadata: {error}"))?;
        let rows = metadata.metadata().file_metadata().num_rows() as u64;
        let elapsed = started.elapsed();
        file.seek(SeekFrom::End(-8)).context("seeking to the Parquet trailer")?;
        let mut trailer = [0u8; 8];
        file.read_exact(&mut trailer).context("reading the Parquet trailer")?;
        let footer_len = trailer
            .get(..4)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_le_bytes)
            .unwrap_or_default();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: u64::from(footer_len) + 8,
                matched_rows: rows,
            },
        })
    }

    fn read_all_columns(&self) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let all: Vec<usize> = (0..self.schema.fields().len()).collect();
        let groups: Vec<usize> = (0..metadata.metadata().num_row_groups()).collect();
        let started = Instant::now();
        let (batches, bytes) = self.scan(&metadata, &groups, &all)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
            },
        })
    }

    fn read_one_column(&self) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let groups: Vec<usize> = (0..metadata.metadata().num_row_groups()).collect();
        let started = Instant::now();
        let (batches, bytes) = self.scan(&metadata, &groups, &[column::OCCURRED_AT])?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
            },
        })
    }

    fn read_text_column(&self) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let groups: Vec<usize> = (0..metadata.metadata().num_row_groups()).collect();
        let started = Instant::now();
        let (batches, bytes) = self.scan(&metadata, &groups, &[column::NOTE])?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
            },
        })
    }

    fn read_payload_field(&self) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let ordinals = &self.dataset.workload.sample_ordinals;
        let groups: Vec<usize> = row_groups_covering(metadata.metadata(), ordinals);
        let started = Instant::now();
        let (batches, bytes) = self.scan(&metadata, &groups, &[column::CLIENT])?;
        let placement = placement(metadata.metadata(), &groups);
        let values = concat_strings(&batches)?;
        let mut found = 0u64;
        for ordinal in ordinals {
            let Some(offset) = placement.offset_of(*ordinal) else {
                continue;
            };
            if values.get(offset).and_then(Option::as_deref).is_some() {
                found += 1;
            }
        }
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: found,
            },
        })
    }

    fn point_lookup(&self) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let wanted = self.dataset.workload.entity_id.as_str();
        let wanted_hash = identity_hash(wanted);
        let started = Instant::now();
        let mut bytes = 0;
        let mut found = 0u64;
        for group in 0..metadata.metadata().num_row_groups() {
            let (batches, group_bytes) = self.scan(&metadata, &[group], &[column::ENTITY_ID_HASH_LOW])?;
            bytes += group_bytes;
            let mut candidates: Vec<usize> = Vec::new();
            let mut offset = 0usize;
            for batch in &batches {
                let hashes = column_as::<UInt64Array>(batch, 0, "entity id hash")?;
                for row in 0..hashes.len() {
                    if hashes.value(row) == wanted_hash {
                        candidates.push(offset + row);
                    }
                }
                offset += batch.num_rows();
            }
            if candidates.is_empty() {
                continue;
            }
            let (batches, group_bytes) = self.scan(&metadata, &[group], &[column::ENTITY_ID])?;
            bytes += group_bytes;
            let ids = concat_strings(&batches)?;
            found += candidates
                .iter()
                .filter(|row| ids.get(**row).and_then(Option::as_deref) == Some(wanted))
                .count() as u64;
            if found > 0 {
                break;
            }
        }
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: found,
            },
        })
    }

    fn range_search(&self) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let lo = self.dataset.workload.occurred_at_lo;
        let hi = self.dataset.workload.occurred_at_hi;
        let started = Instant::now();
        let survivors: Vec<usize> = (0..metadata.metadata().num_row_groups())
            .filter(|group| overlaps_i64(metadata.metadata(), *group, column::OCCURRED_AT, lo, hi))
            .collect();
        let (batches, bytes) = self.scan(&metadata, &survivors, &[column::OCCURRED_AT])?;
        let mut matched = 0u64;
        for batch in &batches {
            let values = column_as::<Int64Array>(batch, 0, "occurred at")?;
            matched += (0..values.len())
                .filter(|row| (lo..=hi).contains(&values.value(*row)))
                .count() as u64;
        }
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: matched,
            },
        })
    }

    fn value_search(&self) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let groups: Vec<usize> = (0..metadata.metadata().num_row_groups()).collect();
        let wanted = self.dataset.workload.event_type.as_str();
        let started = Instant::now();
        let (batches, bytes) = self.scan(&metadata, &groups, &[column::EVENT_TYPE])?;
        let mut matched = 0u64;
        for batch in &batches {
            let values = column_as::<StringArray>(batch, 0, "event type")?;
            matched += (0..values.len())
                .filter(|row| !values.is_null(*row) && values.value(*row) == wanted)
                .count() as u64;
        }
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: matched,
            },
        })
    }

    fn full_text_search(&self, needle: &str) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let groups: Vec<usize> = (0..metadata.metadata().num_row_groups()).collect();
        let needle = memmem::Finder::new(needle.as_bytes());
        let started = Instant::now();
        let (batches, bytes) = self.scan(&metadata, &groups, &[column::NOTE])?;
        let mut matched = 0u64;
        for batch in &batches {
            let values = column_as::<StringArray>(batch, 0, "note")?;
            matched += (0..values.len())
                .filter(|row| !values.is_null(*row) && needle.find(values.value(*row).as_bytes()).is_some())
                .count() as u64;
        }
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: matched,
            },
        })
    }

    fn update_rows(&self, scratch: &Path) -> Result<Measured> {
        let metadata = Self::metadata(&self.base)?;
        let all: Vec<usize> = (0..self.schema.fields().len()).collect();
        let groups: Vec<usize> = (0..metadata.metadata().num_row_groups()).collect();
        let ordinals = &self.dataset.workload.mutated_ordinals;
        let target = scratch.join("updated.parquet");
        let started = Instant::now();
        let (_, read_bytes) = self.scan(&metadata, &groups, &all)?;
        let rewritten = amended(&self.dataset.rows, ordinals);
        let bytes = encode(&rewritten, &self.schema, &self.spec)?;
        write_file(&target, &bytes)?;
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: read_bytes + bytes.len() as u64,
                matched_rows: ordinals.len() as u64,
            },
        })
    }

    fn delete_rows(&self, scratch: &Path) -> Result<Measured> {
        let ordinals = &self.dataset.workload.mutated_ordinals;
        let target = scratch.join("deleted.parquet");
        let started = Instant::now();
        let kept = without(&self.dataset.rows, ordinals);
        let bytes = encode(&kept, &self.schema, &self.spec)?;
        write_file(&target, &bytes)?;
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: bytes.len() as u64,
                matched_rows: (self.dataset.rows.len() - kept.len()) as u64,
            },
        })
    }

    fn diff_versions(&self) -> Result<Measured> {
        let base = Self::metadata(&self.base)?;
        let next = Self::metadata(&self.next)?;
        let started = Instant::now();
        let mut bytes = 0u64;
        let mut differing = 0u64;
        for group in 0..base.metadata().num_row_groups().min(next.metadata().num_row_groups()) {
            let before_chunks = base.metadata().row_group(group).columns();
            let after_chunks = next.metadata().row_group(group).columns();
            let mut changed_columns: Vec<usize> = Vec::new();
            for (index, (before, after)) in before_chunks.iter().zip(after_chunks.iter()).enumerate() {
                let before_bytes = slice_range(&self.base, before.byte_range())?;
                let after_bytes = slice_range(&self.next, after.byte_range())?;
                bytes += (before_bytes.len() + after_bytes.len()) as u64;
                if before_bytes != after_bytes {
                    changed_columns.push(index);
                }
            }
            if changed_columns.is_empty() {
                continue;
            }
            let before = read_groups(&self.base, &base, &[group], &changed_columns)?;
            let after = read_groups(&self.next, &next, &[group], &changed_columns)?;
            differing += differing_rows(&before, &after)?;
        }
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: differing,
            },
        })
    }
}

impl FormatUnderTest for ParquetFormat<'_> {
    fn name(&self) -> &'static str {
        "parquet"
    }

    fn file_bytes(&self) -> u64 {
        self.base.len() as u64
    }

    fn run(&self, operation: Operation, scratch: &Path) -> Result<Measured> {
        match operation {
            Operation::CreateWrite => self.create_write(scratch),
            Operation::DeleteRows => self.delete_rows(scratch),
            Operation::DiffVersions => self.diff_versions(),
            Operation::FullTextSearch => self.full_text_search(&self.dataset.workload.needle),
            Operation::FullTextSearchAbsent => self.full_text_search(&self.dataset.workload.absent_needle),
            Operation::FullTextSearchRare => self.full_text_search(&self.dataset.workload.rare_needle),
            Operation::OpenMetadata => self.open_metadata(),
            Operation::PointLookup => self.point_lookup(),
            Operation::ReadAllColumns => self.read_all_columns(),
            Operation::ReadOneColumn => self.read_one_column(),
            Operation::ReadPayloadField => self.read_payload_field(),
            Operation::ReadTextColumn => self.read_text_column(),
            Operation::RangeSearch => self.range_search(),
            Operation::UpdateRows => self.update_rows(scratch),
            Operation::ValueSearch => self.value_search(),
        }
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("epoch", DataType::UInt64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("stream_id", DataType::UInt64, false),
        Field::new("stream_sequence", DataType::UInt64, false),
        Field::new("occurred_at", DataType::Int64, false),
        Field::new("ingested_at", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("entity_type", DataType::Utf8, false),
        Field::new("entity_id_hash_low", DataType::UInt64, false),
        Field::new("event_id", DataType::FixedSizeBinary(16), false),
        Field::new("entity_id", DataType::Utf8, true),
        Field::new("actor_id", DataType::Utf8, true),
        Field::new("actor_id_hash_low", DataType::UInt64, false),
        Field::new("dedupe_hash_low", DataType::UInt64, false),
        Field::new("schema_version", DataType::UInt32, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new(FIELD_CHAR_COUNT, DataType::Int64, false),
        Field::new(FIELD_REACTIONS, DataType::Int64, true),
        Field::new(FIELD_KIND, DataType::Utf8, false),
        Field::new(FIELD_NOTE, DataType::Utf8, false),
        Field::new(FIELD_CLIENT, DataType::Utf8, false),
    ])
}

fn encode(rows: &[BenchmarkRow], schema: &Arc<Schema>, spec: &BenchmarkSpec) -> Result<Vec<u8>> {
    let level = ZstdLevel::try_new(ZSTD_LEVEL).map_err(|error| anyhow!("selecting the zstd level: {error}"))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(spec.rows_per_group))
        .build();
    let mut sink = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut sink, Arc::clone(schema), Some(properties))
        .map_err(|error| anyhow!("opening the Parquet writer: {error}"))?;
    for chunk in rows.chunks(spec.rows_per_group) {
        writer
            .write(&batch(chunk, schema)?)
            .map_err(|error| anyhow!("writing a Parquet batch: {error}"))?;
    }
    writer
        .close()
        .map_err(|error| anyhow!("closing the Parquet writer: {error}"))?;
    Ok(sink)
}

fn batch(rows: &[BenchmarkRow], schema: &Arc<Schema>) -> Result<RecordBatch> {
    let event_ids: Vec<[u8; 16]> = rows.iter().map(|row| row.event_id.to_be_bytes()).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|_| EPOCH))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.sequence))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|_| STREAM_ID))),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.stream_sequence),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.occurred_at_nanos),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.ingested_at_nanos),
        )),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| &row.source))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| &row.event_type))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| &row.entity_type))),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| identity_hash(&row.entity_id)),
        )),
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(event_ids.iter())
                .map_err(|error| anyhow!("building the event id column: {error}"))?,
        ),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.entity_id.as_str())),
        )),
        Arc::new(StringArray::from_iter(rows.iter().map(|row| row.actor_id.as_deref()))),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter()
                .map(|row| row.actor_id.as_deref().map(identity_hash).unwrap_or_default()),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| identity_hash(&row.note)),
        )),
        Arc::new(UInt32Array::from_iter_values(rows.iter().map(|_| 1u32))),
        Arc::new(UInt32Array::from_iter_values(rows.iter().map(|_| 0u32))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.char_count))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.reactions))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| &row.kind))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| &row.note))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| &row.client))),
    ];
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| anyhow!("building an Arrow batch: {error}"))
}

fn read_groups(
    bytes: &Bytes,
    metadata: &ArrowReaderMetadata,
    row_groups: &[usize],
    columns: &[usize],
) -> Result<Vec<RecordBatch>> {
    let mask = ProjectionMask::roots(metadata.parquet_schema(), columns.iter().copied());
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes.clone(), metadata.clone())
        .with_row_groups(row_groups.to_vec())
        .with_projection(mask)
        .build()
        .map_err(|error| anyhow!("building the Parquet reader: {error}"))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|error| anyhow!("reading a Parquet batch: {error}"))?);
    }
    Ok(batches)
}

fn chunk_bytes(metadata: &ParquetMetaData, row_groups: &[usize], columns: &[usize]) -> u64 {
    let mut total = 0u64;
    for group in row_groups {
        let Some(row_group) = metadata.row_groups().get(*group) else {
            continue;
        };
        for column in columns {
            if let Some(chunk) = row_group.columns().get(*column) {
                total += chunk.compressed_size().max(0) as u64;
            }
        }
    }
    total
}

fn overlaps_i64(metadata: &ParquetMetaData, group: usize, column: usize, lo: i64, hi: i64) -> bool {
    let Some(chunk) = metadata.row_groups().get(group).and_then(|g| g.columns().get(column)) else {
        return true;
    };
    let Some(Statistics::Int64(stats)) = chunk.statistics() else {
        return true;
    };
    match (stats.min_opt(), stats.max_opt()) {
        (Some(min), Some(max)) => *min <= hi && lo <= *max,
        _ => true,
    }
}

fn row_groups_covering(metadata: &ParquetMetaData, ordinals: &[u64]) -> Vec<usize> {
    let mut groups = Vec::new();
    let mut first = 0u64;
    for (index, group) in metadata.row_groups().iter().enumerate() {
        let last = first + group.num_rows().max(0) as u64;
        if ordinals.iter().any(|ordinal| (first..last).contains(ordinal)) {
            groups.push(index);
        }
        first = last;
    }
    groups
}

struct Placement {
    spans: Vec<(u64, u64, usize)>,
}

impl Placement {
    fn offset_of(&self, ordinal: u64) -> Option<usize> {
        self.spans
            .iter()
            .find(|(first, rows, _)| (*first..first + rows).contains(&ordinal))
            .map(|(first, _, offset)| offset + (ordinal - first) as usize)
    }
}

fn placement(metadata: &ParquetMetaData, groups: &[usize]) -> Placement {
    let mut spans = Vec::with_capacity(groups.len());
    let mut offset = 0usize;
    let mut first_row = 0u64;
    let mut cursor = 0usize;
    for (index, row_group) in metadata.row_groups().iter().enumerate() {
        let rows = row_group.num_rows().max(0) as u64;
        if groups.get(cursor) == Some(&index) {
            spans.push((first_row, rows, offset));
            offset += rows as usize;
            cursor += 1;
        }
        first_row += rows;
    }
    Placement { spans }
}

fn concat_strings(batches: &[RecordBatch]) -> Result<Vec<Option<String>>> {
    let mut values = Vec::new();
    for batch in batches {
        let array = column_as::<StringArray>(batch, 0, "string column")?;
        for row in 0..array.len() {
            values.push((!array.is_null(row)).then(|| array.value(row).to_owned()));
        }
    }
    Ok(values)
}

fn column_as<'a, T: 'static>(batch: &'a RecordBatch, index: usize, what: &str) -> Result<&'a T> {
    batch
        .columns()
        .get(index)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| anyhow!("the {what} column is not the type the benchmark expects"))
}

fn differing_rows(before: &[RecordBatch], after: &[RecordBatch]) -> Result<u64> {
    let column_count = before.first().map(|batch| batch.num_columns()).unwrap_or_default();
    let rows: usize = before.iter().map(RecordBatch::num_rows).sum();
    let mut changed = vec![false; rows];
    for index in 0..column_count {
        let left = concat_column(before, index)?;
        let right = concat_column(after, index)?;
        for (row, flag) in changed.iter_mut().enumerate() {
            *flag = *flag || left.get(row) != right.get(row);
        }
    }
    Ok(changed.iter().filter(|flag| **flag).count() as u64)
}

fn concat_column(batches: &[RecordBatch], index: usize) -> Result<Vec<Option<String>>> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .columns()
            .get(index)
            .ok_or_else(|| anyhow!("a batch is missing column {index}"))?;
        for row in 0..column.len() {
            if column.is_null(row) {
                values.push(None);
                continue;
            }
            values.push(Some(render(column.as_ref(), row)));
        }
    }
    Ok(values)
}

fn render(array: &dyn Array, row: usize) -> String {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return values.value(row).to_owned();
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return values.value(row).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return values.value(row).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return values.value(row).to_string();
    }
    if let Some(values) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return values.value(row).iter().map(|byte| format!("{byte:02x}")).collect();
    }
    format!("{:?}", array.slice(row, 1))
}

fn slice_range(bytes: &Bytes, range: (u64, u64)) -> Result<Bytes> {
    let (start, len) = range;
    let end = start.saturating_add(len) as usize;
    if end > bytes.len() {
        bail!("a Parquet column chunk range runs past the end of the file");
    }
    Ok(bytes.slice(start as usize..end))
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("flushing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
#[path = "test/parquet.rs"]
mod tests;
