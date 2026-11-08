//! Answers every benchmarked operation using the Vortex file format.
//!
//! Each operation is written the way Vortex intends it: a scan carries a projection expression and a filter
//! expression, and the layout tree decides what it can rule out from its own statistics before any segment is
//! fetched. Where OE walks granules by hand and Parquet walks row groups by hand, the Vortex plan is one scan with
//! the predicate pushed into it — that is the format's own idea of how the question should be asked, so it is the
//! plan measured here.
//!
//! Two of Vortex's own capabilities show up directly. A scan takes explicit row indices, so the single-field payload
//! probe asks for exactly the rows it wants rather than decoding the blocks around them. And an update or a delete
//! rewrites the file, because a Vortex file — like a Parquet one — has no way to record that a row has changed or
//! gone.
//!
//! **Version.** The file format measured here is `vortex` 0.77.0, pulled in through the umbrella crate. This
//! benchmark is the only crate in the workspace that depends on Vortex: `oe` no longer embeds Vortex arrays
//! inside OE blocks, so nothing here is shared with the storage path.

use crate::api::{FormatUnderTest, Measured, OperationOutcome};
use crate::constant::*;
use crate::dataset::{Dataset, amended, identity_hash, without};
use crate::model::{BenchmarkRow, BenchmarkSpec, Operation};
use anyhow::{Context, Result, anyhow, bail};
use memchr::memmem;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use vortex::VortexSessionDefault;
use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
use vortex::array::arrays::{Struct, VarBinView};
use vortex::array::expr::{Expression, and, eq, get_item, gt_eq, lit, lt_eq, root};
use vortex::array::validity::Validity;
use vortex::array::{Array, ArrayEq, ArrayRef, Canonical, EqMode, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex::buffer::{Buffer, ByteBuffer};
use vortex::file::{OpenOptionsSessionExt, VortexFile, WriteOptionsSessionExt};
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::segments::{SegmentFuture, SegmentId, SegmentSource};

use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex::session::VortexSession;

/// The columns of the Vortex struct, mirroring the Parquet schema field for field so a scan of one file is a scan of
/// the other. The names are the projection and filter expressions' vocabulary, so they are named once here.
mod column {
    pub const OCCURRED_AT: &str = "occurred_at";
    pub const EVENT_TYPE: &str = "event_type";
    pub const ENTITY_ID_HASH_LOW: &str = "entity_id_hash_low";
    pub const ENTITY_ID: &str = "entity_id";
    pub const NOTE: &str = "note";
    pub const CLIENT: &str = "client";
}

/// Counts the bytes a scan pulls out of the file.
///
/// Vortex resolves a scan into requests for individual segments, so wrapping the file's own segment source and adding
/// up the segments it asks for gives the same "bytes this plan had to touch" figure the other two formats report from
/// their column directories. The wrapper only observes; every request is served by the source underneath.
struct CountingSegments {
    /// Segment ids already counted. A layout asks for the same segment once per split it appears in, and a byte
    /// fetched twice is still one byte of the file, so each id is counted once — the same rule the other two formats'
    /// block and chunk counts follow.
    counted: Mutex<HashSet<SegmentId>>,
    inner: Arc<dyn SegmentSource>,
    /// Byte length of each segment, indexed by segment id, from the file's footer.
    lengths: Vec<u64>,
    total: Arc<AtomicU64>,
}

impl SegmentSource for CountingSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let first_time = self
            .counted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id);
        if first_time {
            let length = self.lengths.get(*id as usize).copied().unwrap_or_default();
            self.total.fetch_add(length, Ordering::Relaxed);
        }
        self.inner.request(id)
    }
}

/// The Vortex side of the comparison: two encoded files over the same dataset.
pub struct VortexFormat<'a> {
    /// The encoded file, held in memory. All three formats read from memory so the comparison measures decoding
    /// rather than the host's page cache.
    base: ByteBuffer,
    dataset: &'a Dataset,
    /// The amended version the diff compares against.
    next: ByteBuffer,
    /// Drives the reader's and writer's async internals on this thread. The session holds only a weak handle to it,
    /// so it has to outlive every scan — which is why it is owned here rather than made per call.
    runtime: CurrentThreadRuntime,
    session: VortexSession,
    spec: BenchmarkSpec,
}

impl<'a> VortexFormat<'a> {
    /// Encodes the dataset and its amended version into `dir`.
    pub fn prepare(dataset: &'a Dataset, spec: &BenchmarkSpec, dir: &Path) -> Result<Self> {
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let base = encode(&dataset.rows, spec, &session, &runtime)?;
        write_file(&dir.join("base.vortex"), &base)?;
        let next = encode(
            &amended(&dataset.rows, &dataset.workload.diff_ordinals),
            spec,
            &session,
            &runtime,
        )?;
        Ok(Self {
            base: ByteBuffer::from(base),
            dataset,
            next: ByteBuffer::from(next),
            runtime,
            session,
            spec: *spec,
        })
    }

    /// Opens the file for one iteration, with a byte counter wrapped around its segment source.
    ///
    /// Every read operation starts from one of these, so no iteration inherits a segment the one before it decoded.
    fn reader(&self, bytes: &ByteBuffer) -> Result<(VortexFile, Arc<AtomicU64>)> {
        let file = self
            .session
            .open_options()
            .open_buffer(bytes.clone())
            .map_err(|error| anyhow!("opening the Vortex file: {error}"))?;
        let lengths: Vec<u64> = file
            .footer()
            .segment_map()
            .iter()
            .map(|segment| u64::from(segment.length))
            .collect();
        let total = Arc::new(AtomicU64::new(0));
        let counting = CountingSegments {
            counted: Mutex::new(HashSet::new()),
            inner: file.segment_source(),
            lengths,
            total: Arc::clone(&total),
        };
        let counted = VortexFile::new(file.footer().clone(), Arc::new(counting), self.session.clone());
        Ok((counted, total))
    }

    /// Runs one scan to completion, returning the chunks it produced and the bytes it fetched.
    fn scan(
        &self,
        file: &VortexFile,
        counter: &Arc<AtomicU64>,
        projection: Option<Expression>,
        filter: Option<Expression>,
        rows: Option<Buffer<u64>>,
    ) -> Result<(Vec<ArrayRef>, u64)> {
        let mut scan = file
            .scan()
            .map_err(|error| anyhow!("planning a Vortex scan: {error}"))?;
        let dtype = file.dtype();
        if let Some(projection) = projection {
            let projection = projection
                .bind(dtype)
                .map_err(|error| anyhow!("binding a Vortex projection: {error}"))?;
            scan = scan.with_projection(projection);
        }
        if let Some(filter) = filter {
            let filter = filter
                .bind(dtype)
                .map_err(|error| anyhow!("binding a Vortex filter: {error}"))?;
            scan = scan.with_filter(filter);
        }
        if let Some(rows) = rows {
            let rows = StrictSortedBuffer::try_new(rows)
                .map_err(|error| anyhow!("Vortex row indices must be strictly sorted: {error}"))?;
            scan = scan.with_row_indices(rows);
        }
        let iter = scan
            .into_array_iter(&self.runtime)
            .map_err(|error| anyhow!("building a Vortex scan iterator: {error}"))?;
        let mut chunks = Vec::new();
        for chunk in iter {
            chunks.push(chunk.map_err(|error| anyhow!("reading a Vortex chunk: {error}"))?);
        }
        Ok((chunks, counter.load(Ordering::Relaxed)))
    }

    fn create_write(&self, scratch: &Path) -> Result<Measured> {
        let target = scratch.join("created.vortex");
        let started = Instant::now();
        let bytes = encode(&self.dataset.rows, &self.spec, &self.session, &self.runtime)?;
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

    /// A cold open: parse the file's footer and read nothing else.
    ///
    /// Unlike the other two, this reads from the copy already in memory — Vortex's in-memory open takes the whole
    /// buffer and slices segments out of it, so there is no tail-only read to time. The parse is the comparable work;
    /// the disk read the other two do is page-cached after their first iteration anyway.
    fn open_metadata(&self) -> Result<Measured> {
        let started = Instant::now();
        let file = self
            .session
            .open_options()
            .open_buffer(self.base.clone())
            .map_err(|error| anyhow!("opening the Vortex file: {error}"))?;
        let rows = file.row_count();
        let elapsed = started.elapsed();
        // The trailing metadata region: whatever sits past the end of the last data segment.
        let data_end = file
            .footer()
            .segment_map()
            .iter()
            .map(|segment| segment.offset + u64::from(segment.length))
            .max()
            .unwrap_or_default();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: (self.base.len() as u64).saturating_sub(data_end),
                matched_rows: rows,
            },
        })
    }

    fn read_all_columns(&self) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, bytes) = self.scan(&file, &counter, None, None, None)?;
        decode(&chunks, &mut ctx)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            },
        })
    }

    fn read_one_column(&self) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, bytes) = self.scan(&file, &counter, Some(field(column::OCCURRED_AT)), None, None)?;
        decode(&chunks, &mut ctx)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            },
        })
    }

    fn read_text_column(&self) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, bytes) = self.scan(&file, &counter, Some(field(column::NOTE)), None, None)?;
        decode(&chunks, &mut ctx)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            },
        })
    }

    /// One payload field for a scattered sample of rows.
    ///
    /// Vortex takes the row indices directly, so the scan asks for exactly those rows rather than for the blocks that
    /// happen to contain them. This is the operation where that matters most.
    fn read_payload_field(&self) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let ordinals = Buffer::copy_from(&self.dataset.workload.sample_ordinals);
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, bytes) = self.scan(&file, &counter, Some(field(column::CLIENT)), None, Some(ordinals))?;
        decode(&chunks, &mut ctx)?;
        // The client column is non-nullable, so every row the scan returned carries a value.
        let found: u64 = chunks.iter().map(|chunk| chunk.len() as u64).sum();
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: found,
            },
        })
    }

    /// Finds the one row carrying an entity id: the hash equality is pushed into the scan, and the id column comes
    /// back for the rows that survive it so the hit can be confirmed.
    fn point_lookup(&self) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let wanted = self.dataset.workload.entity_id.as_str();
        let wanted_hash = identity_hash(wanted);
        let started = Instant::now();
        let (chunks, bytes) = self.scan(
            &file,
            &counter,
            Some(field(column::ENTITY_ID)),
            Some(eq(field(column::ENTITY_ID_HASH_LOW), lit(wanted_hash))),
            None,
        )?;
        let mut ctx = self.session.create_execution_ctx();
        let mut found = 0u64;
        for chunk in &chunks {
            let values = strings_of(chunk, &mut ctx)?;
            // A hash match is a candidate, not an answer: confirm against the id itself before counting the row.
            found += (0..values.len())
                .filter(|row| values.bytes_at(*row).as_slice() == wanted.as_bytes())
                .count() as u64;
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

    /// Counts rows inside a time window, with the window pushed into the scan so the layout tree can rule blocks out
    /// on its own statistics before fetching them.
    fn range_search(&self) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let lo = self.dataset.workload.occurred_at_lo;
        let hi = self.dataset.workload.occurred_at_hi;
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, bytes) = self.scan(
            &file,
            &counter,
            Some(field(column::OCCURRED_AT)),
            Some(and(
                gt_eq(field(column::OCCURRED_AT), lit(lo)),
                lt_eq(field(column::OCCURRED_AT), lit(hi)),
            )),
            None,
        )?;
        decode(&chunks, &mut ctx)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            },
        })
    }

    /// Counts rows whose event type equals one value, with the equality pushed into the scan.
    fn value_search(&self) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let wanted = self.dataset.workload.event_type.clone();
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, bytes) = self.scan(
            &file,
            &counter,
            Some(field(column::EVENT_TYPE)),
            Some(eq(field(column::EVENT_TYPE), lit(wanted))),
            None,
        )?;
        decode(&chunks, &mut ctx)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            },
        })
    }

    /// Counts rows whose free-text field contains a substring. No expression matches a substring, so the column comes
    /// back and the match is made here — the same full scan the other two formats do.
    fn full_text_search(&self, needle: &str) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        // Built once, outside the timing, so all three sides measure the same search over the same needle.
        let needle = memmem::Finder::new(needle.as_bytes());
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, bytes) = self.scan(&file, &counter, Some(field(column::NOTE)), None, None)?;
        let mut matched = 0u64;
        for chunk in &chunks {
            let values = strings_of(chunk, &mut ctx)?;
            for row in 0..values.len() {
                if needle.find(values.bytes_at(row).as_slice()).is_some() {
                    matched += 1;
                }
            }
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

    /// Changes a slice of rows. A Vortex file cannot record that a row has changed, so an update decodes the file,
    /// applies the change, and encodes a new one.
    fn update_rows(&self, scratch: &Path) -> Result<Measured> {
        let (file, counter) = self.reader(&self.base)?;
        let ordinals = &self.dataset.workload.mutated_ordinals;
        let target = scratch.join("updated.vortex");
        let mut ctx = self.session.create_execution_ctx();
        let started = Instant::now();
        let (chunks, read_bytes) = self.scan(&file, &counter, None, None, None)?;
        decode(&chunks, &mut ctx)?;
        let rewritten = amended(&self.dataset.rows, ordinals);
        let bytes = encode(&rewritten, &self.spec, &self.session, &self.runtime)?;
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

    /// Removes a slice of rows. A Vortex file cannot record that a row is gone, so the whole file is rewritten
    /// without them.
    fn delete_rows(&self, scratch: &Path) -> Result<Measured> {
        let ordinals = &self.dataset.workload.mutated_ordinals;
        let target = scratch.join("deleted.vortex");
        let started = Instant::now();
        let kept = without(&self.dataset.rows, ordinals);
        let bytes = encode(&kept, &self.spec, &self.session, &self.runtime)?;
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

    /// Reports how many rows differ between the two versions.
    ///
    /// Vortex records statistics per layout block but no content digest, and its segments carry no row alignment a
    /// caller outside the format can use, so an exact comparison has to read both files and compare values. That is
    /// the same position Parquet is in, minus Parquet's row-group-aligned chunk ranges.
    fn diff_versions(&self) -> Result<Measured> {
        let (base, base_counter) = self.reader(&self.base)?;
        let (next, next_counter) = self.reader(&self.next)?;
        let started = Instant::now();
        let (before, before_bytes) = self.scan(&base, &base_counter, None, None, None)?;
        let (after, after_bytes) = self.scan(&next, &next_counter, None, None, None)?;
        let mut ctx = self.session.create_execution_ctx();
        let differing = differing_rows(&before, &after, &mut ctx)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: before_bytes + after_bytes,
                matched_rows: differing,
            },
        })
    }
}

impl FormatUnderTest for VortexFormat<'_> {
    fn name(&self) -> &'static str {
        "vortex"
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

/// The projection or filter expression naming one top-level column.
fn field(name: &str) -> Expression {
    get_item(name, root())
}

/// One scanned chunk's string column, executed into its canonical form.
///
/// The execution context is threaded in from the caller rather than made per call: building one per chunk is the
/// deprecated convenience path, and paying for it inside a timed loop would be measuring the benchmark, not Vortex.
fn strings_of(chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> Result<Array<VarBinView>> {
    chunk
        .clone()
        .execute::<Array<VarBinView>>(ctx)
        .map_err(|error| anyhow!("executing a Vortex string column: {error}"))
}

/// Forces every column of every scanned chunk into its canonical form.
///
/// A scan hands its chunks back still encoded — `fastlanes.for`, `vortex.fsst`, `vortex.dict` — so a caller that only
/// counts the rows never pays the materialization the OE and Parquet sides pay to answer the same question. Decoding
/// here is what keeps the timings comparable. A struct chunk is decoded field by field, because the canonical form of
/// a struct leaves its fields encoded.
fn decode(chunks: &[ArrayRef], ctx: &mut ExecutionCtx) -> Result<()> {
    for chunk in chunks {
        if chunk.dtype().is_struct() {
            for column in columns_of(chunk, ctx)?.iter() {
                canonicalize(column, ctx)?;
            }
        } else {
            canonicalize(chunk, ctx)?;
        }
    }
    Ok(())
}

/// One scanned array, executed into its canonical form.
fn canonicalize(array: &ArrayRef, ctx: &mut ExecutionCtx) -> Result<()> {
    array
        .clone()
        .execute::<Canonical>(ctx)
        .map(|_| ())
        .map_err(|error| anyhow!("canonicalizing a Vortex column: {error}"))
}

/// Encodes the dataset as one Vortex file, one chunk per group so the file's blocks line up with the other two
/// formats' granules and row groups.
fn encode(
    rows: &[BenchmarkRow],
    spec: &BenchmarkSpec,
    session: &VortexSession,
    runtime: &CurrentThreadRuntime,
) -> Result<Vec<u8>> {
    let chunks: Vec<ArrayRef> = rows
        .chunks(spec.rows_per_group)
        .map(chunk)
        .collect::<Result<Vec<_>>>()?;
    let dtype = chunks
        .first()
        .ok_or_else(|| anyhow!("a Vortex file needs at least one chunk"))?
        .dtype()
        .clone();
    let chunked = vortex::array::arrays::ChunkedArray::try_new(chunks, dtype)
        .map_err(|error| anyhow!("assembling the Vortex chunks: {error}"))?
        .into_array();
    let mut sink = Vec::new();
    session
        .write_options()
        .blocking(runtime)
        .write(&mut sink, chunked.to_array_iterator())
        .map_err(|error| anyhow!("writing the Vortex file: {error}"))?;
    Ok(sink)
}

/// Turns a run of generated rows into one Vortex struct array.
fn chunk(rows: &[BenchmarkRow]) -> Result<ArrayRef> {
    let event_ids: Vec<[u8; 16]> = rows.iter().map(|row| row.event_id.to_be_bytes()).collect();
    let fields: Vec<(&str, ArrayRef)> = vec![
        ("epoch", u64s(rows.iter().map(|_| EPOCH))),
        ("sequence", u64s(rows.iter().map(|row| row.sequence))),
        ("stream_id", u64s(rows.iter().map(|_| STREAM_ID))),
        ("stream_sequence", u64s(rows.iter().map(|row| row.stream_sequence))),
        (column::OCCURRED_AT, i64s(rows.iter().map(|row| row.occurred_at_nanos))),
        ("ingested_at", i64s(rows.iter().map(|row| row.ingested_at_nanos))),
        ("source", strings(rows.iter().map(|row| row.source.as_str()))),
        (
            column::EVENT_TYPE,
            strings(rows.iter().map(|row| row.event_type.as_str())),
        ),
        ("entity_type", strings(rows.iter().map(|row| row.entity_type.as_str()))),
        (
            column::ENTITY_ID_HASH_LOW,
            u64s(rows.iter().map(|row| identity_hash(&row.entity_id))),
        ),
        (
            "event_id",
            VarBinViewArray::from_iter_bin(event_ids.iter()).into_array(),
        ),
        (
            column::ENTITY_ID,
            strings(rows.iter().map(|row| row.entity_id.as_str())),
        ),
        (
            "actor_id",
            VarBinViewArray::from_iter_nullable_str(rows.iter().map(|row| row.actor_id.as_deref())).into_array(),
        ),
        (
            "actor_id_hash_low",
            u64s(
                rows.iter()
                    .map(|row| row.actor_id.as_deref().map(identity_hash).unwrap_or_default()),
            ),
        ),
        ("dedupe_hash_low", u64s(rows.iter().map(|row| identity_hash(&row.note)))),
        ("schema_version", u32s(rows.iter().map(|_| 1u32))),
        ("flags", u32s(rows.iter().map(|_| 0u32))),
        (FIELD_CHAR_COUNT, i64s(rows.iter().map(|row| row.char_count))),
        (
            FIELD_REACTIONS,
            PrimitiveArray::from_option_iter(rows.iter().map(|row| row.reactions)).into_array(),
        ),
        (FIELD_KIND, strings(rows.iter().map(|row| row.kind.as_str()))),
        (column::NOTE, strings(rows.iter().map(|row| row.note.as_str()))),
        (column::CLIENT, strings(rows.iter().map(|row| row.client.as_str()))),
    ];
    Ok(StructArray::from_fields(&fields)
        .map_err(|error| anyhow!("building a Vortex struct array: {error}"))?
        .into_array())
}

fn u64s(values: impl Iterator<Item = u64>) -> ArrayRef {
    primitives(values)
}

fn i64s(values: impl Iterator<Item = i64>) -> ArrayRef {
    primitives(values)
}

fn u32s(values: impl Iterator<Item = u32>) -> ArrayRef {
    primitives(values)
}

/// A non-nullable primitive column from an iterator of values.
fn primitives<T: vortex::array::dtype::NativePType>(values: impl Iterator<Item = T>) -> ArrayRef {
    let collected: Vec<T> = values.collect();
    PrimitiveArray::new(Buffer::copy_from(&collected), Validity::NonNullable).into_array()
}

fn strings<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    VarBinViewArray::from_iter_str(values).into_array()
}

/// Counts the row positions at which two runs of chunks, holding the same columns, disagree.
///
/// Whole columns are compared first: `array_eq` settles a column that did not move in one call, and most columns in
/// most chunks did not move, so almost nothing reaches the per-row loop. Only a column that differs is walked row by
/// row, and only until every row it covers is already known to differ.
fn differing_rows(before: &[ArrayRef], after: &[ArrayRef], ctx: &mut ExecutionCtx) -> Result<u64> {
    if before.len() != after.len() {
        bail!("the two Vortex files came back in different chunk counts, so a row-for-row diff is meaningless");
    }
    let mut differing = 0u64;
    for (before, after) in before.iter().zip(after.iter()) {
        if before.len() != after.len() {
            bail!("two Vortex chunks hold different row counts, so a row-for-row diff is meaningless");
        }
        let mut changed = vec![false; before.len()];
        for (left, right) in columns_of(before, ctx)?.iter().zip(columns_of(after, ctx)?.iter()) {
            if left.array_eq(right, EqMode::Value) {
                continue;
            }
            for (row, flag) in changed.iter_mut().enumerate() {
                if *flag {
                    continue;
                }
                *flag = left
                    .execute_scalar(row, ctx)
                    .map_err(|error| anyhow!("reading row {row} of a Vortex column: {error}"))?
                    != right
                        .execute_scalar(row, ctx)
                        .map_err(|error| anyhow!("reading row {row} of a Vortex column: {error}"))?;
            }
        }
        differing += changed.iter().filter(|flag| **flag).count() as u64;
    }
    Ok(differing)
}

/// One struct chunk's columns, executed into their canonical form.
fn columns_of(chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> Result<Arc<[ArrayRef]>> {
    Ok(chunk
        .clone()
        .execute::<Array<Struct>>(ctx)
        .map_err(|error| anyhow!("executing a Vortex chunk as a struct: {error}"))?
        .into_data_parts()
        .fields
        .into())
}

/// Writes `bytes` to `path` and waits for them to land, so a write's cost includes making it durable.
fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("flushing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
#[path = "test/vortex.rs"]
mod tests;
