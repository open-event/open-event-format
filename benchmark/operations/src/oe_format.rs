//! Answers every benchmarked operation using the OE storage engine.
//!
//! Each operation is written the way the engine intends it: pruning comes from the granule directory in the footer,
//! a column block is fetched through its mark as one range read and decoded on its own, a payload field is pulled for
//! a single row without touching its siblings, a delete is a row-position bitmap written beside the file rather than
//! a rewrite of it, and a diff starts from the per-stripe content hashes the footer already carries.
//!
//! Nothing here reaches past the engine's public interface, so what is measured is what a caller of the `oe` crate
//! would get.
//!
//! See: oe-file-layout/spec.md

use crate::api::{FormatUnderTest, Measured, OperationOutcome, PhaseElapsed};
use crate::constant::*;
use crate::dataset::{Dataset, amended, identity_hash};
use crate::model::{BenchmarkRow, BenchmarkSpec, Operation, WriterPhase};
use anyhow::{Context, Result, anyhow, bail, ensure};
use oe::columns::{FreetextDeclaration, PromotedColumn, PromotionPlan, column_ids};
use oe::encoding::predicate::{
    ContainsMatcher, SharedDictionaryPredicate, StringPredicate, count_string_block_shared, scan_in_parallel,
    translate_for_shared_dictionary,
};
use oe::encoding::{ColumnData, decode_block_range_shared, decode_block_shared};
use oe::events::variant::VariantValue;
use oe::events::{EventFlags, EventId, StreamId, TenantId, TimestampValue};
use oe::indexes::bitmap::RoaringRangeBitmap;
use oe::invariants::io::ThreadPoolEncodeExecutor;
use oe::layout::footer::{ColumnKind, Footer, GranuleEntry};
use oe::layout::reader::{OeFile, OeFooter, PayloadRef, ScanBatch};
use oe::layout::{LayoutTargets, Profile};
use oe::security::FooterEncryption;
use oe::typed_id::TypedIdTestExt;
use oe::writer::build::{
    BuildEnvelope, BuildLifecycle, BuildPayload, BuildPhase, BuildRow, FieldName, OeBuildConfig, OeRowChange,
    SharedStrings, StreamedOe, build_oe_file_streamed, build_oe_file_streamed_profiled, build_oe_file_with_executor,
    update_oe_file_from_changes_profiled_with_executor, update_oe_file_from_changes_with_executor,
    update_oe_file_from_source_changes_profiled_with_executor,
};
use rayon::prelude::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Instant;

/// The schema version every generated row declares. One value for the whole file, so the promoted column resolves for
/// every row rather than falling back to the payload for the older ones.
const SCHEMA_VERSION: u32 = 1;

/// Where a stripe is cut, in estimated uncompressed bytes. Small enough that the default dataset spans several
/// stripes, which is what gives the diff's per-stripe content hashes something to rule out.
const STRIPE_TARGET_BYTES: usize = 4 * 1024 * 1024;

/// The ids of the payload columns the benchmark reads, looked up by name once the file exists.
///
/// Promoted, shredded, and free-text columns are numbered by the writer from its own bases, so the benchmark asks the
/// footer which id each field landed on instead of assuming one.
struct ColumnIds {
    client: u32,
    note: u32,
}

impl ColumnIds {
    fn resolve(footer: &Footer) -> Result<Self> {
        let note = footer
            .freetext
            .iter()
            .find(|entry| entry.declared_field == FIELD_NOTE)
            .map(|entry| entry.column_id)
            .ok_or_else(|| anyhow!("the writer did not give the declared free-text field `{FIELD_NOTE}` a column"))?;
        let client = footer
            .shredded
            .iter()
            .find(|entry| entry.path == FIELD_CLIENT)
            .map(|entry| entry.column_id)
            .ok_or_else(|| anyhow!("the writer did not shred the `{FIELD_CLIENT}` payload path into a column"))?;
        Ok(Self { client, note })
    }
}

/// What one full-text search needs in every granule it scans: the file, the free-text column, and the needle's
/// predicate in both its plain and dictionary-translated forms.
struct FullTextScan<'a> {
    alphabet: Option<&'a [String]>,
    column_id: u32,
    file: &'a OeFile,
    predicate: StringPredicate,
    translated: Option<SharedDictionaryPredicate>,
}

/// The OE side of the comparison: two published files over the same dataset, plus the ids of the columns the
/// benchmark reads.
///
/// Each operation's per-granule work is a named `#[inline(never)]` function (`<operation>_granule`), so a sampling
/// profiler attributes every sample — including those taken on Rayon workers, which do not keep the caller's stack —
/// to the operation it belongs to.
pub struct OeFormat<'a> {
    /// The published file, held in memory. Every format reads from memory so the comparison measures decoding rather
    /// than the host's page cache.
    base: Option<oe::writer::build::BuiltOe>,
    base_path: PathBuf,
    file_bytes: AtomicU64,
    columns: ColumnIds,
    dataset: &'a Dataset,
    /// The lifecycle every OE file of this run is built under. `FreshPublication` unless a `OE_BENCH_*` control
    /// selected another; resolved once in `prepare_selected`, never inside a timed operation.
    lifecycle: BuildLifecycle,
    /// Byte length of the base file's complete cold-open tail: the footer plus its Bao-style proof appendix.
    tail_len: u64,
    /// The amended version the diff compares against, opened and verified once.
    next: Option<OeFile>,
    spec: BenchmarkSpec,
    /// The base file opened and verified once; every iteration's reader is a fresh reader over it.
    template: Option<OeFile>,
    /// `OE_BENCH_WARM_READER` was set: every iteration runs against `template` itself, so whatever one iteration
    /// decoded and cached stays warm for the next — the repeated-query shape the per-iteration fresh reader
    /// deliberately avoids.
    warm_reader: bool,
}

/// The reader one iteration runs against: made for that iteration alone, or the one reader every iteration shares
/// when `OE_BENCH_WARM_READER` is set.
enum IterationReader<'a> {
    Fresh(OeFile),
    Warm(&'a OeFile),
}

impl Deref for IterationReader<'_> {
    type Target = OeFile;

    fn deref(&self) -> &OeFile {
        match self {
            Self::Fresh(file) => file,
            Self::Warm(file) => file,
        }
    }
}

impl<'a> OeFormat<'a> {
    /// Publishes the dataset and its amended version into `dir` and reads back what the benchmark needs to address
    /// them.
    pub fn prepare(dataset: &'a Dataset, spec: &BenchmarkSpec, dir: &Path) -> Result<Self> {
        Self::prepare_selected(dataset, spec, dir, crate::model::OPERATIONS)
    }

    /// Prepares only the artifacts the selected operations consume. In particular, an isolated create/write run does
    /// not build an untimed base or amended OE file before the profiled iteration begins.
    pub fn prepare_selected(
        dataset: &'a Dataset,
        spec: &BenchmarkSpec,
        dir: &Path,
        operations: &[Operation],
    ) -> Result<Self> {
        let base_path = dir.join("base.oe");
        let lifecycle = benchmark_lifecycle()?;
        let needs_base = operations.iter().any(|operation| *operation != Operation::CreateWrite);
        let needs_next = operations.contains(&Operation::DiffVersions);
        let built = needs_base.then(|| build(&dataset.rows, spec, lifecycle)).transpose()?;
        if let Some(built) = &built {
            write_file(&base_path, &built.bytes)?;
        }
        let next = if needs_next {
            let amended_rows = amended(&dataset.rows, &dataset.workload.diff_ordinals);
            let bytes = build(&amended_rows, spec, lifecycle)?.bytes;
            Some(OeFile::open(bytes, None).map_err(|error| anyhow!("opening the amended OE file: {error}"))?)
        } else {
            None
        };

        // Granule cuts must land on the same rows in both versions, or the diff would be comparing blocks that hold
        // different rows and its answer would be meaningless. The writer cuts a granule on whichever of the row
        // target and the payload-byte target trips first, so check the row target is the one that did.
        if let Some(built) = &built {
            for granule in built
                .footer
                .granules
                .iter()
                .take(built.footer.granules.len().saturating_sub(1))
            {
                ensure!(
                    granule.row_count as usize == spec.rows_per_group,
                    "granule {} holds {} rows, not the {} the spec asked for: the payload-byte target cut it early, so \
                     the two versions no longer align row for row",
                    granule.granule_id,
                    granule.row_count,
                    spec.rows_per_group
                );
            }
        }

        let columns = built
            .as_ref()
            .map(|built| ColumnIds::resolve(&built.footer))
            .transpose()?;
        let tail_len = built
            .as_ref()
            .map(|built| built.footer_len.saturating_add(built.tree_len.unwrap_or(0)))
            .unwrap_or_default();
        let template = built
            .as_ref()
            .map(|built| OeFile::open(built.bytes.clone(), None))
            .transpose()
            .map_err(|error| anyhow!("opening the OE file: {error}"))?;
        Ok(Self {
            file_bytes: AtomicU64::new(built.as_ref().map_or(0, |built| built.bytes.len()) as u64),
            base: built,
            base_path,
            columns: columns.unwrap_or(ColumnIds { client: 0, note: 0 }),
            dataset,
            lifecycle,
            tail_len,
            next,
            spec: *spec,
            template,
            warm_reader: std::env::var_os("OE_BENCH_WARM_READER").is_some(),
        })
    }

    /// Opens the base file for one iteration: a fresh reader over the template verified in `prepare`, so no
    /// iteration inherits a decoded block from the one before it and none re-hashes the file's stripes. Under
    /// `OE_BENCH_WARM_READER` it is the template itself, so every iteration inherits everything the last one
    /// decoded and cached.
    fn reader(&self) -> Result<IterationReader<'_>> {
        let template = self
            .template
            .as_ref()
            .ok_or_else(|| anyhow!("the OE base file was not prepared"))?;
        if self.warm_reader {
            return Ok(IterationReader::Warm(template));
        }
        template
            .fresh_reader()
            .map(IterationReader::Fresh)
            .map_err(|error| anyhow!("opening the OE file: {error}"))
    }

    fn next_reader(&self) -> Result<OeFile> {
        let next = self
            .next
            .as_ref()
            .ok_or_else(|| anyhow!("the amended OE file was not prepared"))?;
        next.fresh_reader()
            .map_err(|error| anyhow!("opening the amended OE file: {error}"))
    }

    /// Reads and decodes one `(column, granule)` block, reporting the file bytes it cost.
    ///
    /// The raw read is the engine's own range read through the block's mark; decoding it separately is what lets the
    /// byte count be the block's real on-disk size rather than an estimate.
    fn block(file: &OeFile, column_id: u32, granule_id: u32) -> Result<(ColumnData, u64)> {
        let Ok(raw) = file.read_column_raw(column_id, granule_id) else {
            // A constant block is elided to a zero-length mark and answered from the block's own stats: it costs no
            // file bytes and has nothing to hand out raw, so the materializing decoder rebuilds it from those stats.
            let read = file
                .read_column(column_id, granule_id)
                .map_err(|error| anyhow!("reading column {column_id} of granule {granule_id}: {error}"))?;
            return Ok((read.data, 0));
        };
        let bytes = (raw.body.len() + raw.presence.len()) as u64;
        let data = decode_block_shared(raw.pipeline, raw.body, file.shared_alphabet(column_id))
            .map_err(|error| anyhow!("decoding column {column_id} of granule {granule_id}: {error}"))?;
        Ok((data, bytes))
    }

    /// Reads one column across every granule, decoding each block through `block`, the operation's own named
    /// per-granule function.
    fn scan_column(
        file: &OeFile,
        block: fn(&OeFile, u32) -> Result<(ColumnData, u64)>,
    ) -> Result<(Vec<ColumnData>, u64)> {
        let mut bytes = 0;
        let mut blocks = Vec::with_capacity(file.footer().granules.len());
        for granule in &file.footer().granules {
            let (data, block_bytes) = block(file, granule.granule_id)?;
            bytes += block_bytes;
            blocks.push(data);
        }
        Ok((blocks, bytes))
    }

    /// Converting the generated rows into the writer's input shape is harness work, so it is timed apart from the
    /// build and reported as `input_conversion` rather than charged to OE.
    fn create_write(&self, scratch: &Path) -> Result<Measured> {
        let target = scratch.join("created.oe");
        let tenant_id = TenantId::new_test_id(TENANT_SEED);
        let conversion_started = Instant::now();
        let oe_rows = oe_rows(&self.dataset.rows, tenant_id);
        let input_conversion = conversion_started.elapsed();
        let started = Instant::now();
        let streamed = encode_streamed(oe_rows, &self.spec, tenant_id, self.lifecycle, &target)?;
        self.file_bytes.store(streamed.total_len, Ordering::Relaxed);
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: Some(input_conversion),
            outcome: OperationOutcome {
                bytes: streamed.total_len,
                matched_rows: self.dataset.rows.len() as u64,
            },
        })
    }

    fn create_write_with_phases(&self, scratch: &Path) -> Result<(Measured, Vec<PhaseElapsed>)> {
        let target = scratch.join("created.oe");
        let tenant_id = TenantId::new_test_id(TENANT_SEED);
        let conversion_started = Instant::now();
        let oe_rows = oe_rows(&self.dataset.rows, tenant_id);
        let input_conversion = conversion_started.elapsed();
        let started = Instant::now();
        let (streamed, mut phases) = encode_streamed_profiled(oe_rows, &self.spec, tenant_id, self.lifecycle, &target)?;
        self.file_bytes.store(streamed.total_len, Ordering::Relaxed);
        phases.push(PhaseElapsed {
            elapsed: input_conversion,
            phase: WriterPhase::InputConversion,
        });
        Ok((
            Measured {
                elapsed: started.elapsed(),
                input_conversion: Some(input_conversion),
                outcome: OperationOutcome {
                    bytes: streamed.total_len,
                    matched_rows: self.dataset.rows.len() as u64,
                },
            },
            phases,
        ))
    }

    /// A cold open: one range read of the file's tail, and nothing else. This is the request a reader makes before it
    /// knows anything about the file, so it reads from the file on disk rather than from the copy in memory.
    fn open_metadata(&self) -> Result<Measured> {
        let started = Instant::now();
        let mut file = File::open(&self.base_path).with_context(|| format!("opening {}", self.base_path.display()))?;
        file.seek(SeekFrom::End(-(self.tail_len as i64)))
            .context("seeking to the OE footer")?;
        let mut tail = vec![0u8; self.tail_len as usize];
        file.read_exact(&mut tail).context("reading the OE footer")?;
        let footer = OeFooter::open(&tail).map_err(|error| anyhow!("parsing the OE footer: {error}"))?;
        let rows = footer.footer().exact_counts.row_count;
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: self.tail_len,
                matched_rows: rows,
            },
        })
    }

    /// Every column of every granule through the engine's bulk projected scan, which decodes blocks side by side on
    /// the shared pool under its own memory bound and hands back one batch per granule in order.
    fn read_all_columns(&self) -> Result<Measured> {
        let file = self.reader()?;
        let column_ids: Vec<u32> = file.footer().columns.iter().map(|column| column.column_id).collect();
        let granule_ids = Self::granule_ids(&file);
        let force_serial = Self::serial_scan();
        let started = Instant::now();
        let (bytes, rows) = Self::projected_scan(&file, &column_ids, &granule_ids, force_serial)?;
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: rows,
            },
        })
    }

    /// Same-binary control for READ-008's interleaved measurements: `OE_BENCH_SERIAL_SCAN` keeps the projected scan
    /// on the calling thread. Resolved before an operation starts its clock.
    fn serial_scan() -> bool {
        std::env::var_os("OE_BENCH_SERIAL_SCAN").is_some()
    }

    fn granule_ids(file: &OeFile) -> Vec<u32> {
        file.footer()
            .granules
            .iter()
            .map(|granule| granule.granule_id)
            .collect()
    }

    /// One projected scan through the engine, reporting the file bytes its blocks cost and the rows it produced.
    ///
    /// The rows come from the granule directory, not the decoded value count: a column with gaps stores only the
    /// values that are present, so its block is shorter than the granule. The per-block decode runs in the engine's
    /// own named frame (`OeFile::scan_block`), on pool workers when the scan fans out.
    #[inline(never)]
    fn projected_scan(file: &OeFile, column_ids: &[u32], granule_ids: &[u32], serial: bool) -> Result<(u64, u64)> {
        let mut bytes = 0;
        let mut rows = 0u64;
        let sink = |batch: ScanBatch| {
            bytes += batch.bytes;
            rows += u64::from(batch.row_count);
            Ok(())
        };
        if serial {
            file.scan_projected_serial(column_ids, granule_ids, sink)
        } else {
            file.scan_projected(column_ids, granule_ids, sink)
        }
        .map_err(|error| {
            anyhow!(
                "scanning {} columns over {} granules: {error}",
                column_ids.len(),
                granule_ids.len()
            )
        })?;
        Ok((bytes, rows))
    }

    /// One column across every granule through the same projected scan as `read_all_columns`, so a single-column
    /// scan is timed on the engine's bulk path too.
    fn read_one_column(&self) -> Result<Measured> {
        let file = self.reader()?;
        let granule_ids = Self::granule_ids(&file);
        let force_serial = Self::serial_scan();
        let started = Instant::now();
        let (bytes, rows) = Self::projected_scan(&file, &[column_ids::OCCURRED_AT], &granule_ids, force_serial)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: rows,
            },
        })
    }

    /// The declared free-text column across every granule, view-decoded the way `read_all_columns` reads its string
    /// columns, so the decode of one column of long values is timed on its own rather than alongside every other
    /// column.
    fn read_text_column(&self) -> Result<Measured> {
        let file = self.reader()?;
        let started = Instant::now();
        let mut bytes = 0;
        let mut rows = 0u64;
        for granule in &file.footer().granules {
            bytes += Self::read_text_column_granule(&file, self.columns.note, granule)?;
            rows += u64::from(granule.row_count);
        }
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: rows,
            },
        })
    }

    /// One granule's block of the free-text column, view-decoded, reporting the file bytes it cost.
    #[inline(never)]
    fn read_text_column_granule(file: &OeFile, column_id: u32, granule: &GranuleEntry) -> Result<u64> {
        let mark = file
            .mark(column_id, 0, granule.granule_id)
            .map_err(|error| {
                anyhow!(
                    "locating the free-text column of granule {}: {error}",
                    granule.granule_id
                )
            })?
            .ok_or_else(|| anyhow!("no mark for the free-text column of granule {}", granule.granule_id))?;
        if file
            .read_column_string_views(column_id, granule.granule_id)
            .map_err(|error| {
                anyhow!(
                    "view-decoding the free-text column of granule {}: {error}",
                    granule.granule_id
                )
            })?
            .is_none()
        {
            file.read_column(column_id, granule.granule_id).map_err(|error| {
                anyhow!(
                    "reading the free-text column of granule {}: {error}",
                    granule.granule_id
                )
            })?;
        }
        Ok(mark.compressed_size)
    }

    /// One payload field for a scattered sample of rows, asked for as one batch.
    ///
    /// This is the engine's late-materialization path: the field's column block is located through its mark and the
    /// rows' values read out of it, without decoding their other fields. The sample is handed over whole, so each
    /// granule it reaches into is resolved and its block fetched once for all the sample's rows in it rather than
    /// once per row.
    fn read_payload_field(&self) -> Result<Measured> {
        let file = self.reader()?;
        let ordinals = &self.dataset.workload.sample_ordinals;
        let field = self.dataset.workload.payload_field.as_str();
        let refs: Vec<PayloadRef> = ordinals
            .iter()
            .map(|ordinal| PayloadRef { row_ordinal: *ordinal })
            .collect();
        let started = Instant::now();
        let found = Self::read_payload_field_rows(&file, &refs, field)?;
        let elapsed = started.elapsed();
        // The blocks a reader had to fetch to answer these rows: the field's column block in each granule the sample
        // reached into, counted once each.
        let mut bytes = 0;
        let mut touched: Vec<u32> = Vec::new();
        for granule in &file.footer().granules {
            let first = granule.first_row_ordinal;
            let last = first + u64::from(granule.row_count);
            if ordinals.iter().any(|ordinal| (first..last).contains(ordinal)) {
                touched.push(granule.granule_id);
            }
        }
        for granule_id in touched {
            bytes += Self::block(&file, self.columns.client, granule_id)?.1;
        }
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: found,
            },
        })
    }

    /// The timed engine call: one payload field for every sampled row, counting the rows that carry it. The engine
    /// resolves the granules itself, so this is the frame that names the operation's measured work.
    #[inline(never)]
    fn read_payload_field_rows(file: &OeFile, refs: &[PayloadRef], field: &str) -> Result<u64> {
        let values = file
            .read_payload_paths(refs, field)
            .map_err(|error| anyhow!("reading payload field `{field}` of {} rows: {error}", refs.len()))?;
        Ok(values.iter().filter(|value| value.is_some()).count() as u64)
    }

    /// Finds the one row carrying an entity id: the per-granule identity-hash filters rule out the granules that
    /// cannot hold the hash, then the surviving granules' hash blocks are scanned and every hit is confirmed against
    /// the id itself. Stops at the first granule that holds it.
    ///
    /// `OE_BENCH_NO_ENTITY_HASH_FILTERS` retains the former scan-every-granule path in the same optimized binary, so
    /// IMP-009 can be measured as interleaved control and candidate runs.
    fn point_lookup(&self) -> Result<Measured> {
        let file = self.reader()?;
        let wanted = self.dataset.workload.entity_id.as_str();
        let wanted_hash = identity_hash(wanted);
        let unfiltered = std::env::var_os("OE_BENCH_NO_ENTITY_HASH_FILTERS").is_some();
        let started = Instant::now();
        let mut bytes = 0;
        let mut found = 0u64;
        let candidate_granules = if unfiltered {
            file.footer().granules.iter().collect()
        } else {
            // Charged for what pruning cost: probing the hash rules a granule out only after its filter's bytes have
            // been fetched, and the pruning call consults every granule's.
            bytes += file
                .footer()
                .entity_hash_filters
                .iter()
                .map(|entry| entry.index_len)
                .sum::<u64>();
            file.granules_for_entity_hash(wanted_hash)
        };
        for granule in candidate_granules {
            let (block_bytes, hits) = Self::point_lookup_granule(&file, wanted, wanted_hash, granule.granule_id)?;
            bytes += block_bytes;
            found += hits;
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

    /// Scans one granule's hash block for `wanted_hash` and confirms each hit against the id column, reporting the
    /// file bytes read and the rows confirmed.
    #[inline(never)]
    fn point_lookup_granule(file: &OeFile, wanted: &str, wanted_hash: u64, granule_id: u32) -> Result<(u64, u64)> {
        let (hashes, mut bytes) = Self::block(file, column_ids::ENTITY_ID_HASH_LOW, granule_id)?;
        let ColumnData::U64(hashes) = hashes else {
            bail!("the entity id hash column decoded as something other than u64");
        };
        let candidates: Vec<usize> = hashes
            .iter()
            .enumerate()
            .filter(|(_, hash)| **hash == wanted_hash)
            .map(|(row, _)| row)
            .collect();
        if candidates.is_empty() {
            return Ok((bytes, 0));
        }
        // A hash match is a candidate, not an answer: confirm against the id column before counting the row. Only
        // the candidate's own row is decoded out of the block — a single-row range, not all of the granule's rows,
        // which is the whole point of the hash column narrowing the work first.
        let raw = file
            .read_column_raw(column_ids::ENTITY_ID, granule_id)
            .map_err(|error| anyhow!("reading the entity id column of granule {granule_id}: {error}"))?;
        bytes += (raw.body.len() + raw.presence.len()) as u64;
        let mut found = 0u64;
        for row in candidates {
            let decoded = decode_block_range_shared(
                raw.pipeline,
                raw.body,
                file.shared_alphabet(column_ids::ENTITY_ID),
                row,
                row + 1,
            )
            .map_err(|error| anyhow!("decoding row {row} of the entity id column: {error}"))?;
            let ColumnData::Strings(id) = decoded else {
                bail!("the entity id column decoded as something other than strings");
            };
            if id.get(0).flatten() == Some(wanted) {
                found += 1;
            }
        }
        Ok((bytes, found))
    }

    /// Counts rows inside a time window. The granule directory rules out whole granules on their recorded time range
    /// before any column block is read.
    fn range_search(&self) -> Result<Measured> {
        let file = self.reader()?;
        let lo = self.dataset.workload.occurred_at_lo;
        let hi = self.dataset.workload.occurred_at_hi;
        let started = Instant::now();
        let survivors: Vec<u32> = file
            .granules_for_occurred(lo, hi)
            .iter()
            .map(|granule| granule.granule_id)
            .collect();
        let mut bytes = 0;
        let mut matched = 0u64;
        for granule_id in survivors {
            let (block_bytes, hits) = Self::range_search_granule(&file, lo, hi, granule_id)?;
            bytes += block_bytes;
            matched += hits;
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

    /// Counts one granule's rows inside the window, reporting the file bytes read and the rows counted.
    #[inline(never)]
    fn range_search_granule(file: &OeFile, lo: i64, hi: i64, granule_id: u32) -> Result<(u64, u64)> {
        let (data, bytes) = Self::block(file, column_ids::OCCURRED_AT, granule_id)?;
        let ColumnData::I64(values) = data else {
            bail!("the occurred-at column decoded as something other than i64");
        };
        Ok((
            bytes,
            values.iter().filter(|value| (lo..=hi).contains(value)).count() as u64,
        ))
    }

    /// Counts rows whose event type equals one value. The file's dictionary turns the string into an id once, so the
    /// scan itself compares integers.
    fn value_search(&self) -> Result<Measured> {
        let file = self.reader()?;
        let wanted = self.dataset.workload.event_type.as_str();
        let started = Instant::now();
        let Ok(id) = file
            .footer()
            .dictionaries
            .event_type
            .binary_search_by(|probe| probe.as_str().cmp(wanted))
        else {
            bail!("`{wanted}` is not in the file's event-type dictionary");
        };
        let id = id as u64;
        let (blocks, bytes) = Self::scan_column(&file, Self::value_search_granule)?;
        let mut matched = 0u64;
        for block in blocks {
            let ColumnData::U64(values) = block else {
                bail!("the event type column decoded as something other than u64");
            };
            matched += values.iter().filter(|value| **value == id).count() as u64;
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

    /// One granule's block of the column `value_search` scans; the `black_box` keeps the frame on the stack as above.
    #[inline(never)]
    fn value_search_granule(file: &OeFile, granule_id: u32) -> Result<(ColumnData, u64)> {
        std::hint::black_box(Self::block(file, column_ids::EVENT_TYPE_ID, granule_id))
    }

    /// Counts rows whose declared free-text field contains `needle`, reading only that field's column family — and,
    /// where the field's own token filter proves a granule cannot hold the needle, not even that.
    ///
    /// `scan_granule` is the variant's own named per-granule function, so a profiler tells the dense, rare, and absent
    /// searches apart even on Rayon workers.
    fn full_text_search(
        &self,
        needle: &str,
        scan_granule: fn(&FullTextScan<'_>, &GranuleEntry) -> Result<(u64, u64)>,
    ) -> Result<Measured> {
        let file = self.reader()?;
        // Built once, outside the timing, so all three sides measure the same search over the same needle.
        let predicate = StringPredicate::Contains(ContainsMatcher::new(vec![needle.to_owned()], false));
        let alphabet = file.shared_alphabet(self.columns.note);
        let translated = alphabet.map(|entries| translate_for_shared_dictionary(&predicate, entries));
        let search = FullTextScan {
            alphabet,
            column_id: self.columns.note,
            file: &file,
            predicate,
            translated,
        };
        // Same-binary control for IMP-010's repeated interleaved measurements.
        let force_serial = std::env::var_os("OE_BENCH_SERIAL_DENSE_FULLTEXT").is_some();
        let started = Instant::now();
        let candidates: Vec<_> = file
            .footer()
            .granules
            .iter()
            .filter(|granule| {
                // The declared free-text column carries a trigram filter per granule: when it rules the needle out,
                // the granule holds no match and its column bytes are never read.
                !matches!(
                    file.text_token_filter(self.columns.note, granule.granule_id, 0),
                    Ok(Some(index)) if !index.might_contain_substring(needle)
                )
            })
            .collect();
        let scan = |granule: &&GranuleEntry| scan_granule(&search, granule);
        // The engine's cost model decides whether the survivors fan out: granules are alike within a file, so the
        // first survivor's decoded size stands for all of them, and a scan of one granule, or of granules too small to
        // pay for a worker hand-off, stays on this thread.
        let bytes_per_granule = candidates
            .first()
            .and_then(|granule| file.mark(self.columns.note, 0, granule.granule_id).ok().flatten())
            .map_or(0, |mark| mark.uncompressed_size);
        let (bytes, matched) = if !force_serial && scan_in_parallel(candidates.len(), bytes_per_granule) {
            // One broadcast wakes the whole pool at once — a parallel iterator wakes workers one split at a time —
            // and each worker then claims the next unscanned granule until none is left, so a slower core takes
            // fewer granules rather than holding the tail.
            let next = AtomicUsize::new(0);
            rayon::broadcast(|_| {
                let mut total = (0, 0);
                while let Some(granule) = candidates.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let (bytes, matched) = scan(granule)?;
                    total = (total.0 + bytes, total.1 + matched);
                }
                Ok::<_, anyhow::Error>(total)
            })
            .into_iter()
            .try_fold((0, 0), |(bytes, matched), worker| {
                worker.map(|(more_bytes, more_matched)| (bytes + more_bytes, matched + more_matched))
            })?
        } else {
            candidates.iter().map(scan).try_fold((0, 0), |(bytes, matched), next| {
                next.map(|(more_bytes, more_matched)| (bytes + more_bytes, matched + more_matched))
            })?
        };
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: None,
            outcome: OperationOutcome {
                bytes,
                matched_rows: matched,
            },
        })
    }

    /// The dense search's per-granule scan. The three variants share one body; the `black_box` keeps LTO from
    /// folding their identical frames into one symbol.
    #[inline(never)]
    fn full_text_search_granule(search: &FullTextScan<'_>, granule: &GranuleEntry) -> Result<(u64, u64)> {
        std::hint::black_box("full_text_search");
        Self::scan_free_text_granule(search, granule)
    }

    /// The rare search's per-granule scan.
    #[inline(never)]
    fn full_text_search_rare_granule(search: &FullTextScan<'_>, granule: &GranuleEntry) -> Result<(u64, u64)> {
        std::hint::black_box("full_text_search_rare");
        Self::scan_free_text_granule(search, granule)
    }

    /// The absent search's per-granule scan.
    #[inline(never)]
    fn full_text_search_absent_granule(search: &FullTextScan<'_>, granule: &GranuleEntry) -> Result<(u64, u64)> {
        std::hint::black_box("full_text_search_absent");
        Self::scan_free_text_granule(search, granule)
    }

    /// Searches one granule's free-text block, reporting the file bytes read and the rows that matched.
    fn scan_free_text_granule(search: &FullTextScan<'_>, granule: &GranuleEntry) -> Result<(u64, u64)> {
        let raw = search
            .file
            .read_column_raw(search.column_id, granule.granule_id)
            .map_err(|error| {
                anyhow!(
                    "reading the free-text column of granule {}: {error}",
                    granule.granule_id
                )
            })?;
        let bytes = (raw.body.len() + raw.presence.len()) as u64;
        // The substring test is pushed into the block: the engine searches the block's own value arena in one
        // pass and counts hits directly, so no row's text or row-sized Boolean mask is rebuilt for this aggregate.
        let count =
            match count_string_block_shared(raw.pipeline, raw.body, &search.predicate, search.translated.as_ref())
                .map_err(|error| {
                    anyhow!(
                        "searching the free-text column of granule {}: {error}",
                        granule.granule_id
                    )
                })? {
                Some(count) => count,
                None => match decode_block_shared(raw.pipeline, raw.body, search.alphabet).map_err(|error| {
                    anyhow!(
                        "decoding the free-text column of granule {}: {error}",
                        granule.granule_id
                    )
                })? {
                    ColumnData::Strings(values) => values
                        .iter()
                        .filter(|value| search.predicate.matches_value(*value))
                        .count(),
                    _ => bail!("the free-text column decoded as something other than strings"),
                },
            };
        Ok((bytes, count as u64))
    }

    /// Changes a slice of rows, reusing encoded source blocks whose before/after rows prove unchanged.
    fn update_rows(&self, scratch: &Path) -> Result<Measured> {
        if std::env::var_os("OE_BENCH_FULL_REWRITE_UPDATE").is_some() {
            return self.update_rows_full_rewrite(scratch);
        }
        if std::env::var_os("OE_BENCH_SOURCE_CHANGES_UPDATE").is_some() {
            return self
                .update_rows_from_source_changes(scratch)
                .map(|(measured, _)| measured);
        }
        let ordinals = &self.dataset.workload.mutated_ordinals;
        if ordinals.len().saturating_mul(4) >= self.dataset.rows.len() {
            return self.update_rows_full_rewrite(scratch);
        }
        let target = scratch.join("updated.oe");
        let source = self
            .base
            .as_ref()
            .ok_or_else(|| anyhow!("the OE update needs a base file"))?;
        let tenant_id = TenantId::new_test_id(TENANT_SEED);
        let conversion_started = Instant::now();
        let (after, changes) = update_inputs(&self.dataset.rows, ordinals, tenant_id);
        let input_conversion = conversion_started.elapsed();
        let started = Instant::now();
        let built = update_build(source, after, &changes, &self.spec, tenant_id, self.lifecycle)?;
        let bytes = built.bytes.len() as u64 + built.reused_source_block_bytes;
        write_file(&target, &built.bytes)?;
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: Some(input_conversion),
            outcome: OperationOutcome {
                bytes,
                matched_rows: ordinals.len() as u64,
            },
        })
    }

    /// Retained benchmark control for measuring IMP-002 against the former decode-everything/full-rewrite path in the
    /// same optimized binary. Enabled only by `OE_BENCH_FULL_REWRITE_UPDATE`.
    fn update_rows_full_rewrite(&self, scratch: &Path) -> Result<Measured> {
        let file = self.reader()?;
        let ordinals = &self.dataset.workload.mutated_ordinals;
        let target = scratch.join("updated.oe");
        let tenant_id = TenantId::new_test_id(TENANT_SEED);
        let conversion_started = Instant::now();
        let oe_rows = oe_rows(&amended(&self.dataset.rows, ordinals), tenant_id);
        let input_conversion = conversion_started.elapsed();
        let started = Instant::now();
        let mut bytes = 0;
        for granule in &file.footer().granules {
            for column in &file.footer().columns {
                bytes += Self::block(&file, column.column_id, granule.granule_id)?.1;
            }
        }
        let built = encode(oe_rows, &self.spec, tenant_id, self.lifecycle)?;
        write_file(&target, &built.bytes)?;
        Ok(Measured {
            elapsed: started.elapsed(),
            input_conversion: Some(input_conversion),
            outcome: OperationOutcome {
                bytes: bytes + built.bytes.len() as u64,
                matched_rows: ordinals.len() as u64,
            },
        })
    }

    /// The changes-only update: the writer reads the touched granules back from the source instead of the harness
    /// converting every row, so the input conversion covers only the changed rows. Enabled by
    /// `OE_BENCH_SOURCE_CHANGES_UPDATE`.
    fn update_rows_from_source_changes(&self, scratch: &Path) -> Result<(Measured, Vec<PhaseElapsed>)> {
        let ordinals = &self.dataset.workload.mutated_ordinals;
        let target = scratch.join("updated.oe");
        let source = self
            .base
            .as_ref()
            .ok_or_else(|| anyhow!("the OE update needs a base file"))?;
        let tenant_id = TenantId::new_test_id(TENANT_SEED);
        let conversion_started = Instant::now();
        let changes = change_inputs(&self.dataset.rows, ordinals, tenant_id);
        let input_conversion = conversion_started.elapsed();
        let config = build_config(&self.spec, tenant_id, self.lifecycle);
        let started = Instant::now();
        let (built, mut phases) = update_oe_file_from_source_changes_profiled_with_executor(
            source,
            &changes,
            &config,
            &ThreadPoolEncodeExecutor,
        )
        .map(|(built, profile)| (built, writer_phases(profile.phases)))
        .map_err(|error| anyhow!("updating the OE file from its changes: {error}"))?;
        let bytes = built.bytes.len() as u64 + built.reused_source_block_bytes;
        let write_started = Instant::now();
        write_file(&target, &built.bytes)?;
        phases.push(PhaseElapsed {
            elapsed: write_started.elapsed(),
            phase: WriterPhase::DurableWrite,
        });
        phases.push(PhaseElapsed {
            elapsed: input_conversion,
            phase: WriterPhase::InputConversion,
        });
        Ok((
            Measured {
                elapsed: started.elapsed(),
                input_conversion: Some(input_conversion),
                outcome: OperationOutcome {
                    bytes,
                    matched_rows: ordinals.len() as u64,
                },
            },
            phases,
        ))
    }

    fn update_rows_with_phases(&self, scratch: &Path) -> Result<(Measured, Vec<PhaseElapsed>)> {
        if std::env::var_os("OE_BENCH_SOURCE_CHANGES_UPDATE").is_some() {
            return self.update_rows_from_source_changes(scratch);
        }
        let ordinals = &self.dataset.workload.mutated_ordinals;
        if ordinals.len().saturating_mul(4) >= self.dataset.rows.len() {
            return self
                .update_rows_full_rewrite(scratch)
                .map(|measured| (measured, Vec::new()));
        }
        let target = scratch.join("updated.oe");
        let source = self
            .base
            .as_ref()
            .ok_or_else(|| anyhow!("the OE update needs a base file"))?;
        let tenant_id = TenantId::new_test_id(TENANT_SEED);
        let conversion_started = Instant::now();
        let (after, changes) = update_inputs(&self.dataset.rows, ordinals, tenant_id);
        let input_conversion = conversion_started.elapsed();
        let started = Instant::now();
        let (built, mut phases) =
            update_build_profiled(source, after, &changes, &self.spec, tenant_id, self.lifecycle)?;
        let bytes = built.bytes.len() as u64 + built.reused_source_block_bytes;
        let write_started = Instant::now();
        write_file(&target, &built.bytes)?;
        phases.push(PhaseElapsed {
            elapsed: write_started.elapsed(),
            phase: WriterPhase::DurableWrite,
        });
        phases.push(PhaseElapsed {
            elapsed: input_conversion,
            phase: WriterPhase::InputConversion,
        });
        Ok((
            Measured {
                elapsed: started.elapsed(),
                input_conversion: Some(input_conversion),
                outcome: OperationOutcome {
                    bytes,
                    matched_rows: ordinals.len() as u64,
                },
            },
            phases,
        ))
    }

    /// Removes a slice of rows by writing a deletion vector beside the file: the row positions as an exact run-length
    /// bitmap, in the format's own encoding. The published file is left untouched.
    fn delete_rows(&self, scratch: &Path) -> Result<Measured> {
        let ordinals = &self.dataset.workload.mutated_ordinals;
        let target = scratch.join("deleted.oe.dv");
        let started = Instant::now();
        let bitmap = RoaringRangeBitmap::from_rows(ordinals.iter().copied());
        let encoded = bitmap.encode();
        write_file(&target, &encoded)?;
        let elapsed = started.elapsed();
        Ok(Measured {
            elapsed,
            input_conversion: None,
            outcome: OperationOutcome {
                bytes: encoded.len() as u64,
                matched_rows: bitmap.count(),
            },
        })
    }

    /// Reports how many rows differ between the two versions.
    ///
    /// The footer already carries a content hash per stripe, so the comparison starts by ruling out every stripe
    /// whose hash is unchanged — no column bytes are read for those at all. Only the granules inside a surviving
    /// stripe are decoded and compared column by column.
    fn diff_versions(&self) -> Result<Measured> {
        let base = self.reader()?;
        let next = self.next_reader()?;
        let started = Instant::now();
        let base_sums = &base.footer().stripe_checksums;
        let next_sums = &next.footer().stripe_checksums;
        let changed_stripes: Vec<u32> = (0..base_sums.len().max(next_sums.len()))
            .filter(|index| base_sums.get(*index) != next_sums.get(*index))
            .map(|index| index as u32)
            .collect();
        let candidates: Vec<u32> = base
            .footer()
            .granules
            .iter()
            .chain(next.footer().granules.iter())
            .filter(|granule| changed_stripes.contains(&granule.stripe_id))
            .map(|granule| granule.granule_id)
            .collect::<std::collections::BTreeSet<u32>>()
            .into_iter()
            .collect();

        let mut bytes = 0;
        let mut differing = 0u64;
        for granule_id in candidates {
            let (block_bytes, rows) = Self::diff_versions_granule(&base, &next, granule_id)?;
            bytes += block_bytes;
            differing += rows;
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

    /// Compares one granule column by column across the two versions, reporting the file bytes read and the rows
    /// that differ.
    #[inline(never)]
    fn diff_versions_granule(base: &OeFile, next: &OeFile, granule_id: u32) -> Result<(u64, u64)> {
        let rows = base
            .footer()
            .granules
            .iter()
            .find(|granule| granule.granule_id == granule_id)
            .map(|granule| granule.row_count as usize)
            .unwrap_or_default();
        let mut bytes = 0;
        let mut changed = vec![false; rows];
        for column in &base.footer().columns {
            // The builder is deterministic, so a column block whose rows did not move encodes to the same bytes.
            // Comparing the encoded blocks first settles most columns without decoding either side.
            let (before, after) = match (
                base.read_column_raw(column.column_id, granule_id),
                next.read_column_raw(column.column_id, granule_id),
            ) {
                (Ok(before), Ok(after)) => {
                    bytes +=
                        (before.body.len() + before.presence.len() + after.body.len() + after.presence.len()) as u64;
                    if before.body == after.body && before.presence == after.presence {
                        continue;
                    }
                    (
                        decode_block_shared(before.pipeline, before.body, base.shared_alphabet(column.column_id))
                            .map_err(|error| {
                                anyhow!("decoding column {} of granule {granule_id}: {error}", column.column_id)
                            })?,
                        decode_block_shared(after.pipeline, after.body, next.shared_alphabet(column.column_id))
                            .map_err(|error| {
                                anyhow!("decoding column {} of granule {granule_id}: {error}", column.column_id)
                            })?,
                    )
                }
                // A constant block is elided to a zero-length mark, so it has no stored bytes to compare; both
                // sides read through the materializing decoder, which rebuilds them from their stats.
                _ => (
                    base.read_column(column.column_id, granule_id)
                        .map_err(|error| {
                            anyhow!("reading column {} of granule {granule_id}: {error}", column.column_id)
                        })?
                        .data,
                    next.read_column(column.column_id, granule_id)
                        .map_err(|error| {
                            anyhow!("reading column {} of granule {granule_id}: {error}", column.column_id)
                        })?
                        .data,
                ),
            };
            mark_differences(&before, &after, &mut changed);
        }
        Ok((bytes, changed.iter().filter(|flag| **flag).count() as u64))
    }
}

impl FormatUnderTest for OeFormat<'_> {
    fn name(&self) -> &'static str {
        "oe"
    }

    fn file_bytes(&self) -> u64 {
        self.file_bytes.load(Ordering::Relaxed)
    }

    fn run(&self, operation: Operation, scratch: &Path) -> Result<Measured> {
        match operation {
            Operation::CreateWrite => self.create_write(scratch),
            Operation::DeleteRows => self.delete_rows(scratch),
            Operation::DiffVersions => self.diff_versions(),
            Operation::FullTextSearch => {
                self.full_text_search(&self.dataset.workload.needle, Self::full_text_search_granule)
            }
            Operation::FullTextSearchAbsent => self.full_text_search(
                &self.dataset.workload.absent_needle,
                Self::full_text_search_absent_granule,
            ),
            Operation::FullTextSearchRare => {
                self.full_text_search(&self.dataset.workload.rare_needle, Self::full_text_search_rare_granule)
            }
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

    fn run_with_phases(&self, operation: Operation, scratch: &Path) -> Result<(Measured, Vec<PhaseElapsed>)> {
        match operation {
            Operation::CreateWrite => self.create_write_with_phases(scratch),
            Operation::UpdateRows => self.update_rows_with_phases(scratch),
            _ => self.run(operation, scratch).map(|measured| (measured, Vec::new())),
        }
    }
}

/// Flags every row position at which the two decoded blocks disagree, leaving positions already flagged alone.
fn mark_differences(before: &ColumnData, after: &ColumnData, changed: &mut [bool]) {
    let differs = |row: usize| match (before, after) {
        (ColumnData::U64(a), ColumnData::U64(b)) => a.get(row) != b.get(row),
        (ColumnData::I64(a), ColumnData::I64(b)) => a.get(row) != b.get(row),
        (ColumnData::U128(a), ColumnData::U128(b)) => a.get(row) != b.get(row),
        (ColumnData::F64(a), ColumnData::F64(b)) => a.get(row) != b.get(row),
        (ColumnData::Strings(a), ColumnData::Strings(b)) => a.get(row) != b.get(row),
        (ColumnData::Decimal { values: a, scale: x }, ColumnData::Decimal { values: b, scale: y }) => {
            x != y || a.get(row) != b.get(row)
        }
        // Two blocks of different types are different at every row.
        _ => true,
    };
    for (row, flag) in changed.iter_mut().enumerate() {
        *flag = *flag || differs(row);
    }
}

/// Encodes the dataset as one published OE file.
///
/// `kind` is promoted to a typed column by declaration and `note` is declared free text; `char_count`, `reactions`, and
/// `client` are lifted into columns of their own by the writer's own statistics. Nothing is left in the residual
/// payload, so the file holds exactly the columns the Parquet file holds.
fn build(rows: &[BenchmarkRow], spec: &BenchmarkSpec, lifecycle: BuildLifecycle) -> Result<oe::writer::build::BuiltOe> {
    let tenant_id = TenantId::new_test_id(TENANT_SEED);
    encode(oe_rows(rows, tenant_id), spec, tenant_id, lifecycle)
}

/// The lifecycle every OE file of a run is built under. `OE_BENCH_NO_TRAILING` publishes without any trailing
/// compression stage and `OE_BENCH_SIZE_OPTIMIZED` publishes under the rewrite/compaction cascade, so the stage's
/// read cost and the size-optimized rule can each be measured against the default over the same rows in the same
/// binary. Resolved once, in `prepare_selected`, outside every timed operation.
fn benchmark_lifecycle() -> Result<BuildLifecycle> {
    let no_trailing = std::env::var_os("OE_BENCH_NO_TRAILING").is_some();
    let size_optimized = std::env::var_os("OE_BENCH_SIZE_OPTIMIZED").is_some();
    match (no_trailing, size_optimized) {
        (false, false) => Ok(BuildLifecycle::FreshPublication),
        (true, false) => Ok(BuildLifecycle::FreshPublicationWithoutTrailing),
        (false, true) => Ok(BuildLifecycle::RewriteOrCompaction),
        (true, true) => bail!("OE_BENCH_NO_TRAILING and OE_BENCH_SIZE_OPTIMIZED select different lifecycles"),
    }
}

/// Converts the generated rows into the writer's input shape. Every write path converts through here, so create and
/// update are measured on the same terms. The work is spread over the thread pool: it is harness cost, and done
/// serially it took as long as the build it fed. Each worker shares its strings across the rows it converts.
fn oe_rows(rows: &[BenchmarkRow], tenant_id: TenantId) -> Vec<BuildRow> {
    rows.par_iter()
        .map_init(RowStrings::new, |strings, row| oe_row(row, tenant_id, strings))
        .collect()
}

/// The strings one conversion worker shares across the rows it converts: the payload field names, made once, and
/// the envelope strings, shared from first sight.
struct RowStrings {
    char_count: FieldName,
    client: FieldName,
    kind: FieldName,
    note: FieldName,
    reactions: FieldName,
    shared: SharedStrings,
}

impl RowStrings {
    fn new() -> Self {
        Self {
            char_count: FieldName::from(FIELD_CHAR_COUNT),
            client: FieldName::from(FIELD_CLIENT),
            kind: FieldName::from(FIELD_KIND),
            note: FieldName::from(FIELD_NOTE),
            reactions: FieldName::from(FIELD_REACTIONS),
            shared: SharedStrings::default(),
        }
    }
}

/// The format's own share of a create: rows already in the writer's shape, encoded into a published file.
fn encode(
    oe_rows: Vec<BuildRow>,
    spec: &BenchmarkSpec,
    tenant_id: TenantId,
    lifecycle: BuildLifecycle,
) -> Result<oe::writer::build::BuiltOe> {
    let config = build_config(spec, tenant_id, lifecycle);
    build_oe_file_with_executor(oe_rows, &config, &ThreadPoolEncodeExecutor)
        .map_err(|error| anyhow!("building the OE file: {error}"))
}

/// The format's own share of a create, published straight to `target`: the file is written stripe by stripe as the
/// build lays them out and made durable once, after the last byte.
fn encode_streamed(
    oe_rows: Vec<BuildRow>,
    spec: &BenchmarkSpec,
    tenant_id: TenantId,
    lifecycle: BuildLifecycle,
    target: &Path,
) -> Result<StreamedOe> {
    let config = build_config(spec, tenant_id, lifecycle);
    write_streamed(target, |sink| {
        build_oe_file_streamed(oe_rows, &config, &ThreadPoolEncodeExecutor, sink)
            .map_err(|error| anyhow!("building the OE file: {error}"))
    })
}

/// [`encode_streamed`] with the writer's phases. `durable_write` is what the operation still waits for after the
/// build returns — the writes that had not landed yet, and the sync — since the rest of the write overlapped the
/// build.
fn encode_streamed_profiled(
    oe_rows: Vec<BuildRow>,
    spec: &BenchmarkSpec,
    tenant_id: TenantId,
    lifecycle: BuildLifecycle,
    target: &Path,
) -> Result<(StreamedOe, Vec<PhaseElapsed>)> {
    let config = build_config(spec, tenant_id, lifecycle);
    let mut build_finished = Instant::now();
    let (streamed, profile) = write_streamed(target, |sink| {
        let built = build_oe_file_streamed_profiled(oe_rows, &config, &ThreadPoolEncodeExecutor, sink)
            .map_err(|error| anyhow!("building the OE file: {error}"))?;
        build_finished = Instant::now();
        Ok(built)
    })?;
    let mut phases = writer_phases(profile.phases);
    phases.push(PhaseElapsed {
        elapsed: build_finished.elapsed(),
        phase: WriterPhase::DurableWrite,
    });
    Ok((streamed, phases))
}

/// Runs `build` with a sink that hands each finished byte range to a writer thread, so the ranges land in `path`
/// while the build carries on, then waits for the whole file to reach disk before returning what the build returned.
fn write_streamed<T>(path: &Path, build: impl FnOnce(&mut dyn FnMut(u64, Vec<u8>)) -> Result<T>) -> Result<T> {
    let mut file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let (ranges, incoming) = mpsc::channel::<(u64, Vec<u8>)>();
    std::thread::scope(|scope| {
        let writer = scope.spawn(move || -> std::io::Result<()> {
            for (offset, bytes) in incoming {
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&bytes)?;
            }
            file.sync_all()
        });
        let built = build(&mut |offset, bytes| {
            // A refused send means the writer stopped on an error, which its join below reports.
            let _ = ranges.send((offset, bytes));
        });
        drop(ranges);
        writer
            .join()
            .map_err(|_| anyhow!("the writer thread panicked"))?
            .with_context(|| format!("writing {}", path.display()))?;
        built
    })
}

/// Amends one row the way the workload does: its char_count up by one and its note replaced, with the dedupe hash
/// following the note. Every generated row carries both fields.
fn amend_oe_row(row: &mut BuildRow, ordinal: u64) {
    let note = format!("row {ordinal} corrected by the format benchmark");
    row.envelope.dedupe_hash_low = identity_hash(&note);
    let BuildPayload::Object(fields) = &mut row.payload else {
        return;
    };
    for (name, value) in fields.iter_mut() {
        match (&**name, value) {
            (FIELD_CHAR_COUNT, VariantValue::Int(char_count)) => *char_count += 1,
            (FIELD_NOTE, VariantValue::String(text)) => text.clone_from(&note),
            _ => {}
        }
    }
}

fn update_inputs(
    rows: &[BenchmarkRow],
    ordinals: &[u64],
    tenant_id: TenantId,
) -> (Vec<BuildRow>, Vec<OeRowChange<BuildRow>>) {
    let mut after = oe_rows(rows, tenant_id);
    for &ordinal in ordinals {
        if let Some(row) = after.get_mut(ordinal as usize) {
            amend_oe_row(row, ordinal);
        }
    }
    let mut strings = RowStrings::new();
    let changes = ordinals
        .iter()
        .filter_map(|&ordinal| {
            Some(OeRowChange {
                after: after.get(ordinal as usize)?.clone(),
                before: oe_row(rows.get(ordinal as usize)?, tenant_id, &mut strings),
                row_ordinal: ordinal,
            })
        })
        .collect();
    (after, changes)
}

/// The change set alone — each mutated row before and after — without converting the rest of the file.
fn change_inputs(rows: &[BenchmarkRow], ordinals: &[u64], tenant_id: TenantId) -> Vec<OeRowChange<BuildRow>> {
    let mut strings = RowStrings::new();
    ordinals
        .iter()
        .filter_map(|&ordinal| {
            let before = oe_row(rows.get(ordinal as usize)?, tenant_id, &mut strings);
            let mut after = before.clone();
            amend_oe_row(&mut after, ordinal);
            Some(OeRowChange {
                after,
                before,
                row_ordinal: ordinal,
            })
        })
        .collect()
}

fn update_build(
    source: &oe::writer::build::BuiltOe,
    after: Vec<BuildRow>,
    changes: &[OeRowChange<BuildRow>],
    spec: &BenchmarkSpec,
    tenant_id: TenantId,
    lifecycle: BuildLifecycle,
) -> Result<oe::writer::build::BuiltOe> {
    let config = build_config(spec, tenant_id, lifecycle);
    update_oe_file_from_changes_with_executor(source, after, changes, &config, &ThreadPoolEncodeExecutor)
        .map_err(|error| anyhow!("updating the OE file: {error}"))
}

fn update_build_profiled(
    source: &oe::writer::build::BuiltOe,
    after: Vec<BuildRow>,
    changes: &[OeRowChange<BuildRow>],
    spec: &BenchmarkSpec,
    tenant_id: TenantId,
    lifecycle: BuildLifecycle,
) -> Result<(oe::writer::build::BuiltOe, Vec<PhaseElapsed>)> {
    let config = build_config(spec, tenant_id, lifecycle);
    let (built, profile) =
        update_oe_file_from_changes_profiled_with_executor(source, after, changes, &config, &ThreadPoolEncodeExecutor)
            .map_err(|error| anyhow!("updating the OE file: {error}"))?;
    Ok((built, writer_phases(profile.phases)))
}

fn writer_phases(phases: Vec<(BuildPhase, u64)>) -> Vec<PhaseElapsed> {
    phases
        .into_iter()
        .map(|(phase, nanos)| PhaseElapsed {
            elapsed: std::time::Duration::from_nanos(nanos),
            phase: writer_phase(phase),
        })
        .collect()
}

fn writer_phase(phase: BuildPhase) -> WriterPhase {
    match phase {
        BuildPhase::SourceDecoding => WriterPhase::SourceDecoding,
        BuildPhase::Normalization => WriterPhase::Normalization,
        BuildPhase::DictionaryConstruction => WriterPhase::DictionaryConstruction,
        BuildPhase::PathStatistics => WriterPhase::PathStatistics,
        BuildPhase::GranuleConstruction => WriterPhase::GranuleConstruction,
        BuildPhase::BlockEncoding => WriterPhase::BlockEncoding,
        BuildPhase::Layout => WriterPhase::Layout,
        BuildPhase::FooterConstruction => WriterPhase::FooterConstruction,
        BuildPhase::Integrity => WriterPhase::Integrity,
        BuildPhase::FileAssembly => WriterPhase::FileAssembly,
        BuildPhase::RowDestruction => WriterPhase::RowDestruction,
    }
}

fn build_config(spec: &BenchmarkSpec, tenant_id: TenantId, lifecycle: BuildLifecycle) -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: OCCURRED_AT_ORIGIN_NANOS,
        footer_dek: None,
        footer_encryption: FooterEncryption::Plaintext,
        freetext: FreetextDeclaration {
            fields: vec![FIELD_NOTE.to_owned()],
        },
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle,
        page_size_rows: 0,
        profile: Profile::Event,
        promotion: PromotionPlan {
            columns: vec![PromotedColumn {
                kind: ColumnKind::String,
                name: FIELD_KIND.to_owned(),
                path: FIELD_KIND.to_owned(),
                since_schema_version: SCHEMA_VERSION,
                substring_searchable: false,
            }],
        },
        targets: LayoutTargets {
            index_granularity: spec.rows_per_group,
            // Effectively off: the row target alone decides where a granule is cut, so a granule holds the same rows
            // in every version of the file and the diff compares like with like.
            index_granularity_bytes: usize::MAX,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
            stripe_target_bytes: STRIPE_TARGET_BYTES,
        },
        tenant_id,
        time_series_bucket_nanos: None,
    }
}

/// Turns one generated row into the envelope-plus-payload shape the writer takes, its field names and envelope
/// strings shared through `strings`. Kept out of line so input conversion, which fans out over Rayon workers, keeps
/// its own name in a profile.
#[inline(never)]
fn oe_row(row: &BenchmarkRow, tenant_id: TenantId, strings: &mut RowStrings) -> BuildRow {
    // In ascending key order, as the writer requires.
    let mut fields: Vec<(FieldName, VariantValue)> = Vec::with_capacity(5);
    fields.push((strings.char_count.clone(), VariantValue::Int(row.char_count)));
    fields.push((strings.client.clone(), VariantValue::String(row.client.clone())));
    fields.push((strings.kind.clone(), VariantValue::String(row.kind.clone())));
    fields.push((strings.note.clone(), VariantValue::String(row.note.clone())));
    if let Some(reactions) = row.reactions {
        fields.push((strings.reactions.clone(), VariantValue::Int(reactions)));
    }
    BuildRow {
        envelope: BuildEnvelope {
            account_id: None,
            account_id_hash_low: 0,
            actor_id: row.actor_id.clone(),
            actor_id_hash_low: row.actor_id.as_deref().map(identity_hash).unwrap_or_default(),
            dedupe_hash_high: 0,
            dedupe_hash_low: identity_hash(&row.note),
            entity_id: Some(row.entity_id.clone()),
            entity_id_hash_high: 0,
            entity_id_hash_low: identity_hash(&row.entity_id),
            entity_type: strings.shared.share(&row.entity_type),
            event_id: EventId::new_test_id(row.event_id),
            event_type: strings.shared.share(&row.event_type),
            flags: EventFlags(0),
            ingested_at: TimestampValue::from_physical_nanos(row.ingested_at_nanos),
            occurred_at: TimestampValue::from_physical_nanos(row.occurred_at_nanos),
            schema_version: SCHEMA_VERSION,
            source: strings.shared.share(&row.source),
            stream_id: StreamId(STREAM_ID),
            stream_sequence: row.stream_sequence,
            tenant_id,
            trace_id_hash_low: 0,
        },
        epoch: EPOCH,
        external_id: None,
        payload: BuildPayload::Object(fields),
        provenance: None,
        raw_payload: None,
        relationships: None,
        sequence: row.sequence,
        subject: None,
    }
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
#[path = "test/oe_format.rs"]
mod tests;
