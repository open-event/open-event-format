
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
use oe::encoding::{ColumnData, decode_block_shared};
use oe::events::variant::VariantValue;
use oe::events::{EventFlags, derived_entity_id_hash, EventId, StreamId, TenantId, TimestampValue};
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

const SCHEMA_VERSION: u32 = 1;

const STRIPE_TARGET_BYTES: usize = 4 * 1024 * 1024;

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

struct FullTextScan<'a> {
    alphabet: Option<&'a [String]>,
    column_id: u32,
    file: &'a OeFile,
    predicate: StringPredicate,
    translated: Option<SharedDictionaryPredicate>,
}

pub struct OeFormat<'a> {
    base: Option<oe::writer::build::BuiltOe>,
    base_path: PathBuf,
    file_bytes: AtomicU64,
    columns: ColumnIds,
    dataset: &'a Dataset,
    lifecycle: BuildLifecycle,
    tail_len: u64,
    next: Option<OeFile>,
    spec: BenchmarkSpec,
    template: Option<OeFile>,
    warm_reader: bool,
}

enum IterationReader<'a> {
    Fresh(Box<OeFile>),
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
    pub fn prepare(dataset: &'a Dataset, spec: &BenchmarkSpec, dir: &Path) -> Result<Self> {
        Self::prepare_selected(dataset, spec, dir, crate::model::OPERATIONS)
    }

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
            .map(|file| IterationReader::Fresh(Box::new(file)))
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

    fn block(file: &OeFile, column_id: u32, granule_id: u32) -> Result<(ColumnData, u64)> {
        let Ok(raw) = file.read_column_raw(column_id, granule_id) else {
            let read = file
                .read_column(column_id, granule_id)
                .map_err(|error| anyhow!("reading column {column_id} of granule {granule_id}: {error}"))?;
            return Ok((read.data, 0));
        };
        let bytes = (raw.body.len() + raw.presence.len()) as u64;
        let data = decode_block_shared(raw.pipeline, &raw.body, file.shared_alphabet(column_id))
            .map_err(|error| anyhow!("decoding column {column_id} of granule {granule_id}: {error}"))?;
        Ok((data, bytes))
    }

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

    #[inline(never)]
    fn read_payload_field_rows(file: &OeFile, refs: &[PayloadRef], field: &str) -> Result<u64> {
        let values = file
            .read_payload_paths(refs, field)
            .map_err(|error| anyhow!("reading payload field `{field}` of {} rows: {error}", refs.len()))?;
        Ok(values.iter().filter(|value| value.is_some()).count() as u64)
    }

    fn point_lookup(&self) -> Result<Measured> {
        let file = self.reader()?;
        let wanted = self.dataset.workload.entity_id.as_str();
        let (wanted_hash, _) = derived_entity_id_hash(Some(wanted));
        let unfiltered = std::env::var_os("OE_BENCH_NO_ENTITY_HASH_FILTERS").is_some();
        let started = Instant::now();
        let predicate = StringPredicate::Equals(wanted.to_owned());
        let translated = file.shared_dictionary_predicate(column_ids::ENTITY_ID, &predicate);
        let mut bytes = 0;
        let mut found = 0u64;
        let candidate_granules = if unfiltered {
            file.footer().granules.iter().collect()
        } else {
            bytes += file
                .footer()
                .entity_hash_filters
                .iter()
                .map(|entry| entry.index_len)
                .sum::<u64>();
            file.granules_for_entity_hash(wanted_hash)
        };
        for granule in candidate_granules {
            let (block_bytes, hits) =
                Self::point_lookup_granule(&file, &predicate, translated.as_ref(), granule.granule_id)?;
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

    #[inline(never)]
    fn point_lookup_granule(
        file: &OeFile,
        predicate: &StringPredicate,
        translated: Option<&SharedDictionaryPredicate>,
        granule_id: u32,
    ) -> Result<(u64, u64)> {
        if let Ok(raw) = file.read_column_raw(column_ids::ENTITY_ID, granule_id)
            && let Some(count) = count_string_block_shared(raw.pipeline, &raw.body, predicate, translated)
                .map_err(|error| anyhow!("matching the entity id column of granule {granule_id}: {error}"))?
        {
            return Ok(((raw.body.len() + raw.presence.len()) as u64, count as u64));
        }
        let (ids, bytes) = Self::block(file, column_ids::ENTITY_ID, granule_id)?;
        let ColumnData::Strings(ids) = ids else {
            bail!("the entity id column decoded as something other than strings");
        };
        Ok((bytes, predicate.filter_decoded(&ids).iter().filter(|hit| **hit).count() as u64))
    }

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

    #[inline(never)]
    fn value_search_granule(file: &OeFile, granule_id: u32) -> Result<(ColumnData, u64)> {
        std::hint::black_box(Self::block(file, column_ids::EVENT_TYPE_ID, granule_id))
    }

    fn full_text_search(
        &self,
        needle: &str,
        scan_granule: fn(&FullTextScan<'_>, &GranuleEntry) -> Result<(u64, u64)>,
    ) -> Result<Measured> {
        let file = self.reader()?;
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
        let force_serial = std::env::var_os("OE_BENCH_SERIAL_DENSE_FULLTEXT").is_some();
        let started = Instant::now();
        let candidates: Vec<_> = file
            .footer()
            .granules
            .iter()
            .filter(|granule| {
                !matches!(
                    file.text_token_filter(self.columns.note, granule.granule_id, 0),
                    Ok(Some(index)) if !index.might_contain_substring(needle)
                )
            })
            .collect();
        let scan = |granule: &&GranuleEntry| scan_granule(&search, granule);
        let bytes_per_granule = candidates
            .first()
            .and_then(|granule| file.mark(self.columns.note, 0, granule.granule_id).ok().flatten())
            .map_or(0, |mark| mark.uncompressed_size);
        let (bytes, matched) = if !force_serial && scan_in_parallel(candidates.len(), bytes_per_granule) {
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

    #[inline(never)]
    fn full_text_search_granule(search: &FullTextScan<'_>, granule: &GranuleEntry) -> Result<(u64, u64)> {
        std::hint::black_box("full_text_search");
        Self::scan_free_text_granule(search, granule)
    }

    #[inline(never)]
    fn full_text_search_rare_granule(search: &FullTextScan<'_>, granule: &GranuleEntry) -> Result<(u64, u64)> {
        std::hint::black_box("full_text_search_rare");
        Self::scan_free_text_granule(search, granule)
    }

    #[inline(never)]
    fn full_text_search_absent_granule(search: &FullTextScan<'_>, granule: &GranuleEntry) -> Result<(u64, u64)> {
        std::hint::black_box("full_text_search_absent");
        Self::scan_free_text_granule(search, granule)
    }

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
        let count =
            match count_string_block_shared(raw.pipeline, &raw.body, &search.predicate, search.translated.as_ref())
                .map_err(|error| {
                    anyhow!(
                        "searching the free-text column of granule {}: {error}",
                        granule.granule_id
                    )
                })? {
                Some(count) => count,
                None => match decode_block_shared(raw.pipeline, &raw.body, search.alphabet).map_err(|error| {
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
                        decode_block_shared(before.pipeline, &before.body, base.shared_alphabet(column.column_id))
                            .map_err(|error| {
                                anyhow!("decoding column {} of granule {granule_id}: {error}", column.column_id)
                            })?,
                        decode_block_shared(after.pipeline, &after.body, next.shared_alphabet(column.column_id))
                            .map_err(|error| {
                                anyhow!("decoding column {} of granule {granule_id}: {error}", column.column_id)
                            })?,
                    )
                }
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
        _ => true,
    };
    for (row, flag) in changed.iter_mut().enumerate() {
        *flag = *flag || differs(row);
    }
}

fn build(rows: &[BenchmarkRow], spec: &BenchmarkSpec, lifecycle: BuildLifecycle) -> Result<oe::writer::build::BuiltOe> {
    let tenant_id = TenantId::new_test_id(TENANT_SEED);
    encode(oe_rows(rows, tenant_id), spec, tenant_id, lifecycle)
}

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

fn oe_rows(rows: &[BenchmarkRow], tenant_id: TenantId) -> Vec<BuildRow> {
    rows.par_iter()
        .map_init(RowStrings::new, |strings, row| oe_row(row, tenant_id, strings))
        .collect()
}

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
        delivery_lineage: false,
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
            index_granularity_bytes: usize::MAX,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
            stripe_target_bytes: STRIPE_TARGET_BYTES,
        },
        tenant_id,
        time_series_bucket_nanos: None,
    }
}

#[inline(never)]
fn oe_row(row: &BenchmarkRow, tenant_id: TenantId, strings: &mut RowStrings) -> BuildRow {
    let mut fields: Vec<(FieldName, VariantValue)> = Vec::with_capacity(5);
    fields.push((strings.char_count.clone(), VariantValue::Int(row.char_count)));
    fields.push((strings.client.clone(), VariantValue::String(row.client.clone())));
    fields.push((strings.kind.clone(), VariantValue::String(row.kind.clone())));
    fields.push((strings.note.clone(), VariantValue::String(row.note.clone())));
    if let Some(reactions) = row.reactions {
        fields.push((strings.reactions.clone(), VariantValue::Int(reactions)));
    }
    BuildRow {
        delivery: None,
        envelope: BuildEnvelope {
            account_id: None,
            account_id_hash_low: 0,
            actor_id: row.actor_id.clone(),
            actor_id_hash_low: row.actor_id.as_deref().map(identity_hash).unwrap_or_default(),
            dedupe_hash_high: 0,
            dedupe_hash_low: identity_hash(&row.note),
            entity_id: Some(row.entity_id.clone()),
            entity_id_hash_high: 0,
            entity_id_hash_low: derived_entity_id_hash(Some(&row.entity_id)).0,
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
