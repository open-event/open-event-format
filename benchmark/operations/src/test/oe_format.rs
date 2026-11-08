use super::*;
use crate::dataset::generate;
use oe::encoding::{
    CascadeStrategy, Compression, PipelineId, Transform, ValueKind, encode_block_forced_for_conformance,
    remove_trailing,
};
use oe::writer::build::build_oe_file;
use std::collections::BTreeMap;
use std::hint::black_box;
use tempfile::tempdir;

fn spec() -> BenchmarkSpec {
    BenchmarkSpec {
        iterations: 1,
        mutation_percent: 5,
        range_percent: 10,
        row_count: 500,
        rows_per_group: 64,
        seed: 13,
    }
}

/// The payload must end up entirely in typed columns: `kind` by declaration, `note` as declared free text, and the
/// rest by the writer's own shredding statistics. Anything left in the residual would be data Parquet's file does not
/// carry as a column, and the two sides would no longer be reading the same thing.
#[test]
fn every_payload_field_becomes_a_column_of_its_own() {
    let dir = tempdir().unwrap();
    let dataset = generate(&spec());
    let format = OeFormat::prepare(&dataset, &spec(), dir.path()).unwrap();
    let file = format.reader().unwrap();
    let footer = file.footer();

    let shredded: Vec<&str> = footer.shredded.iter().map(|entry| entry.path.as_str()).collect();
    assert!(shredded.contains(&FIELD_CHAR_COUNT), "{shredded:?}");
    assert!(shredded.contains(&FIELD_CLIENT), "{shredded:?}");
    assert!(shredded.contains(&FIELD_REACTIONS), "{shredded:?}");
    assert!(footer.freetext.iter().any(|entry| entry.declared_field == FIELD_NOTE));
    assert!(footer.columns.iter().any(|column| column.name == FIELD_KIND));
}

/// The diff compares blocks granule for granule, which only means anything if a granule holds the same rows in both
/// versions. `prepare` refuses a spec where it would not; this checks the guard holds for the shape the runner uses.
#[test]
fn granules_hold_exactly_the_rows_the_spec_asked_for() {
    let dir = tempdir().unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = OeFormat::prepare(&dataset, &spec, dir.path()).unwrap();
    let file = format.reader().unwrap();
    let granules = &file.footer().granules;

    assert!(granules.len() > 1, "one granule would prune nothing");
    for granule in granules.iter().take(granules.len() - 1) {
        assert_eq!(granule.row_count as usize, spec.rows_per_group);
    }
}

/// A delete leaves the published file alone and writes a bitmap beside it. The file's own bytes must be untouched, or
/// the delete is really a rewrite wearing a different name.
#[test]
fn a_delete_leaves_the_published_file_untouched() {
    let dir = tempdir().unwrap();
    let scratch = dir.path().join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = OeFormat::prepare(&dataset, &spec, dir.path()).unwrap();
    let before = std::fs::read(dir.path().join("base.oe")).unwrap();

    let measured = format.run(Operation::DeleteRows, &scratch).unwrap();

    assert_eq!(measured.outcome.matched_rows, spec.mutated_row_count() as u64);
    assert_eq!(std::fs::read(dir.path().join("base.oe")).unwrap(), before);
    assert!(
        measured.outcome.bytes < before.len() as u64 / 100,
        "the deletion vector cost {} bytes against a {}-byte file",
        measured.outcome.bytes,
        before.len()
    );
}

/// A cold open reads the tail and nothing more, and still knows how many rows the file holds.
#[test]
fn a_cold_open_reads_only_the_footer() {
    let dir = tempdir().unwrap();
    let scratch = dir.path().join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = OeFormat::prepare(&dataset, &spec, dir.path()).unwrap();

    let measured = format.run(Operation::OpenMetadata, &scratch).unwrap();

    assert_eq!(measured.outcome.matched_rows, spec.row_count as u64);
    assert!(measured.outcome.bytes < format.file_bytes());
}

/// Read-all exercises the engine's whole-column string-view API across the benchmark's string pipelines and still
/// reports the full dataset rather than falling back or dropping sparse values.
#[test]
fn read_all_columns_uses_the_view_capable_workload_path() {
    let dir = tempdir().unwrap();
    let scratch = dir.path().join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = OeFormat::prepare(&dataset, &spec, dir.path()).unwrap();

    let measured = format.run(Operation::ReadAllColumns, &scratch).unwrap();

    assert_eq!(measured.outcome.matched_rows, spec.row_count as u64);
    assert!(measured.outcome.bytes > 0);
}

/// The default dataset (32 granules) published as exactly `stripes` stripes. The writer cuts stripes on an internal
/// byte estimate, so the target that yields the wanted count is found by bisection.
fn file_with_stripes(stripes: usize) -> Vec<u8> {
    let spec = BenchmarkSpec {
        rows_per_group: BenchmarkSpec::default().row_count / 32,
        ..BenchmarkSpec::default()
    };
    let tenant_id = TenantId::new_test_id(TENANT_SEED);
    let rows = oe_rows(&generate(&spec).rows, tenant_id);
    let mut config = build_config(&spec, tenant_id, BuildLifecycle::FreshPublication);
    let (mut low, mut high) = (1usize, 1usize << 40);
    loop {
        config.targets.stripe_target_bytes = low.midpoint(high);
        let built = oe::writer::build::build_oe_file(rows.clone(), &config).unwrap();
        match built.footer.stripes.len().cmp(&stripes) {
            std::cmp::Ordering::Equal => return built.bytes,
            std::cmp::Ordering::Greater => low = config.targets.stripe_target_bytes + 1,
            std::cmp::Ordering::Less => high = config.targets.stripe_target_bytes - 1,
        }
        assert!(low <= high, "no stripe target cuts the dataset into {stripes} stripes");
    }
}

/// The smallest and the median of `samples`, in nanoseconds.
fn min_and_median(samples: &mut [u64]) -> (u64, u64) {
    samples.sort_unstable();
    (samples[0], samples[samples.len() / 2])
}

/// Prints wall and process CPU per whole-file open of the default dataset cut into 1, 4 and 32 stripes, then the
/// same for just the stripe hashing done the nested way (`hash_tree` per stripe inside a `par_iter`) and the flat
/// way (`hash_segments`). Run alone: `cargo test --release -p oe-operations-benchmark open_timing -- --ignored --nocapture`.
#[test]
#[ignore]
#[cfg(unix)]
fn open_timing_by_stripe_count() {
    const REPEATS: usize = 40;
    let mut timed = |label: String, mut work: Box<dyn FnMut() + '_>| {
        let (mut wall, mut cpu) = (Vec::new(), Vec::new());
        for _ in 0..REPEATS {
            let cpu_started = crate::allocation::process_cpu_nanos().unwrap();
            let started = Instant::now();
            work();
            wall.push(started.elapsed().as_nanos() as u64);
            cpu.push(crate::allocation::process_cpu_nanos().unwrap() - cpu_started);
        }
        let (wall_min, wall_median) = min_and_median(&mut wall);
        let (cpu_min, cpu_median) = min_and_median(&mut cpu);
        println!(
            "{label}: wall min {:.3} / median {:.3} ms, cpu min {:.3} / median {:.3} ms",
            wall_min as f64 / 1e6,
            wall_median as f64 / 1e6,
            cpu_min as f64 / 1e6,
            cpu_median as f64 / 1e6
        );
    };
    for stripes in [1usize, 4, 32] {
        let bytes = file_with_stripes(stripes);
        let file = OeFile::open(bytes.clone(), None).unwrap();
        let segments: Vec<&[u8]> = file
            .footer()
            .stripes
            .iter()
            .map(|stripe| &bytes[stripe.file_offset as usize..][..stripe.byte_len as usize])
            .collect();
        let mut input = Some(bytes.clone());
        let mut opened = None;
        timed(
            format!("open {stripes} stripes, {} bytes", bytes.len()),
            Box::new(|| {
                opened = Some(OeFile::open(input.take().unwrap(), None).unwrap());
                input = Some(bytes.clone());
            }),
        );
        drop(opened);
        timed(
            format!("nested hash_tree per stripe, {stripes} stripes"),
            Box::new(|| {
                let _: Vec<_> = segments
                    .par_iter()
                    .map(|segment| oe::file::integrity::hash_tree(segment))
                    .collect();
            }),
        );
        timed(
            format!("flat hash_segments, {stripes} stripes"),
            Box::new(|| {
                let _ = oe::file::integrity::hash_segments(&segments);
            }),
        );
    }
}

/// The default dataset published under `lifecycle`, opened and verified.
fn default_file(lifecycle: BuildLifecycle) -> OeFile {
    let spec = BenchmarkSpec::default();
    let tenant_id = TenantId::new_test_id(TENANT_SEED);
    let rows = oe_rows(&generate(&spec).rows, tenant_id);
    let built = build_oe_file(rows, &build_config(&spec, tenant_id, lifecycle)).unwrap();
    OeFile::open(built.bytes, None).unwrap()
}

/// Prints every column's block-body bytes before (inflated) and after (as stored) its trailing compression stage over
/// the default dataset, grouped by the pipeline its blocks took, so the stage's saving can be read column by column.
/// Elided constant blocks and multi-page columns, which have no single raw body, are left out. Run alone:
/// `cargo test --release -p oe-operations-benchmark trailing_stage_bytes -- --ignored --nocapture`.
#[test]
#[ignore]
fn trailing_stage_bytes_by_column() {
    let file = default_file(BuildLifecycle::FreshPublication);
    println!(
        "{:<26} {:<34} {:>7} {:>11} {:>11} {:>7}",
        "column", "pipeline", "blocks", "before", "after", "saved"
    );
    let (mut total_before, mut total_after) = (0u64, 0u64);
    for column in &file.footer().columns {
        let mut by_pipeline: BTreeMap<String, (usize, u64, u64)> = BTreeMap::new();
        for granule in &file.footer().granules {
            let Ok(raw) = file.read_column_raw(column.column_id, granule.granule_id) else {
                continue;
            };
            let compression = raw.pipeline.compression().unwrap();
            let key = format!(
                "{:?}+{:?}+{compression:?}",
                raw.pipeline.transform().unwrap(),
                raw.pipeline.side_stream().unwrap()
            );
            let entry = by_pipeline.entry(key).or_default();
            entry.0 += 1;
            entry.1 += remove_trailing(compression, raw.body).unwrap().len() as u64;
            entry.2 += raw.body.len() as u64;
        }
        for (pipeline, (blocks, before, after)) in by_pipeline {
            println!(
                "{:<26} {:<34} {blocks:>7} {before:>11} {after:>11} {:>6.1}%",
                column.name,
                pipeline,
                100.0 * (before.saturating_sub(after)) as f64 / before.max(1) as f64
            );
            total_before += before;
            total_after += after;
        }
    }
    println!(
        "{:<26} {:<34} {:>7} {total_before:>11} {total_after:>11} {:>6.1}%",
        "total",
        "",
        "",
        100.0 * (total_before.saturating_sub(total_after)) as f64 / total_before.max(1) as f64
    );
}

/// Times `work` `repeats` times and prints its fastest and median run beside the bytes it consumed per run.
fn time_decode(label: &str, input_bytes: usize, repeats: usize, mut work: impl FnMut()) {
    let mut samples = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        let started = Instant::now();
        work();
        samples.push(started.elapsed().as_nanos() as u64);
    }
    let (min, median) = min_and_median(&mut samples);
    println!(
        "{label:<58} {input_bytes:>9} B  min {:>9.1} us  median {:>9.1} us  {:>8.1} MiB/s",
        min as f64 / 1e3,
        median as f64 / 1e3,
        input_bytes as f64 / (1024.0 * 1024.0) / (median as f64 / 1e9)
    );
}

/// Isolates each decode family over one realistic block of the default dataset — the first granule's block of every
/// column: the trailing stage's inflate on its own for whichever families the file holds (structured Zstandard,
/// seekable Zstandard, LZ4), and each string transform's Arrow view decode over the already-inflated body (FSST,
/// dictionary). No default column encodes raw, so the free-text column is re-encoded raw for that family's rows.
/// Throughput is over the input bytes of the step timed. Run alone:
/// `cargo test --release -p oe-operations-benchmark string_decode_microbenchmarks -- --ignored --nocapture`.
#[test]
#[ignore]
fn string_decode_microbenchmarks() {
    const REPEATS: usize = 200;
    let file = default_file(BuildLifecycle::FreshPublication);
    let granule_id = file.footer().granules[0].granule_id;
    let mut seen = Vec::new();
    for column in &file.footer().columns {
        let Ok(raw) = file.read_column_raw(column.column_id, granule_id) else {
            continue;
        };
        let transform = raw.pipeline.transform().unwrap();
        let compression = raw.pipeline.compression().unwrap();
        // One block per distinct pipeline keeps the listing to the families, not to every column that shares one.
        if seen.contains(&(transform, compression)) {
            continue;
        }
        seen.push((transform, compression));
        let body = remove_trailing(compression, raw.body).unwrap().into_owned();
        if compression != Compression::None {
            time_decode(
                &format!("{:<20} {compression:?} inflate -> {} B", column.name, body.len()),
                raw.body.len(),
                REPEATS,
                || {
                    black_box(remove_trailing(compression, raw.body).unwrap());
                },
            );
        }
        if column.kind == ColumnKind::String {
            let inflated = PipelineId::new(transform, Compression::None, ValueKind::String)
                .with_side_stream(raw.pipeline.side_stream().unwrap());
            time_decode(
                &format!("{:<20} {transform:?} view decode ({} rows)", column.name, raw.row_count),
                body.len(),
                REPEATS,
                || {
                    black_box(
                        file.decode_string_block_views(column.column_id, inflated, &body)
                            .unwrap()
                            .unwrap(),
                    );
                },
            );
        }
    }
    let note_id = ColumnIds::resolve(file.footer()).unwrap().note;
    let note = file.read_column(note_id, granule_id).unwrap().data;
    let raw_block =
        encode_block_forced_for_conformance(&note, false, CascadeStrategy::DecodeOptimized, Transform::RawString)
            .unwrap();
    let raw_compression = raw_block.pipeline.compression().unwrap();
    let raw_body = remove_trailing(raw_compression, &raw_block.bytes).unwrap().into_owned();
    if raw_compression != Compression::None {
        time_decode(
            &format!(
                "{:<20} {raw_compression:?} inflate -> {} B",
                "note as RawString",
                raw_body.len()
            ),
            raw_block.bytes.len(),
            REPEATS,
            || {
                black_box(remove_trailing(raw_compression, &raw_block.bytes).unwrap());
            },
        );
    }
    let inflated = PipelineId::new(Transform::RawString, Compression::None, ValueKind::String);
    time_decode(
        &format!(
            "{:<20} RawString view decode ({} rows)",
            "note as RawString",
            note.row_count()
        ),
        raw_body.len(),
        REPEATS,
        || {
            black_box(
                file.decode_string_block_views(note_id, inflated, &raw_body)
                    .unwrap()
                    .unwrap(),
            );
        },
    );
}

/// Prints the serial and the fanned-out projected scan over the default dataset for one column, two columns, and
/// every column, with the estimated decode bytes and the inflated block bytes each projection carries (the figures
/// the engine's window bound and fan-out rule read), after the one-column read done the way `value_search` still
/// reads — one raw block at a time on the calling thread — as the in-process control for the engine's serial path.
/// Run alone: `cargo test --release -p oe-operations-benchmark scan_timing -- --ignored --nocapture`.
#[test]
#[ignore]
fn scan_timing_by_projection_width() {
    const REPEATS: usize = 200;
    let spec = BenchmarkSpec::default();
    let dataset = generate(&spec);
    let built = build(&dataset.rows, &spec, BuildLifecycle::FreshPublication).unwrap();
    let columns = ColumnIds::resolve(&built.footer).unwrap();
    let all: Vec<u32> = built.footer.columns.iter().map(|column| column.column_id).collect();
    let template = OeFile::open(built.bytes, None).unwrap();
    let granules: Vec<u32> = template
        .footer()
        .granules
        .iter()
        .map(|granule| granule.granule_id)
        .collect();
    let mut samples = Vec::with_capacity(REPEATS);
    for _ in 0..REPEATS {
        let file = template.fresh_reader().unwrap();
        let started = Instant::now();
        OeFormat::scan_column(&file, |file, granule_id| {
            OeFormat::block(file, column_ids::OCCURRED_AT, granule_id)
        })
        .unwrap();
        samples.push(started.elapsed().as_nanos() as u64);
    }
    let (min, median) = min_and_median(&mut samples);
    println!(
        "one column   per-block loop {:>68} min {:>9.1} us  median {:>9.1} us",
        "",
        min as f64 / 1e3,
        median as f64 / 1e3
    );
    for (label, projection) in [
        ("one column", vec![column_ids::OCCURRED_AT]),
        ("two columns", vec![column_ids::OCCURRED_AT, columns.note]),
        ("all columns", all),
    ] {
        let (mut estimated, mut stored, mut inflated) = (0u64, 0u64, 0u64);
        for granule_id in &granules {
            for column_id in &projection {
                let mark = template.mark(*column_id, 0, *granule_id).unwrap().unwrap();
                estimated += mark.uncompressed_size + u64::from(mark.row_count) * 16;
                stored += mark.compressed_size;
                inflated += mark.uncompressed_size;
            }
        }
        for serial in [true, false] {
            let mut samples = Vec::with_capacity(REPEATS);
            for _ in 0..REPEATS {
                let file = template.fresh_reader().unwrap();
                let started = Instant::now();
                OeFormat::projected_scan(&file, &projection, &granules, serial).unwrap();
                samples.push(started.elapsed().as_nanos() as u64);
            }
            let (min, median) = min_and_median(&mut samples);
            println!(
                "{label:<12} {:<8} estimated {estimated:>10} B inflated {inflated:>9} B stored {stored:>9} B  min {:>9.1} us  median {:>9.1} us",
                if serial { "serial" } else { "parallel" },
                min as f64 / 1e3,
                median as f64 / 1e3
            );
        }
    }
}
