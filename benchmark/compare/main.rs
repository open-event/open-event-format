//! Compares OE with Parquet, Vortex and JSON on identical events and writes the numbers to `benchmark/results`.
//!
//! Run from this directory: `cargo run --release -- --sizes 100000,1000000 --reps 5 --out results`

mod dataset;
mod format;
mod ingest;
mod json;
mod oe;
mod parquet;
mod query;
mod report;
mod vortex;

use anyhow::{Result, bail};
use dataset::{Profile, generate};
use format::Format;
use query::{Partial, Query, lookups, queries};
use report::{Measurement, QueryTiming, RedactionTiming, Run};
use std::time::Instant;

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    values[values.len() / 2]
}

fn millis(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

fn pad(mut partial: Partial, query: &Query) -> Partial {
    partial.sums.resize(query.fold.len(), 0);
    partial
}

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| default.to_owned())
}

fn main() -> Result<()> {
    let sizes: Vec<usize> = arg("--sizes", "100000,1000000").split(',').map(str::parse).collect::<Result<_, _>>()?;
    let reps: usize = arg("--reps", "5").parse()?;
    let out = arg("--out", "results");
    let write_reps: usize = arg("--write-reps", "3").parse()?;
    let scratch = std::path::PathBuf::from(arg("--scratch", &std::env::temp_dir().to_string_lossy()));
    let formats: Vec<Box<dyn Format>> = vec![
        Box::new(oe::Oe { lazy: false }),
        Box::new(oe::Oe { lazy: true }),
        Box::new(parquet::Parquet),
        Box::new(vortex::Vortex::new()?),
        Box::new(json::Json { zstd_level: None }),
        Box::new(json::Json { zstd_level: Some(3) }),
    ];
    let wanted = arg("--formats", "");
    let profiles = arg("--profiles", "");
    let mut runs = Vec::new();
    for profile in Profile::ALL {
        if !profiles.is_empty() && !profiles.split(',').any(|p| p == profile.name()) {
            continue;
        }
        for &rows in &sizes {
            let dataset = generate(profile, rows);
            let queries = queries(&dataset);
            let lookups = lookups(&dataset);
            let mut expected: Vec<Option<Partial>> = vec![None; queries.len() + lookups.len()];
            let mut measurements = Vec::new();
            for format in &formats {
                if !wanted.is_empty() && !wanted.split(',').any(|w| w == format.name()) {
                    continue;
                }
                eprintln!("{} {} rows {}", profile.name(), rows, format.name());
                let mut write_ms = Vec::new();
                let mut bytes = Vec::new();
                for _ in 0..write_reps {
                    let started = Instant::now();
                    bytes = format.write(&dataset)?;
                    write_ms.push(millis(started));
                }
                let size = bytes.len();
                let mut open_ms = Vec::new();
                let mut opened = None;
                for _ in 0..reps {
                    let copy = bytes.clone();
                    let started = Instant::now();
                    opened = Some(format.open(&dataset, copy)?);
                    open_ms.push(millis(started));
                }
                let opened = opened.expect("at least one rep");
                let mut cold_ms = Vec::new();
                for _ in 0..reps {
                    let copy = bytes.clone();
                    let started = Instant::now();
                    let fresh = format.open(&dataset, copy)?;
                    fresh.query(&lookups[0])?;
                    cold_ms.push(millis(started));
                }
                let mut timings = Vec::new();
                for (index, query) in queries.iter().enumerate() {
                    let mut samples = Vec::new();
                    let mut result = Partial::default();
                    for rep in 0..=reps {
                        let started = Instant::now();
                        result = pad(opened.query(query)?, query);
                        if rep > 0 {
                            samples.push(millis(started));
                        }
                    }
                    check(&mut expected[index], &result, format.name(), query.name)?;
                    timings.push(QueryTiming {
                        name: query.name.to_owned(),
                        matched_rows: result.rows,
                        median_ms: median(samples),
                    });
                }
                let mut grouped: Vec<(String, Vec<f64>, u64)> = Vec::new();
                for (i, lookup) in lookups.iter().enumerate() {
                    let started = Instant::now();
                    let result = pad(opened.query(lookup)?, lookup);
                    let elapsed = millis(started);
                    check(&mut expected[queries.len() + i], &result, format.name(), lookup.name)?;
                    match grouped.iter_mut().find(|(name, _, _)| name == lookup.name) {
                        Some((_, samples, matched)) => {
                            samples.push(elapsed);
                            *matched += result.rows;
                        }
                        None => grouped.push((lookup.name.to_owned(), vec![elapsed], result.rows)),
                    }
                }
                for (name, samples, matched_rows) in grouped {
                    timings.push(QueryTiming {
                        name,
                        matched_rows,
                        median_ms: median(samples),
                    });
                }
                let mut redactions = Vec::new();
                for (name, removed) in [
                    ("redact_1_event", vec![rows / 2]),
                    ("redact_1pct_of_events", (0..rows).step_by(100).collect::<Vec<_>>()),
                ] {
                    let mut samples = Vec::new();
                    let mut written = 0;
                    for _ in 0..write_reps {
                        let started = Instant::now();
                        written = match opened.redact(&dataset, &removed)? {
                            Some(bytes) => bytes,
                            None => {
                                opened.query(&queries[0])?;
                                format.write(&dataset.rows_without(&removed))?.len() as u64
                            }
                        };
                        samples.push(millis(started));
                    }
                    redactions.push(RedactionTiming {
                        bytes_written: written,
                        median_ms: median(samples),
                        name: name.to_owned(),
                        rows_removed: removed.len(),
                    });
                }
                measurements.push(Measurement {
                    cold_lookup_ms: median(cold_ms),
                    format: format.name().to_owned(),
                    open_ms: median(open_ms),
                    queries: timings,
                    redactions,
                    size_bytes: size as u64,
                    write_ms: median(write_ms),
                });
            }
            runs.push(Run {
                measurements,
                profile: profile.name().to_owned(),
                rows,
            });
        }
    }
    let ingest = if arg("--skip-ingest", "no") == "yes" {
        Vec::new()
    } else {
        eprintln!("durable ingest");
        ingest::run(&scratch)?
    };
    report::write(&out, &runs, &ingest, reps, write_reps)?;
    Ok(())
}

fn check(expected: &mut Option<Partial>, got: &Partial, format: &str, query: &str) -> Result<()> {
    match expected {
        None => *expected = Some(got.clone()),
        Some(want) if want == got => {}
        Some(want) => bail!("{format} disagrees on {query}: expected {want:?}, got {got:?}"),
    }
    Ok(())
}
