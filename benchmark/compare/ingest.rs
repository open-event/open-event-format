//! Durable ingest: how fast each format can take a stream of small batches and acknowledge each one only after it
//! is on disk (file synced). OE's journal appends one aligned frame per batch to a single file; JSON appends lines;
//! Parquet and Vortex cannot append, so each batch becomes its own small file.

use crate::dataset::{Profile, generate};
use crate::format::Format;
use crate::json::Json;
use crate::oe::{EventColumns, journal_frame};
use crate::parquet::Parquet;
use crate::vortex::Vortex;
use anyhow::Result;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

pub const BATCHES: usize = 300;
pub const BATCH_SIZES: [usize; 3] = [1, 16, 128];

pub struct IngestResult {
    pub batch_size: usize,
    pub bytes_on_disk: u64,
    pub events_per_s: f64,
    pub format: &'static str,
    pub p50_ms: f64,
    pub p99_ms: f64,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

pub fn run(scratch: &Path) -> Result<Vec<IngestResult>> {
    let vortex = Vortex::new()?;
    let json = Json { zstd_level: None };
    let mut results = Vec::new();
    for batch_size in BATCH_SIZES {
        let dataset = generate(Profile::Chat, BATCHES * batch_size);
        let columns = EventColumns::new(&dataset);
        for name in ["oej journal", "json (append)", "parquet (file per batch)", "vortex (file per batch)"] {
            let dir = scratch.join(format!("oe-ingest-{}", std::process::id()));
            fs::create_dir_all(&dir)?;
            let mut journal: Option<File> = match name {
                "oej journal" | "json (append)" => Some(OpenOptions::new().create(true).append(true).open(dir.join("log"))?),
                _ => None,
            };
            let mut latencies = Vec::with_capacity(BATCHES);
            let mut bytes = 0u64;
            let run_started = Instant::now();
            for batch in 0..BATCHES {
                let first = batch * batch_size;
                let started = Instant::now();
                match name {
                    "oej journal" => {
                        let events = (first..first + batch_size).map(|i| columns.input(i)).collect::<Result<Vec<_>>>()?;
                        let frame = journal_frame(&events, first, batch as u64)?;
                        bytes += frame.len() as u64;
                        let file = journal.as_mut().expect("journal open");
                        file.write_all(&frame)?;
                        file.sync_data()?;
                    }
                    "json (append)" => {
                        let lines = json.write(&dataset.slice(first, first + batch_size))?;
                        bytes += lines.len() as u64;
                        let file = journal.as_mut().expect("journal open");
                        file.write_all(&lines)?;
                        file.sync_data()?;
                    }
                    _ => {
                        let slice = dataset.slice(first, first + batch_size);
                        let encoded = if name.starts_with("parquet") { Parquet.write(&slice)? } else { vortex.write(&slice)? };
                        bytes += encoded.len() as u64;
                        let mut file = File::create(dir.join(format!("batch-{batch}")))?;
                        file.write_all(&encoded)?;
                        file.sync_all()?;
                    }
                }
                latencies.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            let total = run_started.elapsed().as_secs_f64();
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            results.push(IngestResult {
                batch_size,
                bytes_on_disk: bytes,
                events_per_s: (BATCHES * batch_size) as f64 / total,
                format: name,
                p50_ms: percentile(&latencies, 0.5),
                p99_ms: percentile(&latencies, 0.99),
            });
            drop(journal);
            fs::remove_dir_all(&dir)?;
        }
    }
    Ok(results)
}
