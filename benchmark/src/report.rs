//! Writes the measurements as `results.json` (for tools) and `results.md` (for people).

use crate::ingest::IngestResult;
use anyhow::Result;
use std::fmt::Write;
use std::fs;
use std::process::Command;

pub struct Measurement {
    pub cold_lookup_ms: f64,
    pub format: String,
    pub open_ms: f64,
    pub queries: Vec<QueryTiming>,
    pub redactions: Vec<RedactionTiming>,
    pub size_bytes: u64,
    pub write_ms: f64,
}

pub struct RedactionTiming {
    pub bytes_written: u64,
    pub median_ms: f64,
    pub name: String,
    pub rows_removed: usize,
}

pub struct QueryTiming {
    pub matched_rows: u64,
    pub median_ms: f64,
    pub name: String,
}

pub struct Run {
    pub measurements: Vec<Measurement>,
    pub profile: String,
    pub rows: usize,
}

fn command(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn cpu() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split(':').nth(1))
        .map(|s| s.trim().to_owned())
        .unwrap_or_default()
}

pub fn write(out: &str, runs: &[Run], ingest: &[IngestResult], reps: usize, write_reps: usize) -> Result<()> {
    fs::create_dir_all(out)?;
    let machine = format!(
        "{} ({} logical CPUs), {}, {}",
        cpu(),
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        command("uname", &["-sr"]),
        command("rustc", &["--version"])
    );
    let mut json = String::new();
    writeln!(json, "{{\n  \"machine\": {:?},\n  \"query_repetitions\": {reps},\n  \"write_repetitions\": {write_reps},\n  \"runs\": [", machine)?;
    for (i, run) in runs.iter().enumerate() {
        writeln!(json, "    {{\"profile\": {:?}, \"rows\": {}, \"formats\": [", run.profile, run.rows)?;
        for (j, m) in run.measurements.iter().enumerate() {
            let queries: Vec<String> = m
                .queries
                .iter()
                .map(|q| format!("{{\"name\": {:?}, \"matched_rows\": {}, \"median_ms\": {:.3}}}", q.name, q.matched_rows, q.median_ms))
                .collect();
            let redactions: Vec<String> = m
                .redactions
                .iter()
                .map(|x| format!("{{\"name\": {:?}, \"rows_removed\": {}, \"median_ms\": {:.3}, \"bytes_written\": {}}}", x.name, x.rows_removed, x.median_ms, x.bytes_written))
                .collect();
            writeln!(
                json,
                "      {{\"format\": {:?}, \"size_bytes\": {}, \"bytes_per_event\": {:.2}, \"write_ms\": {:.1}, \"write_events_per_s\": {:.0}, \"open_ms\": {:.3}, \"open_and_first_lookup_ms\": {:.3}, \"queries\": [{}], \"redactions\": [{}]}}{}",
                m.format,
                m.size_bytes,
                m.size_bytes as f64 / run.rows as f64,
                m.write_ms,
                run.rows as f64 / (m.write_ms / 1000.0),
                m.open_ms,
                m.cold_lookup_ms,
                queries.join(", "),
                redactions.join(", "),
                if j + 1 < run.measurements.len() { "," } else { "" }
            )?;
        }
        writeln!(json, "    ]}}{}", if i + 1 < runs.len() { "," } else { "" })?;
    }
    writeln!(json, "  ],\n  \"ingest\": [")?;
    for (i, x) in ingest.iter().enumerate() {
        writeln!(
            json,
            "    {{\"format\": {:?}, \"batch_size\": {}, \"events_per_s\": {:.0}, \"ack_p50_ms\": {:.3}, \"ack_p99_ms\": {:.3}, \"bytes_on_disk\": {}}}{}",
            x.format, x.batch_size, x.events_per_s, x.p50_ms, x.p99_ms, x.bytes_on_disk,
            if i + 1 < ingest.len() { "," } else { "" }
        )?;
    }
    writeln!(json, "  ]\n}}")?;
    fs::write(format!("{out}/results.json"), json)?;

    let mut md = String::new();
    writeln!(md, "# Benchmark results\n")?;
    writeln!(md, "Machine: {machine}\n")?;
    writeln!(
        md,
        "Every format stores the same events. Query times are the median of {reps} runs after one warm-up; write times are the median of {write_reps} runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).\n"
    )?;
    for run in runs {
        writeln!(md, "## {} events: {} rows\n", run.profile, run.rows)?;
        let json_size = run.measurements.iter().find(|m| m.format == "json").map(|m| m.size_bytes);
        writeln!(md, "| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |")?;
        writeln!(md, "|---|---:|---:|---:|---:|---:|---:|---:|")?;
        for m in &run.measurements {
            writeln!(
                md,
                "| {} | {:.2} | {:.1} | {} | {:.0} | {:.0} | {:.2} | {:.2} |",
                m.format,
                m.size_bytes as f64 / 1_048_576.0,
                m.size_bytes as f64 / run.rows as f64,
                json_size.map(|j| format!("{:.1}%", m.size_bytes as f64 * 100.0 / j as f64)).unwrap_or_default(),
                m.write_ms,
                run.rows as f64 / (m.write_ms / 1000.0),
                m.open_ms,
                m.cold_lookup_ms
            )?;
        }
        writeln!(md)?;
        let names: Vec<&str> = run
            .measurements
            .first()
            .map(|m| m.queries.iter().map(|q| q.name.as_str()).collect())
            .unwrap_or_default();
        write!(md, "| Query (median ms; entity_lookup is per lookup) | Rows matched |")?;
        for m in &run.measurements {
            write!(md, " {} |", m.format)?;
        }
        writeln!(md)?;
        write!(md, "|---|---:|")?;
        for _ in &run.measurements {
            write!(md, "---:|")?;
        }
        writeln!(md)?;
        for (i, name) in names.iter().enumerate() {
            let matched = run.measurements.first().map(|m| m.queries[i].matched_rows).unwrap_or(0);
            write!(md, "| {name} | {matched} |")?;
            for m in &run.measurements {
                write!(md, " {:.2} |", m.queries[i].median_ms)?;
            }
            writeln!(md)?;
        }
        writeln!(md)?;
        writeln!(md, "Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.\n")?;
        write!(md, "| Case | Events removed |")?;
        for m in &run.measurements {
            write!(md, " {} (ms) | {} (bytes written) |", m.format, m.format)?;
        }
        writeln!(md)?;
        write!(md, "|---|---:|")?;
        for _ in &run.measurements {
            write!(md, "---:|---:|")?;
        }
        writeln!(md)?;
        if let Some(first) = run.measurements.first() {
            for (i, case) in first.redactions.iter().enumerate() {
                write!(md, "| {} | {} |", case.name, case.rows_removed)?;
                for m in &run.measurements {
                    write!(md, " {:.2} | {} |", m.redactions[i].median_ms, m.redactions[i].bytes_written)?;
                }
                writeln!(md)?;
            }
        }
        writeln!(md)?;
    }
    if !ingest.is_empty() {
        writeln!(md, "## Durable ingest\n")?;
        writeln!(md, "Each batch is acknowledged only after the file is synced to disk ({} batches per row). Parquet and Vortex cannot append, so each batch is a new file.\n", crate::ingest::BATCHES)?;
        writeln!(md, "| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |")?;
        writeln!(md, "|---:|---|---:|---:|---:|---:|")?;
        for x in ingest {
            writeln!(md, "| {} | {} | {:.0} | {:.3} | {:.3} | {} |", x.batch_size, x.format, x.events_per_s, x.p50_ms, x.p99_ms, x.bytes_on_disk)?;
        }
        writeln!(md)?;
    }
    fs::write(format!("{out}/results.md"), md)?;
    Ok(())
}
