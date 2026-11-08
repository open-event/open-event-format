//! Persists one run's report as an appended JSON line, so a number cited from a run stays a checkable, comparable
//! record instead of living only in a terminal or a markdown table.

use crate::model::{BenchmarkReport, PersistedRun};
use anyhow::{Context, Result};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// Appends `report` as one JSON line to `path`, stamped with a fresh run id and the current time. Creates `path` and
/// its parent directory if they do not exist yet; existing lines are never read or rewritten.
pub fn append_run(report: &BenchmarkReport, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let run = PersistedRun {
        report: report.clone(),
        run_id: uuid::Uuid::new_v4().to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };
    let line = serde_json::to_string(&run).context("rendering the run as JSON")?;
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    writeln!(file, "{line}").with_context(|| format!("appending to {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
#[path = "test/results.rs"]
mod tests;
