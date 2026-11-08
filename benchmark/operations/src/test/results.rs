use super::*;
use crate::model::{BenchmarkSpec, Environment, FormatFile, PersistedRun};

fn report() -> BenchmarkReport {
    BenchmarkReport {
        environment: Environment::default(),
        files: vec![FormatFile {
            bytes: 100,
            format: "oe".to_owned(),
        }],
        operations: Vec::new(),
        resident_high_water_bytes: None,
        spec: BenchmarkSpec::default(),
    }
}

#[test]
fn each_run_appends_one_parseable_line_with_a_distinct_run_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("results.jsonl");

    append_run(&report(), &path).unwrap();
    append_run(&report(), &path).unwrap();

    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);

    let runs: Vec<PersistedRun> = lines.iter().map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(runs[0].report, report());
    assert_eq!(runs[1].report, report());
    assert_ne!(runs[0].run_id, runs[1].run_id);
}
