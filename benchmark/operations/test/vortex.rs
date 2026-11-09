use super::*;
use crate::dataset::generate;
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

#[test]
fn the_file_holds_the_same_columns_as_the_other_formats() {
    let dir = tempdir().unwrap();
    let spec = spec();
    let dataset = generate(&spec);
    let format = VortexFormat::prepare(&dataset, &spec, dir.path()).unwrap();
    let (file, _) = format.reader(&format.base).unwrap();
    assert_eq!(file.row_count(), spec.row_count as u64);
}
