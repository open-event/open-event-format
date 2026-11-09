use super::super::clustering::clustering_preserves_sequence_coverage;

#[test]
fn empty_rows_preserve_coverage() {
    assert!(clustering_preserves_sequence_coverage(&[]));
}

#[test]
fn single_bucket_preserves_coverage() {
    let rows = [(1, 0), (2, 0), (3, 0)];
    assert!(clustering_preserves_sequence_coverage(&rows));
}

#[test]
fn contiguous_buckets_preserve_coverage() {
    let rows = [(1, 0), (2, 0), (3, 0), (4, 1), (5, 1), (6, 1)];
    assert!(clustering_preserves_sequence_coverage(&rows));
}

#[test]
fn interleaved_buckets_break_coverage() {
    let rows = [(1, 0), (2, 1), (3, 0)];
    assert!(!clustering_preserves_sequence_coverage(&rows));
}
