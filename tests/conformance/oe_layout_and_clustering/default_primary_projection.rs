use oe::layout::clustering::clustering_preserves_sequence_coverage;

#[test]
fn clustering_preserves_sequence_coverage_test() {
    let non_interleaved: Vec<(u64, u32)> = vec![(1, 0), (2, 0), (3, 0), (4, 0), (5, 1), (6, 1), (7, 1), (8, 1)];
    assert!(
        clustering_preserves_sequence_coverage(&non_interleaved),
        "contiguous bucket ownership: clustering is safe and should be applied"
    );

    let interleaved: Vec<(u64, u32)> = vec![(1, 0), (2, 1), (3, 0)];
    assert!(
        !clustering_preserves_sequence_coverage(&interleaved),
        "interleaved buckets break coverage: clustering must be rejected"
    );
}
