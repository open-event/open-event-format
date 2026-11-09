use oe::benchmarks::{
    BenchGateRegistry, CommittedBaseline, PrunedFileMetadataMeasurement, pruned_file_metadata_gate_passes,
};

#[test]
fn pruned_file_costs_near_zero_planner_metadata() {
    assert!(pruned_file_metadata_gate_passes(&PrunedFileMetadataMeasurement {
        full_granule_marks_share_bytes: 4096,
        planner_metadata_bytes_read: 0,
    }));

    assert!(!pruned_file_metadata_gate_passes(&PrunedFileMetadataMeasurement {
        full_granule_marks_share_bytes: 4096,
        planner_metadata_bytes_read: 4096,
    }));

    assert!(!pruned_file_metadata_gate_passes(&PrunedFileMetadataMeasurement {
        full_granule_marks_share_bytes: 4096,
        planner_metadata_bytes_read: 5000,
    }));

    let mut registry = BenchGateRegistry::default();
    registry.register_benchmark("pruned-file-planner-bytes", "bench:oe-pruned-file-planner-bytes");
    registry.commit_baseline(CommittedBaseline {
        benchmark_id: "bench:oe-pruned-file-planner-bytes".to_owned(),
        equivalence_verified: false,
        measured_value: 0.0,
        tolerance_fraction: 0.0,
    });
    registry.mark_ready("pruned-file-planner-bytes").unwrap();
    assert!(registry.is_ready("pruned-file-planner-bytes"));

    let regression = registry
        .detect_regression(
            "pruned-file-planner-bytes",
            "bench:oe-pruned-file-planner-bytes",
            4096.0,
        )
        .expect("planner-metadata-bytes regression detected");
    assert_eq!(regression.benchmark_id, "bench:oe-pruned-file-planner-bytes");
    assert_eq!(regression.baseline_value, 0.0);
    assert_eq!(regression.measured_value, 4096.0);
    assert!(
        !registry.is_ready("pruned-file-planner-bytes"),
        "pruned-file path must leave the ready set once planner bytes regress above the near-zero budget"
    );
}
