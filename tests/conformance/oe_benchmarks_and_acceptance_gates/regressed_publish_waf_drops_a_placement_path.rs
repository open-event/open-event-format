use oe::benchmarks::{BenchGateRegistry, CommittedBaseline};

#[test]
fn regressed_publish_waf_drops_a_placement_path() {
    let durable_bytes = vec![0xAB_u8; 4096];
    let durable_checksum_before = blake3::hash(&durable_bytes);

    let mut registry = BenchGateRegistry::default();
    registry.register_benchmark("fdp-zns-publish", "bench:oe-fresh-publish-waf");
    registry.commit_baseline(CommittedBaseline {
        benchmark_id: "bench:oe-fresh-publish-waf".to_owned(),
        equivalence_verified: false,
        measured_value: 1.1,
        tolerance_fraction: 0.10,
    });
    registry.mark_ready("fdp-zns-publish").unwrap();
    assert!(registry.is_ready("fdp-zns-publish"));

    let regression = registry.detect_regression("fdp-zns-publish", "bench:oe-fresh-publish-waf", 1.5);
    assert!(regression.is_some(), "WAF regression detected");
    assert!(
        !registry.is_ready("fdp-zns-publish"),
        "FDP/ZNS publish path must leave the ready set after a WAF regression"
    );

    assert_eq!(durable_bytes.len(), 4096);
    assert_eq!(blake3::hash(&durable_bytes), durable_checksum_before);
}
