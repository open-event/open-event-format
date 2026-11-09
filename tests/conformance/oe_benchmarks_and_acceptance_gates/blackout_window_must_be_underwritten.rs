use oe::benchmarks::min_blackout_seconds;

#[test]
fn blackout_window_must_be_underwritten() {
    let corpus = 1_000_000_000_000_u64;
    let throughput = 100_000_000_000_u64;
    let min = min_blackout_seconds(corpus, throughput);
    assert_eq!(min, 10);

    assert_eq!(min_blackout_seconds(10, 3), 4);

    let declared_window = 8_u64;
    assert!(
        declared_window < min,
        "a window of {declared_window}s is shorter than the {min}s minimum and must not be accepted"
    );

    assert_eq!(min, 10);
}
