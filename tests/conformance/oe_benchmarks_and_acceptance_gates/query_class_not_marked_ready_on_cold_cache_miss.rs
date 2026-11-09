use oe::benchmarks::{QueryClassMeasurement, query_class_is_ready};

#[test]
fn query_class_not_marked_ready_on_cold_cache_miss() {
    assert!(!query_class_is_ready(&QueryClassMeasurement {
        cold_cache_passes: false,
        explicitly_excluded: false,
        warm_cache_passes: true,
    }));

    assert!(!query_class_is_ready(&QueryClassMeasurement {
        cold_cache_passes: false,
        explicitly_excluded: false,
        warm_cache_passes: false,
    }));

    assert!(query_class_is_ready(&QueryClassMeasurement {
        cold_cache_passes: true,
        explicitly_excluded: false,
        warm_cache_passes: true,
    }));

    assert!(query_class_is_ready(&QueryClassMeasurement {
        cold_cache_passes: false,
        explicitly_excluded: true,
        warm_cache_passes: false,
    }));
}
