use oe::benchmarks::{AcceleratorConditions, AcceleratorPath, accelerator_path};

fn all_passing() -> AcceleratorConditions {
    AcceleratorConditions {
        batch_exceeds_threshold: true,
        benefit_is_positive: true,
        encoding_compatible: true,
        fallback_available: true,
        parity_passed: true,
        provider_detected: true,
        self_test_passed: true,
    }
}

#[test]
fn accelerator_falls_back_on_failed_parity() {
    assert_eq!(accelerator_path(&all_passing()), AcceleratorPath::Accelerated);

    assert_eq!(
        accelerator_path(&AcceleratorConditions {
            parity_passed: false,
            ..all_passing()
        }),
        AcceleratorPath::Software
    );

    assert_eq!(
        accelerator_path(&AcceleratorConditions {
            provider_detected: false,
            ..all_passing()
        }),
        AcceleratorPath::Software
    );

    assert_eq!(
        accelerator_path(&AcceleratorConditions {
            self_test_passed: false,
            ..all_passing()
        }),
        AcceleratorPath::Software
    );

    assert_eq!(
        accelerator_path(&AcceleratorConditions {
            encoding_compatible: false,
            ..all_passing()
        }),
        AcceleratorPath::Software
    );

    assert_eq!(
        accelerator_path(&AcceleratorConditions {
            batch_exceeds_threshold: false,
            ..all_passing()
        }),
        AcceleratorPath::Software
    );

    assert_eq!(
        accelerator_path(&AcceleratorConditions {
            benefit_is_positive: false,
            ..all_passing()
        }),
        AcceleratorPath::Software
    );

    assert_eq!(
        accelerator_path(&AcceleratorConditions {
            fallback_available: false,
            ..all_passing()
        }),
        AcceleratorPath::Software
    );
}
