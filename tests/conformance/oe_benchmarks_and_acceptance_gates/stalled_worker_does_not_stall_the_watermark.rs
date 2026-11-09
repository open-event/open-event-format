use oe::artifacts::watermark::WatermarkTracker;
use oe::writer::reserve::{LEASE_NANOS, SequenceAllocator};

#[test]
fn stalled_worker_does_not_stall_the_watermark() {
    let mut allocator = SequenceAllocator::new(1);
    let mut watermarks = WatermarkTracker::new();

    let _stalled_lease = allocator.reserve(10, 0);

    let b_lease = allocator.reserve(5, 0);
    let b_range = allocator.harden(b_lease.lease_id, 0).unwrap();
    watermarks.record_durable(b_range);

    assert!(
        watermarks.commit_watermark().is_none(),
        "watermark cannot advance past the gap left by the stalled worker"
    );

    let expired = allocator.expire(LEASE_NANOS + 1);
    assert_eq!(expired.len(), 1, "one abandoned range recovered");
    let void_range = expired[0];

    watermarks.record_durable(void_range);
    let wm = watermarks
        .commit_watermark()
        .expect("watermark advances after void record");
    assert!(
        wm.sequence >= b_range.last_sequence,
        "watermark ({}) must reach at least the end of B's range ({})",
        wm.sequence,
        b_range.last_sequence,
    );
}
