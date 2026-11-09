use oe::artifacts::watermark::WatermarkTracker;
use oe::events::{SequencePoint, SequenceRange};

#[test]
fn snapshot_bound_respected() {
    let mut tracker = WatermarkTracker::new();
    tracker.record_durable(SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 8,
    });
    tracker.record_visible(SequenceRange {
        epoch: 1,
        first_sequence: 1,
        last_sequence: 3,
    });
    tracker.record_visible(SequenceRange {
        epoch: 1,
        first_sequence: 4,
        last_sequence: 5,
    });
    let visibility = tracker.visibility_watermark().unwrap();
    assert_eq!(visibility, SequencePoint { epoch: 1, sequence: 5 });
    let snapshot = tracker.capture_snapshot_watermark().unwrap();
    assert!(snapshot <= visibility);
    assert_eq!(tracker.durable_journal_cursor(), tracker.commit_watermark());
    assert_eq!(
        tracker.commit_watermark(),
        Some(SequencePoint { epoch: 1, sequence: 8 })
    );
    assert_eq!(tracker.live_queryable_cursor(), Some(visibility));
}
