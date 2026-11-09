use oe::invariants::sim::{SerialEncodeExecutor, SimulatedPublishedSet};
use oe::writer::publish::{NoopPeerNotices, OePublisher, PublishFailure, RecordingObserver};

use crate::support;
use oe::invariants::PublishedSet;
#[test]
fn failed_publish_leaks_nothing() {
    let mut world = support::World::new(121);
    let range = world.ingest(3);
    let mut publisher = OePublisher::new();
    publisher.quota_bytes = Some(16);
    let mut set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let error = publisher
        .publish_range(
            &world.storage,
            support::SHARD,
            1,
            1,
            None,
            range,
            &support::build_config(),
            &mut set,
            &oe::object_store::sim::SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap_err();
    assert!(matches!(error, PublishFailure::Verification(_)));
    assert!(notices.published.is_empty(), "no peer notice leaked");
    assert_eq!(observer.staged, vec![1]);
    assert_eq!(observer.rolled_back, vec![1]);
    assert!(observer.promoted.is_empty());
    let (generation, head) = set.head().unwrap();
    assert_eq!(generation, 0);
    assert!(head.files.is_empty(), "manifest unchanged");
}
