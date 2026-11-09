use oe::invariants::sim::{SerialEncodeExecutor, SimulatedPublishedSet};
use oe::writer::publish::{NoopPeerNotices, OePublisher, RecordingObserver, journal_retention_can_advance};

use crate::support;
use oe::invariants::PublishedSet;
#[test]
fn re_publish_same_range() {
    let mut world = support::World::new(111);
    let range = world.ingest(4);
    let mut publisher = OePublisher::new();
    let mut set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let config = support::build_config();
    let first = publisher
        .publish_range(
            &world.storage,
            support::SHARD,
            1,
            1,
            None,
            range,
            &config,
            &mut set,
            &oe::object_store::sim::SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    let second = publisher
        .publish_range(
            &world.storage,
            support::SHARD,
            1,
            1,
            None,
            range,
            &config,
            &mut set,
            &oe::object_store::sim::SimObjectStore::new(),
            &mut observer,
            &mut notices,
            &world.clock,
            &SerialEncodeExecutor,
        )
        .unwrap();
    assert_eq!(first.entry.file_id, second.entry.file_id);
    let (head_id, head) = set.head().unwrap();
    assert_eq!(head_id, first.generation, "no extra generation");
    assert_eq!(head.files.len(), 1, "no double-counted coverage");
    assert_eq!(notices.published.len(), 1, "one peer notice");
    assert!(!journal_retention_can_advance(
        &head,
        &range,
        support::tenant(),
        false,
        false
    ));
    assert!(journal_retention_can_advance(
        &head,
        &range,
        support::tenant(),
        false,
        true
    ));
}
