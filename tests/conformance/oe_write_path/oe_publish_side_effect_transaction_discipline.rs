use oe::writer::publish::{NoopPeerNotices, OePublisher, RecordingObserver};

use crate::support;
use oe::invariants::sim::{SerialEncodeExecutor, SimulatedPublishedSet};
#[test]
fn side_effect_rolled_back_on_lost_publish() {
    let mut world = support::World::new(131);
    let range = world.ingest(2);
    let mut loser = OePublisher::new();
    loser.quota_bytes = Some(1);
    let mut set = SimulatedPublishedSet::new();
    let mut observer = RecordingObserver::default();
    let mut notices = NoopPeerNotices::default();
    let config = support::build_config();
    let _ = loser
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
        .unwrap_err();
    assert_eq!(observer.staged, vec![1]);
    assert_eq!(observer.rolled_back, vec![1], "rollback before serving reads");
    assert!(observer.promoted.is_empty());
    let mut winner = OePublisher::new();
    let mut observer = RecordingObserver::default();
    winner
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
    assert_eq!(observer.staged, vec![1]);
    assert_eq!(observer.promoted, vec![1]);
    assert!(observer.rolled_back.is_empty());
}
