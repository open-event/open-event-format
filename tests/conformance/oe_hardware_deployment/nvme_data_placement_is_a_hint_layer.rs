use crate::support;
use oe::artifacts::segment::{ReplayTail, replay_segment};

#[test]
fn fdp_absent_at_startup() {
    let mut world = support::World::new(30);
    world.ingest(4);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    assert!(!replay.frames.is_empty(), "frame durable without FDP placement hints");
    assert!(
        replay.segment_chain_blake3.is_some(),
        "BLAKE3 segment chain intact without FDP"
    );
    assert!(
        matches!(replay.tail, ReplayTail::Clean),
        "clean tail without FDP: no truncation or corruption"
    );
}
