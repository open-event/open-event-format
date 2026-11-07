//! Checks that what queries can see is decided by the published catalogue, not by which files happen to exist on
//! storage. A finished file that no published catalogue entry references yet is invisible, and that range keeps being
//! served from the recent in-memory store instead.
use crate::support;
use oe::artifacts::overlay::LiveOverlayStore;
use oe::artifacts::segment::replay_segment;
use oe::invariants::PublishedSet;
use oe::invariants::sim::SimulatedPublishedSet;

/// conformance: oe-core-invariants/source-of-truth-and-visibility-ordering/unpublished-oe-is-not-visible
#[test]
fn unpublished_oe_is_not_visible() {
    // A built OE file "exists on storage" but no manifest references it: the manifest, not file existence, defines
    // visibility, and the journal range is still served from OEJ/LiveOverlay.
    let mut world = support::World::new(11);
    let range = world.ingest(4);
    let _file_on_storage = support::built_file(4);
    let head = SimulatedPublishedSet::new().head().unwrap().1;
    assert!(!head.covers(&range, support::tenant()));
    assert_eq!(head.snapshot_files().count(), 0);
    let replay = replay_segment(&world.storage, support::SHARD, 1, 1).unwrap();
    let mut overlay = LiveOverlayStore::new();
    overlay
        .rebuild_from_replay(&replay, 1, 1, |t, r| head.covers(r, t))
        .unwrap();
    assert_eq!(overlay.segment_count(), 1, "still served from LiveOverlay");
}
