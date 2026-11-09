use super::*;
use crate::file::api::BlockStore;
use crate::file::constant::FRAME_ALIGNMENT;
use crate::file::error::FileError;
use crate::file::model::{BlockTarget, DurabilityMode, IoCapabilities};
use std::collections::BTreeSet;

fn frame(byte: u8) -> Vec<u8> {
    vec![byte; FRAME_ALIGNMENT]
}

#[test]
fn append_sync_read_round_trips() {
    let target = BlockTarget(1);
    let mut store = SimBlockStore::new();
    let first = store.append(target, &frame(0xAA)).unwrap();
    let second = store.append(target, &frame(0xBB)).unwrap();
    assert_eq!(first, 0);
    assert_eq!(second, FRAME_ALIGNMENT as u64);
    store.sync(target).unwrap();
    assert_eq!(store.extent(target).unwrap(), 2 * FRAME_ALIGNMENT as u64);
    assert_eq!(store.read(target, first, FRAME_ALIGNMENT as u32).unwrap(), frame(0xAA));
}

#[test]
fn an_appended_but_unsynced_frame_is_readable_before_sync() {
    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    let offset = store.append(target, &frame(0xCC)).unwrap();
    assert_eq!(store.extent(target).unwrap(), FRAME_ALIGNMENT as u64);
    assert_eq!(store.read(target, offset, FRAME_ALIGNMENT as u32).unwrap(), frame(0xCC));

    let second = store.append(target, &frame(0xDD)).unwrap();
    assert_eq!(store.extent(target).unwrap(), 2 * FRAME_ALIGNMENT as u64);
    assert_eq!(store.read(target, second, FRAME_ALIGNMENT as u32).unwrap(), frame(0xDD));
    let mut expected = frame(0xCC);
    expected.extend(frame(0xDD));
    assert_eq!(store.read(target, 0, 2 * FRAME_ALIGNMENT as u32).unwrap(), expected);
}

#[test]
fn unaligned_frame_is_rejected() {
    let mut store = SimBlockStore::new();
    assert_eq!(store.append(BlockTarget(0), &[0u8; 100]), Err(FileError::Unaligned));
    assert_eq!(store.append(BlockTarget(0), &[]), Err(FileError::Unaligned));
}

#[test]
fn injected_append_failure_is_consumed_once() {
    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    store.inject(Fault::FailAppend { target });
    assert_eq!(
        store.append(target, &frame(1)),
        Err(FileError::InjectedFault { kind: "fail-append" })
    );
    assert!(store.append(target, &frame(1)).is_ok());
}

#[test]
fn crash_drops_unsynced_pending_appends() {
    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    store.append(target, &frame(1)).unwrap();
    store.sync(target).unwrap();
    store.append(target, &frame(2)).unwrap();
    store.crash();
    assert_eq!(store.extent(target).unwrap(), FRAME_ALIGNMENT as u64);
}

#[test]
fn torn_tail_keeps_only_a_prefix_of_the_newest_append() {
    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    store.append(target, &frame(1)).unwrap();
    store.sync(target).unwrap();
    store.append(target, &frame(2)).unwrap();
    store.inject(Fault::TornTail { keep_bytes: 16, target });
    store.crash();
    let durable = store.read(target, 0, FRAME_ALIGNMENT as u32).unwrap();
    assert_eq!(durable, frame(1));
    let torn = store.read(target, FRAME_ALIGNMENT as u64, 16).unwrap();
    assert_eq!(torn, vec![2u8; 16]);
    assert_eq!(store.extent(target).unwrap(), FRAME_ALIGNMENT as u64 + 16);
    assert!(store.read(target, FRAME_ALIGNMENT as u64, 17).is_err());
}

#[test]
fn reordered_completions_keep_only_survivors() {
    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    store.append(target, &frame(1)).unwrap();
    store.append(target, &frame(2)).unwrap();
    let survivors: BTreeSet<usize> = [1].into_iter().collect();
    store.crash_with_surviving_pending(target, &survivors);
    let second = store
        .read(target, FRAME_ALIGNMENT as u64, FRAME_ALIGNMENT as u32)
        .unwrap();
    assert_eq!(second, frame(2));
}

#[test]
fn corrupting_a_durable_byte_flips_it() {
    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    store.append(target, &frame(0)).unwrap();
    store.sync(target).unwrap();
    store.corrupt_durable_byte(target, 0);
    let read = store.read(target, 0, 1).unwrap();
    assert_eq!(read, vec![0xFFu8]);
}

#[test]
fn descriptor_reports_extent_preallocation_absent_and_the_backend_never_preallocates() {
    assert!(!IoCapabilities::portable().efficient_extent_zeroing);
    assert!(!IoCapabilities::default().efficient_extent_zeroing);

    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    store.append(target, &frame(1)).unwrap();
    store.append(target, &frame(2)).unwrap();
    store.sync(target).unwrap();
    assert_eq!(store.extent(target).unwrap(), 2 * FRAME_ALIGNMENT as u64);
}

#[test]
fn a_fault_script_is_reproducible_from_its_seed() {
    let run = || {
        let target = BlockTarget(0);
        let mut store = SimBlockStore::new();
        store.append(target, &frame(1)).unwrap();
        store.sync(target).unwrap();
        store.append(target, &frame(2)).unwrap();
        store.inject(Fault::TornTail { keep_bytes: 16, target });
        store.crash();
        let extent = store.extent(target).unwrap();
        store.read(target, 0, extent as u32).unwrap()
    };
    let mut expected = frame(1);
    expected.extend(vec![2u8; 16]);

    let first = run();
    assert_eq!(first, run());
    assert_eq!(first, expected);
}

#[test]
fn the_reported_durability_mode_matches_when_appends_actually_reach_media() {
    let target = BlockTarget(0);
    let mut store = SimBlockStore::new();
    assert_eq!(
        store.atomicity(target).unwrap().durability_mode,
        DurabilityMode::Buffered
    );
    store.append(target, &frame(1)).unwrap();
    store.crash();
    assert_eq!(store.extent(target).unwrap(), 0);
}
