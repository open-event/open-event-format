use super::*;
use crate::typed_id::TypedIdTestExt;
use std::io::Write;

fn receipt(tenant: u128, delivery: u64, dedupe: u64, expiry: i64, status_class: StatusClass) -> RetryReceipt {
    RetryReceipt {
        commit: SequencePoint {
            epoch: 3,
            sequence: delivery,
        },
        dedupe: (dedupe, 7),
        delivery_identity: (delivery, 1),
        expiry_physical_nanos: expiry,
        frame_offset: delivery * 4096,
        shard: ShardId(2),
        status_class,
        tenant_id: TenantId::new_test_id(tenant),
    }
}

#[test]
fn log_entries_round_trip() {
    for status in [
        StatusClass::Acknowledged,
        StatusClass::Indeterminate,
        StatusClass::Rejected,
    ] {
        let entry = LogEntry::Record(receipt(9, 4, 5, -6, status));
        let (entries, good) = parse_entries(&entry.encode());
        assert_eq!(entries, vec![entry.clone()]);
        assert_eq!(good, RETRY_LOG_HEADER_LEN + entry.encode().len() as u64);
    }
    let evict = LogEntry::Evict(i64::MIN);
    assert_eq!(parse_entries(&evict.encode()).0, vec![evict]);
}

#[test]
fn receipts_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let ack = receipt(9, 1, 100, 1_000, StatusClass::Acknowledged);
    let indeterminate = receipt(9, 2, 200, 1_000, StatusClass::Indeterminate);
    {
        let mut store = FileSafeRetryStore::open(dir.path().join("retry")).unwrap();
        store.record(ack.clone());
        store.record(indeterminate.clone());
        let mut moved = ack.clone();
        moved.frame_offset = 99;
        store.record(moved);
        store.sync().unwrap();
    }
    let mut store = FileSafeRetryStore::open(dir.path().join("retry")).unwrap();
    assert_eq!(store.len(), 2);
    assert_eq!(
        store.lookup(ack.tenant_id, ack.delivery_identity).unwrap().frame_offset,
        99
    );
    assert_eq!(
        store.lookup(indeterminate.tenant_id, indeterminate.delivery_identity),
        Some(indeterminate.clone())
    );
    assert!(store.duplicate_within_guard(ack.tenant_id, ack.dedupe, 500));
    assert!(!store.duplicate_within_guard(indeterminate.tenant_id, indeterminate.dedupe, 500));
    assert!(store.take_failure().is_none());
}

#[test]
fn tenants_are_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let a = receipt(9, 1, 100, 1_000, StatusClass::Acknowledged);
    let mut b = receipt(10, 1, 100, 1_000, StatusClass::Rejected);
    b.commit.sequence = 50;
    {
        let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
        store.record(a.clone());
        store.record(b.clone());
    }
    let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(store.lookup(a.tenant_id, a.delivery_identity), Some(a.clone()));
    assert_eq!(store.lookup(b.tenant_id, b.delivery_identity), Some(b.clone()));
    assert!(store.duplicate_within_guard(a.tenant_id, a.dedupe, 0));
    assert!(!store.duplicate_within_guard(b.tenant_id, b.dedupe, 0));
    assert!(store.lookup(TenantId::new_test_id(11), a.delivery_identity).is_none());
}

#[test]
fn expiry_eviction_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let early = receipt(9, 1, 100, 1_000, StatusClass::Acknowledged);
    let late = receipt(9, 2, 200, 5_000, StatusClass::Acknowledged);
    {
        let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
        store.record(early.clone());
        store.record(late.clone());
        assert!(!store.duplicate_within_guard(early.tenant_id, early.dedupe, 2_000));
        assert_eq!(store.len(), 1);
        let entries = store.log_entries();
        store.evict_expired(2_000);
        assert_eq!(store.log_entries(), entries);
    }
    let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(store.len(), 1);
    assert!(store.lookup(early.tenant_id, early.delivery_identity).is_none());
    assert!(store.duplicate_within_guard(late.tenant_id, late.dedupe, 2_000));
    store.evict_expired(5_000);
    assert!(store.is_empty());
    drop(store);
    assert!(FileSafeRetryStore::open(dir.path()).unwrap().is_empty());
}

#[test]
fn a_torn_tail_is_cut_back_to_the_last_whole_record() {
    let dir = tempfile::tempdir().unwrap();
    let kept = receipt(9, 1, 100, 1_000, StatusClass::Acknowledged);
    let torn = receipt(9, 2, 200, 1_000, StatusClass::Acknowledged);
    let (path, good_len) = {
        let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
        store.record(kept.clone());
        let good_len = store.log_len;
        store.record(torn.clone());
        (store.log_path(), good_len)
    };
    let full = std::fs::metadata(&path).unwrap().len();
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(full - 10).unwrap();
    drop(file);

    let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(store.len(), 1);
    assert!(store.lookup(torn.tenant_id, torn.delivery_identity).is_none());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);
    store.record(torn.clone());
    drop(store);
    let store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(store.len(), 2);
}

#[test]
fn a_corrupt_record_ends_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = {
        let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
        store.record(receipt(9, 1, 100, 1_000, StatusClass::Acknowledged));
        store.record(receipt(9, 2, 200, 1_000, StatusClass::Acknowledged));
        store.log_path()
    };
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 20;
    bytes[last] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();
    let store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(store.len(), 1);

    drop(store);
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&u32::MAX.to_le_bytes()).unwrap();
    file.write_all(&[1, 2, 3]).unwrap();
    drop(file);
    let store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(store.len(), 1);
}

#[test]
fn a_torn_header_or_empty_log_opens_empty_but_a_foreign_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(RETRY_LOG_FILE);
    std::fs::write(&path, &RETRY_LOG_MAGIC[..2]).unwrap();
    let mut store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert!(store.is_empty());
    store.record(receipt(9, 1, 100, 1_000, StatusClass::Acknowledged));
    drop(store);
    assert_eq!(FileSafeRetryStore::open(dir.path()).unwrap().len(), 1);

    std::fs::write(&path, b"definitely not a receipt log").unwrap();
    assert!(FileSafeRetryStore::open(dir.path()).is_err());
}

#[test]
fn compaction_keeps_exactly_the_live_rows() {
    let dir = tempfile::tempdir().unwrap();
    let total = RETRY_LOG_COMPACT_MIN_GARBAGE + 10;
    {
        let mut store = FileSafeRetryStore::open(dir.path())
            .unwrap()
            .with_sync_each_record(false);
        for i in 0..total {
            let mut row = receipt(9, i % 4, 100 + i % 4, 1_000, StatusClass::Acknowledged);
            row.frame_offset = i;
            store.record(row);
        }
        assert!(store.log_entries() < total, "automatic compaction ran");
        store.sync().unwrap();
        store.compact().unwrap();
        assert_eq!(store.log_entries(), 4);
        std::fs::write(dir.path().join(RETRY_LOG_COMPACT_FILE), b"partial").unwrap();
    }
    let store = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(store.len(), 4);
    assert_eq!(store.log_entries(), 4);
    assert!(!dir.path().join(RETRY_LOG_COMPACT_FILE).exists());
    let last = store.lookup(TenantId::new_test_id(9), ((total - 1) % 4, 1)).unwrap();
    assert_eq!(last.frame_offset, total - 1);
}

#[test]
fn behaves_like_the_simulation_store() {
    let dir = tempfile::tempdir().unwrap();
    let mut sim = SimSafeRetryStore::new();
    let mut disk = FileSafeRetryStore::open(dir.path()).unwrap();
    let mut state = 0x1234_5678_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for step in 0..500_i64 {
        let roll = next();
        let tenant = u128::from(roll % 3);
        let delivery = (roll >> 8) % 16;
        let dedupe = (roll >> 16) % 8;
        let status = match (roll >> 24) % 3 {
            0 => StatusClass::Acknowledged,
            1 => StatusClass::Indeterminate,
            _ => StatusClass::Rejected,
        };
        let now = step * 10;
        match (roll >> 32) % 4 {
            0 | 1 => {
                let row = receipt(tenant, delivery, dedupe, now + (roll >> 40) as i64 % 400, status);
                sim.record(row.clone());
                disk.record(row);
            }
            2 => assert_eq!(
                sim.acknowledged_within_guard(TenantId::new_test_id(tenant), (dedupe, 7), now),
                disk.acknowledged_within_guard(TenantId::new_test_id(tenant), (dedupe, 7), now)
            ),
            _ => {
                sim.evict_expired(now);
                disk.evict_expired(now);
            }
        }
        assert_eq!(sim.len(), disk.len());
    }
    drop(disk);
    let disk = FileSafeRetryStore::open(dir.path()).unwrap();
    assert_eq!(sim.len(), disk.len());
    for row in sim.receipts() {
        assert_eq!(disk.lookup(row.tenant_id, row.delivery_identity).as_ref(), Some(row));
    }
}
