use super::*;
use crate::invariants::DurabilityMode;

const BLOCK: usize = FRAME_ALIGN as usize;

fn frame(fill: u8, blocks: usize) -> Vec<u8> {
    vec![fill; BLOCK * blocks]
}

fn generation(id: u64) -> ManifestGeneration {
    ManifestGeneration {
        generation: id,
        ..ManifestGeneration::default()
    }
}

#[test]
fn journal_appends_aligned_frames_and_reads_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = FileJournalStorage::open(dir.path().join("journal")).unwrap();
    let shard = ShardId(3);
    assert_eq!(journal.extent(shard).unwrap(), 0);
    assert_eq!(journal.append(shard, &frame(1, 1)).unwrap(), 0);
    assert_eq!(journal.append(shard, &frame(2, 2)).unwrap(), BLOCK as u64);
    journal.sync(shard).unwrap();
    assert_eq!(journal.extent(shard).unwrap(), 3 * BLOCK as u64);
    assert_eq!(journal.read(shard, BLOCK as u64, 8).unwrap(), vec![2; 8]);
    assert_eq!(journal.read(shard, 0, BLOCK as u32).unwrap(), frame(1, 1));
    assert!(journal.shard_path(shard).ends_with("00000003.oej"));
    assert_eq!(journal.shards().unwrap(), vec![shard]);
}

#[test]
fn journal_rejects_unaligned_appends_and_out_of_bounds_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = FileJournalStorage::open(dir.path()).unwrap();
    let shard = ShardId(0);
    assert_eq!(journal.append(shard, &[0; 100]), Err(StorageError::Unaligned));
    journal.append(shard, &frame(1, 1)).unwrap();
    assert_eq!(journal.read(shard, 1, BLOCK as u32), Err(StorageError::OutOfBounds));
    assert_eq!(journal.read(shard, u64::MAX, 2), Err(StorageError::OutOfBounds));
    assert_eq!(journal.read(ShardId(9), 0, 1), Err(StorageError::UnknownShard));
    assert_eq!(journal.truncate(shard, 100), Err(StorageError::Unaligned));
    assert_eq!(
        journal.truncate(shard, 2 * BLOCK as u64),
        Err(StorageError::OutOfBounds)
    );
}

#[test]
fn journal_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut journal = FileJournalStorage::open(dir.path()).unwrap();
        journal.append(ShardId(1), &frame(7, 1)).unwrap();
        journal.append(ShardId(2), &frame(8, 2)).unwrap();
        journal.sync(ShardId(1)).unwrap();
        journal.sync(ShardId(2)).unwrap();
    }
    let mut journal = FileJournalStorage::open(dir.path()).unwrap();
    assert_eq!(journal.shards().unwrap(), vec![ShardId(1), ShardId(2)]);
    assert_eq!(journal.extent(ShardId(2)).unwrap(), 2 * BLOCK as u64);
    assert_eq!(journal.read(ShardId(1), 0, 4).unwrap(), vec![7; 4]);
    assert_eq!(journal.append(ShardId(1), &frame(9, 1)).unwrap(), BLOCK as u64);
    assert_eq!(journal.read(ShardId(1), BLOCK as u64, 4).unwrap(), vec![9; 4]);
}

#[test]
fn journal_torn_tail_is_visible_and_truncate_cuts_it_durably() {
    let dir = tempfile::tempdir().unwrap();
    let path = {
        let mut journal = FileJournalStorage::open(dir.path()).unwrap();
        journal.append(ShardId(0), &frame(1, 2)).unwrap();
        journal.sync(ShardId(0)).unwrap();
        journal.shard_path(ShardId(0))
    };
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&[0xEE; 100]).unwrap();
    drop(file);

    let mut journal = FileJournalStorage::open(dir.path()).unwrap();
    assert_eq!(journal.extent(ShardId(0)).unwrap(), 2 * BLOCK as u64 + 100);
    assert_eq!(journal.append(ShardId(0), &frame(2, 1)), Err(StorageError::Unaligned));
    journal.truncate(ShardId(0), BLOCK as u64).unwrap();
    assert_eq!(journal.extent(ShardId(0)).unwrap(), BLOCK as u64);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), BLOCK as u64);
    assert_eq!(journal.append(ShardId(0), &frame(3, 1)).unwrap(), BLOCK as u64);
    drop(journal);

    let journal = FileJournalStorage::open(dir.path()).unwrap();
    assert_eq!(journal.extent(ShardId(0)).unwrap(), 2 * BLOCK as u64);
    assert_eq!(journal.read(ShardId(0), BLOCK as u64, 2).unwrap(), vec![3; 2]);
}

#[test]
fn journal_reports_recorded_atomicity_and_removes_shards() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = FileJournalStorage::open(dir.path()).unwrap();
    assert_eq!(
        journal.atomicity(ShardId(0)).unwrap(),
        ShardAtomicity::buffered_unprobed()
    );
    let probed = ShardAtomicity {
        atomic_frame_multiple: 16384,
        awupf_bytes: 16384,
        durability_mode: DurabilityMode::DirectIo,
        untorn_write_bytes: 16384,
    };
    let mut journal_probed = FileJournalStorage::open(dir.path()).unwrap().with_atomicity(probed);
    journal_probed.append(ShardId(4), &frame(1, 1)).unwrap();
    assert_eq!(journal_probed.atomicity(ShardId(4)).unwrap(), probed);

    journal.append(ShardId(5), &frame(1, 1)).unwrap();
    assert!(journal.exists(ShardId(5)));
    journal.remove(ShardId(5)).unwrap();
    journal.remove(ShardId(5)).unwrap();
    assert!(!journal.exists(ShardId(5)));
    assert_eq!(journal.extent(ShardId(5)).unwrap(), 0);
}

#[test]
fn published_set_starts_at_empty_generation_zero() {
    let dir = tempfile::tempdir().unwrap();
    let set = FilePublishedSet::open(dir.path()).unwrap();
    let (id, head) = set.head().unwrap();
    assert_eq!(id, 0);
    assert_eq!(head, ManifestGeneration::default());
    assert_eq!(set.generation(5), Err(PublishError::UnknownGeneration));
}

#[test]
fn published_set_generations_are_create_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut set = FilePublishedSet::open(dir.path()).unwrap();
    set.put_generation(generation(1)).unwrap();
    assert_eq!(set.put_generation(generation(1)), Err(PublishError::GenerationExists));
    assert_eq!(set.put_generation(generation(0)), Err(PublishError::GenerationExists));
    let mut other = FilePublishedSet::open(dir.path()).unwrap();
    assert_eq!(other.put_generation(generation(1)), Err(PublishError::GenerationExists));
    assert_eq!(set.generation(1).unwrap(), generation(1));
    let leftovers: Vec<_> = std::fs::read_dir(dir.path().join(PUBLISHED_GENERATIONS_DIR))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(leftovers.len(), 1);
}

#[test]
fn published_set_head_is_compare_and_swap() {
    let dir = tempfile::tempdir().unwrap();
    let mut set = FilePublishedSet::open(dir.path()).unwrap();
    assert_eq!(set.advance_head(0, 1), Err(PublishError::UnknownGeneration));
    set.put_generation(generation(1)).unwrap();
    set.put_generation(generation(2)).unwrap();
    set.advance_head(0, 1).unwrap();
    assert_eq!(set.head().unwrap().0, 1);
    assert_eq!(
        set.advance_head(0, 2),
        Err(PublishError::CasLost { current_generation: 1 })
    );
    set.advance_head(1, 2).unwrap();
    let (id, head) = set.head().unwrap();
    assert_eq!((id, head), (2, generation(2)));
}

#[test]
fn published_set_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut set = FilePublishedSet::open(dir.path()).unwrap();
        let mut published = generation(1);
        published.footer_mirror = None;
        set.put_generation(published).unwrap();
        set.advance_head(0, 1).unwrap();
        std::fs::write(dir.path().join("head.999.tmp"), b"7").unwrap();
    }
    let set = FilePublishedSet::open(dir.path()).unwrap();
    assert_eq!(set.head().unwrap(), (1, generation(1)));
}

#[test]
fn published_set_prunes_old_generations_keeping_n() {
    let dir = tempfile::tempdir().unwrap();
    let kept = 3;
    let mut set = FilePublishedSet::open_keeping(dir.path(), kept).unwrap();
    std::fs::write(
        dir.path()
            .join(PUBLISHED_GENERATIONS_DIR)
            .join("00000000000000000099.1.tmp"),
        b"{}",
    )
    .unwrap();
    let last = 10;
    for id in 1..=last {
        set.put_generation(generation(id)).unwrap();
        set.advance_head(id - 1, id).unwrap();
    }
    assert_eq!(
        set.generation_ids().unwrap(),
        ((last - kept)..=last).collect::<Vec<_>>()
    );
    assert_eq!(set.generation(last - kept - 1), Err(PublishError::UnknownGeneration));
    assert!(set.generation(last - kept).is_ok());
    assert_eq!(
        std::fs::read_dir(dir.path().join(PUBLISHED_GENERATIONS_DIR))
            .unwrap()
            .count(),
        kept as usize + 1
    );
    assert_eq!(PUBLISHED_GENERATIONS_KEPT, 8);
}

fn real_frame(sequence: u64, payload_len: usize) -> Vec<u8> {
    use crate::artifacts::frame::{FrameBuildInput, build_frame};
    use crate::typed_id::TypedIdTestExt;
    let input = FrameBuildInput {
        committed_at_physical: 1,
        created_at_physical: 1,
        dictionary_generation_hint: 0,
        durable_batch_id: sequence,
        epoch: 1,
        event_count: 1,
        first_sequence: sequence,
        flags: 0,
        last_sequence: sequence,
        schema_generation: 0,
        tenant_id: crate::events::TenantId::new_test_id(1),
        writer_id: 1,
        writer_local_batch_id: sequence,
    };
    build_frame(&input, &vec![0xAB; payload_len]).unwrap()
}

#[test]
fn preallocated_journal_tracks_its_logical_end_across_reopen_and_trims_when_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let shard = ShardId(5);
    let first = real_frame(1, 100);
    let second = real_frame(2, 5_000);
    let logical = (first.len() + second.len()) as u64;
    let path = {
        let mut journal = FileJournalStorage::open(dir.path())
            .unwrap()
            .with_preallocation(1 << 20);
        journal.append(shard, &first).unwrap();
        assert_eq!(journal.append(shard, &second).unwrap(), first.len() as u64);
        journal.sync(shard).unwrap();
        let path = journal.shard_path(shard);
        let physical = std::fs::metadata(&path).unwrap().len();
        assert!(physical == 1 << 20 || physical == logical, "physical {physical}");
        assert_eq!(journal.extent(shard).unwrap(), logical);
        std::mem::forget(journal);
        path
    };
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(1 << 20)
        .unwrap();

    let mut journal = FileJournalStorage::open(dir.path())
        .unwrap()
        .with_preallocation(1 << 20);
    assert_eq!(journal.extent(shard).unwrap(), logical);
    assert_eq!(journal.read(shard, 0, first.len() as u32).unwrap(), first);
    assert_eq!(journal.read(shard, logical, 1), Err(StorageError::OutOfBounds));
    let third = real_frame(3, 10);
    assert_eq!(journal.append(shard, &third).unwrap(), logical);
    let logical = logical + third.len() as u64;
    journal.trim(shard).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), logical);
    drop(journal);
    let journal = FileJournalStorage::open(dir.path()).unwrap();
    assert_eq!(journal.extent(shard).unwrap(), logical);
}

#[test]
fn dropping_a_preallocated_journal_cuts_the_tail_and_the_default_does_not_preallocate() {
    let dir = tempfile::tempdir().unwrap();
    let frame = real_frame(1, 10);
    {
        let mut journal = FileJournalStorage::open(dir.path()).unwrap();
        journal.append(ShardId(0), &frame).unwrap();
        assert_eq!(
            std::fs::metadata(journal.shard_path(ShardId(0))).unwrap().len(),
            frame.len() as u64
        );
    }
    let path = {
        let mut journal = FileJournalStorage::open(dir.path())
            .unwrap()
            .with_preallocation(64 * 1024);
        journal.append(ShardId(1), &frame).unwrap();
        journal.shard_path(ShardId(1))
    };
    assert_eq!(std::fs::metadata(&path).unwrap().len(), frame.len() as u64);
}

#[test]
fn a_zeroed_tail_after_garbage_is_not_hidden_from_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = {
        let mut journal = FileJournalStorage::open(dir.path()).unwrap();
        journal.append(ShardId(0), &real_frame(1, 10)).unwrap();
        journal.append(ShardId(0), &frame(0x55, 1)).unwrap();
        journal.append(ShardId(0), &frame(0, 1)).unwrap();
        journal.shard_path(ShardId(0))
    };
    let len = std::fs::metadata(&path).unwrap().len();
    let journal = FileJournalStorage::open(dir.path()).unwrap();
    assert_eq!(journal.extent(ShardId(0)).unwrap(), len);
}

#[test]
fn read_handles_are_cached_bounded_and_revalidated_by_length() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut journal = FileJournalStorage::open(dir.path()).unwrap();
        for id in 0..20 {
            journal.append(ShardId(id), &frame(id as u8 + 1, 1)).unwrap();
        }
    }
    let journal = FileJournalStorage::open(dir.path()).unwrap();
    for id in 0..20 {
        assert_eq!(journal.read(ShardId(id), 0, 1).unwrap(), vec![id as u8 + 1]);
    }
    assert_eq!(journal.cached_readers(), JOURNAL_READ_HANDLES);
    assert_eq!(journal.extent(ShardId(19)).unwrap(), BLOCK as u64);
    {
        let mut writer = FileJournalStorage::open(dir.path()).unwrap();
        writer.append(ShardId(19), &frame(0x77, 1)).unwrap();
    }
    assert_eq!(journal.extent(ShardId(19)).unwrap(), 2 * BLOCK as u64);
    assert_eq!(journal.read(ShardId(19), BLOCK as u64, 1).unwrap(), vec![0x77]);
    assert_eq!(journal.read(ShardId(99), 0, 1), Err(StorageError::UnknownShard));
    assert_eq!(journal.extent(ShardId(99)).unwrap(), 0);
}

#[test]
fn a_reader_of_a_preallocated_shard_sees_frames_appended_into_the_zero_tail() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = tempfile::tempdir().unwrap();
    let shard = ShardId(6);
    let first = real_frame(1, 100);
    let path = {
        let mut journal = FileJournalStorage::open(dir.path()).unwrap();
        journal.append(shard, &first).unwrap();
        journal.shard_path(shard)
    };
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(1 << 20).unwrap();

    let journal = FileJournalStorage::open(dir.path()).unwrap();
    let logical = first.len() as u64;
    assert_eq!(journal.extent(shard).unwrap(), logical);
    assert_eq!(journal.read(shard, 0, first.len() as u32).unwrap(), first);
    assert_eq!(journal.read(shard, logical, 1), Err(StorageError::OutOfBounds));

    let second = real_frame(2, 5_000);
    file.seek(SeekFrom::Start(logical)).unwrap();
    file.write_all(&second).unwrap();
    assert_eq!(journal.extent(shard).unwrap(), logical + second.len() as u64);
    assert_eq!(journal.read(shard, logical, second.len() as u32).unwrap(), second);
}
