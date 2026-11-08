//! Production local-disk versions of the engine's journal and publication interfaces: [`FileJournalStorage`] keeps
//! each journal shard in one append-only file, and [`FilePublishedSet`] keeps the file catalogue as create-only
//! generation files plus an atomically replaced head pointer.
//!
//! Both are plain synchronous `std::fs` code, so they satisfy the synchronous [`JournalStorage`] and [`PublishedSet`]
//! contracts directly (design D4). Every step that must survive a crash ends in an explicit `fsync` of the file and,
//! where a directory entry was created, renamed or removed, of its directory.

use super::constant::{
    JOURNAL_SHARD_EXTENSION, PUBLISHED_GENERATION_EXTENSION, PUBLISHED_GENERATIONS_DIR, PUBLISHED_GENERATIONS_KEPT,
    PUBLISHED_HEAD_FILE, PUBLISHED_STAGING_EXTENSION,
};
use super::{JournalStorage, PublishedSet, ShardAtomicity, ShardId};
use crate::artifacts::FRAME_ALIGN;
use crate::error::{PublishError, StorageError};
use crate::lifecycle::ManifestGeneration;
use hashbrown::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Makes the entries of `dir` (a file created, renamed or removed in it) durable.
pub fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn io_error(op: &'static str) -> impl FnOnce(std::io::Error) -> StorageError {
    move |error| StorageError::Io {
        op,
        detail: error.to_string(),
    }
}

/// Keeps the journal in files on local disk, one append-only file per shard, so an acknowledged event survives a
/// crash.
///
/// Shard `n` is the file `{n as 8 hex digits}.oej` in the journal directory. Frames are appended whole and 4096-byte
/// aligned at the end of the file; `sync` is `fdatasync`, which is what makes an append durable before the engine
/// acknowledges it. A shard's extent is its file length, so a torn tail left by a crash (a partial, possibly unaligned
/// last write) is visible to replay, which cuts it back through `truncate`; `truncate` syncs the shorter length before
/// returning.
///
/// The atomicity recorded for every shard is the conservative buffered answer unless the deployment probed its device
/// and supplied a better one with [`FileJournalStorage::with_atomicity`].
#[derive(Debug)]
pub struct FileJournalStorage {
    atomicity: ShardAtomicity,
    dir: PathBuf,
    shards: HashMap<ShardId, OpenShard>,
}

/// One open shard file, how far it is written, and the atomicity probed for it when it was opened.
#[derive(Debug)]
struct OpenShard {
    atomicity: ShardAtomicity,
    extent: u64,
    file: File,
}

impl FileJournalStorage {
    /// The journal kept in `dir`, created (durably) when missing. Shard files are opened as they are first used.
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        create_dir_durably(&dir)?;
        Ok(Self {
            atomicity: ShardAtomicity::buffered_unprobed(),
            dir,
            shards: HashMap::new(),
        })
    }

    /// Records `atomicity` for every shard opened from now on, for a deployment that probed its device's untorn-write
    /// guarantees itself. The default is [`ShardAtomicity::buffered_unprobed`], which is always correct.
    pub fn with_atomicity(mut self, atomicity: ShardAtomicity) -> Self {
        self.atomicity = atomicity;
        self
    }

    /// The directory the shard files live in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The file holding `shard`.
    pub fn shard_path(&self, shard: ShardId) -> PathBuf {
        self.dir.join(format!("{:08x}.{JOURNAL_SHARD_EXTENSION}", shard.0))
    }

    /// Whether `shard` has a file.
    pub fn exists(&self, shard: ShardId) -> bool {
        self.shards.contains_key(&shard) || self.shard_path(shard).exists()
    }

    /// Every shard with a file in the directory, in id order — what recovery replays after a restart.
    pub fn shards(&self) -> std::io::Result<Vec<ShardId>> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some(JOURNAL_SHARD_EXTENSION) {
                continue;
            }
            let id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .filter(|stem| stem.len() == 8)
                .and_then(|stem| u32::from_str_radix(stem, 16).ok());
            if let Some(id) = id {
                found.push(ShardId(id));
            }
        }
        found.sort_unstable();
        Ok(found)
    }

    /// Deletes `shard`'s file, durably, once nothing needs its frames any more. Removing a shard that is already gone
    /// succeeds.
    pub fn remove(&mut self, shard: ShardId) -> std::io::Result<()> {
        self.shards.remove(&shard);
        match std::fs::remove_file(self.shard_path(shard)) {
            Ok(()) => sync_dir(&self.dir),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// The open file of `shard`, opening (and creating, durably) it on first use.
    fn open_shard(&mut self, shard: ShardId) -> Result<&mut OpenShard, StorageError> {
        if !self.shards.contains_key(&shard) {
            let path = self.shard_path(shard);
            let created = !path.exists();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(io_error("journal open"))?;
            if created {
                sync_dir(&self.dir).map_err(io_error("journal directory sync"))?;
            }
            let extent = file.metadata().map_err(io_error("journal stat"))?.len();
            self.shards.insert(
                shard,
                OpenShard {
                    atomicity: self.atomicity,
                    extent,
                    file,
                },
            );
        }
        self.shards.get_mut(&shard).ok_or(StorageError::UnknownShard)
    }
}

/// Reads `len` bytes at `offset` from `file`, whose written extent is `extent`.
fn read_at(mut file: &File, extent: u64, offset: u64, len: u32) -> Result<Vec<u8>, StorageError> {
    if offset.checked_add(u64::from(len)).is_none_or(|end| end > extent) {
        return Err(StorageError::OutOfBounds);
    }
    file.seek(SeekFrom::Start(offset)).map_err(io_error("journal seek"))?;
    let mut bytes = vec![0; len as usize];
    file.read_exact(&mut bytes).map_err(io_error("journal read"))?;
    Ok(bytes)
}

impl JournalStorage for FileJournalStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, StorageError> {
        if !frame.len().is_multiple_of(FRAME_ALIGN as usize) {
            return Err(StorageError::Unaligned);
        }
        let open = self.open_shard(shard)?;
        let offset = open.extent;
        if !offset.is_multiple_of(u64::from(FRAME_ALIGN)) {
            // A torn tail recovery has not cut back yet: appending here would misalign every later frame.
            return Err(StorageError::Unaligned);
        }
        open.file
            .seek(SeekFrom::Start(offset))
            .map_err(io_error("journal seek"))?;
        if let Err(error) = open.file.write_all(frame) {
            // A short write may leave bytes past the recorded extent; the next append overwrites them, and after a
            // crash replay cuts the torn tail back.
            return Err(io_error("journal append")(error));
        }
        open.extent = offset + frame.len() as u64;
        Ok(offset)
    }

    fn sync(&mut self, shard: ShardId) -> Result<(), StorageError> {
        self.open_shard(shard)?
            .file
            .sync_data()
            .map_err(io_error("journal sync"))
    }

    fn truncate(&mut self, shard: ShardId, len: u64) -> Result<(), StorageError> {
        if !len.is_multiple_of(u64::from(FRAME_ALIGN)) {
            return Err(StorageError::Unaligned);
        }
        let open = self.open_shard(shard)?;
        if len > open.extent {
            return Err(StorageError::OutOfBounds);
        }
        open.file.set_len(len).map_err(io_error("journal truncate"))?;
        open.file.sync_all().map_err(io_error("journal sync"))?;
        open.extent = len;
        Ok(())
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, StorageError> {
        if let Some(open) = self.shards.get(&shard) {
            return read_at(&open.file, open.extent, offset, len);
        }
        let file = File::open(self.shard_path(shard)).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                StorageError::UnknownShard
            } else {
                io_error("journal open")(error)
            }
        })?;
        let extent = file.metadata().map_err(io_error("journal stat"))?.len();
        read_at(&file, extent, offset, len)
    }

    fn extent(&self, shard: ShardId) -> Result<u64, StorageError> {
        if let Some(open) = self.shards.get(&shard) {
            return Ok(open.extent);
        }
        match std::fs::metadata(self.shard_path(shard)) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(io_error("journal stat")(error)),
        }
    }

    fn atomicity(&self, shard: ShardId) -> Result<ShardAtomicity, StorageError> {
        Ok(self.shards.get(&shard).map_or(self.atomicity, |open| open.atomicity))
    }
}

/// Creates `dir` and any missing parents, and makes the new entry durable in its parent.
fn create_dir_durably(dir: &Path) -> std::io::Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dir)?;
    if let Some(parent) = dir.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        sync_dir(parent)?;
    }
    Ok(())
}

fn backend(op: &str, error: impl std::fmt::Display) -> PublishError {
    PublishError::Io {
        detail: format!("catalogue {op}: {error}"),
    }
}

/// Writes `bytes` to a fresh staging file next to `path` and syncs it, returning the staging path.
fn stage(path: &Path, bytes: &[u8]) -> std::io::Result<PathBuf> {
    let staged = path.with_extension(format!("{}.{}", std::process::id(), PUBLISHED_STAGING_EXTENSION));
    let mut file = File::create(&staged)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(staged)
}

/// Keeps the file catalogue on local disk: one create-only JSON file per generation and a head file naming the
/// current one.
///
/// Each generation is written once, to `generations/{id as 20 digits}.json`: the bytes are staged in a synced
/// temporary file and hard-linked into place, which fails when the name already exists, so a generation is never
/// overwritten (`GenerationExists`). The head is replaced by writing a synced temporary file and renaming it over the
/// old one, then syncing the directory, so a crash leaves either the old head or the new one, never a torn one. With
/// no head file the catalogue reads as the empty generation 0.
///
/// `advance_head` is a compare-and-swap against the head as it is on disk: it fails with `CasLost` carrying the
/// current id whenever the head is not `expected`. The read-then-rename is atomic only against writers that share this
/// value (`&mut self`), so a directory must have one `FilePublishedSet` writer at a time; any number of readers may
/// open it. After each successful advance, generations more than the retention count behind the new head are pruned.
#[derive(Debug)]
pub struct FilePublishedSet {
    dir: PathBuf,
    generations_kept: u64,
}

impl FilePublishedSet {
    /// The catalogue kept in `dir`, created when missing, keeping [`PUBLISHED_GENERATIONS_KEPT`] generations behind
    /// the head.
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::open_keeping(dir, PUBLISHED_GENERATIONS_KEPT)
    }

    /// The catalogue kept in `dir`, keeping `generations_kept` generations behind the head when pruning (the head
    /// itself is always kept).
    pub fn open_keeping(dir: impl Into<PathBuf>, generations_kept: u64) -> std::io::Result<Self> {
        let dir = dir.into();
        create_dir_durably(&dir)?;
        create_dir_durably(&dir.join(PUBLISHED_GENERATIONS_DIR))?;
        Ok(Self { dir, generations_kept })
    }

    /// The directory the catalogue lives in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn head_path(&self) -> PathBuf {
        self.dir.join(PUBLISHED_HEAD_FILE)
    }

    fn generations_dir(&self) -> PathBuf {
        self.dir.join(PUBLISHED_GENERATIONS_DIR)
    }

    fn generation_path(&self, id: u64) -> PathBuf {
        self.generations_dir()
            .join(format!("{id:020}.{PUBLISHED_GENERATION_EXTENSION}"))
    }

    fn head_id(&self) -> Result<u64, PublishError> {
        match std::fs::read_to_string(self.head_path()) {
            Ok(text) => text.trim().parse().map_err(|error| backend("head", error)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(backend("head", error)),
        }
    }

    /// The ids of every generation file still on disk, in order.
    pub fn generation_ids(&self) -> Result<Vec<u64>, PublishError> {
        let entries = std::fs::read_dir(self.generations_dir()).map_err(|error| backend("list", error))?;
        let mut ids = Vec::new();
        for entry in entries {
            let path = entry.map_err(|error| backend("list", error))?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some(PUBLISHED_GENERATION_EXTENSION) {
                continue;
            }
            if let Some(id) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    /// Deletes generations more than the retention count behind `head`, and staging files a crash left behind.
    fn prune_before(&self, head: u64) -> Result<(), PublishError> {
        let oldest_kept = head.saturating_sub(self.generations_kept);
        let entries = std::fs::read_dir(self.generations_dir()).map_err(|error| backend("prune", error))?;
        let mut removed = false;
        for entry in entries {
            let path = entry.map_err(|error| backend("prune", error))?.path();
            let stale_staging = path.extension().and_then(|ext| ext.to_str()) == Some(PUBLISHED_STAGING_EXTENSION);
            let id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .filter(|_| path.extension().and_then(|ext| ext.to_str()) == Some(PUBLISHED_GENERATION_EXTENSION))
                .and_then(|stem| stem.parse::<u64>().ok());
            if stale_staging || id.is_some_and(|id| id < oldest_kept) {
                match std::fs::remove_file(&path) {
                    Ok(()) => removed = true,
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(backend("prune", error)),
                }
            }
        }
        if removed {
            sync_dir(&self.generations_dir()).map_err(|error| backend("prune sync", error))?;
        }
        Ok(())
    }
}

impl PublishedSet for FilePublishedSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError> {
        let id = self.head_id()?;
        Ok((id, self.generation(id)?))
    }

    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError> {
        let path = self.generation_path(generation.generation);
        if generation.generation == 0 || path.exists() {
            return Err(PublishError::GenerationExists);
        }
        let bytes = serde_json::to_vec(&generation).map_err(|error| backend("encode", error))?;
        let staged = stage(&path, &bytes).map_err(|error| backend("write", error))?;
        // A hard link is create-only: it fails when the name exists, so two writers of one id cannot both win.
        let linked = std::fs::hard_link(&staged, &path);
        let _ = std::fs::remove_file(&staged);
        match linked {
            Ok(()) => sync_dir(&self.generations_dir()).map_err(|error| backend("write sync", error)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => Err(PublishError::GenerationExists),
            Err(error) => Err(backend("write", error)),
        }
    }

    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError> {
        let current = self.head_id()?;
        if current != expected {
            return Err(PublishError::CasLost {
                current_generation: current,
            });
        }
        if !self.generation_path(next).exists() {
            return Err(PublishError::UnknownGeneration);
        }
        let head = self.head_path();
        let staged = stage(&head, next.to_string().as_bytes()).map_err(|error| backend("head write", error))?;
        if let Err(error) = std::fs::rename(&staged, &head) {
            let _ = std::fs::remove_file(&staged);
            return Err(backend("head write", error));
        }
        sync_dir(&self.dir).map_err(|error| backend("head sync", error))?;
        self.prune_before(next)
    }

    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError> {
        match std::fs::read(self.generation_path(id)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| backend("decode", error)),
            Err(error) if error.kind() == ErrorKind::NotFound && id == 0 => Ok(ManifestGeneration::default()),
            Err(error) if error.kind() == ErrorKind::NotFound => Err(PublishError::UnknownGeneration),
            Err(error) => Err(backend("read", error)),
        }
    }
}

#[cfg(test)]
#[path = "test/disk.rs"]
mod tests;
