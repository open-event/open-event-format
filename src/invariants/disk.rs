use super::constant::{
    JOURNAL_SHARD_EXTENSION, PUBLISHED_GENERATION_EXTENSION, PUBLISHED_GENERATIONS_DIR, PUBLISHED_GENERATIONS_KEPT,
    PUBLISHED_HEAD_FILE, PUBLISHED_STAGING_EXTENSION,
};
use super::{JournalStorage, PublishedSet, ShardAtomicity, ShardId};
use crate::artifacts::FRAME_ALIGN;
use crate::artifacts::frame::precheck_header;
use crate::error::{PublishError, StorageError};
use crate::lifecycle::ManifestGeneration;
use hashbrown::HashMap;
use std::fs::{File, OpenOptions};
#[cfg(not(unix))]
use std::io::Read;
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

pub const JOURNAL_READ_HANDLES: usize = 16;

pub fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn io_error(op: &'static str) -> impl FnOnce(std::io::Error) -> StorageError {
    move |error| StorageError::Io {
        op,
        detail: error.to_string(),
    }
}

#[derive(Debug)]
pub struct FileJournalStorage {
    atomicity: ShardAtomicity,
    dir: PathBuf,
    preallocate_bytes: u64,
    readers: Mutex<ReadHandles>,
    shards: HashMap<ShardId, OpenShard>,
}

#[derive(Debug)]
struct OpenShard {
    allocated: u64,
    atomicity: ShardAtomicity,
    extent: u64,
    file: File,
}

#[derive(Debug, Default)]
struct ReadHandles {
    entries: Vec<ReadHandle>,
}

#[derive(Debug)]
struct ReadHandle {
    file: Arc<File>,
    physical: u64,
    shard: ShardId,
    extent: Option<u64>,
}

fn read_exact_at(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(bytes, offset)
    }
    #[cfg(not(unix))]
    {
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(bytes)
    }
}

pub fn journal_logical_end(file: &File, physical: u64) -> std::io::Result<u64> {
    let align = u64::from(FRAME_ALIGN);
    if physical == 0 || !physical.is_multiple_of(align) {
        return Ok(physical);
    }
    let mut block = vec![0; FRAME_ALIGN as usize];
    read_exact_at(file, &mut block, physical - align)?;
    if block.iter().any(|byte| *byte != 0) {
        return Ok(physical);
    }
    let mut offset = 0;
    while offset + align <= physical {
        read_exact_at(file, &mut block, offset)?;
        let Ok(header) = precheck_header(&block) else {
            break;
        };
        let len = u64::from(header.frame_len);
        if len == 0 || !len.is_multiple_of(align) || offset + len > physical {
            break;
        }
        offset += len;
    }
    let mut at = offset;
    while at < physical {
        read_exact_at(file, &mut block, at)?;
        if block.iter().any(|byte| *byte != 0) {
            return Ok(physical);
        }
        at += align;
    }
    Ok(offset)
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
#[allow(unsafe_code)]
fn preallocate(file: &File, len: u64) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;
    let Ok(len) = libc::off_t::try_from(len) else {
        return Ok(false);
    };
    // SAFETY: the descriptor is borrowed from a live `File` for the duration of the call, and posix_fallocate only
    let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len) };
    match result {
        0 => Ok(true),
        libc::EOPNOTSUPP | libc::EINVAL | libc::ENOSYS => Ok(false),
        errno => Err(std::io::Error::from_raw_os_error(errno)),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
fn preallocate(_file: &File, _len: u64) -> std::io::Result<bool> {
    Ok(false)
}

impl FileJournalStorage {
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        create_dir_durably(&dir)?;
        Ok(Self {
            atomicity: ShardAtomicity::buffered_unprobed(),
            dir,
            preallocate_bytes: 0,
            readers: Mutex::default(),
            shards: HashMap::new(),
        })
    }

    pub fn with_preallocation(mut self, bytes: u64) -> Self {
        self.preallocate_bytes = if bytes == 0 {
            0
        } else {
            bytes.next_multiple_of(u64::from(FRAME_ALIGN))
        };
        self
    }

    pub fn trim(&mut self, shard: ShardId) -> std::io::Result<()> {
        let Some(open) = self.shards.get_mut(&shard) else {
            return Ok(());
        };
        if open.allocated > open.extent {
            open.file.set_len(open.extent)?;
            open.file.sync_all()?;
            open.allocated = open.extent;
        }
        Ok(())
    }

    fn forget_reader(&mut self, shard: ShardId) {
        let readers = self.readers.get_mut().unwrap_or_else(PoisonError::into_inner);
        readers.entries.retain(|entry| entry.shard != shard);
    }

    fn with_reader<T>(
        &self,
        shard: ShardId,
        with: impl FnOnce(&File, u64) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut readers = self.readers.lock().unwrap_or_else(PoisonError::into_inner);
        let (file, extent) = if let Some(index) = readers.entries.iter().position(|entry| entry.shard == shard) {
            let mut entry = readers.entries.remove(index);
            let physical = entry.file.metadata().map_err(io_error("journal stat"))?.len();
            let extent = match entry.extent {
                Some(extent) if physical == entry.physical => extent,
                _ => journal_logical_end(&entry.file, physical).map_err(io_error("journal scan"))?,
            };
            entry.physical = physical;
            entry.extent = (extent == physical).then_some(extent);
            let file = Arc::clone(&entry.file);
            readers.entries.push(entry);
            (file, extent)
        } else {
            let file = File::open(self.shard_path(shard)).map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    StorageError::UnknownShard
                } else {
                    io_error("journal open")(error)
                }
            })?;
            let physical = file.metadata().map_err(io_error("journal stat"))?.len();
            let extent = journal_logical_end(&file, physical).map_err(io_error("journal scan"))?;
            let file = Arc::new(file);
            if readers.entries.len() >= JOURNAL_READ_HANDLES {
                readers.entries.remove(0);
            }
            readers.entries.push(ReadHandle {
                file: Arc::clone(&file),
                physical,
                shard,
                extent: (extent == physical).then_some(extent),
            });
            (file, extent)
        };
        with(&file, extent)
    }

    #[cfg(test)]
    fn cached_readers(&self) -> usize {
        self.readers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .len()
    }

    pub fn with_atomicity(mut self, atomicity: ShardAtomicity) -> Self {
        self.atomicity = atomicity;
        self
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn shard_path(&self, shard: ShardId) -> PathBuf {
        self.dir.join(format!("{:08x}.{JOURNAL_SHARD_EXTENSION}", shard.0))
    }

    pub fn exists(&self, shard: ShardId) -> bool {
        self.shards.contains_key(&shard) || self.shard_path(shard).exists()
    }

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

    pub fn remove(&mut self, shard: ShardId) -> std::io::Result<()> {
        self.shards.remove(&shard);
        self.forget_reader(shard);
        match std::fs::remove_file(self.shard_path(shard)) {
            Ok(()) => sync_dir(&self.dir),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn open_shard(&mut self, shard: ShardId) -> Result<&mut OpenShard, StorageError> {
        if !self.shards.contains_key(&shard) {
            self.forget_reader(shard);
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
            let allocated = file.metadata().map_err(io_error("journal stat"))?.len();
            let extent = journal_logical_end(&file, allocated).map_err(io_error("journal scan"))?;
            self.shards.insert(
                shard,
                OpenShard {
                    allocated,
                    atomicity: self.atomicity,
                    extent,
                    file,
                },
            );
        }
        self.shards.get_mut(&shard).ok_or(StorageError::UnknownShard)
    }
}

impl Drop for FileJournalStorage {
    fn drop(&mut self) {
        for open in self.shards.values_mut() {
            if open.allocated > open.extent && open.file.set_len(open.extent).is_ok() {
                open.allocated = open.extent;
            }
        }
    }
}

fn read_at(file: &File, extent: u64, offset: u64, len: u32) -> Result<Vec<u8>, StorageError> {
    if offset.checked_add(u64::from(len)).is_none_or(|end| end > extent) {
        return Err(StorageError::OutOfBounds);
    }
    let mut bytes = vec![0; len as usize];
    read_exact_at(file, &mut bytes, offset).map_err(io_error("journal read"))?;
    Ok(bytes)
}

impl JournalStorage for FileJournalStorage {
    fn append(&mut self, shard: ShardId, frame: &[u8]) -> Result<u64, StorageError> {
        if !frame.len().is_multiple_of(FRAME_ALIGN as usize) {
            return Err(StorageError::Unaligned);
        }
        let chunk = self.preallocate_bytes;
        let open = self.open_shard(shard)?;
        let offset = open.extent;
        if !offset.is_multiple_of(u64::from(FRAME_ALIGN)) {
            return Err(StorageError::Unaligned);
        }
        let end = offset + frame.len() as u64;
        if chunk > 0 && end > open.allocated {
            let want = end.div_ceil(chunk) * chunk;
            if preallocate(&open.file, want).map_err(io_error("journal preallocate"))? {
                open.allocated = want;
            }
        }
        open.file
            .seek(SeekFrom::Start(offset))
            .map_err(io_error("journal seek"))?;
        if let Err(error) = open.file.write_all(frame) {
            return Err(io_error("journal append")(error));
        }
        open.extent = end;
        open.allocated = open.allocated.max(end);
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
        open.allocated = len;
        Ok(())
    }

    fn read(&self, shard: ShardId, offset: u64, len: u32) -> Result<Vec<u8>, StorageError> {
        if let Some(open) = self.shards.get(&shard) {
            return read_at(&open.file, open.extent, offset, len);
        }
        self.with_reader(shard, |file, extent| read_at(file, extent, offset, len))
    }

    fn extent(&self, shard: ShardId) -> Result<u64, StorageError> {
        if let Some(open) = self.shards.get(&shard) {
            return Ok(open.extent);
        }
        match self.with_reader(shard, |_, extent| Ok(extent)) {
            Err(StorageError::UnknownShard) => Ok(0),
            other => other,
        }
    }

    fn atomicity(&self, shard: ShardId) -> Result<ShardAtomicity, StorageError> {
        Ok(self.shards.get(&shard).map_or(self.atomicity, |open| open.atomicity))
    }
}

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

fn stage(path: &Path, bytes: &[u8]) -> std::io::Result<PathBuf> {
    let staged = path.with_extension(format!("{}.{}", std::process::id(), PUBLISHED_STAGING_EXTENSION));
    let mut file = File::create(&staged)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(staged)
}

#[derive(Debug)]
pub struct FilePublishedSet {
    dir: PathBuf,
    generations_kept: u64,
}

impl FilePublishedSet {
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::open_keeping(dir, PUBLISHED_GENERATIONS_KEPT)
    }

    pub fn open_keeping(dir: impl Into<PathBuf>, generations_kept: u64) -> std::io::Result<Self> {
        let dir = dir.into();
        create_dir_durably(&dir)?;
        create_dir_durably(&dir.join(PUBLISHED_GENERATIONS_DIR))?;
        Ok(Self { dir, generations_kept })
    }

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
