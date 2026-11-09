use super::{AeadScheme, ContentKey, ContentKeyId, SealedContentKey, SubjectKeyStore, TenantDek};
use crate::clock::Clock;
use crate::deletes::{SubjectContentKey, SubjectId};
use crate::error::StorageError;
use crate::events::EventId;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const KEY_FILE_MAGIC: &[u8; 4] = b"OESK";
const KEY_FILE_VERSION: u8 = 1;
const STATE_LIVE: u8 = 1;
const STATE_DESTROYED: u8 = 2;
const EPOCHS_PER_CHECKOUT: u64 = 1 << 32;
const ERASURE_RECORD_LEN: usize = 24;
const ERASURE_RECORD_V2_LEN: usize = 33;
const BACKLOG_RECORD_LEN: usize = 33;
const BACKLOG_PENDING: u8 = 1;
const BACKLOG_RESOLVED: u8 = 2;
const BACKLOG_REKEYED: u8 = 3;
const BACKLOG_RETARGETED: u8 = 4;

fn io(op: &'static str) -> impl Fn(std::io::Error) -> StorageError {
    move |error| StorageError::Io {
        op,
        detail: error.to_string(),
    }
}

fn corrupt(op: &'static str, detail: &str) -> StorageError {
    StorageError::Io {
        op,
        detail: detail.to_owned(),
    }
}

fn sync_dir(dir: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        File::open(dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io("sync subject key directory"))?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let staged = path.with_extension("tmp");
    let mut file = File::create(&staged).map_err(io("stage subject key file"))?;
    file.write_all(bytes).map_err(io("write subject key file"))?;
    file.sync_all().map_err(io("sync subject key file"))?;
    drop(file);
    fs::rename(&staged, path).map_err(io("publish subject key file"))?;
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

fn append_synced(path: &Path, record: &[u8]) -> Result<(), StorageError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(io("open log"))?;
    file.write_all(record).map_err(io("append log"))?;
    file.sync_all().map_err(io("sync log"))
}

fn read_records(path: &Path, record_len: usize) -> Result<Vec<Vec<u8>>, StorageError> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut file) => {
            file.read_to_end(&mut bytes).map_err(io("read log"))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io("open log")(error)),
    }
    Ok(bytes.chunks_exact(record_len).map(<[u8]>::to_vec).collect())
}

fn u128_at(bytes: &[u8], at: usize) -> u128 {
    bytes
        .get(at..at + 16)
        .and_then(|raw| raw.try_into().ok())
        .map_or(0, u128::from_le_bytes)
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    bytes
        .get(at..at + 8)
        .and_then(|raw| raw.try_into().ok())
        .map_or(0, u64::from_le_bytes)
}

fn scheme_byte(scheme: AeadScheme) -> u8 {
    match scheme {
        AeadScheme::AesGcm256 => 0,
        AeadScheme::XChaCha20Poly1305 => 1,
    }
}

fn scheme_of(byte: u8) -> Option<AeadScheme> {
    match byte {
        0 => Some(AeadScheme::AesGcm256),
        1 => Some(AeadScheme::XChaCha20Poly1305),
        _ => None,
    }
}

enum KeyRecord {
    Live {
        epoch_high_water: u64,
        scheme: AeadScheme,
        wrapped: Vec<u8>,
    },
    Destroyed,
}

impl KeyRecord {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(KEY_FILE_MAGIC);
        out.push(KEY_FILE_VERSION);
        match self {
            KeyRecord::Live {
                epoch_high_water,
                scheme,
                wrapped,
            } => {
                out.push(STATE_LIVE);
                out.push(scheme_byte(*scheme));
                out.extend_from_slice(&epoch_high_water.to_le_bytes());
                out.extend_from_slice(&(wrapped.len() as u32).to_le_bytes());
                out.extend_from_slice(wrapped);
            }
            KeyRecord::Destroyed => out.push(STATE_DESTROYED),
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        const OP: &str = "decode subject key file";
        if !bytes.is_empty() && bytes.iter().all(|byte| *byte == 0) {
            return Ok(KeyRecord::Destroyed);
        }
        let rest = bytes
            .strip_prefix(KEY_FILE_MAGIC.as_slice())
            .ok_or_else(|| corrupt(OP, "not a subject key file"))?;
        match rest {
            [KEY_FILE_VERSION, STATE_DESTROYED, ..] => Ok(KeyRecord::Destroyed),
            [KEY_FILE_VERSION, STATE_LIVE, scheme, body @ ..] if body.len() >= 12 => {
                let scheme = scheme_of(*scheme).ok_or_else(|| corrupt(OP, "unknown AEAD scheme"))?;
                let epoch_high_water = u64_at(body, 0);
                let len = body
                    .get(8..12)
                    .and_then(|raw| raw.try_into().ok())
                    .map_or(0, u32::from_le_bytes) as usize;
                let wrapped = body
                    .get(12..12 + len)
                    .ok_or_else(|| corrupt(OP, "truncated wrapped key"))?
                    .to_vec();
                Ok(KeyRecord::Live {
                    epoch_high_water,
                    scheme,
                    wrapped,
                })
            }
            _ => Err(corrupt(OP, "unknown subject key file version or state")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubjectErasure {
    pub erased_at_nanos: i64,
    pub jurisdiction_profile_version: Option<u64>,
    pub subject: SubjectId,
}

pub struct FileSubjectKeyStore {
    checked_out: Mutex<HashMap<SubjectId, SubjectContentKey>>,
    dir: PathBuf,
    scheme: AeadScheme,
    tenant_dek: TenantDek,
}

impl std::fmt::Debug for FileSubjectKeyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSubjectKeyStore")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl FileSubjectKeyStore {
    pub fn open(dir: impl Into<PathBuf>, tenant_dek: TenantDek) -> Result<Self, StorageError> {
        Self::open_with(dir, tenant_dek, AeadScheme::default())
    }

    pub fn open_with(dir: impl Into<PathBuf>, tenant_dek: TenantDek, scheme: AeadScheme) -> Result<Self, StorageError> {
        let dir = dir.into();
        fs::create_dir_all(dir.join("keys")).map_err(io("create subject key directory"))?;
        let store = Self {
            checked_out: Mutex::new(HashMap::new()),
            dir,
            scheme,
            tenant_dek,
        };
        for erasure in store.erasures()? {
            store.destroy_key(erasure.subject)?;
        }
        Ok(store)
    }

    fn key_path(&self, subject: SubjectId) -> PathBuf {
        self.dir
            .join("keys")
            .join(format!("{:032x}.key", subject.uuid().as_u128()))
    }

    fn erasure_log(&self) -> PathBuf {
        self.dir.join("erasures.log")
    }

    fn erasure_log_v2(&self) -> PathBuf {
        self.dir.join("erasures.v2.log")
    }

    fn read_record(&self, subject: SubjectId) -> Result<Option<KeyRecord>, StorageError> {
        match fs::read(self.key_path(subject)) {
            Ok(bytes) => KeyRecord::decode(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io("read subject key file")(error)),
        }
    }

    fn unwrap(&self, subject: SubjectId, wrapped: &[u8], scheme: AeadScheme) -> Result<ContentKey, StorageError> {
        self.tenant_dek
            .unwrap_content_key(wrapped, ContentKeyId::from_uuid(subject.uuid()), scheme)
            .ok_or_else(|| {
                corrupt(
                    "unwrap subject key",
                    "the wrapped key does not open under the tenant DEK",
                )
            })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SubjectId, SubjectContentKey>> {
        self.checked_out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn is_erased(&self, subject: SubjectId) -> Result<bool, StorageError> {
        Ok(matches!(self.read_record(subject)?, Some(KeyRecord::Destroyed)))
    }

    pub fn erasures(&self) -> Result<Vec<SubjectErasure>, StorageError> {
        let mut seen = BTreeMap::new();
        let legacy = read_records(&self.erasure_log(), ERASURE_RECORD_LEN)?
            .into_iter()
            .map(|record| (record, None));
        let versioned = read_records(&self.erasure_log_v2(), ERASURE_RECORD_V2_LEN)?
            .into_iter()
            .map(|record| {
                let version = (record.get(24) == Some(&1)).then(|| u64_at(&record, 25));
                (record, version)
            });
        for (record, version) in legacy.chain(versioned) {
            let subject = SubjectId::from_uuid(uuid::Uuid::from_u128(u128_at(&record, 0)));
            let erased_at_nanos = u64_at(&record, 16) as i64;
            let order = seen.len();
            seen.entry(subject).or_insert((order, erased_at_nanos, version));
        }
        let mut erasures: Vec<(usize, SubjectErasure)> = seen
            .into_iter()
            .map(|(subject, (order, erased_at_nanos, jurisdiction_profile_version))| {
                (
                    order,
                    SubjectErasure {
                        erased_at_nanos,
                        jurisdiction_profile_version,
                        subject,
                    },
                )
            })
            .collect();
        erasures.sort_by_key(|(order, _)| *order);
        Ok(erasures.into_iter().map(|(_, erasure)| erasure).collect())
    }

    pub fn erase_subject(&self, subject: SubjectId, clock: &dyn Clock) -> Result<SubjectErasure, StorageError> {
        self.erase_subject_under_profile(subject, None, clock)
    }

    pub fn erase_subject_under_profile(
        &self,
        subject: SubjectId,
        jurisdiction_profile_version: Option<u64>,
        clock: &dyn Clock,
    ) -> Result<SubjectErasure, StorageError> {
        if let Some(earlier) = self.erasures()?.into_iter().find(|erasure| erasure.subject == subject) {
            self.destroy_key(subject)?;
            return Ok(earlier);
        }
        let erasure = SubjectErasure {
            erased_at_nanos: clock.now_nanos(),
            jurisdiction_profile_version,
            subject,
        };
        let mut record = [0u8; ERASURE_RECORD_V2_LEN];
        record[..16].copy_from_slice(&subject.uuid().as_u128().to_le_bytes());
        record[16..24].copy_from_slice(&(erasure.erased_at_nanos as u64).to_le_bytes());
        if let Some(version) = jurisdiction_profile_version {
            record[24] = 1;
            record[25..].copy_from_slice(&version.to_le_bytes());
        }
        append_synced(&self.erasure_log_v2(), &record)?;
        self.destroy_key(subject)?;
        Ok(erasure)
    }

    fn destroy_key(&self, subject: SubjectId) -> Result<(), StorageError> {
        if let Some(mut key) = self.lock().remove(&subject) {
            key.destroy();
        }
        let path = self.key_path(subject);
        match self.read_record(subject)? {
            Some(KeyRecord::Destroyed) => {
                if fs::read(&path)
                    .map_err(io("read subject key file"))?
                    .starts_with(KEY_FILE_MAGIC)
                {
                    return Ok(());
                }
            }
            Some(KeyRecord::Live { .. }) => {
                let mut file = OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(io("open subject key file for erasure"))?;
                let len = file.metadata().map_err(io("stat subject key file"))?.len();
                file.write_all(&vec![0u8; usize::try_from(len).unwrap_or(0)])
                    .map_err(io("overwrite subject key file"))?;
                file.sync_all().map_err(io("sync overwritten subject key file"))?;
            }
            None => {}
        }
        write_atomically(&path, &KeyRecord::Destroyed.encode())
    }
}

impl SubjectKeyStore for FileSubjectKeyStore {
    fn opening_key(&self, subject: SubjectId) -> Result<Option<SubjectContentKey>, StorageError> {
        match self.read_record(subject)? {
            Some(KeyRecord::Live { scheme, wrapped, .. }) => {
                let key = self.unwrap(subject, &wrapped, scheme)?;
                Ok(Some(SubjectContentKey::from_sealing_key(SealedContentKey::new(
                    key, 0, scheme,
                ))))
            }
            Some(KeyRecord::Destroyed) | None => Ok(None),
        }
    }

    fn sealing_key(&self, subject: SubjectId) -> Result<Option<SubjectContentKey>, StorageError> {
        let mut checked_out = self.lock();
        if let Some(key) = checked_out.get(&subject)
            && !key.is_destroyed()
        {
            return Ok(Some(key.clone()));
        }
        let (key, scheme, first_epoch) = match self.read_record(subject)? {
            Some(KeyRecord::Destroyed) => return Ok(None),
            Some(KeyRecord::Live {
                epoch_high_water,
                scheme,
                wrapped,
            }) => (self.unwrap(subject, &wrapped, scheme)?, scheme, epoch_high_water),
            None => {
                let mut material = zeroize::Zeroizing::new([0u8; 32]);
                getrandom::fill(material.as_mut()).map_err(|error| StorageError::Io {
                    op: "generate subject key",
                    detail: error.to_string(),
                })?;
                (ContentKey::new(*material), self.scheme, 0)
            }
        };
        let epoch_limit = first_epoch
            .checked_add(EPOCHS_PER_CHECKOUT)
            .ok_or_else(|| corrupt("reserve subject key epochs", "the key's epochs are spent"))?;
        let wrapped = self
            .tenant_dek
            .wrap_content_key(&key, ContentKeyId::from_uuid(subject.uuid()), scheme)
            .ok_or_else(|| corrupt("wrap subject key", "wrapping the subject key failed"))?;
        write_atomically(
            &self.key_path(subject),
            &KeyRecord::Live {
                epoch_high_water: epoch_limit,
                scheme,
                wrapped,
            }
            .encode(),
        )?;
        let sealing = SealedContentKey::with_epoch_range(key, first_epoch, epoch_limit, scheme)
            .ok_or_else(|| corrupt("reserve subject key epochs", "empty epoch range"))?;
        let key = SubjectContentKey::from_sealing_key(sealing);
        checked_out.insert(subject, key.clone());
        Ok(Some(key))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacklogEntry {
    pub event_id: EventId,
    pub file_id: u128,
    pub subject: Option<SubjectId>,
}

#[derive(Debug, Clone)]
pub struct FileErasureBacklog {
    path: PathBuf,
}

impl FileErasureBacklog {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn append(&self, tag: u8, first: u128, second: u128) -> Result<(), StorageError> {
        let mut record = [0u8; BACKLOG_RECORD_LEN];
        record[0] = tag;
        record[1..17].copy_from_slice(&first.to_le_bytes());
        record[17..].copy_from_slice(&second.to_le_bytes());
        append_synced(&self.path, &record)
    }

    pub fn record_unresolved(&self, file_id: u128, event_id: EventId) -> Result<(), StorageError> {
        self.append(BACKLOG_PENDING, file_id, event_id.uuid().as_u128())
    }

    pub fn resolve(&self, event_id: EventId, subject: SubjectId) -> Result<(), StorageError> {
        self.append(BACKLOG_RESOLVED, event_id.uuid().as_u128(), subject.uuid().as_u128())
    }

    pub fn mark_rekeyed(&self, event_id: EventId) -> Result<(), StorageError> {
        self.append(BACKLOG_REKEYED, event_id.uuid().as_u128(), 0)
    }

    pub fn retarget(&self, from_file: u128, to_file: u128) -> Result<(), StorageError> {
        self.append(BACKLOG_RETARGETED, from_file, to_file)
    }

    pub fn pending(&self) -> Result<Vec<BacklogEntry>, StorageError> {
        let mut order = Vec::new();
        let mut entries: HashMap<u128, BacklogEntry> = HashMap::new();
        let id = |raw: u128| EventId::from_uuid(uuid::Uuid::from_u128(raw));
        for record in read_records(&self.path, BACKLOG_RECORD_LEN)? {
            let first = u128_at(&record, 1);
            let second = u128_at(&record, 17);
            match record.first().copied().unwrap_or(0) {
                BACKLOG_PENDING => {
                    if entries
                        .insert(
                            second,
                            BacklogEntry {
                                event_id: id(second),
                                file_id: first,
                                subject: None,
                            },
                        )
                        .is_none()
                    {
                        order.push(second);
                    }
                }
                BACKLOG_RESOLVED => {
                    if let Some(entry) = entries.get_mut(&first) {
                        entry.subject = Some(SubjectId::from_uuid(uuid::Uuid::from_u128(second)));
                    }
                }
                BACKLOG_REKEYED => {
                    entries.remove(&first);
                }
                BACKLOG_RETARGETED => {
                    for entry in entries.values_mut().filter(|entry| entry.file_id == first) {
                        entry.file_id = second;
                    }
                }
                _ => return Err(corrupt("read erasure backlog", "unknown record tag")),
            }
        }
        Ok(order.into_iter().filter_map(|event| entries.remove(&event)).collect())
    }
}

#[cfg(test)]
#[path = "test/subject_keys.rs"]
mod tests;
