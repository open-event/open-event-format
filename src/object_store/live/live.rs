use super::*;
use crate::error::{PublishError, StorageError};
use crate::invariants::PublishedSet;
use crate::lifecycle::ManifestGeneration;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

pub struct LivePublishedSet {
    head_read: Mutex<Option<HeadRead>>,
    prefix: String,
    store: Arc<dyn ObjectStore>,
}

#[derive(Debug, Clone)]
struct HeadRead {
    etag: Option<ETag>,
    generation: u64,
}

impl LivePublishedSet {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            head_read: Mutex::new(None),
            prefix: prefix.into(),
            store,
        }
    }

    fn head_key(&self) -> String {
        format!("{}/head", self.prefix)
    }

    fn generation_key(&self, id: u64) -> String {
        format!("{}/generations/{id:020}", self.prefix)
    }

    fn remembered_head(&self) -> MutexGuard<'_, Option<HeadRead>> {
        self.head_read.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn read_head(&self) -> Result<HeadRead, PublishError> {
        let read = match self.store.get(&self.head_key()).map_err(backend)? {
            None => HeadRead {
                etag: None,
                generation: 0,
            },
            Some(object) => HeadRead {
                generation: simdutf8::basic::from_utf8(&object.bytes)
                    .ok()
                    .and_then(|text| text.parse().ok())
                    .ok_or_else(|| PublishError::Io {
                        detail: "catalogue head pointer does not hold a generation id".to_owned(),
                    })?,
                etag: Some(object.etag),
            },
        };
        *self.remembered_head() = Some(read.clone());
        Ok(read)
    }
}

fn backend(error: StorageError) -> PublishError {
    PublishError::Io {
        detail: error.to_string(),
    }
}

impl PublishedSet for LivePublishedSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError> {
        let read = self.read_head()?;
        Ok((read.generation, self.generation(read.generation)?))
    }

    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError> {
        let id = generation.generation;
        if id == 0 {
            return Err(PublishError::GenerationExists);
        }
        let checkpoint = id - id % constant::CHECKPOINT_INTERVAL;
        let stored = if checkpoint == id {
            model::StoredGeneration::Checkpoint(generation)
        } else {
            model::StoredGeneration::Delta(model::GenerationDelta::between(
                &self.generation(checkpoint)?,
                &generation,
            ))
        };
        let bytes = serde_json::to_vec(&stored).map_err(|error| PublishError::Io {
            detail: format!("catalogue generation {id} does not encode: {error}"),
        })?;
        match self
            .store
            .put_if_absent(&self.generation_key(id), &bytes)
            .map_err(backend)?
        {
            PutOutcome::Written { .. } => Ok(()),
            PutOutcome::PreconditionFailed => Err(PublishError::GenerationExists),
        }
    }

    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError> {
        if self.store.stat(&self.generation_key(next)).map_err(backend)?.is_none() {
            return Err(PublishError::UnknownGeneration);
        }
        let remembered = self
            .remembered_head()
            .clone()
            .filter(|read| read.generation == expected);
        let read = match remembered {
            Some(read) => read,
            None => self.read_head()?,
        };
        if read.generation != expected {
            return Err(PublishError::CasLost {
                current_generation: read.generation,
            });
        }
        let pointer = next.to_string().into_bytes();
        let outcome = match &read.etag {
            None => self.store.put_if_absent(&self.head_key(), &pointer),
            Some(etag) => self.store.put_if_match(&self.head_key(), &pointer, etag),
        }
        .map_err(backend)?;
        match outcome {
            PutOutcome::Written { etag } => {
                *self.remembered_head() = Some(HeadRead {
                    etag: Some(etag),
                    generation: next,
                });
                Ok(())
            }
            PutOutcome::PreconditionFailed => Err(PublishError::CasLost {
                current_generation: self.read_head()?.generation,
            }),
        }
    }

    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError> {
        let Some(object) = self.store.get(&self.generation_key(id)).map_err(backend)? else {
            return if id == 0 {
                Ok(ManifestGeneration::default())
            } else {
                Err(PublishError::UnknownGeneration)
            };
        };
        let stored: model::StoredGeneration =
            serde_json::from_slice(&object.bytes).map_err(|error| PublishError::Io {
                detail: format!("catalogue generation {id} does not decode: {error}"),
            })?;
        match stored {
            model::StoredGeneration::Checkpoint(generation) => Ok(generation),
            model::StoredGeneration::Delta(delta) if delta.checkpoint < id => {
                let checkpoint = self.generation(delta.checkpoint)?;
                Ok(delta.apply(checkpoint))
            }
            model::StoredGeneration::Delta(_) => Err(PublishError::Io {
                detail: format!("catalogue generation {id} names a checkpoint that is not earlier than itself"),
            }),
        }
    }
}

#[cfg(test)]
#[path = "test/live.rs"]
mod tests;
