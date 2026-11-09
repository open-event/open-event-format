use crate::deletes::{CorrectionMetadata, CorrectionType, DeletionVector, FieldDeletionVector};
use crate::events::{EventId, SequencePoint};
use hashbrown::{HashMap, HashSet};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldChange {
    Replaced,
    Redacted,
}

#[derive(Debug, Clone)]
pub struct CorrectionRecord {
    pub correcting_event_id: EventId,
    pub field_changes: BTreeMap<String, FieldChange>,
    pub metadata: CorrectionMetadata,
}

impl CorrectionRecord {
    pub fn point(&self) -> SequencePoint {
        SequencePoint {
            epoch: self.metadata.correction_epoch,
            sequence: self.metadata.correction_sequence,
        }
    }

    fn order_key(&self) -> (u64, u64, u64, EventId) {
        (
            self.metadata.correction_epoch,
            self.metadata.correction_sequence,
            self.metadata.correction_generation,
            self.correcting_event_id,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentVersion {
    Event(EventId),
    Retracted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEvent {
    pub chain: Vec<EventId>,
    pub current: CurrentVersion,
    pub replaced_fields: BTreeMap<String, EventId>,
    pub root_event_id: EventId,
    pub winner: Option<CorrectionMetadata>,
    pub withheld_fields: BTreeSet<String>,
}

impl ResolvedEvent {
    pub fn withholds(&self, field: &str) -> bool {
        self.withheld_fields.contains(field)
    }

    pub fn superseded_events(&self) -> impl Iterator<Item = EventId> + '_ {
        let current = match self.current {
            CurrentVersion::Event(id) => Some(id),
            CurrentVersion::Retracted => None,
        };
        std::iter::once(self.root_event_id)
            .chain(self.chain.iter().copied())
            .filter(move |id| Some(*id) != current)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CorrectionResolver {
    by_correcting_event: HashMap<EventId, usize>,
    by_point: HashSet<SequencePoint>,
    direct: HashMap<EventId, Vec<usize>>,
    records: Vec<CorrectionRecord>,
}

impl CorrectionResolver {
    pub fn new(records: impl IntoIterator<Item = CorrectionRecord>) -> Self {
        let mut records: Vec<CorrectionRecord> = records.into_iter().collect();
        records.sort_by_key(CorrectionRecord::order_key);
        let mut seen = HashSet::new();
        records.retain(|record| seen.insert(record.correcting_event_id));
        let mut resolver = Self::default();
        for (index, record) in records.iter().enumerate() {
            resolver.by_correcting_event.insert(record.correcting_event_id, index);
            resolver.by_point.insert(record.point());
            resolver
                .direct
                .entry(record.metadata.corrects_event_id)
                .or_default()
                .push(index);
        }
        resolver.records = records;
        resolver
    }

    pub fn is_correction_at(&self, point: SequencePoint) -> bool {
        self.by_point.contains(&point)
    }

    pub fn is_correction(&self, event_id: EventId) -> bool {
        self.by_correcting_event.contains_key(&event_id)
    }

    pub fn latest_correction(&self, event_id: EventId) -> Option<CorrectionMetadata> {
        let last = *self.direct.get(&event_id)?.last()?;
        self.records.get(last).map(|record| record.metadata.clone())
    }

    pub fn root_of(&self, event_id: EventId) -> EventId {
        let mut current = event_id;
        let mut visited = HashSet::new();
        while let Some(record) = self
            .by_correcting_event
            .get(&current)
            .and_then(|&index| self.records.get(index))
        {
            if !visited.insert(current) {
                break;
            }
            current = record.metadata.corrects_event_id;
        }
        current
    }

    pub fn resolve(&self, event_id: EventId) -> ResolvedEvent {
        let root = self.root_of(event_id);
        let mut on_chain = Vec::new();
        let mut frontier = vec![root];
        let mut visited: HashSet<EventId> = HashSet::new();
        visited.insert(root);
        while let Some(target) = frontier.pop() {
            for &index in self.direct.get(&target).into_iter().flatten() {
                let Some(id) = self.records.get(index).map(|record| record.correcting_event_id) else {
                    continue;
                };
                if visited.insert(id) {
                    on_chain.push(index);
                    frontier.push(id);
                }
            }
        }
        on_chain.sort_unstable();

        let mut resolved = ResolvedEvent {
            chain: Vec::with_capacity(on_chain.len()),
            current: CurrentVersion::Event(root),
            replaced_fields: BTreeMap::new(),
            root_event_id: root,
            winner: None,
            withheld_fields: BTreeSet::new(),
        };
        for record in on_chain.into_iter().filter_map(|index| self.records.get(index)) {
            resolved.chain.push(record.correcting_event_id);
            resolved.winner = Some(record.metadata.clone());
            match record.metadata.correction_type {
                CorrectionType::Replacement => {
                    resolved.current = CurrentVersion::Event(record.correcting_event_id);
                    resolved.replaced_fields.clear();
                    resolved.withheld_fields.clear();
                }
                CorrectionType::Retraction => {
                    resolved.current = CurrentVersion::Retracted;
                    resolved.replaced_fields.clear();
                    resolved.withheld_fields.clear();
                }
                CorrectionType::Amendment => {
                    if resolved.current == CurrentVersion::Retracted {
                        continue;
                    }
                    for (field, change) in &record.field_changes {
                        match change {
                            FieldChange::Replaced => {
                                resolved.withheld_fields.remove(field);
                                resolved
                                    .replaced_fields
                                    .insert(field.clone(), record.correcting_event_id);
                            }
                            FieldChange::Redacted => {
                                resolved.replaced_fields.remove(field);
                                resolved.withheld_fields.insert(field.clone());
                            }
                        }
                    }
                }
            }
        }
        resolved
    }

    pub fn superseded_deletion_vector(&self, events: &[(u64, EventId)]) -> DeletionVector {
        let mut vector = DeletionVector::new();
        for &(ordinal, id) in events {
            if self.is_correction(id) || self.direct.contains_key(&id) {
                let resolved = self.resolve(id);
                if resolved.superseded_events().any(|superseded| superseded == id) {
                    vector.mark_deleted(ordinal);
                }
            }
        }
        vector
    }

    pub fn field_deletion_vectors(&self, events: &[(u64, EventId)]) -> Vec<FieldDeletionVector> {
        let mut by_columns: BTreeMap<Vec<String>, Vec<u64>> = BTreeMap::new();
        for &(ordinal, id) in events {
            if !self.direct.contains_key(&id) && !self.is_correction(id) {
                continue;
            }
            let resolved = self.resolve(id);
            if resolved.current == CurrentVersion::Event(id) && !resolved.withheld_fields.is_empty() {
                let columns = resolved.withheld_fields.into_iter().collect();
                by_columns.entry(columns).or_default().push(ordinal);
            }
        }
        by_columns
            .into_iter()
            .map(|(columns, ordinals)| {
                let mut vector = FieldDeletionVector::new(columns);
                for ordinal in ordinals {
                    vector.mark_affected(ordinal);
                }
                vector
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "test/corrections.rs"]
mod tests;
