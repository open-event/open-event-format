use crate::constant::RECOMPUTE_QUEUE_COMPACTION_FLOOR;
use crate::error::FormatError;
use crate::events::{EventId, SequenceRange};
use crate::file::bytes::{Reader, Writer};
use crate::indexes::bitmap::{RoaringRangeBitmap, RowRange, encoded_runs_len, put_encoded_runs};
use crate::security::{AeadScheme, ContentKey, SealedContentKey};
use hashbrown::HashMap;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct DeletionVector {
    deleted_count: u64,
    deleted_runs: Vec<RowRange>,
}

impl DeletionVector {
    pub fn new() -> Self {
        Self {
            deleted_count: 0,
            deleted_runs: Vec::new(),
        }
    }

    pub fn mark_deleted(&mut self, ordinal: u64) {
        self.mark_range(RowRange {
            start: ordinal,
            end: ordinal + 1,
        });
    }

    fn mark_range(&mut self, range: RowRange) {
        if range.end <= range.start {
            return;
        }
        let first = self.deleted_runs.partition_point(|run| run.end < range.start);
        let mut merged = range;
        let mut past = first;
        while let Some(run) = self.deleted_runs.get(past) {
            if run.start > range.end {
                break;
            }
            merged.start = merged.start.min(run.start);
            merged.end = merged.end.max(run.end);
            past += 1;
        }
        let absorbed: u64 = self
            .deleted_runs
            .get(first..past)
            .unwrap_or_default()
            .iter()
            .map(|run| run.end - run.start)
            .sum();
        self.deleted_count += (merged.end - merged.start) - absorbed;
        self.deleted_runs.splice(first..past, std::iter::once(merged));
    }

    pub fn union_with(&mut self, other: &DeletionVector) {
        for run in &other.deleted_runs {
            self.mark_range(*run);
        }
    }

    #[inline]
    pub fn is_deleted(&self, ordinal: u64) -> bool {
        let index = self.deleted_runs.partition_point(|run| run.end <= ordinal);
        self.deleted_runs.get(index).is_some_and(|run| run.start <= ordinal)
    }

    pub fn deleted_count(&self) -> u64 {
        self.deleted_count
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.deleted_runs.iter().flat_map(|run| run.start..run.end)
    }

    pub fn exclude_deleted(&self, first_ordinal: u64, keep: &mut [bool]) {
        let past_last_ordinal = first_ordinal + keep.len() as u64;
        let first_run = self.deleted_runs.partition_point(|run| run.end <= first_ordinal);
        for run in self.deleted_runs.get(first_run..).unwrap_or_default() {
            if run.start >= past_last_ordinal {
                break;
            }
            let from = (run.start.max(first_ordinal) - first_ordinal) as usize;
            let past = (run.end.min(past_last_ordinal) - first_ordinal) as usize;
            if let Some(span) = keep.get_mut(from..past) {
                span.fill(false);
            }
        }
    }

    pub fn apply(&self, ordinals: &[u64]) -> Vec<u64> {
        let mut kept = Vec::with_capacity(ordinals.len());
        let mut runs = self.deleted_runs.iter().peekable();
        for &ordinal in ordinals {
            while runs.peek().is_some_and(|run| run.end <= ordinal) {
                runs.next();
            }
            if runs.peek().is_none_or(|run| run.start > ordinal) {
                kept.push(ordinal);
            }
        }
        kept
    }
}

impl DeletionVector {
    pub fn deleted_in(&self, first_ordinal: u64, row_count: u32) -> u64 {
        let past_last_ordinal = first_ordinal.saturating_add(u64::from(row_count));
        let first_run = self.deleted_runs.partition_point(|run| run.end <= first_ordinal);
        self.deleted_runs
            .iter()
            .skip(first_run)
            .take_while(|run| run.start < past_last_ordinal)
            .map(|run| run.end.min(past_last_ordinal) - run.start.max(first_ordinal))
            .sum()
    }

    pub fn selection(&self, first_ordinal: u64, row_count: u32, predicate: Option<Vec<bool>>) -> RowSelection {
        let deleted = self.deleted_in(first_ordinal, row_count);
        if deleted >= u64::from(row_count) {
            return RowSelection::None;
        }
        match predicate {
            None if deleted == 0 => RowSelection::All,
            None => {
                let mut keep = vec![true; row_count as usize];
                self.exclude_deleted(first_ordinal, &mut keep);
                RowSelection::Some(keep)
            }
            Some(mut mask) => {
                if deleted > 0 {
                    self.exclude_deleted(first_ordinal, &mut mask);
                }
                RowSelection::from_mask(mask)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSelection {
    All,
    None,
    Some(Vec<bool>),
}

impl RowSelection {
    pub fn from_mask(mask: Vec<bool>) -> Self {
        if !mask.contains(&true) {
            RowSelection::None
        } else if !mask.contains(&false) {
            RowSelection::All
        } else {
            RowSelection::Some(mask)
        }
    }
}

impl Default for DeletionVector {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDeletionVector {
    affected_ordinals: BTreeSet<u64>,
    redacted_columns: Vec<String>,
}

impl FieldDeletionVector {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            affected_ordinals: BTreeSet::new(),
            redacted_columns: columns,
        }
    }

    pub fn mark_affected(&mut self, ordinal: u64) {
        self.affected_ordinals.insert(ordinal);
    }

    pub fn column_redacted(&self, column: &str, ordinal: u64) -> bool {
        self.affected_ordinals.contains(&ordinal) && self.redacted_columns.iter().any(|c| c == column)
    }

    pub fn withholds_raw_payload(&self, ordinal: u64) -> bool {
        self.affected_ordinals.contains(&ordinal)
    }

    pub fn redacted_columns(&self) -> &[String] {
        &self.redacted_columns
    }

    pub fn affected_ordinals(&self) -> impl Iterator<Item = u64> + '_ {
        self.affected_ordinals.iter().copied()
    }

    pub fn affected_count(&self) -> u64 {
        self.affected_ordinals.len() as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowPositionDomain {
    PrimaryRowsetOrdinal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionVectorEncoding {
    Inline,
    NativeBlock,
}

#[derive(Debug, Clone)]
pub struct DeletionVectorRef {
    pub blake3: [u8; 32],
    pub deleted_count: u64,
    pub deletion_vector_generation: u64,
    pub encoding: DeletionVectorEncoding,
    pub row_position_domain: RowPositionDomain,
    pub target_file_id: u128,
    pub target_projection_id: Option<u64>,
    pub target_sequence_range: SequenceRange,
}

#[derive(Debug, Clone)]
pub struct FieldDeletionVectorRef {
    pub blake3: [u8; 32],
    pub deleted_count: u64,
    pub deletion_vector_generation: u64,
    pub redacted_columns: Vec<String>,
    pub target_file_id: u128,
    pub target_projection_id: Option<u64>,
    pub target_sequence_range: SequenceRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrectionType {
    Amendment,
    Replacement,
    Retraction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrectionMetadata {
    pub correction_epoch: u64,
    pub correction_generation: u64,
    pub correction_sequence: u64,
    pub correction_type: CorrectionType,
    pub corrects_event_id: EventId,
}

pub fn latest_corrected_deletion_vector(
    events: &[(u64, EventId)],
    corrections: &[CorrectionMetadata],
) -> DeletionVector {
    if events.is_empty() || corrections.is_empty() {
        return DeletionVector::new();
    }
    let mut ordinal_by_event = HashMap::with_capacity(events.len());
    for (ordinal, id) in events {
        ordinal_by_event.entry(*id).or_insert(*ordinal);
    }
    let mut deleted_runs: Vec<RowRange> = corrections
        .iter()
        .filter_map(|correction| ordinal_by_event.get(&correction.corrects_event_id))
        .map(|&ordinal| RowRange {
            start: ordinal,
            end: ordinal + 1,
        })
        .collect();
    deleted_runs.sort_unstable_by_key(|run| run.start);
    deleted_runs.dedup_by(|next, kept| {
        let touches = next.start <= kept.end;
        if touches {
            kept.end = kept.end.max(next.end);
        }
        touches
    });
    DeletionVector {
        deleted_count: deleted_runs.iter().map(|run| run.end - run.start).sum(),
        deleted_runs,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DeletionAggregateDelta {
    pub deleted_count: u64,
    pub deleted_sum: Option<i128>,
}

impl DeletionAggregateDelta {
    pub fn subtract_count(&self, total_count: u64) -> u64 {
        total_count.saturating_sub(self.deleted_count)
    }

    pub fn subtract_sum(&self, total_sum: i128) -> Option<i128> {
        total_sum.checked_sub(self.deleted_sum?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DerivedColumnState {
    Pending,
    Published { value: f64, watermark_sequence: u64 },
}

#[derive(Debug, Default)]
pub struct DerivedColumnLedger {
    queue_position: HashMap<EventId, usize>,
    recompute_queue: Vec<Option<EventId>>,
    states: HashMap<EventId, DerivedColumnState>,
}

impl DerivedColumnLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn invalidate(&mut self, event_id: EventId, neighbors: &[EventId]) {
        for id in std::iter::once(event_id).chain(neighbors.iter().copied()) {
            let previous = self.states.insert(id, DerivedColumnState::Pending);
            if previous != Some(DerivedColumnState::Pending) {
                self.compact_recompute_queue();
                let position = self.recompute_queue.len();
                self.recompute_queue.push(Some(id));
                self.queue_position.insert(id, position);
            }
        }
    }

    pub fn republish(&mut self, event_id: EventId, value: f64, watermark_sequence: u64) {
        let previous = self.states.insert(
            event_id,
            DerivedColumnState::Published {
                value,
                watermark_sequence,
            },
        );
        if previous == Some(DerivedColumnState::Pending)
            && let Some(position) = self.queue_position.remove(&event_id)
            && let Some(slot) = self.recompute_queue.get_mut(position)
        {
            *slot = None;
            if self.queue_position.is_empty() {
                self.recompute_queue.clear();
            }
        }
    }

    fn compact_recompute_queue(&mut self) {
        let len = self.recompute_queue.len();
        if len <= RECOMPUTE_QUEUE_COMPACTION_FLOOR || self.queue_position.len() > len / 2 {
            return;
        }
        let queue_position = &mut self.queue_position;
        let mut next = 0;
        self.recompute_queue.retain(|slot| {
            let Some(id) = slot else {
                return false;
            };
            if let Some(position) = queue_position.get_mut(id) {
                *position = next;
            }
            next += 1;
            true
        });
    }

    pub fn published_value(&self, event_id: EventId) -> Option<f64> {
        match self.states.get(&event_id)? {
            DerivedColumnState::Published { value, .. } => Some(*value),
            DerivedColumnState::Pending => None,
        }
    }

    pub fn pending_recomputes(&self) -> Vec<EventId> {
        self.recompute_queue.iter().filter_map(|slot| *slot).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityClass {
    BusinessRecord,
    DataSubject,
    LegalEntity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErasureDisposition {
    CryptoShred,
    PhysicalDestruction,
    Retain,
    RestrictSuppress,
}

impl ErasureDisposition {
    fn protection_rank(self) -> u8 {
        match self {
            ErasureDisposition::Retain => 0,
            ErasureDisposition::RestrictSuppress => 1,
            ErasureDisposition::PhysicalDestruction => 2,
            ErasureDisposition::CryptoShred => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityDisposition {
    pub disposition: ErasureDisposition,
    pub retention_carve_out: bool,
}

pub fn resolve_conflicting_dispositions(dispositions: &[EntityDisposition]) -> Option<EntityDisposition> {
    dispositions
        .iter()
        .find(|d| d.retention_carve_out)
        .or_else(|| dispositions.iter().max_by_key(|d| d.disposition.protection_rank()))
        .copied()
}

crate::typed_id::define_typed_id!(SubjectId, SubjectIdTag, "subject");

const REDACTED_FIELD_MARKER: &str = "[redacted]";

#[derive(Debug, Clone)]
pub struct BusinessRecord {
    pub amount: i128,
    pub personal_fields: BTreeMap<String, String>,
    pub subject_id: Option<SubjectId>,
}

pub fn de_identify_business_record(record: &mut BusinessRecord) {
    record.subject_id = None;
    for value in record.personal_fields.values_mut() {
        *value = REDACTED_FIELD_MARKER.to_owned();
    }
}

#[derive(Debug, Clone)]
pub struct SubjectContentKey {
    key: Option<SealedContentKey>,
    key_epoch: u64,
    scheme: AeadScheme,
}

impl zeroize::ZeroizeOnDrop for SubjectContentKey {}

impl SubjectContentKey {
    pub fn generate(key_material: [u8; 32]) -> Self {
        Self::generate_with(key_material, 0, AeadScheme::default())
    }

    pub fn generate_with(key_material: [u8; 32], key_epoch: u64, scheme: AeadScheme) -> Self {
        Self {
            key: Some(SealedContentKey::new(ContentKey::new(key_material), key_epoch, scheme)),
            key_epoch,
            scheme,
        }
    }

    pub fn from_sealing_key(sealing_key: SealedContentKey) -> Self {
        let key_epoch = sealing_key.key_epoch();
        let scheme = sealing_key.scheme();
        Self {
            key: Some(sealing_key),
            key_epoch,
            scheme,
        }
    }

    pub fn key_epoch(&self) -> u64 {
        self.key_epoch
    }

    pub fn scheme(&self) -> AeadScheme {
        self.scheme
    }

    pub fn encrypt(&mut self, block_id: u64, file_id: u128, plaintext: &[u8]) -> Option<Vec<u8>> {
        let key = self.key.as_mut()?;
        let sealed = key.encrypt(block_id, file_id, plaintext)?;
        self.key_epoch = key.key_epoch();
        Some(sealed)
    }

    pub fn destroy(&mut self) {
        self.key = None;
    }

    pub fn is_destroyed(&self) -> bool {
        self.key.is_none()
    }

    fn open(&self, block_id: u64, file_id: u128, sealed: &[u8]) -> RebuiltPayload {
        match &self.key {
            None => RebuiltPayload::Tombstone,
            Some(key) => match key.decrypt(block_id, file_id, sealed) {
                Some(plaintext) => RebuiltPayload::Payload(plaintext),
                None => RebuiltPayload::Rejected,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuiltPayload {
    Payload(Vec<u8>),
    Rejected,
    Tombstone,
}

pub fn rebuild_payload(key: &SubjectContentKey, block_id: u64, file_id: u128, sealed: &[u8]) -> RebuiltPayload {
    key.open(block_id, file_id, sealed)
}

pub const DELETION_VECTOR_ORDINAL_ARRAY_MAX: u64 = 4096;

const MAX_DELETION_VECTOR_DECODED_ROWS: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionVectorWireForm {
    OrdinalArray,
    PositionalRoaring,
}

impl DeletionVectorWireForm {
    fn tag(self) -> u8 {
        match self {
            DeletionVectorWireForm::OrdinalArray => 1,
            DeletionVectorWireForm::PositionalRoaring => 2,
        }
    }
}

pub fn encode_deletion_vector(vector: &DeletionVector) -> (DeletionVectorWireForm, Vec<u8>) {
    let fits_ordinals = vector.deleted_count() < DELETION_VECTOR_ORDINAL_ARRAY_MAX
        && vector.iter().all(|ordinal| ordinal <= u64::from(u32::MAX));
    if fits_ordinals {
        let mut out = Writer::with_capacity(5 + vector.deleted_count() as usize * 4);
        out.put_u8(DeletionVectorWireForm::OrdinalArray.tag());
        out.put_u32(vector.deleted_count() as u32);
        for ordinal in vector.iter() {
            out.put_u32(ordinal as u32);
        }
        (DeletionVectorWireForm::OrdinalArray, out.into_bytes())
    } else {
        let mut out = Writer::with_capacity(1 + encoded_runs_len(vector.deleted_runs.len()));
        out.put_u8(DeletionVectorWireForm::PositionalRoaring.tag());
        put_encoded_runs(&mut out, &vector.deleted_runs);
        (DeletionVectorWireForm::PositionalRoaring, out.into_bytes())
    }
}

pub fn decode_deletion_vector(bytes: &[u8]) -> Result<DeletionVector, FormatError> {
    let mut reader = Reader::new(bytes);
    let tag = reader.u8("deletion vector wire form")?;
    let mut vector = DeletionVector::new();
    if tag == DeletionVectorWireForm::OrdinalArray.tag() {
        let count = reader.u32("deletion vector ordinal count")? as usize;
        let mut previous: Option<u32> = None;
        for _ in 0..count {
            let ordinal = reader.u32("deletion vector ordinal")?;
            if previous.is_some_and(|previous| ordinal <= previous) {
                return Err(FormatError::Structural {
                    rule: "deletion vector ordinals must be strictly ascending",
                });
            }
            previous = Some(ordinal);
            vector.mark_deleted(u64::from(ordinal));
        }
        return Ok(vector);
    }
    if tag == DeletionVectorWireForm::PositionalRoaring.tag() {
        let bitmap = RoaringRangeBitmap::decode(reader.take(reader.remaining(), "deletion vector bitmap")?)?;
        if bitmap.count() > MAX_DELETION_VECTOR_DECODED_ROWS {
            return Err(FormatError::Structural {
                rule: "deletion vector decodes past the row-count bound",
            });
        }
        for range in bitmap.ranges() {
            vector.mark_range(*range);
        }
        return Ok(vector);
    }
    Err(FormatError::Structural {
        rule: "unknown deletion vector wire form",
    })
}

#[cfg(test)]
#[path = "test/deletes.rs"]
mod tests;
