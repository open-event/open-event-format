use super::event::{EnvelopeColumns, OverlayRows, overlay_column, read_entity_hashes, read_u64s};
use super::footer::SortDirection;
use super::reader::{OeFile, PayloadRead};
use crate::artifacts::overlay::{LiveOverlaySegment, LiveOverlayStore};
use crate::columns::column_ids;
use crate::deletes::{CorrectionMetadata, CorrectionType, DeletionVector};
use crate::error::{EntityScanError, FormatError};
use crate::events::variant::VariantValue;
use crate::events::{EventEnvelope, EventId, SequencePoint, TenantId};
use crate::field_deletion_store::FieldRedactions;
use arrow_array::UInt64Array;
use hashbrown::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

pub trait DeletesAndCorrections {
    fn deletion_vector(
        &self,
        tenant_id: TenantId,
        file_id: u128,
    ) -> Result<Option<Arc<DeletionVector>>, EntityScanError>;

    fn field_redactions(
        &self,
        _tenant_id: TenantId,
        _file_id: u128,
    ) -> Result<Option<Arc<FieldRedactions>>, EntityScanError> {
        Ok(None)
    }

    fn is_correction(&self, tenant_id: TenantId, point: SequencePoint) -> Result<bool, EntityScanError>;

    fn latest_correction(
        &self,
        tenant_id: TenantId,
        event_id: EventId,
    ) -> Result<Option<CorrectionMetadata>, EntityScanError>;
}

#[derive(Debug, Clone)]
pub struct EntityScan {
    pub direction: SortDirection,
    pub entity_id_hash_high: u64,
    pub entity_id_hash_low: u64,
    pub limit: Option<usize>,
    pub sequence_range: RangeInclusive<SequencePoint>,
    pub tenant_id: TenantId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntityEvent {
    pub envelope: EventEnvelope,
    pub payload: PayloadRead,
    pub point: SequencePoint,
}

#[derive(Debug, Clone, Copy)]
enum RowSource {
    File {
        file: usize,
        granule_id: u32,
        row_in_granule: usize,
        row_ordinal: u64,
    },
    Overlay {
        row: usize,
        segment: usize,
    },
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    point: SequencePoint,
    rank: (u8, u64),
    source: RowSource,
}

impl EntityScan {
    pub fn run(
        &self,
        files: &[&OeFile],
        overlay: &LiveOverlayStore,
        deletes: &dyn DeletesAndCorrections,
    ) -> Result<Vec<EntityEvent>, EntityScanError> {
        let files: Vec<&OeFile> = files
            .iter()
            .copied()
            .filter(|file| file.header().tenant_id == self.tenant_id)
            .collect();
        let segments: Vec<&LiveOverlaySegment> = overlay
            .segments()
            .filter(|segment| segment.meta.tenant_id == self.tenant_id)
            .collect();
        let mut candidates = Vec::new();
        for (index, file) in files.iter().enumerate() {
            self.file_candidates(index, file, &mut candidates)?;
        }
        for (index, segment) in segments.iter().enumerate() {
            self.overlay_candidates(index, segment, &mut candidates)?;
        }
        candidates.sort_by(|a, b| a.point.cmp(&b.point).then(b.rank.cmp(&a.rank)));
        candidates.dedup_by(|later, earlier| later.point == earlier.point);
        if self.direction == SortDirection::Descending {
            candidates.reverse();
        }

        let deletion_vectors = files
            .iter()
            .map(|file| deletes.deletion_vector(self.tenant_id, file.header().file_id))
            .collect::<Result<Vec<_>, _>>()?;
        let redactions = files
            .iter()
            .map(|file| deletes.field_redactions(self.tenant_id, file.header().file_id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut reader = EventReader {
            envelopes: HashMap::new(),
            files: &files,
            overlay_rows: HashMap::new(),
            redactions: &redactions,
            segments: &segments,
        };
        let mut events = Vec::new();
        for candidate in candidates {
            if self.limit.is_some_and(|limit| events.len() >= limit) {
                break;
            }
            if deletes.is_correction(self.tenant_id, candidate.point)? {
                continue;
            }
            let event = reader.read(candidate.source)?;
            if let Some((envelope, payload)) =
                self.latest_version(candidate.source, event, deletes, &deletion_vectors, &mut reader)?
            {
                events.push(EntityEvent {
                    envelope,
                    payload,
                    point: candidate.point,
                });
            }
        }
        Ok(events)
    }

    fn latest_version(
        &self,
        source: RowSource,
        event: (EventEnvelope, PayloadRead),
        deletes: &dyn DeletesAndCorrections,
        deletion_vectors: &[Option<Arc<DeletionVector>>],
        reader: &mut EventReader<'_>,
    ) -> Result<Option<(EventEnvelope, PayloadRead)>, EntityScanError> {
        let (files, segments) = (reader.files, reader.segments);
        let (mut envelope, mut base) = event;
        let mut amendments = Vec::new();
        let mut current = envelope.event_id;
        let mut on_chain = vec![current];
        let mut served_rows = vec![source];
        while let Some(correction) = deletes.latest_correction(self.tenant_id, current)? {
            let at = SequencePoint {
                epoch: correction.correction_epoch,
                sequence: correction.correction_sequence,
            };
            if correction.correction_type == CorrectionType::Retraction {
                return Ok(None);
            }
            let row = locate(at, files, segments)?;
            let (correcting, changes) = reader.read(row)?;
            current = correcting.event_id;
            if on_chain.contains(&current) {
                return Err(EntityScanError::CorrectionCycle {
                    epoch: at.epoch,
                    sequence: at.sequence,
                });
            }
            on_chain.push(current);
            if correction.correction_type == CorrectionType::Amendment {
                amendments.push(changes);
            } else if let Some(last) = amendments.last_mut() {
                *last = changes;
                served_rows.pop();
            } else {
                envelope = correcting;
                base = changes;
                served_rows.clear();
            }
            served_rows.push(row);
        }
        if served_rows.iter().any(|row| is_deleted(deletion_vectors, *row)) {
            return Ok(None);
        }
        Ok(Some((envelope, amendments.into_iter().fold(base, amend))))
    }

    fn file_candidates(&self, index: usize, file: &OeFile, out: &mut Vec<Candidate>) -> Result<(), FormatError> {
        let from = *self.sequence_range.start();
        let to = *self.sequence_range.end();
        let generation = file.header().generation_id;
        for granule in file.entity_granules(self.entity_id_hash_low, self.entity_id_hash_high, from, to) {
            let rows = granule.row_count as usize;
            let epochs = read_u64s(file, column_ids::EPOCH, granule.granule_id, rows)?;
            let sequences = read_u64s(file, column_ids::SEQUENCE, granule.granule_id, rows)?;
            let (lows, highs) = read_entity_hashes(file, granule.granule_id, rows)?;
            let keys = epochs.iter().zip(&sequences).zip(lows.iter().zip(&highs));
            for (row_in_granule, ((&epoch, &sequence), (&low, &high))) in keys.enumerate() {
                let point = SequencePoint { epoch, sequence };
                if low == self.entity_id_hash_low
                    && high == self.entity_id_hash_high
                    && self.sequence_range.contains(&point)
                {
                    out.push(Candidate {
                        point,
                        rank: (1, generation),
                        source: RowSource::File {
                            file: index,
                            granule_id: granule.granule_id,
                            row_in_granule,
                            row_ordinal: granule.first_row_ordinal + row_in_granule as u64,
                        },
                    });
                }
            }
        }
        Ok(())
    }

    fn overlay_candidates(
        &self,
        index: usize,
        segment: &LiveOverlaySegment,
        out: &mut Vec<Candidate>,
    ) -> Result<(), FormatError> {
        let meta = &segment.meta;
        let first = SequencePoint {
            epoch: meta.epoch,
            sequence: meta.first_sequence,
        };
        let last = SequencePoint {
            epoch: meta.epoch,
            sequence: meta.last_sequence,
        };
        if last < *self.sequence_range.start() || *self.sequence_range.end() < first {
            return Ok(());
        }
        let epochs = overlay_column::<UInt64Array>(segment, "epoch")?.values();
        let sequences = overlay_column::<UInt64Array>(segment, "sequence")?.values();
        let lows = overlay_column::<UInt64Array>(segment, "entity_id_hash_low")?.values();
        let highs = overlay_column::<UInt64Array>(segment, "entity_id_hash_high")?.values();
        let keys = epochs.iter().zip(sequences.iter()).zip(lows.iter().zip(highs.iter()));
        for (row, ((&epoch, &sequence), (&low, &high))) in keys.enumerate() {
            let point = SequencePoint { epoch, sequence };
            if low == self.entity_id_hash_low
                && high == self.entity_id_hash_high
                && self.sequence_range.contains(&point)
            {
                out.push(Candidate {
                    point,
                    rank: (0, 0),
                    source: RowSource::Overlay { row, segment: index },
                });
            }
        }
        Ok(())
    }
}

struct EventReader<'a> {
    envelopes: HashMap<(usize, u32), EnvelopeColumns>,
    files: &'a [&'a OeFile],
    overlay_rows: HashMap<usize, OverlayRows<'a>>,
    redactions: &'a [Option<Arc<FieldRedactions>>],
    segments: &'a [&'a LiveOverlaySegment],
}

impl<'a> EventReader<'a> {
    fn read(&mut self, source: RowSource) -> Result<(EventEnvelope, PayloadRead), FormatError> {
        match source {
            RowSource::File {
                file: index,
                granule_id,
                row_in_granule,
                row_ordinal,
            } => {
                let file = self
                    .files
                    .get(index)
                    .copied()
                    .ok_or(FormatError::RefOutOfRange { what: "scanned file" })?;
                if !self.envelopes.contains_key(&(index, granule_id)) {
                    self.envelopes
                        .insert((index, granule_id), EnvelopeColumns::read(file, granule_id)?);
                }
                let envelope = self
                    .envelopes
                    .get(&(index, granule_id))
                    .ok_or(FormatError::RefOutOfRange { what: "row in granule" })?
                    .envelope(file, row_in_granule)
                    .map_err(|_| FormatError::RefOutOfRange { what: "row in granule" })?;
                let payload = file.payload(row_ordinal)?;
                let payload = match self.redactions.get(index).and_then(Option::as_deref) {
                    Some(redactions) => redactions.redact_payload(row_ordinal, payload),
                    None => payload,
                };
                Ok((envelope, payload))
            }
            RowSource::Overlay { row, segment } => {
                let rows = match self.overlay_rows.entry(segment) {
                    hashbrown::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        let resolved = self.segments.get(segment).copied().ok_or(FormatError::RefOutOfRange {
                            what: "overlay segment",
                        })?;
                        entry.insert(OverlayRows::new(resolved)?)
                    }
                };
                rows.event(row)
            }
        }
    }
}

fn locate(
    point: SequencePoint,
    files: &[&OeFile],
    segments: &[&LiveOverlaySegment],
) -> Result<RowSource, EntityScanError> {
    let mut newest: Option<(u64, RowSource)> = None;
    for (index, file) in files.iter().enumerate() {
        let generation = file.header().generation_id;
        for granule in file.granules_for_sequence(point.epoch, point.sequence, point.sequence) {
            let rows = granule.row_count as usize;
            let epochs = read_u64s(file, column_ids::EPOCH, granule.granule_id, rows)?;
            let sequences = read_u64s(file, column_ids::SEQUENCE, granule.granule_id, rows)?;
            let found = epochs
                .iter()
                .zip(&sequences)
                .position(|(&epoch, &sequence)| epoch == point.epoch && sequence == point.sequence);
            if let Some(row_in_granule) = found
                && newest.is_none_or(|(newest, _)| generation > newest)
            {
                newest = Some((
                    generation,
                    RowSource::File {
                        file: index,
                        granule_id: granule.granule_id,
                        row_in_granule,
                        row_ordinal: granule.first_row_ordinal + row_in_granule as u64,
                    },
                ));
            }
        }
    }
    if let Some((_, source)) = newest {
        return Ok(source);
    }
    segments
        .iter()
        .enumerate()
        .find(|(_, segment)| {
            segment.meta.epoch == point.epoch
                && segment.meta.first_sequence <= point.sequence
                && point.sequence <= segment.meta.last_sequence
        })
        .map(|(index, segment)| RowSource::Overlay {
            row: (point.sequence - segment.meta.first_sequence) as usize,
            segment: index,
        })
        .ok_or(EntityScanError::CorrectionNotFound {
            epoch: point.epoch,
            sequence: point.sequence,
        })
}

fn is_deleted(deletion_vectors: &[Option<Arc<DeletionVector>>], source: RowSource) -> bool {
    match source {
        RowSource::File { file, row_ordinal, .. } => deletion_vectors
            .get(file)
            .and_then(Option::as_ref)
            .is_some_and(|vector| vector.is_deleted(row_ordinal)),
        RowSource::Overlay { .. } => false,
    }
}

fn amend(original: PayloadRead, amendment: PayloadRead) -> PayloadRead {
    match (original, amendment) {
        (PayloadRead::Value(VariantValue::Object(mut fields)), PayloadRead::Value(VariantValue::Object(changed))) => {
            fields.extend(changed);
            PayloadRead::Value(VariantValue::Object(fields))
        }
        (original, PayloadRead::None) => original,
        (_, amendment) => amendment,
    }
}

#[cfg(test)]
#[path = "test/entity_scan.rs"]
mod tests;
