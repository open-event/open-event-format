crate::typed_id::define_typed_id!(TenantId, TenantIdTag, "tenant");

crate::typed_id::define_typed_id!(EventId, EventIdTag, "event");

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TimestampValue {
    physical_nanos: i64,
}

impl TimestampValue {
    pub const fn from_physical_nanos(physical_nanos: i64) -> Self {
        Self { physical_nanos }
    }

    pub const fn physical_nanos(self) -> i64 {
        self.physical_nanos
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DurationValue {
    physical_nanos: i64,
}

impl DurationValue {
    pub const fn from_physical_nanos(physical_nanos: i64) -> Self {
        Self { physical_nanos }
    }

    pub const fn physical_nanos(self) -> i64 {
        self.physical_nanos
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SequencePoint {
    pub epoch: u64,
    pub sequence: u64,
}

impl Ord for SequencePoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.epoch, self.sequence).cmp(&(other.epoch, other.sequence))
    }
}

impl PartialOrd for SequencePoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SequenceRange {
    pub epoch: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
}

impl Ord for SequenceRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.epoch, self.first_sequence, self.last_sequence).cmp(&(
            other.epoch,
            other.first_sequence,
            other.last_sequence,
        ))
    }
}

impl PartialOrd for SequenceRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl SequenceRange {
    pub fn count(&self) -> u64 {
        self.last_sequence.saturating_sub(self.first_sequence).saturating_add(
            if self.last_sequence >= self.first_sequence {
                1
            } else {
                0
            },
        )
    }

    pub fn overlaps(&self, other: &SequenceRange) -> bool {
        self.epoch == other.epoch
            && self.first_sequence <= other.last_sequence
            && other.first_sequence <= self.last_sequence
    }

    pub fn contains(&self, other: &SequenceRange) -> bool {
        self.epoch == other.epoch
            && self.first_sequence <= other.first_sequence
            && other.last_sequence <= self.last_sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EventFlags(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEnvelope {
    pub account_id: Option<String>,
    pub account_id_hash_low: u64,
    pub actor_id: Option<String>,
    pub actor_id_hash_low: u64,
    pub dedupe_hash_high: u64,
    pub dedupe_hash_low: u64,
    pub entity_id: Option<String>,
    pub entity_id_hash_high: u64,
    pub entity_id_hash_low: u64,
    pub entity_type: String,
    pub event_id: EventId,
    pub event_type: String,
    pub flags: EventFlags,
    pub ingested_at: TimestampValue,
    pub occurred_at: TimestampValue,
    pub schema_version: u32,
    pub source: String,
    pub stream_id: StreamId,
    pub stream_sequence: u64,
    pub tenant_id: TenantId,
    pub trace_id_hash_low: u64,
}

pub fn derived_entity_id_hash(entity_id: Option<&str>) -> (u64, u64) {
    match entity_id {
        Some(id) => (crate::indexes::stable_hash(id.as_bytes()), 0),
        None => (0, 0),
    }
}

impl EventEnvelope {
    pub fn entity_hash_is_derived(&self) -> bool {
        derived_entity_id_hash(self.entity_id.as_deref()) == (self.entity_id_hash_low, self.entity_id_hash_high)
    }
}

#[cfg(test)]
#[path = "test/envelope.rs"]
mod tests;
