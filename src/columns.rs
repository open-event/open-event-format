use super::error::FormatError;
use super::events::variant::VariantValue;
use super::layout::footer::ColumnKind;
use hashbrown::{HashMap, HashSet};
use std::cmp::Ordering;

pub mod column_ids {
    pub const EPOCH: u32 = 0;
    pub const SEQUENCE: u32 = 1;
    pub const STREAM_ID: u32 = 2;
    pub const STREAM_SEQUENCE: u32 = 3;
    pub const OCCURRED_AT: u32 = 4;
    pub const INGESTED_AT: u32 = 5;
    pub const SOURCE_ID: u32 = 6;
    pub const EVENT_TYPE_ID: u32 = 7;
    pub const ENTITY_TYPE_ID: u32 = 8;
    pub const ENTITY_ID_HASH_LOW: u32 = 9;
    pub const ENTITY_ID_HASH_HIGH: u32 = 10;
    pub const PAYLOAD_REF: u32 = 11;
    pub const FLAGS: u32 = 12;
    pub const SCHEMA_VERSION: u32 = 13;
    pub const EVENT_ID: u32 = 14;
    pub const ENTITY_ID: u32 = 15;
    pub const ACTOR_ID: u32 = 16;
    pub const ACCOUNT_ID: u32 = 17;
    pub const ACTOR_ID_HASH_LOW: u32 = 18;
    pub const ACCOUNT_ID_HASH_LOW: u32 = 19;
    pub const TRACE_ID_HASH_LOW: u32 = 20;
    pub const DEDUPE_HASH_LOW: u32 = 21;
    pub const DEDUPE_HASH_HIGH: u32 = 22;
    pub const PAYLOAD_FLAGS: u32 = 23;

    pub const PROMOTED_BASE: u32 = 1000;
    pub const SHREDDED_BASE: u32 = 2000;
    pub const FREETEXT_BASE: u32 = 3000;
    pub const CONTEXT_BASE: u32 = 4000;
    pub const EMBEDDING_BASE: u32 = 5000;
    pub const PROVENANCE_BASE: u32 = 6000;

    pub const AUTHOR_PUBKEY: u32 = PROVENANCE_BASE;
    pub const SIGNATURE: u32 = PROVENANCE_BASE + 1;
    pub const SIGNATURE_SCHEME: u32 = PROVENANCE_BASE + 2;
    pub const PROTOCOL_EVENT_ID: u32 = PROVENANCE_BASE + 3;
    pub const PROTOCOL_KIND: u32 = PROVENANCE_BASE + 4;
    pub const CLAIMED_AT: u32 = PROVENANCE_BASE + 5;

    pub const RELATIONSHIP_BASE: u32 = 7000;

    pub const PARENT_REF: u32 = RELATIONSHIP_BASE;
    pub const ROOT_REF: u32 = RELATIONSHIP_BASE + 1;
    pub const LINKED_REFS: u32 = RELATIONSHIP_BASE + 2;
    pub const RELATED_REFS: u32 = RELATIONSHIP_BASE + 3;
    pub const PREV_REFS: u32 = RELATIONSHIP_BASE + 4;
    pub const AUTH_REFS: u32 = RELATIONSHIP_BASE + 5;

    pub const SOURCE_FORM_BASE: u32 = 8000;

    pub const RAW_PAYLOAD: u32 = SOURCE_FORM_BASE;
    pub const EXTERNAL_ID: u32 = SOURCE_FORM_BASE + 1;
    pub const SOURCE_SCHEMA: u32 = SOURCE_FORM_BASE + 2;
    pub const SOURCE_DELIVERY: u32 = SOURCE_FORM_BASE + 3;
    pub const CONNECTOR_DELIVERY_HASH: u32 = SOURCE_FORM_BASE + 4;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub column_id: u32,
    pub internal_only: bool,
    pub kind: ColumnKind,
    pub name: &'static str,
    pub nullable: bool,
}

pub const REQUIRED_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        column_id: column_ids::EPOCH,
        name: "epoch",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::SEQUENCE,
        name: "sequence",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::STREAM_ID,
        name: "stream_id",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::STREAM_SEQUENCE,
        name: "stream_sequence",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::OCCURRED_AT,
        name: "occurred_at",
        kind: ColumnKind::I64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::INGESTED_AT,
        name: "ingested_at",
        kind: ColumnKind::I64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::SOURCE_ID,
        name: "source_id",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::EVENT_TYPE_ID,
        name: "event_type_id",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::ENTITY_TYPE_ID,
        name: "entity_type_id",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::ENTITY_ID_HASH_LOW,
        name: "entity_id_hash_low",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::ENTITY_ID_HASH_HIGH,
        name: "entity_id_hash_high",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::PAYLOAD_REF,
        name: "payload_ref",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::FLAGS,
        name: "flags",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::SCHEMA_VERSION,
        name: "schema_version",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::EVENT_ID,
        name: "event_id",
        kind: ColumnKind::U128,
        nullable: false,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::ENTITY_ID,
        name: "entity_id",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::ACTOR_ID,
        name: "actor_id",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::ACCOUNT_ID,
        name: "account_id",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::ACTOR_ID_HASH_LOW,
        name: "actor_id_hash_low",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::ACCOUNT_ID_HASH_LOW,
        name: "account_id_hash_low",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::TRACE_ID_HASH_LOW,
        name: "trace_id_hash_low",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::DEDUPE_HASH_LOW,
        name: "dedupe_hash_low",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::DEDUPE_HASH_HIGH,
        name: "dedupe_hash_high",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::PAYLOAD_FLAGS,
        name: "payload_flags",
        kind: ColumnKind::U64,
        nullable: false,
        internal_only: true,
    },
];

pub const PROVENANCE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        column_id: column_ids::AUTHOR_PUBKEY,
        name: "author_pubkey",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::SIGNATURE,
        name: "signature",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::SIGNATURE_SCHEME,
        name: "signature_scheme",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::PROTOCOL_EVENT_ID,
        name: "protocol_event_id",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::PROTOCOL_KIND,
        name: "protocol_kind",
        kind: ColumnKind::I64,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::CLAIMED_AT,
        name: "claimed_at",
        kind: ColumnKind::I64,
        nullable: true,
        internal_only: false,
    },
];

pub const RELATIONSHIP_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        column_id: column_ids::PARENT_REF,
        name: "parent_ref",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::ROOT_REF,
        name: "root_ref",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::LINKED_REFS,
        name: "linked_refs",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::RELATED_REFS,
        name: "related_refs",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::PREV_REFS,
        name: "prev_refs",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::AUTH_REFS,
        name: "auth_refs",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
];

pub const SOURCE_FORM_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        column_id: column_ids::RAW_PAYLOAD,
        name: "raw_payload",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::EXTERNAL_ID,
        name: "external_id",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: false,
    },
    ColumnSpec {
        column_id: column_ids::SOURCE_SCHEMA,
        name: "source_schema",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::SOURCE_DELIVERY,
        name: "source_delivery",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
    ColumnSpec {
        column_id: column_ids::CONNECTOR_DELIVERY_HASH,
        name: "connector_delivery_hash",
        kind: ColumnKind::String,
        nullable: true,
        internal_only: true,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotedColumn {
    pub kind: ColumnKind,
    pub name: String,
    pub path: String,
    pub since_schema_version: u32,
    pub substring_searchable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PromotionPlan {
    pub columns: Vec<PromotedColumn>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FreetextDeclaration {
    pub fields: Vec<String>,
}

pub const SHRED_MIN_PRESENCE_PERCENT: u32 = 50;
pub const SHRED_MIN_KIND_CONSENSUS_PERCENT: u32 = 95;

pub const SPARSE_SHRED_MIN_PRESENCE_PERCENT: u32 = 5;

pub const SHARED_DICTIONARY_MAX_VALUES: usize = 1024;

pub const UNION_SHARED_DICTIONARY_COLUMNS: [u32; 1] = [column_ids::ENTITY_ID];

pub const UNION_SHARED_DICTIONARY_MAX_VALUES: usize = 1 << 20;

pub const SPARSE_KEYS_PER_FILE_MAX: usize = 16;

const PERCENT_SCALE: u64 = 100;

#[derive(Debug, Default, Clone)]
struct PathObservation {
    by_kind: [u64; 6],
    present: u64,
}

impl PathObservation {
    fn record(&mut self, kind: ColumnKind) {
        if let Some(slot) = self.by_kind.get_mut(kind as usize) {
            *slot += 1;
        }
        self.present += 1;
    }
}

#[derive(Debug, Default)]
pub struct PathStatistics {
    paths: HashMap<String, PathObservation>,
    dense_fields: Vec<PathObservation>,
    sparse_fields: Option<HashMap<u32, PathObservation>>,
    rows: u64,
}

pub(crate) const DENSE_FIELD_STATISTICS_LIMIT: usize = 4_096;

fn scalar_kind(value: &VariantValue) -> Option<ColumnKind> {
    match value {
        VariantValue::Int(_) => Some(ColumnKind::I64),
        VariantValue::Double(_) => Some(ColumnKind::F64),
        VariantValue::Decimal { .. } => Some(ColumnKind::Decimal),
        VariantValue::String(_) => Some(ColumnKind::String),
        _ => None,
    }
}

impl PathStatistics {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn for_field_ids(field_count: usize) -> Self {
        if field_count <= DENSE_FIELD_STATISTICS_LIMIT {
            Self {
                dense_fields: vec![PathObservation::default(); field_count],
                ..Self::default()
            }
        } else {
            Self {
                sparse_fields: Some(HashMap::new()),
                ..Self::default()
            }
        }
    }

    pub fn observe(&mut self, payload: Option<&VariantValue>) {
        self.rows += 1;
        let Some(VariantValue::Object(fields)) = payload else {
            return;
        };
        for (path, value) in fields {
            let Some(kind) = scalar_kind(value) else {
                continue;
            };
            if let Some(observation) = self.paths.get_mut(path.as_str()) {
                observation.record(kind);
            } else {
                self.paths.entry(path.clone()).or_default().record(kind);
            }
        }
    }

    pub(crate) fn observe_field_ids(&mut self, field_ids: &[u32], values: &[VariantValue]) {
        self.rows += 1;
        for (&field_id, value) in field_ids.iter().zip(values) {
            let Some(kind) = scalar_kind(value) else {
                continue;
            };
            if let Some(observation) = self.dense_fields.get_mut(field_id as usize) {
                observation.record(kind);
            } else if let Some(fields) = self.sparse_fields.as_mut() {
                fields.entry(field_id).or_default().record(kind);
            }
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.rows += other.rows;
        for (path, observation) in other.paths {
            let slot = self.paths.entry(path).or_default();
            slot.present += observation.present;
            for (into, from) in slot.by_kind.iter_mut().zip(observation.by_kind) {
                *into += from;
            }
        }
        debug_assert_eq!(self.dense_fields.len(), other.dense_fields.len());
        for (slot, observation) in self.dense_fields.iter_mut().zip(other.dense_fields) {
            slot.present += observation.present;
            for (into, from) in slot.by_kind.iter_mut().zip(observation.by_kind) {
                *into += from;
            }
        }
        if let (Some(into), Some(from)) = (self.sparse_fields.as_mut(), other.sparse_fields) {
            for (field_id, observation) in from {
                let slot = into.entry(field_id).or_default();
                slot.present += observation.present;
                for (into, from) in slot.by_kind.iter_mut().zip(observation.by_kind) {
                    *into += from;
                }
            }
        }
    }

    pub(crate) fn shred_candidates_by_id(
        &self,
        field_names: &[String],
        excluded: &HashSet<u32>,
    ) -> Vec<(String, ColumnKind)> {
        if self.rows == 0 {
            return Vec::new();
        }
        let mut candidates = Vec::new();
        for (field_id, observation) in self.field_observations() {
            if excluded.contains(&field_id)
                || observation.present * PERCENT_SCALE < self.rows * u64::from(SHRED_MIN_PRESENCE_PERCENT)
            {
                continue;
            }
            if let (Some(path), Some(kind)) = (field_names.get(field_id as usize), consensus_kind(observation)) {
                candidates.push((path.clone(), kind));
            }
        }
        candidates.sort_by(|(a, _), (b, _)| a.cmp(b));
        candidates
    }

    pub(crate) fn sparse_candidates_by_id(
        &self,
        field_names: &[String],
        excluded: &HashSet<u32>,
    ) -> Vec<(String, ColumnKind)> {
        if self.rows == 0 {
            return Vec::new();
        }
        select_sparse(self.field_observations().filter_map(|(field_id, observation)| {
            let path = field_names.get(field_id as usize)?;
            if excluded.contains(&field_id) || path.len() > u16::MAX as usize {
                return None;
            }
            let present = observation.present * PERCENT_SCALE;
            if present < self.rows * u64::from(SPARSE_SHRED_MIN_PRESENCE_PERCENT)
                || present >= self.rows * u64::from(SHRED_MIN_PRESENCE_PERCENT)
            {
                return None;
            }
            consensus_kind(observation).map(|kind| (observation.present, path.as_str(), kind))
        }))
    }

    fn field_observations(&self) -> Box<dyn Iterator<Item = (u32, &PathObservation)> + '_> {
        if let Some(fields) = &self.sparse_fields {
            Box::new(fields.iter().map(|(&field_id, observation)| (field_id, observation)))
        } else {
            Box::new(
                self.dense_fields
                    .iter()
                    .enumerate()
                    .map(|(field_id, observation)| (field_id as u32, observation)),
            )
        }
    }

    pub fn shred_candidates(&self, excluded: &[&str]) -> Vec<(String, ColumnKind)> {
        if self.rows == 0 {
            return Vec::new();
        }
        let mut candidates = Vec::new();
        for (path, observation) in &self.paths {
            if excluded.contains(&path.as_str()) {
                continue;
            }
            if observation.present * PERCENT_SCALE < self.rows * u64::from(SHRED_MIN_PRESENCE_PERCENT) {
                continue;
            }
            if let Some(kind) = consensus_kind(observation) {
                candidates.push((path.clone(), kind));
            }
        }
        candidates.sort_by(|(a, _), (b, _)| a.cmp(b));
        candidates
    }

    pub fn sparse_candidates(&self, excluded: &[&str]) -> Vec<(String, ColumnKind)> {
        if self.rows == 0 {
            return Vec::new();
        }
        select_sparse(self.paths.iter().filter_map(|(path, observation)| {
            if excluded.contains(&path.as_str()) || path.len() > u16::MAX as usize {
                return None;
            }
            let present = observation.present * PERCENT_SCALE;
            if present < self.rows * u64::from(SPARSE_SHRED_MIN_PRESENCE_PERCENT)
                || present >= self.rows * u64::from(SHRED_MIN_PRESENCE_PERCENT)
            {
                return None;
            }
            consensus_kind(observation).map(|kind| (observation.present, path.as_str(), kind))
        }))
    }
}

fn select_sparse<'a>(candidates: impl Iterator<Item = (u64, &'a str, ColumnKind)>) -> Vec<(String, ColumnKind)> {
    let mut top: Vec<(u64, &str, ColumnKind)> = Vec::new();
    for candidate in candidates {
        let sorts_first = |kept: &(u64, &str, ColumnKind)| sparse_order(kept, &candidate) != Ordering::Greater;
        if top.len() >= SPARSE_KEYS_PER_FILE_MAX {
            match top.last() {
                Some(worst) if !sorts_first(worst) => {
                    top.pop();
                }
                _ => continue,
            }
        }
        let position = top.partition_point(sorts_first);
        if top.capacity() == 0 {
            top.reserve_exact(SPARSE_KEYS_PER_FILE_MAX);
        }
        top.insert(position, candidate);
    }
    top.into_iter().map(|(_, path, kind)| (path.to_owned(), kind)).collect()
}

fn sparse_order(a: &(u64, &str, ColumnKind), b: &(u64, &str, ColumnKind)) -> Ordering {
    b.0.cmp(&a.0).then(a.1.cmp(b.1))
}

fn consensus_kind(observation: &PathObservation) -> Option<ColumnKind> {
    let (kind_index, kind_count) = observation
        .by_kind
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| **count)
        .map(|(index, count)| (index, *count))?;
    if kind_count * PERCENT_SCALE < observation.present * u64::from(SHRED_MIN_KIND_CONSENSUS_PERCENT) {
        return None;
    }
    ColumnKind::from_u8(kind_index as u8).ok()
}

pub fn promoted_present(column: &PromotedColumn, row_schema_version: u32) -> bool {
    row_schema_version >= column.since_schema_version
}

pub fn validate_promotion_plan(plan: &PromotionPlan) -> Result<(), FormatError> {
    for column in &plan.columns {
        if column.kind == ColumnKind::F64 && (column.name.contains("amount") || column.name.contains("money")) {
            return Err(FormatError::Structural {
                rule: "money promotes to fixed-scale decimal, never float",
            });
        }
        if column.substring_searchable && column.kind != ColumnKind::String {
            return Err(FormatError::Structural {
                rule: "only a string promoted column may be declared substring-searchable",
            });
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "test/columns.rs"]
mod tests;
