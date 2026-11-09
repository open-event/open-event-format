use crate::columns::{PROVENANCE_COLUMNS, RELATIONSHIP_COLUMNS, REQUIRED_COLUMNS, SOURCE_FORM_COLUMNS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    A,
    B,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnFamily {
    ChatInvestigationContextColumns,
    ClassificationLabels,
    ClusterColumns,
    DriverAndCauseColumns,
    EmbeddingColumnsInternal,
    EventEnvelope,
    FreetextColumns,
    PreparedViewLineageColumns,
    RelationshipReferences,
    RevenueAnomalyColumns,
    RevenueMetricColumns,
    SafeRetryAndIngestMode,
    SignedEventProvenance,
    SourceTypeEntity,
}

impl ColumnFamily {
    pub const ALL: [ColumnFamily; 14] = [
        ColumnFamily::EventEnvelope,
        ColumnFamily::SourceTypeEntity,
        ColumnFamily::SafeRetryAndIngestMode,
        ColumnFamily::ClassificationLabels,
        ColumnFamily::FreetextColumns,
        ColumnFamily::ClusterColumns,
        ColumnFamily::EmbeddingColumnsInternal,
        ColumnFamily::RevenueAnomalyColumns,
        ColumnFamily::DriverAndCauseColumns,
        ColumnFamily::RevenueMetricColumns,
        ColumnFamily::PreparedViewLineageColumns,
        ColumnFamily::ChatInvestigationContextColumns,
        ColumnFamily::SignedEventProvenance,
        ColumnFamily::RelationshipReferences,
    ];

    pub fn tier(self) -> Tier {
        match self {
            ColumnFamily::ClusterColumns
            | ColumnFamily::RevenueAnomalyColumns
            | ColumnFamily::DriverAndCauseColumns => Tier::B,
            _ => Tier::A,
        }
    }

    pub fn lives_in_base_file(self) -> bool {
        matches!(self.tier(), Tier::A)
    }

    pub fn internal_only(self) -> bool {
        matches!(
            self,
            ColumnFamily::EmbeddingColumnsInternal
                | ColumnFamily::SafeRetryAndIngestMode
                | ColumnFamily::ClassificationLabels
                | ColumnFamily::RelationshipReferences
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    Internal,
    Public,
}

pub fn column_allowed(column: &str, caller: Caller) -> bool {
    match caller {
        Caller::Internal => true,
        Caller::Public => REQUIRED_COLUMNS
            .iter()
            .chain(PROVENANCE_COLUMNS)
            .chain(RELATIONSHIP_COLUMNS)
            .chain(SOURCE_FORM_COLUMNS)
            .any(|spec| spec.name == column && !spec.internal_only),
    }
}

pub fn authorize_columns<'c>(requested: &[&'c str], caller: Caller) -> (Vec<&'c str>, Vec<&'c str>) {
    let mut allowed = Vec::with_capacity(requested.len());
    let mut dropped = Vec::new();
    for column in requested {
        if column_allowed(column, caller) {
            allowed.push(*column);
        } else {
            dropped.push(*column);
        }
    }
    (allowed, dropped)
}

#[cfg(test)]
#[path = "test/families.rs"]
mod tests;
