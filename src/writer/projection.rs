use crate::error::FormatError;
use crate::invariants::EncodeExecutor;
use crate::layout::footer::{SortDirection, SortednessProof};
use crate::writer::build::{BuildInput, BuildRow, BuiltOe, OeBuildConfig, build_oe_file_in_order};
use rayon::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowOrder {
    Entity,
    Sequence,
}

pub(crate) type RowKey = (u64, u64, u64, u64);

impl RowOrder {
    pub(crate) fn key(self, row: &BuildRow) -> RowKey {
        match self {
            RowOrder::Entity => (
                row.envelope.entity_id_hash_low,
                row.envelope.entity_id_hash_high,
                row.epoch,
                row.sequence,
            ),
            RowOrder::Sequence => (row.epoch, row.sequence, 0, 0),
        }
    }

    pub(crate) fn rule(self) -> &'static str {
        match self {
            RowOrder::Entity => "rows must be strictly (entity_id_hash, epoch, sequence) ordered",
            RowOrder::Sequence => "rows must be strictly (epoch, sequence) ordered",
        }
    }

    pub(crate) fn sortedness_proof(self) -> SortednessProof {
        let column_names: &[&str] = match self {
            RowOrder::Entity => &["entity_id_hash_low", "entity_id_hash_high", "epoch", "sequence"],
            RowOrder::Sequence => &["epoch", "sequence"],
        };
        SortednessProof {
            column_names: column_names.iter().map(|name| (*name).to_owned()).collect(),
            direction: SortDirection::Ascending,
        }
    }
}

pub fn build_entity_projection<R: BuildInput>(
    rows: Vec<R>,
    config: &OeBuildConfig,
    encode: &dyn EncodeExecutor,
) -> Result<BuiltOe, FormatError> {
    let mut rows = R::into_build_rows(rows);
    let mut points: Vec<(u64, u64)> = rows.iter().map(|row| (row.epoch, row.sequence)).collect();
    points.par_sort_unstable();
    let total = points.len();
    points.dedup();
    if points.len() != total {
        return Err(FormatError::Structural {
            rule: "an entity projection holds each (epoch, sequence) point once",
        });
    }
    rows.par_sort_unstable_by_key(|row| RowOrder::Entity.key(row));
    build_oe_file_in_order(rows, config, encode, RowOrder::Entity)
}

#[cfg(test)]
#[path = "test/projection.rs"]
mod tests;
