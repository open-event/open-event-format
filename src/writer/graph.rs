use crate::error::StorageError;
use crate::events::{RelationshipRef, TargetIdSpace};
use crate::layout::graph::{EdgeKind, GraphInput, RowEdges, UnresolvedEdge};
use crate::writer::build::BuildRow;

pub trait EventSequenceNumbers {
    fn sequence_numbers(&self, subject: &[u8], targets: &[&RelationshipRef]) -> Result<Vec<Option<u64>>, StorageError>;
}

pub fn resolve_graph(
    rows: &[BuildRow],
    subject: Vec<u8>,
    first_sn: u64,
    numbers: &dyn EventSequenceNumbers,
) -> Result<GraphInput, StorageError> {
    let lookups: Vec<&RelationshipRef> = rows
        .iter()
        .flat_map(declared)
        .map(|(_, reference)| reference)
        .filter(|reference| reference.space != TargetIdSpace::SubjectSequence)
        .collect();
    let answers = numbers.sequence_numbers(&subject, &lookups)?;
    if answers.len() != lookups.len() {
        return Err(StorageError::Io {
            op: "sequence number lookup",
            detail: format!("asked for {} targets, got {} answers", lookups.len(), answers.len()),
        });
    }
    let mut answers = answers.into_iter();
    let mut graph = GraphInput {
        first_sn,
        subject,
        ..GraphInput::default()
    };
    for (child, row) in (first_sn..).zip(rows) {
        let mut edges = RowEdges::default();
        for (kind, reference) in declared(row) {
            let sn = match reference.space {
                TargetIdSpace::SubjectSequence => <[u8; 8]>::try_from(reference.target_ref.as_slice())
                    .ok()
                    .map(u64::from_be_bytes),
                _ => answers.next().flatten(),
            };
            match (sn, kind) {
                (Some(sn), EdgeKind::Auth) => edges.auth.push(sn),
                (Some(sn), EdgeKind::Prev) => edges.prev.push(sn),
                (None, _) => graph.unresolved.push(UnresolvedEdge {
                    child,
                    parent: reference.clone(),
                }),
            }
        }
        graph.rows.push(edges);
    }
    Ok(graph)
}

fn declared(row: &BuildRow) -> impl Iterator<Item = (EdgeKind, &RelationshipRef)> + '_ {
    row.relationships
        .iter()
        .flat_map(|relationships| relationships.refs())
        .filter_map(|reference| Some((EdgeKind::of(reference.kind)?, reference)))
}

#[cfg(test)]
#[path = "test/graph.rs"]
mod tests;
