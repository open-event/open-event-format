use super::graph::{ChainLabel, ChainLink, EdgeKind};
use crate::artifacts::state_chunk::{StateChange, StateKey, StateRoot};
use crate::error::{FormatError, StateChunkError};
use hashbrown::HashSet;

pub trait EventGraph {
    fn parents_of(&self, subject: &[u8], sns: &[u64], kind: EdgeKind) -> Result<Vec<Vec<u64>>, FormatError>;

    fn children_of(&self, subject: &[u8], sns: &[u64], kind: EdgeKind) -> Result<Vec<Vec<u64>>, FormatError>;

    fn parents(&self, subject: &[u8], sn: u64, kind: EdgeKind) -> Result<Vec<u64>, FormatError> {
        Ok(self.parents_of(subject, &[sn], kind)?.pop().unwrap_or_default())
    }

    fn children(&self, subject: &[u8], sn: u64, kind: EdgeKind) -> Result<Vec<u64>, FormatError> {
        Ok(self.children_of(subject, &[sn], kind)?.pop().unwrap_or_default())
    }

    fn walk<'a>(&'a self, subject: &'a [u8], start: u64, kind: EdgeKind, limit: usize) -> GraphWalk<'a>
    where
        Self: Sized,
    {
        GraphWalk {
            frontier: vec![start],
            graph: self,
            kind,
            remaining: limit,
            subject,
            visited: HashSet::from_iter([start]),
        }
    }
}

pub struct GraphWalk<'a> {
    frontier: Vec<u64>,
    graph: &'a dyn EventGraph,
    kind: EdgeKind,
    remaining: usize,
    subject: &'a [u8],
    visited: HashSet<u64>,
}

impl Iterator for GraphWalk<'_> {
    type Item = Result<Vec<u64>, FormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 || self.frontier.is_empty() {
            return None;
        }
        let parents = match self.graph.parents_of(self.subject, &self.frontier, self.kind) {
            Ok(parents) => parents,
            Err(error) => {
                self.frontier.clear();
                return Some(Err(error));
            }
        };
        let mut level: Vec<u64> = parents
            .into_iter()
            .flatten()
            .filter(|sn| self.visited.insert(*sn))
            .collect();
        level.sort_unstable();
        level.truncate(self.remaining);
        self.remaining -= level.len();
        self.frontier.clone_from(&level);
        (!level.is_empty()).then_some(Ok(level))
    }
}

pub trait EventIds {
    fn resolve_ids(&self, subject: &[u8], ids: &[&[u8]]) -> Result<Vec<Option<u64>>, FormatError>;
}

pub trait AuthChains {
    fn chain_labels(&self, subject: &[u8], sns: &[u64]) -> Result<Vec<Option<ChainLabel>>, FormatError>;

    fn chain_links(&self, subject: &[u8], chains: &[u32]) -> Result<Vec<ChainLink>, FormatError>;
}

pub trait SubjectState {
    fn state_root(&self, subject: &[u8], sn: u64) -> Result<Option<StateRoot>, FormatError>;

    fn state_get(&self, root: &StateRoot, key: StateKey) -> Result<Option<u64>, StateChunkError>;

    fn state_diff(&self, before: &StateRoot, after: &StateRoot) -> Result<Vec<StateChange>, StateChunkError>;
}

#[cfg(test)]
#[path = "test/api.rs"]
mod tests;
