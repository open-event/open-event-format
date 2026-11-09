use super::constant::{DEFAULT_GRAPH_SEAL_AGE_NANOS, DEFAULT_GRAPH_SEAL_EVENTS};
use crate::clock::Clock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphSealPolicy {
    pub max_age_nanos: i64,
    pub max_events: u64,
}

impl Default for GraphSealPolicy {
    fn default() -> Self {
        Self {
            max_age_nanos: DEFAULT_GRAPH_SEAL_AGE_NANOS,
            max_events: DEFAULT_GRAPH_SEAL_EVENTS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenGraphFile {
    pub event_count: u64,
    pub first_event_at_nanos: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphSealTrigger {
    Age,
    EventCount,
}

pub fn should_seal_graph(
    open: &OpenGraphFile,
    policy: &GraphSealPolicy,
    clock: &dyn Clock,
) -> Option<GraphSealTrigger> {
    if open.event_count == 0 {
        return None;
    }
    if open.event_count >= policy.max_events {
        return Some(GraphSealTrigger::EventCount);
    }
    if clock.now_nanos().saturating_sub(open.first_event_at_nanos) >= policy.max_age_nanos {
        return Some(GraphSealTrigger::Age);
    }
    None
}

#[cfg(test)]
#[path = "test/graph_seal.rs"]
mod tests;
