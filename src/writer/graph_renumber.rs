//! Renumbers a run of one subject's graph files when rows leave them, so a compaction can physically drop the rows a
//! deletion vector removes from a graph file.
//!
//! A graph file addresses its events by subject-wide sequence number (SN), with no gaps: row `i` is event
//! `first_sn + i`. Dropping a row therefore shifts every later event of the subject down, and every edge that names a
//! shifted event must follow it. [`renumber_graph`] takes every graph file of the subject from the first one that
//! changes to the subject's last `Active` file, grouped into the files the rewrite will produce, and returns each
//! output's rows and graph with every SN renumbered through one old-to-new map.
//!
//! # Edges to a deleted event
//!
//! A graph's edges are reachability: a `prev` edge says the child came after the parent in the subject's history, an
//! `auth` edge that the parent authorises the child, and both are walked transitively (the auth chain cover is a
//! reachability index over `auth` edges). Dropping an edge to a deleted event would cut every ancestor behind it off
//! from its descendants. So each edge of kind `k` that names a deleted event is *re-parented* onto the deleted event's
//! own parents of kind `k`, resolved the same way when they are deleted too: the child ends up pointing at the nearest
//! surviving ancestors along `k` edges, in the order the deleted event declared them, without repeats. A deleted event
//! with no parents of that kind simply leaves the child without that edge. Reachability among the surviving events is
//! exactly what it was. The same rewrite applies to each row's declared `prev` and `auth` relationships in the
//! `subject_sequence` space, so the rows keep agreeing with the graph; references in other spaces are left as they
//! were declared.
//!
//! A deleted event's own unresolved parents, state root and chain-cover label leave with it. Chain-cover links and
//! depth ranges carry over from the sources unchanged: links name chain positions rather than SNs, and a carried depth
//! range still bounds the surviving events (it may be wider than they need, never narrower).

use crate::deletes::DeletionVector;
use crate::error::FormatError;
use crate::events::{EventRelationships, RelationshipRef, TargetIdSpace};
use crate::layout::graph::{ChainCoverInput, ChainLabel, DepthRange, EdgeKind, GraphEdge, GraphInput, RowEdges};
use crate::layout::reader::OeFile;
use crate::writer::build::BuildRow;
use std::collections::{BTreeMap, HashMap};

/// One graph file entering a renumbering: the file, its rows in row order (with any field redactions already applied),
/// and the deletion vector whose rows the rewrite drops.
pub struct RenumberSource {
    pub deleted: Option<DeletionVector>,
    pub file: OeFile,
    pub rows: Vec<BuildRow>,
}

/// One file a renumbering produces: the rows to build it from and its graph, with every SN renumbered.
#[derive(Debug, Clone, PartialEq)]
pub struct RenumberedGraphFile {
    pub graph: GraphInput,
    pub rows: Vec<BuildRow>,
}

/// What [`renumber_graph`] produced.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphRenumbering {
    /// One per group, in order.
    pub files: Vec<RenumberedGraphFile>,
    /// Every SN of the sources, old to new: `None` for an event whose row was deleted.
    pub sn_map: BTreeMap<u64, Option<u64>>,
}

struct OldEvent {
    deleted: bool,
    edges: RowEdges,
    group: usize,
    label: Option<ChainLabel>,
    root: Option<crate::artifacts::state_chunk::StateRoot>,
    row: BuildRow,
    unresolved: Vec<RelationshipRef>,
}

/// Renumbers `groups`, each a run of one subject's graph files that becomes one output file, the groups together the
/// subject's files from the first one that changes to its last. The sources must follow on from one another across
/// all groups, from one subject; the first source's first SN is kept, and every surviving event after it is numbered
/// densely in order. Edges into events before the first source are left as they are; edges to deleted events are
/// re-parented as the [module docs](self) describe. Each output's inbound edges are rebuilt from the children in the
/// later outputs.
///
/// Refuses no sources, a source without a graph, sources of different subjects, sources that leave a gap or overlap,
/// a source whose row count is not its file's, a group that mixes files with and without a chain cover, and a group
/// whose every event is deleted (it would leave no file to cover its range).
pub fn renumber_graph(groups: Vec<Vec<RenumberSource>>) -> Result<GraphRenumbering, FormatError> {
    let refuse = |rule: &'static str| FormatError::Structural { rule };
    let first = groups
        .iter()
        .flatten()
        .next()
        .and_then(|source| source.file.footer().graph.as_ref())
        .ok_or(refuse("a graph renumbering needs a source graph file"))?;
    let start = first.first_sn;
    let subject = first.subject.clone();

    struct GroupMeta {
        chain_cover: Option<Vec<crate::layout::graph::ChainLink>>,
        depth: Option<DepthRange>,
    }
    let mut metas = Vec::with_capacity(groups.len());
    let mut events: Vec<OldEvent> = Vec::new();
    let mut end_sn = start;
    for (group_index, group) in groups.into_iter().enumerate() {
        let mut meta: Option<GroupMeta> = None;
        for source in group {
            let footer = source.file.footer();
            let directory = footer
                .graph
                .as_ref()
                .ok_or(refuse("every graph renumbering source carries a graph"))?;
            if directory.subject != subject || directory.first_sn != end_sn {
                return Err(refuse(
                    "graph renumbering sources are one subject's files, each starting right after the one before",
                ));
            }
            if source.rows.len() as u64 != footer.exact_counts.row_count {
                return Err(refuse("a graph renumbering source supplies one row per stored row"));
            }
            let links = directory.chain_cover.as_ref().map(|cover| cover.links.clone());
            match &mut meta {
                None => {
                    meta = Some(GroupMeta {
                        chain_cover: links,
                        depth: directory.depth,
                    })
                }
                Some(meta) => {
                    if meta.chain_cover.is_some() != links.is_some() {
                        return Err(refuse("graph rewrite sources all carry a chain cover or none does"));
                    }
                    if let (Some(all), Some(links)) = (&mut meta.chain_cover, links) {
                        all.extend(links);
                    }
                    meta.depth = meta.depth.zip(directory.depth).map(|(a, b)| DepthRange {
                        max: a.max.max(b.max),
                        min: a.min.min(b.min),
                    });
                }
            }
            let file_first = directory.first_sn;
            let mut unresolved: HashMap<u64, Vec<RelationshipRef>> = HashMap::new();
            for edge in &directory.unresolved {
                unresolved.entry(edge.child).or_default().push(edge.parent.clone());
            }
            let roots: HashMap<u64, _> = footer
                .state_roots
                .iter()
                .map(|entry| (file_first + entry.row_ordinal, entry.root))
                .collect();
            let base = events.len();
            for (ordinal, row) in source.rows.into_iter().enumerate() {
                let sn = file_first + ordinal as u64;
                events.push(OldEvent {
                    deleted: source
                        .deleted
                        .as_ref()
                        .is_some_and(|vector| vector.is_deleted(ordinal as u64)),
                    edges: RowEdges::default(),
                    group: group_index,
                    label: None,
                    root: roots.get(&sn).copied(),
                    row,
                    unresolved: unresolved.remove(&sn).unwrap_or_default(),
                });
            }
            for granule in &footer.granules {
                let edges = source.file.granule_edges(granule.granule_id)?;
                let granule_first = file_first + granule.first_row_ordinal;
                for sn in granule_first..granule_first + u64::from(granule.row_count) {
                    let event = events
                        .get_mut(base + (sn - file_first) as usize)
                        .ok_or(refuse("a graph granule lies inside its file"))?;
                    event.edges = RowEdges {
                        auth: edges.parents(sn, EdgeKind::Auth).to_vec(),
                        prev: edges.parents(sn, EdgeKind::Prev).to_vec(),
                    };
                    if directory.chain_cover.is_some() {
                        event.label = Some(
                            edges
                                .label(sn)
                                .ok_or(refuse("a chain-covered graph file labels every event"))?,
                        );
                    }
                }
            }
            end_sn = end_sn
                .checked_add(footer.exact_counts.row_count)
                .ok_or(refuse("graph sequence numbers overflow"))?;
        }
        metas.push(meta.ok_or(refuse("every group of a graph renumbering holds a file"))?);
    }

    // The old-to-new map: survivors numbered densely from `start`.
    let mut sn_map = BTreeMap::new();
    let mut next = start;
    for (old, event) in (start..).zip(&events) {
        if event.deleted {
            sn_map.insert(old, None);
        } else {
            sn_map.insert(old, Some(next));
            next += 1;
        }
    }
    // For each deleted event and kind, the surviving ancestors an edge to it is re-parented onto. Parents are always
    // earlier, so walking in SN order fills each entry before any later event asks for it.
    let mut reparent: HashMap<(u64, EdgeKind), Vec<u64>> = HashMap::new();
    let resolve = |parents: &[u64], kind: EdgeKind, reparent: &HashMap<(u64, EdgeKind), Vec<u64>>| {
        let mut out: Vec<u64> = Vec::with_capacity(parents.len());
        for &parent in parents {
            let resolved: Vec<u64> = if parent < start {
                vec![parent]
            } else {
                match sn_map.get(&parent) {
                    Some(Some(new)) => vec![*new],
                    Some(None) => reparent.get(&(parent, kind)).cloned().unwrap_or_default(),
                    None => return Err(refuse("every parent is earlier than its event")),
                }
            };
            for sn in resolved {
                if !out.contains(&sn) {
                    out.push(sn);
                }
            }
        }
        Ok(out)
    };

    let mut files: Vec<RenumberedGraphFile> = metas
        .iter()
        .map(|meta| RenumberedGraphFile {
            graph: GraphInput {
                chain_cover: meta.chain_cover.as_ref().map(|links| ChainCoverInput {
                    labels: Vec::new(),
                    links: links.clone(),
                }),
                depth: meta.depth,
                first_sn: u64::MAX,
                subject: subject.clone(),
                ..GraphInput::default()
            },
            rows: Vec::new(),
        })
        .collect();
    let mut group_of_new: Vec<usize> = Vec::new();
    let mut new_edges: Vec<(u64, RowEdges)> = Vec::new();
    for (old, mut event) in (start..).zip(events) {
        let auth = resolve(&event.edges.auth, EdgeKind::Auth, &reparent)?;
        let prev = resolve(&event.edges.prev, EdgeKind::Prev, &reparent)?;
        if event.deleted {
            reparent.insert((old, EdgeKind::Auth), auth);
            reparent.insert((old, EdgeKind::Prev), prev);
            continue;
        }
        let new = sn_map
            .get(&old)
            .copied()
            .flatten()
            .ok_or(refuse("a survivor has a new SN"))?;
        if let Some(relationships) = &event.row.relationships {
            let mut refs: Vec<RelationshipRef> = Vec::with_capacity(relationships.refs().len());
            for reference in relationships.refs() {
                let target = (reference.space == TargetIdSpace::SubjectSequence)
                    .then(|| <[u8; 8]>::try_from(reference.target_ref.as_slice()).ok())
                    .flatten()
                    .map(u64::from_be_bytes);
                let replaced = match (target, EdgeKind::of(reference.kind)) {
                    (Some(target), Some(kind)) if target >= start => resolve(&[target], kind, &reparent)?
                        .into_iter()
                        .map(|sn| RelationshipRef::to_subject_sequence(reference.kind, sn))
                        .collect(),
                    _ => vec![reference.clone()],
                };
                for reference in replaced {
                    if !refs.contains(&reference) {
                        refs.push(reference);
                    }
                }
            }
            event.row.relationships = if refs.is_empty() {
                None
            } else {
                Some(EventRelationships::new(refs).map_err(|_| refuse("renumbered relationships stay valid"))?)
            };
        }
        let file = files
            .get_mut(event.group)
            .ok_or(refuse("every event belongs to a group"))?;
        if file.graph.first_sn == u64::MAX {
            file.graph.first_sn = new;
        }
        if let (Some(cover), Some(label)) = (&mut file.graph.chain_cover, event.label) {
            cover.labels.push(label);
        }
        if let Some(root) = event.root {
            file.graph.state_roots.insert(new, root);
        }
        file.graph.unresolved.extend(
            event
                .unresolved
                .into_iter()
                .map(|parent| crate::layout::graph::UnresolvedEdge { child: new, parent }),
        );
        let edges = RowEdges { auth, prev };
        file.graph.rows.push(edges.clone());
        file.rows.push(event.row);
        group_of_new.push(event.group);
        new_edges.push((new, edges));
    }
    if files.iter().any(|file| file.rows.is_empty()) {
        return Err(refuse(
            "a graph rewrite leaves every output file at least one surviving event",
        ));
    }
    // Inbound edges: every edge from a child in a later output to a parent in an earlier one.
    for (index, (child, edges)) in new_edges.iter().enumerate() {
        let Some(&child_group) = group_of_new.get(index) else {
            continue;
        };
        for (kind, parents) in [(EdgeKind::Auth, &edges.auth), (EdgeKind::Prev, &edges.prev)] {
            for &parent in parents {
                let Some(parent_index) = parent.checked_sub(start) else {
                    continue;
                };
                let Some(&parent_group) = group_of_new.get(parent_index as usize) else {
                    continue;
                };
                if parent_group != child_group
                    && let Some(file) = files.get_mut(parent_group)
                {
                    file.graph.inbound.push(GraphEdge {
                        child: *child,
                        kind,
                        parent,
                    });
                }
            }
        }
    }
    Ok(GraphRenumbering { files, sn_map })
}

#[cfg(test)]
#[path = "test/graph_renumber.rs"]
mod tests;
