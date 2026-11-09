use crate::deletes::DeletionVector;
use crate::error::FormatError;
use crate::events::{EventRelationships, RelationshipRef, TargetIdSpace};
use crate::layout::graph::{ChainCoverInput, ChainLabel, DepthRange, EdgeKind, GraphEdge, GraphInput, RowEdges};
use crate::layout::reader::OeFile;
use crate::writer::build::BuildRow;
use std::collections::{BTreeMap, HashMap};

pub struct RenumberSource {
    pub deleted: Option<DeletionVector>,
    pub file: OeFile,
    pub rows: Vec<BuildRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RenumberedGraphFile {
    pub graph: GraphInput,
    pub rows: Vec<BuildRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphRenumbering {
    pub files: Vec<RenumberedGraphFile>,
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
