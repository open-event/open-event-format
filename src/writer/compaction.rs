use super::audit::refuse_audit_rewrite;
use super::build::BuiltOe;
use crate::error::FormatError;
use crate::events::TimestampValue;
use crate::layout::footer::StripeEntry;
use crate::layout::graph::{ChainCoverInput, DepthRange, EdgeKind, GraphEdge, GraphInput, RowEdges};
use crate::layout::reader::OeFile;
use hashbrown::{HashMap, HashSet};

const STRIPE_CHECKSUM_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedColumnEntry {
    pub column_name: String,
    pub settle_horizon: TimestampValue,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DerivedColumnsSidecar {
    pub columns: Vec<DerivedColumnEntry>,
    pub row_count: u64,
}

pub struct FoldedCompaction {
    pub embedded_columns: Vec<DerivedColumnEntry>,
    pub unsettled_sidecar: Option<DerivedColumnsSidecar>,
}

pub fn fold_settled_derived_columns(sidecar: &DerivedColumnsSidecar, as_of: TimestampValue) -> FoldedCompaction {
    let (embedded_columns, unsettled): (Vec<_>, Vec<_>) = sidecar
        .columns
        .iter()
        .cloned()
        .partition(|column| column.settle_horizon <= as_of);
    let unsettled_sidecar = (!unsettled.is_empty()).then_some(DerivedColumnsSidecar {
        columns: unsettled,
        row_count: sidecar.row_count,
    });
    FoldedCompaction {
        embedded_columns,
        unsettled_sidecar,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StripeDisposition {
    Rebuild { stripe_id: u32 },
    Reuse { byte_len: u64, source_offset: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripeChange {
    pub changed: bool,
    pub stripe: StripeEntry,
    pub uncertain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewritePlan {
    pub dispositions: Vec<StripeDisposition>,
}

impl RewritePlan {
    pub fn reuses_every_stripe(&self) -> bool {
        self.dispositions
            .iter()
            .all(|disposition| matches!(disposition, StripeDisposition::Reuse { .. }))
    }

    pub fn reused_bytes(&self) -> u64 {
        self.dispositions
            .iter()
            .map(|disposition| match disposition {
                StripeDisposition::Reuse { byte_len, .. } => *byte_len,
                StripeDisposition::Rebuild { .. } => 0,
            })
            .sum()
    }
}

pub fn plan_stripe_reuse(changes: &[StripeChange], _header_identical_len: u64) -> RewritePlan {
    let dispositions: Vec<StripeDisposition> = changes
        .iter()
        .map(|change| disposition(&change.stripe, !(change.changed || change.uncertain)))
        .collect();

    RewritePlan { dispositions }
}

fn disposition(stripe: &StripeEntry, reusable: bool) -> StripeDisposition {
    if reusable {
        StripeDisposition::Reuse {
            byte_len: stripe.byte_len,
            source_offset: stripe.file_offset,
        }
    } else {
        StripeDisposition::Rebuild {
            stripe_id: stripe.stripe_id,
        }
    }
}

fn stripe_bytes<'a>(built: &'a BuiltOe, stripe: &StripeEntry) -> Option<&'a [u8]> {
    let start = usize::try_from(stripe.file_offset).ok()?;
    let end = start.checked_add(usize::try_from(stripe.byte_len).ok()?)?;
    built.bytes.get(start..end)
}

pub fn plan_rewrite(source: &BuiltOe, replacement: &BuiltOe) -> Result<RewritePlan, FormatError> {
    refuse_audit_rewrite(source.header.profile, replacement.header.profile)?;
    let mut source_by_checksum: HashMap<[u8; STRIPE_CHECKSUM_BYTES], &StripeEntry> = HashMap::default();
    for (stripe, checksum) in source.footer.stripes.iter().zip(&source.footer.stripe_checksums) {
        source_by_checksum.entry(*checksum).or_insert(stripe);
    }

    let dispositions: Vec<StripeDisposition> = replacement
        .footer
        .stripes
        .iter()
        .zip(&replacement.footer.stripe_checksums)
        .map(|(replacement_stripe, checksum)| {
            let reused = source_by_checksum
                .get(checksum)
                .filter(|source_stripe| source_stripe.byte_len == replacement_stripe.byte_len)
                .filter(|source_stripe| {
                    match (
                        stripe_bytes(source, source_stripe),
                        stripe_bytes(replacement, replacement_stripe),
                    ) {
                        (Some(from), Some(to)) => from == to,
                        _ => false,
                    }
                });
            match reused {
                Some(source_stripe) => disposition(source_stripe, true),
                None => disposition(replacement_stripe, false),
            }
        })
        .collect();

    Ok(RewritePlan { dispositions })
}

pub fn rebuild_graph_input(sources: &[OeFile], later: &[OeFile]) -> Result<GraphInput, FormatError> {
    let refuse = |rule: &'static str| FormatError::Structural { rule };
    let first = sources
        .first()
        .and_then(|file| file.footer().graph.as_ref())
        .ok_or(refuse("a graph rewrite needs a source graph file"))?;
    let mut graph = GraphInput {
        chain_cover: first.chain_cover.as_ref().map(|_| ChainCoverInput::default()),
        depth: first.depth,
        first_sn: first.first_sn,
        subject: first.subject.clone(),
        ..GraphInput::default()
    };
    let mut inbound = Vec::new();
    let mut end_sn = first.first_sn;
    for file in sources {
        let footer = file.footer();
        let directory = footer
            .graph
            .as_ref()
            .ok_or(refuse("every graph rewrite source carries a graph"))?;
        if directory.subject != graph.subject || directory.first_sn != end_sn {
            return Err(refuse(
                "graph rewrite sources are one subject's files, each starting right after the one before",
            ));
        }
        if directory.chain_cover.is_some() != graph.chain_cover.is_some() {
            return Err(refuse("graph rewrite sources all carry a chain cover or none does"));
        }
        graph.depth = graph.depth.zip(directory.depth).map(|(a, b)| DepthRange {
            max: a.max.max(b.max),
            min: a.min.min(b.min),
        });
        if let (Some(cover), Some(summary)) = (&mut graph.chain_cover, &directory.chain_cover) {
            cover.links.extend_from_slice(&summary.links);
        }
        graph.unresolved.extend_from_slice(&directory.unresolved);
        for entry in &footer.state_roots {
            graph
                .state_roots
                .insert(directory.first_sn + entry.row_ordinal, entry.root);
        }
        end_sn = end_sn
            .checked_add(footer.exact_counts.row_count)
            .ok_or(refuse("graph sequence numbers overflow"))?;
        for granule in &footer.granules {
            let edges = file.granule_edges(granule.granule_id)?;
            let first_sn = directory.first_sn + granule.first_row_ordinal;
            for sn in first_sn..first_sn + u64::from(granule.row_count) {
                graph.rows.push(RowEdges {
                    auth: edges.parents(sn, EdgeKind::Auth).to_vec(),
                    prev: edges.parents(sn, EdgeKind::Prev).to_vec(),
                });
                if let Some(cover) = &mut graph.chain_cover {
                    cover.labels.push(
                        edges
                            .label(sn)
                            .ok_or(refuse("a chain-covered graph file labels every event"))?,
                    );
                }
                for kind in [EdgeKind::Auth, EdgeKind::Prev] {
                    for child in edges.children_iter(sn, kind) {
                        if child >= end_sn {
                            inbound.push(GraphEdge {
                                child,
                                kind,
                                parent: sn,
                            });
                        }
                    }
                }
            }
        }
    }
    inbound.retain(|edge| edge.child >= end_sn);
    for file in later {
        let footer = file.footer();
        let directory = footer
            .graph
            .as_ref()
            .ok_or(refuse("every later graph file carries a graph"))?;
        if directory.subject != graph.subject || directory.first_sn < end_sn {
            return Err(refuse(
                "later graph files belong to the same subject and start after the rewritten file",
            ));
        }
        for granule in &footer.granules {
            let edges = file.granule_edges(granule.granule_id)?;
            let first_sn = directory.first_sn + granule.first_row_ordinal;
            for child in first_sn..first_sn + u64::from(granule.row_count) {
                for kind in [EdgeKind::Auth, EdgeKind::Prev] {
                    for &parent in edges.parents(child, kind) {
                        if (graph.first_sn..end_sn).contains(&parent) {
                            inbound.push(GraphEdge { child, kind, parent });
                        }
                    }
                }
            }
        }
    }
    let mut seen = HashSet::with_capacity(inbound.len());
    inbound.retain(|edge| seen.insert(*edge));
    graph.inbound = inbound;
    Ok(graph)
}

#[cfg(test)]
#[path = "test/compaction.rs"]
mod tests;
