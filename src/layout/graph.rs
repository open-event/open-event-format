use super::constant::{CHAIN_RANGE_BYTES, GRAPH_EDGE_KINDS, MAX_GRAPH_SUBJECT_BYTES, UNRESOLVED_ENTRY_MIN_BYTES};
use super::footer::{GranuleEntry, StateRootEntry};
use super::reader::OeFile;
use crate::artifacts::state_chunk::StateRoot;
use crate::encoding::{ColumnData, PipelineId, decode_block, encode_block};
use crate::error::FormatError;
use crate::events::{RelationshipKind, RelationshipRef, TargetIdSpace};
use crate::file::bytes::{Reader, Writer};
use crate::indexes::probabilistic::BinaryFuseFilter;
use hashbrown::HashSet;
use std::collections::BTreeMap;
use std::ops::{Range, RangeInclusive};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeKind {
    Auth,
    Prev,
}

impl EdgeKind {
    fn slot(self) -> usize {
        match self {
            EdgeKind::Prev => 0,
            EdgeKind::Auth => 1,
        }
    }

    fn from_slot(slot: u8) -> Option<Self> {
        match slot {
            0 => Some(EdgeKind::Prev),
            1 => Some(EdgeKind::Auth),
            _ => None,
        }
    }

    pub fn of(kind: RelationshipKind) -> Option<Self> {
        match kind {
            RelationshipKind::Auth => Some(EdgeKind::Auth),
            RelationshipKind::Prev => Some(EdgeKind::Prev),
            _ => None,
        }
    }

    pub fn relationship(self) -> RelationshipKind {
        match self {
            EdgeKind::Auth => RelationshipKind::Auth,
            EdgeKind::Prev => RelationshipKind::Prev,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphInput {
    pub chain_cover: Option<ChainCoverInput>,
    pub depth: Option<DepthRange>,
    pub first_sn: u64,
    pub inbound: Vec<GraphEdge>,
    pub rows: Vec<RowEdges>,
    pub state_roots: BTreeMap<u64, StateRoot>,
    pub subject: Vec<u8>,
    pub unresolved: Vec<UnresolvedEdge>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowEdges {
    pub auth: Vec<u64>,
    pub prev: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GraphEdge {
    pub child: u64,
    pub kind: EdgeKind,
    pub parent: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnresolvedEdge {
    pub child: u64,
    pub parent: RelationshipRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChainLabel {
    pub chain_id: u32,
    pub seq: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChainLink {
    pub from_chain: u32,
    pub from_seq: u32,
    pub to_chain: u32,
    pub to_seq: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainCoverInput {
    pub labels: Vec<ChainLabel>,
    pub links: Vec<ChainLink>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChainRange {
    pub chain_id: u32,
    pub max_seq: u32,
    pub min_seq: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DepthRange {
    pub max: u64,
    pub min: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainCover {
    pub chains: Vec<ChainRange>,
    pub links: Vec<ChainLink>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIdIndex {
    pub filter: Vec<u8>,
    pub hashes: Vec<u64>,
    pub sns: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GranuleEdges {
    first_sn: u64,
    forward: Csr,
    inbound: Csr,
    labels: Vec<ChainLabel>,
    reverse: Csr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Csr {
    offsets: Vec<u64>,
    targets: Vec<u64>,
}

impl GranuleEdges {
    pub fn parents(&self, sn: u64, kind: EdgeKind) -> &[u64] {
        self.forward.edges(self.first_sn, sn, kind)
    }

    pub fn children(&self, sn: u64, kind: EdgeKind) -> Vec<u64> {
        self.children_iter(sn, kind).collect()
    }

    pub fn children_iter(&self, sn: u64, kind: EdgeKind) -> impl Iterator<Item = u64> + '_ {
        self.reverse
            .edges(self.first_sn, sn, kind)
            .iter()
            .chain(self.inbound.edges(self.first_sn, sn, kind))
            .copied()
    }

    pub fn label(&self, sn: u64) -> Option<ChainLabel> {
        let row = usize::try_from(sn.checked_sub(self.first_sn)?).ok()?;
        self.labels.get(row).copied()
    }

    pub(super) fn decoded_bytes(&self) -> u64 {
        [&self.forward, &self.inbound, &self.reverse]
            .iter()
            .map(|csr| ((csr.offsets.len() + csr.targets.len()) * size_of::<u64>()) as u64)
            .sum::<u64>()
            + (self.labels.len() * size_of::<ChainLabel>()) as u64
    }
}

impl Csr {
    fn edges(&self, first_sn: u64, sn: u64, kind: EdgeKind) -> &[u64] {
        let Some(row) = sn.checked_sub(first_sn).and_then(|row| usize::try_from(row).ok()) else {
            return &[];
        };
        let slot = row.saturating_mul(GRAPH_EDGE_KINDS).saturating_add(kind.slot());
        let (Some(&start), Some(&end)) = (self.offsets.get(slot), self.offsets.get(slot.saturating_add(1))) else {
            return &[];
        };
        self.targets.get(start as usize..end as usize).unwrap_or_default()
    }
}

pub fn encode_graph_blocks(
    graph: &GraphInput,
    row_count: u64,
    granules: &[GranuleEntry],
) -> Result<BTreeMap<u32, Vec<u8>>, FormatError> {
    let refuse = |rule: &'static str| FormatError::Structural { rule };
    if graph.subject.is_empty() || graph.subject.len() > MAX_GRAPH_SUBJECT_BYTES {
        return Err(refuse("a graph subject is 1 to 255 bytes"));
    }
    if graph.rows.len() as u64 != row_count {
        return Err(refuse("a graph carries one edge entry per row"));
    }
    let end_sn = graph
        .first_sn
        .checked_add(row_count)
        .ok_or(refuse("graph sequence numbers overflow"))?;
    let reverse_slot = |target: u64, kind: EdgeKind| {
        target
            .checked_sub(graph.first_sn)
            .filter(|&row| row < row_count)
            .map(|row| row as usize * GRAPH_EDGE_KINDS + kind.slot())
    };
    let mut reverse_counts = vec![0usize; graph.rows.len() * GRAPH_EDGE_KINDS];
    let mut seen = HashSet::new();
    for (sn, row) in (graph.first_sn..).zip(&graph.rows) {
        for (kind, targets) in [(EdgeKind::Prev, &row.prev), (EdgeKind::Auth, &row.auth)] {
            seen.clear();
            for &target in targets {
                if target >= sn {
                    return Err(refuse("a graph parent must be strictly earlier than its event"));
                }
                if !seen.insert(target) {
                    return Err(refuse("a graph event names the same parent twice"));
                }
                if let Some(count) = reverse_slot(target, kind).and_then(|slot| reverse_counts.get_mut(slot)) {
                    *count += 1;
                }
            }
        }
    }
    for edge in &graph.unresolved {
        if !(graph.first_sn..end_sn).contains(&edge.child) {
            return Err(refuse("an unresolved graph edge's child lives in the file"));
        }
        if EdgeKind::of(edge.parent.kind).is_none() || edge.parent.space == TargetIdSpace::SubjectSequence {
            return Err(refuse(
                "an unresolved graph edge names a prev or auth parent by protocol id",
            ));
        }
    }
    if graph.depth.is_some_and(|depth| depth.min > depth.max) {
        return Err(refuse("a graph depth range's minimum does not pass its maximum"));
    }
    if let Some(cover) = &graph.chain_cover {
        if cover.labels.len() != graph.rows.len() {
            return Err(refuse("a graph chain cover carries one label per row"));
        }
        let mut last_seq: BTreeMap<u32, u32> = BTreeMap::new();
        for label in &cover.labels {
            if last_seq
                .insert(label.chain_id, label.seq)
                .is_some_and(|last| last >= label.seq)
            {
                return Err(refuse("a chain's seq must strictly increase in row order"));
            }
        }
    }
    let mut inbound: Vec<[Vec<u64>; GRAPH_EDGE_KINDS]> = vec![Default::default(); graph.rows.len()];
    for edge in &graph.inbound {
        if edge.child < end_sn {
            return Err(refuse("an inbound graph edge's child lives past the file's last event"));
        }
        let children = edge
            .parent
            .checked_sub(graph.first_sn)
            .and_then(|row| inbound.get_mut(row as usize))
            .and_then(|slots| slots.get_mut(edge.kind.slot()))
            .ok_or(refuse("an inbound graph edge's parent lives in the file"))?;
        children.push(edge.child);
    }
    for children in inbound.iter_mut().flatten() {
        children.sort_unstable();
        if children.windows(2).any(|pair| pair.first() == pair.last()) {
            return Err(refuse("a graph inbound edge is repeated"));
        }
    }
    let mut reverse_bounds = Vec::with_capacity(reverse_counts.len() + 1);
    let mut total = 0;
    reverse_bounds.push(total);
    for &count in &reverse_counts {
        total += count;
        reverse_bounds.push(total);
    }
    let mut reverse_children = vec![0u64; total];
    let mut cursors = reverse_counts;
    for (cursor, &start) in cursors.iter_mut().zip(&reverse_bounds) {
        *cursor = start;
    }
    for (sn, row) in (graph.first_sn..).zip(&graph.rows) {
        for (kind, targets) in [(EdgeKind::Prev, &row.prev), (EdgeKind::Auth, &row.auth)] {
            for &target in targets {
                if let Some(cursor) = reverse_slot(target, kind).and_then(|slot| cursors.get_mut(slot))
                    && let Some(child) = reverse_children.get_mut(*cursor)
                {
                    *child = sn;
                    *cursor += 1;
                }
            }
        }
    }

    let mut blocks = BTreeMap::new();
    for granule in granules {
        let start = granule.first_row_ordinal as usize;
        let range = start..start.saturating_add(granule.row_count as usize);
        let rows = graph
            .rows
            .get(range.clone())
            .ok_or(refuse("granule rows beyond the graph"))?;
        let first_sn = graph.first_sn + granule.first_row_ordinal;
        let forward: Vec<[&[u64]; GRAPH_EDGE_KINDS]> = rows.iter().map(|row| [&row.prev[..], &row.auth[..]]).collect();
        let mut out = Writer::new();
        encode_csr(&mut out, first_sn, &forward)?;
        encode_csr(
            &mut out,
            first_sn,
            &flat_slot_lists(&reverse_bounds, &reverse_children, range.clone()),
        )?;
        encode_csr(
            &mut out,
            first_sn,
            &slot_lists(inbound.get(range.clone()).unwrap_or_default()),
        )?;
        if let Some(cover) = &graph.chain_cover {
            let labels = cover.labels.get(range).unwrap_or_default();
            put_block(
                &mut out,
                &ColumnData::U64(labels.iter().map(|label| u64::from(label.chain_id)).collect()),
            );
            put_block(
                &mut out,
                &ColumnData::U64(labels.iter().map(|label| u64::from(label.seq)).collect()),
            );
        }
        blocks.insert(granule.granule_id, out.into_bytes());
    }
    Ok(blocks)
}

pub fn state_root_entries(graph: &GraphInput, row_count: u64) -> Result<Vec<StateRootEntry>, FormatError> {
    graph
        .state_roots
        .iter()
        .map(|(sn, root)| {
            sn.checked_sub(graph.first_sn)
                .filter(|row| *row < row_count)
                .map(|row_ordinal| StateRootEntry {
                    root: *root,
                    row_ordinal,
                })
                .ok_or(FormatError::Structural {
                    rule: "a state root belongs to an event in the file",
                })
        })
        .collect()
}

fn slot_lists(lists: &[[Vec<u64>; GRAPH_EDGE_KINDS]]) -> Vec<[&[u64]; GRAPH_EDGE_KINDS]> {
    lists.iter().map(|[prev, auth]| [&prev[..], &auth[..]]).collect()
}

fn flat_slot_lists<'a>(
    bounds: &[usize],
    children: &'a [u64],
    rows: Range<usize>,
) -> Vec<[&'a [u64]; GRAPH_EDGE_KINDS]> {
    rows.map(|row| {
        std::array::from_fn(|kind| {
            let slot = row * GRAPH_EDGE_KINDS + kind;
            match (bounds.get(slot), bounds.get(slot + 1)) {
                (Some(&start), Some(&end)) => children.get(start..end).unwrap_or_default(),
                _ => &[],
            }
        })
    })
    .collect()
}

fn encode_csr(out: &mut Writer, first_sn: u64, rows: &[[&[u64]; GRAPH_EDGE_KINDS]]) -> Result<(), FormatError> {
    let mut offsets = Vec::with_capacity(rows.len() * GRAPH_EDGE_KINDS + 1);
    let mut deltas = Vec::new();
    offsets.push(0);
    for (source, lists) in (first_sn..).zip(rows) {
        for targets in lists {
            for &target in *targets {
                let delta =
                    i64::try_from(i128::from(source) - i128::from(target)).map_err(|_| FormatError::Structural {
                        rule: "a graph edge spans more sequence numbers than it can store",
                    })?;
                deltas.push(delta);
            }
            offsets.push(deltas.len() as u64);
        }
    }
    let edge_count = u32::try_from(deltas.len()).map_err(|_| FormatError::Structural {
        rule: "a granule holds more graph edges than it can store",
    })?;
    out.put_u32(edge_count);
    put_block(out, &ColumnData::U64(offsets));
    if edge_count > 0 {
        put_block(out, &ColumnData::I64(deltas));
    }
    Ok(())
}

fn put_block(out: &mut Writer, data: &ColumnData) {
    let encoded = encode_block(data, false);
    out.put_u32(encoded.pipeline.0);
    out.put_u32(encoded.bytes.len() as u32);
    out.put_slice(&encoded.bytes);
}

impl ChainCover {
    pub fn build(input: &ChainCoverInput) -> Self {
        let mut ranges: BTreeMap<u32, (u32, u32)> = BTreeMap::new();
        for label in &input.labels {
            let range = ranges.entry(label.chain_id).or_insert((label.seq, label.seq));
            range.0 = range.0.min(label.seq);
            range.1 = range.1.max(label.seq);
        }
        let mut links = input.links.clone();
        links.sort_unstable();
        links.dedup();
        Self {
            chains: ranges
                .into_iter()
                .map(|(chain_id, (min_seq, max_seq))| ChainRange {
                    chain_id,
                    max_seq,
                    min_seq,
                })
                .collect(),
            links,
        }
    }

    pub(super) fn encode(&self, out: &mut Writer) {
        out.put_u32(self.chains.len() as u32);
        for chain in &self.chains {
            out.put_u32(chain.chain_id);
            out.put_u32(chain.min_seq);
            out.put_u32(chain.max_seq);
        }
        out.put_u32(self.links.len() as u32);
        if self.links.is_empty() {
            return;
        }
        let mut previous = 0;
        let from_chain_deltas = self
            .links
            .iter()
            .map(|link| {
                u64::from(
                    link.from_chain
                        .wrapping_sub(std::mem::replace(&mut previous, link.from_chain)),
                )
            })
            .collect();
        put_block(out, &ColumnData::U64(from_chain_deltas));
        for field in [
            |link: &ChainLink| link.from_seq,
            |link: &ChainLink| link.to_chain,
            |link: &ChainLink| link.to_seq,
        ] {
            put_block(
                out,
                &ColumnData::U64(self.links.iter().map(|link| u64::from(field(link))).collect()),
            );
        }
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let malformed = FormatError::Structural {
            rule: "graph chain cover must hold sorted chains with ordered ranges and sorted, distinct links",
        };
        let mut reader = Reader::new(bytes);
        let chain_count = reader.u32("graph chain count")? as usize;
        let mut chains: Vec<ChainRange> = Vec::with_capacity(chain_count.min(reader.remaining() / CHAIN_RANGE_BYTES));
        for _ in 0..chain_count {
            let chain = ChainRange {
                chain_id: reader.u32("graph chain id")?,
                min_seq: reader.u32("graph chain min seq")?,
                max_seq: reader.u32("graph chain max seq")?,
            };
            if chain.min_seq > chain.max_seq || chains.last().is_some_and(|last| last.chain_id >= chain.chain_id) {
                return Err(malformed);
            }
            chains.push(chain);
        }
        let link_count = reader.u32("graph link count")? as usize;
        let mut links = Vec::new();
        if link_count > 0 {
            let mut column = || -> Result<Vec<u32>, FormatError> {
                let ColumnData::U64(values) = take_block(&mut reader)? else {
                    return Err(malformed.clone());
                };
                let values: Vec<u32> = values
                    .into_iter()
                    .map(u32::try_from)
                    .collect::<Result<_, _>>()
                    .map_err(|_| malformed.clone())?;
                if values.len() != link_count {
                    return Err(malformed.clone());
                }
                Ok(values)
            };
            let (deltas, from_seqs, to_chains, to_seqs) = (column()?, column()?, column()?, column()?);
            let mut from_chain = 0u32;
            for (((delta, from_seq), to_chain), to_seq) in deltas.into_iter().zip(from_seqs).zip(to_chains).zip(to_seqs)
            {
                from_chain = from_chain.checked_add(delta).ok_or(malformed.clone())?;
                let link = ChainLink {
                    from_chain,
                    from_seq,
                    to_chain,
                    to_seq,
                };
                if links.last().is_some_and(|last| *last >= link) {
                    return Err(malformed);
                }
                links.push(link);
            }
        }
        if reader.remaining() != 0 {
            return Err(FormatError::Structural {
                rule: "graph chain cover section has trailing bytes",
            });
        }
        Ok(Self { chains, links })
    }
}

pub fn event_id_hash(event_id: &[u8]) -> u64 {
    xxh3_64(event_id)
}

pub fn event_id_filter_may_hold(filter: &[u8], event_id: &[u8]) -> bool {
    BinaryFuseFilter::contains_encoded(filter, event_id_hash(event_id)).unwrap_or(true)
}

impl EventIdIndex {
    pub fn build<'a>(ids: impl IntoIterator<Item = (&'a [u8], u64)>) -> Result<Option<Self>, FormatError> {
        let mut pairs: Vec<(u64, u64)> = ids.into_iter().map(|(id, sn)| (event_id_hash(id), sn)).collect();
        if pairs.is_empty() {
            return Ok(None);
        }
        pairs.sort_unstable();
        let (hashes, sns): (Vec<u64>, Vec<u64>) = pairs.into_iter().unzip();
        let filter = BinaryFuseFilter::build(&hashes)?.encode();
        Ok(Some(Self { filter, hashes, sns }))
    }

    fn sn_of(
        &self,
        event_id: &[u8],
        mut stored_id: impl FnMut(u64) -> Result<Option<Vec<u8>>, FormatError>,
    ) -> Result<Option<u64>, FormatError> {
        for &sn in self.candidates(event_id) {
            if stored_id(sn)?.as_deref() == Some(event_id) {
                return Ok(Some(sn));
            }
        }
        Ok(None)
    }

    pub(super) fn candidates(&self, event_id: &[u8]) -> &[u64] {
        let hash = event_id_hash(event_id);
        let start = self.hashes.partition_point(|&stored| stored < hash);
        let end = self.hashes.partition_point(|&stored| stored <= hash);
        self.sns.get(start..end).unwrap_or_default()
    }

    pub(super) fn encode(&self, out: &mut Writer) {
        put_block(out, &ColumnData::U64(self.hashes.clone()));
        put_block(out, &ColumnData::U64(self.sns.clone()));
        out.put_u32(self.filter.len() as u32);
        out.put_slice(&self.filter);
    }

    pub(super) fn decode(bytes: &[u8], first_sn: u64, end_sn: u64) -> Result<Self, FormatError> {
        let mut reader = Reader::new(bytes);
        let hashes = take_block(&mut reader)?;
        let sns = take_block(&mut reader)?;
        let filter_len = reader.u32("graph event id filter length")? as usize;
        let filter = reader.take(filter_len, "graph event id filter")?.to_vec();
        BinaryFuseFilter::parse(&filter)?;
        let (ColumnData::U64(hashes), ColumnData::U64(sns)) = (hashes, sns) else {
            return Err(FormatError::Structural {
                rule: "graph event id hashes and sequence numbers must decode to u64 columns",
            });
        };
        let pairs: Vec<(u64, u64)> = hashes.iter().copied().zip(sns.iter().copied()).collect();
        if reader.remaining() != 0
            || hashes.is_empty()
            || hashes.len() != sns.len()
            || pairs.windows(2).any(|pair| pair.first() >= pair.last())
            || sns.iter().any(|sn| !(first_sn..end_sn).contains(sn))
        {
            return Err(FormatError::Structural {
                rule: "graph event id index must hold sorted, distinct pairs naming events in the file",
            });
        }
        Ok(Self { filter, hashes, sns })
    }
}

pub(super) fn encode_unresolved(out: &mut Writer, edges: &[UnresolvedEdge]) {
    out.put_u32(edges.len() as u32);
    for edge in edges {
        let value = edge.parent.column_value();
        out.put_u64(edge.child);
        out.put_u8(EdgeKind::of(edge.parent.kind).map_or(u8::MAX, |kind| kind.slot() as u8));
        out.put_u16(value.len() as u16);
        out.put_slice(value.as_bytes());
    }
}

pub(super) fn decode_unresolved(bytes: &[u8], first_sn: u64, end_sn: u64) -> Result<Vec<UnresolvedEdge>, FormatError> {
    let malformed = FormatError::Structural {
        rule: "graph unresolved parents must be prev or auth references by protocol id, sorted by child",
    };
    let mut reader = Reader::new(bytes);
    let count = reader.u32("graph unresolved count")? as usize;
    let mut edges: Vec<UnresolvedEdge> = Vec::with_capacity(count.min(reader.remaining() / UNRESOLVED_ENTRY_MIN_BYTES));
    for _ in 0..count {
        let child = reader.u64("graph unresolved child")?;
        let kind = EdgeKind::from_slot(reader.u8("graph unresolved kind")?).ok_or(malformed.clone())?;
        let len = usize::from(reader.u16("graph unresolved reference length")?);
        let value = std::str::from_utf8(reader.take(len, "graph unresolved reference")?).map_err(|_| {
            FormatError::InvalidUtf8 {
                what: "graph unresolved reference",
            }
        })?;
        let parent = RelationshipRef::parse_column_value(kind.relationship(), value).map_err(|_| malformed.clone())?;
        if parent.space == TargetIdSpace::SubjectSequence
            || !(first_sn..end_sn).contains(&child)
            || edges.last().is_some_and(|last| last.child > child)
        {
            return Err(malformed);
        }
        edges.push(UnresolvedEdge { child, parent });
    }
    if reader.remaining() != 0 {
        return Err(FormatError::Structural {
            rule: "graph unresolved section has trailing bytes",
        });
    }
    Ok(edges)
}

fn take_block(reader: &mut Reader<'_>) -> Result<ColumnData, FormatError> {
    let pipeline = PipelineId(reader.u32("graph block pipeline id")?);
    let len = reader.u32("graph block byte length")? as usize;
    decode_block(pipeline, reader.take(len, "graph block bytes")?)
}

pub fn decode_granule_edges(
    bytes: &[u8],
    first_sn: u64,
    rows: u32,
    file_end_sn: u64,
    chain_cover: bool,
) -> Result<GranuleEdges, FormatError> {
    let mut reader = Reader::new(bytes);
    let forward = decode_csr(&mut reader, first_sn, rows, |source, target| target < source)?;
    let reverse = decode_csr(&mut reader, first_sn, rows, |source, target| {
        source < target && target < file_end_sn
    })?;
    let inbound = decode_csr(&mut reader, first_sn, rows, |_, target| target >= file_end_sn)?;
    let mut labels = Vec::new();
    if chain_cover {
        let (ColumnData::U64(chain_ids), ColumnData::U64(seqs)) = (take_block(&mut reader)?, take_block(&mut reader)?)
        else {
            return Err(FormatError::Structural {
                rule: "graph chain labels must decode to u64 columns",
            });
        };
        if chain_ids.len() != rows as usize || seqs.len() != rows as usize {
            return Err(FormatError::Structural {
                rule: "graph chain labels hold one label per row",
            });
        }
        for (chain_id, seq) in chain_ids.into_iter().zip(seqs) {
            let (Ok(chain_id), Ok(seq)) = (u32::try_from(chain_id), u32::try_from(seq)) else {
                return Err(FormatError::RefOutOfRange {
                    what: "graph chain label",
                });
            };
            labels.push(ChainLabel { chain_id, seq });
        }
    }
    if reader.remaining() != 0 {
        return Err(FormatError::Structural {
            rule: "graph edge block has trailing bytes",
        });
    }
    Ok(GranuleEdges {
        first_sn,
        forward,
        inbound,
        labels,
        reverse,
    })
}

fn decode_csr(
    reader: &mut Reader<'_>,
    first_sn: u64,
    rows: u32,
    allowed: impl Fn(u64, u64) -> bool,
) -> Result<Csr, FormatError> {
    let edge_count = u64::from(reader.u32("graph edge count")?);
    let ColumnData::U64(offsets) = take_block(reader)? else {
        return Err(FormatError::Structural {
            rule: "graph offsets must decode to a u64 column",
        });
    };
    let slots = rows as usize * GRAPH_EDGE_KINDS;
    if offsets.len() != slots + 1
        || offsets.first() != Some(&0)
        || offsets.last() != Some(&edge_count)
        || offsets.windows(2).any(|pair| pair.first() > pair.last())
    {
        return Err(FormatError::Structural {
            rule: "graph offsets must run in order from zero to the edge count, one per slot",
        });
    }
    if edge_count == 0 {
        return Ok(Csr {
            offsets,
            targets: Vec::new(),
        });
    }
    let ColumnData::I64(deltas) = take_block(reader)? else {
        return Err(FormatError::Structural {
            rule: "graph targets must decode to an i64 column",
        });
    };
    if deltas.len() as u64 != edge_count {
        return Err(FormatError::Structural {
            rule: "graph targets must hold exactly the edge count",
        });
    }
    let mut targets: Vec<u64> = bytemuck::cast_vec(deltas);
    for (slot, pair) in offsets.windows(2).enumerate() {
        let (Some(&start), Some(&end)) = (pair.first(), pair.last()) else {
            continue;
        };
        let source = first_sn
            .checked_add((slot / GRAPH_EDGE_KINDS) as u64)
            .ok_or(FormatError::RefOutOfRange {
                what: "graph event sequence number",
            })?;
        for word in targets.get_mut(start as usize..end as usize).unwrap_or_default() {
            let delta = *word as i64;
            *word = u64::try_from(i128::from(source) - i128::from(delta))
                .ok()
                .filter(|&target| allowed(source, target))
                .ok_or(FormatError::RefOutOfRange {
                    what: "graph edge target",
                })?;
        }
    }
    Ok(Csr { offsets, targets })
}

impl OeFile {
    pub fn granule_edges(&self, granule_id: u32) -> Result<GranuleEdges, FormatError> {
        let (position, len) = self.graph_block_range(granule_id)?;
        let footer = self.footer();
        let first_sn = footer.graph.as_ref().map_or(0, |directory| directory.first_sn);
        let granule = footer
            .granules
            .iter()
            .find(|granule| granule.granule_id == granule_id)
            .ok_or(FormatError::RefOutOfRange { what: "granule id" })?;
        let bytes = self.data(position as usize, len as usize, "graph edge block")?;
        decode_granule_edges(
            bytes,
            first_sn.saturating_add(granule.first_row_ordinal),
            granule.row_count,
            first_sn.saturating_add(footer.exact_counts.row_count),
            footer.graph.as_ref().is_some_and(|graph| graph.chain_cover.is_some()),
        )
    }

    pub fn graph_chain_labels(&self, sns: &[u64]) -> Result<Vec<Option<ChainLabel>>, FormatError> {
        if self
            .footer()
            .graph
            .as_ref()
            .is_none_or(|graph| graph.chain_cover.is_none())
        {
            return Ok(vec![None; sns.len()]);
        }
        let mut labels = Vec::with_capacity(sns.len());
        for &sn in sns {
            let label = match self.graph_granule_of(sn) {
                Some(granule_id) => self.cached_granule_edges(granule_id)?.label(sn),
                None => None,
            };
            labels.push(label);
        }
        Ok(labels)
    }

    pub fn graph_chain_links(&self, chains: &[u32]) -> Vec<ChainLink> {
        let Some(cover) = self
            .footer()
            .graph
            .as_ref()
            .and_then(|graph| graph.chain_cover.as_ref())
        else {
            return Vec::new();
        };
        let normalized: Vec<u32>;
        let chains = if chains.is_sorted_by(|a, b| a < b) {
            chains
        } else {
            let mut owned = chains.to_vec();
            owned.sort_unstable();
            owned.dedup();
            normalized = owned;
            &normalized
        };
        let mut links: Vec<ChainLink> = Vec::new();
        for &chain in chains {
            let start = cover.links.partition_point(|link| link.from_chain < chain);
            let end = cover.links.partition_point(|link| link.from_chain <= chain);
            links.extend_from_slice(cover.links.get(start..end).unwrap_or_default());
        }
        links
    }

    pub fn graph_may_hold_chain(&self, chain_id: u32, seqs: RangeInclusive<u32>) -> bool {
        let Some(cover) = self
            .footer()
            .graph
            .as_ref()
            .and_then(|graph| graph.chain_cover.as_ref())
        else {
            return false;
        };
        cover
            .chains
            .binary_search_by_key(&chain_id, |chain| chain.chain_id)
            .ok()
            .and_then(|index| cover.chains.get(index))
            .is_some_and(|chain| chain.min_seq <= *seqs.end() && *seqs.start() <= chain.max_seq)
    }

    pub fn graph_sn_of(&self, event_id: &[u8]) -> Result<Option<u64>, FormatError> {
        let Some(index) = self.footer().graph.as_ref().and_then(|graph| graph.event_ids.as_ref()) else {
            return Ok(None);
        };
        index.sn_of(event_id, |sn| match self.graph_row_of(sn) {
            Some(row) => self.external_id(row),
            None => Ok(None),
        })
    }

    pub fn graph_sn_of_row(&self, row_ordinal: u64) -> Option<u64> {
        let footer = self.footer();
        if row_ordinal >= footer.exact_counts.row_count {
            return None;
        }
        footer.graph.as_ref()?.first_sn.checked_add(row_ordinal)
    }

    pub fn graph_row_of(&self, sn: u64) -> Option<u64> {
        let footer = self.footer();
        let row = sn.checked_sub(footer.graph.as_ref()?.first_sn)?;
        (row < footer.exact_counts.row_count).then_some(row)
    }

    pub fn graph_has_unresolved_edges(&self, sn: u64) -> bool {
        !self.graph_unresolved_parents(sn).is_empty()
    }

    pub fn graph_unresolved_parents(&self, sn: u64) -> &[UnresolvedEdge] {
        let Some(graph) = self.footer().graph.as_ref() else {
            return &[];
        };
        let start = graph.unresolved.partition_point(|edge| edge.child < sn);
        let end = graph.unresolved.partition_point(|edge| edge.child <= sn);
        graph.unresolved.get(start..end).unwrap_or_default()
    }

    pub fn state_root(&self, sn: u64) -> Option<StateRoot> {
        let footer = self.footer();
        let row = sn.checked_sub(footer.graph.as_ref()?.first_sn)?;
        let index = footer
            .state_roots
            .binary_search_by_key(&row, |entry| entry.row_ordinal)
            .ok()?;
        footer.state_roots.get(index).map(|entry| entry.root)
    }

    pub fn graph_granule_of(&self, sn: u64) -> Option<u32> {
        let footer = self.footer();
        let row = sn.checked_sub(footer.graph.as_ref()?.first_sn)?;
        let after = footer
            .granules
            .partition_point(|granule| granule.first_row_ordinal <= row);
        let granule = footer.granules.get(after.checked_sub(1)?)?;
        (row < granule.first_row_ordinal + u64::from(granule.row_count)).then_some(granule.granule_id)
    }

    pub(super) fn graph_block_range(&self, granule_id: u32) -> Result<(u64, u64), FormatError> {
        let footer = self.footer();
        let missing = FormatError::RefOutOfRange {
            what: "no graph edge block for the granule",
        };
        let entry = footer
            .graph
            .as_ref()
            .and_then(|directory| directory.blocks.iter().find(|entry| entry.granule_id == granule_id))
            .ok_or(missing)?;
        let position = self.file_position(granule_id, entry.offset)?;
        Ok((position as u64, entry.len))
    }
}

#[cfg(test)]
#[path = "test/graph.rs"]
pub(crate) mod tests;
