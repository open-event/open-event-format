use super::bytes::slice;
use super::constant::{TREE_TRAILER_LEN, TREE_TRAILER_MAGIC};
use super::error::CodecError;
use blake3::CHUNK_LEN;
use blake3::hazmat::*;
use rayon::prelude::*;

const PARALLEL_HASH_MIN_LEN: usize = 1 << 20;

const NODE_LEN: usize = blake3::OUT_LEN * 2;

pub fn crc64_nvme(bytes: &[u8]) -> u64 {
    crc_fast::crc64_nvme(bytes)
}

pub fn crc64_matches(bytes: &[u8], expected: u64) -> bool {
    crc64_nvme(bytes) == expected
}

pub struct StreamingCrc64Nvme {
    crc: crc_fast::Digest,
}

impl StreamingCrc64Nvme {
    pub fn new() -> Self {
        Self {
            crc: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.crc.update(bytes);
    }

    pub fn combine(&mut self, suffix: &Self) {
        self.crc.combine(&suffix.crc);
    }

    pub fn finalize(&self) -> u64 {
        self.crc.finalize()
    }
}

impl Default for StreamingCrc64Nvme {
    fn default() -> Self {
        Self::new()
    }
}

pub fn hash_tree(bytes: &[u8]) -> blake3::Hash {
    if bytes.len() < PARALLEL_HASH_MIN_LEN {
        return blake3::hash(bytes);
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update_rayon(bytes);
    hasher.finalize()
}

pub fn hash_segments(segments: &[&[u8]]) -> Vec<blake3::Hash> {
    let pieces: Vec<(&[u8], usize)> = segments
        .iter()
        .flat_map(|segment| {
            (0..segment.len().max(1))
                .step_by(PARALLEL_HASH_MIN_LEN)
                .map(move |offset| (*segment, offset))
        })
        .collect();
    let piece_cvs: Vec<[u8; blake3::OUT_LEN]> = pieces
        .par_iter()
        .map(|&(segment, offset)| {
            if segment.len() <= PARALLEL_HASH_MIN_LEN {
                return *blake3::hash(segment).as_bytes();
            }
            leaf_cv(segment, offset, (offset + PARALLEL_HASH_MIN_LEN).min(segment.len()))
        })
        .collect();
    let mut remaining = piece_cvs.as_slice();
    segments
        .iter()
        .map(|segment| {
            let (cvs, rest) = remaining.split_at(segment.len().max(1).div_ceil(PARALLEL_HASH_MIN_LEN));
            remaining = rest;
            blake3::Hash::from_bytes(merge_pieces(cvs, segment.len(), true))
        })
        .collect()
}

fn merge_pieces(cvs: &[[u8; blake3::OUT_LEN]], len: usize, is_root: bool) -> [u8; blake3::OUT_LEN] {
    let [first, rest @ ..] = cvs else {
        return [0; blake3::OUT_LEN];
    };
    if rest.is_empty() {
        return *first;
    }
    let left_len = left_subtree_len(len as u64) as usize;
    let (left_cvs, right_cvs) = cvs.split_at(left_len / PARALLEL_HASH_MIN_LEN);
    let left = merge_pieces(left_cvs, left_len, false);
    let right = merge_pieces(right_cvs, len - left_len, false);
    if is_root {
        *merge_subtrees_root(&left, &right, Mode::Hash).as_bytes()
    } else {
        merge_subtrees_non_root(&left, &right, Mode::Hash)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboardObject<'a> {
    pub content: &'a [u8],
    pub tree: Option<&'a [u8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFault {
    Corrupt,
    TreeUnusable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltOutboard {
    pub root: [u8; blake3::OUT_LEN],
    pub tree: Option<Vec<u8>>,
}

pub fn outboard_tree_bytes(content_len: usize, chunk_group_bytes: usize) -> usize {
    let group = chunk_group_bytes.max(CHUNK_LEN);
    node_count(content_len, group).saturating_mul(NODE_LEN)
}

pub fn build_outboard_tree_and_root(content: &[u8], chunk_group_bytes: usize) -> BuiltOutboard {
    let group = chunk_group_bytes.max(CHUNK_LEN);
    let tree_len = outboard_tree_bytes(content.len(), group);
    if tree_len == 0 {
        return BuiltOutboard {
            root: *hash_tree(content).as_bytes(),
            tree: None,
        };
    }

    let mut nodes = vec![0u8; tree_len];
    let _ = subtree_nodes_into(content, 0, content.len(), group, &mut nodes);
    let (Ok(left), Ok(right)) = (cv_at(&nodes, 0), cv_at(&nodes, blake3::OUT_LEN)) else {
        return BuiltOutboard {
            root: *hash_tree(content).as_bytes(),
            tree: None,
        };
    };
    BuiltOutboard {
        root: *merge_subtrees_root(&left, &right, Mode::Hash).as_bytes(),
        tree: Some(nodes),
    }
}

pub fn build_outboard_tree(content: &[u8], chunk_group_bytes: usize) -> Option<Vec<u8>> {
    build_outboard_tree_and_root(content, chunk_group_bytes).tree
}

pub fn attach_outboard_tree(mut content: Vec<u8>, chunk_group_bytes: usize) -> Vec<u8> {
    let Some(tree) = build_outboard_tree(&content, chunk_group_bytes) else {
        return content;
    };
    let tree_len = tree.len() as u64;
    content.extend_from_slice(&tree);
    content.extend_from_slice(&tree_len.to_le_bytes());
    content.extend_from_slice(&TREE_TRAILER_MAGIC);
    content
}

pub fn parse_outboard_object<'a>(object: &'a [u8], content_magic: &[u8; 4]) -> Result<OutboardObject<'a>, CodecError> {
    if object.len() < 4 {
        return Ok(OutboardObject {
            content: object,
            tree: None,
        });
    }
    let tail = slice(object, object.len() - 4, 4, "object trailing magic")?;
    if tail != TREE_TRAILER_MAGIC {
        return Ok(OutboardObject {
            content: object,
            tree: None,
        });
    }
    if object.len() < TREE_TRAILER_LEN {
        return Err(CodecError::Truncated {
            what: "outboard trailer",
        });
    }
    let len_bytes = slice(object, object.len() - TREE_TRAILER_LEN, 8, "outboard tree length")?;
    let tree_len = u64::from_le_bytes(len_bytes.try_into().unwrap_or([0; 8])) as usize;
    let content_end = object
        .len()
        .checked_sub(TREE_TRAILER_LEN)
        .and_then(|n| n.checked_sub(tree_len))
        .ok_or(CodecError::Structural {
            rule: "outboard tree length exceeds object",
        })?;
    let content = slice(object, 0, content_end, "outboard content")?;
    let tree = slice(object, content_end, tree_len, "outboard tree")?;
    if content.len() < 4 || slice(content, content.len() - 4, 4, "content end magic")? != content_magic {
        return Err(CodecError::BadMagic {
            expected: "outboard content magic",
        });
    }
    Ok(OutboardObject {
        content,
        tree: Some(tree),
    })
}

pub fn verify_range<'a>(
    content: &'a [u8],
    tree: &[u8],
    root: &[u8; blake3::OUT_LEN],
    start: u64,
    len: u64,
    chunk_group_bytes: usize,
) -> Result<&'a [u8], RangeFault> {
    let group = chunk_group_bytes.max(CHUNK_LEN);
    let n = content.len();
    let start = usize::try_from(start).map_err(|_| RangeFault::Corrupt)?;
    let end = start
        .checked_add(usize::try_from(len).map_err(|_| RangeFault::Corrupt)?)
        .ok_or(RangeFault::Corrupt)?;
    if end > n {
        return Err(RangeFault::Corrupt);
    }
    if n <= group {
        return Err(RangeFault::TreeUnusable);
    }
    if len == 0 {
        return Err(RangeFault::TreeUnusable);
    }
    let mut cursor = 0usize;
    verify_subtree(content, tree, &mut cursor, 0, n, root, true, start, end, group)?;
    slice(content, start, end - start, "verified range").map_err(|_| RangeFault::Corrupt)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the proof needs authenticated content geometry plus fetched and requested subranges"
)]
pub fn verify_range_from_slice<'a>(
    content_len: u64,
    fetched: &'a [u8],
    fetched_start: u64,
    tree: &[u8],
    root: &[u8; blake3::OUT_LEN],
    start: u64,
    len: u64,
    chunk_group_bytes: usize,
) -> Result<&'a [u8], RangeFault> {
    let n = usize::try_from(content_len).map_err(|_| RangeFault::Corrupt)?;
    let fetched_start = usize::try_from(fetched_start).map_err(|_| RangeFault::Corrupt)?;
    let fetched_end = fetched_start.checked_add(fetched.len()).ok_or(RangeFault::Corrupt)?;
    let start = usize::try_from(start).map_err(|_| RangeFault::Corrupt)?;
    let end = start
        .checked_add(usize::try_from(len).map_err(|_| RangeFault::Corrupt)?)
        .ok_or(RangeFault::Corrupt)?;
    if fetched_end > n || end > n || start < fetched_start || end > fetched_end || len == 0 {
        return Err(RangeFault::Corrupt);
    }
    let group = chunk_group_bytes.max(CHUNK_LEN);
    if n <= group {
        if fetched_start != 0 || fetched_end != n || hash_tree(fetched).as_bytes() != root {
            return Err(RangeFault::Corrupt);
        }
    } else {
        if tree.len() != outboard_tree_bytes(n, group) {
            return Err(RangeFault::TreeUnusable);
        }
        let mut cursor = 0usize;
        verify_subtree_slice(
            fetched,
            fetched_start,
            tree,
            &mut cursor,
            0,
            n,
            root,
            true,
            start,
            end,
            group,
        )?;
    }
    let local_start = start - fetched_start;
    slice(fetched, local_start, end - start, "verified range").map_err(|_| RangeFault::Corrupt)
}

pub fn verify_object_range<'a>(
    object: &'a [u8],
    root: &[u8; blake3::OUT_LEN],
    start: u64,
    len: u64,
    chunk_group_bytes: usize,
    content_magic: &[u8; 4],
) -> Result<&'a [u8], CodecError> {
    let group = chunk_group_bytes.max(CHUNK_LEN);
    if let Ok(parsed) = parse_outboard_object(object, content_magic) {
        let content = parsed.content;
        if let Some(tree) = parsed.tree {
            match verify_range(content, tree, root, start, len, group) {
                Ok(bytes) => return Ok(bytes),
                Err(RangeFault::Corrupt) => {
                    return Err(CodecError::Blake3Mismatch {
                        scope: "verified range",
                    });
                }
                Err(RangeFault::TreeUnusable) => {}
            }
        }
        return verify_whole(content, root, start, len);
    }
    let boundary = derived_content_end(object.len(), group);
    for candidate in boundary.into_iter().chain(std::iter::once(object.len())) {
        if let Some(content) = object.get(..candidate)
            && let Ok(bytes) = verify_whole(content, root, start, len)
        {
            return Ok(bytes);
        }
    }
    Err(CodecError::Blake3Mismatch { scope: "whole content" })
}

fn derived_content_end(object_len: usize, group: usize) -> Option<usize> {
    let total = |content_len: usize| content_len + node_count(content_len, group) * NODE_LEN + TREE_TRAILER_LEN;
    let (mut low, mut high) = (0usize, object_len.checked_sub(TREE_TRAILER_LEN)?);
    while low <= high {
        let mid = low + (high - low) / 2;
        match total(mid).cmp(&object_len) {
            std::cmp::Ordering::Equal => return Some(mid),
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid.checked_sub(1)?,
        }
    }
    None
}

fn verify_whole<'a>(
    content: &'a [u8],
    root: &[u8; blake3::OUT_LEN],
    start: u64,
    len: u64,
) -> Result<&'a [u8], CodecError> {
    if blake3::hash(content).as_bytes() != root {
        return Err(CodecError::Blake3Mismatch { scope: "whole content" });
    }
    let start = usize::try_from(start).map_err(|_| CodecError::Truncated { what: "range start" })?;
    let len = usize::try_from(len).map_err(|_| CodecError::Truncated { what: "range length" })?;
    slice(content, start, len, "verified range")
}

fn subtree_nodes_into(content: &[u8], lo: usize, hi: usize, group: usize, out: &mut [u8]) -> [u8; blake3::OUT_LEN] {
    if hi - lo <= group {
        debug_assert!(out.is_empty());
        return leaf_cv(content, lo, hi);
    }
    let left_len = left_subtree_len((hi - lo) as u64) as usize;
    let mid = lo + left_len;
    let (node, children) = out.split_at_mut(NODE_LEN);
    let left_tree_len = outboard_tree_bytes(mid - lo, group);
    let (left_out, right_out) = children.split_at_mut(left_tree_len);
    let (left_cv, right_cv) = rayon::join(
        || subtree_nodes_into(content, lo, mid, group, left_out),
        || subtree_nodes_into(content, mid, hi, group, right_out),
    );
    let (left_node, right_node) = node.split_at_mut(blake3::OUT_LEN);
    left_node.copy_from_slice(&left_cv);
    right_node.copy_from_slice(&right_cv);
    merge_subtrees_non_root(&left_cv, &right_cv, Mode::Hash)
}

#[expect(
    clippy::too_many_arguments,
    reason = "one recursive walk carries the tree cursor, the subtree bounds, and the query range"
)]
fn verify_subtree(
    content: &[u8],
    tree: &[u8],
    cursor: &mut usize,
    lo: usize,
    hi: usize,
    expected: &[u8; blake3::OUT_LEN],
    is_root: bool,
    range_start: usize,
    range_end: usize,
    group: usize,
) -> Result<(), RangeFault> {
    if hi - lo <= group {
        if lo < range_end && range_start < hi {
            let cv = leaf_cv(content, lo, hi);
            if &cv != expected {
                return Err(RangeFault::Corrupt);
            }
        }
        return Ok(());
    }
    let node = slice(tree, *cursor, NODE_LEN, "outboard node").map_err(|_| RangeFault::TreeUnusable)?;
    let left_cv = cv_at(node, 0)?;
    let right_cv = cv_at(node, blake3::OUT_LEN)?;
    *cursor += NODE_LEN;
    let actual = if is_root {
        *merge_subtrees_root(&left_cv, &right_cv, Mode::Hash).as_bytes()
    } else {
        merge_subtrees_non_root(&left_cv, &right_cv, Mode::Hash)
    };
    if &actual != expected {
        return Err(RangeFault::TreeUnusable);
    }
    let mid = lo + left_subtree_len((hi - lo) as u64) as usize;
    if lo < range_end && range_start < mid {
        verify_subtree(
            content,
            tree,
            cursor,
            lo,
            mid,
            &left_cv,
            false,
            range_start,
            range_end,
            group,
        )?;
    } else {
        *cursor += node_count(mid - lo, group) * NODE_LEN;
    }
    if mid < range_end && range_start < hi {
        verify_subtree(
            content,
            tree,
            cursor,
            mid,
            hi,
            &right_cv,
            false,
            range_start,
            range_end,
            group,
        )?;
    } else {
        *cursor += node_count(hi - mid, group) * NODE_LEN;
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "one recursive walk carries proof, fetched-slice, subtree, and requested-range geometry"
)]
fn verify_subtree_slice(
    fetched: &[u8],
    fetched_start: usize,
    tree: &[u8],
    cursor: &mut usize,
    lo: usize,
    hi: usize,
    expected: &[u8; blake3::OUT_LEN],
    is_root: bool,
    range_start: usize,
    range_end: usize,
    group: usize,
) -> Result<(), RangeFault> {
    if hi - lo <= group {
        if lo < range_end && range_start < hi {
            let local_lo = lo.checked_sub(fetched_start).ok_or(RangeFault::Corrupt)?;
            let local_hi = hi.checked_sub(fetched_start).ok_or(RangeFault::Corrupt)?;
            let bytes = fetched.get(local_lo..local_hi).ok_or(RangeFault::Corrupt)?;
            let mut hasher = blake3::Hasher::new();
            hasher.set_input_offset(lo as u64);
            hasher.update(bytes);
            if &hasher.finalize_non_root() != expected {
                return Err(RangeFault::Corrupt);
            }
        }
        return Ok(());
    }
    let node = slice(tree, *cursor, NODE_LEN, "outboard node").map_err(|_| RangeFault::TreeUnusable)?;
    let left_cv = cv_at(node, 0)?;
    let right_cv = cv_at(node, blake3::OUT_LEN)?;
    *cursor += NODE_LEN;
    let actual = if is_root {
        *merge_subtrees_root(&left_cv, &right_cv, Mode::Hash).as_bytes()
    } else {
        merge_subtrees_non_root(&left_cv, &right_cv, Mode::Hash)
    };
    if &actual != expected {
        return Err(RangeFault::TreeUnusable);
    }
    let mid = lo + left_subtree_len((hi - lo) as u64) as usize;
    if lo < range_end && range_start < mid {
        verify_subtree_slice(
            fetched,
            fetched_start,
            tree,
            cursor,
            lo,
            mid,
            &left_cv,
            false,
            range_start,
            range_end,
            group,
        )?;
    } else {
        *cursor += node_count(mid - lo, group) * NODE_LEN;
    }
    if mid < range_end && range_start < hi {
        verify_subtree_slice(
            fetched,
            fetched_start,
            tree,
            cursor,
            mid,
            hi,
            &right_cv,
            false,
            range_start,
            range_end,
            group,
        )?;
    } else {
        *cursor += node_count(hi - mid, group) * NODE_LEN;
    }
    Ok(())
}

fn leaf_cv(content: &[u8], lo: usize, hi: usize) -> [u8; blake3::OUT_LEN] {
    let data = content.get(lo..hi).unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.set_input_offset(lo as u64);
    hasher.update(data);
    hasher.finalize_non_root()
}

fn cv_at(node: &[u8], offset: usize) -> Result<[u8; blake3::OUT_LEN], RangeFault> {
    let bytes = slice(node, offset, blake3::OUT_LEN, "chaining value").map_err(|_| RangeFault::TreeUnusable)?;
    bytes.try_into().map_err(|_| RangeFault::TreeUnusable)
}

fn node_count(len: usize, group: usize) -> usize {
    if len <= group {
        return 0;
    }
    let left_len = left_subtree_len(len as u64) as usize;
    1 + node_count(left_len, group) + node_count(len - left_len, group)
}

#[cfg(test)]
#[path = "test/integrity.rs"]
mod tests;
