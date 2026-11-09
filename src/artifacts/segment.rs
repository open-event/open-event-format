use super::frame::{self, OejFrameHeaderV1};
use super::{BLAKE3_LEN, FRAME_ALIGN, MAX_LARGE_FRAME, MAX_NORMAL_FRAME};
use crate::error::StorageError;
use crate::invariants::{JournalStorage, ShardId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    Active,
    Recyclable,
    Retired,
    Sealed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub first_frame_blake3: Option<[u8; BLAKE3_LEN]>,
    pub first_frame_sequence: Option<u64>,
    pub generation: u64,
    pub last_frame_blake3: Option<[u8; BLAKE3_LEN]>,
    pub last_frame_sequence: Option<u64>,
    pub segment_chain_blake3: Option<[u8; BLAKE3_LEN]>,
    pub segment_id: u64,
    pub state: SegmentState,
    pub write_cursor: u64,
}

impl SegmentDescriptor {
    pub fn new(segment_id: u64, generation: u64) -> Self {
        Self {
            segment_id,
            generation,
            state: SegmentState::Active,
            write_cursor: 0,
            first_frame_sequence: None,
            last_frame_sequence: None,
            first_frame_blake3: None,
            last_frame_blake3: None,
            segment_chain_blake3: None,
        }
    }
}

pub fn recycling_eligible(
    state: SegmentState,
    coverage_published_or_replicated: bool,
    safety_window_elapsed: bool,
    overlay_rebuild_may_require: bool,
    chain_anchor_recorded: bool,
) -> bool {
    state == SegmentState::Sealed
        && coverage_published_or_replicated
        && safety_window_elapsed
        && !overlay_rebuild_may_require
        && chain_anchor_recorded
}

pub fn chain_init(segment_id: u64, generation: u64, frame_blake3: &[u8; BLAKE3_LEN]) -> [u8; BLAKE3_LEN] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&segment_id.to_le_bytes());
    hasher.update(&generation.to_le_bytes());
    hasher.update(frame_blake3);
    *hasher.finalize().as_bytes()
}

pub fn chain_next(previous: &[u8; BLAKE3_LEN], frame_blake3: &[u8; BLAKE3_LEN]) -> [u8; BLAKE3_LEN] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(previous);
    hasher.update(frame_blake3);
    *hasher.finalize().as_bytes()
}

#[derive(Debug, Clone)]
pub struct ReplayedFrame {
    pub frame_bytes: Vec<u8>,
    pub header: OejFrameHeaderV1,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayTail {
    Clean,
    Truncated { offset: u64, trailing_nonzero: bool },
}

#[derive(Debug)]
pub struct ReplayOutcome {
    pub frames: Vec<ReplayedFrame>,
    pub segment_chain_blake3: Option<[u8; BLAKE3_LEN]>,
    pub tail: ReplayTail,
}

pub fn replay_segment(
    storage: &dyn JournalStorage,
    shard: ShardId,
    segment_id: u64,
    generation: u64,
) -> Result<ReplayOutcome, StorageError> {
    let extent = storage.extent(shard)?;
    let mut offset = 0u64;
    let mut frames = Vec::new();
    let mut chain: Option<[u8; BLAKE3_LEN]> = None;
    let mut recycled = false;
    let mut tail = ReplayTail::Clean;

    while offset + u64::from(FRAME_ALIGN) <= extent {
        let head_block = storage.read(shard, offset, FRAME_ALIGN)?;
        if super::is_all_zero(&head_block) {
            let beyond = truncated(storage, shard, offset, offset + u64::from(FRAME_ALIGN), extent)?;
            if matches!(
                beyond,
                ReplayTail::Truncated {
                    trailing_nonzero: true,
                    ..
                }
            ) {
                tail = beyond;
            }
            recycled = true;
            break;
        }
        let Ok(precheck) = frame::precheck_header(&head_block) else {
            tail = truncated(storage, shard, offset, offset + u64::from(FRAME_ALIGN), extent)?;
            break;
        };
        let frame_len = u64::from(precheck.frame_len);
        let max_frame_len = if precheck.is_large_event_frame() {
            MAX_LARGE_FRAME
        } else {
            MAX_NORMAL_FRAME
        };
        if frame_len == 0 || frame_len % u64::from(FRAME_ALIGN) != 0 || precheck.frame_len > max_frame_len {
            tail = truncated(storage, shard, offset, offset + u64::from(FRAME_ALIGN), extent)?;
            break;
        }
        if offset + frame_len > extent {
            tail = truncated(storage, shard, offset, offset + frame_len, extent)?;
            break;
        }
        let frame_bytes = if frame_len == u64::from(FRAME_ALIGN) {
            head_block
        } else {
            let mut frame_bytes = head_block;
            frame_bytes.extend_from_slice(&storage.read(
                shard,
                offset + u64::from(FRAME_ALIGN),
                precheck.frame_len - FRAME_ALIGN,
            )?);
            frame_bytes
        };
        let Ok((header, _payload)) = frame::decode_frame(&frame_bytes) else {
            tail = truncated(storage, shard, offset, offset + frame_len, extent)?;
            break;
        };
        chain = Some(match &chain {
            None => chain_init(segment_id, generation, &header.frame_blake3),
            Some(previous) => chain_next(previous, &header.frame_blake3),
        });
        frames.push(ReplayedFrame {
            offset,
            header,
            frame_bytes,
        });
        offset += frame_len;
    }

    if matches!(tail, ReplayTail::Clean) && !recycled && offset < extent {
        tail = truncated(storage, shard, offset, extent, extent)?;
    }

    Ok(ReplayOutcome {
        frames,
        tail,
        segment_chain_blake3: chain,
    })
}

pub fn recover_segment(
    storage: &mut dyn JournalStorage,
    shard: ShardId,
    segment_id: u64,
    generation: u64,
) -> Result<ReplayOutcome, StorageError> {
    let outcome = replay_segment(&*storage, shard, segment_id, generation)?;
    let last_good_end = match outcome.tail {
        ReplayTail::Clean => outcome
            .frames
            .last()
            .map_or(0, |frame| frame.offset + frame.frame_bytes.len() as u64),
        ReplayTail::Truncated { offset, .. } => offset,
    };
    if last_good_end < storage.extent(shard)? {
        storage.truncate(shard, last_good_end)?;
    }
    Ok(outcome)
}

fn truncated(
    storage: &dyn JournalStorage,
    shard: ShardId,
    offset: u64,
    scan_from: u64,
    extent: u64,
) -> Result<ReplayTail, StorageError> {
    let mut scan = scan_from;
    let mut trailing_nonzero = false;
    while scan + u64::from(FRAME_ALIGN) <= extent {
        let block = storage.read(shard, scan, FRAME_ALIGN)?;
        if !super::is_all_zero(&block) {
            trailing_nonzero = true;
            break;
        }
        scan += u64::from(FRAME_ALIGN);
    }
    Ok(ReplayTail::Truncated {
        offset,
        trailing_nonzero,
    })
}

pub fn verify_chain_anchor(descriptor: &SegmentDescriptor, recovered: Option<&[u8; BLAKE3_LEN]>) -> bool {
    match (&descriptor.segment_chain_blake3, recovered) {
        (Some(recorded), Some(recovered)) => recorded == recovered,
        (None, None) => true,
        _ => false,
    }
}

#[cfg(test)]
#[path = "test/segment.rs"]
mod tests;
