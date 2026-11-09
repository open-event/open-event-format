use super::constant::FRAME_ALIGNMENT;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockTarget(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub len: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    Buffered,
    DirectIo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Atomicity {
    pub atomic_frame_multiple: u32,
    pub awupf_bytes: u32,
    pub durability_mode: DurabilityMode,
    pub untorn_write_bytes: u32,
}

impl Atomicity {
    pub const fn buffered_unprobed() -> Self {
        Self {
            atomic_frame_multiple: FRAME_ALIGNMENT as u32,
            awupf_bytes: 0,
            durability_mode: DurabilityMode::Buffered,
            untorn_write_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoCapabilities {
    pub atomic_write_unit_bytes: u32,
    pub direct_io_alignment_bytes: u32,
    pub efficient_extent_zeroing: bool,
    pub large_block_bytes: u32,
    pub nvme_pi: bool,
    pub nvmeof_initiator: bool,
    pub uncached_reads: bool,
    pub zoned_placement: bool,
}

impl IoCapabilities {
    pub const fn portable() -> Self {
        Self {
            atomic_write_unit_bytes: 0,
            direct_io_alignment_bytes: 0,
            efficient_extent_zeroing: false,
            large_block_bytes: 0,
            nvme_pi: false,
            nvmeof_initiator: false,
            uncached_reads: false,
            zoned_placement: false,
        }
    }
}

impl Default for IoCapabilities {
    fn default() -> Self {
        Self::portable()
    }
}
