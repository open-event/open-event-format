use super::constant::{PREFIX_KEY_BYTES, PREFIX_KEY_PREFIX_LEN};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct PrefixKey {
    bytes: [u8; PREFIX_KEY_PREFIX_LEN],
    len: u8,
}

const _: () = assert!(
    size_of::<PrefixKey>() == PREFIX_KEY_BYTES,
    "a prefix key must be exactly its stored width, so a block's keys stay a flat run with nothing between them"
);

impl PrefixKey {
    pub fn of(value: &str) -> Self {
        let mut bytes = [0u8; PREFIX_KEY_PREFIX_LEN];
        for (slot, byte) in bytes.iter_mut().zip(value.bytes()) {
            *slot = byte;
        }
        Self {
            bytes,
            len: value.len().min(PREFIX_KEY_PREFIX_LEN) as u8,
        }
    }

    pub fn from_stored(stored: [u8; PREFIX_KEY_BYTES]) -> Option<Self> {
        let (&len, head) = stored.split_last()?;
        if usize::from(len) > PREFIX_KEY_PREFIX_LEN {
            return None;
        }
        Some(Self {
            bytes: head.try_into().ok()?,
            len,
        })
    }

    pub fn to_stored(self) -> [u8; PREFIX_KEY_BYTES] {
        let mut stored = [0u8; PREFIX_KEY_BYTES];
        for (slot, byte) in stored.iter_mut().zip(self.bytes) {
            *slot = byte;
        }
        if let Some(last) = stored.last_mut() {
            *last = self.len;
        }
        stored
    }

    pub fn compare(self, other: Self) -> Option<Ordering> {
        match (self.bytes, self.len).cmp(&(other.bytes, other.len)) {
            Ordering::Equal if usize::from(self.len) == PREFIX_KEY_PREFIX_LEN => None,
            ordering => Some(ordering),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StringFingerprint(u32);

impl StringFingerprint {
    pub const STORED_BYTES: usize = 4;

    pub fn of(text: &[u8]) -> Self {
        Self(text.iter().fold(0u32, |bits, byte| bits | 1u32 << (byte & 31)))
    }

    pub fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    pub fn might_contain(self, needle: Self) -> bool {
        self.0 & needle.0 == needle.0
    }

    pub fn to_bits(self) -> u32 {
        self.0
    }

    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[cfg(test)]
#[path = "test/sidecar.rs"]
mod tests;
