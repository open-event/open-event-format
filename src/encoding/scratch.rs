use super::constant::{MAX_RETAINED_ARENA_BYTES, MAX_RETAINED_SCRATCH_VALUES};
use std::cell::RefCell;

thread_local! {
    static ARENA_BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
    static UNPACK_BUFFER: RefCell<Option<Vec<u64>>> = const { RefCell::new(None) };
}

pub(crate) fn with_arena_buffer<R>(search: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    let mut buffer = ARENA_BUFFER.with(|slot| slot.borrow_mut().take()).unwrap_or_default();
    buffer.clear();
    let found = search(&mut buffer);
    if buffer.capacity() <= MAX_RETAINED_ARENA_BYTES {
        ARENA_BUFFER.with(|slot| *slot.borrow_mut() = Some(buffer));
    }
    found
}

pub(crate) fn with_unpack_buffer<R>(decode: impl FnOnce(&mut Vec<u64>) -> R) -> R {
    let mut buffer = UNPACK_BUFFER.with(|slot| slot.borrow_mut().take()).unwrap_or_default();
    buffer.clear();
    let decoded = decode(&mut buffer);
    if buffer.capacity() <= MAX_RETAINED_SCRATCH_VALUES {
        UNPACK_BUFFER.with(|slot| *slot.borrow_mut() = Some(buffer));
    }
    decoded
}

#[cfg(test)]
#[path = "test/scratch.rs"]
mod tests;
