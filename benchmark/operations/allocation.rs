
#[cfg(feature = "alloc-counting")]
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "alloc-counting")]
use std::sync::Mutex;
#[cfg(feature = "alloc-counting")]
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllocationTotals {
    pub allocations: u64,
    pub bytes: u64,
    pub largest_bytes: u64,
    pub peak_live_bytes: u64,
    pub reallocations: u64,
}

#[cfg(feature = "alloc-counting")]
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "alloc-counting")]
static BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "alloc-counting")]
static LARGEST_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "alloc-counting")]
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
#[cfg(feature = "alloc-counting")]
static PEAK_LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
#[cfg(feature = "alloc-counting")]
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "alloc-counting")]
static MEASUREMENT: Mutex<()> = Mutex::new(());

pub fn counting_enabled() -> bool {
    cfg!(feature = "alloc-counting")
}

#[cfg(feature = "alloc-counting")]
pub fn measure<R>(work: impl FnOnce() -> R) -> (R, AllocationTotals) {
    let _measurement = MEASUREMENT.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    LARGEST_BYTES.store(0, Ordering::Relaxed);
    let live_at_start = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_LIVE_BYTES.store(live_at_start, Ordering::Relaxed);
    REALLOCATIONS.store(0, Ordering::Relaxed);
    let produced = work();
    let totals = AllocationTotals {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
        largest_bytes: LARGEST_BYTES.load(Ordering::Relaxed),
        peak_live_bytes: PEAK_LIVE_BYTES
            .load(Ordering::Relaxed)
            .saturating_sub(live_at_start)
            .max(0) as u64,
        reallocations: REALLOCATIONS.load(Ordering::Relaxed),
    };
    (produced, totals)
}

#[cfg(not(feature = "alloc-counting"))]
pub fn measure<R>(work: impl FnOnce() -> R) -> (R, AllocationTotals) {
    (work(), AllocationTotals::default())
}

#[cfg(unix)]
pub fn resident_high_water_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` initializes the pointed-to `rusage` on success; the value is only assumed initialized after
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized every field in `usage`.
    let max_rss = unsafe { usage.assume_init() }.ru_maxrss;
    let max_rss = u64::try_from(max_rss).ok()?;
    if cfg!(any(target_os = "macos", target_os = "ios")) {
        Some(max_rss)
    } else {
        Some(max_rss.saturating_mul(1024))
    }
}

#[cfg(unix)]
pub fn process_cpu_nanos() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` initializes the pointed-to `rusage` on success; the value is only assumed initialized after
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized every field in `usage`.
    let usage = unsafe { usage.assume_init() };
    let micros = |time: libc::timeval| time.tv_sec as u64 * 1_000_000 + time.tv_usec as u64;
    Some((micros(usage.ru_utime) + micros(usage.ru_stime)) * 1_000)
}

#[cfg(not(unix))]
pub fn resident_high_water_bytes() -> Option<u64> {
    None
}

#[cfg(feature = "alloc-counting")]
fn record_growth(size: usize, reallocation: bool) {
    let size = size as u64;
    if reallocation {
        REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    } else {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
    BYTES.fetch_add(size, Ordering::Relaxed);
    LARGEST_BYTES.fetch_max(size, Ordering::Relaxed);
    let live = LIVE_BYTES.fetch_add(size as i64, Ordering::Relaxed) + size as i64;
    PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
}

#[cfg(feature = "alloc-counting")]
fn record_release(size: usize) {
    LIVE_BYTES.fetch_sub(size as i64, Ordering::Relaxed);
}

#[cfg(feature = "alloc-counting")]
struct CountingAllocator;

#[cfg(feature = "alloc-counting")]
// SAFETY: every method forwards its arguments unchanged to `System`, which upholds the `GlobalAlloc` contract, and
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` reaches the system allocator exactly as the caller supplied it.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_growth(layout.size(), false);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` reaches the system allocator exactly as the caller supplied it.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_growth(layout.size(), false);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record_release(layout.size());
        // SAFETY: `pointer` and `layout` are the pair the caller guarantees came from this allocator, forwarded
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `pointer`, `layout`, and `new_size` are the caller's own, forwarded unchanged to the `System`
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            record_release(layout.size());
            record_growth(new_size, true);
        }
        moved
    }
}

#[cfg(feature = "alloc-counting")]
#[global_allocator]
static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;

#[cfg(test)]
#[path = "test/allocation.rs"]
mod tests;
