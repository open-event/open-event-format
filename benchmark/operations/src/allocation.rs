//! Counts what the allocator was asked for while one piece of benchmark work runs, so a write-path change can report
//! allocation and peak-memory deltas beside its latency.
//!
//! Wall-clock alone hides allocation churn: a change can shave a millisecond off a build and pay for it in temporary
//! copies, or shrink an allocation count while quietly holding more memory at the peak. Counting both alongside the
//! clock is what lets a buffer-sizing change be accepted or rejected on evidence.
//!
//! Counting is opt-in. Only a build with the `alloc-counting` feature installs the counting allocator, so an ordinary
//! timing run measures the same allocator the product does and pays nothing for the instrumentation; without the
//! feature every total reads zero and [`counting_enabled`] says so. The counters are process-wide rather than
//! per-thread — which is what makes them right for a writer that fans its work across a pool, and what makes two
//! concurrent measurements meaningless, so [`measure`] must not run alongside another one.
//!
//! This crate is dev/bench-only and is never a dependency of the `oe` library.
//!
//! See: oe-write-path/spec.md

#[cfg(feature = "alloc-counting")]
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "alloc-counting")]
use std::sync::Mutex;
#[cfg(feature = "alloc-counting")]
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// What the allocator was asked for over one measured window.
///
/// `bytes` and `peak_live_bytes` are what the caller requested, not what the allocator rounded them up to.
/// `peak_live_bytes` is the allocator's process-wide high-water mark during the window minus the live allocation total
/// when the window opened; it is distinct from the OS resident-set high-water mark.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllocationTotals {
    /// Fresh allocations, reallocations excluded — they are counted on their own below.
    pub allocations: u64,
    /// Total bytes requested across every allocation and reallocation.
    pub bytes: u64,
    pub largest_bytes: u64,
    pub peak_live_bytes: u64,
    /// Capacity growth: a buffer that outgrew its allocation and had to be moved. This is the count a pre-sizing
    /// change is trying to drive to zero.
    pub reallocations: u64,
}

#[cfg(feature = "alloc-counting")]
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "alloc-counting")]
static BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "alloc-counting")]
static LARGEST_BYTES: AtomicU64 = AtomicU64::new(0);
/// Signed so a shrink reallocation can subtract the old block before adding its smaller replacement without wrapping.
#[cfg(feature = "alloc-counting")]
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
#[cfg(feature = "alloc-counting")]
static PEAK_LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
#[cfg(feature = "alloc-counting")]
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
/// Serializes measurement windows. The allocator itself never takes this lock, so allocations made by worker threads
/// inside the window cannot deadlock it.
#[cfg(feature = "alloc-counting")]
static MEASUREMENT: Mutex<()> = Mutex::new(());

/// Whether this build actually counts. Without the `alloc-counting` feature every total is zero, and a benchmark that
/// reports them should say so rather than print a row of noughts as if they were measurements.
pub fn counting_enabled() -> bool {
    cfg!(feature = "alloc-counting")
}

/// Runs `work` and reports what the allocator was asked for while it ran.
///
/// The counters are process-wide, so nothing else may be allocating on another thread for the totals to mean
/// anything — the writer's own worker pool is exactly what this is meant to capture, an unrelated concurrent
/// benchmark is not.
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

/// Direct pass-through when counting is disabled: no lock, atomic access, or allocator hook remains in the ordinary
/// benchmark build.
#[cfg(not(feature = "alloc-counting"))]
pub fn measure<R>(work: impl FnOnce() -> R) -> (R, AllocationTotals) {
    (work(), AllocationTotals::default())
}

/// The process's maximum resident set in bytes, when the target exposes a stable `getrusage` high-water mark.
///
/// This is process-wide and monotonic, unlike [`AllocationTotals::peak_live_bytes`], which is scoped to one measured
/// operation. macOS reports `ru_maxrss` in bytes; Linux and the BSDs report KiB.
#[cfg(unix)]
pub fn resident_high_water_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` initializes the pointed-to `rusage` on success; the value is only assumed initialized after
    // a zero return code. `RUSAGE_SELF` requests statistics for this process and retains no pointer.
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

/// CPU time this process has used so far, user plus system across every thread, in nanoseconds — `None` where the
/// target has no `getrusage`. Two readings around an operation give the CPU it cost, which is what tells a parallel
/// phase's real cost apart from its wall time.
#[cfg(unix)]
pub fn process_cpu_nanos() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` initializes the pointed-to `rusage` on success; the value is only assumed initialized after
    // a zero return code. `RUSAGE_SELF` requests statistics for this process and retains no pointer.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized every field in `usage`.
    let usage = unsafe { usage.assume_init() };
    let micros = |time: libc::timeval| time.tv_sec as u64 * 1_000_000 + time.tv_usec as u64;
    Some((micros(usage.ru_utime) + micros(usage.ru_stime)) * 1_000)
}

/// Targets without `getrusage` omit the resident high-water field from reports.
#[cfg(not(unix))]
pub fn resident_high_water_bytes() -> Option<u64> {
    None
}

/// Records one request for `size` bytes. Atomics only — a counter that allocated would recurse into itself.
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

/// Records the release of `size` bytes.
#[cfg(feature = "alloc-counting")]
fn record_release(size: usize) {
    LIVE_BYTES.fetch_sub(size as i64, Ordering::Relaxed);
}

/// The system allocator with a counter around it. Every request is forwarded unchanged; only the tally is new.
#[cfg(feature = "alloc-counting")]
struct CountingAllocator;

#[cfg(feature = "alloc-counting")]
// SAFETY: every method forwards its arguments unchanged to `System`, which upholds the `GlobalAlloc` contract, and
// returns exactly what `System` returned. The counting around each call touches only atomics — it never reads,
// writes, or retains the memory involved, and never allocates, so it cannot re-enter the allocator.
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
        // unchanged to the `System` allocator that produced the block.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `pointer`, `layout`, and `new_size` are the caller's own, forwarded unchanged to the `System`
        // allocator that produced the block.
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
