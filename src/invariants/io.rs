use super::constant::{JITTER_XORSHIFT_SHIFT_1, JITTER_XORSHIFT_SHIFT_2, JITTER_XORSHIFT_SHIFT_3};
use super::{Clock, MonotonicClock};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct SystemClock {
    rng_state: AtomicU64,
    start: std::time::Instant,
}

impl SystemClock {
    pub fn new() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
            .unwrap_or(0x9E37_79B9_7F4A_7C15);
        Self {
            start: std::time::Instant::now(),
            rng_state: AtomicU64::new(seed | 1),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now_nanos(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

impl MonotonicClock for SystemClock {
    fn monotonic_nanos(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn jitter(&self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        let mut x = self.rng_state.load(Ordering::Relaxed);
        x ^= x << JITTER_XORSHIFT_SHIFT_1;
        x ^= x >> JITTER_XORSHIFT_SHIFT_2;
        x ^= x << JITTER_XORSHIFT_SHIFT_3;
        self.rng_state.store(x, Ordering::Relaxed);
        x % bound
    }
}

#[derive(Debug, Default)]
pub struct ThreadPoolEncodeExecutor;

impl super::EncodeExecutor for ThreadPoolEncodeExecutor {
    fn parallelism(&self) -> usize {
        rayon::current_num_threads().max(1)
    }

    fn run_jobs(&self, job_count: usize, job: &(dyn Fn(usize) + Sync)) {
        (0..job_count).into_par_iter().for_each(job);
    }
}
