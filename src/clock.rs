use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, Ordering};

pub trait Clock: Send + Sync {
    fn now_nanos(&self) -> i64;

    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_nanos(self.now_nanos())
    }
}

#[derive(Debug)]
pub struct FixedClock {
    now_nanos: AtomicI64,
}

impl FixedClock {
    pub fn at(now_nanos: i64) -> Self {
        Self {
            now_nanos: AtomicI64::new(now_nanos),
        }
    }

    pub fn advance(&self, nanos: i64) {
        let _ = self
            .now_nanos
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |now| {
                Some(now.saturating_add(nanos))
            });
    }
}

impl Clock for FixedClock {
    fn now_nanos(&self) -> i64 {
        self.now_nanos.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[path = "test/clock.rs"]
mod tests;
