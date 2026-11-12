//! Monotonic 64-bit sequence ID generator for queue ordering and creation tie-breaking.
//!
//! Produces positive `i64` identifiers compatible with standard `BIGINT NOT NULL` primary keys
//! without relying on database `AUTO_INCREMENT` sequences.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Custom epoch: 2025-01-01T00:00:00Z in microseconds since UNIX_EPOCH.
const CUSTOM_EPOCH_MICROS: u64 = 1_735_689_600_000_000;

/// Bits reserved for intra-microsecond monotonic sequence numbers (4,096/us).
const SEQ_BITS: u32 = 12;
const MAX_MICROS_OFFSET: i64 = (i64::MAX >> SEQ_BITS) - 1;

static LAST_SEQ_ID: AtomicI64 = AtomicI64::new(0);
static LAST_WALL_MS: AtomicI64 = AtomicI64::new(0);

/// Returns a monotonic wall-clock timestamp in milliseconds since Unix epoch.
///
/// Clamped so small backward NTP adjustments within a process never move `now_millis()` backward.
pub(crate) fn now_millis() -> i64 {
    let raw_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0);

    let mut prev = LAST_WALL_MS.load(Ordering::Relaxed);
    loop {
        let clamped = raw_ms.max(prev);
        match LAST_WALL_MS.compare_exchange_weak(prev, clamped, Ordering::SeqCst, Ordering::Relaxed)
        {
            Ok(_) => return clamped,
            Err(actual) => prev = actual,
        }
    }
}

/// Computes `now_millis() + duration` with saturating `i64` arithmetic.
pub(crate) fn timestamp_after(duration: std::time::Duration) -> i64 {
    let delta = duration.as_millis().min(i64::MAX as u128) as i64;
    now_millis().saturating_add(delta)
}

/// Computes `base_ms + duration` with saturating `i64` arithmetic.
pub(crate) fn offset_millis(base_ms: i64, duration: std::time::Duration) -> i64 {
    let delta = duration.as_millis().min(i64::MAX as u128) as i64;
    base_ms.saturating_add(delta)
}

/// Generates a strictly increasing positive `i64` suitable for `BIGINT NOT NULL` primary keys.
///
/// Layout (63 usable positive bits):
/// - Upper 51 bits: microseconds since 2025-01-01 UTC (~71 years of range)
/// - Lower 12 bits: monotonic sub-microsecond counter (4,096 IDs/us before borrowing next us)
pub(crate) fn next_seq_id() -> i64 {
    let raw_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros().min(u64::MAX as u128) as u64)
        .unwrap_or(CUSTOM_EPOCH_MICROS);

    let micros_offset = raw_micros
        .saturating_sub(CUSTOM_EPOCH_MICROS)
        .min(MAX_MICROS_OFFSET as u64) as i64;
    let candidate = (micros_offset << SEQ_BITS).max(1);

    let mut prev = LAST_SEQ_ID.load(Ordering::Relaxed);
    loop {
        let next = if candidate > prev {
            candidate
        } else {
            prev.saturating_add(1)
        };
        match LAST_SEQ_ID.compare_exchange_weak(prev, next, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => prev = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_ids_are_positive_and_strictly_monotonic() {
        let mut prev = 0;
        for _ in 0..10_000 {
            let id = next_seq_id();
            assert!(id > prev, "expected {id} > {prev}");
            prev = id;
        }
    }

    #[test]
    fn now_millis_is_monotonic() {
        let a = now_millis();
        let b = now_millis();
        assert!(b >= a);
    }
}
