//! The injected time source for cephor's time-driven control state.
//!
//! The agent's allocation-pull loop reads [`Clock::now`] for its decay timing, so
//! a [`TestClock`] can drive that decay deterministically in tests. The
//! time-dependent *logic* — the circuit breaker, the token bucket, the decay
//! computation — is kept pure by taking the instant as an explicit parameter
//! rather than reading a clock, so those are unit-tested with hand-built instants;
//! this trait is only the *source* that produces the instants at the top of the
//! loop. Latency stopwatches and one-shot construction stamps read the monotonic
//! clock directly (they measure or mark, they do not gate a control decision).
//! Defined once here so the workspace shares one trait instead of several copies.

use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// A monotonic source of the current instant.
///
/// `Send + Sync` because clocks are shared across the agent's worker tasks;
/// `Debug` so a holder (e.g. the agent runtime) stays `Debug`-derivable behind an
/// `Arc<dyn Clock>`. Both built-in clocks already derive `Debug`.
pub trait Clock: Send + Sync + core::fmt::Debug {
    /// The current monotonic instant.
    fn now(&self) -> Instant;
}

/// The production clock: reads the OS monotonic timer.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// The largest fraction (permille of `base`) the fleet jitter may shave off a periodic sleep.
const JITTER_PERMILLE: u64 = 200;

/// Scale `base` by `permille`/1000. Pure — the entropy is split out of [`jittered`] so the
/// scaling is unit-testable without touching a clock.
fn scale(base: Duration, permille: u64) -> Duration {
    let nanos = base.as_nanos().saturating_mul(u128::from(permille)) / 1000;
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// Map raw entropy to a scale factor in `[1000 - JITTER_PERMILLE, 1000]` permille.
fn jitter_factor_permille(entropy: u64) -> u64 {
    1000 - entropy % (JITTER_PERMILLE + 1)
}

/// `base` shortened by a pseudo-random 0..20%, to decorrelate the fleet's periodic pollers so
/// a recovering shared dependency (redis-queues / Postgres / `CephFS`) is not hit by every
/// node's loop in lockstep — the C7 thundering herd. **Down-only by design:** it never returns
/// a duration LONGER than `base`, so it can never stretch a heartbeat/lease TTL. Deliberately
/// NOT "full jitter" `[0, base]`: a near-zero periodic sleep would busy-poll. Entropy is the
/// wall clock's sub-second nanos — ample for phase decorrelation, never security-facing.
#[must_use]
pub fn jittered(base: Duration) -> Duration {
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| u64::from(elapsed.subsec_nanos()));
    scale(base, jitter_factor_permille(entropy))
}

/// A manually-advanced clock for deterministic tests.
///
/// Available in-crate under `cfg(test)` and to downstream test code via the
/// `test-util` feature. Stores nanoseconds elapsed from a fixed base in an atomic
/// so it stays `Sync` without a lock; only the elapsed offset matters, never the
/// absolute base.
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug)]
pub struct TestClock {
    base: Instant,
    elapsed_nanos: std::sync::atomic::AtomicU64,
}

#[cfg(any(test, feature = "test-util"))]
impl TestClock {
    /// Creates a clock reading its base as "now".
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            elapsed_nanos: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Advances the clock by `delta`.
    pub fn advance(&self, delta: std::time::Duration) {
        let nanos = u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX);
        self.elapsed_nanos.fetch_add(nanos, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        let elapsed = self.elapsed_nanos.load(std::sync::atomic::Ordering::Relaxed);
        self.base + std::time::Duration::from_nanos(elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::{Clock, SystemClock, TestClock};
    use std::time::Duration;

    #[test]
    fn test_clock_advances_by_the_requested_delta() {
        let clock = TestClock::new();
        let start = clock.now();
        clock.advance(Duration::from_secs(5));
        assert_eq!(clock.now().duration_since(start), Duration::from_secs(5));
    }

    #[test]
    fn test_clock_is_frozen_until_advanced() {
        let clock = TestClock::new();
        let first = clock.now();
        assert_eq!(clock.now(), first);
    }

    #[test]
    fn system_clock_is_monotonic() {
        let clock = SystemClock;
        let first = clock.now();
        assert!(clock.now() >= first);
    }

    #[test]
    fn jitter_factor_stays_in_the_down_only_window() {
        for entropy in [0u64, 1, 199, 200, 201, 999, 1_000_000_000, u64::MAX] {
            let factor = super::jitter_factor_permille(entropy);
            assert!(
                (1000 - super::JITTER_PERMILLE..=1000).contains(&factor),
                "factor {factor} out of window for entropy {entropy}",
            );
        }
    }

    #[test]
    fn scale_shortens_but_never_lengthens() {
        let base = Duration::from_secs(10);
        assert_eq!(super::scale(base, 1000), base, "1000 permille is identity");
        assert_eq!(super::scale(base, 800), Duration::from_secs(8));
    }

    #[test]
    fn jittered_never_exceeds_base_and_stays_within_the_window() {
        let base = Duration::from_secs(5);
        let floor = base * (1000 - u32::try_from(super::JITTER_PERMILLE).unwrap()) / 1000;
        for _ in 0..1000 {
            let j = super::jittered(base);
            assert!(j <= base && j >= floor, "jittered {j:?} outside [{floor:?}, {base:?}]");
        }
    }
}
