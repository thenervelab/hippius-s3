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

use std::time::Instant;

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
}
