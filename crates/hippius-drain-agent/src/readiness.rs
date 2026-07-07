//! The drain-readiness signal (C8).
//!
//! Liveness (the heartbeat's file touch) only proves the process is *alive* — but a drain wedged
//! on a hung `CephFS` write, or one whose breaker is open with pending work, keeps heartbeating
//! while draining nothing, so the pod stays "healthy" while it makes no progress. Readiness closes
//! that gap: a k8s `readinessProbe` reads a file the heartbeat touches ONLY while the drain is
//! actually progressing (or idle), so a wedged node goes `NotReady` — surfacing in pod status and
//! gating a rolling update from marching over stuck nodes.
//!
//! [`ReadinessTracker`] is the pure verdict: it folds the cumulative `drained` counter (monotonic)
//! and the current `backlog` level (undrained bytes) from the agent snapshot, and reports READY
//! unless there is a backlog that has made no drained progress for longer than a stall window. The
//! tracker stays Ready through a legitimately long-but-progressing drain (the counter keeps
//! advancing) and through idle (no backlog), so it does not flap those healthy cases.

use std::time::Duration;
use std::time::Instant;

/// Tracks drain progress across heartbeats to decide k8s readiness. Not `Clock`-injected: the
/// wiring passes a real [`Instant`] (like the liveness file mtime), while [`observe`] takes `now`
/// explicitly so the verdict logic is unit-tested with hand-built instants.
///
/// [`observe`]: ReadinessTracker::observe
#[derive(Debug)]
pub struct ReadinessTracker {
    last_processed: u64,
    last_progress: Instant,
    stall: Duration,
}

impl ReadinessTracker {
    /// A tracker seeded at `now` that reports `NotReady` once a backlog sits `stall`-long without
    /// the drain loop processing a single part.
    #[must_use]
    pub fn new(now: Instant, stall: Duration) -> Self {
        Self {
            last_processed: 0,
            last_progress: now,
            stall,
        }
    }

    /// Folds one heartbeat observation and returns whether the drain is READY.
    ///
    /// READY iff the backlog is empty (idle is healthy) OR the `processed` counter has advanced
    /// within `stall` (the loop is cycling). `NotReady` iff there is a backlog but `processed` has
    /// not advanced for longer than `stall` — the loop is WEDGED (blocked on a hung `CephFS` op).
    ///
    /// `processed` is the cumulative count of claims the drain loop CYCLED — committed
    /// (`drained`) + `failed` + `deferred` + `throttled` — NOT just committed, so a node
    /// legitimately deferring not-ready/orphan backlog, failing fast on a degraded pool, or
    /// backing off every claim under an open breaker (a pool-wide Ceph outage) still reads as
    /// cycling (its loop is alive); only a loop that has stopped handling claims at all reads as
    /// wedged. Including `throttled` is what stops a Ceph outage from flipping the whole
    /// `DaemonSet` `NotReady` at once (which would wedge a rolling update). `backlog` is the
    /// current undrained bytes.
    pub fn observe(&mut self, processed: u64, backlog: u64, now: Instant) -> bool {
        if processed > self.last_processed {
            self.last_processed = processed;
            self.last_progress = now;
        }
        if backlog == 0 {
            // Idle is healthy; refresh the clock so a later backlog gets a FULL stall window
            // before it can read as wedged (rather than inheriting a stale idle gap).
            self.last_progress = now;
            return true;
        }
        now.duration_since(self.last_progress) < self.stall
    }
}

#[cfg(test)]
mod tests {
    use super::ReadinessTracker;
    use std::time::Duration;
    use std::time::Instant;

    const STALL: Duration = Duration::from_mins(1);

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    #[test]
    fn idle_is_always_ready() {
        let t0 = Instant::now();
        let mut r = ReadinessTracker::new(t0, STALL);
        assert!(r.observe(0, 0, t0), "no backlog is ready");
        assert!(r.observe(0, 0, at(t0, 300)), "still idle after a long gap is ready");
    }

    #[test]
    fn a_long_but_progressing_drain_stays_ready() {
        let t0 = Instant::now();
        let mut r = ReadinessTracker::new(t0, STALL);
        // Backlog present the whole time, but `drained` advances each tick — never flap NotReady.
        assert!(r.observe(10, 1_000, at(t0, 30)));
        assert!(r.observe(20, 1_000, at(t0, 90)));
        assert!(r.observe(30, 1_000, at(t0, 150)));
    }

    #[test]
    fn a_stalled_backlog_goes_not_ready_past_the_window() {
        let t0 = Instant::now();
        let mut r = ReadinessTracker::new(t0, STALL);
        assert!(r.observe(10, 1_000, at(t0, 5)), "just progressed -> ready");
        assert!(r.observe(10, 1_000, at(t0, 50)), "no progress but within the window -> still ready");
        assert!(!r.observe(10, 1_000, at(t0, 120)), "no progress past the stall window -> NotReady");
    }

    #[test]
    fn progress_after_a_stall_recovers_readiness() {
        let t0 = Instant::now();
        let mut r = ReadinessTracker::new(t0, STALL);
        assert!(!r.observe(0, 1_000, at(t0, 120)), "backlog with no progress from the start -> NotReady");
        assert!(r.observe(5, 1_000, at(t0, 125)), "the counter advancing again -> ready");
    }

    #[test]
    fn clearing_the_backlog_recovers_readiness() {
        let t0 = Instant::now();
        let mut r = ReadinessTracker::new(t0, STALL);
        assert!(!r.observe(0, 1_000, at(t0, 120)), "wedged with a backlog -> NotReady");
        assert!(r.observe(0, 0, at(t0, 121)), "backlog cleared -> ready");
    }
}
