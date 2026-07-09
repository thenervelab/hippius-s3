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
//! and the current count of undrained replication rows from the agent snapshot, and reports READY
//! unless there is undrained work that has made no drained progress for longer than a stall window.
//! The tracker stays Ready through a legitimately long-but-progressing drain (the counter keeps
//! advancing) and through idle (no undrained rows), so it does not flap those healthy cases.
//!
//! The "is there work" signal is the undrained-row COUNT, not the byte backlog: the byte sum joins
//! `parts` and a missing/NULL-size row contributes zero, so a wedged node's byte-backlog can read 0
//! while rows remain — which would falsely read idle -> Ready, the exact wedge C8 catches (#235 D1).

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
    /// A tracker seeded at `now` that reports `NotReady` once undrained work sits `stall`-long
    /// without the drain loop processing a single part.
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
    /// READY iff there are no undrained rows (idle is healthy) OR the `processed` counter has
    /// advanced within `stall` (the loop is cycling). `NotReady` iff there is undrained work but
    /// `processed` has not advanced for longer than `stall` — the loop is WEDGED (blocked on a hung
    /// `CephFS` op).
    ///
    /// `processed` is the cumulative count of claims the drain loop CYCLED — committed
    /// (`drained`) + `failed` + `deferred` + `throttled` — NOT just committed, so a node
    /// legitimately deferring not-ready/orphan backlog, failing fast on a degraded pool, or
    /// backing off every claim under an open breaker (a pool-wide Ceph outage) still reads as
    /// cycling (its loop is alive); only a loop that has stopped handling claims at all reads as
    /// wedged. Including `throttled` is what stops a Ceph outage from flipping the whole
    /// `DaemonSet` `NotReady` at once (which would wedge a rolling update).
    ///
    /// `undrained` is the COUNT of this node's undrained replication rows — NOT the byte backlog.
    /// The byte sum joins `parts` and a missing/NULL-size row contributes zero, so it can read 0
    /// for a wedged node that still owns undrained rows; keying idle on the row COUNT closes that
    /// false-negative (PR #235 D1).
    pub fn observe(&mut self, processed: u64, undrained: u64, now: Instant) -> bool {
        if processed > self.last_processed {
            self.last_processed = processed;
            self.last_progress = now;
        }
        if undrained == 0 {
            // Idle is healthy; refresh the clock so later undrained work gets a FULL stall window
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
        assert!(r.observe(0, 0, t0), "no undrained rows is ready");
        assert!(r.observe(0, 0, at(t0, 300)), "still idle after a long gap is ready");
    }

    #[test]
    fn a_wedged_node_with_undrained_rows_goes_not_ready_even_at_zero_backlog_bytes() {
        // PR #235 D1: the wiring feeds observe the undrained-row COUNT, not the byte backlog. A
        // wedged node whose parts rows are missing has 0 backlog bytes but a nonzero undrained
        // count; feeding that count, the tracker must NOT read idle -> Ready but must go NotReady
        // once the stall window elapses with no drained progress. (Feeding the 0-byte backlog
        // instead would refresh the idle clock every tick and never flip NotReady — the bug.)
        let t0 = Instant::now();
        let mut r = ReadinessTracker::new(t0, STALL);
        assert!(r.observe(0, 1, at(t0, 5)), "one undrained row, fresh window -> still ready");
        assert!(
            !r.observe(0, 1, at(t0, 120)),
            "one undrained row, no progress past the window -> NotReady"
        );
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
