//! The agent's observability snapshot seam.
//!
//! The runtime workers accumulate counters into a [`SnapshotCell`]; the
//! observability layer (M9, feature `otel`) reads an [`AgentSnapshot`] from gauge
//! callbacks and diffs successive reads into rates. The types live here in
//! `hippius-drain-core` — not in `hippius-drain-agent` — precisely so the future read-side,
//! which `hippius-drain-core` cannot depend on the agent for, can reference them.
//!
//! The counters are independent `AtomicU64`s, one per metric, written by disjoint
//! workers (drain writes `drained`/`failed`, the reconciler writes
//! `reconciler_recovered`). That is why a whole-snapshot swap would be wrong here:
//! two workers updating different fields through a load-modify-store would clobber
//! each other. Per-counter atomics compose without coordination — `Relaxed` is
//! correct because each counter is monotonic with no cross-counter ordering
//! dependency (Rust Axiom 92 / interior-mutability selection 73).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

/// How many recent drain latencies the window keeps for the p99 estimate.
const LATENCY_WINDOW: usize = 512;

/// A bounded ring of recent drain latencies, for a windowed p99 estimate.
///
/// The window is pre-sized so `record` (on the hot drain path) is O(1) with no
/// reallocation spike (axiom `rust_quality_126`); the O(n log n) sort lives in
/// `p99`, called only on the cold heartbeat tick. `Duration` is `Ord` (no NaN),
/// so the sort needs no float-comparison policy.
#[derive(Debug)]
struct LatencyWindow {
    samples: VecDeque<Duration>,
}

impl Default for LatencyWindow {
    fn default() -> Self {
        Self {
            samples: VecDeque::with_capacity(LATENCY_WINDOW),
        }
    }
}

impl LatencyWindow {
    /// Records `latency`, evicting the oldest sample once the window is full so
    /// the estimate tracks recent behaviour and the memory stays bounded.
    fn record(&mut self, latency: Duration) {
        if self.samples.len() == LATENCY_WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(latency);
    }

    /// The 99th-percentile latency over the window by nearest-rank, or
    /// [`Duration::ZERO`] when no drains have been sampled.
    fn p99(&self) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted: Vec<Duration> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        // Nearest-rank: rank = ceil(0.99 * n) in 1..=n, in integer math to avoid
        // float casts; the index is rank-1, clamped into the slice. `n <= 512`,
        // so `99 * n` cannot overflow.
        let rank = (99 * sorted.len()).div_ceil(100);
        let index = rank.saturating_sub(1).min(sorted.len() - 1);
        sorted[index]
    }
}

/// A point-in-time view of the agent's drain activity, for metrics.
///
/// Plain monotonic counters the runtime accumulates; the observability layer
/// diffs successive snapshots into rates. `Copy` so a read is a cheap value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentSnapshot {
    /// Chunks drained to the pool and committed `Replicated`.
    pub drained: u64,
    /// Failed chunk-drain attempts — one per chunk whose Ceph copy/verify/commit
    /// errored. Counted per chunk (in `drain_next`), so this shares a unit with
    /// `drained` and the two form a meaningful failure rate (see
    /// [`error_bps`](Self::error_bps)). A post-write enqueue deferral is NOT a
    /// failure — it goes to [`deferred`](Self::deferred) instead.
    pub failed: u64,
    /// Parts whose Ceph write succeeded but whose post-write upload enqueue deferred
    /// (the object's address is not finalized yet, or Redis is unreachable). Counted
    /// separately from `failed` and deliberately kept OUT of `error_bps`, so the
    /// Ceph-write failure rate is not polluted by non-Ceph deferrals.
    pub deferred: u64,
    /// Chunks the reconciler recovered after a dropped `chunk_landed` trigger.
    pub reconciler_recovered: u64,
}

impl AgentSnapshot {
    /// The drain error rate in basis points (`0..=10000`): failed chunk-drain
    /// attempts over all attempts. Zero when nothing has been attempted, so a
    /// fresh node reports a clean signal rather than a divide-by-zero.
    #[must_use]
    pub fn error_bps(&self) -> u16 {
        let total = self.drained.saturating_add(self.failed);
        if total == 0 {
            return 0;
        }
        // u128 intermediate so `failed * 10_000` cannot overflow: a u64
        // `saturating_mul` would silently undercount past ~1.8e15 failures. With
        // `failed <= total`, the ratio is in `0..=10000`, so the `try_from`
        // genuinely cannot fail — the fallback only keeps the function total.
        let bps = u128::from(self.failed) * 10_000 / u128::from(total);
        u16::try_from(bps).unwrap_or(10_000)
    }
}

/// Lock-free counters holding the agent's running drain totals.
///
/// Reads (`load`) are wait-free, so a metric scrape never blocks the runtime —
/// the reason for atomics over a `Mutex`. A `load` reads each counter
/// independently, so a concurrent write may leave the view marginally skewed
/// across counters; acceptable for monotonic metrics diffed into rates.
#[derive(Debug, Default)]
pub struct SnapshotCell {
    drained: AtomicU64,
    failed: AtomicU64,
    deferred: AtomicU64,
    reconciler_recovered: AtomicU64,
    /// Recent drain latencies, behind a `Mutex` because a percentile needs the
    /// whole window (no single atomic suffices). Off the wait-free `load` path.
    latency: Mutex<LatencyWindow>,
}

impl SnapshotCell {
    /// A cell with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `n` to the drained total.
    pub fn record_drained(&self, n: u64) {
        self.drained.fetch_add(n, Ordering::Relaxed);
    }

    /// Records `n` failed chunk-drain attempts.
    pub fn record_failed(&self, n: u64) {
        self.failed.fetch_add(n, Ordering::Relaxed);
    }

    /// Records `n` parts whose drain deferred at the post-write enqueue (not a
    /// Ceph-write failure — see [`AgentSnapshot::deferred`]).
    pub fn record_deferred(&self, n: u64) {
        self.deferred.fetch_add(n, Ordering::Relaxed);
    }

    /// Adds `n` to the reconciler-recovered total.
    pub fn record_reconciled(&self, n: u64) {
        self.reconciler_recovered.fetch_add(n, Ordering::Relaxed);
    }

    /// Records a completed drain's latency for the windowed p99 estimate.
    ///
    /// The lock is held only for an O(1) ring push and is never carried across an
    /// `.await` (this method is synchronous), so a poisoned lock recovers via
    /// `into_inner` — the window is left consistent.
    pub fn record_latency(&self, latency: Duration) {
        self.latency.lock().unwrap_or_else(PoisonError::into_inner).record(latency);
    }

    /// The 99th-percentile drain latency over the recent window (the heartbeat's
    /// Ceph-write saturation signal), or [`Duration::ZERO`] when none sampled.
    #[must_use]
    pub fn p99(&self) -> Duration {
        self.latency.lock().unwrap_or_else(PoisonError::into_inner).p99()
    }

    /// Reads the current counters into a snapshot. Wait-free; any thread.
    #[must_use]
    pub fn load(&self) -> AgentSnapshot {
        AgentSnapshot {
            drained: self.drained.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            deferred: self.deferred.load(Ordering::Relaxed),
            reconciler_recovered: self.reconciler_recovered.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{AgentSnapshot, SnapshotCell};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn a_new_cell_reads_the_zero_snapshot() {
        assert_eq!(SnapshotCell::new().load(), AgentSnapshot::default());
    }

    #[test]
    fn p99_is_zero_with_no_samples() {
        assert_eq!(SnapshotCell::new().p99(), Duration::ZERO);
    }

    #[test]
    fn p99_is_the_nearest_rank_latency_of_the_window() {
        let cell = SnapshotCell::new();
        for ms in 1..=100 {
            cell.record_latency(Duration::from_millis(ms));
        }
        // 100 samples (1..=100ms): nearest-rank p99 is the 99th value = 99ms.
        assert_eq!(cell.p99(), Duration::from_millis(99));
    }

    #[test]
    fn records_accumulate_per_counter() {
        let cell = SnapshotCell::new();
        cell.record_drained(3);
        cell.record_drained(2);
        cell.record_failed(1);
        cell.record_reconciled(4);
        assert_eq!(
            cell.load(),
            AgentSnapshot {
                drained: 5,
                failed: 1,
                deferred: 0,
                reconciler_recovered: 4,
            },
        );
    }

    #[test]
    fn deferred_records_accumulate_without_polluting_error_bps() {
        // P1a: a benign deferral (enqueue not-ready / Redis down) is counted for
        // visibility but is NOT a Ceph-write failure, so it must stay out of error_bps
        // — otherwise the p99/error saturation signal is polluted by non-Ceph events.
        let cell = SnapshotCell::new();
        cell.record_drained(7);
        cell.record_failed(3);
        cell.record_deferred(50);
        let snap = cell.load();
        assert_eq!(snap.deferred, 50, "deferrals are counted for visibility");
        assert_eq!(snap.error_bps(), 3000, "deferred attempts stay out of the Ceph failure rate");
    }

    #[test]
    fn error_bps_is_zero_with_no_attempts() {
        assert_eq!(AgentSnapshot::default().error_bps(), 0);
    }

    #[test]
    fn error_bps_is_all_failures_without_overflow() {
        // Every attempt failed -> 10000 bps. The old u64 `saturating_mul` path
        // saturated `failed * 10_000` and silently undercounted this to ~1 bps; the
        // u128 intermediate is exact even at the u64 ceiling (audit #28).
        let snapshot = AgentSnapshot {
            drained: 0,
            failed: u64::MAX,
            deferred: 0,
            reconciler_recovered: 0,
        };
        assert_eq!(snapshot.error_bps(), 10_000);
    }

    #[test]
    fn error_bps_is_the_failed_fraction_in_basis_points() {
        let snapshot = AgentSnapshot {
            drained: 7,
            failed: 3,
            deferred: 0,
            reconciler_recovered: 0,
        };
        // 3 failed attempts of 10 total attempts = 30%, i.e. 3000 basis points.
        assert_eq!(snapshot.error_bps(), 3000);
    }

    #[test]
    fn concurrent_records_lose_no_updates() {
        // The clobber a whole-snapshot swap would suffer: 8 threads each add 1000
        // to `drained`; lock-free atomics must total exactly 8000.
        let cell = Arc::new(SnapshotCell::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let cell = Arc::clone(&cell);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    cell.record_drained(1);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(cell.load().drained, 8000);
    }
}
