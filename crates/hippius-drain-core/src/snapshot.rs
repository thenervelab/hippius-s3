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
    /// Parts visited by the SSD walks (reconciler + reclaimer), summed across passes.
    ///
    /// The signal that F1 has returned. The walks are O(everything on disk), and retention took
    /// that from "the undrained backlog" to "this node's entire replicated shard" — 2.28 M parts
    /// on prod — without anything reporting the change. A rate derived from this against the
    /// poll interval is the walk's real cost per second.
    pub scan_parts_total: u64,
    /// How long the last SSD walk took, in milliseconds. Watched against the poll interval: a
    /// scan that approaches its own period means the worker is walking continuously.
    pub scan_duration_ms: u64,
    /// Parts recorded from the api's landed-part announcements — the fast discovery path.
    /// Read together with `reconciler_recovered`: this climbing while that stays near zero is
    /// what says the announcement path is carrying discovery. Both near zero means ingest
    /// stopped, not that the fast path is working.
    pub landed_recorded: u64,
    /// Announcements dropped as unparseable. Must stay at zero; nonzero means the api and the
    /// agent disagree about the wire contract, in which case every message is being lost and
    /// discovery has silently fallen back to the reconciler's walk.
    pub landed_dropped: u64,
    /// `failed` (broken/abandoned-upload) SSD parts the reclaim worker unlinked — the
    /// SSD-ingest tier's eviction throughput, distinct from the drain's `CephFS` work.
    pub reclaimed: u64,
    /// Reclaim cycles that aborted on an object-backing read error (`ReclaimError::Backing`):
    /// the servability/orphan gate could not read `object_versions` (a missing table on some
    /// deploy, or a transient PG error). A monotonic COUNT of aborted cycles — each removes
    /// NOTHING (fail-safe), so a sustained rise means the `failed`-part SSD GC is silently
    /// DISABLED and debris accrues, invisible outside logs. Deliberately kept OUT of
    /// [`error_bps`](Self::error_bps): a reclaim stall is not a Ceph-write failure, exactly
    /// like `deferred`/`throttled`.
    pub reclaim_backing_errors: u64,
    /// Claims handed back un-drained because the breaker/throttle `Denied` the tick (the
    /// pool is unhealthy or the write budget is spent). NOT a drain outcome — no part moved
    /// — but the loop DID cycle, so the readiness tracker folds it into `processed`: a
    /// pool-wide Ceph outage trips the breaker on every claim, and without this the agent
    /// would record zero progress and flip `NotReady` even though it is healthily backing off
    /// (a wedge, not an outage, is what readiness must catch). Kept out of `error_bps` (a
    /// throttle is not a Ceph-write failure), exactly like `deferred`.
    pub throttled: u64,
    /// Parts written off as terminal `failed` by the missing-source escalation: the SSD
    /// source was observed gone enough consecutive claims that the row will never drain.
    /// This is the only standing metric of a write-off — the counter is per-process (reset
    /// on restart), `node_undrained_count` excludes `failed`, and the terminal GC deletes
    /// the row, so the WARN log in Loki is the only per-event trace.
    /// Kept OUT of [`error_bps`](Self::error_bps) like `deferred`: a write-off is a data
    /// disposition, not a Ceph-write failure.
    pub written_off: u64,
    /// The subset of [`written_off`](Self::written_off) whose `object_versions` row was
    /// still SERVABLE (or whose servability could not be determined) at write-off time:
    /// acknowledged client data the drain just declared undrainable — a durability
    /// emergency, unlike the routine abandoned-upload write-off. The 2026-07-22/26
    /// incidents wrote off 47 servable versions with only WARN logs; this counter is the
    /// page (`increase(drain_parts_written_off_servable_total[1h]) > 0`). The caller
    /// records the total alongside, so `written_off_servable <= written_off` always.
    /// Out of [`error_bps`](Self::error_bps) for the same reason as the total.
    pub written_off_servable: u64,
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
    scan_parts_total: AtomicU64,
    scan_duration_ms: AtomicU64,
    landed_recorded: AtomicU64,
    landed_dropped: AtomicU64,
    reclaimed: AtomicU64,
    /// Resident parts the read-tier evictor unlinked, and the bytes they freed. Separate from
    /// `reclaimed` (debris the reclaim worker removed) because the two answer different
    /// questions: reclaim rising means junk is accumulating, eviction rising means the cache
    /// is under space pressure and is giving up warm data.
    evicted: AtomicU64,
    evicted_bytes: AtomicU64,
    /// Worklist entries the evictor REFUSED because they were not `replicated`. The durability
    /// invariant, and the reason it is a counter rather than a log line: "has this ever been
    /// non-zero" has to be answerable by an alert rule, not by grepping.
    evict_blocked_unreplicated: AtomicU64,
    /// Aborted-reclaim counter mirroring `reclaimed`: bumped once per reclaim cycle that failed
    /// its object-backing read (`ReclaimError::Backing`). Monotonic, `Relaxed` — a stat counter
    /// with no cross-counter ordering dependency (axiom `rust_quality_92`). See
    /// [`AgentSnapshot::reclaim_backing_errors`] for why it stays out of `error_bps`.
    reclaim_backing_errors: AtomicU64,
    throttled: AtomicU64,
    written_off: AtomicU64,
    written_off_servable: AtomicU64,
    /// Current SSD backlog (undrained bytes) — a LEVEL, not a monotonic counter, so it
    /// has its own atomic (set, not accumulated) rather than living in [`AgentSnapshot`].
    /// The heartbeat worker writes it (from `Store::node_backlog_bytes`) each tick; the
    /// metrics layer reads it as a gauge. Kept off the wait-free `load` path so a scrape
    /// never blocks.
    backlog_bytes: AtomicU64,
    /// Bytes this node holds RESIDENT on SSD to serve reads (`Store::node_cache_bytes`). A
    /// gauge on the same tick as the backlog. Distinct from it precisely because the
    /// allocator must not read warm cache as drain demand.
    cache_bytes: AtomicU64,
    /// Free bytes on the ingest SSD. The third leg of backlog/cache/free: without it a
    /// dashboard cannot tell "cache grew" from "the disk filled".
    free_bytes: AtomicU64,
    /// The allocator's published free-space floor for this node, in permille of disk, plus
    /// one so that 0 can mean "nothing published". A bare 0 would be ambiguous with a
    /// legitimate reserve of 0 (the eviction kill-switch), and reading that as "no floor" on
    /// a node whose allocator simply had not written yet would stop it evicting entirely.
    allocated_reserve_permille_plus_one: AtomicU64,
    /// Count of this node's undrained replication rows (`pending` + `draining`) — a LEVEL like
    /// `backlog_bytes`, set each heartbeat from `Store::node_undrained_count`. This is the C8
    /// wedge signal, kept SEPARATE from `backlog_bytes` on purpose: the byte sum joins `parts`
    /// and a missing/NULL-size row contributes zero, so a wedged node's byte-backlog can read 0
    /// while undrained rows remain — which readiness would misread as idle. The COUNT cannot be
    /// zeroed that way, so readiness keys on it while the gauge keeps reporting bytes.
    undrained_count: AtomicU64,
    /// Age in whole seconds of this node's oldest `pending` replication row — a LEVEL like
    /// `undrained_count`, set each heartbeat from `Store::node_oldest_pending_age_secs`. The
    /// starvation signal the 2026-07-26 incident lacked: one node's oldest pending age
    /// exploding while peers sit near zero means claimable work nobody is finishing.
    oldest_pending_age_secs: AtomicU64,
    /// Current SSD disk saturation in basis points (`0..=10000` = `0.0..=1.0` full) — a
    /// LEVEL like `backlog_bytes`, set each heartbeat from the same `statvfs` probe. This
    /// is the fill fraction that 503s every PUT once it crosses the api's cutoff, so it is
    /// the operational alert signal; distinct from `backlog_bytes` (undrained WORK), since
    /// a leak can fill the disk without any drain demand. bps not f64 so the gauge stays a
    /// plain atomic; the metrics layer scales it to a 0..1 fraction.
    disk_pressure_bps: AtomicU64,
    /// Count of parts currently held `corrupt` on this node — a LEVEL, set each cycle by the
    /// re-drive pass from `Store::count_corrupt_parts`. A nonzero value is a live object whose
    /// pool copy is corrupt, kept alive only by its SSD source: a durability incident (R4), so
    /// it is exported as the `drain_corrupt_parts` gauge and alerted, distinct from the drain's
    /// routine failure counters.
    corrupt_parts: AtomicU64,
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

    /// Records one SSD walk: how many parts it visited and how long it took.
    ///
    /// Called by every worker that walks the cache, so the counter is the fleet's total walk
    /// cost rather than any one worker's. Both halves matter: the count says how big the tree
    /// got, the duration says whether the walk still fits inside its own poll interval.
    pub fn record_scan(&self, parts: u64, duration: Duration) {
        self.scan_parts_total.fetch_add(parts, Ordering::Relaxed);
        self.scan_duration_ms
            .store(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Records one landed-announcement batch: `recorded` parts written, `dropped` unparseable.
    pub fn record_landed(&self, recorded: u64, dropped: u64) {
        self.landed_recorded.fetch_add(recorded, Ordering::Relaxed);
        self.landed_dropped.fetch_add(dropped, Ordering::Relaxed);
    }

    /// Adds `n` to the SSD-reclaim total (terminal parts the reclaim worker unlinked).
    pub fn record_reclaimed(&self, n: u64) {
        self.reclaimed.fetch_add(n, Ordering::Relaxed);
    }

    /// Records one reclaim cycle aborted on an object-backing read error — a `failed`-part GC
    /// pass that read nothing and removed nothing (the `drain_reclaim_backing_errors_total`
    /// counter). One abort per cycle, so it bumps by one, not by a caller-supplied count.
    pub fn record_reclaim_backing_error(&self) {
        self.reclaim_backing_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records `n` claims handed back because the breaker/throttle denied the tick. Counts
    /// as liveness progress (the loop cycled) but is not a drain outcome — see
    /// [`AgentSnapshot::throttled`].
    pub fn record_throttled(&self, n: u64) {
        self.throttled.fetch_add(n, Ordering::Relaxed);
    }

    /// Adds one eviction pass's outcome to the read-tier eviction totals.
    pub fn record_evicted(&self, parts: u64, bytes: u64) {
        self.evicted.fetch_add(parts, Ordering::Relaxed);
        self.evicted_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The cumulative count of parts the evictor unlinked (`drain_ssd_evicted_total`).
    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }

    /// The cumulative bytes the evictor freed (`drain_ssd_evicted_bytes_total`).
    #[must_use]
    pub fn evicted_bytes(&self) -> u64 {
        self.evicted_bytes.load(Ordering::Relaxed)
    }

    /// Records eviction candidates refused for not being `replicated`. Must stay at zero.
    pub fn record_evict_blocked_unreplicated(&self, n: u64) {
        self.evict_blocked_unreplicated.fetch_add(n, Ordering::Relaxed);
    }

    /// Cumulative refused-candidate count (`drain_ssd_evict_blocked_unreplicated_total`).
    #[must_use]
    pub fn evict_blocked_unreplicated(&self) -> u64 {
        self.evict_blocked_unreplicated.load(Ordering::Relaxed)
    }

    /// Records free bytes on the ingest SSD. A gauge: `store`, not add.
    pub fn record_free_bytes(&self, bytes: u64) {
        self.free_bytes.store(bytes, Ordering::Relaxed);
    }

    /// The last-recorded free space (`drain_ssd_free_bytes`).
    #[must_use]
    pub fn free_bytes(&self) -> u64 {
        self.free_bytes.load(Ordering::Relaxed)
    }

    /// Records the bytes currently resident on SSD as read cache. A gauge: `store`, not add.
    pub fn record_cache_bytes(&self, bytes: u64) {
        self.cache_bytes.store(bytes, Ordering::Relaxed);
    }

    /// The last-recorded resident cache size (the `drain_ssd_cache_bytes` gauge).
    #[must_use]
    pub fn cache_bytes(&self) -> u64 {
        self.cache_bytes.load(Ordering::Relaxed)
    }

    /// Records the allocator's published free-space floor for this node (permille of disk).
    pub fn record_allocated_reserve_permille(&self, permille: u16) {
        self.allocated_reserve_permille_plus_one.store(u64::from(permille) + 1, Ordering::Relaxed);
    }

    /// Clears the published floor, so the evictor falls back to its configured one. Used when
    /// the allocation key expires or the leader predates per-node reserves.
    pub fn clear_allocated_reserve_permille(&self) {
        self.allocated_reserve_permille_plus_one.store(0, Ordering::Relaxed);
    }

    /// The allocator's published floor, or `None` when nothing has been published.
    #[must_use]
    pub fn allocated_reserve_permille(&self) -> Option<u16> {
        match self.allocated_reserve_permille_plus_one.load(Ordering::Relaxed) {
            0 => None,
            raw => u16::try_from(raw - 1).ok(),
        }
    }

    /// Records the current SSD backlog (undrained bytes). A gauge: `store`, not add.
    pub fn record_backlog(&self, bytes: u64) {
        self.backlog_bytes.store(bytes, Ordering::Relaxed);
    }

    /// The last-recorded SSD backlog in bytes (the metrics `drain_ssd_backlog_bytes` gauge).
    #[must_use]
    pub fn backlog(&self) -> u64 {
        self.backlog_bytes.load(Ordering::Relaxed)
    }

    /// Records the current count of undrained replication rows (`pending` + `draining`). A gauge:
    /// `store`, not add. The heartbeat writes it from `Store::node_undrained_count` each tick.
    pub fn record_undrained_count(&self, count: u64) {
        self.undrained_count.store(count, Ordering::Relaxed);
    }

    /// The last-recorded count of undrained replication rows — the C8 readiness wedge signal
    /// (nonzero means real drain work remains, even when [`backlog`](Self::backlog) reads 0).
    #[must_use]
    pub fn undrained_count(&self) -> u64 {
        self.undrained_count.load(Ordering::Relaxed)
    }

    /// Records the current age (whole seconds) of this node's oldest `pending` replication
    /// row. A gauge: `store`, not add. The heartbeat writes it from
    /// `Store::node_oldest_pending_age_secs` each tick.
    pub fn record_oldest_pending_age_secs(&self, secs: u64) {
        self.oldest_pending_age_secs.store(secs, Ordering::Relaxed);
    }

    /// The last-recorded oldest-pending age in seconds (the `drain_pending_oldest_age_seconds`
    /// gauge source) — the per-node starvation signal (see the field doc).
    #[must_use]
    pub fn oldest_pending_age_secs(&self) -> u64 {
        self.oldest_pending_age_secs.load(Ordering::Relaxed)
    }

    /// Records `n` parts written off as terminal `failed` by the missing-source escalation
    /// (not a Ceph-write failure — see [`AgentSnapshot::written_off`]).
    pub fn record_written_off(&self, n: u64) {
        self.written_off.fetch_add(n, Ordering::Relaxed);
    }

    /// Records `n` write-offs of parts whose version was still servable (or of unknown
    /// servability) — the page-worthy subset. Callers record the total alongside so
    /// `written_off_servable <= written_off` holds (see
    /// [`AgentSnapshot::written_off_servable`]).
    pub fn record_written_off_servable(&self, n: u64) {
        self.written_off_servable.fetch_add(n, Ordering::Relaxed);
    }

    /// Records the current SSD disk saturation in basis points (`0..=10000`). A gauge:
    /// `store`, not add. The heartbeat writes it from the `statvfs` pressure each tick.
    pub fn record_disk_pressure(&self, bps: u16) {
        self.disk_pressure_bps.store(u64::from(bps), Ordering::Relaxed);
    }

    /// The last-recorded SSD disk saturation in basis points (the `drain_ssd_pressure`
    /// gauge source). Clamped to `10000` on read so the value honors the `bps ∈ [0, 10000]`
    /// contract even if a wider one was ever stored.
    #[must_use]
    pub fn disk_pressure_bps(&self) -> u16 {
        // Clamp on read: the setter widens u16 -> u64, so a stored bps in (10000, 65535]
        // would otherwise round-trip un-clamped and break the [0, 10000] contract. The
        // `.min` makes the value always fit u16, so the `try_from` fallback never fires.
        u16::try_from(self.disk_pressure_bps.load(Ordering::Relaxed).min(10_000)).unwrap_or(10_000)
    }

    /// Records the current count of parts held `corrupt` on this node. A gauge: `store`, not
    /// add. The re-drive pass writes it each cycle from `Store::count_corrupt_parts`.
    pub fn record_corrupt(&self, count: u64) {
        self.corrupt_parts.store(count, Ordering::Relaxed);
    }

    /// The last-recorded count of `corrupt`-held parts (the `drain_corrupt_parts` gauge source).
    #[must_use]
    pub fn corrupt_parts(&self) -> u64 {
        self.corrupt_parts.load(Ordering::Relaxed)
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
            scan_parts_total: self.scan_parts_total.load(Ordering::Relaxed),
            scan_duration_ms: self.scan_duration_ms.load(Ordering::Relaxed),
            landed_recorded: self.landed_recorded.load(Ordering::Relaxed),
            landed_dropped: self.landed_dropped.load(Ordering::Relaxed),
            reclaimed: self.reclaimed.load(Ordering::Relaxed),
            reclaim_backing_errors: self.reclaim_backing_errors.load(Ordering::Relaxed),
            throttled: self.throttled.load(Ordering::Relaxed),
            written_off: self.written_off.load(Ordering::Relaxed),
            written_off_servable: self.written_off_servable.load(Ordering::Relaxed),
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
    fn backlog_is_a_settable_gauge_not_a_counter() {
        let cell = SnapshotCell::new();
        assert_eq!(cell.backlog(), 0, "a fresh cell reports no backlog");
        cell.record_backlog(4096);
        assert_eq!(cell.backlog(), 4096);
        cell.record_backlog(100);
        assert_eq!(cell.backlog(), 100, "backlog is a level: a later record replaces, not accumulates");
    }

    #[test]
    fn undrained_count_is_a_settable_gauge_not_a_counter() {
        // The C8 wedge signal (PR #235 D1): a LEVEL sourced from a COUNT of undrained
        // replication rows, so a later record replaces rather than accumulates — like backlog.
        let cell = SnapshotCell::new();
        assert_eq!(cell.undrained_count(), 0, "a fresh cell reports no undrained rows");
        cell.record_undrained_count(4);
        assert_eq!(cell.undrained_count(), 4);
        cell.record_undrained_count(1);
        assert_eq!(cell.undrained_count(), 1, "undrained count is a level: a later record replaces");
    }

    #[test]
    fn disk_pressure_is_a_settable_gauge_in_basis_points() {
        let cell = SnapshotCell::new();
        assert_eq!(cell.disk_pressure_bps(), 0, "a fresh cell reports no disk pressure");
        cell.record_disk_pressure(8500); // 85% full
        assert_eq!(cell.disk_pressure_bps(), 8500);
        cell.record_disk_pressure(200);
        assert_eq!(cell.disk_pressure_bps(), 200, "disk pressure is a level: a later record replaces");
        cell.record_disk_pressure(12_000); // a u16 wider than the bps ceiling
        assert_eq!(
            cell.disk_pressure_bps(),
            10_000,
            "a stored bps above 10000 reads back clamped to the ceiling"
        );
    }

    #[test]
    fn corrupt_parts_is_a_settable_gauge_not_a_counter() {
        // D5: the R4 standing durability signal (the drain_corrupt_parts alert source). A LEVEL
        // set each cycle from Store::count_corrupt_parts, so a later record REPLACES rather than
        // accumulates — the page fires on this gauge staying nonzero ACROSS cycles (only at-cap
        // unrecoverable parts sustain it), NOT on a single reclaim cycle's held count, which is
        // logged at WARN and self-heals on the next re-drive.
        let cell = SnapshotCell::new();
        assert_eq!(cell.corrupt_parts(), 0, "a fresh cell reports no corrupt parts");
        cell.record_corrupt(3);
        assert_eq!(cell.corrupt_parts(), 3);
        cell.record_corrupt(1);
        assert_eq!(
            cell.corrupt_parts(),
            1,
            "corrupt parts is a level: a later record replaces, not accumulates"
        );
    }

    #[test]
    fn oldest_pending_age_is_a_settable_gauge_not_a_counter() {
        // Task F starvation signal: the age of this node's oldest `pending` row, a LEVEL
        // set each heartbeat from Store::node_oldest_pending_age_secs — so a later record
        // replaces rather than accumulates, exactly like undrained_count.
        let cell = SnapshotCell::new();
        assert_eq!(cell.oldest_pending_age_secs(), 0, "a fresh cell reports no pending age");
        cell.record_oldest_pending_age_secs(12_600); // the 2026-07-26 3.5h wall
        assert_eq!(cell.oldest_pending_age_secs(), 12_600);
        cell.record_oldest_pending_age_secs(10);
        assert_eq!(cell.oldest_pending_age_secs(), 10, "age is a level: a later record replaces");
    }

    #[test]
    fn written_off_records_accumulate_without_polluting_error_bps() {
        // Task F durable write-off signal: a terminal missing-source escalation
        // (status='failed', GC'd later) previously left only a WARN log. The counter is
        // monotonic like `deferred` and — like every non-Ceph outcome — must stay out
        // of error_bps: a write-off is not a Ceph-write failure.
        let cell = SnapshotCell::new();
        cell.record_drained(7);
        cell.record_failed(3);
        cell.record_written_off(1);
        cell.record_written_off(1);
        let snap = cell.load();
        assert_eq!(snap.written_off, 2, "each write-off is counted durably");
        assert_eq!(snap.error_bps(), 3000, "write-offs stay out of the Ceph failure rate");
    }

    #[test]
    fn servable_write_offs_count_in_both_counters_without_polluting_error_bps() {
        // Task 4 (2026-07-22/26 incidents): a write-off of a SERVABLE version is
        // acknowledged-data loss and needs its own alertable counter, while the total
        // stays the total — the caller records BOTH, so servable ≤ total always holds
        // and `drain_parts_written_off_total` keeps meaning every write-off. Like
        // `written_off`, the servable counter is a data disposition, not a Ceph-write
        // failure, so it stays out of error_bps.
        let cell = SnapshotCell::new();
        cell.record_drained(7);
        cell.record_failed(3);
        cell.record_written_off(1);
        cell.record_written_off_servable(1);
        cell.record_written_off(1);
        let snap = cell.load();
        assert_eq!(snap.written_off, 2, "the total counts every write-off, servable or not");
        assert_eq!(snap.written_off_servable, 1, "only the servable write-off pages");
        assert_eq!(snap.error_bps(), 3000, "servable write-offs stay out of the Ceph failure rate");
    }

    #[test]
    fn records_accumulate_per_counter() {
        let cell = SnapshotCell::new();
        cell.record_drained(3);
        cell.record_drained(2);
        cell.record_failed(1);
        cell.record_reconciled(4);
        cell.record_reclaimed(6);
        cell.record_throttled(9);
        assert_eq!(
            cell.load(),
            AgentSnapshot {
                drained: 5,
                failed: 1,
                deferred: 0,
                reconciler_recovered: 4,
                scan_parts_total: 0,
                scan_duration_ms: 0,
                landed_recorded: 0,
                landed_dropped: 0,
                reclaimed: 6,
                reclaim_backing_errors: 0,
                throttled: 9,
                written_off: 0,
                written_off_servable: 0,
            },
        );
    }

    #[test]
    fn scan_parts_accumulate_across_walks_while_the_duration_is_the_latest() {
        // Both SSD walkers record here, so the count must be a fleet total across passes — that
        // is what a rate against the poll interval is derived from. The duration is deliberately
        // NOT summed: it is compared against one poll interval to answer "is this worker walking
        // continuously", and a running total could not answer that.
        let cell = SnapshotCell::new();
        cell.record_scan(2_000_000, Duration::from_millis(400));
        cell.record_scan(280_000, Duration::from_millis(90));

        let snap = cell.load();
        assert_eq!(snap.scan_parts_total, 2_280_000, "walk cost accumulates");
        assert_eq!(snap.scan_duration_ms, 90, "the duration is the most recent walk, not a sum");
    }

    #[test]
    fn landed_records_split_recorded_from_dropped() {
        // The pair that says which discovery path is carrying the work. `dropped` must stay at
        // zero: nonzero means the api and the agent disagree about the message shape, so every
        // announcement is discarded and discovery has silently reverted to the walk.
        let cell = SnapshotCell::new();
        cell.record_landed(7, 0);
        cell.record_landed(3, 2);

        let snap = cell.load();
        assert_eq!(snap.landed_recorded, 10);
        assert_eq!(snap.landed_dropped, 2);
    }

    #[test]
    fn throttled_records_accumulate_without_polluting_error_bps() {
        // A breaker/throttle denial is liveness progress (the loop cycled) but not a
        // Ceph-write failure, so — like `deferred` — it must stay out of error_bps.
        let cell = SnapshotCell::new();
        cell.record_drained(7);
        cell.record_failed(3);
        cell.record_throttled(40);
        let snap = cell.load();
        assert_eq!(snap.throttled, 40, "throttled ticks are counted for readiness/visibility");
        assert_eq!(snap.error_bps(), 3000, "throttled ticks stay out of the Ceph failure rate");
    }

    #[test]
    fn reclaim_backing_errors_accumulate_without_polluting_error_bps() {
        // D4: a backing-read abort disables the `failed`-part SSD GC (the servability gate cannot
        // read `object_versions`) but is NOT a Ceph-write failure, so — like `deferred`/`throttled`
        // — it is counted for the alert yet kept out of the Ceph error rate.
        let cell = SnapshotCell::new();
        cell.record_drained(7);
        cell.record_failed(3);
        cell.record_reclaim_backing_error();
        cell.record_reclaim_backing_error();
        let snap = cell.load();
        assert_eq!(snap.reclaim_backing_errors, 2, "each aborted reclaim cycle is counted");
        assert_eq!(snap.error_bps(), 3000, "backing-read aborts stay out of the Ceph failure rate");
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
            scan_parts_total: 0,
            scan_duration_ms: 0,
            landed_recorded: 0,
            landed_dropped: 0,
            reclaimed: 0,
            reclaim_backing_errors: 0,
            throttled: 0,
            written_off: 0,
            written_off_servable: 0,
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
            scan_parts_total: 0,
            scan_duration_ms: 0,
            landed_recorded: 0,
            landed_dropped: 0,
            reclaimed: 0,
            reclaim_backing_errors: 0,
            throttled: 0,
            written_off: 0,
            written_off_servable: 0,
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
