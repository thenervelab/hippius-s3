//! The agent runtime: the supervised worker topology.
//!
//! [`AgentRuntime`] owns the node-local handles (the `CephFS` pool, the SSD
//! cache, the Postgres store) and registers the long-lived workers with a
//! [`Supervisor`](crate::supervisor): a drain worker that empties the pending
//! part backlog on each poll, a reconciler worker that backfills parts whose
//! landed row is missing (the reconciler is the sole trigger — there is no api
//! NOTIFY in the part model), and an opt-in heartbeat worker that reports this
//! node's disk pressure to the allocator. With rate control on, the drain worker
//! gates through a shared
//! [`Enforcer`] and an allocation-pull worker keeps that enforcer synced to the
//! leader's write-budget. Each worker is a [`run_periodic`] loop that observes
//! the supervisor's cancellation token, so a shutdown signal winds them down
//! gracefully — a tick in flight finishes before the worker exits (axiom
//! `rust_quality_129_async_graceful_shutdown`).

use crate::disk::disk_usage;
use crate::localfs::{LocalFs, LocalSsd};
use crate::supervisor::{RunReport, Supervisor, WorkerName};
use crate::worker::drain_until_empty;
use hippius_drain_core::{
    BreakerConfig, ByteRate, Bytes, CircuitBreaker, Clock, ConcurrencyLimiter, Enforcer, NodeId, NodeObservation, SnapshotCell, Store, SystemClock,
    TokenBucket, UploadEnqueuer, decay_rate, reclaim_ssd, reconcile_parts,
};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Consecutive Ceph-write failures before the circuit breaker opens.
const BREAKER_FAILURES: u32 = 5;
/// How long the breaker stays open before probing Ceph again.
const BREAKER_COOLDOWN: Duration = Duration::from_secs(10);
/// Maximum drains in flight at once (the concurrency gate).
const DRAIN_CONCURRENCY: u32 = 4;

/// Builds the agent's admission enforcer at `rate` with burst `burst`, using
/// fixed breaker/concurrency policy. The breaker opens after [`BREAKER_FAILURES`]
/// consecutive Ceph failures for [`BREAKER_COOLDOWN`]; up to [`DRAIN_CONCURRENCY`]
/// drains run concurrently. The allocation-pull worker then keeps `rate` synced
/// to the leader's budget.
#[must_use]
pub fn default_enforcer(rate: ByteRate, burst: Bytes) -> Enforcer {
    Enforcer::new(
        CircuitBreaker::new(BreakerConfig {
            failure_threshold: BREAKER_FAILURES,
            cooldown: BREAKER_COOLDOWN,
        }),
        TokenBucket::new(rate, burst, Instant::now()),
        ConcurrencyLimiter::new(DRAIN_CONCURRENCY),
    )
}

/// Tick periods and shutdown grace for the runtime's workers.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeConfig {
    /// How often the drain worker polls the pending part backlog.
    pub drain_poll: Duration,
    /// How often the reconciler scans the SSD cache for parts missing a landed row.
    pub reconcile_poll: Duration,
    /// How often the reclaim worker scans the SSD for `failed` (abandoned-upload) parts.
    pub reclaim_poll: Duration,
    /// How long a `failed` part (and an orphan write-temp) is kept before the reclaim
    /// worker unlinks it — a diagnosis / abort-settle window.
    pub reclaim_grace: Duration,
    /// How long workers get to finish an in-flight tick after cancellation
    /// before the supervisor force-aborts them.
    pub grace: Duration,
}

/// Runs `tick` immediately and then once per `period`, until `token` is
/// cancelled. The tick runs to completion before cancellation is checked, so an
/// in-flight drain is never abandoned mid-flight; cancellation is observed during
/// the wait between ticks.
async fn run_periodic<F, Fut>(token: CancellationToken, period: Duration, mut tick: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    loop {
        tick().await;
        tokio::select! {
            () = token.cancelled() => return,
            () = tokio::time::sleep(period) => {}
        }
    }
}

/// What the heartbeat worker needs to report this node to the allocator.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// This node's identity in the fleet.
    pub node: NodeId,
    /// The node's local drain capability — its demand cap (a calibrated value,
    /// not measured live).
    pub max_drain_rate: ByteRate,
    /// How often to publish a heartbeat.
    pub poll: Duration,
}

/// The fast local control loop's wiring: the shared rate limiter plus the inputs
/// the allocation-pull worker needs to keep it synced to the leader's budget.
#[derive(Debug, Clone)]
pub struct RateControl {
    /// The admission valve the drain worker gates through and this worker tunes.
    pub enforcer: Arc<Mutex<Enforcer>>,
    /// This node's identity (the allocation is keyed by it).
    pub node: NodeId,
    /// The rate a silent allocator decays toward (never below it).
    pub floor: ByteRate,
    /// The half-life of that decay.
    pub half_life: Duration,
    /// How often to re-pull the allocation.
    pub poll: Duration,
    /// How long a stored allocation stays authoritative; past it the allocator is
    /// treated as silent on this node and the rate decays toward `floor`.
    pub stale_after: Duration,
}

/// The drain worker's shared dependencies, bundled so the worker fn stays within
/// the argument limit. `enforcer` is `None` for an ungated (unlimited) drain.
struct DrainDeps<E: UploadEnqueuer> {
    ceph: Arc<LocalFs>,
    ssd: Arc<LocalSsd>,
    store: Arc<Store>,
    snapshot: Arc<SnapshotCell>,
    enforcer: Option<Arc<Mutex<Enforcer>>>,
    /// Publishes each part's backend upload request once it's durably on the pool
    /// (drain-direct; the drain is the sole upload producer).
    enqueuer: Arc<E>,
}

/// Drains the part backlog on each `period` poll, until `token` is cancelled.
///
/// The part model has no api NOTIFY: the reconciler is the sole trigger, so a
/// freshly-landed part is picked up within one `period` (or sooner if a concurrent
/// poll is already mid-burst). The backlog drain runs to completion before
/// cancellation is checked, so an in-flight drain is never abandoned mid-flight
/// (axiom `rust_quality_129_async_graceful_shutdown`).
async fn run_drain<E: UploadEnqueuer>(token: CancellationToken, period: Duration, deps: DrainDeps<E>) {
    loop {
        // A drain failure leaves the SSD copy intact (verify-before-unlink), so it is
        // recorded and retried on the next poll; the backlog is never lost. Per-part
        // outcome counting lives in `drain_next`, so the burst result is only logged
        // here — recording it again would double-count.
        match drain_until_empty(
            &deps.ceph,
            &deps.ssd,
            &deps.store,
            deps.enqueuer.as_ref(),
            deps.enforcer.as_ref(),
            Some(&deps.snapshot),
            &token,
            DRAIN_CONCURRENCY as usize,
        )
        .await
        {
            Ok(drained) => tracing::debug!(drained, "drain cycle complete"),
            // Debug-format the error so the `PartDrainError` variant + `DrainStep` + the
            // underlying io errno surface; `%err` (Display) only prints the opaque
            // "draining a part failed" and hides which step/errno actually failed.
            Err(err) => tracing::warn!(error = ?err, "drain cycle failed; SSD copy retained, will retry"),
        }

        tokio::select! {
            () = token.cancelled() => return,
            () = tokio::time::sleep(period) => {}
        }
    }
}

/// Probes SSD disk pressure off the async runtime (statvfs blocks) and upserts
/// this node's heartbeat. Probe or upsert failures are logged and skipped — the
/// next tick retries; a missed heartbeat only ages the node out of the fleet.
async fn heartbeat_once(ssd: &LocalSsd, store: &Store, node: &NodeId, max_drain_rate: ByteRate, snapshot: &SnapshotCell) {
    let root = ssd.root().to_path_buf();
    // statvfs is a blocking syscall — never run it on an executor thread (axiom
    // r4r_ch10_01); spawn_blocking moves it to the blocking pool.
    let usage = match tokio::task::spawn_blocking(move || disk_usage(&root)).await {
        Ok(Ok(usage)) => usage,
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "disk usage probe failed");
            return;
        }
        Err(err) => {
            tracing::warn!(error = %err, "disk usage probe task panicked");
            return;
        }
    };

    // Log the human-facing pressure band so an operator sees this node's severity
    // at a glance; the allocator still consumes the raw pressure, not the zone.
    tracing::debug!(node = %node, zone = ?usage.pressure.zone(), pressure_bps = usage.pressure.bps(), "heartbeat disk pressure");

    // Pressure (allocator weight), backlog (SSD used bytes — the undrained work),
    // and error rate (from the drain counters) are real. p99 latency stays neutral
    // until per-drain timing is wired (the saturation signal, not a demand signal).
    let observation = NodeObservation {
        pressure: usage.pressure,
        backlog: Bytes::new(usage.used_bytes),
        max_drain_rate,
        observed_p99: snapshot.p99(),
        error_bps: snapshot.load().error_bps(),
    };
    if let Err(err) = store.upsert_node_state(node, &observation).await {
        tracing::warn!(error = %err, "heartbeat upsert failed");
    }
}

/// One reclaim pass: evict `failed` (broken/abandoned-upload) SSD parts older than
/// `grace`, then sweep orphan write-temps older than `grace`.
///
/// Reclaim and sweep errors are logged and skipped; the next tick retries, and no part
/// is ever removed without a successful status read (fail-safe).
async fn reclaim_once(ssd: &LocalSsd, store: &Store, snapshot: &SnapshotCell, grace: Duration) {
    match reclaim_ssd(ssd, ssd, store, grace).await {
        Ok(report) => {
            snapshot.record_reclaimed(report.reclaimed);
            if report.reclaimed > 0 {
                // Aggregate, not per-part: an abandoned MPU reclaims many parts at once,
                // so a per-part line would spam.
                tracing::info!(reclaimed = report.reclaimed, "reclaimed broken/abandoned-upload SSD parts");
            }
        }
        Err(err) => tracing::warn!(error = ?err, "ssd reclaim cycle failed; will retry next poll"),
    }

    match ssd.sweep_orphan_tmp(grace).await {
        Ok(0) => {}
        Ok(removed) => tracing::info!(removed, "swept orphan SSD write-temps"),
        Err(err) => tracing::warn!(error = %err, "orphan-temp sweep failed; will retry next poll"),
    }
}

/// Sets the enforcer's rate under its lock. The op is synchronous, so the guard
/// never crosses an `.await` (axiom `rust_quality_74`); a poisoned lock recovers
/// via `into_inner` — the `Enforcer` is a small `Copy` value left consistent.
fn apply_rate(enforcer: &Arc<Mutex<Enforcer>>, rate: ByteRate) {
    enforcer.lock().unwrap_or_else(PoisonError::into_inner).set_rate(rate);
}

/// Pulls this node's leader-assigned budget into the shared enforcer each
/// `poll`, decaying toward `floor` when the allocator goes silent so a stale
/// allocation cannot keep the node hammering Ceph. The async `load_allocation`
/// runs *unlocked*; only the synchronous `set_rate` takes the lock.
async fn run_alloc(token: CancellationToken, store: Arc<Store>, rate_control: RateControl, clock: Arc<dyn Clock>) {
    let RateControl {
        enforcer,
        node,
        floor,
        half_life,
        poll,
        stale_after,
    } = rate_control;
    // Decay state: the last allocated rate and when it landed. A fresh
    // allocation resets both; silence decays `base` toward `floor` by age. The
    // age is measured against the injected `clock`, so decay timing is testable.
    let mut base = floor;
    let mut allocated_at = clock.now();
    loop {
        match store.load_allocation(&node, stale_after).await {
            Ok(Some(allocation)) => {
                base = allocation.budget;
                allocated_at = clock.now();
                apply_rate(&enforcer, base);
            }
            Ok(None) => apply_rate(&enforcer, decay_rate(base, floor, clock.now().duration_since(allocated_at), half_life)),
            Err(err) => tracing::warn!(error = %err, "allocation pull failed; keeping the current budget"),
        }
        tokio::select! {
            () = token.cancelled() => return,
            () = tokio::time::sleep(poll) => {}
        }
    }
}

/// The supervised agent runtime over one node's pool, cache, and store.
///
/// Generic over the [`UploadEnqueuer`] the drain uses to publish each replicated
/// part's backend upload request (drain-direct). Production wires the agent's Redis
/// enqueuer; tests inject a no-op.
#[derive(Debug)]
pub struct AgentRuntime<E: UploadEnqueuer> {
    ceph: Arc<LocalFs>,
    ssd: Arc<LocalSsd>,
    store: Arc<Store>,
    enqueuer: Arc<E>,
    config: RuntimeConfig,
    /// Running drain counters the workers publish into; the observability layer
    /// (M9) reads them. Shared with each worker via `Arc`.
    snapshot: Arc<SnapshotCell>,
    /// Opt-in heartbeat reporting. `None` means the node does not report itself
    /// to the allocator (used by tests and the drain-only e2e).
    heartbeat: Option<HeartbeatConfig>,
    /// Opt-in rate control. `Some` shares an enforcer between the drain worker
    /// (which gates through it) and the allocation-pull worker (which tunes it);
    /// `None` drains ungated.
    rate_control: Option<RateControl>,
    /// The time source the allocation-pull worker reads for its decay timing.
    /// Defaults to [`SystemClock`]; tests inject a [`hippius_drain_core::TestClock`].
    clock: Arc<dyn Clock>,
}

impl<E: UploadEnqueuer + 'static> AgentRuntime<E> {
    /// Builds a runtime over the given handles. The `Arc`s are cloned into each
    /// worker, so the runtime's workers share one pool/cache/store/enqueuer.
    #[must_use]
    pub fn new(ceph: Arc<LocalFs>, ssd: Arc<LocalSsd>, store: Arc<Store>, enqueuer: Arc<E>, config: RuntimeConfig) -> Self {
        Self {
            ceph,
            ssd,
            store,
            enqueuer,
            config,
            snapshot: Arc::new(SnapshotCell::new()),
            heartbeat: None,
            rate_control: None,
            clock: Arc::new(SystemClock),
        }
    }

    /// Overrides the runtime's time source (the allocation worker's decay clock).
    /// Production uses the default [`SystemClock`]; tests inject a deterministic
    /// [`hippius_drain_core::TestClock`] to drive decay without real time passing.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Enables the fast local control loop: the drain worker gates through the
    /// shared enforcer, and an allocation-pull worker keeps that enforcer synced
    /// to the leader's write-budget. Without it the drain runs ungated.
    #[must_use]
    pub fn with_rate_control(mut self, rate_control: RateControl) -> Self {
        self.rate_control = Some(rate_control);
        self
    }

    /// Enables the heartbeat worker, which periodically reports this node's disk
    /// pressure (and capability) to the allocator. Without it the node never
    /// appears in the fleet and is allocated no budget.
    #[must_use]
    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    /// A handle to the runtime's live drain counters. Grab it before [`run`](Self::run)
    /// (which consumes the runtime) to read metrics while the workers publish.
    #[must_use]
    pub fn snapshot(&self) -> Arc<SnapshotCell> {
        Arc::clone(&self.snapshot)
    }

    /// Spawns the workers under a supervisor and runs until `shutdown` resolves
    /// (or a worker faults), returning how every worker wound down.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> RunReport {
        let mut supervisor = Supervisor::new(self.config.grace);

        // Drain worker: empty the pending part backlog on each poll.
        let drain_poll = self.config.drain_poll;
        let deps = DrainDeps {
            ceph: Arc::clone(&self.ceph),
            ssd: Arc::clone(&self.ssd),
            store: Arc::clone(&self.store),
            snapshot: Arc::clone(&self.snapshot),
            // The drain worker and the allocation worker share one enforcer.
            enforcer: self.rate_control.as_ref().map(|rc| Arc::clone(&rc.enforcer)),
            enqueuer: Arc::clone(&self.enqueuer),
        };
        supervisor.spawn(WorkerName::new("drain"), move |token| run_drain(token, drain_poll, deps));

        // Reconciler worker: backfill parts whose landed row is missing.
        let (ssd, store) = (Arc::clone(&self.ssd), Arc::clone(&self.store));
        let reconcile_poll = self.config.reconcile_poll;
        let snapshot = Arc::clone(&self.snapshot);
        supervisor.spawn(WorkerName::new("reconcile"), move |token| {
            run_periodic(token, reconcile_poll, move || {
                let (ssd, store, snapshot) = (Arc::clone(&ssd), Arc::clone(&store), Arc::clone(&snapshot));
                async move {
                    match reconcile_parts(ssd.as_ref(), store.as_ref()).await {
                        Ok(report) => snapshot.record_reconciled(report.recovered),
                        Err(err) => tracing::warn!(error = %err, "reconcile cycle failed"),
                    }
                }
            })
        });

        // SSD reclaim worker: clean up broken/abandoned uploads (`failed` parts) the
        // drain skips forever, plus orphan write-temps — the SSD-ingest tier's GC so
        // abandoned-upload junk does not accumulate on the node-local disk.
        let (ssd, store, snapshot) = (Arc::clone(&self.ssd), Arc::clone(&self.store), Arc::clone(&self.snapshot));
        let reclaim_poll = self.config.reclaim_poll;
        let reclaim_grace = self.config.reclaim_grace;
        supervisor.spawn(WorkerName::new("ssd_reclaim"), move |token| {
            run_periodic(token, reclaim_poll, move || {
                let (ssd, store, snapshot) = (Arc::clone(&ssd), Arc::clone(&store), Arc::clone(&snapshot));
                async move {
                    reclaim_once(&ssd, &store, &snapshot, reclaim_grace).await;
                }
            })
        });

        // Heartbeat worker (opt-in): report this node's disk pressure + capability
        // so the allocator can weight it. Without it the node is never in the fleet.
        if let Some(heartbeat) = self.heartbeat {
            let (ssd, store, snapshot) = (Arc::clone(&self.ssd), Arc::clone(&self.store), Arc::clone(&self.snapshot));
            let HeartbeatConfig { node, max_drain_rate, poll } = heartbeat;
            supervisor.spawn(WorkerName::new("heartbeat"), move |token| {
                run_periodic(token, poll, move || {
                    let (ssd, store, node, snapshot) = (Arc::clone(&ssd), Arc::clone(&store), node.clone(), Arc::clone(&snapshot));
                    async move {
                        heartbeat_once(&ssd, &store, &node, max_drain_rate, &snapshot).await;
                    }
                })
            });
        }

        // Allocation-pull worker (opt-in): apply the leader's write-budget to the
        // shared enforcer, decaying toward the floor when the allocator is silent.
        if let Some(rate_control) = self.rate_control {
            let store = Arc::clone(&self.store);
            let clock = Arc::clone(&self.clock);
            supervisor.spawn(WorkerName::new("allocation"), move |token| run_alloc(token, store, rate_control, clock));
        }

        supervisor.run(shutdown).await
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{AgentRuntime, HeartbeatConfig, RateControl, RuntimeConfig, default_enforcer};
    use crate::localfs::{LocalFs, LocalSsd};
    use crate::supervisor::ShutdownTrigger;
    use core::str::FromStr;
    use hippius_drain_core::{
        Allocation, ByteRate, Bytes, Clock, NodeId, ObjectId, PartKey, PartNumber, PartReplicationStore, ReplicationState, SnapshotCell, Store,
        TestClock, UploadEnqueuer, Version,
    };

    /// A no-op upload enqueuer for the runtime tests (drain-direct's Redis fan-out is
    /// covered by the core partdrain tests + the agent enqueue module).
    struct NoopEnqueuer;
    impl UploadEnqueuer for NoopEnqueuer {
        type Error = std::io::Error;
        async fn enqueue(&self, _part: &PartKey) -> Result<(), std::io::Error> {
            Ok(())
        }
    }
    use sqlx::postgres::PgPool;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    const UUID: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    fn part_at(version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(UUID).unwrap(), Version::new(version), PartNumber::new(number))
    }

    /// Lays a complete SSD part (chunk files + meta.json) and records it pending.
    async fn seed_part(ssd_root: &Path, store: &Store, part: &PartKey) {
        seed_ssd_dir(ssd_root, part);
        store.record_landed_part(part).await.unwrap();
    }

    /// Lays a complete SSD part dir (chunk + meta) WITHOUT a DB row — for the reclaim
    /// test, which sets the row's status/age itself.
    fn seed_ssd_dir(ssd_root: &Path, part: &PartKey) {
        let dir = ssd_root.join(part.relative_dir());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("chunk_0.bin"), b"runtime backlog part").unwrap();
        std::fs::write(dir.join("meta.json"), br#"{"chunk_size":20,"num_chunks":1,"size_bytes":20}"#).unwrap();
    }

    async fn all_replicated(store: &Store, parts: &[PartKey]) -> bool {
        for part in parts {
            let status = <Store as PartReplicationStore>::status(store, part).await.unwrap();
            if status != Some(ReplicationState::Replicated) {
                return false;
            }
        }
        true
    }

    async fn wait_replicated(store: &Store, parts: &[PartKey]) -> bool {
        for _ in 0..100 {
            if all_replicated(store, parts).await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn the_allocation_worker_applies_the_leaders_budget(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::from_pool(pool));
        let node = NodeId::from_str("node-alloc").unwrap();

        // The leader has already written this node a budget.
        store
            .write_allocations(
                1,
                &[Allocation {
                    node: node.clone(),
                    budget: ByteRate::new(750_000),
                }],
            )
            .await
            .unwrap();

        // The enforcer starts at zero, so any non-zero rate is the worker's doing.
        let enforcer = Arc::new(Mutex::new(default_enforcer(ByteRate::new(0), Bytes::new(1 << 20))));
        let runtime = AgentRuntime::new(
            Arc::new(LocalFs::new(pool_dir.path())),
            Arc::new(LocalSsd::new(ssd_dir.path())),
            Arc::clone(&store),
            Arc::new(NoopEnqueuer),
            RuntimeConfig {
                drain_poll: Duration::from_mins(1),
                reconcile_poll: Duration::from_mins(1),
                reclaim_poll: Duration::from_mins(1),
                reclaim_grace: Duration::from_hours(1),
                grace: Duration::from_secs(5),
            },
        )
        .with_rate_control(RateControl {
            enforcer: Arc::clone(&enforcer),
            node,
            floor: ByteRate::new(1_000),
            half_life: Duration::from_secs(30),
            poll: Duration::from_millis(20),
            stale_after: Duration::from_secs(10),
        });

        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run(signal.cancelled_owned()).await });

        // The worker pulls the budget into the enforcer within a few ticks.
        let mut applied = false;
        for _ in 0..100 {
            if enforcer.lock().unwrap().rate() == ByteRate::new(750_000) {
                applied = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(applied, "the allocation worker applied the leader's budget to the enforcer");

        shutdown.cancel();
        assert!(handle.await.unwrap().clean, "every worker wound down within grace");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn the_allocation_worker_decays_on_an_injected_clock(pool: PgPool) {
        // #33: run_alloc reads the injected Clock for its decay timing. Load a
        // budget (base jumps above the floor, allocated_at = clock.now()), then
        // delete the row so the allocator reads "silent" and advance the TestClock
        // past several half-lives. The enforcer rate must decay below the budget
        // toward the floor — proving the *injected* clock drives decay, since no
        // real time passes between the load and the assertion.
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::from_pool(pool.clone()));
        let node = NodeId::from_str("node-decay").unwrap();
        let budget = ByteRate::new(800_000);
        let floor = ByteRate::new(1_000);

        store.write_allocations(1, &[Allocation { node: node.clone(), budget }]).await.unwrap();

        let enforcer = Arc::new(Mutex::new(default_enforcer(ByteRate::new(0), Bytes::new(1 << 20))));
        // Keep the concrete handle (for `advance`) and a trait-object clone (for the
        // runtime); the `let` binding is the unsizing coercion site.
        let clock = Arc::new(TestClock::new());
        let clock_source: Arc<dyn Clock> = clock.clone();
        let runtime = AgentRuntime::new(
            Arc::new(LocalFs::new(pool_dir.path())),
            Arc::new(LocalSsd::new(ssd_dir.path())),
            Arc::clone(&store),
            Arc::new(NoopEnqueuer),
            RuntimeConfig {
                drain_poll: Duration::from_mins(1),
                reconcile_poll: Duration::from_mins(1),
                reclaim_poll: Duration::from_mins(1),
                reclaim_grace: Duration::from_hours(1),
                grace: Duration::from_secs(5),
            },
        )
        .with_rate_control(RateControl {
            enforcer: Arc::clone(&enforcer),
            node: node.clone(),
            floor,
            half_life: Duration::from_secs(1),
            poll: Duration::from_millis(10),
            // SQL staleness off (1h): the DELETE below, not row aging, drives the
            // load to None, so the test controls exactly when decay begins.
            stale_after: Duration::from_hours(1),
        })
        .with_clock(clock_source);

        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run(signal.cancelled_owned()).await });

        // The worker first pulls the budget into the enforcer.
        let mut loaded = false;
        for _ in 0..200 {
            if enforcer.lock().unwrap().rate() == budget {
                loaded = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(loaded, "the worker loaded the budget before decay");

        // The allocator goes silent (row removed), and the injected clock jumps
        // well past the 1s half-life, so decay should drive the rate to the floor.
        sqlx::query("DELETE FROM cephor_allocation WHERE node_id = $1")
            .bind(node.as_str())
            .execute(&pool)
            .await
            .unwrap();
        // Ten half-lives on the injected clock -> the rate collapses to ~floor.
        // This deep decay is reachable ONLY by the injected jump: the sub-second of
        // real time the test spends polling, against the 1s half-life, could only
        // shave the rate slightly. So a near-floor rate proves run_alloc read the
        // injected clock, not the wall clock.
        clock.advance(Duration::from_secs(10));
        let near_floor = floor.get().saturating_mul(2);

        let mut decayed = false;
        for _ in 0..200 {
            if enforcer.lock().unwrap().rate().get() <= near_floor {
                decayed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(decayed, "the injected clock's 10-half-life jump decayed the rate to ~floor");
        assert!(
            enforcer.lock().unwrap().rate().get() >= floor.get(),
            "decay never drops below the configured floor",
        );

        shutdown.cancel();
        assert!(handle.await.unwrap().clean, "every worker wound down within grace");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn the_heartbeat_worker_upserts_this_node_into_the_fleet(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::from_pool(pool));
        let node = NodeId::from_str("node-hb").unwrap();
        // A distinctive capability so the asserted row is unambiguously ours.
        let rate = ByteRate::new(7_000_000);

        let runtime = AgentRuntime::new(
            Arc::new(LocalFs::new(pool_dir.path())),
            Arc::new(LocalSsd::new(ssd_dir.path())),
            Arc::clone(&store),
            Arc::new(NoopEnqueuer),
            RuntimeConfig {
                // Park drain/reconcile; this test exercises only the heartbeat.
                drain_poll: Duration::from_mins(1),
                reconcile_poll: Duration::from_mins(1),
                reclaim_poll: Duration::from_mins(1),
                reclaim_grace: Duration::from_hours(1),
                grace: Duration::from_secs(5),
            },
        )
        .with_heartbeat(HeartbeatConfig {
            node: node.clone(),
            max_drain_rate: rate,
            poll: Duration::from_millis(20),
        });

        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run(signal.cancelled_owned()).await });

        // The heartbeat ticks immediately on spawn; poll the fleet until our node
        // (matched by its distinctive capability) appears.
        let mut seen = false;
        for _ in 0..100 {
            let fleet = store.load_fleet(Duration::from_hours(1)).await.unwrap();
            if fleet
                .iter()
                .any(|(id, observation)| id == &node && observation.max_drain_rate == rate && observation.backlog.get() > 0)
            {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(seen, "the heartbeat worker upserted this node's state");

        shutdown.cancel();
        let report = handle.await.unwrap();
        assert!(report.clean, "every worker wound down within grace");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn the_reconciler_recovers_a_part_with_no_landed_row(pool: PgPool) {
        // The part model's sole trigger: a complete part is on SSD but has no DB row
        // (the api never emits one — the reconciler is the source of truth). The
        // reconciler must record it pending so the drain worker then replicates it.
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::from_pool(pool));

        // Lay a complete part on SSD WITHOUT recording a landed row.
        let part = part_at(5, 1);
        let dir = ssd_dir.path().join(part.relative_dir());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("chunk_0.bin"), b"unregistered part").unwrap();
        std::fs::write(dir.join("meta.json"), br#"{"chunk_size":17,"num_chunks":1,"size_bytes":17}"#).unwrap();

        let runtime = AgentRuntime::new(
            Arc::new(LocalFs::new(pool_dir.path())),
            Arc::new(LocalSsd::new(ssd_dir.path())),
            Arc::clone(&store),
            Arc::new(NoopEnqueuer),
            RuntimeConfig {
                drain_poll: Duration::from_millis(20),
                reconcile_poll: Duration::from_millis(20),
                reclaim_poll: Duration::from_mins(1),
                reclaim_grace: Duration::from_hours(1),
                grace: Duration::from_secs(5),
            },
        );

        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run(signal.cancelled_owned()).await });

        // The reconciler discovers it, records it pending, and the drain replicates it.
        assert!(
            wait_replicated(&store, &[part]).await,
            "the reconciler recovered the unregistered part and the drain replicated it"
        );

        shutdown.cancel();
        let report = handle.await.unwrap();
        assert!(report.clean, "every worker wound down within grace");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn runtime_drains_a_seeded_backlog_then_shuts_down_cleanly(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let store = Store::from_pool(pool);

        // Seed three honest parts and record them pending.
        let mut parts = Vec::new();
        for number in 1..=3_u32 {
            let part = part_at(5, number);
            seed_part(ssd_dir.path(), &store, &part).await;
            parts.push(part);
        }

        let store = Arc::new(store);
        let runtime = AgentRuntime::new(
            Arc::new(LocalFs::new(pool_dir.path())),
            Arc::new(LocalSsd::new(ssd_dir.path())),
            Arc::clone(&store),
            Arc::new(NoopEnqueuer),
            RuntimeConfig {
                drain_poll: Duration::from_millis(20),
                reconcile_poll: Duration::from_millis(50),
                reclaim_poll: Duration::from_mins(1),
                reclaim_grace: Duration::from_hours(1),
                grace: Duration::from_secs(5),
            },
        );

        let snapshot = runtime.snapshot();
        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run(signal.cancelled_owned()).await });

        // The drain worker ticks immediately on spawn, so the backlog drains fast.
        assert!(wait_replicated(&store, &parts).await, "the runtime drained the seeded backlog");

        // Signal shutdown; the workers wind down within grace.
        shutdown.cancel();
        let report = handle.await.unwrap();
        assert!(report.clean, "every worker wound down within grace");
        assert_eq!(report.trigger, ShutdownTrigger::Signal);

        // The drain worker published its counters: all three parts counted once, no
        // failures on the happy path. Exactly 3 — once a part is replicated and its
        // SSD copy unlinked, claim_part never re-claims it, so there is no re-drive.
        let snap = snapshot.load();
        assert_eq!(snap.drained, 3, "every drained part was counted once");
        assert_eq!(snap.failed, 0, "the happy path records no failures");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn reclaim_once_evicts_a_failed_aged_part_and_keeps_a_replicated_one(pool: PgPool) {
        // The SSD-ingest GC, against real Postgres + a real LocalSsd: a `failed` part (a
        // broken/abandoned upload) is unlinked once aged, while a `replicated` part (the
        // drain's own to clean) and the DB rows are left untouched. Driving reclaim_once
        // directly keeps it deterministic — the periodic spawn machinery is the same
        // run_periodic the sibling worker tests cover.
        let ssd_dir = tempfile::tempdir().unwrap();
        let store = Store::from_pool(pool.clone());
        let snapshot = SnapshotCell::new();

        // A failed part (abandoned upload), forced 2h old -> reclaimable.
        let failed = part_at(5, 1);
        seed_ssd_dir(ssd_dir.path(), &failed);
        store.record_landed_part(&failed).await.unwrap();
        force_terminal_2h(&pool, &failed, "failed").await;

        // A replicated part -> the drain's own to clean up; the reclaimer leaves it.
        let replicated = part_at(5, 2);
        seed_ssd_dir(ssd_dir.path(), &replicated);
        store.record_landed_part(&replicated).await.unwrap();
        force_terminal_2h(&pool, &replicated, "replicated").await;

        let ssd = LocalSsd::new(ssd_dir.path());
        super::reclaim_once(&ssd, &store, &snapshot, Duration::from_secs(1)).await;

        assert!(
            !ssd_dir.path().join(failed.relative_dir()).exists(),
            "the aged failed (abandoned-upload) part was evicted from the SSD",
        );
        assert!(
            ssd_dir.path().join(replicated.relative_dir()).exists(),
            "a replicated part is left for the drain, never reclaimed here",
        );
        assert_eq!(snapshot.load().reclaimed, 1, "exactly the failed part was counted");
        // Reclaim only unlinks the SSD copy; the DB row is left intact.
        assert_eq!(
            <Store as PartReplicationStore>::status(&store, &failed).await.unwrap(),
            Some(ReplicationState::Failed),
            "reclaim does not touch the replication row",
        );
    }

    /// Forces a part's row to `status` with `updated_at` backdated 2h, so the reclaim
    /// age reads deterministically older than any test grace.
    async fn force_terminal_2h(pool: &PgPool, part: &PartKey, status: &str) {
        sqlx::query(
            "UPDATE cephor_replication_status SET status = $4, updated_at = now() - interval '2 hours' \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(status)
        .execute(pool)
        .await
        .unwrap();
    }
}
