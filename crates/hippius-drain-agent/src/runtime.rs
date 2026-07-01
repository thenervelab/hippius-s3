//! The agent runtime: the supervised worker topology.
//!
//! [`AgentRuntime`] owns the node-local handles (the `CephFS` pool, the SSD
//! cache, the Postgres store) and registers the long-lived workers with a
//! [`Supervisor`](crate::supervisor): a drain worker that empties the pending
//! part backlog on each poll, a reconciler worker that backfills parts whose
//! landed row is missing (the reconciler is the sole trigger — there is no api
//! NOTIFY in the part model), an `ssd_reclaim` worker that GCs `failed`
//! (broken/abandoned-upload) parts + orphan write-temps off the local SSD, and an
//! opt-in heartbeat worker that reports this node's disk pressure to the allocator.
//! With rate control on, the drain worker
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
    BreakerConfig, ByteRate, Bytes, CircuitBreaker, Clock, ConcurrencyLimiter, CoordError, Coordinator, Enforcer, NodeId, NodeObservation,
    SnapshotCell, Store, StoredAllocation, SystemClock, TokenBucket, UploadEnqueuer, decay_rate, reclaim_ssd, reconcile_parts,
};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Best-effort liveness touch: rewriting the file bumps its mtime, which the k8s exec
/// probe checks for freshness. Errors are ignored — a transient FS blip must not kill an
/// otherwise-healthy process; a persistent failure ages the mtime out and the probe fails,
/// which is the correct outcome (restart a wedged pod).
fn touch_liveness(path: &Path) {
    let _ = std::fs::write(path, b"ok");
}

/// Consecutive Ceph-write failures before the circuit breaker opens.
const BREAKER_FAILURES: u32 = 5;
/// How long the breaker stays open before probing Ceph again.
const BREAKER_COOLDOWN: Duration = Duration::from_secs(10);

/// Builds the agent's admission enforcer at `rate` with burst `burst`, gating up to
/// `concurrency` drains at once. The breaker opens after [`BREAKER_FAILURES`]
/// consecutive Ceph failures for [`BREAKER_COOLDOWN`]; the allocation-pull worker
/// then keeps `rate` synced to the leader's budget. `concurrency` comes from
/// `CEPHOR_DRAIN_CONCURRENCY` so a node can hide fsync latency behind more in-flight
/// parts (the AIMD allocator only tunes bytes/sec).
#[must_use]
pub fn default_enforcer(rate: ByteRate, burst: Bytes, concurrency: u32) -> Enforcer {
    Enforcer::new(
        CircuitBreaker::new(BreakerConfig {
            failure_threshold: BREAKER_FAILURES,
            cooldown: BREAKER_COOLDOWN,
        }),
        TokenBucket::new(rate, burst, Instant::now()),
        ConcurrencyLimiter::new(concurrency),
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
    /// Maximum parts the drain worker processes concurrently (the in-flight gate
    /// that overlaps fsync latency across parts).
    pub drain_concurrency: u32,
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
    /// Maximum parts processed concurrently per drain cycle (`CEPHOR_DRAIN_CONCURRENCY`).
    concurrency: u32,
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
            deps.concurrency as usize,
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
async fn heartbeat_once(ssd: &LocalSsd, coord: &Coordinator, node: &NodeId, max_drain_rate: ByteRate, snapshot: &SnapshotCell) {
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
    if let Err(err) = coord.upsert_node_state(node, &observation).await {
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
            // The one signal that the (deliberately un-handled) replicated crash-orphan
            // leak is actually occurring — otherwise invisible. Warn so it surfaces.
            if report.skipped_replicated > 0 {
                tracing::warn!(
                    skipped_replicated = report.skipped_replicated,
                    "replicated parts still on SSD — drain crash-orphans nothing currently reclaims"
                );
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

/// The enforcer action for one allocation-pull outcome, separated from the async
/// loop so the "a failed pull decays like silence" policy is unit-testable without
/// a live Redis. `Decay` deliberately covers BOTH a TTL-expired allocation
/// (`Ok(None)`) and a failed pull (`Err`): a sustained pull failure must not hold
/// the last budget and keep the node draining full-rate into a possibly-degraded
/// Ceph — the exact incident the rate control exists to dampen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PullAction {
    /// A fresh budget landed: adopt it as the new decay base.
    Adopt(ByteRate),
    /// No budget (TTL-expired) or the pull failed: decay the current base toward floor.
    Decay,
}

/// Maps an allocation-pull result to its enforcer action. A fresh budget is
/// adopted; silence (`Ok(None)`) and any failure (`Err`) both decay.
fn pull_action(pull: &Result<Option<StoredAllocation>, CoordError>) -> PullAction {
    match pull {
        Ok(Some(allocation)) => PullAction::Adopt(allocation.budget),
        Ok(None) | Err(_) => PullAction::Decay,
    }
}

/// Pulls this node's leader-assigned budget into the shared enforcer each
/// `poll`, decaying toward `floor` when the allocator goes silent OR the pull
/// fails, so a stale allocation cannot keep the node hammering Ceph. The async
/// `load_allocation` runs *unlocked*; only the synchronous `set_rate` takes the lock.
async fn run_alloc(token: CancellationToken, coord: Arc<Coordinator>, rate_control: RateControl, clock: Arc<dyn Clock>) {
    let RateControl {
        enforcer,
        node,
        floor,
        half_life,
        poll,
    } = rate_control;
    // Decay state: the last allocated rate and when it landed. A fresh
    // allocation resets both; silence decays `base` toward `floor` by age. The
    // age is measured against the injected `clock`, so decay timing is testable.
    // The allocation key's Redis TTL is the staleness signal: once it expires,
    // load_allocation returns None and the decay path engages.
    let mut base = floor;
    let mut allocated_at = clock.now();
    loop {
        let pull = coord.load_allocation(&node).await;
        if let Err(err) = &pull {
            tracing::warn!(error = %err, "allocation pull failed; decaying toward the floor");
        }
        match pull_action(&pull) {
            PullAction::Adopt(budget) => {
                base = budget;
                allocated_at = clock.now();
                apply_rate(&enforcer, base);
            }
            PullAction::Decay => apply_rate(&enforcer, decay_rate(base, floor, clock.now().duration_since(allocated_at), half_life)),
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
    /// The Redis-backed coordinator the heartbeat + allocation-pull workers use.
    /// Required for those two workers to run; `None` (tests / drain-only e2e) skips
    /// them even if heartbeat / rate control are configured.
    coord: Option<Arc<Coordinator>>,
    /// The time source the allocation-pull worker reads for its decay timing.
    /// Defaults to [`SystemClock`]; tests inject a [`hippius_drain_core::TestClock`].
    clock: Arc<dyn Clock>,
    /// Opt-in liveness file the heartbeat worker touches each tick, for the k8s probe.
    /// `None` (tests / drain-only e2e) writes no file.
    liveness_path: Option<Arc<PathBuf>>,
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
            coord: None,
            clock: Arc::new(SystemClock),
            liveness_path: None,
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

    /// Wires the Redis coordinator the heartbeat + allocation-pull workers need.
    /// Without it those two workers do not run (the drain/reconcile/reclaim workers,
    /// which use only the Postgres store, are unaffected).
    #[must_use]
    pub fn with_coordinator(mut self, coord: Arc<Coordinator>) -> Self {
        self.coord = Some(coord);
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

    /// Sets the liveness file the heartbeat worker touches each tick, so a k8s
    /// `livenessProbe` can restart a wedged (not crashed) pod. Without it (tests /
    /// drain-only e2e) no file is written.
    #[must_use]
    pub fn with_liveness(mut self, path: PathBuf) -> Self {
        self.liveness_path = Some(Arc::new(path));
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
            concurrency: self.config.drain_concurrency,
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
        // so the allocator can weight it. Needs the coordinator; without it the node
        // is never in the fleet.
        if let (Some(heartbeat), Some(coord)) = (self.heartbeat, self.coord.as_ref()) {
            let (ssd, coord, snapshot) = (Arc::clone(&self.ssd), Arc::clone(coord), Arc::clone(&self.snapshot));
            let liveness = self.liveness_path.clone();
            let HeartbeatConfig { node, max_drain_rate, poll } = heartbeat;
            supervisor.spawn(WorkerName::new("heartbeat"), move |token| {
                run_periodic(token, poll, move || {
                    let (ssd, coord, node, snapshot, liveness) = (
                        Arc::clone(&ssd),
                        Arc::clone(&coord),
                        node.clone(),
                        Arc::clone(&snapshot),
                        liveness.clone(),
                    );
                    async move {
                        // Touch liveness FIRST, before the (blocking, possibly early-returning)
                        // disk probe, so a healthy runtime keeps the k8s probe fresh even when a
                        // statvfs blip short-circuits heartbeat_once. The heartbeat cadence is a
                        // bounded, quick tick independent of drain-backlog duration, so it is a
                        // truer runtime-liveness signal than the drain loop (which can legitimately
                        // run long under a large backlog).
                        if let Some(path) = liveness.as_deref() {
                            touch_liveness(path);
                        }
                        heartbeat_once(&ssd, &coord, &node, max_drain_rate, &snapshot).await;
                    }
                })
            });
        }

        // Allocation-pull worker (opt-in): apply the leader's write-budget to the
        // shared enforcer, decaying toward the floor when the allocator is silent.
        // Needs the coordinator; without it the drain stays ungated at its floor.
        if let (Some(rate_control), Some(coord)) = (self.rate_control, self.coord.as_ref()) {
            let coord = Arc::clone(coord);
            let clock = Arc::clone(&self.clock);
            supervisor.spawn(WorkerName::new("allocation"), move |token| run_alloc(token, coord, rate_control, clock));
        }

        supervisor.run(shutdown).await
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr, reason = "tests")]
mod tests {
    use super::{AgentRuntime, HeartbeatConfig, PullAction, RateControl, RuntimeConfig, default_enforcer, pull_action};
    use crate::localfs::{LocalFs, LocalSsd};
    use crate::supervisor::ShutdownTrigger;
    use core::str::FromStr;
    use hippius_drain_core::{
        Allocation, ByteRate, Bytes, Clock, CoordError, Coordinator, NodeId, ObjectId, PartKey, PartNumber, PartReplicationStore, ReplicationState,
        SnapshotCell, Store, StoredAllocation, TestClock, UploadEnqueuer, Version,
    };

    /// A coordinator on the test Redis under a per-test prefix, or `None` to skip the
    /// redis-dependent runtime tests (Rust has no fakeredis; set
    /// `CEPHOR_TEST_REDIS_URL=redis://localhost:6382/0`). `alloc_ttl` lets the decay test
    /// expire a budget deterministically.
    async fn test_coord(prefix: &str, node_ttl: Duration, alloc_ttl: Duration) -> Option<Coordinator> {
        let url = std::env::var("CEPHOR_TEST_REDIS_URL").ok().filter(|value| !value.is_empty())?;
        // Prefix with the process id so a re-run never collides with the prior run's
        // still-TTL'd keys on a shared test Redis.
        let coordinator = Coordinator::connect(&url, node_ttl, alloc_ttl)
            .await
            .expect("connect to the test redis")
            .with_prefix(&format!("{}:{prefix}", std::process::id()));
        Some(coordinator)
    }

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

    #[test]
    fn pull_action_adopts_a_fresh_budget() {
        let allocation = StoredAllocation {
            budget: ByteRate::new(500_000),
            epoch: 7,
        };
        assert_eq!(pull_action(&Ok(Some(allocation))), PullAction::Adopt(ByteRate::new(500_000)));
    }

    #[test]
    fn pull_action_decays_on_a_ttl_expired_allocation() {
        assert_eq!(pull_action(&Ok(None)), PullAction::Decay);
    }

    #[test]
    fn pull_action_decays_on_a_failed_pull() {
        // WI-6: an Err (e.g. a sustained redis-queues outage) must decay toward the floor
        // like silence, NOT hold the last budget and keep hammering a degraded Ceph.
        assert_eq!(pull_action(&Err(CoordError::Invalid { field: "test" })), PullAction::Decay);
    }

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
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn the_allocation_worker_applies_the_leaders_budget(pool: PgPool) {
        let Some(coord) = test_coord("cephor-test:rt-applies:", Duration::from_secs(30), Duration::from_secs(30)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let coord = Arc::new(coord);
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::from_pool(pool));
        let node = NodeId::from_str("node-alloc").unwrap();

        // The leader has already written this node a budget.
        coord
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
        let enforcer = Arc::new(Mutex::new(default_enforcer(ByteRate::new(0), Bytes::new(1 << 20), 4)));
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
                drain_concurrency: 4,
            },
        )
        .with_coordinator(Arc::clone(&coord))
        .with_rate_control(RateControl {
            enforcer: Arc::clone(&enforcer),
            node,
            floor: ByteRate::new(1_000),
            half_life: Duration::from_secs(30),
            poll: Duration::from_millis(20),
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
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn the_allocation_worker_decays_on_an_injected_clock(pool: PgPool) {
        // #33: run_alloc reads the injected Clock for its decay timing. Load a budget
        // (base jumps above the floor, allocated_at = clock.now()), then let the
        // allocation key's short Redis TTL expire so the allocator reads "silent", and
        // advance the TestClock past several half-lives. The enforcer rate must decay
        // toward the floor — proving the *injected* clock drives decay, since no real
        // time passes (on the test clock) between the load and the assertion.
        let Some(coord) = test_coord("cephor-test:rt-decay:", Duration::from_secs(30), Duration::from_secs(1)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let coord = Arc::new(coord);
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::from_pool(pool));
        let node = NodeId::from_str("node-decay").unwrap();
        let budget = ByteRate::new(800_000);
        let floor = ByteRate::new(1_000);

        // alloc_ttl is 1s (above), so this budget key expires ~1s after the write.
        coord.write_allocations(1, &[Allocation { node: node.clone(), budget }]).await.unwrap();

        let enforcer = Arc::new(Mutex::new(default_enforcer(ByteRate::new(0), Bytes::new(1 << 20), 4)));
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
                drain_concurrency: 4,
            },
        )
        .with_coordinator(Arc::clone(&coord))
        .with_rate_control(RateControl {
            enforcer: Arc::clone(&enforcer),
            node: node.clone(),
            floor,
            half_life: Duration::from_secs(1),
            poll: Duration::from_millis(10),
        })
        .with_clock(clock_source);

        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run(signal.cancelled_owned()).await });

        // The worker first pulls the budget into the enforcer. Generous window (~5s):
        // the allocation-pull worker shares a single-threaded runtime with the other
        // workers and can be starved under a full parallel test run.
        let mut loaded = false;
        for _ in 0..500 {
            if enforcer.lock().unwrap().rate() == budget {
                loaded = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(loaded, "the worker loaded the budget before decay");

        // Wait out the 1s alloc-key TTL so load_allocation reads None ("silent"). The
        // injected clock has NOT advanced yet, so allocated_at freezes at the last load
        // and elapsed stays 0 (no decay) until we jump the clock below.
        tokio::time::sleep(Duration::from_millis(1_300)).await;

        // Ten half-lives on the injected clock -> the rate collapses to ~floor. This
        // deep decay is reachable ONLY by the injected jump (the test clock advances
        // 10s while no real test-clock time passed), so a near-floor rate proves
        // run_alloc reads the injected clock, not the wall clock.
        clock.advance(Duration::from_secs(10));
        let near_floor = floor.get().saturating_mul(2);

        // Generous window (~5s) for the same reason: the rate is applied on the next
        // allocation-pull tick, which can lag under a starved parallel test run.
        let mut decayed = false;
        for _ in 0..500 {
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
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn the_heartbeat_worker_upserts_this_node_into_the_fleet(pool: PgPool) {
        let Some(coord) = test_coord("cephor-test:rt-hb:", Duration::from_secs(30), Duration::from_secs(30)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let coord = Arc::new(coord);
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
                drain_concurrency: 4,
            },
        )
        .with_coordinator(Arc::clone(&coord))
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
            let fleet = coord.load_fleet().await.unwrap();
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
                drain_concurrency: 4,
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
                drain_concurrency: 4,
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

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn reclaim_once_also_runs_the_orphan_temp_sweep(pool: PgPool) {
        // reclaim_once must do BOTH the failed-part reclaim AND the orphan-temp sweep.
        // An orphan temp in an incomplete (no-meta) part dir is invisible to the part
        // scan, so if it gets removed it can only be the sweep — proving the wiring.
        let ssd_dir = tempfile::tempdir().unwrap();
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();

        let part = part_at(9, 1);
        let part_dir = ssd_dir.path().join(part.relative_dir());
        std::fs::create_dir_all(&part_dir).unwrap();
        let orphan = part_dir.join("chunk_0.bin.tmp.0badf00d0badf00d");
        std::fs::write(&orphan, b"half-written").unwrap();

        let ssd = LocalSsd::new(ssd_dir.path());
        // Zero grace so the just-written temp is past the (no-)window.
        super::reclaim_once(&ssd, &store, &snapshot, Duration::ZERO).await;

        assert!(!orphan.exists(), "reclaim_once swept the orphan temp");
        assert_eq!(snapshot.load().reclaimed, 0, "no failed part -> nothing reclaimed, only the temp swept");
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
