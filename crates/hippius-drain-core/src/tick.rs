//! The allocator's per-tick control loop and the Ceph ceiling source.
//!
//! `run_tick` is the unit the `hippius-drain-allocator` binary calls on a timer: it
//! contends for leadership and, when leader, reads the fleet, gets the Ceph
//! ceiling, allocates (M2), and writes the budgets stamped with the lease epoch.
//! Leadership, the fleet view, and the budgets all live in the Redis-backed
//! [`Coordinator`] now (not Postgres) — see its module docs for the rationale.

use crate::alloc::{AllocConfig, AllocationPlan, BudgetController, allocate};
use crate::coordination::{CoordError, Coordinator, Lease};
use crate::state::CephCeiling;
use std::time::Duration;

/// A source of the current Ceph write-ceiling.
///
/// Implementations fold their own failures into a conservative ceiling (e.g.
/// `NearFull`/`Critical`) rather than erroring, so a tick always has a ceiling
/// and a probe outage degrades to back-off, never to a panic.
pub trait CephCeilingSource {
    /// The current ceiling.
    fn ceiling(&self) -> impl std::future::Future<Output = CephCeiling> + Send;
}

/// A fixed ceiling source — a safe operational default and the test double.
///
/// The Ceph-mgr REST source (OSD near-full + MDS load over `reqwest`) is a
/// future addition behind an `http` feature: it requires a live Ceph cluster to
/// build/verify against, so it is deferred rather than shipped untested.
#[derive(Debug, Clone, Copy)]
pub struct StaticCeiling(pub CephCeiling);

impl CephCeilingSource for StaticCeiling {
    async fn ceiling(&self) -> CephCeiling {
        self.0
    }
}

/// Inputs to one allocation tick.
#[derive(Debug, Clone)]
pub struct TickConfig {
    /// This allocator instance's identity (the lease holder id).
    pub instance_id: String,
    /// How long an acquired lease stays valid.
    pub lease_ttl: Duration,
    /// Allocation tuning.
    pub alloc: AllocConfig,
}

/// The result of one tick.
#[derive(Debug, Clone)]
pub enum TickOutcome {
    /// This instance was leader; the plan was computed and already written under `epoch`
    /// (the lease epoch stamped on the write). The plan's `controller` is the AIMD state
    /// to carry into the next tick; `epoch` feeds the `drain_leader_epoch` gauge, so a
    /// fleet-wide epoch DECREASE — a redis epoch-counter reset that would silently break
    /// the allocation write-fence — is observable and alertable (S10/S11).
    Led {
        /// The allocation plan written this tick (carries the evolved AIMD controller).
        plan: AllocationPlan,
        /// The lease epoch this tick led under. Monotonic across the fleet by
        /// construction (`INCR` at election).
        epoch: u64,
    },
    /// Another instance holds leadership; nothing was allocated this tick.
    NotLeader,
}

/// Runs one allocation tick.
///
/// Contends for leadership; if not leader, returns [`TickOutcome::NotLeader`]
/// without touching allocations. If leader, reads the fleet (heartbeats that have
/// not TTL-expired), gets the ceiling, allocates from `prev`, and writes the
/// budgets stamped with the lease epoch (so a deposed leader's stale-epoch writes
/// are fenced out by the coordinator).
///
/// # Errors
///
/// [`CoordError`] if any coordination step fails — including [`CoordError::Fenced`]
/// when this instance was deposed between acquiring its lease and writing.
pub async fn run_tick<C: CephCeilingSource>(
    coord: &Coordinator,
    ceiling_source: &C,
    config: &TickConfig,
    prev: BudgetController,
) -> Result<TickOutcome, CoordError> {
    let Some(Lease { epoch }) = coord.acquire_or_renew_leadership(&config.instance_id, config.lease_ttl).await? else {
        return Ok(TickOutcome::NotLeader);
    };
    let fleet = coord.load_fleet().await?;
    let ceiling = ceiling_source.ceiling().await;
    let plan = allocate(&fleet, ceiling, prev, &config.alloc);
    coord.write_allocations(epoch, &plan.allocations).await?;
    Ok(TickOutcome::Led { plan, epoch })
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr, clippy::panic, reason = "tests")]
mod tests {
    use super::{CephCeilingSource, StaticCeiling, TickConfig, TickOutcome, run_tick};
    use crate::alloc::{AllocConfig, Allocation, BudgetController, NodeObservation};
    use crate::coordination::{CoordError, Coordinator};
    use crate::ids::NodeId;
    use crate::state::CephCeiling;
    use crate::units::{ByteRate, Bytes, DiskPressure};
    use core::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    /// A coordinator on the test Redis under a per-test prefix, or `None` to skip (Rust
    /// has no fakeredis; set `CEPHOR_TEST_REDIS_URL=redis://localhost:6382/0`).
    async fn coord(prefix: &str) -> Option<Coordinator> {
        let url = std::env::var("CEPHOR_TEST_REDIS_URL").ok().filter(|value| !value.is_empty())?;
        // Prefix with the process id so a re-run never collides with the prior run's
        // still-TTL'd keys on a shared test Redis.
        let coordinator = Coordinator::connect(&url, Duration::from_secs(30), Duration::from_secs(30))
            .await
            .expect("connect to the test redis")
            .with_prefix(&format!("{}:{prefix}", std::process::id()));
        Some(coordinator)
    }

    fn alloc_config() -> AllocConfig {
        AllocConfig {
            min_total: ByteRate::new(1_000),
            max_total: ByteRate::new(1_000_000_000),
            additive_increase: ByteRate::new(10_000),
            decrease_permille: 800,
            target_p99: Duration::from_secs(2),
            max_error_bps: 100,
            critical_pressure: DiskPressure::try_from(9_000).unwrap(),
            reservation_floor: ByteRate::new(50_000),
        }
    }

    fn tick_config(instance: &str) -> TickConfig {
        TickConfig {
            instance_id: instance.to_owned(),
            lease_ttl: Duration::from_secs(30),
            alloc: alloc_config(),
        }
    }

    fn observation() -> NodeObservation {
        NodeObservation {
            pressure: DiskPressure::try_from(5_000).unwrap(),
            backlog: Bytes::new(9_000_000),
            max_drain_rate: ByteRate::new(5_000_000),
            observed_p99: Duration::from_millis(10),
            error_bps: 0,
        }
    }

    fn open_ceiling() -> StaticCeiling {
        StaticCeiling(CephCeiling::Open(ByteRate::new(1_000_000_000)))
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn leader_allocates_and_writes_budgets() {
        let Some(c) = coord("cephor-test:tick-leads:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();

        let prev = BudgetController::new(ByteRate::new(190_000));
        let outcome = run_tick(&c, &open_ceiling(), &tick_config("alloc-a"), prev).await.unwrap();
        let TickOutcome::Led { epoch, .. } = outcome else {
            panic!("first instance leads, got {outcome:?}");
        };
        assert_eq!(epoch, 1, "Led surfaces the lease epoch it led under");

        let stored = c.load_allocation(&node).await.unwrap().expect("budget written");
        assert_eq!(stored.epoch, 1, "stamped with the lease epoch");
        assert!(stored.budget.get() > 0, "the only hungry node received budget");
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn a_deposed_leader_tick_surfaces_the_fence_not_a_false_led() {
        // M4: a leader deposed between acquiring its lease and writing (a higher epoch
        // already wrote this node's allocation) must NOT report Led for a tick whose
        // writes were discarded — the fence surfaces as CoordError::Fenced.
        let Some(c) = coord("cephor-test:tick-fence:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();
        c.write_allocations(
            100,
            std::slice::from_ref(&Allocation {
                node: node.clone(),
                budget: ByteRate::new(1),
            }),
        )
        .await
        .unwrap();

        // This instance takes a fresh lease (epoch 1) but its write is fenced by the
        // epoch-100 allocation, so run_tick surfaces the fence rather than a false Led.
        let prev = BudgetController::new(ByteRate::new(190_000));
        let err = run_tick(&c, &open_ceiling(), &tick_config("late"), prev).await.unwrap_err();
        assert!(
            matches!(err, CoordError::Fenced { epoch: 1 }),
            "a deposed tick surfaces the fence, got {err:?}"
        );
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn a_second_instance_does_not_lead_or_allocate() {
        let Some(c) = coord("cephor-test:tick-second:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();
        let prev = BudgetController::new(ByteRate::new(190_000));

        let first = run_tick(&c, &open_ceiling(), &tick_config("a"), prev).await.unwrap();
        assert!(matches!(first, TickOutcome::Led { .. }));

        let second = run_tick(&c, &open_ceiling(), &tick_config("b"), prev).await.unwrap();
        assert!(matches!(second, TickOutcome::NotLeader), "b cannot lead while a holds the lease");
    }

    /// A ceiling source that parks at `ceiling()` — the slow step in the R3 window — until
    /// released, signalling when it is reached so a test can interleave a takeover precisely
    /// there. Interior-shared via `Arc<AtomicBool>` because `ceiling(&self)` takes `&self`.
    struct PausingCeiling {
        ceiling: CephCeiling,
        reached: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
    }

    impl CephCeilingSource for PausingCeiling {
        async fn ceiling(&self) -> CephCeiling {
            self.reached.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            self.ceiling
        }
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn r3_a_tick_paused_at_ceiling_is_fenced_when_a_successor_takes_over_mid_tick() {
        // R3 at the tick level (T4-C failpoint): a leader deposed BETWEEN acquiring its lease
        // and writing — while parked in the slow `ceiling()` — must have its write fenced even
        // though the successor has bumped `cephor:epoch` but written NO alloc key yet. This is
        // the exact window `run_tick` opens (acquire → load_fleet → ceiling[SLOW] → write) and
        // that a per-alloc-key fence alone cannot see.
        let Some(c) = coord("cephor-test:r3-tick-window:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        // A hungry node so the plan is non-empty and `write_allocations` actually runs the
        // fenced script (an empty plan is a no-op Ok, which would not exercise the fence).
        c.upsert_node_state(&node, &observation()).await.unwrap();

        let reached = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let ceiling = PausingCeiling {
            ceiling: CephCeiling::Open(ByteRate::new(1_000_000_000)),
            reached: Arc::clone(&reached),
            release: Arc::clone(&release),
        };
        // A takes a SHORT lease so it lapses while parked in `ceiling()`.
        let config = TickConfig {
            instance_id: "a".to_owned(),
            lease_ttl: Duration::from_secs(1),
            alloc: alloc_config(),
        };
        let prev = BudgetController::new(ByteRate::new(190_000));
        let c_a = c.clone();
        let a_tick = tokio::spawn(async move { run_tick(&c_a, &ceiling, &config, prev).await });

        // Wait until A is parked inside `ceiling()` (lease acquired at epoch eA, no write yet).
        while !reached.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Let A's 1s lease lapse, then B takes over: `cephor:epoch` bumps to eB > eA. B does NOT
        // write any allocation (it is modelling a successor still early in its own tick).
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let b = c
            .acquire_or_renew_leadership("b", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("b takes over the lapsed lease");

        // Release A: it finishes `ceiling()` and tries to write at its now-stale epoch eA.
        release.store(true, Ordering::SeqCst);
        let outcome = a_tick.await.unwrap();
        assert!(
            matches!(outcome, Err(CoordError::Fenced { .. })),
            "the deposed A's mid-tick write is fenced (b.epoch={}), got {outcome:?}",
            b.epoch,
        );
        // Nothing was written at the stale epoch: no alloc key exists (A fenced, B never wrote).
        assert!(c.load_allocation(&node).await.unwrap().is_none(), "the fenced tick wrote no budget");
    }
}
