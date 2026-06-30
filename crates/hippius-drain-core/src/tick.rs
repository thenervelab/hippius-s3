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
    /// This instance was leader; the plan was computed and already written. Its
    /// `controller` is the AIMD state to carry into the next tick.
    Led(AllocationPlan),
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
    Ok(TickOutcome::Led(plan))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr, reason = "tests")]
mod tests {
    use super::{StaticCeiling, TickConfig, TickOutcome, run_tick};
    use crate::alloc::{AllocConfig, Allocation, BudgetController, NodeObservation};
    use crate::coordination::{CoordError, Coordinator};
    use crate::ids::NodeId;
    use crate::state::CephCeiling;
    use crate::units::{ByteRate, Bytes, DiskPressure};
    use core::str::FromStr;
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
            target_p99: Duration::from_millis(50),
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
    async fn leader_allocates_and_writes_budgets() {
        let Some(c) = coord("cephor-test:tick-leads:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();

        let prev = BudgetController::new(ByteRate::new(190_000));
        let outcome = run_tick(&c, &open_ceiling(), &tick_config("alloc-a"), prev).await.unwrap();
        assert!(matches!(outcome, TickOutcome::Led(_)), "first instance leads");

        let stored = c.load_allocation(&node).await.unwrap().expect("budget written");
        assert_eq!(stored.epoch, 1, "stamped with the lease epoch");
        assert!(stored.budget.get() > 0, "the only hungry node received budget");
    }

    #[tokio::test]
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
    async fn a_second_instance_does_not_lead_or_allocate() {
        let Some(c) = coord("cephor-test:tick-second:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();
        let prev = BudgetController::new(ByteRate::new(190_000));

        let first = run_tick(&c, &open_ceiling(), &tick_config("a"), prev).await.unwrap();
        assert!(matches!(first, TickOutcome::Led(_)));

        let second = run_tick(&c, &open_ceiling(), &tick_config("b"), prev).await.unwrap();
        assert!(matches!(second, TickOutcome::NotLeader), "b cannot lead while a holds the lease");
    }
}
