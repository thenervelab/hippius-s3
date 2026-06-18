//! The allocator's per-tick control loop and the Ceph ceiling source.
//!
//! `run_tick` is the unit the `hippius-drain-allocator` binary calls on a timer: it
//! contends for leadership and, when leader, reads the fleet, gets the Ceph
//! ceiling, allocates (M2), and writes the budgets stamped with the lease epoch.

use crate::alloc::{AllocConfig, AllocationPlan, BudgetController, allocate};
use crate::state::CephCeiling;
use crate::store::{Lease, Store, StoreError};
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
    /// Heartbeats older than this are ignored when loading the fleet.
    pub stale_after: Duration,
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
/// without touching allocations. If leader, reads the fleet, gets the ceiling,
/// allocates from `prev`, and writes the budgets stamped with the lease epoch
/// (so a deposed leader's stale-epoch writes are fenced out by the store).
///
/// # Errors
///
/// [`StoreError`] if any database step fails.
pub async fn run_tick<C: CephCeilingSource>(
    store: &Store,
    ceiling_source: &C,
    config: &TickConfig,
    prev: BudgetController,
) -> Result<TickOutcome, StoreError> {
    let Some(Lease { epoch }) = store.acquire_or_renew_leadership(&config.instance_id, config.lease_ttl).await? else {
        return Ok(TickOutcome::NotLeader);
    };
    let fleet = store.load_fleet(config.stale_after).await?;
    let ceiling = ceiling_source.ceiling().await;
    let plan = allocate(&fleet, ceiling, prev, &config.alloc);
    store.write_allocations(epoch, &plan.allocations).await?;
    Ok(TickOutcome::Led(plan))
}

#[cfg(test)]
#[cfg(feature = "pg")]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::{StaticCeiling, TickConfig, TickOutcome, run_tick};
    use crate::alloc::{AllocConfig, Allocation, BudgetController, NodeObservation};
    use crate::ids::NodeId;
    use crate::state::CephCeiling;
    use crate::store::{Store, StoreError};
    use crate::units::{ByteRate, Bytes, DiskPressure};
    use core::str::FromStr;
    use sqlx::postgres::PgPool;
    use std::time::Duration;

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
            stale_after: Duration::from_hours(1),
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

    #[sqlx::test]
    async fn leader_allocates_and_writes_budgets(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("n1").unwrap();
        store.upsert_node_state(&node, &observation()).await.unwrap();

        let prev = BudgetController::new(ByteRate::new(190_000));
        let outcome = run_tick(&store, &open_ceiling(), &tick_config("alloc-a"), prev).await.unwrap();
        assert!(matches!(outcome, TickOutcome::Led(_)), "first instance leads");

        let stored = store
            .load_allocation(&node, Duration::from_hours(1))
            .await
            .unwrap()
            .expect("budget written");
        assert_eq!(stored.epoch, 1, "stamped with the lease epoch");
        assert!(stored.budget.get() > 0, "the only hungry node received budget");
    }

    #[sqlx::test]
    async fn a_deposed_leader_tick_surfaces_the_fence_not_a_false_led(pool: PgPool) {
        // M4: a leader deposed between acquiring its lease and writing (a higher
        // epoch already wrote this node's allocation) must NOT report Led for a tick
        // whose writes were discarded — the fence surfaces as StoreError::Fenced.
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("n1").unwrap();
        store.upsert_node_state(&node, &observation()).await.unwrap();
        store
            .write_allocations(
                100,
                std::slice::from_ref(&Allocation {
                    node: node.clone(),
                    budget: ByteRate::new(1),
                }),
            )
            .await
            .unwrap();

        // This instance takes a fresh lease (epoch 1) but its write is fenced by the
        // epoch-100 row, so run_tick surfaces the fence rather than a false Led.
        let prev = BudgetController::new(ByteRate::new(190_000));
        let err = run_tick(&store, &open_ceiling(), &tick_config("late"), prev).await.unwrap_err();
        assert!(
            matches!(err, StoreError::Fenced { epoch: 1 }),
            "a deposed tick surfaces the fence, got {err:?}"
        );
    }

    #[sqlx::test]
    async fn a_second_instance_does_not_lead_or_allocate(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("n1").unwrap();
        store.upsert_node_state(&node, &observation()).await.unwrap();
        let prev = BudgetController::new(ByteRate::new(190_000));

        let first = run_tick(&store, &open_ceiling(), &tick_config("a"), prev).await.unwrap();
        assert!(matches!(first, TickOutcome::Led(_)));

        let second = run_tick(&store, &open_ceiling(), &tick_config("b"), prev).await.unwrap();
        assert!(matches!(second, TickOutcome::NotLeader), "b cannot lead while a holds the lease");
    }
}
