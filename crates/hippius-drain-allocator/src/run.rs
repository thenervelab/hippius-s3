//! The allocator's tick loop: drive [`run_tick`] on a timer until shutdown.

use hippius_drain_core::{BudgetController, ByteRate, CephCeilingSource, Store, StoreError, TickConfig, TickOutcome, run_tick};
use std::future::Future;
use std::time::Duration;

/// Runs the allocator's tick loop until `shutdown` resolves.
///
/// Each tick contends for leadership and, when leader, reads the fleet, gets the
/// ceiling, allocates from the carried AIMD state, and writes the budgets stamped
/// with the lease epoch (see [`run_tick`]). The evolved controller is carried into
/// the next tick so the fleet estimate persists across ticks.
///
/// A fenced write — a higher-epoch leader took over between this instance's lease
/// acquire and its write — is the designed leadership handoff, logged at info and
/// ignored (the next tick observes `NotLeader`). Any other per-tick store error is
/// logged and retried on the next tick, never propagated, so a transient database
/// blip does not end the allocator; the lease TTL covers a truly dead one.
///
/// On shutdown it relinquishes leadership so a successor leads on its next tick
/// rather than waiting out the lease TTL.
///
/// # Errors
///
/// [`StoreError`] only from the final best-effort relinquish
/// ([`Store::relinquish_leadership`]); per-tick errors never propagate.
pub async fn run_allocator<C: CephCeilingSource>(
    store: &Store,
    ceiling: &C,
    config: &TickConfig,
    initial: ByteRate,
    tick: Duration,
    shutdown: impl Future<Output = ()>,
) -> Result<(), StoreError> {
    let mut controller = BudgetController::new(initial);
    tokio::pin!(shutdown);
    loop {
        match run_tick(store, ceiling, config, controller).await {
            Ok(TickOutcome::Led(plan)) => {
                tracing::debug!(
                    fleet_estimate = plan.controller.total().get(),
                    feasible = plan.feasible,
                    nodes = plan.allocations.len(),
                    "led allocation tick"
                );
                // Carry the evolved AIMD state into the next tick.
                controller = plan.controller;
            }
            Ok(TickOutcome::NotLeader) => tracing::debug!("another instance holds leadership this tick"),
            // The store fenced this instance's stale-epoch write: a higher-epoch
            // leader exists, so this instance is deposed. Expected handoff, not an
            // alert. Keep the carried controller so a re-win resumes from a sane rate.
            Err(StoreError::Fenced { epoch }) => tracing::info!(epoch, "allocation fenced; leadership has moved on"),
            Err(err) => tracing::warn!(error = %err, "allocation tick failed; retrying next tick"),
        }
        tokio::select! {
            () = &mut shutdown => break,
            () = tokio::time::sleep(tick) => {}
        }
    }
    // Best-effort handoff; the lease TTL is the backstop if it fails (the caller
    // logs rather than failing shutdown — see Store::relinquish_leadership).
    store.relinquish_leadership(&config.instance_id).await
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::run_allocator;
    use core::str::FromStr;
    use hippius_drain_core::{
        AllocConfig, Allocation, ByteRate, Bytes, CephCeiling, DiskPressure, Lease, NodeId, NodeObservation, StaticCeiling, Store, TickConfig,
    };
    use sqlx::postgres::PgPool;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

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

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn run_allocator_leads_writes_a_budget_then_relinquishes(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("n1").unwrap();
        store.upsert_node_state(&node, &observation()).await.unwrap();

        let config = tick_config("alloc-1");
        let ceiling = open_ceiling();
        let shutdown = CancellationToken::new();

        // Run the loop concurrently with a driver that waits for a budget to land,
        // then signals shutdown — joined (not spawned) so `&store` is shared by ref.
        let runner = run_allocator(
            &store,
            &ceiling,
            &config,
            ByteRate::new(190_000),
            Duration::from_millis(10),
            shutdown.cancelled(),
        );
        let driver = async {
            let mut written = false;
            for _ in 0..200 {
                if store.load_allocation(&node, Duration::from_hours(1)).await.unwrap().is_some() {
                    written = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            shutdown.cancel();
            written
        };
        let (run_result, written) = tokio::join!(runner, driver);
        run_result.unwrap();
        assert!(written, "the leader wrote this node a budget");

        // Leadership was relinquished on shutdown: the lease row is gone, so a fresh
        // instance acquires it immediately at epoch 1 rather than waiting the TTL.
        // (Were it NOT relinquished, alloc-1's unexpired 30s lease would deny this.)
        assert_eq!(
            store.acquire_or_renew_leadership("successor", Duration::from_secs(30)).await.unwrap(),
            Some(Lease { epoch: 1 }),
            "shutdown relinquished leadership, so a successor leads immediately",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn run_allocator_survives_a_fenced_tick(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("n1").unwrap();
        store.upsert_node_state(&node, &observation()).await.unwrap();
        // A higher-epoch leader already wrote this node's allocation, so every tick
        // this instance runs is fenced when it writes its epoch-1 budget.
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

        let config = tick_config("late");
        let ceiling = open_ceiling();
        let shutdown = CancellationToken::new();

        // The loop must survive the fenced ticks (logged, not fatal) and shut down
        // cleanly — run a few ticks, then cancel.
        let runner = run_allocator(
            &store,
            &ceiling,
            &config,
            ByteRate::new(190_000),
            Duration::from_millis(10),
            shutdown.cancelled(),
        );
        let driver = async {
            tokio::time::sleep(Duration::from_millis(60)).await;
            shutdown.cancel();
        };
        let (run_result, ()) = tokio::join!(runner, driver);
        run_result.unwrap();

        // The epoch-100 allocation is untouched: the fence held every tick.
        let stored = store.load_allocation(&node, Duration::from_hours(1)).await.unwrap().unwrap();
        assert_eq!(stored.epoch, 100, "the higher-epoch allocation was never overwritten");
    }
}
