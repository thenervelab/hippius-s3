//! The allocator's tick loop: drive [`run_tick`] on a timer until shutdown.

use hippius_drain_core::{BudgetController, ByteRate, CephCeilingSource, CoordError, Coordinator, TickConfig, TickOutcome, run_tick};
use std::future::Future;
use std::path::Path;
use std::time::Duration;

/// Best-effort liveness touch (see the agent runtime for the rationale): rewriting the
/// file bumps its mtime, which the k8s exec probe checks for freshness. Errors are
/// ignored — a persistent failure ages the mtime out and the probe restarts the pod.
fn touch_liveness(path: &Path) {
    let _ = std::fs::write(path, b"ok");
}

/// Runs the allocator's tick loop until `shutdown` resolves.
///
/// Each tick contends for leadership and, when leader, reads the fleet, gets the
/// ceiling, allocates from the carried AIMD state, and writes the budgets stamped
/// with the lease epoch (see [`run_tick`]). The evolved controller is carried into
/// the next tick so the fleet estimate persists across ticks.
///
/// A fenced write — a higher-epoch leader took over between this instance's lease
/// acquire and its write — is the designed leadership handoff, logged at info and
/// ignored (the next tick observes `NotLeader`). Any other per-tick coordination error
/// is logged and retried on the next tick, never propagated, so a transient Redis blip
/// does not end the allocator; the lease TTL covers a truly dead one.
///
/// On shutdown it relinquishes leadership so a successor leads on its next tick
/// rather than waiting out the lease TTL.
///
/// # Errors
///
/// [`CoordError`] only from the final best-effort relinquish
/// ([`Coordinator::relinquish_leadership`]); per-tick errors never propagate.
pub async fn run_allocator<C: CephCeilingSource>(
    coord: &Coordinator,
    ceiling: &C,
    config: &TickConfig,
    initial: ByteRate,
    tick: Duration,
    liveness: Option<&Path>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), CoordError> {
    let mut controller = BudgetController::new(initial);
    tokio::pin!(shutdown);
    loop {
        // Touch liveness each tick (the tick is bounded and quick) so a k8s probe
        // restarts a wedged — not crashed — loop, e.g. one blocked on a hung Redis.
        if let Some(path) = liveness {
            touch_liveness(path);
        }
        match run_tick(coord, ceiling, config, controller).await {
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
            // The coordinator fenced this instance's stale-epoch write: a higher-epoch
            // leader exists, so this instance is deposed. Expected handoff, not an
            // alert. Keep the carried controller so a re-win resumes from a sane rate.
            Err(CoordError::Fenced { epoch }) => tracing::info!(epoch, "allocation fenced; leadership has moved on"),
            Err(err) => tracing::warn!(error = %err, "allocation tick failed; retrying next tick"),
        }
        tokio::select! {
            () = &mut shutdown => break,
            () = tokio::time::sleep(tick) => {}
        }
    }
    // Best-effort handoff; the lease TTL is the backstop if it fails (the caller
    // logs rather than failing shutdown — see Coordinator::relinquish_leadership).
    coord.relinquish_leadership(&config.instance_id).await
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr, reason = "tests")]
mod tests {
    use super::run_allocator;
    use core::str::FromStr;
    use hippius_drain_core::{
        AllocConfig, Allocation, ByteRate, Bytes, CephCeiling, Coordinator, DiskPressure, NodeId, NodeObservation, StaticCeiling, TickConfig,
    };
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

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
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn run_allocator_leads_writes_a_budget_then_relinquishes() {
        let Some(c) = coord("cephor-test:run-leads:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();

        let config = tick_config("alloc-1");
        let ceiling = open_ceiling();
        let shutdown = CancellationToken::new();

        // Run the loop concurrently with a driver that waits for a budget to land,
        // then signals shutdown — joined (not spawned) so `&c` is shared by ref.
        let runner = run_allocator(
            &c,
            &ceiling,
            &config,
            ByteRate::new(190_000),
            Duration::from_millis(10),
            None,
            shutdown.cancelled(),
        );
        let driver = async {
            let mut written = false;
            for _ in 0..200 {
                if c.load_allocation(&node).await.unwrap().is_some() {
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

        // Leadership was relinquished on shutdown: the lease key is gone, so a fresh
        // instance acquires it immediately rather than waiting the TTL.
        // (Were it NOT relinquished, alloc-1's unexpired 30s lease would deny this.)
        assert!(
            c.acquire_or_renew_leadership("successor", Duration::from_secs(30))
                .await
                .unwrap()
                .is_some(),
            "shutdown relinquished leadership, so a successor leads immediately",
        );
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn run_allocator_survives_a_fenced_tick() {
        let Some(c) = coord("cephor-test:run-fenced:").await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();
        // A higher-epoch leader already wrote this node's allocation, so every tick
        // this instance runs is fenced when it writes its epoch-1 budget.
        c.write_allocations(
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
            &c,
            &ceiling,
            &config,
            ByteRate::new(190_000),
            Duration::from_millis(10),
            None,
            shutdown.cancelled(),
        );
        let driver = async {
            tokio::time::sleep(Duration::from_millis(60)).await;
            shutdown.cancel();
        };
        let (run_result, ()) = tokio::join!(runner, driver);
        run_result.unwrap();

        // The epoch-100 allocation is untouched: the fence held every tick.
        let stored = c.load_allocation(&node).await.unwrap().unwrap();
        assert_eq!(stored.epoch, 100, "the higher-epoch allocation was never overwritten");
    }
}
