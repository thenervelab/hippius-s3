//! T4 rig B — allocator failover soak under leadership churn.
//!
//! Complements the DETERMINISTIC R3 failpoint proof (the
//! `r3_a_tick_paused_at_ceiling_is_fenced_when_a_successor_takes_over_mid_tick` unit test in
//! `hippius-drain-core::tick`, which isolates the exact stale-write window). This rig instead
//! hammers real concurrent failover at scale: N allocator loops contend for one coordinator on
//! the test Redis, and leadership is CHURNED many times (kill the current leader, respawn it).
//! Each round it asserts the fleet **re-converges on a new leader that advances the epoch**,
//! the landed budget's epoch **never rolls backward**, and every allocator **shuts down
//! cleanly** — i.e. no deadlock, no lost leadership, and no split-brain rollback under churn.
//!
//! NOTE: this rig does not, on its own, isolate the R3 *current-era* fence — the per-alloc-key
//! fence already prevents an alloc-key epoch rollback, so the failpoint unit test is what proves
//! the R3-specific window. This rig is the load/failover soak around it.
//!
//! Needs a live Redis (Rust has no fakeredis): `CEPHOR_TEST_REDIS_URL=redis://localhost:6382/0`.
//! Ignored by default; run with `cargo test -p hippius-drain-allocator --test alloc_stress -- --include-ignored`.
//! The toxiproxy redis-pathology cells (Rig A) run against the compose proxy — validated at
//! integration, not here.

#![expect(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr, reason = "integration test")]

use hippius_drain_allocator::run::{AllocatorMetrics, Observability, run_allocator};
use hippius_drain_core::{AllocConfig, ByteRate, Bytes, CephCeiling, Coordinator, DiskPressure, NodeId, NodeObservation, StaticCeiling, TickConfig};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const ALLOCATORS: usize = 4;
const ROUNDS: usize = 10;
const LEASE: Duration = Duration::from_millis(500);
const TICK: Duration = Duration::from_millis(25);
const INITIAL: ByteRate = ByteRate::new(190_000);

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
        base_reserve_permille: 150,
        max_reserve_permille: 400,
    }
}

fn observation() -> NodeObservation {
    NodeObservation {
        pressure: DiskPressure::try_from(5_000).unwrap(),
        backlog: Bytes::new(9_000_000),
        free: Bytes::ZERO,
        cache: Bytes::ZERO,
        max_drain_rate: ByteRate::new(5_000_000),
        observed_p99: Duration::from_millis(10),
        error_bps: 0,
    }
}

/// One running allocator instance: its cancellation handle, its `leader` gauge, and the
/// task's join handle (so a churn round can kill it and wait for it to wind down).
struct Instance {
    token: CancellationToken,
    metrics: Arc<AllocatorMetrics>,
    handle: JoinHandle<Result<(), hippius_drain_core::CoordError>>,
}

/// Spawns one allocator loop. `id` is unique per (slot, respawn) so a restarted instance is
/// a logically fresh process (never renews a prior era's lease).
fn spawn_allocator(coord: &Coordinator, id: String) -> Instance {
    let coord = coord.clone(); // Coordinator: Clone shares the multiplexed connection.
    let token = CancellationToken::new();
    let metrics = Arc::new(AllocatorMetrics::default());
    let task_token = token.clone();
    let task_metrics = Arc::clone(&metrics);
    let handle = tokio::spawn(async move {
        let config = TickConfig {
            instance_id: id,
            lease_ttl: LEASE,
            alloc: alloc_config(),
        };
        let ceiling = StaticCeiling(CephCeiling::Open(ByteRate::new(1_000_000_000)));
        let obs = Observability {
            liveness: None,
            metrics: &task_metrics,
        };
        run_allocator(&coord, &ceiling, &config, INITIAL, TICK, &obs, task_token.cancelled()).await
    });
    Instance { token, metrics, handle }
}

/// The epoch currently stamped on the node's landed allocation, or 0 if none written yet.
async fn landed_epoch(coord: &Coordinator, node: &NodeId) -> u64 {
    coord.load_allocation(node).await.unwrap().map_or(0, |allocation| allocation.epoch)
}

/// Polls `f` until it returns `Some`, or `None` after `timeout`. Sleeps `TICK` between tries.
async fn poll_until<T, F, Fut>(timeout: Duration, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Duration::from_millis(u64::try_from(timeout.as_millis()).unwrap());
    let mut waited = Duration::ZERO;
    loop {
        if let Some(value) = f().await {
            return Some(value);
        }
        if waited >= deadline {
            return None;
        }
        tokio::time::sleep(TICK).await;
        waited += TICK;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires CEPHOR_TEST_REDIS_URL"]
async fn many_allocators_churning_leadership_never_roll_the_epoch_backward() {
    let Ok(url) = std::env::var("CEPHOR_TEST_REDIS_URL") else {
        eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
        return;
    };
    if url.is_empty() {
        eprintln!("skipping: CEPHOR_TEST_REDIS_URL empty");
        return;
    }
    // Unique prefix per process so a re-run never collides with the prior run's TTL'd keys.
    let coord = Coordinator::connect(&url, Duration::from_secs(30), Duration::from_secs(30))
        .await
        .expect("connect to the test redis")
        .with_prefix(&format!("cephor-test:alloc-stress:{}:", std::process::id()));

    let node = NodeId::try_from("n1".to_owned()).unwrap();
    coord.upsert_node_state(&node, &observation()).await.unwrap();

    // Start the fleet.
    let mut fleet: Vec<Instance> = (0..ALLOCATORS).map(|slot| spawn_allocator(&coord, format!("alloc-{slot}-r0"))).collect();

    // Wait for the fleet to converge on a first leader + a first written budget.
    let mut high_epoch = poll_until(Duration::from_secs(5), || async {
        let epoch = landed_epoch(&coord, &node).await;
        (epoch > 0).then_some(epoch)
    })
    .await
    .expect("the fleet elected a leader and wrote a budget");

    // Churn: each round, kill the current leader and respawn it, then wait for a successor
    // to take over (the landed epoch advances). Assert monotonicity at every observation.
    for round in 1..=ROUNDS {
        // Find the slot currently reporting leadership.
        let leader_slot = poll_until(Duration::from_secs(3), || async {
            (0..ALLOCATORS).find(|&slot| fleet[slot].metrics.leader.load(Ordering::Relaxed) == 1)
        })
        .await
        .expect("some instance holds leadership");

        let before = landed_epoch(&coord, &node).await;
        assert!(before >= high_epoch, "epoch rolled back before a churn: {before} < {high_epoch}");
        high_epoch = before;

        // Replace the leader slot with a fresh instance, then cancel + await the old one so
        // it relinquishes its lease before we wait for the successor to take over.
        let killed = std::mem::replace(&mut fleet[leader_slot], spawn_allocator(&coord, format!("alloc-{leader_slot}-r{round}")));
        killed.token.cancel();
        let _ = killed.handle.await;

        // A successor must take over and advance the epoch (a takeover = a fresh higher era).
        let advanced = poll_until(Duration::from_secs(5), || async {
            let epoch = landed_epoch(&coord, &node).await;
            assert!(epoch >= high_epoch, "epoch rolled BACK during a takeover: {epoch} < {high_epoch}");
            (epoch > before).then_some(epoch)
        })
        .await
        .expect("a successor took over and advanced the epoch");
        assert!(advanced > high_epoch, "the takeover produced a strictly higher epoch");
        high_epoch = advanced;
    }

    // Churn happened: the epoch advanced at least once per round.
    assert!(
        high_epoch >= u64::try_from(ROUNDS).unwrap(),
        "expected the epoch to advance through the churn (>= {ROUNDS}), got {high_epoch}",
    );

    // Wind the fleet down; every allocator exits Ok (per-tick errors never propagate).
    for instance in fleet {
        instance.token.cancel();
        instance.handle.await.unwrap().unwrap();
    }
}
