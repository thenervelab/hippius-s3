//! hippius-drain-agent: the per-node drain daemon entry point.
//!
//! Parses [`Config`] from the environment, connects the central Postgres store,
//! assembles the [`AgentRuntime`], and runs it until SIGTERM/SIGINT. The drain
//! trigger is the reconciler (a poll-driven SSD scan), so there is no NOTIFY
//! listener to wire. Schema provisioning (the hippius-drain-core migrations) is a
//! separate deploy step, so the daemon needs no DDL rights.

use hippius_drain_agent::config::{Config, ConfigError};
use hippius_drain_agent::enqueue::RedisEnqueuer;
use hippius_drain_agent::localfs::{LocalFs, LocalSsd};
use hippius_drain_agent::runtime::{AgentRuntime, RateControl, default_enforcer};
use hippius_drain_agent::supervisor::{RunReport, ShutdownTrigger};
use hippius_drain_core::{Bytes, Coordinator, DEFAULT_REDIS_TIMEOUT, Store, StoreError};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

/// The agent never writes allocation keys (the allocator owns that key's TTL), so the
/// alloc TTL on its shared coordinator is never read — a placeholder for the constructor.
const AGENT_ALLOC_TTL_UNUSED: Duration = Duration::from_secs(15);

/// A failure bringing the daemon up. Each variant maps to a distinct operator
/// fix: a bad environment versus an unreachable store or Redis.
#[derive(Debug, Error)]
enum StartupError {
    #[error("invalid configuration")]
    Config(#[from] ConfigError),
    #[error("cannot connect to the state store")]
    Store(#[from] StoreError),
    #[error("cannot connect to the upload-queue Redis")]
    Redis(#[from] redis::RedisError),
}

/// Whether a supervised run ended in a fault — a worker exited during normal
/// operation (panic / early return), or a straggler had to be force-aborted. A
/// clean `Signal`/`NoWorkers` shutdown is not a fault. The process maps this to a
/// non-zero exit so Kubernetes restarts the pod and the crash is alertable,
/// rather than an exit-0 that reads as a clean SIGTERM.
fn faulted(report: &RunReport) -> bool {
    matches!(report.trigger, ShutdownTrigger::WorkerExited(_)) || !report.clean
}

#[tokio::main]
async fn main() -> Result<ExitCode, StartupError> {
    init_tracing();

    let config = Config::from_env()?;
    // Honor the configured claim lease so a crashed/partitioned agent's claim is
    // re-claimable after the TTL (the H1 crash-recovery path) instead of the
    // store's hardcoded default.
    // Scope claims/records to this node: parts live on node-local SSD, so this agent
    // may only drain parts it holds (otherwise it claims a peer's part and fails on
    // the missing local source). See migration 0006 + Store::with_node_id.
    let store = Arc::new(
        Store::connect(&config.database_url)
            .await?
            .with_claim_lease(config.claim_lease)
            .with_defer_backoff(config.defer_backoff)
            .with_node_id(config.node_id.as_str()),
    );

    // Drain-direct: the drain LPUSHes each replicated part's UploadChainRequest to the
    // upload-queue Redis (the api no longer enqueues at PUT). A multiplexed, auto-
    // reconnecting manager shared across the concurrent drains AND the coordinator below
    // (same redis-queues instance — one connection, cloned).
    // Bound every command on the shared manager (coordinator + enqueuer) with a response
    // timeout so a hung redis-queues surfaces as a retryable error rather than wedging a
    // worker loop; the connection timeout bounds the manager's reconnect after a blip.
    let redis_config = redis::aio::ConnectionManagerConfig::new()
        .set_response_timeout(DEFAULT_REDIS_TIMEOUT)
        .set_connection_timeout(DEFAULT_REDIS_TIMEOUT);
    let redis = redis::aio::ConnectionManager::new_with_config(redis::Client::open(config.redis_queues_url.as_str())?, redis_config).await?;
    let enqueuer = Arc::new(RedisEnqueuer::new(Arc::clone(&store), redis.clone(), config.upload_backends.clone()));

    // The Redis-backed coordinator: the heartbeat worker upserts this node's state under
    // `heartbeat_ttl`, and the allocation-pull worker reads its budget. The agent never
    // writes allocations, so its alloc TTL is unused (the allocator owns that key's TTL).
    let coordinator = Arc::new(Coordinator::new(redis, config.heartbeat_ttl, AGENT_ALLOC_TTL_UNUSED));

    // The enforcer starts at the floor (conservative) with a one-second burst of
    // the node's capability; the allocation-pull worker raises it to the leader's
    // budget on its first tick.
    let enforcer = default_enforcer(config.floor_rate, Bytes::new(config.max_drain_rate.get()), config.drain_concurrency);
    let rate_control = RateControl {
        enforcer: Arc::new(Mutex::new(enforcer)),
        node: config.node_id.clone(),
        floor: config.floor_rate,
        half_life: config.decay_half_life,
        poll: config.allocation_poll,
    };

    let runtime = AgentRuntime::new(
        Arc::new(LocalFs::new(&config.pool_root)),
        Arc::new(LocalSsd::new(&config.ssd_root)),
        store,
        enqueuer,
        config.runtime_config(),
    )
    .with_coordinator(coordinator)
    .with_heartbeat(config.heartbeat_config())
    .with_rate_control(rate_control)
    .with_liveness(config.liveness_file.clone());

    tracing::info!(
        pool_root = %config.pool_root.display(),
        ssd_root = %config.ssd_root.display(),
        "hippius-drain-agent started"
    );
    let report = runtime.run(shutdown_signal()).await;
    tracing::info!(trigger = ?report.trigger, clean = report.clean, "hippius-drain-agent stopped");
    // A worker fault exits non-zero so k8s restarts the pod and the fault is alertable;
    // a clean shutdown exits zero. (`exit = deny` lints out std::process::exit, so main
    // returns the ExitCode.)
    Ok(if faulted(&report) { ExitCode::FAILURE } else { ExitCode::SUCCESS })
}

/// Installs the global tracing subscriber, honoring `RUST_LOG` (default `info`).
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Resolves when the process receives SIGINT (Ctrl-C) or SIGTERM — the signal
/// Kubernetes sends on pod termination — so the runtime winds down gracefully.
async fn shutdown_signal() {
    let interrupt = async {
        // A failed ctrl_c install must not resolve this branch — that would fake a
        // shutdown. Park so only a real terminate signal can end the wait.
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                term.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::faulted;
    use hippius_drain_agent::supervisor::{RunReport, ShutdownTrigger, WorkerName};

    #[test]
    fn a_clean_signal_shutdown_is_not_a_fault() {
        let report = RunReport {
            trigger: ShutdownTrigger::Signal,
            exits: vec![],
            clean: true,
        };
        assert!(!faulted(&report), "a clean SIGTERM shutdown exits zero");
    }

    #[test]
    fn no_workers_is_not_a_fault() {
        let report = RunReport {
            trigger: ShutdownTrigger::NoWorkers,
            exits: vec![],
            clean: true,
        };
        assert!(!faulted(&report));
    }

    #[test]
    fn a_worker_exit_is_a_fault_even_if_the_winddown_was_clean() {
        // WI-7: a panicked/early-exited worker escalates to shutdown; even a clean
        // wind-down of the peers must exit non-zero so k8s restarts the pod.
        let report = RunReport {
            trigger: ShutdownTrigger::WorkerExited(WorkerName::new("drain")),
            exits: vec![],
            clean: true,
        };
        assert!(faulted(&report));
    }

    #[test]
    fn an_unclean_winddown_is_a_fault() {
        let report = RunReport {
            trigger: ShutdownTrigger::Signal,
            exits: vec![],
            clean: false,
        };
        assert!(faulted(&report), "a force-aborted straggler is a fault");
    }
}
