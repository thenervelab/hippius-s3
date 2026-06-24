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
use hippius_drain_core::{Bytes, Store, StoreError};
use std::sync::{Arc, Mutex};
use thiserror::Error;

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

#[tokio::main]
async fn main() -> Result<(), StartupError> {
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
    // reconnecting manager shared across the concurrent drains.
    let redis = redis::aio::ConnectionManager::new(redis::Client::open(config.redis_queues_url.as_str())?).await?;
    let enqueuer = Arc::new(RedisEnqueuer::new(Arc::clone(&store), redis, config.upload_backends.clone()));

    // The enforcer starts at the floor (conservative) with a one-second burst of
    // the node's capability; the allocation-pull worker raises it to the leader's
    // budget on its first tick.
    let enforcer = default_enforcer(config.floor_rate, Bytes::new(config.max_drain_rate.get()));
    let rate_control = RateControl {
        enforcer: Arc::new(Mutex::new(enforcer)),
        node: config.node_id.clone(),
        floor: config.floor_rate,
        half_life: config.decay_half_life,
        poll: config.allocation_poll,
        stale_after: config.allocation_stale,
    };

    let runtime = AgentRuntime::new(
        Arc::new(LocalFs::new(&config.pool_root)),
        Arc::new(LocalSsd::new(&config.ssd_root)),
        store,
        enqueuer,
        config.runtime_config(),
    )
    .with_heartbeat(config.heartbeat_config())
    .with_rate_control(rate_control);

    tracing::info!(
        pool_root = %config.pool_root.display(),
        ssd_root = %config.ssd_root.display(),
        "hippius-drain-agent started"
    );
    let report = runtime.run(shutdown_signal()).await;
    tracing::info!(trigger = ?report.trigger, clean = report.clean, "hippius-drain-agent stopped");
    Ok(())
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
