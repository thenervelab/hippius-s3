//! hippius-drain-allocator: the singleton, leader-elected budget allocator entry point.
//!
//! Parses [`AllocatorConfig`] from the environment, connects the central Postgres
//! store, and runs [`run_allocator`] until SIGTERM/SIGINT. The allocation logic
//! itself lives in `hippius-drain-core` ([`hippius_drain_core::run_tick`]); this binary is the
//! deploy wrapper. When `CEPHOR_CEPH_MGR_METRICS_URL` is set and the `http` feature
//! is built, the live Ceph-mgr ceiling probe drives the budget; otherwise a
//! [`StaticCeiling`](hippius_drain_core::StaticCeiling) from config is the fallback.

use hippius_drain_allocator::config::{AllocatorConfig, ConfigError};
use hippius_drain_allocator::run::run_allocator;
use hippius_drain_core::{CephCeilingSource, CoordError, Coordinator, Store, StoreError};
use std::time::Duration;
use thiserror::Error;

/// The agents own the heartbeat TTL (`CEPHOR_HEARTBEAT_TTL_SECS`), so the allocator's
/// coordinator never writes node keys; this value is unused on its side, present only to
/// satisfy the shared [`Coordinator`] constructor.
const ALLOCATOR_NODE_TTL: Duration = Duration::from_secs(30);

/// A failure bringing the allocator up. Each variant maps to a distinct operator
/// fix: a bad environment, an unreachable store / coordination redis, or an
/// unbuildable probe client.
#[derive(Debug, Error)]
enum StartupError {
    #[error("invalid configuration")]
    Config(#[from] ConfigError),
    #[error("cannot connect to the state store")]
    Store(#[from] StoreError),
    #[error("cannot connect to the coordination redis")]
    Coord(#[from] CoordError),
    #[cfg(feature = "http")]
    #[error("cannot build the ceph mgr probe")]
    Probe(#[from] hippius_drain_allocator::probe::ProbeError),
}

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    init_tracing();

    let config = AllocatorConfig::from_env()?;
    let store = Store::connect(&config.database_url).await?;

    // The singleton allocator owns schema provisioning: it deploys before the agents
    // (allocator-first), so applying the migrations here means the agents come up
    // against a ready cephor_* schema and need no DDL rights of their own. Idempotent
    // — sqlx records applied migrations under an advisory lock, so a restart or a
    // brief multi-replica overlap during rollout re-runs nothing.
    store.migrate().await?;

    // Leadership, the fleet view, and the budgets live in Redis (TTL-keyed, epoch-fenced)
    // — not Postgres — so the ~2s leader tick never touches the WAL.
    let coordinator = Coordinator::connect(&config.redis_queues_url, ALLOCATOR_NODE_TTL, config.alloc_ttl).await?;

    tracing::info!(
        instance = %config.instance_id,
        tick_secs = config.tick_interval.as_secs(),
        "hippius-drain-allocator started"
    );
    run_with_ceiling_source(&coordinator, &config).await?;
    tracing::info!("hippius-drain-allocator stopped");
    Ok(())
}

/// Selects the ceiling source — the live mgr probe when a URL is configured (and the
/// `http` feature is built), else the static ceiling — and runs the allocator on it.
async fn run_with_ceiling_source(coord: &Coordinator, config: &AllocatorConfig) -> Result<(), StartupError> {
    #[cfg(feature = "http")]
    if let Some(url) = config.ceph_mgr_metrics_url.clone() {
        // The decay floor is the AIMD floor: while the probe is blind, the fleet
        // backs off toward the same conservative rate the allocator never drops below.
        let probe = hippius_drain_allocator::probe::CephProbe::new(
            url.clone(),
            config.ceph_ceiling,
            config.alloc.min_total,
            config.ceph_thresholds,
            config.ceph_probe_timeout,
        )?;
        tracing::info!(mgr_url = %url, "driving the budget from the live ceph-mgr ceiling probe");
        drive(coord, &probe, config).await;
        return Ok(());
    }
    tracing::info!("no ceph mgr url configured; using the static ceiling");
    drive(coord, &config.ceiling(), config).await;
    Ok(())
}

/// Runs the allocator loop on `ceiling`, logging (not failing on) a shutdown
/// relinquish error — the lease TTL recovers it, so the process still exits cleanly.
async fn drive<C: CephCeilingSource>(coord: &Coordinator, ceiling: &C, config: &AllocatorConfig) {
    if let Err(err) = run_allocator(
        coord,
        ceiling,
        &config.tick_config(),
        config.initial_total,
        config.tick_interval,
        Some(config.liveness_file.as_path()),
        shutdown_signal(),
    )
    .await
    {
        tracing::warn!(error = %err, "relinquishing leadership on shutdown failed; the lease TTL will recover it");
    }
}

/// Installs the global tracing subscriber, honoring `RUST_LOG` (default `info`).
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Resolves when the process receives SIGINT (Ctrl-C) or SIGTERM — the signal
/// Kubernetes sends on pod termination — so the tick loop winds down gracefully.
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
