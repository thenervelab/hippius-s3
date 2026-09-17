//! hippius-drain-allocator: schema migrate + terminal-row GC.
//!
//! After direct-to-Arion the agents drain ungated (no write budget). This binary
//! still owns `sqlx migrate()` (allocator-first) and the periodic GC of aged
//! `replicated`/`failed` rows. It does not scrape Ceph mgr or write `cephor:alloc:*`.

use hippius_drain_allocator::config::{AllocatorConfig, ConfigError};
use hippius_drain_core::{Store, StoreError};
use std::time::Duration;
use thiserror::Error;

/// How often the terminal-row GC sweep runs (coarse — the table grows slowly and each
/// sweep is one bounded DELETE).
const STATUS_GC_INTERVAL: Duration = Duration::from_hours(1);

/// A failure bringing the allocator up.
#[derive(Debug, Error)]
enum StartupError {
    #[error("invalid configuration")]
    Config(#[from] ConfigError),
    #[error("cannot connect to the state store")]
    Store(#[from] StoreError),
}

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    init_tracing();

    let config = AllocatorConfig::from_env()?;
    let store = Store::connect(&config.database_url).await?;

    // The probe requires mtime < 30s. 0021 VALIDATE can run for minutes on prod;
    // a one-shot create would go stale and the kubelet would SIGKILL mid-migrate.
    let liveness = config.liveness_file.clone();
    let heartbeat = tokio::spawn(async move {
        let mut ticks = tokio::time::interval(Duration::from_secs(10));
        loop {
            ticks.tick().await;
            let _ = std::fs::write(&liveness, b"ok");
        }
    });

    // The singleton allocator owns schema provisioning: it deploys before the agents
    // (allocator-first), so applying the migrations here means the agents come up
    // against a ready cephor_* schema and need no DDL rights of their own. Idempotent
    // — sqlx records applied migrations under an advisory lock, so a restart or a
    // brief multi-replica overlap during rollout re-runs nothing.
    let migrated = store.migrate().await;
    heartbeat.abort();
    migrated?;

    // Terminal-row GC: a best-effort background sweep pruning aged replicated/failed rows
    // so cephor_replication_status does not grow unbounded and bloat the hot claim/reconcile
    // scans. The DELETE is idempotent, so a mid-cycle abort on shutdown just re-runs next
    // startup; the allocator is a singleton (replicas:1, Recreate), so a single sweeper runs.
    // `store` is moved here — it is only needed for the migrate above and this sweep.
    let gc_retention = config.status_retention;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(STATUS_GC_INTERVAL).await;
            match store.gc_terminal_status_rows(gc_retention).await {
                Ok(0) => {}
                Ok(pruned) => tracing::info!(pruned, "gc'd terminal replication rows"),
                Err(err) => tracing::warn!(error = %err, "terminal-row gc failed; retrying next cycle"),
            }
        }
    });

    tracing::info!(
        instance = %config.instance_id,
        "hippius-drain-allocator started (migrate+gc; no write budget)"
    );

    let liveness = config.liveness_file.clone();
    let mut ticks = tokio::time::interval(Duration::from_secs(10));
    tokio::pin! {
        let shutdown = shutdown_signal();
    }
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            _ = ticks.tick() => {
                let _ = std::fs::write(&liveness, b"ok");
            }
        }
    }
    tracing::info!("hippius-drain-allocator stopped");
    Ok(())
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
