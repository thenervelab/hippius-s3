//! The agent's per-tick drain unit, wired from the hippius-drain-core contracts.
//!
//! [`drain_next`] claims one pending part and drains it (the product's core act),
//! gated by an optional shared [`Enforcer`]: the local token-bucket + circuit-breaker
//! the allocation-pull worker keeps in sync with the leader's budget. The periodic
//! loop, cancellation, and trigger wiring around this are the supervisor's job (see
//! [`crate::runtime`]); here each call is a single, independently-testable step.

use crate::localfs::{LocalFs, LocalSsd};
use hippius_drain_core::{DrainDecision, DrainOutcome, Enforcer, PartDrainError, PartKey, PartSource, SnapshotCell, Store, StoreError, drain_part};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// A failure during one drain cycle: either claiming the part or draining it.
///
/// Two distinct domains, kept separate so a caller can tell a store/claim problem
/// from a drain problem. `PartDrainError` is `#[non_exhaustive]` (so not constructible
/// here), so both are wrapped via `#[from]` rather than re-mapped.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DrainCycleError {
    /// Claiming (or releasing) a part failed.
    #[error("claiming a part failed")]
    Claim(#[from] StoreError),
    /// Draining the claimed part failed (the SSD copy is left intact).
    #[error("draining a part failed")]
    Drain(#[from] PartDrainError),
}

/// RAII guard that returns a taken concurrency permit if the drain unwinds or is
/// cancelled before [`Enforcer::record_outcome`] runs.
///
/// `try_drain` takes a permit on the `Allowed` path; on the normal path
/// `record_outcome` releases it, after which the guard is [`dismiss`](Self::dismiss)ed.
/// But a panic or a cancellation (the future dropped) at the drain `.await` skips
/// `record_outcome`, leaking the permit and permanently shrinking the concurrency
/// budget. The guard's `Drop` returns the permit on exactly those paths. It holds
/// only the `Arc` (not a `MutexGuard`), so nothing is locked across the `.await`
/// (axiom `rust_quality_74`).
struct PermitGuard<'a> {
    enforcer: &'a Arc<Mutex<Enforcer>>,
    armed: bool,
}

impl<'a> PermitGuard<'a> {
    /// Arms a guard over a just-taken permit.
    fn new(enforcer: &'a Arc<Mutex<Enforcer>>) -> Self {
        Self { enforcer, armed: true }
    }

    /// Disarms the guard: the normal path already released the permit via
    /// `record_outcome`, so `Drop` must not release it a second time.
    fn dismiss(mut self) {
        self.armed = false;
    }
}

impl Drop for PermitGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.enforcer.lock().unwrap_or_else(PoisonError::into_inner).release_permit();
        }
    }
}

/// The SSD byte size the bandwidth gate charges for this part: the sum of its chunk
/// files. `None` if listing or stat-ing any chunk fails — the caller then denies
/// admission and retries rather than charging 0 and draining unmetered (audit F7).
/// Charging 0 on a stat race would defeat the rate gate exactly under SSD I/O
/// pressure, when it matters most.
async fn part_size(ssd: &LocalSsd, part: &PartKey) -> Option<u64> {
    let indices = ssd.list_chunks(part).await.ok()?;
    let mut total = 0_u64;
    for index in indices {
        let path = ssd.chunk_source(part, index).ok()?;
        let meta = tokio::fs::metadata(&path).await.ok()?;
        total = total.saturating_add(meta.len());
    }
    Some(total)
}

/// Claims one pending part and drains it SSD → pool, gated by `enforcer`.
///
/// Returns `Ok(None)` when nothing is pending, or when the enforcer throttled the
/// claimed part (which is returned to pending for a later wake); `Ok(Some(..))` for
/// the part it drained. With `enforcer = None` the drain is ungated. A drain failure
/// leaves the SSD copy intact, so the cycle is always safe to retry.
///
/// # Errors
///
/// [`DrainCycleError::Claim`] if the claim/release query fails;
/// [`DrainCycleError::Drain`] if the copy/verify/commit/unlink sequence fails.
pub async fn drain_next(
    ceph: &LocalFs,
    ssd: &LocalSsd,
    store: &Store,
    enforcer: Option<&Arc<Mutex<Enforcer>>>,
    snapshot: Option<&SnapshotCell>,
) -> Result<Option<DrainOutcome>, DrainCycleError> {
    let Some(claim) = store.claim_part().await? else {
        return Ok(None);
    };

    if let Some(enforcer) = enforcer {
        let Some(bytes) = part_size(ssd, claim.part()).await else {
            // A stat/list failure must not admit the part at zero cost (audit F7): hand
            // the claim back and retry next wake. With node-scoped claims (migration
            // 0006) a missing local part dir no longer happens, so this is a genuine
            // transient I/O error worth backing off on.
            store.release_part(claim.part()).await?;
            tracing::debug!("part size unavailable; part returned to pending");
            return Ok(None);
        };
        // The guard's scope ends before the drain await — a `MutexGuard` must never
        // cross an `.await` (axiom rust_quality_74). Poisoning recovers via
        // `into_inner`: the `Enforcer` is a small `Copy` value whose sync methods
        // cannot panic mid-mutation, so a poisoned guard is still self-consistent.
        let decision = {
            let mut guard = enforcer.lock().unwrap_or_else(PoisonError::into_inner);
            guard.try_drain(bytes, Instant::now())
        };
        if let DrainDecision::Denied(reason) = decision {
            // No concurrency permit was taken on denial; hand the claim back so it
            // retries once the budget refills (the next wake re-claims it).
            store.release_part(claim.part()).await?;
            tracing::debug!(?reason, "drain throttled; part returned to pending");
            return Ok(None);
        }
    }

    // The Allowed path holds a concurrency permit across the drain. Arm an RAII guard
    // so a panic or cancellation at the drain `.await` returns it; the normal path
    // below releases via `record_outcome` and then dismisses the guard.
    let permit = enforcer.map(PermitGuard::new);
    let started = Instant::now();
    let result = drain_part(ceph, ssd, store, &claim).await;
    let elapsed = started.elapsed();
    if let Some(enforcer) = enforcer {
        // Any Ok is a Ceph-write success for the breaker; an Err is a failure.
        // record_outcome releases the permit, so the guard is dismissed afterwards.
        let success = result.is_ok();
        enforcer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .record_outcome(success, Instant::now());
    }
    if let Some(permit) = permit {
        permit.dismiss();
    }
    // Count the outcome per part, not per burst: `drained` and `failed` are both
    // part-drain attempts, so `error_bps` divides like units, and a burst that fails
    // midway keeps the parts it already drained (audit #10 / H2). Latency is sampled
    // only on success — a failed drain's time is not a representative Ceph-write
    // latency for the p99 saturation signal.
    if let Some(snapshot) = snapshot {
        match &result {
            Ok(_) => {
                snapshot.record_drained(1);
                snapshot.record_latency(elapsed);
            }
            Err(_) => snapshot.record_failed(1),
        }
    }
    if result.is_err() {
        // A non-terminal drain failure leaves the part claimed (`draining`) with its
        // SSD copy intact. Return it to `pending` now so a live agent retries on the
        // next wake instead of waiting out the claim lease (the H1 fix). The
        // `draining` guard in `release_part` makes this a no-op for a part a terminal
        // step already advanced (e.g. a byte mismatch moved it to `failed`).
        // Best-effort: if the release itself fails — likely the same store outage that
        // failed the drain — the lease-TTL re-claim is the backstop, so we keep
        // surfacing the original drain error.
        if let Err(release_err) = store.release_part(claim.part()).await {
            tracing::warn!(
                ?release_err,
                "failed to release claim after a drain error; the claim lease will recover it"
            );
        }
    }
    result.map(Some).map_err(DrainCycleError::from)
}

/// Drains pending parts until the backlog is empty, the enforcer throttles, or
/// `token` is cancelled, returning how many were drained.
///
/// Repeats [`drain_next`] until it reports nothing more to do (an empty backlog or a
/// throttle), so one wake handles a burst rather than a single part. A drain failure
/// stops the run and propagates (every failed part's SSD copy is intact, so the next
/// call resumes the backlog).
///
/// Cancellation is observed at each part boundary: on shutdown a large backlog winds
/// down within the supervisor's grace instead of running to completion and being
/// force-aborted mid-drain — a forced abort would strand the in-flight part in
/// `draining` until the claim lease expires (axiom `rust_quality_129_async_graceful_shutdown`).
///
/// # Errors
///
/// The first [`DrainCycleError`] a cycle hits.
pub async fn drain_until_empty(
    ceph: &LocalFs,
    ssd: &LocalSsd,
    store: &Store,
    enforcer: Option<&Arc<Mutex<Enforcer>>>,
    snapshot: Option<&SnapshotCell>,
    token: &CancellationToken,
) -> Result<u64, DrainCycleError> {
    let mut drained = 0;
    // Check cancellation at the top of each cycle (the part boundary): a started drain
    // always runs to its own completion — verify-before-unlink makes that the safe
    // point — but the burst stops claiming new work the moment shutdown is signalled,
    // so the worker exits well within the grace window.
    while !token.is_cancelled() {
        if drain_next(ceph, ssd, store, enforcer, snapshot).await?.is_none() {
            break;
        }
        drained += 1;
    }
    Ok(drained)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::PermitGuard;
    use super::{drain_next, drain_until_empty};
    use crate::localfs::{LocalFs, LocalSsd};
    use core::str::FromStr;
    use hippius_drain_core::{
        BreakerConfig, ByteRate, Bytes, CircuitBreaker, ConcurrencyLimiter, DrainDecision, DrainOutcome, Enforcer, ObjectId, PartKey, PartNumber,
        PartReplicationStore, ReplicationState, SnapshotCell, Store, TokenBucket, Version,
    };
    use sqlx::postgres::PgPool;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;

    const UUID: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    fn part_at(version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(UUID).unwrap(), Version::new(version), PartNumber::new(number))
    }

    async fn status_of(store: &Store, part: &PartKey) -> Option<ReplicationState> {
        <Store as PartReplicationStore>::status(store, part).await.unwrap()
    }

    /// Lays a complete SSD part (`chunk_<i>.bin` files + meta.json) under `ssd_root`
    /// and records it pending — what the api/ingest does for a part.
    async fn seed_part(ssd_root: &Path, store: &Store, part: &PartKey, chunks: &[&[u8]]) {
        let dir = ssd_root.join(part.relative_dir());
        std::fs::create_dir_all(&dir).unwrap();
        for (index, bytes) in chunks.iter().enumerate() {
            std::fs::write(dir.join(format!("chunk_{index}.bin")), bytes).unwrap();
        }
        std::fs::write(dir.join("meta.json"), br#"{"chunk_size":16,"num_chunks":1,"size_bytes":16}"#).unwrap();
        store.record_landed_part(part).await.unwrap();
    }

    /// An enforcer whose token bucket holds `rate` bytes/sec and the same burst.
    fn enforcer_with(rate: u64) -> Arc<Mutex<Enforcer>> {
        Arc::new(Mutex::new(Enforcer::new(
            CircuitBreaker::new(BreakerConfig {
                failure_threshold: 3,
                cooldown: Duration::from_secs(5),
            }),
            TokenBucket::new(ByteRate::new(rate), Bytes::new(rate), Instant::now()),
            ConcurrencyLimiter::new(4),
        )))
    }

    /// An enforcer with an ample bucket but exactly `max` concurrency permits, so
    /// permit accounting is observable through `try_drain` Allowed/Denied.
    fn enforcer_with_concurrency(max: u32) -> Arc<Mutex<Enforcer>> {
        Arc::new(Mutex::new(Enforcer::new(
            CircuitBreaker::new(BreakerConfig {
                failure_threshold: 3,
                cooldown: Duration::from_secs(5),
            }),
            TokenBucket::new(ByteRate::new(1_000_000), Bytes::new(1_000_000), Instant::now()),
            ConcurrencyLimiter::new(max),
        )))
    }

    #[test]
    fn a_dropped_permit_guard_returns_the_concurrency_permit() {
        // One permit total. try_drain takes it; the next is denied. An armed guard
        // dropped without dismiss — the panic/cancel path — must return the permit
        // so a later drain is admitted again (no leak).
        let enforcer = enforcer_with_concurrency(1);
        let now = Instant::now();
        {
            let mut guard = enforcer.lock().unwrap();
            assert_eq!(guard.try_drain(1, now), DrainDecision::Allowed, "the one permit is taken");
            assert!(matches!(guard.try_drain(1, now), DrainDecision::Denied(_)), "no permit remains");
        }
        drop(PermitGuard::new(&enforcer)); // an unwound/cancelled drain
        assert_eq!(
            enforcer.lock().unwrap().try_drain(1, now),
            DrainDecision::Allowed,
            "the dropped guard returned the permit",
        );
    }

    #[test]
    fn a_dismissed_permit_guard_does_not_double_release() {
        // The normal path already released the permit via record_outcome; a
        // dismissed guard must add nothing, or the concurrency budget would inflate.
        let enforcer = enforcer_with_concurrency(1);
        let now = Instant::now();
        {
            let mut guard = enforcer.lock().unwrap();
            assert_eq!(guard.try_drain(1, now), DrainDecision::Allowed);
        }
        enforcer.lock().unwrap().record_outcome(true, now); // releases the permit
        PermitGuard::new(&enforcer).dismiss(); // must NOT release a second time
        let mut guard = enforcer.lock().unwrap();
        assert_eq!(guard.try_drain(1, now), DrainDecision::Allowed, "the single permit is available");
        assert!(
            matches!(guard.try_drain(1, now), DrainDecision::Denied(_)),
            "only one permit exists — a dismissed guard caused no double-release inflation",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn drain_next_claims_then_drains_a_pending_part(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"hello cephor part", b"second chunk"]).await;

        // One ungated cycle claims it and drains it end-to-end.
        let outcome = drain_next(&ceph, &ssd, &store, None, None).await.unwrap();
        assert_eq!(outcome, Some(DrainOutcome::Replicated));

        // The SSD part is freed only after the verified, committed pool copy exists.
        let ssd_part = ssd_dir.path().join(part.relative_dir());
        let pool_part = pool_dir.path().join(part.relative_dir());
        assert!(!ssd_part.exists(), "the SSD part is freed after a verified drain");
        assert_eq!(
            std::fs::read(pool_part.join("chunk_0.bin")).unwrap(),
            b"hello cephor part",
            "the pool holds the durable copy"
        );
        assert!(pool_part.join("meta.json").exists(), "the meta marker landed last");

        // Nothing else is pending: the next cycle is a no-op.
        assert_eq!(drain_next(&ceph, &ssd, &store, None, None).await.unwrap(), None);
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn an_exhausted_budget_throttles_and_returns_the_part(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"throttled part bytes"]).await;
        let ssd_part = ssd_dir.path().join(part.relative_dir());

        // A zero-budget enforcer: the rate gate denies any non-zero drain.
        let empty = enforcer_with(0);
        assert_eq!(drain_next(&ceph, &ssd, &store, Some(&empty), None).await.unwrap(), None);
        assert!(ssd_part.exists(), "a throttled drain leaves the SSD part untouched");
        assert_eq!(
            status_of(&store, &part).await,
            Some(ReplicationState::Pending),
            "the throttled part is returned to pending",
        );

        // With an ample budget, the same part drains.
        let ample = enforcer_with(1_000_000);
        assert_eq!(
            drain_next(&ceph, &ssd, &store, Some(&ample), None).await.unwrap(),
            Some(DrainOutcome::Replicated)
        );
        assert!(!ssd_part.exists(), "the admitted drain frees the SSD part");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_drain_records_its_latency_in_the_snapshot(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");
        let snapshot = SnapshotCell::new();
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"timed part"]).await;

        // A successful drain feeds its latency into the window, so p99 leaves zero.
        assert_eq!(
            drain_next(&ceph, &ssd, &store, None, Some(&snapshot)).await.unwrap(),
            Some(DrainOutcome::Replicated)
        );
        assert!(snapshot.p99() > Duration::ZERO, "the drain's latency was recorded in the snapshot");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_failed_drain_returns_the_claim_to_pending(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");

        // Record a part as landed but never write its SSD files: the drain copy then
        // hits an I/O error mid-cycle (the meta source is "gone"). This is the H1
        // transient-failure case — a non-terminal drain error must not strand the
        // claim in `draining`; a live agent returns it to `pending` so the next wake
        // retries it, rather than waiting out the claim lease.
        let part = part_at(5, 1);
        store.record_landed_part(&part).await.unwrap();

        let err = drain_next(&ceph, &ssd, &store, None, None).await.unwrap_err();
        assert!(
            matches!(err, super::DrainCycleError::Drain(_)),
            "a missing SSD part is a drain failure, got {err:?}"
        );

        assert_eq!(
            status_of(&store, &part).await,
            Some(ReplicationState::Pending),
            "a failed drain returns the claim to pending, not stranded in draining",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_drained_part_increments_the_drained_counter(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");
        let snapshot = SnapshotCell::new();
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"counted part"]).await;

        // Counting lives per part in drain_next, so a single drained part is one
        // drained attempt and zero failed attempts (the units error_bps divides).
        drain_next(&ceph, &ssd, &store, None, Some(&snapshot)).await.unwrap();
        let counts = snapshot.load();
        assert_eq!(counts.drained, 1, "the drained part was counted");
        assert_eq!(counts.failed, 0, "a success records no failure");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_failed_drain_increments_the_failed_counter(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");
        let snapshot = SnapshotCell::new();

        // Recorded landed but no SSD files -> the drain copy fails (one failed attempt).
        let part = part_at(5, 1);
        store.record_landed_part(&part).await.unwrap();

        drain_next(&ceph, &ssd, &store, None, Some(&snapshot)).await.unwrap_err();
        let counts = snapshot.load();
        assert_eq!(counts.failed, 1, "the failed part attempt was counted");
        assert_eq!(counts.drained, 0, "a failure records no drain");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_burst_that_fails_midway_keeps_the_drained_count(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");
        let snapshot = SnapshotCell::new();
        let token = CancellationToken::new();

        // One good part (drains), then one with no SSD files (fails) — claimed in
        // landed_at order, so the good one drains before the burst errors.
        seed_part(ssd_dir.path(), &store, &part_at(5, 1), &[b"good part bytes"]).await;
        store.record_landed_part(&part_at(5, 2)).await.unwrap();

        // The burst drains the good part then fails on the bad one. The #10 fix:
        // the part drained before the failure is NOT discarded.
        drain_until_empty(&ceph, &ssd, &store, None, Some(&snapshot), &token).await.unwrap_err();
        let counts = snapshot.load();
        assert_eq!(counts.drained, 1, "the part drained before the failure is kept");
        assert_eq!(counts.failed, 1, "the failed part is counted once");
        // error_bps is dimensionally clean: 1 failed of 2 attempts = 5000 bps.
        assert_eq!(counts.error_bps(), 5000, "failed attempts over total attempts");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn drain_until_empty_drains_the_whole_backlog(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");

        // Three honest parts pending under one object version.
        for number in 1..=3_u32 {
            seed_part(ssd_dir.path(), &store, &part_at(5, number), &[b"backlog part"]).await;
        }

        let token = CancellationToken::new();
        let drained = drain_until_empty(&ceph, &ssd, &store, None, None, &token).await.unwrap();
        assert_eq!(drained, 3, "every pending part was drained in one run");
        // The backlog is now empty.
        assert_eq!(drain_until_empty(&ceph, &ssd, &store, None, None, &token).await.unwrap(), 0);
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_cancelled_drain_stops_at_the_part_boundary(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool).with_node_id("test-node");

        // A real backlog of three parts.
        for number in 1..=3_u32 {
            seed_part(ssd_dir.path(), &store, &part_at(5, number), &[b"backlog part"]).await;
        }

        // M6: with the token already cancelled (shutdown signalled), the burst must
        // not run to completion — it stops at the first part boundary so the worker
        // honors the supervisor's grace instead of being force-aborted mid-backlog.
        let token = CancellationToken::new();
        token.cancel();
        let drained = drain_until_empty(&ceph, &ssd, &store, None, None, &token).await.unwrap();
        assert_eq!(drained, 0, "a cancelled drain stops before touching the backlog");
    }
}
