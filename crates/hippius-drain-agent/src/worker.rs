//! The agent's per-tick drain unit, wired from the hippius-drain-core contracts.
//!
//! [`drain_next`] claims one pending part and drains it (the product's core act),
//! gated by an optional shared [`Enforcer`]: the local token-bucket + circuit-breaker
//! the allocation-pull worker keeps in sync with the leader's budget. The periodic
//! loop, cancellation, and trigger wiring around this are the supervisor's job (see
//! [`crate::runtime`]); here each call is a single, independently-testable step.

use crate::localfs::{LocalFs, LocalSsd};
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use hippius_drain_core::{
    BreakerSignal, DenyReason, DrainDecision, DrainOutcome, Enforcer, MissingSourceOutcome, PartDrainError, PartKey, PartSource, SnapshotCell, Store,
    StoreError, UploadEnqueuer, breaker_signal_for, drain_part,
};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Missing-source observations before a part is written off as terminal `failed`.
///
/// Conservative on purpose: `failed` is never resurrected (the reconciler only
/// registers parts that exist on SSD, and `record_landed_part` never revives a
/// `failed` row), so a wrong write-off is silent replication loss. At 20 exponential
/// deferrals (5s base, capped at 10min) the row is HOURS old before the 20th distinct
/// `NotFound` lands — a slow or blipping mount does not plausibly produce 20 separate
/// missing-source observations, only a source that is permanently gone does (the
/// 2026-07-22 rows whose SSD dirs no longer exist).
const MISSING_SOURCE_FAIL_ATTEMPTS: u32 = 20;

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

impl DrainCycleError {
    /// Whether this is a benign deferral rather than a real cycle failure — the part
    /// backed off for a reason that is NOT Ceph unhealth (upload context not ready, or a
    /// vanished SSD source; see [`PartDrainError::is_benign_deferral`]). A deferral must
    /// not stop a [`drain_until_empty`] burst: the parts behind the deferred one are often
    /// ready, and starving them is what wedged the drain on in-progress/abandoned MPUs.
    fn is_deferral(&self) -> bool {
        matches!(self, Self::Drain(err) if err.is_benign_deferral())
    }
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
/// files. An error means the size is unknown — the caller must not admit the part at
/// zero cost and drain unmetered (audit F7): charging 0 on a stat race would defeat
/// the rate gate exactly under SSD I/O pressure, when it matters most. The io kind
/// is surfaced so the caller can split a vanished source (`NotFound` — part-specific)
/// from genuine node I/O trouble (everything else — node-global).
async fn part_size(ssd: &LocalSsd, part: &PartKey) -> std::io::Result<u64> {
    let indices = ssd.list_chunks(part).await?;
    if indices.is_empty() {
        // `list_chunks` maps a missing part dir to an empty listing (a scan nicety);
        // here that would admit a vanished source at zero cost, so re-stat the meta
        // marker to surface the missing dir as the `NotFound` it is.
        tokio::fs::metadata(ssd.meta_source(part)?).await?;
        return Ok(0);
    }
    let mut total = 0_u64;
    for index in indices {
        let path = ssd.chunk_source(part, index)?;
        let meta = tokio::fs::metadata(&path).await?;
        total = total.saturating_add(meta.len());
    }
    Ok(total)
}

/// One claim-slot outcome, distinguishing part-specific skips (keep claiming)
/// from node-global stops (budget spent / breaker open / backlog empty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The claimed part drained to the pool.
    Drained(DrainOutcome),
    /// This part cannot proceed right now but others can: it was deferred
    /// (backoff) and the burst must keep refilling.
    Skipped,
    /// Nothing claimable, or a node-global denial: stop refilling this burst.
    Idle,
}

/// Claims one pending part and drains it SSD → pool, gated by `enforcer`.
///
/// Returns [`ClaimOutcome::Drained`] for the part it drained; [`ClaimOutcome::Skipped`]
/// when the claimed part cannot proceed right now but others can (it was deferred
/// with backoff, so the caller keeps the burst claiming); [`ClaimOutcome::Idle`] when
/// nothing is pending or a node-global gate denied — the claimed part, if any, is
/// returned to pending for a later wake. With `enforcer = None` the drain is
/// ungated. A drain failure leaves the SSD copy intact, so the cycle is always
/// safe to retry.
///
/// # Errors
///
/// [`DrainCycleError::Claim`] if the claim/release/defer query fails;
/// [`DrainCycleError::Drain`] if the copy/verify/commit/unlink sequence fails.
pub async fn drain_next<E: UploadEnqueuer>(
    ceph: &LocalFs,
    ssd: &LocalSsd,
    store: &Store,
    enqueuer: &E,
    enforcer: Option<&Arc<Mutex<Enforcer>>>,
    snapshot: Option<&SnapshotCell>,
) -> Result<ClaimOutcome, DrainCycleError> {
    let Some(claim) = store.claim_part().await? else {
        return Ok(ClaimOutcome::Idle);
    };

    if let Some(enforcer) = enforcer {
        let bytes = match part_size(ssd, claim.part()).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // The SSD source is gone (MPU abort / DeleteObject / overwrite deleted
                // the ingest copy) — specific to THIS part, so back it off and keep the
                // burst claiming. Counted as a deferral, exactly like the mid-drain
                // vanished-source case, so the metric does not depend on WHERE the
                // ENOENT surfaced. The store counts the observation and — atomically,
                // inside the same draining-guarded UPDATE — writes the part off as
                // terminal `failed` once it has been observed missing
                // MISSING_SOURCE_FAIL_ATTEMPTS times: a source that is permanently
                // gone (the 2026-07-22 rows) must stop churning claim→defer forever.
                // A write-off is NOT a Ceph failure: it stays off the breaker and out
                // of error_bps (the WARN + the terminal row are its record).
                match store.defer_part_missing_source(claim.part(), MISSING_SOURCE_FAIL_ATTEMPTS).await? {
                    MissingSourceOutcome::Deferred(observations) => {
                        if let Some(snapshot) = snapshot {
                            snapshot.record_deferred(1);
                        }
                        tracing::debug!(observations, "part source missing; part deferred, burst continues");
                    }
                    MissingSourceOutcome::Failed => {
                        tracing::warn!(
                            object_id = %claim.part().object().as_str(),
                            version = claim.part().version().get(),
                            part_number = claim.part().part().get(),
                            observations = MISSING_SOURCE_FAIL_ATTEMPTS,
                            "writing off part: SSD source gone after repeated observations",
                        );
                    }
                    MissingSourceOutcome::Superseded => {
                        tracing::debug!("part source missing but the claim was superseded; nothing recorded");
                    }
                }
                return Ok(ClaimOutcome::Skipped);
            }
            Err(err) => {
                // Any other stat/list failure must not admit the part at zero cost
                // (audit F7) and — unlike a vanished source — smells like genuine
                // transient node I/O trouble, so briefly backing the whole node off is
                // right: hand the claim back (promptly re-claimable) and end the burst.
                store.release_part(claim.part()).await?;
                tracing::debug!(error = ?err, "part size unavailable; part returned to pending");
                return Ok(ClaimOutcome::Idle);
            }
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
            // No concurrency permit was taken on any denial. A denial is liveness
            // progress either way: the loop DID cycle a claim, it just backed off.
            // Recording it keeps the readiness tracker's `processed` count advancing so
            // a pool-wide outage does not flip every node NotReady and wedge a rolling
            // update (a wedge, not a healthy back-off, is what readiness must catch).
            // Kept out of drain outcomes.
            if let Some(snapshot) = snapshot {
                snapshot.record_throttled(1);
            }
            return match reason {
                // Part-specific: an EARLIER overdraft is still being paid off, so only
                // this (oversized) part must wait. Releasing it un-backed-off put it
                // straight back at the oldest-first claim head, where it was denied
                // again every burst and starved the whole node (2026-07-26 incident) —
                // defer it out of the claim set and keep the burst going.
                DenyReason::OverdraftOutstanding => {
                    store.defer_part(claim.part()).await?;
                    tracing::debug!(?reason, "drain deferred; part backed off, burst continues");
                    Ok(ClaimOutcome::Skipped)
                }
                // Node-global: the budget is spent, the breaker is open, or every
                // in-flight slot is taken — no part would fare better, so hand the
                // claim back (promptly re-claimable once the gate reopens) and stop
                // the burst until the next wake.
                DenyReason::BreakerOpen | DenyReason::RateLimited | DenyReason::AtConcurrencyLimit => {
                    store.release_part(claim.part()).await?;
                    tracing::debug!(?reason, "drain throttled; part returned to pending");
                    Ok(ClaimOutcome::Idle)
                }
            };
        }
    }

    // The Allowed path holds a concurrency permit across the drain. Arm an RAII guard
    // so a panic or cancellation at the drain `.await` returns it; the normal path
    // below releases via `record_outcome` and then dismisses the guard.
    let permit = enforcer.map(PermitGuard::new);
    let started = Instant::now();
    let result = drain_part(ceph, ssd, store, enqueuer, &claim).await;
    let elapsed = started.elapsed();
    // Classify the drain outcome once, for both the breaker and the metrics. A benign
    // deferral (`PartDrainError::is_benign_deferral`) is NOT evidence of Ceph unhealth,
    // so it must neither trip the node-global Ceph breaker nor count as a failure (P1a):
    //   - `Enqueue`: the upload context isn't finalized yet (in-progress MPU / Redis blip);
    //   - `Io` ENOENT: the SSD source/part vanished mid-drain (overwrite, concurrent
    //     clean, or a part another cycle already drained) — the pool is fine, there is
    //     just nothing to copy.
    // Misclassifying the ENOENT case as a Ceph failure would open the breaker on a
    // healthy pool and halt ALL draining on the node (stalling unrelated parts). Only a
    // genuine copy/verify/commit I/O error is a Ceph-write failure. A deferred part is
    // returned to `pending` (backed off) below, so a later re-drain retries it.
    // A benign deferral (enqueue not ready / vanished source / incomplete part) AND a
    // store/claim-coordination error (a Postgres blip on commit, or PartClaimLost from a
    // lease-expiry re-claim) both leave the breaker untouched; only a genuine Ceph-write
    // failure trips it. The policy lives in core (breaker_signal_for) so it stays with the
    // error type and is unit-tested there. A store error maps to Deferred for the breaker
    // but is RELEASED (not defer-backed-off) below, since only a genuine not-ready deferral
    // should back off.
    let signal = breaker_signal_for(&result);
    if let Some(enforcer) = enforcer {
        // record_outcome releases the permit, so the guard is dismissed afterwards.
        enforcer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .record_outcome(signal, Instant::now());
    }
    if let Some(permit) = permit {
        permit.dismiss();
    }
    // Count the outcome per part, not per burst: `drained` and `failed` are both
    // part-drain attempts, so `error_bps` divides like units, and a burst that fails
    // midway keeps the parts it already drained (audit #10 / H2). Latency is sampled
    // only on success — a failed drain's time is not a representative Ceph-write
    // latency for the p99 saturation signal. A deferral is neither drained nor failed:
    // it has its own counter so it stays out of `error_bps` (P1a).
    if let Some(snapshot) = snapshot {
        match signal {
            BreakerSignal::CephSuccess => {
                snapshot.record_drained(1);
                snapshot.record_latency(elapsed);
            }
            BreakerSignal::Deferred => snapshot.record_deferred(1),
            BreakerSignal::CephFailure => snapshot.record_failed(1),
        }
    }
    if result.is_err() {
        // A non-terminal drain failure leaves the part claimed (`draining`) with its
        // SSD copy intact. Return it to `pending` now so a live agent retries on the
        // next wake instead of waiting out the claim lease (the H1 fix). The `draining`
        // guard in both store calls makes this a no-op for a part a terminal step
        // already advanced (e.g. a byte mismatch moved it to `failed`).
        //
        // A DEFERRAL (enqueue not ready — the object's address is not finalized yet)
        // is backed off via `defer_part`, not released immediately: otherwise the drain
        // re-claims the same not-ready part on every poll and spins on it, starving the
        // parts that ARE ready to upload. A genuine Ceph-write failure releases promptly.
        // Best-effort: if the release/defer itself fails — likely the same store outage
        // that failed the drain — the lease-TTL re-claim is the backstop, so we keep
        // surfacing the original drain error.
        // Back off (defer_part) ONLY a genuine not-ready deferral, so the drain doesn't
        // spin re-claiming the same not-ready part; a store/claim error (which also maps
        // to the Deferred breaker signal) or a Ceph-write failure releases promptly.
        let returned = match &result {
            Err(err) if err.is_benign_deferral() => store.defer_part(claim.part()).await,
            _ => store.release_part(claim.part()).await,
        };
        if let Err(release_err) = returned {
            tracing::warn!(
                ?release_err,
                "failed to return the claim after a drain error; the claim lease will recover it"
            );
        }
    }
    result.map(ClaimOutcome::Drained).map_err(DrainCycleError::from)
}

/// What one burst accomplished: parts drained to the pool, and part-specific
/// skips that deferred a part (backoff) while the burst kept claiming.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BurstTally {
    /// Parts drained (committed to the pool) in this burst.
    pub drained: u64,
    /// Parts skipped: deferred with backoff — at the admission gate or mid-drain —
    /// without stopping the refill.
    pub skipped: u64,
}

/// Drains pending parts — up to `concurrency` at once — until the backlog is empty,
/// the enforcer stops the node, or `token` is cancelled, returning a [`BurstTally`]
/// of parts drained and parts skipped.
///
/// Runs up to `concurrency` [`drain_next`] calls concurrently rather than one at a
/// time. `claim_part` uses `FOR UPDATE SKIP LOCKED`, so concurrent claims take distinct
/// parts; running them together overlaps the per-part commit `fsync`s, which on the
/// ceph-backed Postgres are the dominant cost (each commit is a slow WAL flush), so a
/// serial loop left the configured concurrency (the `Enforcer`'s `ConcurrencyLimiter`)
/// unused and the backlog draining one slow commit at a time. The admission
/// `Enforcer` still bounds the real rate/concurrency; this just stops the *driver*
/// from being the bottleneck.
///
/// Each slot resolves three ways ([`ClaimOutcome`]): `Drained` counts and refills;
/// `Skipped` (a part-specific back-off — the part was deferred, whether at the
/// admission gate or as a mid-drain benign-deferral `Err`) counts as `skipped` and
/// refills, so one unprocessable part cannot stop the burst and starve the parts
/// behind it (the 2026-07-26 head-of-line incident); `Idle` (empty backlog or a
/// node-global denial) stops claiming new work, but in-flight drains are awaited to
/// completion — never abandoned mid-flight, since dropping one at its await would
/// strand its part in `draining` until the claim lease expires. A drain *failure*
/// likewise stops new claims and is surfaced (the first one) only after the
/// in-flight set drains; each failed `drain_next` has already released its own part.
///
/// Cancellation is observed before claiming each new part: on shutdown the burst stops
/// taking work immediately and only the already-started drains finish, so the worker
/// exits well within the supervisor's grace (axiom `rust_quality_129_async_graceful_shutdown`).
///
/// # Errors
///
/// The first [`DrainCycleError`] a cycle hits (after the in-flight set has drained).
#[expect(
    clippy::too_many_arguments,
    reason = "the drain seams (pool/ssd/store/enqueuer/enforcer/snapshot) + token + concurrency are each distinct injected collaborators; bundling them would just hide the wiring"
)]
pub async fn drain_until_empty<E: UploadEnqueuer>(
    ceph: &LocalFs,
    ssd: &LocalSsd,
    store: &Store,
    enqueuer: &E,
    enforcer: Option<&Arc<Mutex<Enforcer>>>,
    snapshot: Option<&SnapshotCell>,
    token: &CancellationToken,
    concurrency: usize,
) -> Result<BurstTally, DrainCycleError> {
    let concurrency = concurrency.max(1);
    let mut tally = BurstTally::default();
    let mut first_err: Option<DrainCycleError> = None;
    // Cleared by an empty backlog, a throttle, a failure, or cancellation — once we
    // stop refilling we only wind down the in-flight set.
    let mut refill = true;
    let mut inflight = FuturesUnordered::new();

    // Prime the in-flight set (stopping early on cancellation).
    for _ in 0..concurrency {
        if token.is_cancelled() {
            refill = false;
            break;
        }
        inflight.push(drain_next(ceph, ssd, store, enqueuer, enforcer, snapshot));
    }

    // Pushing into a FuturesUnordered while iterating it is supported; the `.next()`
    // borrow ends before the body runs, so the refill push is safe.
    while let Some(outcome) = inflight.next().await {
        // Classify the slot once: does the burst keep claiming after it? A skip — the
        // gated ClaimOutcome::Skipped or a mid-drain benign-deferral Err — deferred its
        // part (backed off out of the claim set), so the burst keeps claiming: the
        // parts behind it are often ready, and stopping here is what starved a node
        // down to 64 parts/hour (2026-07-26). The backoff makes the skipped part
        // unclaimable within this burst, so the burst still terminates once only
        // backed-off parts remain (claim_part then returns None). Idle and real
        // failures stop the refill; each failed drain has already returned its part.
        let keep_claiming = match outcome {
            Ok(ClaimOutcome::Drained(_)) => {
                tally.drained += 1;
                true
            }
            Ok(ClaimOutcome::Skipped) => {
                tally.skipped += 1;
                true
            }
            Ok(ClaimOutcome::Idle) => false,
            Err(err) if err.is_deferral() => {
                tally.skipped += 1;
                true
            }
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
                false
            }
        };
        if keep_claiming && refill && !token.is_cancelled() {
            inflight.push(drain_next(ceph, ssd, store, enqueuer, enforcer, snapshot));
        } else {
            refill = false;
        }
    }

    match first_err {
        Some(err) => Err(err),
        None => Ok(tally),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::PermitGuard;
    use super::{ClaimOutcome, drain_next, drain_until_empty};
    use crate::localfs::{LocalFs, LocalSsd};
    use core::str::FromStr;
    use hippius_drain_core::{
        BreakerConfig, BreakerSignal, ByteRate, Bytes, CircuitBreaker, ConcurrencyLimiter, DrainDecision, DrainOutcome, Enforcer, ObjectId, PartKey,
        PartNumber, PartReplicationStore, ReplicationState, SnapshotCell, Store, TokenBucket, UploadEnqueuer, Version,
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

    /// The row's `(deferred_until set, defer_attempts)` — the observable difference
    /// between a defer (backed off out of the claim set: `(true, n>0)`) and a release
    /// (promptly re-claimable: `(false, unchanged)`).
    async fn defer_state(db: &PgPool, part: &PartKey) -> (bool, i64) {
        sqlx::query_as(
            "SELECT deferred_until IS NOT NULL, defer_attempts::bigint FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .fetch_one(db)
        .await
        .unwrap()
    }

    /// Lays a complete SSD part (`chunk_<i>.bin` files + meta.json) under `ssd_root`
    /// and records it pending — what the api/ingest does for a part.
    async fn seed_part(ssd_root: &Path, store: &Store, part: &PartKey, chunks: &[&[u8]]) {
        let dir = ssd_root.join(part.relative_dir());
        std::fs::create_dir_all(&dir).unwrap();
        for (index, bytes) in chunks.iter().enumerate() {
            std::fs::write(dir.join(format!("chunk_{index}.bin")), bytes).unwrap();
        }
        // num_chunks must match the seeded set, or the drain's completeness gate rejects it.
        let meta = format!(r#"{{"chunk_size":16,"num_chunks":{},"size_bytes":16}}"#, chunks.len());
        std::fs::write(dir.join("meta.json"), meta).unwrap();
        store.record_landed_part(part).await.unwrap();
    }

    /// A no-op upload enqueuer — the drain tests assert claim/copy/commit, not the
    /// Redis fan-out (that's covered by the core partdrain tests + the enqueue module).
    struct NoopEnqueuer;
    impl UploadEnqueuer for NoopEnqueuer {
        type Error = std::io::Error;
        async fn enqueue(&self, _part: &PartKey) -> Result<(), std::io::Error> {
            Ok(())
        }
    }

    /// An enqueuer that always defers — to exercise the post-write deferral path
    /// (`PartDrainError::Enqueue`): the Ceph copy succeeds, then the enqueue fails.
    struct DeferringEnqueuer;
    impl UploadEnqueuer for DeferringEnqueuer {
        type Error = std::io::Error;
        async fn enqueue(&self, _part: &PartKey) -> Result<(), std::io::Error> {
            Err(std::io::Error::other("upload context not ready; will retry"))
        }
    }

    /// Defers part number 1 (not ready) but enqueues every other part — to exercise that
    /// a not-ready part does not stop the burst and starve the ready parts behind it.
    struct DeferPartOneEnqueuer;
    impl UploadEnqueuer for DeferPartOneEnqueuer {
        type Error = std::io::Error;
        async fn enqueue(&self, part: &PartKey) -> Result<(), std::io::Error> {
            if part.part().get() == 1 {
                Err(std::io::Error::other("upload context not ready; will retry"))
            } else {
                Ok(())
            }
        }
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
        enforcer.lock().unwrap().record_outcome(BreakerSignal::CephSuccess, now); // releases the permit
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
        let store = Store::from_pool(pool);
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"hello cephor part", b"second chunk"]).await;

        // One ungated cycle claims it and drains it end-to-end.
        let outcome = drain_next(&ceph, &ssd, &store, &NoopEnqueuer, None, None).await.unwrap();
        assert_eq!(outcome, ClaimOutcome::Drained(DrainOutcome::Replicated));

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
        assert_eq!(
            drain_next(&ceph, &ssd, &store, &NoopEnqueuer, None, None).await.unwrap(),
            ClaimOutcome::Idle
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn an_exhausted_budget_throttles_and_returns_the_part(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"throttled part bytes"]).await;
        let ssd_part = ssd_dir.path().join(part.relative_dir());

        // A zero-budget enforcer: the rate gate denies any non-zero drain.
        let empty = enforcer_with(0);
        let snapshot = SnapshotCell::new();
        assert_eq!(
            drain_next(&ceph, &ssd, &store, &NoopEnqueuer, Some(&empty), Some(&snapshot))
                .await
                .unwrap(),
            ClaimOutcome::Idle
        );
        assert!(ssd_part.exists(), "a throttled drain leaves the SSD part untouched");
        assert_eq!(
            status_of(&store, &part).await,
            Some(ReplicationState::Pending),
            "the throttled part is returned to pending",
        );
        // C8: a denial is recorded as a throttled tick (readiness progress), and — being a
        // back-off, not a drain — leaves the drain outcomes and the Ceph error rate untouched.
        let snap = snapshot.load();
        assert_eq!(snap.throttled, 1, "the denied claim counts as a throttled tick");
        assert_eq!((snap.drained, snap.failed, snap.deferred), (0, 0, 0), "a throttle is no drain outcome");

        // With an ample budget, the same part drains.
        let ample = enforcer_with(1_000_000);
        assert_eq!(
            drain_next(&ceph, &ssd, &store, &NoopEnqueuer, Some(&ample), None).await.unwrap(),
            ClaimOutcome::Drained(DrainOutcome::Replicated)
        );
        assert!(!ssd_part.exists(), "the admitted drain frees the SSD part");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_drain_records_its_latency_in_the_snapshot(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"timed part"]).await;

        // A successful drain feeds its latency into the window, so p99 leaves zero.
        assert_eq!(
            drain_next(&ceph, &ssd, &store, &NoopEnqueuer, None, Some(&snapshot)).await.unwrap(),
            ClaimOutcome::Drained(DrainOutcome::Replicated)
        );
        assert!(snapshot.p99() > Duration::ZERO, "the drain's latency was recorded in the snapshot");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_failed_drain_returns_the_claim_to_pending(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);

        // Record a part as landed but never write its SSD files: the drain copy then
        // hits an I/O error mid-cycle (the meta source is "gone"). This is the H1
        // transient-failure case — a non-terminal drain error must not strand the
        // claim in `draining`; a live agent returns it to `pending` so the next wake
        // retries it, rather than waiting out the claim lease.
        let part = part_at(5, 1);
        store.record_landed_part(&part).await.unwrap();

        let err = drain_next(&ceph, &ssd, &store, &NoopEnqueuer, None, None).await.unwrap_err();
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
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"counted part"]).await;

        // Counting lives per part in drain_next, so a single drained part is one
        // drained attempt and zero failed attempts (the units error_bps divides).
        drain_next(&ceph, &ssd, &store, &NoopEnqueuer, None, Some(&snapshot)).await.unwrap();
        let counts = snapshot.load();
        assert_eq!(counts.drained, 1, "the drained part was counted");
        assert_eq!(counts.failed, 0, "a success records no failure");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_vanished_source_skips_with_backoff_and_spares_the_breaker(pool: PgPool) {
        // A part recorded landed but with NO SSD files models the SSD source vanishing
        // out from under the drain: the MPU-abort / DeleteObject / overwrite paths (and,
        // in e2e, the `clear_object_cache` helper) delete the ingest copy. That is NOT
        // Ceph unhealth — there is simply nothing left to copy — and it is specific to
        // THIS part, so the gated path resolves it at the size gate: `Skipped` (deferred
        // with backoff so the burst keeps claiming), counted as a DEFERRAL (not a
        // failure), kept out of `error_bps`, and — the load-bearing part — NOT tripping
        // the node-global Ceph breaker (which would halt draining of every other,
        // healthy part on the node). Old behavior stopped the burst here; the 2026-07-26
        // incident showed one such part starves the whole node.
        let db = pool.clone();
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();
        // Breaker threshold 1: a single Ceph failure would open it, so a subsequently
        // admitted drain is the proof the vanished source never signalled it.
        let enforcer = Arc::new(Mutex::new(Enforcer::new(
            CircuitBreaker::new(BreakerConfig {
                failure_threshold: 1,
                cooldown: Duration::from_secs(5),
            }),
            TokenBucket::new(ByteRate::new(1_000_000), Bytes::new(1_000_000), Instant::now()),
            ConcurrencyLimiter::new(4),
        )));

        let part = part_at(5, 1);
        store.record_landed_part(&part).await.unwrap();

        let step = drain_next(&ceph, &ssd, &store, &NoopEnqueuer, Some(&enforcer), Some(&snapshot))
            .await
            .unwrap();
        assert_eq!(step, ClaimOutcome::Skipped, "a vanished source skips this part; the burst keeps claiming");

        let counts = snapshot.load();
        assert_eq!(counts.deferred, 1, "a vanished source is counted as a deferral");
        assert_eq!(counts.failed, 0, "a vanished source is not a Ceph-write failure");
        assert_eq!(counts.drained, 0, "nothing was committed");
        assert_eq!(counts.error_bps(), 0, "vanished sources stay out of the Ceph failure rate");
        assert_eq!(
            status_of(&store, &part).await,
            Some(ReplicationState::Pending),
            "a deferred part is returned to pending for a later re-drain",
        );
        assert_eq!(
            defer_state(&db, &part).await,
            (true, 1),
            "the part is backed off (deferred), not released into the next claim",
        );
        assert_eq!(
            missing_source_count(&db, &part).await,
            1,
            "the vanished source is counted as one missing-source observation",
        );
        assert_eq!(
            enforcer.lock().unwrap().try_drain(1, Instant::now()),
            DrainDecision::Allowed,
            "a vanished source must not trip the Ceph breaker",
        );
    }

    /// The row's `missing_source_attempts` — the write-off escalation counter.
    async fn missing_source_count(db: &PgPool, part: &PartKey) -> i64 {
        sqlx::query_scalar(
            "SELECT missing_source_attempts::bigint FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .fetch_one(db)
        .await
        .unwrap()
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_source_missing_at_the_threshold_is_written_off_terminally(pool: PgPool) {
        // The prod motivation (2026-07-22 rows): a part whose SSD dir is permanently
        // gone deferred forever — claim → ENOENT → defer, on every node, for days. At
        // the threshold-th missing-source observation the row must go terminal
        // `failed` (never re-claimed, never resurrected) so it stops churning. The
        // write-off is NOT a Ceph failure: it must stay out of error_bps and off the
        // breaker, and the burst keeps claiming (Skipped).
        let db = pool.clone();
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();
        let enforcer = enforcer_with(1_000_000);

        let part = part_at(5, 1);
        store.record_landed_part(&part).await.unwrap();
        // Threshold-1 prior observations: this claim's ENOENT is the deciding one.
        sqlx::query("UPDATE cephor_replication_status SET missing_source_attempts = $1 WHERE object_id = $2")
            .bind(i64::from(super::MISSING_SOURCE_FAIL_ATTEMPTS - 1))
            .bind(part.object().as_str())
            .execute(&db)
            .await
            .unwrap();

        let step = drain_next(&ceph, &ssd, &store, &NoopEnqueuer, Some(&enforcer), Some(&snapshot))
            .await
            .unwrap();
        assert_eq!(step, ClaimOutcome::Skipped, "a write-off is part-specific; the burst keeps claiming");

        assert_eq!(
            status_of(&store, &part).await,
            Some(ReplicationState::Failed),
            "the row is written off terminally — no more claim churn",
        );
        let counts = snapshot.load();
        assert_eq!(counts.failed, 0, "a write-off is not a Ceph-write failure");
        assert_eq!(counts.error_bps(), 0, "the write-off stays out of the Ceph failure rate");
        assert_eq!(
            enforcer.lock().unwrap().try_drain(1, Instant::now()),
            DrainDecision::Allowed,
            "the write-off must not trip the Ceph breaker",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn an_unrelated_defer_history_cannot_fast_track_the_write_off(pool: PgPool) {
        // The amendment's whole point: defer_attempts is shared with overdraft and
        // not-ready deferrals, so a part with a LONG unrelated backoff history must
        // NOT be written off on its first transient NotFound — `failed` is never
        // resurrected, so that would be silent replication loss. Only missing-source
        // observations count.
        let db = pool.clone();
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let enforcer = enforcer_with(1_000_000);

        let part = part_at(5, 1);
        store.record_landed_part(&part).await.unwrap();
        // A long overdraft/not-ready history, but zero missing-source observations.
        sqlx::query("UPDATE cephor_replication_status SET defer_attempts = 25 WHERE object_id = $1")
            .bind(part.object().as_str())
            .execute(&db)
            .await
            .unwrap();

        let step = drain_next(&ceph, &ssd, &store, &NoopEnqueuer, Some(&enforcer), None).await.unwrap();
        assert_eq!(step, ClaimOutcome::Skipped);

        assert_eq!(
            status_of(&store, &part).await,
            Some(ReplicationState::Pending),
            "the first missing-source observation only defers — the unrelated history must not escalate it",
        );
        assert_eq!(
            missing_source_count(&db, &part).await,
            1,
            "the NotFound is counted as the FIRST missing-source observation",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn an_overdraft_denied_part_defers_and_the_burst_continues(pool: PgPool) {
        // The 2026-07-26 head-of-line incident shape: a part denied at the claim head
        // used to be released un-backed-off and the burst stopped — next wake it was
        // oldest again, denied again, and every part behind it starved (64 parts/hour).
        // OverdraftOutstanding is PART-specific (an earlier overdraft is still being
        // paid off), so the part must be deferred with backoff and the burst continue.
        // Intra-burst continuation under OverdraftOutstanding is mostly theoretical —
        // while debt is outstanding the bucket holds zero tokens, so followers are
        // RateLimited (hence the zero-byte follower below); the production win is the
        // backoff removing the oversized part from the claim head across wakes.
        let db = pool.clone();
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();
        // Rate 1 B/s with a 64-byte burst: the ~10 KB debt booked below repays over
        // hours, so it is deterministically outstanding for the whole test.
        let enforcer = Arc::new(Mutex::new(Enforcer::new(
            CircuitBreaker::new(BreakerConfig {
                failure_threshold: 3,
                cooldown: Duration::from_secs(5),
            }),
            TokenBucket::new(ByteRate::new(1), Bytes::new(64), Instant::now()),
            ConcurrencyLimiter::new(4),
        )));
        // Book the outstanding overdraft (Task C semantics): an oversized admission
        // drains the bucket and carries the remainder as debt; record_outcome returns
        // the permit without touching the breaker.
        {
            let mut guard = enforcer.lock().unwrap();
            assert_eq!(
                guard.try_drain(10_000, Instant::now()),
                DrainDecision::Allowed,
                "the first overdraft is admitted"
            );
            guard.record_outcome(BreakerSignal::Deferred, Instant::now());
        }

        // Oldest first: an oversized part (100 B > 64 B burst → overdraft path, denied
        // OverdraftOutstanding while the debt is unpaid), then a zero-byte part the
        // empty bucket can still admit (a 0-byte charge needs no tokens).
        let oversized = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &oversized, &[&[7_u8; 100]]).await;
        let tiny = part_at(5, 2);
        seed_part(ssd_dir.path(), &store, &tiny, &[b""]).await;

        let token = CancellationToken::new();
        let tally = drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, Some(&enforcer), Some(&snapshot), &token, 1)
            .await
            .unwrap();

        assert_eq!(
            (tally.drained, tally.skipped),
            (1, 1),
            "the burst skipped the denied part and kept claiming"
        );
        assert_eq!(
            status_of(&store, &tiny).await,
            Some(ReplicationState::Replicated),
            "the part behind the denied one drained in the same cycle",
        );
        assert_eq!(
            status_of(&store, &oversized).await,
            Some(ReplicationState::Pending),
            "the denied part stays pending"
        );
        assert_eq!(
            defer_state(&db, &oversized).await,
            (true, 1),
            "the denied part is backed off out of the claim set, not released to the head",
        );
        assert_eq!(snapshot.load().throttled, 1, "the denial still records a throttled readiness tick");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_missing_ssd_source_defers_and_the_burst_continues(pool: PgPool) {
        // Burst-level counterpart of the vanished-source skip: with the missing-source
        // part claimed FIRST (oldest), the ready part behind it must still drain in the
        // SAME cycle instead of waiting for the next wake.
        let db = pool.clone();
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);

        let missing = part_at(5, 1);
        store.record_landed_part(&missing).await.unwrap();
        let ready = part_at(5, 2);
        seed_part(ssd_dir.path(), &store, &ready, &[b"ready part"]).await;

        let enforcer = enforcer_with(1_000_000);
        let token = CancellationToken::new();
        let tally = drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, Some(&enforcer), None, &token, 1)
            .await
            .unwrap();

        assert_eq!(
            (tally.drained, tally.skipped),
            (1, 1),
            "the missing source skipped; the ready part drained"
        );
        assert_eq!(
            status_of(&store, &ready).await,
            Some(ReplicationState::Replicated),
            "the ready part is not starved"
        );
        assert_eq!(defer_state(&db, &missing).await, (true, 1), "the missing-source part is backed off");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_mid_drain_deferral_counts_as_a_skip_in_the_tally(pool: PgPool) {
        // UNGATED (enforcer None), the size gate never runs, so a missing source only
        // surfaces mid-drain as a benign-deferral Err. That arm defers with backoff and
        // keeps the burst refilling — which IS the documented meaning of `skipped` — so
        // the tally must count it, or the cycle log undercounts what the burst skipped.
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);

        let missing = part_at(5, 1);
        store.record_landed_part(&missing).await.unwrap();
        let ready = part_at(5, 2);
        seed_part(ssd_dir.path(), &store, &ready, &[b"ready part"]).await;

        let token = CancellationToken::new();
        let tally = drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, None, None, &token, 1)
            .await
            .unwrap();

        assert_eq!(
            (tally.drained, tally.skipped),
            (1, 1),
            "the mid-drain deferral is a skip: burst continued, part backed off"
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_non_notfound_stat_failure_releases_the_part_and_idles_the_burst(pool: PgPool) {
        // A stat/list failure that is NOT ENOENT (here NotADirectory: a regular file
        // where the part directory should be) smells like genuine node I/O trouble, so
        // the whole node briefly backs off: the part is RELEASED (no backoff — retried
        // promptly next wake) and the burst stops, leaving the parts behind untouched.
        let db = pool.clone();
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);

        let broken = part_at(5, 1);
        let dir = ssd_dir.path().join(broken.relative_dir());
        std::fs::create_dir_all(dir.parent().unwrap()).unwrap();
        std::fs::write(&dir, b"not a directory").unwrap();
        store.record_landed_part(&broken).await.unwrap();
        let behind = part_at(5, 2);
        seed_part(ssd_dir.path(), &store, &behind, &[b"ready part"]).await;

        let enforcer = enforcer_with(1_000_000);
        let token = CancellationToken::new();
        let tally = drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, Some(&enforcer), None, &token, 1)
            .await
            .unwrap();

        assert_eq!((tally.drained, tally.skipped), (0, 0), "node I/O trouble idles the whole burst");
        assert_eq!(
            status_of(&store, &broken).await,
            Some(ReplicationState::Pending),
            "the part is handed back"
        );
        assert_eq!(
            defer_state(&db, &broken).await,
            (false, 0),
            "released without backoff: a transient node blip retries on the next wake",
        );
        assert_eq!(
            status_of(&store, &behind).await,
            Some(ReplicationState::Pending),
            "the burst stopped before reaching the part behind",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_not_ready_enqueue_commits_replicated_and_lands_on_the_sweep_worklist(pool: PgPool) {
        // Tier-2 decoupled commit end-to-end through drain_next: a part whose Ceph copy + verify
        // succeed but whose backend enqueue is not ready (an in-flight MPU whose address is NULL)
        // now (1) COMMITS Replicated and frees the SSD copy — it does NOT defer + re-copy as
        // before; (2) counts as `drained`, not `deferred`/`failed`; (3) is left unstamped
        // (upload_enqueued_at NULL) on the enqueue-sweep worklist so the publish is re-driven once
        // the address lands. The enforcer's breaker trips on a single Ceph failure, so a
        // subsequently-admitted drain proves a successful commit never signalled it.
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();
        let enforcer = Arc::new(Mutex::new(Enforcer::new(
            CircuitBreaker::new(BreakerConfig {
                failure_threshold: 1,
                cooldown: Duration::from_secs(5),
            }),
            TokenBucket::new(ByteRate::new(1_000_000), Bytes::new(1_000_000), Instant::now()),
            ConcurrencyLimiter::new(4),
        )));
        let part = part_at(5, 1);
        seed_part(ssd_dir.path(), &store, &part, &[b"deferred part bytes"]).await;

        let outcome = drain_next(&ceph, &ssd, &store, &DeferringEnqueuer, Some(&enforcer), Some(&snapshot))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ClaimOutcome::Drained(DrainOutcome::Replicated),
            "a not-ready enqueue still commits Replicated"
        );

        let counts = snapshot.load();
        assert_eq!(counts.drained, 1, "the part was committed (Ceph-durable)");
        assert_eq!(counts.deferred, 0, "a not-ready enqueue is no longer a deferral");
        assert_eq!(counts.failed, 0, "and it is not a Ceph-write failure");
        assert_eq!(counts.error_bps(), 0, "so it stays out of the Ceph failure rate");

        assert_eq!(
            status_of(&store, &part).await,
            Some(ReplicationState::Replicated),
            "the Ceph commit is decoupled from the address-gated enqueue",
        );
        assert!(
            !ssd_dir.path().join(part.relative_dir()).exists(),
            "the SSD copy is freed — the uploader reads the chunks from the shared pool",
        );
        let worklist = store.list_replicated_unenqueued_parts(10).await.unwrap();
        assert!(
            worklist.contains(&part),
            "the un-enqueued replicated part is on the enqueue-sweep worklist",
        );

        // The breaker (threshold 1) never saw a failure, so a fresh drain is still
        // admitted; the released permit means concurrency is not the blocker.
        assert_eq!(
            enforcer.lock().unwrap().try_drain(1, Instant::now()),
            DrainDecision::Allowed,
            "a committed drain neither tripped the breaker nor leaked the permit",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_burst_that_fails_midway_keeps_the_drained_count(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        let snapshot = SnapshotCell::new();
        let token = CancellationToken::new();

        // One good part (drains), then one whose POOL destination is blocked so its copy
        // fails with a GENUINE (non-ENOENT) Ceph-write error — a stand-in for a sick/full
        // pool, which unlike a vanished SSD source (ENOENT, a benign deferral) MUST count
        // as a failure. Both seeded on SSD so the bad one reaches the pool write; claimed
        // in landed_at order, so the good one drains first.
        seed_part(ssd_dir.path(), &store, &part_at(5, 1), &[b"good part bytes"]).await;
        seed_part(ssd_dir.path(), &store, &part_at(5, 2), &[b"bad part bytes"]).await;
        // A regular file where part 2's pool directory must go makes the copy's
        // `create_dir_all` fail with AlreadyExists (deterministic and root-safe, unlike a
        // chmod-based denial that root would bypass in CI).
        let blocked = pool_dir.path().join(part_at(5, 2).relative_dir());
        std::fs::create_dir_all(blocked.parent().unwrap()).unwrap();
        std::fs::write(&blocked, b"not a directory").unwrap();

        // The burst drains the good part then fails on the bad one. The #10 fix:
        // the part drained before the failure is NOT discarded. concurrency=1 keeps the
        // strict landed_at order this test asserts on.
        drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, None, Some(&snapshot), &token, 1)
            .await
            .unwrap_err();
        let counts = snapshot.load();
        assert_eq!(counts.drained, 1, "the part drained before the failure is kept");
        assert_eq!(counts.failed, 1, "the failed part is counted once");
        // error_bps is dimensionally clean: 1 failed of 2 attempts = 5000 bps.
        assert_eq!(counts.error_bps(), 5000, "failed attempts over total attempts");
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_not_ready_enqueue_does_not_stall_the_burst_and_only_it_awaits_the_sweep(pool: PgPool) {
        // Tier-2 decoupled commit: a not-ready enqueue no longer defers, so ALL three parts
        // commit Replicated in one burst — part 1's backend enqueue is not ready (address NULL)
        // but it still commits, and parts 2 and 3 enqueue inline. Only part 1 is left unstamped
        // on the enqueue-sweep worklist; parts 2 and 3 are stamped and off it. concurrency=1
        // forces serial claims so part 1 (the oldest) is reached first.
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);
        for number in 1..=3_u32 {
            seed_part(ssd_dir.path(), &store, &part_at(5, number), &[b"backlog part"]).await;
        }

        let token = CancellationToken::new();
        let drained = drain_until_empty(&ceph, &ssd, &store, &DeferPartOneEnqueuer, None, None, &token, 1)
            .await
            .unwrap()
            .drained;
        assert_eq!(drained, 3, "all three parts committed (a not-ready enqueue no longer defers)");

        for number in 1..=3_u32 {
            assert_eq!(
                status_of(&store, &part_at(5, number)).await,
                Some(ReplicationState::Replicated),
                "part {number} committed Replicated",
            );
        }
        let worklist = store.list_replicated_unenqueued_parts(10).await.unwrap();
        assert_eq!(
            worklist,
            vec![part_at(5, 1)],
            "only the not-ready part 1 awaits the enqueue sweep; parts 2 & 3 enqueued inline",
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn drain_until_empty_drains_the_whole_backlog(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);

        // Three honest parts pending under one object version.
        for number in 1..=3_u32 {
            seed_part(ssd_dir.path(), &store, &part_at(5, number), &[b"backlog part"]).await;
        }

        let token = CancellationToken::new();
        let drained = drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, None, None, &token, 4)
            .await
            .unwrap()
            .drained;
        assert_eq!(drained, 3, "every pending part was drained in one run");
        // The backlog is now empty.
        assert_eq!(
            drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, None, None, &token, 4)
                .await
                .unwrap()
                .drained,
            0
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn drain_until_empty_refills_beyond_the_initial_concurrency_wave(pool: PgPool) {
        // F14: a backlog larger than the concurrency must still drain fully — the
        // in-flight set is refilled as each drain completes, not capped at one wave.
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);

        for number in 1..=8_u32 {
            seed_part(ssd_dir.path(), &store, &part_at(5, number), &[b"backlog part"]).await;
        }

        let token = CancellationToken::new();
        let drained = drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, None, None, &token, 3)
            .await
            .unwrap()
            .drained;
        assert_eq!(drained, 8, "all 8 parts drained with concurrency 3 (refill works)");
        assert_eq!(
            drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, None, None, &token, 3)
                .await
                .unwrap()
                .drained,
            0
        );
    }

    #[sqlx::test(migrations = "../hippius-drain-core/migrations")]
    async fn a_cancelled_drain_stops_at_the_part_boundary(pool: PgPool) {
        let ssd_dir = tempfile::tempdir().unwrap();
        let pool_dir = tempfile::tempdir().unwrap();
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = Store::from_pool(pool);

        // A real backlog of three parts.
        for number in 1..=3_u32 {
            seed_part(ssd_dir.path(), &store, &part_at(5, number), &[b"backlog part"]).await;
        }

        // M6: with the token already cancelled (shutdown signalled), the burst must
        // not run to completion — it stops at the first part boundary so the worker
        // honors the supervisor's grace instead of being force-aborted mid-backlog.
        let token = CancellationToken::new();
        token.cancel();
        let drained = drain_until_empty(&ceph, &ssd, &store, &NoopEnqueuer, None, None, &token, 4)
            .await
            .unwrap()
            .drained;
        assert_eq!(drained, 0, "a cancelled drain stops before touching the backlog");
    }
}
