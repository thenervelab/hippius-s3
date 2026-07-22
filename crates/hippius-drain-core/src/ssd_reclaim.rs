//! The SSD-ingest reclaim backstop: clean up **broken / abandoned uploads** left on
//! the node-local SSD.
//!
//! Scope is deliberately narrow — only [`Failed`](ReplicationState::Failed) parts: an
//! aborted or abandoned upload (an MPU abort, an abandoned MPU the reaper marked
//! terminal, or a failed single-part PUT). `claim_part` skips a `failed` row forever,
//! so the drain never unlinks it and its SSD bytes leak with nothing else to reclaim
//! them. This worker is that missing owner.
//!
//! But `failed` is not a clean proxy for "safe to delete": the drain's corruption path
//! (`mark_failed` on a persistent `ChunkMismatch`) can mark a part of a *servable, live*
//! object `failed` when its pool copy is corrupt — and then this SSD part is the **last
//! good source**, not junk. So the `failed` reclaim is gated on the version being
//! **unservable**: an aged `failed` part is reclaimed only when its `object_versions` row
//! is absent or unservable (the abandoned-upload shape); a `failed` part whose version is
//! still servable is the corrupt-live case — left in place, counted `skipped_corrupt`, and
//! alarmed by the agent. The servable/unservable split is the same download-servability
//! predicate the Python A21 sweep and `janitor_part_terminally_abandoned.sql` use, so one
//! definition of "servable" governs the mark path and this delete gate.
//!
//! It also reclaims **deleted-object orphans**: a part with NO replication row whose
//! `object_versions` row is gone (a hard-deleted/purged object), once aged past
//! `orphan_grace`. Such a part has no terminal `failed` row to key on — its cephor row
//! was pruned or never written — so the `failed` path above cannot reach it and it leaks
//! forever (`skipped_absent`). Safety rests on the api's reserve-before-write ordering:
//! the `object_versions` row is created *before* any part hits the ingest SSD, so an
//! absent row can only mean the object was deleted, never an in-flight upload. A
//! present-but-unservable row (an aborted/abandoned upload) is left to the central
//! `failed`-marking sweep + the `failed` path here, NOT treated as an orphan — that
//! avoids racing an in-progress MPU whose reserved row is also unservable.
//!
//! It reclaims **`replicated` crash-orphans**: on the happy path the drain unlinks its own
//! SSD copy the instant it commits a replication. A replicated copy that lingers is a
//! **drain crash-orphan** — a crash between the `mark_replicated` commit and the unlink —
//! which `claim_part` never re-selects, so nothing else re-drives the unlink and it leaks
//! (an inode/dir leak on `/s3-data`, unbounded across agent restarts). This worker re-drives
//! it: a `replicated` part older than `replicated_grace` is unlinked, which is exactly what
//! the happy-path unlink would have done. The safety argument is that this is strictly weaker
//! than the happy path, not stronger: that unlink runs milliseconds after the commit, so
//! re-driving it after a grace introduces no risk the happy path does not already accept.
//! Note the ingest SSD IS read — it is the api-local reader's *primary* tier, with the
//! `CephFS` pool as its read *fallback* (`DualFileSystemPartsStore`) — so a same-node GET can
//! hit the SSD copy first. But `mark_replicated` is committed only after the pool copy is
//! written, byte-verified, and fsynced, so deleting the SSD copy merely makes a same-node read
//! fall through to that durable fallback (the exact behaviour the fallback tier exists for),
//! identical to the drain's own post-commit unlink. The pool copy is thus authoritative — the
//! `replicated` state IS the drain's own record that the `CephFS` copy exists — and, unlike a
//! `failed` part, a `replicated` one is never a corrupt-live object's last good source (a
//! corrupt pool copy transitions the row to `failed`/`corrupt`, out of this arm), so no
//! servability gate is needed. The grace only avoids racing a just-committed part whose
//! in-process unlink has not yet run; a young `replicated` part is left (`skipped_replicated`).
//! (An in-flight MPU whose address takes longer than the grace to finalize is reclaimed while
//! still `replicated`/un-enqueued — safe: reads are served from the pool and the janitor's
//! replication gate holds the pool copy until the upload backends have it.) `pending`/`draining`
//! parts are live (owned by the drain pipeline) and a no-row part whose object still exists may
//! be mid-upload — both are left strictly alone.
//!
//! Safety: `failed` is a terminal sink (nothing returns a row to `pending` except
//! `release_part`/`defer_part`, each guarded on `status='draining'`), so the read can
//! never race back to a live part; removal is idempotent; a brief `grace` keeps a
//! just-failed part (a diagnosis window, and headroom so an in-flight abort txn is not
//! raced). Age uses the store's clock, so the grace has no agent-clock dependence.
//!
//! Like [`crate::reconcile_parts`] the orchestrator is I/O-free: it is generic over
//! the [`PartScan`] discovery seam, a [`PartRemover`] removal seam, and a
//! [`ReclaimLog`] batched-status seam, so it is tested with in-memory fakes while the
//! real `tokio`/Postgres impls live at the edges (`LocalSsd`, this crate's `Store`).

use crate::apipart::PartKey;
use crate::reconcile::PartScan;
use crate::state::ReplicationState;
use core::future::Future;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use thiserror::Error;

/// The SSD removal seam: unlink a reclaimed part's whole directory.
///
/// Idempotent — an already-absent part is `Ok` — so a reclaim racing the drain's
/// own success-path unlink (or a re-drive after a crash) is harmless. Implemented by
/// `hippius-drain-agent`'s `LocalSsd`; faked in tests. The future is `Send` so the
/// reclaim can be spawned on the multithreaded runtime.
pub trait PartRemover: Send + Sync {
    /// Unlinks the part's whole directory from the SSD cache. Named `unlink_part`
    /// (not `remove_part`) so a concrete `LocalSsd` call stays unambiguous when this
    /// seam and [`PartSource`](crate::PartSource) (the drain's success-path unlink)
    /// are both in scope.
    fn unlink_part(&self, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// A scanned part's stored replication state and how long it has held it.
///
/// `age` is `now() - updated_at` measured by the store's clock (not the agent's), so
/// the grace comparison has no clock-skew dependence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartStatusAge {
    /// The part's current replication state.
    pub state: ReplicationState,
    /// How long the part has held that state (`now() - updated_at`).
    pub age: Duration,
}

/// The store seam the reclaim worker needs: read the replication state + age of a
/// whole batch of scanned parts in ONE round-trip.
///
/// Distinct from [`PartLandingLog`](crate::PartLandingLog) (the reconciler's per-part
/// `status`): the reclaim worker scans the entire SSD each cycle, so a per-part SELECT
/// would be O(backlog) round-trips. Implemented by [`crate::Store`] (under `pg`) with a
/// single `IN (UNNEST(...))` query; faked in tests.
pub trait ReclaimLog: Send + Sync {
    /// Store-specific failure, boxed into [`ReclaimError::Log`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The replication state + age of every `parts` entry the store has a row for.
    /// A part with no row is simply absent from the map (the caller skips it).
    fn part_states(&self, parts: &[PartKey]) -> impl Future<Output = Result<HashMap<PartKey, PartStatusAge>, Self::Error>> + Send;
}

/// The `object_versions` backing seam: answers two questions about a batch of parts, both
/// resolved against the same table so they share one seam (and one [`ReclaimError::Backing`]
/// error bucket) rather than fanning into separate traits.
///
/// - [`unbacked_parts`](Self::unbacked_parts) — which parts have NO row at all (a
///   hard-deleted object). Consulted only for parts the reclaim log has no replication row
///   for (the [`skipped_absent`](ReclaimReport::skipped_absent) tail), to split a genuinely
///   orphaned part from one merely mid-upload or pre-reconcile. The safety rests on the
///   api's reserve-before-write ordering: the `object_versions` row is created *before* any
///   part is written to the ingest SSD, so a part whose `(object_id, version)` has NO row
///   can only be a deleted object — never an in-flight upload.
/// - [`servable_parts`](Self::servable_parts) — which parts' version row still SERVES a GET.
///   Consulted only for aged `failed` parts, to hold back the corrupt-live case (a servable
///   object whose pool copy is corrupt) from the `failed` reclaim. Row *presence* alone is
///   not enough here: an aborted/abandoned upload leaves a present-but-unservable row, which
///   `unbacked_parts` counts as backed but which is still safe to reclaim — so the two
///   questions are genuinely distinct and both are needed.
///
/// Implemented by [`crate::Store`] (under `pg`) with batched PK lookups against
/// `object_versions`; faked in tests. The futures are `Send` for the multithreaded runtime.
pub trait BackingLog: Send + Sync {
    /// Store-specific failure, boxed into [`ReclaimError::Backing`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The subset of `parts` whose `(object_id, version)` has NO `object_versions` row.
    /// A part WITH a row is absent from the result (it has live backing — left alone).
    fn unbacked_parts(&self, parts: &[PartKey]) -> impl Future<Output = Result<HashSet<PartKey>, Self::Error>> + Send;

    /// The subset of `parts` whose `(object_id, version)` row EXISTS and is SERVABLE — the
    /// inverse of `janitor_part_terminally_abandoned.sql`'s unservable predicate
    /// (`address IS NULL AND size_bytes <= 0 AND COALESCE(md5_hash,'') = ''`) for the servable
    /// disjuncts, i.e. a row with `address` set OR `size_bytes > 0` OR a non-empty `md5_hash`.
    /// A part with no row, or with an unservable (abandoned-upload) row, is absent from the
    /// result — the reclaim treats those as safe to delete. A returned part is the corrupt-live
    /// case the `failed` reclaim must NOT delete. The two predicates are not bit-identical under
    /// a NULL `size_bytes`: a bare all-NULL row is unservable here (so reclaimable) while the
    /// janitor's `size_bytes <= 0` is NULL so it never sweeps it — both fail safe, since a
    /// bare-NULL row can serve no GET.
    fn servable_parts(&self, parts: &[PartKey]) -> impl Future<Output = Result<HashSet<PartKey>, Self::Error>> + Send;
}

/// What one reclaim pass did, tallied by the part's disposition. `scanned` always
/// equals the sum of the eight outcome counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReclaimReport {
    /// Parts seen on SSD.
    pub scanned: u64,
    /// `failed` parts (aborted/abandoned uploads) reclaimed.
    pub reclaimed: u64,
    /// No-DB-backing orphans reclaimed: a part with no replication row AND no
    /// `object_versions` row (a hard-deleted object), aged past `orphan_grace`. The
    /// deleted-object leak the `failed` path cannot reach — see the module doc.
    pub reclaimed_orphan: u64,
    /// `replicated` crash-orphans reclaimed: the drain committed `mark_replicated` but crashed
    /// before unlinking its SSD copy, so nothing re-drove the unlink. Reclaimed once older than
    /// `replicated_grace` — re-driving the happy-path unlink the crash skipped. See the module
    /// doc and [`skipped_replicated`](Self::skipped_replicated).
    pub reclaimed_replicated: u64,
    /// Left alone because still `pending`/`draining` — owned by the drain pipeline.
    pub skipped_live: u64,
    /// Left alone because `replicated` but still within `replicated_grace` — a just-committed
    /// part whose happy-path unlink may not have run yet. Aged ones are reclaimed
    /// ([`reclaimed_replicated`](Self::reclaimed_replicated)), so this counts only the
    /// transient in-flight-unlink window, not an unreclaimable leak.
    pub skipped_replicated: u64,
    /// Left alone because the store has no replication row and the part still has a live
    /// `object_versions` row (pre-reconcile or mid-upload) — or is not yet past
    /// `orphan_grace`. The absolute safety gate for a part with no terminal signal.
    pub skipped_absent: u64,
    /// `failed` but within the grace window (diagnosis / abort-race headroom).
    pub skipped_young: u64,
    /// A part held because its live object's pool copy is corrupt (R4): either a first-class
    /// `Corrupt` row (the drain's `ChunkMismatch` path marks a servable object `corrupt`
    /// directly) OR the defense-in-depth aged-`failed`+still-servable case not yet promoted.
    /// This SSD part is the last good source, so it is NEVER reclaimed. This is LIVE, not
    /// hypothetical — the drain marks servable parts `corrupt` today — so a non-zero value is a
    /// real durability incident, not routine GC; the agent logs it at ERROR and the
    /// `drain_corrupt_parts` gauge/alert pages. The re-drive worker recovers these.
    pub skipped_corrupt: u64,
}

impl ReclaimReport {
    /// The sum of the per-disposition categories.
    ///
    /// Every scanned part falls into exactly one, so this must equal
    /// [`scanned`](Self::scanned) — the report's invariant, which [`reclaim_ssd`]
    /// `debug_asserts` before returning.
    #[must_use]
    pub fn categorized(&self) -> u64 {
        self.reclaimed
            .saturating_add(self.reclaimed_orphan)
            .saturating_add(self.reclaimed_replicated)
            .saturating_add(self.skipped_live)
            .saturating_add(self.skipped_replicated)
            .saturating_add(self.skipped_absent)
            .saturating_add(self.skipped_young)
            .saturating_add(self.skipped_corrupt)
    }
}

/// A reclaim failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReclaimError {
    /// Walking the SSD cache failed.
    #[error("scanning the SSD cache failed")]
    Scan(#[source] std::io::Error),
    /// The batched status read failed. Boxed so the orchestrator stays decoupled
    /// from any one store backend.
    #[error("the reclaim log failed during a reclaim pass")]
    Log(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The batched object-backing read failed. Kept distinct from [`Log`](Self::Log) so
    /// ops can tell a `cephor_replication_status` read from an `object_versions` read.
    #[error("the object-backing read failed during a reclaim pass")]
    Backing(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Unlinking a reclaimed part's directory failed.
    #[error("removing a reclaimed part from the SSD cache failed")]
    Remove(#[source] std::io::Error),
}

impl ReclaimError {
    /// Boxes a [`ReclaimLog::Error`] into [`ReclaimError::Log`].
    fn log<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Log(Box::new(err))
    }

    /// Boxes a [`BackingLog::Error`] into [`ReclaimError::Backing`].
    fn backing<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Backing(Box::new(err))
    }
}

/// Reclaims broken/abandoned-upload (`failed`) parts, `replicated` crash-orphans, and
/// no-DB-backing orphans from the SSD cache, once aged.
///
/// Scans every complete part on SSD, reads all their replication states in one batch, and
/// unlinks each `failed` part that is older than `grace` **and whose version is unservable**
/// (an aged `failed` part whose object is still servable is the corrupt-live case — held
/// back as `skipped_corrupt`, never deleted; see the module doc). A part with NO replication
/// row is checked against [`BackingLog::unbacked_parts`]: if its object was hard-deleted (no
/// `object_versions` row) AND it has aged past `orphan_grace`, it is a deleted-object orphan
/// and is reclaimed too. A `replicated` part older than `replicated_grace` is a drain
/// crash-orphan (the drain crashed between the `mark_replicated` commit and its own unlink)
/// and is unlinked — re-driving the happy-path unlink the crash skipped. Everything else is
/// left untouched: `pending`/`draining` are live (drain-owned), a young `replicated` part
/// may have an in-flight unlink still pending, and a no-row part that still has a live
/// `object_versions` row may be mid-upload — the absolute safety gate.
///
/// The servability read ([`BackingLog::servable_parts`]) and the orphan-backing read
/// ([`BackingLog::unbacked_parts`]) each run over a disjoint subset (aged-`failed` vs
/// no-row) and are both empty on the steady-state happy path, so neither adds a round-trip
/// unless there is actually broken data to adjudicate.
///
/// The `failed` and `replicated` graces use the store clock (the row's `updated_at`), so
/// neither has an agent-clock dependence; the orphan grace uses the part's SSD `meta.json`
/// age ([`DiscoveredPart::age`](crate::DiscoveredPart)), since a deleted object has no DB
/// row to date. Orphan reclaim therefore has an agent-clock dependence that the status-path
/// reclaims do not — `orphan_grace` is set generously to absorb it.
///
/// # Errors
///
/// - [`ReclaimError::Scan`] if walking the cache fails.
/// - [`ReclaimError::Log`] if the batched status read fails (nothing is removed).
/// - [`ReclaimError::Backing`] if the batched object-backing read fails (nothing is removed).
/// - [`ReclaimError::Remove`] if unlinking a reclaimed part fails.
pub async fn reclaim_ssd<S, R, L, B>(
    scanner: &S,
    remover: &R,
    log: &L,
    backing: &B,
    grace: Duration,
    orphan_grace: Duration,
    replicated_grace: Duration,
) -> Result<ReclaimReport, ReclaimError>
where
    S: PartScan,
    R: PartRemover,
    L: ReclaimLog,
    B: BackingLog,
{
    let parts = scanner.scan_parts().await.map_err(ReclaimError::Scan)?;
    let mut report = ReclaimReport::default();
    if parts.is_empty() {
        return Ok(report);
    }

    // One batched status read for the whole scan — never a per-part SELECT (the
    // reconciler's O(backlog) cost the reclaim worker must not repeat).
    let keys: Vec<PartKey> = parts.iter().map(|discovered| discovered.part.clone()).collect();
    let states = log.part_states(&keys).await.map_err(ReclaimError::log)?;

    // The object-backing read is needed only for parts with no replication row (the
    // skipped_absent tail). Gathering them first keeps it one batched query over that
    // subset — usually a small fraction of the scan, empty on the steady-state happy path.
    let absent: Vec<PartKey> = parts
        .iter()
        .filter(|discovered| !states.contains_key(&discovered.part))
        .map(|discovered| discovered.part.clone())
        .collect();
    let unbacked = if absent.is_empty() {
        HashSet::new()
    } else {
        backing.unbacked_parts(&absent).await.map_err(ReclaimError::backing)?
    };

    // The servability read guards the `failed` reclaim against deleting a corrupt-live
    // object's last good copy. Scoped to aged `failed` parts only (the sole reclaim
    // candidates on the status path): a young `failed`, or any non-`failed` state, is
    // decided without it, so this stays off the happy path exactly like the backing read.
    let failed_aged: Vec<PartKey> = parts
        .iter()
        .filter(|discovered| {
            states
                .get(&discovered.part)
                .is_some_and(|status| status.state == ReplicationState::Failed && status.age >= grace)
        })
        .map(|discovered| discovered.part.clone())
        .collect();
    let servable = if failed_aged.is_empty() {
        HashSet::new()
    } else {
        backing.servable_parts(&failed_aged).await.map_err(ReclaimError::backing)?
    };

    for discovered in parts {
        report.scanned += 1;
        let part = &discovered.part;

        // No replication row. Reclaim ONLY a deleted-object orphan: no `object_versions`
        // row (unbacked) AND aged past `orphan_grace`. A part whose version still has a
        // live row is mid-upload or pre-reconcile — never touched (the absolute safety
        // gate; reserve-before-write means an absent row can only be a deleted object).
        let Some(status) = states.get(part) else {
            if unbacked.contains(part) && discovered.age >= orphan_grace {
                remover.unlink_part(part).await.map_err(ReclaimError::Remove)?;
                report.reclaimed_orphan += 1;
            } else {
                report.skipped_absent += 1;
            }
            continue;
        };

        match status.state {
            // Live: owned by the drain pipeline.
            ReplicationState::Pending | ReplicationState::Draining => report.skipped_live += 1,
            // Replicated: the drain unlinks its own SSD copy the instant it commits, so a
            // lingering one is a crash-orphan (a crash between the commit and the unlink).
            // Re-drive that unlink once past `replicated_grace` — exactly what the happy path
            // would have done, and strictly weaker than it (see the module doc). No
            // servability gate is needed: unlike `failed`, a `replicated` part is never a
            // corrupt-live object's last good source. A young one is left in case its
            // in-flight unlink has simply not run yet.
            ReplicationState::Replicated => {
                if status.age >= replicated_grace {
                    remover.unlink_part(part).await.map_err(ReclaimError::Remove)?;
                    report.reclaimed_replicated += 1;
                } else {
                    report.skipped_replicated += 1;
                }
            }
            // Failed = a broken/abandoned upload (MPU abort, abandoned MPU, or a failed
            // single-part PUT) — reclaimed once past grace — UNLESS the version is still
            // servable, in which case `failed` means "corrupt pool copy on a live object"
            // and this SSD part is the last good source (skipped_corrupt; never deleted).
            ReplicationState::Failed => {
                if status.age < grace {
                    report.skipped_young += 1;
                } else if servable.contains(part) {
                    report.skipped_corrupt += 1;
                } else {
                    remover.unlink_part(part).await.map_err(ReclaimError::Remove)?;
                    report.reclaimed += 1;
                }
            }
            // Corrupt = a live object whose pool copy is corrupt, marked directly by the drain
            // (R4). Its SSD copy is the last good source and the re-drive worker owns it, so it
            // is NEVER reclaimed however aged — held and counted (the `Failed`+servable arm above
            // stays as defense-in-depth for a row not yet promoted to `Corrupt`).
            ReplicationState::Corrupt => report.skipped_corrupt += 1,
        }
    }

    debug_assert_eq!(
        report.scanned,
        report.categorized(),
        "every scanned part lands in exactly one disposition"
    );
    Ok(report)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{BackingLog, PartRemover, PartStatusAge, ReclaimError, ReclaimLog, ReclaimReport, reclaim_ssd};
    use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
    use crate::reconcile::{DiscoveredPart, PartScan};
    use crate::state::ReplicationState;
    use core::future::Future;
    use core::str::FromStr;
    use std::collections::{HashMap, HashSet};
    use std::io;
    use std::sync::Mutex;
    use std::time::Duration;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";
    const UUID_B: &str = "00000000-0000-4000-8000-000000000000";

    fn part_at(uuid: &str, version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(uuid).unwrap(), Version::new(version), PartNumber::new(number))
    }

    fn key(part: &PartKey) -> String {
        part.relative_dir().to_string_lossy().into_owned()
    }

    /// A scanner yielding a fixed part list (or a fault). `of` stamps every part with a
    /// stale FS age (older than any test's `ORPHAN_GRACE`), so an absent part is orphan-age
    /// by default; `of_aged` pins each part's age for the orphan-grace boundary tests.
    struct FakeScan {
        parts: Vec<DiscoveredPart>,
        fail: bool,
    }

    impl FakeScan {
        fn of(parts: &[PartKey]) -> Self {
            Self {
                parts: parts.iter().map(|p| DiscoveredPart { part: p.clone(), age: HOUR }).collect(),
                fail: false,
            }
        }

        fn of_aged(parts: &[(PartKey, Duration)]) -> Self {
            Self {
                parts: parts.iter().map(|(p, age)| DiscoveredPart { part: p.clone(), age: *age }).collect(),
                fail: false,
            }
        }
    }

    impl PartScan for FakeScan {
        fn scan_parts(&self) -> impl Future<Output = io::Result<Vec<DiscoveredPart>>> + Send {
            let result = if self.fail {
                Err(io::Error::other("scan failed"))
            } else {
                Ok(self.parts.clone())
            };
            async move { result }
        }
    }

    /// The object-backing fake, over two independent axes: `unbacked` (their object was
    /// deleted → returned by `unbacked_parts`) and `servable` (their version still serves a
    /// GET → returned by `servable_parts`). `all_backed` (the default) leaves both empty, so
    /// no part is an orphan and none is servable — exactly the state the pre-WI-20b
    /// `failed`-only tests assume (aged `failed` parts reclaim, none held as corrupt-live).
    /// Records what each read was asked about so a test can assert each stays scoped to its
    /// subset (no-row parts for backing; aged-`failed` parts for servability).
    #[derive(Default)]
    struct FakeBacking {
        unbacked: HashSet<String>,
        servable: HashSet<String>,
        asked: Mutex<Vec<String>>,
        servable_asked: Mutex<Vec<String>>,
        fail: bool,
    }

    impl FakeBacking {
        fn all_backed() -> Self {
            Self::default()
        }

        fn unbacked(parts: &[&PartKey]) -> Self {
            Self {
                unbacked: parts.iter().map(|p| key(p)).collect(),
                ..Self::default()
            }
        }

        fn servable(parts: &[&PartKey]) -> Self {
            Self {
                servable: parts.iter().map(|p| key(p)).collect(),
                ..Self::default()
            }
        }

        fn asked(&self) -> Vec<String> {
            let mut out = self.asked.lock().unwrap().clone();
            out.sort();
            out
        }

        fn servable_asked(&self) -> Vec<String> {
            let mut out = self.servable_asked.lock().unwrap().clone();
            out.sort();
            out
        }
    }

    impl BackingLog for FakeBacking {
        type Error = io::Error;

        fn unbacked_parts(&self, parts: &[PartKey]) -> impl Future<Output = Result<HashSet<PartKey>, io::Error>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("backing read failed"))
            } else {
                self.asked.lock().unwrap().extend(parts.iter().map(key));
                Ok(parts.iter().filter(|p| self.unbacked.contains(&key(p))).cloned().collect())
            };
            async move { outcome }
        }

        fn servable_parts(&self, parts: &[PartKey]) -> impl Future<Output = Result<HashSet<PartKey>, io::Error>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("servability read failed"))
            } else {
                self.servable_asked.lock().unwrap().extend(parts.iter().map(key));
                Ok(parts.iter().filter(|p| self.servable.contains(&key(p))).cloned().collect())
            };
            async move { outcome }
        }
    }

    /// Records every part it is asked to remove; optionally faults.
    #[derive(Default)]
    struct FakeRemover {
        removed: Mutex<Vec<String>>,
        fail: bool,
    }

    impl FakeRemover {
        fn removed(&self) -> Vec<String> {
            let mut out = self.removed.lock().unwrap().clone();
            out.sort();
            out
        }
    }

    impl PartRemover for FakeRemover {
        fn unlink_part(&self, part: &PartKey) -> impl Future<Output = io::Result<()>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("remove failed"))
            } else {
                self.removed.lock().unwrap().push(key(part));
                Ok(())
            };
            async move { outcome }
        }
    }

    /// An in-memory batched status log: a state+age map plus a call counter (so a test
    /// can assert the read is batched into exactly one call).
    #[derive(Default)]
    struct FakeLog {
        states: HashMap<String, PartStatusAge>,
        calls: Mutex<u32>,
        fail: bool,
    }

    impl FakeLog {
        fn with(entries: &[(&PartKey, ReplicationState, Duration)]) -> Self {
            let mut states = HashMap::new();
            for (part, state, age) in entries {
                states.insert(key(part), PartStatusAge { state: *state, age: *age });
            }
            Self {
                states,
                calls: Mutex::new(0),
                fail: false,
            }
        }

        fn calls(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    impl ReclaimLog for FakeLog {
        type Error = io::Error;

        fn part_states(&self, parts: &[PartKey]) -> impl Future<Output = Result<HashMap<PartKey, PartStatusAge>, io::Error>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("status failed"))
            } else {
                *self.calls.lock().unwrap() += 1;
                let mut out = HashMap::new();
                for part in parts {
                    if let Some(status) = self.states.get(&key(part)) {
                        out.insert(part.clone(), *status);
                    }
                }
                Ok(out)
            };
            async move { outcome }
        }
    }

    const HOUR: Duration = Duration::from_hours(1);
    const GRACE: Duration = Duration::from_mins(30);
    const ORPHAN_GRACE: Duration = Duration::from_mins(45);
    // Larger than the `HOUR` age the existing `replicated` fixtures use, so those parts stay
    // within grace (`skipped_replicated`); the reclaim-when-aged cases below use ages past it.
    const REPLICATED_GRACE: Duration = Duration::from_hours(2);

    #[tokio::test]
    async fn an_aged_failed_part_is_reclaimed() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, HOUR)]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed, 1);
        assert_eq!(remover.removed(), vec![key(&part)], "the aged failed (abandoned) part was unlinked");
    }

    #[tokio::test]
    async fn a_replicated_part_within_grace_is_left_for_the_drains_own_unlink() {
        // A `replicated` part younger than `replicated_grace` may have just committed, with its
        // happy-path unlink still in flight — leave it; the crash-orphan reclaim waits out the
        // grace before re-driving. HOUR < REPLICATED_GRACE (2h), so this is within grace.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Replicated, HOUR)]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.skipped_replicated, 1);
        assert_eq!(report.reclaimed_replicated, 0);
        assert!(remover.removed().is_empty(), "a within-grace replicated part is not reclaimed");
    }

    #[tokio::test]
    async fn an_aged_replicated_crash_orphan_is_reclaimed() {
        // The leak this fix targets: the drain committed `mark_replicated` but crashed before
        // unlinking its SSD copy (agent SIGKILL on eviction/OOM/restart), so nothing re-drove
        // the unlink and the dir lingers forever. Past `replicated_grace` it is unlinked —
        // re-driving the happy-path unlink the crash skipped.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Replicated, Duration::from_hours(3))]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed_replicated, 1);
        assert_eq!(report.skipped_replicated, 0);
        assert_eq!(remover.removed(), vec![key(&part)], "the aged replicated crash-orphan was unlinked");
    }

    #[tokio::test]
    async fn a_replicated_crash_orphan_exactly_at_the_grace_boundary_is_reclaimed() {
        // The gate is `age >= replicated_grace -> reclaim`, mirroring the failed/orphan arms,
        // so age == grace reclaims. Pins the boundary against a future `>=` vs `>` slip.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Replicated, REPLICATED_GRACE)]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed_replicated, 1, "age == replicated_grace reclaims");
        assert_eq!(report.skipped_replicated, 0);
    }

    #[tokio::test]
    async fn an_aged_replicated_crash_orphan_is_reclaimed_even_when_servable() {
        // Unlike `failed`, a `replicated` part is reclaimed regardless of servability — a
        // servable object is the normal case, and its pool copy is authoritative (a corrupt
        // pool copy would have transitioned the row to failed/corrupt, out of this arm). The
        // servability read is for the `failed` arm only and must NOT gate this one, so it is
        // never even consulted for a `replicated` part.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Replicated, Duration::from_hours(3))]);
        let backing = FakeBacking::servable(&[&part]);

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed_replicated, 1, "a servable replicated part is still reclaimed");
        assert_eq!(remover.removed(), vec![key(&part)]);
        assert!(
            backing.servable_asked().is_empty(),
            "the servability read is never consulted for a replicated part"
        );
    }

    #[tokio::test]
    async fn pending_draining_replicated_and_no_row_parts_are_never_reclaimed() {
        // The absolute safety invariant: a part the drain still owns (pending/draining) or one
        // with no row must NEVER be unlinked here, regardless of age; a `replicated` part is
        // left too while within `replicated_grace` (HOUR < 2h here — the aged-replicated
        // crash-orphan reclaim is exercised separately below).
        let pending = part_at(UUID_A, 1, 1);
        let draining = part_at(UUID_A, 1, 2);
        let replicated = part_at(UUID_A, 1, 3);
        let absent = part_at(UUID_B, 7, 1);
        let scan = FakeScan::of(&[pending.clone(), draining.clone(), replicated.clone(), absent.clone()]);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[
            (&pending, ReplicationState::Pending, HOUR),
            (&draining, ReplicationState::Draining, HOUR),
            (&replicated, ReplicationState::Replicated, HOUR),
            // `absent` has no row in the log at all.
        ]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed, 0, "nothing but a failed part is ever reclaimed");
        assert_eq!(report.skipped_live, 2);
        assert_eq!(report.skipped_replicated, 1);
        assert_eq!(report.skipped_absent, 1);
        assert!(remover.removed().is_empty(), "no part was unlinked");
    }

    #[tokio::test]
    async fn a_young_failed_part_is_kept_within_the_grace_window() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        // Failed but only just now — within grace (an abort txn may still be settling,
        // and a corruption sample is worth a brief diagnosis window).
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, Duration::from_mins(1))]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.skipped_young, 1);
        assert!(remover.removed().is_empty(), "a young failed part is kept");
    }

    #[tokio::test]
    async fn a_failed_part_exactly_at_the_grace_boundary_is_reclaimed() {
        // The gate is `age < grace -> keep`, so age == grace falls through to reclaim.
        // Pins the boundary so a future `<` vs `<=` slip is caught.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, GRACE)]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed, 1, "age == grace reclaims");
        assert_eq!(report.skipped_young, 0);
    }

    #[tokio::test]
    async fn the_status_read_is_batched_into_one_call() {
        let parts: Vec<PartKey> = (1..=5).map(|n| part_at(UUID_A, 5, n)).collect();
        let scan = FakeScan::of(&parts);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&parts.iter().map(|p| (p, ReplicationState::Failed, HOUR)).collect::<Vec<_>>());

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed, 5);
        assert_eq!(log.calls(), 1, "all five parts' statuses were read in one batched call");
    }

    #[tokio::test]
    async fn a_mixed_cache_tallies_each_disposition_and_sums_to_scanned() {
        let fail = part_at(UUID_A, 1, 1); // aged failed -> reclaimed
        let repl = part_at(UUID_A, 1, 2); // replicated -> left for the drain
        let live = part_at(UUID_A, 1, 3); // draining -> live
        let young = part_at(UUID_A, 1, 4); // young failed -> kept
        let absent = part_at(UUID_B, 7, 1); // no row -> absent
        let scan = FakeScan::of(&[fail.clone(), repl.clone(), live.clone(), young.clone(), absent.clone()]);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[
            (&fail, ReplicationState::Failed, HOUR),
            (&repl, ReplicationState::Replicated, HOUR),
            (&live, ReplicationState::Draining, HOUR),
            (&young, ReplicationState::Failed, Duration::from_secs(1)),
        ]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(
            report,
            ReclaimReport {
                scanned: 5,
                reclaimed: 1,
                reclaimed_orphan: 0,
                reclaimed_replicated: 0,
                skipped_live: 1,
                skipped_replicated: 1,
                skipped_absent: 1,
                skipped_young: 1,
                skipped_corrupt: 0,
            }
        );
        assert_eq!(
            report.scanned,
            report.categorized(),
            "every scanned part lands in exactly one disposition"
        );
        assert_eq!(remover.removed(), vec![key(&fail)], "exactly the one aged failed part");
    }

    #[tokio::test]
    async fn a_scan_failure_is_surfaced_and_nothing_is_removed() {
        let scan = FakeScan { parts: vec![], fail: true };
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let err = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap_err();
        assert!(matches!(err, ReclaimError::Scan(_)), "got: {err:?}");
        assert!(remover.removed().is_empty());
    }

    #[tokio::test]
    async fn a_status_read_failure_is_surfaced_and_nothing_is_removed() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(&[part]);
        let remover = FakeRemover::default();
        let log = FakeLog {
            fail: true,
            ..FakeLog::default()
        };
        let err = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap_err();
        assert!(matches!(err, ReclaimError::Log(_)), "got: {err:?}");
        assert!(remover.removed().is_empty(), "a failed status read removes nothing (fail-safe)");
    }

    #[tokio::test]
    async fn a_remove_failure_is_surfaced() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover {
            fail: true,
            ..FakeRemover::default()
        };
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, HOUR)]);
        let err = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap_err();
        assert!(matches!(err, ReclaimError::Remove(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn an_empty_cache_is_a_noop() {
        let scan = FakeScan::of(&[]);
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report, ReclaimReport::default());
        assert_eq!(log.calls(), 0, "an empty scan never queries the store");
    }

    // ------------------------------------------ servability gate (R4 corrupt-live guard)

    #[tokio::test]
    async fn a_servable_object_with_a_failed_row_is_never_reclaimed() {
        // R4: an aged `failed` part whose version is still servable is a live object with a
        // corrupt pool copy — this SSD part is the last good source. Held as skipped_corrupt,
        // never unlinked, however aged.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, HOUR)]);
        let backing = FakeBacking::servable(&[&part]);

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.skipped_corrupt, 1);
        assert_eq!(report.reclaimed, 0);
        assert!(remover.removed().is_empty(), "a servable object's last good copy is preserved");
    }

    #[tokio::test]
    async fn an_unservable_aged_failed_part_reclaims_beside_a_servable_sibling() {
        // The discriminator in action: two aged `failed` parts, one servable (corrupt-live,
        // kept) and one not (abandoned upload, reclaimed). Same state, opposite disposition.
        let abandoned = part_at(UUID_A, 1, 1);
        let corrupt_live = part_at(UUID_A, 2, 1);
        let scan = FakeScan::of(&[abandoned.clone(), corrupt_live.clone()]);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[
            (&abandoned, ReplicationState::Failed, HOUR),
            (&corrupt_live, ReplicationState::Failed, HOUR),
        ]);
        let backing = FakeBacking::servable(&[&corrupt_live]);

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed, 1);
        assert_eq!(report.skipped_corrupt, 1);
        assert_eq!(remover.removed(), vec![key(&abandoned)], "only the unservable (abandoned) part");
    }

    #[tokio::test]
    async fn the_servability_read_covers_only_aged_failed_parts() {
        // Scoped exactly like the backing read: a young `failed`, a `pending`, and a
        // `replicated` part are decided without a servability check — only the aged `failed`
        // candidate is asked about, keeping the object_versions read off the happy path.
        let aged_failed = part_at(UUID_A, 1, 1);
        let young_failed = part_at(UUID_A, 1, 2);
        let pending = part_at(UUID_A, 1, 3);
        let replicated = part_at(UUID_A, 1, 4);
        let scan = FakeScan::of(&[aged_failed.clone(), young_failed.clone(), pending.clone(), replicated.clone()]);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[
            (&aged_failed, ReplicationState::Failed, HOUR),
            (&young_failed, ReplicationState::Failed, Duration::from_secs(1)),
            (&pending, ReplicationState::Pending, HOUR),
            (&replicated, ReplicationState::Replicated, HOUR),
        ]);
        let backing = FakeBacking::all_backed();

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed, 1, "the aged failed part reclaims (not servable)");
        assert_eq!(
            backing.servable_asked(),
            vec![key(&aged_failed)],
            "only the aged failed part was servability-checked"
        );
    }

    #[tokio::test]
    async fn a_servability_read_failure_is_surfaced_and_nothing_is_removed() {
        // The servability read shares the Backing error bucket (both hit object_versions).
        // No absent parts here, so only the servability read runs — its failure must abort
        // the whole pass fail-safe, removing nothing.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, HOUR)]);
        let backing = FakeBacking {
            fail: true,
            ..FakeBacking::default()
        };
        let err = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap_err();
        assert!(matches!(err, ReclaimError::Backing(_)), "got: {err:?}");
        assert!(remover.removed().is_empty(), "a failed servability read removes nothing (fail-safe)");
    }

    // A property test over arbitrary aged `failed` parts each independently servable-or-not:
    // the reclaimed set is EXACTLY the unservable parts and skipped_corrupt EXACTLY the
    // servable count — the corrupt-live guard never deletes a servable part and never spares
    // an unservable one. The shrinker probes the servable/unservable partition a handful of
    // fixtures cannot.
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(200))]
        #[test]
        fn failed_reclaim_removes_exactly_the_unservable_parts(
            specs in proptest::collection::vec((0u32..64, proptest::bool::ANY), 0..40)
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            rt.block_on(async {
                let mut entries: Vec<(PartKey, ReplicationState, Duration)> = Vec::new();
                let mut servable_refs: Vec<PartKey> = Vec::new();
                let mut parts: Vec<PartKey> = Vec::new();
                let mut expected_removed: Vec<String> = Vec::new();
                let mut expected_corrupt = 0u64;
                for (i, (number, is_servable)) in specs.iter().enumerate() {
                    // Distinct parts: vary version by index so no two collide. All aged
                    // failed, so servability is the sole discriminator.
                    let part = part_at(UUID_A, u32::try_from(i).unwrap() + 1, *number);
                    parts.push(part.clone());
                    entries.push((part.clone(), ReplicationState::Failed, HOUR));
                    if *is_servable {
                        servable_refs.push(part.clone());
                        expected_corrupt += 1;
                    } else {
                        expected_removed.push(key(&part));
                    }
                }
                let scan = FakeScan::of(&parts);
                let remover = FakeRemover::default();
                let entry_refs: Vec<(&PartKey, ReplicationState, Duration)> =
                    entries.iter().map(|(p, s, a)| (p, *s, *a)).collect();
                let log = FakeLog::with(&entry_refs);
                let refs: Vec<&PartKey> = servable_refs.iter().collect();
                let backing = FakeBacking::servable(&refs);

                let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE).await.unwrap();
                expected_removed.sort();
                proptest::prop_assert_eq!(remover.removed(), expected_removed.clone());
                proptest::prop_assert_eq!(usize::try_from(report.reclaimed).unwrap(), expected_removed.len());
                proptest::prop_assert_eq!(report.skipped_corrupt, expected_corrupt);
                proptest::prop_assert_eq!(report.scanned, report.categorized());
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn a_corrupt_state_part_is_never_reclaimed_however_aged() {
        // R4 first-class state: a `Corrupt` part (drain marked it directly — a live object whose
        // pool copy is corrupt) is held unconditionally, no servability re-check needed, however
        // aged. The state itself says "last good source"; never unlink.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Corrupt, HOUR)]);

        let report = reclaim_ssd(&scan, &remover, &log, &FakeBacking::all_backed(), GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.skipped_corrupt, 1);
        assert_eq!(report.reclaimed, 0);
        assert!(remover.removed().is_empty(), "a Corrupt part's SSD source is preserved");
    }

    #[tokio::test]
    async fn a_servable_failed_part_at_exactly_the_grace_boundary_is_held() {
        // WI-G boundary: the failed-aged filter is `age >= grace` and the reclaim arm is
        // `age < grace ? young : ...`, so a part at EXACTLY age==grace must be adjudicated (not
        // young) and, being servable, held — the two boundary expressions must agree.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, GRACE)]);
        let backing = FakeBacking::servable(&[&part]);

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(
            report.skipped_corrupt, 1,
            "a servable failed part exactly at grace is held, not reclaimed"
        );
        assert_eq!(report.skipped_young, 0);
        assert!(remover.removed().is_empty());
    }

    // ----------------------------------------------- deleted-object orphans (WI-20b)

    #[tokio::test]
    async fn an_aged_no_row_orphan_of_a_deleted_object_is_reclaimed() {
        // The WI-20b leak: no replication row (its cephor row was pruned or never written)
        // AND no object_versions row (the object was hard-deleted) AND aged on SSD.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of_aged(&[(part.clone(), HOUR)]);
        let remover = FakeRemover::default();
        let log = FakeLog::default(); // no replication row
        let backing = FakeBacking::unbacked(&[&part]); // object_versions row gone

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed_orphan, 1);
        assert_eq!(report.skipped_absent, 0);
        assert_eq!(remover.removed(), vec![key(&part)], "the deleted-object orphan was unlinked");
    }

    #[tokio::test]
    async fn a_no_row_part_whose_object_still_exists_is_never_reclaimed() {
        // The absolute safety gate: no replication row but the object_versions row is still
        // present (mid-upload or pre-reconcile) — must never be unlinked, however aged.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of_aged(&[(part.clone(), HOUR)]);
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let backing = FakeBacking::all_backed(); // object_versions row present

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed_orphan, 0);
        assert_eq!(report.skipped_absent, 1);
        assert!(remover.removed().is_empty(), "a part whose object still exists is protected");
    }

    #[tokio::test]
    async fn a_young_no_row_orphan_within_the_orphan_grace_is_kept() {
        // Unbacked but freshly landed: a delete racing an in-flight re-create is possible,
        // so the grace holds the part until it is unambiguously stale.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of_aged(&[(part.clone(), Duration::from_mins(1))]);
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let backing = FakeBacking::unbacked(&[&part]);

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.skipped_absent, 1);
        assert_eq!(report.reclaimed_orphan, 0);
        assert!(remover.removed().is_empty(), "a young orphan is kept");
    }

    #[tokio::test]
    async fn an_orphan_exactly_at_the_orphan_grace_boundary_is_reclaimed() {
        // The gate is `age < grace -> keep`, so age == orphan_grace reclaims. Pins the
        // boundary against a future `<` vs `<=` slip.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of_aged(&[(part.clone(), ORPHAN_GRACE)]);
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let backing = FakeBacking::unbacked(&[&part]);

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed_orphan, 1, "age == orphan_grace reclaims");
        assert_eq!(report.skipped_absent, 0);
    }

    #[tokio::test]
    async fn the_backing_read_covers_only_the_no_row_parts() {
        // The read is scoped to the skipped_absent tail: a part WITH a replication row is
        // never asked about (its disposition is decided by the batched status read alone),
        // so the object_versions query stays off the happy path.
        let failed = part_at(UUID_A, 1, 1);
        let orphan = part_at(UUID_A, 1, 2);
        let scan = FakeScan::of_aged(&[(failed.clone(), HOUR), (orphan.clone(), HOUR)]);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&failed, ReplicationState::Failed, HOUR)]);
        let backing = FakeBacking::unbacked(&[&orphan]);

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.reclaimed, 1, "the failed part reclaimed via the status path");
        assert_eq!(report.reclaimed_orphan, 1, "the orphan reclaimed via the backing path");
        assert_eq!(backing.asked(), vec![key(&orphan)], "only the no-row part was backing-checked");
    }

    #[tokio::test]
    async fn a_backing_read_failure_is_surfaced_and_nothing_is_removed() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of_aged(&[(part.clone(), HOUR)]);
        let remover = FakeRemover::default();
        let log = FakeLog::default(); // absent → triggers the backing read
        let backing = FakeBacking {
            fail: true,
            ..FakeBacking::default()
        };
        let err = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap_err();
        assert!(matches!(err, ReclaimError::Backing(_)), "got: {err:?}");
        assert!(remover.removed().is_empty(), "a failed backing read removes nothing (fail-safe)");
    }

    #[tokio::test]
    async fn a_fully_backed_absent_cache_never_queries_backing_for_removal() {
        // All absent parts are backed → the backing read runs but yields no orphan; every
        // part is skipped_absent and nothing is removed.
        let a = part_at(UUID_A, 1, 1);
        let b = part_at(UUID_A, 1, 2);
        let scan = FakeScan::of_aged(&[(a.clone(), HOUR), (b.clone(), HOUR)]);
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let backing = FakeBacking::all_backed();

        let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, ORPHAN_GRACE, REPLICATED_GRACE)
            .await
            .unwrap();
        assert_eq!(report.skipped_absent, 2);
        assert_eq!(report.reclaimed_orphan, 0);
        assert!(remover.removed().is_empty());
    }

    // A property test over arbitrary no-row parts: the reclaimed-orphan set is EXACTLY the
    // parts that are both unbacked AND aged past the orphan grace, and everything else is
    // preserved (`skipped_absent`). The shrinker probes the age/backing boundary combos a
    // handful of fixtures cannot. Mirrors the WI-19 plan's reclaim proptest for the failed
    // path, extended to the orphan path.
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(200))]
        #[test]
        fn orphan_reclaim_removes_exactly_unbacked_and_aged_no_row_parts(
            specs in proptest::collection::vec((0u32..64, proptest::bool::ANY, 0u64..7200), 0..40)
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            rt.block_on(async {
                let orphan_grace = Duration::from_hours(1);
                let mut aged: Vec<(PartKey, Duration)> = Vec::new();
                let mut unbacked_refs: Vec<PartKey> = Vec::new();
                let mut expected_removed: Vec<String> = Vec::new();
                for (i, (number, is_unbacked, age_secs)) in specs.iter().enumerate() {
                    // Distinct parts: vary version by index so no two collide.
                    let part = part_at(UUID_A, u32::try_from(i).unwrap() + 1, *number);
                    let age = Duration::from_secs(*age_secs);
                    aged.push((part.clone(), age));
                    if *is_unbacked {
                        unbacked_refs.push(part.clone());
                        if age >= orphan_grace {
                            expected_removed.push(key(&part));
                        }
                    }
                }
                let scan = FakeScan::of_aged(&aged);
                let remover = FakeRemover::default();
                let log = FakeLog::default(); // every part has no replication row
                let refs: Vec<&PartKey> = unbacked_refs.iter().collect();
                let backing = FakeBacking::unbacked(&refs);

                let report = reclaim_ssd(&scan, &remover, &log, &backing, GRACE, orphan_grace, REPLICATED_GRACE).await.unwrap();
                expected_removed.sort();
                proptest::prop_assert_eq!(remover.removed(), expected_removed.clone());
                proptest::prop_assert_eq!(usize::try_from(report.reclaimed_orphan).unwrap(), expected_removed.len());
                proptest::prop_assert_eq!(report.scanned, report.categorized());
                Ok(())
            })?;
        }
    }
}
