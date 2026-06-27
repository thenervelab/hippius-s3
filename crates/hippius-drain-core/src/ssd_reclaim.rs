//! The SSD-ingest reclaim backstop: delete node-local SSD parts the drain is done
//! with, once they have aged past a grace.
//!
//! The drain's success path already unlinks a part right after `mark_replicated`,
//! so in steady state the SSD self-clears. This worker reclaims the leftovers that
//! path misses, both terminal in the replication log:
//! - a [`Replicated`](ReplicationState::Replicated) part orphaned by a crash between
//!   the commit and the unlink (the at-least-once gap), and
//! - a [`Failed`](ReplicationState::Failed) part — an aborted/abandoned upload that
//!   `claim_part` skips forever, so the drain never unlinks it.
//!
//! It NEVER touches a `pending`/`draining` part (owned by the drain pipeline) or a
//! part with no row (pre-reconcile / mid-upload). The gate is absolute and mirrors
//! the CephFS-pool janitor's replication gate: `replicated`/`failed` are terminal
//! sinks (nothing returns a row to `pending` except `release_part`/`defer_part`,
//! each guarded on `status='draining'`), so a terminal read cannot race back to a
//! live part, and removal is idempotent, so racing the drain's own unlink is safe.
//!
//! A `failed` part is kept for the grace window too (a byte-mismatch copy is worth a
//! brief diagnosis window), then reclaimed — the abandoned-upload leak is the same
//! `failed` state and must not be kept forever.
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

/// What one reclaim pass did, tallied by the part's disposition. `scanned` always
/// equals the sum of the five outcome counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReclaimReport {
    /// Parts seen on SSD.
    pub scanned: u64,
    /// `replicated` parts (durable on `CephFS`) reclaimed — crash-orphaned SSD copies.
    pub reclaimed_replicated: u64,
    /// `failed` parts (aborted/abandoned uploads) reclaimed.
    pub reclaimed_failed: u64,
    /// Left alone because still `pending`/`draining` — owned by the drain pipeline.
    pub skipped_live: u64,
    /// Left alone because the store has no row yet — pre-reconcile or mid-upload.
    pub skipped_absent: u64,
    /// Terminal but within the grace window (diagnosis / drain-race headroom).
    pub skipped_young: u64,
}

impl ReclaimReport {
    /// Total parts reclaimed this pass (`replicated` + `failed`).
    #[must_use]
    pub fn total_reclaimed(&self) -> u64 {
        self.reclaimed_replicated.saturating_add(self.reclaimed_failed)
    }

    /// The sum of the per-disposition categories.
    ///
    /// Every scanned part falls into exactly one, so this must equal
    /// [`scanned`](Self::scanned) — the report's invariant, which [`reclaim_ssd`]
    /// `debug_assert`s before returning.
    #[must_use]
    pub fn categorized(&self) -> u64 {
        self.reclaimed_replicated
            .saturating_add(self.reclaimed_failed)
            .saturating_add(self.skipped_live)
            .saturating_add(self.skipped_absent)
            .saturating_add(self.skipped_young)
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
    /// Unlinking a reclaimed part's directory failed.
    #[error("removing a reclaimed part from the SSD cache failed")]
    Remove(#[source] std::io::Error),
}

impl ReclaimError {
    /// Boxes a [`ReclaimLog::Error`] into [`ReclaimError::Log`].
    fn log<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Log(Box::new(err))
    }
}

/// The two terminal states a reclaim may act on. Carrying the kind from the age gate
/// to the tally keeps the per-state count exhaustive with no impossible match arm.
#[derive(Debug, Clone, Copy)]
enum Terminal {
    Replicated,
    Failed,
}

/// Reclaims terminal, aged parts from the SSD cache.
///
/// Scans every complete part on SSD, reads all their states in one batch, and for
/// each one that is terminal (`replicated`/`failed`) AND older than `grace`, unlinks
/// its directory. `pending`/`draining` parts and parts with no row are left untouched
/// — the absolute safety gate (never delete a part the drain still owns or has not
/// recorded). The caller resolves `grace` from the disk-pressure mode.
///
/// # Errors
///
/// - [`ReclaimError::Scan`] if walking the cache fails.
/// - [`ReclaimError::Log`] if the batched status read fails (nothing is removed).
/// - [`ReclaimError::Remove`] if unlinking a reclaimed part fails.
pub async fn reclaim_ssd<S, R, L>(scanner: &S, remover: &R, log: &L, grace: Duration) -> Result<ReclaimReport, ReclaimError>
where
    S: PartScan,
    R: PartRemover,
    L: ReclaimLog,
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

    for discovered in parts {
        report.scanned += 1;
        let part = &discovered.part;

        // No row yet: a part the reconciler has not recorded, or one the api is still
        // writing. Never delete it — there is no terminal signal that it is done.
        let Some(status) = states.get(part) else {
            report.skipped_absent += 1;
            continue;
        };

        // pending/draining are owned by the drain pipeline; the age gate keeps a
        // just-terminal part for the grace window. Everything else is reclaimable.
        let terminal = match status.state {
            ReplicationState::Pending | ReplicationState::Draining => {
                report.skipped_live += 1;
                continue;
            }
            ReplicationState::Replicated => Terminal::Replicated,
            ReplicationState::Failed => Terminal::Failed,
        };
        if status.age < grace {
            report.skipped_young += 1;
            continue;
        }

        remover.unlink_part(part).await.map_err(ReclaimError::Remove)?;
        match terminal {
            Terminal::Replicated => report.reclaimed_replicated += 1,
            Terminal::Failed => report.reclaimed_failed += 1,
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
    use super::{PartRemover, PartStatusAge, ReclaimError, ReclaimLog, ReclaimReport, reclaim_ssd};
    use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
    use crate::reconcile::{DiscoveredPart, PartScan};
    use crate::state::ReplicationState;
    use core::future::Future;
    use core::str::FromStr;
    use std::collections::HashMap;
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

    /// A scanner yielding a fixed part list (or a fault).
    struct FakeScan {
        parts: Vec<DiscoveredPart>,
        fail: bool,
    }

    impl FakeScan {
        fn of(parts: &[PartKey]) -> Self {
            Self {
                parts: parts.iter().map(|p| DiscoveredPart { part: p.clone() }).collect(),
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

    #[tokio::test]
    async fn an_aged_replicated_part_is_reclaimed() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Replicated, HOUR)]);

        let report = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap();
        assert_eq!(report.reclaimed_replicated, 1);
        assert_eq!(remover.removed(), vec![key(&part)], "the aged replicated part was unlinked");
    }

    #[tokio::test]
    async fn an_aged_failed_part_is_reclaimed() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[(&part, ReplicationState::Failed, HOUR)]);

        let report = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap();
        assert_eq!(report.reclaimed_failed, 1);
        assert_eq!(remover.removed(), vec![key(&part)], "the aged failed (abandoned) part was unlinked");
    }

    #[tokio::test]
    async fn pending_draining_and_no_row_parts_are_never_reclaimed() {
        // The absolute safety invariant: a part the drain still owns (pending/draining)
        // or has no row for must NEVER be unlinked, regardless of age.
        let pending = part_at(UUID_A, 1, 1);
        let draining = part_at(UUID_A, 1, 2);
        let absent = part_at(UUID_B, 7, 1);
        let scan = FakeScan::of(&[pending.clone(), draining.clone(), absent.clone()]);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[
            (&pending, ReplicationState::Pending, HOUR),
            (&draining, ReplicationState::Draining, HOUR),
            // `absent` has no row in the log at all.
        ]);

        let report = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap();
        assert_eq!(
            report.reclaimed_replicated + report.reclaimed_failed,
            0,
            "nothing live or unrecorded is reclaimed"
        );
        assert_eq!(report.skipped_live, 2);
        assert_eq!(report.skipped_absent, 1);
        assert!(remover.removed().is_empty(), "no part was unlinked");
    }

    #[tokio::test]
    async fn a_young_terminal_part_is_kept_within_the_grace_window() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakeScan::of(std::slice::from_ref(&part));
        let remover = FakeRemover::default();
        // Terminal but only just replicated — within grace (drain may be racing the
        // unlink, or a failed copy is in its diagnosis window).
        let log = FakeLog::with(&[(&part, ReplicationState::Replicated, Duration::from_mins(1))]);

        let report = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap();
        assert_eq!(report.skipped_young, 1);
        assert!(remover.removed().is_empty(), "a young terminal part is kept");
    }

    #[tokio::test]
    async fn the_status_read_is_batched_into_one_call() {
        let parts: Vec<PartKey> = (1..=5).map(|n| part_at(UUID_A, 5, n)).collect();
        let scan = FakeScan::of(&parts);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&parts.iter().map(|p| (p, ReplicationState::Replicated, HOUR)).collect::<Vec<_>>());

        let report = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap();
        assert_eq!(report.reclaimed_replicated, 5);
        assert_eq!(log.calls(), 1, "all five parts' statuses were read in one batched call");
    }

    #[tokio::test]
    async fn a_mixed_cache_tallies_each_disposition_and_sums_to_scanned() {
        let repl = part_at(UUID_A, 1, 1); // aged replicated -> reclaimed
        let fail = part_at(UUID_A, 1, 2); // aged failed -> reclaimed
        let live = part_at(UUID_A, 1, 3); // draining -> live
        let young = part_at(UUID_A, 1, 4); // young replicated -> kept
        let absent = part_at(UUID_B, 7, 1); // no row -> absent
        let scan = FakeScan::of(&[repl.clone(), fail.clone(), live.clone(), young.clone(), absent.clone()]);
        let remover = FakeRemover::default();
        let log = FakeLog::with(&[
            (&repl, ReplicationState::Replicated, HOUR),
            (&fail, ReplicationState::Failed, HOUR),
            (&live, ReplicationState::Draining, HOUR),
            (&young, ReplicationState::Replicated, Duration::from_secs(1)),
        ]);

        let report = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap();
        assert_eq!(
            report,
            ReclaimReport {
                scanned: 5,
                reclaimed_replicated: 1,
                reclaimed_failed: 1,
                skipped_live: 1,
                skipped_absent: 1,
                skipped_young: 1,
            }
        );
        assert_eq!(
            report.scanned,
            report.categorized(),
            "every scanned part lands in exactly one disposition"
        );
        assert_eq!(report.total_reclaimed(), 2);
        // removed() is sorted ascending: repl is part_1, fail is part_2.
        assert_eq!(remover.removed(), vec![key(&repl), key(&fail)], "exactly the two aged terminal parts");
    }

    #[tokio::test]
    async fn a_scan_failure_is_surfaced_and_nothing_is_removed() {
        let scan = FakeScan { parts: vec![], fail: true };
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let err = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap_err();
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
        let err = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap_err();
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
        let log = FakeLog::with(&[(&part, ReplicationState::Replicated, HOUR)]);
        let err = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap_err();
        assert!(matches!(err, ReclaimError::Remove(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn an_empty_cache_is_a_noop() {
        let scan = FakeScan::of(&[]);
        let remover = FakeRemover::default();
        let log = FakeLog::default();
        let report = reclaim_ssd(&scan, &remover, &log, GRACE).await.unwrap();
        assert_eq!(report, ReclaimReport::default());
        assert_eq!(log.calls(), 0, "an empty scan never queries the store");
    }
}
