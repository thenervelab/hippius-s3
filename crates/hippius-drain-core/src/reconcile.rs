//! The reconciler backstop: scan the SSD cache and re-enqueue any part the drain
//! pipeline does not yet know about.
//!
//! In the api part model there is no NOTIFY fast path: the reconciler is the sole
//! drain trigger (design §5). It walks the cache, and for every complete part (one
//! bearing a `meta.json` marker) with no replication-status row it records one as
//! `pending`, so the normal claim/drain pipeline picks it up. Recording is
//! idempotent, so reconciling repeatedly is safe.
//!
//! Like [`crate::drain_part`], the orchestrator here is I/O-free: it is generic over
//! a [`PartScan`] discovery seam and a [`PartLandingLog`] store seam, so it is tested
//! with in-memory fakes and the real `tokio`/Postgres impls live at the edges
//! (`hippius-drain-agent`'s `LocalSsd`, this crate's `Store`).

use crate::apipart::PartKey;
use crate::state::ReplicationState;
use core::future::Future;
use thiserror::Error;

/// What one reconcile pass found, tallied by the part's prior status. `scanned`
/// always equals the sum of the five category counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Parts seen on SSD.
    pub scanned: u64,
    /// Had no row and were recorded as pending (a recovered missing trigger).
    pub recovered: u64,
    /// Were a legacy NULL-node `pending` row this node adopted (stamped its `node_id`)
    /// so node-scoped `claim_part` can drain them (G2). Distinct from `recovered`
    /// (which had no row at all) and `already_pending` (already node-owned).
    pub adopted: u64,
    /// Already pending and node-owned — a worker will claim them.
    pub already_pending: u64,
    /// Already draining: a live claim in flight, or a stale orphan that
    /// `claim_part` re-claims once its claim outlives the lease (the H1 path).
    pub in_flight: u64,
    /// Already replicated, yet still on SSD — debris from a crash between commit
    /// and unlink. Counted for visibility; the actual sweep is the SSD-lifecycle
    /// work (deferred), not the reconciler's job here.
    pub replicated_orphan: u64,
    /// Marked failed (e.g. a byte mismatch); the SSD copy is kept for diagnosis, so
    /// the reconciler leaves it alone.
    pub failed: u64,
}

impl ReconcileReport {
    /// The sum of the per-status categories.
    ///
    /// Every scanned part falls into exactly one category, so this must equal
    /// [`scanned`](Self::scanned) — the report's invariant, which [`reconcile_parts`]
    /// `debug_assert`s before returning. Exposed so callers (and that assertion) can
    /// check the tally is internally consistent rather than trusting it blind.
    #[must_use]
    pub fn categorized(&self) -> u64 {
        self.recovered
            .saturating_add(self.adopted)
            .saturating_add(self.already_pending)
            .saturating_add(self.in_flight)
            .saturating_add(self.replicated_orphan)
            .saturating_add(self.failed)
    }
}

/// A reconcile failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReconcileError {
    /// Walking the SSD cache failed.
    #[error("scanning the SSD cache failed")]
    Scan(#[source] std::io::Error),
    /// A landing-log operation (status read or record) failed. Boxed so the
    /// orchestrator stays decoupled from any one store backend, and to keep the
    /// error type small on the happy path.
    #[error("the landing log failed during reconciliation")]
    Log(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl ReconcileError {
    /// Boxes a [`PartLandingLog::Error`] into [`ReconcileError::Log`].
    fn log<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Log(Box::new(err))
    }
}

/// A part found on the local SSD cache during a reconciler scan.
///
/// Carries a validated [`PartKey`], so the discovered identity is already
/// traversal-safe and names exactly `(object, version, part)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredPart {
    /// The part `(object_id, version, part_number)` whose dir was found on SSD.
    pub part: PartKey,
}

/// The SSD-cache discovery seam for the api part layout: enumerate the parts whose
/// `meta.json` marker is present on local SSD (a complete part is safe to drain).
///
/// Implemented by `hippius-drain-agent`'s `LocalSsd` (a `tokio::fs` walk of the
/// `<object>/v<version>/part_<n>/` layout); faked in tests. The future is `Send` so
/// a reconcile can be spawned on the multithreaded runtime.
pub trait PartScan: Send + Sync {
    /// Every complete part presently on the SSD cache.
    fn scan_parts(&self) -> impl Future<Output = std::io::Result<Vec<DiscoveredPart>>> + Send;
}

/// A part's stored status as the reconciler sees it: its replication state plus
/// whether the row is an adoptable legacy row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartStatus {
    /// The part's replication state.
    pub state: ReplicationState,
    /// True when the row is `pending` with no owning `node_id` — a legacy row
    /// predating node-scoping. Such a row is invisible to the node-scoped
    /// [`claim_part`](crate::Store::claim_part) and so never drains; the node that
    /// still holds the part on SSD (the one scanning it now) must adopt it (stamp its
    /// `node_id`) to make it claimable (G2). Adopting only these — never the common
    /// node-owned `pending` row — avoids a per-cycle write on the slow store.
    pub adoptable: bool,
}

/// The store seam the part reconciler needs: read a part's status, and record a
/// freshly-landed part as pending. The part analogue of [`LandingLog`], implemented
/// by [`crate::Store`] (under `pg`).
pub trait PartLandingLog: Send + Sync {
    /// Store-specific failure, boxed into [`ReconcileError::Log`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The part's status (state + adoptability), or `None` when the store has no row.
    fn status(&self, part: &PartKey) -> impl Future<Output = Result<Option<PartStatus>, Self::Error>> + Send;

    /// Record a part as landed (`pending`). Idempotent: a part that already has a row
    /// is left untouched UNLESS it is a NULL-node row, which it adopts to this node —
    /// so the reconciler can both recover a dropped trigger and adopt a legacy row.
    fn record_landed(&self, part: &PartKey) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Reconciles the SSD cache against the landing log over the api part layout.
///
/// The part analogue of [`reconcile`]: scans every complete part on SSD and, for
/// each one the store has no row for, records it as pending so the drain pipeline
/// claims it — recovering a dropped trigger. Parts already known are tallied by
/// status and otherwise left alone. Shares [`ReconcileReport`] / [`ReconcileError`]
/// with the chunk path: the per-status tally is identical regardless of the unit.
///
/// # Errors
///
/// - [`ReconcileError::Scan`] if walking the cache fails.
/// - [`ReconcileError::Log`] if a status read or record fails.
pub async fn reconcile_parts<S, L>(scanner: &S, log: &L) -> Result<ReconcileReport, ReconcileError>
where
    S: PartScan,
    L: PartLandingLog,
{
    let parts = scanner.scan_parts().await.map_err(ReconcileError::Scan)?;
    let mut report = ReconcileReport::default();
    for discovered in parts {
        report.scanned += 1;
        match log.status(&discovered.part).await.map_err(ReconcileError::log)? {
            None => {
                log.record_landed(&discovered.part).await.map_err(ReconcileError::log)?;
                report.recovered += 1;
            }
            // A legacy NULL-node `pending` row: adopt it to this node (which holds the
            // part on SSD — we just scanned it) via the idempotent record_landed UPSERT,
            // so node-scoped claim_part can finally drain it (G2). Checked before the
            // state match so an adoptable row is never miscounted as already_pending.
            Some(status) if status.adoptable => {
                log.record_landed(&discovered.part).await.map_err(ReconcileError::log)?;
                report.adopted += 1;
            }
            Some(status) => match status.state {
                ReplicationState::Pending => report.already_pending += 1,
                ReplicationState::Draining => report.in_flight += 1,
                ReplicationState::Replicated => report.replicated_orphan += 1,
                ReplicationState::Failed => report.failed += 1,
            },
        }
    }
    debug_assert_eq!(report.scanned, report.categorized(), "every scanned part lands in exactly one category");
    Ok(report)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod part_tests {
    use super::{DiscoveredPart, PartLandingLog, PartScan, PartStatus, ReconcileError, ReconcileReport, reconcile_parts};
    use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
    use crate::state::ReplicationState;
    use core::future::Future;
    use core::str::FromStr;
    use std::collections::HashMap;
    use std::io;
    use std::sync::Mutex;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";
    const UUID_B: &str = "00000000-0000-4000-8000-000000000000";

    fn part_at(uuid: &str, version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(uuid).unwrap(), Version::new(version), PartNumber::new(number))
    }

    fn discovered(uuid: &str, version: u32, number: u32) -> DiscoveredPart {
        DiscoveredPart {
            part: part_at(uuid, version, number),
        }
    }

    /// A scanner yielding a fixed part list (or a fault).
    struct FakePartScan {
        parts: Vec<DiscoveredPart>,
        fail: bool,
    }

    impl PartScan for FakePartScan {
        fn scan_parts(&self) -> impl Future<Output = io::Result<Vec<DiscoveredPart>>> + Send {
            let result = if self.fail {
                Err(io::Error::other("scan failed"))
            } else {
                Ok(self.parts.clone())
            };
            async move { result }
        }
    }

    /// An in-memory part landing log: a status map keyed by the part's relative dir,
    /// plus a record of every recorded part.
    #[derive(Default)]
    struct FakePartLog {
        statuses: Mutex<HashMap<String, PartStatus>>,
        recorded: Mutex<Vec<String>>,
        fail_status: bool,
    }

    fn part_key_string(part: &PartKey) -> String {
        part.relative_dir().to_string_lossy().into_owned()
    }

    impl FakePartLog {
        fn with(statuses: &[(&PartKey, ReplicationState)]) -> Self {
            let log = FakePartLog::default();
            for (part, state) in statuses {
                log.statuses.lock().unwrap().insert(
                    part_key_string(part),
                    PartStatus {
                        state: *state,
                        adoptable: false,
                    },
                );
            }
            log
        }

        /// A log holding one legacy NULL-node `pending` row (G2): `pending` + adoptable.
        fn with_adoptable(part: &PartKey) -> Self {
            let log = FakePartLog::default();
            log.statuses.lock().unwrap().insert(
                part_key_string(part),
                PartStatus {
                    state: ReplicationState::Pending,
                    adoptable: true,
                },
            );
            log
        }

        fn recorded_parts(&self) -> Vec<String> {
            self.recorded.lock().unwrap().clone()
        }
    }

    impl PartLandingLog for FakePartLog {
        type Error = io::Error;

        fn status(&self, part: &PartKey) -> impl Future<Output = Result<Option<PartStatus>, io::Error>> + Send {
            let outcome = if self.fail_status {
                Err(io::Error::other("status failed"))
            } else {
                Ok(self.statuses.lock().unwrap().get(&part_key_string(part)).copied())
            };
            async move { outcome }
        }

        fn record_landed(&self, part: &PartKey) -> impl Future<Output = Result<(), io::Error>> + Send {
            self.recorded.lock().unwrap().push(part_key_string(part));
            async move { Ok(()) }
        }
    }

    #[tokio::test]
    async fn a_missing_part_is_recovered_as_pending() {
        let scan = FakePartScan {
            parts: vec![discovered(UUID_A, 5, 1)],
            fail: false,
        };
        let log = FakePartLog::default(); // no row -> status None
        let report = reconcile_parts(&scan, &log).await.unwrap();
        assert_eq!(report.scanned, 1);
        assert_eq!(report.recovered, 1);
        assert_eq!(
            log.recorded_parts(),
            vec![part_key_string(&part_at(UUID_A, 5, 1))],
            "the missing part was recorded as pending",
        );
    }

    #[tokio::test]
    async fn a_known_pending_part_is_not_re_recorded() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakePartScan {
            parts: vec![discovered(UUID_A, 5, 1)],
            fail: false,
        };
        let log = FakePartLog::with(&[(&part, ReplicationState::Pending)]);
        let report = reconcile_parts(&scan, &log).await.unwrap();
        assert_eq!(report.already_pending, 1);
        assert_eq!(report.recovered, 0);
        assert!(log.recorded_parts().is_empty(), "an already-known part is left alone");
    }

    #[tokio::test]
    async fn a_replicated_ssd_part_is_counted_as_an_orphan() {
        let part = part_at(UUID_A, 5, 1);
        let scan = FakePartScan {
            parts: vec![discovered(UUID_A, 5, 1)],
            fail: false,
        };
        let log = FakePartLog::with(&[(&part, ReplicationState::Replicated)]);
        let report = reconcile_parts(&scan, &log).await.unwrap();
        assert_eq!(report.replicated_orphan, 1);
        assert!(log.recorded_parts().is_empty());
    }

    #[tokio::test]
    async fn a_mixed_cache_tallies_each_category_and_sums_to_scanned() {
        // Five distinct parts across two objects, one per status plus a fresh one.
        let pend = part_at(UUID_A, 1, 2);
        let drain = part_at(UUID_A, 1, 3);
        let done = part_at(UUID_B, 7, 1);
        let bad = part_at(UUID_B, 7, 2);
        let scan = FakePartScan {
            parts: vec![
                discovered(UUID_A, 1, 1), // new
                DiscoveredPart { part: pend.clone() },
                DiscoveredPart { part: drain.clone() },
                DiscoveredPart { part: done.clone() },
                DiscoveredPart { part: bad.clone() },
            ],
            fail: false,
        };
        let log = FakePartLog::with(&[
            (&pend, ReplicationState::Pending),
            (&drain, ReplicationState::Draining),
            (&done, ReplicationState::Replicated),
            (&bad, ReplicationState::Failed),
        ]);
        let report = reconcile_parts(&scan, &log).await.unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                scanned: 5,
                recovered: 1,
                adopted: 0,
                already_pending: 1,
                in_flight: 1,
                replicated_orphan: 1,
                failed: 1,
            }
        );
        assert_eq!(report.scanned, report.categorized(), "every scanned part lands in exactly one category");
        assert_eq!(log.recorded_parts(), vec![part_key_string(&part_at(UUID_A, 1, 1))]);
    }

    #[tokio::test]
    async fn a_legacy_nodeless_pending_part_is_adopted() {
        // G2: a `pending` row with no owning node (a row predating node-scoping) is
        // invisible to the node-scoped claim_part. The reconciler on the node that
        // holds the part re-records it (adopting it to this node) rather than leaving
        // it stuck — and does NOT count it as an ordinary already_pending row.
        let part = part_at(UUID_A, 5, 1);
        let scan = FakePartScan {
            parts: vec![discovered(UUID_A, 5, 1)],
            fail: false,
        };
        let log = FakePartLog::with_adoptable(&part);
        let report = reconcile_parts(&scan, &log).await.unwrap();
        assert_eq!(report.adopted, 1, "the legacy nodeless pending row was adopted");
        assert_eq!(report.already_pending, 0, "an adoptable row is not an ordinary already_pending");
        assert_eq!(report.scanned, report.categorized());
        assert_eq!(
            log.recorded_parts(),
            vec![part_key_string(&part)],
            "adoption re-records the part (the idempotent UPSERT stamps this node)",
        );
    }

    #[tokio::test]
    async fn a_scan_failure_is_surfaced() {
        let scan = FakePartScan { parts: vec![], fail: true };
        let log = FakePartLog::default();
        let err = reconcile_parts(&scan, &log).await.unwrap_err();
        assert!(matches!(err, ReconcileError::Scan(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn a_log_failure_is_surfaced() {
        let scan = FakePartScan {
            parts: vec![discovered(UUID_A, 5, 1)],
            fail: false,
        };
        let log = FakePartLog {
            fail_status: true,
            ..FakePartLog::default()
        };
        let err = reconcile_parts(&scan, &log).await.unwrap_err();
        assert!(matches!(err, ReconcileError::Log(_)), "got: {err:?}");
    }
}
