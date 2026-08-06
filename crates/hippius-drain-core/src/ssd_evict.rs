//! The SSD read-tier evictor: keeps a free-space floor on the ingest `NVMe` by reclaiming the
//! oldest RESIDENT parts — the ones the drain deliberately kept after replicating them.
//!
//! This worker exists because the drain no longer unlinks on replicate. Retaining replicated
//! parts is what makes a local GET read at ~705 MB/s off `NVMe` instead of ~94 MB/s off the
//! `CephFS` pool, but it also removes the mechanism that used to free the disk. **This evictor
//! is now the only thing standing between a retained read cache and a full ingest disk**, so
//! its failure mode is a 503 on PUT (`fs_cache_pressure`), not merely a cold cache.
//!
//! # What it may delete, and the invariant that bounds it
//!
//! Only a part that is `replicated` AND marked resident. A `replicated` part has a durable,
//! byte-verified, committed pool copy — that commit is precisely the drain's own record that
//! the `CephFS` copy exists — so unlinking the SSD copy costs latency, never data: a read falls
//! through to the pool tier (`DualFileSystemPartsStore`'s fallback), exactly as it did before
//! retention. Every other state is off limits, and for a sharper reason than tidiness:
//!
//! - `pending`/`draining` — the SSD copy is the ONLY durable copy. Deleting it is data loss.
//! - `failed`/`corrupt` — the SSD copy may be a live object's last good source (see
//!   [`crate::ssd_reclaim`]'s corrupt-live guard).
//!
//! The SQL worklist already filters to resident+`replicated`, but this module re-checks the
//! state it is handed and refuses anything else, counting it in
//! [`skipped_unreplicated`](EvictionReport::skipped_unreplicated). That counter must stay at
//! zero forever; a non-zero value means the worklist query and this invariant have diverged,
//! and it is wired to an alert rather than a log line. Defending the rule here as well as in
//! SQL makes it a unit-testable property instead of a detail of one query.
//!
//! # Policy vs. mechanism
//!
//! Eviction order is FIFO by `resident_at` — oldest retained first — supplied by the store's
//! indexed cursor. That is a deliberate v1: it is O(evicted), not O(resident), so it does not
//! repeat the janitor's walk-based eviction, which is readdir-bound at ~36 obj/s and starves
//! its own tail. True LRU needs node-local read recency, which does not exist yet (the shared
//! `fs_cache_inventory.last_access_at` is fleet-wide, so a read on one node would look hot on
//! every node). The mechanism here is policy-agnostic — ordering lives entirely in the store's
//! `ORDER BY` — so promoting FIFO to recency later changes one query, not this worker.
//!
//! # Hysteresis
//!
//! Eviction runs only once free space drops below `reserve`, and then frees down to
//! `reserve + headroom`. Without that gap the evictor would re-arm on almost every cycle,
//! trickling single parts and holding the disk pinned at exactly the threshold where
//! `fs_cache_pressure` starts issuing 503s.
//!
//! Like [`crate::reclaim_ssd`] the orchestrator is I/O-free: generic over a [`ResidentLog`]
//! worklist seam and a [`PartRemover`](crate::PartRemover) removal seam, so it is driven by
//! in-memory fakes in tests while the `tokio`/Postgres impls live at the edges.

use crate::apipart::PartKey;
use crate::ssd_reclaim::PartRemover;
use crate::state::ReplicationState;
use core::future::Future;
use thiserror::Error;

/// One resident part as the eviction worklist reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentPart {
    /// The part occupying SSD space.
    pub part: PartKey,
    /// Its size, summed from `parts.size_bytes`. Drives how far down the worklist the
    /// evictor must walk to free what it needs. A row whose `parts` entry is missing or
    /// NULL-sized contributes zero, so it is still evicted but frees no *accounted* bytes —
    /// the pass simply continues to the next part rather than stopping short.
    pub bytes: u64,
    /// The part's replication state, re-checked here against the eviction invariant.
    pub state: ReplicationState,
}

/// The free-space floor this pass must restore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictionTarget {
    /// Free bytes on the ingest SSD right now (`statvfs`).
    pub free: u64,
    /// The floor to maintain. Eviction arms only when `free < reserve`.
    pub reserve: u64,
    /// How far PAST the floor to free once armed, so the evictor does not re-arm every
    /// cycle and pin the disk at the 503 threshold.
    pub headroom: u64,
}

impl EvictionTarget {
    /// Bytes this pass must free: zero unless free space has fallen under `reserve`, in
    /// which case enough to reach `reserve + headroom`.
    ///
    /// Saturating throughout, so a `reserve + headroom` that overflows, or a `free` already
    /// above the floor, yields a well-formed zero rather than wrapping into a demand to
    /// evict the entire cache.
    #[must_use]
    pub fn deficit(self) -> u64 {
        if self.free >= self.reserve {
            return 0;
        }
        self.reserve.saturating_add(self.headroom).saturating_sub(self.free)
    }
}

/// What one eviction pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvictionReport {
    /// Parts unlinked from the SSD.
    pub evicted: u64,
    /// Accounted bytes freed (the sum of the evicted parts' `bytes`).
    pub freed_bytes: u64,
    /// Worklist entries refused because their state was not `replicated`. **Must stay zero** —
    /// see the module doc. A non-zero value means the worklist query no longer agrees with
    /// the eviction invariant, and is alert-worthy, not log-worthy.
    pub skipped_unreplicated: u64,
    /// True when the pass ran out of evictable parts before reaching its deficit — the disk
    /// is filling with something eviction cannot reclaim (backlog, debris, or another
    /// writer). Distinguishes "nothing to do" from "could not do enough".
    pub starved: bool,
}

/// An eviction failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EvictionError {
    /// Reading the eviction worklist failed. Boxed so the orchestrator stays decoupled from
    /// any one store backend.
    #[error("reading the eviction worklist failed")]
    Log(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Recording the eviction failed AFTER the part was unlinked. Surfaced rather than
    /// swallowed: the SSD copy is gone but the row still reads resident, so the next pass
    /// would hand back a part that no longer exists.
    #[error("recording an eviction failed")]
    Mark(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Unlinking an evicted part's directory failed.
    #[error("unlinking an evicted part from the SSD cache failed")]
    Remove(#[source] std::io::Error),
}

impl EvictionError {
    /// Boxes a [`ResidentLog::Error`] from the worklist read into [`EvictionError::Log`].
    fn log<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Log(Box::new(err))
    }

    /// Boxes a [`ResidentLog::Error`] from the eviction record into [`EvictionError::Mark`].
    fn mark<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Mark(Box::new(err))
    }
}

/// The eviction worklist seam: which resident parts to reclaim, and recording that they were.
///
/// Implemented by [`crate::Store`] (under `pg`) against the
/// `cephor_replication_resident_idx` partial index; faked in tests.
pub trait ResidentLog: Send + Sync {
    /// Store-specific failure, boxed into [`EvictionError`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// This node's resident parts, **oldest-resident first**, at most `limit`.
    ///
    /// The ordering IS the eviction policy (see the module doc); the orchestrator walks
    /// whatever order it is handed and never re-sorts.
    fn evictable_parts(&self, limit: u32) -> impl Future<Output = Result<Vec<ResidentPart>, Self::Error>> + Send;

    /// Stamps `evicted_at` on each part, so it leaves the resident set and the next pass's
    /// worklist. Batched: a per-part UPDATE would put a round-trip in the eviction inner
    /// loop, which runs under disk pressure.
    fn mark_evicted(&self, parts: &[PartKey]) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Frees SSD space by evicting the oldest resident parts until `target`'s deficit is met.
///
/// A no-op when free space is at or above `target.reserve` — the common case, and it costs
/// nothing: the worklist is not even queried. Once armed, walks the store's oldest-first
/// cursor, unlinking each `replicated` part and stamping `evicted_at`, and stops as soon as
/// enough accounted bytes are freed.
///
/// Order of operations per part is **unlink, then mark** — deliberately, because the two
/// crash windows are not equally bad. Unlink-then-crash leaves a row that reads resident with
/// no SSD copy: the next pass hands it back, the idempotent unlink succeeds on an already
/// absent dir, and it is marked then. Mark-then-crash would leave a part on disk that no
/// worklist will ever return again — a permanent leak that only a full walk could find, which
/// is exactly the scan this design avoids.
///
/// # Errors
///
/// - [`EvictionError::Log`] if reading the worklist fails (nothing is removed).
/// - [`EvictionError::Remove`] if unlinking fails.
/// - [`EvictionError::Mark`] if recording the evictions fails after unlinking.
pub async fn evict_to_target<L, R>(log: &L, remover: &R, target: EvictionTarget, batch: u32) -> Result<EvictionReport, EvictionError>
where
    L: ResidentLog,
    R: PartRemover,
{
    let mut report = EvictionReport::default();
    let deficit = target.deficit();
    if deficit == 0 {
        return Ok(report);
    }

    let candidates = log.evictable_parts(batch).await.map_err(EvictionError::log)?;
    let mut evicted = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if report.freed_bytes >= deficit {
            break;
        }
        // Defense in depth against worklist drift — see the module doc. Skipped, never
        // deleted, however badly the pass needs the space.
        if candidate.state != ReplicationState::Replicated {
            report.skipped_unreplicated += 1;
            continue;
        }
        remover.unlink_part(&candidate.part).await.map_err(EvictionError::Remove)?;
        report.evicted += 1;
        report.freed_bytes = report.freed_bytes.saturating_add(candidate.bytes);
        evicted.push(candidate.part);
    }

    // Marked only after the unlinks land, so a crash mid-pass leaves rows that read resident
    // with no SSD copy — replayed harmlessly by the next pass's idempotent unlink — rather
    // than parts on disk that no worklist will ever return again.
    if !evicted.is_empty() {
        log.mark_evicted(&evicted).await.map_err(EvictionError::mark)?;
    }
    report.starved = report.freed_bytes < deficit;
    Ok(report)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{EvictionReport, EvictionTarget, ResidentLog, ResidentPart, evict_to_target};
    use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
    use crate::ssd_reclaim::PartRemover;
    use crate::state::ReplicationState;
    use core::future::Future;
    use core::str::FromStr;
    use std::io;
    use std::sync::Mutex;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";
    const GIB: u64 = 1024 * 1024 * 1024;

    fn part_at(version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(UUID_A).unwrap(), Version::new(version), PartNumber::new(number))
    }

    fn key(part: &PartKey) -> String {
        part.relative_dir().to_string_lossy().into_owned()
    }

    fn resident(version: u32, bytes: u64) -> ResidentPart {
        ResidentPart {
            part: part_at(version, 1),
            bytes,
            state: ReplicationState::Replicated,
        }
    }

    /// A worklist that hands back a fixed oldest-first list, recording how it was queried.
    #[derive(Default)]
    struct FakeLog {
        parts: Vec<ResidentPart>,
        marked: Mutex<Vec<String>>,
        queries: Mutex<u32>,
        fail: bool,
    }

    impl FakeLog {
        fn of(parts: &[ResidentPart]) -> Self {
            Self {
                parts: parts.to_vec(),
                ..Self::default()
            }
        }

        fn marked(&self) -> Vec<String> {
            self.marked.lock().unwrap().clone()
        }

        fn queries(&self) -> u32 {
            *self.queries.lock().unwrap()
        }
    }

    impl ResidentLog for FakeLog {
        type Error = io::Error;

        fn evictable_parts(&self, limit: u32) -> impl Future<Output = Result<Vec<ResidentPart>, io::Error>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("worklist read failed"))
            } else {
                *self.queries.lock().unwrap() += 1;
                Ok(self.parts.iter().take(limit as usize).cloned().collect())
            };
            async move { outcome }
        }

        fn mark_evicted(&self, parts: &[PartKey]) -> impl Future<Output = Result<(), io::Error>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("mark failed"))
            } else {
                self.marked.lock().unwrap().extend(parts.iter().map(key));
                Ok(())
            };
            async move { outcome }
        }
    }

    #[derive(Default)]
    struct FakeRemover {
        removed: Mutex<Vec<String>>,
    }

    impl FakeRemover {
        fn removed(&self) -> Vec<String> {
            self.removed.lock().unwrap().clone()
        }
    }

    impl PartRemover for FakeRemover {
        fn unlink_part(&self, part: &PartKey) -> impl Future<Output = io::Result<()>> + Send {
            self.removed.lock().unwrap().push(key(part));
            async { Ok(()) }
        }
    }

    #[tokio::test]
    async fn a_disk_above_the_reserve_evicts_nothing_and_never_queries_the_worklist() {
        // The steady state, and it must be free: with headroom to spare there is no reason to
        // give up cached parts, and no reason to pay for the worklist query either.
        let log = FakeLog::of(&[resident(1, GIB)]);
        let remover = FakeRemover::default();
        let target = EvictionTarget {
            free: 500 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, target, 128).await.unwrap();

        assert_eq!(report, EvictionReport::default());
        assert!(remover.removed().is_empty(), "nothing is evicted above the reserve");
        assert_eq!(log.queries(), 0, "an unarmed evictor does not query the worklist");
    }

    #[tokio::test]
    async fn a_disk_below_the_reserve_evicts_oldest_first_up_to_the_headroom() {
        // Armed: free (300 GiB) is under the reserve (350 GiB), so the pass must free down to
        // reserve + headroom = 400 GiB, i.e. a 100 GiB deficit. The worklist is oldest-first,
        // so it takes the first two 60 GiB parts (120 GiB >= 100 GiB) and stops — it must NOT
        // drain the whole cache just because more was on offer.
        let log = FakeLog::of(&[resident(1, 60 * GIB), resident(2, 60 * GIB), resident(3, 60 * GIB)]);
        let remover = FakeRemover::default();
        let target = EvictionTarget {
            free: 300 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, target, 128).await.unwrap();

        assert_eq!(report.evicted, 2, "only as many parts as the deficit needs");
        assert_eq!(report.freed_bytes, 120 * GIB);
        assert!(!report.starved);
        assert_eq!(
            remover.removed(),
            vec![key(&part_at(1, 1)), key(&part_at(2, 1))],
            "oldest-resident first, in worklist order"
        );
        assert_eq!(remover.removed(), log.marked(), "every unlinked part is recorded evicted");
    }

    #[tokio::test]
    async fn an_unreplicated_part_on_the_worklist_is_never_unlinked() {
        // THE invariant. A pending/draining part's SSD copy is the only durable copy, and a
        // failed/corrupt one may be a live object's last good source. If the worklist query
        // ever drifts and offers one, this must refuse it and say so loudly — never delete it
        // to satisfy a deficit.
        let mut pending = resident(1, 60 * GIB);
        pending.state = ReplicationState::Pending;
        let mut corrupt = resident(2, 60 * GIB);
        corrupt.state = ReplicationState::Corrupt;
        let log = FakeLog::of(&[pending, corrupt, resident(3, 60 * GIB)]);
        let remover = FakeRemover::default();
        let target = EvictionTarget {
            free: 300 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, target, 128).await.unwrap();

        assert_eq!(report.skipped_unreplicated, 2, "both non-replicated parts refused");
        assert_eq!(remover.removed(), vec![key(&part_at(3, 1))], "only the replicated part was unlinked");
    }

    #[tokio::test]
    async fn exhausting_the_worklist_before_the_deficit_reports_starved() {
        // The disk is filling with something eviction cannot reclaim — undrained backlog, or
        // debris the reclaimer owns. Evicting everything available is right, but the pass must
        // report that it could not get there, so the condition is alertable rather than
        // looking like a quiet success.
        let log = FakeLog::of(&[resident(1, 10 * GIB)]);
        let remover = FakeRemover::default();
        let target = EvictionTarget {
            free: 10 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, target, 128).await.unwrap();

        assert_eq!(report.evicted, 1);
        assert_eq!(report.freed_bytes, 10 * GIB);
        assert!(report.starved, "could not reach the deficit");
    }

    #[test]
    fn the_deficit_is_zero_above_the_reserve_and_reaches_past_it_when_armed() {
        let above = EvictionTarget {
            free: 400 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };
        assert_eq!(above.deficit(), 0);

        // Exactly at the reserve is NOT armed: the floor is a floor, not a trigger.
        let at = EvictionTarget {
            free: 350 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };
        assert_eq!(at.deficit(), 0);

        let below = EvictionTarget {
            free: 300 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };
        assert_eq!(below.deficit(), 100 * GIB, "frees to reserve + headroom, not just to reserve");
    }
}
