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
use crate::clock::Clock;
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
    /// Accounted bytes freed (the sum of the evicted parts' `bytes`). Reporting only — the
    /// pass decides when to stop from re-probed free space, not from this, because a part
    /// whose size was unknown at residency time contributes zero here while still freeing
    /// real disk.
    pub freed_bytes: u64,
    /// Worklist queries issued. A pass that closes a large deficit necessarily pages, so this
    /// separates "one page was enough" from "we walked the cursor".
    pub pages: u32,
    /// Worklist entries refused because their state was not `replicated`. **Must stay zero** —
    /// see the module doc. A non-zero value means the worklist query no longer agrees with
    /// the eviction invariant, and is alert-worthy, not log-worthy.
    pub skipped_unreplicated: u64,
    /// True only when the pass ran out of EVICTABLE PARTS before reaching its deficit — the
    /// disk is filling with something eviction cannot reclaim (backlog, debris, or a foreign
    /// writer). Not self-correcting, so it is the alertable condition.
    ///
    /// Deliberately NOT set when the pass merely hit its time budget with work still queued:
    /// that is [`budget_exhausted`](Self::budget_exhausted), which the next tick resumes. The
    /// two were conflated before — one page was the whole pass, so any deficit larger than a
    /// page reported `starved` and logged at ERROR during entirely normal catch-up, which
    /// discredits the signal exactly when it starts mattering.
    pub starved: bool,
    /// True when the pass stopped on its wall-clock budget with the deficit unmet and parts
    /// still available. Expected and self-correcting: the next tick picks up where this left
    /// off. INFO, never ERROR.
    pub budget_exhausted: bool,
}

/// Bounds on ONE eviction pass.
///
/// `page` is the size of a single worklist query, **not** the pass budget. Those were the same
/// thing before, and prod's part-size distribution is why that failed: measured p50 is 686 bytes
/// against a 408 KB shard mean, so a fixed part count is almost uncorrelated with bytes freed —
/// a page walking the small-part tail frees ~350 KB against a ~175 GB deficit. Progress is
/// measured in bytes and bounded by time; the page is just how much is fetched per round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictionPass {
    /// Rows fetched per worklist query.
    pub page: u32,
    /// Wall-clock ceiling for the whole pass, so one pass cannot monopolise the disk the drain
    /// is also writing to. The remainder carries to the next tick.
    pub max_duration: core::time::Duration,
}

/// Reads free space on the ingest SSD, so a long pass converges on reality instead of on the
/// single `statvfs` it started with.
///
/// A seam rather than a direct syscall for the same reason as the other two: the orchestrator
/// stays I/O-free and the paging logic is driven by scripted values in tests.
pub trait FreeSpaceProbe: Send + Sync {
    /// Probe failure, which is non-fatal — see [`evict_to_target`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// Bytes free to an unprivileged writer right now.
    fn free_bytes(&self) -> impl Future<Output = Result<u64, Self::Error>> + Send;
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

/// Frees SSD space by evicting oldest-resident parts, **paging until the byte deficit is met**.
///
/// A no-op when free space is at or above `target.reserve` — the common case, and it costs
/// nothing: the worklist is not even queried. Once armed, it repeatedly fetches a page of the
/// store's oldest-first cursor, unlinks each `replicated` part, stamps them evicted, and
/// re-probes free space before deciding whether to continue.
///
/// # Why it pages rather than taking one batch
///
/// The armed deficit is `reserve + headroom - free`, i.e. the headroom is a fixed share of the
/// whole disk: ~175 GB on prod's 3.5 TB ingest `NVMe`. One 512-part page frees ~209 MB at the
/// measured 408 KB shard mean — and as little as ~350 KB if it walks the small-part tail, since
/// measured p50 is 686 bytes. A single-page pass therefore could not close a deficit in any
/// realistic time, and reported `starved` while doing so.
///
/// # Termination
///
/// Three exits, each reported distinctly because they need different responses:
///
/// - **Deficit met** — the pass succeeded. Neither flag set.
/// - **Short page** — the worklist is exhausted. Sets [`starved`](EvictionReport::starved) if a
///   deficit remains: nothing evictable is left and no amount of retrying helps. Alertable.
/// - **Time budget** — sets [`budget_exhausted`](EvictionReport::budget_exhausted). Work
///   remains and the next tick resumes it. Routine.
///
/// A full page that yields no eviction at all (every entry refused by the invariant) also stops
/// the pass as starved, because continuing would re-fetch the same rows forever.
///
/// # Order of operations
///
/// Per page: **unlink, then mark** — deliberately, because the two crash windows are not equally
/// bad. Unlink-then-crash leaves a row that reads resident with no SSD copy: the next pass hands
/// it back, the idempotent unlink succeeds on an already-absent dir, and it is marked then.
/// Mark-then-crash would leave a part on disk that no worklist will ever return again — a
/// permanent leak that only a full walk could find, which is exactly the scan this design avoids.
/// Marking also *must* precede the next query, or the cursor returns the same page forever.
///
/// A failing free-space probe is **not** fatal: the pass falls back to advancing its own estimate
/// by the accounted bytes it freed. Losing the probe should cost accuracy, not the eviction that
/// is keeping ingest alive.
///
/// # Errors
///
/// - [`EvictionError::Log`] if reading the worklist fails (nothing is removed).
/// - [`EvictionError::Remove`] if unlinking fails.
/// - [`EvictionError::Mark`] if recording the evictions fails after unlinking.
pub async fn evict_to_target<L, R, P>(
    log: &L,
    remover: &R,
    probe: &P,
    clock: &dyn Clock,
    target: EvictionTarget,
    pass: EvictionPass,
) -> Result<EvictionReport, EvictionError>
where
    L: ResidentLog,
    R: PartRemover,
    P: FreeSpaceProbe,
{
    let mut report = EvictionReport::default();
    let mut target = target;
    if target.deficit() == 0 {
        return Ok(report);
    }
    // Fixed ONCE, at arm time, and deliberately not re-derived from `deficit()` per page.
    // `deficit()` reports zero as soon as `free >= reserve`, so a loop that re-tested it would
    // stop at the floor and never reach `reserve + headroom` — silently discarding the headroom
    // and re-arming the evictor on nearly every poll, which is the hysteresis this whole design
    // exists to provide. Captured as an absolute free-space goal instead.
    let goal = target.reserve.saturating_add(target.headroom);
    let started = clock.now();
    // A zero page would query forever without making progress; treat it as one row.
    let page = pass.page.max(1);

    loop {
        if clock.now().duration_since(started) >= pass.max_duration {
            report.budget_exhausted = true;
            break;
        }

        let candidates = log.evictable_parts(page).await.map_err(EvictionError::log)?;
        report.pages = report.pages.saturating_add(1);
        let returned = candidates.len();

        let mut evicted = Vec::with_capacity(returned);
        let mut freed_this_page = 0u64;
        // Projected free space if every unlink so far has freed its accounted bytes. Only used
        // to stop EARLY within a page — the authoritative value is re-probed below. Without it
        // the pass evicts in whole pages and can overshoot the goal by up to `page - 1` parts,
        // which at prod's 14 GB maximum part size is a lot of cache given away for nothing.
        let mut projected = target.free;
        for candidate in candidates {
            if projected >= goal {
                break;
            }
            // Defense in depth against worklist drift — see the module doc. Skipped, never
            // deleted, however badly the pass needs the space.
            if candidate.state != ReplicationState::Replicated {
                report.skipped_unreplicated += 1;
                continue;
            }
            projected = projected.saturating_add(candidate.bytes);
            remover.unlink_part(&candidate.part).await.map_err(EvictionError::Remove)?;
            report.evicted += 1;
            freed_this_page = freed_this_page.saturating_add(candidate.bytes);
            evicted.push(candidate.part);
        }
        report.freed_bytes = report.freed_bytes.saturating_add(freed_this_page);

        if !evicted.is_empty() {
            log.mark_evicted(&evicted).await.map_err(EvictionError::mark)?;
        }

        // Re-probe rather than trusting the accounted sum: `bytes` is denormalized at residency
        // time and a part whose size was unknown records zero, so accounting alone can report
        // "freed nothing" while real space was reclaimed — and would spin the loop.
        let probed = probe.free_bytes().await.ok();
        target.free = probed.unwrap_or_else(|| target.free.saturating_add(freed_this_page));

        if target.free >= goal {
            break;
        }
        // Neither signal moved: the probe is unavailable AND every part on that page was
        // unaccounted (`bytes = 0`, which migration 0016 explicitly anticipates). Continuing
        // would unlink a full page per iteration until the wall clock with no evidence any of
        // it helps — deleting the read tier blind. Progress that cannot be measured is
        // starvation, not success.
        //
        // Only when BOTH are true. A probe that works and reports no gain is a different
        // situation — something else is filling the disk as fast as this frees it — and there
        // the right answer IS to keep evicting until the time budget.
        if probed.is_none() && freed_this_page == 0 {
            report.starved = true;
            break;
        }
        if returned < page as usize {
            // The cursor is exhausted and the floor is still not restored.
            report.starved = true;
            break;
        }
        if evicted.is_empty() {
            // A full page that yielded nothing evictable: re-querying returns the same rows.
            report.starved = true;
            break;
        }
    }

    Ok(report)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{EvictionPass, EvictionReport, EvictionTarget, FreeSpaceProbe, ResidentLog, ResidentPart, evict_to_target};
    use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
    use crate::clock::{Clock, TestClock};
    use crate::ssd_reclaim::PartRemover;
    use crate::state::ReplicationState;
    use core::future::Future;
    use core::str::FromStr;
    use core::time::Duration;
    use std::io;
    use std::sync::Mutex;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";
    const GIB: u64 = 1024 * 1024 * 1024;
    /// Generous relative to every test's simulated work, so only the test that means to hit the
    /// budget does.
    const AMPLE: Duration = Duration::from_hours(1);

    fn pass(page: u32) -> EvictionPass {
        EvictionPass { page, max_duration: AMPLE }
    }

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

    /// A worklist whose cursor actually advances: `mark_evicted` removes rows, exactly as
    /// `drop_residency`'s DELETE does. Modelling that is the point — a fake that kept handing
    /// back the same page would hide the bug where the pass forgets to mark before re-querying.
    #[derive(Default)]
    struct FakeLog {
        parts: Mutex<Vec<ResidentPart>>,
        marked: Mutex<Vec<String>>,
        served: Mutex<Vec<String>>,
        queries: Mutex<u32>,
        fail: bool,
    }

    impl FakeLog {
        fn of(parts: &[ResidentPart]) -> Self {
            Self {
                parts: Mutex::new(parts.to_vec()),
                ..Self::default()
            }
        }

        fn marked(&self) -> Vec<String> {
            self.marked.lock().unwrap().clone()
        }

        fn served(&self) -> Vec<String> {
            self.served.lock().unwrap().clone()
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
                let page: Vec<ResidentPart> = self.parts.lock().unwrap().iter().take(limit as usize).cloned().collect();
                self.served.lock().unwrap().extend(page.iter().map(|p| key(&p.part)));
                Ok(page)
            };
            async move { outcome }
        }

        fn mark_evicted(&self, parts: &[PartKey]) -> impl Future<Output = Result<(), io::Error>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("mark failed"))
            } else {
                let gone: Vec<String> = parts.iter().map(key).collect();
                self.parts.lock().unwrap().retain(|p| !gone.contains(&key(&p.part)));
                self.marked.lock().unwrap().extend(gone);
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

    /// Reports free space as "whatever it started at, plus everything unlinked so far" — the
    /// behaviour a real disk has, so the loop's stop condition is exercised for real rather than
    /// against a constant.
    struct FakeProbe {
        free: Mutex<u64>,
        per_part: u64,
        fail: bool,
    }

    impl FakeProbe {
        fn new(free: u64, per_part: u64) -> Self {
            Self {
                free: Mutex::new(free),
                per_part,
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                free: Mutex::new(0),
                per_part: 0,
                fail: true,
            }
        }
    }

    impl FreeSpaceProbe for FakeProbe {
        type Error = io::Error;

        fn free_bytes(&self) -> impl Future<Output = Result<u64, io::Error>> + Send {
            let outcome = if self.fail {
                Err(io::Error::other("statvfs failed"))
            } else {
                Ok(*self.free.lock().unwrap())
            };
            async move { outcome }
        }
    }

    /// A probe driven by the remover: every unlink credits `per_part` bytes of free space.
    struct CountingProbe<'a> {
        probe: &'a FakeProbe,
        remover: &'a FakeRemover,
    }

    impl FreeSpaceProbe for CountingProbe<'_> {
        type Error = io::Error;

        fn free_bytes(&self) -> impl Future<Output = Result<u64, io::Error>> + Send {
            let removed = self.remover.removed().len() as u64;
            let base = *self.probe.free.lock().unwrap();
            let free = base + removed * self.probe.per_part;
            async move { Ok(free) }
        }
    }

    #[tokio::test]
    async fn a_disk_above_the_reserve_evicts_nothing_and_never_queries_the_worklist() {
        // The steady state, and it must be free: with headroom to spare there is no reason to
        // give up cached parts, and no reason to pay for the worklist query either.
        let log = FakeLog::of(&[resident(1, GIB)]);
        let remover = FakeRemover::default();
        let probe = FakeProbe::new(500 * GIB, 0);
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 500 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, &probe, &clock, target, pass(128)).await.unwrap();

        assert_eq!(report, EvictionReport::default());
        assert!(remover.removed().is_empty(), "nothing is evicted above the reserve");
        assert_eq!(log.queries(), 0, "an unarmed evictor does not query the worklist");
    }

    #[tokio::test]
    async fn a_deficit_larger_than_one_page_is_still_fully_closed() {
        // THE Phase B regression test, and it fails on the pre-fix single-page implementation.
        //
        // Prod shape in miniature: the deficit needs 20 parts but a page returns 4. The old code
        // took ONE page, freed a fifth of what was needed, and reported `starved` at ERROR — for
        // the ~7 hours a real 175 GB deficit takes at 512 parts per 30 s. Paging must close it,
        // and `starved` must stay false, because nothing here is wrong.
        let parts: Vec<ResidentPart> = (1..=40).map(|v| resident(v, GIB)).collect();
        let log = FakeLog::of(&parts);
        let remover = FakeRemover::default();
        let probe = FakeProbe::new(300 * GIB, GIB);
        let counting = CountingProbe {
            probe: &probe,
            remover: &remover,
        };
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 300 * GIB,
            reserve: 310 * GIB,
            headroom: 10 * GIB,
        };
        assert_eq!(target.deficit(), 20 * GIB, "20 parts' worth of deficit");

        let report = evict_to_target(&log, &remover, &counting, &clock, target, pass(4)).await.unwrap();

        assert_eq!(report.evicted, 20, "paged until the deficit was met");
        assert!(report.pages >= 5, "more than one query: {} pages", report.pages);
        assert!(!report.starved, "a deficit larger than a page is not starvation");
        assert!(!report.budget_exhausted);
    }

    #[tokio::test]
    async fn each_page_is_disjoint_because_marking_precedes_the_next_query() {
        // If the pass re-queried before marking, the cursor would return the same rows forever:
        // it would unlink the same parts repeatedly, count them repeatedly, and never terminate.
        // Serving every part exactly once is the observable form of "mark, then re-query".
        let parts: Vec<ResidentPart> = (1..=12).map(|v| resident(v, GIB)).collect();
        let log = FakeLog::of(&parts);
        let remover = FakeRemover::default();
        let probe = FakeProbe::new(300 * GIB, GIB);
        let counting = CountingProbe {
            probe: &probe,
            remover: &remover,
        };
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 300 * GIB,
            reserve: 305 * GIB,
            headroom: 5 * GIB,
        };

        let report = evict_to_target(&log, &remover, &counting, &clock, target, pass(3)).await.unwrap();

        let served = log.served();
        let mut unique = served.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(served.len(), unique.len(), "a part was served twice: {served:?}");
        assert_eq!(remover.removed(), log.marked(), "every unlinked part is recorded evicted");
        // Exactly the 10 GiB the goal needs, NOT the 12 a whole-page pass would give away: the
        // page is the query size, never the eviction quantum.
        assert_eq!(report.evicted, 10, "no over-eviction past the goal");
    }

    #[tokio::test]
    async fn an_unreplicated_part_is_never_unlinked_on_any_page() {
        // THE durability invariant, now checked across paging rather than within one batch. A
        // pending/draining part's SSD copy is the only durable copy, and a failed/corrupt one may
        // be a live object's last good source. Refused however badly the pass needs the space,
        // and counted so the divergence is alertable.
        let mut parts = vec![resident(1, GIB), resident(2, GIB)];
        let mut pending = resident(3, 60 * GIB);
        pending.state = ReplicationState::Pending;
        let mut corrupt = resident(4, 60 * GIB);
        corrupt.state = ReplicationState::Corrupt;
        parts.push(pending);
        parts.push(corrupt);
        parts.extend((5..=8).map(|v| resident(v, GIB)));
        let log = FakeLog::of(&parts);
        let remover = FakeRemover::default();
        let probe = FakeProbe::new(300 * GIB, GIB);
        let counting = CountingProbe {
            probe: &probe,
            remover: &remover,
        };
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 300 * GIB,
            reserve: 320 * GIB,
            headroom: 5 * GIB,
        };

        let report = evict_to_target(&log, &remover, &counting, &clock, target, pass(2)).await.unwrap();

        assert_eq!(report.skipped_unreplicated, 2, "both non-replicated parts refused");
        let removed = remover.removed();
        assert!(!removed.contains(&key(&part_at(3, 1))), "the pending part was unlinked");
        assert!(!removed.contains(&key(&part_at(4, 1))), "the corrupt part was unlinked");
    }

    #[tokio::test]
    async fn exhausting_the_worklist_before_the_deficit_reports_starved() {
        // The disk is filling with something eviction cannot reclaim — undrained backlog, or
        // debris the reclaimer owns. Evicting everything available is right, but the pass must
        // report that it could not get there, because this one is NOT self-correcting and is the
        // condition that ends in 503s on PUT.
        let log = FakeLog::of(&[resident(1, 10 * GIB)]);
        let remover = FakeRemover::default();
        let probe = FakeProbe::new(10 * GIB, 10 * GIB);
        let counting = CountingProbe {
            probe: &probe,
            remover: &remover,
        };
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 10 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, &counting, &clock, target, pass(128)).await.unwrap();

        assert_eq!(report.evicted, 1);
        assert!(report.starved, "genuinely out of evictable parts");
        assert!(!report.budget_exhausted, "this is exhaustion, not a time budget");
    }

    #[tokio::test]
    async fn a_full_page_that_yields_nothing_evictable_stops_rather_than_spinning() {
        // Worklist drift in its worst form: every row is refused, so evicting makes no progress
        // and the cursor never advances. Without this guard the pass would re-fetch the same page
        // until its time budget, doing nothing but load the database.
        let parts: Vec<ResidentPart> = (1..=4)
            .map(|v| {
                let mut p = resident(v, GIB);
                p.state = ReplicationState::Pending;
                p
            })
            .collect();
        let log = FakeLog::of(&parts);
        let remover = FakeRemover::default();
        let probe = FakeProbe::new(300 * GIB, 0);
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 300 * GIB,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, &probe, &clock, target, pass(4)).await.unwrap();

        assert_eq!(report.pages, 1, "stopped after the first unproductive page");
        assert_eq!(report.skipped_unreplicated, 4);
        assert!(report.starved);
        assert!(remover.removed().is_empty());
    }

    #[tokio::test]
    async fn hitting_the_time_budget_reports_budget_exhausted_not_starved() {
        // The routine "more to do next tick" case. It must NOT set `starved`: that is the
        // alertable, non-self-correcting condition, and conflating the two is exactly what made
        // the pre-fix signal useless.
        let parts: Vec<ResidentPart> = (1..=100).map(|v| resident(v, GIB)).collect();
        let log = FakeLog::of(&parts);
        let remover = FakeRemover::default();
        let probe = FakeProbe::new(0, 0);
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 0,
            reserve: 350 * GIB,
            headroom: 50 * GIB,
        };
        // Zero budget: the deadline has passed before the first page is even fetched.
        let budgeted = EvictionPass {
            page: 4,
            max_duration: Duration::ZERO,
        };

        let report = evict_to_target(&log, &remover, &probe, &clock, target, budgeted).await.unwrap();

        assert!(report.budget_exhausted, "stopped on the wall clock");
        assert!(!report.starved, "work remained; the next tick resumes it");
        assert_eq!(report.pages, 0);
        assert_eq!(clock.now().duration_since(clock.now()), Duration::ZERO, "clock is deterministic");
    }

    #[tokio::test]
    async fn a_failing_free_space_probe_falls_back_to_accounted_bytes() {
        // Losing statvfs must cost accuracy, not the eviction that is keeping ingest alive. The
        // pass advances its own estimate by what it accounted for and still terminates.
        let parts: Vec<ResidentPart> = (1..=8).map(|v| resident(v, GIB)).collect();
        let log = FakeLog::of(&parts);
        let remover = FakeRemover::default();
        let probe = FakeProbe::failing();
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 300 * GIB,
            reserve: 303 * GIB,
            headroom: GIB,
        };

        let report = evict_to_target(&log, &remover, &probe, &clock, target, pass(2)).await.unwrap();

        assert_eq!(report.evicted, 4, "4 GiB deficit closed from accounted bytes alone");
        assert!(!report.starved);
    }

    #[tokio::test]
    async fn a_failing_probe_with_unaccounted_parts_must_not_evict_unbounded() {
        // Zero-byte residency rows are anticipated by design (migration 0016: "a part whose
        // size is unknown records 0 — it is still evictable, it just frees no accounted
        // bytes"). Combine that with a failing statvfs and NOTHING advances: free does not
        // move, the projection does not move, the page is full so exhaustion never fires, and
        // parts WERE evicted so the no-progress guard never fires either. The pass then unlinks
        // a full page per iteration until its wall clock — deleting the read tier on no
        // evidence that it is helping.
        let parts: Vec<ResidentPart> = (1..=200).map(|v| resident(v, 0)).collect();
        let log = FakeLog::of(&parts);
        let remover = FakeRemover::default();
        let probe = FakeProbe::failing();
        let clock = TestClock::new();
        let target = EvictionTarget {
            free: 100 * GIB,
            reserve: 300 * GIB,
            headroom: 50 * GIB,
        };

        let report = evict_to_target(&log, &remover, &probe, &clock, target, pass(8)).await.unwrap();

        assert!(
            report.evicted <= 8,
            "no measurable progress must stop the pass after one page, evicted {}",
            report.evicted
        );
        assert!(report.starved, "unmeasurable progress is starvation, not success");
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
