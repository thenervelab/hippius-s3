//! The per-**part** crash-safe drain state machine and its I/O contracts.
//!
//! The api's unit is a *part* (`<object_id>/v<version>/part_<n>/` holding `chunk_<i>.bin`
//! files and a `meta.json` marker) on the node-local SSD. Draining a part no longer copies
//! it anywhere: the drain verifies the SSD part is whole, records its content digest, and
//! hands it to the node-local backend uploader, which reads the same SSD and uploads to Arion.
//! This module holds only the contracts ([`PartSource`], [`PartReplicationStore`],
//! [`UploadEnqueuer`]) and the pure async orchestration ([`drain_part`]); the `tokio`/`sha2`
//! impls live in `hippius-drain-agent`, and tests drive it with in-memory fakes.
//!
//! # The ordering that must not change
//!
//! `completeness gate → hash every chunk → publish the backend upload → claim residency →
//! commit Uploading`. The publish comes BEFORE the commit, so the hand-off is at-least-once: a
//! crash between the two leaves the part `draining`, the claim lease lapses, and a re-drain
//! publishes it again (the Python uploader is idempotent — `chunk_backend ON CONFLICT`). The
//! reverse order could commit a part nobody was ever told to upload, and an `uploading` row
//! pins its SSD copy against eviction forever.
//!
//! `Uploading` is NOT terminal for this crate: the uploader flips it to `Replicated` once every
//! chunk has a live backend row, and the agent's upload sweep does the same from `chunk_backend`
//! coverage (the DB-authoritative backstop), re-publishing a row that sits `uploading` too long.
//! Only a `Replicated` part is read-tier cache the evictor may unlink — the SSD copy of an
//! `uploading` part is the only copy there is.
//!
//! A part whose `object_versions.address` is not written yet (an in-flight MPU before
//! `CompleteMultipartUpload`) cannot be published: the enqueuer reports
//! [`EnqueueOutcome::NotReady`] and the part is DEFERRED — nothing is committed, nothing is
//! hashed twice. `CompleteMultipartUpload` clears the deferral, so the part is re-claimed
//! promptly once it can be published. Deferring is cheap now that a drain copies nothing; the
//! decoupled-commit shape the pool needed (commit first, publish later from a sweep) is what
//! let never-publishable rows pile up at the head of that sweep's worklist.

use crate::apipart::{ChunkIndex, PartKey, PartMeta};
use crate::enforce::BreakerSignal;
use crate::redrive::{PartDigest, part_digest};
use crate::state::ReplicationState;
use core::future::Future;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Which step an I/O error struck, for diagnostics. Every step touches the node-local SSD;
/// nothing in the drain writes to shared storage any more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// Reading the local SSD source — listing chunks, the part meta, or opening a
    /// chunk/meta source.
    SsdRead,
    /// Hashing a chunk's bytes off the SSD for the content digest.
    Hash,
}

impl core::fmt::Display for DrainStep {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::SsdRead => "ssd_read",
            Self::Hash => "hash",
        })
    }
}

/// What a successful drain accomplished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The part was verified whole, hashed, its backend upload published, and the row
    /// committed `Uploading`. The SSD copy is retained — it is what the uploader reads.
    Enqueued,
    /// A prior run already handed this part to the uploader (`Uploading`) or the backend has
    /// it (`Replicated`); nothing to do. An idempotent no-op recovering a claim re-won after
    /// a crash that struck after the commit.
    AlreadyEnqueued,
}

/// What the enqueuer did with a part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// The backend `UploadChainRequest` was published.
    Published,
    /// The part cannot be published yet: its `object_versions.address` is still NULL (an
    /// in-flight MPU) or the version row is absent. Not a failure — the drain defers the part
    /// and `CompleteMultipartUpload` wakes it.
    NotReady,
}

/// A part claimed for draining by exactly one agent.
///
/// Not `Clone`: a claim is a capability (the SKIP-LOCKED row claim's in-process
/// echo), and cloning it would model two agents draining the same part.
#[derive(Debug)]
pub struct ClaimedPart {
    part: PartKey,
    claim_seq: i64,
}

impl ClaimedPart {
    /// Binds a claim to its part and the fencing token the store stamped on it.
    ///
    /// `claim_seq` is an opaque, per-claim monotonic token the store returns from
    /// `claim_part`; the commit (`mark_uploading`) is guarded by it so a claim
    /// re-won after lease expiry fences the stale original claimer. Off-store callers
    /// (unit tests of the drain pipeline that never touch Postgres) pass any value.
    #[must_use]
    pub fn new(part: PartKey, claim_seq: i64) -> Self {
        Self { part, claim_seq }
    }

    /// The claimed part.
    #[must_use]
    pub fn part(&self) -> &PartKey {
        &self.part
    }

    /// The store fencing token stamped when this part was claimed.
    #[must_use]
    pub fn claim_seq(&self) -> i64 {
        self.claim_seq
    }
}

/// Proof that every chunk the part's `meta.json` declares is present on SSD and was hashed
/// into the digest being committed.
///
/// The unit field is private, so — the sealed-marker idiom — no code outside this
/// module can construct a `PartVerified`. Its only constructor is the gate + hash loop in
/// [`drain_part`]. Because [`PartReplicationStore::mark_uploading`] demands
/// `&PartVerified`, committing a truncated or unhashed part does not type-check.
#[derive(Debug)]
pub struct PartVerified(());

impl PartVerified {
    /// Test-only constructor so the in-crate Postgres `PartReplicationStore` tests
    /// can supply the proof `mark_uploading` demands. Crate-private and gated to the
    /// `pg` store-test configuration (mirroring [`crate::Verified::for_test`]), so the
    /// external unforgeability seal — no `PartVerified` outside this module's loop — is
    /// untouched.
    #[cfg(all(test, feature = "pg"))]
    pub(crate) fn for_test() -> Self {
        Self(())
    }
}

/// The node-local SSD ingest cache a part is drained *from*.
pub trait PartSource: Send + Sync {
    /// The chunk indices present in the part's SSD dir (its `chunk_<i>.bin` files).
    ///
    /// # Errors
    ///
    /// An absent part dir is `Err(NotFound)`, never an empty chunk set — the reland
    /// divergence check tells "part gone" (the Vanished alarm) from "part with zero
    /// chunks" (a well-defined empty digest) on exactly this contract, and a lenient
    /// implementation silently disables that alarm.
    fn list_chunks(&self, part: &PartKey) -> impl Future<Output = std::io::Result<Vec<ChunkIndex>>> + Send;

    /// The on-disk path of one chunk's bytes on SSD.
    ///
    /// # Errors
    ///
    /// An [`io::Error`](std::io::Error) of kind `InvalidInput` if the part renders
    /// to an unsafe path (defense in depth — [`PartKey`] is already traversal-safe).
    fn chunk_source(&self, part: &PartKey, index: ChunkIndex) -> std::io::Result<PathBuf>;

    /// The on-disk path of the part's `meta.json` on SSD.
    ///
    /// # Errors
    ///
    /// As [`chunk_source`](PartSource::chunk_source).
    fn meta_source(&self, part: &PartKey) -> std::io::Result<PathBuf>;

    /// Parse the part's `meta.json` manifest from SSD, so the drain can assert the on-disk
    /// chunk set matches the declared `num_chunks` before it commits — a part whose chunks
    /// were partly removed after its meta landed must never be handed to the uploader.
    ///
    /// # Errors
    ///
    /// An [`io::Error`](std::io::Error): `NotFound` if the meta is absent (a benign
    /// not-ready deferral), or `InvalidData` if it is malformed.
    fn part_meta(&self, part: &PartKey) -> impl Future<Output = std::io::Result<PartMeta>> + Send;

    /// The lowercase-hex content hash of one source chunk.
    fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> impl Future<Output = std::io::Result<String>> + Send;
}

/// The shared `CephFS` pool the drain USED to copy parts into.
///
/// No longer consulted by [`drain_part`]: the uploader reads the node's SSD directly. Kept
/// only until the pool is unmounted (PR 2), for the agent's `LocalFs` impl and its tests.
// TODO: delete with the pool (PR 2).
pub trait PartPool: Send + Sync {
    /// Durably copy `source` into the pool at the part's `chunk_<index>.bin`,
    /// returning the lowercase-hex SHA-256 of the bytes streamed during the copy.
    fn persist_chunk(&self, source: &Path, part: &PartKey, index: ChunkIndex) -> impl Future<Output = std::io::Result<String>> + Send;

    /// Durably copy `source` into the pool at the part's `meta.json`.
    fn persist_meta(&self, source: &Path, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Fsync the part's directory once, after every chunk + meta has been renamed into place.
    fn finalize_part(&self, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;

    /// The lowercase-hex content hash of one pooled chunk.
    fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> impl Future<Output = std::io::Result<String>> + Send;

    /// Remove a part's pool dir. Idempotent.
    fn remove_part(&self, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// The central replication-status store the drain commits its result to.
pub trait PartReplicationStore: Send + Sync {
    /// Store-specific failure, boxed into [`PartDrainError::Store`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The part's current replication state, or `None` when the store has no row.
    fn status(&self, part: &PartKey) -> impl Future<Output = Result<Option<ReplicationState>, Self::Error>> + Send;

    /// Commit the part as `Uploading`: its backend upload has been published and the uploader
    /// now owns the next transition. The unforgeable `&PartVerified` proves the SSD part was
    /// whole and hashed, so this cannot be called on a truncated part.
    ///
    /// `digest` records WHAT was handed over — the fold of the per-chunk hashes. It is written
    /// by the same statement as the status, not a follow-up, so there is no window in which a
    /// part reads `uploading` with a stale or absent digest (which [`crate::verdict_for_reland`]
    /// would then have to treat as unverifiable).
    fn mark_uploading(&self, part: &ClaimedPart, proof: &PartVerified, digest: &PartDigest) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Record that this node is KEEPING the part's SSD copy, with its size for the evictor's
    /// accounting.
    ///
    /// Called just BEFORE the commit, not after, and the order matters. The part is already on
    /// the disk, so recording residency first means a crash between the two leaves a residency
    /// row for a still-`draining` part — which the eviction worklist's status guard refuses, and
    /// which the next successful commit simply overwrites. The reverse order could commit a
    /// part whose residency was never recorded: a copy on the disk that no evictor can see and
    /// no `cache_bytes` sum counts, leaking space until the node fills. (The residency row only
    /// becomes evictable once the uploader flips the part `replicated`.)
    fn mark_resident(&self, part: &PartKey, bytes: u64) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Publishes the per-part backend upload request to the node-local uploader.
///
/// Called by [`drain_part`] **before** `mark_uploading` commits, so the hand-off is
/// at-least-once (see the module docs). The impl loads the request fields from the app schema
/// and pushes to the queue; when the upload context is not ready yet (an in-progress MPU whose
/// `object_versions.address` is still NULL) it reports [`EnqueueOutcome::NotReady`] rather than
/// an error, and the drain defers the part. A genuine failure (the queue is unreachable) is an
/// `Err`, and is likewise a deferral: nothing was committed, so a later re-drain retries.
///
/// The trait is storage-generic (takes only a [`PartKey`]); the concrete impl lives in
/// the agent, which loads the request fields from the store and pushes to Redis — so
/// `hippius-drain-core` stays free of Redis and the app schema.
pub trait UploadEnqueuer: Send + Sync {
    /// Impl-specific failure — the publish itself failed (not "not ready", which is an
    /// [`EnqueueOutcome`]).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Publish the part's backend upload request. Idempotent at the consumer, so a
    /// re-publish after a crash between publish and commit is safe.
    fn enqueue(&self, part: &PartKey) -> impl Future<Output = Result<EnqueueOutcome, Self::Error>> + Send;
}

/// A part-drain failure. Every variant leaves the SSD copy intact, so a failed
/// drain is always safe to retry.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PartDrainError {
    /// An I/O step failed; the SSD copy is left intact for a later retry.
    #[error("part drain failed during {step}")]
    Io {
        /// The checkpoint that failed.
        step: DrainStep,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The replication store rejected a state transition; nothing was unlinked.
    #[error("replication store rejected the drain")]
    Store(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The SSD part's `meta.json` declares more (or different) chunks than are present on
    /// disk — the part is incomplete (its chunks were partly removed after meta landed, or
    /// an ingest crash left it torn). It is NOT committed; the SSD copy is left intact and
    /// the part is deferred for a later re-drain.
    #[error("part is incomplete: meta declares {declared} chunks but {present} are present on SSD")]
    IncompleteSource {
        /// The `num_chunks` the meta declared.
        declared: u32,
        /// The number of `chunk_<i>.bin` files actually present on SSD.
        present: u32,
    },
    /// The part cannot be published yet — its version has no `address` (an in-flight MPU).
    /// Nothing was committed; the part is deferred and `CompleteMultipartUpload` wakes it.
    #[error("upload context not ready (object_versions.address is NULL); deferred")]
    NotReady,
    /// Publishing the backend upload request failed (the queue is unreachable). Nothing was
    /// committed; the part is deferred and a later re-drain re-publishes.
    #[error("publishing the backend upload request failed")]
    Enqueue(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl PartDrainError {
    /// Box a store-specific error into [`PartDrainError::Store`].
    fn store<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Store(Box::new(err))
    }

    /// Box an enqueuer-specific error into [`PartDrainError::Enqueue`].
    fn enqueue<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Enqueue(Box::new(err))
    }

    /// Whether this failure is a benign deferral — the part could not be drained *right now*
    /// for a reason that is not evidence of node unhealth, so the caller backs it off
    /// (`defer_part`, exponential) instead of releasing it to the head of the claim ring where
    /// it would be re-claimed every poll and starve the parts behind it (the 2026-07-26
    /// head-of-line incident).
    ///
    /// - [`NotReady`](Self::NotReady): the address is not written yet; Complete wakes it.
    /// - [`Enqueue`](Self::Enqueue): the queue is unreachable; nothing would fare better now.
    /// - [`IncompleteSource`](Self::IncompleteSource): the SSD part is not whole yet.
    /// - [`Io`](Self::Io) with [`ErrorKind::NotFound`](std::io::ErrorKind::NotFound): the
    ///   SSD source/part vanished mid-drain — an overwrite, a concurrent clean, or a part
    ///   another cycle already drained. Any OTHER `Io` error (`EIO`, permission…) is a local
    ///   disk fault, and a [`Store`](Self::Store) error is a Postgres blip; both release
    ///   promptly rather than back off, since neither is a property of the part.
    #[must_use]
    pub fn is_benign_deferral(&self) -> bool {
        match self {
            Self::IncompleteSource { .. } | Self::NotReady | Self::Enqueue(_) => true,
            Self::Io { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
            Self::Store(_) => false,
        }
    }

    /// Tag an I/O error with the step at which it struck.
    fn io(step: DrainStep) -> impl FnOnce(std::io::Error) -> Self {
        move |source| Self::Io { step, source }
    }
}

/// The circuit-breaker signal for a completed drain outcome. `Ok` succeeds; every failure is a
/// deferral — the drain writes to no shared storage any more, so no drain error is evidence of
/// pool unhealth and none may open the node-global breaker. Lives with the error type (not the
/// agent) so the policy is unit-testable and the agent worker is a thin caller.
#[must_use]
pub fn breaker_signal_for(result: &Result<DrainOutcome, PartDrainError>) -> BreakerSignal {
    match result {
        Ok(_) => BreakerSignal::CephSuccess,
        Err(_) => BreakerSignal::Deferred,
    }
}

/// Drains one claimed part: verifies it is whole on SSD, records its content digest, publishes
/// its backend upload, and commits `Uploading`.
///
/// Implements the module-level ordering; each step is idempotent, so a crash at any
/// point leaves a state a later re-drain recovers from. The SSD copy is never touched.
///
/// # Errors
///
/// - [`PartDrainError::Io`] if listing, reading the meta, or hashing fails.
/// - [`PartDrainError::IncompleteSource`] if the on-disk chunk set is not the declared one.
/// - [`PartDrainError::NotReady`] / [`PartDrainError::Enqueue`] if the upload could not be
///   published (deferred).
/// - [`PartDrainError::Store`] if a store transition fails.
pub async fn drain_part<S, R, E>(ssd: &S, store: &R, enqueuer: &E, claim: &ClaimedPart) -> Result<DrainOutcome, PartDrainError>
where
    S: PartSource,
    R: PartReplicationStore,
    E: UploadEnqueuer,
{
    let part = claim.part();

    // Idempotent fast path: a prior run already handed this part over (or the backend has it),
    // so re-publishing would only duplicate work at the uploader. The SSD copy is left in
    // place — it is what the uploader reads, and afterwards this node's read tier.
    if matches!(
        store.status(part).await.map_err(PartDrainError::store)?,
        Some(ReplicationState::Uploading | ReplicationState::Replicated)
    ) {
        return Ok(DrainOutcome::AlreadyEnqueued);
    }

    let chunks = ssd.list_chunks(part).await.map_err(PartDrainError::io(DrainStep::SsdRead))?;

    // Completeness gate: meta.json is the api's part-complete marker, but a part whose
    // chunks were partly removed after meta landed still scans as "has files". Read the
    // manifest and assert the on-disk set is EXACTLY {0..num_chunks} before handing the part
    // to the uploader, so a truncated part is deferred rather than uploaded short. Since
    // list_chunks returns ascending indices, the enumerate check also rejects a hole (e.g.
    // {0,1,3} against num_chunks=3), not just a short count.
    let meta = ssd.part_meta(part).await.map_err(PartDrainError::io(DrainStep::SsdRead))?;
    let present = u32::try_from(chunks.len()).unwrap_or(u32::MAX);
    let complete = present == meta.num_chunks && chunks.iter().enumerate().all(|(i, c)| c.get() == u32::try_from(i).unwrap_or(u32::MAX));
    if !complete {
        return Err(PartDrainError::IncompleteSource {
            declared: meta.num_chunks,
            present,
        });
    }

    // The digest of what is being handed over, folded from every chunk's hash in index order.
    // It is what a later landed announcement for this same key is compared against: an
    // `UploadPart` retry that lands DIFFERENT bytes under the same (object, version, part) is
    // legal S3, and without the digest nothing could tell "announced again" from "written
    // again" (B-2). One full SSD read per part; the pool copy used to get this for free.
    let mut hashes: Vec<String> = Vec::with_capacity(chunks.len());
    for index in &chunks {
        hashes.push(ssd.chunk_hash(part, *index).await.map_err(PartDrainError::io(DrainStep::Hash))?);
    }
    let verified = PartVerified(());

    // Publish BEFORE committing (at-least-once, see the module docs). Not-ready is the
    // in-flight-MPU case and is a deferral, not a failure: nothing below runs, so the part
    // carries no residency row and no digest until it can actually be published.
    match enqueuer.enqueue(part).await.map_err(PartDrainError::enqueue)? {
        EnqueueOutcome::Published => {}
        EnqueueOutcome::NotReady => return Err(PartDrainError::NotReady),
    }

    // Claim the SSD copy as this node's cache BEFORE committing — see `mark_resident`. The size
    // comes from the manifest already read for the completeness gate, so this costs no extra
    // I/O and never has to join `parts` on the drain path.
    store.mark_resident(part, meta.size_bytes).await.map_err(PartDrainError::store)?;

    store
        .mark_uploading(claim, &verified, &part_digest(&hashes))
        .await
        .map_err(PartDrainError::store)?;

    Ok(DrainOutcome::Enqueued)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::{
        ClaimedPart, DrainOutcome, DrainStep, EnqueueOutcome, PartDrainError, PartReplicationStore, PartSource, PartVerified, UploadEnqueuer,
        breaker_signal_for, drain_part,
    };
    use crate::apipart::{ChunkIndex, ObjectId, PartKey, PartMeta, PartNumber, Version};
    use crate::enforce::BreakerSignal;
    use crate::redrive::{PartDigest, RelandVerdict, observed_part_digest, verdict_for_reland};
    use crate::state::ReplicationState;
    use core::future::Future;
    use core::str::FromStr;
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::io;
    use std::path::PathBuf;
    use std::sync::Mutex;

    const UUID: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    #[test]
    fn benign_deferrals_are_the_part_specific_waits_not_the_node_faults() {
        // Deferred (backed off): the part itself is not ready — its address is unwritten, the
        // queue is down, it is not whole yet, or it vanished. Released promptly: a local disk
        // fault or a store blip, neither of which is a property of the part.
        assert!(PartDrainError::NotReady.is_benign_deferral(), "an unwritten address is a deferral");
        assert!(
            PartDrainError::enqueue(io::Error::other("redis down")).is_benign_deferral(),
            "an unreachable queue is a deferral"
        );
        assert!(
            PartDrainError::IncompleteSource { declared: 3, present: 2 }.is_benign_deferral(),
            "an incomplete SSD part is a deferral"
        );
        assert!(
            PartDrainError::Io {
                step: DrainStep::SsdRead,
                source: io::Error::from(io::ErrorKind::NotFound),
            }
            .is_benign_deferral(),
            "a vanished SSD source (ENOENT) is a deferral"
        );
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::TimedOut,
            io::ErrorKind::Other,
        ] {
            assert!(
                !PartDrainError::Io {
                    step: DrainStep::Hash,
                    source: io::Error::from(kind),
                }
                .is_benign_deferral(),
                "a real local I/O error ({kind:?}) releases promptly"
            );
        }
        assert!(
            !PartDrainError::store(io::Error::from(io::ErrorKind::Other)).is_benign_deferral(),
            "a store rejection releases promptly"
        );
    }

    #[test]
    fn no_drain_error_opens_the_breaker() {
        // The drain writes to no shared storage, so nothing it hits is evidence of pool
        // unhealth. Ok succeeds; everything else is a deferral for the breaker.
        assert_eq!(breaker_signal_for(&Ok(DrainOutcome::Enqueued)), BreakerSignal::CephSuccess);
        for err in [
            PartDrainError::NotReady,
            PartDrainError::enqueue(io::Error::other("redis down")),
            PartDrainError::IncompleteSource { declared: 3, present: 2 },
            PartDrainError::store(io::Error::from(io::ErrorKind::Other)),
            PartDrainError::Io {
                step: DrainStep::Hash,
                source: io::Error::from(io::ErrorKind::Other),
            },
        ] {
            assert_eq!(breaker_signal_for(&Err(err)), BreakerSignal::Deferred);
        }
    }

    /// The step (if any) at which the fakes inject a failure.
    #[derive(Default, Clone, Copy, PartialEq, Eq)]
    enum Fault {
        #[default]
        None,
        ListChunks,
        SourceHash,
        Commit,
        /// The publish itself fails (the queue is unreachable).
        Enqueue,
    }

    /// One part's contents: chunk index -> content hash, and whether meta landed.
    #[derive(Default, Clone)]
    struct PartState {
        chunks: BTreeMap<u32, String>,
        has_meta: bool,
        /// The `num_chunks` the part's meta declares. `None` ⇒ it matches the chunks
        /// actually present (a complete part); `Some(n)` lets a test declare more than are
        /// on disk to exercise the completeness gate.
        declared_chunks: Option<u32>,
    }

    /// The shared in-memory world.
    #[derive(Default)]
    struct World {
        ssd: HashMap<String, PartState>,
        status: HashMap<String, ReplicationState>,
        /// Parts claimed as this node's cache (`mark_resident`), and the size recorded for each.
        resident: HashMap<String, u64>,
        fault: Fault,
        /// When set, the enqueuer reports `NotReady` (the address is not written yet).
        not_ready: bool,
        /// Parts the enqueuer published, in order.
        enqueued: Vec<String>,
        /// The content digest recorded by each commit — the store column `content_sha256`,
        /// which a later re-landing compares against to detect a rewritten part (B-2).
        committed_digest: HashMap<String, PartDigest>,
    }

    /// One struct implementing all three part contracts.
    #[derive(Default)]
    struct Fakes {
        world: Mutex<World>,
    }

    fn part() -> PartKey {
        PartKey::new(ObjectId::from_str(UUID).unwrap(), Version::new(5), PartNumber::new(1))
    }

    fn key_of(part: &PartKey) -> String {
        part.relative_dir().to_string_lossy().into_owned()
    }

    impl Fakes {
        /// A world with a single `Pending` part whose chunks carry the given hashes.
        fn seeded(part: &PartKey, chunk_hashes: &[(u32, &str)]) -> Self {
            let fakes = Fakes::default();
            let mut state = PartState::default();
            for &(index, hash) in chunk_hashes {
                state.chunks.insert(index, hash.to_owned());
            }
            state.has_meta = true;
            let mut world = fakes.world.lock().unwrap();
            world.ssd.insert(key_of(part), state);
            world.status.insert(key_of(part), ReplicationState::Pending);
            drop(world);
            fakes
        }

        fn fault(self, fault: Fault) -> Self {
            self.world.lock().unwrap().fault = fault;
            self
        }

        /// The address is not written yet: the enqueuer reports `NotReady`.
        fn not_ready(self) -> Self {
            self.world.lock().unwrap().not_ready = true;
            self
        }

        /// Declare the part's meta `num_chunks` as `n` regardless of how many chunks were
        /// seeded, to exercise the completeness gate against a truncated on-disk set.
        fn declare_chunks(self, n: u32) -> Self {
            for state in self.world.lock().unwrap().ssd.values_mut() {
                state.declared_chunks = Some(n);
            }
            self
        }

        fn enqueued(&self) -> Vec<String> {
            self.world.lock().unwrap().enqueued.clone()
        }

        fn clear_faults(&self) {
            let mut world = self.world.lock().unwrap();
            world.fault = Fault::None;
            world.not_ready = false;
        }

        fn status_of(&self, part: &PartKey) -> Option<ReplicationState> {
            self.world.lock().unwrap().status.get(&key_of(part)).copied()
        }

        fn ssd_has(&self, part: &PartKey) -> bool {
            self.world.lock().unwrap().ssd.contains_key(&key_of(part))
        }

        fn resident_bytes(&self, part: &PartKey) -> Option<u64> {
            self.world.lock().unwrap().resident.get(&key_of(part)).copied()
        }

        /// The digest recorded by the last commit (the `content_sha256` column).
        fn committed_digest(&self, part: &PartKey) -> Option<PartDigest> {
            self.world.lock().unwrap().committed_digest.get(&key_of(part)).cloned()
        }

        /// Replaces the part's SSD chunk contents — an `UploadPart` retry landing DIFFERENT
        /// bytes under the same `(object, version, part)` key, which is legal S3 before
        /// `CompleteMultipartUpload`.
        fn reupload_ssd(&self, part: &PartKey, chunk_hashes: &[(u32, &str)]) {
            let mut state = PartState::default();
            for &(index, hash) in chunk_hashes {
                state.chunks.insert(index, hash.to_owned());
            }
            state.has_meta = true;
            self.world.lock().unwrap().ssd.insert(key_of(part), state);
        }

        /// Forget the recorded digest, modelling a row committed before `content_sha256`
        /// existed (the legacy-backfill case).
        fn forget_committed_digest(&self, part: &PartKey) {
            self.world.lock().unwrap().committed_digest.remove(&key_of(part));
        }

        /// The uploader's side of the hand-off: every chunk has a live backend row now.
        fn backend_acked(&self, part: &PartKey) {
            self.world.lock().unwrap().status.insert(key_of(part), ReplicationState::Replicated);
        }

        /// The landed-announcement handler, modelling `Store::redrive_diverged_part`'s guarded
        /// UPDATE: derive the digest from disk, ask the pure policy, and on a re-drive verdict
        /// return the row to `pending`. Returns the verdict so a test can assert on it.
        async fn handle_reland(&self, part: &PartKey) -> RelandVerdict {
            let state = self.status_of(part).expect("the announcement names a known part");
            let observed = observed_part_digest(self, part).await.expect("the SSD part is readable");
            let verdict = verdict_for_reland(state, self.committed_digest(part).as_ref(), &observed);
            if verdict.redrives() {
                self.world.lock().unwrap().status.insert(key_of(part), ReplicationState::Pending);
            }
            verdict
        }
    }

    impl PartSource for Fakes {
        fn list_chunks(&self, part: &PartKey) -> impl Future<Output = io::Result<Vec<ChunkIndex>>> + Send {
            let part = part.clone();
            async move {
                let world = self.world.lock().unwrap();
                if world.fault == Fault::ListChunks {
                    return Err(io::Error::other("list failed"));
                }
                let state = world
                    .ssd
                    .get(&key_of(&part))
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no ssd part"))?;
                Ok(state.chunks.keys().map(|&i| ChunkIndex::new(i)).collect())
            }
        }

        fn chunk_source(&self, part: &PartKey, index: ChunkIndex) -> io::Result<PathBuf> {
            Ok(part.relative_dir().join(format!("chunk_{}.bin", index.get())))
        }

        fn meta_source(&self, part: &PartKey) -> io::Result<PathBuf> {
            Ok(part.relative_dir().join("meta.json"))
        }

        fn part_meta(&self, part: &PartKey) -> impl Future<Output = io::Result<PartMeta>> + Send {
            let part = part.clone();
            async move {
                let world = self.world.lock().unwrap();
                let state = world
                    .ssd
                    .get(&key_of(&part))
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no ssd part"))?;
                let present = u32::try_from(state.chunks.len()).unwrap_or(u32::MAX);
                Ok(PartMeta {
                    chunk_size: 4,
                    num_chunks: state.declared_chunks.unwrap_or(present),
                    size_bytes: 4,
                })
            }
        }

        fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> impl Future<Output = io::Result<String>> + Send {
            let part = part.clone();
            async move {
                let world = self.world.lock().unwrap();
                if world.fault == Fault::SourceHash {
                    return Err(io::Error::other("source hash failed"));
                }
                world
                    .ssd
                    .get(&key_of(&part))
                    .and_then(|s| s.chunks.get(&index.get()).cloned())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no ssd chunk"))
            }
        }
    }

    impl PartReplicationStore for Fakes {
        type Error = io::Error;

        fn status(&self, part: &PartKey) -> impl Future<Output = Result<Option<ReplicationState>, io::Error>> + Send {
            let part = part.clone();
            async move { Ok(self.world.lock().unwrap().status.get(&key_of(&part)).copied()) }
        }

        fn mark_resident(&self, part: &PartKey, bytes: u64) -> impl Future<Output = Result<(), io::Error>> + Send {
            let part = part.clone();
            async move {
                self.world.lock().unwrap().resident.insert(key_of(&part), bytes);
                Ok(())
            }
        }

        fn mark_uploading(
            &self,
            part: &ClaimedPart,
            _proof: &PartVerified,
            digest: &PartDigest,
        ) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = key_of(part.part());
            let digest = digest.clone();
            async move {
                let mut world = self.world.lock().unwrap();
                if world.fault == Fault::Commit {
                    return Err(io::Error::other("commit failed"));
                }
                world.status.insert(key.clone(), ReplicationState::Uploading);
                world.committed_digest.insert(key, digest);
                Ok(())
            }
        }
    }

    impl UploadEnqueuer for Fakes {
        type Error = io::Error;

        fn enqueue(&self, part: &PartKey) -> impl Future<Output = Result<EnqueueOutcome, io::Error>> + Send {
            let key = key_of(part);
            async move {
                let mut world = self.world.lock().unwrap();
                if world.fault == Fault::Enqueue {
                    return Err(io::Error::other("enqueue failed"));
                }
                if world.not_ready {
                    return Ok(EnqueueOutcome::NotReady);
                }
                world.enqueued.push(key);
                Ok(EnqueueOutcome::Published)
            }
        }
    }

    fn claim(part: &PartKey) -> ClaimedPart {
        // The in-memory store below ignores the fencing token, so any value works here.
        ClaimedPart::new(part.clone(), 0)
    }

    #[tokio::test]
    async fn happy_path_hashes_publishes_claims_residency_and_commits_uploading() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]);

        let outcome = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

        assert_eq!(outcome, DrainOutcome::Enqueued);
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Uploading));
        assert_eq!(fakes.enqueued(), vec![key_of(&part)], "the backend upload was published");
        assert_eq!(fakes.resident_bytes(&part), Some(4), "the SSD copy is claimed as this node's cache");
        assert!(fakes.committed_digest(&part).is_some(), "the commit records what was handed over");
        assert!(
            fakes.ssd_has(&part),
            "the SSD copy is RETAINED: it is what the uploader reads, and afterwards the read tier",
        );
    }

    #[tokio::test]
    async fn a_not_ready_publish_defers_without_committing_or_claiming_residency() {
        // An in-flight MPU: the address is not written until CompleteMultipartUpload. Nothing
        // is committed — no residency row, no digest, no status change — so the part carries
        // no state the evictor or the reland check could act on before it is publishable.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]).not_ready();

        let err = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::NotReady));
        assert!(err.is_benign_deferral(), "deferred with backoff, not released to the claim head");
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending), "nothing was committed");
        assert!(fakes.enqueued().is_empty());
        assert_eq!(fakes.resident_bytes(&part), None, "no residency claim for a part not handed over");
        assert!(fakes.committed_digest(&part).is_none());

        // Complete writes the address; the re-drain publishes and commits.
        fakes.clear_faults();
        assert_eq!(drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap(), DrainOutcome::Enqueued);
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Uploading));
    }

    #[tokio::test]
    async fn a_failed_publish_defers_without_committing() {
        // The queue is unreachable. The publish comes before the commit, so a failed publish
        // leaves nothing to unwind: the part stays pending and a later re-drain retries.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]).fault(Fault::Enqueue);

        let err = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::Enqueue(_)));
        assert!(err.is_benign_deferral());
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending));
        assert_eq!(fakes.resident_bytes(&part), None);
    }

    #[tokio::test]
    async fn an_already_handed_over_part_is_a_noop_that_does_not_republish() {
        // Both `Uploading` (the uploader owns it) and `Replicated` (the backend has it) are
        // done from the drain's point of view; a re-claim after a post-commit crash must not
        // publish a second request or touch the retained SSD copy.
        for prior in [ReplicationState::Uploading, ReplicationState::Replicated] {
            let part = part();
            let fakes = Fakes::seeded(&part, &[(0, "h0")]);
            fakes.world.lock().unwrap().status.insert(key_of(&part), prior);

            let outcome = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

            assert_eq!(outcome, DrainOutcome::AlreadyEnqueued);
            assert_eq!(fakes.status_of(&part), Some(prior), "the state is left as it was");
            assert!(fakes.enqueued().is_empty(), "no duplicate publish for a {prior:?} part");
            assert!(fakes.ssd_has(&part));
        }
    }

    #[tokio::test]
    async fn an_incomplete_part_defers_without_publishing_or_committing() {
        // meta.json declares 3 chunks but only 2 landed (an ingest crash, or chunks removed
        // after meta): the part must not be handed to the uploader short. Deferred, SSD intact.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]).declare_chunks(3);

        let err = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::IncompleteSource { declared: 3, present: 2 }));
        assert!(err.is_benign_deferral());
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending));
        assert!(fakes.enqueued().is_empty(), "an incomplete part is never published");
        assert!(fakes.ssd_has(&part));
    }

    #[tokio::test]
    async fn a_missing_interior_index_is_caught_even_when_the_count_matches() {
        // {0, 2} against num_chunks=2: the count matches but chunk 1 is missing. The gate
        // checks the exact index set, not just the count.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (2, "h2")]).declare_chunks(2);

        let err = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::IncompleteSource { declared: 2, present: 2 }));
        assert!(fakes.enqueued().is_empty());
    }

    #[tokio::test]
    async fn a_source_hash_failure_never_publishes() {
        // The digest is computed BEFORE the publish, so a part whose bytes cannot be read is
        // never handed to an uploader that would fail on the same read.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]).fault(Fault::SourceHash);

        let err = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::Io { step: DrainStep::Hash, .. }));
        assert!(fakes.enqueued().is_empty());
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending));
    }

    #[tokio::test]
    async fn a_vanished_source_is_a_benign_deferral() {
        let part = part();
        let fakes = Fakes::default(); // no SSD part at all
        fakes.world.lock().unwrap().status.insert(key_of(&part), ReplicationState::Pending);

        let err = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(
            err,
            PartDrainError::Io {
                step: DrainStep::SsdRead,
                ..
            }
        ));
        assert!(err.is_benign_deferral(), "ENOENT on the source is not node unhealth");
    }

    #[tokio::test]
    async fn crash_after_publish_before_commit_republishes_on_the_redrive() {
        // The at-least-once hand-off: the publish succeeded, the commit failed (a store blip,
        // or the agent died in between). The part stays claimable; the re-drain publishes
        // AGAIN and then commits. The duplicate is the uploader's to dedup (chunk_backend ON
        // CONFLICT) — the alternative, commit-then-publish, could commit a part nobody uploads.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]).fault(Fault::Commit);

        let err = drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();
        assert!(matches!(err, PartDrainError::Store(_)));
        assert_eq!(fakes.enqueued(), vec![key_of(&part)], "the publish happened before the failed commit");
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending));
        assert!(fakes.ssd_has(&part), "a failed commit never touches the SSD copy");

        fakes.clear_faults();
        assert_eq!(drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap(), DrainOutcome::Enqueued);
        assert_eq!(fakes.enqueued(), vec![key_of(&part), key_of(&part)], "re-published, not skipped");
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Uploading));
    }

    #[tokio::test]
    async fn draining_twice_is_idempotent() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]);

        assert_eq!(drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap(), DrainOutcome::Enqueued);
        assert_eq!(
            drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap(),
            DrainOutcome::AlreadyEnqueued
        );
        assert_eq!(fakes.enqueued().len(), 1, "one publish for one hand-off");
    }

    #[tokio::test]
    async fn a_handed_over_part_whose_ssd_content_is_replaced_is_redriven_and_republished() {
        // B-2, the regression the digest exists for. An `UploadPart` retry may land DIFFERENT
        // bytes under the same (object, version, part) key before Complete. Attempt one is
        // handed over; attempt two overwrites the SSD. Nothing used to re-drive the row, so
        // the backend kept attempt one's ciphertext — which AEAD-verifies cleanly under the
        // unchanged DEK/AAD, making it silent wrong plaintext rather than an error. Whether the
        // uploader has already acked (Replicated) or not (Uploading), a diverged digest returns
        // the part to the drainable set and the re-drain publishes the new bytes.
        for ack_first in [false, true] {
            let part = part();
            let fakes = Fakes::seeded(&part, &[(0, "attempt1-c0"), (1, "attempt1-c1")]);
            drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();
            if ack_first {
                fakes.backend_acked(&part);
            }

            fakes.reupload_ssd(&part, &[(0, "attempt2-c0"), (1, "attempt2-c1")]);
            let verdict = fakes.handle_reland(&part).await;

            assert_eq!(
                verdict,
                RelandVerdict::Diverged,
                "a rewritten part's digest no longer matches (acked={ack_first})"
            );
            assert_eq!(
                fakes.status_of(&part),
                Some(ReplicationState::Pending),
                "a diverged part returns to the drainable set"
            );

            drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();
            assert_eq!(fakes.enqueued().len(), 2, "the re-drive publishes the SECOND attempt's bytes");
            assert_ne!(
                fakes.committed_digest(&part),
                Some(crate::redrive::part_digest(&["attempt1-c0".to_owned(), "attempt1-c1".to_owned()])),
                "the recorded digest is the new content's",
            );
        }
    }

    #[tokio::test]
    async fn a_handed_over_part_whose_content_is_unchanged_is_not_redriven() {
        // The common path, and the one that would be catastrophic to get wrong: a duplicate
        // announcement, or the reconciler backstop racing the fast path, must not re-upload
        // the node's whole shard. The digest is what distinguishes "announced again" from
        // "written again"; without it the only options are re-drive everything or nothing.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]);
        drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

        assert_eq!(fakes.handle_reland(&part).await, RelandVerdict::Unchanged);
        assert_eq!(
            fakes.status_of(&part),
            Some(ReplicationState::Uploading),
            "an unchanged part stays handed over"
        );

        fakes.backend_acked(&part);
        assert_eq!(fakes.handle_reland(&part).await, RelandVerdict::Unchanged);
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Replicated));
    }

    #[tokio::test]
    async fn a_handed_over_part_with_no_recorded_digest_is_redriven_rather_than_assumed_intact() {
        // Decision on NULL: a part committed before content digests shipped cannot be compared.
        // "Unknown" must not resolve to "fine" on an integrity check, so a re-landing of such a
        // part re-drives. It costs nothing at deploy because it is only ever evaluated when an
        // announcement arrives for an already-committed part — a rewrite, by construction.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]);
        drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();
        fakes.forget_committed_digest(&part);

        let verdict = fakes.handle_reland(&part).await;

        assert_eq!(verdict, RelandVerdict::Unverifiable, "counted apart from a proven divergence");
        assert!(verdict.redrives(), "an unverifiable committed part re-drives, fail-safe");
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending));
    }

    #[tokio::test]
    async fn a_redrive_is_idempotent_and_does_not_loop_on_a_repeatedly_announced_part() {
        // A re-drive must not spin. The second announcement for the SAME rewrite finds the row
        // already `pending` (not handed over), so it is a no-op; and once the re-drive commits,
        // the freshly-recorded digest matches the disk, so further announcements read Unchanged.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "v1")]);
        drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();
        fakes.reupload_ssd(&part, &[(0, "v2")]);

        assert_eq!(fakes.handle_reland(&part).await, RelandVerdict::Diverged);
        assert_eq!(
            fakes.handle_reland(&part).await,
            RelandVerdict::NotDrained,
            "a duplicate announcement for an already re-driven part changes nothing",
        );

        drain_part(&fakes, &fakes, &fakes, &claim(&part)).await.unwrap();
        assert_eq!(
            fakes.handle_reland(&part).await,
            RelandVerdict::Unchanged,
            "once re-drained, the recorded digest matches the disk again — the loop terminates",
        );
    }

    #[tokio::test]
    async fn a_corrupt_part_is_left_to_the_bounded_redrive_worker() {
        // A `corrupt` part is owned by `redrive_corrupt_parts`, which caps attempts. Re-driving
        // it from the announcement path would bypass that cap — the same reason the reconciler
        // refuses to re-record it.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]);
        fakes.world.lock().unwrap().status.insert(key_of(&part), ReplicationState::Corrupt);

        assert_eq!(fakes.handle_reland(&part).await, RelandVerdict::NotDrained);
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Corrupt));
    }

    #[tokio::test]
    async fn the_committed_digest_binds_the_chunk_set_not_just_the_bytes() {
        // A truncated part must not share a digest with the full one. The fold hashes the chunk
        // COUNT and length-delimits each hash, so dropping a trailing chunk changes the digest
        // even though every remaining chunk hash is identical.
        let part = part();
        let full = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]);
        drain_part(&full, &full, &full, &claim(&part)).await.unwrap();

        let truncated = Fakes::seeded(&part, &[(0, "h0")]);
        drain_part(&truncated, &truncated, &truncated, &claim(&part)).await.unwrap();

        assert_ne!(
            full.committed_digest(&part),
            truncated.committed_digest(&part),
            "a truncated chunk set must not alias the full one",
        );
    }
}
