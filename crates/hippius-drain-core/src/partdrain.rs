//! The per-**part** crash-safe drain state machine and its I/O contracts.
//!
//! This is the hippius-s3 re-homing of [`crate::drain`]: the api's unit is a *part*
//! (`<object_id>/v<version>/part_<n>/` holding `chunk_<i>.bin` files and a
//! `meta.json` marker), not a content-addressed chunk, so the drain copies the whole
//! part tree path-preservingly from SSD to `CephFS`. Like `drain`, this module holds
//! only the contracts ([`PartSource`], [`PartPool`], [`PartReplicationStore`]) and
//! the pure async orchestration ([`drain_part`]); the `tokio`/`sha2` impls live in
//! `hippius-drain-agent`, and tests drive it with in-memory fakes.
//!
//! # The ordering that must not change
//!
//! `persist every chunk (copy+fsync+rename) → byte-verify each copy → persist
//! meta.json LAST → commit Replicated → unlink the SSD part`. `meta.json` is the
//! reader's readiness gate, so writing it last means a reader never sees a
//! half-copied part on `CephFS`; and the SSD copy — the only durable one until the
//! pool copy is complete and committed — is unlinked only on the post-commit `Ok`
//! path. [`PartVerified`] makes "commit before verify" a compile error.

use crate::apipart::{ChunkIndex, PartKey};
use crate::state::ReplicationState;
use core::future::Future;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Which durability checkpoint an I/O error struck, for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// Copying, fsync, and atomic rename onto `CephFS`.
    Persist,
    /// Re-hashing a `CephFS` copy.
    Hash,
    /// Removing a corrupt `CephFS` copy after a verify mismatch.
    Cleanup,
    /// Unlinking the SSD copy after a durable commit.
    Unlink,
}

impl core::fmt::Display for DrainStep {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Persist => "persist",
            Self::Hash => "hash",
            Self::Cleanup => "cleanup",
            Self::Unlink => "unlink",
        })
    }
}

/// What a successful drain accomplished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The part was copied, verified, committed, and the SSD copy unlinked.
    Replicated,
    /// A prior run had already committed this part; this call only ensured the SSD
    /// copy was unlinked. An idempotent no-op recovering a crash that struck between
    /// commit and unlink.
    AlreadyReplicated,
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
    /// `claim_part`; the commit (`mark_replicated`) is guarded by it so a claim
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

/// Proof that every chunk of a part was copied to `CephFS` and verified byte-equal
/// to its SSD source.
///
/// The unit field is private, so — the sealed-marker idiom — no code outside this
/// module can construct a `PartVerified`. Its only constructor is the verify loop in
/// [`drain_part`]. Because [`PartReplicationStore::mark_replicated`] demands
/// `&PartVerified`, committing `Replicated` without a passing verification does not
/// type-check.
#[derive(Debug)]
pub struct PartVerified(());

impl PartVerified {
    /// Test-only constructor so the in-crate Postgres `PartReplicationStore` tests
    /// can supply the proof `mark_replicated` demands. Crate-private and gated to the
    /// `pg` store-test configuration (mirroring [`crate::Verified::for_test`]), so the
    /// external unforgeability seal — no `PartVerified` outside this module's verify
    /// loop — is untouched.
    #[cfg(all(test, feature = "pg"))]
    pub(crate) fn for_test() -> Self {
        Self(())
    }
}

/// The node-local SSD ingest cache a part is drained *from*.
pub trait PartSource: Send + Sync {
    /// The chunk indices present in the part's SSD dir (its `chunk_<i>.bin` files).
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

    /// The lowercase-hex content hash of one source chunk (to verify the copy).
    fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> impl Future<Output = std::io::Result<String>> + Send;

    /// Unlink the part's whole SSD dir after a committed drain; an already-absent
    /// dir is `Ok` (idempotent, so a re-drive after a crash still converges).
    fn remove_part(&self, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// The durable shared `CephFS` pool a part is drained *to*.
///
/// [`persist_chunk`](PartPool::persist_chunk) and [`persist_meta`](PartPool::persist_meta)
/// must be crash-atomic: once either returns `Ok`, a power loss leaves the complete
/// file, never a torn one.
pub trait PartPool: Send + Sync {
    /// Durably copy `source` into the pool at the part's `chunk_<index>.bin`,
    /// returning the lowercase-hex SHA-256 of the bytes streamed during the copy.
    /// The copy fsyncs the chunk file (`fdatasync`) but NOT the parent dir — the
    /// single per-part dir-fsync is deferred to [`finalize_part`](PartPool::finalize_part),
    /// so a 64-chunk part costs one dir-fsync, not 64.
    fn persist_chunk(&self, source: &Path, part: &PartKey, index: ChunkIndex) -> impl Future<Output = std::io::Result<String>> + Send;

    /// Durably copy `source` into the pool at the part's `meta.json`. Called LAST,
    /// after every chunk is verified, so a reader's meta gate flips only when the
    /// whole part is durably present. Like [`persist_chunk`](PartPool::persist_chunk)
    /// it does not fsync the dir — [`finalize_part`](PartPool::finalize_part) does.
    fn persist_meta(&self, source: &Path, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;

    /// Fsync the part's directory once, after every chunk + meta has been renamed
    /// into place, so all those directory entries become durable together. Meta is
    /// renamed before this call, so chunks+meta flush atomically when the dir entry
    /// flushes — a crash before this leaves the part `draining` (re-drained), losing
    /// no durability since `mark_replicated` only commits after it succeeds.
    fn finalize_part(&self, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;

    /// The lowercase-hex content hash of one pooled chunk (to verify the copy).
    fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> impl Future<Output = std::io::Result<String>> + Send;

    /// Remove a corrupt part's pool dir after a verify mismatch — the copy was
    /// persisted but never committed, so deleting it is safe and leaves the SSD
    /// source intact. Idempotent.
    fn remove_part(&self, part: &PartKey) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// The central replication-status store the drain commits its result to.
pub trait PartReplicationStore: Send + Sync {
    /// Store-specific failure, boxed into [`PartDrainError::Store`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The part's current replication state, or `None` when the store has no row.
    fn status(&self, part: &PartKey) -> impl Future<Output = Result<Option<ReplicationState>, Self::Error>> + Send;

    /// Commit the part as `Replicated`. The unforgeable `&PartVerified` proves the
    /// copy was verified, so this cannot be called before verification.
    fn mark_replicated(&self, part: &ClaimedPart, proof: &PartVerified) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Record that the part's drain failed (e.g. a byte-mismatch on copy).
    fn mark_failed(&self, part: &ClaimedPart, reason: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Publishes the per-part backend upload request once the part is durably on the pool.
///
/// Called by [`drain_part`] **after** the verified copy + meta are persisted but
/// **before** `mark_replicated` commits — so a crash between the enqueue and the commit
/// leaves the part `draining`, which a later re-drain re-enqueues (a harmless duplicate;
/// the backend uploader is idempotent). This is the at-least-once seam that lets the
/// drain be the sole upload producer without a separate notify/sweep.
///
/// The trait is storage-generic (takes only a [`PartKey`]); the concrete impl lives in
/// the agent, which loads the request fields from the store and pushes to Redis — so
/// `hippius-drain-core` stays free of Redis and the app schema.
pub trait UploadEnqueuer: Send + Sync {
    /// Impl-specific failure, boxed into [`PartDrainError::Enqueue`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// Enqueue the part's backend upload request(s). Idempotent at the consumer, so a
    /// retry after a transient failure (or a re-drain) is safe.
    fn enqueue(&self, part: &PartKey) -> impl Future<Output = Result<(), Self::Error>> + Send;
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
    /// A pooled chunk's bytes did not match its SSD source — the copy is corrupt.
    /// The part is marked `Failed`, the partial pool copy is removed, and the SSD
    /// copy is left intact.
    #[error("chunk {index} copy mismatch: source {source_hash}, pool {pool_hash}")]
    ChunkMismatch {
        /// The chunk whose copy did not match.
        index: ChunkIndex,
        /// The hash of the SSD source bytes.
        source_hash: Box<str>,
        /// The hash of the pooled copy.
        pool_hash: Box<str>,
    },
    /// The replication store rejected a state transition; nothing was unlinked.
    #[error("replication store rejected the drain")]
    Store(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Enqueuing the backend upload request failed (e.g. Redis unavailable, or the
    /// upload context isn't ready yet). The part is NOT committed, so it stays
    /// `draining` and a later re-drain re-enqueues it — no upload is lost.
    #[error("enqueuing the backend upload failed")]
    Enqueue(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl PartDrainError {
    /// Box a store-specific error into [`PartDrainError::Store`].
    fn store<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Store(Box::new(err))
    }

    /// Box an enqueuer error into [`PartDrainError::Enqueue`].
    fn enqueue<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Enqueue(Box::new(err))
    }

    /// Whether this failure is a benign deferral rather than a Ceph-write failure — the
    /// part could not be drained *right now* for a reason that is NOT evidence of `CephFS`
    /// unhealth, so the caller must neither trip the node-global Ceph breaker nor count
    /// it as a failure; the part is backed off and a later re-drain retries it.
    ///
    /// Two cases:
    /// - [`Enqueue`](Self::Enqueue): the upload context isn't ready (an in-progress MPU
    ///   whose `object_versions.address` is still NULL, or Redis briefly unreachable).
    /// - [`Io`](Self::Io) with [`ErrorKind::NotFound`](std::io::ErrorKind::NotFound): the
    ///   SSD source/part vanished mid-drain — an overwrite, a concurrent clean, or a part
    ///   another cycle already drained and unlinked. The pool is healthy; there is simply
    ///   nothing to copy. Any OTHER `Io` error (`EIO`, `ENOTCONN`, permission, no-space…)
    ///   is a genuine write failure and still trips the breaker.
    ///
    /// Scoped by `ErrorKind`, not by which side raised it: in principle a pool-side
    /// `ENOENT` (the pool dir removed between `create_dir_all` and the file write) would
    /// also read as benign, but nothing removes an actively-draining part's pool dir, and
    /// a real degrading `CephFS` mount surfaces as `ENOTCONN`/`EIO` (kind `Other`), not
    /// `NotFound` — so a genuine pool failure still trips the breaker. Distinguishing the
    /// source-open `ENOENT` from a pool-write `ENOENT` at the type level is a tracked
    /// follow-up (would need a dedicated `DrainStep`/source-open error tag).
    #[must_use]
    pub fn is_benign_deferral(&self) -> bool {
        match self {
            Self::Enqueue(_) => true,
            Self::Io { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
            _ => false,
        }
    }

    /// Tag an I/O error with the step at which it struck.
    fn io(step: DrainStep) -> impl FnOnce(std::io::Error) -> Self {
        move |source| Self::Io { step, source }
    }
}

/// Drains one claimed part from local SSD to the `CephFS` pool, crash-safely.
///
/// Implements the module-level ordering; each step is idempotent, so a crash at any
/// point leaves a state a later re-drain recovers from, and the SSD part is removed
/// only after a durable, verified, committed pool copy exists.
///
/// # Errors
///
/// - [`PartDrainError::Io`] if listing, persisting, hashing, or unlinking fails.
/// - [`PartDrainError::ChunkMismatch`] if a pooled chunk does not match its source
///   (the part is marked `Failed` first).
/// - [`PartDrainError::Store`] if a store transition fails.
pub async fn drain_part<F, S, R, E>(ceph: &F, ssd: &S, store: &R, enqueuer: &E, claim: &ClaimedPart) -> Result<DrainOutcome, PartDrainError>
where
    F: PartPool,
    S: PartSource,
    R: PartReplicationStore,
    E: UploadEnqueuer,
{
    let part = claim.part();

    // Idempotent fast path: a prior run already committed this part, so the pool
    // copy is durable. The only remaining obligation is to free the SSD copy — the
    // previous run may have crashed between commit and unlink.
    if store.status(part).await.map_err(PartDrainError::store)? == Some(ReplicationState::Replicated) {
        ssd.remove_part(part).await.map_err(PartDrainError::io(DrainStep::Unlink))?;
        return Ok(DrainOutcome::AlreadyReplicated);
    }

    // Copy every chunk, hashing it ONCE during the copy stream (no SSD re-read). The
    // copy-time hash is the source bytes as written, so a torn pool write is caught by
    // re-reading the pool copy and comparing — but only for a SAMPLE (the first and last
    // chunk), not every chunk: the per-chunk pool readback was a full second read of the
    // slowest tier. Content-addressed naming makes writes idempotent + self-naming and the
    // drain re-drives on any failure, so the interior chunks trust the copy; the sampled
    // endpoints still catch a systematic copy/IO fault. A mismatch marks the part Failed,
    // drops the partial pool copy, and leaves the SSD source intact — never commit it.
    let chunks = ssd.list_chunks(part).await.map_err(PartDrainError::io(DrainStep::Persist))?;
    let (first, last) = (chunks.first().copied(), chunks.last().copied());
    for index in &chunks {
        let index = *index;
        let source = ssd.chunk_source(part, index).map_err(PartDrainError::io(DrainStep::Persist))?;
        let copy_hash = ceph
            .persist_chunk(&source, part, index)
            .await
            .map_err(PartDrainError::io(DrainStep::Persist))?;
        if Some(index) == first || Some(index) == last {
            let pool_hash = ceph.chunk_hash(part, index).await.map_err(PartDrainError::io(DrainStep::Hash))?;
            if pool_hash != copy_hash {
                store
                    .mark_failed(claim, "chunk copy byte mismatch")
                    .await
                    .map_err(PartDrainError::store)?;
                ceph.remove_part(part).await.map_err(PartDrainError::io(DrainStep::Cleanup))?;
                return Err(PartDrainError::ChunkMismatch {
                    index,
                    source_hash: copy_hash.into_boxed_str(),
                    pool_hash: pool_hash.into_boxed_str(),
                });
            }
        }
    }

    // Persist meta LAST — only now, with every chunk durably copied (and the sampled
    // endpoints byte-verified), may the reader's `meta.json` gate flip on the pool copy.
    let meta_source = ssd.meta_source(part).map_err(PartDrainError::io(DrainStep::Persist))?;
    ceph.persist_meta(&meta_source, part)
        .await
        .map_err(PartDrainError::io(DrainStep::Persist))?;
    // ONE dir-fsync for the whole part, now that every chunk + meta is renamed into place
    // (Task 1: batch the fsync). Precedes commit/enqueue so the pool copy is durable before
    // the SSD source can be removed.
    ceph.finalize_part(part).await.map_err(PartDrainError::io(DrainStep::Persist))?;
    let verified = PartVerified(());

    // Enqueue the backend upload BEFORE committing (at-least-once): the part is now
    // durably on the pool and readable, so the uploader can serve it. If this fails the
    // part stays `draining` and a re-drain re-enqueues — no upload is lost, and the dup
    // is harmless (idempotent uploader). The drain is thus the sole upload producer with
    // no separate notify/sweep.
    enqueuer.enqueue(part).await.map_err(PartDrainError::enqueue)?;

    // Commit Replicated. Only past this point — a durable, verified, committed copy
    // exists AND its upload is enqueued — is removing the SSD part safe.
    store.mark_replicated(claim, &verified).await.map_err(PartDrainError::store)?;

    ssd.remove_part(part).await.map_err(PartDrainError::io(DrainStep::Unlink))?;
    Ok(DrainOutcome::Replicated)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::{
        ClaimedPart, DrainOutcome, DrainStep, PartDrainError, PartPool, PartReplicationStore, PartSource, PartVerified, UploadEnqueuer, drain_part,
    };
    use crate::apipart::{ChunkIndex, ObjectId, PartKey, PartNumber, Version};
    use crate::state::ReplicationState;
    use core::future::Future;
    use core::str::FromStr;
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    const UUID: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    #[test]
    fn is_benign_deferral_spares_the_breaker_only_for_enqueue_and_vanished_source() {
        // ENOENT reading the SSD source (overwrite / concurrent clean / already drained)
        // is benign — the pool is healthy, there is just nothing to copy. It must NOT trip
        // the node-global Ceph breaker.
        let vanished = PartDrainError::Io {
            step: DrainStep::Persist,
            source: io::Error::from(io::ErrorKind::NotFound),
        };
        assert!(vanished.is_benign_deferral(), "a vanished SSD source (ENOENT) is a benign deferral");

        // Not-ready upload context is likewise benign (in-progress MPU / Redis blip).
        let not_ready = PartDrainError::enqueue(io::Error::from(io::ErrorKind::ConnectionRefused));
        assert!(not_ready.is_benign_deferral(), "an enqueue failure is a benign deferral");

        // Every OTHER I/O error is a genuine Ceph-write failure and MUST still trip the
        // breaker — the whole point of the breaker is to react to a degrading pool.
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::TimedOut,
            io::ErrorKind::Other, // e.g. a raw EIO / ENOTCONN from a sick CephFS mount
        ] {
            let real = PartDrainError::Io {
                step: DrainStep::Persist,
                source: io::Error::from(kind),
            };
            assert!(!real.is_benign_deferral(), "a real I/O error ({kind:?}) must still trip the breaker");
        }

        // A byte-mismatch corruption and a store rejection are not deferrals either.
        let mismatch = PartDrainError::ChunkMismatch {
            index: ChunkIndex::new(0),
            source_hash: "aaa".into(),
            pool_hash: "bbb".into(),
        };
        assert!(!mismatch.is_benign_deferral(), "a chunk byte-mismatch is not a benign deferral");
        let store_err = PartDrainError::store(io::Error::from(io::ErrorKind::Other));
        assert!(!store_err.is_benign_deferral(), "a store rejection is not a benign deferral");
    }

    /// The step (if any) at which the fakes inject an I/O failure.
    #[derive(Default, Clone, Copy, PartialEq, Eq)]
    enum Fault {
        #[default]
        None,
        ListChunks,
        PersistChunk,
        PersistMeta,
        SourceHash,
        PoolHash,
        Cleanup,
        Commit,
        Unlink,
    }

    /// One part's contents: chunk index -> content hash, and whether meta landed.
    #[derive(Default, Clone)]
    struct PartState {
        chunks: BTreeMap<u32, String>,
        has_meta: bool,
    }

    /// The shared in-memory world. A chunk maps to the content hash its bytes would
    /// produce — an honest copy keeps the same hash; a corrupt persist rewrites it.
    #[derive(Default)]
    struct World {
        ssd: HashMap<String, PartState>,
        pool: HashMap<String, PartState>,
        status: HashMap<String, ReplicationState>,
        fault: Fault,
        corrupt_persist: bool,
        /// Specific chunk indices a persist corrupts (in addition to `corrupt_persist`),
        /// so a test can corrupt only an interior chunk vs. a sampled endpoint.
        corrupt_chunks: std::collections::HashSet<u32>,
        /// When set, the upload enqueue fails (to test the at-least-once seam).
        enqueue_fault: bool,
        /// Parts the enqueuer was asked to enqueue, in order.
        enqueued: Vec<String>,
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

        fn corrupt_persist(self) -> Self {
            self.world.lock().unwrap().corrupt_persist = true;
            self
        }

        /// Corrupt the persisted copy of one specific chunk index only.
        fn corrupt_chunk(self, index: u32) -> Self {
            self.world.lock().unwrap().corrupt_chunks.insert(index);
            self
        }

        fn enqueue_fault(self) -> Self {
            self.world.lock().unwrap().enqueue_fault = true;
            self
        }

        fn enqueued(&self) -> Vec<String> {
            self.world.lock().unwrap().enqueued.clone()
        }

        fn clear_faults(&self) {
            let mut world = self.world.lock().unwrap();
            world.fault = Fault::None;
            world.corrupt_persist = false;
            world.corrupt_chunks.clear();
        }

        fn status_of(&self, part: &PartKey) -> Option<ReplicationState> {
            self.world.lock().unwrap().status.get(&key_of(part)).copied()
        }

        fn ssd_has(&self, part: &PartKey) -> bool {
            self.world.lock().unwrap().ssd.contains_key(&key_of(part))
        }

        fn pool_part(&self, part: &PartKey) -> Option<PartState> {
            self.world.lock().unwrap().pool.get(&key_of(part)).cloned()
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

        fn remove_part(&self, part: &PartKey) -> impl Future<Output = io::Result<()>> + Send {
            let part = part.clone();
            async move {
                let mut world = self.world.lock().unwrap();
                if world.fault == Fault::Unlink {
                    return Err(io::Error::other("unlink failed"));
                }
                world.ssd.remove(&key_of(&part));
                Ok(())
            }
        }
    }

    impl PartPool for Fakes {
        fn persist_chunk(&self, _source: &Path, part: &PartKey, index: ChunkIndex) -> impl Future<Output = io::Result<String>> + Send {
            let part = part.clone();
            async move {
                let mut world = self.world.lock().unwrap();
                if world.fault == Fault::PersistChunk {
                    return Err(io::Error::other("persist chunk failed"));
                }
                let corrupt = world.corrupt_persist || world.corrupt_chunks.contains(&index.get());
                let source_hash = world
                    .ssd
                    .get(&key_of(&part))
                    .and_then(|s| s.chunks.get(&index.get()).cloned())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no ssd source"))?;
                // A corrupt persist writes different bytes (hash) than the source. The
                // returned hash is the copy-time (source) hash either way — mirrors the
                // real localfs, where the copy streams + hashes the source while a torn
                // write only diverges on the pool re-read.
                let written = if corrupt {
                    format!("corrupt-{}", index.get())
                } else {
                    source_hash.clone()
                };
                world.pool.entry(key_of(&part)).or_default().chunks.insert(index.get(), written);
                Ok(source_hash)
            }
        }

        fn persist_meta(&self, _source: &Path, part: &PartKey) -> impl Future<Output = io::Result<()>> + Send {
            let part = part.clone();
            async move {
                let mut world = self.world.lock().unwrap();
                if world.fault == Fault::PersistMeta {
                    return Err(io::Error::other("persist meta failed"));
                }
                world.pool.entry(key_of(&part)).or_default().has_meta = true;
                Ok(())
            }
        }

        async fn finalize_part(&self, _part: &PartKey) -> io::Result<()> {
            // The in-memory pool needs no fsync; the real localfs dir-fsync is exercised
            // by the e2e + localfs tests.
            Ok(())
        }

        fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> impl Future<Output = io::Result<String>> + Send {
            let part = part.clone();
            async move {
                let world = self.world.lock().unwrap();
                if world.fault == Fault::PoolHash {
                    return Err(io::Error::other("pool hash failed"));
                }
                world
                    .pool
                    .get(&key_of(&part))
                    .and_then(|s| s.chunks.get(&index.get()).cloned())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no pool chunk"))
            }
        }

        fn remove_part(&self, part: &PartKey) -> impl Future<Output = io::Result<()>> + Send {
            let part = part.clone();
            async move {
                let mut world = self.world.lock().unwrap();
                if world.fault == Fault::Cleanup {
                    return Err(io::Error::other("pool cleanup failed"));
                }
                world.pool.remove(&key_of(&part));
                Ok(())
            }
        }
    }

    impl PartReplicationStore for Fakes {
        type Error = io::Error;

        fn status(&self, part: &PartKey) -> impl Future<Output = Result<Option<ReplicationState>, io::Error>> + Send {
            let part = part.clone();
            async move { Ok(self.world.lock().unwrap().status.get(&key_of(&part)).copied()) }
        }

        fn mark_replicated(&self, part: &ClaimedPart, _proof: &PartVerified) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = key_of(part.part());
            async move {
                let mut world = self.world.lock().unwrap();
                if world.fault == Fault::Commit {
                    return Err(io::Error::other("commit failed"));
                }
                world.status.insert(key, ReplicationState::Replicated);
                Ok(())
            }
        }

        fn mark_failed(&self, part: &ClaimedPart, _reason: &str) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = key_of(part.part());
            async move {
                self.world.lock().unwrap().status.insert(key, ReplicationState::Failed);
                Ok(())
            }
        }
    }

    impl UploadEnqueuer for Fakes {
        type Error = io::Error;

        fn enqueue(&self, part: &PartKey) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = key_of(part);
            async move {
                let mut world = self.world.lock().unwrap();
                if world.enqueue_fault {
                    return Err(io::Error::other("enqueue failed"));
                }
                world.enqueued.push(key);
                Ok(())
            }
        }
    }

    fn claim(part: &PartKey) -> ClaimedPart {
        // The in-memory store below ignores the fencing token, so any value works here.
        ClaimedPart::new(part.clone(), 0)
    }

    #[tokio::test]
    async fn happy_path_copies_every_chunk_then_meta_then_commits_then_unlinks() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]);

        let outcome = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

        assert_eq!(outcome, DrainOutcome::Replicated);
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Replicated));
        let pooled = fakes.pool_part(&part).expect("part landed on CephFS");
        assert_eq!(pooled.chunks.get(&0).map(String::as_str), Some("h0"));
        assert_eq!(pooled.chunks.get(&1).map(String::as_str), Some("h1"));
        assert!(pooled.has_meta, "meta.json written");
        assert!(!fakes.ssd_has(&part), "SSD part unlinked only after commit");
        assert_eq!(fakes.enqueued(), vec![key_of(&part)], "the backend upload was enqueued");
    }

    #[tokio::test]
    async fn an_enqueue_failure_never_commits_and_preserves_the_ssd_copy() {
        // At-least-once: if enqueuing the upload fails, the part is NOT committed and the
        // SSD copy is kept, so a re-drain re-enqueues — no upload is lost.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]).enqueue_fault();

        let err = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::Enqueue(_)), "got: {err:?}");
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending), "must NOT commit Replicated");
        assert!(fakes.ssd_has(&part), "SSD copy kept for the re-drain");
    }

    #[tokio::test]
    async fn already_replicated_part_only_unlinks_the_ssd_copy() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]);
        fakes.world.lock().unwrap().status.insert(key_of(&part), ReplicationState::Replicated);

        let outcome = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

        assert_eq!(outcome, DrainOutcome::AlreadyReplicated);
        assert!(!fakes.ssd_has(&part), "lingering SSD copy reclaimed");
        assert!(fakes.pool_part(&part).is_none(), "no redundant re-copy to the pool");
    }

    #[tokio::test]
    async fn a_corrupt_chunk_copy_marks_failed_and_preserves_the_ssd_copy() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]).corrupt_persist();

        let err = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::ChunkMismatch { index, .. } if index == ChunkIndex::new(0)));
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Failed));
        assert!(fakes.ssd_has(&part), "corrupt drain must NOT delete the SSD copy");
        assert!(fakes.pool_part(&part).is_none(), "the corrupt, never-committed pool copy is removed");
    }

    #[tokio::test]
    async fn a_corrupt_last_chunk_is_caught_by_the_sampled_readback() {
        // The sampled verify re-reads the first and last chunk. A torn LAST chunk must
        // still trip ChunkMismatch even though the interior chunks are now trusted.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1"), (2, "h2")]).corrupt_chunk(2);

        let err = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::ChunkMismatch { index, .. } if index == ChunkIndex::new(2)));
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Failed));
        assert!(fakes.ssd_has(&part), "corrupt drain must NOT delete the SSD copy");
    }

    #[tokio::test]
    async fn a_corrupt_interior_chunk_is_not_caught_by_the_sampled_readback() {
        // Pins the sampling trade-off: only the first + last chunk are re-read, so a torn
        // INTERIOR chunk is trusted (content-addressed naming + re-drive cover it elsewhere).
        // The drain therefore commits — the test documents what sampling deliberately skips.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1"), (2, "h2")]).corrupt_chunk(1);

        let outcome = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

        assert_eq!(
            outcome,
            DrainOutcome::Replicated,
            "an interior torn copy is not sampled, so the drain commits"
        );
        let pooled = fakes.pool_part(&part).expect("part landed on CephFS");
        assert_eq!(
            pooled.chunks.get(&1).map(String::as_str),
            Some("corrupt-1"),
            "the interior corruption went undetected"
        );
    }

    #[tokio::test]
    async fn meta_is_persisted_only_after_every_chunk_is_verified() {
        // If a chunk copy fails, meta must NOT have been written — a reader's meta
        // gate must never flip on a partially-copied part.
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]).fault(Fault::PersistChunk);

        let err = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(
            err,
            PartDrainError::Io {
                step: DrainStep::Persist,
                ..
            }
        ));
        assert!(!fakes.pool_part(&part).is_some_and(|p| p.has_meta), "meta must not precede chunk copies");
    }

    #[tokio::test]
    async fn commit_failure_never_unlinks_the_ssd_copy() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]).fault(Fault::Commit);

        let err = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap_err();

        assert!(matches!(err, PartDrainError::Store(_)));
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Pending), "commit did not take");
        assert!(fakes.ssd_has(&part), "SSD copy preserved when commit fails");
    }

    #[tokio::test]
    async fn crash_before_commit_then_redrive_completes_the_drain() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0"), (1, "h1")]).fault(Fault::Commit);
        assert!(drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.is_err());
        assert!(fakes.ssd_has(&part), "interrupted drain kept the SSD copy");

        fakes.clear_faults();
        let outcome = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

        assert_eq!(outcome, DrainOutcome::Replicated);
        assert!(!fakes.ssd_has(&part));
        assert_eq!(fakes.status_of(&part), Some(ReplicationState::Replicated));
    }

    #[tokio::test]
    async fn draining_twice_is_idempotent() {
        let part = part();
        let fakes = Fakes::seeded(&part, &[(0, "h0")]);

        let first = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap();
        let second = drain_part(&fakes, &fakes, &fakes, &fakes, &claim(&part)).await.unwrap();

        assert_eq!(first, DrainOutcome::Replicated);
        assert_eq!(second, DrainOutcome::AlreadyReplicated);
    }
}
