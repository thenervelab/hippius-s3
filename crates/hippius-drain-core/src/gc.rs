//! Garbage collection of in-flight terminal upload debris.
//!
//! Eager per-chunk draining means an upload that goes terminal (aborted,
//! canceled, or failed — and so was NEVER committed) can leave chunks on BOTH the
//! SSD ingest cache and the `CephFS` pool. cephor owns reclaiming that debris (the
//! janitor is off the ingest path); committed-object deletes stay with
//! `arion-unpinner`. This module reclaims one terminal file's folders.
//!
//! Authorization is a capability: [`gc_object`] requires a [`GcClaim`], and the
//! only production constructor is the store's claim layer (M3, after it wins the
//! `SKIP LOCKED` `cephor_gc_state` marker). An agent therefore cannot reclaim a
//! file it has not won an exclusive, marker-backed claim for.

use crate::ids::FileId;
use core::future::Future;
use std::io;
use thiserror::Error;

/// The durable shared `CephFS` pool, as GC sees it: the one operation reclaim needs
/// is removing a terminal file's whole folder. (The drain's path-preserving part
/// copy lives in [`crate::PartPool`]; this is the GC-only reclaim contract.)
pub trait CephFs: Send + Sync {
    /// Remove an entire file's pool folder (`<file>/`) — the GC reclaim of a terminal
    /// upload. Surfaces "already absent" (`NotFound`) to its caller rather than
    /// swallowing it, so GC can report whether it reclaimed anything.
    fn remove_object(&self, file: &FileId) -> impl Future<Output = io::Result<()>> + Send;
}

/// The node-local SSD ingest cache, as GC sees it: reclaim a terminal file's folder.
pub trait SsdCache: Send + Sync {
    /// Remove an entire file's SSD folder (`<file>/`) — the GC reclaim of a terminal
    /// upload. Surfaces `NotFound` to its caller (see [`CephFs::remove_object`]).
    fn remove_object(&self, file: &FileId) -> impl Future<Output = io::Result<()>> + Send;
}

/// A won, marker-backed authorization to GC one terminal file's debris.
///
/// Like [`crate::Verified`], the field is private: only this crate constructs a
/// `GcClaim`, and in production only the store's claim layer does (after winning
/// the `SKIP LOCKED` `cephor_gc_state` marker). An agent thus cannot forge
/// authority to delete a file's data. The cross-process exclusion is the DB
/// claim; this token is the in-process proof that the claim was won.
///
/// Deferred: binding the claim to the owning object's *terminal* state is the api
/// write-fence contract; until it lands, the marker proves "won the claim" but not
/// "object is terminal". A future `GcClaim<'tx>` will also bind it to the
/// authorizing transaction's lifetime — and must do so with
/// `PhantomData<fn(&'tx ()) -> &'tx ()>` (invariant in `'tx`), NOT the `&'tx mut ()`
/// the plan sketched, which the variance table shows is *covariant* in `'tx` and
/// would let the witness outlive its transaction.
#[derive(Debug)]
pub struct GcClaim {
    file: FileId,
}

impl GcClaim {
    /// Mints a claim. Crate-private so only the store's claim layer (the winner of
    /// the durable marker) can authorize a GC; external crates cannot forge one.
    /// Gated to the configurations that mint claims — the Postgres store and tests
    /// — so a store-less build does not carry an unused constructor.
    #[cfg(any(test, feature = "pg"))]
    pub(crate) fn new(file: FileId) -> Self {
        Self { file }
    }

    /// The terminal file this claim authorizes reclaiming.
    #[must_use]
    pub fn file(&self) -> &FileId {
        &self.file
    }
}

/// Which side's folder a GC error struck, for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcTarget {
    /// The node-local SSD ingest cache.
    Ssd,
    /// The shared `CephFS` pool.
    Pool,
}

impl core::fmt::Display for GcTarget {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Ssd => "SSD",
            Self::Pool => "pool",
        })
    }
}

/// What a GC reclaim accomplished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcOutcome {
    /// At least one folder existed and was removed.
    Reclaimed,
    /// Every folder was already absent — an idempotent no-op (a prior GC, or a
    /// drain that already unlinked the chunks, got there first).
    AlreadyGone,
    /// A folder was concurrently written into (`DirectoryNotEmpty`): some debris
    /// was removed but not all. Re-claim and retry once the writer is fenced out.
    Retryable,
}

/// A GC failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GcError {
    /// An unexpected I/O error while reclaiming a folder.
    #[error("gc failed to reclaim the {target} folder")]
    Io {
        /// Which side failed.
        target: GcTarget,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
}

/// Maps a folder-removal result to a GC outcome.
///
/// The two benign edges are the documented contract of `remove_dir_all`:
/// `NotFound` is returned only when nothing was removed (already gone), and
/// `DirectoryNotEmpty` means a concurrent writer added entries mid-removal (retry
/// after fencing). Any other error is a real failure.
fn classify(result: io::Result<()>, target: GcTarget) -> Result<GcOutcome, GcError> {
    match result {
        Ok(()) => Ok(GcOutcome::Reclaimed),
        Err(err) => match err.kind() {
            io::ErrorKind::NotFound => Ok(GcOutcome::AlreadyGone),
            io::ErrorKind::DirectoryNotEmpty => Ok(GcOutcome::Retryable),
            _ => Err(GcError::Io { target, source: err }),
        },
    }
}

/// Folds two folder-reclaim outcomes into the object's overall outcome.
///
/// `Retryable` dominates (any incomplete removal makes the whole reclaim
/// incomplete), then `Reclaimed` (something was freed), and `AlreadyGone` only
/// when both sides were already absent.
fn combine(a: GcOutcome, b: GcOutcome) -> GcOutcome {
    use GcOutcome::{AlreadyGone, Reclaimed, Retryable};
    match (a, b) {
        (Retryable, _) | (_, Retryable) => Retryable,
        (Reclaimed, _) | (_, Reclaimed) => Reclaimed,
        (AlreadyGone, AlreadyGone) => AlreadyGone,
    }
}

/// Reclaims a terminal file's debris from the SSD cache and the `CephFS` pool.
///
/// Both removals are idempotent at the outcome level (an absent folder is
/// `AlreadyGone`, not an error), so a crash mid-reclaim is recovered by a
/// re-claim. The `SKIP LOCKED` DB claim behind `claim` is the cross-process
/// exclusion, so two agents never double-delete.
///
/// SSD-local reclaim (no shared-write hazard) always runs. `CephFS`-folder
/// reclaim is gated behind the `gc-cephfs` feature and ships **off** until the api
/// write-fence is confirmed (the design's owned task), so a late chunk write
/// cannot race the folder removal.
///
/// # Errors
///
/// [`GcError::Io`] if either removal fails with anything but the two benign edges.
#[cfg_attr(
    not(feature = "gc-cephfs"),
    expect(unused_variables, reason = "ceph is used only when the gc-cephfs feature gates in pool reclaim")
)]
pub async fn gc_object<F, S>(ceph: &F, ssd: &S, claim: &GcClaim) -> Result<GcOutcome, GcError>
where
    F: CephFs,
    S: SsdCache,
{
    let file = claim.file();

    // SSD-local reclaim has no shared-write hazard, so it always runs.
    let ssd_outcome = classify(ssd.remove_object(file).await, GcTarget::Ssd)?;

    // CephFS-folder reclaim shares the pool with the api's writer; it is gated off
    // until the write-fence is confirmed so a late chunk write cannot race it.
    #[cfg(feature = "gc-cephfs")]
    let pool_outcome = classify(ceph.remove_object(file).await, GcTarget::Pool)?;
    #[cfg(not(feature = "gc-cephfs"))]
    let pool_outcome = GcOutcome::AlreadyGone;

    Ok(combine(ssd_outcome, pool_outcome))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{CephFs, GcClaim, GcError, GcOutcome, GcTarget, SsdCache, classify, combine, gc_object};
    use crate::ids::FileId;
    use core::future::Future;
    use core::str::FromStr;
    use proptest::prelude::*;
    use std::collections::HashSet;
    use std::io;
    use std::sync::Mutex;

    #[derive(Default)]
    struct GcWorld {
        ssd_files: HashSet<String>,
        pool_files: HashSet<String>,
        ssd_fault: Option<io::ErrorKind>,
        pool_fault: Option<io::ErrorKind>,
    }

    /// Fakes both filesystem contracts over in-memory folder sets so GC outcomes
    /// can be exercised — including injected `DirectoryNotEmpty`/error edges —
    /// without touching a disk.
    #[derive(Default)]
    struct GcFakes {
        world: Mutex<GcWorld>,
    }

    impl GcFakes {
        fn ssd_has(&self, file: &str) -> bool {
            self.world.lock().unwrap().ssd_files.contains(file)
        }

        fn pool_has(&self, file: &str) -> bool {
            self.world.lock().unwrap().pool_files.contains(file)
        }
    }

    fn remove_folder(present: &mut HashSet<String>, file: &str, fault: Option<io::ErrorKind>) -> io::Result<()> {
        if let Some(kind) = fault {
            return Err(io::Error::new(kind, "injected"));
        }
        if present.remove(file) {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "absent"))
        }
    }

    impl CephFs for GcFakes {
        fn remove_object(&self, file: &FileId) -> impl Future<Output = io::Result<()>> + Send {
            let file = file.as_str().to_owned();
            async move {
                let mut world = self.world.lock().unwrap();
                let fault = world.pool_fault;
                remove_folder(&mut world.pool_files, &file, fault)
            }
        }
    }

    impl SsdCache for GcFakes {
        fn remove_object(&self, file: &FileId) -> impl Future<Output = io::Result<()>> + Send {
            let file = file.as_str().to_owned();
            async move {
                let mut world = self.world.lock().unwrap();
                let fault = world.ssd_fault;
                remove_folder(&mut world.ssd_files, &file, fault)
            }
        }
    }

    fn claim(file: &str) -> GcClaim {
        GcClaim::new(FileId::from_str(file).unwrap())
    }

    #[test]
    fn classify_maps_each_documented_edge() {
        assert!(matches!(classify(Ok(()), GcTarget::Ssd), Ok(GcOutcome::Reclaimed)));
        let not_found = Err(io::Error::new(io::ErrorKind::NotFound, "x"));
        assert!(matches!(classify(not_found, GcTarget::Ssd), Ok(GcOutcome::AlreadyGone)));
        let non_empty = Err(io::Error::new(io::ErrorKind::DirectoryNotEmpty, "x"));
        assert!(matches!(classify(non_empty, GcTarget::Pool), Ok(GcOutcome::Retryable)));
        let denied = Err(io::Error::new(io::ErrorKind::PermissionDenied, "x"));
        assert!(matches!(
            classify(denied, GcTarget::Pool),
            Err(GcError::Io { target: GcTarget::Pool, .. })
        ));
    }

    #[tokio::test]
    async fn reclaims_ssd_debris() {
        let fakes = GcFakes::default();
        fakes.world.lock().unwrap().ssd_files.insert("file-1".to_owned());

        let outcome = gc_object(&fakes, &fakes, &claim("file-1")).await.unwrap();

        assert_eq!(outcome, GcOutcome::Reclaimed);
        assert!(!fakes.ssd_has("file-1"), "SSD folder reclaimed");
    }

    #[tokio::test]
    async fn absent_everywhere_is_already_gone() {
        let fakes = GcFakes::default();

        let outcome = gc_object(&fakes, &fakes, &claim("ghost")).await.unwrap();

        assert_eq!(outcome, GcOutcome::AlreadyGone);
    }

    #[tokio::test]
    async fn unexpected_ssd_error_propagates() {
        let fakes = GcFakes::default();
        {
            let mut world = fakes.world.lock().unwrap();
            world.ssd_files.insert("file-1".to_owned());
            world.ssd_fault = Some(io::ErrorKind::PermissionDenied);
        }

        let err = gc_object(&fakes, &fakes, &claim("file-1")).await.unwrap_err();

        assert!(matches!(err, GcError::Io { target: GcTarget::Ssd, .. }));
    }

    #[cfg(not(feature = "gc-cephfs"))]
    #[tokio::test]
    async fn cephfs_reclaim_is_fenced_off_by_default() {
        // With the gc-cephfs feature off, the pool folder must be left untouched
        // (the api write-fence is unconfirmed); only SSD debris is reclaimed.
        let fakes = GcFakes::default();
        fakes.world.lock().unwrap().pool_files.insert("file-1".to_owned());

        let outcome = gc_object(&fakes, &fakes, &claim("file-1")).await.unwrap();

        assert_eq!(outcome, GcOutcome::AlreadyGone, "SSD absent, pool fenced -> nothing reclaimed");
        assert!(
            fakes.pool_has("file-1"),
            "pool folder must NOT be removed while the write-fence is unconfirmed"
        );
    }

    #[cfg(feature = "gc-cephfs")]
    #[tokio::test]
    async fn cephfs_reclaim_removes_the_pool_folder_when_enabled() {
        let fakes = GcFakes::default();
        fakes.world.lock().unwrap().pool_files.insert("file-1".to_owned());

        let outcome = gc_object(&fakes, &fakes, &claim("file-1")).await.unwrap();

        assert_eq!(outcome, GcOutcome::Reclaimed);
        assert!(!fakes.pool_has("file-1"), "pool folder reclaimed when the feature is on");
    }

    #[cfg(feature = "gc-cephfs")]
    #[tokio::test]
    async fn concurrent_pool_writer_yields_retryable() {
        let fakes = GcFakes::default();
        {
            let mut world = fakes.world.lock().unwrap();
            world.pool_files.insert("file-1".to_owned());
            world.pool_fault = Some(io::ErrorKind::DirectoryNotEmpty);
        }

        let outcome = gc_object(&fakes, &fakes, &claim("file-1")).await.unwrap();

        assert_eq!(outcome, GcOutcome::Retryable);
    }

    proptest! {
        /// `combine` is commutative, idempotent, and `Retryable`-absorbing.
        #[test]
        fn combine_is_commutative_and_retryable_absorbs(a in outcome(), b in outcome()) {
            prop_assert_eq!(combine(a, b), combine(b, a));
            prop_assert_eq!(combine(a, a), a);
            if a == GcOutcome::Retryable || b == GcOutcome::Retryable {
                prop_assert_eq!(combine(a, b), GcOutcome::Retryable);
            }
        }
    }

    fn outcome() -> impl Strategy<Value = GcOutcome> {
        prop_oneof![Just(GcOutcome::Reclaimed), Just(GcOutcome::AlreadyGone), Just(GcOutcome::Retryable)]
    }
}
