//! Postgres-backed durable state: part replication status / claims, the landed-part
//! log, GC claims, and upload context. The loss-tolerant coordination state (leader
//! lease, node heartbeats, per-node allocations) lives in [`Coordinator`](crate::Coordinator)
//! on Redis, not here.
//!
//! Uses runtime `sqlx` queries rather than the compile-checked `query!` macro,
//! deliberately: this environment cannot produce a committable `.sqlx` offline
//! cache, and runtime queries keep the crate buildable with no database present.
//! Correctness is verified by the `#[sqlx::test]` integration tests (run with
//! `--features pg`), which apply the real migrations to a real Postgres and
//! exercise every query — so schema drift is caught at test time. Revisit
//! `query!` once CI Postgres infra exists.

use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
use crate::gc::GcClaim;
use crate::ids::FileId;
use crate::partdrain::{ClaimedPart, PartReplicationStore, PartVerified};
use crate::reconcile::PartLandingLog;
use crate::reconcile::PartStatus;
use crate::ssd_reclaim::{PartStatusAge, ReclaimLog};
use crate::state::ReplicationState;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;

/// Errors from the central state store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// A query or connection failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// Applying migrations failed.
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// A byte count exceeded the i64 range a Postgres BIGINT can store.
    #[error("value {value} exceeds the i64 range of a Postgres BIGINT")]
    OutOfRange {
        /// The offending value.
        value: u64,
    },
    /// A stored value violated its domain type's invariant when read back.
    #[error("stored {field} value {value} is not valid for its domain type")]
    Invalid {
        /// The column whose value was invalid.
        field: &'static str,
        /// The offending value.
        value: i64,
    },
    /// A part commit matched no `draining` row — the claim was lost to a concurrent
    /// transition (e.g. a re-claim after lease expiry). The SSD copy must NOT be
    /// unlinked, so the orchestrator surfaces this rather than a false success. The
    /// part is named by its relative dir; `Box<str>` keeps `StoreError` small.
    #[error("replication claim lost for part `{part}`")]
    PartClaimLost {
        /// The part (relative dir) whose claim was lost.
        part: Box<str>,
    },
    /// A status column held a value outside the four known replication states.
    /// The table's CHECK constraint should make this unreachable; the variant
    /// exists so schema drift surfaces as a typed error, never a panic.
    #[error("unknown replication status `{value}`")]
    UnknownState {
        /// The unrecognized status text.
        value: Box<str>,
    },
}

type Result<T> = core::result::Result<T, StoreError>;

/// Parses a stored status string back into a [`ReplicationState`]. The four
/// literals match the `cephor_replication_status.status` CHECK constraint and
/// the `'draining'`/`'replicated'`/etc. literals the write queries set.
fn state_from_db(raw: &str) -> Result<ReplicationState> {
    match raw {
        "pending" => Ok(ReplicationState::Pending),
        "draining" => Ok(ReplicationState::Draining),
        "replicated" => Ok(ReplicationState::Replicated),
        "failed" => Ok(ReplicationState::Failed),
        other => Err(StoreError::UnknownState { value: other.into() }),
    }
}

/// How many parts one [`ReclaimLog::part_states`] query covers. A whole-SSD scan is
/// chunked into batches of this many tuples so a pathological backlog cannot build
/// one giant `IN (...)` list; the worker still reads every part, just over a few
/// queries instead of one unbounded one.
const RECLAIM_STATUS_BATCH: usize = 500;

/// Converts an `EXTRACT(EPOCH FROM (now() - updated_at))` age in seconds into a
/// [`Duration`], clamping a negative (clock skew) or non-finite value to zero. A
/// clamped-to-zero age reads as "just updated", so the reclaim age gate keeps the
/// part — the fail-safe direction.
fn age_from_secs(secs: f64) -> Duration {
    Duration::try_from_secs_f64(secs.max(0.0)).unwrap_or(Duration::ZERO)
}

/// An advisory pending-part backlog item from [`Store::list_landed_pending_parts`].
///
/// Confers no claim, only reports that a complete part is awaiting drain (for the
/// reconciler / backlog metrics). Claiming it is a separate [`Store::claim_part`].
#[derive(Debug, Clone)]
pub struct PendingPart {
    /// The part `(object_id, version, part_number)` awaiting drain.
    pub part: PartKey,
}

/// A part identity read back from `cephor_replication_status`.
///
/// `version`/`part_number` are stored as `BIGINT` (the domain numbers are `u32`,
/// whose max exceeds `i32`), so they read back as `i64` and are range-checked into
/// `u32` on the way to the domain newtypes.
#[derive(sqlx::FromRow)]
struct PartRow {
    object_id: String,
    version: i64,
    part_number: i64,
}

impl PartRow {
    /// Validates the three columns into a [`PartKey`]. A malformed object id or an
    /// out-of-`u32`-range number is schema drift (the api writes validated parts),
    /// surfaced as [`StoreError::Invalid`] rather than silently coerced.
    fn into_part(self) -> Result<PartKey> {
        let object = ObjectId::try_from(self.object_id).map_err(|_| StoreError::Invalid {
            field: "object_id",
            value: 0,
        })?;
        let version = u32::try_from(self.version).map_err(|_| StoreError::Invalid {
            field: "version",
            value: self.version,
        })?;
        let part = u32::try_from(self.part_number).map_err(|_| StoreError::Invalid {
            field: "part_number",
            value: self.part_number,
        })?;
        Ok(PartKey::new(object, Version::new(version), PartNumber::new(part)))
    }

    fn into_pending(self) -> Result<PendingPart> {
        Ok(PendingPart { part: self.into_part()? })
    }
}

/// A claimed part read back from the `claim_part` UPDATE … RETURNING — the part
/// identity plus the [`claim_seq`](ClaimedPart::claim_seq) fencing token the claim
/// stamped, so the commit can prove it still holds the claim it was granted.
#[derive(sqlx::FromRow)]
struct ClaimedPartRow {
    object_id: String,
    version: i64,
    part_number: i64,
    claim_seq: i64,
}

impl ClaimedPartRow {
    fn into_claimed(self) -> Result<ClaimedPart> {
        let part = PartRow {
            object_id: self.object_id,
            version: self.version,
            part_number: self.part_number,
        }
        .into_part()?;
        Ok(ClaimedPart::new(part, self.claim_seq))
    }
}

/// The default claim lease: a `draining` row whose claim is older than this is
/// treated as abandoned and re-claimable. Long enough that an ordinary slow
/// drain is never reclaimed out from under a live agent, short enough that a
/// crashed claim recovers promptly. Override per deployment via
/// [`Store::with_claim_lease`].
const DEFAULT_CLAIM_LEASE: Duration = Duration::from_mins(5);

/// The default deferral backoff: how long a part that deferred (enqueue not ready —
/// `object_versions.address` not finalized yet) is parked before `claim_part` will
/// re-claim it. Short enough that a just-completed MPU's parts upload promptly, long
/// enough that the drain does not spin on not-ready parts every poll. Override via
/// [`Store::with_defer_backoff`].
const DEFAULT_DEFER_BACKOFF: Duration = Duration::from_secs(5);

/// Handle to the Postgres central state. Cheap to clone (shares the pool).
#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
    /// How long a `draining` claim is honored before [`Store::claim_chunk`]
    /// treats it as abandoned and re-claims it (the H1 crash-recovery TTL).
    claim_lease: Duration,
    /// How long a deferred part is backed off before it is re-claimable, so the
    /// drain does not re-claim a not-ready part on every poll (which would starve
    /// the ready ones). Applied by [`Store::defer_part`], honored by `claim_part`.
    defer_backoff: Duration,
    /// The agent's node id. Parts live on node-local SSD, so a part may only be
    /// drained by the node that holds it: `record_landed_part` stamps this and
    /// `claim_part` scopes to it. `None` for the allocator, which records/claims
    /// no parts.
    node_id: Option<String>,
}

impl Store {
    /// Connects a pool to `url`.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] if the connection cannot be established.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        Ok(Self {
            pool,
            claim_lease: DEFAULT_CLAIM_LEASE,
            defer_backoff: DEFAULT_DEFER_BACKOFF,
            node_id: None,
        })
    }

    /// Wraps an existing pool (used by the integration tests). Defaults a `node_id` so
    /// the part claim/record tests work without each setting one; cross-node tests
    /// override it via [`with_node_id`](Self::with_node_id). Production builds the store
    /// with [`connect`](Self::connect) + `with_node_id`, never this.
    #[must_use]
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            claim_lease: DEFAULT_CLAIM_LEASE,
            defer_backoff: DEFAULT_DEFER_BACKOFF,
            node_id: Some("test-node".to_owned()),
        }
    }

    /// Sets the claim lease TTL (the daemon wires this from config). A
    /// `draining` claim older than `lease` is re-claimable by another agent.
    #[must_use]
    pub fn with_claim_lease(mut self, lease: Duration) -> Self {
        self.claim_lease = lease;
        self
    }

    /// Sets the deferral backoff (the daemon wires this from `CEPHOR_DEFER_BACKOFF_SECS`).
    /// A part that defers is not re-claimable until `backoff` has elapsed.
    #[must_use]
    pub fn with_defer_backoff(mut self, backoff: Duration) -> Self {
        self.defer_backoff = backoff;
        self
    }

    /// Sets the agent's node id (the daemon wires this from `CEPHOR_NODE_ID`).
    /// Required for `record_landed_part`/`claim_part` to scope parts to this node;
    /// the allocator leaves it unset.
    #[must_use]
    pub fn with_node_id(mut self, node: &str) -> Self {
        self.node_id = Some(node.to_owned());
        self
    }

    /// Applies any pending migrations.
    ///
    /// # Errors
    ///
    /// [`StoreError::Migrate`] if a migration fails.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Claims a file for GC. Returns `Some(GcClaim)` if this caller won the claim,
    /// `None` if another agent already holds a live, incomplete claim — so the
    /// reclaim runs once. The returned [`GcClaim`] is the capability
    /// [`crate::gc_object`] requires; this is its only production constructor, so
    /// winning the durable marker is the sole path to authorizing a reclaim.
    ///
    /// A claim whose holder crashed before [`complete_gc`](Store::complete_gc) is
    /// re-winnable once `claimed_at` ages past the claim lease — without this, a
    /// single crashed claimant would wedge a file's GC forever (the marker row
    /// conflicts but never completes). A *completed* claim is never re-won; a
    /// *fresh* (within-lease) claim is left to its current holder. Mirrors
    /// [`claim_chunk`](Store::claim_chunk)'s lease-based re-claim.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn claim_gc(&self, file: &FileId) -> Result<Option<GcClaim>> {
        let row = sqlx::query_as::<_, (String,)>(
            "INSERT INTO cephor_gc_state (file_id, claimed_at) VALUES ($1, now()) \
             ON CONFLICT (file_id) DO UPDATE SET claimed_at = now() \
                WHERE cephor_gc_state.completed_at IS NULL \
                  AND cephor_gc_state.claimed_at < now() - $2 * interval '1 second' \
             RETURNING file_id",
        )
        .bind(file.as_str())
        .bind(self.claim_lease.as_secs_f64())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|_| GcClaim::new(file.clone())))
    }

    /// Marks a claimed file's GC complete.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn complete_gc(&self, file: &FileId) -> Result<()> {
        sqlx::query("UPDATE cephor_gc_state SET completed_at = now() WHERE file_id = $1")
            .bind(file.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Whether a file's GC has completed.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn is_gc_complete(&self, file: &FileId) -> Result<bool> {
        let row = sqlx::query_as::<_, (bool,)>("SELECT completed_at IS NOT NULL FROM cephor_gc_state WHERE file_id = $1")
            .bind(file.as_str())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some_and(|(done,)| done))
    }

    /// Records that a part has landed on SSD and awaits drain (the reconciler-only
    /// landed signal). Idempotent: a repeat for the same `(object_id, version,
    /// part_number)` is a no-op, so the reconciler's backstop scan can call it safely.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn record_landed_part(&self, part: &PartKey) -> Result<()> {
        // Stamp the recording node so `claim_part` only drains parts whose data is on
        // this node's SSD. The UPSERT self-heals legacy rows: a row first written
        // without a node (or by an older agent) is adopted by whichever node still
        // holds the part locally and re-records it; a row already owned is left alone.
        sqlx::query(
            "INSERT INTO cephor_replication_status (object_id, version, part_number, status, node_id) \
             VALUES ($1, $2, $3, 'pending', $4) \
             ON CONFLICT (object_id, version, part_number) \
             DO UPDATE SET node_id = EXCLUDED.node_id WHERE cephor_replication_status.node_id IS NULL",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(self.node_id.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Claims one pending part for draining, transitioning it `pending → draining`
    /// (the part analogue of [`claim_chunk`](Store::claim_chunk)).
    ///
    /// `FOR UPDATE SKIP LOCKED` is the cross-process exclusion; a row another claimer
    /// holds is skipped rather than blocked on. The selector also re-claims a
    /// `draining` part whose claim has outlived the lease — the crash-recovery path —
    /// re-stamping `claimed_at`. A NULL `claimed_at` never satisfies the `<`
    /// predicate, so a `draining` row is reclaimed only once a claim timestamp exists.
    /// Returns `None` when nothing is pending.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`]; [`StoreError::Invalid`] if a stored part is malformed.
    pub async fn claim_part(&self) -> Result<Option<ClaimedPart>> {
        // Stamp a fresh fencing token (nextval) on the claim and return it: the commit
        // (mark_replicated) is guarded by it, so a claim re-won here after lease expiry
        // gets a NEW token and the prior claimant's stale commit fences out (F4).
        let row = sqlx::query_as::<_, ClaimedPartRow>(
            "UPDATE cephor_replication_status \
             SET status = 'draining', updated_at = now(), claimed_at = now(), claim_seq = nextval('cephor_claim_seq') \
             WHERE (object_id, version, part_number) IN ( \
                SELECT object_id, version, part_number FROM cephor_replication_status \
                WHERE node_id = $2 \
                  AND ( (status = 'pending' AND (deferred_until IS NULL OR deferred_until <= now())) \
                        OR (status = 'draining' AND claimed_at < now() - $1 * interval '1 second') ) \
                ORDER BY landed_at \
                FOR UPDATE SKIP LOCKED LIMIT 1 \
             ) RETURNING object_id, version, part_number, claim_seq",
        )
        .bind(self.claim_lease.as_secs_f64())
        .bind(self.node_id.as_deref())
        .fetch_optional(&self.pool)
        .await?;
        row.map(ClaimedPartRow::into_claimed).transpose()
    }

    /// Returns a claimed (`draining`) part to `pending` so it is re-claimed on a
    /// later wake (the part analogue of [`release_claim`](Store::release_claim)).
    /// The `status = 'draining'` guard makes it a no-op if the row has since advanced,
    /// so a late release cannot resurrect a finished part.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn release_part(&self, part: &PartKey) -> Result<()> {
        // Clear deferred_until too: a release is the Ceph-failure retry path, which
        // should be re-claimable immediately — it must not inherit a stale backoff a
        // prior deferral parked on the row.
        sqlx::query(
            "UPDATE cephor_replication_status SET status = 'pending', updated_at = now(), claimed_at = NULL, deferred_until = NULL \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining'",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Returns a claimed (`draining`) part to `pending` but backed off for
    /// [`defer_backoff`](Store::with_defer_backoff) — the path for a part whose drain
    /// deferred because its upload enqueue was not ready yet (the object's address is
    /// not finalized). `claim_part` skips a `pending` row whose `deferred_until` is
    /// still in the future, so the drain stops re-claiming the same not-ready part on
    /// every poll (which would spin on it and starve the parts that are ready). Guarded
    /// on `draining` like [`release_part`](Store::release_part), so a late defer cannot
    /// resurrect a finished part.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn defer_part(&self, part: &PartKey) -> Result<()> {
        sqlx::query(
            "UPDATE cephor_replication_status \
             SET status = 'pending', updated_at = now(), claimed_at = NULL, deferred_until = now() + $4 * interval '1 second' \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining'",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(self.defer_backoff.as_secs_f64())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lists up to `limit` pending parts, oldest first — the reconciler's advisory
    /// backlog view. Confers no claim; see [`Store::claim_part`].
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`]; [`StoreError::Invalid`] if a stored part is malformed.
    pub async fn list_landed_pending_parts(&self, limit: u32) -> Result<Vec<PendingPart>> {
        let rows = sqlx::query_as::<_, PartRow>(
            "SELECT object_id, version, part_number FROM cephor_replication_status \
             WHERE status = 'pending' ORDER BY landed_at LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(PartRow::into_pending).collect()
    }

    /// Loads the non-derivable fields the agent needs to build a part's backend
    /// `UploadChainRequest` (s3-2.1 PR-11, drain-direct enqueue): `bucket_name`,
    /// `object_key`, the main-account `address`, and the latest `upload_id` (MPU only).
    /// Everything else the uploader re-derives by `object_id`.
    ///
    /// Returns `None` when the version row is absent OR its `address` is still NULL —
    /// the api writes `address` at PUT/MPU-complete, so a NULL means the part landed on
    /// SSD before the api finished (a rare race). The caller treats `None` as not-ready
    /// and retries on a later wake rather than enqueuing an incomplete request.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on query failure.
    pub async fn load_upload_context(&self, part: &PartKey) -> Result<Option<UploadContext>> {
        let row = sqlx::query_as::<_, UploadContextRow>(
            // The upload_id subquery is scoped to the PART's own version via `parts`
            // (not object_id alone): an object first uploaded via MPU and later
            // overwritten by a simple PUT keeps a `multipart_uploads` row, so an
            // object_id-only lookup would stamp that stale upload_id onto the
            // simple-PUT part and flip the uploader's request name from `simple::` to
            // `multipart::`. Keying on `parts.object_version = $2` ties the upload
            // identity to the version actually being drained.
            "SELECT b.bucket_name, o.object_key, ov.address, \
                    ( SELECT mu.upload_id::text FROM parts p \
                      JOIN multipart_uploads mu ON mu.upload_id = p.upload_id \
                      WHERE p.object_id = o.object_id AND p.object_version = $2 \
                      ORDER BY mu.initiated_at DESC LIMIT 1 ) AS upload_id \
             FROM objects o \
             JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = $2 \
             JOIN buckets b ON b.bucket_id = o.bucket_id \
             WHERE o.object_id = $1::uuid",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| {
            r.address.map(|address| UploadContext {
                object_id: part.object().as_str().to_owned(),
                object_version: part.version().get(),
                part_number: part.part().get(),
                bucket_name: r.bucket_name,
                object_key: r.object_key,
                address,
                upload_id: r.upload_id,
            })
        }))
    }
}

/// The non-derivable fields for a part's backend upload request, read from the app
/// tables by [`Store::load_upload_context`]. Consumed by the agent's upload enqueuer.
#[derive(Debug, Clone)]
pub struct UploadContext {
    /// The object UUID (as text, the wire form).
    pub object_id: String,
    /// The object version.
    pub object_version: u32,
    /// The part number being enqueued.
    pub part_number: u32,
    /// The bucket the object lives in.
    pub bucket_name: String,
    /// The object key.
    pub object_key: String,
    /// The main-account address (the backend upload identity).
    pub address: String,
    /// The MPU upload id, if this object was a multipart upload.
    pub upload_id: Option<String>,
}

#[derive(sqlx::FromRow)]
struct UploadContextRow {
    bucket_name: String,
    object_key: String,
    address: Option<String>,
    upload_id: Option<String>,
}

/// Postgres-backed part replication state for the drain orchestrator.
///
/// The status guard on commit (`WHERE status = 'draining'`) is the correctness
/// anchor: a commit that matches no row means the claim was lost, surfaced as
/// [`StoreError::PartClaimLost`] so the orchestrator never unlinks the SSD copy.
impl PartReplicationStore for Store {
    type Error = StoreError;

    async fn status(&self, part: &PartKey) -> Result<Option<ReplicationState>> {
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT status FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(status,)| state_from_db(&status)).transpose()
    }

    async fn mark_replicated(&self, claim: &ClaimedPart, _proof: &PartVerified) -> Result<()> {
        // Guard on `draining` AND the claim's fencing token: only the agent that still
        // holds THIS claim may commit. Zero rows means the claim was lost — either the
        // row left `draining`, or it was re-claimed (a new claim_seq) after the lease,
        // e.g. this agent stalled past the lease and another re-won the part. Either
        // way the caller must NOT unlink the SSD copy — surface PartClaimLost, not a
        // false Ok. The claim_seq is what distinguishes "I still hold it" from "someone
        // re-won it and it's draining again under them" (F4).
        let part = claim.part();
        let affected = sqlx::query(
            "UPDATE cephor_replication_status SET status = 'replicated', updated_at = now() \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining' AND claim_seq = $4",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(claim.claim_seq())
        .execute(&self.pool)
        .await?
        .rows_affected();
        if affected == 0 {
            return Err(StoreError::PartClaimLost {
                part: part.relative_dir().to_string_lossy().into_owned().into(),
            });
        }
        Ok(())
    }

    async fn mark_failed(&self, claim: &ClaimedPart, _reason: &str) -> Result<()> {
        // Guarded on `draining` AND this claim's fencing token, mirroring mark_replicated:
        // only the agent that still holds THIS claim may terminally fail the part. A fenced
        // stale claimant (its claim re-won after the lease) matches zero rows and MUST NOT
        // flip the live re-claimed part to `failed`. Zero rows is a harmless no-op here
        // (unlike mark_replicated) because mark_failed authorizes no SSD unlink — so it is
        // NOT surfaced as PartClaimLost, which also keeps it idempotent (a second call
        // finds status='failed', not 'draining'). Clear claimed_at, like release_part, so a
        // failed part holds no lingering live-claim timestamp (F18).
        let part = claim.part();
        sqlx::query(
            "UPDATE cephor_replication_status SET status = 'failed', updated_at = now(), claimed_at = NULL \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining' AND claim_seq = $4",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(claim.claim_seq())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// The part reconciler's view of the store: read a part's status (+ adoptability)
/// and record/adopt a freshly-landed one (via the inherent
/// [`Store::record_landed_part`]), so the reconciler shares one definition of the
/// landed write with the drain path.
impl PartLandingLog for Store {
    type Error = StoreError;

    async fn status(&self, part: &PartKey) -> Result<Option<PartStatus>> {
        // One read returns both the state and adoptability — a `pending` row with no
        // owning node is a legacy row the scanning node should adopt (G2). Reading
        // node_id here (rather than writing every cycle) is what keeps the reconciler
        // from a per-cycle write against the slow store for already-owned rows.
        let row = sqlx::query_as::<_, (String, bool)>(
            "SELECT status, (status = 'pending' AND node_id IS NULL) \
             FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some((status, adoptable)) => Ok(Some(PartStatus {
                state: state_from_db(&status)?,
                adoptable,
            })),
        }
    }

    async fn record_landed(&self, part: &PartKey) -> Result<()> {
        Store::record_landed_part(self, part).await
    }
}

/// The reclaim worker's batched view of the store: read the replication state + age
/// of many parts in ONE round-trip, so the per-cycle SSD scan never fans out into a
/// per-part SELECT (the reconciler's O(backlog) cost the worker must not repeat).
impl ReclaimLog for Store {
    type Error = StoreError;

    async fn part_states(&self, parts: &[PartKey]) -> Result<HashMap<PartKey, PartStatusAge>> {
        let mut out = HashMap::with_capacity(parts.len());
        // Chunk the IN-list so a pathological backlog never builds one giant query.
        for batch in parts.chunks(RECLAIM_STATUS_BATCH) {
            let mut object_ids: Vec<&str> = Vec::with_capacity(batch.len());
            let mut versions: Vec<i64> = Vec::with_capacity(batch.len());
            let mut part_numbers: Vec<i64> = Vec::with_capacity(batch.len());
            for part in batch {
                object_ids.push(part.object().as_str());
                versions.push(i64::from(part.version().get()));
                part_numbers.push(i64::from(part.part().get()));
            }
            // Match the batch by its PK tuple via UNNEST'd parallel arrays — one query,
            // a PK lookup per element (no new index needed). A part with no row simply
            // does not come back, so the caller treats it as absent.
            let rows = sqlx::query_as::<_, (String, i64, i64, String, f64)>(
                // EXTRACT(EPOCH ...) is `numeric` on PG 14+, which does not decode into
                // f64 — cast to float8 so sqlx reads the age as a plain double.
                "SELECT object_id, version, part_number, status, \
                        EXTRACT(EPOCH FROM (now() - updated_at))::float8 \
                 FROM cephor_replication_status \
                 WHERE (object_id, version, part_number) IN \
                       (SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::bigint[]))",
            )
            .bind(&object_ids)
            .bind(&versions)
            .bind(&part_numbers)
            .fetch_all(&self.pool)
            .await?;
            for (object_id, version, part_number, status, age_secs) in rows {
                let part = PartRow {
                    object_id,
                    version,
                    part_number,
                }
                .into_part()?;
                out.insert(
                    part,
                    PartStatusAge {
                        state: state_from_db(&status)?,
                        age: age_from_secs(age_secs),
                    },
                );
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
#[cfg(feature = "pg")]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::Store;
    use crate::ids::FileId;
    use core::str::FromStr;
    use sqlx::postgres::PgPool;

    #[sqlx::test]
    async fn claim_part_is_scoped_to_the_recording_node(pool: PgPool) {
        // G1 regression: parts live on node-local SSD, so an agent must only claim parts
        // it recorded (holds locally). Before node-scoping, the global claim let an agent
        // grab a peer's part and fail at the missing-local meta copy, churning forever.
        use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
        let part_a = PartKey::new(
            ObjectId::from_str("466916c0-d61b-4518-b81b-9576b574270a").unwrap(),
            Version::new(1),
            PartNumber::new(1),
        );
        let part_b = PartKey::new(
            ObjectId::from_str("00000000-0000-4000-8000-000000000000").unwrap(),
            Version::new(1),
            PartNumber::new(1),
        );
        let node_a = Store::from_pool(pool.clone()).with_node_id("node-a");
        let node_b = Store::from_pool(pool.clone()).with_node_id("node-b");

        node_a.record_landed_part(&part_a).await.unwrap();
        node_b.record_landed_part(&part_b).await.unwrap();

        let claim = node_a.claim_part().await.unwrap().expect("node-a claims its own part");
        assert_eq!(claim.part(), &part_a);
        assert!(
            node_a.claim_part().await.unwrap().is_none(),
            "node-a must not claim node-b's part (its only other pending row)",
        );
        let claim_b = node_b.claim_part().await.unwrap().expect("node-b claims its own part");
        assert_eq!(claim_b.part(), &part_b);
    }

    #[sqlx::test]
    async fn record_landed_part_adopts_a_legacy_nodeless_row(pool: PgPool) {
        // The UPSERT self-heals rows written before node_id existed (NULL node): the node
        // that still holds the part locally re-records it and stamps its node_id, so the
        // claim then sees it.
        use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
        let part = PartKey::new(
            ObjectId::from_str("466916c0-d61b-4518-b81b-9576b574270a").unwrap(),
            Version::new(2),
            PartNumber::new(3),
        );
        sqlx::query("INSERT INTO cephor_replication_status (object_id, version, part_number, status) VALUES ($1, 2, 3, 'pending')")
            .bind(part.object().as_str())
            .execute(&pool)
            .await
            .unwrap();
        let node_a = Store::from_pool(pool.clone()).with_node_id("node-a");
        assert!(
            node_a.claim_part().await.unwrap().is_none(),
            "a NULL-node row is unclaimable until adopted"
        );
        node_a.record_landed_part(&part).await.unwrap();
        assert!(
            node_a.claim_part().await.unwrap().is_some(),
            "after re-recording, node-a owns and claims it"
        );
    }

    #[sqlx::test]
    async fn gc_claim_is_exclusive_then_completes(pool: PgPool) {
        let store = Store::from_pool(pool);
        let file = FileId::from_str("file-1").unwrap();
        assert!(store.claim_gc(&file).await.unwrap().is_some(), "first claim wins");
        assert!(store.claim_gc(&file).await.unwrap().is_none(), "second claim loses");
        assert!(!store.is_gc_complete(&file).await.unwrap());
        store.complete_gc(&file).await.unwrap();
        assert!(store.is_gc_complete(&file).await.unwrap());
    }

    #[sqlx::test]
    async fn a_stale_incomplete_gc_claim_is_reclaimable(pool: PgPool) {
        // The GC analogue of the chunk-claim crash-recovery fix (#13): an agent won
        // a GC claim (marker row inserted) then crashed before `complete_gc`. The
        // claim is now older than the lease, so a fresh claim must reclaim the
        // wedged file — without this, the conflicting, never-completed row would
        // block that file's reclaim forever.
        let store = Store::from_pool(pool.clone());
        let file = FileId::from_str("file-1").unwrap();
        assert!(store.claim_gc(&file).await.unwrap().is_some(), "first claim wins");
        assert!(store.claim_gc(&file).await.unwrap().is_none(), "a fresh claim is still held");

        // Age the claim past the lease (backdating is the only deterministic way to
        // fast-forward the lease clock — there is no public API to age a claim).
        sqlx::query("UPDATE cephor_gc_state SET claimed_at = now() - interval '1 hour' WHERE file_id = $1")
            .bind(file.as_str())
            .execute(&pool)
            .await
            .unwrap();

        assert!(
            store.claim_gc(&file).await.unwrap().is_some(),
            "a stale, incomplete GC claim is re-winnable",
        );
    }

    #[sqlx::test]
    async fn a_completed_gc_claim_is_never_reclaimed(pool: PgPool) {
        // Once a file's GC has completed its debris is gone, so even an aged claim
        // row must not hand out a new claim (which would re-run a pointless reclaim).
        let store = Store::from_pool(pool.clone());
        let file = FileId::from_str("file-1").unwrap();
        store.claim_gc(&file).await.unwrap().expect("first claim wins");
        store.complete_gc(&file).await.unwrap();

        sqlx::query("UPDATE cephor_gc_state SET claimed_at = now() - interval '1 hour' WHERE file_id = $1")
            .bind(file.as_str())
            .execute(&pool)
            .await
            .unwrap();

        assert!(
            store.claim_gc(&file).await.unwrap().is_none(),
            "a completed GC is terminal and never re-claimed, even when aged",
        );
    }
}

#[cfg(test)]
#[cfg(feature = "pg")]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod part_tests {
    use super::{Store, StoreError};
    use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
    use crate::partdrain::{ClaimedPart, PartReplicationStore, PartVerified};
    use crate::ssd_reclaim::ReclaimLog;
    use crate::state::ReplicationState;
    use core::str::FromStr;
    use sqlx::postgres::PgPool;
    use std::time::Duration;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";
    const UUID_B: &str = "00000000-0000-4000-8000-000000000000";

    fn part(uuid: &str, version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(uuid).unwrap(), Version::new(version), PartNumber::new(number))
    }

    /// Forces a part's row to a terminal status with a backdated `updated_at`, so the
    /// reclaim age reads deterministically older than any plausible grace.
    async fn force_terminal(pool: &PgPool, part: &PartKey, status: &str) {
        sqlx::query(
            "UPDATE cephor_replication_status SET status = $4, updated_at = now() - interval '2 hours' \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(status)
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn part_states_returns_state_and_age_for_known_parts_only(pool: PgPool) {
        let store = Store::from_pool(pool.clone());
        let pending = part(UUID_A, 5, 1);
        let replicated = part(UUID_A, 5, 2);
        let failed = part(UUID_A, 5, 3);
        let absent = part(UUID_B, 7, 1);

        store.record_landed_part(&pending).await.unwrap();
        store.record_landed_part(&replicated).await.unwrap();
        force_terminal(&pool, &replicated, "replicated").await;
        store.record_landed_part(&failed).await.unwrap();
        force_terminal(&pool, &failed, "failed").await;

        let states = <Store as ReclaimLog>::part_states(&store, &[pending.clone(), replicated.clone(), failed.clone(), absent.clone()])
            .await
            .unwrap();

        assert_eq!(states.len(), 3, "the part with no row is omitted (treated as absent)");
        assert!(!states.contains_key(&absent), "an unknown part has no entry");

        let pending_status = states.get(&pending).expect("pending part present");
        assert_eq!(pending_status.state, ReplicationState::Pending);
        assert!(pending_status.age < Duration::from_hours(1), "a freshly landed row reads young");

        let replicated_status = states.get(&replicated).expect("replicated part present");
        assert_eq!(replicated_status.state, ReplicationState::Replicated);
        assert!(replicated_status.age >= Duration::from_hours(1), "the backdated row reads ~2h old");

        assert_eq!(states.get(&failed).expect("failed part present").state, ReplicationState::Failed);
    }

    #[sqlx::test]
    async fn part_states_of_an_empty_request_is_empty(pool: PgPool) {
        let store = Store::from_pool(pool);
        let states = <Store as ReclaimLog>::part_states(&store, &[]).await.unwrap();
        assert!(states.is_empty(), "no parts requested -> no query, empty map");
    }

    #[sqlx::test]
    async fn claim_returns_none_when_nothing_is_pending(pool: PgPool) {
        let store = Store::from_pool(pool);
        assert!(store.claim_part().await.unwrap().is_none());
    }

    #[sqlx::test]
    async fn status_is_none_for_an_unknown_part(pool: PgPool) {
        let store = Store::from_pool(pool);
        assert_eq!(store.status(&part(UUID_A, 5, 1)).await.unwrap(), None);
    }

    #[sqlx::test]
    async fn record_landed_part_is_idempotent(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.record_landed_part(&p).await.unwrap(); // ON CONFLICT DO NOTHING
        assert_eq!(
            store.list_landed_pending_parts(10).await.unwrap().len(),
            1,
            "a repeat landing must not duplicate the row"
        );
    }

    #[sqlx::test]
    async fn the_same_object_version_with_a_different_part_is_a_distinct_row(pool: PgPool) {
        // The PK is the full (object, version, part) triple, so two parts of one
        // object version are distinct backlog rows — the api uploads them separately.
        let store = Store::from_pool(pool);
        store.record_landed_part(&part(UUID_A, 5, 1)).await.unwrap();
        store.record_landed_part(&part(UUID_A, 5, 2)).await.unwrap();
        assert_eq!(store.list_landed_pending_parts(10).await.unwrap().len(), 2);
    }

    #[sqlx::test]
    async fn record_then_claim_transitions_pending_to_draining(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Pending));

        let claimed = store.claim_part().await.unwrap().expect("a pending part is claimable");
        assert_eq!(claimed.part(), &p);
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Draining));
        assert!(store.claim_part().await.unwrap().is_none(), "the claimed part is no longer pending");
    }

    #[sqlx::test]
    async fn release_part_returns_a_drained_part_to_pending(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().expect("a pending part is claimable");
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Draining));

        store.release_part(claimed.part()).await.unwrap();
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Pending));
        assert!(store.claim_part().await.unwrap().is_some(), "the released part is re-claimable");
    }

    #[sqlx::test]
    async fn claim_reclaims_a_stale_draining_part(pool: PgPool) {
        // The crash-recovery case: an agent claimed a part (status -> draining) then
        // crashed mid-drain without releasing it. The claim is now older than the
        // lease, so a fresh claim must reclaim the abandoned row.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().expect("a pending part is claimable");

        sqlx::query("UPDATE cephor_replication_status SET claimed_at = now() - interval '1 hour' WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();

        let reclaimed = store.claim_part().await.unwrap().expect("a stale draining part is re-claimable");
        assert_eq!(reclaimed.part(), &p);
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Draining));
    }

    #[sqlx::test]
    async fn claim_does_not_reclaim_a_fresh_draining_part(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().expect("a pending part is claimable");
        assert!(
            store.claim_part().await.unwrap().is_none(),
            "a freshly-claimed draining part is still held, not re-claimable",
        );
    }

    #[sqlx::test]
    async fn mark_replicated_commits_a_claimed_part(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_replicated(&claimed, &PartVerified::for_test()).await.unwrap();
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Replicated));
    }

    #[sqlx::test]
    async fn mark_replicated_without_a_draining_claim_is_part_claim_lost(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap(); // status = pending, never claimed
        // A ClaimedPart constructed without claiming must NOT be able to commit: the
        // row is still 'pending', so the 'draining' guard matches no row (the token
        // value is irrelevant here — the status guard already rejects it).
        let unclaimed = ClaimedPart::new(p.clone(), 0);
        let err = store.mark_replicated(&unclaimed, &PartVerified::for_test()).await.unwrap_err();
        let expected = p.relative_dir().to_string_lossy().into_owned();
        assert!(
            matches!(err, StoreError::PartClaimLost { ref part } if part.as_ref() == expected),
            "expected PartClaimLost for {expected:?}, got: {err:?}",
        );
        assert_eq!(
            store.status(&p).await.unwrap(),
            Some(ReplicationState::Pending),
            "a lost commit leaves the row untouched",
        );
    }

    #[sqlx::test]
    async fn mark_failed_sets_failed_and_is_idempotent(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_failed(&claimed, "chunk copy byte mismatch").await.unwrap();
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Failed));
        store.mark_failed(&claimed, "again").await.unwrap(); // idempotent no-op
    }

    #[sqlx::test]
    async fn mark_failed_clears_claimed_at(pool: PgPool) {
        // F18: a failed part holds no live claim, so its claimed_at must be nulled
        // (mirroring release_part) — otherwise a lingering timestamp misrepresents it
        // as freshly claimed.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap(); // stamps claimed_at
        store.mark_failed(&claimed, "byte mismatch").await.unwrap();
        let claimed_at_is_null: bool = sqlx::query_scalar(
            "SELECT claimed_at IS NULL FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(p.object().as_str())
        .bind(i64::from(p.version().get()))
        .bind(i64::from(p.part().get()))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(claimed_at_is_null, "mark_failed clears claimed_at");
    }

    #[sqlx::test]
    async fn a_reclaim_after_lease_expiry_fences_the_stale_committer(pool: PgPool) {
        // F4: the crash-recovery race. Agent 1 claims a part; its claim ages past the
        // lease; another claim re-wins it (here the same store, after backdating
        // claimed_at) and gets a FRESH fencing token. Agent 1 then finishes and tries
        // to commit — the row is `draining` again (under the new claim), so the bare
        // status guard would wrongly accept it and unlink the SSD copy. The claim_seq
        // guard rejects the stale commit (PartClaimLost) while the live claim commits.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let first = store.claim_part().await.unwrap().expect("the pending part is claimable");

        sqlx::query("UPDATE cephor_replication_status SET claimed_at = now() - interval '1 hour' WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();
        let second = store.claim_part().await.unwrap().expect("the stale claim is re-won past the lease");
        assert_ne!(first.claim_seq(), second.claim_seq(), "the re-claim gets a fresh fencing token");

        let err = store.mark_replicated(&first, &PartVerified::for_test()).await.unwrap_err();
        let expected = p.relative_dir().to_string_lossy().into_owned();
        assert!(
            matches!(err, StoreError::PartClaimLost { ref part } if part.as_ref() == expected),
            "the stale claimant is fenced out, got: {err:?}",
        );
        assert_eq!(
            store.status(&p).await.unwrap(),
            Some(ReplicationState::Draining),
            "the fenced commit leaves the live claim's draining row untouched",
        );
        store.mark_replicated(&second, &PartVerified::for_test()).await.unwrap();
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Replicated));
    }

    #[sqlx::test]
    async fn mark_failed_by_a_fenced_stale_claimant_does_not_fail_the_live_part(pool: PgPool) {
        // WI-3: the mark_failed counterpart of the mark_replicated fence. A stale claimant
        // whose claim was re-won after the lease must NOT flip the live re-claimed part to
        // `failed` — its guarded UPDATE matches zero rows (a harmless no-op, not an error,
        // since mark_failed authorizes no SSD unlink).
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let first = store.claim_part().await.unwrap().expect("the pending part is claimable");

        sqlx::query("UPDATE cephor_replication_status SET claimed_at = now() - interval '1 hour' WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();
        let second = store.claim_part().await.unwrap().expect("the stale claim is re-won past the lease");
        assert_ne!(first.claim_seq(), second.claim_seq(), "the re-claim gets a fresh fencing token");

        // The fenced (stale) claimant tries to fail the part — a no-op, not an error.
        store.mark_failed(&first, "chunk copy byte mismatch").await.unwrap();
        assert_eq!(
            store.status(&p).await.unwrap(),
            Some(ReplicationState::Draining),
            "the live re-claimed part is untouched by the fenced claimant's mark_failed",
        );

        // The live claimant can still fail it.
        store.mark_failed(&second, "chunk copy byte mismatch").await.unwrap();
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Failed));
    }

    #[sqlx::test]
    async fn list_landed_pending_parts_returns_the_pending_set_and_excludes_claimed(pool: PgPool) {
        let store = Store::from_pool(pool);
        let (p1, p2) = (part(UUID_A, 5, 1), part(UUID_B, 7, 3));
        store.record_landed_part(&p1).await.unwrap();
        store.record_landed_part(&p2).await.unwrap();

        // Assert the set (sorted by relative dir), not the arrival order: two
        // now()-stamped rows can tie at the DB clock's resolution.
        let mut dirs: Vec<_> = store
            .list_landed_pending_parts(10)
            .await
            .unwrap()
            .iter()
            .map(|p| p.part.relative_dir().to_string_lossy().into_owned())
            .collect();
        dirs.sort();
        let mut expected = vec![
            p1.relative_dir().to_string_lossy().into_owned(),
            p2.relative_dir().to_string_lossy().into_owned(),
        ];
        expected.sort();
        assert_eq!(dirs, expected);

        // Claiming one removes it from the pending backlog.
        store.claim_part().await.unwrap().unwrap();
        assert_eq!(store.list_landed_pending_parts(10).await.unwrap().len(), 1);
    }

    #[sqlx::test]
    async fn reconcile_parts_recovers_a_dropped_trigger_and_makes_it_claimable(pool: PgPool) {
        use crate::reconcile::{DiscoveredPart, PartScan, reconcile_parts};
        use core::future::Future;

        // A scanner standing in for the SSD walk: it sees one complete part on SSD
        // whose landed trigger was dropped (no DB row).
        struct OnePart(DiscoveredPart);
        impl PartScan for OnePart {
            fn scan_parts(&self) -> impl Future<Output = std::io::Result<Vec<DiscoveredPart>>> + Send {
                let parts = vec![self.0.clone()];
                async move { Ok(parts) }
            }
        }

        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        assert!(store.claim_part().await.unwrap().is_none(), "no row exists before the reconcile");

        let scanner = OnePart(DiscoveredPart { part: p.clone() });
        let report = reconcile_parts(&scanner, &store).await.unwrap();
        assert_eq!(report.recovered, 1, "the dropped-trigger part was recovered to pending");

        let claimed = store.claim_part().await.unwrap().expect("the recovered part is claimable");
        assert_eq!(claimed.part(), &p);
    }

    #[sqlx::test]
    async fn reconcile_adopts_a_legacy_nodeless_pending_part(pool: PgPool) {
        // G2: a `pending` row with no owning node (written before node-scoping) is
        // invisible to the node-scoped claim_part, so it never drains. The reconciler
        // on the node that still holds the part on SSD adopts it (stamps its node_id)
        // via the idempotent record_landed UPSERT, making it claimable again.
        use crate::reconcile::{DiscoveredPart, PartScan, reconcile_parts};
        use core::future::Future;

        struct OnePart(DiscoveredPart);
        impl PartScan for OnePart {
            fn scan_parts(&self) -> impl Future<Output = std::io::Result<Vec<DiscoveredPart>>> + Send {
                let parts = vec![self.0.clone()];
                async move { Ok(parts) }
            }
        }

        let p = part(UUID_A, 5, 1);
        sqlx::query(
            "INSERT INTO cephor_replication_status (object_id, version, part_number, status, node_id) \
             VALUES ($1, $2, $3, 'pending', NULL)",
        )
        .bind(p.object().as_str())
        .bind(i64::from(p.version().get()))
        .bind(i64::from(p.part().get()))
        .execute(&pool)
        .await
        .unwrap();

        let node_b = Store::from_pool(pool.clone()).with_node_id("node-b");
        assert!(
            node_b.claim_part().await.unwrap().is_none(),
            "a NULL-node row is unclaimable by any node before adoption",
        );

        let scanner = OnePart(DiscoveredPart { part: p.clone() });
        let report = reconcile_parts(&scanner, &node_b).await.unwrap();
        assert_eq!(report.adopted, 1, "the legacy nodeless row was adopted by the scanning node");

        let claimed = node_b.claim_part().await.unwrap().expect("the adopted part is now claimable by node-b");
        assert_eq!(claimed.part(), &p);
    }

    // --- load_upload_context (the drain-direct enqueue's read of the app tables) ---

    const MPU_UPLOAD_ID: &str = "11111111-1111-4111-8111-111111111111";

    /// Creates the minimal slice of the api schema `load_upload_context` reads
    /// (`buckets`, `objects`, `object_versions`, `multipart_uploads`, `parts`) — only
    /// the columns the query touches. The drain-core migrations are cephor-only, so a
    /// contract test for the cross-table query has to stand the app tables up itself.
    /// Executed statement-by-statement because sqlx prepares each query (the extended
    /// protocol rejects multiple statements in one prepare).
    async fn create_app_schema(pool: &PgPool) {
        for ddl in [
            "CREATE TABLE buckets (bucket_id uuid PRIMARY KEY, bucket_name text NOT NULL)",
            "CREATE TABLE objects (object_id uuid PRIMARY KEY, bucket_id uuid NOT NULL, object_key text NOT NULL)",
            "CREATE TABLE object_versions (object_id uuid NOT NULL, object_version bigint NOT NULL, address text, \
             PRIMARY KEY (object_id, object_version))",
            "CREATE TABLE multipart_uploads (upload_id uuid PRIMARY KEY, object_id uuid, initiated_at timestamptz NOT NULL)",
            "CREATE TABLE parts (object_id uuid NOT NULL, object_version bigint NOT NULL, part_number bigint NOT NULL, upload_id uuid)",
        ] {
            sqlx::query(ddl).execute(pool).await.unwrap();
        }
    }

    /// Seeds one bucket + object and one of its versions (with `address`, possibly NULL).
    async fn seed_object_version(pool: &PgPool, object: &str, version: i64, address: Option<&str>) {
        sqlx::query("INSERT INTO buckets (bucket_id, bucket_name) VALUES ($1::uuid, 'b') ON CONFLICT DO NOTHING")
            .bind(UUID_B)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO objects (object_id, bucket_id, object_key) VALUES ($1::uuid, $2::uuid, 'k') ON CONFLICT DO NOTHING")
            .bind(object)
            .bind(UUID_B)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO object_versions (object_id, object_version, address) VALUES ($1::uuid, $2, $3)")
            .bind(object)
            .bind(version)
            .bind(address)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Records a part row at `(object, version, number)` linked to `upload_id` (NULL for
    /// a simple-PUT part). Mirrors how the api populates `parts` per object version.
    async fn seed_part_row(pool: &PgPool, object: &str, version: i64, number: i64, upload_id: Option<&str>) {
        sqlx::query("INSERT INTO parts (object_id, object_version, part_number, upload_id) VALUES ($1::uuid, $2, $3, $4::uuid)")
            .bind(object)
            .bind(version)
            .bind(number)
            .bind(upload_id)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Records a multipart upload header (its `initiated_at` is the latest-wins tiebreak).
    async fn seed_multipart_upload(pool: &PgPool, upload_id: &str, object: &str) {
        sqlx::query("INSERT INTO multipart_uploads (upload_id, object_id, initiated_at) VALUES ($1::uuid, $2::uuid, now())")
            .bind(upload_id)
            .bind(object)
            .execute(pool)
            .await
            .unwrap();
    }

    #[sqlx::test]
    async fn load_upload_context_returns_the_context_when_the_address_is_set(pool: PgPool) {
        create_app_schema(&pool).await;
        seed_object_version(&pool, UUID_A, 5, Some("hippius-addr")).await;
        let store = Store::from_pool(pool);

        let ctx = store
            .load_upload_context(&part(UUID_A, 5, 1))
            .await
            .unwrap()
            .expect("a version with an address yields a context");
        assert_eq!(ctx.address, "hippius-addr");
        assert_eq!(ctx.bucket_name, "b");
        assert_eq!(ctx.object_key, "k");
        assert_eq!(ctx.object_version, 5);
        assert_eq!(ctx.part_number, 1);
        assert_eq!(ctx.upload_id, None, "a part with no multipart row is a simple upload");
    }

    #[sqlx::test]
    async fn load_upload_context_is_none_when_the_address_is_null(pool: PgPool) {
        // The api writes `address` at PUT/MPU-complete; until then the part is on SSD
        // but not enqueueable, so the drain must treat it as not-ready (None), not 500.
        create_app_schema(&pool).await;
        seed_object_version(&pool, UUID_A, 5, None).await;
        let store = Store::from_pool(pool);
        assert_eq!(store.load_upload_context(&part(UUID_A, 5, 1)).await.unwrap().map(|c| c.address), None);
    }

    #[sqlx::test]
    async fn load_upload_context_is_none_for_a_missing_version_row(pool: PgPool) {
        create_app_schema(&pool).await;
        seed_object_version(&pool, UUID_A, 5, Some("addr")).await;
        let store = Store::from_pool(pool);
        // Version 9 was never written, so the inner join drops the row entirely.
        assert!(store.load_upload_context(&part(UUID_A, 9, 1)).await.unwrap().is_none());
    }

    #[sqlx::test]
    async fn load_upload_context_scopes_the_upload_id_to_the_part_version(pool: PgPool) {
        // The regression the version scope fixes: an object first uploaded via MPU
        // (version 1) and later overwritten by a simple PUT (version 2, same object_id)
        // must NOT stamp the stale MPU `upload_id` onto the simple-PUT part — that would
        // flip the uploader's request name from `simple::` to `multipart::`. Keying the
        // subquery on the part's own version (via `parts`) instead of object_id alone
        // is what keeps each version's upload identity correct.
        create_app_schema(&pool).await;
        seed_multipart_upload(&pool, MPU_UPLOAD_ID, UUID_A).await;
        // Version 1: a multipart upload — its part links to the MPU header.
        seed_object_version(&pool, UUID_A, 1, Some("addr-v1")).await;
        seed_part_row(&pool, UUID_A, 1, 1, Some(MPU_UPLOAD_ID)).await;
        // Version 2: a simple PUT overwrite — its part has no upload linkage.
        seed_object_version(&pool, UUID_A, 2, Some("addr-v2")).await;
        seed_part_row(&pool, UUID_A, 2, 1, None).await;
        let store = Store::from_pool(pool);

        let v1 = store.load_upload_context(&part(UUID_A, 1, 1)).await.unwrap().expect("v1 context");
        assert_eq!(v1.upload_id.as_deref(), Some(MPU_UPLOAD_ID), "the MPU version keeps its upload_id");

        let v2 = store.load_upload_context(&part(UUID_A, 2, 1)).await.unwrap().expect("v2 context");
        assert_eq!(v2.upload_id, None, "the simple-PUT version must not inherit the prior MPU's upload_id");
    }

    // --- deferral backoff (a NotReady part must not be re-claimed every poll) ---

    #[sqlx::test]
    async fn a_deferred_part_is_not_reclaimable_until_its_backoff_elapses(pool: PgPool) {
        // An enqueue deferral (object address not finalized yet) backs the part off so
        // the drain stops re-claiming it on every poll — which would otherwise spin on
        // not-ready parts and starve the parts that ARE ready to upload. It becomes
        // claimable again only once deferred_until elapses.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        assert_eq!(store.claim_part().await.unwrap().as_ref().map(ClaimedPart::part), Some(&p));

        store.defer_part(&p).await.unwrap();
        assert!(
            store.claim_part().await.unwrap().is_none(),
            "a freshly deferred part is backed off, not immediately re-claimable",
        );

        // Backdating deferred_until is the only deterministic way to fast-forward the
        // backoff clock (it is the server clock, like the claim lease).
        sqlx::query("UPDATE cephor_replication_status SET deferred_until = now() - interval '1 second' WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            store.claim_part().await.unwrap().is_some(),
            "once the backoff elapses the deferred part is claimable again",
        );
    }

    #[sqlx::test]
    async fn release_part_clears_a_prior_deferral_backoff(pool: PgPool) {
        // A Ceph-write failure (release_part) must retry promptly, so it clears any
        // backoff a prior deferral set rather than leaving the part parked.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().unwrap();
        store.defer_part(&p).await.unwrap();
        assert!(store.claim_part().await.unwrap().is_none(), "deferred -> backed off");

        sqlx::query("UPDATE cephor_replication_status SET deferred_until = now() - interval '1 second' WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();
        store.claim_part().await.unwrap().expect("claimable after the backoff elapsed");
        store.release_part(&p).await.unwrap();
        assert!(
            store.claim_part().await.unwrap().is_some(),
            "release clears the backoff, so a Ceph-failed part retries immediately",
        );
    }
}
