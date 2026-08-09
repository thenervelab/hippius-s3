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
use crate::redrive::PartDigest;
use crate::ssd_evict::{ResidentLog, ResidentPart};
use crate::ssd_reclaim::{BackingLog, PartStatusAge, ReclaimLog};
use crate::state::ReplicationState;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::collections::HashSet;
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

/// Parses a stored status string back into a [`ReplicationState`]. The five
/// literals match the `cephor_replication_status.status` CHECK constraint and
/// the `'draining'`/`'replicated'`/etc. literals the write queries set.
fn state_from_db(raw: &str) -> Result<ReplicationState> {
    match raw {
        "pending" => Ok(ReplicationState::Pending),
        "draining" => Ok(ReplicationState::Draining),
        "replicated" => Ok(ReplicationState::Replicated),
        "failed" => Ok(ReplicationState::Failed),
        "corrupt" => Ok(ReplicationState::Corrupt),
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

/// The ceiling on the exponential deferral backoff: a part that keeps deferring
/// (an abandoned MPU that never finalizes) doubles its backoff per attempt but is
/// never parked longer than this, so it still re-checks within minutes of the
/// address finally landing. Override via [`Store::with_defer_backoff_cap`].
const DEFAULT_DEFER_BACKOFF_CAP: Duration = Duration::from_mins(10);

/// How long a re-landed `replicated` part stays off the eviction worklist, measured from the
/// `relanded_at` stamp [`Store::record_landed_part`] writes when an announcement names a part
/// that already committed — the B-2 rewrite signal.
///
/// The gate must outlive the whole record→check→disposition path in one landed tick: the
/// divergence check re-reads and hashes the entire part off SSD, and a tick checks up to the
/// pop batch (512) of re-landed parts sequentially. Ten minutes covers that with a wide margin
/// while bounding the failure mode of a crash between record and check: such a part is merely
/// unevictable for ten minutes, and only re-announced parts (rare by construction) are ever
/// gated at all, so the space this can withhold from an armed evictor is negligible.
const RELAND_EVICTION_GRACE: Duration = Duration::from_mins(10);

/// What [`Store::defer_part_missing_source`] did to the row — decided atomically
/// inside the guarded UPDATE, so no concurrent claim can interleave between the
/// deferral and the terminal escalation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingSourceOutcome {
    /// Below the threshold: deferred back to `pending` with the shared exponential
    /// backoff, carrying the row's new missing-source observation count.
    Deferred(u64),
    /// The observation count reached the threshold: the row is now terminal `failed`
    /// (never resurrected — the write-off is deliberate and conservative).
    Failed,
    /// The `status='draining'` guard missed — the row advanced under a concurrent
    /// transition (e.g. a lease-expiry re-claim) — so nothing changed. A no-op
    /// defer cannot escalate.
    Superseded,
}

/// What [`Store::record_landed_part`] found for the part BEFORE it recorded the landing.
///
/// A freshly-recorded part reports `Pending` with no digest. The case that matters is
/// `Replicated`: the api announces a part only from `WriteThroughPartsWriter.write_meta`, the
/// single choke point every upload path funnels through, so an announcement for an already-
/// committed part means that part was WRITTEN AGAIN — the B-2 divergence shape. The digest is
/// what tells a genuine rewrite from a duplicate announcement; see [`crate::verdict_for_reland`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandedOutcome {
    /// The part's replication state before this landing was recorded. The upsert never touches
    /// `status`, so this is the prior state.
    pub state: ReplicationState,
    /// The content digest recorded at the part's last commit, if it has one. `None` for a part
    /// that has never committed, and for one committed before `content_sha256` shipped.
    pub digest: Option<PartDigest>,
}

/// Splices the shared exponential-backoff expression between two SQL literal halves,
/// yielding one `&'static str` (sqlx 0.9's `SqlSafeStr` rejects runtime-built strings,
/// so this is `concat!`, not `format!`). The expression — base × 2^`defer_attempts`,
/// exponent clamped at 16 so `power` cannot overflow, product capped by the outer
/// `LEAST` — is defined ONCE here so the cap/clamp geometry cannot drift between
/// [`Store::defer_part`] and [`Store::defer_part_missing_source`]. It reads the row's
/// OLD `defer_attempts` (UPDATE right-hand sides see pre-update values) and requires
/// the splicing query to bind `$4` = base backoff seconds and `$5` = cap seconds.
macro_rules! with_defer_backoff_sql {
    ($prefix:expr, $suffix:expr) => {
        concat!(
            $prefix,
            "now() + LEAST($4 * power(2::float8, LEAST(defer_attempts, 16)), $5) * interval '1 second'",
            $suffix
        )
    };
}

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
    /// This is the BASE of the per-row exponential backoff; the row's
    /// `defer_attempts` doubles it per deferral up to [`defer_backoff_cap`](Self::with_defer_backoff_cap).
    defer_backoff: Duration,
    /// Ceiling on the exponential deferral backoff, so a chronically not-ready part
    /// still re-checks within minutes of its address finally landing.
    defer_backoff_cap: Duration,
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
            defer_backoff_cap: DEFAULT_DEFER_BACKOFF_CAP,
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
            defer_backoff_cap: DEFAULT_DEFER_BACKOFF_CAP,
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

    /// Sets the exponential-deferral ceiling (the daemon wires this from
    /// `CEPHOR_DEFER_BACKOFF_CAP_SECS`). No deferral parks a part longer than `cap`.
    #[must_use]
    pub fn with_defer_backoff_cap(mut self, cap: Duration) -> Self {
        self.defer_backoff_cap = cap;
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

    /// Deletes terminal replication rows older than `retention`, returning how many were
    /// removed. Terminal rows are inert — nothing returns one to a live state
    /// (`release_part`/`defer_part` are guarded on `status='draining'`) — so aged ones are
    /// pure debris that bloat the hot `claim_part` / reconcile scans. NEVER touches
    /// `pending`/`draining` (live) rows. Idempotent and safe to run concurrently: a row
    /// deleted by one sweep simply is not matched by another.
    ///
    /// "Terminal" here is `failed` OR a `replicated` row whose backend upload has ALREADY
    /// been enqueued (`upload_enqueued_at IS NOT NULL`). A `replicated` row still awaiting its
    /// enqueue (`upload_enqueued_at IS NULL` — an in-flight MPU part committed to Ceph but not
    /// yet published) is the enqueue sweep's worklist, so deleting it would DROP the backend
    /// upload; it is spared until the sweep stamps it (or the reaper flips it `failed` as an
    /// abandoned orphan, which this then GCs).
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] if the delete fails.
    pub async fn gc_terminal_status_rows(&self, retention: Duration) -> Result<u64> {
        let affected = sqlx::query(
            "DELETE FROM cephor_replication_status \
             WHERE (status = 'failed' OR (status = 'replicated' AND upload_enqueued_at IS NOT NULL)) \
               AND updated_at < now() - (interval '1 second' * $1)",
        )
        .bind(retention.as_secs_f64())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected)
    }

    /// The bytes this node currently holds RESIDENT on its ingest SSD to serve reads.
    ///
    /// The heartbeat's `cache_bytes`: evictable-on-demand space that counts toward ingest
    /// headroom rather than against it, so a disk full of warm cache does not read to the
    /// allocator as a node critically behind on draining.
    ///
    /// Served by `cephor_ssd_residency_evict_idx`, reading the denormalized `bytes` rather
    /// than joining the ~140M-row `parts` table. A part whose size was unknown at residency
    /// time contributes zero, which under-reports cache — the fail-safe direction, since
    /// understating evictable space overstates drain urgency rather than hiding it.
    ///
    /// # Errors
    ///
    /// [`StoreError`] if the query fails.
    pub async fn node_cache_bytes(&self, node: &str) -> Result<u64> {
        // Same guard as the eviction worklist, and for the same reason: `cache_bytes` means
        // EVICTABLE bytes. A resident part that is not `replicated` — a re-driven corrupt part
        // back in `pending`, say — is on the disk but cannot be evicted, and it is already
        // counted by `node_backlog_bytes` as the undrained work it is. Counting it here too
        // would double-count it AND overstate the node's ingest headroom, which understates its
        // drain urgency to the allocator: the one direction this signal must never err in.
        let (bytes,): (i64,) = sqlx::query_as(
            "SELECT COALESCE(SUM(r.bytes), 0)::bigint \
             FROM cephor_ssd_residency r \
             JOIN cephor_replication_status s \
               ON s.object_id = r.object_id AND s.version = r.version AND s.part_number = r.part_number \
             WHERE r.node_id = $1 AND s.status = 'replicated'",
        )
        .bind(node)
        .fetch_one(&self.pool)
        .await?;
        Ok(u64::try_from(bytes).unwrap_or(0))
    }

    /// The true drain backlog for `node`: the total `parts.size_bytes` of every part this
    /// node still owns as `pending`/`draining` in `cephor_replication_status`.
    ///
    /// This is the WI-20c reconcile of `drain_ssd_backlog_bytes`, which the heartbeat used
    /// to source from raw SSD occupancy (`statvfs`). Occupancy OVERCOUNTS the backlog by
    /// the A21/orphan leak — aborted-upload and deleted-object bytes sit on the SSD but are
    /// not undrained WORK — so a leaking node reads hundreds of GB of "backlog" with no
    /// drain demand. Keying on the terminal-filtered replication rows counts only parts the
    /// drain will actually replicate. A part whose `parts` row is absent contributes zero
    /// (INNER JOIN), and a NULL `size_bytes` is treated as zero (COALESCE), so the sum is a
    /// lower bound that never over-reports. Uses the node-scoped pending index.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] if the query fails.
    pub async fn node_backlog_bytes(&self, node: &str) -> Result<u64> {
        let (bytes,): (i64,) = sqlx::query_as(
            "SELECT COALESCE(SUM(p.size_bytes), 0)::bigint \
             FROM cephor_replication_status crs \
             JOIN parts p \
               ON p.object_id = crs.object_id::uuid \
              AND p.object_version = crs.version \
              AND p.part_number = crs.part_number \
             WHERE crs.node_id = $1 AND crs.status IN ('pending', 'draining')",
        )
        .bind(node)
        .fetch_one(&self.pool)
        .await?;
        // SUM of non-negative byte counts is >= 0; the cast guards a corrupt negative row.
        Ok(u64::try_from(bytes).unwrap_or(0))
    }

    /// The count of `node`'s undrained replication rows (`pending` + `draining`) — the C8
    /// readiness wedge signal. Unlike [`node_backlog_bytes`](Self::node_backlog_bytes) this does
    /// NOT join `parts`: a row whose `parts` row is absent or carries a NULL/0 `size_bytes`
    /// contributes zero bytes to that SUM but is still undrained WORK, so the byte-backlog of a
    /// wedged node can read 0 while rows remain — which readiness would misread as idle. Counting
    /// the replication rows directly cannot be zeroed that way, so it is the signal readiness gates
    /// on. Mirrors the params of `node_backlog_bytes`.
    ///
    /// Served by `cephor_replication_status_undrained_by_node` (0013). NOT by the node-scoped
    /// pending index, which is `WHERE status = 'pending'` and so never covered a query that also
    /// matches `'draining'` — before 0013 this fell to the orphan index and post-filtered by node.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] if the query fails.
    pub async fn node_undrained_count(&self, node: &str) -> Result<u64> {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT count(*)::bigint FROM cephor_replication_status \
             WHERE node_id = $1 AND status IN ('pending', 'draining')",
        )
        .bind(node)
        .fetch_one(&self.pool)
        .await?;
        // count(*) is >= 0; the cast guards against an impossible negative for total-ness.
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// The age in whole seconds of `node`'s oldest `pending` replication row, or 0 when none
    /// — the per-node starvation signal that would have caught 2026-07-26 at ~03:40: one
    /// node's oldest pending age past 30min while its peers sat near zero (a part landed
    /// 03:48 sat unclaimed 3.5h, diagnosable only by hand-written SQL at the time).
    ///
    /// `pending` ONLY — a `draining` row is being worked, so the signal is specifically
    /// "claimable-or-backed-off work nobody has finished". Deferred rows are deliberately
    /// INCLUDED (a backed-off row is still `pending`): an in-progress/abandoned-MPU wall
    /// aging past hours is exactly what the operator must see.
    ///
    /// Served index-only by `cephor_replication_status_pending` (0006: `(node_id, landed_at)
    /// WHERE status = 'pending'`) — the min is the first entry under the node prefix.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] if the query fails.
    pub async fn node_oldest_pending_age_secs(&self, node: &str) -> Result<u64> {
        let (secs,): (i64,) = sqlx::query_as(
            "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(landed_at)))::bigint, 0) \
             FROM cephor_replication_status \
             WHERE node_id = $1 AND status = 'pending'",
        )
        .bind(node)
        .fetch_one(&self.pool)
        .await?;
        // Clock skew could put min(landed_at) marginally in the future; clamp to 0 rather
        // than wrap into an absurd u64 age.
        Ok(u64::try_from(secs).unwrap_or(0))
    }

    /// R4 re-drive: reset this node's `corrupt` parts back to `pending` for a fresh SSD->pool
    /// copy (overwriting the corrupt pool copy from the intact SSD source), bounded by
    /// `max_attempts`. Only rows still under the cap are reset; each reset bumps
    /// `corrupt_attempts`, so a persistently-unrecoverable pool copy stops re-driving after
    /// `max_attempts` and is held `corrupt` (paged via the corrupt-backlog gauge/alert) rather
    /// than looping forever. Clears `claimed_at`/`deferred_until` so the part is immediately
    /// re-claimable. Returns how many parts were re-driven.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn redrive_corrupt_parts(&self, max_attempts: i32) -> Result<u64> {
        let affected = sqlx::query(
            "UPDATE cephor_replication_status \
             SET status = 'pending', corrupt_attempts = corrupt_attempts + 1, \
                 claimed_at = NULL, deferred_until = NULL, updated_at = now() \
             WHERE node_id = $1 AND status = 'corrupt' AND corrupt_attempts < $2",
        )
        .bind(self.node_id.as_deref())
        .bind(max_attempts)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected)
    }

    /// The count of this node's parts currently held in `corrupt` (the `drain_corrupt_parts`
    /// gauge). A nonzero value is a live object with a corrupt pool copy being kept alive by its
    /// SSD source — a durability incident, not routine GC. Includes rows still eligible for
    /// re-drive AND those held at the attempt cap (the standing, unrecoverable backlog).
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn count_corrupt_parts(&self) -> Result<u64> {
        let (count,): (i64,) = sqlx::query_as("SELECT count(*)::bigint FROM cephor_replication_status WHERE node_id = $1 AND status = 'corrupt'")
            .bind(self.node_id.as_deref())
            .fetch_one(&self.pool)
            .await?;
        Ok(u64::try_from(count).unwrap_or(0))
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

    /// Records that a part has landed on SSD and awaits drain. Idempotent: a repeat for the
    /// same `(object_id, version, part_number)` never changes `status`, so the reconciler's
    /// backstop scan and the announcement fast path can both call it freely.
    ///
    /// Returns what the row looked like BEFORE this call, because a landed announcement naming
    /// an already-`replicated` part is the one observable signal that a committed part was
    /// rewritten (B-2) — see [`crate::verdict_for_reland`]. The status is untouched by the
    /// upsert, so the returned state is the prior state.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`]; [`StoreError::Invalid`] if the stored status is unknown.
    pub async fn record_landed_part(&self, part: &PartKey) -> Result<LandedOutcome> {
        // Stamp the recording node so `claim_part` only drains parts whose data is on
        // this node's SSD. The UPSERT self-heals legacy rows: a row first written
        // without a node (or by an older agent) is adopted by whichever node still
        // holds the part locally and re-records it; a row already owned is left alone.
        //
        // COALESCE, not a `WHERE node_id IS NULL` conflict guard: it expresses the identical
        // adoption rule (keep the existing owner if there is one) while letting DO UPDATE
        // always fire, which is what makes RETURNING report on a conflicting row at all. A
        // guarded conflict action returns NOTHING when its WHERE fails, i.e. in exactly the
        // already-known case the divergence check needs to see. The cost is a row rewrite on
        // the conflict path only; the common announcement is a fresh part, hence a plain INSERT.
        //
        // relanded_at is stamped IN THE SAME STATEMENT as the prior-state read, and only when
        // that prior state is 'replicated' — the B-2 rewrite signal. It is what takes the part
        // off the eviction worklist (see `evictable_parts`) for the window between this record
        // and the divergence check's disposition: a rewritten committed part otherwise ranks as
        // maximally COLD (nothing on the rewrite path touches the residency recency the evictor
        // sorts on), so the evictor could unlink the ONLY copy of the new bytes before the
        // check hashes them — after which nothing re-drives and the pool serves stale bytes
        // forever. One statement, not a follow-up UPDATE, so there is no interleaving in which
        // the outcome reports Replicated while the eviction gate is not yet armed.
        let row = sqlx::query_as::<_, (String, Option<String>)>(
            "INSERT INTO cephor_replication_status (object_id, version, part_number, status, node_id) \
             VALUES ($1, $2, $3, 'pending', $4) \
             ON CONFLICT (object_id, version, part_number) \
             DO UPDATE SET node_id = COALESCE(cephor_replication_status.node_id, EXCLUDED.node_id), \
                           relanded_at = CASE WHEN cephor_replication_status.status = 'replicated' \
                                              THEN now() ELSE cephor_replication_status.relanded_at END \
             RETURNING status, content_sha256",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(self.node_id.as_deref())
        .fetch_one(&self.pool)
        .await?;
        let (status, digest) = row;
        Ok(LandedOutcome {
            state: state_from_db(&status)?,
            digest: digest.map(PartDigest::from_stored),
        })
    }

    /// Returns a `replicated` part to `pending` because its SSD content no longer matches the
    /// digest recorded at commit — the pool holds superseded bytes (B-2). Returns whether a row
    /// was actually re-driven.
    ///
    /// # Why this is the safe direction, always
    ///
    /// `replicated → pending` can only make a part LESS evictable. `ResidentLog::evictable_parts`
    /// joins the replication row and filters `status = 'replicated'`, so the instant this commits
    /// the part leaves the eviction worklist — and it leaves it while its pool copy is the stale
    /// one, i.e. exactly while the SSD copy is again the only good one. There is no interleaving
    /// in which this widens what the evictor may unlink, which is why residency is deliberately
    /// left alone (the part is still on the disk; `redrive_corrupt_parts` reasons identically).
    ///
    /// `upload_enqueued_at` is cleared too: the backend already shipped the superseded bytes, so
    /// the part must go back on the enqueue sweep's worklist. (Whether the backend then ACCEPTS
    /// the re-upload is the uploader's business — it dedups on `chunk_backend`, so a stale
    /// backend copy may survive this. That is a separate seam, not one the drain can close.)
    ///
    /// `defer_attempts` is deliberately NOT reset, matching [`release_part`](Self::release_part):
    /// a re-uploaded MPU part is precisely the address-still-NULL case, so zeroing the escalation
    /// on every retry would re-arm the head-of-line starvation the exponential backoff exists to
    /// prevent.
    ///
    /// The `content_sha256 IS DISTINCT FROM` guard carries both the idempotency and the NULL
    /// policy in one operator: a second call for the same rewrite finds `status <> 'replicated'`
    /// and no-ops, and a NULL digest (committed before this shipped) is DISTINCT FROM any
    /// observed value, so an unverifiable part re-drives rather than being assumed intact.
    ///
    /// `observed` is COMPARED, never stored. Only a commit may write `content_sha256`, because
    /// only a commit knows what actually reached the pool — the caller hashed the disk at some
    /// earlier instant, and a client mid-retry may already have replaced those bytes again.
    /// Stamping the read value here would record a digest for content the pool never received.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn redrive_diverged_part(&self, part: &PartKey, observed: &PartDigest) -> Result<bool> {
        let affected = sqlx::query(
            "UPDATE cephor_replication_status \
             SET status = 'pending', claimed_at = NULL, deferred_until = NULL, \
                 upload_enqueued_at = NULL, updated_at = now() \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND node_id = $4 \
               AND status = 'replicated' AND content_sha256 IS DISTINCT FROM $5",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(self.node_id.as_deref())
        .bind(observed.as_str())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected > 0)
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
        // prior deferral parked on the row. defer_attempts is deliberately left alone:
        // the escalation tracks the not-ready condition, which a Ceph blip says
        // nothing about, so resetting it here would re-arm the starvation spiral.
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
    /// The backoff is EXPONENTIAL per row (base × 2^`defer_attempts`, capped): a fixed
    /// backoff let a wall of not-ready in-progress-MPU parts — oldest by `landed_at`,
    /// so at the claim head — re-enter the claimable set every interval and consume
    /// every claim slot, starving all younger parts (the 2026-07-26 head-of-line
    /// starvation incident). Doubling per deferral moves a not-ready wall out of the
    /// claim hot path geometrically. The inner `LEAST(defer_attempts, 16)` clamps the
    /// exponent so `power` cannot overflow; the outer `LEAST` enforces
    /// [`defer_backoff_cap`](Store::with_defer_backoff_cap). `release_part` deliberately
    /// preserves `defer_attempts` — a transient Ceph-failure retry must not erase the
    /// not-ready escalation.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure.
    pub async fn defer_part(&self, part: &PartKey) -> Result<()> {
        sqlx::query(with_defer_backoff_sql!(
            "UPDATE cephor_replication_status \
             SET status = 'pending', updated_at = now(), claimed_at = NULL, \
                 defer_attempts = defer_attempts + 1, \
                 deferred_until = ",
            " WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining'"
        ))
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(self.defer_backoff.as_secs_f64())
        .bind(self.defer_backoff_cap.as_secs_f64())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Defers a claimed part whose SSD source directory is GONE (ENOENT at the size
    /// gate), counting the observation toward the terminal write-off — and, when the
    /// count reaches `fail_threshold`, flips the row terminal `failed` instead.
    ///
    /// Why a separate counter (`missing_source_attempts`, migration 0015) instead of
    /// keying the escalation on `defer_attempts`: that counter is SHARED with the
    /// overdraft (`OverdraftOutstanding`) and not-ready (in-progress MPU) deferral
    /// paths, so a healthy oversized part paying off a long debt — or an MPU part
    /// waiting on its address — could accumulate attempts and then be written off on
    /// its FIRST transient `NotFound`. `failed` is never resurrected (the reconciler
    /// only registers parts that exist on SSD, and `record_landed_part`'s ON CONFLICT
    /// never touches status), so a wrong write-off is silent replication loss; only
    /// genuine missing-source observations may count toward it.
    ///
    /// The defer and the escalation are ONE guarded UPDATE (`status='draining'`, the
    /// pending↔failed choice a CASE on the incremented count): a defer-then-fail pair
    /// would flip the row `pending` first, opening a window where a concurrent claim
    /// re-takes it and the follow-up fail either misses its guard or clobbers a live
    /// claim. Below the threshold the row defers exactly like [`defer_part`]
    /// (`defer_attempts` still increments — the backoff geometry stays shared); the
    /// new observation count is RETURNED so the caller needs no second round trip.
    /// A guard miss (the row advanced concurrently) is [`MissingSourceOutcome::Superseded`]:
    /// a no-op defer cannot escalate.
    ///
    /// `fail_threshold` must be greater than 1: a threshold of 0 or 1 writes the part
    /// off on its FIRST observation — the exact transient-`NotFound` hazard this
    /// design exists to prevent — so debug builds assert it (release builds execute
    /// the first-observation write-off as written).
    ///
    /// [`defer_part`]: Store::defer_part
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure; [`StoreError::Invalid`] if the stored
    /// counter reads back negative (unreachable by construction).
    pub async fn defer_part_missing_source(&self, part: &PartKey, fail_threshold: u32) -> Result<MissingSourceOutcome> {
        debug_assert!(
            fail_threshold > 1,
            "fail_threshold {fail_threshold} writes a part off on its first missing-source observation"
        );
        let row: Option<(i32, String)> = sqlx::query_as(with_defer_backoff_sql!(
            "UPDATE cephor_replication_status \
             SET missing_source_attempts = missing_source_attempts + 1, \
                 defer_attempts = defer_attempts + 1, \
                 status = CASE WHEN missing_source_attempts + 1 >= $6 THEN 'failed' ELSE 'pending' END, \
                 updated_at = now(), claimed_at = NULL, \
                 deferred_until = CASE WHEN missing_source_attempts + 1 >= $6 THEN NULL ELSE ",
            " END \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining' \
             RETURNING missing_source_attempts, status"
        ))
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(self.defer_backoff.as_secs_f64())
        .bind(self.defer_backoff_cap.as_secs_f64())
        .bind(i64::from(fail_threshold))
        .fetch_optional(&self.pool)
        .await?;
        let Some((observations, status)) = row else {
            return Ok(MissingSourceOutcome::Superseded);
        };
        if status == "failed" {
            return Ok(MissingSourceOutcome::Failed);
        }
        let observations = u64::try_from(observations).map_err(|_| StoreError::Invalid {
            field: "missing_source_attempts",
            value: i64::from(observations),
        })?;
        Ok(MissingSourceOutcome::Deferred(observations))
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

    /// The enqueue sweep's worklist: up to `limit` of THIS node's `replicated` parts whose
    /// backend upload has not yet been published (`upload_enqueued_at IS NULL`), oldest-
    /// committed first. These are parts drained to the pool before their object's address was
    /// finalized (an in-flight MPU) — the sweep re-attempts `enqueue` for each and, on success,
    /// stamps [`mark_upload_enqueued`](PartReplicationStore::mark_upload_enqueued). Node-scoped
    /// like `claim_part` (a part's upload identity is loaded via `load_upload_context`, but the
    /// worklist is this node's own committed parts). Uses the `cephor_replication_unenqueued_idx`
    /// partial index, so the scan is proportional to the small outstanding set.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`]; [`StoreError::Invalid`] if a stored part is malformed.
    pub async fn list_replicated_unenqueued_parts(&self, limit: u32) -> Result<Vec<PartKey>> {
        let rows = sqlx::query_as::<_, PartRow>(
            "SELECT object_id, version, part_number FROM cephor_replication_status \
             WHERE node_id = $1 AND status = 'replicated' AND upload_enqueued_at IS NULL \
             ORDER BY updated_at LIMIT $2",
        )
        .bind(self.node_id.as_deref())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(PartRow::into_part).collect()
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

    async fn mark_resident(&self, part: &PartKey, bytes: u64) -> Result<()> {
        self.record_resident(part, bytes).await
    }

    async fn mark_replicated(&self, claim: &ClaimedPart, _proof: &PartVerified, digest: &PartDigest) -> Result<()> {
        // Guard on `draining` AND the claim's fencing token: only the agent that still
        // holds THIS claim may commit. Zero rows means the claim was lost — either the
        // row left `draining`, or it was re-claimed (a new claim_seq) after the lease,
        // e.g. this agent stalled past the lease and another re-won the part. Either
        // way the caller must NOT unlink the SSD copy — surface PartClaimLost, not a
        // false Ok. The claim_seq is what distinguishes "I still hold it" from "someone
        // re-won it and it's draining again under them" (F4).
        let part = claim.part();
        // Reset corrupt_attempts on a genuine commit: the counter bounds re-drives for ONE
        // corruption episode, so a part that recovered (corrupt→pending→replicated) must not
        // carry a spent budget if the same row is ever corrupted again. Harmless on the common
        // path (it is already 0).
        // content_sha256 is written HERE, not by a follow-up statement: a part that reads
        // `replicated` with a missing digest is unverifiable, and the re-landing check treats
        // unverifiable as diverged — so a two-statement commit would leave a crash window whose
        // recovery is a needless full re-copy of the part (B-2).
        let affected = sqlx::query(
            "UPDATE cephor_replication_status \
             SET status = 'replicated', corrupt_attempts = 0, content_sha256 = $5, updated_at = now() \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining' AND claim_seq = $4",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(claim.claim_seq())
        .bind(digest.as_str())
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

    async fn mark_upload_enqueued(&self, part: &PartKey) -> Result<()> {
        // Stamp the backend-enqueue completion. Guarded on `status = 'replicated'` (never a
        // claim, since the enqueue sweep holds none): only a Ceph-durable part can have its
        // upload published. Idempotent — a re-stamp (inline enqueue racing the sweep, or a
        // re-drive) just re-writes now(). Zero rows (the part left `replicated`, e.g. a reaper
        // flipped it `failed`) is a harmless no-op: there is nothing to publish for it anymore.
        sqlx::query(
            "UPDATE cephor_replication_status SET upload_enqueued_at = now() \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'replicated'",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .execute(&self.pool)
        .await?;
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

    async fn mark_corrupt(&self, claim: &ClaimedPart, _reason: &str) -> Result<()> {
        // Same claim-fence + idempotency shape as mark_failed (a fenced stale claimant matches
        // zero rows and must NOT touch the live re-claimed part; a second call finds 'corrupt',
        // not 'draining'). Clears claimed_at like mark_failed. The corrupt_attempts counter is
        // left as-is: a fresh corruption reuses whatever re-drive budget the row already spent.
        let part = claim.part();
        sqlx::query(
            "UPDATE cephor_replication_status SET status = 'corrupt', updated_at = now(), claimed_at = NULL \
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

    async fn is_version_servable(&self, part: &PartKey) -> Result<bool> {
        // The inverse of janitor_part_terminally_abandoned.sql's unservable predicate for the
        // servable disjuncts (address set / size>0 / md5 set), shared with the reclaim gate's
        // servable_parts: a version SERVES a GET if its address is set, OR it has a real size,
        // OR an md5. address is written AFTER size/md5 in a separate step, so a fully-servable
        // version briefly has address=NULL (the mid-finalize window); the size/md5 disjuncts
        // keep such a live version from reading as unservable. Do NOT reduce to address-only.
        // A missing row (deleted object) is not servable. NB: not bit-identical to the janitor
        // under a NULL size_bytes — a bare all-NULL row is unservable here (`size_bytes > 0` is
        // NULL -> not servable) yet the janitor's `size_bytes <= 0` is also NULL so it does not
        // sweep it; both are fail-safe (a bare-NULL row can serve no GET).
        let servable = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS ( \
                SELECT 1 FROM object_versions ov \
                WHERE ov.object_id = $1::uuid AND ov.object_version = $2 \
                  AND (ov.address IS NOT NULL OR ov.size_bytes > 0 OR COALESCE(ov.md5_hash, '') <> '') \
             )",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .fetch_one(&self.pool)
        .await?;
        Ok(servable)
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

    async fn statuses(&self, parts: &[PartKey]) -> Result<HashMap<PartKey, PartStatus>> {
        // The batched status read: one query per ~500 parts (chunked so a pathological
        // backlog never builds one giant IN-list), matching by PK tuple via UNNEST'd
        // parallel arrays. Selects the same (status, adoptability) as `status`. A part with
        // no row simply does not come back, so the caller treats it as absent.
        let mut out = HashMap::with_capacity(parts.len());
        for batch in parts.chunks(RECLAIM_STATUS_BATCH) {
            let mut object_ids: Vec<&str> = Vec::with_capacity(batch.len());
            let mut versions: Vec<i64> = Vec::with_capacity(batch.len());
            let mut part_numbers: Vec<i64> = Vec::with_capacity(batch.len());
            for part in batch {
                object_ids.push(part.object().as_str());
                versions.push(i64::from(part.version().get()));
                part_numbers.push(i64::from(part.part().get()));
            }
            let rows = sqlx::query_as::<_, (String, i64, i64, String, bool)>(
                "SELECT object_id, version, part_number, status, (status = 'pending' AND node_id IS NULL) \
                 FROM cephor_replication_status \
                 WHERE (object_id, version, part_number) IN \
                       (SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::bigint[]))",
            )
            .bind(&object_ids)
            .bind(&versions)
            .bind(&part_numbers)
            .fetch_all(&self.pool)
            .await?;
            for (object_id, version, part_number, status, adoptable) in rows {
                let part = PartRow {
                    object_id,
                    version,
                    part_number,
                }
                .into_part()?;
                out.insert(
                    part,
                    PartStatus {
                        state: state_from_db(&status)?,
                        adoptable,
                    },
                );
            }
        }
        Ok(out)
    }

    async fn record_landed(&self, part: &PartKey) -> Result<()> {
        // The reconciler only records parts it found with no row or an adoptable one, so the
        // prior-state report has nothing to tell it — the divergence check belongs to the
        // announcement path, which is the only one that observes a rewrite (see LandedOutcome).
        Store::record_landed_part(self, part).await.map(|_| ())
    }
}

/// The reclaim worker's batched view of the store: read the replication state + age
/// of many parts in ONE round-trip, so the per-cycle SSD scan never fans out into a
/// per-part SELECT (the reconciler's O(backlog) cost the worker must not repeat).
impl ResidentLog for Store {
    type Error = StoreError;

    async fn evictable_parts(&self, limit: u32) -> Result<Vec<ResidentPart>> {
        // Node-scoped: residency is per (node, part), and a node can only unlink what is on
        // its own disk. An agent with no node id (the allocator) evicts nothing.
        let Some(node) = self.node_id.as_deref() else {
            return Ok(Vec::new());
        };
        // The join to cephor_replication_status carries the status guard, and it is
        // load-bearing rather than redundant with residency: the two are INDEPENDENT axes.
        // `redrive_corrupt_parts` resets a corrupt part to `pending` for a fresh copy without
        // touching its residency (correctly — the part is still on the disk), so a re-driven
        // part is resident-and-pending, and its SSD copy is once again the only durable one.
        // Selecting on residency alone would offer it to the evictor. The orchestrator refuses
        // it regardless, but a worklist that never emits it is the real guard.
        //
        // INNER JOIN: a residency row with no replication row means the object was hard-deleted,
        // which is the reclaimer's disposition (deleted-object orphan), not the evictor's. It is
        // excluded here and `prune_residency` clears the row when the reclaimer unlinks the part.
        //
        // THIS ORDER BY IS THE EVICTION POLICY; the orchestrator walks it and never re-sorts.
        //
        // Least-recently-USED, not least-recently-admitted. `resident_at` says when a part
        // joined the cache, which says nothing about whether anyone is still reading it — that
        // is FIFO, and FIFO is a poor fit here: a training set re-read every epoch is maximally
        // skewed, so evicting by arrival order tends to take exactly the parts about to be read
        // again, each costing a peer-or-pool read plus a local write to restore.
        //
        // COALESCE gives the fallback for free: a part nobody has read since 0017 shipped has no
        // recency, and using its residency time is precisely the old behaviour. No backfill, and
        // no window where the order is undefined.
        //
        // Must match `cephor_ssd_residency_recency_idx` expression-for-expression, or the
        // planner sorts the whole ~2M-row resident set on every pass — reintroducing in Postgres
        // the O(resident) cost this evictor exists to avoid.
        //
        // The relanded_at guard is the B-2 eviction gate. A committed part that was rewritten on
        // SSD still reads 'replicated' and its residency recency predates the rewrite, so it
        // ranks as maximally COLD here while its SSD copy is the ONLY copy of the client's new
        // bytes — the pool holds the superseded ones. Excluding rows re-landed within the grace
        // keeps the evictor off the part until the divergence check has disposed of it: a
        // Diverged verdict flips it to 'pending' (the status join then excludes it), and an
        // Unchanged verdict means the pool copy matches, at which point eviction is safe again
        // the moment the grace lapses. Filtered per joined row, like the status guard, so it
        // costs nothing on the recency index scan.
        let rows = sqlx::query_as::<_, (String, i64, i64, String, i64)>(
            "SELECT r.object_id, r.version, r.part_number, s.status, r.bytes \
             FROM cephor_ssd_residency r \
             JOIN cephor_replication_status s \
               ON s.object_id = r.object_id AND s.version = r.version AND s.part_number = r.part_number \
             WHERE r.node_id = $1 AND s.status = 'replicated' \
               AND (s.relanded_at IS NULL OR s.relanded_at < now() - ($3 * interval '1 second')) \
             ORDER BY COALESCE(r.last_read_at, r.resident_at) \
             LIMIT $2",
        )
        .bind(node)
        .bind(i64::from(limit))
        .bind(RELAND_EVICTION_GRACE.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for (object_id, version, part_number, status, bytes) in rows {
            let part = PartRow {
                object_id,
                version,
                part_number,
            }
            .into_part()?;
            out.push(ResidentPart {
                part,
                bytes: u64::try_from(bytes).unwrap_or(0),
                state: state_from_db(&status)?,
            });
        }
        Ok(out)
    }

    async fn mark_evicted(&self, parts: &[PartKey]) -> Result<()> {
        self.drop_residency(parts).await
    }
}

impl Store {
    /// This node's `failed` parts older than `grace`, oldest first, at most `limit`.
    ///
    /// Node-scoped: a part lives only on the SSD of the node that ingested it, so another node's
    /// `failed` row names a file this agent cannot unlink. Without a node id (the allocator)
    /// there is nothing to reclaim.
    ///
    /// **Candidates only.** Servability — "is this the last good copy of a live object?" — is
    /// deliberately absent: [`BackingLog::servable_parts`](crate::BackingLog::servable_parts)
    /// owns that predicate, and it already carries a MUST-stay-in-lockstep warning shared with
    /// `janitor_part_terminally_abandoned.sql` and the A21 sweep. A third copy here is how such
    /// a warning gets quietly broken, and the failure mode is deleting a live object's last good
    /// source. [`reclaim_failed`](crate::reclaim_failed) applies the guard to what this returns.
    ///
    /// `updated_at` is the store clock, so the grace has no agent-clock dependence — the same
    /// property the walk-driven path relied on. Oldest first so a backlog drains in age order
    /// rather than starving its own tail.
    ///
    /// # Errors
    ///
    /// [`StoreError`] if the query fails.
    pub async fn reclaimable_failed_parts_impl(&self, grace: Duration, limit: u32) -> Result<Vec<PartKey>> {
        let Some(node) = self.node_id.as_deref() else {
            return Ok(Vec::new());
        };
        let rows = sqlx::query_as::<_, (String, i64, i64)>(
            "SELECT object_id, version, part_number \
             FROM cephor_replication_status \
             WHERE node_id = $1 AND status = 'failed' AND reclaimed_at IS NULL \
               AND updated_at < now() - make_interval(secs => $2) \
             ORDER BY updated_at \
             LIMIT $3",
        )
        .bind(node)
        .bind(grace.as_secs_f64())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|(object_id, version, part_number)| {
                PartRow {
                    object_id,
                    version,
                    part_number,
                }
                .into_part()
            })
            .collect()
    }

    /// Records that `part` is now resident on this node's SSD, with its size for the eviction
    /// cursor's accounting.
    ///
    /// Called by the drain just BEFORE it commits `replicated` (it retains its copy — see
    /// [`PartReplicationStore::mark_resident`](crate::PartReplicationStore::mark_resident) for why
    /// that order and not the reverse), and by the api after a read-through promotion copies a
    /// part onto a node that did not ingest it. Idempotent on re-drive: a part that goes
    /// `corrupt → pending → replicated` re-asserts residency it never actually lost.
    ///
    /// # A second process writes this row, with the opposite conflict action
    ///
    /// `ResidencyRecorder` in `hippius_s3/cache/residency.py` upserts the same primary key with
    /// `DO UPDATE SET bytes = cephor_ssd_residency.bytes + EXCLUDED.bytes` — it ACCUMULATES, where
    /// this OVERWRITES. Neither is wrong on its own: the drain knows the whole part's size at
    /// commit and states it, while promotion learns the part one chunk at a time and has to add.
    ///
    /// They do not collide on a live (node, part) because the drain records only on its own
    /// commit; a part resident on this node is served from this node and therefore never promoted
    /// here; and eviction DELETEs the row and unlinks the directory together, resetting both
    /// writers to the same empty starting point.
    ///
    /// **Nothing enforces that.** No constraint, no trigger, no shared code path holds the two
    /// semantics apart — it is an argument about who writes when, and it is only as true as the
    /// read path. The shape that breaks it is a PARTIALLY promoted part: locality is decided per
    /// CHUNK (a range GET promotes only the chunks it touches) while residency is keyed per PART,
    /// so a node can hold some chunks of a part and still miss the rest. If that node later
    /// drain-commits the same part, one writer's figure replaces or is added to the other's. The
    /// damage is a wrong `bytes`, which is what the evictor sums against its deficit — so the
    /// symptom is a pass that stops short or walks too far, reporting success either way.
    ///
    /// # Errors
    ///
    /// [`StoreError`] if the write fails.
    pub async fn record_resident(&self, part: &PartKey, bytes: u64) -> Result<()> {
        let Some(node) = self.node_id.as_deref() else {
            return Ok(());
        };
        sqlx::query(
            "INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, bytes) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (node_id, object_id, version, part_number) \
             DO UPDATE SET bytes = EXCLUDED.bytes",
        )
        .bind(node)
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .bind(i64::try_from(bytes).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Drops this node's residency rows for `parts` — the counterpart to
    /// [`record_resident`](Self::record_resident), called by the evictor after it unlinks.
    ///
    /// The reclaim worker deliberately does NOT call this, because the parts it removes never
    /// had a residency row: residency is recorded only at the drain's commit, and everything
    /// the reclaimer removes is either terminal (`failed`) or has no replication row at all
    /// (a deleted-object orphan). The one loose end is a part that was resident and then had
    /// its object hard-deleted: its replication row goes, the reclaimer frees the disk space,
    /// and the residency row is left behind — but both `evictable_parts` and `node_cache_bytes`
    /// INNER JOIN the replication row, so it is invisible to each and drifts no signal. It is a
    /// dead row bounded by the hard-delete rate, not a leak of disk or of accounting.
    ///
    /// # Errors
    ///
    /// [`StoreError`] if the delete fails.
    pub async fn drop_residency(&self, parts: &[PartKey]) -> Result<()> {
        let Some(node) = self.node_id.as_deref() else {
            return Ok(());
        };
        if parts.is_empty() {
            return Ok(());
        }
        let mut object_ids: Vec<&str> = Vec::with_capacity(parts.len());
        let mut versions: Vec<i64> = Vec::with_capacity(parts.len());
        let mut part_numbers: Vec<i64> = Vec::with_capacity(parts.len());
        for part in parts {
            object_ids.push(part.object().as_str());
            versions.push(i64::from(part.version().get()));
            part_numbers.push(i64::from(part.part().get()));
        }
        sqlx::query(
            "DELETE FROM cephor_ssd_residency \
             WHERE node_id = $1 \
               AND (object_id, version, part_number) IN \
                   (SELECT * FROM UNNEST($2::text[], $3::bigint[], $4::bigint[]))",
        )
        .bind(node)
        .bind(&object_ids)
        .bind(&versions)
        .bind(&part_numbers)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl ReclaimLog for Store {
    type Error = StoreError;

    async fn reclaimable_failed_parts(&self, grace: Duration, limit: u32) -> Result<Vec<PartKey>> {
        self.reclaimable_failed_parts_impl(grace, limit).await
    }

    async fn mark_failed_reclaimed(&self, parts: &[PartKey]) -> Result<()> {
        let Some(node) = self.node_id.as_deref() else {
            return Ok(());
        };
        if parts.is_empty() {
            return Ok(());
        }
        let mut object_ids: Vec<&str> = Vec::with_capacity(parts.len());
        let mut versions: Vec<i64> = Vec::with_capacity(parts.len());
        let mut part_numbers: Vec<i64> = Vec::with_capacity(parts.len());
        for part in parts {
            object_ids.push(part.object().as_str());
            versions.push(i64::from(part.version().get()));
            part_numbers.push(i64::from(part.part().get()));
        }
        // Batched: a per-part UPDATE would put a round-trip in the reclaim inner loop. Node
        // scoped like every other write here — a peer's row names a file this agent never
        // touched, so marking it would claim work it did not do.
        sqlx::query(
            "UPDATE cephor_replication_status SET reclaimed_at = now() \
             WHERE node_id = $1 \
               AND (object_id, version, part_number) IN \
                   (SELECT * FROM UNNEST($2::text[], $3::bigint[], $4::bigint[]))",
        )
        .bind(node)
        .bind(&object_ids)
        .bind(&versions)
        .bind(&part_numbers)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

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

/// The reclaim worker's object-backing view: which scanned parts have no live
/// `object_versions` row. Only the parts with no replication row reach this, so the batch
/// is the reclaim's `skipped_absent` tail — usually small, empty on the steady state.
impl BackingLog for Store {
    type Error = StoreError;

    async fn unbacked_parts(&self, parts: &[PartKey]) -> Result<HashSet<PartKey>> {
        let mut out = HashSet::with_capacity(parts.len());
        // Chunk the request so a pathological backlog never builds one giant array.
        for batch in parts.chunks(RECLAIM_STATUS_BATCH) {
            let mut object_ids: Vec<&str> = Vec::with_capacity(batch.len());
            let mut versions: Vec<i64> = Vec::with_capacity(batch.len());
            let mut part_numbers: Vec<i64> = Vec::with_capacity(batch.len());
            for part in batch {
                object_ids.push(part.object().as_str());
                versions.push(i64::from(part.version().get()));
                part_numbers.push(i64::from(part.part().get()));
            }
            // Echo back exactly the input parts whose (object_id, version) has NO
            // object_versions row. NOT EXISTS against the ov PK stays index-only; the
            // object_id is echoed from the input UNNEST (never read from object_versions),
            // so the reconstructed PartKey matches the caller's key verbatim — no
            // canonical-text round-trip to get wrong. Backing is ROW PRESENCE only: a
            // present-but-unservable row (an aborted/abandoned or in-flight MPU) counts as
            // backed and is NOT returned, so this never races the central `failed` sweep.
            let rows = sqlx::query_as::<_, (String, i64, i64)>(
                "SELECT t.object_id, t.version, t.part_number \
                 FROM UNNEST($1::text[], $2::bigint[], $3::bigint[]) AS t(object_id, version, part_number) \
                 WHERE NOT EXISTS ( \
                    SELECT 1 FROM object_versions ov \
                    WHERE ov.object_id = t.object_id::uuid AND ov.object_version = t.version \
                 )",
            )
            .bind(&object_ids)
            .bind(&versions)
            .bind(&part_numbers)
            .fetch_all(&self.pool)
            .await?;
            for (object_id, version, part_number) in rows {
                out.insert(
                    PartRow {
                        object_id,
                        version,
                        part_number,
                    }
                    .into_part()?,
                );
            }
        }
        Ok(out)
    }

    async fn servable_parts(&self, parts: &[PartKey]) -> Result<HashSet<PartKey>> {
        let mut out = HashSet::with_capacity(parts.len());
        // Chunk the request so a pathological backlog never builds one giant array.
        for batch in parts.chunks(RECLAIM_STATUS_BATCH) {
            let mut object_ids: Vec<&str> = Vec::with_capacity(batch.len());
            let mut versions: Vec<i64> = Vec::with_capacity(batch.len());
            let mut part_numbers: Vec<i64> = Vec::with_capacity(batch.len());
            for part in batch {
                object_ids.push(part.object().as_str());
                versions.push(i64::from(part.version().get()));
                part_numbers.push(i64::from(part.part().get()));
            }
            // Echo back exactly the input parts whose (object_id, version) row EXISTS and is
            // SERVABLE — the inverse of janitor_part_terminally_abandoned.sql's unservable
            // predicate for the servable disjuncts. MUST stay in lockstep with that file (and
            // the A21 sweep's list_orphan_replication_versions.sql): servable = address
            // written, OR a real size, OR an md5 — the download filter `(size_bytes > 0 OR
            // md5_hash <> '')` plus a set address. `address` is written AFTER size/md5 in a
            // separate step, so a fully-servable version briefly has address=NULL (the
            // mid-finalize window); the size/md5 disjuncts are what keep such a live version
            // from being read as reclaimable. Do NOT "simplify" to address-only. The two
            // predicates are NOT bit-identical under a NULL size_bytes: a bare all-NULL row is
            // unservable here (so reclaimable) while the janitor's `size_bytes <= 0` is NULL so
            // it never sweeps it — both fail safe, since a bare-NULL row can serve no GET. The
            // object_id is echoed from the input UNNEST so the reconstructed PartKey matches
            // the caller's key verbatim.
            let rows = sqlx::query_as::<_, (String, i64, i64)>(
                "SELECT t.object_id, t.version, t.part_number \
                 FROM UNNEST($1::text[], $2::bigint[], $3::bigint[]) AS t(object_id, version, part_number) \
                 WHERE EXISTS ( \
                    SELECT 1 FROM object_versions ov \
                    WHERE ov.object_id = t.object_id::uuid AND ov.object_version = t.version \
                      AND (ov.address IS NOT NULL OR ov.size_bytes > 0 OR COALESCE(ov.md5_hash, '') <> '') \
                 )",
            )
            .bind(&object_ids)
            .bind(&versions)
            .bind(&part_numbers)
            .fetch_all(&self.pool)
            .await?;
            for (object_id, version, part_number) in rows {
                out.insert(
                    PartRow {
                        object_id,
                        version,
                        part_number,
                    }
                    .into_part()?,
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
    use super::{MissingSourceOutcome, Store, StoreError};
    use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
    use crate::partdrain::{ClaimedPart, PartReplicationStore, PartVerified};
    use crate::redrive::PartDigest;
    use crate::ssd_reclaim::ReclaimLog;
    use crate::state::ReplicationState;
    use core::str::FromStr;
    use sqlx::postgres::PgPool;
    use std::collections::HashSet;
    use std::time::Duration;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";
    const UUID_B: &str = "00000000-0000-4000-8000-000000000000";

    /// A stand-in content digest for commits whose test is not about divergence detection.
    fn test_digest() -> PartDigest {
        crate::redrive::part_digest(&["chunk-0-hash"])
    }

    fn part(uuid: &str, version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(uuid).unwrap(), Version::new(version), PartNumber::new(number))
    }

    /// Forces a part's row to a terminal status with a backdated `updated_at`, so the
    /// reclaim age reads deterministically older than any plausible grace.
    #[sqlx::test]
    async fn gc_terminal_status_rows_prunes_only_aged_terminal_rows(pool: PgPool) {
        // WI-17 + Tier-2: prune aged `failed` rows and aged `replicated` rows whose backend
        // upload was ALREADY enqueued (upload_enqueued_at set). Keep: a FRESH terminal row, any
        // pending/draining (live) row, AND — the Tier-2 guard — an aged `replicated` row still
        // awaiting its enqueue (upload_enqueued_at NULL), the enqueue sweep's worklist: pruning
        // it would drop the backend upload.
        let store = Store::from_pool(pool.clone());
        let fresh = part(UUID_A, 5, 1); // replicated + enqueued, young -> kept (age gate)
        let old_replicated_enqueued = part(UUID_A, 5, 2); // aged + enqueued -> pruned
        let old_replicated_unenqueued = part(UUID_A, 5, 5); // aged, NOT enqueued -> spared (worklist)
        let old_failed = part(UUID_A, 5, 3); // aged failed -> pruned
        let pending = part(UUID_A, 5, 4); // live -> kept
        for p in [&fresh, &old_replicated_enqueued, &old_replicated_unenqueued, &old_failed, &pending] {
            store.record_landed_part(p).await.unwrap();
        }
        // Fresh: replicated + enqueued but updated_at ~now, so it is younger than the retention —
        // isolating the age gate from the enqueue guard.
        sqlx::query(
            "UPDATE cephor_replication_status SET status = 'replicated', upload_enqueued_at = now() \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(fresh.object().as_str())
        .bind(i64::from(fresh.version().get()))
        .bind(i64::from(fresh.part().get()))
        .execute(&pool)
        .await
        .unwrap();
        force_terminal(&pool, &old_replicated_enqueued, "replicated").await; // backdated 2h
        // mark_upload_enqueued leaves updated_at untouched, so the row stays aged AND enqueued.
        store.mark_upload_enqueued(&old_replicated_enqueued).await.unwrap();
        force_terminal(&pool, &old_replicated_unenqueued, "replicated").await; // backdated 2h, left unstamped
        force_terminal(&pool, &old_failed, "failed").await; // backdated 2h

        let pruned = store.gc_terminal_status_rows(Duration::from_hours(1)).await.unwrap();

        assert_eq!(pruned, 2, "only the aged failed + aged enqueued-replicated rows are pruned");
        assert_eq!(
            store.status(&fresh).await.unwrap(),
            Some(ReplicationState::Replicated),
            "a fresh terminal row is kept"
        );
        assert_eq!(
            store.status(&old_replicated_enqueued).await.unwrap(),
            None,
            "the aged, already-enqueued replicated row was pruned"
        );
        assert_eq!(
            store.status(&old_replicated_unenqueued).await.unwrap(),
            Some(ReplicationState::Replicated),
            "an aged replicated row still awaiting its enqueue is SPARED (the sweep worklist)"
        );
        assert_eq!(store.status(&old_failed).await.unwrap(), None, "the aged failed row was pruned");
        assert_eq!(
            store.status(&pending).await.unwrap(),
            Some(ReplicationState::Pending),
            "a live pending row is never pruned"
        );
    }

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
    async fn statuses_batches_state_and_adoptability_matching_per_part_status(pool: PgPool) {
        // WI-13: the batched reconciler read returns the same (state, adoptability) as the
        // per-part status() for known parts, and omits a part with no row.
        let store = Store::from_pool(pool.clone());
        let pending = part(UUID_A, 5, 1);
        let replicated = part(UUID_A, 5, 2);
        let absent = part(UUID_B, 7, 1);

        store.record_landed_part(&pending).await.unwrap();
        store.record_landed_part(&replicated).await.unwrap();
        force_terminal(&pool, &replicated, "replicated").await;

        let batched = <Store as crate::reconcile::PartLandingLog>::statuses(&store, &[pending.clone(), replicated.clone(), absent.clone()])
            .await
            .unwrap();

        assert_eq!(batched.len(), 2, "the part with no row is omitted (treated as absent)");
        assert!(!batched.contains_key(&absent));
        assert_eq!(batched.get(&pending).expect("pending present").state, ReplicationState::Pending);
        assert_eq!(batched.get(&replicated).expect("replicated present").state, ReplicationState::Replicated);
        // The batched result agrees with the single-part path it replaces.
        let single = <Store as crate::reconcile::PartLandingLog>::status(&store, &pending)
            .await
            .unwrap()
            .expect("pending present");
        assert_eq!(
            batched.get(&pending).unwrap().adoptable,
            single.adoptable,
            "batched adoptability matches per-part status()",
        );
    }

    #[sqlx::test]
    async fn part_states_of_an_empty_request_is_empty(pool: PgPool) {
        let store = Store::from_pool(pool);
        let states = <Store as ReclaimLog>::part_states(&store, &[]).await.unwrap();
        assert!(states.is_empty(), "no parts requested -> no query, empty map");
    }

    #[sqlx::test]
    async fn unbacked_parts_returns_only_parts_whose_object_version_row_is_gone(pool: PgPool) {
        use crate::ssd_reclaim::BackingLog;
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone());

        // A live object_versions row exists — the part is BACKED (mid-upload / live object).
        let backed = part(UUID_A, 1, 1);
        seed_object_version(&pool, UUID_A, 1, Some("5Faddr")).await;
        // No object_versions row at all — the object was hard-deleted → UNBACKED.
        let deleted = part(UUID_B, 2, 1);

        let unbacked = store.unbacked_parts(&[backed, deleted.clone()]).await.unwrap();
        assert_eq!(unbacked, HashSet::from([deleted]), "only the deleted object's part is unbacked");
    }

    #[sqlx::test]
    async fn a_present_but_unservable_version_is_still_backed(pool: PgPool) {
        // The load-bearing distinction from the WI-20a servability sweep: backing is ROW
        // PRESENCE, not servability. An aborted/abandoned or in-flight MPU has a present
        // object_versions row (address NULL, size 0) — it must NOT be reported unbacked,
        // so the orphan reclaim never races the central `failed` path (or an in-flight MPU).
        use crate::ssd_reclaim::BackingLog;
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone());

        seed_object_version(&pool, UUID_A, 1, None).await; // present but address NULL (unservable)

        let unbacked = store.unbacked_parts(&[part(UUID_A, 1, 1)]).await.unwrap();
        assert!(unbacked.is_empty(), "a present-but-unservable version is backed, never orphan-reclaimed");
    }

    #[sqlx::test]
    async fn every_part_of_a_deleted_version_is_unbacked_and_a_sibling_version_is_isolated(pool: PgPool) {
        // All parts of a deleted version are unbacked; a live sibling version on the same
        // object is untouched (version-scoped, so deleting v2's SSD cannot strand v1).
        use crate::ssd_reclaim::BackingLog;
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone());

        seed_object_version(&pool, UUID_A, 1, Some("5Flive")).await; // v1 live
        let v1 = part(UUID_A, 1, 1);
        // v2 has no object_versions row (deleted) with two parts.
        let v2_p1 = part(UUID_A, 2, 1);
        let v2_p2 = part(UUID_A, 2, 2);

        let unbacked = store.unbacked_parts(&[v1, v2_p1.clone(), v2_p2.clone()]).await.unwrap();
        assert_eq!(unbacked, HashSet::from([v2_p1, v2_p2]), "both v2 parts unbacked; live v1 protected");
    }

    #[sqlx::test]
    async fn unbacked_parts_of_an_empty_request_is_empty(pool: PgPool) {
        use crate::ssd_reclaim::BackingLog;
        let store = Store::from_pool(pool);
        let unbacked = store.unbacked_parts(&[]).await.unwrap();
        assert!(unbacked.is_empty());
    }

    #[sqlx::test]
    async fn servable_parts_returns_each_servable_row_and_excludes_unservable_and_missing(pool: PgPool) {
        // The corrupt-live guard's discriminator, exercising every disjunct of the servable
        // predicate (address set / size>0 / md5 set) AND its two exclusions (unservable row,
        // no row). Each disjunct is checked in isolation because `address` is written AFTER
        // size/md5 in a separate step, so the size/md5-only rows are real mid-finalize states
        // a servable object passes through — dropping them would strand a live GET.
        use crate::ssd_reclaim::BackingLog;
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone());

        // address set, size/md5 unset → servable.
        seed_ov(&pool, UUID_A, 1, Some("5Faddr"), None, None).await;
        // address NULL but size>0 → servable (the mid-finalize window: size written, address not yet).
        seed_ov(&pool, UUID_A, 2, None, Some(4096), None).await;
        // address NULL, size 0, md5 set → servable (md5 alone satisfies the download filter).
        seed_ov(&pool, UUID_A, 3, None, Some(0), Some("d41d8cd9")).await;
        // address NULL, size 0, md5 '' → UNSERVABLE (the abandoned-upload shape).
        seed_ov(&pool, UUID_A, 4, None, Some(0), Some("")).await;
        // address NULL, size NULL, md5 NULL → UNSERVABLE here, so RECLAIMABLE (bare reserved
        // row). This is the one shape where reclaim and the janitor diverge: `size_bytes > 0`
        // is NULL -> not servable -> reclaimable here, while the janitor's `size_bytes <= 0` is
        // also NULL so it never sweeps it. Both fail safe (a bare-NULL row can serve no GET);
        // this case pins that intentional, non-bit-identical behavior.
        seed_ov(&pool, UUID_A, 5, None, None, None).await;
        // no object_versions row at all (v6) → UNSERVABLE (deleted object).

        let addr = part(UUID_A, 1, 1);
        let sized = part(UUID_A, 2, 1);
        let md5 = part(UUID_A, 3, 1);
        let empty = part(UUID_A, 4, 1);
        let bare = part(UUID_A, 5, 1);
        let missing = part(UUID_A, 6, 1);

        let servable = store
            .servable_parts(&[addr.clone(), sized.clone(), md5.clone(), empty, bare, missing])
            .await
            .unwrap();
        assert_eq!(
            servable,
            HashSet::from([addr, sized, md5]),
            "each servable disjunct is returned; unservable and missing rows are excluded"
        );
    }

    #[sqlx::test]
    async fn servable_parts_of_an_empty_request_is_empty(pool: PgPool) {
        use crate::ssd_reclaim::BackingLog;
        let store = Store::from_pool(pool);
        assert!(store.servable_parts(&[]).await.unwrap().is_empty());
    }

    #[sqlx::test]
    async fn is_version_servable_is_the_drain_time_corrupt_discriminator(pool: PgPool) {
        // The R4 mark-path discriminator: same predicate as servable_parts, one part at a time.
        // A servable version (any disjunct) -> Corrupt; an unservable/missing one -> Failed.
        use crate::partdrain::PartReplicationStore;
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone());
        seed_ov(&pool, UUID_A, 1, Some("5Faddr"), None, None).await; // address set -> servable
        seed_ov(&pool, UUID_A, 2, None, Some(4096), None).await; // size>0 -> servable (mid-finalize)
        seed_ov(&pool, UUID_A, 3, None, Some(0), Some("")).await; // abandoned-upload -> unservable
        // no row for v4 -> unservable (deleted object)

        assert!(store.is_version_servable(&part(UUID_A, 1, 1)).await.unwrap(), "address set is servable");
        assert!(store.is_version_servable(&part(UUID_A, 2, 1)).await.unwrap(), "a real size is servable");
        assert!(
            !store.is_version_servable(&part(UUID_A, 3, 1)).await.unwrap(),
            "the abandoned-upload shape is not"
        );
        assert!(
            !store.is_version_servable(&part(UUID_A, 4, 1)).await.unwrap(),
            "a missing row is not servable"
        );
    }

    /// Inserts one `object_versions` row with explicit servability columns (address, size,
    /// md5). Used by the `servable_parts` predicate tests where each disjunct matters.
    async fn seed_ov(pool: &PgPool, object: &str, version: i64, address: Option<&str>, size_bytes: Option<i64>, md5: Option<&str>) {
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
        sqlx::query("INSERT INTO object_versions (object_id, object_version, address, size_bytes, md5_hash) VALUES ($1::uuid, $2, $3, $4, $5)")
            .bind(object)
            .bind(version)
            .bind(address)
            .bind(size_bytes)
            .bind(md5)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Inserts one `cephor_replication_status` row with an explicit node + status.
    async fn seed_status_node(pool: &PgPool, object: &str, version: i64, number: i64, status: &str, node: &str) {
        sqlx::query(
            "INSERT INTO cephor_replication_status (object_id, version, part_number, status, node_id) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(object)
        .bind(version)
        .bind(number)
        .bind(status)
        .bind(node)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Inserts one `parts` row with an explicit `size_bytes`.
    async fn seed_part_size(pool: &PgPool, object: &str, version: i64, number: i64, size_bytes: Option<i64>) {
        sqlx::query("INSERT INTO parts (object_id, object_version, part_number, size_bytes) VALUES ($1::uuid, $2, $3, $4)")
            .bind(object)
            .bind(version)
            .bind(number)
            .bind(size_bytes)
            .execute(pool)
            .await
            .unwrap();
    }

    // ------------------------------------------------------ SSD residency (Phase 2 retention)
    use crate::ssd_evict::ResidentLog;

    #[sqlx::test]
    async fn committing_a_replication_marks_the_part_resident_on_the_ssd(pool: PgPool) {
        // The drain now KEEPS its SSD copy after replicating, so the commit is exactly when a
        // part joins the read tier. Residency is a SEPARATE statement from the commit — the
        // commit writes only `status`/`corrupt_attempts`/`updated_at` — so what this asserts is
        // the pair's end state: committed AND accounted, i.e. on the evictor's worklist and in
        // the heartbeat's cache_bytes. `drain_part` is what orders the two, and it records
        // residency FIRST so no crash can leave a committed part unaccounted.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);
        seed_part_size(&pool, UUID_A, 5, 1, Some(4096)).await;
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();

        store.mark_replicated(&claimed, &PartVerified::for_test(), &test_digest()).await.unwrap();

        store.record_resident(&p, 4096).await.unwrap();
        assert_eq!(store.node_cache_bytes("node-a").await.unwrap(), 4096, "the retained part is cache");
        let worklist = store.evictable_parts(10).await.unwrap();
        assert_eq!(worklist.len(), 1);
        assert_eq!(worklist[0].part, p);
        assert_eq!(worklist[0].state, ReplicationState::Replicated);
    }

    #[sqlx::test]
    async fn a_legacy_replicated_row_is_not_resident_and_needs_no_backfill(pool: PgPool) {
        // The migration's load-bearing property. Prod carries ~11M 'replicated' rows whose SSD
        // copies were unlinked long before retention existed. Keyed on a positive resident_at
        // marker they are simply not resident: the evictor never chases parts that are gone,
        // and cache_bytes does not report a phantom multi-terabyte cache.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        seed_status_node(&pool, UUID_A, 1, 1, "replicated", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 1, Some(999_999)).await;

        assert_eq!(store.node_cache_bytes("node-a").await.unwrap(), 0, "a pre-retention row is not cache");
        assert!(store.evictable_parts(10).await.unwrap().is_empty());
    }

    // ------------------------------------------------------------- B-2 re-landing divergence

    #[sqlx::test]
    async fn recording_a_landing_reports_the_prior_state_and_the_digest_committed_for_it(pool: PgPool) {
        // The announcement path's whole input. A first landing has nothing to report; a landing
        // for a committed part must hand back the state AND the digest, or the divergence check
        // has to make a second round trip on the hot discovery path to learn them.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);

        let first = store.record_landed_part(&p).await.unwrap();
        assert_eq!(first.state, ReplicationState::Pending, "a fresh part records as pending");
        assert_eq!(first.digest, None, "nothing has been committed for it yet");

        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_replicated(&claimed, &PartVerified::for_test(), &test_digest()).await.unwrap();

        let second = store.record_landed_part(&p).await.unwrap();
        assert_eq!(second.state, ReplicationState::Replicated, "the upsert never touches status");
        assert_eq!(second.digest, Some(test_digest()), "the commit's digest comes back for comparison");
    }

    #[sqlx::test]
    async fn a_relanding_whose_content_matches_the_commit_does_not_redrive(pool: PgPool) {
        // The common path. A duplicate announcement, or the reconciler backstop racing the fast
        // path, must leave a committed part alone — re-driving on every re-announcement would
        // re-copy the node's whole shard to Ceph for nothing.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_replicated(&claimed, &PartVerified::for_test(), &test_digest()).await.unwrap();

        assert!(!store.redrive_diverged_part(&p, &test_digest()).await.unwrap());
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Replicated));
    }

    #[sqlx::test]
    async fn a_relanding_with_changed_content_redrives_and_reopens_the_backend_enqueue(pool: PgPool) {
        // B-2 at the store layer. The part must go back to `pending` so the drain re-copies it,
        // AND `upload_enqueued_at` must clear: the backend already shipped the superseded bytes,
        // so the enqueue sweep has to publish this part again.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_replicated(&claimed, &PartVerified::for_test(), &test_digest()).await.unwrap();
        store.mark_upload_enqueued(&p).await.unwrap();

        let rewritten = crate::redrive::part_digest(&["different-chunk-0-hash"]);
        assert!(store.redrive_diverged_part(&p, &rewritten).await.unwrap());

        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Pending));
        assert_eq!(
            store.list_replicated_unenqueued_parts(10).await.unwrap(),
            Vec::new(),
            "a pending part is off the enqueue worklist until it re-commits",
        );
        let (cleared,): (bool,) = sqlx::query_as("SELECT upload_enqueued_at IS NULL FROM cephor_replication_status WHERE object_id = $1")
            .bind(UUID_A)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(cleared, "the stale backend publish was cleared for a re-publish");
        assert!(store.claim_part().await.unwrap().is_some(), "and the part is immediately re-claimable");
    }

    #[sqlx::test]
    async fn a_relanded_part_committed_without_a_digest_redrives_rather_than_being_assumed_intact(pool: PgPool) {
        // The NULL policy, which the `IS DISTINCT FROM` guard carries. Rows committed before
        // content_sha256 shipped cannot be compared; "unknown" must not resolve to "fine" on an
        // integrity check. This costs nothing at deploy because it is only evaluated when an
        // announcement names an already-`replicated` part — a rewrite, by construction.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        seed_status_node(&pool, UUID_A, 1, 1, "replicated", "node-a").await;
        let p = part(UUID_A, 1, 1);

        assert_eq!(store.record_landed_part(&p).await.unwrap().digest, None, "a legacy row has no digest");
        assert!(store.redrive_diverged_part(&p, &test_digest()).await.unwrap());
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Pending));
    }

    #[sqlx::test]
    async fn a_relanding_of_a_committed_part_gates_it_off_the_eviction_worklist(pool: PgPool) {
        // The evict-vs-reland race at the store layer. A rewritten committed part still reads
        // 'replicated' and its residency recency predates the rewrite, so without this gate the
        // LRU worklist ranks the ONLY copy of the client's new bytes as its coldest candidate —
        // and an eviction before the divergence check destroys those bytes while the pool keeps
        // the superseded ones, permanently and silently. The re-landing record must therefore
        // arm the gate in the SAME statement that reports the prior state.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);
        seed_part_size(&pool, UUID_A, 5, 1, Some(4096)).await;
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_replicated(&claimed, &PartVerified::for_test(), &test_digest()).await.unwrap();
        store.record_resident(&p, 4096).await.unwrap();
        assert_eq!(
            store.evictable_parts(10).await.unwrap().len(),
            1,
            "the pre-commit record left the gate unarmed: committed and resident is evictable",
        );

        let outcome = store.record_landed_part(&p).await.unwrap();

        assert_eq!(
            outcome.state,
            ReplicationState::Replicated,
            "the re-landing sees the committed prior state"
        );
        assert!(
            store.evictable_parts(10).await.unwrap().is_empty(),
            "a re-landed committed part is off the worklist while its divergence check is pending",
        );
        // The gate is time-bounded, not permanent: a crash between the record and the check
        // must cost ten minutes of one part's evictability, never pin cache forever.
        sqlx::query("UPDATE cephor_replication_status SET relanded_at = now() - interval '11 minutes' WHERE object_id = $1")
            .bind(UUID_A)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            store.evictable_parts(10).await.unwrap().len(),
            1,
            "an expired gate releases the part back to the worklist",
        );
    }

    #[sqlx::test]
    async fn the_worklist_admits_exactly_replicated_rows_past_the_reland_grace(pool: PgPool) {
        // The eviction worklist's admission predicate over its FULL domain — every status
        // crossed with every reland-gate state — exhaustively, because a finite domain makes
        // exhaustion stronger than sampling. Admission must be exactly
        // {replicated} × {never re-landed, grace lapsed}: every other status means the SSD
        // copy is (or may again become) the only durable one, and an in-grace re-land is the
        // B-2 window where an eviction destroys the client's new bytes.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let statuses = ["pending", "draining", "replicated", "failed", "corrupt"];
        // 0 = never re-landed, 1 = re-landed now (in grace), 2 = re-landed 11 min ago (lapsed).
        let relands = [0i32, 1, 2];
        let mut expected = Vec::new();
        let mut number = 0i64;
        for status in statuses {
            for reland in relands {
                number += 1;
                let p = part(UUID_A, 7, u32::try_from(number).unwrap());
                store.record_landed_part(&p).await.unwrap();
                store.record_resident(&p, 4096).await.unwrap();
                sqlx::query(
                    "UPDATE cephor_replication_status \
                     SET status = $1, \
                         relanded_at = CASE $4::int WHEN 0 THEN NULL WHEN 1 THEN now() \
                                       ELSE now() - interval '11 minutes' END \
                     WHERE object_id = $2 AND version = 7 AND part_number = $3",
                )
                .bind(status)
                .bind(UUID_A)
                .bind(number)
                .bind(reland)
                .execute(&pool)
                .await
                .unwrap();
                let in_grace = reland == 1;
                if status == "replicated" && !in_grace {
                    expected.push(number);
                }
            }
        }
        let mut admitted: Vec<i64> = store
            .evictable_parts(50)
            .await
            .unwrap()
            .iter()
            .map(|r| i64::from(r.part.part().get()))
            .collect();
        admitted.sort_unstable();
        assert_eq!(admitted, expected, "admission must be exactly replicated × out-of-grace");
    }

    #[sqlx::test]
    async fn a_relanding_conflict_never_touches_updated_at(pool: PgPool) {
        // `updated_at` drives the GC retention window, the failed-reclaim grace and
        // `oldest_pending_age`. The conflict SET deliberately updates only `node_id` and
        // `relanded_at`; a future `updated_at = now()` added there would keep a repeatedly
        // announced terminal part young forever — never aging into GC or reclaim — with
        // nothing else failing. Pinned here because it is the single most plausible edit
        // to that statement.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        sqlx::query("UPDATE cephor_replication_status SET updated_at = now() - interval '3 days' WHERE object_id = $1")
            .bind(UUID_A)
            .execute(&pool)
            .await
            .unwrap();

        store.record_landed_part(&p).await.unwrap();

        let (aged,): (bool,) = sqlx::query_as("SELECT updated_at < now() - interval '2 days' FROM cephor_replication_status WHERE object_id = $1")
            .bind(UUID_A)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(aged, "the conflict path must leave updated_at alone");
    }

    #[sqlx::test]
    async fn a_relanding_of_an_uncommitted_part_leaves_the_eviction_gate_unarmed(pool: PgPool) {
        // Announcements for still-pending parts are the COMMON conflict (an MPU part announced,
        // then re-announced before its drain). They have no committed pool copy that could go
        // stale, so stamping them would gate parts the divergence check will never look at.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.record_landed_part(&p).await.unwrap();

        let (unarmed,): (bool,) = sqlx::query_as("SELECT relanded_at IS NULL FROM cephor_replication_status WHERE object_id = $1")
            .bind(UUID_A)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(unarmed, "a pending part's re-announcement must not arm the eviction gate");
    }

    #[sqlx::test]
    async fn a_diverged_redrive_takes_the_part_off_the_eviction_worklist(pool: PgPool) {
        // The constraint that must never be weakened: the evictor may not unlink a part whose
        // only good copy is the SSD one. A diverged part is exactly that — its pool copy holds
        // superseded bytes — so the `replicated → pending` reset has to remove it from the
        // worklist, which `evictable_parts`' status join does. Residency is deliberately left
        // in place (the part IS still on the disk), same as the corrupt re-drive.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        let p = part(UUID_A, 5, 1);
        seed_part_size(&pool, UUID_A, 5, 1, Some(4096)).await;
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_replicated(&claimed, &PartVerified::for_test(), &test_digest()).await.unwrap();
        store.record_resident(&p, 4096).await.unwrap();
        assert_eq!(store.evictable_parts(10).await.unwrap().len(), 1, "committed and resident is evictable");

        store
            .redrive_diverged_part(&p, &crate::redrive::part_digest(&["rewritten"]))
            .await
            .unwrap();

        assert!(
            store.evictable_parts(10).await.unwrap().is_empty(),
            "a re-driven part is unevictable while its pool copy is the stale one",
        );
        // The residency ROW is untouched — the bytes really are still on this disk — but they stop
        // counting as CACHE, because cache means EVICTABLE and a `pending` part is not. This is
        // `node_cache_bytes`' documented contract, not an accident of the re-drive: it names this
        // exact case ("a re-driven corrupt part back in `pending`") as the thing it must exclude,
        // because counting it would double-count against `node_backlog_bytes` and overstate the
        // node's headroom to the allocator. So assert the MOVE, which proves both halves at once.
        assert_eq!(
            store.node_cache_bytes("node-a").await.unwrap(),
            0,
            "a re-driven part is no longer evictable, so it is no longer cache",
        );
        assert_eq!(
            store.node_backlog_bytes("node-a").await.unwrap(),
            4096,
            "and it is now undrained work — the bytes moved, they did not vanish",
        );
    }

    #[sqlx::test]
    async fn a_redrive_is_scoped_to_the_node_that_holds_the_part(pool: PgPool) {
        // A part lives only on the SSD of the node that ingested it. A peer that somehow saw the
        // announcement must not reset a row naming a disk it cannot read — the part would go
        // `pending` on a node whose claim_part can never pick it up, stalling it indefinitely.
        create_app_schema(&pool).await;
        seed_status_node(&pool, UUID_A, 1, 1, "replicated", "node-a").await;
        let other = Store::from_pool(pool.clone()).with_node_id("node-b");

        assert!(!other.redrive_diverged_part(&part(UUID_A, 1, 1), &test_digest()).await.unwrap());
        assert_eq!(
            other.status(&part(UUID_A, 1, 1)).await.unwrap(),
            Some(ReplicationState::Replicated),
            "another node's committed part is not this node's to re-drive",
        );
    }

    #[sqlx::test]
    async fn a_redriven_corrupt_part_leaves_the_eviction_worklist_while_it_is_pending_again(pool: PgPool) {
        // Residency and status are INDEPENDENT, and conflating them is a data-loss bug.
        // `redrive_corrupt_parts` resets a corrupt part to 'pending' so the drain re-copies it,
        // and deliberately does NOT clear resident_at — the part really is still on the disk.
        // But while it is pending its SSD copy is once again the ONLY durable copy, so it must
        // vanish from the worklist until the re-drive commits and makes it replicated again.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        seed_status_node(&pool, UUID_A, 1, 1, "corrupt", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 1, Some(100)).await;
        store.record_resident(&part(UUID_A, 1, 1), 100).await.unwrap();

        let redriven = store.redrive_corrupt_parts(3).await.unwrap();
        assert_eq!(redriven, 1, "the corrupt part went back to pending for a fresh copy");

        assert!(
            store.evictable_parts(10).await.unwrap().is_empty(),
            "a resident-but-pending part is not evictable — its SSD copy is the only durable one",
        );
        assert_eq!(
            store.node_cache_bytes("node-a").await.unwrap(),
            0,
            "nor does it count as evictable cache toward the allocator's ingest headroom",
        );
    }

    #[sqlx::test]
    async fn the_eviction_worklist_is_oldest_first_scoped_to_this_node_and_excludes_evicted(pool: PgPool) {
        // Three properties the evictor's correctness rests on: FIFO order (the policy),
        // node-scoping (a node must never evict a peer's part — it does not even hold it),
        // and that a marked part leaves the worklist (or the next pass would re-offer it
        // forever and never make progress).
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        for (n, node) in [(1, "node-a"), (2, "node-a"), (3, "node-b")] {
            seed_status_node(&pool, UUID_A, 1, n, "replicated", node).await;
            seed_part_size(&pool, UUID_A, 1, n, Some(100)).await;
        }
        // Residency rows on the OWNING node, with distinct resident_at so FIFO order is
        // unambiguous; part 2 is the OLDER one. Part 3 is node-b's, so it must never appear.
        for (n, node, hours_ago) in [(1_i64, "node-a", 1_i32), (2, "node-a", 3), (3, "node-b", 2)] {
            sqlx::query(
                "INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, bytes, resident_at) \
                 VALUES ($1, $2, 1, $3, 100, now() - make_interval(hours => $4))",
            )
            .bind(node)
            .bind(UUID_A)
            .bind(n)
            .bind(hours_ago)
            .execute(&pool)
            .await
            .unwrap();
        }

        let worklist = store.evictable_parts(10).await.unwrap();
        let numbers: Vec<u32> = worklist.iter().map(|r| r.part.part().get()).collect();
        assert_eq!(numbers, vec![2, 1], "oldest-resident first, and node-b's part is not ours");
        assert_eq!(store.node_cache_bytes("node-a").await.unwrap(), 200);

        store.mark_evicted(&[part(UUID_A, 1, 2)]).await.unwrap();

        let after: Vec<u32> = store.evictable_parts(10).await.unwrap().iter().map(|r| r.part.part().get()).collect();
        assert_eq!(after, vec![1], "an evicted part leaves the worklist");
        assert_eq!(store.node_cache_bytes("node-a").await.unwrap(), 100, "and stops counting as cache");
    }

    #[sqlx::test]
    async fn eviction_orders_on_last_read_falling_back_to_residency(pool: PgPool) {
        // The Phase H property, and it fails on FIFO. `old_but_hot` joined the cache first, so
        // arrival order would evict it — but it is the one still being read, which for a working
        // set re-read every epoch is exactly the part about to be needed again. Evicting it costs
        // a peer-or-pool read plus a local write to put it straight back.
        //
        // `never_read` has a NULL last_read_at and must fall back to its residency time, so the
        // pre-0017 population still orders sanely with no backfill.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        for n in 1..=3 {
            seed_status_node(&pool, UUID_A, 1, n, "replicated", "node-a").await;
            seed_part_size(&pool, UUID_A, 1, n, Some(100)).await;
        }
        // part 1: resident longest, but read most recently -> must survive
        // part 2: resident recently, never read            -> falls back to resident_at
        // part 3: resident a while ago, read a while ago   -> the true LRU victim
        sqlx::query(
            "INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, bytes, resident_at, last_read_at) VALUES \
             ('node-a', $1, 1, 1, 100, now() - make_interval(hours => 10), now() - make_interval(mins => 1)), \
             ('node-a', $1, 1, 2, 100, now() - make_interval(hours => 2),  NULL), \
             ('node-a', $1, 1, 3, 100, now() - make_interval(hours => 9),  now() - make_interval(hours => 5))",
        )
        .bind(UUID_A)
        .execute(&pool)
        .await
        .unwrap();

        let order: Vec<u32> = store.evictable_parts(10).await.unwrap().iter().map(|r| r.part.part().get()).collect();

        assert_eq!(
            order,
            vec![3, 2, 1],
            "least-recently-USED first: the hot part resident longest must be evicted LAST"
        );
    }

    #[sqlx::test]
    async fn a_local_read_moves_a_part_to_the_back_of_the_eviction_queue(pool: PgPool) {
        // The other half: serving a part from local flash has to actually change its fate, or
        // the ordering above is decorative.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        for n in 1_u32..=2 {
            seed_status_node(&pool, UUID_A, 1, i64::from(n), "replicated", "node-a").await;
            seed_part_size(&pool, UUID_A, 1, i64::from(n), Some(100)).await;
            store.record_resident(&part(UUID_A, 1, n), 100).await.unwrap();
        }
        // Age part 1 so it is the victim, then read it.
        sqlx::query("UPDATE cephor_ssd_residency SET resident_at = now() - make_interval(hours => 5) WHERE part_number = 1")
            .execute(&pool)
            .await
            .unwrap();
        let first: Vec<u32> = store.evictable_parts(10).await.unwrap().iter().map(|r| r.part.part().get()).collect();
        assert_eq!(first, vec![1, 2], "part 1 is the victim before it is read");

        // The UPDATE the api's ReadRecencyRecorder issues. Pinned here because this ordering is
        // the contract that recorder has to satisfy, and the consequence of getting it wrong —
        // a hot part evicted and immediately re-fetched — is silent.
        sqlx::query(
            "UPDATE cephor_ssd_residency SET last_read_at = now()              WHERE node_id = $1 AND object_id = $2 AND version = 1 AND part_number = 1",
        )
        .bind("node-a")
        .bind(UUID_A)
        .execute(&pool)
        .await
        .unwrap();

        let after: Vec<u32> = store.evictable_parts(10).await.unwrap().iter().map(|r| r.part.part().get()).collect();
        assert_eq!(after, vec![2, 1], "reading it moved it behind the untouched part");
    }

    #[sqlx::test]
    async fn touching_a_part_this_node_does_not_hold_changes_nothing(pool: PgPool) {
        // The read path calls this on any local hit, and a peer's row must not be reachable:
        // recency is per (node, part), and stamping a peer's row would protect a copy on a disk
        // this node cannot see while leaving its own unprotected.
        create_app_schema(&pool).await;
        seed_status_node(&pool, UUID_A, 1, 1, "replicated", "node-b").await;
        seed_part_size(&pool, UUID_A, 1, 1, Some(100)).await;
        sqlx::query("INSERT INTO cephor_ssd_residency (node_id, object_id, version, part_number, bytes) VALUES ('node-b', $1, 1, 1, 100)")
            .bind(UUID_A)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query(
            "UPDATE cephor_ssd_residency SET last_read_at = now()              WHERE node_id = $1 AND object_id = $2 AND version = 1 AND part_number = 1",
        )
        .bind("node-a")
        .bind(UUID_A)
        .execute(&pool)
        .await
        .unwrap();

        let (touched,): (i64,) = sqlx::query_as("SELECT count(*) FROM cephor_ssd_residency WHERE last_read_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(touched, 0, "another node's residency row was stamped");
    }

    #[sqlx::test]
    async fn node_backlog_bytes_sums_only_this_nodes_pending_and_draining_parts(pool: PgPool) {
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone());

        // node-a, undrained (pending+draining): counted -> 100 + 200 = 300.
        seed_status_node(&pool, UUID_A, 1, 1, "pending", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 1, Some(100)).await;
        seed_status_node(&pool, UUID_A, 1, 2, "draining", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 2, Some(200)).await;
        // node-a terminal (replicated/failed): excluded — no longer undrained work.
        seed_status_node(&pool, UUID_A, 1, 3, "replicated", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 3, Some(999)).await;
        seed_status_node(&pool, UUID_A, 1, 4, "failed", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 4, Some(888)).await;
        // A peer node's pending part: excluded (its bytes are its own node's backlog).
        seed_status_node(&pool, UUID_B, 2, 1, "pending", "node-b").await;
        seed_part_size(&pool, UUID_B, 2, 1, Some(500)).await;
        // node-a pending with a NULL size and one with no parts row at all: both contribute 0.
        seed_status_node(&pool, UUID_A, 1, 5, "pending", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 5, None).await;
        seed_status_node(&pool, UUID_A, 1, 6, "pending", "node-a").await; // no parts row

        assert_eq!(store.node_backlog_bytes("node-a").await.unwrap(), 300);
        assert_eq!(store.node_backlog_bytes("node-b").await.unwrap(), 500);
        assert_eq!(
            store.node_backlog_bytes("node-c").await.unwrap(),
            0,
            "a node with no rows has zero backlog"
        );
    }

    #[sqlx::test]
    async fn node_undrained_count_counts_every_undrained_row_even_when_backlog_bytes_is_zero(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
        // C8 wedge signal (PR #235 D1): a draining row whose `parts` row is missing or has a
        // NULL/0 size contributes ZERO bytes to node_backlog_bytes (INNER JOIN + COALESCE), so a
        // wedged node's byte-backlog can read 0 while real undrained work remains. The COUNT does
        // NOT join `parts`, so those exact rows still register — that is why readiness keys on it.
        create_app_schema(&pool).await;
        let store = Store::from_pool(pool.clone());

        // node-a undrained rows that ALL contribute 0 bytes: a draining row with no parts row, a
        // pending row with a NULL size, and a pending row with a 0 size. node_backlog_bytes == 0.
        seed_status_node(&pool, UUID_A, 1, 1, "draining", "node-a").await; // no parts row
        seed_status_node(&pool, UUID_A, 1, 2, "pending", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 2, None).await; // NULL size
        seed_status_node(&pool, UUID_A, 1, 3, "pending", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 3, Some(0)).await; // zero size
        // Terminal rows are drained work, excluded from the count.
        seed_status_node(&pool, UUID_A, 1, 4, "replicated", "node-a").await;
        seed_part_size(&pool, UUID_A, 1, 4, Some(999)).await;
        // A peer node's pending row belongs to its own node's count.
        seed_status_node(&pool, UUID_B, 2, 1, "pending", "node-b").await;

        assert_eq!(store.node_backlog_bytes("node-a").await?, 0, "every undrained row contributes 0 bytes");
        assert_eq!(
            store.node_undrained_count("node-a").await?,
            3,
            "the wedged node's 3 undrained rows are counted despite the 0-byte backlog"
        );
        assert_eq!(store.node_undrained_count("node-b").await?, 1, "counts only this node's rows");
        assert_eq!(store.node_undrained_count("node-c").await?, 0, "a node with no rows is idle");
        Ok(())
    }

    /// Backdates a status row's `landed_at` by `secs`, to age it for the starvation gauge.
    async fn backdate_landed(pool: &PgPool, object: &str, version: i64, number: i64, secs: i64) {
        sqlx::query(
            "UPDATE cephor_replication_status SET landed_at = now() - (interval '1 second' * $4) \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(object)
        .bind(version)
        .bind(number)
        .bind(secs)
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn node_oldest_pending_age_secs_is_the_age_of_the_oldest_pending_row_only(pool: PgPool) {
        let store = Store::from_pool(pool.clone());
        assert_eq!(
            store.node_oldest_pending_age_secs("node-a").await.unwrap(),
            0,
            "a node with no pending rows reports zero age, not an error"
        );

        // node-a: two pending rows, 90s and 300s old — the oldest wins. The 300s row is
        // ALSO backed off (deferred_until in the future): a deferred row is still
        // `pending`, and an MPU wall aging past hours is exactly what the gauge must show.
        seed_status_node(&pool, UUID_A, 1, 1, "pending", "node-a").await;
        backdate_landed(&pool, UUID_A, 1, 1, 90).await;
        seed_status_node(&pool, UUID_A, 1, 2, "pending", "node-a").await;
        backdate_landed(&pool, UUID_A, 1, 2, 300).await;
        sqlx::query("UPDATE cephor_replication_status SET deferred_until = now() + interval '10 minutes' WHERE part_number = 2")
            .execute(&pool)
            .await
            .unwrap();
        // Far older draining/replicated/failed rows: a draining row is being worked and
        // terminal rows are done — none is starving claimable work, so none counts.
        seed_status_node(&pool, UUID_A, 1, 3, "draining", "node-a").await;
        backdate_landed(&pool, UUID_A, 1, 3, 9_000).await;
        seed_status_node(&pool, UUID_A, 1, 4, "replicated", "node-a").await;
        backdate_landed(&pool, UUID_A, 1, 4, 9_000).await;
        seed_status_node(&pool, UUID_A, 1, 5, "failed", "node-a").await;
        backdate_landed(&pool, UUID_A, 1, 5, 9_000).await;
        // A peer node's even older pending row is that node's starvation, not this one's.
        seed_status_node(&pool, UUID_B, 2, 1, "pending", "node-b").await;
        backdate_landed(&pool, UUID_B, 2, 1, 9_000).await;

        let age = store.node_oldest_pending_age_secs("node-a").await.unwrap();
        assert!(
            (300..330).contains(&age),
            "the oldest pending row (deferred, 300s) wins over the younger one and the non-pending rows, got {age}"
        );
        let peer = store.node_oldest_pending_age_secs("node-b").await.unwrap();
        assert!(peer >= 9_000, "the peer's pending age is scoped to the peer, got {peer}");
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
    async fn mark_corrupt_sets_the_corrupt_state_on_a_claimed_part(pool: PgPool) {
        // R4: a servable object's persistent byte-mismatch is marked `corrupt` (held), distinct
        // from `failed` (abandoned upload). Claim-fenced like mark_failed; clears claimed_at.
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store
            .mark_corrupt(&claimed, "chunk copy byte mismatch on a servable object")
            .await
            .unwrap();
        assert_eq!(store.status(&p).await.unwrap(), Some(ReplicationState::Corrupt));
    }

    #[sqlx::test]
    async fn redrive_resets_corrupt_parts_under_the_cap_and_holds_those_at_it(pool: PgPool) {
        // The bounded re-drive: a `corrupt` part under the attempt cap is reset to `pending`
        // (re-claimable, its attempt count bumped) so the drain re-copies from the intact SSD
        // source; a part already at the cap is left `corrupt` (held + paged), never looping.
        let store = Store::from_pool(pool.clone());
        let under = part(UUID_A, 5, 1);
        let at_cap = part(UUID_A, 5, 2);
        seed_status_node(&pool, UUID_A, 5, 1, "corrupt", "test-node").await;
        seed_status_node(&pool, UUID_A, 5, 2, "corrupt", "test-node").await;
        sqlx::query("UPDATE cephor_replication_status SET corrupt_attempts = 3 WHERE object_id = $1 AND part_number = 2")
            .bind(UUID_A)
            .execute(&pool)
            .await
            .unwrap();

        let redriven = store.redrive_corrupt_parts(3).await.unwrap();
        assert_eq!(redriven, 1, "only the under-cap corrupt part is re-driven");
        assert_eq!(
            store.status(&under).await.unwrap(),
            Some(ReplicationState::Pending),
            "re-driven back to pending"
        );
        assert_eq!(
            store.status(&at_cap).await.unwrap(),
            Some(ReplicationState::Corrupt),
            "the capped part is held"
        );
        let bumped: i32 = sqlx::query_scalar("SELECT corrupt_attempts FROM cephor_replication_status WHERE object_id = $1 AND part_number = 1")
            .bind(UUID_A)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(bumped, 1, "the re-drive increments the attempt counter");
    }

    #[sqlx::test]
    async fn count_corrupt_parts_counts_only_this_nodes_corrupt_rows(pool: PgPool) {
        let store = Store::from_pool(pool.clone());
        seed_status_node(&pool, UUID_A, 5, 1, "corrupt", "test-node").await;
        seed_status_node(&pool, UUID_A, 5, 2, "corrupt", "test-node").await;
        seed_status_node(&pool, UUID_A, 5, 3, "failed", "test-node").await; // not corrupt
        seed_status_node(&pool, UUID_B, 6, 1, "corrupt", "other-node").await; // a peer node
        assert_eq!(store.count_corrupt_parts().await.unwrap(), 2, "counts only this node's corrupt rows");
    }

    #[sqlx::test]
    async fn mark_replicated_commits_a_claimed_part(pool: PgPool) {
        let store = Store::from_pool(pool);
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        let claimed = store.claim_part().await.unwrap().unwrap();
        store.mark_replicated(&claimed, &PartVerified::for_test(), &test_digest()).await.unwrap();
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
        let err = store
            .mark_replicated(&unclaimed, &PartVerified::for_test(), &test_digest())
            .await
            .unwrap_err();
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

        let err = store
            .mark_replicated(&first, &PartVerified::for_test(), &test_digest())
            .await
            .unwrap_err();
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
        store.mark_replicated(&second, &PartVerified::for_test(), &test_digest()).await.unwrap();
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

        let scanner = OnePart(DiscoveredPart {
            part: p.clone(),
            age: std::time::Duration::ZERO,
        });
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

        let scanner = OnePart(DiscoveredPart {
            part: p.clone(),
            age: std::time::Duration::ZERO,
        });
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
             size_bytes bigint, md5_hash text, \
             PRIMARY KEY (object_id, object_version))",
            "CREATE TABLE multipart_uploads (upload_id uuid PRIMARY KEY, object_id uuid, initiated_at timestamptz NOT NULL)",
            "CREATE TABLE parts (object_id uuid NOT NULL, object_version bigint NOT NULL, part_number bigint NOT NULL, \
             size_bytes bigint, upload_id uuid)",
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

    /// Seconds until the row's `deferred_until`, measured against the same DB clock
    /// that stamped it — so the assertion is immune to test-host/DB clock skew.
    async fn defer_remaining_secs(pool: &PgPool, part: &PartKey) -> f64 {
        sqlx::query_scalar(
            "SELECT EXTRACT(EPOCH FROM (deferred_until - now()))::float8 FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test]
    async fn defer_part_backs_off_exponentially(pool: PgPool) {
        // The 2026-07-26 head-of-line starvation incident: with a FIXED backoff, a wall
        // of not-ready MPU parts re-entered the claimable head every interval and — being
        // oldest by landed_at — consumed every claim slot, starving every younger part.
        // Repeated deferrals must therefore back off geometrically: 5s, 10s, 20s, ...
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();

        let mut observed = Vec::new();
        for _ in 0..3 {
            store.claim_part().await.unwrap().expect("the part is claimable");
            store.defer_part(&p).await.unwrap();
            observed.push(defer_remaining_secs(&pool, &p).await);
            // Clear the parked backoff so the next round can re-claim immediately.
            sqlx::query("UPDATE cephor_replication_status SET deferred_until = NULL WHERE object_id = $1")
                .bind(p.object().as_str())
                .execute(&pool)
                .await
                .unwrap();
        }
        for (n, (remaining, expected)) in observed.iter().zip([5.0_f64, 10.0, 20.0]).enumerate() {
            assert!(
                (expected - 1.0..=expected + 1.0).contains(remaining),
                "deferral #{n} should park the part ~{expected}s, got {remaining}s (all: {observed:?})",
            );
        }
    }

    #[sqlx::test]
    async fn defer_backoff_is_capped(pool: PgPool) {
        // A part that has deferred many times (an abandoned MPU that never finalizes)
        // must park at the cap, not for days: uncapped, 30 attempts would be
        // 5 * 2^16 s (~3.8 days) even after the exponent clamp.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().expect("the part is claimable");
        sqlx::query("UPDATE cephor_replication_status SET defer_attempts = 30 WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();

        store.defer_part(&p).await.unwrap();
        let remaining = defer_remaining_secs(&pool, &p).await;
        assert!(remaining <= 601.0, "the deferral must not exceed the 600s cap, got {remaining}s");
        assert!(
            remaining >= 599.0,
            "a 30-attempt row is parked at the FULL cap, not the base backoff, got {remaining}s",
        );
    }

    #[sqlx::test]
    async fn release_part_preserves_defer_attempts(pool: PgPool) {
        // A transient Ceph-failure release retries promptly (deferred_until cleared)
        // but must NOT erase the not-ready escalation: if the part defers again, the
        // backoff continues geometrically instead of restarting at the base interval.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().expect("the part is claimable");
        store.defer_part(&p).await.unwrap();
        sqlx::query("UPDATE cephor_replication_status SET deferred_until = now() - interval '1 second' WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();
        store.claim_part().await.unwrap().expect("re-claimable once the backoff elapsed");

        store.release_part(&p).await.unwrap();
        let (attempts, deferred_is_null): (i32, bool) = sqlx::query_as(
            "SELECT defer_attempts, deferred_until IS NULL FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(p.object().as_str())
        .bind(i64::from(p.version().get()))
        .bind(i64::from(p.part().get()))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(attempts, 1, "release must not reset the defer escalation");
        assert!(deferred_is_null, "release clears the parked backoff for a prompt retry");
    }

    /// The row's `(defer_attempts, missing_source_attempts)` — the pair the
    /// missing-source escalation must keep separable (the shared-counter hazard).
    async fn attempt_counters(pool: &PgPool, part: &PartKey) -> (i64, i64) {
        sqlx::query_as(
            "SELECT defer_attempts::bigint, missing_source_attempts::bigint FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test]
    async fn defer_part_missing_source_counts_the_observation_and_keeps_the_shared_backoff(pool: PgPool) {
        // A missing-source deferral is a normal exponential defer PLUS one
        // missing-source observation: both counters advance, the backoff stays the
        // shared defer_attempts geometry (5s, 10s, ...), and the returned count lets
        // the worker escalate without a second round trip.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();

        store.claim_part().await.unwrap().expect("the part is claimable");
        let first = store.defer_part_missing_source(&p, 20).await.unwrap();
        assert_eq!(first, MissingSourceOutcome::Deferred(1), "the first observation is counted and returned");
        assert_eq!(
            attempt_counters(&pool, &p).await,
            (1, 1),
            "both counters advance on a missing-source defer"
        );
        let remaining = defer_remaining_secs(&pool, &p).await;
        assert!(
            (4.0..=6.0).contains(&remaining),
            "the first backoff is the shared 5s base, got {remaining}s"
        );
        assert_eq!(
            <Store as PartReplicationStore>::status(&store, &p).await.unwrap(),
            Some(ReplicationState::Pending),
            "below the threshold the part is deferred back to pending",
        );

        sqlx::query("UPDATE cephor_replication_status SET deferred_until = NULL WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();
        store.claim_part().await.unwrap().expect("re-claimable once the backoff is cleared");
        let second = store.defer_part_missing_source(&p, 20).await.unwrap();
        assert_eq!(second, MissingSourceOutcome::Deferred(2));
        let remaining = defer_remaining_secs(&pool, &p).await;
        assert!(
            (9.0..=11.0).contains(&remaining),
            "the backoff doubles like a plain defer, got {remaining}s"
        );
    }

    #[sqlx::test]
    async fn defer_part_missing_source_fails_the_row_atomically_at_the_threshold(pool: PgPool) {
        // Reaching the threshold must flip the row terminal `failed` IN the same
        // guarded UPDATE that would otherwise defer it: a defer-then-fail pair would
        // open a window where the deferred (pending) row is re-claimed between the
        // two statements and the fail either misses or clobbers a live claim.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().expect("the part is claimable");
        sqlx::query("UPDATE cephor_replication_status SET missing_source_attempts = 19 WHERE object_id = $1")
            .bind(p.object().as_str())
            .execute(&pool)
            .await
            .unwrap();

        let outcome = store.defer_part_missing_source(&p, 20).await.unwrap();
        assert_eq!(outcome, MissingSourceOutcome::Failed, "the 20th observation writes the part off");
        assert_eq!(
            <Store as PartReplicationStore>::status(&store, &p).await.unwrap(),
            Some(ReplicationState::Failed),
            "the row is terminal failed",
        );
        assert!(
            store.claim_part().await.unwrap().is_none(),
            "a failed row is out of the claim set for good — no more churn",
        );
        let unparked: bool = sqlx::query_scalar(
            "SELECT deferred_until IS NULL FROM cephor_replication_status \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(p.object().as_str())
        .bind(i64::from(p.version().get()))
        .bind(i64::from(p.part().get()))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(unparked, "the fail arm parks no backoff — deferred_until is cleared, not set");
    }

    // debug_assert compiles out in release, so this pin only exists where the guard does.
    #[cfg(debug_assertions)]
    #[sqlx::test]
    #[should_panic(expected = "first missing-source observation")]
    async fn defer_part_missing_source_rejects_a_degenerate_threshold_in_debug(pool: PgPool) {
        // The domain edge, pinned: a threshold of 0 or 1 means the FIRST observation
        // writes the part off — the exact transient-NotFound hazard the separate
        // counter exists to prevent — so debug builds refuse it outright (in release
        // the documented first-observation write-off would execute as written).
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().expect("the part is claimable");
        let _ = store.defer_part_missing_source(&p, 1).await;
    }

    #[sqlx::test]
    async fn defer_part_missing_source_is_a_no_op_off_the_draining_guard(pool: PgPool) {
        // Like defer_part/release_part, the `status='draining'` guard makes a late
        // call a no-op when the row has since advanced — and a no-op defer must not
        // escalate, so the outcome is Superseded, never Failed.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();

        // Never claimed: the row is 'pending', so the guard misses.
        let outcome = store.defer_part_missing_source(&p, 20).await.unwrap();
        assert_eq!(outcome, MissingSourceOutcome::Superseded, "a guard miss reports Superseded, not a count");
        assert_eq!(attempt_counters(&pool, &p).await, (0, 0), "a guard miss touches neither counter");
        assert_eq!(
            <Store as PartReplicationStore>::status(&store, &p).await.unwrap(),
            Some(ReplicationState::Pending),
            "the row is left exactly as it was",
        );
    }

    #[sqlx::test]
    async fn plain_defer_part_never_touches_missing_source_attempts(pool: PgPool) {
        // The amendment's core invariant: overdraft/not-ready deferrals go through
        // defer_part and must NOT count toward the missing-source write-off — only
        // defer_part_missing_source observes a vanished source.
        let store = Store::from_pool(pool.clone());
        let p = part(UUID_A, 5, 1);
        store.record_landed_part(&p).await.unwrap();
        store.claim_part().await.unwrap().expect("the part is claimable");

        store.defer_part(&p).await.unwrap();
        assert_eq!(
            attempt_counters(&pool, &p).await,
            (1, 0),
            "a plain defer advances only the shared backoff counter",
        );
    }

    /// Drives one part `pending → draining → replicated` (the drain's commit path) and returns it.
    async fn seed_replicated(store: &Store, uuid: &str, version: u32, part_number: u32) -> crate::apipart::PartKey {
        use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
        use crate::partdrain::PartVerified;
        use core::str::FromStr;
        let part = PartKey::new(ObjectId::from_str(uuid).unwrap(), Version::new(version), PartNumber::new(part_number));
        store.record_landed_part(&part).await.unwrap();
        let claim = store.claim_part().await.unwrap().expect("claims the seeded pending part");
        assert_eq!(claim.part(), &part);
        store.mark_replicated(&claim, &PartVerified::for_test(), &test_digest()).await.unwrap();
        part
    }

    #[sqlx::test]
    async fn the_enqueue_sweep_worklist_tracks_the_backend_publish(pool: PgPool) {
        // Tier-2: a part committed `replicated` before its address landed is on the sweep
        // worklist until mark_upload_enqueued stamps it (what the enqueue sweep does after a
        // successful publish). The worklist is node-scoped and ordered by commit time.
        let store = Store::from_pool(pool);
        let part = seed_replicated(&store, "466916c0-d61b-4518-b81b-9576b574270a", 5, 1).await;

        let before = store.list_replicated_unenqueued_parts(10).await.unwrap();
        assert_eq!(before, vec![part.clone()], "a replicated, un-enqueued part is on the worklist");

        store.mark_upload_enqueued(&part).await.unwrap();
        assert!(
            store.list_replicated_unenqueued_parts(10).await.unwrap().is_empty(),
            "once stamped, the part drops off the sweep worklist",
        );
        // Idempotent: a second stamp (inline enqueue racing the sweep) is a harmless no-op.
        store.mark_upload_enqueued(&part).await.unwrap();
    }

    #[sqlx::test]
    async fn gc_spares_a_replicated_row_awaiting_enqueue_but_reaps_a_stamped_one(pool: PgPool) {
        // The GC guard: gc_terminal_status_rows must NOT delete a `replicated` row whose backend
        // upload is still outstanding (upload_enqueued_at IS NULL) — that would drop the enqueue
        // sweep's worklist item and lose the backend upload. A stamped `replicated` row is
        // genuinely terminal and IS reaped.
        let store = Store::from_pool(pool);
        let unenqueued = seed_replicated(&store, "466916c0-d61b-4518-b81b-9576b574270a", 5, 1).await;
        let enqueued = seed_replicated(&store, "466916c0-d61b-4518-b81b-9576b574270a", 5, 2).await;
        store.mark_upload_enqueued(&enqueued).await.unwrap();

        // Retention ZERO makes every row "aged"; only the stamped one is eligible.
        let removed = store.gc_terminal_status_rows(Duration::ZERO).await.unwrap();
        assert_eq!(removed, 1, "only the stamped (fully terminal) replicated row is reaped");
        assert_eq!(
            <Store as PartReplicationStore>::status(&store, &unenqueued).await.unwrap(),
            Some(crate::state::ReplicationState::Replicated),
            "the un-enqueued row is spared so the enqueue sweep can still publish it",
        );
        assert_eq!(
            <Store as PartReplicationStore>::status(&store, &enqueued).await.unwrap(),
            None,
            "the stamped row was GC'd",
        );
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod failed_worklist_tests {
    use super::{PartKey, Store};
    use crate::apipart::{ObjectId, PartNumber, Version};
    use crate::ssd_reclaim::ReclaimLog;
    use core::str::FromStr;
    use core::time::Duration;
    use sqlx::postgres::PgPool;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    fn part(n: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(UUID_A).unwrap(), Version::new(1), PartNumber::new(n))
    }

    /// Seeds a replication row with an explicit age, so grace boundaries are exact.
    async fn seed(pool: &PgPool, n: i64, status: &str, node: &str, age_secs: f64) {
        sqlx::query(
            "INSERT INTO cephor_replication_status (object_id, version, part_number, status, node_id, updated_at) \
             VALUES ($1, 1, $2, $3, $4, now() - make_interval(secs => $5))",
        )
        .bind(UUID_A)
        .bind(n)
        .bind(status)
        .bind(node)
        .bind(age_secs)
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn the_worklist_is_node_scoped_aged_and_failed_only(pool: PgPool) {
        // Three independent filters, each load-bearing. Node scope: a peer's `failed` row names
        // a file this agent cannot unlink. Grace: the diagnosis / abort-settle window. Status:
        // everything else is either live (drain-owned), retained read tier, or the R4 corrupt
        // hold — none of it this worker's to delete.
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        seed(&pool, 1, "failed", "node-a", 7200.0).await; // aged, ours   -> candidate
        seed(&pool, 2, "failed", "node-a", 60.0).await; // young, ours   -> held by grace
        seed(&pool, 3, "failed", "node-b", 7200.0).await; // aged, peer's -> not ours to touch
        seed(&pool, 4, "replicated", "node-a", 7200.0).await; // the read tier
        seed(&pool, 5, "pending", "node-a", 7200.0).await; // live, drain-owned
        seed(&pool, 6, "corrupt", "node-a", 7200.0).await; // R4 hold, never reclaimed

        let got = store.reclaimable_failed_parts(Duration::from_hours(1), 100).await.unwrap();

        assert_eq!(got, vec![part(1)], "only this node's aged failed part is a candidate");
    }

    #[sqlx::test]
    async fn the_worklist_is_oldest_first_and_honours_the_limit(pool: PgPool) {
        // Oldest first so a backlog drains in age order rather than starving its own tail — the
        // same property the eviction cursor needs, for the same reason.
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        seed(&pool, 1, "failed", "node-a", 7200.0).await;
        seed(&pool, 2, "failed", "node-a", 90000.0).await;
        seed(&pool, 3, "failed", "node-a", 10800.0).await;

        let got = store.reclaimable_failed_parts(Duration::from_hours(1), 2).await.unwrap();

        assert_eq!(got, vec![part(2), part(3)], "oldest first, capped at the limit");
    }

    #[sqlx::test]
    async fn a_reclaimed_row_leaves_the_failed_worklist(pool: PgPool) {
        // The store half of the cursor fix. Unlinking changes nothing about the row, so without
        // the stamp this query re-offers the same page every poll — for the seven days until
        // gc_terminal_status_rows removes it.
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        seed(&pool, 1, "failed", "node-a", 7200.0).await;
        seed(&pool, 2, "failed", "node-a", 7200.0).await;

        assert_eq!(store.reclaimable_failed_parts(Duration::from_hours(1), 10).await.unwrap().len(), 2);

        <Store as ReclaimLog>::mark_failed_reclaimed(&store, &[part(1)]).await.unwrap();

        let left = store.reclaimable_failed_parts(Duration::from_hours(1), 10).await.unwrap();
        assert_eq!(left, vec![part(2)], "the marked part is gone from the worklist");
    }

    #[sqlx::test]
    async fn marking_is_node_scoped(pool: PgPool) {
        // A peer's row names a file this agent never touched, so marking it would claim work it
        // did not do — and take the part off the worklist of the node that actually holds it.
        let store = Store::from_pool(pool.clone()).with_node_id("node-a");
        seed(&pool, 1, "failed", "node-b", 7200.0).await;

        <Store as ReclaimLog>::mark_failed_reclaimed(&store, &[part(1)]).await.unwrap();

        let (marked,): (i64,) = sqlx::query_as("SELECT count(*) FROM cephor_replication_status WHERE reclaimed_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(marked, 0, "another node's row was marked");
    }
    #[sqlx::test]
    async fn an_agent_with_no_node_id_reclaims_nothing(pool: PgPool) {
        // The allocator shares this Store type but holds no node identity and owns no disk.
        let store = Store::from_pool(pool.clone());
        seed(&pool, 1, "failed", "node-a", 7200.0).await;

        assert!(store.reclaimable_failed_parts(Duration::from_hours(1), 100).await.unwrap().is_empty());
    }
}
