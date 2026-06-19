//! Postgres-backed central state: node heartbeats, allocations, and GC claims.
//!
//! Uses runtime `sqlx` queries rather than the compile-checked `query!` macro,
//! deliberately: this environment cannot produce a committable `.sqlx` offline
//! cache, and runtime queries keep the crate buildable with no database present.
//! Correctness is verified by the `#[sqlx::test]` integration tests (run with
//! `--features pg`), which apply the real migrations to a real Postgres and
//! exercise every query — so schema drift is caught at test time. Revisit
//! `query!` once CI Postgres infra exists.

use crate::alloc::{Allocation, FleetView, NodeObservation};
use crate::apipart::{ObjectId, PartKey, PartNumber, Version};
use crate::gc::GcClaim;
use crate::ids::{FileId, NodeId};
use crate::partdrain::{ClaimedPart, PartReplicationStore, PartVerified};
use crate::reconcile::PartLandingLog;
use crate::state::ReplicationState;
use crate::units::{ByteRate, Bytes, DiskPressure};
use sqlx::postgres::{PgPool, PgPoolOptions};
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
    /// An allocation write was rejected by the epoch fence: a higher-epoch leader
    /// exists, so this instance is deposed. Surfaced (rather than a false `Ok`) so
    /// a split-brain leader learns it lost rather than reporting itself as acting.
    #[error("allocation write fenced: epoch {epoch} is no longer the leader")]
    Fenced {
        /// The (now-deposed) epoch whose write was rejected.
        epoch: u64,
    },
}

type Result<T> = core::result::Result<T, StoreError>;

/// Checked `u64 -> i64` for storing a byte count in a BIGINT.
fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| StoreError::OutOfRange { value })
}

/// Checked `i64 -> u64` for a column constrained non-negative.
fn nonneg_u64(field: &'static str, value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| StoreError::Invalid { field, value })
}

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

#[derive(sqlx::FromRow)]
struct NodeStateRow {
    node_id: String,
    pressure_bps: i32,
    backlog_bytes: i64,
    max_drain_rate: i64,
    observed_p99_ns: i64,
    error_bps: i32,
}

impl NodeStateRow {
    fn into_domain(self) -> Result<(NodeId, NodeObservation)> {
        let invalid = |field: &'static str, value: i64| StoreError::Invalid { field, value };
        let node = NodeId::try_from(self.node_id).map_err(|_| invalid("node_id", 0))?;
        let pressure_bps = u16::try_from(self.pressure_bps).map_err(|_| invalid("pressure_bps", i64::from(self.pressure_bps)))?;
        let pressure = DiskPressure::try_from(pressure_bps).map_err(|_| invalid("pressure_bps", i64::from(self.pressure_bps)))?;
        let error_bps = u16::try_from(self.error_bps).map_err(|_| invalid("error_bps", i64::from(self.error_bps)))?;
        let observation = NodeObservation {
            pressure,
            backlog: Bytes::new(nonneg_u64("backlog_bytes", self.backlog_bytes)?),
            max_drain_rate: ByteRate::new(nonneg_u64("max_drain_rate", self.max_drain_rate)?),
            observed_p99: Duration::from_nanos(nonneg_u64("observed_p99_ns", self.observed_p99_ns)?),
            error_bps,
        };
        Ok((node, observation))
    }
}

#[derive(sqlx::FromRow)]
struct AllocationRow {
    budget_bytes: i64,
    fencing_epoch: i64,
}

/// A node's current allocation, read back from the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredAllocation {
    /// The allocated write rate.
    pub budget: ByteRate,
    /// The leader epoch that wrote it (for fencing checks).
    pub epoch: u64,
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

    fn into_claimed(self) -> Result<ClaimedPart> {
        Ok(ClaimedPart::new(self.into_part()?))
    }

    fn into_pending(self) -> Result<PendingPart> {
        Ok(PendingPart { part: self.into_part()? })
    }
}

/// A held leadership lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// The fencing epoch for this leadership era. Every allocation write made
    /// while holding this lease is stamped with it, so a deposed leader's
    /// lower-epoch writes are rejected.
    pub epoch: u64,
}

/// The default claim lease: a `draining` row whose claim is older than this is
/// treated as abandoned and re-claimable. Long enough that an ordinary slow
/// drain is never reclaimed out from under a live agent, short enough that a
/// crashed claim recovers promptly. Override per deployment via
/// [`Store::with_claim_lease`].
const DEFAULT_CLAIM_LEASE: Duration = Duration::from_mins(5);

/// Handle to the Postgres central state. Cheap to clone (shares the pool).
#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
    /// How long a `draining` claim is honored before [`Store::claim_chunk`]
    /// treats it as abandoned and re-claims it (the H1 crash-recovery TTL).
    claim_lease: Duration,
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
            node_id: None,
        })
    }

    /// Wraps an existing pool (used by the integration tests).
    #[must_use]
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            claim_lease: DEFAULT_CLAIM_LEASE,
            node_id: None,
        }
    }

    /// Sets the claim lease TTL (the daemon wires this from config). A
    /// `draining` claim older than `lease` is re-claimable by another agent.
    #[must_use]
    pub fn with_claim_lease(mut self, lease: Duration) -> Self {
        self.claim_lease = lease;
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

    /// Upserts a node's heartbeat with a server-stamped `updated_at`.
    ///
    /// # Errors
    ///
    /// [`StoreError::OutOfRange`] if a byte count exceeds i64; [`StoreError::Database`] on failure.
    pub async fn upsert_node_state(&self, node: &NodeId, observation: &NodeObservation) -> Result<()> {
        // p99 nanos: u128 -> i64 cannot overflow for any realistic latency; clamp defensively.
        let p99_ns = i64::try_from(observation.observed_p99.as_nanos()).unwrap_or(i64::MAX);
        sqlx::query(
            "INSERT INTO cephor_node_state \
                (node_id, pressure_bps, backlog_bytes, max_drain_rate, observed_p99_ns, error_bps, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, now()) \
             ON CONFLICT (node_id) DO UPDATE SET \
                pressure_bps = $2, backlog_bytes = $3, max_drain_rate = $4, \
                observed_p99_ns = $5, error_bps = $6, updated_at = now()",
        )
        .bind(node.as_str())
        .bind(i32::from(observation.pressure.bps()))
        .bind(to_i64(observation.backlog.get())?)
        .bind(to_i64(observation.max_drain_rate.get())?)
        .bind(p99_ns)
        .bind(i32::from(observation.error_bps))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Loads every heartbeat fresher than `stale_after` into a [`FleetView`].
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on query failure; [`StoreError::Invalid`] if a stored row violates a domain invariant.
    pub async fn load_fleet(&self, stale_after: Duration) -> Result<FleetView> {
        let rows = sqlx::query_as::<_, NodeStateRow>(
            "SELECT node_id, pressure_bps, backlog_bytes, max_drain_rate, observed_p99_ns, error_bps \
             FROM cephor_node_state \
             WHERE updated_at >= now() - make_interval(secs => $1)",
        )
        .bind(stale_after.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;
        let mut fleet = FleetView::new();
        for row in rows {
            let (node, observation) = row.into_domain()?;
            fleet.insert(node, observation);
        }
        Ok(fleet)
    }

    /// Writes per-node allocations stamped with `epoch`, fenced so a lower epoch
    /// cannot overwrite a higher one — a deposed leader's writes are ignored.
    ///
    /// # Errors
    ///
    /// [`StoreError::OutOfRange`] if a budget exceeds i64; [`StoreError::Database`] on failure.
    pub async fn write_allocations(&self, epoch: u64, allocations: &[Allocation]) -> Result<()> {
        let epoch_i = to_i64(epoch)?;
        let mut tx = self.pool.begin().await?;
        let mut present: Vec<&str> = Vec::with_capacity(allocations.len());
        for allocation in allocations {
            let result = sqlx::query(
                "INSERT INTO cephor_allocation (node_id, budget_bytes, fencing_epoch, updated_at) \
                 VALUES ($1, $2, $3, now()) \
                 ON CONFLICT (node_id) DO UPDATE SET \
                    budget_bytes = $2, fencing_epoch = $3, updated_at = now() \
                 WHERE cephor_allocation.fencing_epoch <= $3",
            )
            .bind(allocation.node.as_str())
            .bind(to_i64(allocation.budget.get())?)
            .bind(epoch_i)
            .execute(&mut *tx)
            .await?;
            // A 0-row upsert means the ON CONFLICT fence (fencing_epoch <= $3)
            // rejected the write: a higher-epoch leader exists, so this instance is
            // deposed. Surface it (the early return drops the tx, rolling back)
            // rather than committing a no-op and reporting a false success — the same
            // discipline as claim_chunk's ClaimLost. The fence is plan-wide: if one
            // node is fenced this leader has lost, so we abandon the rest.
            if result.rows_affected() == 0 {
                return Err(StoreError::Fenced { epoch });
            }
            present.push(allocation.node.as_str());
        }
        // Conserve the fleet-wide budget: a node absent from this plan has left the
        // fleet, so zero its lingering allotment instead of leaving a stale (often
        // large) budget that inflates the table-wide sum past the Ceph ceiling
        // (audit M1). Fenced by `fencing_epoch <= $2` so a deposed leader cannot
        // zero the acting leader's writes, mirroring the upsert fence. The row is
        // kept (not deleted) so its epoch keeps fencing later writes; `updated_at`
        // is set to the epoch sentinel so the agent reads it as stale and decays
        // toward its floor rather than enforcing the zero.
        sqlx::query(
            "UPDATE cephor_allocation SET budget_bytes = 0, updated_at = to_timestamp(0) \
             WHERE fencing_epoch <= $1 AND node_id <> ALL($2) AND budget_bytes <> 0",
        )
        .bind(epoch_i)
        .bind(&present)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Reads a node's current allocation, if one exists.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`] on failure; [`StoreError::Invalid`] if a stored value is out of domain range.
    pub async fn load_allocation(&self, node: &NodeId, stale_after: Duration) -> Result<Option<StoredAllocation>> {
        // A row not refreshed within `stale_after` is treated as absent: the leader
        // has gone silent on this node (it left the fleet, or the node is
        // partitioned), so the agent must fall back to decaying toward its floor
        // rather than re-pinning a lingering budget. This is what makes the
        // designed decay-toward-floor path reachable (audit M1). A departed row the
        // allocator zeroed carries a sentinel-epoch `updated_at`, so it reads stale
        // here regardless of its (zero) budget.
        let row = sqlx::query_as::<_, AllocationRow>(
            "SELECT budget_bytes, fencing_epoch FROM cephor_allocation \
             WHERE node_id = $1 AND updated_at > now() - $2 * interval '1 second'",
        )
        .bind(node.as_str())
        .bind(stale_after.as_secs_f64())
        .fetch_optional(&self.pool)
        .await?;
        match row {
            None => Ok(None),
            Some(row) => Ok(Some(StoredAllocation {
                budget: ByteRate::new(nonneg_u64("budget_bytes", row.budget_bytes)?),
                epoch: nonneg_u64("fencing_epoch", row.fencing_epoch)?,
            })),
        }
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

    /// Acquires or renews the singleton leadership lease for `instance_id`.
    ///
    /// Returns `Some(Lease)` when this instance holds leadership after the call
    /// (a fresh acquisition, a takeover of an expired lease, or a renewal of its
    /// own), or `None` when another instance holds an unexpired lease. Re-acquiring
    /// any *expired* lease — including this instance's own, lapsed after a freeze —
    /// bumps the epoch (a new leadership era), so a stale in-flight write from the
    /// previous era is fenced; only renewing a still-valid own lease keeps the
    /// epoch. All time comparisons use the database clock, so agents' clock skew
    /// does not affect election.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn acquire_or_renew_leadership(&self, instance_id: &str, ttl: Duration) -> Result<Option<Lease>> {
        let row = sqlx::query_as::<_, (i64,)>(
            "INSERT INTO cephor_leader_lease (only_row, instance_id, epoch, expires_at) \
             VALUES (TRUE, $1, 1, now() + make_interval(secs => $2)) \
             ON CONFLICT (only_row) DO UPDATE SET \
                instance_id = $1, \
                epoch = CASE WHEN cephor_leader_lease.instance_id = $1 AND cephor_leader_lease.expires_at >= now() \
                             THEN cephor_leader_lease.epoch \
                             ELSE cephor_leader_lease.epoch + 1 END, \
                expires_at = now() + make_interval(secs => $2) \
             WHERE cephor_leader_lease.expires_at < now() OR cephor_leader_lease.instance_id = $1 \
             RETURNING epoch",
        )
        .bind(instance_id)
        .bind(ttl.as_secs_f64())
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((epoch,)) => Ok(Some(Lease {
                epoch: nonneg_u64("epoch", epoch)?,
            })),
            None => Ok(None),
        }
    }

    /// Relinquishes leadership held by `instance_id` — the allocator binary's
    /// graceful-shutdown handoff (called on SIGTERM/SIGINT once the
    /// `hippius-drain-allocator` runtime exists; today its entry point is a skeleton).
    ///
    /// Best-effort by design: deleting the lease lets a successor take over on its
    /// next tick *immediately* rather than waiting out the TTL, but it is purely an
    /// optimization. If the call is skipped — a crash, a `SIGKILL`, a network
    /// partition, or simply the not-yet-built binary — the lease's `expires_at` TTL
    /// is the backstop: the row ages out and `acquire_or_renew_leadership` hands a
    /// new epoch to the next acquirer. So leadership is never permanently stranded
    /// whether or not this runs; it only shortens the failover gap.
    ///
    /// # Errors
    ///
    /// [`StoreError::Database`].
    pub async fn relinquish_leadership(&self, instance_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM cephor_leader_lease WHERE instance_id = $1 AND only_row")
            .bind(instance_id)
            .execute(&self.pool)
            .await?;
        Ok(())
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
        let row = sqlx::query_as::<_, PartRow>(
            "UPDATE cephor_replication_status SET status = 'draining', updated_at = now(), claimed_at = now() \
             WHERE (object_id, version, part_number) IN ( \
                SELECT object_id, version, part_number FROM cephor_replication_status \
                WHERE node_id = $2 \
                  AND ( status = 'pending' \
                        OR (status = 'draining' AND claimed_at < now() - $1 * interval '1 second') ) \
                ORDER BY landed_at \
                FOR UPDATE SKIP LOCKED LIMIT 1 \
             ) RETURNING object_id, version, part_number",
        )
        .bind(self.claim_lease.as_secs_f64())
        .bind(self.node_id.as_deref())
        .fetch_optional(&self.pool)
        .await?;
        row.map(PartRow::into_claimed).transpose()
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
        sqlx::query(
            "UPDATE cephor_replication_status SET status = 'pending', updated_at = now(), claimed_at = NULL \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining'",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
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
        // Guard on `draining`: only a part this agent claimed may be committed. Zero
        // rows means the claim was lost (reset/re-claimed), so the caller must not
        // unlink the SSD copy — surface PartClaimLost instead of a false Ok.
        let part = claim.part();
        let affected = sqlx::query(
            "UPDATE cephor_replication_status SET status = 'replicated', updated_at = now() \
             WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'draining'",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
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
        // Terminal sink: a corrupt part is marked Failed regardless of its prior
        // state, and an already-absent row is a harmless no-op (idempotent).
        let part = claim.part();
        sqlx::query(
            "UPDATE cephor_replication_status SET status = 'failed', updated_at = now() \
             WHERE object_id = $1 AND version = $2 AND part_number = $3",
        )
        .bind(part.object().as_str())
        .bind(i64::from(part.version().get()))
        .bind(i64::from(part.part().get()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// The part reconciler's view of the store: read a part's status and record a
/// freshly-landed one. Delegates to the [`PartReplicationStore::status`] and the
/// inherent [`Store::record_landed_part`], so the reconciler shares one definition
/// of each operation with the drain path.
impl PartLandingLog for Store {
    type Error = StoreError;

    async fn status(&self, part: &PartKey) -> Result<Option<ReplicationState>> {
        <Self as PartReplicationStore>::status(self, part).await
    }

    async fn record_landed(&self, part: &PartKey) -> Result<()> {
        Store::record_landed_part(self, part).await
    }
}

#[cfg(test)]
#[cfg(feature = "pg")]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::{Store, StoreError, StoredAllocation};
    use crate::alloc::{Allocation, NodeObservation};
    use crate::ids::{FileId, NodeId};
    use crate::units::{ByteRate, Bytes, DiskPressure};
    use core::str::FromStr;
    use sqlx::postgres::PgPool;
    use std::time::Duration;

    fn observation(pressure_bps: u16, backlog: u64, rate: u64) -> NodeObservation {
        NodeObservation {
            pressure: DiskPressure::try_from(pressure_bps).unwrap(),
            backlog: Bytes::new(backlog),
            max_drain_rate: ByteRate::new(rate),
            observed_p99: Duration::from_millis(12),
            error_bps: 5,
        }
    }

    #[sqlx::test]
    async fn heartbeat_round_trips(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("node-a").unwrap();
        store.upsert_node_state(&node, &observation(4_200, 9_000_000, 5_000_000)).await.unwrap();

        let fleet = store.load_fleet(Duration::from_hours(1)).await.unwrap();
        let (id, obs) = fleet.iter().next().expect("node present");
        assert_eq!(id.as_str(), "node-a");
        assert_eq!(obs.pressure.bps(), 4_200);
        assert_eq!(obs.backlog.get(), 9_000_000);
        assert_eq!(obs.max_drain_rate.get(), 5_000_000);
        assert_eq!(obs.error_bps, 5);
    }

    #[sqlx::test]
    async fn upsert_replaces_prior_heartbeat(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("node-a").unwrap();
        store.upsert_node_state(&node, &observation(1_000, 1, 1)).await.unwrap();
        store.upsert_node_state(&node, &observation(8_000, 2, 2)).await.unwrap();
        let fleet = store.load_fleet(Duration::from_hours(1)).await.unwrap();
        assert_eq!(fleet.len(), 1, "upsert must not create a second row");
        assert_eq!(fleet.iter().next().unwrap().1.pressure.bps(), 8_000);
    }

    #[sqlx::test]
    async fn stale_heartbeats_are_filtered_out(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("node-a").unwrap();
        store.upsert_node_state(&node, &observation(1_000, 1, 1)).await.unwrap();
        // A zero freshness window excludes a row stamped even microseconds ago.
        let fresh = store.load_fleet(Duration::ZERO).await.unwrap();
        assert!(fresh.is_empty(), "row stamped before the query instant is stale at window 0");
        assert_eq!(store.load_fleet(Duration::from_hours(1)).await.unwrap().len(), 1);
    }

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
    async fn allocation_write_and_read_back(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("node-a").unwrap();
        let alloc = Allocation {
            node: node.clone(),
            budget: ByteRate::new(123_456),
        };
        store.write_allocations(7, std::slice::from_ref(&alloc)).await.unwrap();
        let fresh = Duration::from_hours(1);
        assert_eq!(
            store.load_allocation(&node, fresh).await.unwrap(),
            Some(StoredAllocation {
                budget: ByteRate::new(123_456),
                epoch: 7
            })
        );
        assert_eq!(store.load_allocation(&NodeId::from_str("absent").unwrap(), fresh).await.unwrap(), None);
    }

    #[sqlx::test]
    async fn load_allocation_ignores_a_stale_row(pool: PgPool) {
        // M1: a row not refreshed within the staleness window reads as absent, so
        // the agent's decay-toward-floor path engages instead of re-pinning a
        // lingering budget. Backdating updated_at is the only way to fast-forward
        // the staleness clock deterministically.
        let store = Store::from_pool(pool.clone());
        let node = NodeId::from_str("node-a").unwrap();
        let alloc = Allocation {
            node: node.clone(),
            budget: ByteRate::new(50_000_000),
        };
        store.write_allocations(1, std::slice::from_ref(&alloc)).await.unwrap();
        sqlx::query("UPDATE cephor_allocation SET updated_at = now() - interval '1 hour' WHERE node_id = $1")
            .bind(node.as_str())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            store.load_allocation(&node, Duration::from_secs(10)).await.unwrap(),
            None,
            "a row older than the staleness window reads as absent",
        );
    }

    #[sqlx::test]
    async fn write_allocations_zeroes_a_departed_node(pool: PgPool) {
        // A node present one tick, gone the next: its budget is zeroed (table-wide
        // conservation) and the sentinel updated_at makes it read as stale.
        let store = Store::from_pool(pool);
        let (a, b) = (NodeId::from_str("node-a").unwrap(), NodeId::from_str("node-b").unwrap());
        let fresh = Duration::from_hours(1);
        let plan = |budget| Allocation {
            node: a.clone(),
            budget: ByteRate::new(budget),
        };
        // Tick 1: both nodes present.
        store
            .write_allocations(
                1,
                &[
                    plan(40_000_000),
                    Allocation {
                        node: b.clone(),
                        budget: ByteRate::new(60_000_000),
                    },
                ],
            )
            .await
            .unwrap();
        // Tick 2: only A heartbeats; B departed. B's row must be zeroed and stale.
        store.write_allocations(2, std::slice::from_ref(&plan(80_000_000))).await.unwrap();
        assert_eq!(
            store.load_allocation(&a, fresh).await.unwrap().map(|s| s.budget),
            Some(ByteRate::new(80_000_000)),
            "the present node keeps its fresh allocation",
        );
        assert_eq!(
            store.load_allocation(&b, fresh).await.unwrap(),
            None,
            "the departed node's zeroed, sentinel-dated row reads as absent",
        );
    }

    #[sqlx::test]
    async fn lower_epoch_cannot_overwrite_a_higher_one(pool: PgPool) {
        let store = Store::from_pool(pool);
        let node = NodeId::from_str("node-a").unwrap();
        let at = |epoch, budget| {
            let a = Allocation {
                node: node.clone(),
                budget: ByteRate::new(budget),
            };
            (epoch, a)
        };
        let (e5, a5) = at(5, 500);
        store.write_allocations(e5, std::slice::from_ref(&a5)).await.unwrap();
        // A deposed leader (epoch 3) must not overwrite — and learns it was fenced
        // rather than getting a false success (M4 split-brain visibility).
        let (e3, a3) = at(3, 300);
        let err = store.write_allocations(e3, std::slice::from_ref(&a3)).await.unwrap_err();
        assert!(
            matches!(err, StoreError::Fenced { epoch: 3 }),
            "a fenced write surfaces StoreError::Fenced, got {err:?}",
        );
        let fresh = Duration::from_hours(1);
        assert_eq!(
            store.load_allocation(&node, fresh).await.unwrap().unwrap().epoch,
            5,
            "stale epoch was fenced out"
        );
        // A newer leader (epoch 6) takes over.
        let (e6, a6) = at(6, 600);
        store.write_allocations(e6, std::slice::from_ref(&a6)).await.unwrap();
        assert_eq!(store.load_allocation(&node, fresh).await.unwrap().unwrap().budget, ByteRate::new(600));
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

    #[sqlx::test]
    async fn leadership_is_exclusive_until_expiry(pool: PgPool) {
        use super::Lease;
        let store = Store::from_pool(pool);
        let long = Duration::from_secs(30);
        assert_eq!(store.acquire_or_renew_leadership("a", long).await.unwrap(), Some(Lease { epoch: 1 }));
        // A different instance cannot take an unexpired lease.
        assert_eq!(store.acquire_or_renew_leadership("b", long).await.unwrap(), None);
        // The holder renews; the epoch is unchanged.
        assert_eq!(store.acquire_or_renew_leadership("a", long).await.unwrap(), Some(Lease { epoch: 1 }));
    }

    #[sqlx::test]
    async fn expired_lease_is_taken_over_with_a_new_epoch(pool: PgPool) {
        use super::Lease;
        let store = Store::from_pool(pool);
        assert_eq!(
            store.acquire_or_renew_leadership("a", Duration::from_millis(40)).await.unwrap(),
            Some(Lease { epoch: 1 })
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        // Taking over a *different* expired leader starts a new era -> epoch 2,
        // so the deposed leader's writes can be fenced out.
        assert_eq!(
            store.acquire_or_renew_leadership("b", Duration::from_secs(30)).await.unwrap(),
            Some(Lease { epoch: 2 })
        );
    }

    #[sqlx::test]
    async fn self_reacquire_after_expiry_starts_a_new_epoch(pool: PgPool) {
        use super::Lease;
        // M5: an instance that lets its OWN lease lapse (a long STW GC pause, a host
        // freeze, an NTP step) and then re-acquires must start a NEW epoch — not
        // keep the old one — so a stale in-flight write from the pre-freeze era is
        // fenced by the `<=` allocation fence instead of degrading to last-writer-wins.
        let store = Store::from_pool(pool.clone());
        let long = Duration::from_secs(30);
        assert_eq!(store.acquire_or_renew_leadership("a", long).await.unwrap(), Some(Lease { epoch: 1 }));
        // Force its own lease to expire (backdate is the only deterministic way).
        sqlx::query("UPDATE cephor_leader_lease SET expires_at = now() - interval '1 second'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            store.acquire_or_renew_leadership("a", long).await.unwrap(),
            Some(Lease { epoch: 2 }),
            "re-acquiring one's own expired lease starts a new era",
        );
    }

    #[sqlx::test]
    async fn relinquish_frees_the_lease(pool: PgPool) {
        use super::Lease;
        let store = Store::from_pool(pool);
        assert_eq!(
            store.acquire_or_renew_leadership("a", Duration::from_secs(30)).await.unwrap(),
            Some(Lease { epoch: 1 })
        );
        store.relinquish_leadership("a").await.unwrap();
        // The row is gone, so the next acquirer starts a fresh lease at epoch 1.
        assert_eq!(
            store.acquire_or_renew_leadership("b", Duration::from_secs(30)).await.unwrap(),
            Some(Lease { epoch: 1 })
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
    use crate::state::ReplicationState;
    use core::str::FromStr;
    use sqlx::postgres::PgPool;

    const UUID_A: &str = "466916c0-d61b-4518-b81b-9576b574270a";
    const UUID_B: &str = "00000000-0000-4000-8000-000000000000";

    fn part(uuid: &str, version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(uuid).unwrap(), Version::new(version), PartNumber::new(number))
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
        // row is still 'pending', so the 'draining' guard matches no row.
        let unclaimed = ClaimedPart::new(p.clone());
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
}
