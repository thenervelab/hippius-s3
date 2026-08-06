//! Redis-backed ephemeral coordination: leader lease, node heartbeats, and per-node
//! allocations.
//!
//! This is the loss-tolerant counterpart to the durable [`Store`](crate::Store). All three
//! kinds of state are TTL-keyed — expiry IS the death signal, so there is no reaper: a node
//! that stops heartbeating ages out of the fleet, a lease that is not renewed lapses, and a
//! budget the leader stops refreshing decays to absent (the agent then decays toward its
//! floor). Keeping this off Postgres spares it the ~2s leader-tick write churn (measured
//! 6995x/3659x/2023x autovacuum on staging) and removes a circular recovery dependency:
//! when Ceph degrades — the very scenario the drain exists for — PG's WAL fsync spikes, and
//! the coordinator must not need PG to write down what it is doing.
//!
//! The F4 leader-epoch fence: the epoch lives in the lease value (bumped on every takeover
//! via an `INCR` counter), and an allocation write is a compare-and-set in ONE atomic Lua
//! `EVAL` against BOTH the current-era counter (`cephor:epoch`, the R3 anchor — bumped at
//! election before any slow tick work) AND each node's stored epoch, so a deposed or
//! partitioned leader can never overwrite a live leader's budgets — including in the window
//! after a successor is elected but before it has written any allocation.

use crate::alloc::Allocation;
use crate::alloc::FleetView;
use crate::alloc::NodeObservation;
use crate::ids::NodeId;
use crate::units::ByteRate;
use crate::units::Bytes;
use crate::units::DiskPressure;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;
use thiserror::Error;

/// The default key namespace for every coordination key on the shared redis-queues
/// instance. Override per test via [`Coordinator::with_prefix`] for isolation.
const DEFAULT_PREFIX: &str = "cephor:";

/// Per-command response timeout (and per-attempt connection timeout) for the coordinator's
/// Redis manager. Bounds every command so a reachable-but-blocked redis-queues (an fsync
/// stall, a BGSAVE, a half-open TCP after a blip) surfaces as a retryable error instead of
/// hanging the allocator tick / agent heartbeat / allocation-pull loop indefinitely — the
/// error-retry paths only fire on an `Err`, never on a silent hang.
pub const DEFAULT_REDIS_TIMEOUT: Duration = Duration::from_secs(5);

/// Acquire-or-renew the singleton lease. `KEYS[1]`=lease, `KEYS[2]`=epoch counter;
/// `ARGV[1]`=instance id, `ARGV[2]`=ttl secs. Returns the held epoch, or nil if another
/// instance holds an unexpired lease. An absent (expired/fresh) lease bumps the epoch via
/// `INCR` — a new leadership era — so a stale in-flight write from the prior era is fenced;
/// renewing one's own still-valid lease keeps the epoch. Expiry is the database clock's, so
/// agents' clock skew does not affect election.
const LEASE_SCRIPT: &str = r"
local cur = redis.call('GET', KEYS[1])
if cur then
  local v = cjson.decode(cur)
  if v.instance == ARGV[1] then
    redis.call('SET', KEYS[1], cur, 'EX', ARGV[2])
    return v.epoch
  end
  return nil
end
local epoch = redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], cjson.encode({instance = ARGV[1], epoch = epoch}), 'EX', ARGV[2])
return epoch
";

/// Relinquish the lease iff this instance still holds it. `KEYS[1]`=lease, `ARGV[1]`=instance.
/// A no-op when a successor already took over (a deposed instance must not delete the live
/// leader's lease). Always returns 1 — best-effort, the TTL is the backstop.
const RELINQUISH_SCRIPT: &str = r"
local cur = redis.call('GET', KEYS[1])
if cur then
  local v = cjson.decode(cur)
  if v.instance == ARGV[1] then
    redis.call('DEL', KEYS[1])
  end
end
return 1
";

/// Fenced per-node allocation write. `KEYS[1]`=the epoch counter (`cephor:epoch`);
/// `KEYS[2..]`=the alloc keys (one per node); `ARGV[1]`=epoch, `ARGV[2]`=ttl secs, then the
/// budgets for each alloc key (parallel to `KEYS[2..]`), then the reserves in the same order.
/// Appending the reserves after the budgets — rather than interleaving — keeps the budget
/// indexing identical to before. Returns 0 — writing NOTHING — if this leader is deposed,
/// else writes every alloc key and returns 1.
///
/// The deposed test is TWO checks, both against `epoch`:
/// 1. **Current-era fence (R3):** the counter at `KEYS[1]` is `INCR`'d to the new era the
///    instant a successor wins the lease ([`LEASE_SCRIPT`]), BEFORE it does any slow work
///    (`load_fleet`/`ceiling`) or writes any alloc key. So a deposed leader that finishes a
///    slow tick and writes at its stale epoch is rejected here even when NO alloc key has
///    been written at the new era yet — the window a per-alloc-key check alone misses.
/// 2. **Per-alloc-key fence:** reject if any node's already-stored epoch is higher. Kept as
///    a secondary guard (and the sole guard for a direct write made without a prior election,
///    e.g. in tests, where `KEYS[1]` is absent).
///
/// The passes (check all, then write all) make the fence plan-wide and atomic: a split-brain
/// leader cannot partially overwrite budgets.
const WRITE_ALLOC_SCRIPT: &str = r"
local epoch = tonumber(ARGV[1])
local ttl = ARGV[2]
local current = redis.call('GET', KEYS[1])
if current and tonumber(current) > epoch then
  return 0
end
for i = 2, #KEYS do
  local cur = redis.call('GET', KEYS[i])
  if cur then
    local v = cjson.decode(cur)
    if tonumber(v.epoch) > epoch then
      return 0
    end
  end
end
for i = 2, #KEYS do
  redis.call('SET', KEYS[i], cjson.encode({budget = ARGV[1 + i], reserve = ARGV[#KEYS + i], epoch = epoch}), 'EX', ttl)
end
return 1
";

/// Errors from the Redis-backed coordinator.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CoordError {
    /// A Redis command or connection failed.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
    /// (De)serializing a stored value failed.
    #[error("coordination serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    /// A stored value violated its domain type's invariant when read back. The writers
    /// only store in-range values, so this is corruption / drift, surfaced not panicked.
    #[error("stored {field} value is not valid for its domain type")]
    Invalid {
        /// The field whose stored value was invalid.
        field: &'static str,
    },
    /// An allocation write was rejected by the epoch fence: a higher-epoch leader exists,
    /// so this instance is deposed. Surfaced (rather than a false `Ok`) so a split-brain
    /// leader learns it lost. Mirrors the old PG `StoreError::Fenced`.
    #[error("allocation write fenced: epoch {epoch} is no longer the leader")]
    Fenced {
        /// The (now-deposed) epoch whose write was rejected.
        epoch: u64,
    },
}

type Result<T> = core::result::Result<T, CoordError>;

/// A held leadership lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// The fencing epoch for this leadership era. Every allocation write made while
    /// holding this lease is stamped with it, so a deposed leader's lower-epoch writes
    /// are rejected.
    pub epoch: u64,
}

/// A node's current allocation, read back from the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredAllocation {
    /// The allocated write rate.
    pub budget: ByteRate,
    /// The leader epoch that wrote it (for fencing checks).
    pub epoch: u64,
    /// The free-space floor the evictor should hold, in permille of disk. `None` when the
    /// writing leader predates Phase 4, in which case the agent keeps its configured floor.
    pub reserve_permille: Option<u16>,
}

/// The wire form of a node heartbeat. Stored as JSON under each node's TTL'd key; a DTO
/// (not serde on the domain type) so deserialization runs through the validating
/// conversions below rather than bypassing each newtype's invariant.
#[derive(Serialize, Deserialize)]
struct NodeStateJson {
    pressure_bps: u16,
    backlog_bytes: u64,
    max_drain_rate_bps: u64,
    observed_p99_ns: u64,
    error_bps: u16,
    /// Residency, absent from heartbeats written by a pre-retention agent. `Option` (not
    /// `#[serde(default)]` to zero) because "not reported" and "reported as zero" must stay
    /// distinguishable: zeroes would derive full drain pressure for any backlogged node and
    /// hand every un-rolled node a reservation floor mid-roll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    free_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_bytes: Option<u64>,
}

impl NodeStateJson {
    fn from_observation(observation: &NodeObservation) -> Self {
        Self {
            pressure_bps: observation.pressure.bps(),
            backlog_bytes: observation.backlog.get(),
            max_drain_rate_bps: observation.max_drain_rate.get(),
            // p99 nanos: u128 -> u64 cannot overflow for any realistic latency; clamp defensively.
            observed_p99_ns: u64::try_from(observation.observed_p99.as_nanos()).unwrap_or(u64::MAX),
            error_bps: observation.error_bps,
            free_bytes: Some(observation.free.get()),
            cache_bytes: Some(observation.cache.get()),
        }
    }

    /// Derives drain urgency from residency when the reporting agent sent it, else falls back
    /// to the raw fullness that agent measured.
    ///
    /// This is the one place the two heartbeat generations are reconciled, so the allocator
    /// core downstream sees a single meaning for `pressure` regardless of which agents have
    /// rolled. The fallback arm is dead once every agent reports residency and should be
    /// deleted then, along with the `Option`s.
    fn into_observation(self) -> Result<NodeObservation> {
        let reported = DiskPressure::try_from(self.pressure_bps).map_err(|_| CoordError::Invalid { field: "pressure_bps" })?;
        let backlog = Bytes::new(self.backlog_bytes);
        let (pressure, free, cache) = match (self.free_bytes, self.cache_bytes) {
            (Some(free), Some(cache)) => (
                DiskPressure::from_drain_demand(backlog, Bytes::new(free), Bytes::new(cache)),
                Bytes::new(free),
                Bytes::new(cache),
            ),
            _ => (reported, Bytes::ZERO, Bytes::ZERO),
        };
        Ok(NodeObservation {
            pressure,
            backlog,
            free,
            cache,
            max_drain_rate: ByteRate::new(self.max_drain_rate_bps),
            observed_p99: Duration::from_nanos(self.observed_p99_ns),
            error_bps: self.error_bps,
        })
    }
}

/// The wire form of a per-node allocation written by [`WRITE_ALLOC_SCRIPT`]. `budget` is a
/// decimal string, not a JSON number, so a multi-gigabyte/sec budget keeps full `u64`
/// precision through Lua's double-only number type.
#[derive(Deserialize)]
struct AllocJson {
    budget: String,
    epoch: u64,
    /// Absent from allocations written by a pre-Phase-4 leader. `Option` so a rolling
    /// allocator update degrades to the agent's configured static reserve rather than
    /// deserializing to zero — which would read as "hold no free space at all".
    #[serde(default)]
    reserve: Option<String>,
}

/// Handle to the Redis-backed coordination state. Cheap to clone (the `ConnectionManager`
/// is a shared multiplexed handle).
#[derive(Clone)]
pub struct Coordinator {
    conn: ConnectionManager,
    prefix: String,
    /// TTL on each heartbeat key — the fleet-staleness window (the agent sets this from
    /// `CEPHOR_HEARTBEAT_TTL_SECS`). Only [`upsert_node_state`](Self::upsert_node_state)
    /// uses it; the allocator's coordinator leaves it at a default it never reads.
    node_ttl: Duration,
    /// TTL on each allocation key — past it the agent reads no allocation and decays toward
    /// its floor (the allocator sets this from `CEPHOR_ALLOCATION_TTL_SECS`). Only
    /// [`write_allocations`](Self::write_allocations) uses it.
    alloc_ttl: Duration,
}

// Manual Debug: redis's `ConnectionManager` is not `Debug`, so derive can't apply.
impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator")
            .field("prefix", &self.prefix)
            .field("node_ttl", &self.node_ttl)
            .field("alloc_ttl", &self.alloc_ttl)
            .finish_non_exhaustive()
    }
}

impl Coordinator {
    /// Connects a multiplexed, auto-reconnecting manager to `url`.
    ///
    /// # Errors
    ///
    /// [`CoordError::Redis`] if the client cannot be opened or the connection established.
    pub async fn connect(url: &str, node_ttl: Duration, alloc_ttl: Duration) -> Result<Self> {
        // Bound every command with a response timeout ([`DEFAULT_REDIS_TIMEOUT`]) so a
        // reachable-but-blocked redis-queues surfaces as a retryable error rather than
        // hanging a coordination loop; the connection timeout bounds the manager's
        // (re)connect after a blip.
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(DEFAULT_REDIS_TIMEOUT)
            .set_connection_timeout(DEFAULT_REDIS_TIMEOUT);
        let conn = ConnectionManager::new_with_config(redis::Client::open(url)?, config).await?;
        Ok(Self::new(conn, node_ttl, alloc_ttl))
    }

    /// Builds a coordinator over an existing connection manager — used by the agent, which
    /// shares one manager with its upload enqueuer (the same redis-queues instance).
    #[must_use]
    pub fn new(conn: ConnectionManager, node_ttl: Duration, alloc_ttl: Duration) -> Self {
        Self {
            conn,
            prefix: DEFAULT_PREFIX.to_owned(),
            node_ttl,
            alloc_ttl,
        }
    }

    /// Overrides the key namespace (tests isolate parallel runs with a unique prefix).
    #[must_use]
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        prefix.clone_into(&mut self.prefix);
        self
    }

    fn leader_key(&self) -> String {
        format!("{}leader", self.prefix)
    }

    fn epoch_key(&self) -> String {
        format!("{}epoch", self.prefix)
    }

    fn nodes_key(&self) -> String {
        format!("{}nodes", self.prefix)
    }

    fn node_key(&self, node: &NodeId) -> String {
        format!("{}node:{}", self.prefix, node.as_str())
    }

    fn alloc_key(&self, node: &NodeId) -> String {
        format!("{}alloc:{}", self.prefix, node.as_str())
    }

    /// Acquires or renews the singleton leadership lease for `instance_id`.
    ///
    /// Returns `Some(Lease)` when this instance holds leadership after the call (a fresh
    /// acquisition, a takeover of an expired lease, or a renewal of its own — a takeover
    /// bumps the epoch, a renewal keeps it), or `None` when another instance holds an
    /// unexpired lease.
    ///
    /// # Errors
    ///
    /// [`CoordError::Redis`] on a command failure; [`CoordError::Invalid`] if the stored
    /// epoch is out of range.
    pub async fn acquire_or_renew_leadership(&self, instance_id: &str, ttl: Duration) -> Result<Option<Lease>> {
        let mut conn = self.conn.clone();
        let epoch: Option<i64> = redis::Script::new(LEASE_SCRIPT)
            .key(self.leader_key())
            .key(self.epoch_key())
            .arg(instance_id)
            .arg(ttl.as_secs().max(1))
            .invoke_async(&mut conn)
            .await?;
        match epoch {
            Some(epoch) => Ok(Some(Lease {
                epoch: nonneg_u64("epoch", epoch)?,
            })),
            None => Ok(None),
        }
    }

    /// Relinquishes leadership held by `instance_id` — the allocator's graceful-shutdown
    /// handoff. Best-effort: a no-op if a successor already took over, and the lease TTL is
    /// the backstop if this is skipped (crash / `SIGKILL` / partition).
    ///
    /// # Errors
    ///
    /// [`CoordError::Redis`] on a command failure.
    pub async fn relinquish_leadership(&self, instance_id: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i64 = redis::Script::new(RELINQUISH_SCRIPT)
            .key(self.leader_key())
            .arg(instance_id)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Publishes (upserts) this node's heartbeat with a fresh TTL, indexing it in the
    /// fleet set so [`load_fleet`](Self::load_fleet) need not scan the keyspace.
    ///
    /// # Errors
    ///
    /// [`CoordError::Serde`] if the observation cannot serialize; [`CoordError::Redis`]
    /// on a command failure.
    pub async fn upsert_node_state(&self, node: &NodeId, observation: &NodeObservation) -> Result<()> {
        let payload = serde_json::to_string(&NodeStateJson::from_observation(observation))?;
        let mut conn = self.conn.clone();
        let _: () = conn.set_ex(self.node_key(node), payload, self.node_ttl.as_secs().max(1)).await?;
        let _: () = conn.sadd(self.nodes_key(), node.as_str()).await?;
        Ok(())
    }

    /// Loads every live heartbeat into a [`FleetView`]. A node whose key has TTL-expired is
    /// pruned from the index set in passing, so a departed node drops out of the fleet
    /// without a reaper.
    ///
    /// # Errors
    ///
    /// [`CoordError::Redis`] on a command failure; [`CoordError::Serde`]/[`CoordError::Invalid`]
    /// if a stored heartbeat is malformed.
    pub async fn load_fleet(&self) -> Result<FleetView> {
        let mut conn = self.conn.clone();
        let ids: Vec<String> = conn.smembers(self.nodes_key()).await?;
        let mut fleet = FleetView::new();
        if ids.is_empty() {
            return Ok(fleet);
        }
        let keys: Vec<String> = ids.iter().map(|id| format!("{}node:{id}", self.prefix)).collect();
        let values: Vec<Option<String>> = conn.mget(&keys).await?;
        let mut expired: Vec<&str> = Vec::new();
        for (id, value) in ids.iter().zip(values) {
            match value {
                Some(json) => {
                    let node = NodeId::try_from(id.clone()).map_err(|_| CoordError::Invalid { field: "node_id" })?;
                    let observation = serde_json::from_str::<NodeStateJson>(&json)?.into_observation()?;
                    fleet.insert(node, observation);
                }
                None => expired.push(id),
            }
        }
        if !expired.is_empty() {
            let _: () = conn.srem(self.nodes_key(), expired).await?;
        }
        Ok(fleet)
    }

    /// Writes per-node allocations stamped with `epoch`, fenced atomically so a lower epoch
    /// cannot overwrite a higher one (the deposed-leader guard). A node dropped from the plan
    /// is conserved by its key's TTL expiring (no zeroing pass needed, unlike the old PG path).
    ///
    /// An empty plan writes nothing but is NOT a bare `Ok` short-circuit: it still runs the
    /// current-era fence (the EVAL sees only `KEYS[1]` = `cephor:epoch`, so both of the
    /// script's write loops — which start at `KEYS[2]` — are empty). This preserves the
    /// split-brain invariant on the empty-plan path: a deposed leader whose tick allocated
    /// nothing (no hungry nodes) still learns it lost via [`CoordError::Fenced`] rather than a
    /// false `Ok` that `run_tick` would surface as a stale `Led`.
    ///
    /// # Errors
    ///
    /// [`CoordError::Fenced`] if a higher-epoch leader exists (nothing was written);
    /// [`CoordError::Redis`] on a command failure.
    pub async fn write_allocations(&self, epoch: u64, allocations: &[Allocation]) -> Result<()> {
        let mut conn = self.conn.clone();
        let script = redis::Script::new(WRITE_ALLOC_SCRIPT);
        let mut invocation = script.prepare_invoke();
        // KEYS[1] is the epoch counter (the current-era fence anchor); the alloc keys follow.
        // On an empty plan this is the only key, so the EVAL is a pure fence — reads
        // `cephor:epoch`, mutates nothing, returns Fenced-or-ok.
        invocation.key(self.epoch_key());
        for allocation in allocations {
            invocation.key(self.alloc_key(&allocation.node));
        }
        invocation.arg(epoch).arg(self.alloc_ttl.as_secs().max(1));
        for allocation in allocations {
            // Budget as a decimal string preserves full u64 precision past Lua's doubles.
            invocation.arg(allocation.budget.get().to_string());
        }
        for allocation in allocations {
            invocation.arg(allocation.reserve_permille.to_string());
        }
        let written: i64 = invocation.invoke_async(&mut conn).await?;
        if written == 0 {
            return Err(CoordError::Fenced { epoch });
        }
        Ok(())
    }

    /// Reads a node's current allocation, if one exists (absent = TTL-expired or never
    /// written, so the caller decays toward its floor).
    ///
    /// # Errors
    ///
    /// [`CoordError::Redis`] on a command failure; [`CoordError::Serde`]/[`CoordError::Invalid`]
    /// if the stored value is malformed.
    pub async fn load_allocation(&self, node: &NodeId) -> Result<Option<StoredAllocation>> {
        let mut conn = self.conn.clone();
        let value: Option<String> = conn.get(self.alloc_key(node)).await?;
        match value {
            None => Ok(None),
            Some(json) => {
                let allocation: AllocJson = serde_json::from_str(&json)?;
                let budget = allocation.budget.parse::<u64>().map_err(|_| CoordError::Invalid { field: "budget" })?;
                // A malformed reserve degrades to None (the agent's static floor) rather than
                // failing the pull: losing the budget refresh would decay the whole node.
                let reserve_permille = allocation.reserve.as_deref().and_then(|raw| raw.parse::<u16>().ok());
                Ok(Some(StoredAllocation {
                    budget: ByteRate::new(budget),
                    epoch: allocation.epoch,
                    reserve_permille,
                }))
            }
        }
    }
}

/// Checked `i64 -> u64` for a value Redis stored as a (non-negative) integer.
fn nonneg_u64(field: &'static str, value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| CoordError::Invalid { field })
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr, reason = "tests")]
mod tests {
    use super::Coordinator;
    use crate::alloc::Allocation;
    use crate::alloc::NodeObservation;
    use crate::ids::NodeId;
    use crate::units::ByteRate;
    use crate::units::Bytes;
    use crate::units::DiskPressure;
    use core::str::FromStr;
    use std::time::Duration;

    /// A live test Redis URL, or `None` to skip (Rust has no fakeredis; these need a real
    /// instance — `CEPHOR_TEST_REDIS_URL=redis://localhost:6382/0`). A skip prints a note so
    /// a silent pass is not mistaken for coverage.
    fn test_url() -> Option<String> {
        std::env::var("CEPHOR_TEST_REDIS_URL").ok().filter(|value| !value.is_empty())
    }

    /// Builds a coordinator on the test Redis under a per-test prefix (parallel isolation),
    /// or returns `None` to skip the test.
    async fn coord(prefix: &str, node_ttl: Duration, alloc_ttl: Duration) -> Option<Coordinator> {
        let url = test_url()?;
        // Prefix with the process id so a re-run never collides with the prior run's
        // still-TTL'd keys on a shared test Redis (the per-test `prefix` isolates within a run).
        let coordinator = Coordinator::connect(&url, node_ttl, alloc_ttl)
            .await
            .expect("connect to the test redis")
            .with_prefix(&format!("{}:{prefix}", std::process::id()));
        Some(coordinator)
    }

    fn observation() -> NodeObservation {
        NodeObservation {
            pressure: DiskPressure::try_from(5_000).unwrap(),
            backlog: Bytes::new(9_000_000),
            free: Bytes::ZERO,
            cache: Bytes::ZERO,
            max_drain_rate: ByteRate::new(5_000_000),
            observed_p99: Duration::from_millis(10),
            error_bps: 0,
        }
    }

    // ------------------------------------------------ heartbeat wire compatibility (Phase 1)

    #[test]
    fn a_pre_retention_heartbeat_still_parses_during_a_rolling_agent_update() {
        // The DaemonSet rolls one node at a time, so a NEW allocator necessarily reads OLD
        // agents' heartbeats for the duration. The old form carries no residency fields; if
        // that failed to deserialize, `load_fleet` would drop those nodes, they would be
        // allocated nothing, and their drain would stall mid-roll. It must parse, and it must
        // keep reporting exactly the raw pressure it measured — today's behaviour, unchanged.
        let old = r#"{"pressure_bps":5000,"backlog_bytes":9000000,"max_drain_rate_bps":5000000,"observed_p99_ns":10000000,"error_bps":0}"#;
        let parsed: super::NodeStateJson = serde_json::from_str(old).expect("the pre-retention wire form must still parse");
        let observation = parsed.into_observation().unwrap();
        assert_eq!(
            observation.pressure,
            DiskPressure::try_from(5_000).unwrap(),
            "with no residency fields the allocator falls back to the reported fullness"
        );
        assert_eq!(observation.backlog, Bytes::new(9_000_000));
    }

    #[test]
    fn a_heartbeat_reporting_residency_derives_pressure_from_ingest_headroom() {
        // The new form: a node 95% full of RETAINED, evictable read cache with no undrained
        // work. Raw fullness says 9500 bps (critical, earns a reservation floor and a heavy
        // water-fill weight); ingest headroom says it is perfectly caught up. The allocator
        // must believe the latter, or growing the read tier bids for Ceph budget it does not need.
        let new = r#"{"pressure_bps":9500,"backlog_bytes":0,"max_drain_rate_bps":5000000,"observed_p99_ns":10000000,"error_bps":0,"free_bytes":50000000,"cache_bytes":950000000}"#;
        let parsed: super::NodeStateJson = serde_json::from_str(new).expect("the residency wire form must parse");
        let observation = parsed.into_observation().unwrap();
        assert_eq!(
            observation.pressure,
            DiskPressure::ZERO,
            "a disk full of evictable cache with no backlog is not under drain pressure"
        );
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn lease_acquires_renews_keeping_the_epoch_and_blocks_a_second_holder() {
        let Some(c) = coord("cephor-test:lease-renew:", Duration::from_secs(5), Duration::from_secs(5)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let first = c
            .acquire_or_renew_leadership("a", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("a leads");
        // Renewing the OWN still-valid lease keeps the epoch.
        let renew = c
            .acquire_or_renew_leadership("a", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("a renews");
        assert_eq!(renew.epoch, first.epoch, "renewal keeps the epoch");
        // A second instance cannot lead while a holds an unexpired lease.
        assert_eq!(
            c.acquire_or_renew_leadership("b", Duration::from_secs(30)).await.unwrap(),
            None,
            "b is denied"
        );
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn an_expired_lease_is_taken_over_with_a_bumped_epoch() {
        // A 1s lease that we let lapse: the next acquirer is a NEW era (epoch + 1), which
        // fences any stale in-flight write from the previous era.
        let Some(c) = coord("cephor-test:lease-takeover:", Duration::from_secs(5), Duration::from_secs(5)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let first = c
            .acquire_or_renew_leadership("a", Duration::from_secs(1))
            .await
            .unwrap()
            .expect("a leads");
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let next = c
            .acquire_or_renew_leadership("b", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("b takes over");
        assert!(next.epoch > first.epoch, "takeover bumps the epoch ({} > {})", next.epoch, first.epoch);
        // Relinquish lets a successor lead immediately rather than waiting the TTL.
        c.relinquish_leadership("b").await.unwrap();
        assert!(
            c.acquire_or_renew_leadership("d", Duration::from_secs(30)).await.unwrap().is_some(),
            "successor leads after relinquish"
        );
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn a_heartbeat_expires_out_of_the_fleet() {
        // A 1s node TTL: present right after the upsert, gone after it lapses.
        let Some(c) = coord("cephor-test:fleet-ttl:", Duration::from_secs(1), Duration::from_secs(5)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        c.upsert_node_state(&node, &observation()).await.unwrap();
        assert_eq!(c.load_fleet().await.unwrap().len(), 1, "the heartbeating node is in the fleet");
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert_eq!(c.load_fleet().await.unwrap().len(), 0, "a node that stops heartbeating ages out");
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn an_allocation_round_trips_and_a_stale_epoch_write_is_fenced() {
        let Some(c) = coord("cephor-test:alloc-fence:", Duration::from_secs(5), Duration::from_secs(30)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        let alloc = |budget| Allocation {
            node: node.clone(),
            budget: ByteRate::new(budget),
            reserve_permille: 150,
        };
        // Epoch 5 writes a budget; it reads back with its epoch.
        c.write_allocations(5, std::slice::from_ref(&alloc(1_000))).await.unwrap();
        let stored = c.load_allocation(&node).await.unwrap().expect("budget written");
        assert_eq!(stored.budget, ByteRate::new(1_000));
        assert_eq!(stored.epoch, 5);
        // A lower-epoch (deposed) leader's write is fenced and changes nothing.
        let err = c.write_allocations(3, std::slice::from_ref(&alloc(2_000))).await.unwrap_err();
        assert!(
            matches!(err, super::CoordError::Fenced { epoch: 3 }),
            "stale-epoch write fenced, got {err:?}"
        );
        assert_eq!(
            c.load_allocation(&node).await.unwrap().unwrap().budget,
            ByteRate::new(1_000),
            "the fenced write did not land"
        );
        // A higher epoch (a new leader) may overwrite.
        c.write_allocations(6, std::slice::from_ref(&alloc(2_000))).await.unwrap();
        assert_eq!(c.load_allocation(&node).await.unwrap().unwrap().budget, ByteRate::new(2_000));
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn a_fresh_lease_after_a_lost_leadership_key_fences_the_deposed_leaders_late_write() {
        // Models redis losing/evicting the lease key under memory pressure: the deposed
        // leader keeps its stale epoch, a successor takes over with a strictly higher
        // epoch, and the deposed leader's late allocation write must still be fenced. The
        // fence anchor is the epoch COUNTER, not the lease key, so a lost lease alone must
        // never let a zombie leader corrupt allocations (F3, lease-key-loss variant).
        let Some(c) = coord("cephor-test:lost-lease-fence:", Duration::from_secs(30), Duration::from_secs(30)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        let alloc = |budget| Allocation {
            node: node.clone(),
            budget: ByteRate::new(budget),
            reserve_permille: 150,
        };

        // A leads (epoch e) and writes a budget.
        let a = c
            .acquire_or_renew_leadership("a", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("a leads");
        c.write_allocations(a.epoch, std::slice::from_ref(&alloc(1_000))).await.unwrap();

        // Redis loses the lease key (eviction / flush) — but the epoch counter survives.
        let mut conn = c.conn.clone();
        let _: i64 = redis::cmd("DEL").arg(c.leader_key()).query_async(&mut conn).await.unwrap();

        // B sees no lease and takes over with a strictly higher epoch (the counter persisted).
        let b = c
            .acquire_or_renew_leadership("b", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("b takes over the lost lease");
        assert!(
            b.epoch > a.epoch,
            "takeover after a lost lease still bumps the epoch ({} > {})",
            b.epoch,
            a.epoch
        );
        c.write_allocations(b.epoch, std::slice::from_ref(&alloc(2_000))).await.unwrap();

        // The deposed leader A (still holding its stale epoch) tries to write: fenced.
        let err = c.write_allocations(a.epoch, std::slice::from_ref(&alloc(9_999))).await.unwrap_err();
        assert!(
            matches!(err, super::CoordError::Fenced { epoch } if epoch == a.epoch),
            "deposed leader fenced, got {err:?}"
        );
        assert_eq!(
            c.load_allocation(&node).await.unwrap().unwrap().budget,
            ByteRate::new(2_000),
            "only the new leader's budget stands after a lost-lease takeover",
        );
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn a_deposed_leaders_stale_write_is_fenced_before_the_successor_writes_any_alloc() {
        // R3: the fence must reject a deposed leader's write even when the successor has NOT
        // written any alloc key yet — the anchor is `cephor:epoch` (bumped at election), not a
        // previously-written alloc value. Every OTHER fence test pre-seeds a higher-epoch alloc
        // key before the stale write, so this exact window is otherwise never exercised.
        let Some(c) = coord("cephor-test:r3-window:", Duration::from_secs(30), Duration::from_secs(30)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };
        let node = NodeId::from_str("n1").unwrap();
        let alloc = |budget| Allocation {
            node: node.clone(),
            budget: ByteRate::new(budget),
            reserve_permille: 150,
        };

        // A wins a SHORT lease (epoch eA, which bumps cephor:epoch to eA) and writes a budget.
        let a = c
            .acquire_or_renew_leadership("a", Duration::from_secs(1))
            .await
            .unwrap()
            .expect("a leads");
        c.write_allocations(a.epoch, std::slice::from_ref(&alloc(1_000))).await.unwrap();

        // A's lease lapses; B takes over — cephor:epoch is now eB > eA. Crucially B has done NO
        // allocation write yet (it is mid-tick: load_fleet → ceiling → allocate). The alloc key
        // therefore still carries eA, so the per-alloc-key check alone would NOT fence A.
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let b = c
            .acquire_or_renew_leadership("b", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("b takes over");
        assert!(b.epoch > a.epoch, "takeover bumps the epoch ({} > {})", b.epoch, a.epoch);

        // The deposed A finishes its slow tick and writes at its stale epoch. PRE-FIX this
        // slips through (alloc key holds eA, not > eA); the current-era fence rejects it.
        let err = c.write_allocations(a.epoch, std::slice::from_ref(&alloc(9_999))).await.unwrap_err();
        assert!(
            matches!(err, super::CoordError::Fenced { epoch } if epoch == a.epoch),
            "the deposed leader's stale write is fenced by the current era, got {err:?}"
        );
        assert_eq!(
            c.load_allocation(&node).await.unwrap().unwrap().budget,
            ByteRate::new(1_000),
            "the fenced stale write did not overwrite the budget",
        );
    }

    #[tokio::test]
    #[ignore = "requires CEPHOR_TEST_REDIS_URL"]
    async fn a_deposed_leader_with_an_empty_plan_is_still_fenced() {
        // D3: an empty allocation plan must STILL run the current-era fence. Pre-fix,
        // write_allocations short-circuited `Ok(())` on an empty slice and never consulted
        // `cephor:epoch`, so a leader deposed between acquiring its lease and a tick that
        // happened to allocate nothing (no hungry nodes) got a false `Ok` — run_tick then
        // reported a stale `Led` instead of surfacing the fence. This exercises the empty
        // slice directly; every other fence test passes at least one node.
        let Some(c) = coord("cephor-test:empty-plan-fence:", Duration::from_secs(30), Duration::from_secs(30)).await else {
            eprintln!("skipping: CEPHOR_TEST_REDIS_URL unset");
            return;
        };

        // A wins a SHORT lease (epoch eA, which bumps cephor:epoch to eA).
        let a = c
            .acquire_or_renew_leadership("a", Duration::from_secs(1))
            .await
            .unwrap()
            .expect("a leads");
        // The CURRENT leader's empty plan is not a false positive: its epoch equals the era,
        // so the fence passes and nothing is written.
        c.write_allocations(a.epoch, &[])
            .await
            .expect("current leader's empty plan is not fenced");

        // A's lease lapses; B takes over — cephor:epoch is now eB > eA.
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let b = c
            .acquire_or_renew_leadership("b", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("b takes over");
        assert!(b.epoch > a.epoch, "takeover bumps the epoch ({} > {})", b.epoch, a.epoch);

        // The deposed A finishes a tick with an EMPTY plan and writes at its stale epoch. The
        // current-era fence must reject it rather than returning a false `Ok`.
        let err = c.write_allocations(a.epoch, &[]).await.unwrap_err();
        assert!(
            matches!(err, super::CoordError::Fenced { epoch } if epoch == a.epoch),
            "an empty-plan write by a deposed leader is fenced, got {err:?}"
        );
    }
}
