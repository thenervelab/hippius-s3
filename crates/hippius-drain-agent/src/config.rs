//! Daemon configuration parsed from the environment.
//!
//! [`Config::from_env`] is the only public entry; the parsing core
//! ([`Config::from_lookup`]) takes a lookup closure so it is tested with a
//! fixture map instead of the process-global environment — which is shared
//! mutable state that races across parallel tests.

use crate::runtime::{EvictionPolicy, HeartbeatConfig, RuntimeConfig};
use core::str::FromStr;
use hippius_drain_core::{ByteRate, NodeId};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Drain-poll period when `CEPHOR_DRAIN_POLL_SECS` is unset. The drain is purely polling — there is
/// no api NOTIFY / `chunk_landed` wake in the part model (the reconciler is the sole trigger, see
/// `runtime.rs`), so a part landing just after a poll waits up to this interval to be claimed (DR-6).
const DEFAULT_DRAIN_POLL: Duration = Duration::from_secs(5);
/// Reconciler scan period when `CEPHOR_RECONCILE_POLL_SECS` is unset.
const DEFAULT_RECONCILE_POLL: Duration = Duration::from_mins(1);
/// Enqueue-sweep period when `CEPHOR_ENQUEUE_POLL_SECS` is unset. How often a `replicated`
/// part whose address was not finalized at drain time (an in-flight MPU) is retried for its
/// backend upload. Off the read path (the object serves from the pool), so a few seconds is
/// ample; short enough that a completed MPU's parts publish promptly.
const DEFAULT_ENQUEUE_POLL: Duration = Duration::from_secs(5);
/// Shutdown grace when `CEPHOR_GRACE_SECS` is unset.
const DEFAULT_GRACE: Duration = Duration::from_secs(30);
/// Heartbeat period when `CEPHOR_HEARTBEAT_POLL_SECS` is unset.
const DEFAULT_HEARTBEAT_POLL: Duration = Duration::from_secs(10);
/// Node drain capability (bytes/sec) when `CEPHOR_MAX_DRAIN_RATE_BPS` is unset.
const DEFAULT_MAX_DRAIN_RATE_BPS: u64 = 100_000_000;
/// Floor the rate decays toward (bytes/sec) when `CEPHOR_FLOOR_RATE_BPS` is unset.
const DEFAULT_FLOOR_RATE_BPS: u64 = 1_000_000;
/// Decay half-life when `CEPHOR_DECAY_HALF_LIFE_SECS` is unset.
const DEFAULT_DECAY_HALF_LIFE: Duration = Duration::from_secs(30);
/// Allocation re-pull period when `CEPHOR_ALLOCATION_POLL_SECS` is unset.
const DEFAULT_ALLOCATION_POLL: Duration = Duration::from_secs(2);
/// Maximum parts drained concurrently when `CEPHOR_DRAIN_CONCURRENCY` is unset.
/// Tuning this lets the node hide fsync latency behind more in-flight parts (the
/// AIMD allocator only tunes bytes/sec, never the concurrency).
const DEFAULT_DRAIN_CONCURRENCY: u32 = 4;
/// Max R4 re-drive attempts when `CEPHOR_REDRIVE_MAX_ATTEMPTS` is unset. A corrupt-live part
/// is re-copied from its intact SSD source this many times before being held `corrupt` and
/// paged — enough to ride out a transient pool-copy corruption without looping on a durable one.
const DEFAULT_REDRIVE_MAX_ATTEMPTS: u32 = 3;
/// Claim lease TTL when `CEPHOR_CLAIM_LEASE_TTL_SECS` is unset: a `draining`
/// claim older than this is treated as abandoned (the H1 crash-recovery TTL).
/// Mirrors the store-side default; long enough not to reclaim a live slow drain,
/// short enough to recover a crashed claim promptly.
const DEFAULT_CLAIM_LEASE: Duration = Duration::from_mins(5);
/// Deferral backoff when `CEPHOR_DEFER_BACKOFF_SECS` is unset: how long a part whose
/// drain deferred (enqueue not ready — the object's address is not finalized yet) is
/// parked before it is re-claimable, so the drain does not spin on not-ready parts every
/// poll. Mirrors the store-side default.
const DEFAULT_DEFER_BACKOFF: Duration = Duration::from_secs(5);
/// Deferral-backoff ceiling when `CEPHOR_DEFER_BACKOFF_CAP_SECS` is unset: the per-row
/// exponential backoff (base × 2^attempts) never parks a part longer than this, so a
/// chronically not-ready part still re-checks within minutes of its address finally
/// landing. Mirrors the store-side default.
const DEFAULT_DEFER_BACKOFF_CAP: Duration = Duration::from_mins(10);
/// Heartbeat key TTL when `CEPHOR_HEARTBEAT_TTL_SECS` is unset: how long this node's
/// heartbeat stays live in the fleet before it must be refreshed. This IS the
/// fleet-staleness window (the allocator drops a node whose key has expired), so it must
/// be a few heartbeat periods (~10s) wide — long enough to survive a missed beat.
const DEFAULT_HEARTBEAT_TTL: Duration = Duration::from_secs(30);
/// Reclaim scan period when `CEPHOR_RECLAIM_POLL_SECS` is unset: the SSD-ingest GC
/// runs less often than the drain — eviction is a backstop, not a hot path.
const DEFAULT_RECLAIM_POLL: Duration = Duration::from_mins(5);
/// Orphan-reclaim grace when `CEPHOR_ORPHAN_RECLAIM_GRACE_SECS` is unset: how long a
/// no-DB-backing part (its object hard-deleted) is kept on SSD before eviction. Longer
/// than the `failed` grace because this age is the FS `meta.json` mtime (a deleted object
/// has no DB row to date), so a generous window absorbs the agent-clock dependence and
/// any delete-racing-recreate window. A deleted object never returns, so there is no cost
/// to waiting.
const DEFAULT_ORPHAN_RECLAIM_GRACE: Duration = Duration::from_hours(24);
/// Reclaim grace when `CEPHOR_RECLAIM_GRACE_SECS` is unset: how long a `failed`
/// (abandoned-upload) part — and an orphan write-temp — is kept on SSD before
/// eviction (a diagnosis / abort-settle window).
const DEFAULT_RECLAIM_GRACE: Duration = Duration::from_hours(1);

/// How often the read-tier evictor probes free space when `CEPHOR_EVICT_POLL_SECS` is unset.
/// Frequent, because with retention on this is the only worker freeing the ingest SSD and a
/// burst of ingest can breach the floor between polls. A pass that is not armed costs one
/// `statvfs` and no query at all, so a short poll is close to free.
const DEFAULT_EVICT_POLL: Duration = Duration::from_secs(30);

/// The evictor's own free-space floor, in permille of the ingest disk, when
/// `CEPHOR_EVICT_RESERVE_PERMILLE` is unset. This is the FALLBACK: when the allocator is
/// publishing a per-node reserve, that wins. Chosen to sit well clear of the api's
/// `fs_cache_pressure` 503 threshold (80 permille free): the evictor must be reclaiming well
/// before ingest starts refusing writes, never racing it. ~575 GB on the 3.84 TB prod `NVMe`s.
const DEFAULT_EVICT_RESERVE_PERMILLE: u16 = 150;

/// How far past the floor one armed pass frees, in permille of the disk, when
/// `CEPHOR_EVICT_HEADROOM_PERMILLE` is unset. Without this gap the evictor re-arms on nearly
/// every poll and pins the disk at the threshold.
const DEFAULT_EVICT_HEADROOM_PERMILLE: u16 = 50;

/// Parts considered per eviction pass when `CEPHOR_EVICT_BATCH` is unset. At a 4 MiB chunk
/// and typical part sizes this frees tens of GB per pass — enough to close a breach in a few
/// polls without one pass monopolising the disk.
const DEFAULT_EVICT_BATCH: u32 = 512;

/// Default path of the liveness file the runtime touches each heartbeat tick; the
/// k8s `livenessProbe` checks its freshness. Container-local `/tmp` is always writable.
const DEFAULT_LIVENESS_FILE: &str = "/tmp/hippius-drain-agent.alive";

/// Default path of the readiness file the runtime touches only while the drain is progressing
/// (C8); the k8s `readinessProbe` checks its freshness to mark a wedged node `NotReady`.
const DEFAULT_READINESS_FILE: &str = "/tmp/hippius-drain-agent.ready";

/// The daemon's startup configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Postgres connection URL for the central state store.
    pub database_url: String,
    /// Root of the shared `CephFS` pool mount — the drain destination.
    pub pool_root: PathBuf,
    /// Root of the local SSD ingest cache — the drain source.
    pub ssd_root: PathBuf,
    /// Drain-worker poll floor.
    pub drain_poll: Duration,
    /// Reconciler scan period.
    pub reconcile_poll: Duration,
    /// Enqueue-sweep period: how often a `replicated` part whose backend upload was not
    /// enqueued at drain time (address not yet finalized) is retried.
    pub enqueue_poll: Duration,
    /// Grace given to in-flight ticks on shutdown before a forced abort.
    pub grace: Duration,
    /// This node's identity in the fleet (the allocator keys heartbeats by it).
    pub node_id: NodeId,
    /// How often to publish a heartbeat to the allocator.
    pub heartbeat_poll: Duration,
    /// The node's local drain capability — its demand cap, reported in heartbeats.
    pub max_drain_rate: ByteRate,
    /// The rate a silent allocator decays toward (never starves a node entirely).
    pub floor_rate: ByteRate,
    /// Half-life of the decay applied when the allocator goes silent.
    pub decay_half_life: Duration,
    /// How often the allocation-pull worker re-reads the leader's budget.
    pub allocation_poll: Duration,
    /// How long a `draining` claim is honored before another agent re-claims it
    /// (the H1 crash-recovery TTL; wired into the store via `with_claim_lease`).
    pub claim_lease: Duration,
    /// How long a deferred (enqueue-not-ready) part is backed off before re-claim,
    /// wired into the store via `with_defer_backoff`. The base of the per-row
    /// exponential backoff, doubled per deferral up to `defer_backoff_cap`.
    pub defer_backoff: Duration,
    /// Ceiling on the exponential deferral backoff, wired into the store via
    /// `with_defer_backoff_cap`.
    pub defer_backoff_cap: Duration,
    /// TTL stamped on this node's heartbeat key — the fleet-staleness window the
    /// allocator sees (a node whose key expires drops out of the fleet).
    pub heartbeat_ttl: Duration,
    /// Redis URL for the upload queues — the drain pushes each replicated part's
    /// `UploadChainRequest` here (drain-direct; the drain is the sole upload producer).
    pub redis_queues_url: String,
    /// Backends to enqueue each part's upload to (`{backend}_upload_requests`), from
    /// `HIPPIUS_UPLOAD_BACKENDS` (comma-list). Defaults to `["arion"]`.
    pub upload_backends: Vec<String>,
    /// `HIPPIUS_BACKUP_BACKENDS` (comma-list). Defaults to EMPTY — a backup backend is a
    /// deliberate opt-in. The janitor gate requires coverage on `upload_backends ∪
    /// backup_backends` before reclaiming a part, so the enqueuer MUST push to the same
    /// union ([`enqueue_backends`](Config::enqueue_backends)) — otherwise a configured
    /// backup backend is required-but-never-enqueued and the gate deadlocks (C10).
    pub backup_backends: Vec<String>,
    /// How often the SSD-reclaim worker scans for `failed` (abandoned-upload) parts.
    pub reclaim_poll: Duration,
    /// How long a `failed` part (and an orphan write-temp) is kept on SSD before
    /// eviction (a diagnosis / abort-settle window).
    pub reclaim_grace: Duration,
    /// How long a no-DB-backing part (its object hard-deleted) is kept on SSD before the
    /// orphan reclaim evicts it. Keyed on the part's FS `meta.json` age, so set generously.
    pub orphan_reclaim_grace: Duration,
    /// Maximum parts the drain worker processes concurrently — the in-flight gate
    /// that lets the node overlap fsync latency across parts.
    pub drain_concurrency: u32,
    /// Max times an R4 `corrupt` part is re-driven before it is held and paged. Bounds the
    /// re-drive so a persistently-bad pool copy cannot loop forever.
    pub redrive_max_attempts: u32,
    /// How often the read-tier evictor probes the ingest disk's free space.
    pub evict_poll: Duration,
    /// The evictor's fallback free-space floor, in permille of the ingest disk, used when the
    /// allocator is not publishing a per-node reserve. Zero disables eviction — and with
    /// retention on, that means nothing frees the SSD.
    pub evict_reserve_permille: u16,
    /// How far past the floor an armed eviction pass frees, in permille of the disk.
    pub evict_headroom_permille: u16,
    /// Parts considered per eviction pass.
    pub evict_batch: u32,
    /// Path of the liveness file the runtime touches each heartbeat tick; a k8s
    /// `livenessProbe` checks its freshness to restart a wedged (not crashed) pod.
    pub liveness_file: PathBuf,
    /// Path of the readiness file the runtime touches only while the drain is progressing (C8);
    /// a k8s `readinessProbe` checks its freshness to mark a wedged node `NotReady`.
    pub readiness_file: PathBuf,
}

/// A failure parsing the daemon configuration from the environment.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A required variable was unset or empty.
    #[error("missing required environment variable `{0}`")]
    Missing(&'static str),
    /// A variable held a value that did not parse as a count of seconds.
    #[error("environment variable `{var}` has invalid value `{value}`")]
    Invalid {
        /// The offending variable.
        var: &'static str,
        /// The value that failed to parse.
        value: String,
        /// The underlying integer-parse failure.
        #[source]
        source: ParseIntError,
    },
    /// A variable held a value that is not a valid identifier (e.g. all whitespace).
    #[error("environment variable `{var}` is not a valid identifier: `{value}`")]
    InvalidIdentifier {
        /// The offending variable.
        var: &'static str,
        /// The value that failed validation.
        value: String,
    },
    /// A rate variable was zero, which would silently stall the node (a zero floor
    /// never lets a silent-allocator node drain; a zero capability advertises no
    /// drain capacity at all). A misconfigured rate must fail fast, not degrade.
    #[error("environment variable `{var}` must be greater than zero")]
    NonPositive {
        /// The offending variable.
        var: &'static str,
    },
    /// A count variable exceeded its representable maximum (e.g. a drain
    /// concurrency past `u32::MAX`). A misconfiguration must fail fast.
    #[error("environment variable `{var}` value {value} exceeds the maximum {limit}")]
    OutOfRange {
        /// The offending variable.
        var: &'static str,
        /// The value supplied.
        value: u64,
        /// The maximum it may take.
        limit: u64,
    },
}

impl Config {
    /// Reads the configuration from the process environment.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] if a required variable is missing/empty/blank, a duration
    /// or rate variable does not parse as a non-negative integer, or a rate
    /// variable is zero.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// The [`RuntimeConfig`](crate::runtime::RuntimeConfig) the runtime needs.
    #[must_use]
    pub fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            drain_poll: self.drain_poll,
            reconcile_poll: self.reconcile_poll,
            enqueue_poll: self.enqueue_poll,
            reclaim_poll: self.reclaim_poll,
            reclaim_grace: self.reclaim_grace,
            orphan_reclaim_grace: self.orphan_reclaim_grace,
            grace: self.grace,
            drain_concurrency: self.drain_concurrency,
            redrive_max_attempts: self.redrive_max_attempts,
            evict_poll: self.evict_poll,
            evict_policy: EvictionPolicy {
                reserve_permille: self.evict_reserve_permille,
                headroom_permille: self.evict_headroom_permille,
                batch: self.evict_batch,
            },
        }
    }

    /// The backends the drain enqueuer pushes a replicated part to: `upload_backends`
    /// unioned with `backup_backends`, deduped, upload order preserved.
    ///
    /// This closes the C10 backup half: without the union, a configured backup backend
    /// would be required by the janitor gate (`is_replicated_on_all_backends` unions the
    /// per-version `upload_backends` with `HIPPIUS_BACKUP_BACKENDS`) but never enqueued, so
    /// the part could never reach full coverage and the janitor would never reclaim its
    /// SSD copy — a permanent leak/deadlock.
    ///
    /// It is NOT fully per-version aware: the gate keys on each version's *persisted*
    /// `object_versions.upload_backends` (and forces `['ipfs']` for a `migration` version),
    /// while this uses the agent's *current* global `config.upload_backends`. The two match
    /// only when every drained version's persisted set equals the current config and no
    /// `migration` version transits the drain. A migration or config-drifted version can
    /// therefore still be required-but-under-enqueued (the same leak, not data loss) — a
    /// residual the G2 replication-gate sentinel is there to surface. The complete fix reads
    /// the per-version set in the enqueuer (`load_upload_context` already reads that row).
    #[must_use]
    pub fn enqueue_backends(&self) -> Vec<String> {
        let mut backends = self.upload_backends.clone();
        for backup in &self.backup_backends {
            if !backends.contains(backup) {
                backends.push(backup.clone());
            }
        }
        backends
    }

    /// The [`HeartbeatConfig`](crate::runtime::HeartbeatConfig) for this node.
    #[must_use]
    pub fn heartbeat_config(&self) -> HeartbeatConfig {
        HeartbeatConfig {
            node: self.node_id.clone(),
            max_drain_rate: self.max_drain_rate,
            poll: self.heartbeat_poll,
        }
    }

    /// Parsing core: resolves each key through `get`. Separated from
    /// [`from_env`](Self::from_env) so tests drive it with a fixture map instead
    /// of the process-global environment.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            database_url: required(&get, "CEPHOR_DATABASE_URL")?,
            pool_root: required_path(&get, "CEPHOR_POOL_ROOT")?,
            ssd_root: required_path(&get, "CEPHOR_SSD_ROOT")?,
            drain_poll: duration_secs(&get, "CEPHOR_DRAIN_POLL_SECS", DEFAULT_DRAIN_POLL)?,
            reconcile_poll: duration_secs(&get, "CEPHOR_RECONCILE_POLL_SECS", DEFAULT_RECONCILE_POLL)?,
            enqueue_poll: duration_secs(&get, "CEPHOR_ENQUEUE_POLL_SECS", DEFAULT_ENQUEUE_POLL)?,
            grace: duration_secs(&get, "CEPHOR_GRACE_SECS", DEFAULT_GRACE)?,
            node_id: required_node_id(&get, "CEPHOR_NODE_ID")?,
            heartbeat_poll: duration_secs(&get, "CEPHOR_HEARTBEAT_POLL_SECS", DEFAULT_HEARTBEAT_POLL)?,
            max_drain_rate: ByteRate::new(positive_u64_or(&get, "CEPHOR_MAX_DRAIN_RATE_BPS", DEFAULT_MAX_DRAIN_RATE_BPS)?),
            floor_rate: ByteRate::new(positive_u64_or(&get, "CEPHOR_FLOOR_RATE_BPS", DEFAULT_FLOOR_RATE_BPS)?),
            decay_half_life: duration_secs(&get, "CEPHOR_DECAY_HALF_LIFE_SECS", DEFAULT_DECAY_HALF_LIFE)?,
            allocation_poll: duration_secs(&get, "CEPHOR_ALLOCATION_POLL_SECS", DEFAULT_ALLOCATION_POLL)?,
            claim_lease: duration_secs(&get, "CEPHOR_CLAIM_LEASE_TTL_SECS", DEFAULT_CLAIM_LEASE)?,
            // Burst termination leans on this being nonzero: a skipped part's backoff is
            // what keeps it out of the claim set, so 0 lets a burst re-claim/re-defer the
            // same part in a tight loop until cancellation.
            defer_backoff: duration_secs(&get, "CEPHOR_DEFER_BACKOFF_SECS", DEFAULT_DEFER_BACKOFF)?,
            defer_backoff_cap: duration_secs(&get, "CEPHOR_DEFER_BACKOFF_CAP_SECS", DEFAULT_DEFER_BACKOFF_CAP)?,
            heartbeat_ttl: duration_secs(&get, "CEPHOR_HEARTBEAT_TTL_SECS", DEFAULT_HEARTBEAT_TTL)?,
            redis_queues_url: required(&get, "REDIS_QUEUES_URL")?,
            upload_backends: parse_backends(&get, "HIPPIUS_UPLOAD_BACKENDS"),
            backup_backends: parse_optional_backends(&get, "HIPPIUS_BACKUP_BACKENDS"),
            reclaim_poll: duration_secs(&get, "CEPHOR_RECLAIM_POLL_SECS", DEFAULT_RECLAIM_POLL)?,
            reclaim_grace: duration_secs(&get, "CEPHOR_RECLAIM_GRACE_SECS", DEFAULT_RECLAIM_GRACE)?,
            orphan_reclaim_grace: duration_secs(&get, "CEPHOR_ORPHAN_RECLAIM_GRACE_SECS", DEFAULT_ORPHAN_RECLAIM_GRACE)?,
            drain_concurrency: positive_u32_or(&get, "CEPHOR_DRAIN_CONCURRENCY", DEFAULT_DRAIN_CONCURRENCY)?,
            redrive_max_attempts: positive_u32_or(&get, "CEPHOR_REDRIVE_MAX_ATTEMPTS", DEFAULT_REDRIVE_MAX_ATTEMPTS)?,
            evict_poll: duration_secs(&get, "CEPHOR_EVICT_POLL_SECS", DEFAULT_EVICT_POLL)?,
            evict_reserve_permille: permille_or(&get, "CEPHOR_EVICT_RESERVE_PERMILLE", DEFAULT_EVICT_RESERVE_PERMILLE)?,
            evict_headroom_permille: permille_or(&get, "CEPHOR_EVICT_HEADROOM_PERMILLE", DEFAULT_EVICT_HEADROOM_PERMILLE)?,
            evict_batch: positive_u32_or(&get, "CEPHOR_EVICT_BATCH", DEFAULT_EVICT_BATCH)?,
            liveness_file: path_or(&get, "CEPHOR_LIVENESS_FILE", DEFAULT_LIVENESS_FILE),
            readiness_file: path_or(&get, "CEPHOR_READINESS_FILE", DEFAULT_READINESS_FILE),
        })
    }
}

/// Parses a comma-separated backend list (`HIPPIUS_UPLOAD_BACKENDS`), trimming and
/// dropping empties. Defaults to `["arion"]` when unset or empty (mirrors the Python
/// `config.upload_backends` default).
fn parse_backends(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Vec<String> {
    let parsed = parse_optional_backends(get, var);
    if parsed.is_empty() { vec!["arion".to_owned()] } else { parsed }
}

/// Parses a comma-separated backend list with an EMPTY default (no fallback backend) —
/// for `HIPPIUS_BACKUP_BACKENDS`, where "unset" must mean "no backup", never `["arion"]`.
fn parse_optional_backends(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Vec<String> {
    get(var)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Resolves a required identifier variable into a validated [`NodeId`].
fn required_node_id(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Result<NodeId, ConfigError> {
    let value = required(get, var)?;
    NodeId::from_str(&value).map_err(|_| ConfigError::InvalidIdentifier { var, value })
}

/// Resolves an optional integer variable, falling back to `default` when unset.
/// A present-but-unparsable value is a loud error, not a silent fallback.
fn u64_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u64) -> Result<u64, ConfigError> {
    match get(var) {
        None => Ok(default),
        Some(value) => value.parse::<u64>().map_err(|source| ConfigError::Invalid { var, value, source }),
    }
}

/// Resolves an optional positive-rate variable. Builds on [`u64_or`] but rejects
/// an explicit zero: a zero rate (drain floor or capability) silently stalls the
/// node, so it must fail fast rather than parse cleanly into a useless config.
fn positive_u64_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u64) -> Result<u64, ConfigError> {
    match u64_or(get, var, default)? {
        0 => Err(ConfigError::NonPositive { var }),
        value => Ok(value),
    }
}

/// Resolves an optional positive count variable as a `u32`. Builds on [`u64_or`]
/// but rejects an explicit zero (a zero drain concurrency would stall the worker)
/// and a value past `u32::MAX`, mirroring [`positive_u64_or`]'s fail-fast contract.
/// A permille value in `0..=1000`.
///
/// Deliberately NOT `positive_u32_or`: zero is a meaningful value for the eviction reserve
/// (it disables eviction, the operator kill-switch), so rejecting it would turn a supported
/// setting into a startup failure. The upper bound is checked because a reserve above the
/// whole disk can never be satisfied — the evictor would evict the entire cache every pass and still
/// report `starved` forever.
fn permille_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u16) -> Result<u16, ConfigError> {
    let value = u64_or(get, var, u64::from(default))?;
    if value > 1_000 {
        return Err(ConfigError::OutOfRange { var, value, limit: 1_000 });
    }
    Ok(u16::try_from(value).unwrap_or(1_000))
}

fn positive_u32_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u32) -> Result<u32, ConfigError> {
    let value = u64_or(get, var, u64::from(default))?;
    match u32::try_from(value) {
        Ok(0) => Err(ConfigError::NonPositive { var }),
        Ok(value) => Ok(value),
        Err(_) => Err(ConfigError::OutOfRange {
            var,
            value,
            limit: u64::from(u32::MAX),
        }),
    }
}

/// Resolves a required filesystem path, rejecting unset, empty, or whitespace-only
/// values as [`Missing`](ConfigError::Missing). A blank path is the dangerous case
/// `required` alone misses: it is non-empty, so it would pass into a `PathBuf` and
/// silently resolve to a junk location the agent then scans/drains — a quiet
/// misconfiguration rather than a startup failure.
fn required_path(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Result<PathBuf, ConfigError> {
    match get(var) {
        Some(value) if !value.trim().is_empty() => Ok(PathBuf::from(value)),
        _ => Err(ConfigError::Missing(var)),
    }
}

/// Resolves an optional path variable, falling back to `default` when unset or blank.
fn path_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: &str) -> PathBuf {
    match get(var) {
        Some(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => PathBuf::from(default),
    }
}

/// Resolves a required variable, treating unset and empty as the same failure
/// (an empty `DATABASE_URL` is as unusable as a missing one).
fn required(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Result<String, ConfigError> {
    get(var).filter(|value| !value.is_empty()).ok_or(ConfigError::Missing(var))
}

/// Resolves an optional duration given as an integer count of seconds, falling
/// back to `default` when unset. A present-but-unparsable value is an error
/// rather than a silent fallback — a typo'd interval should fail loudly.
fn duration_secs(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: Duration) -> Result<Duration, ConfigError> {
    Ok(Duration::from_secs(u64_or(get, var, default.as_secs())?))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{
        Config, ConfigError, DEFAULT_ALLOCATION_POLL, DEFAULT_CLAIM_LEASE, DEFAULT_DECAY_HALF_LIFE, DEFAULT_DEFER_BACKOFF_CAP,
        DEFAULT_DRAIN_CONCURRENCY, DEFAULT_DRAIN_POLL, DEFAULT_FLOOR_RATE_BPS, DEFAULT_HEARTBEAT_POLL, DEFAULT_HEARTBEAT_TTL,
        DEFAULT_MAX_DRAIN_RATE_BPS, DEFAULT_ORPHAN_RECLAIM_GRACE, DEFAULT_RECLAIM_GRACE, DEFAULT_RECLAIM_POLL,
    };
    use core::str::FromStr;
    use hippius_drain_core::{ByteRate, NodeId};
    use std::path::PathBuf;
    use std::time::Duration;

    /// A `get` closure backed by an owned fixture list (no process env touched).
    fn lookup(pairs: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        let owned: Vec<(String, String)> = pairs.iter().map(|&(key, value)| (key.to_string(), value.to_string())).collect();
        move |key| owned.iter().find_map(|(k, v)| (k.as_str() == key).then(|| v.clone()))
    }

    fn required_only() -> Vec<(&'static str, &'static str)> {
        vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_POOL_ROOT", "/mnt/pool"),
            ("CEPHOR_SSD_ROOT", "/mnt/ssd"),
            ("CEPHOR_NODE_ID", "node-7"),
            ("REDIS_QUEUES_URL", "redis://localhost:6382/0"),
        ]
    }

    #[test]
    fn reads_required_vars_and_defaults_the_rest() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.database_url, "postgres://localhost/cephor");
        assert_eq!(config.pool_root, PathBuf::from("/mnt/pool"));
        assert_eq!(config.ssd_root, PathBuf::from("/mnt/ssd"));
        assert_eq!(config.drain_poll, DEFAULT_DRAIN_POLL);
    }

    #[test]
    fn liveness_file_defaults_and_is_overridable() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.liveness_file, PathBuf::from("/tmp/hippius-drain-agent.alive"));
        let mut pairs = required_only();
        pairs.push(("CEPHOR_LIVENESS_FILE", "/var/run/agent.alive"));
        let overridden = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(overridden.liveness_file, PathBuf::from("/var/run/agent.alive"));
    }

    #[test]
    fn readiness_file_defaults_and_is_overridable() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.readiness_file, PathBuf::from("/tmp/hippius-drain-agent.ready"));
        let mut pairs = required_only();
        pairs.push(("CEPHOR_READINESS_FILE", "/var/run/agent.ready"));
        let overridden = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(overridden.readiness_file, PathBuf::from("/var/run/agent.ready"));
    }

    #[test]
    fn reads_the_node_id_and_defaults_the_heartbeat_fields() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.node_id, NodeId::from_str("node-7").unwrap());
        assert_eq!(config.heartbeat_poll, DEFAULT_HEARTBEAT_POLL);
        assert_eq!(config.max_drain_rate, ByteRate::new(DEFAULT_MAX_DRAIN_RATE_BPS));
    }

    #[test]
    fn a_missing_node_id_reports_it() {
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_POOL_ROOT", "/mnt/pool"),
            ("CEPHOR_SSD_ROOT", "/mnt/ssd"),
        ];
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("CEPHOR_NODE_ID")));
    }

    #[test]
    fn a_whitespace_node_id_is_an_invalid_identifier() {
        // Non-empty (passes the required check) but not a valid identifier.
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_POOL_ROOT", "/mnt/pool"),
            ("CEPHOR_SSD_ROOT", "/mnt/ssd"),
            ("CEPHOR_NODE_ID", "   "),
        ];
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidIdentifier { var: "CEPHOR_NODE_ID", .. }));
    }

    #[test]
    fn a_numeric_max_drain_rate_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_MAX_DRAIN_RATE_BPS", "5000"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.max_drain_rate, ByteRate::new(5000));
    }

    #[test]
    fn defaults_the_rate_control_knobs() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.floor_rate, ByteRate::new(DEFAULT_FLOOR_RATE_BPS));
        assert_eq!(config.decay_half_life, DEFAULT_DECAY_HALF_LIFE);
        assert_eq!(config.allocation_poll, DEFAULT_ALLOCATION_POLL);
    }

    #[test]
    fn defaults_the_claim_lease() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.claim_lease, DEFAULT_CLAIM_LEASE);
    }

    #[test]
    fn a_numeric_claim_lease_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CLAIM_LEASE_TTL_SECS", "600"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.claim_lease, Duration::from_mins(10));
    }

    #[test]
    fn defaults_the_defer_backoff_cap() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.defer_backoff_cap, DEFAULT_DEFER_BACKOFF_CAP);
    }

    #[test]
    fn a_numeric_defer_backoff_cap_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_DEFER_BACKOFF_CAP_SECS", "1200"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.defer_backoff_cap, Duration::from_mins(20));
    }

    #[test]
    fn defaults_the_heartbeat_ttl() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.heartbeat_ttl, DEFAULT_HEARTBEAT_TTL);
    }

    #[test]
    fn a_numeric_heartbeat_ttl_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_HEARTBEAT_TTL_SECS", "45"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.heartbeat_ttl, Duration::from_secs(45));
    }

    #[test]
    fn defaults_the_reclaim_knobs() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.reclaim_poll, DEFAULT_RECLAIM_POLL);
        assert_eq!(config.reclaim_grace, DEFAULT_RECLAIM_GRACE);
        assert_eq!(config.orphan_reclaim_grace, DEFAULT_ORPHAN_RECLAIM_GRACE);
    }

    #[test]
    fn numeric_reclaim_knobs_override_the_defaults() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_RECLAIM_POLL_SECS", "120"));
        pairs.push(("CEPHOR_RECLAIM_GRACE_SECS", "600"));
        pairs.push(("CEPHOR_ORPHAN_RECLAIM_GRACE_SECS", "7200"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.reclaim_poll, Duration::from_mins(2));
        assert_eq!(config.reclaim_grace, Duration::from_mins(10));
        assert_eq!(config.orphan_reclaim_grace, Duration::from_hours(2));
    }

    #[test]
    fn a_numeric_floor_rate_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_FLOOR_RATE_BPS", "2048"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.floor_rate, ByteRate::new(2048));
    }

    #[test]
    fn a_zero_floor_rate_is_rejected() {
        // A zero floor would let a silent-allocator node decay to no draining at
        // all, defeating the floor's never-starve purpose — fail fast instead.
        let mut pairs = required_only();
        pairs.push(("CEPHOR_FLOOR_RATE_BPS", "0"));
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::NonPositive {
                var: "CEPHOR_FLOOR_RATE_BPS"
            }
        ));
    }

    #[test]
    fn a_zero_max_drain_rate_is_rejected() {
        // A node advertising zero drain capability is useless — reject it loudly.
        let mut pairs = required_only();
        pairs.push(("CEPHOR_MAX_DRAIN_RATE_BPS", "0"));
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::NonPositive {
                var: "CEPHOR_MAX_DRAIN_RATE_BPS"
            }
        ));
    }

    #[test]
    fn a_whitespace_pool_root_is_rejected() {
        // Non-empty (passes the bare is_empty check) but blank: a whitespace path
        // would silently resolve to a junk directory, so it is treated as missing.
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_POOL_ROOT", "  \t "),
            ("CEPHOR_SSD_ROOT", "/mnt/ssd"),
            ("CEPHOR_NODE_ID", "node-7"),
        ];
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("CEPHOR_POOL_ROOT")));
    }

    #[test]
    fn a_whitespace_ssd_root_is_rejected() {
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_POOL_ROOT", "/mnt/pool"),
            ("CEPHOR_SSD_ROOT", "   "),
            ("CEPHOR_NODE_ID", "node-7"),
        ];
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("CEPHOR_SSD_ROOT")));
    }

    #[test]
    fn a_missing_required_var_reports_which_one() {
        let pairs = vec![("CEPHOR_POOL_ROOT", "/mnt/pool"), ("CEPHOR_SSD_ROOT", "/mnt/ssd")];
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("CEPHOR_DATABASE_URL")));
    }

    #[test]
    fn an_empty_required_var_is_treated_as_missing() {
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", ""),
            ("CEPHOR_POOL_ROOT", "/mnt/pool"),
            ("CEPHOR_SSD_ROOT", "/mnt/ssd"),
        ];
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("CEPHOR_DATABASE_URL")));
    }

    #[test]
    fn a_non_numeric_duration_is_invalid() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_DRAIN_POLL_SECS", "soon"));
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Invalid {
                var: "CEPHOR_DRAIN_POLL_SECS",
                ..
            }
        ));
    }

    #[test]
    fn a_numeric_duration_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_RECONCILE_POLL_SECS", "120"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.reconcile_poll, Duration::from_mins(2));
    }

    #[test]
    fn reads_the_redis_url_and_defaults_upload_backends_to_arion() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.redis_queues_url, "redis://localhost:6382/0");
        assert_eq!(config.upload_backends, vec!["arion".to_owned()]);
    }

    #[test]
    fn a_missing_redis_url_is_reported() {
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_POOL_ROOT", "/mnt/pool"),
            ("CEPHOR_SSD_ROOT", "/mnt/ssd"),
            ("CEPHOR_NODE_ID", "node-7"),
        ];
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("REDIS_QUEUES_URL")));
    }

    #[test]
    fn defaults_the_drain_concurrency() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.drain_concurrency, DEFAULT_DRAIN_CONCURRENCY);
    }

    #[test]
    fn a_numeric_drain_concurrency_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_DRAIN_CONCURRENCY", "12"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.drain_concurrency, 12);
    }

    #[test]
    fn a_zero_drain_concurrency_is_rejected() {
        // A zero concurrency would stall the drain worker entirely — fail fast.
        let mut pairs = required_only();
        pairs.push(("CEPHOR_DRAIN_CONCURRENCY", "0"));
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::NonPositive {
                var: "CEPHOR_DRAIN_CONCURRENCY"
            }
        ));
    }

    #[test]
    fn a_drain_concurrency_past_u32_is_out_of_range() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_DRAIN_CONCURRENCY", "4294967296"));
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::OutOfRange {
                var: "CEPHOR_DRAIN_CONCURRENCY",
                ..
            }
        ));
    }

    #[test]
    fn upload_backends_parses_a_trimmed_comma_list() {
        let mut pairs = required_only();
        pairs.push(("HIPPIUS_UPLOAD_BACKENDS", " arion , ovh "));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.upload_backends, vec!["arion".to_owned(), "ovh".to_owned()]);
    }

    #[test]
    fn backup_backends_default_to_empty_not_arion() {
        // Unlike upload_backends (which defaults to ["arion"]), backup must default EMPTY:
        // a spurious default backup backend would make the janitor gate require coverage
        // the enqueuer then has to satisfy for no reason.
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert!(config.backup_backends.is_empty(), "no HIPPIUS_BACKUP_BACKENDS -> no backup backends");
    }

    #[test]
    fn backup_backends_parse_a_trimmed_comma_list() {
        let mut pairs = required_only();
        pairs.push(("HIPPIUS_BACKUP_BACKENDS", " ipfs , s3backup "));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.backup_backends, vec!["ipfs".to_owned(), "s3backup".to_owned()]);
    }

    #[test]
    fn enqueue_backends_is_the_deduped_ordered_union_of_upload_and_backup() {
        // C10: the enqueuer must push to every backend the janitor gate requires
        // (upload ∪ backup). Upload order is preserved; a backup already in upload is not
        // duplicated; a genuinely new backup backend is appended.
        let mut pairs = required_only();
        pairs.push(("HIPPIUS_UPLOAD_BACKENDS", "arion,ipfs"));
        pairs.push(("HIPPIUS_BACKUP_BACKENDS", "ipfs,s3backup")); // ipfs overlaps, s3backup is new
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(
            config.enqueue_backends(),
            vec!["arion".to_owned(), "ipfs".to_owned(), "s3backup".to_owned()],
            "upload order kept, overlap deduped, new backup appended",
        );
    }

    #[test]
    fn enqueue_backends_with_no_backup_is_just_upload() {
        let mut pairs = required_only();
        pairs.push(("HIPPIUS_UPLOAD_BACKENDS", "arion"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.enqueue_backends(), vec!["arion".to_owned()]);
    }
}
