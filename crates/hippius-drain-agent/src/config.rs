//! Daemon configuration parsed from the environment.
//!
//! [`Config::from_env`] is the only public entry; the parsing core
//! ([`Config::from_lookup`]) takes a lookup closure so it is tested with a
//! fixture map instead of the process-global environment — which is shared
//! mutable state that races across parallel tests.

use crate::runtime::{HeartbeatConfig, RuntimeConfig};
use core::str::FromStr;
use hippius_drain_core::{ByteRate, NodeId};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Drain-poll floor when `CEPHOR_DRAIN_POLL_SECS` is unset (the `chunk_landed`
/// trigger wakes the worker sooner; this is only the backstop).
const DEFAULT_DRAIN_POLL: Duration = Duration::from_secs(5);
/// Reconciler scan period when `CEPHOR_RECONCILE_POLL_SECS` is unset.
const DEFAULT_RECONCILE_POLL: Duration = Duration::from_mins(1);
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
/// Allocation staleness window when `CEPHOR_ALLOCATION_STALE_SECS` is unset: past
/// it the allocator is treated as silent on this node and the rate decays toward
/// the floor (a few allocator ticks — the allocator tick is ~2s).
const DEFAULT_ALLOCATION_STALE: Duration = Duration::from_secs(10);
/// Reclaim scan period when `CEPHOR_RECLAIM_POLL_SECS` is unset: the SSD-ingest GC
/// runs less often than the drain — eviction is a backstop, not a hot path.
const DEFAULT_RECLAIM_POLL: Duration = Duration::from_mins(5);
/// Reclaim grace when `CEPHOR_RECLAIM_GRACE_SECS` is unset: how long a terminal part
/// is kept on SSD before eviction under normal pressure (a generous diagnosis /
/// drain-race window; also the orphan-temp sweep's max age).
const DEFAULT_RECLAIM_GRACE: Duration = Duration::from_hours(1);
/// Reclaim grace under disk pressure when `CEPHOR_RECLAIM_PRESSURE_GRACE_SECS` is
/// unset: evict terminal parts sooner to relieve a filling SSD.
const DEFAULT_RECLAIM_PRESSURE_GRACE: Duration = Duration::from_mins(1);
/// SSD fullness (basis points) at/above which reclaim uses the pressure grace, when
/// `CEPHOR_RECLAIM_PRESSURE_BPS` is unset — 90%, just under the `fs_cache_pressure`
/// 503 cliff, so eviction accelerates before the api starts rejecting PUTs.
const DEFAULT_RECLAIM_PRESSURE_BPS: u16 = 9000;

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
    /// wired into the store via `with_defer_backoff`.
    pub defer_backoff: Duration,
    /// How long a stored allocation stays authoritative before the agent treats
    /// the allocator as silent and decays toward the floor (M1 conservation).
    pub allocation_stale: Duration,
    /// Redis URL for the upload queues — the drain pushes each replicated part's
    /// `UploadChainRequest` here (drain-direct; the drain is the sole upload producer).
    pub redis_queues_url: String,
    /// Backends to enqueue each part's upload to (`{backend}_upload_requests`), from
    /// `HIPPIUS_UPLOAD_BACKENDS` (comma-list). Defaults to `["arion"]`.
    pub upload_backends: Vec<String>,
    /// How often the SSD-reclaim worker scans for terminal aged parts to evict.
    pub reclaim_poll: Duration,
    /// How long a terminal (replicated/failed) part is kept on SSD before eviction
    /// under normal pressure.
    pub reclaim_grace: Duration,
    /// The shorter grace used when the SSD is under pressure.
    pub reclaim_pressure_grace: Duration,
    /// SSD fullness (basis points) at/above which reclaim uses the pressure grace.
    pub reclaim_pressure_bps: u16,
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
            reclaim_poll: self.reclaim_poll,
            reclaim_grace: self.reclaim_grace,
            reclaim_pressure_grace: self.reclaim_pressure_grace,
            reclaim_pressure_bps: self.reclaim_pressure_bps,
            grace: self.grace,
        }
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
            grace: duration_secs(&get, "CEPHOR_GRACE_SECS", DEFAULT_GRACE)?,
            node_id: required_node_id(&get, "CEPHOR_NODE_ID")?,
            heartbeat_poll: duration_secs(&get, "CEPHOR_HEARTBEAT_POLL_SECS", DEFAULT_HEARTBEAT_POLL)?,
            max_drain_rate: ByteRate::new(positive_u64_or(&get, "CEPHOR_MAX_DRAIN_RATE_BPS", DEFAULT_MAX_DRAIN_RATE_BPS)?),
            floor_rate: ByteRate::new(positive_u64_or(&get, "CEPHOR_FLOOR_RATE_BPS", DEFAULT_FLOOR_RATE_BPS)?),
            decay_half_life: duration_secs(&get, "CEPHOR_DECAY_HALF_LIFE_SECS", DEFAULT_DECAY_HALF_LIFE)?,
            allocation_poll: duration_secs(&get, "CEPHOR_ALLOCATION_POLL_SECS", DEFAULT_ALLOCATION_POLL)?,
            claim_lease: duration_secs(&get, "CEPHOR_CLAIM_LEASE_TTL_SECS", DEFAULT_CLAIM_LEASE)?,
            defer_backoff: duration_secs(&get, "CEPHOR_DEFER_BACKOFF_SECS", DEFAULT_DEFER_BACKOFF)?,
            allocation_stale: duration_secs(&get, "CEPHOR_ALLOCATION_STALE_SECS", DEFAULT_ALLOCATION_STALE)?,
            redis_queues_url: required(&get, "REDIS_QUEUES_URL")?,
            upload_backends: parse_backends(&get, "HIPPIUS_UPLOAD_BACKENDS"),
            reclaim_poll: duration_secs(&get, "CEPHOR_RECLAIM_POLL_SECS", DEFAULT_RECLAIM_POLL)?,
            reclaim_grace: duration_secs(&get, "CEPHOR_RECLAIM_GRACE_SECS", DEFAULT_RECLAIM_GRACE)?,
            reclaim_pressure_grace: duration_secs(&get, "CEPHOR_RECLAIM_PRESSURE_GRACE_SECS", DEFAULT_RECLAIM_PRESSURE_GRACE)?,
            reclaim_pressure_bps: bps_or(&get, "CEPHOR_RECLAIM_PRESSURE_BPS", DEFAULT_RECLAIM_PRESSURE_BPS)?,
        })
    }
}

/// Parses a comma-separated backend list (`HIPPIUS_UPLOAD_BACKENDS`), trimming and
/// dropping empties. Defaults to `["arion"]` when unset or empty (mirrors the Python
/// `config.upload_backends` default).
fn parse_backends(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Vec<String> {
    let parsed: Vec<String> = get(var)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if parsed.is_empty() { vec!["arion".to_owned()] } else { parsed }
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

/// Resolves an optional basis-points (`0..=10000`) variable. Builds on [`u64_or`]
/// (so a present-but-unparsable value still errors), then clamps a value above 10000
/// down to 10000 — a "never enter pressure mode" config that is pointless but safe,
/// not a startup failure.
fn bps_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u16) -> Result<u16, ConfigError> {
    let raw = u64_or(get, var, u64::from(default))?;
    Ok(u16::try_from(raw.min(10_000)).unwrap_or(default))
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
        Config, ConfigError, DEFAULT_ALLOCATION_POLL, DEFAULT_ALLOCATION_STALE, DEFAULT_CLAIM_LEASE, DEFAULT_DECAY_HALF_LIFE, DEFAULT_DRAIN_POLL,
        DEFAULT_FLOOR_RATE_BPS, DEFAULT_HEARTBEAT_POLL, DEFAULT_MAX_DRAIN_RATE_BPS, DEFAULT_RECLAIM_GRACE, DEFAULT_RECLAIM_POLL,
        DEFAULT_RECLAIM_PRESSURE_BPS, DEFAULT_RECLAIM_PRESSURE_GRACE,
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
    fn defaults_the_allocation_stale_window() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.allocation_stale, DEFAULT_ALLOCATION_STALE);
    }

    #[test]
    fn a_numeric_allocation_stale_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_ALLOCATION_STALE_SECS", "30"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.allocation_stale, Duration::from_secs(30));
    }

    #[test]
    fn defaults_the_reclaim_knobs() {
        let config = Config::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.reclaim_poll, DEFAULT_RECLAIM_POLL);
        assert_eq!(config.reclaim_grace, DEFAULT_RECLAIM_GRACE);
        assert_eq!(config.reclaim_pressure_grace, DEFAULT_RECLAIM_PRESSURE_GRACE);
        assert_eq!(config.reclaim_pressure_bps, DEFAULT_RECLAIM_PRESSURE_BPS);
    }

    #[test]
    fn numeric_reclaim_knobs_override_the_defaults() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_RECLAIM_POLL_SECS", "120"));
        pairs.push(("CEPHOR_RECLAIM_GRACE_SECS", "600"));
        pairs.push(("CEPHOR_RECLAIM_PRESSURE_GRACE_SECS", "30"));
        pairs.push(("CEPHOR_RECLAIM_PRESSURE_BPS", "8500"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.reclaim_poll, Duration::from_mins(2));
        assert_eq!(config.reclaim_grace, Duration::from_mins(10));
        assert_eq!(config.reclaim_pressure_grace, Duration::from_secs(30));
        assert_eq!(config.reclaim_pressure_bps, 8500);
    }

    #[test]
    fn an_out_of_range_reclaim_pressure_bps_is_clamped_not_rejected() {
        // Above 10000 just means "never enter pressure mode" — a pointless but safe
        // config, clamped rather than failing startup.
        let mut pairs = required_only();
        pairs.push(("CEPHOR_RECLAIM_PRESSURE_BPS", "99999"));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.reclaim_pressure_bps, 10_000);
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
    fn upload_backends_parses_a_trimmed_comma_list() {
        let mut pairs = required_only();
        pairs.push(("HIPPIUS_UPLOAD_BACKENDS", " arion , ovh "));
        let config = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.upload_backends, vec!["arion".to_owned(), "ovh".to_owned()]);
    }
}
