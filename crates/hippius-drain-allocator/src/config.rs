//! Allocator configuration parsed from the environment.
//!
//! [`AllocatorConfig::from_env`] is the only public entry; the parsing core
//! ([`AllocatorConfig::from_lookup`]) takes a lookup closure so it is tested with
//! a fixture map instead of the process-global environment — which is shared
//! mutable state that races across parallel tests (mirrors `hippius-drain-agent`'s
//! config).

use hippius_drain_core::{AllocConfig, ByteRate, CephCeiling, CephThresholds, DiskPressure, StaticCeiling, TickConfig};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Default path of the liveness file the tick loop touches each iteration; the k8s
/// `livenessProbe` checks its freshness. Container-local `/tmp` is always writable.
const DEFAULT_LIVENESS_FILE: &str = "/tmp/hippius-drain-allocator.alive";

/// Leader-lease TTL when `CEPHOR_LEADER_LEASE_TTL_SECS` is unset. Several ticks
/// long so a brief stall does not lose leadership, short enough to fail over.
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
/// Allocation tick period when `CEPHOR_ALLOCATOR_TICK_SECS` is unset.
///
/// Each tick renews the leader lease and rewrites allocations — two hot-row writes,
/// each a slow WAL fsync on the ceph-backed Postgres (where they were measured at
/// 2–5 s). 5 s (was 2 s) cuts that write churn ~2.5× while still giving the 30 s lease
/// six renewals of headroom; rate allocation does not need sub-5 s updates. Tune via
/// `CEPHOR_ALLOCATOR_TICK_SECS` per deployment.
const DEFAULT_TICK: Duration = Duration::from_secs(5);
/// TTL on each per-node allocation key when `CEPHOR_ALLOCATION_TTL_SECS` is unset.
/// Past it the agent reads no budget and decays toward its floor, so it must be a few
/// allocator ticks wide — long enough to survive a missed tick, short enough that a
/// departed node's budget conserves promptly. Replaces the old PG node-absence zeroing.
const DEFAULT_ALLOCATION_TTL: Duration = Duration::from_secs(15);
/// Static Ceph write-ceiling (bytes/sec) when `CEPHOR_CEPH_CEILING_BPS` is unset.
/// The operational default until the live Ceph-mgr probe lands; assumes Ceph is
/// open at this rate, so the real cap is `max_total` and per-node demand.
const DEFAULT_CEPH_CEILING_BPS: u64 = 1_000_000_000;
/// AIMD floor (bytes/sec) the fleet estimate never drops below.
const DEFAULT_MIN_TOTAL_BPS: u64 = 1_000_000;
/// AIMD ceiling (bytes/sec) the fleet estimate never climbs above.
const DEFAULT_MAX_TOTAL_BPS: u64 = 1_000_000_000;
/// Additive increase (bytes/sec) applied each healthy tick.
const DEFAULT_ADDITIVE_INCREASE_BPS: u64 = 10_000_000;
/// Multiplicative-decrease parts-per-thousand kept on back-off (800 => keep 80%).
const DEFAULT_DECREASE_PERMILLE: u16 = 800;
/// p99 *per-part* drain latency (ms) above which the fleet is considered saturated and the AIMD
/// estimate backs off toward `min_total`.
///
/// This is the p99 of `drain_part` — a whole part's SSD→CephFS copy + fsync + readback-verify —
/// NOT a small point op. Measured healthy p99 on staging is ~330–410 ms (breaker closed,
/// `error_bps=0`). The former default of **50 ms** was physically unreachable for a multi-MB part
/// copy, so `observed_p99 > target_p99` fired on EVERY tick → the fleet write-budget was pinned
/// at the `min_total` 1 MB/s floor permanently, throttling the whole drain to ~1 MB/s regardless
/// of Ceph health (measured live: `drain_fleet_estimate_bps = 1_000_000`). 2000 ms gives ~5×
/// headroom over the healthy baseline: the AIMD ramps up while Ceph keeps p99 well under 2 s, and
/// only a genuine multi-second slowdown throttles it. Tune per-cluster via
/// `CEPHOR_ALLOC_TARGET_P99_MS`. (Future: compare against a rolling baseline rather than an
/// absolute target; an absolute target must be set above the real per-part copy time.)
const DEFAULT_TARGET_P99_MS: u64 = 2_000;
/// Error rate (basis points) above which the fleet is considered saturated.
const DEFAULT_MAX_ERROR_BPS: u16 = 100;
/// Pressure (basis points) at/above which a node earns a reservation floor.
const DEFAULT_CRITICAL_PRESSURE_BPS: u16 = 9_000;
/// Guaranteed per-node floor (bytes/sec) for critical-pressure nodes.
const DEFAULT_RESERVATION_FLOOR_BPS: u64 = 1_000_000;

/// The evictor's free-space floor when the drain is keeping up, in permille of disk. Must stay
/// clear of the api's `fs_cache_pressure` 503 gate (80 permille free) so eviction is always
/// reclaiming before ingest is refused.
const DEFAULT_BASE_RESERVE_PERMILLE: u16 = 150;

/// The floor when the drain is fully stalled. Raising it is what buys ingest runway: a
/// throttled drain means backlog grows, and freeing cache EARLY is the only lever that keeps
/// `fs_cache_pressure` from refusing PUTs later. Deliberately well under the whole disk — the
/// point is headroom for incoming backlog, not an emptied cache.
const DEFAULT_MAX_RESERVE_PERMILLE: u16 = 400;
/// Ceph near-full watermark (basis points) when `CEPHOR_CEPH_NEARFULL_BPS` is unset.
/// Mirrors the cluster's `nearfull_ratio` of 0.85.
const DEFAULT_CEPH_NEARFULL_BPS: u16 = 8_500;
/// Ceph full watermark (basis points) when `CEPHOR_CEPH_FULL_BPS` is unset. Mirrors
/// the cluster's `full_ratio` of 0.95.
const DEFAULT_CEPH_FULL_BPS: u16 = 9_500;
/// Per-scrape timeout for the live mgr probe when `CEPHOR_CEPH_PROBE_TIMEOUT_SECS`
/// is unset — short relative to the tick so a hung mgr decays rather than stalls.
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// The fleet-wide rate a near-full ceiling carries when
/// `CEPHOR_CEPH_NEARFULL_RATE_BPS` is unset. Deliberately far below any plausible
/// `min_total`: the 2026-07-24 incident showed the AIMD floor (raised to 50 MB/s
/// in prod) is an operator latency knob, not a fullness brake, so near-full must
/// bound the drain through the ceiling clamp instead. 10 MB/s keeps a relief
/// valve open for SSD pressure while a near-full pool fills in days, not hours.
const DEFAULT_CEPH_NEARFULL_RATE_BPS: u64 = 10_000_000;

/// How long a terminal (`replicated`/`failed`) replication row is kept before the periodic
/// GC prunes it, when `CEPHOR_STATUS_RETENTION_SECS` is unset. A week comfortably exceeds
/// the janitor cycle + any abort-settle window, so a `failed` row is never pruned before
/// the reclaim path has had a chance to act on it.
const DEFAULT_STATUS_RETENTION: Duration = Duration::from_hours(7 * 24);

/// The allocator's startup configuration.
#[derive(Debug, Clone)]
pub struct AllocatorConfig {
    /// Postgres connection URL for the central state store.
    pub database_url: String,
    /// This allocator instance's identity (the leader-lease holder id).
    pub instance_id: String,
    /// How long an acquired leadership lease stays valid.
    pub lease_ttl: Duration,
    /// How often to run an allocation tick.
    pub tick_interval: Duration,
    /// Redis URL for the coordination state (leader lease / heartbeats / allocations) —
    /// the redis-queues instance the agents also use.
    pub redis_queues_url: String,
    /// TTL stamped on each per-node allocation key the allocator writes.
    pub alloc_ttl: Duration,
    /// The static Ceph write-ceiling (the open-rate the live probe hands out when
    /// Ceph is healthy, and the whole ceiling when no probe URL is configured).
    pub ceph_ceiling: ByteRate,
    /// The AIMD estimate the first tick starts from.
    pub initial_total: ByteRate,
    /// Allocation tuning (AIMD + reservation + saturation thresholds).
    pub alloc: AllocConfig,
    /// The Ceph mgr prometheus exporter URL. `Some` selects the live ceiling probe;
    /// `None` falls back to the static ceiling.
    pub ceph_mgr_metrics_url: Option<String>,
    /// The near-full / full watermarks the live probe classifies against.
    pub ceph_thresholds: CephThresholds,
    /// The reduced fleet-wide rate a near-full ceiling carries
    /// (`CEPHOR_CEPH_NEARFULL_RATE_BPS`).
    pub ceph_nearfull_rate: ByteRate,
    /// The pools whose `percent_used` additionally gate the live probe's ceiling
    /// (`CEPHOR_CEPH_POOLS`, comma-separated; the fullest binds). Empty gates on
    /// cluster-wide signals only.
    pub ceph_pools: Vec<String>,
    /// Per-scrape timeout for the live probe.
    pub ceph_probe_timeout: Duration,
    /// Path of the liveness file the tick loop touches each iteration; a k8s
    /// `livenessProbe` checks its freshness to restart a wedged (not crashed) allocator.
    pub liveness_file: PathBuf,
    /// How long a terminal (`replicated`/`failed`) replication row is kept before the
    /// periodic GC sweep prunes it (`CEPHOR_STATUS_RETENTION_SECS`).
    pub status_retention: Duration,
}

/// A failure parsing the allocator configuration from the environment.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A required variable was unset, empty, or all whitespace.
    #[error("missing required environment variable `{0}`")]
    Missing(&'static str),
    /// A variable held a value that did not parse as an integer.
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
    /// A rate variable was zero, which would stall allocation (a zero ceiling or
    /// AIMD bound allocates nothing). A misconfigured rate must fail fast.
    #[error("environment variable `{var}` must be greater than zero")]
    NonPositive {
        /// The offending variable.
        var: &'static str,
    },
    /// A value exceeded its allowed maximum (a permille over 1000, a pressure over
    /// 10000 basis points, or a min-total above the max-total).
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

impl AllocatorConfig {
    /// Reads the configuration from the process environment.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] if a required variable is missing/blank, an integer
    /// variable does not parse, a rate is zero, or a value is out of range.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// The [`TickConfig`] one allocation tick needs.
    #[must_use]
    pub fn tick_config(&self) -> TickConfig {
        TickConfig {
            instance_id: self.instance_id.clone(),
            lease_ttl: self.lease_ttl,
            alloc: self.alloc,
        }
    }

    /// The static ceiling source, used when no `CEPHOR_CEPH_MGR_METRICS_URL` is set
    /// (the live Ceph-mgr probe is the configured path; this is the fallback).
    #[must_use]
    pub fn ceiling(&self) -> StaticCeiling {
        StaticCeiling(CephCeiling::Open(self.ceph_ceiling))
    }

    /// The settings the live Ceph-mgr probe scrapes `url` with.
    ///
    /// The blind-decay floor is the near-full rate, *not* `alloc.min_total`: while the
    /// probe cannot read the mgr it cannot tell an open pool from a full one, so the
    /// rate the fleet keeps must be no looser than the one a KNOWN near-full pool
    /// carries. `min_total` is an operator latency knob tuned to keep the drain out of
    /// its collapse threshold and may sit far above any safe blind rate — and the
    /// 2026-07-24 incident shape (a renamed/missing target pool) reaches exactly this
    /// path, because a pool the scrape does not mention is a parse failure and so a
    /// decay. Sharing one variable for both made a floor raise silently loosen the
    /// fail-safe.
    #[cfg(feature = "http")]
    #[must_use]
    pub fn probe_settings(&self, url: String) -> crate::probe::ProbeSettings {
        crate::probe::ProbeSettings {
            url,
            ceiling_rate: self.ceph_ceiling,
            nearfull_rate: self.ceph_nearfull_rate,
            floor: self.ceph_nearfull_rate,
            thresholds: self.ceph_thresholds,
            timeout: self.ceph_probe_timeout,
            pools: self.ceph_pools.clone(),
        }
    }

    /// Parsing core: resolves each key through `get`. Separated from
    /// [`from_env`](Self::from_env) so tests drive it with a fixture map.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let min_total = positive_u64(&get, "CEPHOR_ALLOC_MIN_TOTAL_BPS", DEFAULT_MIN_TOTAL_BPS)?;
        let max_total = positive_u64(&get, "CEPHOR_ALLOC_MAX_TOTAL_BPS", DEFAULT_MAX_TOTAL_BPS)?;
        if min_total > max_total {
            return Err(ConfigError::OutOfRange {
                var: "CEPHOR_ALLOC_MIN_TOTAL_BPS",
                value: min_total,
                limit: max_total,
            });
        }
        let alloc = AllocConfig {
            min_total: ByteRate::new(min_total),
            max_total: ByteRate::new(max_total),
            additive_increase: ByteRate::new(u64_or(&get, "CEPHOR_ALLOC_ADDITIVE_INCREASE_BPS", DEFAULT_ADDITIVE_INCREASE_BPS)?),
            decrease_permille: permille(&get, "CEPHOR_ALLOC_DECREASE_PERMILLE", DEFAULT_DECREASE_PERMILLE)?,
            target_p99: duration_millis(&get, "CEPHOR_ALLOC_TARGET_P99_MS", DEFAULT_TARGET_P99_MS)?,
            max_error_bps: u16_or(&get, "CEPHOR_ALLOC_MAX_ERROR_BPS", DEFAULT_MAX_ERROR_BPS)?,
            critical_pressure: critical_pressure(&get, "CEPHOR_ALLOC_CRITICAL_PRESSURE_BPS", DEFAULT_CRITICAL_PRESSURE_BPS)?,
            reservation_floor: ByteRate::new(u64_or(&get, "CEPHOR_ALLOC_RESERVATION_FLOOR_BPS", DEFAULT_RESERVATION_FLOOR_BPS)?),
            base_reserve_permille: permille_or(&get, "CEPHOR_ALLOC_BASE_RESERVE_PERMILLE", DEFAULT_BASE_RESERVE_PERMILLE)?,
            max_reserve_permille: permille_or(&get, "CEPHOR_ALLOC_MAX_RESERVE_PERMILLE", DEFAULT_MAX_RESERVE_PERMILLE)?,
        };
        let ceph_ceiling = positive_u64(&get, "CEPHOR_CEPH_CEILING_BPS", DEFAULT_CEPH_CEILING_BPS)?;
        let ceph_nearfull_rate = positive_u64(&get, "CEPHOR_CEPH_NEARFULL_RATE_BPS", DEFAULT_CEPH_NEARFULL_RATE_BPS)?;
        if ceph_nearfull_rate > ceph_ceiling {
            // NearFull carrying more budget than Open would invert the "fuller is
            // never looser" ceiling invariant.
            return Err(ConfigError::OutOfRange {
                var: "CEPHOR_CEPH_NEARFULL_RATE_BPS",
                value: ceph_nearfull_rate,
                limit: ceph_ceiling,
            });
        }
        Ok(Self {
            database_url: required(&get, "CEPHOR_DATABASE_URL")?,
            instance_id: required(&get, "CEPHOR_ALLOCATOR_INSTANCE_ID")?,
            lease_ttl: duration_secs(&get, "CEPHOR_LEADER_LEASE_TTL_SECS", DEFAULT_LEASE_TTL)?,
            tick_interval: duration_secs(&get, "CEPHOR_ALLOCATOR_TICK_SECS", DEFAULT_TICK)?,
            redis_queues_url: required(&get, "REDIS_QUEUES_URL")?,
            alloc_ttl: duration_secs(&get, "CEPHOR_ALLOCATION_TTL_SECS", DEFAULT_ALLOCATION_TTL)?,
            ceph_ceiling: ByteRate::new(ceph_ceiling),
            // The first tick starts from the AIMD floor unless overridden, so a
            // fresh leader ramps up from a safe rate rather than a guess.
            initial_total: ByteRate::new(u64_or(&get, "CEPHOR_ALLOC_INITIAL_TOTAL_BPS", min_total)?),
            alloc,
            ceph_mgr_metrics_url: optional(&get, "CEPHOR_CEPH_MGR_METRICS_URL"),
            ceph_thresholds: ceph_thresholds(&get)?,
            ceph_nearfull_rate: ByteRate::new(ceph_nearfull_rate),
            ceph_pools: name_list(&get, "CEPHOR_CEPH_POOLS"),
            ceph_probe_timeout: duration_secs(&get, "CEPHOR_CEPH_PROBE_TIMEOUT_SECS", DEFAULT_PROBE_TIMEOUT)?,
            liveness_file: path_or(&get, "CEPHOR_LIVENESS_FILE", DEFAULT_LIVENESS_FILE),
            status_retention: duration_secs(&get, "CEPHOR_STATUS_RETENTION_SECS", DEFAULT_STATUS_RETENTION)?,
        })
    }
}

/// Resolves the near-full / full watermarks, rejecting a near-full above full (which
/// would let a full cluster classify as merely near-full).
fn ceph_thresholds(get: &impl Fn(&str) -> Option<String>) -> Result<CephThresholds, ConfigError> {
    let nearfull = critical_pressure(get, "CEPHOR_CEPH_NEARFULL_BPS", DEFAULT_CEPH_NEARFULL_BPS)?;
    let full = critical_pressure(get, "CEPHOR_CEPH_FULL_BPS", DEFAULT_CEPH_FULL_BPS)?;
    CephThresholds::new(nearfull, full).map_err(|_| ConfigError::OutOfRange {
        var: "CEPHOR_CEPH_NEARFULL_BPS",
        value: u64::from(nearfull.bps()),
        limit: u64::from(full.bps()),
    })
}

/// Resolves a required variable, treating unset, empty, and all-whitespace as the
/// same failure — a blank URL or instance id is as unusable as a missing one.
fn required(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Result<String, ConfigError> {
    match get(var) {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(ConfigError::Missing(var)),
    }
}

/// Resolves an optional variable, treating unset, empty, and all-whitespace alike as
/// absent — a blank mgr URL must not select the probe with an unusable endpoint.
fn optional(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Option<String> {
    get(var).filter(|value| !value.trim().is_empty())
}

/// Resolves a comma-separated name list: entries are trimmed, blanks dropped, so
/// `"a, b,,c "` reads as `["a","b","c"]` and an unset or all-blank variable as empty.
fn name_list(get: &impl Fn(&str) -> Option<String>, var: &'static str) -> Vec<String> {
    get(var)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Resolves an optional path variable, falling back to `default` when unset or blank.
fn path_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: &str) -> PathBuf {
    match get(var) {
        Some(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => PathBuf::from(default),
    }
}

/// Resolves an optional integer variable, falling back to `default` when unset.
/// A present-but-unparsable value is a loud error, not a silent fallback.
/// A permille value in `0..=1000`.
///
/// Zero is meaningful here — it disables the evictor — so this cannot reuse `positive_u64`,
/// which would turn a supported setting into a startup failure.
fn permille_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u16) -> Result<u16, ConfigError> {
    let value = u64_or(get, var, u64::from(default))?;
    if value > 1_000 {
        return Err(ConfigError::OutOfRange { var, value, limit: 1_000 });
    }
    Ok(u16::try_from(value).unwrap_or(1_000))
}

fn u64_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u64) -> Result<u64, ConfigError> {
    match get(var) {
        None => Ok(default),
        Some(value) => value.parse::<u64>().map_err(|source| ConfigError::Invalid { var, value, source }),
    }
}

/// Like [`u64_or`] but rejects an explicit zero — a zero rate stalls allocation.
fn positive_u64(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u64) -> Result<u64, ConfigError> {
    match u64_or(get, var, default)? {
        0 => Err(ConfigError::NonPositive { var }),
        value => Ok(value),
    }
}

/// Resolves an optional `u16` variable (the parse rejects values above 65535).
fn u16_or(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u16) -> Result<u16, ConfigError> {
    match get(var) {
        None => Ok(default),
        Some(value) => value.parse::<u16>().map_err(|source| ConfigError::Invalid { var, value, source }),
    }
}

/// Resolves a parts-per-thousand value, rejecting anything above 1000 (the
/// `AllocConfig::decrease_permille` precondition).
fn permille(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u16) -> Result<u16, ConfigError> {
    let value = u16_or(get, var, default)?;
    if value > 1_000 {
        return Err(ConfigError::OutOfRange {
            var,
            value: u64::from(value),
            limit: 1_000,
        });
    }
    Ok(value)
}

/// Resolves a disk-pressure value (basis points), rejecting anything above 10000
/// via the [`DiskPressure`] invariant.
fn critical_pressure(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: u16) -> Result<DiskPressure, ConfigError> {
    let bps = u16_or(get, var, default)?;
    DiskPressure::try_from(bps).map_err(|_| ConfigError::OutOfRange {
        var,
        value: u64::from(bps),
        limit: u64::from(DiskPressure::MAX_BPS),
    })
}

/// Resolves an optional duration given as an integer count of seconds.
fn duration_secs(get: &impl Fn(&str) -> Option<String>, var: &'static str, default: Duration) -> Result<Duration, ConfigError> {
    Ok(Duration::from_secs(u64_or(get, var, default.as_secs())?))
}

/// Resolves an optional duration given as an integer count of milliseconds.
fn duration_millis(get: &impl Fn(&str) -> Option<String>, var: &'static str, default_ms: u64) -> Result<Duration, ConfigError> {
    Ok(Duration::from_millis(u64_or(get, var, default_ms)?))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{
        AllocatorConfig, ConfigError, DEFAULT_ALLOCATION_TTL, DEFAULT_CRITICAL_PRESSURE_BPS, DEFAULT_DECREASE_PERMILLE, DEFAULT_MAX_TOTAL_BPS,
        DEFAULT_MIN_TOTAL_BPS, DEFAULT_STATUS_RETENTION, DEFAULT_TICK,
    };
    #[cfg(feature = "http")]
    use hippius_drain_core::decay;
    use hippius_drain_core::{ByteRate, CephCeiling, DiskPressure};

    /// A `get` closure backed by an owned fixture list (no process env touched).
    fn lookup(pairs: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        let owned: Vec<(String, String)> = pairs.iter().map(|&(key, value)| (key.to_string(), value.to_string())).collect();
        move |key| owned.iter().find_map(|(k, v)| (k.as_str() == key).then(|| v.clone()))
    }

    fn required_only() -> Vec<(&'static str, &'static str)> {
        vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_ALLOCATOR_INSTANCE_ID", "alloc-1"),
            ("REDIS_QUEUES_URL", "redis://localhost:6382/0"),
        ]
    }

    #[test]
    fn reads_required_vars_and_defaults_the_rest() {
        let config = AllocatorConfig::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.database_url, "postgres://localhost/cephor");
        assert_eq!(config.instance_id, "alloc-1");
        assert_eq!(config.tick_interval, DEFAULT_TICK);
        assert_eq!(config.alloc.min_total, ByteRate::new(DEFAULT_MIN_TOTAL_BPS));
        assert_eq!(config.alloc.max_total, ByteRate::new(DEFAULT_MAX_TOTAL_BPS));
        assert_eq!(config.alloc.decrease_permille, DEFAULT_DECREASE_PERMILLE);
        assert_eq!(
            config.alloc.critical_pressure,
            DiskPressure::try_from(DEFAULT_CRITICAL_PRESSURE_BPS).unwrap()
        );
        // The first-tick estimate defaults to the AIMD floor.
        assert_eq!(config.initial_total, ByteRate::new(DEFAULT_MIN_TOTAL_BPS));
    }

    #[test]
    fn status_retention_defaults_to_a_week_and_is_overridable() {
        let config = AllocatorConfig::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.status_retention, DEFAULT_STATUS_RETENTION);
        let mut pairs = required_only();
        pairs.push(("CEPHOR_STATUS_RETENTION_SECS", "3600"));
        let overridden = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(overridden.status_retention, std::time::Duration::from_hours(1));
    }

    #[test]
    fn liveness_file_defaults_and_is_overridable() {
        let config = AllocatorConfig::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.liveness_file, std::path::PathBuf::from("/tmp/hippius-drain-allocator.alive"));
        let mut pairs = required_only();
        pairs.push(("CEPHOR_LIVENESS_FILE", "/var/run/allocator.alive"));
        let overridden = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(overridden.liveness_file, std::path::PathBuf::from("/var/run/allocator.alive"));
    }

    #[test]
    fn tick_config_and_ceiling_reflect_the_config() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_CEILING_BPS", "500000"));
        // The tiny test ceiling sits below the default near-full rate, which the
        // ordering check would (correctly) reject; pick a rate under the ceiling.
        pairs.push(("CEPHOR_CEPH_NEARFULL_RATE_BPS", "100000"));
        let config = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        let tick = config.tick_config();
        assert_eq!(tick.instance_id, "alloc-1");
        assert_eq!(tick.lease_ttl, config.lease_ttl);
        assert_eq!(config.ceiling().0, CephCeiling::Open(ByteRate::new(500_000)));
    }

    #[test]
    fn a_missing_database_url_reports_it() {
        let pairs = vec![("CEPHOR_ALLOCATOR_INSTANCE_ID", "alloc-1")];
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("CEPHOR_DATABASE_URL")));
    }

    #[test]
    fn reads_the_redis_url_and_defaults_the_allocation_ttl() {
        let config = AllocatorConfig::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.redis_queues_url, "redis://localhost:6382/0");
        assert_eq!(config.alloc_ttl, DEFAULT_ALLOCATION_TTL);
    }

    #[test]
    fn a_numeric_allocation_ttl_overrides_the_default() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_ALLOCATION_TTL_SECS", "20"));
        let config = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.alloc_ttl, std::time::Duration::from_secs(20));
    }

    #[test]
    fn a_missing_redis_url_is_reported() {
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_ALLOCATOR_INSTANCE_ID", "alloc-1"),
        ];
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("REDIS_QUEUES_URL")));
    }

    #[test]
    fn a_whitespace_instance_id_is_treated_as_missing() {
        let pairs = vec![
            ("CEPHOR_DATABASE_URL", "postgres://localhost/cephor"),
            ("CEPHOR_ALLOCATOR_INSTANCE_ID", "  \t "),
        ];
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(err, ConfigError::Missing("CEPHOR_ALLOCATOR_INSTANCE_ID")));
    }

    #[test]
    fn a_zero_ceph_ceiling_is_rejected() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_CEILING_BPS", "0"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::NonPositive {
                var: "CEPHOR_CEPH_CEILING_BPS"
            }
        ));
    }

    #[test]
    fn a_permille_above_1000_is_out_of_range() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_ALLOC_DECREASE_PERMILLE", "1500"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::OutOfRange {
                var: "CEPHOR_ALLOC_DECREASE_PERMILLE",
                value: 1500,
                limit: 1000,
            }
        ));
    }

    #[test]
    fn a_critical_pressure_above_10000_is_out_of_range() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_ALLOC_CRITICAL_PRESSURE_BPS", "12000"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::OutOfRange {
                var: "CEPHOR_ALLOC_CRITICAL_PRESSURE_BPS",
                value: 12000,
                limit: 10000,
            }
        ));
    }

    #[test]
    fn a_min_total_above_max_total_is_out_of_range() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_ALLOC_MIN_TOTAL_BPS", "2000000000"));
        pairs.push(("CEPHOR_ALLOC_MAX_TOTAL_BPS", "1000000000"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::OutOfRange {
                var: "CEPHOR_ALLOC_MIN_TOTAL_BPS",
                value: 2_000_000_000,
                limit: 1_000_000_000,
            }
        ));
    }

    #[test]
    fn a_non_numeric_tick_is_invalid() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_ALLOCATOR_TICK_SECS", "soon"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Invalid {
                var: "CEPHOR_ALLOCATOR_TICK_SECS",
                ..
            }
        ));
    }

    #[test]
    fn the_mgr_url_is_none_by_default_and_thresholds_default_to_the_cluster_ratios() {
        use hippius_drain_core::{CephCeiling, CephReport, classify};
        let config = AllocatorConfig::from_lookup(lookup(&required_only())).unwrap();
        assert_eq!(config.ceph_mgr_metrics_url, None, "no probe URL means the static fallback");
        assert_eq!(config.ceph_probe_timeout, super::DEFAULT_PROBE_TIMEOUT);
        // The default thresholds classify an 85% report as near-full and 95% as full.
        let report = |bps: u16| CephReport {
            osd_full: false,
            osd_nearfull: false,
            used: Some(DiskPressure::try_from(bps).unwrap()),
            pool_used: None,
        };
        assert_eq!(
            classify(&report(8_500), config.ceph_ceiling, config.ceph_nearfull_rate, &config.ceph_thresholds),
            CephCeiling::NearFull(config.ceph_nearfull_rate)
        );
        assert_eq!(
            classify(&report(9_500), config.ceph_ceiling, config.ceph_nearfull_rate, &config.ceph_thresholds),
            CephCeiling::Critical
        );
    }

    #[test]
    fn the_pool_gate_and_nearfull_rate_default_off_and_conservative() {
        let config = AllocatorConfig::from_lookup(lookup(&required_only())).unwrap();
        assert!(config.ceph_pools.is_empty(), "pool gating is opt-in per deployment");
        assert_eq!(config.ceph_nearfull_rate, ByteRate::new(10_000_000));
    }

    #[test]
    fn the_pools_and_nearfull_rate_are_read_from_the_environment() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_POOLS", "ceph-filesystem-data0, ceph-filesystem-metadata,ceph-blockpool"));
        pairs.push(("CEPHOR_CEPH_NEARFULL_RATE_BPS", "5000000"));
        let config = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.ceph_pools, ["ceph-filesystem-data0", "ceph-filesystem-metadata", "ceph-blockpool"]);
        assert_eq!(config.ceph_nearfull_rate, ByteRate::new(5_000_000));
    }

    #[test]
    fn a_blank_pool_list_is_treated_as_absent() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_POOLS", " , ,"));
        let config = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        assert!(config.ceph_pools.is_empty(), "commas and whitespace alone select no pools");
    }

    #[test]
    fn a_nearfull_rate_above_the_ceiling_is_rejected() {
        // NearFull carrying more budget than Open would invert the "fuller is
        // never looser" invariant the classifier property-tests.
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_CEILING_BPS", "1000000000"));
        pairs.push(("CEPHOR_CEPH_NEARFULL_RATE_BPS", "2000000000"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::OutOfRange {
                var: "CEPHOR_CEPH_NEARFULL_RATE_BPS",
                value: 2_000_000_000,
                limit: 1_000_000_000,
            }
        ));
    }

    #[cfg(feature = "http")]
    #[test]
    fn the_blind_probe_floor_is_never_looser_than_the_nearfull_rate() {
        // A blind probe cannot tell an open pool from a full one, so the rate it bottoms
        // out at must be the rate a KNOWN near-full pool carries — whatever the AIMD
        // floor happens to be tuned to. While the two shared
        // CEPHOR_ALLOC_MIN_TOTAL_BPS, prod's 250 MB/s latency floor let the fleet keep
        // writing 5x the near-full rate for as long as the mgr stayed unreachable.
        let mut pairs = required_only();
        pairs.push(("CEPHOR_ALLOC_MIN_TOTAL_BPS", "250000000"));
        pairs.push(("CEPHOR_CEPH_NEARFULL_RATE_BPS", "50000000"));
        let config = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        let settings = config.probe_settings("http://rook-ceph-mgr.rook-ceph.svc:9283/metrics".to_owned());
        assert_eq!(settings.floor, config.ceph_nearfull_rate, "the decay floor tracks the near-full rate");
        assert_eq!(
            decay(CephCeiling::Open(ByteRate::new(1_000_000_000)), u32::MAX, settings.floor),
            CephCeiling::Open(ByteRate::new(50_000_000)),
            "an indefinitely blind probe bottoms out at the near-full rate, not the AIMD floor",
        );
    }

    #[test]
    fn a_zero_nearfull_rate_is_rejected() {
        // Zero would silence the near-full relief valve entirely; a full stop is
        // Critical's job, so a zero here is a misconfiguration.
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_NEARFULL_RATE_BPS", "0"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::NonPositive {
                var: "CEPHOR_CEPH_NEARFULL_RATE_BPS"
            }
        ));
    }

    #[test]
    fn a_configured_mgr_url_is_read() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_MGR_METRICS_URL", "http://rook-ceph-mgr.rook-ceph.svc:9283/metrics"));
        let config = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(
            config.ceph_mgr_metrics_url.as_deref(),
            Some("http://rook-ceph-mgr.rook-ceph.svc:9283/metrics")
        );
    }

    #[test]
    fn a_blank_mgr_url_is_treated_as_absent() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_MGR_METRICS_URL", "   "));
        let config = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(config.ceph_mgr_metrics_url, None, "a blank URL must not select an unusable probe");
    }

    #[test]
    fn nearfull_above_full_thresholds_are_rejected() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_NEARFULL_BPS", "9600"));
        pairs.push(("CEPHOR_CEPH_FULL_BPS", "9500"));
        let err = AllocatorConfig::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::OutOfRange {
                var: "CEPHOR_CEPH_NEARFULL_BPS",
                value: 9_600,
                limit: 9_500,
            }
        ));
    }
}
