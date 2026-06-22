//! Allocator configuration parsed from the environment.
//!
//! [`AllocatorConfig::from_env`] is the only public entry; the parsing core
//! ([`AllocatorConfig::from_lookup`]) takes a lookup closure so it is tested with
//! a fixture map instead of the process-global environment — which is shared
//! mutable state that races across parallel tests (mirrors `hippius-drain-agent`'s
//! config).

use hippius_drain_core::{AllocConfig, ByteRate, CephCeiling, CephThresholds, DiskPressure, StaticCeiling, TickConfig};
use std::num::ParseIntError;
use std::time::Duration;
use thiserror::Error;

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
/// Heartbeats older than this are ignored when loading the fleet
/// (`CEPHOR_FLEET_STALE_SECS`). Several agent heartbeat periods (~10s) wide.
const DEFAULT_FLEET_STALE: Duration = Duration::from_secs(30);
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
/// p99 latency (ms) above which the fleet is considered saturated.
const DEFAULT_TARGET_P99_MS: u64 = 50;
/// Error rate (basis points) above which the fleet is considered saturated.
const DEFAULT_MAX_ERROR_BPS: u16 = 100;
/// Pressure (basis points) at/above which a node earns a reservation floor.
const DEFAULT_CRITICAL_PRESSURE_BPS: u16 = 9_000;
/// Guaranteed per-node floor (bytes/sec) for critical-pressure nodes.
const DEFAULT_RESERVATION_FLOOR_BPS: u64 = 1_000_000;
/// Ceph near-full watermark (basis points) when `CEPHOR_CEPH_NEARFULL_BPS` is unset.
/// Mirrors the cluster's `nearfull_ratio` of 0.85.
const DEFAULT_CEPH_NEARFULL_BPS: u16 = 8_500;
/// Ceph full watermark (basis points) when `CEPHOR_CEPH_FULL_BPS` is unset. Mirrors
/// the cluster's `full_ratio` of 0.95.
const DEFAULT_CEPH_FULL_BPS: u16 = 9_500;
/// Per-scrape timeout for the live mgr probe when `CEPHOR_CEPH_PROBE_TIMEOUT_SECS`
/// is unset — short relative to the tick so a hung mgr decays rather than stalls.
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// Heartbeats older than this are ignored when reading the fleet.
    pub fleet_stale: Duration,
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
    /// Per-scrape timeout for the live probe.
    pub ceph_probe_timeout: Duration,
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
            stale_after: self.fleet_stale,
            alloc: self.alloc,
        }
    }

    /// The static ceiling source, used when no `CEPHOR_CEPH_MGR_METRICS_URL` is set
    /// (the live Ceph-mgr probe is the configured path; this is the fallback).
    #[must_use]
    pub fn ceiling(&self) -> StaticCeiling {
        StaticCeiling(CephCeiling::Open(self.ceph_ceiling))
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
        };
        Ok(Self {
            database_url: required(&get, "CEPHOR_DATABASE_URL")?,
            instance_id: required(&get, "CEPHOR_ALLOCATOR_INSTANCE_ID")?,
            lease_ttl: duration_secs(&get, "CEPHOR_LEADER_LEASE_TTL_SECS", DEFAULT_LEASE_TTL)?,
            tick_interval: duration_secs(&get, "CEPHOR_ALLOCATOR_TICK_SECS", DEFAULT_TICK)?,
            fleet_stale: duration_secs(&get, "CEPHOR_FLEET_STALE_SECS", DEFAULT_FLEET_STALE)?,
            ceph_ceiling: ByteRate::new(positive_u64(&get, "CEPHOR_CEPH_CEILING_BPS", DEFAULT_CEPH_CEILING_BPS)?),
            // The first tick starts from the AIMD floor unless overridden, so a
            // fresh leader ramps up from a safe rate rather than a guess.
            initial_total: ByteRate::new(u64_or(&get, "CEPHOR_ALLOC_INITIAL_TOTAL_BPS", min_total)?),
            alloc,
            ceph_mgr_metrics_url: optional(&get, "CEPHOR_CEPH_MGR_METRICS_URL"),
            ceph_thresholds: ceph_thresholds(&get)?,
            ceph_probe_timeout: duration_secs(&get, "CEPHOR_CEPH_PROBE_TIMEOUT_SECS", DEFAULT_PROBE_TIMEOUT)?,
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

/// Resolves an optional integer variable, falling back to `default` when unset.
/// A present-but-unparsable value is a loud error, not a silent fallback.
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
        AllocatorConfig, ConfigError, DEFAULT_CRITICAL_PRESSURE_BPS, DEFAULT_DECREASE_PERMILLE, DEFAULT_MAX_TOTAL_BPS, DEFAULT_MIN_TOTAL_BPS,
        DEFAULT_TICK,
    };
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
    fn tick_config_and_ceiling_reflect_the_config() {
        let mut pairs = required_only();
        pairs.push(("CEPHOR_CEPH_CEILING_BPS", "500000"));
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
        };
        assert_eq!(
            classify(&report(8_500), config.ceph_ceiling, &config.ceph_thresholds),
            CephCeiling::NearFull(config.ceph_ceiling)
        );
        assert_eq!(
            classify(&report(9_500), config.ceph_ceiling, &config.ceph_thresholds),
            CephCeiling::Critical
        );
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
