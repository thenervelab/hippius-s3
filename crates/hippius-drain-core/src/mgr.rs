//! Live Ceph-mgr ceiling: the pure half of the allocator's `CephCeilingSource`.
//!
//! This module turns the Ceph mgr **prometheus exporter** text into a single
//! fleet-wide [`CephCeiling`]. It is deliberately I/O-free — the `reqwest` shell
//! that fetches `/metrics` lives in `hippius-drain-allocator` — so the parse,
//! classify, and fail-safe-decay logic is testable as pure functions.
//!
//! Three fullness signals gate the ceiling: Ceph's own `OSD_NEARFULL` / `OSD_FULL`
//! health checks (per-OSD-aware, computed by Ceph), the always-present
//! `ceph_cluster_total_bytes` / `ceph_cluster_total_used_bytes` fleet ratio, and —
//! when a target pool is configured — that pool's `ceph_pool_percent_used`. The pool
//! signal exists because of the 2026-07-24 incident: the drain's target pool
//! (`CephFS` data) hit ~98% `%USED` while the cluster ratio sat at 72% and no OSD had
//! crossed its near-full ratio, so the ceiling stayed `Open` while the pool filled.
//! Pool `%USED` (`stored / (stored + max_avail)`) is the earliest fullness signal for
//! a pool whose CRUSH subtree fills ahead of the fleet average.

use crate::error::Error;
use crate::state::CephCeiling;
use crate::units::{ByteRate, DiskPressure};
use thiserror::Error as ThisError;

/// The always-present fleet-capacity metric. Its absence means the response is not
/// the Ceph mgr exporter (wrong endpoint, an error page), so the probe must fail
/// safe rather than read "no near-full series" as healthy.
const METRIC_TOTAL_BYTES: &str = "ceph_cluster_total_bytes";
/// The used-capacity counterpart of [`METRIC_TOTAL_BYTES`].
const METRIC_USED_BYTES: &str = "ceph_cluster_total_used_bytes";
/// Per-pool identity series; its `name`/`pool_id` labels map a pool name to the id
/// the capacity series are keyed by.
const METRIC_POOL_METADATA: &str = "ceph_pool_metadata";
/// Per-pool fullness fraction (`stored / (stored + max_avail)`, `0.0..=1.0`).
const METRIC_POOL_PERCENT_USED: &str = "ceph_pool_percent_used";

/// The parsed, flavor-agnostic near-full signal set from one mgr scrape.
///
/// `osd_full` / `osd_nearfull` default to `false`: the exporter emits a
/// `ceph_health_detail{name="OSD_NEARFULL"}` series only while that check is firing,
/// so an absent series is a healthy OSD set, not missing data. `used` is the
/// fleet-wide fullness fraction, `None` when the capacity metrics are absent or the
/// cluster reports zero total bytes (so the ratio cannot be formed without dividing
/// by zero).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CephReport {
    /// At least one OSD has crossed the full ratio — Ceph blocks writes.
    pub osd_full: bool,
    /// At least one OSD has crossed the near-full (or backfill-full) ratio.
    pub osd_nearfull: bool,
    /// Fleet-wide used fraction, when the capacity metrics were present.
    pub used: Option<DiskPressure>,
    /// The *fullest* requested pool's used fraction (the binding constraint —
    /// classification cares only about the pool closest to full). `None` only when
    /// no pools were requested — a requested pool that cannot be resolved is a
    /// parse error, so a missing pool signal fails safe instead of reading as
    /// healthy.
    pub pool_used: Option<DiskPressure>,
}

/// A failure turning mgr exporter text into a [`CephReport`].
///
/// `#[non_exhaustive]` because the parser's failure surface may grow (new mgr
/// versions, new required metrics) without that being a breaking change for the
/// allocator's fold-to-decay match.
#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum ProbeParseError {
    /// A metric that must be present was absent — the body is not a Ceph mgr scrape.
    #[error("required metric `{metric}` was absent from the mgr exporter output")]
    MissingMetric {
        /// The metric family that was expected but not found.
        metric: &'static str,
    },
    /// A metric was present but its value did not parse as a finite, in-range number.
    #[error("metric `{metric}` had a malformed value `{value}`")]
    MalformedValue {
        /// The metric family whose value failed to parse.
        metric: &'static str,
        /// The offending raw value.
        value: String,
    },
    /// The requested target pool had no `ceph_pool_metadata` series — the pool does
    /// not exist on this cluster (or was renamed), so its fullness cannot be read.
    #[error("target pool `{pool}` was absent from the mgr exporter output")]
    MissingPool {
        /// The pool name that was requested but not found.
        pool: String,
    },
}

/// Parses Ceph mgr prometheus exporter text into a [`CephReport`].
///
/// Every pool named in `target_pools` is resolved to its
/// [`METRIC_POOL_PERCENT_USED`] via its [`METRIC_POOL_METADATA`] `pool_id`; the
/// report carries the fullest one. An empty slice skips pool gating.
///
/// # Errors
///
/// [`ProbeParseError::MissingMetric`] if [`METRIC_TOTAL_BYTES`] is absent (the body
/// is not a mgr scrape) or a requested pool has no percent-used series,
/// [`ProbeParseError::MissingPool`] if a requested pool has no metadata series, or
/// [`ProbeParseError::MalformedValue`] if a recognized metric carries a value that
/// is not a finite number forming a valid `0.0..=1.0` ratio.
pub fn parse_prometheus_metrics(text: &str, target_pools: &[&str]) -> Result<CephReport, ProbeParseError> {
    let mut total: Option<f64> = None;
    let mut used: Option<f64> = None;
    let mut osd_full = false;
    let mut osd_nearfull = false;
    // Name->id pairs for requested pools, and all percent-used series, collected
    // before resolution because the exporter does not guarantee the metadata series
    // (which names a pool) precedes its capacity series.
    let mut pool_ids: Vec<(String, String)> = Vec::new();
    let mut pool_percents: Vec<(String, f64)> = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        // `#` lines are HELP/TYPE comments; blank lines separate metric families.
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let family = metric_family(line);
        if family == METRIC_TOTAL_BYTES {
            total = Some(finite_value(line, METRIC_TOTAL_BYTES)?);
        } else if family == METRIC_USED_BYTES {
            used = Some(finite_value(line, METRIC_USED_BYTES)?);
        } else if family == "ceph_health_detail" {
            // The exporter emits a series per health check only while it fires, with
            // value 1.0. A malformed/absent value is read as "not firing" rather than
            // a hard error — the cluster ratio is the always-present backstop.
            if health_check_firing(line, "name=\"OSD_FULL\"") {
                osd_full = true;
            }
            if health_check_firing(line, "name=\"OSD_NEARFULL\"") || health_check_firing(line, "name=\"OSD_BACKFILLFULL\"") {
                osd_nearfull = true;
            }
        } else if family == METRIC_POOL_METADATA
            && !target_pools.is_empty()
            // The extracted label value runs to the closing quote, so the match is
            // exact: a target `data` never matches a `name="data0"` series.
            && let Some(name) = label_value(line, "name=\"")
            && target_pools.contains(&name)
            && let Some(id) = label_value(line, "pool_id=\"")
        {
            pool_ids.push((name.to_owned(), id.to_owned()));
        } else if family == METRIC_POOL_PERCENT_USED
            && !target_pools.is_empty()
            && let Some(id) = label_value(line, "pool_id=\"")
        {
            pool_percents.push((id.to_owned(), finite_value(line, METRIC_POOL_PERCENT_USED)?));
        }
    }

    let Some(total) = total else {
        return Err(ProbeParseError::MissingMetric { metric: METRIC_TOTAL_BYTES });
    };
    let used = used_fraction(total, used)?;
    let pool_used = resolve_pool_used(target_pools, &pool_ids, &pool_percents)?;
    Ok(CephReport {
        osd_full,
        osd_nearfull,
        used,
        pool_used,
    })
}

/// Resolves the fullest requested pool's used fraction from the collected series.
///
/// Any requested pool that cannot be resolved is an error, never a skip: the pool
/// gate exists to fail safe, so a missing pool must decay the ceiling rather than
/// silently drop out of the max. A fraction just past `1.0` (exporter rounding)
/// clamps to fully-used, mirroring [`used_fraction`].
fn resolve_pool_used(
    target_pools: &[&str],
    pool_ids: &[(String, String)],
    pool_percents: &[(String, f64)],
) -> Result<Option<DiskPressure>, ProbeParseError> {
    let mut fullest: Option<DiskPressure> = None;
    for pool in target_pools {
        let Some((_, id)) = pool_ids.iter().find(|(name, _)| name == pool) else {
            return Err(ProbeParseError::MissingPool { pool: (*pool).to_owned() });
        };
        let Some((_, fraction)) = pool_percents.iter().find(|(pool_id, _)| pool_id == id) else {
            return Err(ProbeParseError::MissingMetric {
                metric: METRIC_POOL_PERCENT_USED,
            });
        };
        let pressure = DiskPressure::from_fraction(fraction.clamp(0.0, 1.0)).map_err(|_| ProbeParseError::MalformedValue {
            metric: METRIC_POOL_PERCENT_USED,
            value: fraction.to_string(),
        })?;
        fullest = Some(fullest.map_or(pressure, |current| current.max(pressure)));
    }
    Ok(fullest)
}

/// The label value following `needle` (e.g. `pool_id="`), up to its closing quote.
fn label_value<'a>(line: &'a str, needle: &str) -> Option<&'a str> {
    let start = line.find(needle)? + needle.len();
    line[start..].split('"').next()
}

/// The metric family name: everything before the label block `{` or the value.
fn metric_family(line: &str) -> &str {
    let end = line.find(['{', ' ', '\t']).unwrap_or(line.len());
    &line[..end]
}

/// Parses the trailing value of a metric line, rejecting a non-finite number.
fn finite_value(line: &str, metric: &'static str) -> Result<f64, ProbeParseError> {
    let raw = line.split_whitespace().next_back().unwrap_or("");
    let value = raw.parse::<f64>().map_err(|_| ProbeParseError::MalformedValue {
        metric,
        value: raw.to_owned(),
    })?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(ProbeParseError::MalformedValue {
            metric,
            value: raw.to_owned(),
        })
    }
}

/// True when a `ceph_health_detail` line carries `name_label` and a value `>= 1.0`.
fn health_check_firing(line: &str, name_label: &str) -> bool {
    line.contains(name_label)
        && line
            .split_whitespace()
            .next_back()
            .and_then(|v| v.parse::<f64>().ok())
            .is_some_and(|v| v >= 1.0)
}

/// Forms the fleet used-fraction from total/used bytes.
///
/// Returns `None` (not an error) when `used` is absent or `total` is zero — the
/// ratio is undefined, and the near-full health flags remain the signal. A used
/// value exceeding total (rounding artifact) clamps to fully-used.
fn used_fraction(total: f64, used: Option<f64>) -> Result<Option<DiskPressure>, ProbeParseError> {
    let (Some(used), true) = (used, total > 0.0) else {
        return Ok(None);
    };
    let fraction = (used / total).clamp(0.0, 1.0);
    let pressure = DiskPressure::from_fraction(fraction).map_err(|_| ProbeParseError::MalformedValue {
        metric: METRIC_USED_BYTES,
        value: used.to_string(),
    })?;
    Ok(Some(pressure))
}

/// The fleet-fullness watermarks that turn a [`CephReport`] into a ceiling band.
///
/// Modeled as [`DiskPressure`] (basis points) rather than `f64` so the comparisons
/// are integer and deterministic, mirroring the rest of the domain. Mirrors Ceph's
/// own `nearfull_ratio` / `full_ratio` (defaults 0.85 / 0.95 on the target cluster).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CephThresholds {
    nearfull: DiskPressure,
    full: DiskPressure,
}

impl CephThresholds {
    /// Builds the watermarks, requiring `nearfull <= full`.
    ///
    /// # Errors
    ///
    /// [`Error::CephThresholdOrder`] if `nearfull` exceeds `full` — an ordering that
    /// would let a full cluster classify as merely near-full.
    pub fn new(nearfull: DiskPressure, full: DiskPressure) -> Result<Self, Error> {
        if nearfull > full {
            return Err(Error::CephThresholdOrder {
                nearfull_bps: nearfull.bps(),
                full_bps: full.bps(),
            });
        }
        Ok(Self { nearfull, full })
    }
}

/// Classifies a [`CephReport`] into a fleet-wide [`CephCeiling`].
///
/// Precedence is full-then-near-full: a firing `OSD_FULL` check or a used fraction
/// at/above the full watermark yields [`CephCeiling::Critical`] (Ceph blocks writes,
/// so the fleet must buffer on SSD); a firing near-full check or a used fraction
/// at/above the near-full watermark yields [`CephCeiling::NearFull`]; otherwise
/// [`CephCeiling::Open`].
///
/// `NearFull` carries `nearfull_rate`, not the open `ceiling_rate`: the AIMD's
/// multiplicative back-off alone proved insufficient in the 2026-07-24 incident —
/// its estimate is clamped at the operator-tuned `min_total` floor (raised to
/// 50 MB/s in prod for latency reasons), so the fleet kept draining at the floor
/// into a near-full pool indefinitely. Carrying a reduced rate lets the ceiling
/// clamp (`estimate.min(budget)`) bound the drain below any AIMD floor.
#[must_use]
pub fn classify(report: &CephReport, ceiling_rate: ByteRate, nearfull_rate: ByteRate, thresholds: &CephThresholds) -> CephCeiling {
    let at = |fraction: Option<DiskPressure>, watermark: DiskPressure| fraction.is_some_and(|f| f >= watermark);
    if report.osd_full || at(report.used, thresholds.full) || at(report.pool_used, thresholds.full) {
        CephCeiling::Critical
    } else if report.osd_nearfull || at(report.used, thresholds.nearfull) || at(report.pool_used, thresholds.nearfull) {
        CephCeiling::NearFull(nearfull_rate)
    } else {
        CephCeiling::Open(ceiling_rate)
    }
}

/// The fail-safe ceiling to use when a probe attempt fails.
///
/// A blind probe must not keep handing out a stale full-rate ceiling, but it also
/// must not fabricate a near-full reading Ceph never reported. So `decay` keeps the
/// last-known **band** (an `Open` cluster stays `Open`, never minting a fake
/// `NearFull`) and instead shrinks the carried rate — halving it once per
/// `consecutive_failures` toward `floor` (the same deterministic shift-and-clamp as
/// [`decay_rate`](crate::decay_rate), keyed on failure count rather than elapsed
/// time). The allocator then drains at the shrinking floor while the probe is blind.
/// `Critical` already carries no rate, so it is returned unchanged. Zero failures is
/// the identity.
#[must_use]
pub fn decay(last_known: CephCeiling, consecutive_failures: u32, floor: ByteRate) -> CephCeiling {
    match last_known {
        CephCeiling::Open(rate) => CephCeiling::Open(halve_to_floor(rate, consecutive_failures, floor)),
        CephCeiling::NearFull(rate) => CephCeiling::NearFull(halve_to_floor(rate, consecutive_failures, floor)),
        CephCeiling::Critical => CephCeiling::Critical,
    }
}

/// Halves `base` once per failure toward `floor`, deterministically and without
/// dropping below the floor (or rising above `base` when `floor > base`).
fn halve_to_floor(base: ByteRate, failures: u32, floor: ByteRate) -> ByteRate {
    // A u64 shift of 64+ is UB; cap at 63, by which point the value is already 0 and
    // the floor clamp governs.
    let shift = failures.min(63);
    let decayed = base.get() >> shift;
    ByteRate::new(decayed.max(floor.get()).min(base.get()))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::{CephReport, ProbeParseError, parse_prometheus_metrics};
    use crate::units::DiskPressure;
    use proptest::prelude::*;

    /// A trimmed-but-real healthy scrape: the shape captured live from
    /// `rook-ceph-mgr:9283/metrics` (cluster ~27.6% used, no OSD near-full series).
    const HEALTHY: &str = "\
# HELP ceph_health_status Cluster health status
# TYPE ceph_health_status untyped
ceph_health_status 1.0
ceph_cluster_total_bytes 115222679470080.0
ceph_cluster_total_used_bytes 31791022084096.0
ceph_health_detail{name=\"MON_DISK_LOW\",severity=\"HEALTH_WARN\"} 1.0
ceph_osd_up{ceph_daemon=\"osd.0\"} 1.0
";

    /// The pool series shape captured live from `rook-ceph-mgr:9283/metrics` on
    /// 2026-07-24 (the incident cluster: pool 5 at ~95% while the fleet sat at 74%).
    const POOL_SERIES: &str = "\
ceph_pool_metadata{pool_id=\"2\",name=\"ceph-blockpool\",type=\"replicated\",description=\"replica:3\",compression_mode=\"none\"} 1.0
ceph_pool_metadata{pool_id=\"5\",name=\"ceph-filesystem-data0\",type=\"replicated\",description=\"replica:3\",compression_mode=\"none\"} 1.0
ceph_pool_percent_used{pool_id=\"2\"} 0.6975
ceph_pool_percent_used{pool_id=\"5\"} 0.9507322907447815
";

    #[test]
    fn healthy_scrape_reports_no_nearfull_and_the_used_fraction() {
        let report = parse_prometheus_metrics(HEALTHY, &[]).unwrap();
        // ~27.59% used -> 2759 bps (rounded), and neither OSD health check firing.
        let expected_used = DiskPressure::from_fraction(31_791_022_084_096.0 / 115_222_679_470_080.0).unwrap();
        assert_eq!(
            report,
            CephReport {
                osd_full: false,
                osd_nearfull: false,
                used: Some(expected_used),
                pool_used: None,
            }
        );
    }

    #[test]
    fn the_target_pools_used_fraction_is_resolved_via_its_metadata_id() {
        let scrape = format!("{HEALTHY}{POOL_SERIES}");
        let report = parse_prometheus_metrics(&scrape, &["ceph-filesystem-data0"]).unwrap();
        let expected = DiskPressure::from_fraction(0.950_732_290_744_781_5).unwrap();
        assert_eq!(report.pool_used, Some(expected), "pool 5's fraction, not pool 2's");
    }

    #[test]
    fn pool_series_order_does_not_matter() {
        // The exporter may emit percent-used series before the metadata that names
        // the pool; resolution must be order-independent.
        let scrape = format!(
            "{HEALTHY}\
ceph_pool_percent_used{{pool_id=\"5\"}} 0.5\n\
ceph_pool_metadata{{pool_id=\"5\",name=\"data\",type=\"replicated\"}} 1.0\n"
        );
        let report = parse_prometheus_metrics(&scrape, &["data"]).unwrap();
        assert_eq!(report.pool_used, Some(DiskPressure::from_fraction(0.5).unwrap()));
    }

    #[test]
    fn a_pool_name_that_prefixes_the_target_is_not_matched() {
        // `name="data0"` must not resolve for target `data` (nor vice versa): the
        // match is on the full quoted label value.
        let scrape = format!(
            "{HEALTHY}\
ceph_pool_metadata{{pool_id=\"7\",name=\"data0\",type=\"replicated\"}} 1.0\n\
ceph_pool_percent_used{{pool_id=\"7\"}} 0.99\n"
        );
        let err = parse_prometheus_metrics(&scrape, &["data"]).unwrap_err();
        assert!(matches!(err, ProbeParseError::MissingPool { ref pool } if pool == "data"));
    }

    #[test]
    fn the_fullest_of_several_requested_pools_gates() {
        // The gate covers every pool the stack writes (CephFS data + metadata,
        // the RBD blockpool); the binding constraint is whichever is fullest —
        // here the metadata pool, deliberately listed second.
        let report = parse_prometheus_metrics(&format!("{HEALTHY}{POOL_SERIES}"), &["ceph-blockpool", "ceph-filesystem-data0"]).unwrap();
        let expected = DiskPressure::from_fraction(0.950_732_290_744_781_5).unwrap();
        assert_eq!(report.pool_used, Some(expected), "the fullest pool wins, regardless of list order");
    }

    #[test]
    fn any_missing_pool_of_several_is_an_error() {
        // A typo'd or renamed pool anywhere in the list must fail safe, not
        // silently shrink the gate to the pools that did resolve.
        let scrape = format!("{HEALTHY}{POOL_SERIES}");
        let err = parse_prometheus_metrics(&scrape, &["ceph-filesystem-data0", "nonexistent"]).unwrap_err();
        assert!(matches!(err, ProbeParseError::MissingPool { ref pool } if pool == "nonexistent"));
    }

    #[test]
    fn a_requested_pool_with_no_metadata_is_a_missing_pool_error() {
        // Fail safe: a configured pool the exporter does not know cannot silently
        // read as healthy while that pool fills.
        let err = parse_prometheus_metrics(HEALTHY, &["ceph-filesystem-data0"]).unwrap_err();
        assert!(matches!(err, ProbeParseError::MissingPool { ref pool } if pool == "ceph-filesystem-data0"));
    }

    #[test]
    fn a_requested_pool_with_no_percent_series_is_a_missing_metric_error() {
        let scrape = format!("{HEALTHY}ceph_pool_metadata{{pool_id=\"5\",name=\"data\",type=\"replicated\"}} 1.0\n");
        let err = parse_prometheus_metrics(&scrape, &["data"]).unwrap_err();
        assert!(matches!(err, ProbeParseError::MissingMetric { metric } if metric == "ceph_pool_percent_used"));
    }

    #[test]
    fn a_malformed_pool_percent_value_is_rejected() {
        let scrape = format!(
            "{HEALTHY}\
ceph_pool_metadata{{pool_id=\"5\",name=\"data\",type=\"replicated\"}} 1.0\n\
ceph_pool_percent_used{{pool_id=\"5\"}} NaN\n"
        );
        let err = parse_prometheus_metrics(&scrape, &["data"]).unwrap_err();
        assert!(matches!(err, ProbeParseError::MalformedValue { metric, .. } if metric == "ceph_pool_percent_used"));
    }

    #[test]
    fn a_pool_percent_above_one_clamps_to_fully_used() {
        // Rounding artifacts can push the exporter fraction past 1.0; clamp rather
        // than reject, mirroring the cluster ratio's behavior.
        let scrape = format!(
            "{HEALTHY}\
ceph_pool_metadata{{pool_id=\"5\",name=\"data\",type=\"replicated\"}} 1.0\n\
ceph_pool_percent_used{{pool_id=\"5\"}} 1.0000001\n"
        );
        let report = parse_prometheus_metrics(&scrape, &["data"]).unwrap();
        assert_eq!(report.pool_used, Some(DiskPressure::from_fraction(1.0).unwrap()));
    }

    #[test]
    fn an_osd_nearfull_health_check_sets_nearfull() {
        let scrape = format!("{HEALTHY}ceph_health_detail{{name=\"OSD_NEARFULL\",severity=\"HEALTH_WARN\"}} 1.0\n");
        let report = parse_prometheus_metrics(&scrape, &[]).unwrap();
        assert!(report.osd_nearfull, "the OSD_NEARFULL series is firing");
        assert!(!report.osd_full, "no OSD_FULL series present");
    }

    #[test]
    fn an_osd_backfillfull_health_check_also_sets_nearfull() {
        // BACKFILLFULL is the earlier-warning band; treat it as near-full too. The
        // substring guard must not let OSD_BACKFILLFULL trip the OSD_FULL branch.
        let scrape = format!("{HEALTHY}ceph_health_detail{{name=\"OSD_BACKFILLFULL\",severity=\"HEALTH_WARN\"}} 1.0\n");
        let report = parse_prometheus_metrics(&scrape, &[]).unwrap();
        assert!(report.osd_nearfull);
        assert!(!report.osd_full, "BACKFILLFULL must not be misread as OSD_FULL");
    }

    #[test]
    fn an_osd_full_health_check_sets_full() {
        let scrape = format!("{HEALTHY}ceph_health_detail{{name=\"OSD_FULL\",severity=\"HEALTH_ERR\"}} 1.0\n");
        let report = parse_prometheus_metrics(&scrape, &[]).unwrap();
        assert!(report.osd_full, "the OSD_FULL series is firing");
    }

    #[test]
    fn a_nonfiring_health_series_value_zero_does_not_set_the_flag() {
        // The exporter can emit a known check at 0.0 when not active; absence of
        // value >= 1.0 must read as not-firing.
        let scrape = format!("{HEALTHY}ceph_health_detail{{name=\"OSD_NEARFULL\",severity=\"HEALTH_WARN\"}} 0.0\n");
        let report = parse_prometheus_metrics(&scrape, &[]).unwrap();
        assert!(!report.osd_nearfull, "a 0.0 value is not a firing check");
    }

    #[test]
    fn a_body_without_the_capacity_metric_is_a_missing_metric_error() {
        // An error page or the wrong endpoint: no ceph_cluster_total_bytes. Must fail
        // safe rather than read the absent near-full series as healthy.
        let err = parse_prometheus_metrics("not a ceph scrape\nrandom 1\n", &[]).unwrap_err();
        assert!(matches!(err, ProbeParseError::MissingMetric { metric } if metric == "ceph_cluster_total_bytes"));
    }

    #[test]
    fn a_nonnumeric_capacity_value_is_a_malformed_value_error() {
        let scrape = "ceph_cluster_total_bytes oops\nceph_cluster_total_used_bytes 1.0\n";
        let err = parse_prometheus_metrics(scrape, &[]).unwrap_err();
        assert!(matches!(
            err,
            ProbeParseError::MalformedValue { metric, ref value } if metric == "ceph_cluster_total_bytes" && value == "oops"
        ));
    }

    #[test]
    fn a_nonfinite_capacity_value_is_rejected() {
        // f64::parse accepts "NaN"/"inf"; a capacity that is not finite cannot form a
        // ratio and must be a malformed value, not a silently-dropped signal.
        let scrape = "ceph_cluster_total_bytes NaN\nceph_cluster_total_used_bytes 1.0\n";
        let err = parse_prometheus_metrics(scrape, &[]).unwrap_err();
        assert!(matches!(err, ProbeParseError::MalformedValue { metric, .. } if metric == "ceph_cluster_total_bytes"));
    }

    #[test]
    fn zero_total_bytes_yields_no_used_fraction_without_dividing_by_zero() {
        let scrape = "ceph_cluster_total_bytes 0.0\nceph_cluster_total_used_bytes 0.0\n";
        let report = parse_prometheus_metrics(scrape, &[]).unwrap();
        assert_eq!(report.used, None, "a zero-capacity cluster yields no ratio");
    }

    use super::{CephThresholds, classify, decay};
    use crate::error::Error;
    use crate::state::CephCeiling;
    use crate::units::ByteRate;

    const CEILING: ByteRate = ByteRate::new(1_000_000_000);
    const NEARFULL_RATE: ByteRate = ByteRate::new(10_000_000);
    const FLOOR: ByteRate = ByteRate::new(1_000_000);

    fn thresholds() -> CephThresholds {
        // 0.85 / 0.95 — the live cluster's nearfull / full ratios.
        CephThresholds::new(DiskPressure::try_from(8_500).unwrap(), DiskPressure::try_from(9_500).unwrap()).unwrap()
    }

    fn report(osd_full: bool, osd_nearfull: bool, used_bps: Option<u16>) -> CephReport {
        CephReport {
            osd_full,
            osd_nearfull,
            used: used_bps.map(|b| DiskPressure::try_from(b).unwrap()),
            pool_used: None,
        }
    }

    fn pool_report(pool_bps: u16) -> CephReport {
        CephReport {
            osd_full: false,
            osd_nearfull: false,
            used: Some(DiskPressure::try_from(2_000).unwrap()),
            pool_used: Some(DiskPressure::try_from(pool_bps).unwrap()),
        }
    }

    #[test]
    fn thresholds_reject_nearfull_above_full() {
        let err = CephThresholds::new(DiskPressure::try_from(9_600).unwrap(), DiskPressure::try_from(9_500).unwrap()).unwrap_err();
        assert!(matches!(
            err,
            Error::CephThresholdOrder { nearfull_bps, full_bps } if nearfull_bps == 9_600 && full_bps == 9_500
        ));
    }

    #[test]
    fn a_healthy_report_is_open() {
        let ceiling = classify(&report(false, false, Some(2_759)), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::Open(CEILING));
    }

    #[test]
    fn an_osd_full_flag_is_critical() {
        let ceiling = classify(&report(true, false, Some(2_000)), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::Critical, "OSD_FULL blocks writes regardless of the ratio");
    }

    #[test]
    fn used_at_or_above_full_is_critical() {
        assert_eq!(
            classify(&report(false, false, Some(9_500)), CEILING, NEARFULL_RATE, &thresholds()),
            CephCeiling::Critical
        );
        assert_eq!(
            classify(&report(false, false, Some(9_999)), CEILING, NEARFULL_RATE, &thresholds()),
            CephCeiling::Critical
        );
    }

    #[test]
    fn an_osd_nearfull_flag_is_nearfull() {
        let ceiling = classify(&report(false, true, Some(1_000)), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::NearFull(NEARFULL_RATE));
    }

    #[test]
    fn used_between_nearfull_and_full_is_nearfull() {
        assert_eq!(
            classify(&report(false, false, Some(8_500)), CEILING, NEARFULL_RATE, &thresholds()),
            CephCeiling::NearFull(NEARFULL_RATE)
        );
        assert_eq!(
            classify(&report(false, false, Some(9_499)), CEILING, NEARFULL_RATE, &thresholds()),
            CephCeiling::NearFull(NEARFULL_RATE)
        );
    }

    #[test]
    fn full_takes_precedence_over_nearfull() {
        // Both flags firing: Critical wins (a full OSD blocks writes).
        let ceiling = classify(&report(true, true, Some(9_900)), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::Critical);
    }

    #[test]
    fn absent_used_fraction_classifies_on_flags_alone() {
        assert_eq!(
            classify(&report(false, false, None), CEILING, NEARFULL_RATE, &thresholds()),
            CephCeiling::Open(CEILING)
        );
        assert_eq!(
            classify(&report(false, true, None), CEILING, NEARFULL_RATE, &thresholds()),
            CephCeiling::NearFull(NEARFULL_RATE)
        );
        assert_eq!(
            classify(&report(true, false, None), CEILING, NEARFULL_RATE, &thresholds()),
            CephCeiling::Critical
        );
    }

    #[test]
    fn a_full_target_pool_is_critical_even_when_cluster_and_osds_read_healthy() {
        // The 2026-07-24 incident shape: pool ~98% while the cluster ratio is ~72%
        // and no OSD health check fires. The pool signal alone must go Critical.
        let ceiling = classify(&pool_report(9_800), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::Critical);
    }

    #[test]
    fn a_nearfull_target_pool_is_nearfull() {
        let ceiling = classify(&pool_report(8_600), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::NearFull(NEARFULL_RATE));
    }

    #[test]
    fn a_healthy_target_pool_leaves_the_ceiling_open() {
        let ceiling = classify(&pool_report(5_000), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::Open(CEILING));
    }

    #[test]
    fn nearfull_carries_the_reduced_rate_not_the_open_ceiling() {
        // Regression for 2026-07-24: NearFull used to carry the open ceiling rate,
        // so the AIMD's min_total floor (50 MB/s in prod) was the only brake and the
        // drain kept flushing into a near-full pool. The reduced rate must win the
        // `estimate.min(budget)` clamp regardless of any AIMD floor.
        let ceiling = classify(&report(false, true, Some(1_000)), CEILING, NEARFULL_RATE, &thresholds());
        assert_eq!(ceiling, CephCeiling::NearFull(NEARFULL_RATE));
        assert_ne!(NEARFULL_RATE, CEILING, "the test is vacuous if the rates coincide");
    }

    #[test]
    fn decay_is_identity_at_zero_failures() {
        let open = CephCeiling::Open(CEILING);
        assert_eq!(decay(open, 0, FLOOR), open);
        assert_eq!(
            decay(CephCeiling::NearFull(NEARFULL_RATE), 0, FLOOR),
            CephCeiling::NearFull(NEARFULL_RATE)
        );
    }

    #[test]
    fn decay_halves_the_rate_per_failure_keeping_the_band() {
        // One failure halves; two quarters. The band stays Open — never a fake NearFull.
        assert_eq!(decay(CephCeiling::Open(CEILING), 1, FLOOR), CephCeiling::Open(ByteRate::new(500_000_000)));
        assert_eq!(decay(CephCeiling::Open(CEILING), 2, FLOOR), CephCeiling::Open(ByteRate::new(250_000_000)));
        assert_eq!(
            decay(CephCeiling::NearFull(NEARFULL_RATE), 1, FLOOR),
            CephCeiling::NearFull(ByteRate::new(5_000_000))
        );
    }

    #[test]
    fn decay_clamps_at_the_floor_and_never_below() {
        // After enough halvings the rate would underflow the floor; it clamps there.
        assert_eq!(decay(CephCeiling::Open(CEILING), 63, FLOOR), CephCeiling::Open(FLOOR));
        assert_eq!(decay(CephCeiling::Open(CEILING), u32::MAX, FLOOR), CephCeiling::Open(FLOOR));
    }

    #[test]
    fn decay_leaves_critical_at_zero() {
        assert_eq!(decay(CephCeiling::Critical, 5, FLOOR), CephCeiling::Critical);
    }

    proptest! {
        /// Fail-safe is monotone: more consecutive failures never *raise* the carried
        /// rate, and the band is never loosened (an Open input stays Open — no fake
        /// NearFull — and the rate is non-increasing in the failure count).
        #[test]
        fn decay_is_non_increasing_in_failures(base in 1u64..=u64::MAX, lo in 0u32..=80, extra in 0u32..=80) {
            let open = CephCeiling::Open(ByteRate::new(base));
            let earlier = decay(open, lo, FLOOR);
            let later = decay(open, lo.saturating_add(extra), FLOOR);
            match (earlier, later) {
                (CephCeiling::Open(e), CephCeiling::Open(l)) => prop_assert!(l.get() <= e.get()),
                _ => prop_assert!(false, "an Open input must decay within the Open band"),
            }
        }

        /// Monotonicity: a strictly fuller report never yields a *looser* ceiling.
        /// Encoded via a severity rank Open < NearFull < Critical over rising used%.
        #[test]
        fn fuller_used_never_loosens_the_ceiling(lo in 0u16..=10_000, hi in 0u16..=10_000) {
            prop_assume!(lo <= hi);
            let t = thresholds();
            let rank = |c: CephCeiling| match c {
                CephCeiling::Open(_) => 0u8,
                CephCeiling::NearFull(_) => 1,
                CephCeiling::Critical => 2,
            };
            let low = rank(classify(&report(false, false, Some(lo)), CEILING, NEARFULL_RATE, &t));
            let high = rank(classify(&report(false, false, Some(hi)), CEILING, NEARFULL_RATE, &t));
            prop_assert!(high >= low, "more-full ({hi}bps) must not be looser than less-full ({lo}bps)");
        }

        /// Pool monotonicity: a fuller target pool never yields a *looser* ceiling.
        #[test]
        fn fuller_pool_never_loosens_the_ceiling(lo in 0u16..=10_000, hi in 0u16..=10_000) {
            prop_assume!(lo <= hi);
            let t = thresholds();
            let rank = |c: CephCeiling| match c {
                CephCeiling::Open(_) => 0u8,
                CephCeiling::NearFull(_) => 1,
                CephCeiling::Critical => 2,
            };
            let low = rank(classify(&pool_report(lo), CEILING, NEARFULL_RATE, &t));
            let high = rank(classify(&pool_report(hi), CEILING, NEARFULL_RATE, &t));
            prop_assert!(high >= low, "a fuller pool ({hi}bps) must not be looser than a less-full one ({lo}bps)");
        }

        /// The parser is a boundary over untrusted bytes: it must never panic on
        /// arbitrary input, only return a report or a typed error.
        #[test]
        fn never_panics_on_arbitrary_text(text in ".*") {
            let _ = parse_prometheus_metrics(&text, &[]);
            let _ = parse_prometheus_metrics(&text, &["pool"]);
        }

        /// Any well-formed capacity pair yields a used fraction matching the ratio
        /// (within one basis point), and never errors.
        #[test]
        fn well_formed_capacity_round_trips_the_ratio(total in 1u32..=u32::MAX, used_frac in 0.0f64..=1.0) {
            let total_f = f64::from(total);
            let used_f = (used_frac * total_f).floor();
            let scrape = format!("ceph_cluster_total_bytes {total_f}\nceph_cluster_total_used_bytes {used_f}\n");
            let report = parse_prometheus_metrics(&scrape, &[]).unwrap();
            let used = report.used.expect("a positive total with a used value forms a fraction");
            let expected = DiskPressure::from_fraction((used_f / total_f).clamp(0.0, 1.0)).unwrap();
            prop_assert_eq!(used, expected);
        }
    }
}
