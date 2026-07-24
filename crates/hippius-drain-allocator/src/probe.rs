//! The live Ceph-mgr ceiling probe: the I/O shell around `hippius-drain-core`'s
//! pure parse/classify/decay.
//!
//! [`CephProbe`] implements [`CephCeilingSource`] by scraping the Ceph mgr
//! prometheus exporter once per tick and folding the result into a [`CephCeiling`].
//! A probe failure never propagates — the trait is infallible by contract — so the
//! probe holds the last *successful* ceiling and a consecutive-failure count and
//! returns a decayed ceiling on failure (see [`hippius_drain_core::decay`]): it backs
//! off toward a floor while blind, and never fabricates a near-full reading.

use hippius_drain_core::{
    ByteRate, CephCeiling, CephCeilingSource, CephReport, CephThresholds, ProbeParseError, classify, decay, parse_prometheus_metrics,
};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use thiserror::Error;

/// A failure performing one mgr scrape.
///
/// `#[non_exhaustive]` because the probe's failure surface may grow (auth, new
/// transports) without breaking the allocator's fold-to-decay, which treats every
/// variant identically: decay the last-known ceiling.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProbeError {
    /// The HTTP request itself failed (connect refused, timeout, transport error).
    #[error("ceph mgr request failed")]
    Http(#[from] reqwest::Error),
    /// The mgr answered with a non-success HTTP status.
    #[error("ceph mgr returned HTTP status {code}")]
    Status {
        /// The status code returned.
        code: u16,
    },
    /// The body was fetched but did not parse as mgr exporter output.
    #[error("ceph mgr response did not parse")]
    Parse(#[from] ProbeParseError),
}

/// The probe's across-tick state: the last *successful* ceiling and how many scrapes
/// have failed since. `last_known` is the decay base — it is never overwritten with a
/// decayed value, so `N` failures decay it once by `N`, not compound once per tick.
#[derive(Debug, Clone, Copy)]
struct ProbeState {
    last_known: CephCeiling,
    consecutive_failures: u32,
}

/// Everything [`CephProbe::new`] needs. A settings struct rather than positional
/// parameters: the probe's signal set grows (the target pool and the reduced
/// near-full rate were added after the 2026-07-24 pool-fill incident), and seven
/// positional arguments of mostly-`ByteRate` types invite transposition bugs.
#[derive(Debug, Clone)]
pub struct ProbeSettings {
    /// The mgr prometheus exporter URL to scrape.
    pub url: String,
    /// The open ceiling handed to the allocator when Ceph is healthy.
    pub ceiling_rate: ByteRate,
    /// The reduced rate a near-full cluster or pool carries — this must bound the
    /// drain below any AIMD floor, so it is deliberately not `ceiling_rate`.
    pub nearfull_rate: ByteRate,
    /// The conservative rate the fail-safe decays toward while the probe is blind.
    pub floor: ByteRate,
    /// The near-full / full watermarks.
    pub thresholds: CephThresholds,
    /// Per-scrape HTTP timeout.
    pub timeout: Duration,
    /// The pools whose `percent_used` additionally gate the ceiling (the fullest
    /// one binds). Empty gates on cluster-wide signals only — the pre-incident
    /// behavior, kept for clusters with no stable pool names.
    pub pools: Vec<String>,
}

/// A [`CephCeilingSource`] backed by the Ceph mgr prometheus exporter.
#[derive(Debug)]
pub struct CephProbe {
    client: reqwest::Client,
    settings: ProbeSettings,
    state: Mutex<ProbeState>,
}

impl CephProbe {
    /// Builds a probe for the mgr exporter described by `settings`.
    ///
    /// # Errors
    ///
    /// [`ProbeError::Http`] if the HTTP client cannot be built (e.g. an invalid TLS
    /// or timeout configuration).
    pub fn new(settings: ProbeSettings) -> Result<Self, ProbeError> {
        let client = reqwest::Client::builder().timeout(settings.timeout).build()?;
        let last_known = CephCeiling::Open(settings.ceiling_rate);
        Ok(Self {
            client,
            settings,
            state: Mutex::new(ProbeState {
                last_known,
                consecutive_failures: 0,
            }),
        })
    }

    /// How many consecutive scrapes have failed (0 right after a success).
    ///
    /// Exposed for observability/alerting: a sustained non-zero count is the signal
    /// that the allocator is flying blind on a decayed ceiling.
    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.lock().consecutive_failures
    }

    /// Performs one scrape: GET the exporter, check status, parse the body.
    ///
    /// # Errors
    ///
    /// [`ProbeError`] on transport failure, a non-success status, or unparseable body.
    async fn probe(&self) -> Result<CephReport, ProbeError> {
        let response = self.client.get(&self.settings.url).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(ProbeError::Status { code: status.as_u16() });
        }
        let body = response.text().await?;
        let pools: Vec<&str> = self.settings.pools.iter().map(String::as_str).collect();
        Ok(parse_prometheus_metrics(&body, &pools)?)
    }

    /// Locks the state, recovering the guard if a previous holder poisoned it. The
    /// probe never panics while holding the lock, so poisoning is unreachable, but
    /// recovering keeps the no-panic contract without an `unwrap`.
    fn lock(&self) -> std::sync::MutexGuard<'_, ProbeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl CephCeilingSource for CephProbe {
    async fn ceiling(&self) -> CephCeiling {
        // Snapshot the decay base, then drop the guard *before* the await: holding a
        // std::sync::Mutex guard across an await makes the future !Send and is denied
        // by `await_holding_lock` (axiom rust_quality_74).
        let last_known = self.lock().last_known;

        match self.probe().await {
            Ok(report) => {
                let ceiling = classify(
                    &report,
                    self.settings.ceiling_rate,
                    self.settings.nearfull_rate,
                    &self.settings.thresholds,
                );
                let mut state = self.lock();
                state.last_known = ceiling;
                state.consecutive_failures = 0;
                ceiling
            }
            Err(err) => {
                let failures = self.lock().consecutive_failures.saturating_add(1);
                // Decay the last *successful* ceiling by the new failure count — never
                // the already-decayed value, so the back-off is geometric in failures,
                // not compounded per tick. `last_known` is deliberately left intact.
                let ceiling = decay(last_known, failures, self.settings.floor);
                self.lock().consecutive_failures = failures;
                tracing::warn!(error = %err, consecutive_failures = failures, ?ceiling, "ceph mgr probe failed; using decayed ceiling");
                ceiling
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{CephProbe, ProbeSettings};
    use hippius_drain_core::DiskPressure;
    use hippius_drain_core::{ByteRate, CephCeiling, CephCeilingSource, CephThresholds};
    use std::time::Duration;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CEILING: ByteRate = ByteRate::new(1_000_000_000);
    const NEARFULL_RATE: ByteRate = ByteRate::new(10_000_000);
    const FLOOR: ByteRate = ByteRate::new(1_000_000);

    /// The trimmed-but-real healthy scrape captured from the live mgr exporter.
    const HEALTHY: &str = "\
ceph_health_status 1.0
ceph_cluster_total_bytes 115222679470080.0
ceph_cluster_total_used_bytes 31791022084096.0
ceph_health_detail{name=\"MON_DISK_LOW\",severity=\"HEALTH_WARN\"} 1.0
";

    fn thresholds() -> CephThresholds {
        CephThresholds::new(DiskPressure::try_from(8_500).unwrap(), DiskPressure::try_from(9_500).unwrap()).unwrap()
    }

    fn settings(url: String) -> ProbeSettings {
        ProbeSettings {
            url,
            ceiling_rate: CEILING,
            nearfull_rate: NEARFULL_RATE,
            floor: FLOOR,
            thresholds: thresholds(),
            timeout: Duration::from_secs(2),
            pools: Vec::new(),
        }
    }

    fn probe(url: String) -> CephProbe {
        CephProbe::new(settings(url)).unwrap()
    }

    fn pool_probe(url: String, pools: &[&str]) -> CephProbe {
        CephProbe::new(ProbeSettings {
            pools: pools.iter().map(|&p| p.to_owned()).collect(),
            ..settings(url)
        })
        .unwrap()
    }

    async fn server_returning(status: u16, body: &str) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status).set_body_string(body))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn a_healthy_scrape_yields_an_open_ceiling() {
        let server = server_returning(200, HEALTHY).await;
        let probe = probe(format!("{}/metrics", server.uri()));
        assert_eq!(probe.ceiling().await, CephCeiling::Open(CEILING));
        assert_eq!(probe.consecutive_failures(), 0, "a success resets the failure count");
    }

    #[tokio::test]
    async fn an_osd_nearfull_scrape_yields_a_nearfull_ceiling_at_the_reduced_rate() {
        let body = format!("{HEALTHY}ceph_health_detail{{name=\"OSD_NEARFULL\",severity=\"HEALTH_WARN\"}} 1.0\n");
        let server = server_returning(200, &body).await;
        let probe = probe(format!("{}/metrics", server.uri()));
        // The reduced rate, not the open ceiling: the ceiling clamp must bound the
        // drain below any operator-raised AIMD floor (2026-07-24 incident).
        assert_eq!(probe.ceiling().await, CephCeiling::NearFull(NEARFULL_RATE));
    }

    #[tokio::test]
    async fn a_full_target_pool_yields_critical_on_an_otherwise_healthy_scrape() {
        // The 2026-07-24 incident shape end-to-end: cluster ~27% used, no OSD checks
        // firing, but the drain's target pool at 98%.
        let body = format!(
            "{HEALTHY}\
ceph_pool_metadata{{pool_id=\"5\",name=\"ceph-filesystem-data0\",type=\"replicated\"}} 1.0\n\
ceph_pool_percent_used{{pool_id=\"5\"}} 0.98\n"
        );
        let server = server_returning(200, &body).await;
        let probe = pool_probe(format!("{}/metrics", server.uri()), &["ceph-filesystem-data0"]);
        assert_eq!(probe.ceiling().await, CephCeiling::Critical);
    }

    #[tokio::test]
    async fn a_nearfull_target_pool_yields_the_reduced_nearfull_rate() {
        let body = format!(
            "{HEALTHY}\
ceph_pool_metadata{{pool_id=\"5\",name=\"ceph-filesystem-data0\",type=\"replicated\"}} 1.0\n\
ceph_pool_percent_used{{pool_id=\"5\"}} 0.88\n"
        );
        let server = server_returning(200, &body).await;
        let probe = pool_probe(format!("{}/metrics", server.uri()), &["ceph-filesystem-data0"]);
        assert_eq!(probe.ceiling().await, CephCeiling::NearFull(NEARFULL_RATE));
    }

    #[tokio::test]
    async fn the_fullest_of_several_configured_pools_gates_through_the_probe() {
        // Both CephFS pools plus the blockpool are configured; the fullest (the
        // metadata pool here) drives the band even though the others are healthy.
        let body = format!(
            "{HEALTHY}\
ceph_pool_metadata{{pool_id=\"2\",name=\"ceph-blockpool\",type=\"replicated\"}} 1.0\n\
ceph_pool_metadata{{pool_id=\"3\",name=\"ceph-filesystem-metadata\",type=\"replicated\"}} 1.0\n\
ceph_pool_metadata{{pool_id=\"5\",name=\"ceph-filesystem-data0\",type=\"replicated\"}} 1.0\n\
ceph_pool_percent_used{{pool_id=\"2\"}} 0.10\n\
ceph_pool_percent_used{{pool_id=\"3\"}} 0.97\n\
ceph_pool_percent_used{{pool_id=\"5\"}} 0.20\n"
        );
        let server = server_returning(200, &body).await;
        let probe = pool_probe(
            format!("{}/metrics", server.uri()),
            &["ceph-blockpool", "ceph-filesystem-metadata", "ceph-filesystem-data0"],
        );
        assert_eq!(probe.ceiling().await, CephCeiling::Critical);
    }

    #[tokio::test]
    async fn a_scrape_missing_the_configured_pool_decays_rather_than_reading_healthy() {
        // A renamed/deleted target pool must fail safe like any other parse failure.
        let server = server_returning(200, HEALTHY).await;
        let probe = pool_probe(format!("{}/metrics", server.uri()), &["ceph-filesystem-data0"]);
        assert_eq!(probe.ceiling().await, CephCeiling::Open(ByteRate::new(500_000_000)));
        assert_eq!(probe.consecutive_failures(), 1);
    }

    #[tokio::test]
    async fn an_osd_full_scrape_yields_a_critical_ceiling() {
        let body = format!("{HEALTHY}ceph_health_detail{{name=\"OSD_FULL\",severity=\"HEALTH_ERR\"}} 1.0\n");
        let server = server_returning(200, &body).await;
        let probe = probe(format!("{}/metrics", server.uri()));
        assert_eq!(probe.ceiling().await, CephCeiling::Critical);
    }

    #[tokio::test]
    async fn a_503_decays_the_last_known_ceiling() {
        let server = server_returning(503, "service unavailable").await;
        let probe = probe(format!("{}/metrics", server.uri()));
        // Last-known starts Open(CEILING); one failure halves the carried rate.
        assert_eq!(probe.ceiling().await, CephCeiling::Open(ByteRate::new(500_000_000)));
        assert_eq!(probe.consecutive_failures(), 1);
    }

    #[tokio::test]
    async fn an_unparseable_body_decays_the_last_known_ceiling() {
        // 200 OK but not mgr output (e.g. an ingress error page): the missing
        // capacity metric must fail safe to a decayed ceiling, not read as healthy.
        let server = server_returning(200, "<html>not ceph</html>").await;
        let probe = probe(format!("{}/metrics", server.uri()));
        assert_eq!(probe.ceiling().await, CephCeiling::Open(ByteRate::new(500_000_000)));
        assert_eq!(probe.consecutive_failures(), 1);
    }

    #[tokio::test]
    async fn consecutive_failures_deepen_the_decay() {
        let server = server_returning(503, "down").await;
        let probe = probe(format!("{}/metrics", server.uri()));
        assert_eq!(probe.ceiling().await, CephCeiling::Open(ByteRate::new(500_000_000)));
        assert_eq!(probe.ceiling().await, CephCeiling::Open(ByteRate::new(250_000_000)));
        assert_eq!(probe.consecutive_failures(), 2);
    }

    #[tokio::test]
    async fn a_connection_error_decays_rather_than_panicking() {
        // A transport error must fold to decay. Point at a never-bound port directly
        // instead of dropping a MockServer and reusing its freed port: under concurrent
        // tests another MockServer can rebind that port, so the request would
        // unexpectedly succeed (a flaky no-decay). 127.0.0.1:1 is never listened on, so
        // the connect refuses deterministically.
        let probe = probe("http://127.0.0.1:1/metrics".to_owned());
        assert_eq!(probe.ceiling().await, CephCeiling::Open(ByteRate::new(500_000_000)));
        assert_eq!(probe.consecutive_failures(), 1);
    }

    #[tokio::test]
    async fn a_success_after_failures_resets_the_count() {
        // First GET 503 (one failure), then healthy thereafter.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503).set_body_string("down"))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(HEALTHY))
            .mount(&server)
            .await;

        let probe = probe(format!("{}/metrics", server.uri()));
        assert_eq!(
            probe.ceiling().await,
            CephCeiling::Open(ByteRate::new(500_000_000)),
            "the 503 decays once"
        );
        assert_eq!(probe.consecutive_failures(), 1);
        assert_eq!(probe.ceiling().await, CephCeiling::Open(CEILING), "the success restores the open ceiling");
        assert_eq!(probe.consecutive_failures(), 0, "and resets the failure count");
    }
}
