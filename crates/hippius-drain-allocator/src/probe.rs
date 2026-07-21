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

/// A [`CephCeilingSource`] backed by the Ceph mgr prometheus exporter.
#[derive(Debug)]
pub struct CephProbe {
    client: reqwest::Client,
    url: String,
    ceiling_rate: ByteRate,
    floor: ByteRate,
    thresholds: CephThresholds,
    state: Mutex<ProbeState>,
}

impl CephProbe {
    /// Builds a probe for the mgr exporter at `url`.
    ///
    /// `ceiling_rate` is the open ceiling handed to the allocator when Ceph is
    /// healthy; `floor` is the conservative rate the fail-safe decays toward while
    /// the probe is blind; `thresholds` are the near-full / full watermarks.
    ///
    /// # Errors
    ///
    /// [`ProbeError::Http`] if the HTTP client cannot be built (e.g. an invalid TLS
    /// or timeout configuration).
    pub fn new(url: String, ceiling_rate: ByteRate, floor: ByteRate, thresholds: CephThresholds, timeout: Duration) -> Result<Self, ProbeError> {
        let client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            url,
            ceiling_rate,
            floor,
            thresholds,
            state: Mutex::new(ProbeState {
                last_known: CephCeiling::Open(ceiling_rate),
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
        let response = self.client.get(&self.url).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(ProbeError::Status { code: status.as_u16() });
        }
        let body = response.text().await?;
        Ok(parse_prometheus_metrics(&body)?)
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
                let ceiling = classify(&report, self.ceiling_rate, &self.thresholds);
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
                let ceiling = decay(last_known, failures, self.floor);
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
    use super::CephProbe;
    use hippius_drain_core::DiskPressure;
    use hippius_drain_core::{ByteRate, CephCeiling, CephCeilingSource, CephThresholds};
    use std::time::Duration;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CEILING: ByteRate = ByteRate::new(1_000_000_000);
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

    fn probe(url: String) -> CephProbe {
        CephProbe::new(url, CEILING, FLOOR, thresholds(), Duration::from_secs(2)).unwrap()
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
    async fn an_osd_nearfull_scrape_yields_a_nearfull_ceiling() {
        let body = format!("{HEALTHY}ceph_health_detail{{name=\"OSD_NEARFULL\",severity=\"HEALTH_WARN\"}} 1.0\n");
        let server = server_returning(200, &body).await;
        let probe = probe(format!("{}/metrics", server.uri()));
        assert_eq!(probe.ceiling().await, CephCeiling::NearFull(CEILING));
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
