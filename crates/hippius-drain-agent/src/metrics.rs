//! OTLP metrics export for the drain agent (feature `otel`).
//!
//! Mirrors `hippius_s3/otel_setup.py`: a gRPC OTLP exporter pushing to the
//! otel-collector on a periodic reader, with `service.name` set on the resource (the
//! collector turns the OTLP resource attributes — merged from `OTEL_RESOURCE_ATTRIBUTES`
//! — into Prometheus labels). Every value is read from the already-computed
//! [`SnapshotCell`] / [`Enforcer`], so a metric scrape never measures anything itself.

use hippius_drain_core::Enforcer;
use hippius_drain_core::SnapshotCell;
use opentelemetry_otlp::MetricExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

/// How often the reader collects + pushes metrics (matches the Python worker cadence).
const EXPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Owns the meter provider (for a final flush on shutdown) and keeps the observable
/// instruments alive for the process lifetime — their callbacks fire on the reader thread,
/// so the handles must outlive `init`.
pub struct MetricsHandle {
    provider: SdkMeterProvider,
    _instruments: Vec<Box<dyn std::any::Any>>,
}

// `_instruments` holds `Box<dyn Any>` (no Debug); a manual impl satisfies the workspace's
// missing-Debug lint without leaking the opaque instrument handles.
impl std::fmt::Debug for MetricsHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsHandle").finish_non_exhaustive()
    }
}

impl MetricsHandle {
    /// Flush and shut the exporter down on process exit (a best-effort final export).
    pub fn shutdown(self) {
        if let Err(err) = self.provider.shutdown() {
            tracing::warn!(error = %err, "meter provider shutdown failed");
        }
    }
}

/// Whether metrics export is enabled (`ENABLE_MONITORING` truthy), mirroring the Python
/// `otel_setup` guard so a dev/test run without a collector exports nothing.
fn monitoring_enabled() -> bool {
    std::env::var("ENABLE_MONITORING").is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
}

/// Builds the OTLP gRPC meter provider and installs it globally, or returns `None` on a
/// build failure (logged, non-fatal — the daemon runs without metrics rather than aborting).
fn build_provider(service_name: &'static str) -> Option<SdkMeterProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| "http://otel-collector:4317".to_owned());
    let exporter = match MetricExporter::builder().with_tonic().with_endpoint(endpoint).build() {
        Ok(exporter) => exporter,
        Err(err) => {
            tracing::warn!(error = %err, "failed to build the OTLP metric exporter; metrics disabled");
            return None;
        }
    };
    let reader = PeriodicReader::builder(exporter).with_interval(EXPORT_INTERVAL).build();
    let resource = Resource::builder().with_service_name(service_name).build();
    let provider = SdkMeterProvider::builder().with_reader(reader).with_resource(resource).build();
    opentelemetry::global::set_meter_provider(provider.clone());
    Some(provider)
}

/// Builds the meter provider and registers the agent's metrics, or returns `None` when
/// monitoring is disabled or the exporter cannot be built. `enforcer` is `None` for an
/// ungated drain (no breaker to observe).
#[must_use]
pub fn init(service_name: &'static str, snapshot: &Arc<SnapshotCell>, enforcer: Option<&Arc<Mutex<Enforcer>>>) -> Option<MetricsHandle> {
    if !monitoring_enabled() {
        return None;
    }
    let provider = build_provider(service_name)?;
    let meter = opentelemetry::global::meter("hippius-drain-agent");
    let mut instruments: Vec<Box<dyn std::any::Any>> = Vec::new();

    // Throughput: parts committed Replicated (monotonic counter).
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_parts_replicated_total")
            .with_callback(move |observer| observer.observe(snap.load().drained, &[]))
            .build(),
    ));

    // Saturation: undrained SSD bytes (gauge). Backlog growth is what fills the SSD and
    // 503s every PUT on the node, so it is the key operational drain gauge.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_ssd_backlog_bytes")
            .with_callback(move |observer| observer.observe(snap.backlog(), &[]))
            .build(),
    ));

    // Silent-failure alarm: the Ceph breaker (trips at debug-level today) as a 0/1 gauge.
    if let Some(enforcer) = enforcer {
        let enforcer = Arc::clone(enforcer);
        instruments.push(Box::new(
            meter
                .u64_observable_gauge("drain_breaker_open")
                .with_callback(move |observer| {
                    let open = enforcer.lock().unwrap_or_else(PoisonError::into_inner).breaker_open(Instant::now());
                    observer.observe(u64::from(open), &[]);
                })
                .build(),
        ));
    }

    Some(MetricsHandle {
        provider,
        _instruments: instruments,
    })
}
