//! OTLP metrics export for the allocator (feature `otel`).
//!
//! Mirrors `hippius_s3/otel_setup.py` (gRPC OTLP to the otel-collector, periodic reader,
//! `service.name` on the resource). The two gauges read the plain-atomic
//! [`AllocatorMetrics`](crate::run::AllocatorMetrics) the tick loop updates, so a scrape
//! never touches the loop's state.

use crate::run::AllocatorMetrics;
use opentelemetry_otlp::MetricExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// How often the reader collects + pushes metrics (matches the Python worker cadence).
const EXPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Owns the meter provider (for a final flush) and keeps the observable instruments alive
/// for the process lifetime — their callbacks fire on the reader thread.
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

/// Whether metrics export is enabled (`ENABLE_MONITORING` truthy).
fn monitoring_enabled() -> bool {
    std::env::var("ENABLE_MONITORING").is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
}

/// Builds the OTLP gRPC meter provider and installs it globally, or returns `None` on a
/// build failure (logged, non-fatal).
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

/// Builds the meter provider and registers the allocator's gauges (`drain_leader`,
/// `drain_fleet_estimate_bps`), or returns `None` when monitoring is disabled or the
/// exporter cannot be built. Both gauges read the shared [`AllocatorMetrics`].
#[must_use]
pub fn init(service_name: &'static str, metrics: &Arc<AllocatorMetrics>) -> Option<MetricsHandle> {
    if !monitoring_enabled() {
        return None;
    }
    let provider = build_provider(service_name)?;
    let meter = opentelemetry::global::meter("hippius-drain-allocator");
    let mut instruments: Vec<Box<dyn std::any::Any>> = Vec::new();

    // Is this instance the leader (did leader election converge)? 0/1 gauge.
    let m = Arc::clone(metrics);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_leader")
            .with_callback(move |observer| observer.observe(m.leader.load(Ordering::Relaxed), &[]))
            .build(),
    ));

    // The fleet write-budget estimate handed out on the last led tick.
    let m = Arc::clone(metrics);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_fleet_estimate_bps")
            .with_callback(move |observer| observer.observe(m.fleet_estimate_bps.load(Ordering::Relaxed), &[]))
            .build(),
    ));

    Some(MetricsHandle {
        provider,
        _instruments: instruments,
    })
}
