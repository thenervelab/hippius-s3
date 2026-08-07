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

/// Registers the SSD read-tier instruments: how much is resident, and how fast the evictor is
/// giving it back.
///
/// Split out of [`init`] to keep it under the 100-line limit, and because these three read as
/// one story: `cache_bytes` climbing while `evicted_total` stays flat is a node heading for
/// `fs_cache_pressure` 503s.
fn register_ssd_tier_instruments(meter: &opentelemetry::metrics::Meter, snapshot: &Arc<SnapshotCell>, instruments: &mut Vec<Box<dyn std::any::Any>>) {
    // Resident read-tier size (gauge): bytes this node holds on SSD to serve reads. The
    // counterpart to the backlog gauge, and deliberately separate from it — the allocator
    // counts this toward ingest headroom, not against it, so conflating the two is what would
    // make a warm cache read as a drain emergency. Also the direct answer to "is the read tier
    // actually filling up", which was the whole point of retaining parts.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_ssd_cache_bytes")
            .with_callback(move |observer| observer.observe(snap.cache_bytes(), &[]))
            .build(),
    ));

    // Eviction throughput (monotonic counters): parts and bytes the read-tier evictor
    // reclaimed. With retention on, the evictor is the ONLY worker freeing the ingest SSD, so
    // this staying flat while cache_bytes climbs toward the disk size is the tell that ingest
    // is heading for fs_cache_pressure 503s.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_ssd_evicted_total")
            .with_callback(move |observer| observer.observe(snap.evicted(), &[]))
            .build(),
    ));
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_ssd_evicted_bytes_total")
            .with_callback(move |observer| observer.observe(snap.evicted_bytes(), &[]))
            .build(),
    ));

    // The eviction durability invariant, as a counter so an alert can assert it stays at zero.
    // Non-zero means the worklist offered a part whose SSD copy may be the only durable one —
    // refused, but the query and the invariant have diverged.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_ssd_evict_blocked_unreplicated_total")
            .with_callback(move |observer| observer.observe(snap.evict_blocked_unreplicated(), &[]))
            .build(),
    ));

    // Free space: the third leg of backlog/cache/free. Without it a dashboard cannot separate
    // "the read cache grew" from "the disk is filling with backlog".
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_ssd_free_bytes")
            .with_callback(move |observer| observer.observe(snap.free_bytes(), &[]))
            .build(),
    ));
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

    // Backlog: undrained SSD bytes (gauge) — the DB-sourced true drain demand (pending +
    // draining part bytes), NOT raw disk occupancy, so the A21/orphan leak no longer
    // inflates it (WI-20c). The heartbeat writes it from Store::node_backlog_bytes.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_ssd_backlog_bytes")
            .with_callback(move |observer| observer.observe(snap.backlog(), &[]))
            .build(),
    ));

    register_ssd_tier_instruments(&meter, snapshot, &mut instruments);

    // Discovery path split. `landed_recorded` climbing while `reconciler_recovered` stays near
    // zero is what says the api's announcements are carrying discovery and the reconciler's
    // whole-disk walk is genuinely idle. Both near zero means ingest stopped — NOT that the
    // fast path is working — which is why the pair has to be read together.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_landed_recorded_total")
            .with_callback(move |observer| observer.observe(snap.load().landed_recorded, &[]))
            .build(),
    ));
    // Must stay at zero. Nonzero means the api and this agent disagree about the message shape,
    // so every announcement is being discarded and discovery has silently reverted to the walk.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_landed_dropped_total")
            .with_callback(move |observer| observer.observe(snap.load().landed_dropped, &[]))
            .build(),
    ));

    // Reclaim throughput: terminal SSD parts the reclaim worker unlinked (monotonic counter).
    // The SSD-ingest tier's eviction rate — distinct from the drain's CephFS throughput — so a
    // stalled reclaim (backlog rising while this stays flat) is visible without reading logs.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_ssd_reclaimed_total")
            .with_callback(move |observer| observer.observe(snap.load().reclaimed, &[]))
            .build(),
    ));

    // Reclaim-disabled alarm: cycles the `failed`-part SSD GC aborted on an object-backing read
    // error (`object_versions` missing on a deploy, or a transient PG error). On this path the
    // reclaim reads nothing and removes NOTHING — it is silently disabled while failing safe, so
    // without a metric the GC stops and SSD debris accrues invisibly until drain_ssd_pressure trips
    // the critical PUT-shedding alert. A monotonic counter: a sustained increase pages.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_reclaim_backing_errors_total")
            .with_callback(move |observer| observer.observe(snap.load().reclaim_backing_errors, &[]))
            .build(),
    ));

    // Durability alarm: parts held `corrupt` (a live object whose pool copy is corrupt, kept
    // alive by its SSD source — R4). A gauge, not a counter: it falls when a re-drive recovers
    // a part. Any nonzero value pages (the alert), distinct from the drain's routine failures.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_corrupt_parts")
            .with_callback(move |observer| observer.observe(snap.corrupt_parts(), &[]))
            .build(),
    ));

    // Saturation: SSD disk fill fraction (0.0..=1.0). This is what crosses the api's
    // fs_cache_pressure cutoff and 503s every PUT on the node, so it is the operational
    // alert signal — and unlike the backlog it rises with a leak even at zero drain demand.
    // Sourced from the heartbeat's statvfs probe (bps in the snapshot), scaled to a fraction.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .f64_observable_gauge("drain_ssd_pressure")
            .with_callback(move |observer| observer.observe(f64::from(snap.disk_pressure_bps()) / 10_000.0, &[]))
            .build(),
    ));

    // Starvation: age of this node's oldest still-`pending` row (gauge, seconds), refreshed
    // by the heartbeat from Store::node_oldest_pending_age_secs. The 2026-07-26 incident's
    // earliest unambiguous signal — one node's age exploding while its peers sat near zero —
    // was only reachable by hand-written SQL; this makes it a standing per-node gauge.
    // Deferred (backed-off) rows count: an MPU wall aging past hours is exactly the tell.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_gauge("drain_pending_oldest_age_seconds")
            .with_callback(move |observer| observer.observe(snap.oldest_pending_age_secs(), &[]))
            .build(),
    ));

    // Write-off alarm: parts escalated to terminal `failed` by the missing-source path
    // (monotonic counter). The only DURABLE signal of a write-off — the row is excluded from
    // node_undrained_count and later deleted by the terminal GC, leaving just a WARN log.
    // Deliberately not folded into error_bps/failed: a write-off is not a Ceph failure.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_parts_written_off_total")
            .with_callback(move |observer| observer.observe(snap.load().written_off, &[]))
            .build(),
    ));

    // Durability page: the subset of write-offs whose version was still SERVABLE (or of
    // unknown servability) — acknowledged client data declared undrainable. The 2026-07-22/26
    // incidents left this shape observable only as WARN logs; any increase pages
    // (`increase(drain_parts_written_off_servable_total[1h]) > 0`). Always <= the total above,
    // which keeps counting every write-off, servable or not.
    let snap = Arc::clone(snapshot);
    instruments.push(Box::new(
        meter
            .u64_observable_counter("drain_parts_written_off_servable_total")
            .with_callback(move |observer| observer.observe(snap.load().written_off_servable, &[]))
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
