# OTLP exporter backpressure floods ERROR logs

## Summary

The OpenTelemetry OTLP/gRPC exporter's own internal logger
(`opentelemetry.exporter.otlp.proto.grpc.exporter` and sibling modules under the parent logger
`opentelemetry.exporter.otlp.proto.grpc`) emits `logger.error(...)` lines like:

```
Failed to export <signal> to <collector>, error code: StatusCode.UNAVAILABLE,
error details: data refused due to high memory usage
```

whenever the prod otel-collector's `memory_limiter` processor sheds load. These are telemetry-only,
transient backpressure events — not service errors — but they land at ERROR level and flood the
ERROR dashboards/alerts across every pod (api, gateway, arion-*, workers).

## Impact

- `server_errors=0` — no user-facing request ever fails because of these lines.
- Fleet-wide ERROR-dashboard and alert noise: every pod that exports traces/metrics emits them,
  drowning out genuine ERRORs and inflating error-rate panels.
- The app has no OTLP *log* exporter; app logs flow to Loki via `LokiLoggerHandler`, so these lines
  are purely the SDK's own internal reporting of failed telemetry exports.

## Root cause

Two layers:

1. **True upstream cause (infra):** the collector's `memory_limiter` processor in
   [k8s/base/otel-collector.yaml](../../k8s/base/otel-collector.yaml) refuses data when it is over its
   memory limit, returning `StatusCode.UNAVAILABLE` to exporters. Under load this is expected
   backpressure, not an outage.
2. **Symptom (app-side):** the OTLP SDK's internal exporter logger reports each refused export at
   ERROR level. The app-side fix below only de-noises the ERROR stream; it does not change the
   collector behavior.

## Fix

Add a `logging.Filter` (`OtelExporterBackpressureFilter`) in
[hippius_s3/logging_config.py](../../hippius_s3/logging_config.py) that DOWNGRADES a record at ERROR
level (or above) **whose logger name starts with `opentelemetry.exporter.otlp.proto.grpc`** to
WARNING — mutating `record.levelno`/`record.levelname` — and always returns `True` (never drops it).

It is attached inside `setup_loki_logging(...)` to the shared **handlers** (exactly like the existing
`RayIDFilter`), NOT to a logger:

```python
otel_backpressure_filter = OtelExporterBackpressureFilter()
for handler in handlers:
    handler.addFilter(otel_backpressure_filter)
```

### Why handler-attached + name-scoped (the important subtlety)

The noisy lines are emitted by the exporter's **child** loggers
(`...grpc.trace_exporter`, `...grpc.metric_exporter`, `...grpc.exporter`). A `logging.Filter`
attached to the *parent* logger `opentelemetry.exporter.otlp.proto.grpc` would **not** fire for them:
when a child logger emits, Python applies only the child's own logger-level filters, then propagates
the record to *ancestor handlers* via `callHandlers` — it never runs ancestor **logger** filters.
So the filter has to live on the **handlers** (which every propagated record passes through). Because
a handler sees every record from every logger, the filter is then **name-scoped** so it only touches
the otel exporter lines and leaves genuine app ERRORs alone.

The lines stay VISIBLE at WARNING — a persistent collector outage is still discoverable — but they no
longer inflate ERROR counts/alerts.

## Testing

Unit tests in [tests/unit/test_logging_config.py](../../tests/unit/test_logging_config.py) exercise the
real filter class and the real propagation path:

- An ERROR (and a CRITICAL) record from an otel exporter **child** logger is downgraded to WARNING;
  `filter()` returns `True`.
- A record already at WARNING (and one at INFO) is left unchanged.
- A NON-otel ERROR (e.g. `hippius_s3.api`) is left at ERROR — name scoping proven.
- End-to-end regression: a real `...grpc.trace_exporter` logger emitting at ERROR through a
  handler-attached filter is observed as WARNING, while a `hippius_s3.api.*` ERROR stays ERROR. This
  is the exact case the (rejected) parent-logger attachment would have missed.
- `setup_loki_logging` attaches the filter to the handlers, and NOT as a logger-level filter on the
  exporter logger.
- `filter()` never returns `False` for any level/name (never drops a record).

## Alternatives considered

- **Bump the collector memory / tune `memory_limiter`** — the correct real fix for the backpressure
  itself, but a separate infra change with its own capacity trade-offs; does not belong in the app and
  would still leave transient ERRORs during spikes.
- **Fully silence the logger** (`setLevel(CRITICAL)` / drop the records) — rejected: it hides genuine,
  persistent collector outages. Downgrading to WARNING keeps the signal while removing the false ERRORs.
