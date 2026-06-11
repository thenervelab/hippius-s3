#!/usr/bin/env python3
"""Disk canary: per-mount health probe for the FS cache volumes on the cache node.

Every interval, for each configured mount: lstat the root, then write -> read -> delete a
heartbeat file under <mount>/.canary/. Each check runs in a thread with a hard timeout because a
stale CephFS kernel mount HANGS IO syscalls (it doesn't error) — the 2026-06-11 incident surfaced
as `permission denied` on lstat, but the hang case must page too.

Emits OTel observable gauges (same pipeline as the janitor):
  disk_canary_mount_healthy{mount=...}  1 healthy / 0 unhealthy
  disk_canary_op_seconds{mount=...}     duration of the last successful check
and logs `DISK_CANARY_FAIL mount=... reason=...` at ERROR for Loki-side visibility.

Grafana pages on `disk_canary_mount_healthy < 1` AND on metric absence (node too broken to run
the canary at all — the pods-stuck-in-Init case).
"""

import asyncio
import logging
import os
import socket
import sys
import time
import uuid
from pathlib import Path

from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.sentry import init_sentry


config = get_config()

setup_loki_logging(config, "disk-canary", include_ray_id=False)
logger = logging.getLogger(__name__)
init_sentry("disk-canary", is_worker=True)

CHECK_INTERVAL_SECONDS = float(os.getenv("DISK_CANARY_INTERVAL_SECONDS", "30"))
# A healthy local write+fsync is milliseconds; a stale mount hangs forever. 10s is unambiguous.
CHECK_TIMEOUT_SECONDS = float(os.getenv("DISK_CANARY_TIMEOUT_SECONDS", "10"))
MOUNTS = [
    m.strip()
    for m in os.getenv(
        "DISK_CANARY_MOUNTS",
        "/var/lib/hippius/local_object_cache,/var/lib/hippius/object_cache",
    ).split(",")
    if m.strip()
]

_mount_healthy: dict[str, int] = dict.fromkeys(MOUNTS, 1)
_mount_op_seconds: dict[str, float] = dict.fromkeys(MOUNTS, 0.0)


def _obs_healthy(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(v, {"mount": m}) for m, v in _mount_healthy.items()]


def _obs_op_seconds(_: object) -> list[otel_metrics.Observation]:
    return [otel_metrics.Observation(v, {"mount": m}) for m, v in _mount_op_seconds.items()]


def _setup_metrics() -> None:
    existing = otel_metrics.get_meter_provider()
    if not isinstance(existing, MeterProvider):
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
        service_name = os.getenv("OTEL_SERVICE_NAME", "hippius-s3")
        resource = Resource.create({"service.name": service_name, "service.instance.id": socket.gethostname()})
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=endpoint, insecure=True),
            export_interval_millis=10000,
        )
        otel_metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))

    meter = otel_metrics.get_meter("disk-canary")
    meter.create_observable_gauge(
        name="disk_canary_mount_healthy",
        callbacks=[_obs_healthy],
        description="1 if the mount passed the last write/read/delete probe, 0 otherwise",
    )
    meter.create_observable_gauge(
        name="disk_canary_op_seconds",
        callbacks=[_obs_op_seconds],
        description="Duration of the last successful probe per mount",
    )


def check_mount(mount: str) -> float:
    """Synchronous probe of one mount: lstat root, write -> read -> delete a heartbeat file.

    Returns the elapsed seconds on success; raises on any failure. Runs inside a thread with an
    outer timeout because stale network mounts hang rather than error.
    """
    started = time.monotonic()
    root = Path(mount)
    os.lstat(root)  # stale CephFS staging mounts fail exactly here (permission denied)

    canary_dir = root / ".canary"
    canary_dir.mkdir(exist_ok=True)
    payload = uuid.uuid4().bytes
    heartbeat = canary_dir / f"heartbeat.{socket.gethostname()}"
    tmp = canary_dir / f".tmp.{uuid.uuid4().hex}"
    with tmp.open("wb") as fp:
        fp.write(payload)
        fp.flush()
        os.fsync(fp.fileno())
    tmp.replace(heartbeat)
    read_back = heartbeat.read_bytes()
    if read_back != payload:
        raise RuntimeError(f"read-back mismatch ({len(read_back)} bytes)")
    heartbeat.unlink()
    return time.monotonic() - started


async def _check_one(mount: str) -> None:
    try:
        elapsed = await asyncio.wait_for(asyncio.to_thread(check_mount, mount), timeout=CHECK_TIMEOUT_SECONDS)
        if _mount_healthy[mount] == 0:
            logger.info(f"DISK_CANARY_RECOVERED mount={mount} op_seconds={elapsed:.3f}")
        _mount_healthy[mount] = 1
        _mount_op_seconds[mount] = elapsed
    except TimeoutError:
        _mount_healthy[mount] = 0
        logger.error(f"DISK_CANARY_FAIL mount={mount} reason=timeout_after_{CHECK_TIMEOUT_SECONDS}s (stale mount?)")
    except Exception as e:
        _mount_healthy[mount] = 0
        logger.error(f"DISK_CANARY_FAIL mount={mount} reason={type(e).__name__}: {e}")


async def run_disk_canary_loop() -> None:
    _setup_metrics()
    logger.info(
        f"Starting disk canary (mounts={MOUNTS} interval={CHECK_INTERVAL_SECONDS}s timeout={CHECK_TIMEOUT_SECONDS}s)"
    )
    while True:
        # NOTE: a hung to_thread check leaks its worker thread past the timeout; that is acceptable
        # — the gauge flips to 0 and the pod gets recycled when the node is remediated.
        await asyncio.gather(*[_check_one(m) for m in MOUNTS])
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(run_disk_canary_loop())
