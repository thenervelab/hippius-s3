import logging
import os
from typing import Literal
from typing import Optional
from typing import Union

from fastapi import Request
from fastapi import Response
from opentelemetry import metrics
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from hippius_s3.otel_setup import build_metric_views
from hippius_s3.otel_setup import build_resource


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


# The storage tiers a chunk read can be served from, closed by construction so the `tier`
# label cannot drift into unbounded cardinality.
ChunkReadTier = Literal["local", "peer", "pool"]

# Why a peer fetch did not happen, or its answer was not used. Closed by construction, like
# ChunkReadTier. The reasons demand different responses and must stay distinguishable:
# `client_cap` and `server_busy` are capacity; `peer_miss` is the peer having evicted the chunk
# between the residency read and the fetch, which is routine and settles on its own; and the
# rest mean something is wrong. `bad_peer_url` is a peer registration this cluster did not
# write, `bad_length` a peer serving bodies that are not the chunk that was asked for,
# `unknown_size` a chunk with no recorded ciphertext size to check an answer against, and
# `peer_error` any other non-200 — a half-rolled deploy, which is a pod to go find.
#
# Every one of these has to be counted, including the routine `peer_miss`. The alternative is
# that a wholesale peer-tier failure — an eviction storm, or a bad image on every pod — shows up
# only as a DROP in `chunk_reads_by_tier_total{tier=peer}`, and an absence is indistinguishable
# from the tier simply being idle, so nothing can alert on it.
PeerShedReason = Literal[
    "client_cap",
    "server_busy",
    "bad_peer_url",
    "bad_length",
    "unknown_size",
    "peer_miss",
    "peer_error",
]

# Why a chunk that could have been promoted onto local flash was not. Closed by construction.
# `residency_failed` is the fail-closed arm: the claim that makes this node's evictor the owner
# of the copy did not land, so the copy is not made.
PromotionSkipReason = Literal["disk_pressure", "residency_failed"]

# Which way the drain agent's published promote floor differs from this process's configured
# one. Closed by construction, like the labels above.
PromoteFloorDivergence = Literal["stricter", "looser"]

# Outcome of a read-recency stamp. Closed by construction. `failed` is separate rather than
# uncounted because the write is best-effort and swallowed: without it, a recency path that is
# erroring on every read is indistinguishable from one that is being fully absorbed by the
# sampler, and both look like silence.
ReadRecencyOutcome = Literal["written", "failed"]
# Why an announcement did not reach redis-queues. `timeout` is deliberately its own value:
# it is the outcome a slow (rather than broken) queue produces, and the one the bound on
# the publish introduces, so it has to be distinguishable from an outright error.
LandedAnnounceOutcome = Literal["timeout", "error"]

# Where the bytes that failed a chunk's AEAD check were found, and what came of it. Closed by
# construction. `local` means a copy on this node's flash was removed and the read retried from
# the next tier; `remote` means nothing local held it (a peer or the pool served it), so there
# was nothing this node could invalidate and no retry was worth making. A sustained
# `local`/`recovered` rate is a poisoner planting bad bytes on this node — the pool copy is fine.
# Anything `unrecovered` survived a tier change, so it is a key or object fault, not local
# corruption; the two must stay distinguishable or a DEK fault reads like cache poisoning.
AeadFailureTier = Literal["local", "remote"]
AeadFailureOutcome = Literal["recovered", "unrecovered"]


class MetricsCollector:
    """OTel metrics for the API, gateway and workers.

    Every attribute passed here becomes a Prometheus label, so the value set of each
    one must be BOUNDED and closed — methods, status codes, backends, named handlers.
    Never add an attribute whose values are open-ended or chosen by a caller: account
    ids, bucket names, object keys, request paths, error strings. Those axes grow
    forever and each new value permanently costs a series.

    This is not theoretical. main_account/subaccount_id were labels here until
    2026-07-17 and had grown to 87% of the namespace's ~51k series (115 accounts, one
    new axis value per customer) while no dashboard or alert ever read them. Send
    per-account detail to the gateway audit log or to spans instead — both index
    high-cardinality keys by design.
    """

    def __init__(self, redis_client: Union[Redis, RedisCluster, None] = None):
        # Optional: redis-less workers (e.g. the cachet health checker) still want the
        # counter/histogram surface. The observable gauges read cached ints, not the
        # client, so None is safe.
        self.redis_client = redis_client
        self.meter = metrics.get_meter(__name__)
        self._queue_lengths: dict[str, int] = {}
        self._used_mem = 0
        self._max_mem = 0
        self._backup_last_success_timestamp = 0.0
        self._db_pool_size = 0
        self._db_pool_free = 0
        self._db_pool_used = 0
        self._setup_metrics()

    def _setup_metrics(self) -> None:
        self.http_requests_total = self.meter.create_counter(
            name="http_requests_total", description="Total number of HTTP requests", unit="1"
        )

        self.http_request_duration = self.meter.create_histogram(
            name="http_request_duration_seconds", description="HTTP request duration in seconds", unit="s"
        )

        # Time in the middleware chain OUTSIDE the audited window: auth, ACL, account resolution,
        # input validation, and the BaseHTTPMiddleware wrapping around them.
        #
        # Both `http_request_duration_seconds` and the audit log's `processing_time_ms` are measured
        # from INSIDE the chain — audit sits 18 layers deep — so neither can see the 17 layers above
        # it, SigV4 verification and the ACL check among them. Measured on prod 2026-08-31 that blind
        # spot was the same order as everything it wrapped: a ~930 B PUT spent ~247 ms inside the
        # window and ~261 ms outside it, and a request REJECTED by auth — no handler, no DB, no
        # disk — still cost 176 ms. This histogram is what makes that half of the request visible.
        self.http_pre_handler_duration = self.meter.create_histogram(
            name="http_pre_handler_duration_seconds",
            description="Request time spent in the middleware chain outside the audited window",
            unit="s",
        )

        # Origin TTFB, measured from gateway_start_time (the earliest clock any layer can read,
        # stamped just inside CORS) to the moment the request stops waiting on us:
        #   - requests that carry a body (PUT/UploadPart/POST): the first body byte the app reads
        #     off the wire, i.e. all auth/ACL/DB work that gates accepting the upload;
        #   - everything else (GET/HEAD/list/delete): response start — which for GetObject already
        #     includes the first decrypted chunk, since the endpoint peeks it before returning the
        #     StreamingResponse (A2 bound in object_reader.py).
        # `http_request_duration_seconds` cannot serve this purpose: on uploads it is dominated by
        # draining the client's body, so it measures the client's bandwidth, not our latency.
        self.http_request_ttfb = self.meter.create_histogram(
            name="http_request_ttfb_seconds",
            description="Time from request arrival to first accepted upload byte (writes) or response start (reads)",
            unit="s",
        )

        self.http_request_bytes = self.meter.create_counter(
            name="http_request_bytes_total", description="Total bytes in HTTP requests", unit="bytes"
        )

        self.http_response_bytes = self.meter.create_counter(
            name="http_response_bytes_total", description="Total bytes in HTTP responses", unit="bytes"
        )

        self.s3_bytes_uploaded = self.meter.create_counter(
            name="s3_bytes_uploaded_total", description="Total bytes uploaded to S3", unit="bytes"
        )

        self.s3_bytes_downloaded = self.meter.create_counter(
            name="s3_bytes_downloaded_total", description="Total bytes downloaded from S3", unit="bytes"
        )

        self.s3_operations_total = self.meter.create_counter(
            name="s3_operations_total", description="Total S3 operations by type", unit="1"
        )

        self.s3_errors_total = self.meter.create_counter(
            name="s3_errors_total", description="Total S3 errors by type", unit="1"
        )

        self.fs_cache_shed_total = self.meter.create_counter(
            name="fs_cache_shed_total",
            description="Writes rejected by the FS-cache-pressure gate, by reason and pressure mode",
            unit="1",
        )

        self.cache_hits = self.meter.create_counter(name="cache_hits_total", description="Total cache hits", unit="1")

        self.cache_misses = self.meter.create_counter(
            name="cache_misses_total", description="Total cache misses", unit="1"
        )

        # Which storage tier actually served a chunk: local NVMe, a peer node's NVMe, or the
        # CephFS pool. Without this split the SSD read tier is unmeasurable — every tier reads
        # as "cache" — so there is no way to tell whether retention, promotion, and peer fetch
        # are doing anything, or to catch a silent regression back to all-pool reads.
        # Every peer fetch that did not yield bytes, under the reason it did not — see
        # PeerShedReason. This is the only POSITIVE signal the peer tier has: without it a tier
        # that has failed wholesale reads on a dashboard exactly like a tier nobody is using.
        self.peer_fetch_shed = self.meter.create_counter(
            name="peer_fetch_shed_total",
            description="Peer chunk fetches that yielded no bytes, by reason (see PeerShedReason)",
            unit="1",
        )

        self.chunk_reads_by_tier = self.meter.create_counter(
            name="chunk_reads_by_tier_total",
            description="Chunk reads served, by storage tier (local|peer|pool)",
            unit="1",
        )

        # Chunks served but deliberately NOT copied onto local flash. This is the promotion
        # backpressure made visible: `disk_pressure` must start rising BEFORE fs_cache_shed does,
        # because promotion yielding is what keeps the disk from reaching the PUT-refusal
        # threshold. Flat at zero while free space falls means the gate is not engaging.
        # `residency_failed` says promotion is off because the residency DB is unreachable —
        # sustained, it means the read tier has stopped warming and only this counter says so.
        self.promotion_skipped = self.meter.create_counter(
            name="promotion_skipped_total",
            description="Chunks not promoted to the local read tier, by reason (disk_pressure|residency_failed)",
            unit="1",
        )

        # A residency claim that could not be given back after its disk write failed. Each one
        # leaves phantom bytes in cephor_ssd_residency (the claim ACCUMULATES on conflict), so a
        # sustained rate means node_cache_bytes is inflating and the allocator is steering on a
        # figure the disk does not hold. Reaching this at all takes a DB fault inside the same
        # promotion whose claim just succeeded, so it should be near zero.
        self.residency_release_failures = self.meter.create_counter(
            name="residency_release_failures_total",
            description="Residency claims left in place because the compensating decrement failed",
            unit="1",
        )

        # An aborted MPU's directory was removed from this node's SSD but its residency rows
        # could not be. Inert while they linger (the version is `failed`, and every residency
        # reader requires `replicated`) and deleted by the drain's failed-part reclaim later,
        # but a sustained rate means this node's ledger is drifting from its disk on every
        # abort, which nothing else reports.
        self.residency_drop_failures = self.meter.create_counter(
            name="residency_drop_failures_total",
            description="Aborted-version residency rows left in place because the delete failed",
            unit="1",
        )

        # A landed-part announcement the api could not hand to redis-queues. This MUST be
        # alertable rather than a log line, because the announcement is the only trigger for the
        # B-2 divergence check: the reconciler tallies an already-`replicated` part as an orphan
        # and deliberately does not content-check it, so a lost announcement for a RE-uploaded
        # part means the pool keeps the previous attempt's ciphertext and serves it under the new
        # ETag, decrypting cleanly, permanently. `drain_landed_dropped_total` on the agent counts
        # only messages it could not PARSE, so it cannot see a message that never arrived.
        # `outcome` is a Literal, so cardinality is bounded.
        self.landed_announce_failures = self.meter.create_counter(
            name="landed_announce_failures_total",
            description="Landed-part announcements the api failed to enqueue, by outcome",
            unit="1",
        )

        # Gate decisions taken on the drain agent's published floor rather than the configured
        # one. `stricter` rising means the allocator has raised this node's eviction reserve —
        # the read tier is deliberately backing off a stressed disk, and that must be visible
        # rather than looking like promotion silently stopped working.
        self.promote_floor_divergence = self.meter.create_counter(
            name="promote_floor_divergence_total",
            description="Promotion gate decisions using a published floor that differs from the configured one (stricter|looser)",
            unit="1",
        )

        # Read-recency stamps actually written to cephor_ssd_residency. This is a DB UPDATE on
        # the read path, sampled to at most one per part per window — so the rate is bounded by
        # DISTINCT parts read per window, not by read throughput. That bound is weakest for the
        # workload the read tier exists for: a full-shard scan (a training epoch) touches far
        # more distinct parts than the sampler's memo holds, so the memo stops absorbing and
        # every part read becomes one write. Counting it is what tells us whether that is
        # happening before Postgres does.
        self.read_recency_writes = self.meter.create_counter(
            name="read_recency_writes_total",
            description="last_read_at stamps written to cephor_ssd_residency, by outcome (written|failed)",
            unit="1",
        )

        # Chunks whose stored ciphertext failed to authenticate. Every one is either bad bytes in
        # a cache or a key fault, and before this counter existed both were invisible — the only
        # handling was a 500 mapped by class name at the edge. The tier is what makes a poisoner
        # actionable: `local` failures are a copy this node holds and just dropped, so a sustained
        # rate names the node being poisoned rather than the object being broken.
        self.chunk_aead_failures = self.meter.create_counter(
            name="chunk_aead_failures_total",
            description="Chunk decrypts that failed authentication, by tier (local|remote) and "
            "outcome (recovered|unrecovered)",
            unit="1",
        )

        self.uploader_requests_total = self.meter.create_counter(
            name="uploader_requests_total",
            description="Total uploader requests processed",
            unit="1",
        )

        self.uploader_requests_retried_total = self.meter.create_counter(
            name="uploader_requests_retried_total",
            description="Total uploader requests retried",
            unit="1",
        )

        self.uploader_duration = self.meter.create_histogram(
            name="uploader_duration_seconds",
            description="Duration of uploader processing",
            unit="s",
        )

        self.uploader_chunks_uploaded = self.meter.create_counter(
            name="uploader_chunks_uploaded_total",
            description="Total chunks uploaded to backends",
            unit="1",
        )

        self.uploader_dlq_total = self.meter.create_counter(
            name="uploader_dlq_total",
            description="Total requests moved to Dead Letter Queue",
            unit="1",
        )

        self.unpinner_requests_total = self.meter.create_counter(
            name="unpinner_requests_total",
            description="Total unpinner requests processed",
            unit="1",
        )

        self.unpinner_files_unpinned = self.meter.create_counter(
            name="unpinner_files_unpinned_total",
            description="Total files unpinned from backends",
            unit="1",
        )

        self.downloader_requests_total = self.meter.create_counter(
            name="downloader_requests_total",
            description="Total downloader requests processed",
            unit="1",
        )

        self.downloader_duration = self.meter.create_histogram(
            name="downloader_duration_seconds",
            description="Duration of downloader processing",
            unit="s",
        )

        self.downloader_chunks_fetched = self.meter.create_counter(
            name="downloader_chunks_fetched_total",
            description="Total chunks fetched from backends",
            unit="1",
        )

        self.unpinner_duration = self.meter.create_histogram(
            name="unpinner_duration_seconds",
            description="Duration of unpinner processing",
            unit="s",
        )

        self.unpinner_requests_retried_total = self.meter.create_counter(
            name="unpinner_requests_retried_total",
            description="Total unpinner requests retried",
            unit="1",
        )

        self.unpinner_dlq_total = self.meter.create_counter(
            name="unpinner_dlq_total",
            description="Total unpinner requests moved to Dead Letter Queue",
            unit="1",
        )

        self.backup_cycles_total = self.meter.create_counter(
            name="backup_cycles_total",
            description="Total backup cycles completed",
            unit="1",
        )

        self.backup_database_duration = self.meter.create_histogram(
            name="backup_database_duration_seconds",
            description="Duration to backup each database",
            unit="s",
        )

        self.backup_database_size = self.meter.create_histogram(
            name="backup_database_size_bytes",
            description="Backup file size per database",
            unit="bytes",
        )

        self.backup_upload_duration = self.meter.create_histogram(
            name="backup_upload_duration_seconds",
            description="S3 upload duration per database backup",
            unit="s",
        )

        self.backup_databases_count = self.meter.create_counter(
            name="backup_databases_count",
            description="Count of databases backed up per cycle",
            unit="1",
        )

        self.backup_cleanup_deleted_count = self.meter.create_counter(
            name="backup_cleanup_deleted_count",
            description="Old backups deleted during retention cleanup",
            unit="1",
        )

        self.meter.create_observable_gauge(
            name="redis_memory_used_bytes", callbacks=[self._obs_redis_used_mem], description="Redis used memory bytes"
        )

        self.meter.create_observable_gauge(
            name="redis_memory_max_bytes", callbacks=[self._obs_redis_max_mem], description="Redis max memory bytes"
        )

        self.meter.create_observable_gauge(
            name="hippius_queue_length", callbacks=[self._obs_queue_lengths], description="Length of Redis queues"
        )

        self.meter.create_observable_gauge(
            name="backup_last_success_timestamp",
            callbacks=[self._obs_backup_last_success],
            description="Unix timestamp of last successful backup cycle",
        )

        self.meter.create_observable_gauge(
            name="db_pool_size",
            callbacks=[self._obs_db_pool_size],
            description="Database connection pool current size",
        )
        self.meter.create_observable_gauge(
            name="db_pool_free_connections",
            callbacks=[self._obs_db_pool_free],
            description="Database connection pool free connections",
        )
        self.meter.create_observable_gauge(
            name="db_pool_used_connections",
            callbacks=[self._obs_db_pool_used],
            description="Database connection pool used connections",
        )

        self.gateway_overhead_duration = self.meter.create_histogram(
            name="gateway_overhead_seconds",
            description="Gateway middleware processing time excluding body streaming",
            unit="s",
        )

        self.auth_cache_hits = self.meter.create_counter(
            name="auth_cache_hits_total",
            description="Total auth cache hits",
            unit="1",
        )

        self.auth_cache_misses = self.meter.create_counter(
            name="auth_cache_misses_total",
            description="Total auth cache misses",
            unit="1",
        )

        self.seed_auth_cache_hits = self.meter.create_counter(
            name="seed_auth_cache_hits_total",
            description="Total seed phrase auth cache hits",
            unit="1",
        )

        self.seed_auth_cache_misses = self.meter.create_counter(
            name="seed_auth_cache_misses_total",
            description="Total seed phrase auth cache misses",
            unit="1",
        )

        # Legacy names kept alive as aliases of s3_bytes_uploaded/downloaded. The gateway
        # that used to feed them from the forwarded byte stream is gone (PR #420 merge);
        # external consumers still query these names, so record_data_transfer mirrors
        # the s3_bytes_* increments onto them.
        self.gateway_bytes_received = self.meter.create_counter(
            name="gateway_bytes_received_total",
            description="Alias of s3_bytes_uploaded_total (legacy gateway name)",
            unit="bytes",
        )

        self.gateway_bytes_sent = self.meter.create_counter(
            name="gateway_bytes_sent_total",
            description="Alias of s3_bytes_downloaded_total (legacy gateway name)",
            unit="bytes",
        )

        # --- Background worker loops (previously un-metered) ---

        self.mpu_reaper_cycles_total = self.meter.create_counter(
            name="mpu_reaper_cycles_total", description="Total MPU reaper cycles run", unit="1"
        )
        self.mpu_reaper_versions_reaped_total = self.meter.create_counter(
            name="mpu_reaper_versions_reaped_total", description="Abandoned MPU versions reaped", unit="1"
        )
        # A21 sweep counter, kept distinct from the reaper's: the soak gate watches this to
        # assert pending/draining orphans stay bounded (a rising sweep count with no fresh
        # aborts means a leak the reaper is blind to). See s3-2.1 WI-20a / soak gate.
        self.mpu_reaper_orphans_swept_total = self.meter.create_counter(
            name="mpu_reaper_orphans_swept_total",
            description="Leaked cephor orphan versions marked terminal by the sweep",
            unit="1",
        )
        self.mpu_reaper_duration_seconds = self.meter.create_histogram(
            name="mpu_reaper_duration_seconds", description="MPU reaper cycle duration", unit="s"
        )
        self.mpu_reaper_oldest_reaped_age_seconds = self.meter.create_histogram(
            name="mpu_reaper_oldest_reaped_age_seconds",
            description="Age of the oldest abandoned upload reaped in a cycle (reaper lag)",
            unit="s",
        )

        self.purger_jobs_total = self.meter.create_counter(
            name="purger_jobs_total",
            description="Account purge jobs finished, by result (done|failed)",
            unit="1",
        )
        self.purger_objects_deleted_total = self.meter.create_counter(
            name="purger_objects_deleted_total",
            description="Objects soft-deleted by account purge jobs",
            unit="1",
        )
        self.purger_duration_seconds = self.meter.create_histogram(
            name="purger_duration_seconds", description="Account purge job duration", unit="s"
        )
        self.purger_backpressure_waits_total = self.meter.create_counter(
            name="purger_backpressure_waits_total",
            description="Times a purge job parked because an unpin queue was over its high-water mark",
            unit="1",
        )

        self.orphan_checker_cycles_total = self.meter.create_counter(
            name="orphan_checker_cycles_total", description="Total orphan-checker cycles run", unit="1"
        )
        self.orphan_checker_files_scanned_total = self.meter.create_counter(
            name="orphan_checker_files_scanned_total",
            description="On-chain files scanned by the orphan checker",
            unit="1",
        )
        self.orphan_checker_orphans_found_total = self.meter.create_counter(
            name="orphan_checker_orphans_found_total", description="Orphaned files found + enqueued for unpin", unit="1"
        )
        self.orphan_checker_duration_seconds = self.meter.create_histogram(
            name="orphan_checker_duration_seconds", description="Orphan-checker cycle duration", unit="s"
        )

        self.account_cacher_cycles_total = self.meter.create_counter(
            name="account_cacher_cycles_total", description="Total account-cacher cycles run", unit="1"
        )
        self.account_cacher_accounts_cached_total = self.meter.create_counter(
            name="account_cacher_accounts_cached_total",
            description="Account credit rows cached from Substrate",
            unit="1",
        )
        self.account_cacher_duration_seconds = self.meter.create_histogram(
            name="account_cacher_duration_seconds", description="Account-cacher cycle duration", unit="s"
        )

        self.cachet_health_checks_total = self.meter.create_counter(
            name="cachet_health_checks_total", description="Gateway health checks run by the cachet worker", unit="1"
        )
        self.cachet_updates_total = self.meter.create_counter(
            name="cachet_updates_total", description="Cachet status-page updates pushed", unit="1"
        )

        self.dlq_pushed_total = self.meter.create_counter(
            name="dlq_pushed_total", description="Entries pushed to a dead-letter queue", unit="1"
        )
        self.dlq_requeued_total = self.meter.create_counter(
            name="dlq_requeued_total", description="Entries requeued out of a dead-letter queue", unit="1"
        )
        self.dlq_dropped_total = self.meter.create_counter(
            name="dlq_dropped_total", description="Entries dropped because a dead-letter queue is at its cap", unit="1"
        )

        logger.info("Metrics setup complete")

    def _obs_redis_used_mem(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._used_mem, {})]

    def _obs_redis_max_mem(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._max_mem, {})]

    def _obs_queue_lengths(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(length, {"queue_name": name}) for name, length in self._queue_lengths.items()]

    def set_queue_length(self, queue_name: str, length: int) -> None:
        self._queue_lengths[queue_name] = length

    def _obs_backup_last_success(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._backup_last_success_timestamp, {})]

    def _obs_db_pool_size(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._db_pool_size, {})]

    def _obs_db_pool_free(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._db_pool_free, {})]

    def _obs_db_pool_used(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._db_pool_used, {})]

    def update_db_pool_metrics(self, size: int, free: int) -> None:
        self._db_pool_size = size
        self._db_pool_free = free
        self._db_pool_used = size - free

    def record_http_request(
        self,
        request: Request,
        response: Response,
        duration: float,
        handler: Optional[str] = None,
        ttfb: Optional[float] = None,
    ) -> None:
        # handler falls back to "unknown", never to request.url.path: the path carries
        # the object key, so one caller passing handler=None would mint a series per
        # object and take Prometheus down.
        attributes = {
            "method": request.method,
            "handler": handler or "unknown",
            "status_code": str(response.status_code),
        }

        self.http_requests_total.add(1, attributes=attributes)
        self.http_request_duration.record(duration, attributes=attributes)
        if ttfb is not None:
            self.http_request_ttfb.record(ttfb, attributes=attributes)

        request_content_length = request.headers.get("content-length")
        if request_content_length:
            self.http_request_bytes.add(int(request_content_length), attributes={**attributes, "direction": "in"})

        response_content_length = response.headers.get("content-length")
        if response_content_length:
            self.http_response_bytes.add(int(response_content_length), attributes={**attributes, "direction": "out"})

    def record_pre_handler_duration(self, method: str, status_code: int, duration: float) -> None:
        """Record time spent in the middleware chain outside the audited window.

        Labels are method and status only. Deliberately NOT the handler or path: the path carries
        the object key, and this is recorded for every request including the ones rejected before
        routing — where no handler name exists to fall back to.
        """
        self.http_pre_handler_duration.record(duration, attributes={"method": method, "status_code": str(status_code)})

    def record_s3_operation(
        self,
        operation: str,
        bucket_name: str,
        success: bool = True,
    ) -> None:
        attributes = {"operation": operation, "success": str(success).lower()}

        self.s3_operations_total.add(1, attributes=attributes)

    def record_data_transfer(
        self,
        operation: str,
        bytes_transferred: int,
        bucket_name: str,
    ) -> None:
        # BYTES ONLY. The operation COUNT is owned solely by record_s3_operation — this
        # method must not also bump s3_operations_total, or every data op (put_object,
        # get_object, upload_part, CompleteMPU calls BOTH) double-counts, split across two
        # label shapes ({operation} here vs {operation, success} there). That inflated
        # s3_operations_total 2x. Any op that should be counted calls record_s3_operation.
        attributes = {
            "operation": operation,
        }

        if operation in ["upload", "put_object", "post_object", "upload_part"]:
            self.s3_bytes_uploaded.add(bytes_transferred, attributes=attributes)
            self.gateway_bytes_received.add(bytes_transferred, attributes=attributes)
        elif operation in ["download", "get_object"]:
            self.s3_bytes_downloaded.add(bytes_transferred, attributes=attributes)
            self.gateway_bytes_sent.add(bytes_transferred, attributes=attributes)

    def record_error(
        self,
        error_type: str,
        operation: str,
        bucket_name: Optional[str] = None,
    ) -> None:
        """Record error metrics"""
        attributes = {
            "error_type": error_type,
            "operation": operation,
        }

        self.s3_errors_total.add(1, attributes=attributes)

    def record_fs_cache_shed(self, reason: str, pressure_mode: str) -> None:
        """Record a write rejected by the FS-cache-pressure gate.

        The gate short-circuits as the OUTERMOST middleware (it must answer before the body is
        read), so metrics_middleware — the innermost — never runs on a shed request and the 503
        appears in no request counter. Recording here is what makes a pressure event visible;
        without it the only evidence is a log line.
        """
        self.fs_cache_shed_total.add(1, attributes={"reason": reason, "pressure_mode": pressure_mode})

    def record_cache_operation(
        self,
        hit: bool,
        operation: str,
    ) -> None:
        attributes = {"operation": operation}

        if hit:
            self.cache_hits.add(1, attributes=attributes)
        else:
            self.cache_misses.add(1, attributes=attributes)

    def record_peer_fetch_shed(self, reason: PeerShedReason) -> None:
        """Count a declined peer fetch. `reason` is a Literal, so the label stays bounded."""
        self.peer_fetch_shed.add(1, attributes={"reason": reason})

    def record_promotion_skipped(self, reason: PromotionSkipReason) -> None:
        """Count a chunk served without being promoted. `reason` is a Literal, so bounded."""
        self.promotion_skipped.add(1, attributes={"reason": reason})

    def record_promote_floor_divergence(self, direction: PromoteFloorDivergence) -> None:
        """Count a gate decision on a published floor unequal to the configured one."""
        self.promote_floor_divergence.add(1, attributes={"direction": direction})

    def record_residency_release_failure(self) -> None:
        """Count a claim whose compensating decrement failed, leaving phantom bytes accounted."""
        self.residency_release_failures.add(1)

    def record_residency_drop_failure(self) -> None:
        """Count an aborted version whose residency rows outlived its directory on this node."""
        self.residency_drop_failures.add(1)

    def record_landed_announce_failure(self, outcome: LandedAnnounceOutcome) -> None:
        """Count one announcement that did not reach the queue. `outcome` is a Literal."""
        self.landed_announce_failures.add(1, attributes={"outcome": outcome})

    def record_read_recency_write(self, outcome: ReadRecencyOutcome) -> None:
        """Count one `last_read_at` stamp attempt. `outcome` is a Literal, so bounded."""
        self.read_recency_writes.add(1, attributes={"outcome": outcome})

    def record_aead_failure(self, tier: AeadFailureTier, outcome: AeadFailureOutcome) -> None:
        """Count one chunk that failed to authenticate. Both labels are Literals, so bounded."""
        self.chunk_aead_failures.add(1, attributes={"tier": tier, "outcome": outcome})

    def record_chunk_read_tier(self, tier: ChunkReadTier) -> None:
        """Count one chunk read against the tier that served it.

        The `Literal` is what keeps this label bounded: three values fixed in code, so it
        cannot become a cardinality problem the way a caller-supplied string would.
        """
        self.chunk_reads_by_tier.add(1, attributes={"tier": tier})

    def record_uploader_operation(
        self,
        success: bool,
        backend: str = "",
        num_chunks: int = 0,
        duration: Optional[float] = None,
        attempt: Optional[int] = None,
        error_type: Optional[str] = None,
        status_code: str = "",
    ) -> None:
        attributes = {
            "success": str(success).lower(),
        }
        if backend:
            attributes["backend"] = backend
        if status_code:
            attributes["status_code"] = status_code

        self.uploader_requests_total.add(1, attributes=attributes)

        if attempt is not None:
            retry_attributes = {
                "attempt": str(attempt),
            }
            if backend:
                retry_attributes["backend"] = backend
            self.uploader_requests_retried_total.add(1, attributes=retry_attributes)
        elif error_type is not None:
            dlq_attributes = {
                "error_type": error_type,
            }
            if backend:
                dlq_attributes["backend"] = backend
            self.uploader_dlq_total.add(1, attributes=dlq_attributes)

        if num_chunks > 0:
            self.uploader_chunks_uploaded.add(num_chunks, attributes=attributes)

        if duration is not None:
            self.uploader_duration.record(duration, attributes=attributes)

    def record_unpinner_operation(
        self,
        success: bool,
        backend: str = "",
        num_files: int = 0,
        duration: Optional[float] = None,
        attempt: Optional[int] = None,
        error_type: Optional[str] = None,
    ) -> None:
        attributes = {
            "success": str(success).lower(),
        }
        if backend:
            attributes["backend"] = backend

        if attempt is not None:
            retry_attributes = {
                "attempt": str(attempt),
            }
            if backend:
                retry_attributes["backend"] = backend
            self.unpinner_requests_retried_total.add(1, attributes=retry_attributes)
        elif error_type is not None:
            dlq_attributes = {
                "error_type": error_type,
            }
            if backend:
                dlq_attributes["backend"] = backend
            self.unpinner_dlq_total.add(1, attributes=dlq_attributes)
        else:
            self.unpinner_requests_total.add(1, attributes=attributes)

            if num_files > 0:
                self.unpinner_files_unpinned.add(num_files, attributes=attributes)

            if duration is not None:
                self.unpinner_duration.record(duration, attributes=attributes)

    def record_downloader_operation(
        self,
        backend: str,
        success: bool,
        duration: Optional[float] = None,
        num_chunks: int = 0,
    ) -> None:
        attributes = {
            "backend": backend,
            "success": str(success).lower(),
        }

        self.downloader_requests_total.add(1, attributes=attributes)

        if num_chunks > 0:
            self.downloader_chunks_fetched.add(num_chunks, attributes=attributes)

        if duration is not None:
            self.downloader_duration.record(duration, attributes=attributes)

    def record_gateway_overhead(
        self,
        duration: float,
        method: str,
        status_code: int,
        handler: Optional[str] = None,
    ) -> None:
        attributes: dict[str, str] = {
            "method": method,
            "status_code": str(status_code),
        }
        if handler:
            attributes["handler"] = handler

        self.gateway_overhead_duration.record(duration, attributes=attributes)

    def record_auth_cache(self, hit: bool) -> None:
        if hit:
            self.auth_cache_hits.add(1)
        else:
            self.auth_cache_misses.add(1)

    def record_seed_auth_cache(self, hit: bool) -> None:
        if hit:
            self.seed_auth_cache_hits.add(1)
        else:
            self.seed_auth_cache_misses.add(1)

    def record_backup_operation(
        self,
        database_name: str,
        success: bool,
        backup_duration: Optional[float] = None,
        backup_size_bytes: Optional[int] = None,
        upload_duration: Optional[float] = None,
    ) -> None:
        attributes = {
            "database": database_name,
            "success": str(success).lower(),
        }

        if backup_duration is not None:
            self.backup_database_duration.record(backup_duration, attributes=attributes)

        if backup_size_bytes is not None:
            self.backup_database_size.record(backup_size_bytes, attributes=attributes)

        if upload_duration is not None:
            self.backup_upload_duration.record(upload_duration, attributes=attributes)

        if success:
            self.backup_databases_count.add(1, attributes=attributes)

    def record_backup_cycle(self, success: bool, num_databases: int = 0) -> None:
        attributes = {"success": str(success).lower()}
        self.backup_cycles_total.add(1, attributes=attributes)

        if success:
            import time

            self._backup_last_success_timestamp = time.time()

    def record_backup_cleanup(self, database_name: str, deleted_count: int) -> None:
        attributes = {"database": database_name}
        self.backup_cleanup_deleted_count.add(deleted_count, attributes=attributes)

    def record_mpu_reaper_cycle(
        self,
        success: bool,
        reaped: int,
        duration: float,
        oldest_reaped_age: Optional[float] = None,
        swept: int = 0,
    ) -> None:
        self.mpu_reaper_cycles_total.add(1, attributes={"success": str(success).lower()})
        self.mpu_reaper_duration_seconds.record(duration)
        if reaped > 0:
            self.mpu_reaper_versions_reaped_total.add(reaped)
        if swept > 0:
            self.mpu_reaper_orphans_swept_total.add(swept)
        if oldest_reaped_age is not None:
            self.mpu_reaper_oldest_reaped_age_seconds.record(oldest_reaped_age)

    def record_purger_job(self, success: bool, deleted_objects: int, duration: float) -> None:
        self.purger_jobs_total.add(1, attributes={"result": "done" if success else "failed"})
        self.purger_duration_seconds.record(duration)
        if deleted_objects > 0:
            self.purger_objects_deleted_total.add(deleted_objects)

    def record_purger_backpressure_wait(self) -> None:
        self.purger_backpressure_waits_total.add(1)

    def record_orphan_checker_cycle(
        self,
        success: bool,
        files_scanned: int,
        orphans_found: int,
        duration: float,
    ) -> None:
        self.orphan_checker_cycles_total.add(1, attributes={"success": str(success).lower()})
        self.orphan_checker_duration_seconds.record(duration)
        if files_scanned > 0:
            self.orphan_checker_files_scanned_total.add(files_scanned)
        if orphans_found > 0:
            self.orphan_checker_orphans_found_total.add(orphans_found)

    def record_account_cacher_cycle(
        self,
        success: bool,
        accounts_cached: int,
        duration: float,
    ) -> None:
        self.account_cacher_cycles_total.add(1, attributes={"success": str(success).lower()})
        self.account_cacher_duration_seconds.record(duration)
        if accounts_cached > 0:
            self.account_cacher_accounts_cached_total.add(accounts_cached)

    def record_cachet_check(self, status: str, update_success: bool) -> None:
        self.cachet_health_checks_total.add(1, attributes={"status": status})
        self.cachet_updates_total.add(1, attributes={"success": str(update_success).lower()})

    def record_dlq_push(self, queue: str, error_type: str) -> None:
        self.dlq_pushed_total.add(1, attributes={"queue": queue, "error_type": error_type})

    def record_dlq_requeue(self, queue: str, count: int = 1) -> None:
        if count > 0:
            self.dlq_requeued_total.add(count, attributes={"queue": queue})

    def record_dlq_dropped(self, queue: str, error_type: str) -> None:
        self.dlq_dropped_total.add(1, attributes={"queue": queue, "error_type": error_type})


class NullMetricsCollector:
    def __init__(self) -> None:
        self.http_requests_total = None
        self.http_request_duration = None

    def record_http_request(self, *args: object, **kwargs: object) -> None:
        pass

    def record_pre_handler_duration(self, *args: object, **kwargs: object) -> None:
        pass

    def record_s3_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_data_transfer(self, *args: object, **kwargs: object) -> None:
        pass

    def record_error(self, *args: object, **kwargs: object) -> None:
        pass

    def record_fs_cache_shed(self, *args: object, **kwargs: object) -> None:
        pass

    def record_chunk_read_tier(self, *args: object, **kwargs: object) -> None:
        pass

    def record_aead_failure(self, *args: object, **kwargs: object) -> None:
        pass

    def record_peer_fetch_shed(self, *args: object, **kwargs: object) -> None:
        pass

    def record_promotion_skipped(self, *args: object, **kwargs: object) -> None:
        pass

    def record_promote_floor_divergence(self, *args: object, **kwargs: object) -> None:
        pass

    def record_residency_release_failure(self, *args: object, **kwargs: object) -> None:
        pass

    def record_residency_drop_failure(self, *args: object, **kwargs: object) -> None:
        pass

    def record_landed_announce_failure(self, *args: object, **kwargs: object) -> None:
        pass

    def record_read_recency_write(self, *args: object, **kwargs: object) -> None:
        pass

    def record_cache_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_uploader_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_unpinner_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_downloader_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_gateway_overhead(self, *args: object, **kwargs: object) -> None:
        pass

    def record_auth_cache(self, *args: object, **kwargs: object) -> None:
        pass

    def record_seed_auth_cache(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cycle(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cleanup(self, *args: object, **kwargs: object) -> None:
        pass

    def record_mpu_reaper_cycle(self, *args: object, **kwargs: object) -> None:
        pass

    def record_purger_job(self, *args: object, **kwargs: object) -> None:
        pass

    def record_purger_backpressure_wait(self, *args: object, **kwargs: object) -> None:
        pass

    def record_orphan_checker_cycle(self, *args: object, **kwargs: object) -> None:
        pass

    def record_account_cacher_cycle(self, *args: object, **kwargs: object) -> None:
        pass

    def record_cachet_check(self, *args: object, **kwargs: object) -> None:
        pass

    def record_dlq_push(self, *args: object, **kwargs: object) -> None:
        pass

    def record_dlq_requeue(self, *args: object, **kwargs: object) -> None:
        pass

    def record_dlq_dropped(self, *args: object, **kwargs: object) -> None:
        pass


_metrics_collector: MetricsCollector | NullMetricsCollector = NullMetricsCollector()


def get_metrics_collector() -> MetricsCollector | NullMetricsCollector:
    return _metrics_collector


def set_metrics_collector(collector: MetricsCollector | NullMetricsCollector) -> None:
    global _metrics_collector
    _metrics_collector = collector


def initialize_metrics_collector(
    redis_client: Union[Redis, RedisCluster, None] = None,
) -> MetricsCollector | NullMetricsCollector:
    if os.getenv("ENABLE_MONITORING", "false").lower() not in ("true", "1", "yes"):
        logger.info("Monitoring disabled, using NullMetricsCollector")
        null_collector = NullMetricsCollector()
        set_metrics_collector(null_collector)
        return null_collector

    # Only create a MeterProvider if one hasn't been set already
    # (configure_otel in otel_setup.py may have already initialized it)
    current_provider = metrics.get_meter_provider()
    if isinstance(current_provider, MeterProvider):
        logger.info("MeterProvider already configured (by otel_setup), skipping duplicate init")
    else:
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
        service_name = os.getenv("OTEL_SERVICE_NAME", "hippius-s3")

        # Same per-process identity as configure_otel. No deployed config reaches this
        # branch — api/gateway call configure_otel, and workers run under
        # opentelemetry-instrument, which sets a MeterProvider before the worker body
        # runs, so the isinstance guard above short-circuits. It survives only for a
        # bare `python workers/...` with monitoring on. Build the resource the one right
        # way regardless: an earlier version of this line hardcoded the hostname on the
        # reasoning that only single-process workers land here, which is exactly the kind
        # of confident assumption about who-calls-what that produced the 105x in the
        # first place. If init order ever changes and api/gateway fall through to here,
        # this must not silently reintroduce it.
        resource = build_resource(service_name)

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=endpoint, insecure=True),
            export_interval_millis=10000,
        )

        provider = MeterProvider(resource=resource, metric_readers=[metric_reader], views=build_metric_views())
        metrics.set_meter_provider(provider)
        logger.info(f"Monitoring enabled, exporting to {endpoint}")

    collector = MetricsCollector(redis_client)
    set_metrics_collector(collector)
    return collector


def enrich_span_with_account_info(
    main_account: Optional[str] = None,
    subaccount_id: Optional[str] = None,
    bucket_name: Optional[str] = None,
    object_key: Optional[str] = None,
) -> None:
    span = trace.get_current_span()
    if span.is_recording():
        if main_account:
            span.set_attribute("hippius.account.main", main_account)
        if subaccount_id:
            span.set_attribute("hippius.account.sub", subaccount_id)
        if bucket_name:
            span.set_attribute("aws.s3.bucket", bucket_name)
        if object_key:
            span.set_attribute("aws.s3.key", object_key)
