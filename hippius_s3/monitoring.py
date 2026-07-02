import logging
import os
import socket
from typing import Optional
from typing import Union

from fastapi import Request
from fastapi import Response
from opentelemetry import metrics
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class MetricsCollector:
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

        self.cache_hits = self.meter.create_counter(name="cache_hits_total", description="Total cache hits", unit="1")

        self.cache_misses = self.meter.create_counter(
            name="cache_misses_total", description="Total cache misses", unit="1"
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

        self.gateway_bytes_received = self.meter.create_counter(
            name="gateway_bytes_received_total",
            description="Total bytes received from clients through the gateway",
            unit="bytes",
        )

        self.gateway_bytes_sent = self.meter.create_counter(
            name="gateway_bytes_sent_total",
            description="Total bytes sent to clients through the gateway",
            unit="bytes",
        )

        # --- Background worker loops (previously un-metered) ---

        self.mpu_reaper_cycles_total = self.meter.create_counter(
            name="mpu_reaper_cycles_total", description="Total MPU reaper cycles run", unit="1"
        )
        self.mpu_reaper_versions_reaped_total = self.meter.create_counter(
            name="mpu_reaper_versions_reaped_total", description="Abandoned MPU versions reaped", unit="1"
        )
        self.mpu_reaper_duration_seconds = self.meter.create_histogram(
            name="mpu_reaper_duration_seconds", description="MPU reaper cycle duration", unit="s"
        )
        self.mpu_reaper_oldest_reaped_age_seconds = self.meter.create_histogram(
            name="mpu_reaper_oldest_reaped_age_seconds",
            description="Age of the oldest abandoned upload reaped in a cycle (reaper lag)",
            unit="s",
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
        main_account: Optional[str] = None,
        subaccount_id: Optional[str] = None,
        handler: Optional[str] = None,
    ) -> None:
        attributes = {
            "method": request.method,
            "handler": handler or request.url.path,
            "status_code": str(response.status_code),
        }

        if main_account:
            attributes["main_account"] = main_account

        if subaccount_id:
            attributes["subaccount_id"] = subaccount_id

        self.http_requests_total.add(1, attributes=attributes)
        self.http_request_duration.record(duration, attributes=attributes)

        request_content_length = request.headers.get("content-length")
        if request_content_length:
            self.http_request_bytes.add(int(request_content_length), attributes={**attributes, "direction": "in"})

        response_content_length = response.headers.get("content-length")
        if response_content_length:
            self.http_response_bytes.add(int(response_content_length), attributes={**attributes, "direction": "out"})

    def record_s3_operation(
        self,
        operation: str,
        bucket_name: str,
        main_account: Optional[str] = None,
        subaccount_id: Optional[str] = None,
        success: bool = True,
    ) -> None:
        attributes = {"operation": operation, "success": str(success).lower()}

        if main_account:
            attributes["main_account"] = main_account

        if subaccount_id:
            attributes["subaccount_id"] = subaccount_id

        self.s3_operations_total.add(1, attributes=attributes)

    def record_data_transfer(
        self,
        operation: str,
        bytes_transferred: int,
        bucket_name: str,
        main_account: Optional[str] = None,
        subaccount_id: Optional[str] = None,
    ) -> None:
        attributes = {
            "operation": operation,
        }

        if main_account:
            attributes["main_account"] = main_account

        if subaccount_id:
            attributes["subaccount_id"] = subaccount_id

        if operation in ["upload", "put_object", "post_object", "upload_part"]:
            self.s3_bytes_uploaded.add(bytes_transferred, attributes=attributes)
        elif operation in ["download", "get_object"]:
            self.s3_bytes_downloaded.add(bytes_transferred, attributes=attributes)

        self.s3_operations_total.add(1, attributes=attributes)

    def record_error(
        self,
        error_type: str,
        operation: str,
        bucket_name: Optional[str] = None,
        main_account: Optional[str] = None,
    ) -> None:
        """Record error metrics"""
        attributes = {
            "error_type": error_type,
            "operation": operation,
        }

        if main_account:
            attributes["main_account"] = main_account

        self.s3_errors_total.add(1, attributes=attributes)

    def record_cache_operation(
        self,
        hit: bool,
        operation: str,
        main_account: Optional[str] = None,
    ) -> None:
        attributes = {"operation": operation}

        if main_account:
            attributes["main_account"] = main_account

        if hit:
            self.cache_hits.add(1, attributes=attributes)
        else:
            self.cache_misses.add(1, attributes=attributes)

    def record_uploader_operation(
        self,
        main_account: str,
        success: bool,
        backend: str = "",
        num_chunks: int = 0,
        duration: Optional[float] = None,
        attempt: Optional[int] = None,
        error_type: Optional[str] = None,
        status_code: str = "",
    ) -> None:
        attributes = {
            "main_account": main_account,
            "success": str(success).lower(),
        }
        if backend:
            attributes["backend"] = backend
        if status_code:
            attributes["status_code"] = status_code

        self.uploader_requests_total.add(1, attributes=attributes)

        if attempt is not None:
            retry_attributes = {
                "main_account": main_account,
                "attempt": str(attempt),
            }
            if backend:
                retry_attributes["backend"] = backend
            self.uploader_requests_retried_total.add(1, attributes=retry_attributes)
        elif error_type is not None:
            dlq_attributes = {
                "main_account": main_account,
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
        main_account: str,
        success: bool,
        backend: str = "",
        num_files: int = 0,
        duration: Optional[float] = None,
        attempt: Optional[int] = None,
        error_type: Optional[str] = None,
    ) -> None:
        attributes = {
            "main_account": main_account,
            "success": str(success).lower(),
        }
        if backend:
            attributes["backend"] = backend

        if attempt is not None:
            retry_attributes = {
                "main_account": main_account,
                "attempt": str(attempt),
            }
            if backend:
                retry_attributes["backend"] = backend
            self.unpinner_requests_retried_total.add(1, attributes=retry_attributes)
        elif error_type is not None:
            dlq_attributes = {
                "main_account": main_account,
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
        main_account: str,
        success: bool,
        duration: Optional[float] = None,
        num_chunks: int = 0,
    ) -> None:
        attributes = {
            "backend": backend,
            "main_account": main_account,
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
        main_account: Optional[str] = None,
    ) -> None:
        attributes: dict[str, str] = {
            "method": method,
            "status_code": str(status_code),
        }
        if handler:
            attributes["handler"] = handler
        if main_account:
            attributes["main_account"] = main_account

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

    def record_gateway_bandwidth(
        self,
        bytes_received: int,
        bytes_sent: int,
        method: str,
        status_code: int,
    ) -> None:
        if bytes_received > 0:
            self.gateway_bytes_received.add(bytes_received, {"method": method})
        if bytes_sent > 0:
            self.gateway_bytes_sent.add(bytes_sent, {"method": method, "status_code": str(status_code)})

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
    ) -> None:
        self.mpu_reaper_cycles_total.add(1, attributes={"success": str(success).lower()})
        self.mpu_reaper_duration_seconds.record(duration)
        if reaped > 0:
            self.mpu_reaper_versions_reaped_total.add(reaped)
        if oldest_reaped_age is not None:
            self.mpu_reaper_oldest_reaped_age_seconds.record(oldest_reaped_age)

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


class NullMetricsCollector:
    def __init__(self) -> None:
        self.http_requests_total = None
        self.http_request_duration = None

    def record_http_request(self, *args: object, **kwargs: object) -> None:
        pass

    def record_s3_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_data_transfer(self, *args: object, **kwargs: object) -> None:
        pass

    def record_error(self, *args: object, **kwargs: object) -> None:
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

    def record_gateway_bandwidth(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cycle(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cleanup(self, *args: object, **kwargs: object) -> None:
        pass

    def record_mpu_reaper_cycle(self, *args: object, **kwargs: object) -> None:
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

        resource = Resource.create({"service.name": service_name, "service.instance.id": socket.gethostname()})

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=endpoint, insecure=True),
            export_interval_millis=10000,
        )

        provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
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
