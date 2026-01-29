import logging
import os
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
    def __init__(self, redis_client: Union[Redis, RedisCluster]):
        self.redis_client = redis_client
        self.meter = metrics.get_meter(__name__)
        self._upload_len = 0
        self._unpin_len = 0
        self._substrate_len = 0
        self._download_len = 0
        self._main_db_size = 0
        self._accounts_db_size = 0
        self._chain_db_size = 0
        self._substrate_db_size = 0
        self._rate_limiting_db_size = 0
        self._used_mem = 0
        self._max_mem = 0
        self._pin_checker_missing_cids: dict[str, int] = {}
        self._object_status_counts: dict[str, int] = {}
        self._backup_last_success_timestamp = 0.0
        # FS store metrics
        self._fs_store_oldest_age_seconds = 0.0
        self._fs_store_parts_on_disk = 0
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

        self.s3_objects_total = self.meter.create_counter(
            name="s3_objects_total", description="Total number of S3 objects", unit="1"
        )

        self.s3_buckets_total = self.meter.create_counter(
            name="s3_buckets_total", description="Total number of S3 buckets", unit="1"
        )

        self.multipart_uploads_active = self.meter.create_up_down_counter(
            name="multipart_uploads_active", description="Number of active multipart uploads", unit="1"
        )

        self.ipfs_operations_total = self.meter.create_counter(
            name="ipfs_operations_total", description="Total IPFS operations", unit="1"
        )

        self.ipfs_operation_duration = self.meter.create_histogram(
            name="ipfs_operation_duration_seconds", description="IPFS operation duration in seconds", unit="s"
        )

        self.cache_downloads_total = self.meter.create_counter(
            name="cache_downloads_total", description="Total downloads served from Redis cache", unit="1"
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

        self.multipart_parts_uploaded = self.meter.create_counter(
            name="multipart_parts_uploaded_total", description="Total multipart parts uploaded", unit="1"
        )

        self.multipart_uploads_completed = self.meter.create_counter(
            name="multipart_uploads_completed_total", description="Total completed multipart uploads", unit="1"
        )

        self.s3_errors_total = self.meter.create_counter(
            name="s3_errors_total", description="Total S3 errors by type", unit="1"
        )

        self.cache_hits = self.meter.create_counter(name="cache_hits_total", description="Total cache hits", unit="1")

        self.cache_misses = self.meter.create_counter(
            name="cache_misses_total", description="Total cache misses", unit="1"
        )

        self.object_size_bytes = self.meter.create_histogram(
            name="object_size_bytes", description="Distribution of object sizes", unit="bytes"
        )

        self.substrate_requests_total = self.meter.create_counter(
            name="substrate_requests_total",
            description="Total substrate blockchain requests processed",
            unit="1",
        )

        self.substrate_requests_retried_total = self.meter.create_counter(
            name="substrate_requests_retried_total",
            description="Total substrate requests retried",
            unit="1",
        )

        self.substrate_batch_duration = self.meter.create_histogram(
            name="substrate_batch_duration_seconds",
            description="Duration of substrate batch processing",
            unit="s",
        )

        self.substrate_cids_submitted = self.meter.create_counter(
            name="substrate_cids_submitted_total",
            description="Total CIDs submitted to substrate blockchain",
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
            description="Total chunks uploaded to IPFS",
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
            description="Total files unpinned from IPFS",
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
            name="hippius_redis_db_size",
            callbacks=[self._obs_db_sizes],
            description="Total keys in Redis databases (DBSIZE)",
        )

        self.meter.create_observable_gauge(
            name="pin_checker_missing_cids",
            callbacks=[self._obs_pin_checker_missing_cids],
            description="Number of missing CIDs per user (S3 DB vs Chain)",
        )

        self.meter.create_observable_gauge(
            name="object_status_counts",
            callbacks=[self._obs_object_status_counts],
            description="Total count of objects by status across all users",
        )

        # FS store gauges
        self.meter.create_observable_gauge(
            name="fs_store_oldest_age_seconds",
            callbacks=[self._obs_fs_store_oldest_age],
            description="Age in seconds of the oldest part directory in the FS store",
        )
        self.meter.create_observable_gauge(
            name="fs_store_parts_on_disk",
            callbacks=[self._obs_fs_store_parts_on_disk],
            description="Approximate count of part directories on disk in the FS store",
        )

        # FS janitor deletions counter
        self.fs_janitor_deleted_total = self.meter.create_counter(
            name="fs_janitor_deleted_total",
            description="Total number of FS parts deleted by the janitor",
            unit="1",
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

        self.performance_index = self.meter.create_histogram(
            name="hippius_performance_index",
            description="Normalized performance index score (0-100)",
            unit="1",
        )

        self.throughput_mbps = self.meter.create_histogram(
            name="hippius_throughput_mbps",
            description="Data transfer throughput in MB/s",
            unit="MB/s",
        )

        self.overhead_ms = self.meter.create_histogram(
            name="hippius_overhead_ms",
            description="Fixed overhead latency in milliseconds",
            unit="ms",
        )

        logger.info(
            "Performance metrics histograms created: hippius_performance_index, hippius_throughput_mbps, hippius_overhead_ms"
        )

    def _obs_redis_used_mem(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._used_mem, {})]

    def _obs_redis_max_mem(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._max_mem, {})]

    def _obs_queue_lengths(self, _: object) -> list[metrics.Observation]:
        return [
            metrics.Observation(self._upload_len, {"queue_name": "upload_requests"}),
            metrics.Observation(self._unpin_len, {"queue_name": "unpin_requests"}),
            metrics.Observation(self._substrate_len, {"queue_name": "substrate_requests"}),
            metrics.Observation(self._download_len, {"queue_name": "download_requests"}),
        ]

    def _obs_db_sizes(self, _: object) -> list[metrics.Observation]:
        return [
            metrics.Observation(self._main_db_size, {"redis_instance": "main"}),
            metrics.Observation(self._accounts_db_size, {"redis_instance": "accounts"}),
            metrics.Observation(self._chain_db_size, {"redis_instance": "chain"}),
            metrics.Observation(self._substrate_db_size, {"redis_instance": "substrate"}),
            metrics.Observation(self._rate_limiting_db_size, {"redis_instance": "rate_limiting"}),
        ]

    def _obs_pin_checker_missing_cids(self, _: object) -> list[metrics.Observation]:
        return [
            metrics.Observation(count, {"main_account": user}) for user, count in self._pin_checker_missing_cids.items()
        ]

    def _obs_object_status_counts(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(count, {"status": status}) for status, count in self._object_status_counts.items()]

    def _obs_backup_last_success(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._backup_last_success_timestamp, {})]

    def _obs_fs_store_oldest_age(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(float(self._fs_store_oldest_age_seconds), {})]

    def _obs_fs_store_parts_on_disk(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(int(self._fs_store_parts_on_disk), {})]

    def _obs_db_pool_size(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._db_pool_size, {})]

    def _obs_db_pool_free(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._db_pool_free, {})]

    def _obs_db_pool_used(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._db_pool_used, {})]

    # Public setters for FS metrics
    def set_fs_store_oldest_age_seconds(self, age_seconds: float) -> None:
        self._fs_store_oldest_age_seconds = float(max(0.0, age_seconds))

    def set_fs_store_parts_on_disk(self, count: int) -> None:
        self._fs_store_parts_on_disk = int(max(0, count))

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

        if operation in ["put_object", "post_object"]:
            self.s3_objects_total.add(1, attributes=attributes)
        elif operation in ["put_bucket"]:
            self.s3_buckets_total.add(1, attributes=attributes)

    def record_ipfs_operation(
        self,
        operation: str,
        duration: float,
        cid: Optional[str] = None,
        success: bool = True,
        main_account: Optional[str] = None,
    ) -> None:
        attributes = {"operation": operation, "success": str(success).lower()}

        if main_account:
            attributes["main_account"] = main_account

        self.ipfs_operations_total.add(1, attributes=attributes)
        self.ipfs_operation_duration.record(duration, attributes=attributes)

    def record_cache_download(
        self,
        object_type: str,
        range_request: bool = False,
        main_account: Optional[str] = None,
    ) -> None:
        attributes = {
            "object_type": object_type,
            "range_request": str(range_request).lower(),
        }

        if main_account:
            attributes["main_account"] = main_account

        self.cache_downloads_total.add(1, attributes=attributes)

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

        if bytes_transferred > 0:
            self.object_size_bytes.record(bytes_transferred, attributes=attributes)

    def record_multipart_operation(
        self,
        operation: str,
        main_account: Optional[str] = None,
    ) -> None:
        attributes = {"operation": operation}

        if main_account:
            attributes["main_account"] = main_account

        if operation == "upload_part":
            self.multipart_parts_uploaded.add(1, attributes=attributes)
        elif operation == "complete_upload":
            self.multipart_uploads_completed.add(1, attributes=attributes)
            self.multipart_uploads_active.add(-1, attributes=attributes)
        elif operation == "initiate_upload":
            self.multipart_uploads_active.add(1, attributes=attributes)
        elif operation == "abort_upload":
            self.multipart_uploads_active.add(-1, attributes=attributes)

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

    def record_substrate_operation(
        self,
        main_account: str,
        success: bool,
        num_cids: int = 0,
        duration: Optional[float] = None,
        attempt: Optional[int] = None,
    ) -> None:
        attributes = {
            "main_account": main_account,
            "success": str(success).lower(),
        }

        if attempt is not None:
            retry_attributes = {
                "main_account": main_account,
                "attempt": str(attempt),
            }
            self.substrate_requests_retried_total.add(1, attributes=retry_attributes)
        else:
            self.substrate_requests_total.add(1, attributes=attributes)

            if num_cids > 0:
                self.substrate_cids_submitted.add(num_cids, attributes=attributes)

            if duration is not None:
                self.substrate_batch_duration.record(duration, attributes=attributes)

    def record_uploader_operation(
        self,
        main_account: str,
        success: bool,
        num_chunks: int = 0,
        duration: Optional[float] = None,
        attempt: Optional[int] = None,
        error_type: Optional[str] = None,
    ) -> None:
        attributes = {
            "main_account": main_account,
            "success": str(success).lower(),
        }

        if attempt is not None:
            retry_attributes = {
                "main_account": main_account,
                "attempt": str(attempt),
            }
            self.uploader_requests_retried_total.add(1, attributes=retry_attributes)
        elif error_type is not None:
            dlq_attributes = {
                "main_account": main_account,
                "error_type": error_type,
            }
            self.uploader_dlq_total.add(1, attributes=dlq_attributes)
        else:
            self.uploader_requests_total.add(1, attributes=attributes)

            if num_chunks > 0:
                self.uploader_chunks_uploaded.add(num_chunks, attributes=attributes)

            if duration is not None:
                self.uploader_duration.record(duration, attributes=attributes)

    def record_unpinner_operation(
        self,
        main_account: str,
        success: bool,
        num_files: int = 0,
        duration: Optional[float] = None,
        attempt: Optional[int] = None,
        error_type: Optional[str] = None,
    ) -> None:
        attributes = {
            "main_account": main_account,
            "success": str(success).lower(),
        }

        if attempt is not None:
            retry_attributes = {
                "main_account": main_account,
                "attempt": str(attempt),
            }
            self.unpinner_requests_retried_total.add(1, attributes=retry_attributes)
        elif error_type is not None:
            dlq_attributes = {
                "main_account": main_account,
                "error_type": error_type,
            }
            self.unpinner_dlq_total.add(1, attributes=dlq_attributes)
        else:
            self.unpinner_requests_total.add(1, attributes=attributes)

            if num_files > 0:
                self.unpinner_files_unpinned.add(num_files, attributes=attributes)

            if duration is not None:
                self.unpinner_duration.record(duration, attributes=attributes)

    def set_pin_checker_missing_cids(self, user: str, count: int) -> None:
        self._pin_checker_missing_cids[user] = count

    def set_object_status_counts(self, status_counts: dict[str, int]) -> None:
        self._object_status_counts = status_counts

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

    def record_orphan_checker_operation(
        self, orphans_found: int = 0, files_checked: int = 0, success: bool = True
    ) -> None:
        pass

    def record_performance_metrics(
        self,
        operation: str,
        bucket: str,
        performance_index: float,
        throughput_mbps: float,
        overhead_ms: float,
    ) -> None:
        attributes = {"operation": operation, "bucket": bucket}
        self.performance_index.record(performance_index, attributes=attributes)
        self.throughput_mbps.record(throughput_mbps, attributes=attributes)
        self.overhead_ms.record(overhead_ms, attributes=attributes)
        logger.info(
            f"PERF_METRICS: operation={operation}, bucket={bucket}, "
            f"index={performance_index:.2f}, throughput={throughput_mbps:.2f}MB/s, overhead={overhead_ms:.2f}ms"
        )


class NullMetricsCollector:
    def __init__(self) -> None:
        self.http_requests_total = None
        self.http_request_duration = None

    def record_http_request(self, *args: object, **kwargs: object) -> None:
        pass

    def record_s3_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_ipfs_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_cache_download(self, *args: object, **kwargs: object) -> None:
        pass

    def record_data_transfer(self, *args: object, **kwargs: object) -> None:
        pass

    def record_multipart_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_error(self, *args: object, **kwargs: object) -> None:
        pass

    def record_cache_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_substrate_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_uploader_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_unpinner_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def set_pin_checker_missing_cids(self, *args: object, **kwargs: object) -> None:
        pass

    def set_object_status_counts(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cycle(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cleanup(self, *args: object, **kwargs: object) -> None:
        pass

    def record_orphan_checker_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_performance_metrics(self, *args: object, **kwargs: object) -> None:
        pass


_metrics_collector: MetricsCollector | NullMetricsCollector = NullMetricsCollector()


def get_metrics_collector() -> MetricsCollector | NullMetricsCollector:
    return _metrics_collector


def set_metrics_collector(collector: MetricsCollector | NullMetricsCollector) -> None:
    global _metrics_collector
    _metrics_collector = collector


def initialize_metrics_collector(redis_client: Union[Redis, RedisCluster]) -> MetricsCollector | NullMetricsCollector:
    if os.getenv("ENABLE_MONITORING", "false").lower() not in ("true", "1", "yes"):
        logger.info("Monitoring disabled, using NullMetricsCollector")
        null_collector = NullMetricsCollector()
        set_metrics_collector(null_collector)
        return null_collector

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    service_name = os.getenv("OTEL_SERVICE_NAME", "hippius-s3")

    resource = Resource.create({"service.name": service_name})

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint, insecure=True),
        export_interval_millis=10000,
    )

    provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(provider)

    collector = MetricsCollector(redis_client)
    set_metrics_collector(collector)
    logger.info(f"Monitoring enabled, exporting to {endpoint}")
    return collector


class SimpleMetricsCollector:
    def __init__(self) -> None:
        self.meter = metrics.get_meter(__name__)
        self._backup_last_success_timestamp = 0.0
        self._setup_backup_metrics()

    def _setup_backup_metrics(self) -> None:
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
            name="backup_last_success_timestamp",
            callbacks=[self._obs_backup_last_success],
            description="Unix timestamp of last successful backup cycle",
        )

    def _obs_backup_last_success(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._backup_last_success_timestamp, {})]

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


class NullSimpleMetricsCollector:
    def record_backup_operation(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cycle(self, *args: object, **kwargs: object) -> None:
        pass

    def record_backup_cleanup(self, *args: object, **kwargs: object) -> None:
        pass


def initialize_metrics_simple(
    service_name: str = "hippius-s3-backup",
) -> SimpleMetricsCollector | NullSimpleMetricsCollector:
    if os.getenv("ENABLE_MONITORING", "false").lower() not in ("true", "1", "yes"):
        logger.info(f"Monitoring disabled for {service_name}, using NullSimpleMetricsCollector")
        return NullSimpleMetricsCollector()

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")

    resource = Resource.create({"service.name": service_name})

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint, insecure=True),
        export_interval_millis=10000,
    )

    provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(provider)

    logger.info(f"Monitoring enabled for {service_name}, exporting to {endpoint}")
    return SimpleMetricsCollector()


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
