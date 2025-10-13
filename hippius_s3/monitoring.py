import logging
from typing import Optional

import redis.asyncio as async_redis
from fastapi import Request
from fastapi import Response
from opentelemetry import metrics
from opentelemetry import trace


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class MetricsCollector:
    def __init__(self, redis_client: async_redis.Redis):
        self.redis_client = redis_client
        self.meter = metrics.get_meter(__name__)
        self._upload_len = 0
        self._unpin_len = 0
        self._substrate_len = 0
        self._main_db_size = 0
        self._accounts_db_size = 0
        self._chain_db_size = 0
        self._used_mem = 0
        self._max_mem = 0
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

    def _obs_redis_used_mem(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._used_mem, {})]

    def _obs_redis_max_mem(self, _: object) -> list[metrics.Observation]:
        return [metrics.Observation(self._max_mem, {})]

    def _obs_queue_lengths(self, _: object) -> list[metrics.Observation]:
        return [
            metrics.Observation(self._upload_len, {"queue_name": "upload_requests"}),
            metrics.Observation(self._unpin_len, {"queue_name": "unpin_requests"}),
            metrics.Observation(self._substrate_len, {"queue_name": "substrate_requests"}),
        ]

    def _obs_db_sizes(self, _: object) -> list[metrics.Observation]:
        return [
            metrics.Observation(self._main_db_size, {"redis_instance": "main"}),
            metrics.Observation(self._accounts_db_size, {"redis_instance": "accounts"}),
            metrics.Observation(self._chain_db_size, {"redis_instance": "chain"}),
        ]

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
    ) -> None:
        attributes = {
            "main_account": main_account,
            "success": str(success).lower(),
        }

        self.unpinner_requests_total.add(1, attributes=attributes)

        if num_files > 0:
            self.unpinner_files_unpinned.add(num_files, attributes=attributes)

        if duration is not None:
            self.unpinner_duration.record(duration, attributes=attributes)


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


_metrics_collector: MetricsCollector | NullMetricsCollector = NullMetricsCollector()


def get_metrics_collector() -> MetricsCollector | NullMetricsCollector:
    return _metrics_collector


def set_metrics_collector(collector: MetricsCollector | NullMetricsCollector) -> None:
    global _metrics_collector
    _metrics_collector = collector


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
