import logging
import time
from typing import Awaitable
from typing import Callable
from typing import Optional

import redis.asyncio as async_redis
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from prometheus_client import start_http_server


logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, redis_client: async_redis.Redis):
        self.redis_client = redis_client

        # Set up OpenTelemetry resource
        resource = Resource.create(
            {
                "service.name": "hippius-s3-api",
                "service.version": "1.0.0",
            }
        )

        # Set up Prometheus metrics reader
        prometheus_reader = PrometheusMetricReader()

        # Set up meter provider
        provider = MeterProvider(
            resource=resource,
            metric_readers=[prometheus_reader],
        )

        # Set the global meter provider
        metrics.set_meter_provider(provider)

        # Get a meter
        self.meter = metrics.get_meter(__name__)

        # Define custom metrics
        self._setup_metrics()

    def _setup_metrics(self) -> None:
        # HTTP request metrics with account info
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

        # S3 operation metrics
        self.s3_objects_total = self.meter.create_counter(
            name="s3_objects_total", description="Total number of S3 objects", unit="1"
        )

        self.s3_buckets_total = self.meter.create_counter(
            name="s3_buckets_total", description="Total number of S3 buckets", unit="1"
        )

        # Multipart upload metrics
        self.multipart_uploads_active = self.meter.create_up_down_counter(
            name="multipart_uploads_active", description="Number of active multipart uploads", unit="1"
        )

        self.multipart_chunks_redis = self.meter.create_up_down_counter(
            name="multipart_chunks_redis_total", description="Number of multipart chunks stored in Redis", unit="1"
        )

        # Queue metrics
        self.queue_length = self.meter.create_up_down_counter(
            name="queue_length", description="Length of Redis queues", unit="1"
        )

        # IPFS operation metrics
        self.ipfs_operations_total = self.meter.create_counter(
            name="ipfs_operations_total", description="Total IPFS operations", unit="1"
        )

        self.ipfs_operation_duration = self.meter.create_histogram(
            name="ipfs_operation_duration_seconds", description="IPFS operation duration in seconds", unit="s"
        )

        # Cache download metrics
        self.cache_downloads_total = self.meter.create_counter(
            name="cache_downloads_total", description="Total downloads served from Redis cache", unit="1"
        )

        # Data transfer metrics
        self.s3_bytes_uploaded = self.meter.create_counter(
            name="s3_bytes_uploaded_total", description="Total bytes uploaded to S3", unit="bytes"
        )

        self.s3_bytes_downloaded = self.meter.create_counter(
            name="s3_bytes_downloaded_total", description="Total bytes downloaded from S3", unit="bytes"
        )

        # S3 operations counter by type
        self.s3_operations_total = self.meter.create_counter(
            name="s3_operations_total", description="Total S3 operations by type", unit="1"
        )

        # Multipart operation metrics
        self.multipart_parts_uploaded = self.meter.create_counter(
            name="multipart_parts_uploaded_total", description="Total multipart parts uploaded", unit="1"
        )

        self.multipart_uploads_completed = self.meter.create_counter(
            name="multipart_uploads_completed_total", description="Total completed multipart uploads", unit="1"
        )

        # Error tracking
        self.s3_errors_total = self.meter.create_counter(
            name="s3_errors_total", description="Total S3 errors by type", unit="1"
        )

        # Cache performance
        self.cache_hits = self.meter.create_counter(name="cache_hits_total", description="Total cache hits", unit="1")

        self.cache_misses = self.meter.create_counter(
            name="cache_misses_total", description="Total cache misses", unit="1"
        )

        # Object size distribution
        self.object_size_bytes = self.meter.create_histogram(
            name="object_size_bytes", description="Distribution of object sizes", unit="bytes"
        )

    async def update_queue_metrics(self) -> None:
        """Update queue length metrics from Redis"""
        try:
            upload_queue_length = int(await self.redis_client.llen("upload_requests"))  # type: ignore[misc]
            unpin_queue_length = int(await self.redis_client.llen("unpin_requests"))  # type: ignore[misc]

            self.queue_length.add(upload_queue_length, attributes={"queue_name": "upload_requests"})

            self.queue_length.add(unpin_queue_length, attributes={"queue_name": "unpin_requests"})

        except Exception as e:
            logger.error(f"Failed to update queue metrics: {e}")

    async def update_multipart_chunks_metric(self) -> None:
        """Count multipart chunks in Redis"""
        try:
            # Pattern to match multipart chunk keys
            pattern = "multipart:*:part:*"
            keys = [key async for key in self.redis_client.scan_iter(match=pattern)]

            self.multipart_chunks_redis.add(len(keys), attributes={"type": "multipart_chunks"})

        except Exception as e:
            logger.error(f"Failed to update multipart chunks metric: {e}")

    def record_http_request(
        self,
        request: Request,
        response: Response,
        duration: float,
        main_account: Optional[str] = None,
        subaccount_id: Optional[str] = None,
    ) -> None:
        """Record HTTP request metrics with account information"""

        # Create attributes dictionary
        attributes = {
            "method": request.method,
            "handler": request.url.path,
            "status_code": str(response.status_code),
        }

        # Add account information if available
        if main_account:
            attributes["main_account"] = main_account
        if subaccount_id:
            attributes["subaccount_id"] = subaccount_id

        # Record metrics
        self.http_requests_total.add(1, attributes=attributes)
        self.http_request_duration.record(duration, attributes=attributes)

        # Record bytes if available
        if hasattr(request, "content_length") and request.content_length:
            self.http_request_bytes.add(request.content_length, attributes={**attributes, "direction": "in"})

        if hasattr(response, "content_length") and response.headers.get("content-length"):
            self.http_response_bytes.add(
                int(response.headers["content-length"]), attributes={**attributes, "direction": "out"}
            )

    def record_s3_operation(
        self,
        operation: str,
        bucket_name: str,
        object_key: Optional[str] = None,
        main_account: Optional[str] = None,
        subaccount_id: Optional[str] = None,
        success: bool = True,
    ) -> None:
        """Record S3 operation metrics"""
        attributes = {"operation": operation, "bucket_name": bucket_name, "success": str(success).lower()}

        if object_key:
            attributes["object_key"] = object_key
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
        """Record IPFS operation metrics"""
        attributes = {"operation": operation, "success": str(success).lower()}

        if cid:
            attributes["cid"] = cid
        if main_account:
            attributes["main_account"] = main_account

        self.ipfs_operations_total.add(1, attributes=attributes)
        self.ipfs_operation_duration.record(duration, attributes=attributes)

    def record_cache_download(
        self,
        object_type: str,  # "simple" or "multipart"
        range_request: bool = False,
        main_account: Optional[str] = None,
    ) -> None:
        """Record cache download metrics"""
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
        object_key: Optional[str] = None,
        main_account: Optional[str] = None,
        subaccount_id: Optional[str] = None,
    ) -> None:
        """Record data transfer metrics"""
        attributes = {
            "operation": operation,
            "bucket_name": bucket_name,
        }

        if object_key:
            attributes["object_key"] = object_key
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
        upload_id: Optional[str] = None,
        part_number: Optional[int] = None,
        main_account: Optional[str] = None,
        subaccount_id: Optional[str] = None,
    ) -> None:
        """Record multipart operation metrics"""
        attributes = {
            "operation": operation,
        }

        if upload_id:
            attributes["upload_id"] = upload_id
        if part_number:
            attributes["part_number"] = str(part_number)
        if main_account:
            attributes["main_account"] = main_account
        if subaccount_id:
            attributes["subaccount_id"] = subaccount_id

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

        if bucket_name:
            attributes["bucket_name"] = bucket_name
        if main_account:
            attributes["main_account"] = main_account

        self.s3_errors_total.add(1, attributes=attributes)

    def record_cache_operation(
        self,
        hit: bool,
        operation: str,
        main_account: Optional[str] = None,
    ) -> None:
        """Record cache hit/miss metrics"""
        attributes = {
            "operation": operation,
        }

        if main_account:
            attributes["main_account"] = main_account

        if hit:
            self.cache_hits.add(1, attributes=attributes)
        else:
            self.cache_misses.add(1, attributes=attributes)


# Global metrics collector instance
metrics_collector: Optional[MetricsCollector] = None


def setup_monitoring(app: FastAPI, redis_client: async_redis.Redis, port: int = 8000) -> MetricsCollector:
    """Set up monitoring for the FastAPI application"""
    global metrics_collector

    logger.info("Setting up OpenTelemetry monitoring...")

    # Initialize metrics collector
    metrics_collector = MetricsCollector(redis_client)

    # Start Prometheus HTTP server for metrics endpoint
    start_http_server(port=port, addr="0.0.0.0")
    logger.info(f"Prometheus metrics server started on port {port}")

    # Auto-instrument FastAPI
    FastAPIInstrumentor.instrument_app(app)

    # Auto-instrument Redis
    RedisInstrumentor().instrument()

    # Auto-instrument AsyncPG (PostgreSQL)
    AsyncPGInstrumentor().instrument()

    # Add middleware for custom metrics
    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        start_time = time.time()

        # Process request
        response = await call_next(request)

        # Calculate duration
        duration = time.time() - start_time

        # Extract account info from request state if available
        main_account = None
        subaccount_id = None

        if hasattr(request.state, "account"):
            main_account = getattr(request.state.account, "main_account", None)
            subaccount_id = getattr(request.state.account, "id", None)

        # Record metrics
        if metrics_collector:
            metrics_collector.record_http_request(
                request=request,
                response=response,
                duration=duration,
                main_account=main_account,
                subaccount_id=subaccount_id,
            )

        return response

    logger.info("OpenTelemetry monitoring setup completed")

    return metrics_collector


def get_metrics_collector() -> Optional[MetricsCollector]:
    """Get the global metrics collector instance"""
    return metrics_collector
