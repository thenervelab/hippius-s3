import dataclasses
import ipaddress
import uuid

import dotenv

from hippius_s3.utils import env


dotenv.load_dotenv()

IpNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


def _parse_cidrs(value: str | None) -> tuple[IpNetwork, ...]:
    """Parse a comma-separated CIDR list into network objects.

    Parsed once, when the Config singleton is built, because ip_whitelist_middleware matches
    against this on every request to the api. A malformed or host-bits-set entry raises here, at
    startup, rather than being silently skipped and quietly widening the boundary.
    """
    return tuple(ipaddress.ip_network(part.strip()) for part in str(value or "").split(",") if part.strip())


# Deliberately NOT pinned to any one cluster's pod/service ranges: a CIDR that does not cover the
# gateway 403s every forwarded request and takes the whole api down, so narrowing this is an opt-in
# per deployment rather than a default.
#
# 192.168.0.0/16 is RFC1918 but is deliberately absent, and should not be "restored" on the grounds
# that it completes the set. Nothing here uses it — k8s pod networks are 10.x and docker-compose
# bridges land in 172.16/12 — and this list is the api's authorization boundary, so an unused range
# is boundary we are carrying for free. A deployment that genuinely needs it adds one entry.
_DEFAULT_IP_WHITELIST_CIDRS = "10.0.0.0/8,172.16.0.0/12,127.0.0.1/32,::1/128"


def _parse_csv_urls(value: str | None) -> list[str]:
    out: list[str] = []
    for part in str(value or "").split(","):
        u = part.strip().strip('"').strip("'")
        if not u:
            continue
        out.append(u.rstrip("/"))
    # Preserve order but de-dup
    deduped: list[str] = []
    seen: set[str] = set()
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        deduped.append(u)
    return deduped


def _parse_backends(value: str | None, default: str = "arion") -> list[str]:
    """Parse comma-separated list of backend names."""
    value = value or default
    return [b.strip() for b in value.split(",") if b.strip()]


def _parse_account_whitelist() -> list[str]:
    """Parse comma-separated account whitelist from environment variable."""
    import os

    whitelist_str = os.environ.get("HIPPIUS_ORPHAN_WORKER_ACCOUNT_WHITELIST", "")
    if whitelist_str:
        return [a.strip() for a in whitelist_str.split(",") if a.strip()]
    return []


@dataclasses.dataclass
class Config:
    """Application configuration settings."""

    # Database Configuration
    database_url: str = env("DATABASE_URL")
    # Inline default prevents KeyError during class init; runtime fallback to DATABASE_URL is applied in get_config()
    encryption_database_url: str = env("HIPPIUS_KEYSTORE_DATABASE_URL:", convert=str)

    # Security
    frontend_hmac_secret: str = env("FRONTEND_HMAC_SECRET")
    rate_limit_per_minute: int = env("RATE_LIMIT_PER_MINUTE", convert=int)
    max_request_size_mb: int = env("MAX_REQUEST_SIZE_MB", convert=int)
    # The api performs no authentication of its own — it trusts the X-Hippius-* headers the gateway
    # stamps. These CIDRs are therefore the entire boundary that makes "only the gateway reaches the
    # api" true, so they have to mean exactly what they say.
    api_ip_whitelist_cidrs: tuple[IpNetwork, ...] = env(
        f"API_IP_WHITELIST_CIDRS:{_DEFAULT_IP_WHITELIST_CIDRS}", convert=_parse_cidrs
    )

    # Logging
    log_level: str = env("LOG_LEVEL")
    loki_url: str = env("LOKI_URL:", convert=str)
    loki_enabled: bool = env("LOKI_ENABLED:false", convert=lambda x: x.lower() == "true")

    # Server Configuration
    host: str = env("HOST")
    port: int = env("PORT", convert=int)
    environment: str = env("ENVIRONMENT")
    debug: bool = env("DEBUG", convert=lambda x: x.lower() == "true")

    # Feature Flags
    enable_audit_logging: bool = env("ENABLE_AUDIT_LOGGING", convert=lambda x: x.lower() == "true")
    enable_api_docs: bool = env("ENABLE_API_DOCS", convert=lambda x: x.lower() == "true")
    enable_request_profiling: bool = env("ENABLE_REQUEST_PROFILING:false", convert=lambda x: x.lower() == "true")
    enable_banhammer: bool = env("ENABLE_BANHAMMER:true", convert=lambda x: x.lower() == "true")
    enable_public_read: bool = env("HIPPIUS_ENABLE_PUBLIC_READ:true", convert=lambda x: x.lower() == "true")
    public_bucket_cache_ttl_seconds: int = env("PUBLIC_BUCKET_CACHE_TTL_SECONDS:60", convert=int)
    enable_bypass_credit_check: bool = env("HIPPIUS_BYPASS_CREDIT_CHECK:false", convert=lambda x: x.lower() == "true")
    read_only_mode: bool = env("HIPPIUS_READ_ONLY_MODE:false", convert=lambda x: x.lower() == "true")
    # LS-1: do the ListObjectsV2 delimiter rollup in SQL (a loose-index skip-scan) instead of the
    # Python collapse. Opt-in — the CommonPrefixes/pagination contract is client-facing; a differential
    # test proves byte-identical output, and the Python path stays as the default rollback.
    list_objects_sql_rollup: bool = env("HIPPIUS_LIST_OBJECTS_SQL_ROLLUP:false", convert=lambda x: x.lower() == "true")

    # S3 Validation Limits
    min_bucket_name_length: int = env("MIN_BUCKET_NAME_LENGTH", convert=int)
    max_bucket_name_length: int = env("MAX_BUCKET_NAME_LENGTH", convert=int)
    max_object_key_length: int = env("MAX_OBJECT_KEY_LENGTH", convert=int)
    max_metadata_size: int = env("MAX_METADATA_SIZE", convert=int)

    # Blockchain
    substrate_url: str = env("HIPPIUS_SUBSTRATE_URL")
    hippius_service_key: str = env("HIPPIUS_SERVICE_KEY")
    arion_service_key: str = env("ARION_SERVICE_KEY")
    arion_bearer_token: str = env("ARION_BEARER_TOKEN")
    hippius_secret_decryption_material: str = env("HIPPIUS_AUTH_ENCRYPTION_KEY")

    validator_region: str = env("HIPPIUS_VALIDATOR_REGION")
    hippius_api_base_url: str = env("HIPPIUS_API_BASE_URL:https://api.hippius.com/")
    arion_billing_bypass_key: str = env("ARION_BILLING_BYPASS_KEY:")
    arion_rate_limiting_proxy_bypass_key: str = env("ARION_RATE_LIMITING_PROXY_BYPASS_KEY:")
    arion_base_url: str = env("HIPPIUS_ARION_BASE_URL:https://arion.hippius.com/")
    arion_verify_ssl: bool = env("HIPPIUS_ARION_VERIFY_SSL:true", convert=lambda x: x.lower() == "true")
    # Per-attempt cap for the can_upload billing gate specifically. Unlike every other call on
    # ArionClient this one runs inside the gateway's request path, so its worst case is gateway
    # capacity, not worker throughput. Tunable so an incident can shorten it without a deploy.
    can_upload_timeout_seconds: float = env("CAN_UPLOAD_TIMEOUT_SECONDS:3.0", convert=float)

    # Redis for caching/rate limiting
    redis_url: str = env("REDIS_URL")

    # Redis for account caching (persistent)
    redis_accounts_url: str = env("REDIS_ACCOUNTS_URL:redis://127.0.0.1:6380/0")

    # Redis for rate limiting and banhammer
    redis_rate_limiting_url: str = env("REDIS_RATE_LIMITING_URL:redis://127.0.0.1:6383/0")

    # Redis for queues (persistent)
    redis_queues_url: str = env("REDIS_QUEUES_URL:redis://127.0.0.1:6382/0")

    # Database connection pool configuration
    db_pool_min_size: int = env("API_DB_POOL_MIN_SIZE:5", convert=int)
    db_pool_max_size: int = env("API_DB_POOL_MAX_SIZE:20", convert=int)
    db_pool_max_queries: int = env("API_DB_POOL_MAX_QUERIES:50000", convert=int)
    db_pool_max_inactive_lifetime: int = env("API_DB_POOL_MAX_INACTIVE_LIFETIME:300", convert=int)
    db_pool_command_timeout: int = env("API_DB_POOL_COMMAND_TIMEOUT:30", convert=int)
    # Bounded wait for a free pooled connection. asyncpg blocks indefinitely by default;
    # a timeout lets the hot path fail fast with a retryable 503 instead of hanging under contention.
    db_pool_acquire_timeout: float = env("API_DB_POOL_ACQUIRE_TIMEOUT:5.0", convert=float)

    # API signing key for pre-signed URLs
    # Generated on first run if not provided
    api_signing_key: str = env("API_SIGNING_KEY:" + str(uuid.uuid4()))

    # S3 multipart upload settings
    max_multipart_part_size: int = 512 * 1024 * 1024  # 512 MiB per part
    max_multipart_part_count: int = 10000  # AWS S3 standard
    max_object_size: int = 0  # Computed at runtime in get_config()

    # worker specific settings
    unpinner_sleep_loop: float = 5.0
    downloader_sleep_loop: float = 0.01
    cacher_loop_sleep: float = 60.0  # 1 minute
    pin_checker_loop_sleep: float = 7200.0  # 2 hours
    orphan_checker_loop_sleep: int = env("ORPHAN_CHECKER_LOOP_SLEEP:7200", convert=int)  # 2 hours
    orphan_checker_batch_size: int = env("ORPHAN_CHECKER_BATCH_SIZE:500", convert=int)  # Files per API call
    orphan_checker_account_whitelist: list[str] = dataclasses.field(default_factory=_parse_account_whitelist)

    # Uploader configuration (supersedes legacy pinner config)
    uploader_max_attempts: int = env("HIPPIUS_UPLOADER_MAX_ATTEMPTS:7", convert=int)
    uploader_backoff_base_ms: int = env("HIPPIUS_UPLOADER_BACKOFF_BASE_MS:500", convert=int)
    uploader_backoff_max_ms: int = env("HIPPIUS_UPLOADER_BACKOFF_MAX_MS:60000", convert=int)
    uploader_multipart_max_concurrency: int = env("HIPPIUS_UPLOADER_MULTIPART_MAX_CONCURRENCY:5", convert=int)
    # Per-pod request concurrency: how many upload requests one uploader pod processes
    # at once. The serial outer loop was the aggregate-throughput ceiling on the
    # 1-chunk-dominated queue (within-part parallelism can't help single-chunk objects).
    uploader_max_inflight: int = env("HIPPIUS_UPLOADER_MAX_INFLIGHT:4", convert=int)
    # Single shared per-pod bound on concurrent Arion upload POSTs across ALL in-flight
    # requests — the one throttle on the scarce resource (Arion calls), so outer
    # concurrency can't stampede the backend.
    arion_upload_concurrency: int = env("HIPPIUS_ARION_UPLOAD_CONCURRENCY:8", convert=int)
    # Per-pod uploader DB pool size. Concurrent conn demand/pod is ~arion_upload_concurrency
    # (chunk tasks, one short acquire at a time) + a few outer acquires. Keep this modest:
    # it's multiplied by the uploader replica count against Postgres max_connections — e.g.
    # 40 pods x 12 = 480, leaving headroom for api/gateway/downloader/etc. Raise alongside
    # arion_upload_concurrency, watching total connections.
    uploader_db_pool_max: int = env("HIPPIUS_UPLOADER_DB_POOL_MAX:12", convert=int)
    # Heavy validation gating (legacy PINNER_VALIDATE_COVERAGE supported for compat)
    uploader_validate_coverage: bool = env("UPLOADER_VALIDATE_COVERAGE:false", convert=lambda x: x.lower() == "true")
    # Deadline (seconds) for polling meta.json on the shared FS cache before
    # giving up. The api pod writes meta last after fsync. On prod the api and
    # consumer workers are co-located on the cache node so reads see meta
    # immediately. On staging (and as defence-in-depth on prod) the worker
    # may live on a different node from the producer, so meta visibility can
    # lag the producer's fsync by ~hundreds of ms on a RWX CephFS mount.
    # Within this window a missing meta = "writer hasn't propagated yet" =
    # poll. After the window = genuine fault → raise (classifier routes
    # to DLQ as permanent).
    fs_meta_wait_seconds: float = env("HIPPIUS_FS_META_WAIT_SECONDS:30", convert=float)

    # Unpinner configuration
    # How many unpin requests one unpinner pod processes concurrently (outer bounded-dispatch,
    # mirroring the uploader). The old loop was a serial consumer.
    unpinner_max_inflight: int = env("HIPPIUS_UNPINNER_MAX_INFLIGHT:8", convert=int)
    # Single shared per-pod bound on concurrent Arion DELETEs across ALL in-flight requests — one
    # fat request expands to ~N chunk identifiers, so this is what actually unlocks delete throughput.
    unpinner_parallelism: int = env("HIPPIUS_UNPINNER_PARALLELISM:5", convert=int)
    # Explicit CAP on the per-pod unpinner DB pool. The loop sizes the pool to the throughput-optimal
    # `max_inflight + parallelism` but never above this cap, so raising max_inflight (an ops secret)
    # can't balloon Postgres connections (per pod × replicas runs close to server max_connections).
    # The cap is clamped up to the deadlock-safe floor (parallelism + 1); see run_unpinner_loop.
    unpinner_db_pool_max: int = env("HIPPIUS_UNPINNER_DB_POOL_MAX:16", convert=int)
    unpinner_max_attempts: int = env("HIPPIUS_UNPINNER_MAX_ATTEMPTS:5", convert=int)
    unpinner_backoff_base_ms: int = env("HIPPIUS_UNPINNER_BACKOFF_BASE_MS:1000", convert=int)
    unpinner_backoff_max_ms: int = env("HIPPIUS_UNPINNER_BACKOFF_MAX_MS:60000", convert=int)
    # Batch-delete via HCFS POST /delete_files (up to 1000 file deletes per HTTP call) instead of one
    # DELETE per chunk. Feature-flagged OFF: prod stays per-file until the flag is flipped AND HCFS
    # confirms /delete_files accepts our empty folder_hash. See docs/issues/unpinner-batch-delete.md.
    unpinner_batch_delete_enabled: bool = env(
        "HIPPIUS_UNPINNER_BATCH_DELETE:false", convert=lambda x: x.lower() == "true"
    )
    # Max file_ids per batch call — hard-capped at 1000 (HCFS limit) by the loop regardless of this.
    unpinner_batch_max_files: int = env("HIPPIUS_UNPINNER_BATCH_MAX_FILES:1000", convert=int)
    # folder_hash sent to /delete_files. Our S3 objects are uploaded with NO folder, so "" (root).
    # MERGE-BLOCKER: HCFS must confirm empty folder_hash works before this is used in prod.
    unpinner_folder_hash: str = env("HIPPIUS_ARION_FOLDER_HASH:", convert=str)

    # Per-operation backend lists (queue names derived as {backend}_{op}_requests)
    upload_backends: list[str] = env("HIPPIUS_UPLOAD_BACKENDS:arion", convert=_parse_backends)
    download_backends: list[str] = env("HIPPIUS_DOWNLOAD_BACKENDS:arion", convert=_parse_backends)
    delete_backends: list[str] = env("HIPPIUS_DELETE_BACKENDS:arion", convert=_parse_backends)
    # Optional additional backends that must have replicated a chunk before
    # the janitor is allowed to evict it from the FS cache. Unioned with
    # upload_backends when checking "fully replicated".
    backup_backends: list[str] = env("HIPPIUS_BACKUP_BACKENDS:", convert=lambda v: _parse_backends(v, default=""))

    # Cache TTL (shared across components — still used for pub/sub wait timeout)
    cache_ttl_seconds: int = env("HIPPIUS_CACHE_TTL:3600", convert=int)
    # Bound on how long a GET waits for its FIRST chunk before failing fast with a retryable 503
    # (DownloadNotReadyError). Keeps an un-drained/never-arriving object (e.g. a part not yet on any
    # backend) from hanging the whole request up to cache_ttl_seconds (~1h). Later chunks keep the
    # full wait — once the first chunk lands the object is actively draining.
    #
    # MUST STAY BELOW THE CLIENT'S READ TIMEOUT. This was 90s, which is above boto3's 60s default,
    # so the fail-fast never actually reached anyone: the client hung up at 60s and got a dead
    # socket, which is NOT retryable, while the 503 SlowDown this raises IS. Worse, from the
    # server's side the request later completed 200, so nothing was recorded as a failure.
    # Observed 2026-07-23 14:50:05 — a presigned GET returned 200 after processing_time_ms=60167,
    # 167ms after the client had already given up.
    #
    # 25s leaves room for two client-side retries inside one 60s budget. A cross-node
    # read-after-write costs ~60s end to end (the part lands on one node's SSD; a reconciler
    # notices, the drain copies it, an enqueue sweep publishes, the uploader uploads — every stage
    # a poll, not an event), so on a fresh object the FIRST attempt is EXPECTED to 503 and a retry
    # to succeed. That is the mechanism working, not a failure. Making the read genuinely fast is a
    # separate problem: serve it from the peer that holds the data instead of waiting out the
    # pipeline.
    stream_first_chunk_timeout_seconds: int = env("HIPPIUS_STREAM_FIRST_CHUNK_TIMEOUT_SECONDS:25", convert=int)
    # A3: bound on how long the streamer waits for EACH subsequent chunk (after the first). Without
    # it a later chunk whose backend fetch permanently fails stalls the already-committed 200
    # response up to cache_ttl_seconds (~1h) mid-stream; this caps that to a bounded fail (the
    # stream breaks and the client retries). Generous (5 min) so a healthy-but-slow drain never
    # trips it, but far below the 1h cache TTL.
    stream_chunk_timeout_seconds: int = env("HIPPIUS_STREAM_CHUNK_TIMEOUT_SECONDS:300", convert=int)
    # Hot-retention window for the FS cache: chunks read within this window are protected from
    # janitor deletion so frequently-accessed content stays on NVMe. Touched on every read by the
    # API/streamer, and HALVED at elevated pressure / zeroed at critical (_effective_hot_retention).
    #
    # 4h, raised from 1h on 2026-07-25. The 1h value (itself tightened from 3h earlier the same day,
    # on the theory that "a missed re-fetch is one backend read, cheap next to a full pool") turned
    # out to have the causality backwards: the janitor has NO LRU — it evicts on a binary atime
    # threshold in filesystem-walk order — so this window IS the working-set protection. Too short
    # and the live working set reads as "cold", gets evicted, is re-read, re-fetched from a backend
    # and re-written into the pool. That refill flywheel is what pinned the CephFS pool near-full
    # during the 2026-07-24 incident: pool writes ran at 172 MiB/s of pure churn and eviction never
    # got ahead. Raising the window collapsed it to ~54 MiB/s and the pool finally drained.
    #
    # 4h is empirically cheap: it retains only ~6% of walked parts (hot_parts 3546 / parts_seen
    # 56666 at pressure 0). Prod carries the same value in k8s/base/workers-deployments.yaml — keep
    # the two in step, and do not "optimise" this downward without re-checking the churn rate.
    fs_cache_hot_retention_seconds: int = env("HIPPIUS_FS_CACHE_HOT_RETENTION_SECONDS:14400", convert=int)
    # Unified object part chunk size (bytes) for cache and range math
    object_chunk_size_bytes: int = env("HIPPIUS_CHUNK_SIZE_BYTES:4194304", convert=int)
    # Downloader behavior (default: no whole-part backfill)
    downloader_allow_part_backfill: bool = env(
        "DOWNLOADER_ALLOW_PART_BACKFILL:false", convert=lambda x: x.lower() == "true"
    )

    # Downloader retry tuning (used by per-backend downloader workers)
    downloader_chunk_retries: int = env("DOWNLOADER_CHUNK_RETRIES:3", convert=int)
    downloader_retry_base_seconds: float = env("DOWNLOADER_RETRY_BASE_SECONDS:0.1", convert=float)
    downloader_retry_jitter_seconds: float = env("DOWNLOADER_RETRY_JITTER_SECONDS:0.1", convert=float)
    # Request-level retry: when a whole DownloadChainRequest fails (Arion exhaustion or a process
    # error), requeue it via a per-backend retry ZSET + 2s mover instead of dropping it (A12).
    # Mirrors the uploader's request-level retry (uploader_max_attempts / _backoff_*_ms).
    downloader_max_attempts: int = env("HIPPIUS_DOWNLOADER_MAX_ATTEMPTS:5", convert=int)
    downloader_backoff_base_ms: int = env("HIPPIUS_DOWNLOADER_BACKOFF_BASE_MS:500", convert=int)
    downloader_backoff_max_ms: int = env("HIPPIUS_DOWNLOADER_BACKOFF_MAX_MS:60000", convert=int)
    downloader_semaphore: int = env("DOWNLOADER_SEMAPHORE:20", convert=int)
    # Max concurrent DownloadChainRequests a single downloader pod processes.
    # The main loop dequeues and spawns tasks up to this cap; the semaphore
    # above still bounds total concurrent chunk fetches across all tasks.
    # Range-heavy read patterns produce many 1-part DCRs, so parallelising
    # DCRs is what delivers real backend throughput.
    downloader_max_inflight: int = env("DOWNLOADER_MAX_INFLIGHT:10", convert=int)
    # When multiple streamers hit a cache miss on the same part concurrently,
    # only one enqueues a DownloadChainRequest; the others wait via pub/sub.
    # The Redis lock that enforces this is cleared by the downloader on
    # completion (compare-and-delete on the enqueuer's token, A5), and this TTL
    # caps the worst-case hang if the downloader crashes mid-request. Raised to
    # 600s (A5) so a legitimately slow multi-chunk part download does not expire
    # the lock mid-flight and let a second streamer enqueue a duplicate DCR.
    download_coalesce_lock_ttl_seconds: int = env("DOWNLOAD_COALESCE_LOCK_TTL:600", convert=int)
    # DB-1: config-driven downloader Postgres pool (was hardcoded min=2/max=20). Audit
    # Σ(replicas × pool_max) across roles against Postgres max_connections before raising.
    downloader_db_pool_min: int = env("HIPPIUS_DOWNLOADER_DB_POOL_MIN:2", convert=int)
    downloader_db_pool_max: int = env("HIPPIUS_DOWNLOADER_DB_POOL_MAX:20", convert=int)
    # CF-3: depth of the encrypt producer/consumer queue per streaming write. Peak buffered memory
    # per PUT ≈ chunk_size × this. Exposed so it can move with chunk size (CF-1).
    write_queue_maxsize: int = env("HIPPIUS_WRITE_QUEUE_MAXSIZE:16", convert=int)
    # RD-2 / WU-1: worker threads for the dedicated AES-GCM encrypt/decrypt pool (off the event loop).
    crypto_pool_workers: int = env("HIPPIUS_CRYPTO_POOL_WORKERS:4", convert=int)
    # NET-3: keep the expensive mTLS KMS connection warm across sparse/bursty calls.
    ovh_kms_keepalive_expiry_seconds: int = env("HIPPIUS_OVH_KMS_KEEPALIVE_EXPIRY:300", convert=int)

    # Crypto configuration
    # hip-enc/legacy: SecretBox per-chunk (legacy objects)
    # hip-enc/aes256gcm: AES-256-GCM per-chunk (no upload_id in AAD)
    kek_db_pool_min_size: int = env("KEK_DB_POOL_MIN_SIZE:1", convert=int)
    kek_db_pool_max_size: int = env("KEK_DB_POOL_MAX_SIZE:10", convert=int)
    kek_cache_ttl_seconds: int = env("KEK_CACHE_TTL_SECONDS:300", convert=int)

    # KMS Mode Configuration
    # - "required": KMS is mandatory, fail at startup if not configured (prod/staging/e2e)
    # - "disabled": KMS is off, use local software wrapping (dev only)
    # No silent fallback - misconfiguration crashes fast.
    kms_mode: str = env("HIPPIUS_KMS_MODE:disabled", convert=str)

    # OVH KMS Configuration (for KEK wrapping)
    # Required when kms_mode=required, ignored when kms_mode=disabled
    ovh_kms_endpoint: str = env("HIPPIUS_OVH_KMS_ENDPOINT:", convert=str)
    ovh_kms_okms_id: str = env("HIPPIUS_OVH_KMS_OKMS_ID:", convert=str)
    ovh_kms_default_key_id: str = env("HIPPIUS_OVH_KMS_DEFAULT_KEY_ID:", convert=str)
    ovh_kms_cert_path: str = env("HIPPIUS_OVH_KMS_CERT_PATH:", convert=str)
    ovh_kms_key_path: str = env("HIPPIUS_OVH_KMS_KEY_PATH:", convert=str)
    # CA path (optional):
    # - If set: validates server cert against this CA (private CA / certificate pinning)
    # - If empty: uses system trust store (e.g., Let's Encrypt public CA)
    ovh_kms_ca_path: str = env("HIPPIUS_OVH_KMS_CA_PATH:", convert=str)
    ovh_kms_timeout_seconds: float = env("HIPPIUS_OVH_KMS_TIMEOUT_SECONDS:30.0", convert=float)
    ovh_kms_max_retries: int = env("HIPPIUS_OVH_KMS_MAX_RETRIES:3", convert=int)
    ovh_kms_retry_base_ms: int = env("HIPPIUS_OVH_KMS_RETRY_BASE_MS:500", convert=int)
    ovh_kms_retry_max_ms: int = env("HIPPIUS_OVH_KMS_RETRY_MAX_MS:5000", convert=int)

    # endpoint chunk download settings
    redis_read_chunk_timeout: int = 60

    # initial stream timeout (seconds) before sending first byte
    http_stream_initial_timeout_seconds: float = env("HTTP_STREAM_INITIAL_TIMEOUT_SECONDS:5", convert=float)

    # RQ-1: use ONE pub/sub subscription per stream (demuxed to per-chunk events) instead of a fresh
    # subscribe/unsubscribe per cold chunk. Correctness-sensitive (the demux + FS re-check race
    # guard); opt-in with a per-chunk fallback so it can be rolled back without a redeploy.
    stream_single_subscription: bool = env(
        "HIPPIUS_STREAM_SINGLE_SUBSCRIPTION:false", convert=lambda x: x.lower() == "true"
    )
    # Download streaming prefetch window (number of chunks to fetch concurrently).
    # Helps cache-hit throughput by reducing per-chunk Redis roundtrip stalls.
    http_stream_prefetch_chunks: int = env("HTTP_STREAM_PREFETCH_CHUNKS:16", convert=int)

    # DLQ configuration
    dlq_dir: str = env("HIPPIUS_DLQ_DIR:/tmp/hippius_dlq")
    dlq_archive_dir: str = env("HIPPIUS_DLQ_ARCHIVE_DIR:/tmp/hippius_dlq_archive")
    # Soft cap on entries per DLQ list (best-effort: a non-atomic LLEN+LPUSH may overshoot by up to
    # the number of concurrent pushers). The DLQ lives on redis-queues (2GB, noeviction) alongside the
    # drain's cephor:* lease/fence keys, work queues and notify:* pub/sub — a permanent-error storm on
    # an uncapped DLQ can fill the instance and fail ALL writes pipeline-wide. At the cap, push() is a
    # no-op (drop-newest). This drops the failure RECORD, never object data: the janitor's absolute
    # replication gate still refuses to evict a non-replicated chunk. Entries already in the DLQ remain
    # requeuable via scripts/dlq_requeue.py; a dropped failure's durable trace is object_versions.status
    # (e.g. 'failed'), and the 50%/90% alerts fire long before the cap so drops shouldn't occur in
    # practice. 0 (or negative) disables the cap.
    dlq_max_entries: int = env("HIPPIUS_DLQ_MAX_ENTRIES:10000", convert=int)

    # Object parts filesystem cache configuration
    object_cache_dir: str = env("HIPPIUS_OBJECT_CACHE_DIR:/var/lib/hippius/object_cache")
    object_cache_fallback_dir: str = env("HIPPIUS_OBJECT_CACHE_FALLBACK_DIR:", convert=str)
    # Copy a pool-served chunk onto this node's local NVMe so the next read of it comes off
    # flash (~6 ms/chunk) instead of CephFS (~40 ms). OFF by default and deliberately so: a
    # promoted copy is only reclaimable by a drain-agent evictor running on the same node, so
    # enabling it where no evictor runs would fill the disk with copies nothing owns.
    object_cache_promote_on_read: bool = env(
        "HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ:false", convert=lambda x: x.lower() == "true"
    )
    # Free-space floor below which a read stops promoting onto local flash. Promotion is the
    # only unthrottled writer to the ingest SSD and it shares that mount with ingest — on an
    # ingest node HIPPIUS_OBJECT_CACHE_DIR is the drain agent's CEPHOR_SSD_ROOT — so warming
    # the cache must yield to accepting writes.
    #
    # 0.175 is not arbitrary: it has to sit strictly INSIDE the drain evictor's hysteresis
    # band, between its reserve (150 permille) and its target (reserve + headroom = 200
    # permille). Equal to the target it chatters; above the target it deadlocks, because the
    # evictor only frees back to its target and could never restore enough for promotion to
    # resume. See FreeSpaceGate and test_promotion_pressure_guard.py.
    promote_min_free_ratio: float = env("HIPPIUS_PROMOTE_MIN_FREE_RATIO:0.175", convert=float)
    # Read a chunk from the peer node that holds it on flash (~6 ms + ~1 ms network) before
    # falling through to the CephFS pool (~40 ms). Resolved per PART: only ~2% of multi-part
    # objects have every part on one node, so there is no single node to route a request to.
    # OFF by default; needs NODE_NAME + POD_IP so peers can find each other.
    peer_fetch_enabled: bool = env("HIPPIUS_PEER_FETCH_ENABLED:false", convert=lambda x: x.lower() == "true")
    # How long a pod's published peer address stays valid. The TTL IS the liveness signal, so
    # it must exceed the refresh interval comfortably or a live pod flickers out of the map.
    peer_registry_ttl_seconds: int = env("HIPPIUS_PEER_REGISTRY_TTL_SECONDS:90", convert=int)
    peer_registry_refresh_seconds: int = env("HIPPIUS_PEER_REGISTRY_REFRESH_SECONDS:30", convert=int)
    # Bound on a peer read. A slow peer must lose to the pool quickly rather than adding its
    # latency on top of the pool read that follows.
    # The fallback costs ~40 ms (a CephFS pool read), so this is a loss-cut, not a deadline: a
    # peer that has not answered in 0.5s is already an order of magnitude worse than giving up,
    # and waiting the old 2.0s meant paying 50x the fallback before then ALSO paying the
    # fallback. Generous against a ~7 ms healthy peer read.
    peer_fetch_timeout_seconds: float = env("HIPPIUS_PEER_FETCH_TIMEOUT_SECONDS:0.5", convert=float)
    # An OVERALL bound on one peer fetch, connect included. httpx has no such thing: its
    # `Timeout` carries connect/read/write/pool only, and `read` bounds the gap BETWEEN body
    # pieces — so under the value above alone, a peer sending one piece every 0.4s holds the
    # connection and a growing buffer with no limit at all.
    # Deliberately NOT the same value: 0.5s of silence is a dead peer, but 0.5s to deliver a
    # whole 4 MiB chunk is a peer doing its job, so reusing the loss-cut as a deadline would
    # abort healthy large-chunk fetches. This is a backstop against a peer that never finishes,
    # not a second loss-cut.
    peer_fetch_deadline_seconds: float = env("HIPPIUS_PEER_FETCH_DEADLINE_SECONDS:2.0", convert=float)
    # Per-peer fanout: concurrent fetches this pod will have in flight to any ONE peer, and
    # concurrent peer requests this pod will SERVE. Both are needed — the client cap bounds
    # what one pod sends, but five pods each within their own cap still add up at the peer,
    # so the serving side sheds with 503 to protect its own ingest.
    # Must be >= http_stream_prefetch_chunks. Every chunk of one PART resolves to the same
    # peer, so a cap below the prefetch depth makes a single reader shed its own window to the
    # pool and book it as `client_cap` contention that does not exist. `effective_max_inflight`
    # enforces that floor at wiring time, so a stale value degrades to a warning, not a
    # silently halved peer tier.
    peer_fetch_max_inflight: int = env("HIPPIUS_PEER_FETCH_MAX_INFLIGHT:16", convert=int)
    peer_serve_max_inflight: int = env("HIPPIUS_PEER_SERVE_MAX_INFLIGHT:16", convert=int)
    # SERVE this node's flash to peers. Deliberately its own flag rather than a rider on
    # peer_fetch_enabled, which governs what this pod READS. Coupling them is how the serve
    # route came to be mounted on every api pod while the flag said the feature was off — and
    # `ip_whitelist` never bounded it, because the gateway is a pod on that same network and
    # proxies arbitrary paths from the internet. So this flag has to gate the mount, not just
    # the behaviour.
    #
    # `x.lower() == "true"` and not `convert=bool`: `bool("false")` is True — and `env` runs
    # the converter on the ":false" default too, so `convert=bool` makes the flag DEFAULT-ON
    # and impossible to turn off. The exact inversion this flag exists to prevent.
    peer_serve_enabled: bool = env("HIPPIUS_PEER_SERVE_ENABLED:false", convert=lambda x: x.lower() == "true")
    # Shared secret every peer presents on /internal/parts. It is sufficient BECAUSE the gateway
    # strips all inbound x-hippius-* headers before forwarding, so no client can supply it; the
    # pod network on its own is not a boundary. Empty means the route is not mounted AT ALL —
    # there must be no configuration meaning "serving, authentication off", since that state is
    # indistinguishable from the defect. repr=False so a stray str(config) cannot leak it.
    internal_peer_secret: str = env("HIPPIUS_INTERNAL_PEER_SECRET:", convert=str, repr=False)
    # 24h since last WRITE — the read path no longer bumps timestamps (read
    # recency lives in fs_cache_inventory.last_access_at), so this gates on
    # time since the part landed, not since it was last streamed.
    fs_cache_gc_max_age_seconds: int = env("HIPPIUS_FS_CACHE_GC_MAX_AGE_SECONDS:86400", convert=int)
    # An MPU is "abandoned" only after this long with NO part activity (not just since
    # initiation) — a user can pause a multipart upload and resume it, so the window must
    # exceed a realistic pause. 2 days gives leeway over a full-day pause + resume.
    mpu_stale_seconds: int = env("HIPPIUS_MPU_STALE_SECONDS:172800", convert=int)  # 2 days
    # Idle grace before an orphaned cephor_replication_status version (pending/draining,
    # unservable) is swept to `failed` by the A21 reaper sweep — the reaper's *eligibility* window.
    # The janitor's aged-pending-orphan GAUGE is now DECOUPLED onto aged_orphan_gauge_grace_seconds
    # (below), so this can stay the long (48h) leak backstop without making the leak-VISIBILITY gauge
    # read non-zero forever. Defaults to mpu_stale_seconds (tunable independently of it).
    mpu_sweep_grace_seconds: int = env("HIPPIUS_MPU_SWEEP_GRACE_SECONDS:172800", convert=int)  # 2 days
    # Idle grace for the janitor's aged-pending-orphan GAUGE only — deliberately DECOUPLED from
    # mpu_sweep_grace_seconds. The sweep grace above is the reaper's *eligibility* window (48h);
    # this is a leak-VISIBILITY window and must be SMALL. The gauge exists so a short (e.g. 6h)
    # soak can see a re-introduced A21 leak: an orphan aged only a couple hours must register, so
    # the soak gate's slope≈0 assertion is meaningful. Fed the 48h reaper grace, a soak-fresh leak
    # never ages past the window and the gate passes vacuously — the failure C3 fixes. 1h default:
    # long enough to exclude a still-arriving upload, short enough to surface a leak within a soak.
    aged_orphan_gauge_grace_seconds: int = env("HIPPIUS_AGED_ORPHAN_GAUGE_GRACE_SECONDS:3600", convert=int)  # 1h
    # How often the abandoned-multipart-upload reaper sweeps. The reaper auto-aborts
    # never-finalized uploads older than mpu_stale_seconds (address never written),
    # purging their SSD parts + drain replication rows so the drain stops re-deferring.
    mpu_reaper_interval_seconds: int = env("HIPPIUS_MPU_REAPER_INTERVAL_SECONDS:120", convert=int)  # every 2 min
    # Hard ceiling on any single reaper statement. The reaper's pool had NO command_timeout, so
    # on 2026-07-23 one abandoned-upload query ran for 96 MINUTES on a bad plan. The damage was
    # not the slowness: a long statement pins its snapshot, so the xmin horizon stops advancing
    # and VACUUM reclaims nothing database-wide — cephor_replication_status held ~499k dead
    # tuples through 429 autovacuum runs, its partial indexes bloated, the drain fell behind and
    # a cross-node read took 83s. The plan is fixed (list_abandoned_versions.sql, ~8s), but a
    # timeout is what bounds the DAMAGE of the next bad plan rather than that one instance.
    # Well above the measured runtime, far below anything that can hurt the horizon; a cycle
    # that trips it is logged and retried on the next interval.
    mpu_reaper_statement_timeout_seconds: int = env("HIPPIUS_MPU_REAPER_STATEMENT_TIMEOUT_SECONDS:60", convert=int)
    # Replication SLA grace for the G2 under-replication sentinel. `address` is stamped at
    # PUT completion but the chunk_backend coverage row is written much later by the async
    # drain→pool→backend pipeline, so every servable chunk is briefly under-covered while it
    # replicates NORMALLY. The sentinel only counts a servable, under-covered chunk once its
    # part landed (parts.uploaded_at) longer ago than this window — comfortably above normal
    # drain→backend replication latency — so in-flight replication is excluded and only
    # genuinely-stuck chunks page. Default 15m as a wide margin over the sub-minute latency
    # replication normally takes when the drain is not backlogged.
    # CAVEAT: this is a fixed WALL-CLOCK window, so it cannot distinguish "stuck" from "slow
    # under load" — under a sustained upload spike or a partial drain slowdown, NORMAL in-flight
    # replication can legitimately exceed 15m and re-trip the false page this grace exists to
    # remove (a queue-depth/backlog signal would; a clock cannot). It is env-tunable via
    # HIPPIUS_REPLICATION_SLA_SECONDS precisely so an operator can widen it for a known-slow
    # backlog rather than eat spurious pages. The resulting detection latency for a genuinely
    # stuck chunk (~SLA + the alert's for:10m ≈ 25m) is a deliberate, acceptable tradeoff for a
    # sentinel whose gap only bites under disk-pressure eviction, not at the moment of the stall.
    replication_sla_seconds: int = env("HIPPIUS_REPLICATION_SLA_SECONDS:900", convert=int)  # 15 min
    # Bounded concurrency for the janitor's per-part DB checks + deletes. The
    # cleanup loops are DB-roundtrip bound; this is how many parts are processed
    # in parallel (each over its own pooled connection).
    janitor_concurrency: int = env("HIPPIUS_JANITOR_CONCURRENCY:32", convert=int)
    # Concurrency for the FS *walk* itself (distinct from janitor_concurrency, which is the
    # per-part DB roundtrip fan-out). The cache root is a single flat directory of millions of
    # object dirs on CephFS; a single-threaded walk is metadata-latency bound (~40 objects/s
    # measured on prod 2026-07-23, so a full pass over ~2.8M objects is ~20h and never completes
    # inside a cycle — which starves every phase after it). This fans the per-object descent
    # (scandir + stat) across a thread pool so many CephFS metadata roundtrips are in flight at
    # once. Set to 1 for the legacy serial walk. Kept modest by default because the same CephFS
    # MDS serves live GET/PUT — do not crank without watching MDS latency.
    janitor_walk_concurrency: int = env("HIPPIUS_JANITOR_WALK_CONCURRENCY:8", convert=int)
    # Wall-clock budget for each FS-walk phase (stale-cleanup, age-GC, tmp-sweep). Once exceeded
    # the walk stops enqueuing new object dirs and the phase returns, so the cycle always
    # completes and the phases after it — plus the DB-only durability sentinel + aged-orphan
    # gauge which now run FIRST regardless — keep ticking. 0 = unbounded. Automatically lifted to
    # unbounded under CRITICAL disk pressure — freeing space must never be capped by a clock.
    janitor_walk_budget_seconds: int = env("HIPPIUS_JANITOR_WALK_BUDGET_SECONDS:480", convert=int)
    # Number of hash-shards the FS walk rotates through, one per cycle, for FAIR coverage: each
    # cycle descends only into object dirs where crc32(object_id) % shards == cycle_shard, so a
    # full sweep of the tree takes `shards` cycles and no object waits behind an always-truncated
    # prefix. SIZING: a shard must fit inside the budget or its tail is never reached — pick
    # shards ≳ (objects / (walk_concurrency · per-thread-obj/s · budget_s)). At ~2.8M objects,
    # concurrency 8 (~8× the ~40 obj/s serial rate measured on prod) and a 240s budget, one shard
    # is ~44k objects → comfortably inside budget; a full sweep is ~64 cycles (~10h at the 600s
    # normal sleep). Raise it if the cache grows or the walk logs truncated=True; 1 = walk the
    # whole tree every cycle. Forced to 1 under CRITICAL disk pressure so eviction sees the whole
    # tree in a single unbounded walk.
    janitor_walk_shards: int = env("HIPPIUS_JANITOR_WALK_SHARDS:64", convert=int)
    # Under ELEVATED pressure keep rotating a small number of shards instead of collapsing to a
    # single head-restarting whole-tree walk (the 480s budget truncates a 15.6M-entry readdir long
    # before the tail; with shards=1 the tail is never reached). CRITICAL still forces shards=1 +
    # unbounded budget — freeing space beats coverage fairness there.
    janitor_elevated_walk_shards: int = env("HIPPIUS_JANITOR_ELEVATED_WALK_SHARDS:8", convert=int)
    # Sleep between cycles while under any disk pressure. 120s of a ~600s cycle was dead time
    # exactly when eviction throughput mattered most.
    janitor_pressure_sleep_seconds: int = env("HIPPIUS_JANITOR_PRESSURE_SLEEP_SECONDS:15", convert=int)
    # Pool-fullness gate for the janitor's disk-pressure probe. _pressure_mode reads statvfs on the
    # cache mount, which sees the CephFS *PVC quota* — NOT the backing pool. On 2026-07-24 statvfs
    # read 69% while ceph-filesystem-data0 sat at 94%, so the janitor stayed in Normal mode (10min
    # sleep, 64-shard sweep, hot retention honored) while the pool filled to the read-only cliff.
    # When BOTH are set, the janitor also scrapes ceph_pool_percent_used for these pools from the
    # mgr exporter (the same signal PR #337 gave the drain allocator) and takes the MAX of that and
    # the local statvfs ratio: the pool signal can only ever RAISE pressure, never mask it. Empty =
    # statvfs-only (pre-incident behavior). Point the URL at the mgr exporter and list the same
    # pools as the drain's CEPHOR_CEPH_POOLS.
    janitor_ceph_mgr_metrics_url: str = env("HIPPIUS_JANITOR_CEPH_MGR_METRICS_URL:")
    janitor_ceph_pools: str = env("HIPPIUS_JANITOR_CEPH_POOLS:")
    # Per-scrape timeout for the pool-fullness probe; short relative to the cycle so a hung mgr
    # falls back to statvfs rather than stalling the loop.
    janitor_ceph_probe_timeout_seconds: float = env("HIPPIUS_JANITOR_CEPH_PROBE_TIMEOUT_SECONDS:5", convert=float)
    # Max soft-deleted objects hard-deleted per janitor cycle. The find query is
    # an index-probe over this many candidates; keep it bounded so a large
    # backlog drains gradually instead of in one DELETE-cascade burst.
    janitor_hard_delete_batch: int = env("HIPPIUS_JANITOR_HARD_DELETE_BATCH:30000", convert=int)
    # SQL discovery phase: inventory ROWS SCANNED per keyset page (the slice window that then gets
    # filtered for evictability — not the candidate count), and the per-cycle delete budget (0
    # disables the phase entirely — the runtime kill switch for prod rollback).
    janitor_sql_page_size: int = env("HIPPIUS_JANITOR_SQL_PAGE_SIZE:1000", convert=int)
    janitor_sql_max_deletes_per_cycle: int = env("HIPPIUS_JANITOR_SQL_MAX_DELETES_PER_CYCLE:50000", convert=int)
    # Per-page asyncpg query timeout for candidate discovery. A sparse ring (a head of young/hot/
    # under-replicated rows) can make one keyset page scan the whole inventory before it fills, so
    # this bounds per-page scan volume: a timeout ENDS discovery this cycle with the cursor left at
    # the last completed page, and the next cycle resumes from that spot.
    janitor_sql_query_timeout_seconds: float = env("HIPPIUS_JANITOR_SQL_QUERY_TIMEOUT_SECONDS:30", convert=float)

    # Filesystem cache disk-pressure backoff (ingress control).
    # Threshold can be expressed as either absolute bytes or ratio; we trigger if ANY threshold is hit.
    fs_cache_min_free_bytes: int = env("HIPPIUS_FS_CACHE_MIN_FREE_BYTES:10737418240", convert=int)  # 10 GiB
    fs_cache_min_free_ratio: float = env("HIPPIUS_FS_CACHE_MIN_FREE_RATIO:0.08", convert=float)  # 8%

    # Retry-After (seconds) for SlowDown responses caused by disk pressure.
    fs_cache_retry_after_seconds: float = env("HIPPIUS_FS_CACHE_RETRY_AFTER_SECONDS:2", convert=float)

    # Storage version to assign for newly created/overwritten objects
    # Only v5 is supported
    target_storage_version: int = env("HIPPIUS_TARGET_STORAGE_VERSION:5", convert=int)

    # Cachet health monitoring
    cachet_api_url: str = env("CACHET_API_URL:", convert=str)
    cachet_api_key: str = env("CACHET_API_KEY:", convert=str)
    cachet_component_id: int = env("CACHET_COMPONENT_ID:0", convert=int)


_config_singleton: "Config | None" = None


def reset_config() -> None:
    """Drop the memoized config so the next get_config() rebuilds it.

    Config is immutable after startup, so get_config() memoizes a single instance. Tests that mutate
    the environment between cases call this to force a rebuild.
    """
    global _config_singleton
    _config_singleton = None


def get_config() -> Config:
    """Get application configuration (memoized; immutable after startup)."""
    global _config_singleton
    if _config_singleton is not None:
        return _config_singleton

    try:
        cfg = Config()
    except KeyError as e:
        # env() raises KeyError for required vars without defaults (e.g. ENVIRONMENT).
        raise ValueError(f"{e.args[0]} environment variable is required but not set or empty") from None

    # Validate required ENVIRONMENT variable
    env_value = cfg.environment
    if not env_value or not str(env_value).strip():
        raise ValueError("ENVIRONMENT variable is required but not set or empty")

    # Ensure a usable keystore DSN (falls back to DATABASE_URL)
    if not cfg.encryption_database_url:
        object.__setattr__(cfg, "encryption_database_url", cfg.database_url)

    # Enforce environment constraints:
    # - Only in 'test' can enable_bypass_credit_check be True
    if env_value.lower() != "test":
        object.__setattr__(cfg, "enable_bypass_credit_check", False)

    # Compute max object size: min of S3 standard (5 TiB) and (part_size × part_count)
    s3_max_object_size = 5 * 1024**4  # 5 TiB - AWS S3 max object size
    computed_max = cfg.max_multipart_part_size * cfg.max_multipart_part_count
    object.__setattr__(cfg, "max_object_size", min(s3_max_object_size, computed_max))

    # Validate KMS mode
    if cfg.kms_mode not in ("required", "disabled"):
        raise ValueError(f"HIPPIUS_KMS_MODE must be 'required' or 'disabled', got: {cfg.kms_mode}")

    if cfg.kms_mode == "required":
        # KMS is mandatory - validate all env vars are present at config time
        kms_vars = [
            ("ovh_kms_endpoint", "HIPPIUS_OVH_KMS_ENDPOINT"),
            ("ovh_kms_okms_id", "HIPPIUS_OVH_KMS_OKMS_ID"),
            ("ovh_kms_default_key_id", "HIPPIUS_OVH_KMS_DEFAULT_KEY_ID"),
            ("ovh_kms_cert_path", "HIPPIUS_OVH_KMS_CERT_PATH"),
            ("ovh_kms_key_path", "HIPPIUS_OVH_KMS_KEY_PATH"),
        ]
        for attr, env_var in kms_vars:
            if not getattr(cfg, attr):
                raise ValueError(f"{env_var} is required when HIPPIUS_KMS_MODE=required")

        # Note: Cert file existence is validated at startup via init_kms_client()  # to handle docker-compose race conditions where cert files may be  # generated by another container.

    # Local wrapping requires a secret to derive the wrapping key
    if cfg.kms_mode == "disabled" and not cfg.hippius_secret_decryption_material:
        raise ValueError(
            "HIPPIUS_AUTH_ENCRYPTION_KEY is required when HIPPIUS_KMS_MODE=disabled (used for local KEK wrapping)"
        )

    # Cache only after successful validation so a bad config never poisons the singleton.
    _config_singleton = cfg
    return cfg
