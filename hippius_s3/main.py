"""Main application module for Hippius S3 service."""

import asyncio
import logging
import os
import platform
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from typing import AsyncGenerator

import asyncpg
import httpx
import redis.asyncio as async_redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from gateway.middlewares.account import account_middleware
from gateway.middlewares.acl import acl_middleware
from gateway.middlewares.acl_subresource import acl_subresource_middleware
from gateway.middlewares.ats_purge import ats_purge_middleware
from gateway.middlewares.audit_log import audit_log_middleware
from gateway.middlewares.auth_probe import auth_probe_middleware
from gateway.middlewares.auth_router import auth_router_middleware
from gateway.middlewares.cache_control import cache_control_middleware
from gateway.middlewares.cache_invalidation import cache_invalidation_middleware
from gateway.middlewares.cors import cors_middleware
from gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware
from gateway.middlewares.input_validation import input_validation_middleware
from gateway.middlewares.path_normalization import path_normalization_middleware
from gateway.middlewares.ray_id import ray_id_middleware
from gateway.middlewares.read_only import read_only_middleware
from gateway.middlewares.trailing_slash import trailing_slash_normalizer
from gateway.services.acl_service import ACLService
from hippius_s3.api.internal_parts import router as internal_parts_router
from hippius_s3.api.middlewares.fs_cache_pressure import fs_cache_pressure_middleware
from hippius_s3.api.middlewares.metrics import metrics_middleware
from hippius_s3.api.middlewares.profiler import SpeedscopeProfilerMiddleware
from hippius_s3.api.middlewares.request_context import request_context_middleware
from hippius_s3.api.middlewares.tracing import tracing_middleware
from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.api.s3.public_router import router as public_router
from hippius_s3.api.s3.router import router as s3_router_new
from hippius_s3.api.sub_token_scopes import router as sub_token_scopes_router
from hippius_s3.api.user import router as user_router
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.cache import create_fs_store
from hippius_s3.cache.peers import PEER_PORT
from hippius_s3.cache.peers import PeerChunkFetcher
from hippius_s3.cache.peers import PeerRegistry
from hippius_s3.cache.peers import effective_max_inflight
from hippius_s3.cache.read_recency import initialize_read_recency_recorder
from hippius_s3.cache.replication_probe import create_replication_suspect_probe
from hippius_s3.cache.residency import create_residency_recorder
from hippius_s3.config import Config
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.metrics_collector_task import BackgroundMetricsCollector
from hippius_s3.peer_auth import validate_peer_secret
from hippius_s3.promote_floor import create_published_floor_source
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScopeRepository
from hippius_s3.writer.landed import initialize_landed_publisher


logger = logging.getLogger(__name__)


def _warn_if_no_aes_hw_accel() -> None:
    """Best-effort warning when AES-NI (x86) isn't advertised.

    This is a heuristic for performance expectations when using AES-GCM. On Linux,
    we check /proc/cpuinfo for the 'aes' flag. In containers, this generally reflects
    the host CPU flags exposed to the workload.
    """
    try:
        if platform.system().lower() != "linux":
            return
        txt = Path("/proc/cpuinfo").read_text(encoding="utf-8").lower()
        # cpuinfo lines include: "flags : ... aes ..."
        if "flags" in txt and re.search(r"\baes\b", txt) is None:
            logger.warning(
                "AES hardware acceleration flag not detected in /proc/cpuinfo. "
                "AES-GCM may be significantly slower on this node."
            )
    except Exception:
        # Don't fail startup for a best-effort performance hint.
        return


async def postgres_create_pool(database_url: str, config: Config) -> asyncpg.Pool:
    """Create and return a Postgres connection pool.

    Args:
        database_url: Postgres connection URL
        config: Application configuration with pool settings

    Returns:
        Connection pool for Postgres
    """
    return await asyncpg.create_pool(
        database_url,
        min_size=config.db_pool_min_size,
        max_size=config.db_pool_max_size,
        max_queries=config.db_pool_max_queries,
        max_inactive_connection_lifetime=config.db_pool_max_inactive_lifetime,
        command_timeout=config.db_pool_command_timeout,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """FastAPI application lifespan handler."""
    try:
        app.state.config = get_config()
        config = app.state.config
        _warn_if_no_aes_hw_accel()

        # Initialize KMS client (fail-fast in required mode, no-op in disabled mode)
        from hippius_s3.services.kek_service import init_kms_client

        await init_kms_client(config)

        app.state.postgres_pool = await postgres_create_pool(config.database_url, config)
        logger.info(f"Postgres connection pool created: min={config.db_pool_min_size}, max={config.db_pool_max_size}")

        from hippius_s3.redis_utils import create_redis_client

        app.state.redis_client = create_redis_client(config.redis_url)
        logger.info("Redis client initialized")

        app.state.redis_accounts_client = async_redis.from_url(config.redis_accounts_url)
        logger.info("Redis accounts client initialized")

        app.state.redis_rate_limiting_client = async_redis.from_url(config.redis_rate_limiting_url)
        logger.info("Redis rate limiting client initialized")

        app.state.redis_queues_client = async_redis.from_url(config.redis_queues_url)
        logger.info("Redis queues client initialized")

        app.state.redis_acl = async_redis.from_url(config.redis_acl_url, decode_responses=True)
        logger.info("Redis ACL client initialized")

        app.state.acl_service = ACLService(
            db_pool=app.state.postgres_pool,
            redis_client=app.state.redis_acl,
            cache_ttl=config.acl_cache_ttl_seconds,
        )
        logger.info("ACLService initialized")

        from hippius_s3.services.arion_service import ArionClient
        from hippius_s3.services.hippius_api_service import HippiusApiClient

        app.state.arion_client = ArionClient(
            base_url=config.arion_base_url,
            service_key=config.arion_service_key,
        )
        logger.info("ArionClient initialized")

        # NET-5: one long-lived HippiusApiClient so auth-cache misses reuse a warm connection pool
        # instead of building and tearing down a client per miss.
        app.state.hippius_api_client = HippiusApiClient()
        logger.info("HippiusApiClient initialized")

        from hippius_s3.queue import initialize_queue_client
        from hippius_s3.redis_cache import initialize_cache_client

        initialize_queue_client(app.state.redis_queues_client)
        logger.info("Queue client initialized")

        initialize_cache_client(app.state.redis_client)
        logger.info("Cache Redis client initialized")

        # IPFS service not needed in API container; workers own IPFS interactions

        # Cache repositories
        # Chunks are stored on the shared filesystem (via FileSystemPartsStore).
        # Redis is used only for pub/sub chunk-ready notifications (queues_client).
        # A promoted chunk lands on a node that did not ingest the part, so it must be
        # claimed for THIS node or the drain-agent's evictor — scoped by node_id — can never
        # reclaim it. No node identity means no recorder, which disables promotion outright.
        app.state.residency_recorder = create_residency_recorder(app.state.postgres_pool, os.getenv("NODE_NAME", ""))
        # Peer tier: resolve who holds a part on flash and read it from them before the pool.
        # Peers address each other by POD IP (not a hostPort on the node IP), which keeps the
        # traffic on the pod network; peer requests authenticate with the shared secret
        # (is_authorized_peer_fetch) since the merge removed the ip_whitelist middleware.
        node_name = os.getenv("NODE_NAME", "")
        pod_ip = os.getenv("POD_IP", "")
        app.state.peer_registry = None
        peer_fetch = None
        if config.peer_fetch_enabled and node_name and pod_ip:
            app.state.peer_http = httpx.AsyncClient(timeout=config.peer_fetch_timeout_seconds)
            app.state.peer_registry = PeerRegistry(
                app.state.redis_client,
                node_name,
                # Built from the same constant the resolver pins addresses to, so a peer's
                # registration and the allow-list that admits it cannot drift apart.
                f"http://{pod_ip}:{PEER_PORT}",
                config.peer_registry_ttl_seconds,
            )
            await app.state.peer_registry.register()
            # The published address carries a TTL, so it must be refreshed for the pod's
            # lifetime; a single registration would lapse and drop this node out of the map.
            app.state.peer_refresh_task = asyncio.create_task(
                app.state.peer_registry.run_refresh(config.peer_registry_refresh_seconds)
            )
            peer_fetch = PeerChunkFetcher(
                app.state.postgres_pool,
                app.state.peer_registry,
                node_name,
                app.state.peer_http,
                # Floored at the prefetch depth: chunks of one part all resolve to the same
                # peer, so a lower cap has a single reader shedding its own window to the pool.
                max_inflight=effective_max_inflight(
                    config.peer_fetch_max_inflight,
                    config.http_stream_prefetch_chunks,
                ),
                auth_secret=config.internal_peer_secret,
                # The httpx timeout above bounds each socket operation, never the whole
                # response; this is what stops a peer that drips bytes forever.
                deadline_seconds=config.peer_fetch_deadline_seconds,
            )
            logger.info("Peer chunk fetch enabled for node %s", node_name)
            if not config.internal_peer_secret:
                # Fetching is on but there is no secret to present, so every peer refuses with a
                # 404 and every read falls to the pool. That degradation is invisible from the
                # outside — reads still succeed — and shows up only as
                # chunk_reads_by_tier_total{tier=peer} flat, so it has to announce itself. The
                # case is reachable by deploy ordering: the image can land before the secret.
                logger.warning(
                    "HIPPIUS_PEER_FETCH_ENABLED is on but HIPPIUS_INTERNAL_PEER_SECRET is unset — "
                    "peers will refuse every fetch and all reads will fall through to the pool"
                )

        # Serving-side state, read by the /internal/parts endpoint. Keyed off peer_serve_enabled
        # and nothing else, so it is set on every path that mounts the route. It used to hang off
        # the fetch block above on the reasoning that "a node that fetches from peers is also one
        # peers fetch from" — true, but it is the converse that bites: the route was mounted
        # unconditionally, so with fetching off the endpoint was live with NO cap at all
        # (internal_parts.py reads a missing limiter as "no limit"). A cap that only exists when
        # an unrelated flag is on is not a cap.
        if config.peer_serve_enabled:
            app.state.peer_auth_secret = config.internal_peer_secret
            app.state.peer_serve_limiter = asyncio.Semaphore(config.peer_serve_max_inflight)
            logger.info("Peer chunk serving enabled with an in-flight cap of %d", config.peer_serve_max_inflight)

        # Eviction orders on COALESCE(last_read_at, resident_at), so a locally-served part has
        # to say so or the evictor falls back to arrival order — which for a re-read working set
        # tends to drop exactly the parts about to be needed again. Installed as a module
        # singleton (not merely app state) because the WRITE path stamps through it too:
        # `write_meta` marks a just-(re)written part hot so a rewrite of a replicated part
        # cannot rank as the evictor's coldest candidate while its landed announcement is still
        # in flight to the drain agent.
        app.state.read_recency_recorder = initialize_read_recency_recorder(app.state.postgres_pool, node_name)
        # Promotion must stay inside the band THIS node's evictor is holding, and the drain
        # allocator moves that band at runtime — so the floor is read from the agent rather than
        # configured here. The queues Redis is the only instance both processes share (the agent
        # has no main-Redis URL); an unavailable key falls back to `promote_min_free_ratio`.
        published_floor = create_published_floor_source(app.state.redis_queues_client, node_name)
        app.state.fs_store = create_fs_store(
            config,
            on_promote=app.state.residency_recorder,
            # The claim's other half, from the SAME recorder: a claim whose disk write fails
            # must be subtracted back or the accumulating row inflates on every retry.
            on_promote_release=(
                app.state.residency_recorder.release if app.state.residency_recorder is not None else None
            ),
            peer_fetch=peer_fetch,
            on_local_read=app.state.read_recency_recorder,
            published_floor=published_floor.ratio if published_floor is not None else None,
            # AEAD-retry guard: a redrive marks a part's pool copy stale while `chunk_exists`
            # still passes, so the invalidation path re-checks the status freshly per failure.
            replication_suspect=create_replication_suspect_probe(app.state.postgres_pool),
        )
        app.state.obj_cache = RedisObjectPartsCache(
            app.state.redis_client,
            queues_client=app.state.redis_queues_client,
            fs_store=app.state.fs_store,
        )
        # Tell this node's drain agent about each part as it lands, so discovery costs one queue
        # pop instead of the reconciler's walk of the node's entire replicated shard. Best-effort
        # and node-scoped: without NODE_NAME there is no way to say whose agent should drain the
        # part, so nothing is published and the reconciler keeps doing the job.
        if initialize_landed_publisher(app.state.redis_queues_client, node_name) is not None:
            logger.info("Landed-part announcements enabled for node %s", node_name)
        logger.info("Cache repositories initialized")

        app.state.sub_token_scope_repo = SubTokenScopeRepository(db_pool=app.state.postgres_pool)
        logger.info("SubTokenScopeRepository initialized")

        from hippius_s3.monitoring import MetricsCollector
        from hippius_s3.monitoring import set_metrics_collector

        app.state.metrics_collector = MetricsCollector(app.state.redis_client)
        set_metrics_collector(app.state.metrics_collector)

        logger.info("Metrics collector initialized")
        logger.info("Tracing and metrics handled by programmatic OTel init")

        # Start background metrics collection
        app.state.background_metrics_collector = BackgroundMetricsCollector(
            app.state.metrics_collector,
            app.state.redis_client,
            app.state.redis_accounts_client,
            app.state.redis_rate_limiting_client,
            app.state.redis_queues_client,
        )
        await app.state.background_metrics_collector.start()
        logger.info("Background metrics collection started")

        async def collect_pool_metrics() -> None:
            while True:
                await asyncio.sleep(60)
                if hasattr(app.state, "postgres_pool") and hasattr(app.state, "metrics_collector"):
                    pool = app.state.postgres_pool
                    size = pool.get_size()
                    free = pool.get_idle_size()
                    app.state.metrics_collector.update_db_pool_metrics(size, free)

        asyncio.create_task(collect_pool_metrics())
        logger.info("Pool metrics collection task started")

        # Read-recency tracker: feeds fs_cache_inventory.last_access_at so the
        # janitor's hot retention sees reads (atime can't — see access_tracker).
        from hippius_s3.cache.access_tracker import initialize_access_tracker

        tracker = initialize_access_tracker(
            app.state.postgres_pool,
            hot_window_seconds=float(config.fs_cache_hot_retention_seconds),
        )
        app.state.access_tracker_task = asyncio.create_task(tracker.run())
        logger.info("Access tracker flush task started")

        yield

    finally:
        try:
            if hasattr(app.state, "access_tracker_task"):
                app.state.access_tracker_task.cancel()
                import contextlib

                with contextlib.suppress(asyncio.CancelledError):
                    await app.state.access_tracker_task
        except Exception:
            logger.exception("Error stopping access tracker task")

        try:
            # Stop background metrics collection
            if hasattr(app.state, "background_metrics_collector"):
                await app.state.background_metrics_collector.stop()
                logger.info("Background metrics collection stopped")
        except Exception:
            logger.exception("Error shutting down background metrics collector")

        try:
            await app.state.redis_client.close()
            logger.info("Redis client closed")
        except Exception:
            logger.exception("Error shutting down Redis client")

        try:
            await app.state.redis_accounts_client.close()
            logger.info("Redis accounts client closed")
        except Exception:
            logger.exception("Error shutting down Redis accounts client")

        try:
            await app.state.redis_rate_limiting_client.close()
            logger.info("Redis rate limiting client closed")
        except Exception:
            logger.exception("Error shutting down Redis rate limiting client")

        try:
            await app.state.redis_queues_client.close()
            logger.info("Redis queues client closed")
        except Exception:
            logger.exception("Error shutting down Redis queues client")

        try:
            await app.state.redis_acl.close()
            logger.info("Redis ACL client closed")
        except Exception:
            logger.exception("Error shutting down Redis ACL client")

        try:
            await app.state.arion_client.close()
            logger.info("ArionClient closed")
        except Exception:
            logger.exception("Error shutting down ArionClient")

        try:
            await app.state.hippius_api_client.close()
            logger.info("HippiusApiClient closed")
        except Exception:
            logger.exception("Error shutting down HippiusApiClient")

        try:
            from gateway.services import ats_cache_client

            await ats_cache_client.close()
            logger.info("ATS cache client closed")
        except Exception:
            logger.exception("Error shutting down ATS cache client")

        try:
            await app.state.postgres_pool.close()
            logger.info("Postgres connection pool closed")
        except Exception:
            logger.exception("Error shutting down postgres pool")

        try:
            from hippius_s3.services.kek_service import close_kek_pool

            await close_kek_pool()
            logger.info("KEK connection pool closed")
        except Exception:
            logger.exception("Error shutting down KEK pool")


def factory() -> FastAPI:
    """Factory function to create and configure the FastAPI application."""
    from hippius_s3.otel_setup import configure_otel

    configure_otel("hippius-s3-api")

    load_dotenv()
    config = get_config()
    setup_loki_logging(config, "api")

    # A malformed peer secret fails on the READ path: httpx ascii-encodes header values and raises
    # UnicodeEncodeError, which is a ValueError, so the fetcher's `except (httpx.HTTPError, OSError)`
    # does not absorb it into a pool fallback the way it absorbs every other peer failure. The GET
    # fails outright, arbitrarily long after the deploy that misconfigured it.
    #
    # Here and not in get_config(): the api is the only process that puts this secret on a wire
    # (PeerChunkFetcher is constructed in this module's lifespan and nowhere else — workers call
    # create_fs_store without a peer_fetch), so validating at config load would crash the janitor,
    # uploader and unpinner over a value they never read.
    #
    # After setup_loki_logging on purpose, so the traceback reaches the log store an operator
    # actually reads rather than only container stderr.
    validate_peer_secret(config.internal_peer_secret)

    from hippius_s3.sentry import init_sentry

    init_sentry("hippius-s3-api")

    app = FastAPI(
        title="Hippius S3",
        description="Hippius S3 Gateway",
        docs_url="/docs" if config.enable_api_docs else None,
        redoc_url="/redoc" if config.enable_api_docs else None,
        swagger_favicon_url="/static/favicon.ico",
        lifespan=lifespan,
        debug=config.debug,
        default_response_class=Response,
    )

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title=app.title,
            version="1.0.0",
            description=app.description,
            routes=app.routes,
        )
        openapi_schema["components"]["securitySchemes"] = {
            "Access Key": {
                "type": "http",
                "scheme": "bearer",
                "description": (
                    "Bearer token authentication using Hippius access keys. "
                    "Format: 'hip_' followed by alphanumeric characters. "
                    "Example: hip_abc123def456ghi789. "
                    "Obtain from https://console.hippius.com/dashboard/settings"
                ),
            }
        }
        openapi_schema["security"] = [{"Access Key": []}]
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # ty: ignore[invalid-assignment]

    # Starlette: last-registered = outermost. On the request path, outer
    # middlewares run first; the handler runs last; the response unwinds back
    # through them. The list below is ordered innermost → outermost. This is
    # the pre-merge gateway chain with the api's middlewares composed in at
    # the positions the forward hop used to occupy:
    #
    #   cors → ray_id → path_normalization → cache_control → ats_purge
    #   → cache_invalidation → [read_only] → fs_cache_pressure → input_validation
    #   → auth_router → trailing_slash → account → acl → frontend_hmac
    #   → [audit_log] → request_context → tracing → metrics → auth_probe
    #   → acl_subresource → routers
    #
    # Load-bearing orderings:
    # - auth_probe and acl_subresource MUST stay inner to auth_router + acl.
    #   Moving either outward lets unauthenticated callers reach them: the
    #   probe would answer 200 to anyone, and the ?acl handlers would run
    #   without a permission check.
    # - request_context MUST stay inner to acl (it consumes the
    #   bucket_owner_id acl resolves) and outer to everything that reads
    #   request.state.account with bucket-owner semantics.
    # - fs_cache_pressure sits OUTSIDE auth on purpose: a shed PUT costs a
    #   503 and no SigV4/Arion work. Pre-merge the gateway defeated this by
    #   relaying the whole body before the api could answer.
    app.middleware("http")(acl_subresource_middleware)
    app.middleware("http")(auth_probe_middleware)
    app.middleware("http")(metrics_middleware)
    app.middleware("http")(tracing_middleware)
    app.middleware("http")(request_context_middleware)
    # Outer to request_context on purpose: the audit log attributes operations to the
    # CALLER (request.state.account as the account middleware built it); request_context
    # then rewrites state.account with bucket-owner semantics for the handlers/metrics.
    if config.enable_audit_logging:
        app.middleware("http")(audit_log_middleware)
    app.middleware("http")(verify_frontend_hmac_middleware)
    app.middleware("http")(acl_middleware)
    app.middleware("http")(account_middleware)
    app.middleware("http")(trailing_slash_normalizer)
    app.middleware("http")(auth_router_middleware)
    app.middleware("http")(input_validation_middleware)
    app.middleware("http")(fs_cache_pressure_middleware)
    if config.read_only_mode:
        app.middleware("http")(read_only_middleware)
    # Inside CORS so Cache-Control lands before CORS wraps the response.
    app.middleware("http")(cache_invalidation_middleware)
    app.middleware("http")(ats_purge_middleware)
    app.middleware("http")(cache_control_middleware)
    # One path view for router and every security layer — set once, before anything
    # reads scope["path"]. Replaces the collapse httpx used to apply in the forwarder;
    # without it the router acts on the raw path while auth/acl judge the collapsed one.
    app.middleware("http")(path_normalization_middleware)
    # Second-outermost: stamp ray_id before auth/acl/account/validation run, so
    # every inner middleware logs a real ray_id (GW-2). Kept inside CORS so CORS
    # still wraps error responses, including the ray_id header.
    app.middleware("http")(ray_id_middleware)
    # Outermost: CORS must wrap everything so error responses get CORS headers
    app.middleware("http")(cors_middleware)
    if config.enable_request_profiling:
        app.add_middleware(SpeedscopeProfilerMiddleware)

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> Response:
        # The full read-path mapping (not-ready → 503, pool saturation → 503, key/crypto → 503/500,
        # unsupported storage/suite → 501) is a testable pure function in errors.py. A recognized
        # failure returns a well-formed S3 error; anything else re-raises to uvicorn's 500.
        mapped = s3_errors.map_read_path_exception(exc)
        if mapped is not None:
            return mapped
        raise exc

    @app.get("/robots.txt", include_in_schema=False)
    async def robots_txt() -> Response:
        """Serve robots.txt to prevent crawler indexing."""
        content = """User-agent: *
Disallow: /

# Explicitly disallow common crawlers
User-agent: Googlebot
Disallow: /

User-agent: Bingbot
Disallow: /

User-agent: Slurp
Disallow: /

User-agent: DuckDuckBot
Disallow: /

User-agent: Baiduspider
Disallow: /

User-agent: YandexBot
Disallow: /

User-agent: facebookexternalhit
Disallow: /

User-agent: Twitterbot
Disallow: /"""
        return Response(
            content=content,
            media_type="text/plain",
        )

    @app.get("/health", include_in_schema=False, response_class=JSONResponse)
    async def health() -> JSONResponse:
        """Health check endpoint for monitoring."""
        return JSONResponse(content={"status": "healthy"})

    app.include_router(user_router, prefix="/user")
    app.include_router(sub_token_scopes_router, prefix="/user/sub-tokens")
    app.include_router(public_router, prefix="")
    # Mounted only when serving is on AND a secret exists: an absent route is the only honest
    # representation of "peer serving is off", since a mounted one is reachable from the public
    # internet (the gateway proxies arbitrary paths and anonymous GET is a valid auth outcome).
    # MUST stay ahead of s3_router_new — Starlette matches in registration order, and the S3
    # catch-all /{bucket}/{key:path} would otherwise swallow this path as a GetObject on a
    # bucket named `internal`. Pinned by tests/unit/test_internal_route_precedence.py.
    if config.peer_serve_enabled and config.internal_peer_secret:
        app.include_router(internal_parts_router, prefix="")
    app.include_router(s3_router_new, prefix="")

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app
