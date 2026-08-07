from __future__ import annotations


# First path segments that never reach the S3 forwarder as a bucket. A bucket created under one of
# these is unreachable by its owner yet permanently holds the globally-unique name — prod incident
# 2026-08-03, buckets "docs" and "docs2".
#
# Three independent sources, and ALL must be reflected here:
#   1. A real gateway route swallows the request:
#        /health                                    (gateway/main.py)
#        /docs, /redoc, /openapi.json, /docs/cache  (gateway/routers/docs.py)
#   2. auth_router exempts the segment from SigV4, so no identity is stamped and the row lands
#      with an empty main_account:
#        + /user, /robots.txt, /metrics             (gateway/middlewares/auth_router.py)
#   3. A non-S3 route on the INTERNAL API shadows the S3 catch-all:
#        + /public, /internal                       (hippius_s3/main.py include_router order)
#
# Note that (2) is the larger set: /user, /robots.txt and /metrics are NOT routes — the gateway
# has no handler for any of them and forwards them to the internal API — they are dangerous
# purely because authentication is skipped.
#
# (3) is the one that is easy to miss, because nothing about it is gateway-side. The gateway
# forwards every path it does not serve itself, so the api's routing table strands bucket names
# just as effectively as the gateway's. Starlette matches in REGISTRATION ORDER, so only what
# hippius_s3/main.py mounts ahead of s3_router_new can shadow /{bucket_name}/{key:path} — which
# is why /static is absent below despite being a mount: it is registered after s3_router_new and
# so is shadowed BY S3, not shadowing it. The pre-existing guard walks auth_router's exempt set
# and therefore cannot see this class at all; tests/unit/test_api_routes_are_reserved_names.py
# walks the api's real route table instead.
#
# Matched EXACTLY, never by prefix. auth_router was the only place that ever matched route names
# with startswith(), and it no longer does, so `docs2` is an ordinary bucket name. Banning every
# name merely starting with one of these would reject `user-uploads`, `metrics-prod` and
# `static-assets` for no security benefit.
#
# This module is the single definition. It lives under hippius_s3/ rather than gateway/ because
# only hippius_s3 is packaged into the wheel, and hippius_s3/scripts consumes it too.
#
# Consumers, all of which must keep agreeing:
#   - gateway/middlewares/input_validation.py  rejects CreateBucket on these names
#   - gateway/middlewares/auth_router.py       its exempt segments must be a SUBSET of this
#   - hippius_s3/main.py                       every segment it routes ahead of s3_router_new
#   - hippius_s3/scripts/report_reserved_name_buckets.py   audits for existing collisions
RESERVED_BUCKET_SEGMENTS = frozenset(
    {
        "docs",
        "health",
        "internal",
        "metrics",
        "openapi.json",
        "public",
        "redoc",
        "robots.txt",
        "user",
    }
)

# acl_middleware returns before any permission check for these — see the
# `path == "/health" or path.startswith("/user/")` guard at the top of gateway/middlewares/acl.py.
# A bucket on one of these names therefore has neither authentication NOR authorization in front
# of its objects, which is strictly worse than the merely-stranded case above.
ACL_BYPASSED_SEGMENTS = frozenset({"health", "user"})
