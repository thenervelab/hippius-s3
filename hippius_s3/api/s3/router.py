from __future__ import annotations

from fastapi import APIRouter

from hippius_s3.api.s3.buckets.router import router as buckets_router
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.api.s3.objects.router import router as objects_router


# Top-level S3 router (scaffolding). Resource routers will be included here incrementally.
router = APIRouter(tags=["s3"])
router.include_router(buckets_router, prefix="")
router.include_router(objects_router, prefix="")
router.include_router(multipart_router, prefix="")
