#!/usr/bin/env python3
"""Manifest builder worker: builds and publishes object manifests to IPFS after append operations settle.

This worker runs with eventual consistency (~10 min lag) to ensure manifests are published only
after all parts are uploaded and available. It scans for objects that haven't had manifests built
recently and builds them asynchronously.
"""

import asyncio
import dataclasses
import json
import logging
from typing import Any

import asyncpg
import redis.asyncio as redis

from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService


logger = logging.getLogger(__name__)
config = get_config()


@dataclasses.dataclass
class ManifestBuildRequest:
    """Request to build a manifest for an object."""

    object_id: str
    append_version: int


async def scan_for_manifest_candidates(pool: Any) -> list[ManifestBuildRequest]:
    """Scan for objects that need manifest building.

    Returns objects where:
    - last_append_at is > 10 minutes old (configurable stabilization window)
    - append_version > manifest_built_for_version OR manifest_built_for_version IS NULL
    """
    stabilization_window_sec = getattr(config, "manifest_stabilization_window_sec", 600)  # 10 min default
    async with pool.acquire() as db:
        rows = await db.fetch(
            """
        SELECT object_id, append_version
        FROM objects
        WHERE last_append_at < NOW() - make_interval(secs => $1)
          AND (manifest_built_for_version IS NULL OR append_version > manifest_built_for_version)
        LIMIT 100  -- Batch size
        """,
            stabilization_window_sec,
        )
    return [
        ManifestBuildRequest(object_id=str(row["object_id"]), append_version=int(row["append_version"])) for row in rows
    ]


async def build_and_publish_manifest(
    pool: Any, ipfs_service: IPFSService, request: ManifestBuildRequest, redis_client: Any
) -> bool:
    """Build and publish manifest for an object. Returns True on success."""

    # Check if we're already building this manifest (dedupe)
    building_key = f"manifest:building:{request.object_id}:{request.append_version}"
    set_result = await redis_client.set(building_key, "1", nx=True, ex=300)  # 5 min timeout
    if not set_result:
        logger.debug(f"Manifest already building for {request.object_id}:{request.append_version}")
        return True  # Consider this a success to avoid retries

    try:
        # Verify object still exists and version matches
        async with pool.acquire() as db:
            object_row = await db.fetchrow(
                """
            SELECT o.*, b.bucket_name
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE o.object_id = $1
            """,
                request.object_id,
            )
        if not object_row:
            logger.warning(f"Object {request.object_id} no longer exists")
            return True

        current_version = int(object_row["append_version"] or 0)
        if current_version > request.append_version:
            logger.info(f"Object {request.object_id} version advanced to {current_version}, upgrading build request")
            request.append_version = current_version  # Upgrade to current version
        elif current_version < request.append_version:
            logger.warning(
                f"Object {request.object_id} version regressed from {request.append_version} to {current_version}"
            )
            return False  # Wait for stabilization

        # Fetch all parts for this object
        async with pool.acquire() as db:
            parts_rows = await db.fetch(
                """
                SELECT part_number, ipfs_cid, size_bytes, etag
                FROM parts
                WHERE object_id = $1
                ORDER BY part_number
                """,
                request.object_id,
            )

        # Verify all parts have non-pending CIDs
        if not parts_rows:
            logger.info(f"Object {request.object_id} has no parts yet, requeueing after stabilization window")
            return False

        incomplete_parts = [p for p in parts_rows if not p["ipfs_cid"] or p["ipfs_cid"] == "pending"]
        if incomplete_parts:
            logger.info(f"Object {request.object_id} has {len(incomplete_parts)} incomplete parts, requeueing")
            return False  # Requeue with backoff

        # Calculate offsets and verify total size
        parts = []
        total_size = 0
        offset = 0

        for part_row in parts_rows:
            part_number = int(part_row["part_number"])
            cid = str(part_row["ipfs_cid"])
            size_bytes = int(part_row["size_bytes"])
            etag = str(part_row["etag"] or "")

            parts.append(
                {"part_number": part_number, "cid": cid, "size_bytes": size_bytes, "offset": offset, "etag": etag}
            )

            offset += size_bytes
            total_size += size_bytes

        # Verify total size matches object size
        object_size = int(object_row["size_bytes"] or 0)
        if total_size != object_size:
            logger.error(f"Size mismatch for {request.object_id}: parts={total_size}, object={object_size}")
            return False

        # Build manifest JSON
        manifest = {
            "object_id": str(request.object_id),
            "object_key": str(object_row["object_key"]),
            "bucket_name": str(object_row["bucket_name"]),
            "total_size": total_size,
            "append_version": request.append_version,
            "parts": parts,
            "etag": str(object_row["md5_hash"] or ""),
            "content_type": str(object_row["content_type"] or "application/octet-stream"),
            "created_at": str(object_row["created_at"]),
            "metadata": object_row.get("metadata") or {},
        }

        manifest_data = json.dumps(manifest, separators=(",", ":")).encode()

        # Publish manifest to IPFS; toggle chain publish based on config (env-backed)

        result = await ipfs_service.client.s3_publish(
            content=manifest_data,
            encrypt=False,
            seed_phrase=config.resubmission_seed_phrase,
            subaccount_id=config.resubmission_seed_phrase,
            bucket_name=str(object_row["bucket_name"]),
            file_name=f"{object_row['object_key']}_manifest_v{request.append_version}.json",
            store_node=config.ipfs_store_url,
            pin_node=config.ipfs_store_url,
            substrate_url=config.substrate_url,
            publish=config.publish_to_chain,
        )
        manifest_cid = result.cid

        # Update object with manifest info
        async with pool.acquire() as db:
            await db.execute(
                """
                UPDATE objects
                SET manifest_cid = $1,
                    manifest_built_for_version = $2,
                    manifest_built_at = NOW()
                WHERE object_id = $3 AND append_version = $4
                """,
                manifest_cid,
                request.append_version,
                request.object_id,
                request.append_version,
            )

        logger.info(f"Published manifest for {request.object_id}:{request.append_version}, CID: {manifest_cid}")
        return True

    except Exception:
        logger.exception(f"Failed to build manifest for {request.object_id}:{request.append_version}")
        return False
    finally:
        # Clean up building flag
        await redis_client.delete(building_key)


async def run_manifest_builder_loop(pool: Any, ipfs_service: IPFSService, redis_client: Any):
    """Main manifest builder loop."""
    scan_interval_sec = getattr(config, "manifest_scan_interval_sec", 120)  # 2 min default
    max_concurrency = getattr(config, "manifest_builder_max_concurrency", 5)

    logger.info(f"Starting manifest builder: scan_interval={scan_interval_sec}s, max_concurrency={max_concurrency}")

    while True:
        try:
            # Scan for candidates
            candidates = await scan_for_manifest_candidates(pool)
            if candidates:
                logger.info(f"Found {len(candidates)} manifest candidates")

                # Process in batches with concurrency limit
                semaphore = asyncio.Semaphore(max_concurrency)

                async def process_with_semaphore(candidate: ManifestBuildRequest, semaphore=semaphore):
                    async with semaphore:
                        success = await build_and_publish_manifest(pool, ipfs_service, candidate, redis_client)
                        if not success:
                            # Requeue with backoff (simplified: just wait for next scan)
                            logger.debug(
                                f"Requeueing manifest build for {candidate.object_id}:{candidate.append_version}"
                            )

                tasks = [process_with_semaphore(candidate) for candidate in candidates]
                await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.sleep(scan_interval_sec)

        except Exception:
            logger.exception("Error in manifest builder loop")
            await asyncio.sleep(60)  # Backoff on errors


async def main():
    """Main entry point."""

    # Database connection
    db_url = config.database_url
    pool = await asyncpg.create_pool(
        db_url, min_size=1, max_size=max(2, getattr(config, "manifest_builder_max_concurrency", 5))
    )

    # Redis connection
    redis_client = redis.from_url(config.redis_url)

    # IPFS service
    ipfs_service = IPFSService(config, redis_client)

    try:
        await run_manifest_builder_loop(pool, ipfs_service, redis_client)
    finally:
        await pool.close()
        await redis_client.close()


if __name__ == "__main__":
    import dataclasses  # For dataclass support

    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
