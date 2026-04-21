from __future__ import annotations

import time
from typing import Iterable
from typing import Optional

import psycopg  # type: ignore[import-untyped]
import redis  # type: ignore[import-untyped]


def get_object_id_and_version(
    bucket_name: str, object_key: str, *, dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius"
) -> tuple[str, int]:
    """Fetch (object_id, current_object_version) for a (bucket_name, object_key)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT o.object_id, o.current_object_version
                FROM objects o
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                ORDER BY o.created_at DESC
                LIMIT 1
                """,
            (bucket_name, object_key),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("object_not_found")
        return str(row[0]), int(row[1] or 1)


def get_object_id(
    bucket_name: str, object_key: str, *, dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius"
) -> str:
    """Back-compat helper: return only object_id."""
    oid, _ver = get_object_id_and_version(bucket_name, object_key, dsn=dsn)
    return oid


def get_object_cids(
    bucket_name: str,
    object_key: str,
    *,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> tuple[str, int, str, list[str], Optional[str]]:
    """Get all CIDs for an object including parts, chunks, and object-level CID.

    Returns: (object_id, object_version, main_account_id, part_cids, object_cid)
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.object_id, o.current_object_version, b.main_account_id
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.bucket_name = %s AND o.object_key = %s
            """,
            (bucket_name, object_key),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("object_not_found")
        object_id, object_version, main_account_id = str(row[0]), int(row[1] or 1), str(row[2])

        cur.execute(
            """
            SELECT COALESCE(c.cid, p.ipfs_cid) as cid
            FROM parts p
            LEFT JOIN cids c ON p.cid_id = c.id
            WHERE p.object_id = %s AND p.object_version = %s
            AND COALESCE(c.cid, p.ipfs_cid) IS NOT NULL
            AND COALESCE(c.cid, p.ipfs_cid) != 'pending'
            ORDER BY p.part_number
            """,
            (object_id, object_version),
        )
        part_cids = [str(row[0]) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT pc.cid
            FROM parts p
            JOIN part_chunks pc ON pc.part_id = p.part_id
            WHERE p.object_id = %s AND p.object_version = %s
            AND pc.cid IS NOT NULL
            AND pc.cid != 'pending'
            ORDER BY p.part_number, pc.chunk_index
            """,
            (object_id, object_version),
        )
        chunk_cids = [str(row[0]) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT COALESCE(c.cid, ov.ipfs_cid) as cid
            FROM object_versions ov
            LEFT JOIN cids c ON ov.cid_id = c.id
            WHERE ov.object_id = %s AND ov.object_version = %s
            AND COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
            AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'
            """,
            (object_id, object_version),
        )
        obj_cid_row = cur.fetchone()
        object_cid = str(obj_cid_row[0]) if obj_cid_row else None

        return object_id, object_version, main_account_id, part_cids + chunk_cids, object_cid


essentially_all_parts = range(0, 256)


def clear_object_cache(
    object_id: str,
    parts: Iterable[int] | None = None,
    *,
    redis_url: str = "redis://localhost:6379/0",
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> None:
    """Clear FS cache entries for `object_id` so the next GET is a cache miss.

    After the FS-cache migration, chunks live on the filesystem (not Redis).
    This helper walks the known cache mount points and removes the
    object's part tree. It also best-effort clears any leftover Redis keys
    for the object in case an older deploy populated them — safe if empty.
    """
    import shutil
    from pathlib import Path

    import psycopg

    # Resolve current object_version (used to namespace part directories).
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.current_object_version
            FROM objects o
            WHERE o.object_id = %s
            LIMIT 1
            """,
            (object_id,),
        )
        row = cur.fetchone()
        object_version = int(row[0]) if row and row[0] is not None else 1

    # Wipe per-part directories from the FS cache. Both legacy (CephFS)
    # and local NVMe mount points are covered to keep the helper robust
    # across dev / staging / prod layouts.
    if parts is None:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT part_number
                FROM parts
                WHERE object_id = %s
                ORDER BY part_number
                """,
                (object_id,),
            )
            parts_list = [row[0] for row in cur.fetchall()]
            if 1 not in parts_list:
                parts_list.insert(0, 1)
            parts = parts_list

    # Delete the part directories from INSIDE the API container first. The
    # files there are owned by the container user and a host-side `rmtree`
    # on a bind-mount usually fails silently on the CI runner (different UID),
    # which silently leaves the cache populated and breaks follow-up
    # "source == pipeline" assertions.
    from .compose import compose_exec

    for pn in parts:
        container_part_dir = f"/var/lib/hippius/object_cache/{object_id}/v{object_version}/part_{int(pn)}"
        try:
            compose_exec("api", ["rm", "-rf", container_part_dir])
        except Exception:  # docker-compose not available (pure-host run); fall through to host rmtree
            pass
        # Also try the NVMe mount path (production / CDN regions)
        container_nvme_part_dir = f"/var/lib/hippius/local_object_cache/{object_id}/v{object_version}/part_{int(pn)}"
        try:
            compose_exec("api", ["rm", "-rf", container_nvme_part_dir])
        except Exception:
            pass

    # Belt-and-suspenders: also remove from the host. Harmless if the path
    # doesn't exist or the rmtree can't delete (ignore_errors=True).
    for cache_dir in ("/var/lib/hippius/object_cache", "/var/lib/hippius/local_object_cache"):
        obj_dir = Path(cache_dir) / object_id
        if not obj_dir.exists():
            continue
        for pn in parts:
            part_dir = obj_dir / f"v{object_version}" / f"part_{int(pn)}"
            if part_dir.exists():
                shutil.rmtree(part_dir, ignore_errors=True)

    # Best-effort: remove the download-in-progress coalescing locks so a
    # subsequent miss re-enqueues instead of waiting for a phantom worker.
    try:
        r = redis.Redis.from_url(redis_url)
        for pn in parts:
            r.delete(f"download_in_progress:{object_id}:v:{object_version}:part:{int(pn)}")
    except Exception:
        pass


def read_part_from_cache(
    object_id: str,
    part_number: int,
    *,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> Optional[bytes]:
    """Assemble part bytes from the FS cache if fully present; otherwise None.

    Walks the known FS cache mount points (local NVMe primary, CephFS
    fallback) and stitches chunks using meta.json.
    """
    import json as _json
    from pathlib import Path

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.current_object_version
            FROM objects o
            WHERE o.object_id = %s
            LIMIT 1
            """,
            (object_id,),
        )
        row = cur.fetchone()
        object_version = int(row[0]) if row and row[0] is not None else 1

    for cache_dir in ("/var/lib/hippius/local_object_cache", "/var/lib/hippius/object_cache"):
        part_dir = Path(cache_dir) / object_id / f"v{object_version}" / f"part_{int(part_number)}"
        meta_path = part_dir / "meta.json"
        if not meta_path.exists():
            continue
        try:
            meta = _json.loads(meta_path.read_text())
        except Exception:
            continue
        num_chunks = int(meta.get("num_chunks", 0))
        if num_chunks <= 0:
            return b""
        chunks: list[bytes] = []
        missing = False
        for i in range(num_chunks):
            chunk_path = part_dir / f"chunk_{i}.bin"
            if not chunk_path.exists():
                missing = True
                break
            chunks.append(chunk_path.read_bytes())
        if missing:
            continue
        return b"".join(chunks)
    return None


def wait_for_parts_cids(
    bucket_name: str,
    object_key: str,
    *,
    min_count: int,
    backend: str = "arion",
    timeout_seconds: float = 20.0,
    deadline: float | None = None,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> bool:
    """Wait until at least min_count parts for the object have backend identifiers in chunk_backend.

    In the multi-backend design, upload workers only write to chunk_backend. This helper
    therefore treats an object as "pipeline-ready" when enough parts have *all* their
    chunk_backend rows present for the given backend.

    If *deadline* is given it takes precedence over *timeout_seconds*.

    Returns True if ready within timeout, False otherwise.
    """
    print(
        f"DEBUG: wait_for_parts_cids called for {bucket_name}/{object_key}, "
        f"backend={backend} expecting min_count={min_count}"
    )
    if deadline is None:
        deadline = time.time() + timeout_seconds
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        while time.time() < deadline:
            cur.execute(
                """
                WITH chunks AS (
                    SELECT
                        o.object_id,
                        o.current_object_version AS object_version,
                        p.part_number,
                        pc.id AS chunk_id
                    FROM objects o
                    JOIN buckets b ON b.bucket_id = o.bucket_id
                    JOIN parts p ON p.object_id = o.object_id AND p.object_version = o.current_object_version
                    JOIN part_chunks pc ON pc.part_id = p.part_id
                    WHERE b.bucket_name = %s
                      AND o.object_key = %s
                      AND o.deleted_at IS NULL
                ),
                per_part AS (
                    SELECT
                        c.part_number,
                        COUNT(DISTINCT c.chunk_id) AS total_chunks,
                        COUNT(DISTINCT c.chunk_id) FILTER (
                            WHERE cb.backend = %s
                              AND NOT cb.deleted
                              AND cb.backend_identifier IS NOT NULL
                        ) AS backend_chunks
                    FROM chunks c
                    LEFT JOIN chunk_backend cb ON cb.chunk_id = c.chunk_id
                    GROUP BY c.part_number
                )
                SELECT COUNT(*)
                FROM per_part
                WHERE total_chunks > 0
                  AND backend_chunks = total_chunks
                """,
                (bucket_name, object_key, backend),
            )
            row = cur.fetchone()
            count = int(row[0]) if row else 0
            print(f"DEBUG: Fully-backed parts count: {count} (need {min_count})")
            if count >= min_count:
                print(f"DEBUG: wait_for_parts_cids SUCCESS - found {count} fully-backed parts")
                return True
            print(f"DEBUG: wait_for_parts_cids still waiting... ({count}/{min_count})")
            time.sleep(0.3)
    print(f"DEBUG: wait_for_parts_cids TIMEOUT - only found {count} non-pending parts after {timeout_seconds}s")
    return False


def wait_for_all_backends_ready(
    bucket_name: str,
    object_key: str,
    *,
    min_count: int,
    backends: list[str] | None = None,
    timeout_seconds: float = 30.0,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> bool:
    """Wait until min_count parts are fully backed on ALL specified backends.

    Uses a single shared deadline so total wait is at most *timeout_seconds*.
    """
    if backends is None:
        backends = ["arion"]
    shared_deadline = time.time() + timeout_seconds
    for backend in backends:
        if not wait_for_parts_cids(
            bucket_name,
            object_key,
            min_count=min_count,
            backend=backend,
            deadline=shared_deadline,
            dsn=dsn,
        ):
            return False
    return True


def make_all_object_parts_pending(
    bucket_name: str,
    object_key: str,
    *,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> str:
    """Simulate "not yet processed by workers" by clearing backend registrations.

    Returns the object_id of the affected object.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        # First get the object_id
        cur.execute(
            """
            SELECT o.object_id
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.bucket_name = %s AND o.object_key = %s
            """,
            (bucket_name, object_key),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Object {bucket_name}/{object_key} not found")
        object_id = str(row[0])

        # Mark all backend registrations as deleted so pipeline hydration cannot use them.
        # This models a state where the content is still readable from cache, but workers
        # have not yet registered backend identifiers.
        cur.execute(
            """
            UPDATE chunk_backend cb
               SET deleted = true,
                   deleted_at = now()
              FROM part_chunks pc
              JOIN parts p ON p.part_id = pc.part_id
             WHERE cb.chunk_id = pc.id
               AND p.object_id = %s
               AND NOT cb.deleted
            """,
            (object_id,),
        )

        conn.commit()

        return object_id
