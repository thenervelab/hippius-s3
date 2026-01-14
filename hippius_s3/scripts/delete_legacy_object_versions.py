from __future__ import annotations

import argparse
import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass

import asyncpg  # type: ignore[import-untyped]

from hippius_s3.config import get_config


@dataclass(frozen=True)
class DeleteCandidate:
    object_id: str
    object_version: int
    bucket_name: str
    object_key: str
    storage_version: int
    is_current: bool


async def main_async(args: argparse.Namespace) -> int:
    cfg = get_config()
    db = await asyncpg.connect(cfg.database_url)  # type: ignore[arg-type]
    try:
        max_sv = int(args.max_storage_version)
        include_current = bool(args.include_current)
        include_objects_without_v4 = bool(args.include_objects_without_v4)
        bucket = args.bucket or None

        # Build deletion candidates:
        # - legacy object_versions (storage_version <= max_sv)
        # - by default, only for objects that already have some v4+ version (safe: keeps the object usable)
        #
        # IMPORTANT:
        # - For objects with NO v4+ versions, deleting their only legacy version directly will usually fail
        #   because objects.current_object_version references object_versions (RESTRICT).
        #   To delete "legacy-only objects", we delete the *object row* instead (cascades to versions/parts).
        rows = await db.fetch(
            """
            WITH obj_flags AS (
              SELECT
                object_id,
                BOOL_OR(storage_version >= 4) AS has_v4
              FROM object_versions
              GROUP BY object_id
            )
            SELECT
              b.bucket_name,
              o.object_key,
              ov.object_id,
              ov.object_version,
              ov.storage_version,
              (ov.object_version = o.current_object_version) AS is_current,
              f.has_v4
            FROM object_versions ov
            JOIN objects o ON o.object_id = ov.object_id
            JOIN buckets b ON b.bucket_id = o.bucket_id
            JOIN obj_flags f ON f.object_id = ov.object_id
            WHERE ov.storage_version <= $1
              AND ($2::text IS NULL OR b.bucket_name = $2)
              AND ($3::bool OR ov.object_version <> o.current_object_version)
              AND ($4::bool OR f.has_v4)
            ORDER BY b.bucket_name, o.object_key, ov.object_id, ov.object_version
            LIMIT COALESCE($5::int, 1000000)
            """,
            max_sv,
            bucket,
            include_current,
            include_objects_without_v4,
            (args.limit if args.limit and args.limit > 0 else None),
        )

        # Identify legacy-only objects (no v4+ versions). If include_objects_without_v4 is set,
        # we will delete these objects (not individual versions) to bypass the current-version FK.
        legacy_only_object_ids: set[str] = set()
        if include_objects_without_v4:
            legacy_only_object_ids = {str(r["object_id"]) for r in rows if not bool(r["has_v4"])}

        candidates: list[DeleteCandidate] = [
            DeleteCandidate(
                object_id=str(r["object_id"]),
                object_version=int(r["object_version"]),
                bucket_name=str(r["bucket_name"]),
                object_key=str(r["object_key"]),
                storage_version=int(r["storage_version"]),
                is_current=bool(r["is_current"]),
            )
            for r in rows
        ]

        if args.dry_run:
            print(
                json.dumps(
                    {
                        "candidates": len(candidates),
                        "legacy_only_objects": len(legacy_only_object_ids),
                        "max_storage_version": max_sv,
                        "include_current": include_current,
                        "include_objects_without_v4": include_objects_without_v4,
                        "bucket_filter": args.bucket or "",
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            # Show a few examples
            for c in candidates[:25]:
                print(
                    json.dumps(
                        {
                            "bucket": c.bucket_name,
                            "key": c.object_key,
                            "object_id": c.object_id,
                            "object_version": c.object_version,
                            "storage_version": c.storage_version,
                            "is_current": c.is_current,
                        },
                        sort_keys=True,
                    )
                )
            return 0

        if not args.yes:
            raise SystemExit("Refusing to delete without --yes (use --dry-run first)")

        deleted_versions = 0
        deleted_objects = 0

        # 1) If requested, delete legacy-only objects entirely (cascades to versions/parts).
        # This is the only reliable way to delete an object whose current version is legacy.
        if legacy_only_object_ids:
            for oid in sorted(legacy_only_object_ids):
                await db.execute("DELETE FROM objects WHERE object_id = $1::uuid", oid)
                deleted_objects += 1

        # Deleting object_versions will cascade-delete parts and part_chunks due to FKs:
        # - parts_object_version_fk ON DELETE CASCADE
        # - part_chunks_part_id_fkey ON DELETE CASCADE (via parts)
        #
        # NOTE: objects_current_version_fk is ON DELETE RESTRICT, so deleting current versions
        # requires first moving current_object_version (not handled here; use --include-current only knowingly).
        for c in candidates:
            # Skip versions for objects we deleted above.
            if c.object_id in legacy_only_object_ids:
                continue
            if c.is_current and not include_current:
                # Defensive: avoid surprising failures unless user explicitly asked.
                continue

            await db.execute(
                """
                DELETE FROM object_versions
                 WHERE object_id = $1::uuid
                   AND object_version = $2::bigint
                """,
                c.object_id,
                int(c.object_version),
            )
            deleted_versions += 1

        # Optionally: delete objects that now have zero versions.
        if args.delete_empty_objects:
            # Remove objects with no remaining versions (safe; versions table is authoritative).
            r2 = await db.execute(
                """
                DELETE FROM objects o
                WHERE NOT EXISTS (
                  SELECT 1 FROM object_versions ov WHERE ov.object_id = o.object_id
                )
                """
            )
            # asyncpg returns strings like "DELETE <n>"
            with suppress(Exception):
                deleted_objects += int(str(r2).split()[-1])

        print(
            json.dumps(
                {
                    "deleted_object_versions": deleted_versions,
                    "deleted_objects": deleted_objects,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    finally:
        await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Delete legacy (storage_version<=N) object_versions, optionally cleaning up now-empty objects."
    )
    ap.add_argument("--max-storage-version", type=int, default=3, help="Delete versions with storage_version <= N")
    ap.add_argument("--bucket", default="", help="Optional bucket_name filter")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to consider (0 = no limit)")
    ap.add_argument(
        "--include-current",
        action="store_true",
        help="Include current versions (dangerous; may be blocked by FK and break reads). Default: exclude.",
    )
    ap.add_argument(
        "--include-objects-without-v4",
        action="store_true",
        help=(
            "Also delete objects that have no v4+ versions by deleting the object row (cascades to all versions). "
            "Default: exclude."
        ),
    )
    ap.add_argument(
        "--delete-empty-objects",
        action="store_true",
        help="After deleting versions, also delete objects with zero remaining versions.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print counts/examples; do not delete")
    ap.add_argument("--yes", action="store_true", help="Required to actually delete")
    args = ap.parse_args()
    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
