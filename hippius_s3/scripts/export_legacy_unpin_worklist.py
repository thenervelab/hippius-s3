from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Optional
from typing import TextIO

import asyncpg  # type: ignore[import-untyped]

from hippius_s3.config import get_config


@dataclass(frozen=True)
class UnpinWorkItem:
    address: str
    object_id: str
    object_version: int
    cid: str
    bucket_name: str
    object_key: str
    cid_kind: str
    part_number: Optional[int] = None
    chunk_index: Optional[int] = None

    def to_unpin_chain_request(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "object_id": self.object_id,
            "object_version": int(self.object_version),
            "cid": self.cid,
        }

    def to_debug_dict(self) -> dict[str, Any]:
        return {
            **self.to_unpin_chain_request(),
            "bucket_name": self.bucket_name,
            "object_key": self.object_key,
            "cid_kind": self.cid_kind,
            "part_number": self.part_number,
            "chunk_index": self.chunk_index,
        }


def _norm_cid(cid: str | None) -> str:
    c = str(cid or "").strip()
    if c.lower() in {"", "none", "pending"}:
        return ""
    return c


async def main_async(args: argparse.Namespace, *, out_file: TextIO, output_jsonl: str) -> int:
    cfg = get_config()
    db = await asyncpg.connect(cfg.database_url)  # type: ignore[arg-type]
    try:
        max_sv = int(args.max_storage_version)
        include_current = bool(args.include_current)
        bucket = args.bucket or None

        # IMPORTANT: We intentionally dedupe in SQL (GROUP BY main_account_id, cid) to avoid
        # unbounded Python memory usage on large datasets.
        query = """
            WITH target_versions AS (
              SELECT
                b.main_account_id,
                b.bucket_name,
                o.object_key,
                ov.object_id,
                ov.object_version,
                ov.storage_version,
                COALESCE(c.cid, ov.ipfs_cid) AS object_level_cid
              FROM object_versions ov
              JOIN objects o ON o.object_id = ov.object_id
              JOIN buckets b ON b.bucket_id = o.bucket_id
              LEFT JOIN cids c ON c.id = ov.cid_id
              WHERE ov.storage_version <= $1
                AND ($2::text IS NULL OR b.bucket_name = $2)
                AND ($3::bool OR ov.object_version <> o.current_object_version)
            ),
            object_cids AS (
              SELECT
                main_account_id,
                bucket_name,
                object_key,
                object_id,
                object_version,
                'object_version'::text AS cid_kind,
                NULL::int AS part_number,
                NULL::int AS chunk_index,
                object_level_cid AS cid
              FROM target_versions
            ),
            part_cids AS (
              SELECT
                tv.main_account_id,
                tv.bucket_name,
                tv.object_key,
                tv.object_id,
                tv.object_version,
                'part'::text AS cid_kind,
                p.part_number::int AS part_number,
                NULL::int AS chunk_index,
                p.ipfs_cid AS cid
              FROM target_versions tv
              JOIN parts p
                ON p.object_id = tv.object_id
               AND p.object_version = tv.object_version
            ),
            chunk_cids AS (
              SELECT
                tv.main_account_id,
                tv.bucket_name,
                tv.object_key,
                tv.object_id,
                tv.object_version,
                'chunk'::text AS cid_kind,
                p.part_number::int AS part_number,
                pc.chunk_index::int AS chunk_index,
                pc.cid AS cid
              FROM target_versions tv
              JOIN parts p
                ON p.object_id = tv.object_id
               AND p.object_version = tv.object_version
              JOIN part_chunks pc
                ON pc.part_id = p.part_id
            ),
            all_cids AS (
              SELECT *
              FROM (
              SELECT * FROM object_cids
              UNION ALL
              SELECT * FROM part_cids
              UNION ALL
              SELECT * FROM chunk_cids
              ) u
              WHERE cid IS NOT NULL
                AND LOWER(TRIM(cid)) NOT IN ('', 'none', 'pending')
            )
            SELECT
              main_account_id,
              cid,
              MIN(bucket_name) AS bucket_name,
              MIN(object_key) AS object_key,
              MIN(object_id)::uuid AS object_id,
              MIN(object_version)::bigint AS object_version,
              CASE
                WHEN COUNT(DISTINCT cid_kind) > 1 THEN 'mixed'
                ELSE MIN(cid_kind)
              END AS cid_kind,
              MIN(part_number) AS part_number,
              MIN(chunk_index) AS chunk_index
            FROM all_cids
            GROUP BY main_account_id, cid
            """

        # Stream rows to avoid loading huge datasets into memory.
        selected_cid_rows = 0
        skipped_no_cid = 0
        unpin_items = 0
        unpin_items_deduped = 0

        limit = int(args.limit or 0)

        async with db.transaction():
            async for r in db.cursor(query, max_sv, bucket, include_current):
                selected_cid_rows += 1
                if limit > 0 and selected_cid_rows > limit:
                    break

                cid = _norm_cid(r["cid"])
                if not cid:
                    skipped_no_cid += 1
                    continue

                unpin_items += 1
                unpin_items_deduped += 1

                it = UnpinWorkItem(
                    address=str(r["main_account_id"]),
                    bucket_name=str(r["bucket_name"]),
                    object_key=str(r["object_key"]),
                    object_id=str(r["object_id"]),
                    object_version=int(r["object_version"]),
                    cid=cid,
                    cid_kind=str(r["cid_kind"]),
                    part_number=(int(r["part_number"]) if r["part_number"] is not None else None),
                    chunk_index=(int(r["chunk_index"]) if r["chunk_index"] is not None else None),
                )
                out_file.write(json.dumps(it.to_debug_dict(), sort_keys=True) + "\n")

        print(
            json.dumps(
                {
                    "selected_cid_rows": selected_cid_rows,
                    "skipped_no_cid": skipped_no_cid,
                    "unpin_items": unpin_items,
                    "unpin_items_deduped_by_address_and_cid": unpin_items_deduped,
                    "output_jsonl": output_jsonl,
                    "max_storage_version": max_sv,
                    "include_current": include_current,
                    "bucket_filter": args.bucket or "",
                    "included_cid_kinds": ["object_version", "part", "chunk"],
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
        description=(
            "Export a JSONL receipt of IPFS CIDs to unpin for legacy (storage_version<=N) object versions.\n\n"
            "This always exports ALL known CID sources for each selected object version:\n"
            "- object_versions.cid_id / object_versions.ipfs_cid\n"
            "- parts.ipfs_cid\n"
            "- part_chunks.cid\n\n"
            "This lets you delete DB rows immediately while retaining an unpin worklist for later."
        )
    )
    ap.add_argument(
        "--max-storage-version",
        type=int,
        default=3,
        help="Include object_versions with storage_version <= this value (default: 3, i.e. non-v4).",
    )
    ap.add_argument("--bucket", default="", help="Optional bucket_name filter")
    ap.add_argument(
        "--include-current",
        action="store_true",
        help="Also include current versions (dangerous). Default: only non-current versions.",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max CID rows to scan (0 = no limit). Note: this counts rows across object/part/chunk CID sources.",
    )
    ap.add_argument(
        "--output-jsonl",
        default="",
        help="Write JSONL receipt (each line includes address/object_id/object_version/cid + bucket/key + cid_kind).",
    )
    args = ap.parse_args()
    output_jsonl = str(args.output_jsonl or "").strip()
    if not output_jsonl:
        raise SystemExit("--output-jsonl is required (this script is meant to produce an auditable receipt).")
    out_path = Path(output_jsonl)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        rc = asyncio.run(main_async(args, out_file=f, output_jsonl=output_jsonl))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
