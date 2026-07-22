from __future__ import annotations

import logging
from typing import Any
from typing import Iterable
from typing import List

from .db_meta import read_parts_plain_and_chunk_sizes_batch
from .types import ChunkPlanItem
from .types import RangeRequest


logger = logging.getLogger(__name__)

# A6: fallback chunk size when a part has bytes but a missing/zero `chunk_size_bytes` (DB
# inconsistency). MUST match the downloader's eager-meta fallback (`downloader.py`) so the plan and
# the written chunks agree on chunk count/boundaries.
_DEFAULT_CHUNK_SIZE_BYTES = 4 * 1024 * 1024


async def build_chunk_plan(
    db: Any,
    object_id: str,
    parts: Iterable[dict],
    rng: RangeRequest | None,
    *,
    object_version: int,
) -> List[ChunkPlanItem]:
    # Normalize and sort parts
    ordered = sorted(parts, key=lambda x: int(x.get("part_number", 0)))

    part_numbers = [int(p.get("part_number", 0)) for p in ordered]
    # RD-3: the GET path already carries size_bytes + chunk_size_bytes on each part (from the parts
    # catalog), so size the plan from them and skip the query. Fall back to the batch query when any
    # part lacks them — copy/UploadPartCopy callers and the envelope-race re-read pass thinner lists.
    if ordered and all("size_bytes" in p and int(p.get("chunk_size_bytes") or 0) > 0 for p in ordered):
        size_map = {int(p["part_number"]): (int(p.get("size_bytes") or 0), int(p["chunk_size_bytes"])) for p in ordered}
    else:
        size_map = await read_parts_plain_and_chunk_sizes_batch(db, object_id, part_numbers, int(object_version))

    sizes: list[tuple[int, int, int]] = []  # (part_number, plain_size, chunk_size)
    for pn in part_numbers:
        ps, cs = size_map.get(pn, (0, 0))
        sizes.append((pn, ps, cs))

    offsets: dict[int, int] = {}
    acc = 0
    for pn, ps, _ in sizes:
        offsets[int(pn)] = acc
        acc += int(ps)

    plan: List[ChunkPlanItem] = []
    for pn, plain_size, chunk_size in sizes:
        if plain_size <= 0:
            continue  # genuinely empty part — nothing to plan
        if chunk_size <= 0:
            # A6: size>0 but no chunk_size is a DB inconsistency. Dropping the part here would
            # truncate the response while Content-Length still counts its bytes. Mirror the
            # downloader's 4 MiB fallback so the bytes are planned; warn so the inconsistency shows.
            logger.warning(
                "planner: part %s v%s has plain_size=%d but chunk_size=%d; using %d-byte fallback",
                pn,
                object_version,
                plain_size,
                chunk_size,
                _DEFAULT_CHUNK_SIZE_BYTES,
            )
            chunk_size = _DEFAULT_CHUNK_SIZE_BYTES
        num_chunks = (plain_size + chunk_size - 1) // chunk_size
        part_offset = int(offsets.get(int(pn), 0))

        if rng is None:
            plan.extend(ChunkPlanItem(part_number=pn, chunk_index=ci) for ci in range(num_chunks))
            continue

        # range: compute intersection with this part
        part_start = part_offset
        part_end_incl = part_offset + plain_size - 1
        if rng.end < part_start or rng.start > part_end_incl:
            continue

        local_start = max(0, rng.start - part_start)
        local_end_incl = min(plain_size - 1, rng.end - part_start)
        start_chunk = local_start // chunk_size
        end_chunk = local_end_incl // chunk_size
        for ci in range(start_chunk, end_chunk + 1):
            chunk_pt_start = ci * chunk_size
            s_local = max(0, local_start - chunk_pt_start)
            e_local_excl = min(chunk_size, (local_end_incl - chunk_pt_start) + 1)
            if s_local == 0 and e_local_excl == chunk_size:
                plan.append(ChunkPlanItem(part_number=pn, chunk_index=ci))
            else:
                plan.append(
                    ChunkPlanItem(
                        part_number=pn, chunk_index=ci, slice_start=int(s_local), slice_end_excl=int(e_local_excl)
                    )
                )
    return plan
