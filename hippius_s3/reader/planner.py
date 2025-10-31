from __future__ import annotations

import contextlib
import logging
from typing import Any
from typing import Iterable
from typing import List

from .db_meta import read_part_plain_and_chunk_size
from .types import ChunkPlanItem
from .types import RangeRequest


logger = logging.getLogger(__name__)


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

    # Preload sizes to avoid repeated DB calls and compute offsets
    sizes: list[tuple[int, int, int]] = []  # (part_number, plain_size, chunk_size)
    for p in ordered:
        pn = int(p.get("part_number", 0))
        ps, cs = await read_part_plain_and_chunk_size(db, object_id, pn, int(object_version))
        sizes.append((pn, ps, cs))
    with contextlib.suppress(Exception):
        logger.debug("PLAN sizes: %s", sizes)

    offsets: dict[int, int] = {}
    acc = 0
    for pn, ps, _ in sizes:
        offsets[int(pn)] = acc
        acc += int(ps)

    plan: List[ChunkPlanItem] = []
    for pn, plain_size, chunk_size in sizes:
        if plain_size <= 0 or chunk_size <= 0:
            continue
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
