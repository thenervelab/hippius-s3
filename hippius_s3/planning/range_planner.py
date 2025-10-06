"""Pure planning logic for range requests.

No IO; deterministic mapping from part sizes and chunk size to minimal chunk indices.
"""

from __future__ import annotations

from typing import TypedDict


class PartOffset(TypedDict):
    part_number: int
    offset: int
    plain_size: int


class PartInput(TypedDict):
    part_number: int
    plain_size: int


def build_part_offsets(parts: list[PartInput]) -> list[PartOffset]:
    """Build plaintext offset map for each part.

    Args:
        parts: List of {part_number, plain_size} from DB, ordered by part_number.

    Returns:
        List of {part_number, offset, plain_size} with cumulative offsets.
    """
    result: list[PartOffset] = []
    cumulative = 0
    for p in sorted(parts, key=lambda x: x["part_number"]):
        result.append(
            {
                "part_number": p["part_number"],
                "offset": cumulative,
                "plain_size": p["plain_size"],
            }
        )
        cumulative += p["plain_size"]
    return result


def plan_indices_for_range(
    offsets: list[PartOffset],
    range_start: int,
    range_end: int,
    chunk_size: int,
) -> dict[int, list[int]]:
    """Compute minimal per-part chunk indices for a plaintext range.

    Args:
        offsets: Part offset map from build_part_offsets.
        range_start: Inclusive start byte (plaintext, 0-based).
        range_end: Inclusive end byte (plaintext, 0-based).
        chunk_size: Chunk size in bytes (from DB or config).

    Returns:
        dict[part_number, list[chunk_index]] for chunks overlapping the range.
    """
    result: dict[int, list[int]] = {}
    for part in offsets:
        pn = part["part_number"]
        part_start = part["offset"]
        part_end = part_start + part["plain_size"] - 1

        # Skip if part doesn't overlap range
        if part_end < range_start or part_start > range_end:
            continue

        # Compute local range within this part
        local_start = max(0, range_start - part_start)
        local_end = min(part["plain_size"] - 1, range_end - part_start)

        # Map to chunk indices
        if chunk_size <= 0:
            raise ValueError(f"Invalid chunk_size={chunk_size} for part {pn}; cannot plan range")
        start_chunk = local_start // chunk_size
        end_chunk = local_end // chunk_size

        result[pn] = list(range(start_chunk, end_chunk + 1))

    return result
