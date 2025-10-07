from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from typing import Optional


@dataclass
class RangeRequest:
    start: int
    end: int


Source = Literal["cache", "pipeline"]


@dataclass
class ChunkPlanItem:
    part_number: int
    chunk_index: int
    # Slice on plaintext bytes within this chunk. None means full chunk.
    slice_start: Optional[int] = None
    slice_end_excl: Optional[int] = None
