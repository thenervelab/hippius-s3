from typing import Dict
from typing import List
from typing import Tuple


def parse_range_header(range_header: str, total_size: int) -> Tuple[int, int]:
    header = range_header.lower().strip()
    if not header.startswith("bytes="):
        raise ValueError(f"Invalid range format: {range_header}")

    spec = header[len("bytes=") :]

    if spec.startswith("-"):
        try:
            suffix = int(spec[1:])
        except ValueError as e:
            raise ValueError(f"Invalid range suffix: {range_header}") from e
        if suffix <= 0:
            raise ValueError(f"Invalid range suffix: {range_header}")
        end = total_size - 1
        start = max(0, total_size - suffix)
        return start, end

    parts = spec.split("-", 1)
    if len(parts) != 2 or not parts[0].isdigit():
        raise ValueError(f"Invalid range format: {range_header}")

    start = int(parts[0])
    if start >= total_size:
        raise ValueError("Range start beyond file size")

    if parts[1]:
        if not parts[1].isdigit():
            raise ValueError(f"Invalid range end: {range_header}")
        end = int(parts[1])
        if end < start:
            # AWS behavior: invalid range (start > end) is treated as no range (full object)
            return 0, total_size - 1
        end = min(end, total_size - 1)
        return start, end

    return start, total_size - 1


def calculate_chunks_for_range(start_byte: int, end_byte: int, parts_info: List[Dict]) -> List[int]:
    needed_parts: List[int] = []
    current_offset = 0
    for part in parts_info:
        part_start = current_offset
        # part_end is not needed directly; inline comparison for clarity
        if (current_offset + part["size_bytes"] - 1) >= start_byte and part_start <= end_byte:
            needed_parts.append(part["part_number"])
        current_offset += part["size_bytes"]
        if part_start > end_byte:
            break
    return needed_parts


def extract_range_from_chunks(
    chunks_data: List[bytes],
    start_byte: int,
    end_byte: int,
    parts_info: List[Dict],
    needed_parts: List[int],
) -> bytes:
    current_offset = 0
    range_data = b""
    chunk_idx = 0
    for part in parts_info:
        part_start = current_offset
        if part["part_number"] in needed_parts:
            chunk_data = chunks_data[chunk_idx]
            # Compute slice within this chunk (end is exclusive)
            slice_start = max(0, start_byte - part_start)
            # Desired absolute end within this part (exclusive)
            desired_end_exclusive = (end_byte - part_start) + 1
            # Cap by part size and ensure not before slice_start
            capped_end_exclusive = min(part["size_bytes"], max(slice_start, desired_end_exclusive))
            range_data += chunk_data[slice_start:capped_end_exclusive]
            chunk_idx += 1
        current_offset += part["size_bytes"]
        if current_offset > end_byte:
            break
    return range_data
