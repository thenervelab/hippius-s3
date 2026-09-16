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
