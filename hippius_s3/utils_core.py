"""Utility functions for the Hippius S3 service."""

import dataclasses
import functools
import logging
import os
import pathlib
import re
import time
import typing
from typing import Any
from typing import AsyncIterator
from typing import Callable
from typing import Dict
from typing import Tuple
from typing import TypeVar

import asyncpg
from fastapi import Request


T = TypeVar("T")

logger = logging.getLogger(__name__)


async def iter_request_body(request: Request) -> AsyncIterator[bytes]:
    """
    Stream request body with optional AWS chunked decoding.

    AWS CLI sends data in aws-chunked format even through proxies:
    Format: [chunk-size-hex]\\r\\n[chunk-data]\\r\\n...[0]\\r\\n[optional-trailers]\\r\\n\\r\\n
    """
    content_encoding = request.headers.get("content-encoding", "").lower()
    force_chunked = "aws-chunked" in content_encoding
    if not force_chunked:
        decoded_length = request.headers.get("x-amz-decoded-content-length")
        sha256 = request.headers.get("x-amz-content-sha256", "").upper()
        if decoded_length or "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" in sha256:
            force_chunked = True

    stream_iter = request.stream().__aiter__()
    buffer = bytearray()
    eof = False

    async def _read_next() -> bool:
        nonlocal eof
        try:
            chunk = await stream_iter.__anext__()
        except StopAsyncIteration:
            eof = True
            return False
        if chunk:
            buffer.extend(chunk)
        return True

    def _parse_chunk_size(line: bytes) -> int | None:
        try:
            text = line.decode("ascii", errors="ignore").strip()
            if ";" in text:
                text = text.split(";", 1)[0]
            if not text:
                return None
            return int(text, 16)
        except Exception:
            return None

    async def _drain_trailers() -> None:
        while True:
            marker = buffer.find(b"\r\n\r\n")
            if marker != -1:
                return
            if eof:
                return
            await _read_next()

    # Prime the buffer once for streaming mode
    await _read_next()

    if not force_chunked:
        if buffer:
            yield bytes(buffer)
            buffer.clear()
        async for chunk in stream_iter:
            if chunk:
                yield chunk
        return

    # AWS chunked decode
    while True:
        # Read chunk size line
        line_end = buffer.find(b"\r\n")
        while line_end == -1:
            if eof:
                return
            await _read_next()
            line_end = buffer.find(b"\r\n")

        line = bytes(buffer[:line_end])
        del buffer[: line_end + 2]
        size = _parse_chunk_size(line)
        if size is None:
            raise ValueError(f"invalid_chunk_size_line:{line!r}")
        if size == 0:
            await _drain_trailers()
            return

        # Ensure we have full chunk + trailing CRLF
        while len(buffer) < size + 2:
            if eof:
                raise ValueError("incomplete_chunk_body")
            await _read_next()

        data = bytes(buffer[:size])
        del buffer[:size]
        if buffer[:2] != b"\r\n":
            raise ValueError("invalid_chunk_terminator")
        del buffer[:2]
        if data:
            yield data


async def get_request_body(request: Request) -> bytes:
    """
    Get request body (fully buffered) while honoring AWS chunked encoding.

    Prefer iter_request_body() for streaming workloads.
    """
    start_time = time.time()
    chunks: list[bytes] = []
    chunk_count = 0

    async for chunk in iter_request_body(request):
        chunks.append(chunk)
        chunk_count += 1

    raw_body = b"".join(chunks)
    body_time = time.time() - start_time
    logger.debug(f"Streaming read took {body_time:.3f}s, {chunk_count} chunks, size: {len(raw_body)} bytes")
    return raw_body


def _is_aws_chunked_body(body: bytes, request: Request) -> bool:
    """Check if body contains AWS chunked encoding format"""
    if not body or len(body) < 3:
        return False

    # Check headers for aws-chunked encoding
    content_encoding = request.headers.get("content-encoding", "").lower()
    if "aws-chunked" in content_encoding:
        return True

    # Check if body starts with hex chunk size pattern
    # AWS chunked format starts with hex digits followed by \r\n
    chunked_pattern = rb"^[0-9a-fA-F]+\r\n"
    return bool(re.match(chunked_pattern, body))


def _decode_aws_chunked_body(chunked_data: bytes) -> Tuple[bytes, Dict[str, str]]:
    """
    Decode AWS chunked transfer encoding format.

    Returns:
        Tuple of (decoded_content, trailers_dict)
    """
    if not chunked_data:
        return b"", {}

    result = bytearray()
    offset = 0
    trailers: Dict[str, str] = {}

    while offset < len(chunked_data):
        # Find the end of the chunk size line
        chunk_size_end = chunked_data.find(b"\r\n", offset)
        if chunk_size_end == -1:
            break

        # Parse chunk size (hexadecimal)
        chunk_size_str = chunked_data[offset:chunk_size_end].decode("ascii", errors="ignore")

        # Remove any chunk extensions (semicolon and beyond)
        if ";" in chunk_size_str:
            chunk_size_str = chunk_size_str.split(";")[0]

        try:
            chunk_size = int(chunk_size_str.strip(), 16)
        except ValueError:
            # If we can't parse the chunk size, assume it's not chunked
            logger.warning(f"Could not parse chunk size '{chunk_size_str}', returning original data")
            return chunked_data, {}

        # If chunk size is 0, we've reached the trailers section
        if chunk_size == 0:
            # Parse any trailing headers/checksums
            trailer_start = chunk_size_end + 2
            _parse_trailers(chunked_data[trailer_start:], trailers)
            break

        # Extract the chunk data
        chunk_start = chunk_size_end + 2  # Skip \r\n
        chunk_end = chunk_start + chunk_size

        if chunk_end > len(chunked_data):
            logger.warning(f"Chunk size {chunk_size} exceeds remaining data")
            break

        result.extend(chunked_data[chunk_start:chunk_end])

        # Move to next chunk (skip trailing \r\n)
        offset = chunk_end + 2

    return bytes(result), trailers


def _parse_trailers(trailer_data: bytes, trailers: Dict[str, str]) -> None:
    """Parse HTTP trailers from the end of chunked data"""
    try:
        trailer_text = trailer_data.decode("ascii", errors="ignore")
        lines = trailer_text.split("\r\n")

        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                trailers[key.strip()] = value.strip()
    except Exception as e:
        logger.warning(f"Could not parse trailers: {e}")


def env(key: str, convert: Callable[[str], T] = typing.cast(Callable[[str], T], str), **kwargs: Any) -> T:
    """Load a value from environment variables with optional default and type conversion."""
    key, partition, default = key.partition(":")

    def default_factory(
        key_val: str = key, default_val: str = default, convert_func: Callable[[str], T] = convert
    ) -> T:
        if key_val in os.environ:
            return convert_func(os.environ[key_val])

        if partition == ":":
            return convert_func(default_val)

        raise KeyError(key_val)

    return typing.cast(T, dataclasses.field(default_factory=default_factory, **kwargs))


@functools.cache
def get_query(name: str) -> str:
    """Load SQL query from disk once and cache it in memory."""
    file_name = f"{name}.sql"
    # Use pathlib to get parent directory (hippius_s3/) from utils_core.py location
    path = pathlib.Path(__file__).parent.joinpath("sql").joinpath("queries").joinpath(file_name)

    logger.debug(f"Loading query from disk: {file_name}")
    with path.open("r") as fp:
        return fp.read().strip()


async def upsert_cid_and_get_id(db: asyncpg.Pool, cid: str) -> str:
    """Insert or get existing CID and return the cid_id (UUID)."""
    result = await db.fetchrow(get_query("upsert_cid"), cid)
    return str(result["id"])


async def get_object_download_info(db: asyncpg.Pool, object_id: str) -> dict:
    """Get complete download information for an object (simple or multipart)."""
    result = await db.fetchrow(get_query("get_object_download_info_by_id"), object_id)

    if not result:
        raise ValueError(f"Object not found: {object_id}")

    storage_version = int(result.get("storage_version") or 0)
    download_chunks = result["download_chunks"]
    # Defensive: legacy download paths require concrete CIDs. If they're missing, treat as not-ready
    # so callers can retry instead of failing later with confusing nulls.
    if storage_version > 0 and storage_version < 4:
        try:
            chunks = list(download_chunks) if isinstance(download_chunks, list) else []
            for c in chunks:
                cid = c.get("cid") if isinstance(c, dict) else None  # type: ignore[union-attr]
                cid_norm = str(cid or "").strip().lower()
                if cid_norm in {"", "none", "pending"}:
                    raise RuntimeError("download_not_ready_missing_cids")
        except Exception as e:
            if str(e) == "download_not_ready_missing_cids":
                raise RuntimeError(f"download_not_ready_missing_cids: object_id={object_id}") from None
            # If parsing fails, be conservative and let callers retry.
            raise RuntimeError(f"download_not_ready_missing_cids: object_id={object_id}") from None

    return {
        "object_id": result["object_id"],
        "multipart": result["multipart"],
        "storage_version": storage_version,
        "needs_decryption": result["needs_decryption"],
        "download_chunks": download_chunks,
    }
