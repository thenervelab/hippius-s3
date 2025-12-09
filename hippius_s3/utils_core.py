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
from typing import Callable
from typing import Dict
from typing import Tuple
from typing import TypeVar

import asyncpg
from fastapi import Request


T = TypeVar("T")

logger = logging.getLogger(__name__)


async def get_request_body(request: Request) -> bytes:
    """
    Get request body properly handling AWS chunked encoding from HAProxy.

    AWS CLI sends data in aws-chunked format even through proxies:
    Format: [chunk-size-hex]\r\n[chunk-data]\r\n...[0]\r\n[optional-trailers]\r\n\r\n
    """
    start_time = time.time()

    # Use streaming instead of request.body() to avoid blocking
    chunks = []
    chunk_count = 0

    async for chunk in request.stream():
        chunks.append(chunk)
        chunk_count += 1

    raw_body = b"".join(chunks)
    body_time = time.time() - start_time
    logger.debug(f"Streaming read took {body_time:.3f}s, {chunk_count} chunks, size: {len(raw_body)} bytes")

    # Check if body needs de-chunking
    if _is_aws_chunked_body(raw_body, request):
        decode_start = time.time()
        decoded_body, trailers = _decode_aws_chunked_body(raw_body)
        decode_time = time.time() - decode_start
        logger.debug(f"AWS chunked decode took {decode_time:.3f}s: {len(raw_body)} -> {len(decoded_body)} bytes")
        if trailers:
            logger.debug(f"Found trailers: {trailers}")
        return decoded_body

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
    """Insert or get existing CID and return the cid_id (UUID).

    DEPRECATED: This function uses the old asyncpg pool interface.
    New code should use CIDRepository directly with AsyncSession.
    """
    result = await db.fetchrow(get_query("upsert_cid"), cid)
    return str(result["id"])


async def get_object_download_info(db: asyncpg.Pool, object_id: str) -> dict:
    """Get complete download information for an object (simple or multipart)."""
    result = await db.fetchrow(get_query("get_object_download_info_by_id"), object_id)

    if not result:
        raise ValueError(f"Object not found: {object_id}")

    return {
        "object_id": result["object_id"],
        "multipart": result["multipart"],
        "needs_decryption": result["needs_decryption"],
        "download_chunks": result["download_chunks"],
    }
