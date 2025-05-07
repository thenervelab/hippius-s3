"""Utility functions for the Hippius S3 service."""

import dataclasses
import functools
import logging
import os
import pathlib
import typing
from typing import Any
from typing import Callable
from typing import TypeVar


T = TypeVar("T")

logger = logging.getLogger(__name__)


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
    path = pathlib.Path(__file__).parent.joinpath("sql").joinpath("queries").joinpath(file_name)

    logger.info(f"Loading/caching query: {file_name}")
    with path.open("r") as fp:
        return fp.read().strip()
