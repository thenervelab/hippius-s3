"""KM-4: get_config() is memoized (config is immutable after startup).

get_config() rebuilt a fresh Config dataclass and re-ran validation on every call — ~5x/PUT in the
KEK layer alone. Memoize it to a single instance, with reset_config() to drop the cached instance
(used by the test harness, which mutates env between tests).
"""

from __future__ import annotations

import pytest

from hippius_s3 import config as config_mod


def test_get_config_returns_the_same_instance() -> None:
    assert config_mod.get_config() is config_mod.get_config()


def test_reset_config_rebuilds_and_picks_up_env(monkeypatch: pytest.MonkeyPatch) -> None:
    first = config_mod.get_config()
    bumped = first.object_chunk_size_bytes + 4096
    monkeypatch.setenv("HIPPIUS_CHUNK_SIZE_BYTES", str(bumped))

    # Without a reset the cached instance is still returned.
    assert config_mod.get_config() is first

    config_mod.reset_config()
    second = config_mod.get_config()
    assert second is not first
    assert second.object_chunk_size_bytes == bumped
