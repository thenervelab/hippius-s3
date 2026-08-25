"""Boolean env flags must parse "false" as False.

`bool(str)` is truthiness of a NON-EMPTY STRING, so `convert=bool` turned an explicit
`HIPPIUS_PEER_FETCH_ENABLED=false` into "enabled" — and because `env` runs the converter on
the `:false` default too, the flag was DEFAULT-ON and impossible to turn off at all. Both
read-tier flags shipped that way; `peer_serve_enabled` got the correct converter in review,
these pin its two siblings so the pattern cannot quietly come back.
"""

from __future__ import annotations

import pytest

from hippius_s3.config import Config


_FLAGS = [
    ("HIPPIUS_PEER_FETCH_ENABLED", "peer_fetch_enabled"),
    ("HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ", "object_cache_promote_on_read"),
    ("HIPPIUS_PEER_SERVE_ENABLED", "peer_serve_enabled"),
]


@pytest.mark.parametrize(("env_var", "attr"), _FLAGS)
def test_an_explicit_false_disables_the_flag(monkeypatch, env_var: str, attr: str) -> None:
    monkeypatch.setenv(env_var, "false")
    assert getattr(Config(), attr) is False, f"{env_var}=false must mean OFF, not bool('false')"


@pytest.mark.parametrize(("env_var", "attr"), _FLAGS)
def test_an_explicit_true_enables_the_flag(monkeypatch, env_var: str, attr: str) -> None:
    monkeypatch.setenv(env_var, "true")
    assert getattr(Config(), attr) is True


@pytest.mark.parametrize(("env_var", "attr"), _FLAGS)
def test_unset_defaults_to_off(monkeypatch, env_var: str, attr: str) -> None:
    # The converter runs on the ":false" default string too, so this is the case `convert=bool`
    # silently inverted: an operator who never heard of the flag was running the feature.
    monkeypatch.delenv(env_var, raising=False)
    assert getattr(Config(), attr) is False
