"""Every connection that runs a bucket recompute must raise `statement_timeout` itself.

recompute_bucket_storage_usage() is a full aggregate over one bucket, on the PRIMARY. Both callers
must bound it server-side, not only with asyncpg's `timeout=` -- that cancels by sending a cancel
request, client-driven and best-effort, whereas statement_timeout is enforced by the backend.

CORRECTION, because the original version of this module asserted otherwise: production's
statement_timeout is 0, UNBOUNDED. The "1-minute server-side limit" it was written against was a
measurement error -- a precheck script SET the value itself and then read its own setting back.
Measured properly on both prod and staging: `source = default`, `reset_val = 0`, and no
per-database or per-role rolconfig anywhere.

So these connections IMPOSE a bound rather than raise a too-tight one. That is still worth a test:
an unbounded full aggregate over a 165 GB / 91 GB table pair on the primary is the read-storm shape
that has stalled this cluster and forced a failover before, and recompute holds the rollup's global
advisory lock while it runs -- so an unbounded one pauses the compactor indefinitely and the
reconciler makes no further progress.

It asserts on the CONNECTION SETUP rather than on behaviour, because the behaviour only diverges
against a server configuration that cannot be reproduced in-process.
"""

from __future__ import annotations

import ast
import pathlib

import pytest


_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _server_settings_of(path: pathlib.Path, factory: str) -> dict[str, str] | None:
    """The literal keys of the `server_settings=` kwarg on a call to `factory` in `path`.

    Static rather than executed: both call sites build their connection at worker/script startup,
    behind `get_config()` and a live event loop, and neither is worth standing up to read one
    kwarg. Values are not evaluated -- only that the key is passed and what it is named.
    """
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", None)
        if name != factory:
            continue
        for kw in node.keywords:
            if kw.arg == "server_settings" and isinstance(kw.value, ast.Dict):
                return {
                    k.value: ast.unparse(v)
                    for k, v in zip(kw.value.keys, kw.value.values, strict=False)
                    if isinstance(k, ast.Constant)
                }
    return None


def test_the_usage_rollup_pool_raises_statement_timeout() -> None:
    settings = _server_settings_of(_ROOT / "workers/run_usage_rollup_in_loop.py", "create_pool")

    assert settings is not None, "the usage-rollup pool passes no server_settings at all"
    assert "statement_timeout" in settings, (
        "the usage-rollup pool does not raise statement_timeout, so the reconciler will be killed "
        "by the server's own limit on exactly the largest bucket -- see this module's docstring"
    )
    assert "usage_reconcile_timeout_seconds" in settings["statement_timeout"], (
        "statement_timeout should be derived from config.usage_reconcile_timeout_seconds so the two "
        f"bounds cannot drift apart; got {settings['statement_timeout']!r}"
    )


def test_the_backfill_connection_raises_statement_timeout() -> None:
    settings = _server_settings_of(_ROOT / "hippius_s3/scripts/backfill_bucket_storage_usage.py", "connect")

    assert settings is not None, "the backfill connection passes no server_settings at all"
    assert "statement_timeout" in settings, (
        "the backfill does not raise statement_timeout, so it aborts on the largest bucket every "
        "run and backfilled_at can never be set"
    )
    assert "timeout" in settings["statement_timeout"], (
        "statement_timeout should be derived from --timeout so one flag moves both bounds; "
        f"got {settings['statement_timeout']!r}"
    )


def test_the_static_reader_actually_finds_a_call() -> None:
    """Guard the guard: a reader that matches nothing would pass by returning None everywhere.

    Both assertions above would then fail rather than pass, so this is not strictly load-bearing --
    but a future refactor that renames the factory would turn them into confusing failures instead
    of the clear one below.
    """
    assert _server_settings_of(_ROOT / "workers/run_usage_rollup_in_loop.py", "create_pool") is not None
    assert _server_settings_of(_ROOT / "workers/run_usage_rollup_in_loop.py", "no_such_factory") is None


@pytest.mark.parametrize("seconds", [60.0, 300.0, 600.0])
def test_the_timeout_is_rendered_as_milliseconds(seconds: float) -> None:
    """Postgres reads a bare `statement_timeout` as MILLISECONDS.

    Passing "300" instead of "300000" would set a 300ms timeout and break every recompute, not just
    the big one -- a far louder failure than the one being fixed, but worth pinning the unit.
    """
    rendered = f"{int(seconds * 1000)}"

    assert rendered == str(int(seconds) * 1000)
    assert int(rendered) >= 60_000


def test_backfill_default_timeout_clears_the_production_server_limit() -> None:
    """A default under a minute would make the bound tighter than the work, not a safety net.

    Production's server-side statement_timeout is 0, so this value IS the only bound; the largest
    measured bucket aggregate is ~22s with several above 30s, so anything near that would start
    failing legitimate recomputes.
    """
    source = (_ROOT / "hippius_s3/scripts/backfill_bucket_storage_usage.py").read_text()
    tree = ast.parse(source)

    defaults = [
        float(ast.literal_eval(kw.value))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "add_argument"
        for arg in node.args[:1]
        if isinstance(arg, ast.Constant) and arg.value == "--timeout"
        for kw in node.keywords
        if kw.arg == "default"
    ]

    assert defaults, "could not find the --timeout default"
    assert defaults[0] > 60.0, (
        f"--timeout defaults to {defaults[0]}s, which is too close to the measured cost of the "
        "largest buckets (~22s measured, several over 30s) to be a safety net rather than a limit"
    )
