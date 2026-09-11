"""Every connection that runs a bucket recompute must raise `statement_timeout` itself.

recompute_bucket_storage_usage() is a full aggregate over one bucket. Production sets a 1-minute
server-side statement_timeout for the application role, and the largest bucket takes longer than
that, so the server kills it. asyncpg's own `timeout=` cannot rescue it: whichever of the two
limits is SHORTER is the one that fires, and the server's was.

The consequences were both silent-ish and permanent:
  * the backfill aborts on that bucket every run, so `backfilled_at` is never set and the rollout
    cannot complete -- safe, because the flag is written only after a complete pass, but stuck;
  * the reconciler fails its cycle whenever that bucket reaches the head of the
    least-recently-recomputed queue, so the single bucket most worth verifying is the one bucket it
    can never verify.

Neither shows up on staging, which has no bucket anywhere near 60s -- so a test is the only thing
that keeps this from regressing. It asserts on the CONNECTION SETUP rather than on behaviour,
because the behaviour only diverges against a specific server configuration we cannot reproduce
in-process.
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
    """The default has to exceed prod's 1-minute statement_timeout, or the fix changes nothing."""
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
        f"--timeout defaults to {defaults[0]}s, which does not clear production's 60s server-side "
        "statement_timeout -- raising it on the connection would then be pointless"
    )
