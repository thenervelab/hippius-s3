"""Tests for the janitor's Ceph pool-fullness gate (2026-07-24 incident, handoff item #3).

statvfs on the cache mount sees the CephFS *PVC quota*, not the backing pool, so the janitor's
disk-pressure probe stayed in Normal mode (10min sleep, 64-shard sweep, hot retention honored)
while `ceph-filesystem-data0` filled to the read-only cliff. The gate makes _pressure_mode key on
max(statvfs ratio, fullest configured pool's %USED) from the mgr exporter — the same signal the
drain allocator got in PR #337. These cover the parse, the fail-safe fallbacks, and the max.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from workers import run_janitor_in_loop as janitor


# The pool series shape captured live from rook-ceph-mgr:9283/metrics (pool 5 fullest at ~95%).
HEALTHY = "ceph_cluster_total_bytes 115222679470080.0\nceph_cluster_total_used_bytes 31791022084096.0\n"
POOL_SERIES = (
    'ceph_pool_metadata{pool_id="2",name="ceph-blockpool",type="replicated"} 1.0\n'
    'ceph_pool_metadata{pool_id="3",name="ceph-filesystem-metadata",type="replicated"} 1.0\n'
    'ceph_pool_metadata{pool_id="5",name="ceph-filesystem-data0",type="replicated"} 1.0\n'
    'ceph_pool_percent_used{pool_id="2"} 0.6975\n'
    'ceph_pool_percent_used{pool_id="3"} 0.0072\n'
    'ceph_pool_percent_used{pool_id="5"} 0.9507322907447815\n'
)


# ------------------------------------------------------------------ _label_value


def test_label_value_extracts_up_to_the_closing_quote() -> None:
    line = 'ceph_pool_metadata{pool_id="5",name="ceph-filesystem-data0",type="replicated"} 1.0'
    assert janitor._label_value(line, 'pool_id="') == "5"
    assert janitor._label_value(line, 'name="') == "ceph-filesystem-data0"


def test_label_value_absent_needle_is_none() -> None:
    assert janitor._label_value("ceph_pool_percent_used 0.5", 'pool_id="') is None


# --------------------------------------------------------- _parse_pool_percent_used


def test_parse_resolves_the_pool_fraction_via_its_metadata_id() -> None:
    got = janitor._parse_pool_percent_used(HEALTHY + POOL_SERIES, ["ceph-filesystem-data0"])
    assert got == pytest.approx(0.9507322907447815), "pool 5's fraction, not pool 2's"


def test_parse_fullest_of_several_pools_wins_regardless_of_order() -> None:
    # data0 (95%) is fuller than blockpool (70%) and metadata (0.7%); order must not matter.
    got = janitor._parse_pool_percent_used(
        HEALTHY + POOL_SERIES, ["ceph-blockpool", "ceph-filesystem-data0", "ceph-filesystem-metadata"]
    )
    assert got == pytest.approx(0.9507322907447815)


def test_parse_series_order_does_not_matter() -> None:
    # percent-used emitted BEFORE the metadata that names the pool must still resolve.
    body = HEALTHY + 'ceph_pool_percent_used{pool_id="5"} 0.5\nceph_pool_metadata{pool_id="5",name="data"} 1.0\n'
    assert janitor._parse_pool_percent_used(body, ["data"]) == pytest.approx(0.5)


def test_parse_missing_pool_is_none_fail_safe() -> None:
    # A typo'd/renamed pool anywhere in the list must fail safe (fall back to statvfs), never
    # silently shrink the gate to the pools that did resolve.
    assert janitor._parse_pool_percent_used(HEALTHY + POOL_SERIES, ["ceph-filesystem-data0", "nope"]) is None


def test_parse_pool_absent_entirely_is_none() -> None:
    assert janitor._parse_pool_percent_used(HEALTHY, ["ceph-filesystem-data0"]) is None


def test_parse_prefix_name_is_not_matched() -> None:
    # name="data0" must not resolve for target "data": the label match is exact.
    body = HEALTHY + 'ceph_pool_metadata{pool_id="7",name="data0"} 1.0\nceph_pool_percent_used{pool_id="7"} 0.99\n'
    assert janitor._parse_pool_percent_used(body, ["data"]) is None


def test_parse_fraction_above_one_clamps() -> None:
    body = HEALTHY + 'ceph_pool_metadata{pool_id="5",name="data"} 1.0\nceph_pool_percent_used{pool_id="5"} 1.0000001\n'
    assert janitor._parse_pool_percent_used(body, ["data"]) == pytest.approx(1.0)


@pytest.mark.parametrize("bad", ["NaN", "inf", "garbage"])
def test_parse_non_finite_or_garbage_percent_reads_missing(bad: str) -> None:
    # A NaN/inf/garbage value is not a ratio — it is dropped, so the pool has no valid percent and
    # the whole probe reads "missing" → None (fail safe to statvfs), never a silent healthy read.
    body = HEALTHY + f'ceph_pool_metadata{{pool_id="5",name="data"}} 1.0\nceph_pool_percent_used{{pool_id="5"}} {bad}\n'
    assert janitor._parse_pool_percent_used(body, ["data"]) is None


# --------------------------------------------------------- _fetch_pool_percent_used


@pytest.mark.asyncio
async def test_fetch_returns_none_when_unconfigured(monkeypatch) -> None:
    monkeypatch.setattr(janitor.config, "janitor_ceph_mgr_metrics_url", "")
    monkeypatch.setattr(janitor.config, "janitor_ceph_pools", "")
    assert await janitor._fetch_pool_percent_used() is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_scrapes_and_parses_the_fullest_pool(monkeypatch) -> None:
    url = "http://mgr.test/metrics"
    monkeypatch.setattr(janitor.config, "janitor_ceph_mgr_metrics_url", url)
    monkeypatch.setattr(janitor.config, "janitor_ceph_pools", "ceph-filesystem-data0, ceph-blockpool")
    monkeypatch.setattr(janitor.config, "janitor_ceph_probe_timeout_seconds", 2.0)
    respx.get(url).mock(return_value=httpx.Response(200, text=HEALTHY + POOL_SERIES))
    assert await janitor._fetch_pool_percent_used() == pytest.approx(0.9507322907447815)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_falls_back_to_none_on_http_error(monkeypatch) -> None:
    url = "http://mgr.test/metrics"
    monkeypatch.setattr(janitor.config, "janitor_ceph_mgr_metrics_url", url)
    monkeypatch.setattr(janitor.config, "janitor_ceph_pools", "ceph-filesystem-data0")
    monkeypatch.setattr(janitor.config, "janitor_ceph_probe_timeout_seconds", 2.0)
    respx.get(url).mock(return_value=httpx.Response(503))
    assert await janitor._fetch_pool_percent_used() is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_falls_back_to_none_on_connect_error(monkeypatch) -> None:
    url = "http://mgr.test/metrics"
    monkeypatch.setattr(janitor.config, "janitor_ceph_mgr_metrics_url", url)
    monkeypatch.setattr(janitor.config, "janitor_ceph_pools", "ceph-filesystem-data0")
    monkeypatch.setattr(janitor.config, "janitor_ceph_probe_timeout_seconds", 2.0)
    respx.get(url).mock(side_effect=httpx.ConnectError("refused"))
    assert await janitor._fetch_pool_percent_used() is None


# ------------------------------------------------------------------ _pressure_mode max


@pytest.mark.parametrize(
    ("pool_pct", "expected"),
    [
        (None, 0),  # gate off → statvfs (10%) alone → Normal
        (0.50, 0),  # pool calm → still Normal
        (0.88, 1),  # pool near-full → Elevated even though statvfs says 10%
        (0.96, 2),  # pool at cliff → Critical (the incident: statvfs blind at 10%)
    ],
)
def test_pressure_mode_takes_the_max_of_statvfs_and_pool(monkeypatch, pool_pct, expected) -> None:
    class FakeUsage:
        used = 100
        total = 1000  # 10% locally → Normal on statvfs alone

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())
    monkeypatch.setattr(janitor, "_fs_pool_percent_used", pool_pct)
    janitor._prev_pressure_mode = 0
    assert janitor._pressure_mode(Path("/tmp")) == expected


def test_pressure_mode_local_statvfs_still_drives_when_pool_gate_off(monkeypatch) -> None:
    # Gate unconfigured (pool None): a genuinely full local disk must still escalate.
    class FakeUsage:
        used = 970
        total = 1000  # 97% locally → Critical

    monkeypatch.setattr(janitor.shutil, "disk_usage", lambda p: FakeUsage())
    monkeypatch.setattr(janitor, "_fs_pool_percent_used", None)
    janitor._prev_pressure_mode = 0
    assert janitor._pressure_mode(Path("/tmp")) == 2
