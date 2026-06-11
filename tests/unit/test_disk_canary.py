"""Tests for the disk-canary mount probe (workers/run_disk_canary_in_loop.py check_mount)."""

from __future__ import annotations

import os

import pytest

from workers.run_disk_canary_in_loop import check_mount


def test_healthy_mount_passes_and_cleans_up(tmp_path):
    elapsed = check_mount(str(tmp_path))
    assert elapsed >= 0
    canary_dir = tmp_path / ".canary"
    assert canary_dir.is_dir()
    assert list(canary_dir.glob("heartbeat.*")) == [], "heartbeat must be deleted after the probe"
    assert list(canary_dir.glob(".tmp.*")) == [], "tmp file must be renamed away"


def test_missing_mount_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        check_mount(str(tmp_path / "does-not-exist"))


def test_unwritable_mount_raises(tmp_path):
    if os.geteuid() == 0:
        pytest.skip("root bypasses permission bits")
    target = tmp_path / "ro"
    target.mkdir()
    canary_dir = target / ".canary"
    canary_dir.mkdir()
    canary_dir.chmod(0o555)
    try:
        with pytest.raises(PermissionError):
            check_mount(str(target))
    finally:
        canary_dir.chmod(0o755)


def test_repeated_probes_are_idempotent(tmp_path):
    for _ in range(3):
        check_mount(str(tmp_path))
    assert list((tmp_path / ".canary").iterdir()) == []
