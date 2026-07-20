from __future__ import annotations

import datetime
import re
import sys
from typing import Any

import asyncpg
import pytest

from hippius_s3.scripts import report_broken_v5_rows as rpt


_MUTATING = re.compile(r"\b(INSERT|UPDATE|DELETE|TRUNCATE|DROP|ALTER|CREATE|MERGE)\b", re.IGNORECASE)


class FakeConnection:
    def __init__(self, cohort_rows: list[dict[str, Any]], sample_rows: list[dict[str, Any]]) -> None:
        self._results = [cohort_rows, sample_rows]
        self.ops: list[tuple[str, str, tuple[Any, ...]]] = []

    async def execute(self, sql: str, *params: Any) -> str:
        self.ops.append(("execute", sql, params))
        return "SET"

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        self.ops.append(("fetch", sql, params))
        return self._results.pop(0)

    async def close(self) -> None:
        self.ops.append(("close", "", ()))


def _cohort(
    *,
    multipart: bool | None,
    status: str | None,
    live_current: bool,
    empty_placeholder: bool,
    row_count: int,
) -> dict[str, Any]:
    return {
        "multipart": multipart,
        "status": status,
        "live_current": live_current,
        "empty_placeholder": empty_placeholder,
        "row_count": row_count,
        "oldest": datetime.datetime(2026, 1, 26, tzinfo=datetime.timezone.utc),
        "newest": datetime.datetime(2026, 7, 16, tzinfo=datetime.timezone.utc),
    }


class TestQueryBuilding:
    def test_cohort_query_is_bounded_when_since_days_given(self) -> None:
        sql = rpt.build_cohort_query(since_days=90)
        assert "ov.created_at >= NOW() - ($1::int * INTERVAL '1 day')" in sql

    def test_cohort_query_has_no_time_clause_when_unbounded(self) -> None:
        sql = rpt.build_cohort_query(since_days=None)
        assert "created_at >=" not in sql
        assert "$1" not in sql

    def test_sample_limit_param_index_shifts_with_the_time_bound(self) -> None:
        assert rpt.build_sample_query(since_days=90).rstrip().endswith("LIMIT $2")
        assert rpt.build_sample_query(since_days=None).rstrip().endswith("LIMIT $1")

    def test_params_match_the_placeholders(self) -> None:
        assert rpt.cohort_params(since_days=90) == (90,)
        assert rpt.cohort_params(since_days=None) == ()
        assert rpt.sample_params(since_days=90, limit=20) == (90, 20)
        assert rpt.sample_params(since_days=None, limit=20) == (20,)

    @pytest.mark.parametrize("since_days", [90, None])
    def test_every_query_is_a_bare_select(self, since_days: int | None) -> None:
        for sql in (rpt.build_cohort_query(since_days=since_days), rpt.build_sample_query(since_days=since_days)):
            assert sql.lstrip().upper().startswith("SELECT")
            assert not _MUTATING.search(sql)

    def test_predicate_catches_kek_present_but_dek_missing(self) -> None:
        # delete_v4.sql only keys on kek_id IS NULL and misses this half of the cohort.
        assert "ov.kek_id IS NULL OR ov.wrapped_dek IS NULL" in rpt.build_cohort_query(since_days=None)


class TestReport:
    def test_totals_and_live_current_subset(self) -> None:
        rows = [
            _cohort(multipart=True, status="publishing", live_current=False, empty_placeholder=True, row_count=145115),
            _cohort(multipart=False, status="failed", live_current=False, empty_placeholder=True, row_count=51870),
            _cohort(multipart=False, status="publishing", live_current=True, empty_placeholder=False, row_count=26),
        ]
        report = rpt.build_report(rows, [], since_days=90, limit=20)

        assert report["total_broken_rows"] == 197011
        assert report["live_current_rows"] == 26
        assert report["empty_placeholder_rows"] == 196985
        assert report["cohorts"][0]["row_count"] == 145115

    def test_bound_is_labelled_partial_so_it_is_not_read_as_a_total(self) -> None:
        report = rpt.build_report([], [], since_days=90, limit=20)
        assert report["bound"]["since_days"] == 90
        assert "partial" in report["bound"]["scope"].lower()

    def test_unbounded_scope_is_labelled_complete(self) -> None:
        report = rpt.build_report([], [], since_days=None, limit=20)
        assert report["bound"]["since_days"] is None
        assert "partial" not in report["bound"]["scope"].lower()

    def test_exit_code_flags_live_current_rows_for_monitoring(self) -> None:
        broken = rpt.build_report(
            [_cohort(multipart=False, status="publishing", live_current=True, empty_placeholder=False, row_count=1)],
            [],
            since_days=90,
            limit=20,
        )
        clean = rpt.build_report(
            [_cohort(multipart=True, status="publishing", live_current=False, empty_placeholder=True, row_count=99)],
            [],
            since_days=90,
            limit=20,
        )
        assert rpt.exit_code(broken) != 0
        assert rpt.exit_code(clean) == 0


class TestReadOnly:
    @pytest.mark.asyncio
    async def test_session_is_pinned_read_only_before_any_query(self) -> None:
        conn = FakeConnection([], [])
        await rpt.run_report(conn, since_days=90, limit=20, statement_timeout_ms=30000)

        kinds = [op[0] for op in conn.ops]
        first_fetch = kinds.index("fetch")
        setup = " ".join(op[1] for op in conn.ops[:first_fetch] if op[0] == "execute")
        assert "default_transaction_read_only = on" in setup
        assert "statement_timeout = 30000" in setup

    @pytest.mark.asyncio
    async def test_run_report_only_ever_fetches_selects(self) -> None:
        conn = FakeConnection([], [])
        await rpt.run_report(conn, since_days=None, limit=5, statement_timeout_ms=1000)

        for kind, sql, _params in conn.ops:
            if kind == "fetch":
                assert sql.lstrip().upper().startswith("SELECT")
                assert not _MUTATING.search(sql)

    @pytest.mark.asyncio
    async def test_run_report_returns_the_report_payload(self) -> None:
        cohort = [_cohort(multipart=True, status="publishing", live_current=True, empty_placeholder=False, row_count=3)]
        samples = [{"object_id": "abc", "object_version": 2}]
        conn = FakeConnection(cohort, samples)

        report = await rpt.run_report(conn, since_days=7, limit=1, statement_timeout_ms=1000)

        assert report["live_current_rows"] == 3
        assert report["samples"] == samples
        assert report["sample_limit"] == 1


class TestCli:
    def test_parser_exposes_no_mutating_mode(self) -> None:
        parser = rpt.build_parser()
        flags = {opt for action in parser._actions for opt in action.option_strings}
        assert not flags & {"--execute", "--delete", "--apply", "--no-dry-run", "--force"}

    def test_since_days_must_be_positive_or_explicitly_all_time(self) -> None:
        parser = rpt.build_parser()
        assert parser.parse_args([]).since_days > 0
        with pytest.raises(SystemExit):
            parser.parse_args(["--since-days", "0"])
        assert parser.parse_args(["--all-time"]).all_time is True

    def test_resolved_since_days_is_none_only_for_all_time(self) -> None:
        parser = rpt.build_parser()
        assert rpt.resolve_since_days(parser.parse_args(["--since-days", "30"])) == 30
        assert rpt.resolve_since_days(parser.parse_args(["--all-time"])) is None

    def test_scope_warns_the_scan_is_full_in_both_modes(self) -> None:
        # The time window bounds the result, not the work: the broken-v5 predicate lost its indexes to
        # migration, so a reader must not infer "90 days" means "cheap enough for the primary".
        for since_days in (90, None):
            scope = rpt.build_report([], [], since_days=since_days, limit=20)["bound"]["scope"].lower()
            assert "full sequential scan" in scope
            assert "replica" in scope

    def test_operational_failure_is_distinguishable_from_a_real_finding(self) -> None:
        # A statement timeout is the likely outcome against prod. If it shared exit code 1 with
        # "broken rows found", a monitoring probe would read every timeout as a permanent positive.
        assert rpt.EXIT_OPERATIONAL_FAILURE != rpt.EXIT_BROKEN_ROWS_FOUND
        assert rpt.EXIT_OPERATIONAL_FAILURE != rpt.EXIT_OK
        assert rpt.exit_code(rpt.build_report([], [], since_days=90, limit=20)) == rpt.EXIT_OK

    @pytest.mark.parametrize(
        "exc",
        [
            asyncpg.exceptions.QueryCanceledError("canceling statement due to statement timeout"),
            OSError("connection refused"),
        ],
        ids=["statement_timeout", "db_unreachable"],
    )
    def test_main_exits_operational_on_db_failure(self, exc: Exception, monkeypatch: pytest.MonkeyPatch) -> None:
        async def failing(_args: Any) -> int:
            raise exc

        monkeypatch.setattr(rpt, "main_async", failing)
        monkeypatch.setattr(sys, "argv", ["report-broken-v5-rows"])
        with pytest.raises(SystemExit) as caught:
            rpt.main()
        assert caught.value.code == rpt.EXIT_OPERATIONAL_FAILURE
