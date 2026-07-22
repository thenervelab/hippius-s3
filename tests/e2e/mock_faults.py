"""Shared fault-injection controller for the e2e mock services (WI-19 §4.4).

Today the mocks have NO fault modes — they only 404 on genuine not-found. The chaos matrix
(F1/F3/F4/F8) and the allocator Redis-pathology cells need the backends to misbehave on demand:
return 500, stall, truncate a body, fail only after N successes, or refuse an upload. This module
is the single place that behaviour lives so all three mocks (arion / kms / hippius-api) share it.

Each mock does:
    from mock_faults import install_fault_controller
    fault = install_fault_controller(app, service="arion")
and, at the top of every handler:
    directive = await fault.gate("upload")          # may sleep or raise HTTPException
    ... use directive.truncate_bytes / directive.reject where relevant ...

Configuration, two ways (runtime wins over env):
  * env at boot — MOCK_FAULT (a JSON rule or list of rules), or the flat
    MOCK_FAULT_MODE / MOCK_FAULT_OP / MOCK_FAULT_STATUS / MOCK_FAULT_DELAY_S /
    MOCK_FAULT_AFTER_N / MOCK_FAULT_TRUNCATE_BYTES knobs for a single rule.
  * runtime — the control API this module mounts: POST /_fault {rule|[rules]},
    GET /_fault, DELETE /_fault (clear), POST /_fault/reset (zero the per-op counters WITHOUT
    changing rules). Tests drive faults through these.

Per-op call counters (which drive `fail_after_n`) reset on EVERY POST /_fault and on DELETE, so a
test that (re-)POSTs its rules always starts from a fresh countdown and cannot inherit a prior
test's count. POST /_fault/reset re-arms the countdown while keeping the same rules — for a test
that wants several `fail_after_n` cycles without re-POSTing.

Modes (matched by `op`, or "all"):
  off           no-op
  error         raise HTTP `status` (default 500)
  slow          await `delay_s` then proceed
  fail_after_n  succeed `after_n` times, then `error` on every later call
  truncate      proceed but tell the handler to cut its body to `truncate_bytes`
  reject        proceed but tell the handler to answer "no" (e.g. can_upload=false)

Process-local, single replica — good enough for the e2e mocks.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import fields

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request


@dataclass
class FaultRule:
    op: str = "all"  # "upload" | "download" | "delete" | "can_upload" | "generate" | "decrypt" | "auth" | "all"
    mode: str = "off"  # off | error | slow | fail_after_n | truncate | reject
    status: int = 500
    delay_s: float = 0.0
    after_n: int = 0
    truncate_bytes: int = 0


@dataclass
class FaultDirective:
    truncate_bytes: int | None = None
    reject: bool = False


def _rule_from_dict(d: dict) -> FaultRule:
    allowed = {f.name for f in fields(FaultRule)}
    return FaultRule(**{k: v for k, v in d.items() if k in allowed})


class FaultController:
    def __init__(self, service: str) -> None:
        self.service = service
        self._rules: list[FaultRule] = []
        self._counts: dict[str, int] = {}
        self._load_env()

    def _load_env(self) -> None:
        raw = os.environ.get("MOCK_FAULT")
        if raw:
            # Swallow a malformed MOCK_FAULT env — boot with no fault rather than crash the mock.
            with contextlib.suppress(Exception):
                data = json.loads(raw)
                self.set_rules(data if isinstance(data, list) else [data])
        mode = os.environ.get("MOCK_FAULT_MODE")
        if mode:
            self._rules.append(
                FaultRule(
                    op=os.environ.get("MOCK_FAULT_OP", "all"),
                    mode=mode,
                    status=int(os.environ.get("MOCK_FAULT_STATUS", "500")),
                    delay_s=float(os.environ.get("MOCK_FAULT_DELAY_S", "0")),
                    after_n=int(os.environ.get("MOCK_FAULT_AFTER_N", "0")),
                    truncate_bytes=int(os.environ.get("MOCK_FAULT_TRUNCATE_BYTES", "0")),
                )
            )

    def set_rules(self, rules: list[dict]) -> None:
        self._rules = [_rule_from_dict(r) for r in rules]
        self._counts.clear()

    def clear(self) -> None:
        self._rules = []
        self._counts.clear()

    def reset_counts(self) -> None:
        """Zero the per-op call counters WITHOUT touching the active rules.

        `fail_after_n` counts up in `self._counts` and only ever resets on set_rules/clear, so a
        second test reusing the SAME rules (without re-POSTing) would inherit the first test's count
        and trip the fault early. This lets a test re-arm the countdown between phases explicitly.
        """
        self._counts.clear()

    def _match(self, op: str) -> FaultRule | None:
        for r in self._rules:
            if r.mode != "off" and r.op in (op, "all"):
                return r
        return None

    async def gate(self, op: str) -> FaultDirective:
        rule = self._match(op)
        if rule is None:
            return FaultDirective()
        count = self._counts.get(op, 0) + 1
        self._counts[op] = count

        if rule.mode == "slow":
            await asyncio.sleep(rule.delay_s)
            return FaultDirective()
        if rule.mode == "error":
            raise HTTPException(status_code=rule.status, detail=f"injected fault ({self.service}:{op})")
        if rule.mode == "fail_after_n":
            if count > rule.after_n:
                raise HTTPException(status_code=rule.status, detail=f"injected fail_after_n ({self.service}:{op})")
            return FaultDirective()
        if rule.mode == "truncate":
            return FaultDirective(truncate_bytes=rule.truncate_bytes)
        if rule.mode == "reject":
            return FaultDirective(reject=True)
        return FaultDirective()


def install_fault_controller(app: FastAPI, service: str) -> FaultController:
    ctl = FaultController(service)

    @app.get("/_fault")
    async def _get_fault() -> dict:
        return {"service": service, "rules": [asdict(r) for r in ctl._rules]}

    @app.post("/_fault")
    async def _set_fault(request: Request) -> dict:
        body = await request.json()
        rules = body if isinstance(body, list) else [body]
        ctl.set_rules(rules)
        return {"ok": True, "rules": [asdict(r) for r in ctl._rules]}

    @app.delete("/_fault")
    async def _clear_fault() -> dict:
        ctl.clear()
        return {"ok": True}

    @app.post("/_fault/reset")
    async def _reset_counts() -> dict:
        # Re-arm fail_after_n countdowns without disturbing the active rules.
        ctl.reset_counts()
        return {"ok": True}

    return ctl
