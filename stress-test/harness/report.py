"""Scenario results + a human/JSON PASS-FAIL report."""

from __future__ import annotations

import dataclasses
import json
import pathlib
import time


@dataclasses.dataclass
class Result:
    name: str
    invariant: str          # which G/S/criterion this asserts
    passed: bool
    detail: str             # one-line evidence (numbers, counts)
    criteria: str           # the pass condition, in words
    skipped: bool = False
    metrics: dict = dataclasses.field(default_factory=dict)


class Report:
    def __init__(self, target: str) -> None:
        self.target = target
        self.results: list[Result] = []
        self.started = time.time()

    def add(self, r: Result) -> Result:
        self.results.append(r)
        tag = "SKIP" if r.skipped else ("PASS" if r.passed else "FAIL")
        print(f"  [{tag}] {r.name} — {r.detail}")
        return r

    @property
    def failed(self) -> list[Result]:
        return [r for r in self.results if not r.passed and not r.skipped]

    @property
    def go(self) -> bool:
        return len(self.failed) == 0

    def summary_line(self) -> str:
        p = sum(1 for r in self.results if r.passed and not r.skipped)
        f = len(self.failed)
        s = sum(1 for r in self.results if r.skipped)
        return f"{p} pass, {f} fail, {s} skip"

    def to_markdown(self) -> str:
        lines = [
            f"# Production-readiness harness run — {self.target}",
            "",
            f"**Verdict: {'🟢 GO' if self.go else '🔴 NO-GO'}** · {self.summary_line()} · "
            f"{time.time() - self.started:.0f}s",
            "",
            "| Result | Scenario | Asserts | Evidence | Criteria |",
            "|---|---|---|---|---|",
        ]
        for r in self.results:
            tag = "⏭️ SKIP" if r.skipped else ("✅ PASS" if r.passed else "❌ FAIL")
            lines.append(
                f"| {tag} | {r.name} | {r.invariant} | {r.detail} | {r.criteria} |"
            )
        return "\n".join(lines) + "\n"

    def dump(self, md_path: pathlib.Path, json_path: pathlib.Path) -> None:
        md_path.write_text(self.to_markdown())
        json_path.write_text(json.dumps(
            {"target": self.target, "go": self.go, "summary": self.summary_line(),
             "results": [dataclasses.asdict(r) for r in self.results]}, indent=2))
