"""Data-quality framework for events (same design as Project 1, event rules).

OOP rule objects (``QualityCheck``) plug into a ``QualitySuite`` that aggregates
a pass/fail ``QualityReport``. Used to gate events before they hit the stores.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Iterable, Sequence


@dataclass
class CheckResult:
    name: str
    passed: bool
    failed_rows: int
    total_rows: int
    severity: str
    detail: str = ""

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total_rows == 0 else round(1 - self.failed_rows / self.total_rows, 4)


class QualityCheck(ABC):
    severity: str = "error"

    def __init__(self, severity: str | None = None) -> None:
        if severity:
            self.severity = severity

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def run(self, rows: Sequence[dict]) -> CheckResult: ...

    def _result(self, rows: Sequence[dict], failed: int, detail: str = "") -> CheckResult:
        return CheckResult(self.name, failed == 0, failed, len(rows), self.severity, detail)


class NotNullCheck(QualityCheck):
    def __init__(self, columns: list[str], **kw) -> None:
        super().__init__(**kw)
        self.columns = columns

    @property
    def name(self) -> str:
        return f"not_null({', '.join(self.columns)})"

    def run(self, rows: Sequence[dict]) -> CheckResult:
        failed = sum(1 for r in rows
                     if any(r.get(c) is None or r.get(c) == "" for c in self.columns))
        return self._result(rows, failed)


class AllowedValuesCheck(QualityCheck):
    def __init__(self, column: str, allowed: Iterable[Any], **kw) -> None:
        super().__init__(**kw)
        self.column = column
        self.allowed = set(allowed)

    @property
    def name(self) -> str:
        return f"allowed_values({self.column})"

    def run(self, rows: Sequence[dict]) -> CheckResult:
        failed = sum(1 for r in rows if r.get(self.column) not in self.allowed)
        return self._result(rows, failed)


class RangeCheck(QualityCheck):
    def __init__(self, column: str, min_value: float | None = None,
                 max_value: float | None = None, **kw) -> None:
        super().__init__(**kw)
        self.column, self.min_value, self.max_value = column, min_value, max_value

    @property
    def name(self) -> str:
        return f"range({self.column})"

    def run(self, rows: Sequence[dict]) -> CheckResult:
        failed = 0
        for r in rows:
            v = r.get(self.column)
            if v is None:
                continue
            if (self.min_value is not None and v < self.min_value) or \
               (self.max_value is not None and v > self.max_value):
                failed += 1
        return self._result(rows, failed)


class UniqueCheck(QualityCheck):
    def __init__(self, column: str, **kw) -> None:
        super().__init__(**kw)
        self.column = column

    @property
    def name(self) -> str:
        return f"unique({self.column})"

    def run(self, rows: Sequence[dict]) -> CheckResult:
        seen: set = set()
        dups = 0
        for r in rows:
            k = r.get(self.column)
            if k in seen:
                dups += 1
            seen.add(k)
        return self._result(rows, dups)


@dataclass
class QualityReport:
    total_rows: int
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if r.severity == "error")

    def to_dict(self) -> dict:
        return {"total_rows": self.total_rows, "passed": self.passed,
                "checks": [asdict(r) | {"pass_rate": r.pass_rate} for r in self.results]}

    def save(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")

    def pretty(self) -> str:
        lines = [f"Event quality - {self.total_rows} rows - "
                 f"{'PASS' if self.passed else 'FAIL'}"]
        for r in self.results:
            mark = "ok " if r.passed else ("ERR" if r.severity == "error" else "warn")
            lines.append(f"  [{mark}] {r.name:<30} {r.failed_rows} failed ({r.pass_rate:.1%})")
        return "\n".join(lines)


class QualitySuite:
    def __init__(self, checks: list[QualityCheck]) -> None:
        self.checks = checks

    def run(self, rows: Sequence[dict]) -> QualityReport:
        report = QualityReport(total_rows=len(rows))
        for check in self.checks:
            report.results.append(check.run(rows))
        return report


def default_event_suite() -> QualitySuite:
    return QualitySuite([
        NotNullCheck(["event_id", "timestamp", "service"]),
        AllowedValuesCheck("level", ["DEBUG", "INFO", "WARN", "ERROR"]),
        RangeCheck("status_code", min_value=100, max_value=599),
        RangeCheck("latency_ms", min_value=0, max_value=60000, severity="warning"),
        UniqueCheck("event_id"),
    ])
