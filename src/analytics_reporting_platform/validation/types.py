"""Shared result types for the Monthly DWH validation suite."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional

CheckStatus = Literal["PASS", "FAIL", "WARN", "SKIP"]
CategoryStatus = Literal["PASS", "FAIL", "WARN", "SKIP"]
FinalStatus = Literal["PASS", "FAIL"]

CategoryName = Literal[
    "coverage",
    "contracts",
    "uniqueness",
    "value_ranges",
    "raw_calc_reconciliation",
    "calc_cdm_reconciliation",
    "report_contract",
    "reference_comparison",
    "monthly_only",
]


@dataclass(frozen=True)
class SuiteCheck:
    """One atomic validation check (safe for logs: no secrets / no raw KPI dumps)."""

    category: CategoryName
    name: str
    status: CheckStatus
    detail: Optional[str] = None
    critical: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CategoryResult:
    """Aggregated status for one report category."""

    name: CategoryName
    status: CategoryStatus
    passed: int
    total: int
    checks: list[SuiteCheck] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "passed": self.passed,
            "total": self.total,
            "checks": [c.to_dict() for c in self.checks],
            "failed": [c.to_dict() for c in self.checks if c.status == "FAIL"],
            "warnings": [c.to_dict() for c in self.checks if c.status == "WARN"],
        }


@dataclass
class SuiteResult:
    """Full Monthly DWH validation suite outcome."""

    final_status: FinalStatus
    report_end: str
    categories: list[CategoryResult]
    generated_at: str
    read_only: bool = True
    execution_port: Optional[int] = None
    reference_port: Optional[int] = None

    def failed_critical(self) -> list[SuiteCheck]:
        out: list[SuiteCheck] = []
        for cat in self.categories:
            for check in cat.checks:
                if check.status == "FAIL" and check.critical:
                    out.append(check)
        return out

    def warnings(self) -> list[SuiteCheck]:
        out: list[SuiteCheck] = []
        for cat in self.categories:
            for check in cat.checks:
                if check.status == "WARN":
                    out.append(check)
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "final_status": self.final_status,
            "report_end": self.report_end,
            "generated_at": self.generated_at,
            "read_only": self.read_only,
            "execution_port": self.execution_port,
            "reference_port": self.reference_port,
            "categories": [c.to_dict() for c in self.categories],
            "failed_critical": [c.to_dict() for c in self.failed_critical()],
            "warnings": [c.to_dict() for c in self.warnings()],
        }


def summarize_category(name: CategoryName, checks: list[SuiteCheck]) -> CategoryResult:
    """PASS if no FAIL; WARN if any WARN and no FAIL; SKIP if empty."""
    if not checks:
        return CategoryResult(name=name, status="SKIP", passed=0, total=0, checks=[])
    total = len(checks)
    passed = sum(1 for c in checks if c.status == "PASS")
    if any(c.status == "FAIL" for c in checks):
        status: CategoryStatus = "FAIL"
    elif any(c.status == "WARN" for c in checks):
        status = "WARN"
    else:
        status = "PASS"
    return CategoryResult(name=name, status=status, passed=passed, total=total, checks=checks)


__all__ = [
    "CategoryName",
    "CategoryResult",
    "CategoryStatus",
    "CheckStatus",
    "FinalStatus",
    "SuiteCheck",
    "SuiteResult",
    "summarize_category",
]
