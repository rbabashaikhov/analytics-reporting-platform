"""Read-only Monthly post-run validation harness (not formal DQ)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal, Mapping, Optional, Sequence

from sqlalchemy.engine import Engine

from analytics_reporting_platform.execution.period import (
    ExecutionMonthInput,
    validate_execution_month,
)
from analytics_reporting_platform.validation.signatures import (
    MonthlySignatureBundle,
    TablePeriodSignature,
    capture_monthly_signatures,
)
from analytics_reporting_platform.validation.specs import (
    ALL_SPECS,
    CALC_SPECS,
    CDM_SPECS,
    RAW_SPECS,
    ValidationTableSpec,
)

CheckStatus = Literal["PASS", "FAIL"]
OverallStatus = Literal["PASS", "FAIL"]


@dataclass(frozen=True)
class ValidationCheck:
    name: str
    status: CheckStatus
    detail: Optional[str] = None
    row_count: Optional[int] = None


@dataclass(frozen=True)
class MonthlyValidationResult:
    month: date
    overall: OverallStatus
    checks: tuple[ValidationCheck, ...]

    @property
    def failed_checks(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.checks if c.status == "FAIL")

    def summary_lines(self) -> list[str]:
        by_prefix: dict[str, list[ValidationCheck]] = {
            "RAW": [],
            "CALC": [],
            "CDM": [],
            "Duplicates": [],
            "Scope": [],
            "Other": [],
        }
        for check in self.checks:
            if check.name.startswith("raw."):
                by_prefix["RAW"].append(check)
            elif check.name.startswith("calc."):
                by_prefix["CALC"].append(check)
            elif check.name.startswith("cdm."):
                by_prefix["CDM"].append(check)
            elif check.name.startswith("dup."):
                by_prefix["Duplicates"].append(check)
            elif check.name.startswith("scope."):
                by_prefix["Scope"].append(check)
            else:
                by_prefix["Other"].append(check)

        def group_status(items: list[ValidationCheck]) -> str:
            if not items:
                return "SKIP"
            return "FAIL" if any(i.status == "FAIL" for i in items) else "PASS"

        lines = [f"Month: {self.month.strftime('%Y-%m')}"]
        for label in ("RAW", "CALC", "CDM", "Duplicates", "Scope"):
            lines.append(f"{label}: {group_status(by_prefix[label])}")
        lines.append(f"Overall: {self.overall}")
        if self.failed_checks:
            lines.append("failed_checks=" + ",".join(self.failed_checks))
        for check in self.checks:
            if check.row_count is not None and check.name.endswith(".present"):
                lines.append(f"rows.{check.name[:-8]}={check.row_count}")
        return lines


def _layer_presence_checks(
    *,
    layer: str,
    specs: Sequence[ValidationTableSpec],
    signatures: Mapping[str, TablePeriodSignature],
) -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    for spec in specs:
        sig = signatures.get(spec.table)
        if sig is None:
            checks.append(
                ValidationCheck(
                    name=f"{layer}.{spec.mart}.present",
                    status="FAIL",
                    detail="missing signature for table",
                    row_count=0,
                )
            )
            continue
        ok = sig.row_count > 0
        checks.append(
            ValidationCheck(
                name=f"{layer}.{spec.mart}.present",
                status="PASS" if ok else "FAIL",
                detail=None if ok else "missing target month rows",
                row_count=sig.row_count,
            )
        )
    return checks


def _duplicate_checks(
    signatures: Mapping[str, TablePeriodSignature],
    specs: Sequence[ValidationTableSpec],
) -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    for spec in specs:
        sig = signatures.get(spec.table)
        if sig is None:
            checks.append(
                ValidationCheck(
                    name=f"dup.{spec.layer}.{spec.mart}",
                    status="FAIL",
                    detail="missing signature for table",
                )
            )
            continue
        ok = sig.duplicate_key_groups == 0
        checks.append(
            ValidationCheck(
                name=f"dup.{spec.layer}.{spec.mart}",
                status="PASS" if ok else "FAIL",
                detail=None if ok else f"duplicate_key_groups={sig.duplicate_key_groups}",
                row_count=sig.row_count,
            )
        )
    return checks


def _norm_month_token(value: str) -> str:
    text = str(value).strip()[:10]
    if len(text) == 7 and text[4] == "-":
        return f"{text}-01"
    return text


def compare_monthly_signatures(
    before: MonthlySignatureBundle,
    after: MonthlySignatureBundle,
    *,
    allowed_changed_months: Sequence[str],
    month_scoped_tables: Optional[Sequence[str]] = None,
) -> list[ValidationCheck]:
    """Fail when month row-counts change outside the allowed set.

    By default only **month-scoped** tables are enforced for unrelated-period
    protection. Window/trend tables may rewrite a multi-month range on rebuild;
    those are excluded unless ``month_scoped_tables`` is overridden.
    """
    allowed = {_norm_month_token(m) for m in allowed_changed_months}
    if month_scoped_tables is None:
        month_scoped_tables = tuple(
            s.table for s in ALL_SPECS if s.scope == "month"
        )
    scoped = set(month_scoped_tables)

    before_map = {
        (row["layer"], row["table"], _norm_month_token(str(row["month"]))): int(row["row_count"])
        for row in before.month_counts
        if row["table"] in scoped
    }
    after_map = {
        (row["layer"], row["table"], _norm_month_token(str(row["month"]))): int(row["row_count"])
        for row in after.month_counts
        if row["table"] in scoped
    }
    keys = set(before_map) | set(after_map)
    unexpected: list[str] = []
    for key in sorted(keys):
        layer, table, month = key
        b = before_map.get(key, 0)
        a = after_map.get(key, 0)
        if b == a:
            continue
        if month in allowed:
            continue
        unexpected.append(f"{layer}.{table}.{month[:7]}:{b}->{a}")

    if unexpected:
        return [
            ValidationCheck(
                name="scope.unrelated_periods",
                status="FAIL",
                detail="unexpected_changes=" + ";".join(unexpected[:20]),
            )
        ]
    return [
        ValidationCheck(
            name="scope.unrelated_periods",
            status="PASS",
            detail="no unexpected period count changes on month-scoped tables",
        )
    ]


def validate_monthly_raw_history(
    *,
    raw_engine: Engine,
    month: ExecutionMonthInput,
) -> MonthlyValidationResult:
    """Validate RAW Monthly history only (no CALC/CDM expectations).

    Used for 2025 lookback months that exist solely to support 2026 MoM/YoY.
    """
    target = validate_execution_month(month, allow_future=True)
    bundle = capture_monthly_signatures(
        raw_engine=raw_engine,
        calc_engine=raw_engine,  # unused — specs are RAW-only
        cdm_engine=raw_engine,
        month=target,
        specs=tuple(s for s in RAW_SPECS if s.mart != "wordstat_brand_queries"),
    )
    by_table = {t.table: t for t in bundle.tables}
    adobe_raw = [s for s in RAW_SPECS if s.mart != "wordstat_brand_queries"]
    checks: list[ValidationCheck] = []
    checks.extend(
        _layer_presence_checks(layer="raw", specs=adobe_raw, signatures=by_table)
    )
    checks.extend(_duplicate_checks(by_table, adobe_raw))
    overall: OverallStatus = "FAIL" if any(c.status == "FAIL" for c in checks) else "PASS"
    return MonthlyValidationResult(month=target, overall=overall, checks=tuple(checks))


def validate_monthly_period(
    *,
    raw_engine: Engine,
    calc_engine: Engine,
    cdm_engine: Engine,
    month: ExecutionMonthInput,
    before: Optional[MonthlySignatureBundle] = None,
    after: Optional[MonthlySignatureBundle] = None,
    specs: Sequence[ValidationTableSpec] = ALL_SPECS,
) -> MonthlyValidationResult:
    """Run read-only presence/duplicate/(optional) scope checks for one month."""
    target = validate_execution_month(month, allow_future=True)
    bundle = after or capture_monthly_signatures(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        cdm_engine=cdm_engine,
        month=target,
        specs=specs,
    )
    by_table = {t.table: t for t in bundle.tables}

    checks: list[ValidationCheck] = []
    # Prefer explicit layer slices from ``specs``; empty slice means "skip layer"
    # only when specs was customized. Default ALL_SPECS includes every layer.
    if specs is ALL_SPECS:
        raw_specs = list(RAW_SPECS)
        calc_specs = list(CALC_SPECS)
        cdm_specs = list(CDM_SPECS)
    else:
        raw_specs = [s for s in specs if s.layer == "raw"]
        calc_specs = [s for s in specs if s.layer == "calc"]
        cdm_specs = [s for s in specs if s.layer == "cdm"]

    if raw_specs:
        checks.extend(
            _layer_presence_checks(layer="raw", specs=raw_specs, signatures=by_table)
        )
    if calc_specs:
        checks.extend(
            _layer_presence_checks(layer="calc", specs=calc_specs, signatures=by_table)
        )
    if cdm_specs:
        checks.extend(
            _layer_presence_checks(layer="cdm", specs=cdm_specs, signatures=by_table)
        )
    checks.extend(
        _duplicate_checks(by_table, list(raw_specs) + list(calc_specs) + list(cdm_specs))
    )

    if before is not None:
        effective_after = bundle
        checks.extend(
            compare_monthly_signatures(
                before,
                effective_after,
                allowed_changed_months=(target.isoformat(), target.strftime("%Y-%m")),
            )
        )

    overall: OverallStatus = "FAIL" if any(c.status == "FAIL" for c in checks) else "PASS"
    return MonthlyValidationResult(month=target, overall=overall, checks=tuple(checks))


__all__ = [
    "MonthlyValidationResult",
    "ValidationCheck",
    "compare_monthly_signatures",
    "validate_monthly_period",
    "validate_monthly_raw_history",
]
