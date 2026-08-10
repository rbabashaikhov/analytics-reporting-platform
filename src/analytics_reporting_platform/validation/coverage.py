"""Period coverage checks for Monthly RAW / CALC / CDM layers."""

from __future__ import annotations

from datetime import date
from typing import Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.validation.specs import (
    CALC_SPECS,
    CDM_SPECS,
    LayerName,
    RAW_SPECS,
    ValidationTableSpec,
)
from analytics_reporting_platform.validation.types import SuiteCheck


def month_range(start: date, end: date) -> list[date]:
    """Inclusive month-start range."""
    if start.day != 1 or end.day != 1:
        raise ValueError("month_range expects month-start dates")
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def _quote(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def _table_exists(engine: Engine, table: str) -> bool:
    sql = text(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table
        )
        """
    )
    with engine.connect() as conn:
        return bool(conn.execute(sql, {"table": table}).scalar_one())


def _present_months(engine: Engine, table: str) -> set[date]:
    sql = text(
        f"""
        SELECT DISTINCT month::date AS m
        FROM {_quote(table)}
        WHERE month IS NOT NULL
        ORDER BY 1
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return {r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])[:10]) for r in rows}


def check_layer_coverage(
    *,
    layer: LayerName,
    engines: dict[LayerName, Engine],
    specs: Sequence[ValidationTableSpec],
    expected_months: Sequence[date],
) -> list[SuiteCheck]:
    """For each mart: every expected month has row_count > 0; report missing/extra."""
    checks: list[SuiteCheck] = []
    expected = set(expected_months)
    engine = engines[layer]

    # Layer-level mart inventory
    expected_tables = {s.table for s in specs}
    present_tables: set[str] = set()
    sql = text(
        """
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE :pat
        """
    )
    prefix = {"raw": "raw_aa_monthly_%", "calc": "calc_%", "cdm": "cdm_%"}[layer]
    with engine.connect() as conn:
        rows = conn.execute(sql, {"pat": prefix}).fetchall()
    # Filter to monthly-ish; weekly guard handles weekly separately.
    for (name,) in rows:
        t = str(name)
        if "weekly" in t:
            continue
        if layer == "calc" and not t.startswith("calc_"):
            continue
        if layer == "cdm" and not t.startswith("cdm_"):
            continue
        if layer == "raw" and not t.startswith("raw_aa_monthly_"):
            continue
        present_tables.add(t)

    missing_marts = sorted(expected_tables - present_tables)
    extra_marts = sorted(present_tables - expected_tables)
    checks.append(
        SuiteCheck(
            category="coverage",
            name=f"coverage.{layer}.mart_inventory",
            status="PASS" if not missing_marts and not extra_marts else "FAIL",
            detail=(
                None
                if not missing_marts and not extra_marts
                else f"present={len(present_tables & expected_tables)}/"
                f"{len(expected_tables)}; missing={','.join(missing_marts) or '-'}; "
                f"extra={','.join(extra_marts) or '-'}"
            ),
        )
    )

    for spec in specs:
        if not _table_exists(engine, spec.table):
            checks.append(
                SuiteCheck(
                    category="coverage",
                    name=f"coverage.{layer}.{spec.mart}.periods",
                    status="FAIL",
                    detail="table missing",
                )
            )
            continue
        present = _present_months(engine, spec.table)
        missing = sorted(expected - present)
        # Extra periods outside expected are WARN for window tables (trend may include 2025),
        # FAIL for month-scoped reporting layers when unexpected future/past gaps matter.
        if spec.scope == "window_contains_month":
            # Must contain all expected; extras allowed (broader trend window).
            ok = not missing
            extra = sorted(present - expected)
            detail_parts = []
            if missing:
                detail_parts.append(
                    "missing=" + ",".join(m.strftime("%Y-%m") for m in missing)
                )
            if extra:
                detail_parts.append(
                    "extra_ok=" + ",".join(m.strftime("%Y-%m") for m in extra[:6])
                    + ("..." if len(extra) > 6 else "")
                )
            # Also verify each expected month has rows > 0 (already implied by DISTINCT).
            empty_expected = []
            for m in expected_months:
                sql_c = text(
                    f"SELECT COUNT(*) FROM {_quote(spec.table)} WHERE month = :m"
                )
                with engine.connect() as conn:
                    n = int(conn.execute(sql_c, {"m": m}).scalar_one())
                if n == 0:
                    empty_expected.append(m.strftime("%Y-%m"))
            if empty_expected:
                ok = False
                detail_parts.append("empty=" + ",".join(empty_expected))
            checks.append(
                SuiteCheck(
                    category="coverage",
                    name=f"coverage.{layer}.{spec.mart}.periods",
                    status="PASS" if ok else "FAIL",
                    detail="; ".join(detail_parts) if detail_parts else (
                        f"expected={len(expected)} present_containing={len(present & expected)}"
                    ),
                )
            )
        else:
            extra = sorted(present - expected)
            empty_expected = []
            for m in expected_months:
                sql_c = text(
                    f"SELECT COUNT(*) FROM {_quote(spec.table)} WHERE month = :m"
                )
                with engine.connect() as conn:
                    n = int(conn.execute(sql_c, {"m": m}).scalar_one())
                if n == 0:
                    empty_expected.append(m.strftime("%Y-%m"))
            ok = not missing and not empty_expected
            detail_parts = []
            if missing or empty_expected:
                detail_parts.append(
                    "missing="
                    + ",".join(sorted(set(
                        [m.strftime("%Y-%m") for m in missing] + empty_expected
                    )))
                )
            if extra:
                detail_parts.append(
                    "extra=" + ",".join(m.strftime("%Y-%m") for m in extra)
                )
                # Extra months on month-scoped reporting tables → WARN (not critical fail)
                checks.append(
                    SuiteCheck(
                        category="coverage",
                        name=f"coverage.{layer}.{spec.mart}.extra_periods",
                        status="WARN",
                        critical=False,
                        detail="extra=" + ",".join(m.strftime("%Y-%m") for m in extra),
                    )
                )
            checks.append(
                SuiteCheck(
                    category="coverage",
                    name=f"coverage.{layer}.{spec.mart}.periods",
                    status="PASS" if ok else "FAIL",
                    detail="; ".join(detail_parts) if detail_parts else (
                        f"expected={len(expected)} present={len(present & expected)}"
                    ),
                )
            )
    return checks


def check_coverage(
    engines: dict[LayerName, Engine],
    *,
    raw_start: date,
    raw_end: date,
    calc_start: date,
    calc_end: date,
    cdm_start: date,
    cdm_end: date,
) -> list[SuiteCheck]:
    """Full Monthly coverage suite across RAW/CALC/CDM."""
    from analytics_reporting_platform.cdm.models import DEFAULT_REPORT_START

    checks: list[SuiteCheck] = []
    adobe_raw = tuple(s for s in RAW_SPECS if s.mart != "wordstat_brand_queries")
    wordstat_raw = tuple(s for s in RAW_SPECS if s.mart == "wordstat_brand_queries")
    checks.extend(
        check_layer_coverage(
            layer="raw",
            engines=engines,
            specs=adobe_raw,
            expected_months=month_range(raw_start, raw_end),
        )
    )
    if wordstat_raw:
        ws_start = max(raw_start, DEFAULT_REPORT_START)
        if ws_start <= raw_end:
            checks.extend(
                check_layer_coverage(
                    layer="raw",
                    engines=engines,
                    specs=wordstat_raw,
                    expected_months=month_range(ws_start, raw_end),
                )
            )
    checks.extend(
        check_layer_coverage(
            layer="calc",
            engines=engines,
            specs=CALC_SPECS,
            expected_months=month_range(calc_start, calc_end),
        )
    )
    checks.extend(
        check_layer_coverage(
            layer="cdm",
            engines=engines,
            specs=CDM_SPECS,
            expected_months=month_range(cdm_start, cdm_end),
        )
    )
    return checks


__all__ = ["check_coverage", "check_layer_coverage", "month_range"]
