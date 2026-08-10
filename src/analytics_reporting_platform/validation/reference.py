"""Read-only reference (frozen) comparison — secondary diagnostic only.

Statuses: MATCH / DIFFERENCE / NOT_COMPARABLE.
A DIFFERENCE alone must not force FINAL FAIL when internal reconciliation is clean.
"""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Literal, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.validation.coverage import month_range
from analytics_reporting_platform.validation.specs import ALL_SPECS, LayerName, ValidationTableSpec
from analytics_reporting_platform.validation.types import SuiteCheck

RefStatus = Literal["MATCH", "DIFFERENCE", "NOT_COMPARABLE"]


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
        f"SELECT DISTINCT month::date FROM {_quote(table)} WHERE month IS NOT NULL"
    )
    with engine.connect() as conn:
        return {
            r[0] if isinstance(r[0], date) else date.fromisoformat(str(r[0])[:10])
            for r in conn.execute(sql).fetchall()
        }


def _month_count(engine: Engine, table: str, month: date) -> int:
    sql = text(f"SELECT COUNT(*) FROM {_quote(table)} WHERE month = :m")
    with engine.connect() as conn:
        return int(conn.execute(sql, {"m": month}).scalar_one())


def _key_fingerprint(
    engine: Engine, table: str, month: date, business_key: Sequence[str]
) -> str:
    """Opaque digest of business keys only (no measures / no URLs in output)."""
    cols = ", ".join(_quote(c) for c in business_key)
    sql = text(
        f"""
        SELECT {cols} FROM {_quote(table)}
        WHERE month = :m ORDER BY {cols}
        """
    )
    digest = hashlib.sha256()
    with engine.connect() as conn:
        rows = conn.execute(sql, {"m": month}).fetchall()
    digest.update(f"n={len(rows)}|".encode())
    for row in rows:
        digest.update(("|".join("" if v is None else str(v) for v in row) + "\n").encode())
    return digest.hexdigest()


def compare_to_reference(
    *,
    exec_engines: dict[LayerName, Engine],
    ref_engines: Optional[dict[LayerName, Engine]],
    overlap_start: date,
    overlap_end: date,
    specs: Sequence[ValidationTableSpec] = ALL_SPECS,
) -> list[SuiteCheck]:
    """Compare overlapping periods between execution and frozen reference DBs."""
    checks: list[SuiteCheck] = []
    if ref_engines is None:
        checks.append(
            SuiteCheck(
                category="reference_comparison",
                name="reference.availability",
                status="WARN",
                critical=False,
                detail="reference engines not provided; skipped",
            )
        )
        return checks

    # Connectivity smoke
    try:
        for layer, eng in ref_engines.items():
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
        checks.append(
            SuiteCheck(
                category="reference_comparison",
                name="reference.availability",
                status="PASS",
                critical=False,
                detail="frozen reference reachable read-only",
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            SuiteCheck(
                category="reference_comparison",
                name="reference.availability",
                status="WARN",
                critical=False,
                detail=f"NOT_COMPARABLE: {type(exc).__name__}",
            )
        )
        return checks

    months = month_range(overlap_start, overlap_end)
    match_n = diff_n = nc_n = 0

    for spec in specs:
        exec_eng = exec_engines[spec.layer]
        ref_eng = ref_engines[spec.layer]
        if not _table_exists(exec_eng, spec.table) or not _table_exists(ref_eng, spec.table):
            nc_n += 1
            checks.append(
                SuiteCheck(
                    category="reference_comparison",
                    name=f"reference.{spec.layer}.{spec.mart}",
                    status="WARN",
                    critical=False,
                    detail="NOT_COMPARABLE: table missing on exec or reference",
                )
            )
            continue

        exec_months = _present_months(exec_eng, spec.table)
        ref_months = _present_months(ref_eng, spec.table)
        overlap = [m for m in months if m in exec_months and m in ref_months]
        if not overlap:
            nc_n += 1
            checks.append(
                SuiteCheck(
                    category="reference_comparison",
                    name=f"reference.{spec.layer}.{spec.mart}",
                    status="WARN",
                    critical=False,
                    detail="NOT_COMPARABLE: no overlapping months in range",
                )
            )
            continue

        month_diffs = 0
        for m in overlap:
            ec = _month_count(exec_eng, spec.table, m)
            rc = _month_count(ref_eng, spec.table, m)
            if ec != rc:
                month_diffs += 1
                continue
            # For top_pages / URL tables: compare counts only (already done) —
            # key fingerprint includes page_url internally but is not logged.
            ef = _key_fingerprint(exec_eng, spec.table, m, spec.business_key)
            rf = _key_fingerprint(ref_eng, spec.table, m, spec.business_key)
            if ef != rf:
                month_diffs += 1

        if month_diffs == 0:
            match_n += 1
            status_label: RefStatus = "MATCH"
            check_status = "PASS"
        else:
            diff_n += 1
            status_label = "DIFFERENCE"
            check_status = "WARN"  # secondary diagnostic — not critical FAIL
        checks.append(
            SuiteCheck(
                category="reference_comparison",
                name=f"reference.{spec.layer}.{spec.mart}",
                status=check_status,  # type: ignore[arg-type]
                critical=False,
                detail=(
                    f"{status_label}; overlap_months={len(overlap)}; "
                    f"diff_months={month_diffs}"
                ),
            )
        )

    checks.append(
        SuiteCheck(
            category="reference_comparison",
            name="reference.summary",
            status="PASS" if diff_n == 0 and nc_n == 0 else "WARN",
            critical=False,
            detail=f"MATCH={match_n}; DIFFERENCE={diff_n}; NOT_COMPARABLE={nc_n}",
        )
    )
    return checks


__all__ = ["RefStatus", "compare_to_reference"]
