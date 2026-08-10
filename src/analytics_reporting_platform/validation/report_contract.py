"""Report-contract check: last reporting month fully present across all CDM marts."""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.validation.specs import CDM_SPECS
from analytics_reporting_platform.validation.types import SuiteCheck


def check_report_contract(
    *,
    cdm_engine: Engine,
    report_end: date,
) -> list[SuiteCheck]:
    """Verify report_end is represented in all 15 CDM marts (Monthly report contract)."""
    checks: list[SuiteCheck] = []
    missing: list[str] = []
    empty: list[str] = []

    for spec in CDM_SPECS:
        exists_sql = text(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :t
            )
            """
        )
        with cdm_engine.connect() as conn:
            exists = bool(conn.execute(exists_sql, {"t": spec.table}).scalar_one())
        if not exists:
            missing.append(spec.mart)
            continue
        cnt_sql = text(f"SELECT COUNT(*) FROM {spec.table} WHERE month = :m")
        with cdm_engine.connect() as conn:
            n = int(conn.execute(cnt_sql, {"m": report_end}).scalar_one())
        if n == 0:
            empty.append(spec.mart)

    present = len(CDM_SPECS) - len(missing) - len(empty)
    ok = not missing and not empty
    checks.append(
        SuiteCheck(
            category="report_contract",
            name="report_contract.cdm_marts_for_report_end",
            status="PASS" if ok else "FAIL",
            detail=(
                f"report_end={report_end.strftime('%Y-%m')}; "
                f"present={present}/15"
                + (f"; missing_tables={','.join(missing)}" if missing else "")
                + (f"; empty_for_month={','.join(empty)}" if empty else "")
            ),
        )
    )

    # month_type last_month labels on mix tables for report_end
    for table, col in (
        ("cdm_traffic_source_mix", "channel_name"),
        ("cdm_organic_search_engine_mix", "search_engine"),
        ("cdm_sections_mix", "section_name"),
    ):
        sql = text(
            f"""
            SELECT COUNT(*) FROM {table}
            WHERE month = :m AND month_type = 'last_month'
            """
        )
        with cdm_engine.connect() as conn:
            n = int(conn.execute(sql, {"m": report_end}).scalar_one())
        checks.append(
            SuiteCheck(
                category="report_contract",
                name=f"report_contract.{table}.last_month_label",
                status="PASS" if n > 0 else "FAIL",
                detail=None if n > 0 else f"no last_month rows for {report_end.strftime('%Y-%m')}",
            )
        )
    return checks


__all__ = ["check_report_contract"]
