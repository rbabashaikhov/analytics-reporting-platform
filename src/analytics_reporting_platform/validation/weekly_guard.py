"""Monthly-only guard: Portfolio Edition execution must not contain Weekly platform tables."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.validation.specs import LayerName
from analytics_reporting_platform.validation.types import SuiteCheck

# Known Weekly physical tables from frozen / legacy inventory (must be absent on Monthly sandbox).
WEEKLY_TABLES: dict[LayerName, tuple[str, ...]] = {
    "raw": (
        "raw_aa_weekly_organic_metrics",
        "raw_aa_weekly_organic_by_section",
        "raw_aa_weekly_organic_pages_by_section",
    ),
    "calc": (
        "calc_organic_weekly_kpi",
        "calc_organic_weekly_by_section",
        "calc_organic_weekly_pages_by_section",
    ),
    "cdm": (
        "cdm_organic_weekly_overview",
        "cdm_organic_weekly_sections",
        "cdm_organic_weekly_pages",
        "cdm_organic_weekly_pages_excel",
        "cdm_organic_explore_buying_guides",
    ),
}


def check_monthly_only(engines: dict[LayerName, Engine]) -> list[SuiteCheck]:
    """FAIL if any Weekly platform table exists in the execution environment."""
    checks: list[SuiteCheck] = []
    for layer, tables in WEEKLY_TABLES.items():
        engine = engines[layer]
        present: list[str] = []
        sql = text(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY(:tables)
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, {"tables": list(tables)}).fetchall()
        present = [str(r[0]) for r in rows]

        # Also catch any other *weekly* tables in public schema.
        sql_any = text(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name ILIKE '%weekly%'
            ORDER BY 1
            """
        )
        with engine.connect() as conn:
            any_weekly = [str(r[0]) for r in conn.execute(sql_any).fetchall()]
        all_present = sorted(set(present) | set(any_weekly))
        checks.append(
            SuiteCheck(
                category="monthly_only",
                name=f"monthly_only.{layer}.no_weekly_tables",
                status="PASS" if not all_present else "FAIL",
                detail=None if not all_present else f"weekly_tables={','.join(all_present)}",
            )
        )
    return checks


__all__ = ["WEEKLY_TABLES", "check_monthly_only"]
