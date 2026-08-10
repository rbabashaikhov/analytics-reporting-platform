"""CDM Explore & Buying Guides marts."""

from __future__ import annotations

from datetime import datetime
from typing import Final, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.cdm.models import (
    CdmError,
    CdmMartResult,
    CdmPeriodConfig,
    MonthInput,
    parse_month_start,
)
from analytics_reporting_platform.cdm.repository import PersistCdmResult, persist_cdm_month, persist_cdm_window
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

EXPLORE_BG_SECTIONS: Final[tuple[str, ...]] = ("explore", "buying_guides")

EXPLORE_BG_KPI_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "section_name",
    "kpi_name",
    "kpi_value",
    "mom_pct_change",
    "yoy_pct_change",
    "display_order",
    "cdm_dt",
)
EXPLORE_BG_KPI_KEYS: Final[tuple[str, ...]] = ("month", "section_name", "kpi_name")

EXPLORE_BG_TREND_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "explore_organic_visits",
    "buying_guides_organic_visits",
    "total_organic_visits",
    "cdm_dt",
)
EXPLORE_BG_TREND_KEYS: Final[tuple[str, ...]] = ("month",)


def build_explore_buying_guides_kpi(
    calc_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    month_start = parse_month_start(month)
    sections_sql = ", ".join(f"'{s}'" for s in EXPLORE_BG_SECTIONS)
    sql = text(
        f"""
        SELECT month, section_name, kpi_name, kpi_value,
               mom_pct_change, yoy_pct_change, display_order
        FROM calc_explore_buying_guides_kpi
        WHERE month = :month
          AND section_name IN ({sections_sql})
          AND kpi_name IN ('total_visits', 'organic_visits')
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_explore_buying_guides_kpi source returned no rows for month")
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["kpi_value"] = pd.to_numeric(frame["kpi_value"], errors="coerce")
    frame["mom_pct_change"] = pd.to_numeric(frame["mom_pct_change"], errors="coerce")
    frame["yoy_pct_change"] = pd.to_numeric(frame["yoy_pct_change"], errors="coerce")
    frame["display_order"] = pd.to_numeric(frame["display_order"], errors="coerce").astype("Int64")
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = (
        frame.loc[:, list(EXPLORE_BG_KPI_OUTPUT)]
        .sort_values(["section_name", "display_order"])
        .reset_index(drop=True)
    )
    if out.duplicated(subset=list(EXPLORE_BG_KPI_KEYS)).any():
        raise CdmError("cdm_explore_buying_guides_kpi duplicate business keys")
    return CdmMartResult(mart="explore_buying_guides_kpi", dataframe=out, month=month_start)


def persist_explore_buying_guides_kpi(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="explore_buying_guides_kpi",
        table="cdm_explore_buying_guides_kpi",
        output_columns=EXPLORE_BG_KPI_OUTPUT,
        business_key_columns=EXPLORE_BG_KPI_KEYS,
        required_non_null=("month", "section_name", "kpi_name", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_explore_buying_guides_trend(
    raw_engine: Engine,
    period: CdmPeriodConfig,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    sql = text(
        """
        SELECT month, section_name, segment_name, metric_value
        FROM raw_aa_monthly_by_section
        WHERE month BETWEEN :start_month AND :end_month
          AND section_name IN ('explore', 'buying_guides')
          AND segment_name = 'organic'
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"start_month": period.trend_start, "end_month": period.report_end},
        )
    if df.empty:
        raise CdmError("cdm_explore_buying_guides_trend source returned no rows")
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    pivot = (
        frame.pivot_table(
            index="month",
            columns="section_name",
            values="metric_value",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    if "explore" not in pivot.columns:
        pivot["explore"] = 0
    if "buying_guides" not in pivot.columns:
        pivot["buying_guides"] = 0
    result = pd.DataFrame(
        {
            "month": pivot["month"].dt.date,
            "explore_organic_visits": pivot["explore"],
            "buying_guides_organic_visits": pivot["buying_guides"],
        }
    )
    result["total_organic_visits"] = (
        result["explore_organic_visits"] + result["buying_guides_organic_visits"]
    )
    result["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = result.loc[:, list(EXPLORE_BG_TREND_OUTPUT)].sort_values("month").reset_index(drop=True)
    if out.duplicated(subset=list(EXPLORE_BG_TREND_KEYS)).any():
        raise CdmError("cdm_explore_buying_guides_trend duplicate business keys")
    return CdmMartResult(
        mart="explore_buying_guides_trend",
        dataframe=out,
        window_start=period.trend_start,
        window_end=period.report_end,
    )


def persist_explore_buying_guides_trend(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    period: CdmPeriodConfig,
    *,
    mode: str = "replace",
) -> PersistCdmResult:
    return persist_cdm_window(
        cdm_engine,
        dataframe,
        mart="explore_buying_guides_trend",
        table="cdm_explore_buying_guides_trend",
        window_start=period.trend_start,
        window_end=period.report_end,
        output_columns=EXPLORE_BG_TREND_OUTPUT,
        business_key_columns=EXPLORE_BG_TREND_KEYS,
        required_non_null=("month", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


__all__ = [
    "EXPLORE_BG_KPI_KEYS",
    "EXPLORE_BG_KPI_OUTPUT",
    "EXPLORE_BG_SECTIONS",
    "EXPLORE_BG_TREND_KEYS",
    "EXPLORE_BG_TREND_OUTPUT",
    "build_explore_buying_guides_kpi",
    "build_explore_buying_guides_trend",
    "persist_explore_buying_guides_kpi",
    "persist_explore_buying_guides_trend",
]
