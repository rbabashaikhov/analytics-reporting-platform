"""CDM Geo / AI traffic marts."""

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
    add_display_order,
    parse_month_start,
)
from analytics_reporting_platform.cdm.repository import PersistCdmResult, persist_cdm_month, persist_cdm_window
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

SECTION_LIST: Final[tuple[str, ...]] = (
    "support",
    "mx",
    "explore",
    "he",
    "da",
    "other",
)

GEO_KPI_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "geo_visits",
    "mom_pct_change",
    "yoy_pct_change",
    "cdm_dt",
)
GEO_KPI_KEYS: Final[tuple[str, ...]] = ("month",)

GEO_TREND_OUTPUT: Final[tuple[str, ...]] = ("month", "geo_visits", "cdm_dt")
GEO_TREND_KEYS: Final[tuple[str, ...]] = ("month",)

GEO_SECTION_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "section_name",
    "section_visits",
    "section_share",
    "mom_pp_change",
    "display_order",
    "cdm_dt",
)
GEO_SECTION_KEYS: Final[tuple[str, ...]] = ("month", "section_name")


def build_geo_traffic_kpi(
    calc_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    month_start = parse_month_start(month)
    sql = text(
        """
        SELECT month, metric_value AS geo_visits, mom_pct_change, yoy_pct_change
        FROM calc_monthly_kpi
        WHERE month = :month AND segment_name = 'ai' AND metric_name = 'visits'
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_geo_traffic_kpi source returned no rows for month")
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = frame.loc[:, list(GEO_KPI_OUTPUT)].reset_index(drop=True)
    if out.duplicated(subset=list(GEO_KPI_KEYS)).any():
        raise CdmError("cdm_geo_traffic_kpi duplicate business keys")
    return CdmMartResult(mart="geo_traffic_kpi", dataframe=out, month=month_start)


def persist_geo_traffic_kpi(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="geo_traffic_kpi",
        table="cdm_geo_traffic_kpi",
        output_columns=GEO_KPI_OUTPUT,
        business_key_columns=GEO_KPI_KEYS,
        required_non_null=("month", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_geo_traffic_trend(
    raw_engine: Engine,
    period: CdmPeriodConfig,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    sql = text(
        """
        SELECT month, metric_value AS geo_visits
        FROM raw_aa_monthly_metrics
        WHERE month BETWEEN :start_month AND :end_month
          AND segment_name = 'ai' AND metric_name = 'visits'
        ORDER BY month
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"start_month": period.trend_start, "end_month": period.report_end},
        )
    if df.empty:
        raise CdmError("cdm_geo_traffic_trend source returned no rows")
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = frame.loc[:, list(GEO_TREND_OUTPUT)].sort_values("month").reset_index(drop=True)
    if out.duplicated(subset=list(GEO_TREND_KEYS)).any():
        raise CdmError("cdm_geo_traffic_trend duplicate business keys")
    return CdmMartResult(
        mart="geo_traffic_trend",
        dataframe=out,
        window_start=period.trend_start,
        window_end=period.report_end,
    )


def persist_geo_traffic_trend(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    period: CdmPeriodConfig,
    *,
    mode: str = "replace",
) -> PersistCdmResult:
    return persist_cdm_window(
        cdm_engine,
        dataframe,
        mart="geo_traffic_trend",
        table="cdm_geo_traffic_trend",
        window_start=period.trend_start,
        window_end=period.report_end,
        output_columns=GEO_TREND_OUTPUT,
        business_key_columns=GEO_TREND_KEYS,
        required_non_null=("month", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_geo_traffic_by_section(
    calc_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    month_start = parse_month_start(month)
    sql = text(
        """
        SELECT month, section_name, section_visits, section_share, mom_pp_change
        FROM calc_monthly_section_share
        WHERE month = :month AND segment_name = 'ai'
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_geo_traffic_by_section source returned no rows for month")
    frame = add_display_order(df, "section_name", SECTION_LIST)
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = (
        frame.loc[:, list(GEO_SECTION_OUTPUT)]
        .sort_values("display_order")
        .reset_index(drop=True)
    )
    if out.duplicated(subset=list(GEO_SECTION_KEYS)).any():
        raise CdmError("cdm_geo_traffic_by_section duplicate business keys")
    return CdmMartResult(mart="geo_traffic_by_section", dataframe=out, month=month_start)


def persist_geo_traffic_by_section(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="geo_traffic_by_section",
        table="cdm_geo_traffic_by_section",
        output_columns=GEO_SECTION_OUTPUT,
        business_key_columns=GEO_SECTION_KEYS,
        required_non_null=("month", "section_name", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


__all__ = [
    "GEO_KPI_KEYS",
    "GEO_KPI_OUTPUT",
    "GEO_SECTION_KEYS",
    "GEO_SECTION_OUTPUT",
    "GEO_TREND_KEYS",
    "GEO_TREND_OUTPUT",
    "SECTION_LIST",
    "build_geo_traffic_by_section",
    "build_geo_traffic_kpi",
    "build_geo_traffic_trend",
    "persist_geo_traffic_by_section",
    "persist_geo_traffic_kpi",
    "persist_geo_traffic_trend",
]
