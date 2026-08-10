"""CDM Organic Search marts."""

from __future__ import annotations

from datetime import date, datetime
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
    month_type_label,
    parse_month_start,
)
from analytics_reporting_platform.cdm.repository import PersistCdmResult, persist_cdm_month, persist_cdm_window
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

ORGANIC_KPI_ORDER: Final[tuple[str, ...]] = (
    "organic_visits",
    "google_visits",
    "yandex_visits",
)
SEARCH_ENGINE_ORDER: Final[tuple[str, ...]] = (
    "organic_total",
    "yandex",
    "google",
    "other_search_engines",
)

ORGANIC_KPI_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "kpi_name",
    "kpi_value",
    "mom_pct_change",
    "yoy_pct_change",
    "display_order",
    "cdm_dt",
)
ORGANIC_KPI_KEYS: Final[tuple[str, ...]] = ("month", "kpi_name")

ENGINE_MIX_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "month_type",
    "search_engine",
    "search_engine_visits",
    "search_engine_share",
    "mom_pp_change",
    "display_order",
    "cdm_dt",
)
ENGINE_MIX_KEYS: Final[tuple[str, ...]] = ("month", "search_engine")

ENGINE_TREND_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "google_visits",
    "yandex_visits",
    "other_search_engines_visits",
    "organic_total_visits",
    "cdm_dt",
)
ENGINE_TREND_KEYS: Final[tuple[str, ...]] = ("month",)


def build_organic_search_kpi(
    calc_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    month_start = parse_month_start(month)
    sql = text(
        """
        SELECT month, segment_name, metric_value, mom_pct_change, yoy_pct_change
        FROM calc_monthly_kpi
        WHERE month = :month
          AND metric_name = 'visits'
          AND segment_name IN ('organic', 'google', 'yandex')
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_organic_search_kpi source returned no rows for month")
    frame = df.copy()
    frame["kpi_name"] = frame["segment_name"].map(
        {"organic": "organic_visits", "google": "google_visits", "yandex": "yandex_visits"}
    )
    frame = frame[frame["kpi_name"].notna()].copy()
    frame["kpi_value"] = frame["metric_value"]
    frame = add_display_order(frame, "kpi_name", ORGANIC_KPI_ORDER)
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = (
        frame.loc[:, list(ORGANIC_KPI_OUTPUT)]
        .sort_values("display_order")
        .reset_index(drop=True)
    )
    if out.duplicated(subset=list(ORGANIC_KPI_KEYS)).any():
        raise CdmError("cdm_organic_search_kpi duplicate business keys")
    missing = sorted(set(ORGANIC_KPI_ORDER) - set(out["kpi_name"]))
    if missing:
        raise CdmError("cdm_organic_search_kpi missing kpi(s): " + ", ".join(missing))
    return CdmMartResult(mart="organic_search_kpi", dataframe=out, month=month_start)


def persist_organic_search_kpi(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="organic_search_kpi",
        table="cdm_organic_search_kpi",
        output_columns=ORGANIC_KPI_OUTPUT,
        business_key_columns=ORGANIC_KPI_KEYS,
        required_non_null=("month", "kpi_name", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_organic_search_engine_mix(
    calc_engine: Engine,
    month: MonthInput,
    *,
    report_end: MonthInput,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    month_start = parse_month_start(month)
    end = parse_month_start(report_end)
    sql = text(
        """
        SELECT month, search_engine, search_engine_visits, search_engine_share, mom_pp_change
        FROM calc_monthly_search_engines
        WHERE month = :month
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_organic_search_engine_mix source returned no rows for month")
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    frame["month_type"] = frame["month"].dt.date.map(lambda m: month_type_label(m, end))
    frame = add_display_order(frame, "search_engine", SEARCH_ENGINE_ORDER)
    frame["month"] = frame["month"].dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = (
        frame.loc[:, list(ENGINE_MIX_OUTPUT)]
        .sort_values("display_order")
        .reset_index(drop=True)
    )
    if out.duplicated(subset=list(ENGINE_MIX_KEYS)).any():
        raise CdmError("cdm_organic_search_engine_mix duplicate business keys")
    return CdmMartResult(mart="organic_search_engine_mix", dataframe=out, month=month_start)


def persist_organic_search_engine_mix(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="organic_search_engine_mix",
        table="cdm_organic_search_engine_mix",
        output_columns=ENGINE_MIX_OUTPUT,
        business_key_columns=ENGINE_MIX_KEYS,
        required_non_null=("month", "search_engine", "month_type", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_organic_search_engine_trend(
    raw_engine: Engine,
    period: CdmPeriodConfig,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    sql = text(
        """
        SELECT month, segment_name, metric_value
        FROM raw_aa_monthly_metrics
        WHERE month BETWEEN :start_month AND :end_month
          AND metric_name = 'visits'
          AND segment_name IN ('organic', 'google', 'yandex')
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"start_month": period.trend_start, "end_month": period.report_end},
        )
    if df.empty:
        raise CdmError("cdm_organic_search_engine_trend source returned no rows")
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    p = (
        frame.pivot_table(
            index="month", columns="segment_name", values="metric_value", aggfunc="sum"
        )
        .reset_index()
    )
    for c in ("organic", "google", "yandex"):
        if c not in p.columns:
            p[c] = 0.0
    p["other_search_engines_visits"] = (p["organic"] - p["google"] - p["yandex"]).clip(lower=0)
    p = p.rename(
        columns={
            "google": "google_visits",
            "yandex": "yandex_visits",
            "organic": "organic_total_visits",
        }
    )
    p["month"] = p["month"].dt.date
    p["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = p.loc[:, list(ENGINE_TREND_OUTPUT)].sort_values("month").reset_index(drop=True)
    if out.duplicated(subset=list(ENGINE_TREND_KEYS)).any():
        raise CdmError("cdm_organic_search_engine_trend duplicate business keys")
    return CdmMartResult(
        mart="organic_search_engine_trend",
        dataframe=out,
        window_start=period.trend_start,
        window_end=period.report_end,
    )


def persist_organic_search_engine_trend(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    period: CdmPeriodConfig,
    *,
    mode: str = "replace",
) -> PersistCdmResult:
    return persist_cdm_window(
        cdm_engine,
        dataframe,
        mart="organic_search_engine_trend",
        table="cdm_organic_search_engine_trend",
        window_start=period.trend_start,
        window_end=period.report_end,
        output_columns=ENGINE_TREND_OUTPUT,
        business_key_columns=ENGINE_TREND_KEYS,
        required_non_null=("month", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


__all__ = [
    "ENGINE_MIX_KEYS",
    "ENGINE_MIX_OUTPUT",
    "ENGINE_TREND_KEYS",
    "ENGINE_TREND_OUTPUT",
    "ORGANIC_KPI_KEYS",
    "ORGANIC_KPI_ORDER",
    "ORGANIC_KPI_OUTPUT",
    "SEARCH_ENGINE_ORDER",
    "build_organic_search_engine_mix",
    "build_organic_search_engine_trend",
    "build_organic_search_kpi",
    "persist_organic_search_engine_mix",
    "persist_organic_search_engine_trend",
    "persist_organic_search_kpi",
]
