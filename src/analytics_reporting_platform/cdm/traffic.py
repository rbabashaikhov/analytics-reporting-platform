"""CDM Traffic Trends marts: traffic_kpi, overall_traffic_trend, traffic_source_mix."""

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

MARKETING_CHANNELS: Final[tuple[str, ...]] = (
    "organic",
    "direct",
    "app",
    "email",
    "sns",
    "push",
    "other_channel",
)

OVERALL_TRAFFIC_SEGMENTS: Final[tuple[str, ...]] = (
    "organic",
    "own_group",
    "paid_group",
    "other_group",
    "total",
)

TRAFFIC_KPI_ORDER: Final[tuple[str, ...]] = (
    "total_visits",
    "organic_visits",
    "average_time_on_site",
    "bounce_rate",
)

TRAFFIC_KPI_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "kpi_name",
    "kpi_value",
    "mom_pct_change",
    "yoy_pct_change",
    "mom_pp_change",
    "yoy_pp_change",
    "change_type",
    "value_format",
    "display_order",
    "cdm_dt",
)
TRAFFIC_KPI_KEYS: Final[tuple[str, ...]] = ("month", "kpi_name")

OVERALL_TREND_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "organic_visits",
    "own_group_visits",
    "paid_group_visits",
    "other_group_visits",
    "total_visits",
    "cdm_dt",
)
OVERALL_TREND_KEYS: Final[tuple[str, ...]] = ("month",)

SOURCE_MIX_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "month_type",
    "channel_name",
    "channel_visits",
    "channel_share",
    "mom_pp_change",
    "display_order",
    "cdm_dt",
)
SOURCE_MIX_KEYS: Final[tuple[str, ...]] = ("month", "channel_name")


def _kpi_name(segment_name: str, metric_name: str) -> Optional[str]:
    if segment_name == "total" and metric_name == "visits":
        return "total_visits"
    if segment_name == "organic" and metric_name == "visits":
        return "organic_visits"
    if segment_name == "total" and metric_name == "avg_time":
        return "average_time_on_site"
    if segment_name == "total" and metric_name == "bounce_rate":
        return "bounce_rate"
    return None


def build_traffic_kpi(
    calc_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    """Build ``cdm_traffic_kpi`` for one report month from ``calc_monthly_kpi``."""
    month_start = parse_month_start(month)
    sql = text(
        """
        SELECT month, segment_name, metric_name, metric_value,
               mom_pct_change, yoy_pct_change, mom_pp_change, yoy_pp_change,
               change_type, value_format
        FROM calc_monthly_kpi
        WHERE month = :month
          AND ((segment_name = 'total' AND metric_name IN ('visits', 'avg_time', 'bounce_rate'))
            OR (segment_name = 'organic' AND metric_name = 'visits'))
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_traffic_kpi source returned no rows for month")
    out = _transform_traffic_kpi(df, cdm_dt=coalesce_platform_now(cdm_dt))
    return CdmMartResult(mart="traffic_kpi", dataframe=out, month=month_start)


def _transform_traffic_kpi(df: pd.DataFrame, *, cdm_dt: datetime) -> pd.DataFrame:
    frame = df.copy()
    frame["kpi_name"] = frame.apply(
        lambda r: _kpi_name(str(r["segment_name"]), str(r["metric_name"])), axis=1
    )
    frame = frame[frame["kpi_name"].notna()].copy()
    frame["kpi_value"] = frame["metric_value"]
    frame = add_display_order(frame, "kpi_name", TRAFFIC_KPI_ORDER)
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["cdm_dt"] = cdm_dt
    out = frame.loc[:, list(TRAFFIC_KPI_OUTPUT)].sort_values("display_order").reset_index(drop=True)
    if out.duplicated(subset=list(TRAFFIC_KPI_KEYS)).any():
        raise CdmError("cdm_traffic_kpi duplicate business keys")
    missing = sorted(set(TRAFFIC_KPI_ORDER) - set(out["kpi_name"]))
    if missing:
        raise CdmError("cdm_traffic_kpi missing kpi(s): " + ", ".join(missing))
    return out


def persist_traffic_kpi(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: str = "replace",
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="traffic_kpi",
        table="cdm_traffic_kpi",
        output_columns=TRAFFIC_KPI_OUTPUT,
        business_key_columns=TRAFFIC_KPI_KEYS,
        required_non_null=("month", "kpi_name", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_overall_traffic_trend(
    raw_engine: Engine,
    period: CdmPeriodConfig,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    """Build ``cdm_overall_traffic_trend`` for the historical trend window."""
    segs = ", ".join(f"'{s}'" for s in OVERALL_TRAFFIC_SEGMENTS)
    sql = text(
        f"""
        SELECT month, segment_name, metric_value
        FROM raw_aa_monthly_metrics
        WHERE month BETWEEN :start_month AND :end_month
          AND metric_name = 'visits'
          AND segment_name IN ({segs})
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"start_month": period.trend_start, "end_month": period.report_end},
        )
    if df.empty:
        raise CdmError("cdm_overall_traffic_trend source returned no rows")
    out = _transform_overall_traffic_trend(df, cdm_dt=coalesce_platform_now(cdm_dt))
    return CdmMartResult(
        mart="overall_traffic_trend",
        dataframe=out,
        window_start=period.trend_start,
        window_end=period.report_end,
    )


def _transform_overall_traffic_trend(df: pd.DataFrame, *, cdm_dt: datetime) -> pd.DataFrame:
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    p = (
        frame.pivot_table(
            index="month", columns="segment_name", values="metric_value", aggfunc="sum"
        )
        .reset_index()
    )
    for c in OVERALL_TRAFFIC_SEGMENTS:
        if c not in p.columns:
            p[c] = 0.0
    p = p.rename(
        columns={
            "organic": "organic_visits",
            "own_group": "own_group_visits",
            "paid_group": "paid_group_visits",
            "other_group": "other_group_visits",
            "total": "total_visits",
        }
    )
    p["month"] = p["month"].dt.date
    p["cdm_dt"] = cdm_dt
    out = p.loc[:, list(OVERALL_TREND_OUTPUT)].sort_values("month").reset_index(drop=True)
    if out.duplicated(subset=list(OVERALL_TREND_KEYS)).any():
        raise CdmError("cdm_overall_traffic_trend duplicate business keys")
    return out


def persist_overall_traffic_trend(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    period: CdmPeriodConfig,
    *,
    mode: str = "replace",
) -> PersistCdmResult:
    return persist_cdm_window(
        cdm_engine,
        dataframe,
        mart="overall_traffic_trend",
        table="cdm_overall_traffic_trend",
        window_start=period.trend_start,
        window_end=period.report_end,
        output_columns=OVERALL_TREND_OUTPUT,
        business_key_columns=OVERALL_TREND_KEYS,
        required_non_null=("month", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_traffic_source_mix(
    calc_engine: Engine,
    month: MonthInput,
    *,
    report_end: MonthInput,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    """Build ``cdm_traffic_source_mix`` for one month from ``calc_monthly_channel_mix``."""
    month_start = parse_month_start(month)
    end = parse_month_start(report_end)
    sql = text(
        """
        SELECT month, channel_name, channel_visits, channel_share, mom_pp_change
        FROM calc_monthly_channel_mix
        WHERE month = :month
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_traffic_source_mix source returned no rows for month")
    out = _transform_traffic_source_mix(df, report_end=end, cdm_dt=coalesce_platform_now(cdm_dt))
    return CdmMartResult(mart="traffic_source_mix", dataframe=out, month=month_start)


def _transform_traffic_source_mix(
    df: pd.DataFrame, *, report_end: date, cdm_dt: datetime
) -> pd.DataFrame:
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    frame["month_type"] = frame["month"].dt.date.map(lambda m: month_type_label(m, report_end))
    frame = add_display_order(frame, "channel_name", MARKETING_CHANNELS)
    frame["month"] = frame["month"].dt.date
    frame["cdm_dt"] = cdm_dt
    out = (
        frame.loc[:, list(SOURCE_MIX_OUTPUT)]
        .sort_values("display_order")
        .reset_index(drop=True)
    )
    if out.duplicated(subset=list(SOURCE_MIX_KEYS)).any():
        raise CdmError("cdm_traffic_source_mix duplicate business keys")
    return out


def persist_traffic_source_mix(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: str = "replace",
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="traffic_source_mix",
        table="cdm_traffic_source_mix",
        output_columns=SOURCE_MIX_OUTPUT,
        business_key_columns=SOURCE_MIX_KEYS,
        required_non_null=("month", "channel_name", "month_type", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


__all__ = [
    "MARKETING_CHANNELS",
    "OVERALL_TRAFFIC_SEGMENTS",
    "TRAFFIC_KPI_KEYS",
    "TRAFFIC_KPI_ORDER",
    "TRAFFIC_KPI_OUTPUT",
    "OVERALL_TREND_KEYS",
    "OVERALL_TREND_OUTPUT",
    "SOURCE_MIX_KEYS",
    "SOURCE_MIX_OUTPUT",
    "build_overall_traffic_trend",
    "build_traffic_kpi",
    "build_traffic_source_mix",
    "persist_overall_traffic_trend",
    "persist_traffic_kpi",
    "persist_traffic_source_mix",
]
