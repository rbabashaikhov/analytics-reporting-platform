"""Build ``calc_monthly_channel_mix`` from persisted RAW monthly metrics.

Full-private CALC mart. Historical semantics preserved from the Monthly CALC notebook.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Final, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.calc.models import (
    CalcError,
    MonthInput,
    MonthlyChannelMixResult,
    parse_month_start,
    prev_month_start,
)
from analytics_reporting_platform.calc.monthly_kpi import safe_divide
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

# Historical marketing channel segment_name values (private operational taxonomy).
MARKETING_CHANNELS: Final[tuple[str, ...]] = (
    "organic",
    "direct",
    "app",
    "email",
    "sns",
    "push",
    "other_channel",
)

OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "month",
    "channel_name",
    "channel_id",
    "channel_visits",
    "channel_share",
    "prev_month_share",
    "mom_pp_change",
    "calc_dt",
)

BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month", "channel_name")

SOURCE_TABLE: Final = "raw_aa_monthly_metrics"
SOURCE_METRIC: Final = "visits"
TOTAL_SEGMENT: Final = "total"


def _raw_visits_month_has_rows(raw_engine: Engine, month: date) -> bool:
    sql = text(
        f"""
        SELECT 1 FROM {SOURCE_TABLE}
        WHERE month = :month AND metric_name = :metric
        LIMIT 1
        """
    )
    with raw_engine.connect() as conn:
        return conn.execute(sql, {"month": month, "metric": SOURCE_METRIC}).fetchone() is not None


def validate_raw_lookback(raw_engine: Engine, month: MonthInput) -> date:
    """Ensure current and previous-month RAW visits rows exist for channel mix."""
    month_start = parse_month_start(month)
    required = {
        "current": month_start,
        "mom": prev_month_start(month_start),
    }
    missing = [
        label for label, m in required.items() if not _raw_visits_month_has_rows(raw_engine, m)
    ]
    if missing:
        raise CalcError(
            "Missing required RAW monthly visits period(s) for calc_monthly_channel_mix: "
            + ", ".join(missing)
        )
    return month_start


def build_monthly_channel_mix(
    raw_engine: Engine,
    month: MonthInput,
    *,
    calc_dt: Optional[datetime] = None,
) -> MonthlyChannelMixResult:
    """Build ``calc_monthly_channel_mix`` for one explicit month from ``raw_aa_monthly_metrics``.

    Lookback window: ``[month - 1 month, month]`` (historical notebook MoM share).
    Requires current and previous-month visits rows; current month must include
    ``total`` plus all historical marketing channels.
    """
    month_start = validate_raw_lookback(raw_engine, month)
    min_month = prev_month_start(month_start)
    max_month = month_start

    sql = text(
        f"""
        SELECT month, segment_name, segment_id, metric_name, metric_value
        FROM {SOURCE_TABLE}
        WHERE month BETWEEN :min_month AND :max_month
          AND metric_name = :metric
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"min_month": min_month, "max_month": max_month, "metric": SOURCE_METRIC},
        )

    if df.empty:
        raise CalcError("RAW monthly visits lookback query returned no rows")

    dataframe = _transform_monthly_channel_mix(
        df,
        month_start=month_start,
        calc_dt=coalesce_platform_now(calc_dt),
    )
    return MonthlyChannelMixResult(month=month_start, dataframe=dataframe)


def _transform_monthly_channel_mix(
    df: pd.DataFrame,
    *,
    month_start: date,
    calc_dt: datetime,
) -> pd.DataFrame:
    """Apply historical channel share + MoM pp enrichment; filter to requested month."""
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    frame["metric_value"] = pd.to_numeric(frame["metric_value"], errors="coerce")

    total_df = (
        frame[frame["segment_name"] == TOTAL_SEGMENT][["month", "metric_value"]]
        .rename(columns={"metric_value": "site_total_visits"})
    )
    channels_df = frame[frame["segment_name"].isin(MARKETING_CHANNELS)].copy()
    if channels_df.empty:
        raise CalcError("calc_monthly_channel_mix found no marketing channel visits rows")

    result = channels_df.merge(total_df, on="month", how="left")
    result["channel_name"] = result["segment_name"]
    result["channel_id"] = result["segment_id"]
    result["channel_visits"] = result["metric_value"]
    result["channel_share"] = result.apply(
        lambda r: safe_divide(r["channel_visits"], r["site_total_visits"]),
        axis=1,
    )

    prev_share_df = result[["month", "channel_name", "channel_share"]].copy()
    prev_share_df["month"] = prev_share_df["month"] + pd.DateOffset(months=1)
    prev_share_df = prev_share_df.rename(columns={"channel_share": "prev_month_share"})

    result = result.merge(prev_share_df, on=["month", "channel_name"], how="left")
    result["mom_pp_change"] = result["channel_share"] - result["prev_month_share"]

    target = pd.Timestamp(month_start)
    result = result[result["month"] == target].copy()
    if result.empty:
        raise CalcError("calc_monthly_channel_mix produced no rows for the requested month")

    present = set(result["channel_name"].tolist())
    expected = set(MARKETING_CHANNELS)
    missing_channels = sorted(expected - present)
    if missing_channels:
        raise CalcError(
            "calc_monthly_channel_mix missing required marketing channel(s) for month: "
            + ", ".join(missing_channels)
        )

    current_total = total_df[total_df["month"] == target]
    if current_total.empty:
        raise CalcError(
            "calc_monthly_channel_mix requires total segment visits for the requested month"
        )

    result["month"] = result["month"].dt.date
    result["calc_dt"] = calc_dt
    out = result.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)

    if out.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).any():
        raise CalcError("calc_monthly_channel_mix result has duplicate business keys")
    return out


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "MARKETING_CHANNELS",
    "OUTPUT_COLUMNS",
    "SOURCE_METRIC",
    "SOURCE_TABLE",
    "TOTAL_SEGMENT",
    "build_monthly_channel_mix",
    "validate_raw_lookback",
]
