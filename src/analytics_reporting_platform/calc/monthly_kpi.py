"""Build ``calc_monthly_kpi`` from persisted RAW monthly metrics.

Full-private CALC mart. Historical semantics preserved from the Monthly CALC notebook.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Final, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.calc.models import (
    CalcError,
    MonthInput,
    MonthlyKpiResult,
    parse_month_start,
    prev_month_start,
    prev_year_month_start,
)
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

# Historical classification sets (logical RAW metric_name values).
RATE_METRICS: Final[frozenset[str]] = frozenset({"bounce_rate"})
DURATION_METRICS: Final[frozenset[str]] = frozenset({"avg_time"})

OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "month",
    "segment_name",
    "segment_id",
    "metric_name",
    "metric_value",
    "prev_month_value",
    "prev_year_value",
    "mom_abs_change",
    "mom_pct_change",
    "yoy_abs_change",
    "yoy_pct_change",
    "mom_pp_change",
    "yoy_pp_change",
    "change_type",
    "value_format",
    "calc_dt",
)

BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month", "segment_name", "metric_name")

SOURCE_TABLE: Final = "raw_aa_monthly_metrics"


def safe_divide(numerator: Any, denominator: Any) -> Optional[float]:
    """Historical safe division: None when either side is null or denominator is 0."""
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return None
    if numerator is None or pd.isna(numerator):
        return None
    return float(numerator) / float(denominator)


def metric_change_type(metric_name: str) -> str:
    """Return ``pp`` for rate/share metrics, otherwise ``pct``."""
    if (
        metric_name in RATE_METRICS
        or metric_name.endswith("_rate")
        or metric_name.endswith("_share")
    ):
        return "pp"
    return "pct"


def metric_value_format(metric_name: str) -> str:
    """Return historical display format label for a metric name."""
    if (
        metric_name in RATE_METRICS
        or metric_name.endswith("_rate")
        or metric_name.endswith("_share")
    ):
        return "percent"
    if metric_name in DURATION_METRICS:
        return "duration_seconds"
    return "number"


def _raw_month_has_rows(raw_engine: Engine, month: date) -> bool:
    sql = text(
        f"SELECT 1 FROM {SOURCE_TABLE} WHERE month = :month LIMIT 1"
    )
    with raw_engine.connect() as conn:
        return conn.execute(sql, {"month": month}).fetchone() is not None


def validate_raw_lookback(raw_engine: Engine, month: MonthInput) -> date:
    """Ensure current, MoM, and YoY RAW months exist for ``calc_monthly_kpi``."""
    month_start = parse_month_start(month)
    required = {
        "current": month_start,
        "mom": prev_month_start(month_start),
        "yoy": prev_year_month_start(month_start),
    }
    missing = [label for label, m in required.items() if not _raw_month_has_rows(raw_engine, m)]
    if missing:
        raise CalcError(
            "Missing required RAW monthly metrics period(s) for calc_monthly_kpi: "
            + ", ".join(missing)
        )
    return month_start


def build_monthly_kpi(
    raw_engine: Engine,
    month: MonthInput,
    *,
    calc_dt: Optional[datetime] = None,
) -> MonthlyKpiResult:
    """Build ``calc_monthly_kpi`` for one explicit month from ``raw_aa_monthly_metrics``.

    Lookback window loaded from RAW: ``[month - 1 year, month]`` (historical notebook).
    Requires current, previous-month, and previous-year RAW periods to be present.
    """
    month_start = validate_raw_lookback(raw_engine, month)
    min_month = prev_year_month_start(month_start)
    max_month = month_start

    sql = text(
        f"""
        SELECT month, segment_name, segment_id, metric_name, metric_value
        FROM {SOURCE_TABLE}
        WHERE month BETWEEN :min_month AND :max_month
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"min_month": min_month, "max_month": max_month},
        )

    if df.empty:
        raise CalcError("RAW monthly metrics lookback query returned no rows")

    dataframe = _transform_monthly_kpi(
        df,
        month_start=month_start,
        calc_dt=coalesce_platform_now(calc_dt),
    )
    return MonthlyKpiResult(month=month_start, dataframe=dataframe)


def _transform_monthly_kpi(
    df: pd.DataFrame,
    *,
    month_start: date,
    calc_dt: datetime,
) -> pd.DataFrame:
    """Apply historical MoM/YoY enrichment and filter to the requested month."""
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    frame["metric_value"] = pd.to_numeric(frame["metric_value"], errors="coerce")
    frame = frame.sort_values(["segment_name", "metric_name", "month"])

    prev_month_df = frame[["month", "segment_name", "metric_name", "metric_value"]].copy()
    prev_month_df["month"] = prev_month_df["month"] + pd.DateOffset(months=1)
    prev_month_df = prev_month_df.rename(columns={"metric_value": "prev_month_value"})

    prev_year_df = frame[["month", "segment_name", "metric_name", "metric_value"]].copy()
    prev_year_df["month"] = prev_year_df["month"] + pd.DateOffset(years=1)
    prev_year_df = prev_year_df.rename(columns={"metric_value": "prev_year_value"})

    result = (
        frame.merge(prev_month_df, on=["month", "segment_name", "metric_name"], how="left")
        .merge(prev_year_df, on=["month", "segment_name", "metric_name"], how="left")
    )

    result["mom_abs_change"] = result["metric_value"] - result["prev_month_value"]
    result["yoy_abs_change"] = result["metric_value"] - result["prev_year_value"]
    result["change_type"] = result["metric_name"].apply(metric_change_type)
    result["value_format"] = result["metric_name"].apply(metric_value_format)

    result["mom_pct_change"] = result.apply(
        lambda r: safe_divide(r["mom_abs_change"], r["prev_month_value"])
        if r["change_type"] == "pct"
        else None,
        axis=1,
    )
    result["yoy_pct_change"] = result.apply(
        lambda r: safe_divide(r["yoy_abs_change"], r["prev_year_value"])
        if r["change_type"] == "pct"
        else None,
        axis=1,
    )
    result["mom_pp_change"] = result.apply(
        lambda r: r["mom_abs_change"] if r["change_type"] == "pp" else None,
        axis=1,
    )
    result["yoy_pp_change"] = result.apply(
        lambda r: r["yoy_abs_change"] if r["change_type"] == "pp" else None,
        axis=1,
    )

    target = pd.Timestamp(month_start)
    result = result[result["month"] == target].copy()
    if result.empty:
        raise CalcError("calc_monthly_kpi produced no rows for the requested month")

    result["month"] = result["month"].dt.date
    result["calc_dt"] = calc_dt
    out = result.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)

    if out.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).any():
        raise CalcError("calc_monthly_kpi result has duplicate business keys")
    return out


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "DURATION_METRICS",
    "OUTPUT_COLUMNS",
    "RATE_METRICS",
    "SOURCE_TABLE",
    "build_monthly_kpi",
    "metric_change_type",
    "metric_value_format",
    "safe_divide",
    "validate_raw_lookback",
]
