"""Build ``calc_monthly_search_engines`` from persisted RAW monthly metrics.

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
    MonthlySearchEnginesResult,
    parse_month_start,
    prev_month_start,
)
from analytics_reporting_platform.calc.monthly_kpi import safe_divide
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

# Historical organic search-engine segment_name values (private operational taxonomy).
SOURCE_SEGMENTS: Final[tuple[str, ...]] = ("organic", "yandex", "google")

# Output category order matches historical notebook construction.
SEARCH_ENGINE_CATEGORIES: Final[tuple[str, ...]] = (
    "organic_total",
    "yandex",
    "google",
    "other_search_engines",
)

OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "month",
    "search_engine",
    "search_engine_visits",
    "search_engine_share",
    "prev_month_share",
    "mom_pp_change",
    "calc_dt",
)

BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month", "search_engine")

SOURCE_TABLE: Final = "raw_aa_monthly_metrics"
SOURCE_METRIC: Final = "visits"


def _raw_search_month_has_rows(raw_engine: Engine, month: date) -> bool:
    sql = text(
        f"""
        SELECT 1 FROM {SOURCE_TABLE}
        WHERE month = :month
          AND metric_name = :metric
          AND segment_name IN ('organic', 'yandex', 'google')
        LIMIT 1
        """
    )
    with raw_engine.connect() as conn:
        return conn.execute(
            sql, {"month": month, "metric": SOURCE_METRIC}
        ).fetchone() is not None


def validate_raw_lookback(raw_engine: Engine, month: MonthInput) -> date:
    """Ensure current and previous-month RAW search-engine visits rows exist."""
    month_start = parse_month_start(month)
    required = {
        "current": month_start,
        "mom": prev_month_start(month_start),
    }
    missing = [
        label
        for label, m in required.items()
        if not _raw_search_month_has_rows(raw_engine, m)
    ]
    if missing:
        raise CalcError(
            "Missing required RAW monthly visits period(s) for "
            "calc_monthly_search_engines: " + ", ".join(missing)
        )
    return month_start


def build_monthly_search_engines(
    raw_engine: Engine,
    month: MonthInput,
    *,
    calc_dt: Optional[datetime] = None,
) -> MonthlySearchEnginesResult:
    """Build ``calc_monthly_search_engines`` for one explicit month.

    Lookback window: ``[month - 1 month, month]`` (historical notebook MoM share).
    Requires organic / yandex / google visits rows for current and previous month.
    """
    month_start = validate_raw_lookback(raw_engine, month)
    min_month = prev_month_start(month_start)
    max_month = month_start

    sql = text(
        f"""
        SELECT month, segment_name, metric_value
        FROM {SOURCE_TABLE}
        WHERE month BETWEEN :min_month AND :max_month
          AND metric_name = :metric
          AND segment_name IN ('organic', 'yandex', 'google')
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "min_month": min_month,
                "max_month": max_month,
                "metric": SOURCE_METRIC,
            },
        )

    if df.empty:
        raise CalcError("RAW monthly search-engine visits lookback query returned no rows")

    dataframe = _transform_monthly_search_engines(
        df,
        month_start=month_start,
        calc_dt=coalesce_platform_now(calc_dt),
    )
    return MonthlySearchEnginesResult(month=month_start, dataframe=dataframe)


def _transform_monthly_search_engines(
    df: pd.DataFrame,
    *,
    month_start: date,
    calc_dt: datetime,
) -> pd.DataFrame:
    """Apply historical organic search-engine mix + MoM pp enrichment."""
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    frame["metric_value"] = pd.to_numeric(frame["metric_value"], errors="coerce")

    pivot = (
        frame.pivot_table(
            index="month",
            columns="segment_name",
            values="metric_value",
            aggfunc="sum",
        )
        .reset_index()
    )

    for col in SOURCE_SEGMENTS:
        if col not in pivot.columns:
            pivot[col] = 0.0

    pivot["other_search_engines"] = pivot["organic"] - pivot["yandex"] - pivot["google"]
    pivot["other_search_engines"] = pivot["other_search_engines"].clip(lower=0)

    rows: list[dict[object, object]] = []
    for _, row in pivot.iterrows():
        organic_total = row["organic"]
        categories = {
            "organic_total": row["organic"],
            "yandex": row["yandex"],
            "google": row["google"],
            "other_search_engines": row["other_search_engines"],
        }
        for search_engine, visits in categories.items():
            rows.append(
                {
                    "month": row["month"],
                    "search_engine": search_engine,
                    "search_engine_visits": visits,
                    "search_engine_share": safe_divide(visits, organic_total),
                }
            )

    result = pd.DataFrame(rows)
    if result.empty:
        raise CalcError("calc_monthly_search_engines produced no category rows")

    prev_share_df = result[["month", "search_engine", "search_engine_share"]].copy()
    prev_share_df["month"] = prev_share_df["month"] + pd.DateOffset(months=1)
    prev_share_df = prev_share_df.rename(columns={"search_engine_share": "prev_month_share"})

    result = result.merge(prev_share_df, on=["month", "search_engine"], how="left")
    result["mom_pp_change"] = result["search_engine_share"] - result["prev_month_share"]

    target = pd.Timestamp(month_start)
    result = result[result["month"] == target].copy()
    if result.empty:
        raise CalcError("calc_monthly_search_engines produced no rows for the requested month")

    present = set(result["search_engine"].tolist())
    expected = set(SEARCH_ENGINE_CATEGORIES)
    missing = sorted(expected - present)
    if missing:
        raise CalcError(
            "calc_monthly_search_engines missing required search_engine categor(y/ies): "
            + ", ".join(missing)
        )

    result["month"] = result["month"].dt.date
    result["calc_dt"] = calc_dt
    out = result.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)

    if out.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).any():
        raise CalcError("calc_monthly_search_engines result has duplicate business keys")
    return out


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "OUTPUT_COLUMNS",
    "SEARCH_ENGINE_CATEGORIES",
    "SOURCE_METRIC",
    "SOURCE_SEGMENTS",
    "SOURCE_TABLE",
    "build_monthly_search_engines",
    "validate_raw_lookback",
]
