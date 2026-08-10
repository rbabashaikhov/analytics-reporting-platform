"""Build ``calc_explore_buying_guides_kpi`` from persisted RAW by-section visits.

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
    ExploreBuyingGuidesKpiResult,
    MonthInput,
    parse_month_start,
    prev_month_start,
    prev_year_month_start,
)
from analytics_reporting_platform.calc.monthly_kpi import safe_divide
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

# Historical Explore & Buying Guides section_name values.
EXPLORE_BG_SECTIONS: Final[tuple[str, ...]] = (
    "explore",
    "buying_guides",
)

# Historical KPI output order / display_order mapping (1-based).
EXPLORE_BG_KPI_ORDER: Final[tuple[str, ...]] = (
    "total_visits",
    "organic_visits",
)

# RAW segment_name → output kpi_name.
SEGMENT_TO_KPI: Final[dict[str, str]] = {
    "total": "total_visits",
    "organic": "organic_visits",
}

OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "month",
    "section_name",
    "kpi_name",
    "kpi_value",
    "prev_month_value",
    "prev_year_value",
    "mom_pct_change",
    "yoy_pct_change",
    "display_order",
    "calc_dt",
)

BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month", "section_name", "kpi_name")

SOURCE_TABLE: Final = "raw_aa_monthly_by_section"

_SECTIONS_SQL: Final = ", ".join(f"'{s}'" for s in EXPLORE_BG_SECTIONS)
_SEGMENTS_SQL: Final = "'total', 'organic'"


def _raw_explore_bg_month_has_rows(raw_engine: Engine, month: date) -> bool:
    sql = text(
        f"""
        SELECT 1 FROM {SOURCE_TABLE}
        WHERE month = :month
          AND segment_name IN ({_SEGMENTS_SQL})
          AND section_name IN ({_SECTIONS_SQL})
        LIMIT 1
        """
    )
    with raw_engine.connect() as conn:
        return conn.execute(sql, {"month": month}).fetchone() is not None


def validate_raw_lookback(raw_engine: Engine, month: MonthInput) -> date:
    """Ensure current, MoM, and YoY RAW by-section rows exist for explore/BG KPIs."""
    month_start = parse_month_start(month)
    required = {
        "current": month_start,
        "mom": prev_month_start(month_start),
        "yoy": prev_year_month_start(month_start),
    }
    missing = [
        label
        for label, m in required.items()
        if not _raw_explore_bg_month_has_rows(raw_engine, m)
    ]
    if missing:
        raise CalcError(
            "Missing required RAW monthly by-section period(s) for "
            "calc_explore_buying_guides_kpi: " + ", ".join(missing)
        )
    return month_start


def build_explore_buying_guides_kpi(
    raw_engine: Engine,
    month: MonthInput,
    *,
    calc_dt: Optional[datetime] = None,
) -> ExploreBuyingGuidesKpiResult:
    """Build ``calc_explore_buying_guides_kpi`` for one explicit month.

    Lookback window: ``[month - 1 year, month]`` (historical notebook MoM + YoY).
    Requires filtered total/organic × explore/buying_guides rows for current,
    previous-month, and previous-year periods.
    """
    month_start = validate_raw_lookback(raw_engine, month)
    min_month = prev_year_month_start(month_start)
    max_month = month_start

    sql = text(
        f"""
        SELECT month, segment_name, section_name, metric_value
        FROM {SOURCE_TABLE}
        WHERE month BETWEEN :min_month AND :max_month
          AND segment_name IN ({_SEGMENTS_SQL})
          AND section_name IN ({_SECTIONS_SQL})
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"min_month": min_month, "max_month": max_month},
        )

    dataframe = _transform_explore_buying_guides_kpi(
        df,
        month_start=month_start,
        min_month=min_month,
        max_month=max_month,
        calc_dt=coalesce_platform_now(calc_dt),
    )
    return ExploreBuyingGuidesKpiResult(month=month_start, dataframe=dataframe)


def _transform_explore_buying_guides_kpi(
    df: pd.DataFrame,
    *,
    month_start: date,
    min_month: date,
    max_month: date,
    calc_dt: datetime,
) -> pd.DataFrame:
    """Apply historical explore/BG KPI grid + MoM/YoY enrichment."""
    all_months = pd.date_range(start=min_month, end=max_month, freq="MS")
    grid = (
        pd.MultiIndex.from_product(
            [all_months, list(EXPLORE_BG_SECTIONS), list(EXPLORE_BG_KPI_ORDER)],
            names=["month", "section_name", "kpi_name"],
        ).to_frame(index=False)
    )

    if df.empty:
        result = grid.copy()
        result["kpi_value"] = 0.0
    else:
        frame = df.copy()
        frame["month"] = pd.to_datetime(frame["month"])
        frame["metric_value"] = pd.to_numeric(frame["metric_value"], errors="coerce").fillna(
            0.0
        )
        frame["kpi_name"] = frame["segment_name"].map(SEGMENT_TO_KPI)
        frame = frame[frame["kpi_name"].notna()].copy()

        grouped = (
            frame.groupby(["month", "section_name", "kpi_name"], as_index=False)[
                "metric_value"
            ]
            .sum()
            .rename(columns={"metric_value": "kpi_value"})
        )
        result = grid.merge(grouped, on=["month", "section_name", "kpi_name"], how="left")
        result["kpi_value"] = result["kpi_value"].fillna(0.0)

    prev_month_df = result[["month", "section_name", "kpi_name", "kpi_value"]].copy()
    prev_month_df["month"] = prev_month_df["month"] + pd.DateOffset(months=1)
    prev_month_df = prev_month_df.rename(columns={"kpi_value": "prev_month_value"})

    prev_year_df = result[["month", "section_name", "kpi_name", "kpi_value"]].copy()
    prev_year_df["month"] = prev_year_df["month"] + pd.DateOffset(years=1)
    prev_year_df = prev_year_df.rename(columns={"kpi_value": "prev_year_value"})

    result = result.merge(
        prev_month_df, on=["month", "section_name", "kpi_name"], how="left"
    ).merge(prev_year_df, on=["month", "section_name", "kpi_name"], how="left")

    result["mom_pct_change"] = result.apply(
        lambda r: safe_divide(r["kpi_value"] - r["prev_month_value"], r["prev_month_value"]),
        axis=1,
    )
    result["yoy_pct_change"] = result.apply(
        lambda r: safe_divide(r["kpi_value"] - r["prev_year_value"], r["prev_year_value"]),
        axis=1,
    )

    target = pd.Timestamp(month_start)
    result = result[result["month"] == target].copy()
    if result.empty:
        raise CalcError(
            "calc_explore_buying_guides_kpi produced no rows for the requested month"
        )

    expected_keys = {
        (section, kpi)
        for section in EXPLORE_BG_SECTIONS
        for kpi in EXPLORE_BG_KPI_ORDER
    }
    present_keys = {
        (str(r.section_name), str(r.kpi_name)) for r in result.itertuples(index=False)
    }
    missing = sorted(expected_keys - present_keys)
    if missing:
        raise CalcError(
            "calc_explore_buying_guides_kpi missing required section/kpi key(s): "
            + ", ".join(f"{s}/{k}" for s, k in missing)
        )

    display_order_map = {name: i + 1 for i, name in enumerate(EXPLORE_BG_KPI_ORDER)}
    result["display_order"] = result["kpi_name"].map(display_order_map)
    result["month"] = result["month"].dt.date
    result["calc_dt"] = calc_dt

    section_order = {name: i for i, name in enumerate(EXPLORE_BG_SECTIONS)}
    kpi_order = {name: i for i, name in enumerate(EXPLORE_BG_KPI_ORDER)}
    result["_sec_ord"] = result["section_name"].map(section_order)
    result["_kpi_ord"] = result["kpi_name"].map(kpi_order)
    result = result.sort_values(["_sec_ord", "_kpi_ord"]).drop(
        columns=["_sec_ord", "_kpi_ord"]
    )

    out = result.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)
    if out.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).any():
        raise CalcError("calc_explore_buying_guides_kpi result has duplicate business keys")
    return out


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "EXPLORE_BG_KPI_ORDER",
    "EXPLORE_BG_SECTIONS",
    "OUTPUT_COLUMNS",
    "SEGMENT_TO_KPI",
    "SOURCE_TABLE",
    "build_explore_buying_guides_kpi",
    "validate_raw_lookback",
]
