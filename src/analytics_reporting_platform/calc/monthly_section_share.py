"""Build ``calc_monthly_section_share`` from persisted RAW by-section visits.

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
    MonthlySectionShareResult,
    parse_month_start,
    prev_month_start,
)
from analytics_reporting_platform.calc.monthly_kpi import safe_divide
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

# Historical section-share segment_name values (private operational taxonomy).
SECTION_SHARE_SEGMENTS: Final[tuple[str, ...]] = (
    "total",
    "ai",
    "samsung_devices",
    "apple_devices",
)

# Historical section_name values (private operational taxonomy).
SECTIONS: Final[tuple[str, ...]] = (
    "support",
    "mx",
    "explore",
    "he",
    "da",
    "other",
)

OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "month",
    "segment_name",
    "segment_id",
    "section_name",
    "section_id",
    "section_visits",
    "section_share",
    "prev_month_share",
    "mom_pp_change",
    "calc_dt",
)

BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month", "segment_name", "section_name")

SOURCE_TABLE: Final = "raw_aa_monthly_by_section"

_SEGMENTS_SQL: Final = ", ".join(f"'{s}'" for s in SECTION_SHARE_SEGMENTS)
_SECTIONS_SQL: Final = ", ".join(f"'{s}'" for s in SECTIONS)


def _raw_section_month_has_rows(raw_engine: Engine, month: date) -> bool:
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
    """Ensure current and previous-month RAW by-section rows exist."""
    month_start = parse_month_start(month)
    required = {
        "current": month_start,
        "mom": prev_month_start(month_start),
    }
    missing = [
        label
        for label, m in required.items()
        if not _raw_section_month_has_rows(raw_engine, m)
    ]
    if missing:
        raise CalcError(
            "Missing required RAW monthly by-section period(s) for "
            "calc_monthly_section_share: " + ", ".join(missing)
        )
    return month_start


def build_monthly_section_share(
    raw_engine: Engine,
    month: MonthInput,
    *,
    calc_dt: Optional[datetime] = None,
) -> MonthlySectionShareResult:
    """Build ``calc_monthly_section_share`` for one explicit month.

    Lookback window: ``[month - 1 month, month]`` (historical notebook MoM share).
    Requires filtered segment×section rows for current and previous month.
    """
    month_start = validate_raw_lookback(raw_engine, month)
    min_month = prev_month_start(month_start)
    max_month = month_start

    sql = text(
        f"""
        SELECT month, segment_name, segment_id, section_name, section_id, metric_value
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

    if df.empty:
        raise CalcError("RAW monthly by-section lookback query returned no rows")

    dataframe = _transform_monthly_section_share(
        df,
        month_start=month_start,
        calc_dt=coalesce_platform_now(calc_dt),
    )
    return MonthlySectionShareResult(month=month_start, dataframe=dataframe)


def _transform_monthly_section_share(
    df: pd.DataFrame,
    *,
    month_start: date,
    calc_dt: datetime,
) -> pd.DataFrame:
    """Apply historical section share + MoM pp enrichment; filter to requested month."""
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    frame["metric_value"] = pd.to_numeric(frame["metric_value"], errors="coerce")
    frame["section_visits"] = frame["metric_value"]

    totals = (
        frame.groupby(["month", "segment_name"], as_index=False)["section_visits"]
        .sum()
        .rename(columns={"section_visits": "segment_sections_total"})
    )

    result = frame.merge(totals, on=["month", "segment_name"], how="left")
    result["section_share"] = result.apply(
        lambda r: safe_divide(r["section_visits"], r["segment_sections_total"]),
        axis=1,
    )

    prev_share_df = result[
        ["month", "segment_name", "section_name", "section_share"]
    ].copy()
    prev_share_df["month"] = prev_share_df["month"] + pd.DateOffset(months=1)
    prev_share_df = prev_share_df.rename(columns={"section_share": "prev_month_share"})

    result = result.merge(
        prev_share_df,
        on=["month", "segment_name", "section_name"],
        how="left",
    )
    result["mom_pp_change"] = result["section_share"] - result["prev_month_share"]

    target = pd.Timestamp(month_start)
    result = result[result["month"] == target].copy()
    if result.empty:
        raise CalcError("calc_monthly_section_share produced no rows for the requested month")

    present_segments = set(result["segment_name"].tolist())
    missing_segments = sorted(set(SECTION_SHARE_SEGMENTS) - present_segments)
    if missing_segments:
        raise CalcError(
            "calc_monthly_section_share missing required segment(s) for month: "
            + ", ".join(missing_segments)
        )

    for segment in SECTION_SHARE_SEGMENTS:
        present_sections = set(
            result.loc[result["segment_name"] == segment, "section_name"].tolist()
        )
        missing_sections = sorted(set(SECTIONS) - present_sections)
        if missing_sections:
            raise CalcError(
                "calc_monthly_section_share missing required section(s) for "
                f"segment {segment!r}: " + ", ".join(missing_sections)
            )

    result["month"] = result["month"].dt.date
    result["calc_dt"] = calc_dt

    segment_order = {name: i for i, name in enumerate(SECTION_SHARE_SEGMENTS)}
    section_order = {name: i for i, name in enumerate(SECTIONS)}
    result["_seg_ord"] = result["segment_name"].map(segment_order)
    result["_sec_ord"] = result["section_name"].map(section_order)
    result = result.sort_values(["_seg_ord", "_sec_ord"]).drop(
        columns=["_seg_ord", "_sec_ord"]
    )

    out = result.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)

    if out.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).any():
        raise CalcError("calc_monthly_section_share result has duplicate business keys")
    return out


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "OUTPUT_COLUMNS",
    "SECTIONS",
    "SECTION_SHARE_SEGMENTS",
    "SOURCE_TABLE",
    "build_monthly_section_share",
    "validate_raw_lookback",
]
