"""CDM sections / device-section mix and Top Pages marts."""

from __future__ import annotations

from datetime import date, datetime
from typing import Final, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.cdm.geo import SECTION_LIST
from analytics_reporting_platform.cdm.models import (
    CdmError,
    CdmMartResult,
    MonthInput,
    add_display_order,
    month_type_label,
    parse_month_start,
)
from analytics_reporting_platform.cdm.repository import PersistCdmResult, persist_cdm_month
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

DEVICE_SEGMENTS: Final[tuple[str, ...]] = ("samsung_devices", "apple_devices")

SECTIONS_MIX_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "month_type",
    "section_name",
    "section_visits",
    "section_share",
    "mom_pp_change",
    "display_order",
    "cdm_dt",
)
SECTIONS_MIX_KEYS: Final[tuple[str, ...]] = ("month", "section_name")

DEVICE_SECTION_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "device_segment",
    "section_name",
    "section_visits",
    "section_share",
    "mom_pp_change",
    "display_order",
    "cdm_dt",
)
DEVICE_SECTION_KEYS: Final[tuple[str, ...]] = ("month", "device_segment", "section_name")

TOP_PAGES_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "page_rank",
    "page_url",
    "visits",
    "cdm_dt",
)
TOP_PAGES_KEYS: Final[tuple[str, ...]] = ("month", "page_url")


def build_sections_mix(
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
        SELECT month, section_name, section_visits, section_share, mom_pp_change
        FROM calc_monthly_section_share
        WHERE month = :month AND segment_name = 'total'
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_sections_mix source returned no rows for month")
    frame = df.copy()
    frame["month"] = pd.to_datetime(frame["month"])
    frame["month_type"] = frame["month"].dt.date.map(lambda m: month_type_label(m, end))
    frame = add_display_order(frame, "section_name", SECTION_LIST)
    frame["month"] = frame["month"].dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = (
        frame.loc[:, list(SECTIONS_MIX_OUTPUT)]
        .sort_values("display_order")
        .reset_index(drop=True)
    )
    if out.duplicated(subset=list(SECTIONS_MIX_KEYS)).any():
        raise CdmError("cdm_sections_mix duplicate business keys")
    return CdmMartResult(mart="sections_mix", dataframe=out, month=month_start)


def persist_sections_mix(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="sections_mix",
        table="cdm_sections_mix",
        output_columns=SECTIONS_MIX_OUTPUT,
        business_key_columns=SECTIONS_MIX_KEYS,
        required_non_null=("month", "section_name", "month_type", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_device_section_mix(
    calc_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    month_start = parse_month_start(month)
    segs = ", ".join(f"'{s}'" for s in DEVICE_SEGMENTS)
    sql = text(
        f"""
        SELECT month, segment_name AS device_segment, section_name,
               section_visits, section_share, mom_pp_change
        FROM calc_monthly_section_share
        WHERE month = :month AND segment_name IN ({segs})
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_device_section_mix source returned no rows for month")
    frame = add_display_order(df, "section_name", SECTION_LIST)
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = (
        frame.loc[:, list(DEVICE_SECTION_OUTPUT)]
        .sort_values(["device_segment", "display_order"])
        .reset_index(drop=True)
    )
    if out.duplicated(subset=list(DEVICE_SECTION_KEYS)).any():
        raise CdmError("cdm_device_section_mix duplicate business keys")
    return CdmMartResult(mart="device_section_mix", dataframe=out, month=month_start)


def persist_device_section_mix(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="device_section_mix",
        table="cdm_device_section_mix",
        output_columns=DEVICE_SECTION_OUTPUT,
        business_key_columns=DEVICE_SECTION_KEYS,
        required_non_null=("month", "device_segment", "section_name", "display_order", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


def build_top_pages(
    raw_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    """Build ``cdm_top_pages`` for one month from ``raw_aa_monthly_by_url``.

    Private operational mart. Callers must not log page_url contents.
    """
    month_start = parse_month_start(month)
    sql = text(
        """
        SELECT month, page_url, visits
        FROM raw_aa_monthly_by_url
        WHERE month = :month
        ORDER BY month, visits DESC
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_top_pages source returned no rows for month")
    frame = df.copy()
    frame["page_rank"] = (
        frame.groupby("month")["visits"].rank(method="first", ascending=False).astype(int)
    )
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    frame["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = frame.loc[:, list(TOP_PAGES_OUTPUT)].sort_values("page_rank").reset_index(drop=True)
    if out.duplicated(subset=list(TOP_PAGES_KEYS)).any():
        raise CdmError("cdm_top_pages duplicate business keys")
    return CdmMartResult(mart="top_pages", dataframe=out, month=month_start)


def persist_top_pages(
    cdm_engine: Engine, dataframe: pd.DataFrame, month: MonthInput, *, mode: str = "replace"
) -> PersistCdmResult:
    return persist_cdm_month(
        cdm_engine,
        dataframe,
        month,
        mart="top_pages",
        table="cdm_top_pages",
        output_columns=TOP_PAGES_OUTPUT,
        business_key_columns=TOP_PAGES_KEYS,
        required_non_null=("month", "page_url", "page_rank", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


__all__ = [
    "DEVICE_SEGMENTS",
    "DEVICE_SECTION_KEYS",
    "DEVICE_SECTION_OUTPUT",
    "SECTIONS_MIX_KEYS",
    "SECTIONS_MIX_OUTPUT",
    "TOP_PAGES_KEYS",
    "TOP_PAGES_OUTPUT",
    "build_device_section_mix",
    "build_sections_mix",
    "build_top_pages",
    "persist_device_section_mix",
    "persist_sections_mix",
    "persist_top_pages",
]
