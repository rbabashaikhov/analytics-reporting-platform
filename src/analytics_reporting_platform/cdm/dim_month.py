"""CDM month dimension."""

from __future__ import annotations

from datetime import datetime
from typing import Final, Optional

import pandas as pd

from analytics_reporting_platform.cdm.models import CdmError, CdmMartResult, CdmPeriodConfig
from analytics_reporting_platform.cdm.repository import PersistCdmResult, persist_cdm_window
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

DIM_MONTH_OUTPUT: Final[tuple[str, ...]] = (
    "month",
    "year",
    "month_num",
    "year_month",
    "month_sort",
    "is_loaded_month",
    "cdm_dt",
)
DIM_MONTH_KEYS: Final[tuple[str, ...]] = ("month",)


def build_dim_month(
    period: CdmPeriodConfig,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    """Build ``cdm_dim_month`` for trend_start..report_end (historical notebook)."""
    months = pd.date_range(period.trend_start, period.report_end, freq="MS")
    if len(months) == 0:
        raise CdmError("cdm_dim_month produced empty month range")
    df = pd.DataFrame({"month": months})
    df["year"] = df["month"].dt.year
    df["month_num"] = df["month"].dt.month
    df["year_month"] = df["month"].dt.strftime("%Y-%m")
    df["month_sort"] = df["month"].dt.strftime("%Y%m").astype(int)
    df["is_loaded_month"] = True
    df["month"] = df["month"].dt.date
    df["cdm_dt"] = coalesce_platform_now(cdm_dt)
    out = df.loc[:, list(DIM_MONTH_OUTPUT)].sort_values("month").reset_index(drop=True)
    if out.duplicated(subset=list(DIM_MONTH_KEYS)).any():
        raise CdmError("cdm_dim_month duplicate months")
    return CdmMartResult(
        mart="dim_month",
        dataframe=out,
        window_start=period.trend_start,
        window_end=period.report_end,
    )


def persist_dim_month(
    cdm_engine,
    dataframe: pd.DataFrame,
    period: CdmPeriodConfig,
    *,
    mode: str = "replace",
) -> PersistCdmResult:
    return persist_cdm_window(
        cdm_engine,
        dataframe,
        mart="dim_month",
        table="cdm_dim_month",
        window_start=period.trend_start,
        window_end=period.report_end,
        output_columns=DIM_MONTH_OUTPUT,
        business_key_columns=DIM_MONTH_KEYS,
        required_non_null=("month", "year", "month_num", "year_month", "month_sort", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


__all__ = [
    "DIM_MONTH_KEYS",
    "DIM_MONTH_OUTPUT",
    "build_dim_month",
    "persist_dim_month",
]
