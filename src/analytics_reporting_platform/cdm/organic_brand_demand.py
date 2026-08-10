"""CDM Organic Brand Demand series for Organic Search Trends chart."""

from __future__ import annotations

from datetime import datetime
from typing import Final, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.cdm.models import (
    CdmError,
    CdmMartResult,
    MonthInput,
    parse_month_start,
)
from analytics_reporting_platform.cdm.repository import PersistCdmResult, persist_cdm_month
from analytics_reporting_platform.db.timestamps import coalesce_platform_now

OUTPUT_COLUMNS: Final[tuple[str, ...]] = ("month", "metric", "cdm_dt")
BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month",)
SOURCE_TABLE: Final = "calc_brand_demand"
TARGET_TABLE: Final = "cdm_organic_brand_demand"
LOGICAL_MART: Final = "organic_brand_demand"


def build_organic_brand_demand(
    calc_engine: Engine,
    month: MonthInput,
    *,
    cdm_dt: Optional[datetime] = None,
) -> CdmMartResult:
    """Publish CALC brand_demand as CDM ``metric`` (one row per month)."""
    month_start = parse_month_start(month)
    sql = text(
        """
        SELECT month, brand_demand
        FROM calc_brand_demand
        WHERE month = :month
        """
    )
    with calc_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})
    if df.empty:
        raise CdmError("cdm_organic_brand_demand source returned no rows for month")
    if len(df) != 1:
        raise CdmError("cdm_organic_brand_demand expected exactly one CALC row per month")
    if pd.isna(df.iloc[0]["brand_demand"]):
        raise CdmError("cdm_organic_brand_demand source metric is null")

    metric = int(df.iloc[0]["brand_demand"])
    if metric < 0:
        raise CdmError("cdm_organic_brand_demand source metric is negative")

    out = pd.DataFrame(
        [
            {
                "month": month_start,
                "metric": metric,
                "cdm_dt": coalesce_platform_now(cdm_dt),
            }
        ],
        columns=list(OUTPUT_COLUMNS),
    )
    return CdmMartResult(mart=LOGICAL_MART, dataframe=out, month=month_start)


def persist_organic_brand_demand(
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
        mart=LOGICAL_MART,
        table=TARGET_TABLE,
        output_columns=OUTPUT_COLUMNS,
        business_key_columns=BUSINESS_KEY_COLUMNS,
        required_non_null=("month", "metric", "cdm_dt"),
        mode=mode,  # type: ignore[arg-type]
    )


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "LOGICAL_MART",
    "OUTPUT_COLUMNS",
    "SOURCE_TABLE",
    "TARGET_TABLE",
    "build_organic_brand_demand",
    "persist_organic_brand_demand",
]
