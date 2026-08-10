"""CALC Brand Demand from Wordstat RAW queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, Optional, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.calc.models import CalcError, MonthInput, parse_month_start
from analytics_reporting_platform.db.timestamps import coalesce_platform_now
from analytics_reporting_platform.raw.wordstat_brand_queries import REQUIRED_QUERIES

OUTPUT_COLUMNS: Final[tuple[str, ...]] = ("month", "brand_demand", "calc_dt")
BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month",)
SOURCE_TABLE: Final = "raw_wordstat_brand_queries"
TARGET_TABLE: Final = "calc_brand_demand"
LOGICAL_MART: Final = "brand_demand"


@dataclass(frozen=True)
class BrandDemandResult:
    """In-memory ``calc_brand_demand`` result."""

    month: date
    dataframe: pd.DataFrame

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))


def build_brand_demand(
    raw_engine: Engine,
    month: MonthInput,
    *,
    calc_dt: Optional[datetime] = None,
    required_queries: Sequence[str] = REQUIRED_QUERIES,
) -> BrandDemandResult:
    """Sum RAW Wordstat counts for required queries into one monthly metric."""
    month_start = parse_month_start(month)
    required = tuple(str(q).strip() for q in required_queries)
    sql = text(
        """
        SELECT query, count AS count
        FROM raw_wordstat_brand_queries
        WHERE month = :month
        """
    )
    with raw_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"month": month_start})

    if df.empty:
        raise CalcError("calc_brand_demand source returned no rows for month")

    present = set(df["query"].astype(str))
    missing = [q for q in required if q not in present]
    if missing:
        raise CalcError(
            "calc_brand_demand missing required RAW query(ies): " + ", ".join(missing)
        )

    subset = df.loc[df["query"].isin(list(required))].copy()
    if subset["query"].duplicated().any():
        raise CalcError("calc_brand_demand RAW source has duplicate query rows")
    if subset["count"].isna().any():
        raise CalcError("calc_brand_demand RAW source has null count")
    if (subset["count"] < 0).any():
        raise CalcError("calc_brand_demand RAW source has negative count")

    brand_demand = int(subset["count"].sum())
    out = pd.DataFrame(
        [
            {
                "month": month_start,
                "brand_demand": brand_demand,
                "calc_dt": coalesce_platform_now(calc_dt),
            }
        ],
        columns=list(OUTPUT_COLUMNS),
    )
    return BrandDemandResult(month=month_start, dataframe=out)


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "BrandDemandResult",
    "LOGICAL_MART",
    "OUTPUT_COLUMNS",
    "SOURCE_TABLE",
    "TARGET_TABLE",
    "build_brand_demand",
]
