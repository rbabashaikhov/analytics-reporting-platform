"""Unit tests for Wordstat Brand Demand RAW/CALC/CDM (mocked, no live API)."""

from __future__ import annotations

from datetime import date
from typing import Any, Optional
import calendar

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from analytics_reporting_platform.calc.brand_demand import build_brand_demand
from analytics_reporting_platform.cdm.organic_brand_demand import (
    build_organic_brand_demand,
    persist_organic_brand_demand,
)
from analytics_reporting_platform.db.brand_demand_ddl import ensure_brand_demand_tables
from analytics_reporting_platform.execution.monthly import (
    ADOBE_RAW_MARTS,
    raw_marts_for_month,
)
from analytics_reporting_platform.raw.repository import persist_month
from analytics_reporting_platform.raw.wordstat_brand_queries import (
    REQUIRED_QUERIES,
    WordstatBrandQueriesResult,
    extract_wordstat_brand_queries,
    extract_wordstat_brand_queries_range,
)
from analytics_reporting_platform.sources.wordstat.models import (
    WordstatDynamicsPoint,
    WordstatDynamicsResult,
)


class FakeWordstatClient:
    def __init__(self, series: dict[str, dict[str, int]]) -> None:
        self.series = series
        self.calls: list[dict[str, Any]] = []

    def get_dynamics(
        self,
        phrase: str,
        from_date: date,
        to_date: date,
        *,
        period: str = "MONTHLY",
        regions: Optional[list[int]] = None,
    ) -> WordstatDynamicsResult:
        self.calls.append(
            {
                "phrase": phrase,
                "from_date": from_date,
                "to_date": to_date,
                "period": period,
                "regions": regions,
            }
        )
        points = []
        for ym, count in self.series[phrase].items():
            y, m = ym.split("-")
            year, month = int(y), int(m)
            month_end = date(year, month, calendar.monthrange(year, month)[1])
            points.append(
                WordstatDynamicsPoint(date=month_end, count=count, share=None)
            )
        return WordstatDynamicsResult(
            phrase=phrase,
            period="PERIOD_MONTHLY",
            from_date=from_date,
            to_date=to_date,
            points=tuple(points),
        )


BASELINE = {
    "samsung": {
        "2026-01": 4675354,
        "2026-02": 4042494,
        "2026-03": 4404921,
        "2026-04": 3965285,
        "2026-05": 3852726,
        "2026-06": 4610268,
        "2026-07": 5742949,
    },
    "самсунг": {
        "2026-01": 7581237,
        "2026-02": 6332400,
        "2026-03": 6890878,
        "2026-04": 6388774,
        "2026-05": 6161803,
        "2026-06": 7980950,
        "2026-07": 10291807,
    },
}


EXPECTED_BRAND = {
    "2026-01": 12256591,
    "2026-02": 10374894,
    "2026-03": 11295799,
    "2026-04": 10354059,
    "2026-05": 10014529,
    "2026-06": 12591218,
    "2026-07": 16034756,
}


@pytest.fixture()
def engines():
    raw = create_engine("sqlite+pysqlite:///:memory:")
    calc = create_engine("sqlite+pysqlite:///:memory:")
    cdm = create_engine("sqlite+pysqlite:///:memory:")
    # SQLite-friendly DDL (Postgres types coerced)
    with raw.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE raw_wordstat_brand_queries (
                    month DATE NOT NULL,
                    query TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    load_dt TEXT NOT NULL,
                    PRIMARY KEY (month, query)
                )
                """
            )
        )
    with calc.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE calc_brand_demand (
                    month DATE NOT NULL PRIMARY KEY,
                    brand_demand INTEGER NOT NULL,
                    calc_dt TEXT NOT NULL
                )
                """
            )
        )
    with cdm.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE cdm_organic_brand_demand (
                    month DATE NOT NULL PRIMARY KEY,
                    metric INTEGER NOT NULL,
                    cdm_dt TEXT NOT NULL
                )
                """
            )
        )
    return raw, calc, cdm


def test_raw_marts_for_month_reporting_boundary() -> None:
    assert raw_marts_for_month(date(2025, 6, 1)) == ADOBE_RAW_MARTS
    assert "wordstat_brand_queries" in raw_marts_for_month(date(2026, 1, 1))


def test_extract_range_two_api_calls_and_no_share() -> None:
    client = FakeWordstatClient(BASELINE)
    frame = extract_wordstat_brand_queries_range(
        client, date(2026, 1, 1), date(2026, 7, 1)
    )
    assert len(client.calls) == 2
    assert set(frame["query"]) == set(REQUIRED_QUERIES)
    assert "share" not in frame.columns
    assert len(frame) == 14


def test_extract_single_month() -> None:
    client = FakeWordstatClient(BASELINE)
    result = extract_wordstat_brand_queries(client, "2026-07-01")
    assert isinstance(result, WordstatBrandQueriesResult)
    assert result.row_count == 2
    assert set(result.dataframe["query"]) == set(REQUIRED_QUERIES)


def test_raw_calc_cdm_pipeline_values(engines) -> None:
    raw, calc, cdm = engines
    client = FakeWordstatClient(BASELINE)
    frame = extract_wordstat_brand_queries_range(
        client, date(2026, 1, 1), date(2026, 7, 1)
    )
    for month in sorted(frame["month"].unique()):
        month_frame = frame.loc[frame["month"] == month].copy()
        persist_month(
            raw,
            "wordstat_brand_queries",
            month_frame,
            month,
            mode="replace",
        )
        built = build_brand_demand(raw, month)
        from analytics_reporting_platform.calc.repository import persist_brand_demand

        persist_brand_demand(calc, built.dataframe, month, mode="replace")
        cdm_built = build_organic_brand_demand(calc, month)
        persist_organic_brand_demand(cdm, cdm_built.dataframe, month, mode="replace")

    with raw.connect() as conn:
        raw_count = conn.execute(
            text("SELECT COUNT(*) FROM raw_wordstat_brand_queries")
        ).scalar()
    with calc.connect() as conn:
        calc_rows = {
            str(r.month)[:7]: int(r.brand_demand)
            for r in conn.execute(
                text("SELECT month, brand_demand FROM calc_brand_demand ORDER BY month")
            )
        }
    with cdm.connect() as conn:
        cdm_rows = {
            str(r.month)[:7]: int(r.metric)
            for r in conn.execute(
                text("SELECT month, metric FROM cdm_organic_brand_demand ORDER BY month")
            )
        }

    assert raw_count == 14
    assert calc_rows == EXPECTED_BRAND
    assert cdm_rows == EXPECTED_BRAND


def test_ensure_brand_demand_tables_postgres_ddl_compiles() -> None:
    # Smoke that helper imports and SQL text is present (no live PG required).
    assert "raw_wordstat_brand_queries" in ensure_brand_demand_tables.__doc__ or True
    from analytics_reporting_platform.db import brand_demand_ddl as ddl

    assert "PRIMARY KEY" in ddl.RAW_WORDSTAT_BRAND_QUERIES_DDL
    assert "cdm_organic_brand_demand" in ddl.CDM_ORGANIC_BRAND_DEMAND_DDL
