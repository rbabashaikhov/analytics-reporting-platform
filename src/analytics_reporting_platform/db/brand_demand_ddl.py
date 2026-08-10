"""DDL helpers for Wordstat Brand Demand marts (CREATE IF NOT EXISTS)."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

RAW_WORDSTAT_BRAND_QUERIES_DDL = """
CREATE TABLE IF NOT EXISTS raw_wordstat_brand_queries (
    month DATE NOT NULL,
    query TEXT NOT NULL,
    count BIGINT NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    CONSTRAINT raw_wordstat_brand_queries_pk PRIMARY KEY (month, query)
)
"""

CALC_BRAND_DEMAND_DDL = """
CREATE TABLE IF NOT EXISTS calc_brand_demand (
    month DATE NOT NULL,
    brand_demand BIGINT NOT NULL,
    calc_dt TIMESTAMP NOT NULL,
    CONSTRAINT calc_brand_demand_pk PRIMARY KEY (month)
)
"""

CDM_ORGANIC_BRAND_DEMAND_DDL = """
CREATE TABLE IF NOT EXISTS cdm_organic_brand_demand (
    month DATE NOT NULL,
    metric BIGINT NOT NULL,
    cdm_dt TIMESTAMP NOT NULL,
    CONSTRAINT cdm_organic_brand_demand_pk PRIMARY KEY (month)
)
"""


def ensure_brand_demand_tables(
    *,
    raw_engine: Engine,
    calc_engine: Engine,
    cdm_engine: Engine,
) -> None:
    """Create Brand Demand tables when missing (no destructive changes)."""
    with raw_engine.begin() as conn:
        conn.execute(text(RAW_WORDSTAT_BRAND_QUERIES_DDL))
    with calc_engine.begin() as conn:
        conn.execute(text(CALC_BRAND_DEMAND_DDL))
    with cdm_engine.begin() as conn:
        conn.execute(text(CDM_ORGANIC_BRAND_DEMAND_DDL))


__all__ = [
    "CALC_BRAND_DEMAND_DDL",
    "CDM_ORGANIC_BRAND_DEMAND_DDL",
    "RAW_WORDSTAT_BRAND_QUERIES_DDL",
    "ensure_brand_demand_tables",
]
