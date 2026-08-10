"""Structural contracts for Monthly RAW / CALC / CDM marts (read-only checks)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final, Literal, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.calc import explore_buying_guides_kpi as explore_calc
from analytics_reporting_platform.calc import brand_demand as brand_demand_mod
from analytics_reporting_platform.calc import monthly_channel_mix as channel_mix
from analytics_reporting_platform.calc import monthly_kpi as kpi_mod
from analytics_reporting_platform.calc import monthly_search_engines as search_engines
from analytics_reporting_platform.calc import monthly_section_share as section_share
from analytics_reporting_platform.cdm import dim_month as dim_month_mod
from analytics_reporting_platform.cdm import explore_buying_guides as explore_cdm
from analytics_reporting_platform.cdm import geo as geo_mod
from analytics_reporting_platform.cdm import organic_brand_demand as organic_brand_mod
from analytics_reporting_platform.cdm import organic_search as organic_mod
from analytics_reporting_platform.cdm import sections as sections_mod
from analytics_reporting_platform.cdm import traffic as traffic_mod
from analytics_reporting_platform.raw.repository import MART_SPECS
from analytics_reporting_platform.validation.specs import (
    CALC_SPECS,
    CDM_SPECS,
    LayerName,
    RAW_SPECS,
    ValidationTableSpec,
)
from analytics_reporting_platform.validation.types import SuiteCheck

ShareBound = tuple[float, float]


@dataclass(frozen=True)
class MartContract:
    """Contract metadata derived from real persist/build definitions."""

    layer: LayerName
    mart: str
    table: str
    columns: tuple[str, ...]
    business_key: tuple[str, ...]
    required_non_null: tuple[str, ...]
    # Columns that must be >= 0 when present (visits / counts / shares).
    non_negative: tuple[str, ...] = ()
    # Share/rate columns expected in [lo, hi] when not null.
    share_bounds: tuple[tuple[str, ShareBound], ...] = ()
    month_grain: bool = True


def _quote(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


SHARE_01: Final[ShareBound] = (0.0, 1.0)
# bounce_rate historically stored as a fraction in [0, 1]
RATE_01: Final[ShareBound] = (0.0, 1.0)


def build_mart_contracts() -> tuple[MartContract, ...]:
    """Authoritative Monthly mart contracts (no invented universal rules)."""
    raw: list[MartContract] = []
    for logical, spec in MART_SPECS.items():
        non_neg: tuple[str, ...]
        if logical == "monthly_by_url":
            non_neg = ("visits",)
        elif logical == "wordstat_brand_queries":
            non_neg = ("count",)
        elif logical in ("monthly_metrics", "monthly_by_section"):
            # metric_value may be duration/rate; non-neg enforced selectively in value checks.
            non_neg = ()
        else:
            non_neg = ()
        raw.append(
            MartContract(
                layer="raw",
                mart=logical,
                table=spec.table_name,
                columns=spec.columns,
                business_key=spec.business_key,
                required_non_null=spec.non_null_columns,
                non_negative=non_neg,
            )
        )

    calc = (
        MartContract(
            layer="calc",
            mart="monthly_kpi",
            table="calc_monthly_kpi",
            columns=kpi_mod.OUTPUT_COLUMNS,
            business_key=kpi_mod.BUSINESS_KEY_COLUMNS,
            required_non_null=(
                "month",
                "segment_name",
                "metric_name",
                "change_type",
                "value_format",
                "calc_dt",
            ),
            # metric_value itself may be rate/duration; non-neg on visits-like enforced in ranges.
            non_negative=(),
            share_bounds=(),
        ),
        MartContract(
            layer="calc",
            mart="monthly_channel_mix",
            table="calc_monthly_channel_mix",
            columns=channel_mix.OUTPUT_COLUMNS,
            business_key=channel_mix.BUSINESS_KEY_COLUMNS,
            required_non_null=("month", "channel_name", "calc_dt"),
            non_negative=("channel_visits", "channel_share"),
            share_bounds=(("channel_share", SHARE_01), ("prev_month_share", SHARE_01)),
        ),
        MartContract(
            layer="calc",
            mart="monthly_search_engines",
            table="calc_monthly_search_engines",
            columns=search_engines.OUTPUT_COLUMNS,
            business_key=search_engines.BUSINESS_KEY_COLUMNS,
            required_non_null=("month", "search_engine", "calc_dt"),
            non_negative=("search_engine_visits", "search_engine_share"),
            share_bounds=(
                ("search_engine_share", SHARE_01),
                ("prev_month_share", SHARE_01),
            ),
        ),
        MartContract(
            layer="calc",
            mart="monthly_section_share",
            table="calc_monthly_section_share",
            columns=section_share.OUTPUT_COLUMNS,
            business_key=section_share.BUSINESS_KEY_COLUMNS,
            required_non_null=("month", "segment_name", "section_name", "calc_dt"),
            non_negative=("section_visits", "section_share"),
            share_bounds=(("section_share", SHARE_01), ("prev_month_share", SHARE_01)),
        ),
        MartContract(
            layer="calc",
            mart="explore_buying_guides_kpi",
            table="calc_explore_buying_guides_kpi",
            columns=explore_calc.OUTPUT_COLUMNS,
            business_key=explore_calc.BUSINESS_KEY_COLUMNS,
            required_non_null=(
                "month",
                "section_name",
                "kpi_name",
                "display_order",
                "calc_dt",
            ),
            non_negative=("kpi_value",),
        ),
        MartContract(
            layer="calc",
            mart="brand_demand",
            table="calc_brand_demand",
            columns=brand_demand_mod.OUTPUT_COLUMNS,
            business_key=brand_demand_mod.BUSINESS_KEY_COLUMNS,
            required_non_null=("month", "brand_demand", "calc_dt"),
            non_negative=("brand_demand",),
        ),
    )

    cdm = (
        MartContract(
            layer="cdm",
            mart="dim_month",
            table="cdm_dim_month",
            columns=dim_month_mod.DIM_MONTH_OUTPUT,
            business_key=dim_month_mod.DIM_MONTH_KEYS,
            required_non_null=(
                "month",
                "year",
                "month_num",
                "year_month",
                "month_sort",
                "cdm_dt",
            ),
        ),
        MartContract(
            layer="cdm",
            mart="traffic_kpi",
            table="cdm_traffic_kpi",
            columns=traffic_mod.TRAFFIC_KPI_OUTPUT,
            business_key=traffic_mod.TRAFFIC_KPI_KEYS,
            required_non_null=("month", "kpi_name", "display_order", "cdm_dt"),
            non_negative=(),
        ),
        MartContract(
            layer="cdm",
            mart="overall_traffic_trend",
            table="cdm_overall_traffic_trend",
            columns=traffic_mod.OVERALL_TREND_OUTPUT,
            business_key=traffic_mod.OVERALL_TREND_KEYS,
            required_non_null=("month", "cdm_dt"),
            non_negative=(
                "organic_visits",
                "own_group_visits",
                "paid_group_visits",
                "other_group_visits",
                "total_visits",
            ),
        ),
        MartContract(
            layer="cdm",
            mart="traffic_source_mix",
            table="cdm_traffic_source_mix",
            columns=traffic_mod.SOURCE_MIX_OUTPUT,
            business_key=traffic_mod.SOURCE_MIX_KEYS,
            required_non_null=(
                "month",
                "channel_name",
                "month_type",
                "display_order",
                "cdm_dt",
            ),
            non_negative=("channel_visits", "channel_share"),
            share_bounds=(("channel_share", SHARE_01),),
        ),
        MartContract(
            layer="cdm",
            mart="geo_traffic_kpi",
            table="cdm_geo_traffic_kpi",
            columns=geo_mod.GEO_KPI_OUTPUT,
            business_key=geo_mod.GEO_KPI_KEYS,
            required_non_null=("month", "cdm_dt"),
            non_negative=("geo_visits",),
        ),
        MartContract(
            layer="cdm",
            mart="geo_traffic_trend",
            table="cdm_geo_traffic_trend",
            columns=geo_mod.GEO_TREND_OUTPUT,
            business_key=geo_mod.GEO_TREND_KEYS,
            required_non_null=("month", "cdm_dt"),
            non_negative=("geo_visits",),
        ),
        MartContract(
            layer="cdm",
            mart="geo_traffic_by_section",
            table="cdm_geo_traffic_by_section",
            columns=geo_mod.GEO_SECTION_OUTPUT,
            business_key=geo_mod.GEO_SECTION_KEYS,
            required_non_null=("month", "section_name", "display_order", "cdm_dt"),
            non_negative=("section_visits", "section_share"),
            share_bounds=(("section_share", SHARE_01),),
        ),
        MartContract(
            layer="cdm",
            mart="organic_search_kpi",
            table="cdm_organic_search_kpi",
            columns=organic_mod.ORGANIC_KPI_OUTPUT,
            business_key=organic_mod.ORGANIC_KPI_KEYS,
            required_non_null=("month", "kpi_name", "display_order", "cdm_dt"),
            non_negative=("kpi_value",),
        ),
        MartContract(
            layer="cdm",
            mart="organic_brand_demand",
            table="cdm_organic_brand_demand",
            columns=organic_brand_mod.OUTPUT_COLUMNS,
            business_key=organic_brand_mod.BUSINESS_KEY_COLUMNS,
            required_non_null=("month", "metric", "cdm_dt"),
            non_negative=("metric",),
        ),
        MartContract(
            layer="cdm",
            mart="organic_search_engine_mix",
            table="cdm_organic_search_engine_mix",
            columns=organic_mod.ENGINE_MIX_OUTPUT,
            business_key=organic_mod.ENGINE_MIX_KEYS,
            required_non_null=(
                "month",
                "search_engine",
                "month_type",
                "display_order",
                "cdm_dt",
            ),
            non_negative=("search_engine_visits", "search_engine_share"),
            share_bounds=(("search_engine_share", SHARE_01),),
        ),
        MartContract(
            layer="cdm",
            mart="organic_search_engine_trend",
            table="cdm_organic_search_engine_trend",
            columns=organic_mod.ENGINE_TREND_OUTPUT,
            business_key=organic_mod.ENGINE_TREND_KEYS,
            required_non_null=("month", "cdm_dt"),
            non_negative=(
                "google_visits",
                "yandex_visits",
                "other_search_engines_visits",
                "organic_total_visits",
            ),
        ),
        MartContract(
            layer="cdm",
            mart="explore_buying_guides_kpi",
            table="cdm_explore_buying_guides_kpi",
            columns=explore_cdm.EXPLORE_BG_KPI_OUTPUT,
            business_key=explore_cdm.EXPLORE_BG_KPI_KEYS,
            required_non_null=(
                "month",
                "section_name",
                "kpi_name",
                "display_order",
                "cdm_dt",
            ),
            non_negative=("kpi_value",),
        ),
        MartContract(
            layer="cdm",
            mart="explore_buying_guides_trend",
            table="cdm_explore_buying_guides_trend",
            columns=explore_cdm.EXPLORE_BG_TREND_OUTPUT,
            business_key=explore_cdm.EXPLORE_BG_TREND_KEYS,
            required_non_null=("month", "cdm_dt"),
            non_negative=(
                "explore_organic_visits",
                "buying_guides_organic_visits",
                "total_organic_visits",
            ),
        ),
        MartContract(
            layer="cdm",
            mart="sections_mix",
            table="cdm_sections_mix",
            columns=sections_mod.SECTIONS_MIX_OUTPUT,
            business_key=sections_mod.SECTIONS_MIX_KEYS,
            required_non_null=(
                "month",
                "section_name",
                "month_type",
                "display_order",
                "cdm_dt",
            ),
            non_negative=("section_visits", "section_share"),
            share_bounds=(("section_share", SHARE_01),),
        ),
        MartContract(
            layer="cdm",
            mart="top_pages",
            table="cdm_top_pages",
            columns=sections_mod.TOP_PAGES_OUTPUT,
            business_key=sections_mod.TOP_PAGES_KEYS,
            required_non_null=("month", "page_url", "page_rank", "cdm_dt"),
            non_negative=("visits", "page_rank"),
        ),
        MartContract(
            layer="cdm",
            mart="device_section_mix",
            table="cdm_device_section_mix",
            columns=sections_mod.DEVICE_SECTION_OUTPUT,
            business_key=sections_mod.DEVICE_SECTION_KEYS,
            required_non_null=(
                "month",
                "device_segment",
                "section_name",
                "display_order",
                "cdm_dt",
            ),
            non_negative=("section_visits", "section_share"),
            share_bounds=(("section_share", SHARE_01),),
        ),
    )
    return tuple(raw) + calc + cdm


MART_CONTRACTS: Final[tuple[MartContract, ...]] = build_mart_contracts()


def _engine_for(layer: LayerName, engines: dict[LayerName, Engine]) -> Engine:
    return engines[layer]


def _table_exists(engine: Engine, table: str) -> bool:
    sql = text(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table
        )
        """
    )
    with engine.connect() as conn:
        return bool(conn.execute(sql, {"table": table}).scalar_one())


def _existing_columns(engine: Engine, table: str) -> set[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :table
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"table": table}).fetchall()
    return {str(r[0]) for r in rows}


def check_table_contracts(
    engines: dict[LayerName, Engine],
    *,
    contracts: Sequence[MartContract] = MART_CONTRACTS,
) -> list[SuiteCheck]:
    """Table exists + required columns present."""
    checks: list[SuiteCheck] = []
    for contract in contracts:
        engine = _engine_for(contract.layer, engines)
        exists = _table_exists(engine, contract.table)
        checks.append(
            SuiteCheck(
                category="contracts",
                name=f"contracts.{contract.layer}.{contract.mart}.table_exists",
                status="PASS" if exists else "FAIL",
                detail=None if exists else f"missing table {contract.table}",
            )
        )
        if not exists:
            checks.append(
                SuiteCheck(
                    category="contracts",
                    name=f"contracts.{contract.layer}.{contract.mart}.required_columns",
                    status="FAIL",
                    detail="skipped: table missing",
                )
            )
            continue
        cols = _existing_columns(engine, contract.table)
        missing = [c for c in contract.columns if c not in cols]
        checks.append(
            SuiteCheck(
                category="contracts",
                name=f"contracts.{contract.layer}.{contract.mart}.required_columns",
                status="PASS" if not missing else "FAIL",
                detail=None if not missing else f"missing_columns={','.join(missing)}",
            )
        )
    return checks


def check_required_dimensions_not_null(
    engines: dict[LayerName, Engine],
    *,
    contracts: Sequence[MartContract] = MART_CONTRACTS,
    months: Optional[Sequence[date]] = None,
) -> list[SuiteCheck]:
    """Required non-null dimension/metadata columns have no NULLs in scope."""
    checks: list[SuiteCheck] = []
    for contract in contracts:
        engine = _engine_for(contract.layer, engines)
        if not _table_exists(engine, contract.table):
            checks.append(
                SuiteCheck(
                    category="contracts",
                    name=f"contracts.{contract.layer}.{contract.mart}.not_null",
                    status="FAIL",
                    detail="table missing",
                )
            )
            continue
        null_parts = []
        for col in contract.required_non_null:
            null_parts.append(f"SUM(CASE WHEN {_quote(col)} IS NULL THEN 1 ELSE 0 END) AS {_quote(col)}")
        where = ""
        params: dict[str, object] = {}
        if months is not None:
            where = "WHERE month = ANY(:months)"
            params["months"] = list(months)
        sql = text(
            f"""
            SELECT {', '.join(null_parts)}
            FROM {_quote(contract.table)}
            {where}
            """
        )
        with engine.connect() as conn:
            row = conn.execute(sql, params).mappings().one()
        bad = [c for c in contract.required_non_null if int(row[c] or 0) > 0]
        checks.append(
            SuiteCheck(
                category="contracts",
                name=f"contracts.{contract.layer}.{contract.mart}.not_null",
                status="PASS" if not bad else "FAIL",
                detail=None if not bad else f"null_columns={','.join(bad)}",
            )
        )
    return checks


def check_month_grain(
    engines: dict[LayerName, Engine],
    *,
    contracts: Sequence[MartContract] = MART_CONTRACTS,
) -> list[SuiteCheck]:
    """month column is date / timestamp at month-start (day = 1)."""
    checks: list[SuiteCheck] = []
    for contract in contracts:
        if not contract.month_grain:
            continue
        engine = _engine_for(contract.layer, engines)
        if not _table_exists(engine, contract.table):
            checks.append(
                SuiteCheck(
                    category="contracts",
                    name=f"contracts.{contract.layer}.{contract.mart}.month_grain",
                    status="FAIL",
                    detail="table missing",
                )
            )
            continue
        sql = text(
            f"""
            SELECT COUNT(*) AS bad
            FROM {_quote(contract.table)}
            WHERE month IS NULL
               OR EXTRACT(DAY FROM month::timestamp) <> 1
            """
        )
        with engine.connect() as conn:
            bad = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="contracts",
                name=f"contracts.{contract.layer}.{contract.mart}.month_grain",
                status="PASS" if bad == 0 else "FAIL",
                detail=None if bad == 0 else f"non_month_start_rows={bad}",
            )
        )
    return checks


def check_uniqueness(
    engines: dict[LayerName, Engine],
    *,
    specs: Sequence[ValidationTableSpec] | None = None,
) -> list[SuiteCheck]:
    """Business-key uniqueness across full table (not only one month)."""
    if specs is None:
        specs = list(RAW_SPECS) + list(CALC_SPECS) + list(CDM_SPECS)
    checks: list[SuiteCheck] = []
    for spec in specs:
        engine = _engine_for(spec.layer, engines)
        if not _table_exists(engine, spec.table):
            checks.append(
                SuiteCheck(
                    category="uniqueness",
                    name=f"uniqueness.{spec.layer}.{spec.mart}",
                    status="FAIL",
                    detail="table missing",
                )
            )
            continue
        cols = ", ".join(_quote(c) for c in spec.business_key)
        sql = text(
            f"""
            SELECT COUNT(*) AS n FROM (
                SELECT {cols}, COUNT(*) AS c
                FROM {_quote(spec.table)}
                GROUP BY {cols}
                HAVING COUNT(*) > 1
            ) d
            """
        )
        with engine.connect() as conn:
            dups = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="uniqueness",
                name=f"uniqueness.{spec.layer}.{spec.mart}",
                status="PASS" if dups == 0 else "FAIL",
                detail=None if dups == 0 else f"duplicate_key_groups={dups}",
            )
        )
    return checks


def check_value_ranges(
    engines: dict[LayerName, Engine],
    *,
    contracts: Sequence[MartContract] = MART_CONTRACTS,
) -> list[SuiteCheck]:
    """Non-negative measures and share/rate bounds where contracted."""
    checks: list[SuiteCheck] = []
    for contract in contracts:
        engine = _engine_for(contract.layer, engines)
        if not _table_exists(engine, contract.table):
            continue

        for col in contract.non_negative:
            sql = text(
                f"""
                SELECT COUNT(*) AS n
                FROM {_quote(contract.table)}
                WHERE {_quote(col)} IS NOT NULL AND {_quote(col)} < 0
                """
            )
            with engine.connect() as conn:
                n = int(conn.execute(sql).scalar_one())
            checks.append(
                SuiteCheck(
                    category="value_ranges",
                    name=f"value_ranges.{contract.layer}.{contract.mart}.{col}_non_negative",
                    status="PASS" if n == 0 else "FAIL",
                    detail=None if n == 0 else f"negative_rows={n}",
                )
            )

        for col, (lo, hi) in contract.share_bounds:
            sql = text(
                f"""
                SELECT COUNT(*) AS n
                FROM {_quote(contract.table)}
                WHERE {_quote(col)} IS NOT NULL
                  AND ({_quote(col)} < :lo OR {_quote(col)} > :hi)
                """
            )
            with engine.connect() as conn:
                n = int(conn.execute(sql, {"lo": lo, "hi": hi}).scalar_one())
            checks.append(
                SuiteCheck(
                    category="value_ranges",
                    name=f"value_ranges.{contract.layer}.{contract.mart}.{col}_bounds",
                    status="PASS" if n == 0 else "FAIL",
                    detail=None if n == 0 else f"out_of_range_rows={n} expected=[{lo},{hi}]",
                )
            )

    # Targeted RAW / CALC metric rules (not universal).
    checks.extend(_specialized_value_checks(engines))
    return checks


def _specialized_value_checks(engines: dict[LayerName, Engine]) -> list[SuiteCheck]:
    """Metric-specific contracts that must not be generalized to all columns."""
    checks: list[SuiteCheck] = []
    raw = engines["raw"]
    calc = engines["calc"]
    cdm = engines["cdm"]

    # RAW visits measures non-negative when metric_name = visits
    if _table_exists(raw, "raw_aa_monthly_metrics"):
        sql = text(
            """
            SELECT COUNT(*) FROM raw_aa_monthly_metrics
            WHERE metric_name = 'visits' AND metric_value IS NOT NULL AND metric_value < 0
            """
        )
        with raw.connect() as conn:
            n = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="value_ranges",
                name="value_ranges.raw.monthly_metrics.visits_non_negative",
                status="PASS" if n == 0 else "FAIL",
                detail=None if n == 0 else f"negative_rows={n}",
            )
        )
        sql = text(
            """
            SELECT COUNT(*) FROM raw_aa_monthly_metrics
            WHERE metric_name = 'bounce_rate' AND metric_value IS NOT NULL
              AND (metric_value < 0 OR metric_value > 1)
            """
        )
        with raw.connect() as conn:
            n = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="value_ranges",
                name="value_ranges.raw.monthly_metrics.bounce_rate_bounds",
                status="PASS" if n == 0 else "FAIL",
                detail=None if n == 0 else f"out_of_range_rows={n}",
            )
        )

    if _table_exists(raw, "raw_aa_monthly_by_section"):
        sql = text(
            """
            SELECT COUNT(*) FROM raw_aa_monthly_by_section
            WHERE metric_value IS NOT NULL AND metric_value < 0
            """
        )
        with raw.connect() as conn:
            n = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="value_ranges",
                name="value_ranges.raw.monthly_by_section.metric_value_non_negative",
                status="PASS" if n == 0 else "FAIL",
                detail=None if n == 0 else f"negative_rows={n}",
            )
        )

    if _table_exists(calc, "calc_monthly_kpi"):
        sql = text(
            """
            SELECT COUNT(*) FROM calc_monthly_kpi
            WHERE metric_name = 'visits' AND metric_value IS NOT NULL AND metric_value < 0
            """
        )
        with calc.connect() as conn:
            n = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="value_ranges",
                name="value_ranges.calc.monthly_kpi.visits_non_negative",
                status="PASS" if n == 0 else "FAIL",
                detail=None if n == 0 else f"negative_rows={n}",
            )
        )
        sql = text(
            """
            SELECT COUNT(*) FROM calc_monthly_kpi
            WHERE metric_name = 'bounce_rate' AND metric_value IS NOT NULL
              AND (metric_value < 0 OR metric_value > 1)
            """
        )
        with calc.connect() as conn:
            n = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="value_ranges",
                name="value_ranges.calc.monthly_kpi.bounce_rate_bounds",
                status="PASS" if n == 0 else "FAIL",
                detail=None if n == 0 else f"out_of_range_rows={n}",
            )
        )

    if _table_exists(cdm, "cdm_traffic_kpi"):
        sql = text(
            """
            SELECT COUNT(*) FROM cdm_traffic_kpi
            WHERE kpi_name = 'bounce_rate' AND kpi_value IS NOT NULL
              AND (kpi_value < 0 OR kpi_value > 1)
            """
        )
        with cdm.connect() as conn:
            n = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="value_ranges",
                name="value_ranges.cdm.traffic_kpi.bounce_rate_bounds",
                status="PASS" if n == 0 else "FAIL",
                detail=None if n == 0 else f"out_of_range_rows={n}",
            )
        )
        sql = text(
            """
            SELECT COUNT(*) FROM cdm_traffic_kpi
            WHERE kpi_name IN ('total_visits', 'organic_visits')
              AND kpi_value IS NOT NULL AND kpi_value < 0
            """
        )
        with cdm.connect() as conn:
            n = int(conn.execute(sql).scalar_one())
        checks.append(
            SuiteCheck(
                category="value_ranges",
                name="value_ranges.cdm.traffic_kpi.visits_non_negative",
                status="PASS" if n == 0 else "FAIL",
                detail=None if n == 0 else f"negative_rows={n}",
            )
        )
    return checks


__all__ = [
    "MART_CONTRACTS",
    "MartContract",
    "build_mart_contracts",
    "check_month_grain",
    "check_required_dimensions_not_null",
    "check_table_contracts",
    "check_uniqueness",
    "check_value_ranges",
]
