"""Table/key registry for read-only Monthly validation (no KPI disclosure)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from analytics_reporting_platform.raw.repository import MART_SPECS

LayerName = Literal["raw", "calc", "cdm"]


@dataclass(frozen=True)
class ValidationTableSpec:
    """Structural metadata for one Monthly table check."""

    layer: LayerName
    mart: str
    table: str
    business_key: tuple[str, ...]
    # month-scoped row filter uses column ``month`` for all current Monthly marts.
    scope: Literal["month", "window_contains_month"] = "month"


def _raw_specs() -> tuple[ValidationTableSpec, ...]:
    out: list[ValidationTableSpec] = []
    for logical, spec in MART_SPECS.items():
        out.append(
            ValidationTableSpec(
                layer="raw",
                mart=logical,
                table=spec.table_name,
                business_key=spec.business_key,
            )
        )
    return tuple(out)


CALC_SPECS: tuple[ValidationTableSpec, ...] = (
    ValidationTableSpec(
        "calc", "monthly_kpi", "calc_monthly_kpi", ("month", "segment_name", "metric_name")
    ),
    ValidationTableSpec(
        "calc", "monthly_channel_mix", "calc_monthly_channel_mix", ("month", "channel_name")
    ),
    ValidationTableSpec(
        "calc",
        "monthly_search_engines",
        "calc_monthly_search_engines",
        ("month", "search_engine"),
    ),
    ValidationTableSpec(
        "calc",
        "monthly_section_share",
        "calc_monthly_section_share",
        ("month", "segment_name", "section_name"),
    ),
    ValidationTableSpec(
        "calc",
        "explore_buying_guides_kpi",
        "calc_explore_buying_guides_kpi",
        ("month", "section_name", "kpi_name"),
    ),
    ValidationTableSpec("calc", "brand_demand", "calc_brand_demand", ("month",)),
)

# Month-scoped CDM + window tables that must contain the target month.
CDM_SPECS: tuple[ValidationTableSpec, ...] = (
    ValidationTableSpec("cdm", "dim_month", "cdm_dim_month", ("month",), "window_contains_month"),
    ValidationTableSpec("cdm", "traffic_kpi", "cdm_traffic_kpi", ("month", "kpi_name")),
    ValidationTableSpec(
        "cdm",
        "overall_traffic_trend",
        "cdm_overall_traffic_trend",
        ("month",),
        "window_contains_month",
    ),
    ValidationTableSpec(
        "cdm", "traffic_source_mix", "cdm_traffic_source_mix", ("month", "channel_name")
    ),
    ValidationTableSpec("cdm", "geo_traffic_kpi", "cdm_geo_traffic_kpi", ("month",)),
    ValidationTableSpec(
        "cdm",
        "geo_traffic_trend",
        "cdm_geo_traffic_trend",
        ("month",),
        "window_contains_month",
    ),
    ValidationTableSpec(
        "cdm",
        "geo_traffic_by_section",
        "cdm_geo_traffic_by_section",
        ("month", "section_name"),
    ),
    ValidationTableSpec(
        "cdm", "organic_search_kpi", "cdm_organic_search_kpi", ("month", "kpi_name")
    ),
    ValidationTableSpec(
        "cdm",
        "organic_search_engine_mix",
        "cdm_organic_search_engine_mix",
        ("month", "search_engine"),
    ),
    ValidationTableSpec(
        "cdm",
        "organic_search_engine_trend",
        "cdm_organic_search_engine_trend",
        ("month",),
        "window_contains_month",
    ),
    ValidationTableSpec(
        "cdm", "organic_brand_demand", "cdm_organic_brand_demand", ("month",)
    ),
    ValidationTableSpec(
        "cdm",
        "explore_buying_guides_kpi",
        "cdm_explore_buying_guides_kpi",
        ("month", "section_name", "kpi_name"),
    ),
    ValidationTableSpec(
        "cdm",
        "explore_buying_guides_trend",
        "cdm_explore_buying_guides_trend",
        ("month",),
        "window_contains_month",
    ),
    ValidationTableSpec(
        "cdm", "sections_mix", "cdm_sections_mix", ("month", "section_name")
    ),
    ValidationTableSpec(
        "cdm", "top_pages", "cdm_top_pages", ("month", "page_url")
    ),
    ValidationTableSpec(
        "cdm",
        "device_section_mix",
        "cdm_device_section_mix",
        ("month", "device_segment", "section_name"),
    ),
)

RAW_SPECS: tuple[ValidationTableSpec, ...] = _raw_specs()
ALL_SPECS: tuple[ValidationTableSpec, ...] = RAW_SPECS + CALC_SPECS + CDM_SPECS

__all__ = [
    "ALL_SPECS",
    "CALC_SPECS",
    "CDM_SPECS",
    "LayerName",
    "RAW_SPECS",
    "ValidationTableSpec",
]
