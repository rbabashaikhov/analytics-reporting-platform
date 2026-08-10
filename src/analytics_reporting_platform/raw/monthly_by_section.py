"""Monthly RAW by-section extractor (``raw_aa_monthly_by_section``) — in-memory only.

Migrates historical Monthly notebook semantics for the by-section mart.
Does not write to PostgreSQL. Private-only URL mart logic is out of public Portfolio scope.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Optional, Protocol

import pandas as pd

from analytics_reporting_platform.config.client_config import AdobeClientConfig
from analytics_reporting_platform.db.timestamps import coalesce_platform_now
from analytics_reporting_platform.raw.models import (
    MonthlyBySectionResult,
    MonthInput,
    RawExtractionError,
    adobe_month_date_range,
    parse_month_start,
)

# Historical mart metric: visits only (Adobe public metric identifier).
BY_SECTION_METRIC_ID = "metrics/visits"

OUTPUT_COLUMNS: tuple[str, ...] = (
    "month",
    "segment_name",
    "segment_id",
    "metric_value",
    "section_name",
    "section_id",
    "load_dt",
)

BUSINESS_KEY_COLUMNS: tuple[str, ...] = ("month", "segment_name", "section_name")


class SupportsPostReport(Protocol):
    def post_report(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class MonthlyBySectionRequestSpec:
    """One planned Adobe Reports request for the by-section mart."""

    segment_name: str
    segment_id: Optional[str]
    section_name: str
    section_id: str
    payload: Mapping[str, Any]

    def __repr__(self) -> str:
        return (
            "MonthlyBySectionRequestSpec("
            f"segment_name={self.segment_name!r}, "
            "segment_id=***, "
            f"section_name={self.section_name!r}, "
            "section_id=***)"
        )


def build_section_payload(
    *,
    date_from: str,
    date_to: str,
    traffic_segment_id: Optional[str],
    section_segment_id: str,
) -> dict[str, Any]:
    """Build a Reports payload for one traffic × section combination.

    Historical order: dateRange, then section segment, then optional traffic segment.
    ``rsid`` is injected by the Reports transport.
    """
    if not isinstance(section_segment_id, str) or not section_segment_id.strip():
        raise RawExtractionError(
            "By-section mart requires a non-empty section segment id"
        )

    global_filters: list[dict[str, Any]] = [
        {
            "type": "dateRange",
            "dateRange": f"{date_from}/{date_to}",
        },
        {
            "type": "segment",
            "segmentId": section_segment_id,
        },
    ]
    if traffic_segment_id:
        global_filters.append(
            {
                "type": "segment",
                "segmentId": traffic_segment_id,
            }
        )

    return {
        "globalFilters": global_filters,
        "metricContainer": {
            "metrics": [{"columnId": "0", "id": BY_SECTION_METRIC_ID}]
        },
        "settings": {
            "countRepeatInstances": True,
            "includeAnnotations": False,
            "limit": 1,
            "page": 0,
            "nonesBehavior": "return-nones",
        },
    }


def build_monthly_by_section_requests(
    client_config: AdobeClientConfig,
    month: MonthInput,
) -> list[MonthlyBySectionRequestSpec]:
    """Build the ordered traffic × section request plan for one month (no HTTP)."""
    month_start = parse_month_start(month)

    if not client_config.section_traffic_segments:
        raise RawExtractionError(
            "By-section mart requires at least one section_traffic_segments entry"
        )
    if not client_config.sections:
        raise RawExtractionError(
            "By-section mart requires at least one sections entry"
        )

    date_from, date_to = adobe_month_date_range(month_start)
    plans: list[MonthlyBySectionRequestSpec] = []

    # Preserve private config key order (historical nested dict iteration).
    for segment_name in client_config.section_traffic_segments:
        traffic_id = client_config.section_traffic_segment_id(segment_name)
        for section_name in client_config.sections:
            section_id = client_config.section_id(section_name)
            if section_id is None:
                raise RawExtractionError(
                    f"By-section mart requires a non-null section id for {section_name!r}"
                )
            payload = build_section_payload(
                date_from=date_from,
                date_to=date_to,
                traffic_segment_id=traffic_id,
                section_segment_id=section_id,
            )
            plans.append(
                MonthlyBySectionRequestSpec(
                    segment_name=segment_name,
                    segment_id=traffic_id,
                    section_name=section_name,
                    section_id=section_id,
                    payload=payload,
                )
            )
    return plans


def _normalize_visits_total(response: Mapping[str, Any]) -> Optional[float]:
    if not isinstance(response, Mapping):
        raise RawExtractionError("Adobe by-section response must be a mapping")

    summary = response.get("summaryData")
    totals_raw: Any = None
    if isinstance(summary, Mapping):
        totals_raw = summary.get("totals")

    if totals_raw is None or totals_raw == []:
        return None
    if not isinstance(totals_raw, list):
        raise RawExtractionError(
            "Adobe by-section response summaryData.totals must be a list when present"
        )

    raw = totals_raw[0]
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise RawExtractionError(
            "Adobe by-section response contained a non-numeric total"
        ) from exc


def extract_monthly_by_section(
    reports_client: SupportsPostReport,
    client_config: AdobeClientConfig,
    month: MonthInput,
    *,
    load_dt: Optional[datetime] = None,
) -> MonthlyBySectionResult:
    """Extract ``raw_aa_monthly_by_section`` for one month into an in-memory DataFrame."""
    month_start = parse_month_start(month)
    plans = build_monthly_by_section_requests(client_config, month_start)
    runtime_load_dt = coalesce_platform_now(load_dt)

    rows: list[dict[str, Any]] = []
    for plan in plans:
        response = reports_client.post_report(plan.payload)
        value = _normalize_visits_total(response)
        rows.append(
            {
                "month": month_start,
                "segment_name": plan.segment_name,
                "segment_id": plan.segment_id,
                "metric_value": value,
                "section_name": plan.section_name,
                "section_id": plan.section_id,
                "load_dt": runtime_load_dt,
            }
        )

    dataframe = pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))
    if not dataframe.empty:
        dataframe["month"] = pd.to_datetime(dataframe["month"]).dt.date
        dataframe["metric_value"] = pd.to_numeric(dataframe["metric_value"], errors="coerce")
        dataframe["load_dt"] = pd.to_datetime(dataframe["load_dt"])

    return MonthlyBySectionResult(
        month=month_start,
        dataframe=dataframe,
        request_count=len(plans),
    )


__all__ = [
    "BY_SECTION_METRIC_ID",
    "BUSINESS_KEY_COLUMNS",
    "OUTPUT_COLUMNS",
    "MonthlyBySectionRequestSpec",
    "build_monthly_by_section_requests",
    "build_section_payload",
    "extract_monthly_by_section",
]
