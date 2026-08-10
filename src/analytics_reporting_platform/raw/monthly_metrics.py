"""Monthly RAW metrics extractor (``raw_aa_monthly_metrics``) — in-memory only.

Migrates historical Monthly notebook semantics for the metrics mart.
Does not write to PostgreSQL. Does not implement by-section or private-only URL marts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Optional, Protocol, Sequence

import pandas as pd

from analytics_reporting_platform.config.client_config import AdobeClientConfig
from analytics_reporting_platform.db.timestamps import coalesce_platform_now
from analytics_reporting_platform.raw.models import (
    MonthlyMetricsResult,
    MonthInput,
    RawExtractionError,
    adobe_month_date_range,
    parse_month_start,
)

# Historical mart specification: Adobe Analytics public metric identifiers
# used by the Monthly metrics RAW design. Not transport concerns; not secrets.
MONTHLY_METRICS_METRIC_MAP: Mapping[str, str] = {
    "visits": "metrics/visits",
    "avg_time": "metrics/averagetimespentonsite",
    "bounce_rate": "metrics/bouncerate",
}

# Historical rule: "total" (no segment filter) requests all three metrics;
# every other configured segment requests visits only.
_TOTAL_SEGMENT_NAME = "total"
_TOTAL_METRIC_NAMES: tuple[str, ...] = ("visits", "avg_time", "bounce_rate")
_SEGMENT_METRIC_NAMES: tuple[str, ...] = ("visits",)

OUTPUT_COLUMNS: tuple[str, ...] = (
    "month",
    "metric_name",
    "metric_value",
    "segment_name",
    "segment_id",
    "load_dt",
)

BUSINESS_KEY_COLUMNS: tuple[str, ...] = ("month", "metric_name", "segment_name")


class SupportsPostReport(Protocol):
    def post_report(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class MonthlyMetricsRequestSpec:
    """One planned Adobe Reports request for the metrics mart."""

    segment_name: str
    segment_id: Optional[str]
    metric_names: tuple[str, ...]
    payload: Mapping[str, Any]

    def __repr__(self) -> str:
        return (
            "MonthlyMetricsRequestSpec("
            f"segment_name={self.segment_name!r}, "
            "segment_id=***, "
            f"metric_names={list(self.metric_names)!r})"
        )


def metric_names_for_segment(segment_name: str) -> tuple[str, ...]:
    """Return historical metric set for a logical segment name."""
    if segment_name == _TOTAL_SEGMENT_NAME:
        return _TOTAL_METRIC_NAMES
    return _SEGMENT_METRIC_NAMES


def build_metrics_payload(
    *,
    metric_names: Sequence[str],
    date_from: str,
    date_to: str,
    segment_id: Optional[str] = None,
) -> dict[str, Any]:
    """Build a Reports API payload for the metrics mart (rsid injected by transport)."""
    unknown = [name for name in metric_names if name not in MONTHLY_METRICS_METRIC_MAP]
    if unknown:
        raise RawExtractionError(
            f"Unknown metric name(s) for monthly metrics mart: {unknown!r}"
        )

    metric_ids = [MONTHLY_METRICS_METRIC_MAP[name] for name in metric_names]
    global_filters: list[dict[str, Any]] = [
        {
            "type": "dateRange",
            "dateRange": f"{date_from}/{date_to}",
        }
    ]
    if segment_id:
        global_filters.append(
            {
                "type": "segment",
                "segmentId": segment_id,
            }
        )

    return {
        "globalFilters": global_filters,
        "metricContainer": {
            "metrics": [
                {"columnId": str(index), "id": metric_id}
                for index, metric_id in enumerate(metric_ids)
            ]
        },
        "settings": {
            "countRepeatInstances": True,
            "includeAnnotations": False,
            "limit": 1,
            "page": 0,
            "nonesBehavior": "return-nones",
        },
    }


def build_monthly_metrics_requests(
    client_config: AdobeClientConfig,
    month: MonthInput,
) -> list[MonthlyMetricsRequestSpec]:
    """Build the ordered request plan for one month (no HTTP)."""
    month_start = parse_month_start(month)
    if _TOTAL_SEGMENT_NAME not in client_config.segments:
        raise RawExtractionError(
            f"Monthly metrics mart requires a {_TOTAL_SEGMENT_NAME!r} segment entry in client config"
        )

    date_from, date_to = adobe_month_date_range(month_start)
    plans: list[MonthlyMetricsRequestSpec] = []

    # Preserve private config key order (historical SEGMENTS dict order).
    for segment_name in client_config.segments:
        segment_id = client_config.segment_id(segment_name)
        metric_names = metric_names_for_segment(segment_name)
        payload = build_metrics_payload(
            metric_names=metric_names,
            date_from=date_from,
            date_to=date_to,
            segment_id=segment_id,
        )
        plans.append(
            MonthlyMetricsRequestSpec(
                segment_name=segment_name,
                segment_id=segment_id,
                metric_names=metric_names,
                payload=payload,
            )
        )
    return plans


def _normalize_totals(
    response: Mapping[str, Any],
    metric_names: Sequence[str],
) -> list[Optional[float]]:
    if not isinstance(response, Mapping):
        raise RawExtractionError("Adobe metrics response must be a mapping")

    summary = response.get("summaryData")
    totals_raw: Any = None
    if isinstance(summary, Mapping):
        totals_raw = summary.get("totals")

    if totals_raw is None:
        return [None] * len(metric_names)

    if not isinstance(totals_raw, list):
        raise RawExtractionError(
            "Adobe metrics response summaryData.totals must be a list when present"
        )

    if len(totals_raw) == 0:
        return [None] * len(metric_names)

    if len(totals_raw) < len(metric_names):
        raise RawExtractionError(
            "Adobe metrics response returned fewer totals than requested metrics"
        )

    values: list[Optional[float]] = []
    for raw in totals_raw[: len(metric_names)]:
        if raw is None:
            values.append(None)
            continue
        try:
            values.append(float(raw))
        except (TypeError, ValueError) as exc:
            raise RawExtractionError(
                "Adobe metrics response contained a non-numeric total"
            ) from exc
    return values


def extract_monthly_metrics(
    reports_client: SupportsPostReport,
    client_config: AdobeClientConfig,
    month: MonthInput,
    *,
    load_dt: Optional[datetime] = None,
) -> MonthlyMetricsResult:
    """Extract ``raw_aa_monthly_metrics`` for one month into an in-memory DataFrame.

    Parameters
    ----------
    reports_client:
        Generic Adobe Reports transport.
    client_config:
        Private Adobe client config (segment ID lookups).
    month:
        Explicit month start (``date`` / ``datetime`` / ``YYYY-MM-01``).
    load_dt:
        Optional fixed load timestamp (tests). Defaults to timezone-aware Europe/Moscow ``platform_now()``.
    """
    month_start = parse_month_start(month)
    plans = build_monthly_metrics_requests(client_config, month_start)
    runtime_load_dt = coalesce_platform_now(load_dt)

    rows: list[dict[str, Any]] = []
    for plan in plans:
        response = reports_client.post_report(plan.payload)
        totals = _normalize_totals(response, plan.metric_names)
        for metric_name, value in zip(plan.metric_names, totals):
            rows.append(
                {
                    "month": month_start,
                    "metric_name": metric_name,
                    "metric_value": value,
                    "segment_name": plan.segment_name,
                    "segment_id": plan.segment_id,
                    "load_dt": runtime_load_dt,
                }
            )

    dataframe = pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))
    if not dataframe.empty:
        dataframe["month"] = pd.to_datetime(dataframe["month"]).dt.date
        dataframe["metric_value"] = pd.to_numeric(dataframe["metric_value"], errors="coerce")
        dataframe["load_dt"] = pd.to_datetime(dataframe["load_dt"])

    return MonthlyMetricsResult(
        month=month_start,
        dataframe=dataframe,
        request_count=len(plans),
    )


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "MONTHLY_METRICS_METRIC_MAP",
    "OUTPUT_COLUMNS",
    "MonthlyMetricsRequestSpec",
    "build_metrics_payload",
    "build_monthly_metrics_requests",
    "extract_monthly_metrics",
    "metric_names_for_segment",
]
