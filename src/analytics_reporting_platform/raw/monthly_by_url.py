"""Monthly RAW by-URL extractor (``raw_aa_monthly_by_url``) — in-memory only.

Full-private operational platform component.
Excluded from the future public Portfolio snapshot / anonymization export.

Does not write to PostgreSQL. Does not anonymize page values (publication concern).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Optional, Protocol

import pandas as pd

from analytics_reporting_platform.config.client_config import AdobeClientConfig
from analytics_reporting_platform.db.timestamps import coalesce_platform_now
from analytics_reporting_platform.raw.models import (
    MonthlyByUrlResult,
    MonthInput,
    RawExtractionError,
    adobe_month_date_range,
    parse_month_start,
)

# Historical mart metric (Adobe public metric identifier).
BY_URL_METRIC_ID = "metrics/visits"

# Historical top-N limit for a single Reports request (non-paginated).
DEFAULT_URL_LIMIT = 30

OUTPUT_COLUMNS: tuple[str, ...] = (
    "month",
    "page_url",
    "visits",
    "load_dt",
)

BUSINESS_KEY_COLUMNS: tuple[str, ...] = ("month", "page_url")


class SupportsPostReport(Protocol):
    def post_report(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class MonthlyByUrlRequestSpec:
    """One planned Adobe Reports request for the by-URL mart."""

    limit: int
    page_url_dimension: str
    payload: Mapping[str, Any]

    def __repr__(self) -> str:
        return (
            "MonthlyByUrlRequestSpec("
            f"limit={self.limit!r}, "
            "page_url_dimension=***)"
        )


def build_url_payload(
    *,
    date_from: str,
    date_to: str,
    page_url_dimension: str,
    limit: int = DEFAULT_URL_LIMIT,
) -> dict[str, Any]:
    """Build a Reports payload for top-N page URLs (rsid injected by transport)."""
    if not isinstance(page_url_dimension, str) or not page_url_dimension.strip():
        raise RawExtractionError("By-URL mart requires a non-empty page_url_dimension")
    if not isinstance(limit, int) or limit < 1:
        raise RawExtractionError("By-URL mart limit must be a positive integer")

    return {
        "globalFilters": [
            {
                "type": "dateRange",
                "dateRange": f"{date_from}/{date_to}",
            }
        ],
        "dimension": page_url_dimension.strip(),
        "metricContainer": {
            "metrics": [{"columnId": "0", "id": BY_URL_METRIC_ID}]
        },
        "settings": {
            "countRepeatInstances": True,
            "includeAnnotations": False,
            "limit": limit,
            "page": 0,
            "nonesBehavior": "return-nones",
        },
        "statistics": {
            "functions": ["col-max", "col-min"],
        },
    }


def build_monthly_by_url_requests(
    client_config: AdobeClientConfig,
    month: MonthInput,
    *,
    limit: int = DEFAULT_URL_LIMIT,
) -> list[MonthlyByUrlRequestSpec]:
    """Build the by-URL request plan for one month (single request historically)."""
    month_start = parse_month_start(month)
    dimension = client_config.require_page_url_dimension()
    date_from, date_to = adobe_month_date_range(month_start)
    payload = build_url_payload(
        date_from=date_from,
        date_to=date_to,
        page_url_dimension=dimension,
        limit=limit,
    )
    return [
        MonthlyByUrlRequestSpec(
            limit=limit,
            page_url_dimension=dimension,
            payload=payload,
        )
    ]


def _normalize_rows(
    response: Mapping[str, Any],
    *,
    month_start: date,
    load_dt: datetime,
) -> list[dict[str, Any]]:
    if not isinstance(response, Mapping):
        raise RawExtractionError("Adobe by-URL response must be a mapping")

    rows_raw = response.get("rows")
    if rows_raw is None:
        return []
    if not isinstance(rows_raw, list):
        raise RawExtractionError("Adobe by-URL response rows must be a list when present")

    out: list[dict[str, Any]] = []
    for row in rows_raw:
        if not isinstance(row, Mapping):
            raise RawExtractionError("Adobe by-URL response row must be a mapping")

        page_url = row.get("value")
        if page_url is None:
            item_id = row.get("itemId")
            page_url = str(item_id) if item_id is not None else None

        metric_data = row.get("data", [])
        if metric_data is None:
            metric_data = []
        if not isinstance(metric_data, list):
            raise RawExtractionError("Adobe by-URL response row data must be a list")

        visits_raw = metric_data[0] if metric_data else None
        visits: Optional[int]
        if visits_raw is None:
            visits = None
        else:
            try:
                visits = int(visits_raw)
            except (TypeError, ValueError) as exc:
                raise RawExtractionError(
                    "Adobe by-URL response contained a non-integer visits value"
                ) from exc

        out.append(
            {
                "month": month_start,
                "page_url": page_url,
                "visits": visits,
                "load_dt": load_dt,
            }
        )
    return out


def extract_monthly_by_url(
    reports_client: SupportsPostReport,
    client_config: AdobeClientConfig,
    month: MonthInput,
    *,
    limit: int = DEFAULT_URL_LIMIT,
    load_dt: Optional[datetime] = None,
) -> MonthlyByUrlResult:
    """Extract ``raw_aa_monthly_by_url`` for one month into an in-memory DataFrame.

    Full-private operational mart. Not for public Portfolio snapshot export.
    """
    month_start = parse_month_start(month)
    plans = build_monthly_by_url_requests(client_config, month_start, limit=limit)
    runtime_load_dt = coalesce_platform_now(load_dt)

    rows: list[dict[str, Any]] = []
    for plan in plans:
        response = reports_client.post_report(plan.payload)
        rows.extend(
            _normalize_rows(response, month_start=month_start, load_dt=runtime_load_dt)
        )

    dataframe = pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))
    if not dataframe.empty:
        dataframe["month"] = pd.to_datetime(dataframe["month"]).dt.date
        dataframe["visits"] = pd.to_numeric(dataframe["visits"], errors="coerce").astype(
            "Int64"
        )
        dataframe["load_dt"] = pd.to_datetime(dataframe["load_dt"])

    return MonthlyByUrlResult(
        month=month_start,
        dataframe=dataframe,
        request_count=len(plans),
    )


__all__ = [
    "BY_URL_METRIC_ID",
    "BUSINESS_KEY_COLUMNS",
    "DEFAULT_URL_LIMIT",
    "OUTPUT_COLUMNS",
    "MonthlyByUrlRequestSpec",
    "build_monthly_by_url_requests",
    "build_url_payload",
    "extract_monthly_by_url",
]
