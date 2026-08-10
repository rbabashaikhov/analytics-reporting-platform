"""Unit tests for Monthly RAW by-URL extractor (mocked Reports; synthetic pages only)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

from analytics_reporting_platform.config.client_config import AdobeClientConfig, ClientConfigError
from analytics_reporting_platform.raw.models import RawExtractionError, adobe_month_date_range
from analytics_reporting_platform.raw.monthly_by_url import (
    BUSINESS_KEY_COLUMNS,
    BY_URL_METRIC_ID,
    DEFAULT_URL_LIMIT,
    OUTPUT_COLUMNS,
    build_monthly_by_url_requests,
    extract_monthly_by_url,
)


SYNTHETIC_RSID = "synthetic-rsid-not-real"
SYNTHETIC_DIMENSION = "variables/evar999"
SYNTHETIC_PAGE_1 = "/demo/page-1"
SYNTHETIC_PAGE_2 = "/demo/page-2"


def _config() -> AdobeClientConfig:
    return AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        page_url_dimension=SYNTHETIC_DIMENSION,
    )


class FakeReports:
    def __init__(self, responses: list[Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._responses = list(responses or [])

    def post_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(dict(payload))
        if not self._responses:
            return {"rows": []}
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_month_range_and_leap_year() -> None:
    plans = build_monthly_by_url_requests(_config(), "2026-07-01")
    assert len(plans) == 1
    assert plans[0].payload["globalFilters"][0]["dateRange"] == (
        "2026-07-01T00:00:00.000/2026-07-31T23:59:59.999"
    )
    assert adobe_month_date_range(date(2024, 2, 1))[1].startswith("2024-02-29T")


def test_request_metric_dimension_settings() -> None:
    plans = build_monthly_by_url_requests(_config(), date(2026, 7, 1))
    payload = plans[0].payload
    assert payload["dimension"] == SYNTHETIC_DIMENSION
    assert payload["metricContainer"]["metrics"][0]["id"] == BY_URL_METRIC_ID
    assert payload["settings"]["limit"] == DEFAULT_URL_LIMIT
    assert payload["settings"]["page"] == 0
    assert payload["settings"]["nonesBehavior"] == "return-nones"
    assert payload["statistics"]["functions"] == ["col-max", "col-min"]
    assert "segmentId" not in str(payload.get("globalFilters"))


def test_single_request_no_pagination() -> None:
    plans = build_monthly_by_url_requests(_config(), "2026-07-01", limit=30)
    assert len(plans) == 1
    assert plans[0].limit == 30


def test_missing_dimension_fails() -> None:
    cfg = AdobeClientConfig(report_suite_id=SYNTHETIC_RSID)
    with pytest.raises(ClientConfigError, match="page_url_dimension"):
        build_monthly_by_url_requests(cfg, "2026-07-01")


def test_extract_normalizes_schema_and_keys() -> None:
    reports = FakeReports(
        [
            {
                "rows": [
                    {"value": SYNTHETIC_PAGE_1, "data": [100]},
                    {"value": SYNTHETIC_PAGE_2, "data": [50]},
                ]
            }
        ]
    )
    result = extract_monthly_by_url(
        reports, _config(), "2026-07-01", load_dt=datetime(2026, 8, 1)
    )
    assert result.request_count == 1
    assert result.row_count == 2
    df = result.dataframe
    assert list(df.columns) == list(OUTPUT_COLUMNS)
    assert int(df.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).sum()) == 0
    assert set(df["page_url"]) == {SYNTHETIC_PAGE_1, SYNTHETIC_PAGE_2}


def test_item_id_fallback_when_value_missing() -> None:
    reports = FakeReports([{"rows": [{"itemId": "demo-item-1", "data": [3]}]}])
    result = extract_monthly_by_url(
        reports, _config(), date(2026, 7, 1), load_dt=datetime(2026, 1, 1)
    )
    assert result.row_count == 1
    assert result.dataframe.iloc[0]["page_url"] == "demo-item-1"
    assert int(result.dataframe.iloc[0]["visits"]) == 3


def test_empty_rows() -> None:
    reports = FakeReports([{"rows": []}])
    result = extract_monthly_by_url(
        reports, _config(), "2026-07-01", load_dt=datetime(2026, 1, 1)
    )
    assert result.row_count == 0
    assert list(result.dataframe.columns) == list(OUTPUT_COLUMNS)


def test_malformed_rows_raise() -> None:
    reports = FakeReports([{"rows": "bad"}])
    with pytest.raises(RawExtractionError, match="rows"):
        extract_monthly_by_url(reports, _config(), "2026-07-01")


def test_repr_masks_dimension() -> None:
    plans = build_monthly_by_url_requests(_config(), "2026-07-01")
    assert SYNTHETIC_DIMENSION not in repr(plans[0])
    assert "page_url_dimension=***" in repr(plans[0])


def test_source_has_no_client_dimension_literal() -> None:
    import analytics_reporting_platform.raw.monthly_by_url as mod

    src = Path(mod.__file__).read_text(encoding="utf-8")
    assert "evar92" not in src
    assert BY_URL_METRIC_ID in src
    assert "Full-private" in src or "full-private" in src.lower() or "FULL PRIVATE" in src or "Full-private" in Path(mod.__file__).read_text()
