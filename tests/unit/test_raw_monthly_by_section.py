"""Unit tests for Monthly RAW by-section extractor (mocked Reports only)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

from analytics_reporting_platform.config.client_config import AdobeClientConfig
from analytics_reporting_platform.raw.models import RawExtractionError, adobe_month_date_range
from analytics_reporting_platform.raw.monthly_by_section import (
    BUSINESS_KEY_COLUMNS,
    BY_SECTION_METRIC_ID,
    OUTPUT_COLUMNS,
    build_monthly_by_section_requests,
    extract_monthly_by_section,
)


SYNTHETIC_RSID = "synthetic-rsid-not-real"
SYNTHETIC_TRAFFIC = "synthetic-traffic-segment-not-real"
SYNTHETIC_SECTION_A = "synthetic-section-a-not-real"
SYNTHETIC_SECTION_B = "synthetic-section-b-not-real"


def _config() -> AdobeClientConfig:
    return AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        section_traffic_segments={
            "total": None,
            "traffic_a": SYNTHETIC_TRAFFIC,
        },
        sections={
            "product_1": SYNTHETIC_SECTION_A,
            "content_section_1": SYNTHETIC_SECTION_B,
        },
    )


class FakeReports:
    def __init__(self, responses: list[Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._responses = list(responses or [])

    def post_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(dict(payload))
        if not self._responses:
            return {"summaryData": {"totals": [1.0]}}
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_month_range_and_leap_year() -> None:
    plans = build_monthly_by_section_requests(_config(), "2026-07-01")
    dr = plans[0].payload["globalFilters"][0]["dateRange"]
    assert dr == "2026-07-01T00:00:00.000/2026-07-31T23:59:59.999"
    assert adobe_month_date_range(date(2024, 2, 1))[1].startswith("2024-02-29T")


def test_request_count_is_cartesian_product() -> None:
    plans = build_monthly_by_section_requests(_config(), date(2026, 7, 1))
    # 2 traffic × 2 sections
    assert len(plans) == 4
    assert [(p.segment_name, p.section_name) for p in plans] == [
        ("total", "product_1"),
        ("total", "content_section_1"),
        ("traffic_a", "product_1"),
        ("traffic_a", "content_section_1"),
    ]


def test_total_omits_traffic_segment_filter() -> None:
    plans = build_monthly_by_section_requests(_config(), "2026-07-01")
    total_plan = plans[0]
    assert total_plan.segment_id is None
    filters = total_plan.payload["globalFilters"]
    assert len(filters) == 2
    assert filters[0]["type"] == "dateRange"
    assert filters[1]["type"] == "segment"
    assert filters[1]["segmentId"] == SYNTHETIC_SECTION_A
    assert total_plan.payload["metricContainer"]["metrics"][0]["id"] == BY_SECTION_METRIC_ID


def test_traffic_segment_appended_after_section() -> None:
    plans = build_monthly_by_section_requests(_config(), "2026-07-01")
    traffic_plan = plans[2]
    filters = traffic_plan.payload["globalFilters"]
    assert len(filters) == 3
    assert filters[1]["segmentId"] == SYNTHETIC_SECTION_A
    assert filters[2]["segmentId"] == SYNTHETIC_TRAFFIC


def test_arbitrary_keys_supported() -> None:
    cfg = AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        section_traffic_segments={"custom_traffic": SYNTHETIC_TRAFFIC},
        sections={"custom_section": SYNTHETIC_SECTION_A},
    )
    plans = build_monthly_by_section_requests(cfg, "2026-07-01")
    assert len(plans) == 1
    assert plans[0].segment_name == "custom_traffic"
    assert plans[0].section_name == "custom_section"


def test_null_section_id_rejected() -> None:
    cfg = AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        section_traffic_segments={"total": None},
        sections={"broken": None},
    )
    with pytest.raises(RawExtractionError, match="non-null section"):
        build_monthly_by_section_requests(cfg, "2026-07-01")


def test_extract_schema_and_business_key() -> None:
    responses = [{"summaryData": {"totals": [float(i)]}} for i in range(4)]
    reports = FakeReports(responses)
    result = extract_monthly_by_section(
        reports, _config(), "2026-07-01", load_dt=datetime(2026, 8, 1)
    )
    assert result.request_count == 4
    assert result.row_count == 4
    df = result.dataframe
    assert list(df.columns) == list(OUTPUT_COLUMNS)
    assert int(df.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).sum()) == 0
    assert set(df["segment_name"]) == {"total", "traffic_a"}
    assert set(df["section_name"]) == {"product_1", "content_section_1"}


def test_empty_totals_become_null() -> None:
    cfg = AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        section_traffic_segments={"total": None},
        sections={"product_1": SYNTHETIC_SECTION_A},
    )
    reports = FakeReports([{"summaryData": {"totals": []}}])
    result = extract_monthly_by_section(reports, cfg, date(2026, 7, 1), load_dt=datetime(2026, 1, 1))
    assert result.row_count == 1
    assert result.dataframe["metric_value"].isna().all()


def test_malformed_totals_raise() -> None:
    cfg = AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        section_traffic_segments={"total": None},
        sections={"product_1": SYNTHETIC_SECTION_A},
    )
    reports = FakeReports([{"summaryData": {"totals": "bad"}}])
    with pytest.raises(RawExtractionError, match="totals"):
        extract_monthly_by_section(reports, cfg, "2026-07-01")


def test_repr_masks_ids() -> None:
    plans = build_monthly_by_section_requests(_config(), "2026-07-01")
    rendered = repr(plans[2])
    assert SYNTHETIC_TRAFFIC not in rendered
    assert SYNTHETIC_SECTION_A not in rendered
    assert "segment_id=***" in rendered
    assert "section_id=***" in rendered


def test_source_has_no_private_taxonomy_or_ids() -> None:
    import analytics_reporting_platform.raw.monthly_by_section as mod

    src = Path(mod.__file__).read_text(encoding="utf-8")
    for token in (
        "organic",
        "buying_guides",
        "samsung_devices",
        "apple_devices",
        SYNTHETIC_SECTION_A,
        SYNTHETIC_TRAFFIC,
    ):
        assert token not in src
    assert BY_SECTION_METRIC_ID in src
