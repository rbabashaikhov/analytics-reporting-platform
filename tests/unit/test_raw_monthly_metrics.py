"""Unit tests for Monthly RAW metrics extractor (mocked Reports only)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

from analytics_reporting_platform.config.client_config import AdobeClientConfig
from analytics_reporting_platform.raw import (
    BUSINESS_KEY_COLUMNS,
    MONTHLY_METRICS_METRIC_MAP,
    OUTPUT_COLUMNS,
    RawExtractionError,
    adobe_month_date_range,
    build_monthly_metrics_requests,
    extract_monthly_metrics,
    parse_month_start,
)


SYNTHETIC_RSID = "synthetic-rsid-not-real"
SYNTHETIC_SEG_A = "synthetic-segment-a-not-real"
SYNTHETIC_SEG_B = "synthetic-segment-b-not-real"


def _config() -> AdobeClientConfig:
    return AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        segments={
            "total": None,
            "segment_a": SYNTHETIC_SEG_A,
            "segment_b": SYNTHETIC_SEG_B,
        },
    )


class FakeReports:
    def __init__(self, responses: list[Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._responses = list(responses or [])

    def post_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(dict(payload))
        if not self._responses:
            return {"summaryData": {"totals": [1.0, 2.0, 3.0]}}
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_valid_month_date_range() -> None:
    assert parse_month_start("2026-07-01") == date(2026, 7, 1)
    assert adobe_month_date_range(date(2026, 7, 1)) == (
        "2026-07-01T00:00:00.000",
        "2026-07-31T23:59:59.999",
    )


def test_february_leap_year_month_end() -> None:
    assert adobe_month_date_range(date(2024, 2, 1)) == (
        "2024-02-01T00:00:00.000",
        "2024-02-29T23:59:59.999",
    )
    assert adobe_month_date_range(date(2025, 2, 1)) == (
        "2025-02-01T00:00:00.000",
        "2025-02-28T23:59:59.999",
    )


def test_non_first_day_rejected() -> None:
    with pytest.raises(RawExtractionError, match="first day"):
        parse_month_start(date(2026, 7, 15))
    with pytest.raises(RawExtractionError):
        parse_month_start("2026-07-15")


def test_request_plan_shape_and_total_semantics() -> None:
    plans = build_monthly_metrics_requests(_config(), "2026-07-01")
    assert len(plans) == 3
    assert plans[0].segment_name == "total"
    assert plans[0].segment_id is None
    assert plans[0].metric_names == ("visits", "avg_time", "bounce_rate")
    assert "segmentId" not in str(plans[0].payload.get("globalFilters"))
    # total payload has only dateRange filter
    assert len(plans[0].payload["globalFilters"]) == 1
    assert plans[0].payload["globalFilters"][0]["type"] == "dateRange"
    assert plans[0].payload["globalFilters"][0]["dateRange"].startswith("2026-07-01T")

    assert plans[1].segment_name == "segment_a"
    assert plans[1].segment_id == SYNTHETIC_SEG_A
    assert plans[1].metric_names == ("visits",)
    assert plans[1].payload["globalFilters"][1]["segmentId"] == SYNTHETIC_SEG_A

    # Adobe metric ids in mart map (public Adobe constants)
    metric_ids = [m["id"] for m in plans[0].payload["metricContainer"]["metrics"]]
    assert metric_ids == [
        MONTHLY_METRICS_METRIC_MAP["visits"],
        MONTHLY_METRICS_METRIC_MAP["avg_time"],
        MONTHLY_METRICS_METRIC_MAP["bounce_rate"],
    ]


def test_request_spec_repr_masks_segment_id() -> None:
    plans = build_monthly_metrics_requests(_config(), date(2026, 7, 1))
    rendered = repr(plans[1])
    assert SYNTHETIC_SEG_A not in rendered
    assert "segment_id=***" in rendered


def test_extract_normalizes_schema_and_keys() -> None:
    responses = [
        {"summaryData": {"totals": [10.0, 20.5, 0.3]}},
        {"summaryData": {"totals": [4.0]}},
        {"summaryData": {"totals": [5.0]}},
    ]
    reports = FakeReports(responses)
    fixed = datetime(2026, 8, 1, 12, 0, 0)
    result = extract_monthly_metrics(
        reports, _config(), "2026-07-01", load_dt=fixed
    )
    assert result.request_count == 3
    assert result.row_count == 5  # 3 + 1 + 1
    df = result.dataframe
    assert list(df.columns) == list(OUTPUT_COLUMNS)
    assert df.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).sum() == 0
    assert set(df["segment_name"]) == {"total", "segment_a", "segment_b"}
    assert set(df.loc[df["segment_name"] == "total", "metric_name"]) == {
        "visits",
        "avg_time",
        "bounce_rate",
    }
    assert len(reports.calls) == 3


def test_empty_totals_become_nulls() -> None:
    reports = FakeReports([{"summaryData": {"totals": []}}])
    cfg = AdobeClientConfig(report_suite_id=SYNTHETIC_RSID, segments={"total": None})
    result = extract_monthly_metrics(reports, cfg, date(2026, 7, 1), load_dt=datetime(2026, 1, 1))
    assert result.row_count == 3
    assert result.dataframe["metric_value"].isna().all()


def test_malformed_totals_raise() -> None:
    reports = FakeReports([{"summaryData": {"totals": "bad"}}])
    cfg = AdobeClientConfig(report_suite_id=SYNTHETIC_RSID, segments={"total": None})
    with pytest.raises(RawExtractionError, match="totals"):
        extract_monthly_metrics(reports, cfg, "2026-07-01")


def test_missing_total_segment_fails() -> None:
    cfg = AdobeClientConfig(
        report_suite_id=SYNTHETIC_RSID,
        segments={"segment_a": SYNTHETIC_SEG_A},
    )
    with pytest.raises(RawExtractionError, match="total"):
        build_monthly_metrics_requests(cfg, "2026-07-01")


def test_source_has_no_real_looking_private_ids() -> None:
    import analytics_reporting_platform.raw.monthly_metrics as mod

    src = Path(mod.__file__).read_text(encoding="utf-8")
    assert SYNTHETIC_SEG_A not in src
    assert "metrics/visits" in src  # public Adobe metric constant expected
    # no hardcoded historical channel inventory beyond total special-case
    for token in ("organic", "yandex", "buying_guides", "own_group"):
        assert token not in src
