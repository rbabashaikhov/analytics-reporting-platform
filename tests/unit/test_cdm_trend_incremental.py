"""Regression: incremental CDM trend windows must expand across report months.

Defect: ``persist_cdm_window(mode='skip')`` treated any rows in the expanding
window as complete, so after the first reporting month (e.g. 2026-01) subsequent
incremental months skipped trend rebuilds and coverage froze.
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from sqlalchemy import create_engine, text

from analytics_reporting_platform.cdm import (
    build_explore_buying_guides_trend,
    build_geo_traffic_trend,
    build_organic_search_engine_trend,
    build_overall_traffic_trend,
    default_period_config,
    persist_explore_buying_guides_trend,
    persist_geo_traffic_trend,
    persist_organic_search_engine_trend,
    persist_overall_traffic_trend,
)
from analytics_reporting_platform.cdm.repository import persist_cdm_window

FIXED = datetime(2026, 8, 10, 12, 0, 0)
TREND_START = date(2025, 1, 1)
REPORT_MONTHS = (date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1))


def _seed_raw() -> object:
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE raw_aa_monthly_metrics (
                    month DATE, segment_name TEXT, metric_name TEXT, metric_value FLOAT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE raw_aa_monthly_by_section (
                    month DATE, segment_name TEXT, section_name TEXT, metric_value FLOAT
                )
                """
            )
        )
    months = pd.date_range(TREND_START, date(2026, 3, 1), freq="MS")
    metric_rows = []
    section_rows = []
    for ts in months:
        m = ts.date()
        for seg, val in (
            ("organic", 100.0),
            ("own_group", 10.0),
            ("paid_group", 5.0),
            ("other_group", 2.0),
            ("total", 200.0),
            ("google", 40.0),
            ("yandex", 30.0),
            ("ai", 7.0),
        ):
            metric_rows.append(
                {
                    "month": m,
                    "segment_name": seg,
                    "metric_name": "visits",
                    "metric_value": val,
                }
            )
        for section in ("explore", "buying_guides"):
            section_rows.append(
                {
                    "month": m,
                    "segment_name": "organic",
                    "section_name": section,
                    "metric_value": 11.0,
                }
            )
    pd.DataFrame(metric_rows).to_sql(
        "raw_aa_monthly_metrics", eng, if_exists="append", index=False
    )
    pd.DataFrame(section_rows).to_sql(
        "raw_aa_monthly_by_section", eng, if_exists="append", index=False
    )
    return eng


def _cdm_trend_engine() -> object:
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE cdm_overall_traffic_trend (
                    month DATE, organic_visits FLOAT, own_group_visits FLOAT,
                    paid_group_visits FLOAT, other_group_visits FLOAT,
                    total_visits FLOAT, cdm_dt TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE cdm_geo_traffic_trend (
                    month DATE, geo_visits FLOAT, cdm_dt TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE cdm_organic_search_engine_trend (
                    month DATE, google_visits FLOAT, yandex_visits FLOAT,
                    other_search_engines_visits FLOAT, organic_total_visits FLOAT,
                    cdm_dt TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE cdm_explore_buying_guides_trend (
                    month DATE, explore_organic_visits FLOAT,
                    buying_guides_organic_visits FLOAT, total_organic_visits FLOAT,
                    cdm_dt TIMESTAMP
                )
                """
            )
        )
    return eng


def _max_month(engine, table: str) -> date:
    with engine.connect() as conn:
        value = conn.execute(text(f"SELECT MAX(month) FROM {table}")).scalar()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _month_present(engine, table: str, month: date) -> bool:
    with engine.connect() as conn:
        n = conn.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE month = :m"), {"m": month}
        ).scalar()
    return int(n or 0) > 0


def test_persist_cdm_window_skip_only_when_window_end_present() -> None:
    """Skip must not fire merely because an earlier subset of the window exists."""
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text("CREATE TABLE cdm_dim_month (month DATE, year INT, cdm_dt TIMESTAMP)")
        )
    # First write: through 2026-01
    frame1 = pd.DataFrame(
        {
            "month": [date(2025, 12, 1), date(2026, 1, 1)],
            "year": [2025, 2026],
            "cdm_dt": [FIXED, FIXED],
        }
    )
    first = persist_cdm_window(
        eng,
        frame1,
        mart="dim_month",
        table="cdm_dim_month",
        window_start=date(2025, 12, 1),
        window_end=date(2026, 1, 1),
        output_columns=("month", "year", "cdm_dt"),
        business_key_columns=("month",),
        required_non_null=("month", "year", "cdm_dt"),
        mode="skip",
    )
    assert first.status == "replaced"

    # Expanding window through 2026-02 must NOT skip (old bug skipped here).
    frame2 = pd.DataFrame(
        {
            "month": [date(2025, 12, 1), date(2026, 1, 1), date(2026, 2, 1)],
            "year": [2025, 2026, 2026],
            "cdm_dt": [FIXED, FIXED, FIXED],
        }
    )
    second = persist_cdm_window(
        eng,
        frame2,
        mart="dim_month",
        table="cdm_dim_month",
        window_start=date(2025, 12, 1),
        window_end=date(2026, 2, 1),
        output_columns=("month", "year", "cdm_dt"),
        business_key_columns=("month",),
        required_non_null=("month", "year", "cdm_dt"),
        mode="skip",
    )
    assert second.status == "replaced"
    assert _max_month(eng, "cdm_dim_month") == date(2026, 2, 1)

    # Re-running the same window_end must skip.
    third = persist_cdm_window(
        eng,
        frame2,
        mart="dim_month",
        table="cdm_dim_month",
        window_start=date(2025, 12, 1),
        window_end=date(2026, 2, 1),
        output_columns=("month", "year", "cdm_dt"),
        business_key_columns=("month",),
        required_non_null=("month", "year", "cdm_dt"),
        mode="skip",
    )
    assert third.status == "skipped"
    eng.dispose()


def test_incremental_trend_marts_expand_across_report_months() -> None:
    """Sequential incremental persists for 2026-01→02→03 must extend all 4 trends."""
    raw = _seed_raw()
    cdm = _cdm_trend_engine()

    builders = (
        (
            "cdm_overall_traffic_trend",
            build_overall_traffic_trend,
            persist_overall_traffic_trend,
        ),
        (
            "cdm_geo_traffic_trend",
            build_geo_traffic_trend,
            persist_geo_traffic_trend,
        ),
        (
            "cdm_organic_search_engine_trend",
            build_organic_search_engine_trend,
            persist_organic_search_engine_trend,
        ),
        (
            "cdm_explore_buying_guides_trend",
            build_explore_buying_guides_trend,
            persist_explore_buying_guides_trend,
        ),
    )

    for report_month in REPORT_MONTHS:
        period = default_period_config(report_month)
        assert period.trend_start == TREND_START
        assert period.report_end == report_month
        for table, build_fn, persist_fn in builders:
            built = build_fn(raw, period, cdm_dt=FIXED)
            result = persist_fn(cdm, built.dataframe, period, mode="skip")
            assert result.status == "replaced", (
                f"{table} should expand for {report_month.isoformat()}, got {result.status}"
            )
            assert _month_present(cdm, table, report_month)
            assert _max_month(cdm, table) == report_month

    # Idempotent re-run of last report month must skip.
    last = REPORT_MONTHS[-1]
    period = default_period_config(last)
    for table, build_fn, persist_fn in builders:
        built = build_fn(raw, period, cdm_dt=FIXED)
        result = persist_fn(cdm, built.dataframe, period, mode="skip")
        assert result.status == "skipped"
        assert _max_month(cdm, table) == last

    raw.dispose()
    cdm.dispose()
