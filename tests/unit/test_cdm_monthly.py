"""Unit tests for Monthly CDM builders (synthetic engines; SQLite isolation)."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from sqlalchemy import create_engine, text

from analytics_reporting_platform.cdm import (
    CdmPeriodConfig,
    build_device_section_mix,
    build_dim_month,
    build_explore_buying_guides_kpi,
    build_explore_buying_guides_trend,
    build_geo_traffic_by_section,
    build_geo_traffic_kpi,
    build_geo_traffic_trend,
    build_organic_search_engine_mix,
    build_organic_search_engine_trend,
    build_organic_search_kpi,
    build_overall_traffic_trend,
    build_sections_mix,
    build_top_pages,
    build_traffic_kpi,
    build_traffic_source_mix,
    month_type_label,
    persist_dim_month,
    persist_traffic_kpi,
)
import analytics_reporting_platform.cdm.repository as cdm_repo

FIXED = datetime(2026, 8, 8, 12, 0, 0)
PERIOD = CdmPeriodConfig(
    report_start=date(2026, 1, 1),
    report_end=date(2026, 7, 1),
    trend_start=date(2025, 7, 1),
)


def _calc_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE calc_monthly_kpi (
                    month DATE, segment_name TEXT, metric_name TEXT, metric_value FLOAT,
                    mom_pct_change FLOAT, yoy_pct_change FLOAT,
                    mom_pp_change FLOAT, yoy_pp_change FLOAT,
                    change_type TEXT, value_format TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE calc_monthly_channel_mix (
                    month DATE, channel_name TEXT, channel_visits FLOAT,
                    channel_share FLOAT, mom_pp_change FLOAT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE calc_monthly_search_engines (
                    month DATE, search_engine TEXT, search_engine_visits FLOAT,
                    search_engine_share FLOAT, mom_pp_change FLOAT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE calc_monthly_section_share (
                    month DATE, segment_name TEXT, section_name TEXT,
                    section_visits FLOAT, section_share FLOAT, mom_pp_change FLOAT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE calc_explore_buying_guides_kpi (
                    month DATE, section_name TEXT, kpi_name TEXT, kpi_value FLOAT,
                    mom_pct_change FLOAT, yoy_pct_change FLOAT, display_order INT
                )
                """
            )
        )
    return eng


def _raw_engine():
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
        conn.execute(
            text(
                """
                CREATE TABLE raw_aa_monthly_by_url (
                    month DATE, page_url TEXT, visits FLOAT
                )
                """
            )
        )
    return eng


def _cdm_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE cdm_traffic_kpi (
                    month DATE, kpi_name TEXT, kpi_value FLOAT,
                    mom_pct_change FLOAT, yoy_pct_change FLOAT,
                    mom_pp_change FLOAT, yoy_pp_change FLOAT,
                    change_type TEXT, value_format TEXT,
                    display_order INT, cdm_dt TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE cdm_dim_month (
                    month DATE, year INT, month_num INT, year_month TEXT,
                    month_sort INT, is_loaded_month INT, cdm_dt TIMESTAMP
                )
                """
            )
        )
    return eng


def test_month_type_labels() -> None:
    end = date(2026, 7, 1)
    assert month_type_label(date(2026, 7, 1), end) == "last_month"
    assert month_type_label(date(2026, 6, 1), end) == "previous_month"
    assert month_type_label(date(2026, 1, 1), end) == "other"


def test_traffic_kpi_and_persist_rollback() -> None:
    calc = _calc_engine()
    cdm = _cdm_engine()
    rows = [
        ("2026-07-01", "total", "visits", 100.0, 0.1, 0.2, None, None, "pct", "number"),
        ("2026-07-01", "organic", "visits", 40.0, 0.1, 0.2, None, None, "pct", "number"),
        ("2026-07-01", "total", "avg_time", 50.0, 0.0, 0.0, None, None, "pct", "duration_seconds"),
        ("2026-07-01", "total", "bounce_rate", 0.3, None, None, 0.01, 0.02, "pp", "percent"),
    ]
    pd.DataFrame(
        rows,
        columns=[
            "month",
            "segment_name",
            "metric_name",
            "metric_value",
            "mom_pct_change",
            "yoy_pct_change",
            "mom_pp_change",
            "yoy_pp_change",
            "change_type",
            "value_format",
        ],
    ).to_sql("calc_monthly_kpi", calc, if_exists="append", index=False)
    built = build_traffic_kpi(calc, "2026-07-01", cdm_dt=FIXED)
    assert built.row_count == 4
    assert list(built.dataframe["kpi_name"]) == [
        "total_visits",
        "organic_visits",
        "average_time_on_site",
        "bounce_rate",
    ]
    res = persist_traffic_kpi(cdm, built.dataframe, "2026-07-01")
    assert res.status == "replaced"
    skip = persist_traffic_kpi(cdm, built.dataframe, "2026-07-01", mode="skip")
    assert skip.status == "skipped"
    orig = cdm_repo._insert

    def boom(conn, table, frame):  # type: ignore[no-untyped-def]
        raise RuntimeError("cdm boom")

    cdm_repo._insert = boom
    try:
        try:
            persist_traffic_kpi(cdm, built.dataframe, "2026-07-01")
            raise AssertionError("expected failure")
        except RuntimeError:
            pass
    finally:
        cdm_repo._insert = orig
    with cdm.connect() as conn:
        n = conn.execute(text("SELECT count(*) FROM cdm_traffic_kpi")).scalar()
    assert int(n) == 4
    calc.dispose()
    cdm.dispose()


def test_overall_trend_and_engine_other_clip() -> None:
    raw = _raw_engine()
    months = [date(2025, 7, 1), date(2026, 7, 1)]
    rows = []
    for m in months:
        for seg, val in (
            ("organic", 100.0),
            ("own_group", 10.0),
            ("paid_group", 5.0),
            ("other_group", 2.0),
            ("total", 200.0),
            ("google", 60.0),
            ("yandex", 50.0),
            ("ai", 7.0),
        ):
            rows.append(
                {"month": m, "segment_name": seg, "metric_name": "visits", "metric_value": val}
            )
    # over-count google+yandex to force clip
    rows.append(
        {
            "month": date(2026, 7, 1),
            "segment_name": "google",
            "metric_name": "visits",
            "metric_value": 80.0,
        }
    )
    pd.DataFrame(rows).to_sql("raw_aa_monthly_metrics", raw, if_exists="append", index=False)
    trend = build_overall_traffic_trend(raw, PERIOD, cdm_dt=FIXED)
    assert trend.row_count == 2
    eng = build_organic_search_engine_trend(raw, PERIOD, cdm_dt=FIXED)
    july = eng.dataframe[eng.dataframe["month"] == date(2026, 7, 1)].iloc[0]
    # organic=100, google=60+80=140, yandex=50 -> other clipped to 0
    assert float(july["other_search_engines_visits"]) == 0.0
    geo = build_geo_traffic_trend(raw, PERIOD, cdm_dt=FIXED)
    assert geo.row_count == 2
    raw.dispose()


def test_mix_month_type_and_sections() -> None:
    calc = _calc_engine()
    channels = ["organic", "direct", "app", "email", "sns", "push", "other_channel"]
    pd.DataFrame(
        [
            {
                "month": date(2026, 7, 1),
                "channel_name": c,
                "channel_visits": 10.0,
                "channel_share": 0.1,
                "mom_pp_change": 0.0,
            }
            for c in channels
        ]
    ).to_sql("calc_monthly_channel_mix", calc, if_exists="append", index=False)
    mix = build_traffic_source_mix(
        calc, "2026-07-01", report_end="2026-07-01", cdm_dt=FIXED
    )
    assert set(mix.dataframe["month_type"]) == {"last_month"}
    sections = ["support", "mx", "explore", "he", "da", "other"]
    rows = []
    for seg in ("total", "ai", "samsung_devices", "apple_devices"):
        for s in sections:
            rows.append(
                {
                    "month": date(2026, 7, 1),
                    "segment_name": seg,
                    "section_name": s,
                    "section_visits": 1.0,
                    "section_share": 1 / 6,
                    "mom_pp_change": 0.0,
                }
            )
    pd.DataFrame(rows).to_sql(
        "calc_monthly_section_share", calc, if_exists="append", index=False
    )
    assert build_sections_mix(calc, "2026-07-01", report_end="2026-07-01", cdm_dt=FIXED).row_count == 6
    assert build_geo_traffic_by_section(calc, "2026-07-01", cdm_dt=FIXED).row_count == 6
    assert build_device_section_mix(calc, "2026-07-01", cdm_dt=FIXED).row_count == 12
    calc.dispose()


def test_organic_kpi_explore_top_dim() -> None:
    calc = _calc_engine()
    raw = _raw_engine()
    cdm = _cdm_engine()
    pd.DataFrame(
        [
            {
                "month": date(2026, 7, 1),
                "segment_name": s,
                "metric_name": "visits",
                "metric_value": 1.0,
                "mom_pct_change": 0.0,
                "yoy_pct_change": 0.0,
                "mom_pp_change": None,
                "yoy_pp_change": None,
                "change_type": "pct",
                "value_format": "number",
            }
            for s in ("organic", "google", "yandex", "ai")
        ]
    ).to_sql("calc_monthly_kpi", calc, if_exists="append", index=False)
    assert build_organic_search_kpi(calc, "2026-07-01", cdm_dt=FIXED).row_count == 3
    assert build_geo_traffic_kpi(calc, "2026-07-01", cdm_dt=FIXED).row_count == 1
    pd.DataFrame(
        [
            {
                "month": date(2026, 7, 1),
                "search_engine": e,
                "search_engine_visits": 1.0,
                "search_engine_share": 0.25,
                "mom_pp_change": 0.0,
            }
            for e in ("organic_total", "yandex", "google", "other_search_engines")
        ]
    ).to_sql("calc_monthly_search_engines", calc, if_exists="append", index=False)
    assert (
        build_organic_search_engine_mix(
            calc, "2026-07-01", report_end="2026-07-01", cdm_dt=FIXED
        ).row_count
        == 4
    )
    pd.DataFrame(
        [
            {
                "month": date(2026, 7, 1),
                "section_name": sec,
                "kpi_name": kpi,
                "kpi_value": 1.0,
                "mom_pct_change": 0.0,
                "yoy_pct_change": 0.0,
                "display_order": 1 if kpi == "total_visits" else 2,
            }
            for sec in ("explore", "buying_guides")
            for kpi in ("total_visits", "organic_visits")
        ]
    ).to_sql("calc_explore_buying_guides_kpi", calc, if_exists="append", index=False)
    assert build_explore_buying_guides_kpi(calc, "2026-07-01", cdm_dt=FIXED).row_count == 4
    pd.DataFrame(
        [
            {
                "month": m,
                "segment_name": "organic",
                "section_name": sec,
                "metric_value": 3.0,
            }
            for m in (date(2025, 7, 1), date(2026, 7, 1))
            for sec in ("explore", "buying_guides")
        ]
    ).to_sql("raw_aa_monthly_by_section", raw, if_exists="append", index=False)
    tr = build_explore_buying_guides_trend(raw, PERIOD, cdm_dt=FIXED)
    assert tr.row_count == 2
    assert float(tr.dataframe.iloc[0]["total_organic_visits"]) == 6.0
    pd.DataFrame(
        [
            {"month": date(2026, 7, 1), "page_url": "/a", "visits": 10.0},
            {"month": date(2026, 7, 1), "page_url": "/b", "visits": 5.0},
        ]
    ).to_sql("raw_aa_monthly_by_url", raw, if_exists="append", index=False)
    top = build_top_pages(raw, "2026-07-01", cdm_dt=FIXED)
    assert top.row_count == 2
    assert int(top.dataframe.iloc[0]["page_rank"]) == 1
    dim = build_dim_month(PERIOD, cdm_dt=FIXED)
    assert dim.row_count == 13  # Jul2025..Jul2026
    persist_dim_month(cdm, dim.dataframe, PERIOD)
    with cdm.connect() as conn:
        n = conn.execute(text("SELECT count(*) FROM cdm_dim_month")).scalar()
    assert int(n) == 13
    calc.dispose()
    raw.dispose()
    cdm.dispose()


if __name__ == "__main__":
    test_month_type_labels()
    test_traffic_kpi_and_persist_rollback()
    test_overall_trend_and_engine_other_clip()
    test_mix_month_type_and_sections()
    test_organic_kpi_explore_top_dim()
    print("OK", 5)
