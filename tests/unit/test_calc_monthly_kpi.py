"""Unit tests for calc_monthly_kpi (synthetic RAW; SQLite isolation)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from analytics_reporting_platform.calc import (
    BUSINESS_KEY_COLUMNS,
    OUTPUT_COLUMNS,
    CalcError,
    build_monthly_kpi,
    metric_change_type,
    persist_monthly_kpi,
    safe_divide,
)
import analytics_reporting_platform.calc.repository as calc_repo
from analytics_reporting_platform.calc.monthly_kpi import _transform_monthly_kpi


FIXED = datetime(2026, 8, 1, 12, 0, 0)


def _raw_engine_with(rows: list[dict[str, Any]]):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE raw_aa_monthly_metrics (
                    month DATE NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value FLOAT,
                    segment_name TEXT NOT NULL,
                    segment_id TEXT,
                    load_dt TIMESTAMP
                )
                """
            )
        )
    pd.DataFrame(rows).to_sql(
        "raw_aa_monthly_metrics", engine, if_exists="append", index=False
    )
    return engine


def _calc_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE calc_monthly_kpi (
                    month DATE NOT NULL,
                    segment_name TEXT NOT NULL,
                    segment_id TEXT,
                    metric_name TEXT NOT NULL,
                    metric_value FLOAT,
                    prev_month_value FLOAT,
                    prev_year_value FLOAT,
                    mom_abs_change FLOAT,
                    mom_pct_change FLOAT,
                    yoy_abs_change FLOAT,
                    yoy_pct_change FLOAT,
                    mom_pp_change FLOAT,
                    yoy_pp_change FLOAT,
                    change_type TEXT,
                    value_format TEXT,
                    calc_dt TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE calc_monthly_channel_mix (
                    month DATE NOT NULL,
                    channel_name TEXT NOT NULL,
                    channel_visits FLOAT,
                    calc_dt TIMESTAMP NOT NULL
                )
                """
            )
        )
    return engine


def _seed_three_periods() -> list[dict[str, Any]]:
    rows = []
    for month, visits, bounce in (
        (date(2025, 7, 1), 100.0, 0.40),
        (date(2026, 6, 1), 200.0, 0.50),
        (date(2026, 7, 1), 250.0, 0.55),
    ):
        rows.append(
            {
                "month": month,
                "segment_name": "total",
                "segment_id": None,
                "metric_name": "visits",
                "metric_value": visits,
                "load_dt": FIXED,
            }
        )
        rows.append(
            {
                "month": month,
                "segment_name": "total",
                "segment_id": None,
                "metric_name": "bounce_rate",
                "metric_value": bounce,
                "load_dt": FIXED,
            }
        )
        rows.append(
            {
                "month": month,
                "segment_name": "total",
                "segment_id": None,
                "metric_name": "avg_time",
                "metric_value": 10.0,
                "load_dt": FIXED,
            }
        )
        rows.append(
            {
                "month": month,
                "segment_name": "segment_a",
                "segment_id": "syn-a",
                "metric_name": "visits",
                "metric_value": visits / 2,
                "load_dt": FIXED,
            }
        )
    return rows


def test_safe_divide_and_change_types() -> None:
    assert safe_divide(10, 0) is None
    assert safe_divide(None, 5) is None
    assert safe_divide(10, 5) == 2.0
    assert metric_change_type("bounce_rate") == "pp"
    assert metric_change_type("visits") == "pct"
    assert metric_change_type("foo_share") == "pp"


def test_july_build_mom_yoy() -> None:
    engine = _raw_engine_with(_seed_three_periods())
    result = build_monthly_kpi(engine, "2026-07-01", calc_dt=FIXED)
    assert result.row_count == 4
    df = result.dataframe
    assert list(df.columns) == list(OUTPUT_COLUMNS)
    assert int(df.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).sum()) == 0

    visits = df[(df["segment_name"] == "total") & (df["metric_name"] == "visits")].iloc[0]
    assert visits["prev_month_value"] == 200.0
    assert visits["prev_year_value"] == 100.0
    assert visits["mom_abs_change"] == 50.0
    assert visits["yoy_abs_change"] == 150.0
    assert visits["mom_pct_change"] == 0.25
    assert visits["yoy_pct_change"] == 1.5
    assert visits["mom_pp_change"] is None or pd.isna(visits["mom_pp_change"])
    assert visits["change_type"] == "pct"

    bounce = df[(df["segment_name"] == "total") & (df["metric_name"] == "bounce_rate")].iloc[0]
    assert bounce["change_type"] == "pp"
    assert bounce["mom_pct_change"] is None or pd.isna(bounce["mom_pct_change"])
    assert abs(float(bounce["mom_pp_change"]) - 0.05) < 1e-9
    assert bounce["value_format"] == "percent"
    engine.dispose()


def test_missing_previous_month_fails() -> None:
    rows = [r for r in _seed_three_periods() if r["month"] != date(2026, 6, 1)]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="mom"):
        build_monthly_kpi(engine, "2026-07-01")
    engine.dispose()


def test_missing_previous_year_fails() -> None:
    rows = [r for r in _seed_three_periods() if r["month"] != date(2025, 7, 1)]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="yoy"):
        build_monthly_kpi(engine, "2026-07-01")
    engine.dispose()


def test_zero_denominator_pct_is_null() -> None:
    rows = _seed_three_periods()
    for r in rows:
        if r["month"] == date(2026, 6, 1) and r["metric_name"] == "visits" and r["segment_name"] == "total":
            r["metric_value"] = 0.0
    engine = _raw_engine_with(rows)
    df = build_monthly_kpi(engine, date(2026, 7, 1), calc_dt=FIXED).dataframe
    visits = df[(df["segment_name"] == "total") & (df["metric_name"] == "visits")].iloc[0]
    assert visits["mom_abs_change"] == 250.0
    assert visits["mom_pct_change"] is None or pd.isna(visits["mom_pct_change"])
    engine.dispose()


def test_persist_replace_and_rollback(monkeypatch: Any) -> None:
    raw = _raw_engine_with(_seed_three_periods())
    calc = _calc_engine()
    # seed other mart + prior month kpi
    with calc.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO calc_monthly_channel_mix(month, channel_name, channel_visits, calc_dt) "
                "VALUES (:m, 'demo', 1, :dt)"
            ),
            {"m": date(2026, 7, 1), "dt": FIXED},
        )
    first = build_monthly_kpi(raw, "2026-07-01", calc_dt=FIXED)
    # also seed June kpi row
    june_frame = first.dataframe.copy()
    june_frame["month"] = date(2026, 6, 1)
    persist_monthly_kpi(calc, june_frame, "2026-06-01")

    res = persist_monthly_kpi(calc, first.dataframe, "2026-07-01")
    assert res.status == "replaced"
    assert res.rows_after == 4

    def boom(conn, table, frame):  # type: ignore[no-untyped-def]
        raise RuntimeError("synthetic calc insert failure")

    monkeypatch.setattr(calc_repo, "_insert", boom)
    with pytest.raises(RuntimeError, match="synthetic calc insert failure"):
        persist_monthly_kpi(calc, first.dataframe, "2026-07-01")

    with calc.connect() as conn:
        july = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_kpi WHERE month = :m"),
            {"m": date(2026, 7, 1)},
        ).scalar()
        june = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_kpi WHERE month = :m"),
            {"m": date(2026, 6, 1)},
        ).scalar()
        other = conn.execute(text("SELECT COUNT(*) FROM calc_monthly_channel_mix")).scalar()
    assert int(july) == 4
    assert int(june) == 4
    assert int(other) == 1
    raw.dispose()
    calc.dispose()


def test_transform_schema_stable() -> None:
    df = pd.DataFrame(_seed_three_periods())
    out = _transform_monthly_kpi(df, month_start=date(2026, 7, 1), calc_dt=FIXED)
    assert list(out.columns) == list(OUTPUT_COLUMNS)
