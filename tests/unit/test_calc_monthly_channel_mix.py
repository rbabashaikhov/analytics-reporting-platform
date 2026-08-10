"""Unit tests for calc_monthly_channel_mix (synthetic RAW; SQLite isolation)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

try:
    import pytest
except ModuleNotFoundError:  # pragma: no cover - private venv may lack pytest
    class _Raises:
        def __init__(self, exc: type[BaseException], match: str | None = None) -> None:
            self.exc = exc
            self.match = match

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
            if exc_type is None:
                raise AssertionError(f"Expected {self.exc.__name__} was not raised")
            if not issubclass(exc_type, self.exc):
                return False
            if self.match is not None and self.match not in str(exc):
                raise AssertionError(f"Expected match {self.match!r} in {exc!r}")
            return True

    class _PytestShim:
        raises = staticmethod(lambda exc, match=None: _Raises(exc, match))

    pytest = _PytestShim()  # type: ignore[assignment]

from analytics_reporting_platform.calc import (
    CHANNEL_MIX_BUSINESS_KEY_COLUMNS,
    CHANNEL_MIX_OUTPUT_COLUMNS,
    MARKETING_CHANNELS,
    CalcError,
    build_monthly_channel_mix,
    persist_monthly_channel_mix,
    safe_divide,
)
import analytics_reporting_platform.calc.repository as calc_repo
from analytics_reporting_platform.calc.monthly_channel_mix import _transform_monthly_channel_mix


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
                CREATE TABLE calc_monthly_channel_mix (
                    month DATE NOT NULL,
                    channel_name TEXT NOT NULL,
                    channel_id TEXT,
                    channel_visits FLOAT,
                    channel_share FLOAT,
                    prev_month_share FLOAT,
                    mom_pp_change FLOAT,
                    calc_dt TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE calc_monthly_kpi (
                    month DATE NOT NULL,
                    segment_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value FLOAT,
                    calc_dt TIMESTAMP NOT NULL
                )
                """
            )
        )
    return engine


def _seed_two_months(
    *,
    june_total: float = 1000.0,
    july_total: float = 2000.0,
    june_channel: float = 100.0,
    july_channel: float = 250.0,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for month, total, channel_visits in (
        (date(2026, 6, 1), june_total, june_channel),
        (date(2026, 7, 1), july_total, july_channel),
    ):
        rows.append(
            {
                "month": month,
                "segment_name": "total",
                "segment_id": None,
                "metric_name": "visits",
                "metric_value": total,
                "load_dt": FIXED,
            }
        )
        # Non-channel / excluded segments must not appear in output.
        rows.append(
            {
                "month": month,
                "segment_name": "ai",
                "segment_id": "syn-ai",
                "metric_name": "visits",
                "metric_value": 5.0,
                "load_dt": FIXED,
            }
        )
        for i, name in enumerate(MARKETING_CHANNELS):
            rows.append(
                {
                    "month": month,
                    "segment_name": name,
                    "segment_id": f"syn-{i}",
                    "metric_name": "visits",
                    "metric_value": channel_visits,
                    "load_dt": FIXED,
                }
            )
            # Non-visits metrics ignored by extractor filter.
            rows.append(
                {
                    "month": month,
                    "segment_name": name,
                    "segment_id": f"syn-{i}",
                    "metric_name": "bounce_rate",
                    "metric_value": 0.5,
                    "load_dt": FIXED,
                }
            )
    return rows


def test_valid_build_share_and_mom_pp() -> None:
    engine = _raw_engine_with(_seed_two_months())
    result = build_monthly_channel_mix(engine, "2026-07-01", calc_dt=FIXED)
    assert result.row_count == 7
    df = result.dataframe
    assert list(df.columns) == list(CHANNEL_MIX_OUTPUT_COLUMNS)
    assert int(df.duplicated(subset=list(CHANNEL_MIX_BUSINESS_KEY_COLUMNS)).sum()) == 0
    assert set(df["channel_name"]) == set(MARKETING_CHANNELS)
    assert "ai" not in set(df["channel_name"])
    assert "total" not in set(df["channel_name"])

    organic = df[df["channel_name"] == "organic"].iloc[0]
    assert organic["channel_visits"] == 250.0
    assert abs(float(organic["channel_share"]) - 0.125) < 1e-12
    assert abs(float(organic["prev_month_share"]) - 0.1) < 1e-12
    assert abs(float(organic["mom_pp_change"]) - 0.025) < 1e-12
    engine.dispose()


def test_zero_denominator_share_is_null() -> None:
    rows = _seed_two_months(july_total=0.0)
    engine = _raw_engine_with(rows)
    df = build_monthly_channel_mix(engine, date(2026, 7, 1), calc_dt=FIXED).dataframe
    organic = df[df["channel_name"] == "organic"].iloc[0]
    assert organic["channel_share"] is None or pd.isna(organic["channel_share"])
    assert organic["mom_pp_change"] is None or pd.isna(organic["mom_pp_change"])
    engine.dispose()


def test_missing_previous_month_fails() -> None:
    rows = [r for r in _seed_two_months() if r["month"] != date(2026, 6, 1)]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="mom"):
        build_monthly_channel_mix(engine, "2026-07-01")
    engine.dispose()


def test_missing_channel_fails() -> None:
    rows = [
        r
        for r in _seed_two_months()
        if not (
            r["month"] == date(2026, 7, 1)
            and r["segment_name"] == "push"
            and r["metric_name"] == "visits"
        )
    ]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="missing required marketing channel"):
        build_monthly_channel_mix(engine, "2026-07-01")
    engine.dispose()


def test_missing_total_fails() -> None:
    rows = [
        r
        for r in _seed_two_months()
        if not (
            r["month"] == date(2026, 7, 1)
            and r["segment_name"] == "total"
            and r["metric_name"] == "visits"
        )
    ]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="total segment visits"):
        build_monthly_channel_mix(engine, "2026-07-01")
    engine.dispose()


def test_duplicate_keys_rejected() -> None:
    df = pd.DataFrame(_seed_two_months())
    df = df[df["metric_name"] == "visits"].copy()
    # Force duplicate channel rows for July before transform by appending.
    dup = df[
        (df["month"] == date(2026, 7, 1)) & (df["segment_name"] == "organic")
    ].copy()
    bad = pd.concat([df, dup], ignore_index=True)
    with pytest.raises(CalcError, match="duplicate business keys"):
        _transform_monthly_channel_mix(bad, month_start=date(2026, 7, 1), calc_dt=FIXED)


def test_persist_replace_rollback_and_no_other_mart(monkeypatch: Any) -> None:
    raw = _raw_engine_with(_seed_two_months())
    calc = _calc_engine()
    with calc.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO calc_monthly_kpi(month, segment_name, metric_name, metric_value, calc_dt) "
                "VALUES (:m, 'total', 'visits', 1, :dt)"
            ),
            {"m": date(2026, 7, 1), "dt": FIXED},
        )

    built = build_monthly_channel_mix(raw, "2026-07-01", calc_dt=FIXED)
    june = built.dataframe.copy()
    june["month"] = date(2026, 6, 1)
    # June share/prev columns are not revalidated beyond schema/month — seed via persist
    # after adjusting channel visits only for uniqueness of month.
    persist_monthly_channel_mix(calc, june, "2026-06-01")

    res = persist_monthly_channel_mix(calc, built.dataframe, "2026-07-01")
    assert res.status == "replaced"
    assert res.rows_after == 7

    def boom(conn, table, frame):  # type: ignore[no-untyped-def]
        raise RuntimeError("synthetic channel_mix insert failure")

    monkeypatch.setattr(calc_repo, "_insert", boom)
    with pytest.raises(RuntimeError, match="synthetic channel_mix insert failure"):
        persist_monthly_channel_mix(calc, built.dataframe, "2026-07-01")

    with calc.connect() as conn:
        july = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_channel_mix WHERE month = :m"),
            {"m": date(2026, 7, 1)},
        ).scalar()
        june_n = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_channel_mix WHERE month = :m"),
            {"m": date(2026, 6, 1)},
        ).scalar()
        kpi_n = conn.execute(text("SELECT COUNT(*) FROM calc_monthly_kpi")).scalar()
    assert int(july) == 7
    assert int(june_n) == 7
    assert int(kpi_n) == 1
    raw.dispose()
    calc.dispose()


def test_safe_divide_zero() -> None:
    assert safe_divide(10, 0) is None
    assert safe_divide(25, 200) == 0.125


def test_transform_schema_stable() -> None:
    df = pd.DataFrame(_seed_two_months())
    df = df[df["metric_name"] == "visits"].copy()
    out = _transform_monthly_channel_mix(df, month_start=date(2026, 7, 1), calc_dt=FIXED)
    assert list(out.columns) == list(CHANNEL_MIX_OUTPUT_COLUMNS)
