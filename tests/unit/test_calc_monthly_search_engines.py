"""Unit tests for calc_monthly_search_engines (synthetic RAW; SQLite isolation)."""

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
    SEARCH_ENGINE_CATEGORIES,
    SEARCH_ENGINES_BUSINESS_KEY_COLUMNS,
    SEARCH_ENGINES_OUTPUT_COLUMNS,
    CalcError,
    build_monthly_search_engines,
    persist_monthly_search_engines,
    safe_divide,
)
import analytics_reporting_platform.calc.repository as calc_repo
from analytics_reporting_platform.calc.monthly_search_engines import (
    _transform_monthly_search_engines,
)


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
                CREATE TABLE calc_monthly_search_engines (
                    month DATE NOT NULL,
                    search_engine TEXT NOT NULL,
                    search_engine_visits FLOAT,
                    search_engine_share FLOAT,
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
    june_organic: float = 1000.0,
    july_organic: float = 2000.0,
    june_yandex: float = 400.0,
    july_yandex: float = 500.0,
    june_google: float = 500.0,
    july_google: float = 1200.0,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for month, organic, yandex, google in (
        (date(2026, 6, 1), june_organic, june_yandex, june_google),
        (date(2026, 7, 1), july_organic, july_yandex, july_google),
    ):
        for name, value in (
            ("organic", organic),
            ("yandex", yandex),
            ("google", google),
        ):
            rows.append(
                {
                    "month": month,
                    "segment_name": name,
                    "segment_id": f"syn-{name}",
                    "metric_name": "visits",
                    "metric_value": value,
                    "load_dt": FIXED,
                }
            )
        # Ignored noise.
        rows.append(
            {
                "month": month,
                "segment_name": "total",
                "segment_id": None,
                "metric_name": "visits",
                "metric_value": 9999.0,
                "load_dt": FIXED,
            }
        )
        rows.append(
            {
                "month": month,
                "segment_name": "organic",
                "segment_id": "syn-organic",
                "metric_name": "bounce_rate",
                "metric_value": 0.4,
                "load_dt": FIXED,
            }
        )
    return rows


def test_valid_build_categories_and_mom_pp() -> None:
    engine = _raw_engine_with(_seed_two_months())
    result = build_monthly_search_engines(engine, "2026-07-01", calc_dt=FIXED)
    assert result.row_count == 4
    df = result.dataframe
    assert list(df.columns) == list(SEARCH_ENGINES_OUTPUT_COLUMNS)
    assert int(df.duplicated(subset=list(SEARCH_ENGINES_BUSINESS_KEY_COLUMNS)).sum()) == 0
    assert list(df["search_engine"]) == list(SEARCH_ENGINE_CATEGORIES)

    organic = df[df["search_engine"] == "organic_total"].iloc[0]
    other = df[df["search_engine"] == "other_search_engines"].iloc[0]
    assert organic["search_engine_visits"] == 2000.0
    assert abs(float(organic["search_engine_share"]) - 1.0) < 1e-12
    # other = 2000 - 500 - 1200 = 300; share = 0.15
    assert other["search_engine_visits"] == 300.0
    assert abs(float(other["search_engine_share"]) - 0.15) < 1e-12
    # June other share = (1000-400-500)/1000 = 0.1; mom_pp = 0.05
    assert abs(float(other["prev_month_share"]) - 0.1) < 1e-12
    assert abs(float(other["mom_pp_change"]) - 0.05) < 1e-12
    engine.dispose()


def test_clip_other_search_engines_at_zero() -> None:
    rows = _seed_two_months(july_organic=100.0, july_yandex=60.0, july_google=60.0)
    engine = _raw_engine_with(rows)
    df = build_monthly_search_engines(engine, date(2026, 7, 1), calc_dt=FIXED).dataframe
    other = df[df["search_engine"] == "other_search_engines"].iloc[0]
    assert float(other["search_engine_visits"]) == 0.0
    engine.dispose()


def test_missing_previous_month_fails() -> None:
    rows = [r for r in _seed_two_months() if r["month"] != date(2026, 6, 1)]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="mom"):
        build_monthly_search_engines(engine, "2026-07-01")
    engine.dispose()


def test_zero_organic_share_null() -> None:
    rows = _seed_two_months(july_organic=0.0, july_yandex=0.0, july_google=0.0)
    engine = _raw_engine_with(rows)
    df = build_monthly_search_engines(engine, "2026-07-01", calc_dt=FIXED).dataframe
    organic = df[df["search_engine"] == "organic_total"].iloc[0]
    assert organic["search_engine_share"] is None or pd.isna(organic["search_engine_share"])
    engine.dispose()


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

    built = build_monthly_search_engines(raw, "2026-07-01", calc_dt=FIXED)
    june = built.dataframe.copy()
    june["month"] = date(2026, 6, 1)
    persist_monthly_search_engines(calc, june, "2026-06-01")

    res = persist_monthly_search_engines(calc, built.dataframe, "2026-07-01")
    assert res.status == "replaced"
    assert res.rows_after == 4

    def boom(conn, table, frame):  # type: ignore[no-untyped-def]
        raise RuntimeError("synthetic search_engines insert failure")

    monkeypatch.setattr(calc_repo, "_insert", boom)
    with pytest.raises(RuntimeError, match="synthetic search_engines insert failure"):
        persist_monthly_search_engines(calc, built.dataframe, "2026-07-01")

    with calc.connect() as conn:
        july = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_search_engines WHERE month = :m"),
            {"m": date(2026, 7, 1)},
        ).scalar()
        june_n = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_search_engines WHERE month = :m"),
            {"m": date(2026, 6, 1)},
        ).scalar()
        kpi_n = conn.execute(text("SELECT COUNT(*) FROM calc_monthly_kpi")).scalar()
    assert int(july) == 4
    assert int(june_n) == 4
    assert int(kpi_n) == 1
    raw.dispose()
    calc.dispose()


def test_safe_divide_zero() -> None:
    assert safe_divide(10, 0) is None
    assert abs(float(safe_divide(300, 2000) or 0) - 0.15) < 1e-12


def test_transform_schema_stable() -> None:
    df = pd.DataFrame(_seed_two_months())
    df = df[
        (df["metric_name"] == "visits")
        & (df["segment_name"].isin(["organic", "yandex", "google"]))
    ].copy()
    out = _transform_monthly_search_engines(
        df, month_start=date(2026, 7, 1), calc_dt=FIXED
    )
    assert list(out.columns) == list(SEARCH_ENGINES_OUTPUT_COLUMNS)
