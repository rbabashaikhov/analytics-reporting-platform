"""Unit tests for calc_explore_buying_guides_kpi (synthetic RAW; SQLite isolation)."""

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
    EXPLORE_BG_BUSINESS_KEY_COLUMNS,
    EXPLORE_BG_KPI_ORDER,
    EXPLORE_BG_OUTPUT_COLUMNS,
    EXPLORE_BG_SECTIONS,
    CalcError,
    build_explore_buying_guides_kpi,
    persist_explore_buying_guides_kpi,
    safe_divide,
)
import analytics_reporting_platform.calc.repository as calc_repo
from analytics_reporting_platform.calc.explore_buying_guides_kpi import (
    _transform_explore_buying_guides_kpi,
)


FIXED = datetime(2026, 8, 1, 12, 0, 0)


def _raw_engine_with(rows: list[dict[str, Any]]):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE raw_aa_monthly_by_section (
                    month DATE NOT NULL,
                    segment_name TEXT NOT NULL,
                    segment_id TEXT,
                    section_name TEXT NOT NULL,
                    section_id TEXT,
                    metric_value FLOAT,
                    load_dt TIMESTAMP
                )
                """
            )
        )
    pd.DataFrame(rows).to_sql(
        "raw_aa_monthly_by_section", engine, if_exists="append", index=False
    )
    return engine


def _calc_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE calc_explore_buying_guides_kpi (
                    month DATE NOT NULL,
                    section_name TEXT NOT NULL,
                    kpi_name TEXT NOT NULL,
                    kpi_value FLOAT,
                    prev_month_value FLOAT,
                    prev_year_value FLOAT,
                    mom_pct_change FLOAT,
                    yoy_pct_change FLOAT,
                    display_order INTEGER,
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


def _seed_lookback(
    *,
    yoy_total: float = 100.0,
    mom_total: float = 150.0,
    cur_total: float = 200.0,
    yoy_organic: float = 40.0,
    mom_organic: float = 50.0,
    cur_organic: float = 80.0,
) -> list[dict[str, Any]]:
    """Seed YoY (2025-07), MoM (2026-06), current (2026-07) for both sections."""
    rows: list[dict[str, Any]] = []
    months = {
        date(2025, 7, 1): (yoy_total, yoy_organic),
        date(2026, 6, 1): (mom_total, mom_organic),
        date(2026, 7, 1): (cur_total, cur_organic),
    }
    for month, (total, organic) in months.items():
        for section in EXPLORE_BG_SECTIONS:
            for segment, value in (("total", total), ("organic", organic)):
                rows.append(
                    {
                        "month": month,
                        "segment_name": segment,
                        "segment_id": f"syn-{segment}",
                        "section_name": section,
                        "section_id": f"syn-{section}",
                        "metric_value": value,
                        "load_dt": FIXED,
                    }
                )
        # Ignored noise.
        rows.append(
            {
                "month": month,
                "segment_name": "ai",
                "segment_id": None,
                "section_name": "explore",
                "section_id": "syn-explore",
                "metric_value": 9999.0,
                "load_dt": FIXED,
            }
        )
    return rows


def test_valid_build_cardinality_mom_yoy() -> None:
    engine = _raw_engine_with(_seed_lookback())
    result = build_explore_buying_guides_kpi(engine, "2026-07-01", calc_dt=FIXED)
    assert result.row_count == 4
    df = result.dataframe
    assert list(df.columns) == list(EXPLORE_BG_OUTPUT_COLUMNS)
    assert int(df.duplicated(subset=list(EXPLORE_BG_BUSINESS_KEY_COLUMNS)).sum()) == 0
    assert set(df["section_name"]) == set(EXPLORE_BG_SECTIONS)
    assert set(df["kpi_name"]) == set(EXPLORE_BG_KPI_ORDER)

    total = df[(df["section_name"] == "explore") & (df["kpi_name"] == "total_visits")].iloc[0]
    assert float(total["kpi_value"]) == 200.0
    assert float(total["prev_month_value"]) == 150.0
    assert float(total["prev_year_value"]) == 100.0
    # mom = (200-150)/150 = 1/3
    assert abs(float(total["mom_pct_change"]) - (50.0 / 150.0)) < 1e-12
    # yoy = (200-100)/100 = 1.0
    assert abs(float(total["yoy_pct_change"]) - 1.0) < 1e-12
    assert int(total["display_order"]) == 1

    organic = df[
        (df["section_name"] == "buying_guides") & (df["kpi_name"] == "organic_visits")
    ].iloc[0]
    assert int(organic["display_order"]) == 2
    engine.dispose()


def test_missing_yoy_fails() -> None:
    rows = [r for r in _seed_lookback() if r["month"] != date(2025, 7, 1)]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="yoy"):
        build_explore_buying_guides_kpi(engine, "2026-07-01")
    engine.dispose()


def test_missing_mom_fails() -> None:
    rows = [r for r in _seed_lookback() if r["month"] != date(2026, 6, 1)]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="mom"):
        build_explore_buying_guides_kpi(engine, "2026-07-01")
    engine.dispose()


def test_zero_prev_pct_null() -> None:
    rows = _seed_lookback(mom_total=0.0, mom_organic=0.0)
    engine = _raw_engine_with(rows)
    df = build_explore_buying_guides_kpi(engine, "2026-07-01", calc_dt=FIXED).dataframe
    total = df[(df["section_name"] == "explore") & (df["kpi_name"] == "total_visits")].iloc[0]
    assert total["mom_pct_change"] is None or pd.isna(total["mom_pct_change"])
    engine.dispose()


def test_persist_replace_skip_rollback(monkeypatch: Any = None) -> None:
    raw = _raw_engine_with(_seed_lookback())
    calc = _calc_engine()
    with calc.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO calc_monthly_kpi(month, segment_name, metric_name, metric_value, calc_dt) "
                "VALUES (:m, 'total', 'visits', 1, :dt)"
            ),
            {"m": date(2026, 7, 1), "dt": FIXED},
        )

    built = build_explore_buying_guides_kpi(raw, "2026-07-01", calc_dt=FIXED)
    june = built.dataframe.copy()
    june["month"] = date(2026, 6, 1)
    persist_explore_buying_guides_kpi(calc, june, "2026-06-01")

    res = persist_explore_buying_guides_kpi(calc, built.dataframe, "2026-07-01")
    assert res.status == "replaced"
    assert res.rows_after == 4

    skip = persist_explore_buying_guides_kpi(
        calc, built.dataframe, "2026-07-01", mode="skip"
    )
    assert skip.status == "skipped"
    assert skip.rows_written == 0

    orig_insert = calc_repo._insert

    def boom(conn, table, frame):  # type: ignore[no-untyped-def]
        raise RuntimeError("synthetic explore_bg insert failure")

    calc_repo._insert = boom
    try:
        with pytest.raises(RuntimeError, match="synthetic explore_bg insert failure"):
            persist_explore_buying_guides_kpi(calc, built.dataframe, "2026-07-01")
    finally:
        calc_repo._insert = orig_insert

    with calc.connect() as conn:
        july = conn.execute(
            text("SELECT COUNT(*) FROM calc_explore_buying_guides_kpi WHERE month = :m"),
            {"m": date(2026, 7, 1)},
        ).scalar()
        june_n = conn.execute(
            text("SELECT COUNT(*) FROM calc_explore_buying_guides_kpi WHERE month = :m"),
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
    assert abs(float(safe_divide(50, 150) or 0) - (50.0 / 150.0)) < 1e-12


def test_transform_schema_stable() -> None:
    df = pd.DataFrame(_seed_lookback())
    df = df[
        df["segment_name"].isin(["total", "organic"])
        & df["section_name"].isin(EXPLORE_BG_SECTIONS)
    ].copy()
    out = _transform_explore_buying_guides_kpi(
        df,
        month_start=date(2026, 7, 1),
        min_month=date(2025, 7, 1),
        max_month=date(2026, 7, 1),
        calc_dt=FIXED,
    )
    assert list(out.columns) == list(EXPLORE_BG_OUTPUT_COLUMNS)
    assert out.iloc[0]["section_name"] == "explore"
    assert out.iloc[0]["kpi_name"] == "total_visits"


if __name__ == "__main__":
    test_valid_build_cardinality_mom_yoy()
    test_missing_yoy_fails()
    test_missing_mom_fails()
    test_zero_prev_pct_null()
    test_persist_replace_skip_rollback()
    test_safe_divide_zero()
    test_transform_schema_stable()
    print("OK", 7)
