"""Unit tests for calc_monthly_section_share (synthetic RAW; SQLite isolation)."""

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
    SECTIONS,
    SECTION_SHARE_BUSINESS_KEY_COLUMNS,
    SECTION_SHARE_OUTPUT_COLUMNS,
    SECTION_SHARE_SEGMENTS,
    CalcError,
    build_monthly_section_share,
    persist_monthly_section_share,
    safe_divide,
)
import analytics_reporting_platform.calc.repository as calc_repo
from analytics_reporting_platform.calc.monthly_section_share import (
    _transform_monthly_section_share,
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
                CREATE TABLE calc_monthly_section_share (
                    month DATE NOT NULL,
                    segment_name TEXT NOT NULL,
                    segment_id TEXT,
                    section_name TEXT NOT NULL,
                    section_id TEXT,
                    section_visits FLOAT,
                    section_share FLOAT,
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


def _seed_two_months() -> list[dict[str, Any]]:
    """Synthetic 4×6 grid for June+July with stable visits for share/MoM checks."""
    rows: list[dict[str, Any]] = []
    # Per-section weights (sum=100) so shares are exact decimals.
    weights = {
        "support": 40.0,
        "mx": 20.0,
        "explore": 15.0,
        "he": 10.0,
        "da": 10.0,
        "other": 5.0,
    }
    for month, scale in ((date(2026, 6, 1), 1.0), (date(2026, 7, 1), 2.0)):
        for segment in SECTION_SHARE_SEGMENTS:
            for section, w in weights.items():
                rows.append(
                    {
                        "month": month,
                        "segment_name": segment,
                        "segment_id": f"syn-{segment}",
                        "section_name": section,
                        "section_id": f"syn-{section}",
                        "metric_value": w * scale,
                        "load_dt": FIXED,
                    }
                )
        # Ignored noise outside taxonomy.
        rows.append(
            {
                "month": month,
                "segment_name": "organic",
                "segment_id": None,
                "section_name": "support",
                "section_id": "syn-support",
                "metric_value": 9999.0,
                "load_dt": FIXED,
            }
        )
    return rows


def test_valid_build_cardinality_and_mom_pp() -> None:
    engine = _raw_engine_with(_seed_two_months())
    result = build_monthly_section_share(engine, "2026-07-01", calc_dt=FIXED)
    assert result.row_count == 24
    df = result.dataframe
    assert list(df.columns) == list(SECTION_SHARE_OUTPUT_COLUMNS)
    assert int(df.duplicated(subset=list(SECTION_SHARE_BUSINESS_KEY_COLUMNS)).sum()) == 0
    assert set(df["segment_name"]) == set(SECTION_SHARE_SEGMENTS)
    assert set(df["section_name"]) == set(SECTIONS)

    total_support = df[
        (df["segment_name"] == "total") & (df["section_name"] == "support")
    ].iloc[0]
    assert float(total_support["section_visits"]) == 80.0  # 40 * 2
    assert abs(float(total_support["section_share"]) - 0.4) < 1e-12
    assert abs(float(total_support["prev_month_share"]) - 0.4) < 1e-12
    assert abs(float(total_support["mom_pp_change"]) - 0.0) < 1e-12
    engine.dispose()


def test_missing_previous_month_fails() -> None:
    rows = [r for r in _seed_two_months() if r["month"] != date(2026, 6, 1)]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="mom"):
        build_monthly_section_share(engine, "2026-07-01")
    engine.dispose()


def test_zero_total_share_null() -> None:
    rows = _seed_two_months()
    for r in rows:
        if r["month"] == date(2026, 7, 1) and r["segment_name"] == "total":
            r["metric_value"] = 0.0
    engine = _raw_engine_with(rows)
    df = build_monthly_section_share(engine, "2026-07-01", calc_dt=FIXED).dataframe
    total_rows = df[df["segment_name"] == "total"]
    assert total_rows["section_share"].isna().all()
    engine.dispose()


def test_missing_section_fails() -> None:
    rows = [
        r
        for r in _seed_two_months()
        if not (
            r["month"] == date(2026, 7, 1)
            and r["segment_name"] == "total"
            and r["section_name"] == "other"
        )
    ]
    engine = _raw_engine_with(rows)
    with pytest.raises(CalcError, match="missing required section"):
        build_monthly_section_share(engine, "2026-07-01", calc_dt=FIXED)
    engine.dispose()


def test_persist_replace_rollback_and_no_other_mart(monkeypatch: Any = None) -> None:
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

    built = build_monthly_section_share(raw, "2026-07-01", calc_dt=FIXED)
    june = built.dataframe.copy()
    june["month"] = date(2026, 6, 1)
    persist_monthly_section_share(calc, june, "2026-06-01")

    res = persist_monthly_section_share(calc, built.dataframe, "2026-07-01")
    assert res.status == "replaced"
    assert res.rows_after == 24

    skip = persist_monthly_section_share(calc, built.dataframe, "2026-07-01", mode="skip")
    assert skip.status == "skipped"
    assert skip.rows_written == 0

    orig_insert = calc_repo._insert

    def boom(conn, table, frame):  # type: ignore[no-untyped-def]
        raise RuntimeError("synthetic section_share insert failure")

    calc_repo._insert = boom
    try:
        with pytest.raises(RuntimeError, match="synthetic section_share insert failure"):
            persist_monthly_section_share(calc, built.dataframe, "2026-07-01")
    finally:
        calc_repo._insert = orig_insert

    with calc.connect() as conn:
        july = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_section_share WHERE month = :m"),
            {"m": date(2026, 7, 1)},
        ).scalar()
        june_n = conn.execute(
            text("SELECT COUNT(*) FROM calc_monthly_section_share WHERE month = :m"),
            {"m": date(2026, 6, 1)},
        ).scalar()
        kpi_n = conn.execute(text("SELECT COUNT(*) FROM calc_monthly_kpi")).scalar()
    assert int(july) == 24
    assert int(june_n) == 24
    assert int(kpi_n) == 1
    raw.dispose()
    calc.dispose()


def test_safe_divide_zero() -> None:
    assert safe_divide(10, 0) is None
    assert abs(float(safe_divide(40, 100) or 0) - 0.4) < 1e-12


def test_transform_schema_stable() -> None:
    df = pd.DataFrame(_seed_two_months())
    df = df[
        df["segment_name"].isin(SECTION_SHARE_SEGMENTS)
        & df["section_name"].isin(SECTIONS)
    ].copy()
    out = _transform_monthly_section_share(
        df, month_start=date(2026, 7, 1), calc_dt=FIXED
    )
    assert list(out.columns) == list(SECTION_SHARE_OUTPUT_COLUMNS)
    assert out.iloc[0]["segment_name"] == "total"
    assert out.iloc[0]["section_name"] == "support"


if __name__ == "__main__":
    test_valid_build_cardinality_and_mom_pp()
    test_missing_previous_month_fails()
    test_zero_total_share_null()
    test_missing_section_fails()
    test_persist_replace_rollback_and_no_other_mart()
    test_safe_divide_zero()
    test_transform_schema_stable()
    print("OK", 7)
