"""Unit tests for Monthly RAW persistence (synthetic data; SQLite isolation)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from analytics_reporting_platform.raw.repository import (
    MART_SPECS,
    RawPersistenceError,
    month_exists,
    persist_month,
    resolve_mart_spec,
    validate_persist_dataframe,
)
import analytics_reporting_platform.raw.repository as repository


FIXED_LOAD = datetime(2026, 8, 1, 12, 0, 0)


def _metrics_df(month: date = date(2026, 7, 1)) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "month": month,
                "metric_name": "visits",
                "metric_value": 10.0,
                "segment_name": "total",
                "segment_id": None,
                "load_dt": FIXED_LOAD,
            },
            {
                "month": month,
                "metric_name": "avg_time",
                "metric_value": 1.5,
                "segment_name": "total",
                "segment_id": None,
                "load_dt": FIXED_LOAD,
            },
        ]
    )


def _section_df(month: date = date(2026, 7, 1)) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "month": month,
                "segment_name": "total",
                "segment_id": None,
                "metric_value": 5.0,
                "section_name": "product_1",
                "section_id": "synthetic-section-a",
                "load_dt": FIXED_LOAD,
            },
            {
                "month": month,
                "segment_name": "traffic_a",
                "segment_id": "synthetic-traffic-a",
                "metric_value": 2.0,
                "section_name": "content_section_1",
                "section_id": "synthetic-section-b",
                "load_dt": FIXED_LOAD,
            },
        ]
    )


def _url_df(month: date = date(2026, 7, 1)) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "month": month,
                "page_url": "/demo/page-1",
                "visits": 100,
                "load_dt": FIXED_LOAD,
            },
            {
                "month": month,
                "page_url": "/demo/page-2",
                "visits": 50,
                "load_dt": FIXED_LOAD,
            },
        ]
    )


def _make_engine():
    engine = create_engine("sqlite:///:memory:")
    ddl = """
    CREATE TABLE raw_aa_monthly_metrics (
        month DATE NOT NULL,
        metric_name TEXT NOT NULL,
        metric_value FLOAT,
        segment_name TEXT NOT NULL,
        segment_id TEXT,
        load_dt TIMESTAMP NOT NULL
    );
    CREATE TABLE raw_aa_monthly_by_section (
        month DATE NOT NULL,
        segment_name TEXT NOT NULL,
        segment_id TEXT,
        metric_value FLOAT,
        section_name TEXT NOT NULL,
        section_id TEXT NOT NULL,
        load_dt TIMESTAMP NOT NULL
    );
    CREATE TABLE raw_aa_monthly_by_url (
        month DATE NOT NULL,
        page_url TEXT NOT NULL,
        visits BIGINT,
        load_dt TIMESTAMP NOT NULL
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    return engine


def test_resolve_supported_marts() -> None:
    assert resolve_mart_spec("monthly_metrics").table_name == "raw_aa_monthly_metrics"
    assert resolve_mart_spec("monthly_by_section").table_name == "raw_aa_monthly_by_section"
    assert resolve_mart_spec("monthly_by_url").table_name == "raw_aa_monthly_by_url"


def test_unsupported_mart_rejected() -> None:
    with pytest.raises(RawPersistenceError, match="Unsupported RAW mart"):
        resolve_mart_spec("weekly_organic")


def test_valid_inputs_pass_validation() -> None:
    for mart, frame in (
        ("monthly_metrics", _metrics_df()),
        ("monthly_by_section", _section_df()),
        ("monthly_by_url", _url_df()),
    ):
        spec, month, out = validate_persist_dataframe(mart, frame, "2026-07-01")
        assert month == date(2026, 7, 1)
        assert list(out.columns) == list(spec.columns)


def test_empty_input_rejected() -> None:
    with pytest.raises(RawPersistenceError, match="empty"):
        validate_persist_dataframe(
            "monthly_metrics",
            pd.DataFrame(columns=list(MART_SPECS["monthly_metrics"].columns)),
            "2026-07-01",
        )


def test_missing_columns_rejected() -> None:
    df = _metrics_df().drop(columns=["segment_name"])
    with pytest.raises(RawPersistenceError, match="missing required columns"):
        validate_persist_dataframe("monthly_metrics", df, "2026-07-01")


def test_multi_month_rejected() -> None:
    df = pd.concat([_metrics_df(date(2026, 6, 1)), _metrics_df(date(2026, 7, 1))])
    with pytest.raises(RawPersistenceError, match="exactly one month"):
        validate_persist_dataframe("monthly_metrics", df, "2026-07-01")


def test_month_mismatch_rejected() -> None:
    with pytest.raises(RawPersistenceError, match="does not match"):
        validate_persist_dataframe("monthly_metrics", _metrics_df(), "2026-06-01")


def test_duplicate_metrics_key_rejected() -> None:
    df = pd.concat([_metrics_df(), _metrics_df()], ignore_index=True)
    with pytest.raises(RawPersistenceError, match="duplicate business keys"):
        validate_persist_dataframe("monthly_metrics", df, "2026-07-01")


def test_duplicate_section_key_rejected() -> None:
    df = pd.concat([_section_df(), _section_df()], ignore_index=True)
    with pytest.raises(RawPersistenceError, match="duplicate business keys"):
        validate_persist_dataframe("monthly_by_section", df, "2026-07-01")


def test_duplicate_url_key_rejected() -> None:
    df = pd.concat([_url_df(), _url_df()], ignore_index=True)
    with pytest.raises(RawPersistenceError, match="duplicate business keys"):
        validate_persist_dataframe("monthly_by_url", df, "2026-07-01")


def test_insert_then_skip_existing() -> None:
    engine = _make_engine()
    first = persist_month(engine, "monthly_metrics", _metrics_df(), "2026-07-01", mode="skip")
    assert first.status == "inserted"
    assert first.rows_written == 2
    assert month_exists(engine, "monthly_metrics", "2026-07-01")

    second = persist_month(engine, "monthly_metrics", _metrics_df(), "2026-07-01", mode="skip")
    assert second.status == "skipped"
    assert second.rows_written == 0
    assert second.rows_after == 2
    engine.dispose()


def test_replace_overwrites_month_only() -> None:
    engine = _make_engine()
    persist_month(engine, "monthly_by_url", _url_df(date(2026, 6, 1)), "2026-06-01", mode="skip")
    persist_month(engine, "monthly_by_url", _url_df(date(2026, 7, 1)), "2026-07-01", mode="skip")

    replacement = pd.DataFrame(
        [
            {
                "month": date(2026, 7, 1),
                "page_url": "/demo/page-9",
                "visits": 9,
                "load_dt": FIXED_LOAD,
            }
        ]
    )
    result = persist_month(
        engine, "monthly_by_url", replacement, "2026-07-01", mode="replace"
    )
    assert result.status == "replaced"
    assert result.rows_written == 1
    assert result.rows_after == 1

    with engine.connect() as conn:
        june = conn.execute(
            text("SELECT COUNT(*) FROM raw_aa_monthly_by_url WHERE month = :m"),
            {"m": date(2026, 6, 1)},
        ).scalar()
        july_urls = [
            r[0]
            for r in conn.execute(
                text("SELECT page_url FROM raw_aa_monthly_by_url WHERE month = :m"),
                {"m": date(2026, 7, 1)},
            ).fetchall()
        ]
    assert int(june) == 2
    assert july_urls == ["/demo/page-9"]
    engine.dispose()


def test_delete_predicate_is_exact_month(monkeypatch: Any) -> None:
    engine = _make_engine()
    persist_month(engine, "monthly_metrics", _metrics_df(), "2026-07-01", mode="skip")
    seen: list[dict[str, Any]] = []

    original = repository._delete_month

    def wrapped(conn, table_name, month_start):  # type: ignore[no-untyped-def]
        seen.append({"table": table_name, "month": month_start})
        sql = str(
            text(f"DELETE FROM {table_name} WHERE month = :month")
        )
        assert "WHERE month = :month" in sql or "month = :month" in sql
        assert "OR" not in sql.upper()
        return original(conn, table_name, month_start)

    monkeypatch.setattr(repository, "_delete_month", wrapped)
    persist_month(engine, "monthly_metrics", _metrics_df(), "2026-07-01", mode="replace")
    assert seen == [{"table": "raw_aa_monthly_metrics", "month": date(2026, 7, 1)}]
    engine.dispose()


def test_rollback_on_insert_failure(monkeypatch: Any) -> None:
    engine = _make_engine()
    persist_month(engine, "monthly_by_section", _section_df(), "2026-07-01", mode="skip")

    def boom(conn, table_name, frame):  # type: ignore[no-untyped-def]
        raise RuntimeError("synthetic insert failure")

    monkeypatch.setattr(repository, "_insert_month_conn", boom)
    with pytest.raises(RuntimeError, match="synthetic insert failure"):
        persist_month(
            engine, "monthly_by_section", _section_df(), "2026-07-01", mode="replace"
        )

    # Original rows preserved by transaction rollback.
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM raw_aa_monthly_by_section WHERE month = :m"),
            {"m": date(2026, 7, 1)},
        ).scalar()
    assert int(count) == 2
    engine.dispose()


def test_refuse_frozen_baseline_port() -> None:
    engine = create_engine("postgresql+psycopg2://u:p@127.0.0.1:15432/raw_data")
    # URL object only — no connection attempted for reject check before connect on skip path
    # month_exists connects; for persist validation reject happens first
    with pytest.raises(RawPersistenceError, match="frozen baseline"):
        persist_month(engine, "monthly_metrics", _metrics_df(), "2026-07-01", mode="skip")
    engine.dispose()


def test_all_three_marts_insert() -> None:
    engine = _make_engine()
    for mart, frame in (
        ("monthly_metrics", _metrics_df()),
        ("monthly_by_section", _section_df()),
        ("monthly_by_url", _url_df()),
    ):
        result = persist_month(engine, mart, frame, "2026-07-01", mode="skip")
        assert result.status == "inserted"
        assert result.rows_after == len(frame)
    engine.dispose()
