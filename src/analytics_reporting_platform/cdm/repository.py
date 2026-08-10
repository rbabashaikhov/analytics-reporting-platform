"""PostgreSQL persistence for full-private Monthly CDM marts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final, Literal, Optional, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import Connection

from analytics_reporting_platform.cdm.models import CdmError, MonthInput, parse_month_start

_FROZEN_BASELINE_PORT: Final = 15432
CdmPersistStatus = Literal["replaced", "skipped"]
CdmPersistMode = Literal["replace", "skip"]


@dataclass(frozen=True)
class PersistCdmResult:
    """Structured CDM persistence outcome (no row contents)."""

    mart: str
    status: CdmPersistStatus
    rows_before: int
    rows_written: int
    rows_after: int
    month: Optional[date] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None


def persist_cdm_month(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mart: str,
    table: str,
    output_columns: Sequence[str],
    business_key_columns: Sequence[str],
    required_non_null: Sequence[str],
    mode: CdmPersistMode = "replace",
) -> PersistCdmResult:
    """Persist one report-month slice for a month-scoped CDM mart."""
    _reject_frozen_baseline(cdm_engine)
    if mode not in ("replace", "skip"):
        raise CdmError(f"Unsupported CDM persist mode: {mode!r}")
    month_start = parse_month_start(month)
    frame = _validate_frame(
        dataframe,
        output_columns=output_columns,
        business_key_columns=business_key_columns,
        required_non_null=required_non_null,
        expected_months={month_start},
    )
    rows_before = _count_month(cdm_engine, table, month_start)
    if mode == "skip" and rows_before > 0:
        return PersistCdmResult(
            mart=mart,
            status="skipped",
            rows_before=rows_before,
            rows_written=0,
            rows_after=rows_before,
            month=month_start,
        )
    with cdm_engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table} WHERE month = :month"), {"month": month_start})
        _insert(conn, table, frame)
    rows_after = _count_month(cdm_engine, table, month_start)
    return PersistCdmResult(
        mart=mart,
        status="replaced",
        rows_before=rows_before,
        rows_written=len(frame),
        rows_after=rows_after,
        month=month_start,
    )


def persist_cdm_window(
    cdm_engine: Engine,
    dataframe: pd.DataFrame,
    *,
    mart: str,
    table: str,
    window_start: date,
    window_end: date,
    output_columns: Sequence[str],
    business_key_columns: Sequence[str],
    required_non_null: Sequence[str],
    mode: CdmPersistMode = "replace",
) -> PersistCdmResult:
    """Replace an inclusive month window (trend / dim marts)."""
    _reject_frozen_baseline(cdm_engine)
    if mode not in ("replace", "skip"):
        raise CdmError(f"Unsupported CDM persist mode: {mode!r}")
    if window_start > window_end:
        raise CdmError("CDM window_start must be <= window_end")
    frame = _validate_frame(
        dataframe,
        output_columns=output_columns,
        business_key_columns=business_key_columns,
        required_non_null=required_non_null,
        expected_months=None,
        window_start=window_start,
        window_end=window_end,
    )
    rows_before = _count_window(cdm_engine, table, window_start, window_end)
    if mode == "skip":
        # Expanding windows (trends / dim_month): skip only when ``window_end`` is
        # already materialized. Counting *any* rows in the window incorrectly freezes
        # coverage after the first report month (e.g. stuck at 2026-01 while later
        # incremental months are skipped).
        end_rows = _count_month(cdm_engine, table, window_end)
        if end_rows > 0:
            return PersistCdmResult(
                mart=mart,
                status="skipped",
                rows_before=rows_before,
                rows_written=0,
                rows_after=rows_before,
                window_start=window_start,
                window_end=window_end,
            )
    with cdm_engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table} WHERE month BETWEEN :start AND :end"),
            {"start": window_start, "end": window_end},
        )
        _insert(conn, table, frame)
    rows_after = _count_window(cdm_engine, table, window_start, window_end)
    return PersistCdmResult(
        mart=mart,
        status="replaced",
        rows_before=rows_before,
        rows_written=len(frame),
        rows_after=rows_after,
        window_start=window_start,
        window_end=window_end,
    )


def _validate_frame(
    dataframe: pd.DataFrame,
    *,
    output_columns: Sequence[str],
    business_key_columns: Sequence[str],
    required_non_null: Sequence[str],
    expected_months: Optional[set[date]],
    window_start: Optional[date] = None,
    window_end: Optional[date] = None,
) -> pd.DataFrame:
    if dataframe is None or not isinstance(dataframe, pd.DataFrame):
        raise CdmError("CDM persistence requires a pandas DataFrame")
    if dataframe.empty:
        raise CdmError("CDM persistence rejected empty DataFrame")
    missing = [c for c in output_columns if c not in dataframe.columns]
    if missing:
        raise CdmError(f"CDM persistence missing required columns: {missing!r}")
    frame = dataframe.loc[:, list(output_columns)].copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    months = set(frame["month"].tolist())
    if expected_months is not None:
        if months != expected_months:
            raise CdmError("CDM DataFrame month set does not match requested month")
    if window_start is not None and window_end is not None:
        if any(m < window_start or m > window_end for m in months):
            raise CdmError("CDM DataFrame contains months outside requested window")
    if frame.duplicated(subset=list(business_key_columns)).any():
        raise CdmError("CDM persistence rejected duplicate business keys")
    for col in required_non_null:
        if frame[col].isna().any():
            raise CdmError(f"CDM persistence rejected nulls in required column {col!r}")
    from analytics_reporting_platform.db.timestamps import normalize_timestamp_column

    frame["cdm_dt"] = normalize_timestamp_column(frame["cdm_dt"])
    return frame


def _count_month(engine: Engine, table: str, month_start: date) -> int:
    with engine.connect() as conn:
        value = conn.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE month = :month"),
            {"month": month_start},
        ).scalar()
    return int(value or 0)


def _count_window(engine: Engine, table: str, start: date, end: date) -> int:
    with engine.connect() as conn:
        value = conn.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE month BETWEEN :start AND :end"),
            {"start": start, "end": end},
        ).scalar()
    return int(value or 0)


def _insert(conn: Connection, table: str, frame: pd.DataFrame) -> None:
    payload = frame.where(pd.notnull(frame), None)
    payload.to_sql(
        table,
        con=conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


def _reject_frozen_baseline(engine: Engine) -> None:
    if getattr(engine.url, "port", None) == _FROZEN_BASELINE_PORT:
        raise CdmError("Refusing CDM persistence against frozen baseline port")


__all__ = [
    "PersistCdmResult",
    "persist_cdm_month",
    "persist_cdm_window",
]
