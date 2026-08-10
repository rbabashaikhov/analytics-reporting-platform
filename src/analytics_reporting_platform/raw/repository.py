"""PostgreSQL persistence for full-private Monthly RAW marts.

Extraction remains separate: normalized DataFrame → validate → persist.
Supports only controlled logical marts mapped to fixed physical tables.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final, Literal, Mapping, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import Connection

from analytics_reporting_platform.raw.models import MonthInput, RawExtractionError, parse_month_start

RawMart = Literal["monthly_metrics", "monthly_by_section", "monthly_by_url"]
PersistMode = Literal["skip", "replace"]
PersistStatus = Literal["inserted", "replaced", "skipped"]

_FROZEN_BASELINE_PORT: Final = 15432


@dataclass(frozen=True)
class MartSpec:
    """Fixed physical mapping and schema rules for one RAW mart."""

    logical: RawMart
    table_name: str
    columns: tuple[str, ...]
    business_key: tuple[str, ...]
    non_null_columns: tuple[str, ...]


MART_SPECS: Mapping[RawMart, MartSpec] = {
    "monthly_metrics": MartSpec(
        logical="monthly_metrics",
        table_name="raw_aa_monthly_metrics",
        columns=(
            "month",
            "metric_name",
            "metric_value",
            "segment_name",
            "segment_id",
            "load_dt",
        ),
        business_key=("month", "metric_name", "segment_name"),
        non_null_columns=("month", "metric_name", "segment_name", "load_dt"),
    ),
    "monthly_by_section": MartSpec(
        logical="monthly_by_section",
        table_name="raw_aa_monthly_by_section",
        columns=(
            "month",
            "segment_name",
            "segment_id",
            "metric_value",
            "section_name",
            "section_id",
            "load_dt",
        ),
        business_key=("month", "segment_name", "section_name"),
        non_null_columns=(
            "month",
            "segment_name",
            "section_name",
            "section_id",
            "load_dt",
        ),
    ),
    "monthly_by_url": MartSpec(
        logical="monthly_by_url",
        table_name="raw_aa_monthly_by_url",
        columns=("month", "page_url", "visits", "load_dt"),
        business_key=("month", "page_url"),
        non_null_columns=("month", "page_url", "load_dt"),
    ),
}


@dataclass(frozen=True)
class PersistMonthResult:
    """Structured persistence outcome (no row contents)."""

    mart: RawMart
    month: date
    mode: PersistMode
    status: PersistStatus
    rows_before: int
    rows_written: int
    rows_after: int

    def __repr__(self) -> str:
        return (
            "PersistMonthResult("
            f"mart={self.mart!r}, "
            f"month={self.month.isoformat()!r}, "
            f"mode={self.mode!r}, "
            f"status={self.status!r}, "
            f"rows_before={self.rows_before}, "
            f"rows_written={self.rows_written}, "
            f"rows_after={self.rows_after})"
        )

    __str__ = __repr__


class RawPersistenceError(RawExtractionError):
    """Raised when RAW persistence validation or write fails."""


def resolve_mart_spec(mart: str) -> MartSpec:
    """Resolve a controlled logical mart name to its fixed table mapping."""
    key = str(mart).strip()
    if key not in MART_SPECS:
        allowed = ", ".join(sorted(MART_SPECS))
        raise RawPersistenceError(
            f"Unsupported RAW mart: {mart!r}. Expected one of: {allowed}"
        )
    return MART_SPECS[key]  # type: ignore[index]


def month_exists(engine: Engine, mart: RawMart | str, month: MonthInput) -> bool:
    """Return True if any row exists for the mart/month."""
    spec = resolve_mart_spec(str(mart))
    month_start = parse_month_start(month)
    _reject_frozen_baseline(engine)
    sql = text(f"SELECT 1 FROM {spec.table_name} WHERE month = :month LIMIT 1")
    with engine.connect() as conn:
        row = conn.execute(sql, {"month": month_start}).fetchone()
    return row is not None


def count_month_rows(engine: Engine, mart: RawMart | str, month: MonthInput) -> int:
    """Count rows for one mart/month."""
    spec = resolve_mart_spec(str(mart))
    month_start = parse_month_start(month)
    _reject_frozen_baseline(engine)
    sql = text(f"SELECT COUNT(*) FROM {spec.table_name} WHERE month = :month")
    with engine.connect() as conn:
        value = conn.execute(sql, {"month": month_start}).scalar()
    return int(value or 0)


def validate_persist_dataframe(
    mart: RawMart | str,
    dataframe: pd.DataFrame,
    month: MonthInput,
) -> tuple[MartSpec, date, pd.DataFrame]:
    """Validate a DataFrame for persistence; return (spec, month, normalized copy)."""
    spec = resolve_mart_spec(str(mart))
    month_start = parse_month_start(month)

    if dataframe is None or not isinstance(dataframe, pd.DataFrame):
        raise RawPersistenceError("Persistence requires a pandas DataFrame")
    if dataframe.empty:
        raise RawPersistenceError("Persistence rejected empty DataFrame")

    missing = [col for col in spec.columns if col not in dataframe.columns]
    if missing:
        raise RawPersistenceError(
            f"Persistence rejected DataFrame missing required columns: {missing!r}"
        )

    frame = dataframe.loc[:, list(spec.columns)].copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.date

    unique_months = sorted({m for m in frame["month"].tolist()})
    if len(unique_months) != 1:
        raise RawPersistenceError(
            "Persistence requires exactly one month in the DataFrame"
        )
    if unique_months[0] != month_start:
        raise RawPersistenceError(
            "Persistence DataFrame month does not match requested month"
        )

    if frame.duplicated(subset=list(spec.business_key)).any():
        raise RawPersistenceError(
            f"Persistence rejected duplicate business keys for mart {spec.logical!r}"
        )

    for col in spec.non_null_columns:
        if frame[col].isna().any():
            raise RawPersistenceError(
                f"Persistence rejected nulls in required column {col!r}"
            )

    if "metric_value" in frame.columns:
        frame["metric_value"] = pd.to_numeric(frame["metric_value"], errors="coerce")
    if "visits" in frame.columns:
        frame["visits"] = pd.to_numeric(frame["visits"], errors="coerce")
        # Preserve nullable integer semantics when possible.
        try:
            frame["visits"] = frame["visits"].astype("Int64")
        except (TypeError, ValueError):
            pass

    from analytics_reporting_platform.db.timestamps import normalize_timestamp_column

    frame["load_dt"] = normalize_timestamp_column(frame["load_dt"])
    return spec, month_start, frame


def persist_month(
    engine: Engine,
    mart: RawMart | str,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: PersistMode = "skip",
) -> PersistMonthResult:
    """Persist one mart for one month.

    Modes
    -----
    skip:
        If the month already exists, do nothing (status=skipped).
        If absent, insert (status=inserted).
    replace:
        Delete the exact month, then insert the full replacement dataset
        in one transaction (status=replaced).
    """
    if mode not in ("skip", "replace"):
        raise RawPersistenceError(f"Unsupported persistence mode: {mode!r}")

    _reject_frozen_baseline(engine)
    spec, month_start, frame = validate_persist_dataframe(mart, dataframe, month)

    rows_before = _count_month(engine, spec.table_name, month_start)

    if mode == "skip" and rows_before > 0:
        return PersistMonthResult(
            mart=spec.logical,
            month=month_start,
            mode=mode,
            status="skipped",
            rows_before=rows_before,
            rows_written=0,
            rows_after=rows_before,
        )

    if mode == "skip":
        _insert_month(engine, spec.table_name, frame)
        rows_after = _count_month(engine, spec.table_name, month_start)
        return PersistMonthResult(
            mart=spec.logical,
            month=month_start,
            mode=mode,
            status="inserted",
            rows_before=rows_before,
            rows_written=len(frame),
            rows_after=rows_after,
        )

    # replace — atomic delete + insert
    with engine.begin() as conn:
        _delete_month(conn, spec.table_name, month_start)
        _insert_month_conn(conn, spec.table_name, frame)

    rows_after = _count_month(engine, spec.table_name, month_start)
    return PersistMonthResult(
        mart=spec.logical,
        month=month_start,
        mode=mode,
        status="replaced",
        rows_before=rows_before,
        rows_written=len(frame),
        rows_after=rows_after,
    )


def _reject_frozen_baseline(engine: Engine) -> None:
    port = getattr(engine.url, "port", None)
    if port == _FROZEN_BASELINE_PORT:
        raise RawPersistenceError(
            "Refusing RAW persistence against frozen baseline port"
        )


def _count_month(engine: Engine, table_name: str, month_start: date) -> int:
    sql = text(f"SELECT COUNT(*) FROM {table_name} WHERE month = :month")
    with engine.connect() as conn:
        value = conn.execute(sql, {"month": month_start}).scalar()
    return int(value or 0)


def _delete_month(conn: Connection, table_name: str, month_start: date) -> None:
    # Table name is from fixed MART_SPECS only; month is bound.
    sql = text(f"DELETE FROM {table_name} WHERE month = :month")
    conn.execute(sql, {"month": month_start})


def _insert_month(engine: Engine, table_name: str, frame: pd.DataFrame) -> None:
    with engine.begin() as conn:
        _insert_month_conn(conn, table_name, frame)


def _insert_month_conn(conn: Connection, table_name: str, frame: pd.DataFrame) -> None:
    # Replace NaN with None for DB nulls (historical notebook behavior).
    payload = frame.where(pd.notnull(frame), None)
    payload.to_sql(
        table_name,
        con=conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


__all__ = [
    "MART_SPECS",
    "MartSpec",
    "PersistMode",
    "PersistMonthResult",
    "PersistStatus",
    "RawMart",
    "RawPersistenceError",
    "count_month_rows",
    "month_exists",
    "persist_month",
    "resolve_mart_spec",
    "validate_persist_dataframe",
]
