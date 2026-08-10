"""PostgreSQL persistence for full-private CALC marts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final, Literal, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import Connection

from analytics_reporting_platform.calc.models import CalcError, MonthInput, parse_month_start
from analytics_reporting_platform.calc import explore_buying_guides_kpi as explore_bg_mod
from analytics_reporting_platform.calc import monthly_channel_mix as channel_mix_mod
from analytics_reporting_platform.calc import monthly_kpi as kpi_mod
from analytics_reporting_platform.calc import monthly_search_engines as search_engines_mod
from analytics_reporting_platform.calc import monthly_section_share as section_share_mod

_FROZEN_BASELINE_PORT: Final = 15432
CalcPersistStatus = Literal["replaced", "skipped"]
CalcPersistMode = Literal["replace", "skip"]


@dataclass(frozen=True)
class PersistCalcMonthResult:
    """Structured CALC persistence outcome (no row contents)."""

    mart: str
    month: date
    status: CalcPersistStatus
    rows_before: int
    rows_written: int
    rows_after: int


def persist_monthly_kpi(
    calc_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: CalcPersistMode = "replace",
) -> PersistCalcMonthResult:
    """Replace ``calc_monthly_kpi`` for exactly one month (historical rebuild semantics)."""
    return _persist_month(
        calc_engine,
        dataframe,
        month,
        mart="monthly_kpi",
        table="calc_monthly_kpi",
        output_columns=kpi_mod.OUTPUT_COLUMNS,
        business_key_columns=kpi_mod.BUSINESS_KEY_COLUMNS,
        required_non_null=(
            "month",
            "segment_name",
            "metric_name",
            "change_type",
            "value_format",
            "calc_dt",
        ),
        mode=mode,
    )


def persist_monthly_channel_mix(
    calc_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: CalcPersistMode = "replace",
) -> PersistCalcMonthResult:
    """Replace ``calc_monthly_channel_mix`` for exactly one month."""
    return _persist_month(
        calc_engine,
        dataframe,
        month,
        mart="monthly_channel_mix",
        table="calc_monthly_channel_mix",
        output_columns=channel_mix_mod.OUTPUT_COLUMNS,
        business_key_columns=channel_mix_mod.BUSINESS_KEY_COLUMNS,
        required_non_null=("month", "channel_name", "calc_dt"),
        mode=mode,
    )


def persist_monthly_search_engines(
    calc_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: CalcPersistMode = "replace",
) -> PersistCalcMonthResult:
    """Persist ``calc_monthly_search_engines`` for exactly one month.

    ``mode="replace"`` deletes the target month then inserts.
    ``mode="skip"`` is a no-op when the month already has rows.
    """
    return _persist_month(
        calc_engine,
        dataframe,
        month,
        mart="monthly_search_engines",
        table="calc_monthly_search_engines",
        output_columns=search_engines_mod.OUTPUT_COLUMNS,
        business_key_columns=search_engines_mod.BUSINESS_KEY_COLUMNS,
        required_non_null=("month", "search_engine", "calc_dt"),
        mode=mode,
    )


def persist_monthly_section_share(
    calc_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: CalcPersistMode = "replace",
) -> PersistCalcMonthResult:
    """Persist ``calc_monthly_section_share`` for exactly one month.

    ``mode="replace"`` deletes the target month then inserts.
    ``mode="skip"`` is a no-op when the month already has rows.
    """
    return _persist_month(
        calc_engine,
        dataframe,
        month,
        mart="monthly_section_share",
        table="calc_monthly_section_share",
        output_columns=section_share_mod.OUTPUT_COLUMNS,
        business_key_columns=section_share_mod.BUSINESS_KEY_COLUMNS,
        required_non_null=("month", "segment_name", "section_name", "calc_dt"),
        mode=mode,
    )


def persist_explore_buying_guides_kpi(
    calc_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mode: CalcPersistMode = "replace",
) -> PersistCalcMonthResult:
    """Persist ``calc_explore_buying_guides_kpi`` for exactly one month.

    ``mode="replace"`` deletes the target month then inserts.
    ``mode="skip"`` is a no-op when the month already has rows.
    """
    return _persist_month(
        calc_engine,
        dataframe,
        month,
        mart="explore_buying_guides_kpi",
        table="calc_explore_buying_guides_kpi",
        output_columns=explore_bg_mod.OUTPUT_COLUMNS,
        business_key_columns=explore_bg_mod.BUSINESS_KEY_COLUMNS,
        required_non_null=("month", "section_name", "kpi_name", "display_order", "calc_dt"),
        mode=mode,
    )


def _persist_month(
    calc_engine: Engine,
    dataframe: pd.DataFrame,
    month: MonthInput,
    *,
    mart: str,
    table: str,
    output_columns: Sequence[str],
    business_key_columns: Sequence[str],
    required_non_null: Sequence[str],
    mode: CalcPersistMode = "replace",
) -> PersistCalcMonthResult:
    _reject_frozen_baseline(calc_engine)
    if mode not in ("replace", "skip"):
        raise CalcError(f"Unsupported CALC persist mode: {mode!r}")
    month_start = parse_month_start(month)
    frame = _validate_frame(
        dataframe,
        month_start,
        output_columns=output_columns,
        business_key_columns=business_key_columns,
        required_non_null=required_non_null,
    )

    rows_before = _count_month(calc_engine, table, month_start)
    if mode == "skip" and rows_before > 0:
        return PersistCalcMonthResult(
            mart=mart,
            month=month_start,
            status="skipped",
            rows_before=rows_before,
            rows_written=0,
            rows_after=rows_before,
        )

    with calc_engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table} WHERE month = :month"), {"month": month_start})
        _insert(conn, table, frame)
    rows_after = _count_month(calc_engine, table, month_start)

    return PersistCalcMonthResult(
        mart=mart,
        month=month_start,
        status="replaced",
        rows_before=rows_before,
        rows_written=len(frame),
        rows_after=rows_after,
    )


def _validate_frame(
    dataframe: pd.DataFrame,
    month_start: date,
    *,
    output_columns: Sequence[str],
    business_key_columns: Sequence[str],
    required_non_null: Sequence[str],
) -> pd.DataFrame:
    if dataframe is None or not isinstance(dataframe, pd.DataFrame):
        raise CalcError("CALC persistence requires a pandas DataFrame")
    if dataframe.empty:
        raise CalcError("CALC persistence rejected empty DataFrame")

    missing = [c for c in output_columns if c not in dataframe.columns]
    if missing:
        raise CalcError(f"CALC persistence missing required columns: {missing!r}")

    frame = dataframe.loc[:, list(output_columns)].copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.date
    months = sorted({m for m in frame["month"].tolist()})
    if len(months) != 1:
        raise CalcError("CALC persistence requires exactly one month in the DataFrame")
    if months[0] != month_start:
        raise CalcError("CALC DataFrame month does not match requested month")
    if frame.duplicated(subset=list(business_key_columns)).any():
        raise CalcError("CALC persistence rejected duplicate business keys")
    for col in required_non_null:
        if frame[col].isna().any():
            raise CalcError(f"CALC persistence rejected nulls in required column {col!r}")
    from analytics_reporting_platform.db.timestamps import normalize_timestamp_column

    frame["calc_dt"] = normalize_timestamp_column(frame["calc_dt"])
    return frame


def _count_month(engine: Engine, table: str, month_start: date) -> int:
    with engine.connect() as conn:
        value = conn.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE month = :month"),
            {"month": month_start},
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
        raise CalcError("Refusing CALC persistence against frozen baseline port")


__all__ = [
    "PersistCalcMonthResult",
    "persist_explore_buying_guides_kpi",
    "persist_monthly_channel_mix",
    "persist_monthly_kpi",
    "persist_monthly_search_engines",
    "persist_monthly_section_share",
]
