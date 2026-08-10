"""Monthly execution period validation."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Iterator, Union

from analytics_reporting_platform.raw.models import MonthInput, parse_month_start

ExecutionMonthInput = Union[MonthInput, str]

_YEAR_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")


class PeriodValidationError(ValueError):
    """Raised when an execution month is malformed or unsupported."""


def current_month_start(today: date | None = None) -> date:
    """Return the first day of the calendar month for ``today`` (default: local today)."""
    value = today if today is not None else date.today()
    return date(value.year, value.month, 1)


def last_completed_month(today: date | None = None) -> date:
    """Return the first day of the last fully completed calendar month.

    On any day in month M, the last completed month is M-1.
    Scheduled Monthly runs use this when no explicit target month is provided.
    """
    current = current_month_start(today)
    if current.month == 1:
        return date(current.year - 1, 12, 1)
    return date(current.year, current.month - 1, 1)


def iter_months(start: ExecutionMonthInput, end: ExecutionMonthInput) -> Iterator[date]:
    """Yield inclusive first-of-month dates from ``start`` through ``end``.

    Used for practical multi-month backfill loops (Airflow trigger / CLI).
    Rejects ranges where ``start`` > ``end``.
    """
    start_m = parse_execution_month(start)
    end_m = parse_execution_month(end)
    if start_m > end_m:
        raise PeriodValidationError(
            f"Invalid month range: start {start_m.isoformat()} is after end {end_m.isoformat()}"
        )
    cursor = start_m
    while cursor <= end_m:
        yield cursor
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


def parse_execution_month(month: ExecutionMonthInput) -> date:
    """Parse CLI/API month input to canonical first-of-month ``date``.

    Accepts:
    - ``date`` / ``datetime`` on day 1
    - ``YYYY-MM-01``
    - ``YYYY-MM`` (normalized to day 1)
    """
    if isinstance(month, str):
        text = month.strip()
        ym = _YEAR_MONTH_RE.fullmatch(text)
        if ym is not None:
            year = int(ym.group(1))
            mon = int(ym.group(2))
            if mon < 1 or mon > 12:
                raise PeriodValidationError(
                    f"Invalid month: month number out of range in {text!r}"
                )
            return date(year, mon, 1)
        try:
            return parse_month_start(text)
        except Exception as exc:
            raise PeriodValidationError(
                "Invalid month: expected YYYY-MM or YYYY-MM-01"
            ) from exc
    try:
        return parse_month_start(month)
    except Exception as exc:
        raise PeriodValidationError(
            "Invalid month: expected YYYY-MM, YYYY-MM-01, or first-of-month date"
        ) from exc


def validate_execution_month(
    month: ExecutionMonthInput,
    *,
    today: date | None = None,
    allow_future: bool = False,
) -> date:
    """Parse and validate an execution month.

    Future months (after the current calendar month-start) are rejected unless
    ``allow_future`` is explicitly True.
    """
    month_start = parse_execution_month(month)
    if not allow_future and month_start > current_month_start(today):
        raise PeriodValidationError(
            f"Unsupported future month: {month_start.isoformat()} "
            f"(current month-start is {current_month_start(today).isoformat()})"
        )
    return month_start


__all__ = [
    "ExecutionMonthInput",
    "PeriodValidationError",
    "current_month_start",
    "iter_months",
    "last_completed_month",
    "parse_execution_month",
    "validate_execution_month",
]
