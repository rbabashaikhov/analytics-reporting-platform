"""Shared RAW-layer models and month helpers."""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Union

MonthInput = Union[date, datetime, str]

_MONTH_START_RE = re.compile(r"^(\d{4})-(\d{2})-01$")


class RawExtractionError(ValueError):
    """Raised when RAW extraction inputs or Adobe responses are invalid."""


def parse_month_start(month: MonthInput) -> date:
    """Parse and validate an explicit month-start date (first day of month)."""
    if isinstance(month, datetime):
        value = month.date()
    elif isinstance(month, date):
        value = month
    elif isinstance(month, str):
        text = month.strip()
        match = _MONTH_START_RE.fullmatch(text)
        if match is None:
            raise RawExtractionError(
                "Invalid month: expected YYYY-MM-01, date, or datetime on the first day"
            )
        value = date(int(match.group(1)), int(match.group(2)), 1)
    else:
        raise RawExtractionError(
            "Invalid month: expected YYYY-MM-01, date, or datetime on the first day"
        )

    if value.day != 1:
        raise RawExtractionError("Invalid month: day must be the first day of the month")
    return value


def month_date_range_bounds(month_start: date) -> tuple[date, date]:
    """Return inclusive calendar month start/end dates (leap-year safe)."""
    if month_start.day != 1:
        raise RawExtractionError("Invalid month: day must be the first day of the month")
    last_day = calendar.monthrange(month_start.year, month_start.month)[1]
    return month_start, date(month_start.year, month_start.month, last_day)


def adobe_month_date_range(month_start: date) -> tuple[str, str]:
    """Build historical Adobe dateRange bounds for a month.

    Format matches the Monthly RAW notebook:
    ``YYYY-MM-DDT00:00:00.000`` .. ``YYYY-MM-DDT23:59:59.999`` (month end).
    """
    start, end = month_date_range_bounds(month_start)
    date_from = f"{start.isoformat()}T00:00:00.000"
    date_to = f"{end.isoformat()}T23:59:59.999"
    return date_from, date_to


@dataclass(frozen=True)
class MonthlyMetricsResult:
    """In-memory Monthly RAW metrics extraction result (not persisted)."""

    month: date
    dataframe: object  # pandas.DataFrame; typed loosely to avoid import cycles in typing
    request_count: int

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


@dataclass(frozen=True)
class MonthlyBySectionResult:
    """In-memory Monthly RAW by-section extraction result (not persisted)."""

    month: date
    dataframe: object
    request_count: int

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


@dataclass(frozen=True)
class MonthlyByUrlResult:
    """In-memory Monthly RAW by-URL extraction result (not persisted).

    Full-private operational mart only — excluded from public Portfolio snapshot.
    """

    month: date
    dataframe: object
    request_count: int

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


__all__ = [
    "MonthInput",
    "RawExtractionError",
    "MonthlyMetricsResult",
    "MonthlyBySectionResult",
    "MonthlyByUrlResult",
    "parse_month_start",
    "month_date_range_bounds",
    "adobe_month_date_range",
]
