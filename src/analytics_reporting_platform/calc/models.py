"""CALC layer shared helpers and result types."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Union

from analytics_reporting_platform.raw.models import (
    MonthInput,
    RawExtractionError,
    parse_month_start,
)

CalcError = RawExtractionError  # shared typed failure base for layer I/O


@dataclass(frozen=True)
class MonthlyKpiResult:
    """In-memory ``calc_monthly_kpi`` result (may be persisted separately)."""

    month: date
    dataframe: object

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


@dataclass(frozen=True)
class MonthlyChannelMixResult:
    """In-memory ``calc_monthly_channel_mix`` result (may be persisted separately)."""

    month: date
    dataframe: object

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


@dataclass(frozen=True)
class MonthlySearchEnginesResult:
    """In-memory ``calc_monthly_search_engines`` result (may be persisted separately)."""

    month: date
    dataframe: object

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


@dataclass(frozen=True)
class MonthlySectionShareResult:
    """In-memory ``calc_monthly_section_share`` result (may be persisted separately)."""

    month: date
    dataframe: object

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


@dataclass(frozen=True)
class ExploreBuyingGuidesKpiResult:
    """In-memory ``calc_explore_buying_guides_kpi`` result (may be persisted separately)."""

    month: date
    dataframe: object

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


def prev_month_start(month: date) -> date:
    """Return the first day of the previous calendar month."""
    if month.month == 1:
        return date(month.year - 1, 12, 1)
    return date(month.year, month.month - 1, 1)


def prev_year_month_start(month: date) -> date:
    """Return the same month-start one year earlier."""
    return date(month.year - 1, month.month, 1)


__all__ = [
    "CalcError",
    "MonthInput",
    "ExploreBuyingGuidesKpiResult",
    "MonthlyChannelMixResult",
    "MonthlyKpiResult",
    "MonthlySearchEnginesResult",
    "MonthlySectionShareResult",
    "parse_month_start",
    "prev_month_start",
    "prev_year_month_start",
]
