"""CDM layer shared helpers, period config, and typed result shells."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Sequence

import pandas as pd

from analytics_reporting_platform.raw.models import (
    MonthInput,
    RawExtractionError,
    parse_month_start,
)

CdmError = RawExtractionError


@dataclass(frozen=True)
class CdmPeriodConfig:
    """Historical Monthly CDM report vs trend windows."""

    report_start: date
    report_end: date
    trend_start: date

    def __post_init__(self) -> None:
        if self.report_start > self.report_end:
            raise CdmError("CDM report_start must be <= report_end")
        if self.trend_start > self.report_end:
            raise CdmError("CDM trend_start must be <= report_end")


# Historical notebook defaults for the July 2026 sandbox rebuild.
DEFAULT_REPORT_START: date = date(2026, 1, 1)
DEFAULT_TREND_START: date = date(2025, 1, 1)


def default_period_config(report_end: MonthInput) -> CdmPeriodConfig:
    """Build period config with historical start defaults and explicit end month.

    When ``report_end`` is before the notebook defaults (e.g. chronological
    backfill of 2025), clamp ``report_start`` / ``trend_start`` so the window
    remains valid (``start <= end``). Month-scoped CDM builders still target
    the explicit month; trend builders use the clamped trend window.
    """
    end = parse_month_start(report_end)
    return CdmPeriodConfig(
        report_start=min(DEFAULT_REPORT_START, end),
        report_end=end,
        trend_start=min(DEFAULT_TREND_START, end),
    )


def prev_month_start(month: date) -> date:
    if month.month == 1:
        return date(month.year - 1, 12, 1)
    return date(month.year, month.month - 1, 1)


def month_type_label(month: date, report_end: date) -> str:
    """Historical ``month_type`` helper relative to report end."""
    if month == report_end:
        return "last_month"
    if month == prev_month_start(report_end):
        return "previous_month"
    return "other"


def add_display_order(
    df: pd.DataFrame,
    column: str,
    order_list: Sequence[str],
) -> pd.DataFrame:
    """Map categorical values to 1-based historical display_order."""
    order_map = {value: i + 1 for i, value in enumerate(order_list)}
    out = df.copy()
    out["display_order"] = out[column].map(order_map)
    return out


def safe_divide(numerator: object, denominator: object) -> Optional[float]:
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return None
    if numerator is None or pd.isna(numerator):
        return None
    return float(numerator) / float(denominator)


@dataclass(frozen=True)
class CdmMartResult:
    """Generic in-memory CDM result (dataframe + optional month/window)."""

    mart: str
    dataframe: object
    month: Optional[date] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))  # type: ignore[arg-type]


__all__ = [
    "CdmError",
    "CdmMartResult",
    "CdmPeriodConfig",
    "DEFAULT_REPORT_START",
    "DEFAULT_TREND_START",
    "MonthInput",
    "add_display_order",
    "default_period_config",
    "month_type_label",
    "parse_month_start",
    "prev_month_start",
    "safe_divide",
]
