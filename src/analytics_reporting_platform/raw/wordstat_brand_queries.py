"""RAW Wordstat Brand Demand queries (month × query × count)."""

from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, Optional, Protocol, Sequence

import pandas as pd

from analytics_reporting_platform.db.timestamps import coalesce_platform_now
from analytics_reporting_platform.raw.models import MonthInput, RawExtractionError, parse_month_start

logger = logging.getLogger(__name__)

REQUIRED_QUERIES: Final[tuple[str, ...]] = ("samsung", "самсунг")
OUTPUT_COLUMNS: Final[tuple[str, ...]] = ("month", "query", "count", "load_dt")
BUSINESS_KEY_COLUMNS: Final[tuple[str, ...]] = ("month", "query")
SOURCE_TABLE: Final = "raw_wordstat_brand_queries"
LOGICAL_MART: Final = "wordstat_brand_queries"


class SupportsWordstatDynamics(Protocol):
    def get_dynamics(
        self,
        phrase: str,
        from_date: date,
        to_date: date,
        *,
        period: str = "MONTHLY",
        regions: Optional[Sequence[int]] = None,
    ) -> object: ...


@dataclass(frozen=True)
class WordstatBrandQueriesResult:
    """In-memory RAW Wordstat brand-query extract for one month."""

    month: date
    dataframe: pd.DataFrame

    @property
    def row_count(self) -> int:
        return int(len(self.dataframe))


def _month_end(month_start: date) -> date:
    last = calendar.monthrange(month_start.year, month_start.month)[1]
    return date(month_start.year, month_start.month, last)


def extract_wordstat_brand_queries_range(
    wordstat_client: SupportsWordstatDynamics,
    from_month: MonthInput,
    to_month: MonthInput,
    *,
    load_dt: Optional[datetime] = None,
    queries: Sequence[str] = REQUIRED_QUERIES,
) -> pd.DataFrame:
    """Fetch Wordstat counts for required queries over an inclusive month range.

    Uses one dynamics API call per query for the whole range (not one call per month).
    Does not persist. Does not store ``share``.
    """
    start = parse_month_start(from_month)
    end = parse_month_start(to_month)
    if end < start:
        raise RawExtractionError("Wordstat range to_month must be on/after from_month")

    stamp = coalesce_platform_now(load_dt)
    rows: list[dict[str, object]] = []
    required = tuple(str(q).strip() for q in queries)
    if not required:
        raise RawExtractionError("Wordstat brand queries list must not be empty")

    from_date = start
    to_date = _month_end(end)
    for query in required:
        logger.info(
            "wordstat_raw_extract query=%r from=%s to=%s",
            query,
            from_date.isoformat(),
            to_date.isoformat(),
        )
        result = wordstat_client.get_dynamics(
            query,
            from_date,
            to_date,
            period="MONTHLY",
        )
        points = getattr(result, "points", ())
        months_seen: set[date] = set()
        for point in points:
            point_month = date(point.date.year, point.date.month, 1)
            if point_month < start or point_month > end:
                continue
            months_seen.add(point_month)
            rows.append(
                {
                    "month": point_month,
                    "query": query,
                    "count": int(point.count),
                    "load_dt": stamp,
                }
            )
        expected_months = _iter_months(start, end)
        missing = [m for m in expected_months if m not in months_seen]
        if missing:
            missing_txt = ",".join(m.strftime("%Y-%m") for m in missing)
            raise RawExtractionError(
                f"Wordstat dynamics missing month(s) for query={query!r}: {missing_txt}"
            )

    frame = pd.DataFrame(rows, columns=list(OUTPUT_COLUMNS))
    if frame.empty:
        raise RawExtractionError("Wordstat brand-query extract returned no rows")
    if frame.duplicated(subset=list(BUSINESS_KEY_COLUMNS)).any():
        raise RawExtractionError("Wordstat brand-query extract has duplicate month×query keys")
    return (
        frame.sort_values(["month", "query"])
        .reset_index(drop=True)
    )


def extract_wordstat_brand_queries(
    wordstat_client: SupportsWordstatDynamics,
    month: MonthInput,
    *,
    load_dt: Optional[datetime] = None,
    queries: Sequence[str] = REQUIRED_QUERIES,
) -> WordstatBrandQueriesResult:
    """Extract required Wordstat brand queries for exactly one month."""
    month_start = parse_month_start(month)
    frame = extract_wordstat_brand_queries_range(
        wordstat_client,
        month_start,
        month_start,
        load_dt=load_dt,
        queries=queries,
    )
    month_frame = frame.loc[frame["month"] == month_start].copy().reset_index(drop=True)
    _validate_required_queries(month_frame, month_start, queries=queries)
    return WordstatBrandQueriesResult(month=month_start, dataframe=month_frame)


def _validate_required_queries(
    frame: pd.DataFrame,
    month_start: date,
    *,
    queries: Sequence[str],
) -> None:
    present = set(frame.loc[frame["month"] == month_start, "query"].astype(str))
    required = set(str(q).strip() for q in queries)
    missing = sorted(required - present)
    if missing:
        raise RawExtractionError(
            "Wordstat brand-query extract missing required query(ies): "
            + ", ".join(missing)
        )
    if (frame["count"] < 0).any():
        raise RawExtractionError("Wordstat brand-query extract has negative count")


def _iter_months(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


__all__ = [
    "BUSINESS_KEY_COLUMNS",
    "LOGICAL_MART",
    "OUTPUT_COLUMNS",
    "REQUIRED_QUERIES",
    "SOURCE_TABLE",
    "WordstatBrandQueriesResult",
    "extract_wordstat_brand_queries",
    "extract_wordstat_brand_queries_range",
]
