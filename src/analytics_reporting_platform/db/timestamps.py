"""Platform technical timestamp helpers (load_dt / calc_dt / cdm_dt).

PostgreSQL Monthly columns use ``TIMESTAMP WITHOUT TIME ZONE``. Application
code forms timezone-aware Europe/Moscow instants; persistence stores the
Moscow wall-clock (naive) so values display as MSK without fixed ``+3`` math.

Existing historical rows are not rewritten.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd

PLATFORM_TZ = ZoneInfo("Europe/Moscow")


def platform_now() -> datetime:
    """Timezone-aware 'now' in Europe/Moscow for new RAW/CALC/CDM stamps."""
    return datetime.now(PLATFORM_TZ)


def as_platform_tz(value: datetime) -> datetime:
    """Return an aware Europe/Moscow datetime.

    Naive values are treated as already representing Moscow wall time (attach
    tzinfo without shifting). Aware values are converted via ZoneInfo.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=PLATFORM_TZ)
    return value.astimezone(PLATFORM_TZ)


def for_timestamp_without_time_zone(value: datetime) -> datetime:
    """Normalize a datetime for ``TIMESTAMP WITHOUT TIME ZONE`` storage.

    Aware inputs are converted to Europe/Moscow, then tzinfo is dropped so the
    stored wall clock matches MSK. Naive inputs are left unchanged (fixtures /
    already-normalized values).
    """
    if not isinstance(value, datetime):
        raise TypeError(f"Expected datetime, got {type(value)!r}")
    if value.tzinfo is None:
        return value
    return value.astimezone(PLATFORM_TZ).replace(tzinfo=None)


def normalize_timestamp_column(series: pd.Series) -> pd.Series:
    """Normalize a DataFrame timestamp column for TIMESTAMP WITHOUT TIME ZONE."""

    def _one(raw: Any) -> Any:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return raw
        if isinstance(raw, pd.Timestamp):
            if pd.isna(raw):
                return raw
            return for_timestamp_without_time_zone(raw.to_pydatetime())
        if isinstance(raw, datetime):
            return for_timestamp_without_time_zone(raw)
        ts = pd.Timestamp(raw)
        if pd.isna(ts):
            return ts
        return for_timestamp_without_time_zone(ts.to_pydatetime())

    return series.map(_one)


def coalesce_platform_now(value: Optional[datetime]) -> datetime:
    """Return ``value`` if provided, else ``platform_now()``."""
    return value if value is not None else platform_now()


__all__ = [
    "PLATFORM_TZ",
    "as_platform_tz",
    "coalesce_platform_now",
    "for_timestamp_without_time_zone",
    "normalize_timestamp_column",
    "platform_now",
]
