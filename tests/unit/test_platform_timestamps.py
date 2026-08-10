"""Timezone semantics for Monthly technical stamps (load_dt / calc_dt / cdm_dt)."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from analytics_reporting_platform.db.timestamps import (
    PLATFORM_TZ,
    as_platform_tz,
    for_timestamp_without_time_zone,
    normalize_timestamp_column,
    platform_now,
)


def test_platform_now_is_europe_moscow_aware() -> None:
    now = platform_now()
    assert now.tzinfo is not None
    assert now.tzinfo == PLATFORM_TZ
    assert now.utcoffset() == datetime.now(PLATFORM_TZ).utcoffset()


def test_fixed_utc_instant_stores_as_moscow_wall_clock() -> None:
    """UTC noon on 2026-08-10 must persist as 15:00 MSK wall clock (no +3 hack)."""
    utc_instant = datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = for_timestamp_without_time_zone(utc_instant)
    assert stored == datetime(2026, 8, 10, 15, 0, 0)
    assert stored.tzinfo is None

    interpreted = as_platform_tz(stored)
    assert interpreted.tzinfo == PLATFORM_TZ
    assert interpreted.isoformat() == "2026-08-10T15:00:00+03:00"
    assert interpreted.astimezone(timezone.utc) == utc_instant


def test_normalize_timestamp_column_converts_aware_utc() -> None:
    series = pd.Series([datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)])
    out = normalize_timestamp_column(series)
    assert list(out) == [datetime(2026, 8, 10, 15, 0, 0)]


def test_naive_fixture_timestamps_unchanged() -> None:
    """Explicit naive fixtures (historical test style) stay as wall-clock values."""
    naive = datetime(2026, 8, 1, 10, 30, 0)
    assert for_timestamp_without_time_zone(naive) == naive
