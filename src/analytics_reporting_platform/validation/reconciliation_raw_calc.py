"""Independent RAW → CALC reconciliation (read-only SQL aggregates).

Does not invoke production builders; recomputes contracted invariants from RAW
and compares to persisted CALC. Integer/count metrics use exact equality
(within float epsilon). Share/rate metrics use an explicit small tolerance.
"""

from __future__ import annotations

from datetime import date
from typing import Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.calc.monthly_channel_mix import MARKETING_CHANNELS
from analytics_reporting_platform.calc.monthly_search_engines import SEARCH_ENGINE_CATEGORIES
from analytics_reporting_platform.calc.monthly_section_share import (
    SECTION_SHARE_SEGMENTS,
    SECTIONS,
)
from analytics_reporting_platform.calc.explore_buying_guides_kpi import (
    EXPLORE_BG_KPI_ORDER,
    EXPLORE_BG_SECTIONS,
)
from analytics_reporting_platform.validation.coverage import month_range
from analytics_reporting_platform.validation.types import SuiteCheck

# Absolute tolerance for float/share comparisons.
FLOAT_TOL: float = 1e-6
# Visits stored as float: treat as exact when abs(diff) <= this.
COUNT_TOL: float = 1e-6


def reconcile_raw_to_calc(
    *,
    raw_engine: Engine,
    calc_engine: Engine,
    calc_start: date,
    calc_end: date,
) -> list[SuiteCheck]:
    """Run all five CALC mart reconciliations for the CALC coverage window."""
    months = month_range(calc_start, calc_end)
    checks: list[SuiteCheck] = []
    checks.extend(_reconcile_monthly_kpi(raw_engine, calc_engine, months))
    checks.extend(_reconcile_channel_mix(raw_engine, calc_engine, months))
    checks.extend(_reconcile_search_engines(raw_engine, calc_engine, months))
    checks.extend(_reconcile_section_share(raw_engine, calc_engine, months))
    checks.extend(_reconcile_explore_bg(raw_engine, calc_engine, months))
    checks.extend(_reconcile_brand_demand(raw_engine, calc_engine, months))
    return checks


def _reconcile_monthly_kpi(
    raw_engine: Engine, calc_engine: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    """calc.metric_value must equal RAW metric_value for same (month, segment, metric)."""
    checks: list[SuiteCheck] = []
    # Cross-database: pull both into temp comparison via two queries + Python.
    raw_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_name, metric_value::float8 AS v
        FROM raw_aa_monthly_metrics
        WHERE month = ANY(:months)
        """
    )
    calc_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_name, metric_value::float8 AS v
        FROM calc_monthly_kpi
        WHERE month = ANY(:months)
        """
    )
    with raw_engine.connect() as conn:
        raw_rows = {
            (r.month, r.segment_name, r.metric_name): r.v
            for r in conn.execute(raw_sql, {"months": list(months)}).fetchall()
        }
    with calc_engine.connect() as conn:
        calc_rows = {
            (r.month, r.segment_name, r.metric_name): r.v
            for r in conn.execute(calc_sql, {"months": list(months)}).fetchall()
        }

    missing_in_raw = [k for k in calc_rows if k not in raw_rows]
    value_mismatches = 0
    for key, cv in calc_rows.items():
        if key not in raw_rows:
            continue
        rv = raw_rows[key]
        if rv is None and cv is None:
            continue
        if rv is None or cv is None:
            value_mismatches += 1
            continue
        if abs(float(rv) - float(cv)) > COUNT_TOL:
            value_mismatches += 1

    # Row-count invariant: every calc key must exist in RAW for that month
    checks.append(
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.monthly_kpi.metric_value_equals_raw",
            status="PASS" if value_mismatches == 0 and not missing_in_raw else "FAIL",
            detail=(
                None
                if value_mismatches == 0 and not missing_in_raw
                else f"value_mismatches={value_mismatches}; calc_keys_missing_in_raw={len(missing_in_raw)}"
            ),
        )
    )

    # MoM abs change: metric_value - prev_month_value when prev present
    mom_sql = text(
        """
        SELECT COUNT(*) FROM calc_monthly_kpi
        WHERE month = ANY(:months)
          AND prev_month_value IS NOT NULL
          AND ABS((metric_value - prev_month_value) - mom_abs_change) > :tol
        """
    )
    with calc_engine.connect() as conn:
        mom_bad = int(conn.execute(mom_sql, {"months": list(months), "tol": FLOAT_TOL}).scalar_one())
    checks.append(
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.monthly_kpi.mom_abs_change_identity",
            status="PASS" if mom_bad == 0 else "FAIL",
            detail=None if mom_bad == 0 else f"mismatches={mom_bad}",
        )
    )
    return checks


def _reconcile_channel_mix(
    raw_engine: Engine, calc_engine: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    """channel_visits = RAW visits for channel segment; share = visits / total."""
    checks: list[SuiteCheck] = []
    channels = list(MARKETING_CHANNELS)
    raw_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_value::float8 AS v
        FROM raw_aa_monthly_metrics
        WHERE month = ANY(:months)
          AND metric_name = 'visits'
          AND segment_name = ANY(:channels)
        """
    )
    total_sql = text(
        """
        SELECT month::date AS month, metric_value::float8 AS total_v
        FROM raw_aa_monthly_metrics
        WHERE month = ANY(:months)
          AND metric_name = 'visits'
          AND segment_name = 'total'
        """
    )
    calc_sql = text(
        """
        SELECT month::date AS month, channel_name,
               channel_visits::float8 AS visits,
               channel_share::float8 AS share
        FROM calc_monthly_channel_mix
        WHERE month = ANY(:months)
        """
    )
    with raw_engine.connect() as conn:
        raw_ch = {
            (r.month, r.segment_name): r.v
            for r in conn.execute(
                raw_sql, {"months": list(months), "channels": channels}
            ).fetchall()
        }
        totals = {
            r.month: r.total_v
            for r in conn.execute(total_sql, {"months": list(months)}).fetchall()
        }
    with calc_engine.connect() as conn:
        calc_rows = list(conn.execute(calc_sql, {"months": list(months)}).fetchall())

    visit_mm = 0
    share_mm = 0
    for r in calc_rows:
        key = (r.month, r.channel_name)
        rv = raw_ch.get(key)
        if rv is None or abs(float(rv) - float(r.visits)) > COUNT_TOL:
            visit_mm += 1
        total = totals.get(r.month)
        if total is None or total == 0:
            if r.share is not None:
                share_mm += 1
        else:
            expected = float(rv) / float(total) if rv is not None else None
            if expected is None or r.share is None or abs(expected - float(r.share)) > FLOAT_TOL:
                share_mm += 1

    # Expected 7 channels per month
    expected_keys = {(m, c) for m in months for c in channels}
    present_keys = {(r.month, r.channel_name) for r in calc_rows}
    missing_keys = expected_keys - present_keys

    checks.append(
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.channel_mix.visits_and_share",
            status="PASS" if visit_mm == 0 and share_mm == 0 and not missing_keys else "FAIL",
            detail=(
                None
                if visit_mm == 0 and share_mm == 0 and not missing_keys
                else f"visit_mismatches={visit_mm}; share_mismatches={share_mm}; "
                f"missing_keys={len(missing_keys)}"
            ),
        )
    )
    return checks


def _reconcile_search_engines(
    raw_engine: Engine, calc_engine: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    """organic_total/yandex/google from RAW; other = clip(organic - yandex - google)."""
    checks: list[SuiteCheck] = []
    raw_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_value::float8 AS v
        FROM raw_aa_monthly_metrics
        WHERE month = ANY(:months)
          AND metric_name = 'visits'
          AND segment_name IN ('organic', 'yandex', 'google')
        """
    )
    calc_sql = text(
        """
        SELECT month::date AS month, search_engine,
               search_engine_visits::float8 AS visits,
               search_engine_share::float8 AS share
        FROM calc_monthly_search_engines
        WHERE month = ANY(:months)
        """
    )
    with raw_engine.connect() as conn:
        raw = {}
        for r in conn.execute(raw_sql, {"months": list(months)}).fetchall():
            raw.setdefault(r.month, {})[r.segment_name] = float(r.v or 0)
    with calc_engine.connect() as conn:
        calc_rows = list(conn.execute(calc_sql, {"months": list(months)}).fetchall())

    visit_mm = 0
    share_mm = 0
    for r in calc_rows:
        src = raw.get(r.month, {})
        organic = float(src.get("organic", 0))
        yandex = float(src.get("yandex", 0))
        google = float(src.get("google", 0))
        other = max(0.0, organic - yandex - google)
        expected_map = {
            "organic_total": organic,
            "yandex": yandex,
            "google": google,
            "other_search_engines": other,
        }
        expected = expected_map.get(str(r.search_engine))
        if expected is None or abs(expected - float(r.visits)) > COUNT_TOL:
            visit_mm += 1
        if organic == 0:
            exp_share = None
        else:
            exp_share = expected / organic if expected is not None else None
        if exp_share is None:
            # When organic==0, share is expected NULL (safe_divide).
            if r.share is not None:
                share_mm += 1
        elif r.share is None or abs(exp_share - float(r.share)) > FLOAT_TOL:
            share_mm += 1

    expected_keys = {(m, e) for m in months for e in SEARCH_ENGINE_CATEGORIES}
    present = {(r.month, r.search_engine) for r in calc_rows}
    missing = expected_keys - present
    checks.append(
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.search_engines.visits_and_share",
            status="PASS" if visit_mm == 0 and share_mm == 0 and not missing else "FAIL",
            detail=(
                None
                if visit_mm == 0 and share_mm == 0 and not missing
                else f"visit_mismatches={visit_mm}; share_mismatches={share_mm}; "
                f"missing_keys={len(missing)}"
            ),
        )
    )
    return checks


def _reconcile_section_share(
    raw_engine: Engine, calc_engine: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    """section_visits from RAW; share = visits / sum(sections) per segment."""
    checks: list[SuiteCheck] = []
    segs = list(SECTION_SHARE_SEGMENTS)
    secs = list(SECTIONS)
    raw_sql = text(
        """
        SELECT month::date AS month, segment_name, section_name, metric_value::float8 AS v
        FROM raw_aa_monthly_by_section
        WHERE month = ANY(:months)
          AND segment_name = ANY(:segs)
          AND section_name = ANY(:secs)
        """
    )
    calc_sql = text(
        """
        SELECT month::date AS month, segment_name, section_name,
               section_visits::float8 AS visits,
               section_share::float8 AS share
        FROM calc_monthly_section_share
        WHERE month = ANY(:months)
        """
    )
    with raw_engine.connect() as conn:
        raw_rows = {
            (r.month, r.segment_name, r.section_name): float(r.v or 0)
            for r in conn.execute(
                raw_sql, {"months": list(months), "segs": segs, "secs": secs}
            ).fetchall()
        }
    # totals per month+segment
    totals: dict[tuple[date, str], float] = {}
    for (m, seg, _sec), v in raw_rows.items():
        totals[(m, seg)] = totals.get((m, seg), 0.0) + v

    with calc_engine.connect() as conn:
        calc_rows = list(conn.execute(calc_sql, {"months": list(months)}).fetchall())

    visit_mm = 0
    share_mm = 0
    for r in calc_rows:
        key = (r.month, r.segment_name, r.section_name)
        rv = raw_rows.get(key)
        if rv is None or abs(rv - float(r.visits)) > COUNT_TOL:
            visit_mm += 1
        tot = totals.get((r.month, r.segment_name), 0.0)
        if tot == 0:
            if r.share is not None:
                share_mm += 1
        else:
            exp = (rv if rv is not None else 0.0) / tot
            if r.share is None or abs(exp - float(r.share)) > FLOAT_TOL:
                share_mm += 1

    expected_keys = {(m, s, sec) for m in months for s in segs for sec in secs}
    present = {(r.month, r.segment_name, r.section_name) for r in calc_rows}
    missing = expected_keys - present
    checks.append(
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.section_share.visits_and_share",
            status="PASS" if visit_mm == 0 and share_mm == 0 and not missing else "FAIL",
            detail=(
                None
                if visit_mm == 0 and share_mm == 0 and not missing
                else f"visit_mismatches={visit_mm}; share_mismatches={share_mm}; "
                f"missing_keys={len(missing)}"
            ),
        )
    )
    return checks


def _reconcile_explore_bg(
    raw_engine: Engine, calc_engine: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    """kpi_value from RAW by-section: total→total_visits, organic→organic_visits."""
    checks: list[SuiteCheck] = []
    raw_sql = text(
        """
        SELECT month::date AS month, section_name, segment_name, metric_value::float8 AS v
        FROM raw_aa_monthly_by_section
        WHERE month = ANY(:months)
          AND section_name = ANY(:sections)
          AND segment_name IN ('total', 'organic')
        """
    )
    calc_sql = text(
        """
        SELECT month::date AS month, section_name, kpi_name, kpi_value::float8 AS v
        FROM calc_explore_buying_guides_kpi
        WHERE month = ANY(:months)
        """
    )
    seg_to_kpi = {"total": "total_visits", "organic": "organic_visits"}
    with raw_engine.connect() as conn:
        raw_map: dict[tuple[date, str, str], float] = {}
        for r in conn.execute(
            raw_sql, {"months": list(months), "sections": list(EXPLORE_BG_SECTIONS)}
        ).fetchall():
            kpi = seg_to_kpi[r.segment_name]
            key = (r.month, r.section_name, kpi)
            raw_map[key] = raw_map.get(key, 0.0) + float(r.v or 0)
    with calc_engine.connect() as conn:
        calc_rows = list(conn.execute(calc_sql, {"months": list(months)}).fetchall())

    mm = 0
    for r in calc_rows:
        key = (r.month, r.section_name, r.kpi_name)
        # Missing RAW → expected 0 (builder fills grid with 0)
        expected = raw_map.get(key, 0.0)
        if r.v is None or abs(expected - float(r.v)) > COUNT_TOL:
            mm += 1

    expected_keys = {
        (m, s, k) for m in months for s in EXPLORE_BG_SECTIONS for k in EXPLORE_BG_KPI_ORDER
    }
    present = {(r.month, r.section_name, r.kpi_name) for r in calc_rows}
    missing = expected_keys - present
    checks.append(
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.explore_buying_guides_kpi.kpi_value",
            status="PASS" if mm == 0 and not missing else "FAIL",
            detail=(
                None
                if mm == 0 and not missing
                else f"value_mismatches={mm}; missing_keys={len(missing)}"
            ),
        )
    )
    return checks


def _reconcile_brand_demand(
    raw_engine: Engine, calc_engine: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    """calc.brand_demand must equal RAW samsung + самсунг counts per month."""
    from analytics_reporting_platform.raw.wordstat_brand_queries import REQUIRED_QUERIES

    checks: list[SuiteCheck] = []
    raw_sql = text(
        """
        SELECT month::date AS month, query, count::bigint AS count
        FROM raw_wordstat_brand_queries
        WHERE month = ANY(:months)
          AND query = ANY(:queries)
        """
    )
    calc_sql = text(
        """
        SELECT month::date AS month, brand_demand::bigint AS brand_demand
        FROM calc_brand_demand
        WHERE month = ANY(:months)
        """
    )
    with raw_engine.connect() as conn:
        raw_rows = list(
            conn.execute(
                raw_sql,
                {"months": list(months), "queries": list(REQUIRED_QUERIES)},
            ).fetchall()
        )
    with calc_engine.connect() as conn:
        calc_rows = {
            r.month: int(r.brand_demand)
            for r in conn.execute(calc_sql, {"months": list(months)}).fetchall()
        }

    by_month: dict[date, dict[str, int]] = {m: {} for m in months}
    for r in raw_rows:
        by_month.setdefault(r.month, {})[str(r.query)] = int(r.count)

    missing_queries = 0
    value_mismatches = 0
    missing_calc = 0
    for month in months:
        qmap = by_month.get(month, {})
        for query in REQUIRED_QUERIES:
            if query not in qmap:
                missing_queries += 1
        expected = sum(int(qmap.get(q, 0)) for q in REQUIRED_QUERIES)
        if month not in calc_rows:
            missing_calc += 1
            continue
        if abs(float(calc_rows[month]) - float(expected)) > COUNT_TOL:
            value_mismatches += 1

    ok = missing_queries == 0 and value_mismatches == 0 and missing_calc == 0
    checks.append(
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.brand_demand.sum_queries",
            status="PASS" if ok else "FAIL",
            detail=(
                None
                if ok
                else (
                    f"missing_queries={missing_queries}; "
                    f"value_mismatches={value_mismatches}; "
                    f"missing_calc_months={missing_calc}"
                )
            ),
        )
    )
    return checks


__all__ = ["FLOAT_TOL", "COUNT_TOL", "reconcile_raw_to_calc"]
