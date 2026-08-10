"""Independent CALC/RAW → CDM reconciliation (read-only).

Validates that CDM values are reproducible from contracted sources without
rewriting production transforms. Sensitive dimensions (page_url) are never
logged — only mismatch counts / statuses.
"""

from __future__ import annotations

from datetime import date
from typing import Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.cdm.models import DEFAULT_TREND_START, month_type_label
from analytics_reporting_platform.cdm.traffic import TRAFFIC_KPI_ORDER
from analytics_reporting_platform.cdm.organic_search import ORGANIC_KPI_ORDER, SEARCH_ENGINE_ORDER
from analytics_reporting_platform.cdm.explore_buying_guides import EXPLORE_BG_SECTIONS
from analytics_reporting_platform.cdm.geo import SECTION_LIST
from analytics_reporting_platform.cdm.sections import DEVICE_SEGMENTS
from analytics_reporting_platform.calc.monthly_channel_mix import MARKETING_CHANNELS
from analytics_reporting_platform.validation.coverage import month_range
from analytics_reporting_platform.validation.reconciliation_raw_calc import COUNT_TOL, FLOAT_TOL
from analytics_reporting_platform.validation.types import SuiteCheck


def reconcile_calc_to_cdm(
    *,
    raw_engine: Engine,
    calc_engine: Engine,
    cdm_engine: Engine,
    cdm_start: date,
    cdm_end: date,
    trend_start: date = DEFAULT_TREND_START,
) -> list[SuiteCheck]:
    """Reconcile all 15 CDM marts against CALC and/or RAW sources."""
    months = month_range(cdm_start, cdm_end)
    trend_months = month_range(trend_start, cdm_end)
    checks: list[SuiteCheck] = []
    checks.extend(_cdm_traffic_kpi(calc_engine, cdm_engine, months))
    checks.extend(_cdm_traffic_source_mix(calc_engine, cdm_engine, months, cdm_end))
    checks.extend(_cdm_overall_traffic_trend(raw_engine, cdm_engine, trend_months))
    checks.extend(_cdm_geo_kpi(calc_engine, cdm_engine, months))
    checks.extend(_cdm_geo_trend(raw_engine, cdm_engine, trend_months))
    checks.extend(_cdm_geo_by_section(calc_engine, cdm_engine, months))
    checks.extend(_cdm_organic_kpi(calc_engine, cdm_engine, months))
    checks.extend(_cdm_organic_engine_mix(calc_engine, cdm_engine, months, cdm_end))
    checks.extend(_cdm_organic_engine_trend(raw_engine, cdm_engine, trend_months))
    checks.extend(_cdm_explore_kpi(calc_engine, cdm_engine, months))
    checks.extend(_cdm_explore_trend(raw_engine, cdm_engine, trend_months))
    checks.extend(_cdm_sections_mix(calc_engine, cdm_engine, months, cdm_end))
    checks.extend(_cdm_device_section_mix(calc_engine, cdm_engine, months))
    checks.extend(_cdm_top_pages(raw_engine, cdm_engine, months))
    checks.extend(_cdm_dim_month(cdm_engine, trend_months))
    return checks


def _cmp_maps(
    left: dict,
    right: dict,
    *,
    tol: float,
) -> tuple[int, int, int]:
    """Return (mismatches, missing_in_right, missing_in_left)."""
    mm = 0
    for k, lv in left.items():
        if k not in right:
            continue
        rv = right[k]
        if lv is None and rv is None:
            continue
        if lv is None or rv is None:
            mm += 1
            continue
        if abs(float(lv) - float(rv)) > tol:
            mm += 1
    missing_right = len([k for k in left if k not in right])
    missing_left = len([k for k in right if k not in left])
    return mm, missing_right, missing_left


def _cdm_traffic_kpi(calc: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    mapping = {
        ("total", "visits"): "total_visits",
        ("organic", "visits"): "organic_visits",
        ("total", "avg_time"): "average_time_on_site",
        ("total", "bounce_rate"): "bounce_rate",
    }
    calc_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_name,
               metric_value::float8 AS v,
               mom_pct_change::float8 AS mom_pct,
               yoy_pct_change::float8 AS yoy_pct,
               mom_pp_change::float8 AS mom_pp,
               yoy_pp_change::float8 AS yoy_pp
        FROM calc_monthly_kpi
        WHERE month = ANY(:months)
          AND ((segment_name = 'total' AND metric_name IN ('visits', 'avg_time', 'bounce_rate'))
            OR (segment_name = 'organic' AND metric_name = 'visits'))
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, kpi_name, kpi_value::float8 AS v,
               mom_pct_change::float8 AS mom_pct, yoy_pct_change::float8 AS yoy_pct,
               mom_pp_change::float8 AS mom_pp, yoy_pp_change::float8 AS yoy_pp
        FROM cdm_traffic_kpi
        WHERE month = ANY(:months)
        """
    )
    with calc.connect() as conn:
        calc_rows = list(conn.execute(calc_sql, {"months": list(months)}).fetchall())
    with cdm.connect() as conn:
        cdm_rows = {
            (r.month, r.kpi_name): r
            for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }

    mm = 0
    missing = 0
    for r in calc_rows:
        kpi = mapping.get((r.segment_name, r.metric_name))
        if kpi is None:
            continue
        key = (r.month, kpi)
        if key not in cdm_rows:
            missing += 1
            continue
        c = cdm_rows[key]
        for a, b, tol in (
            (r.v, c.v, COUNT_TOL),
            (r.mom_pct, c.mom_pct, FLOAT_TOL),
            (r.yoy_pct, c.yoy_pct, FLOAT_TOL),
            (r.mom_pp, c.mom_pp, FLOAT_TOL),
            (r.yoy_pp, c.yoy_pp, FLOAT_TOL),
        ):
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > tol:
                mm += 1
                break

    expected = {(m, k) for m in months for k in TRAFFIC_KPI_ORDER}
    present = set(cdm_rows)
    key_missing = expected - present
    ok = mm == 0 and missing == 0 and not key_missing
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.traffic_kpi",
            status="PASS" if ok else "FAIL",
            detail=None
            if ok
            else f"value_mismatches={mm}; source_missing={missing}; key_missing={len(key_missing)}",
        )
    ]


def _cdm_traffic_source_mix(
    calc: Engine, cdm: Engine, months: Sequence[date], report_end: date
) -> list[SuiteCheck]:
    calc_sql = text(
        """
        SELECT month::date AS month, channel_name,
               channel_visits::float8 AS visits, channel_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM calc_monthly_channel_mix WHERE month = ANY(:months)
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, channel_name, month_type,
               channel_visits::float8 AS visits, channel_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM cdm_traffic_source_mix WHERE month = ANY(:months)
        """
    )
    with calc.connect() as conn:
        calc_rows = {
            (r.month, r.channel_name): r
            for r in conn.execute(calc_sql, {"months": list(months)}).fetchall()
        }
    with cdm.connect() as conn:
        cdm_rows = {
            (r.month, r.channel_name): r
            for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    type_mm = 0
    type_warn = 0
    for key, c in calc_rows.items():
        if key not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[key]
        for a, b in ((c.visits, d.visits), (c.share, d.share), (c.mom_pp, d.mom_pp)):
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > FLOAT_TOL:
                mm += 1
                break
        expected_type = month_type_label(key[0], report_end)
        if d.month_type != expected_type:
            # Incremental builds label each month relative to itself (always last_month).
            # Critical only for the final report_end month.
            if key[0] == report_end:
                type_mm += 1
            else:
                type_warn += 1
    expected = {(m, ch) for m in months for ch in MARKETING_CHANNELS}
    missing = expected - set(cdm_rows)
    ok = mm == 0 and type_mm == 0 and not missing
    checks = [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.traffic_source_mix",
            status="PASS" if ok else "FAIL",
            detail=None
            if ok
            else f"value_mismatches={mm}; month_type_mismatches_report_end={type_mm}; missing={len(missing)}",
        )
    ]
    if type_warn:
        checks.append(
            SuiteCheck(
                category="calc_cdm_reconciliation",
                name="recon.calc_cdm.traffic_source_mix.month_type_historical",
                status="WARN",
                critical=False,
                detail=(
                    f"historical_months_not_relabeled_to_final_report_end={type_warn} "
                    "(expected under incremental month-as-report_end builds)"
                ),
            )
        )
    return checks


def _cdm_overall_traffic_trend(
    raw: Engine, cdm: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    raw_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_value::float8 AS v
        FROM raw_aa_monthly_metrics
        WHERE month = ANY(:months) AND metric_name = 'visits'
          AND segment_name IN ('organic','own_group','paid_group','other_group','total')
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month,
               organic_visits::float8 AS organic,
               own_group_visits::float8 AS own_group,
               paid_group_visits::float8 AS paid_group,
               other_group_visits::float8 AS other_group,
               total_visits::float8 AS total
        FROM cdm_overall_traffic_trend WHERE month = ANY(:months)
        """
    )
    with raw.connect() as conn:
        raw_map: dict[date, dict[str, float]] = {}
        for r in conn.execute(raw_sql, {"months": list(months)}).fetchall():
            raw_map.setdefault(r.month, {})[r.segment_name] = float(r.v or 0)
    with cdm.connect() as conn:
        cdm_rows = {
            r.month: r for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for m in months:
        src = raw_map.get(m, {})
        if m not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[m]
        for seg, attr in (
            ("organic", "organic"),
            ("own_group", "own_group"),
            ("paid_group", "paid_group"),
            ("other_group", "other_group"),
            ("total", "total"),
        ):
            exp = float(src.get(seg, 0))
            got = getattr(d, attr)
            if got is None or abs(exp - float(got)) > COUNT_TOL:
                mm += 1
                break
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.overall_traffic_trend",
            status="PASS" if mm == 0 else "FAIL",
            detail=None if mm == 0 else f"month_mismatches={mm}",
        )
    ]


def _cdm_geo_kpi(calc: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    calc_sql = text(
        """
        SELECT month::date AS month, metric_value::float8 AS v,
               mom_pct_change::float8 AS mom, yoy_pct_change::float8 AS yoy
        FROM calc_monthly_kpi
        WHERE month = ANY(:months) AND segment_name = 'ai' AND metric_name = 'visits'
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, geo_visits::float8 AS v,
               mom_pct_change::float8 AS mom, yoy_pct_change::float8 AS yoy
        FROM cdm_geo_traffic_kpi WHERE month = ANY(:months)
        """
    )
    with calc.connect() as conn:
        calc_rows = {
            r.month: r for r in conn.execute(calc_sql, {"months": list(months)}).fetchall()
        }
    with cdm.connect() as conn:
        cdm_rows = {
            r.month: r for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm, miss_r, miss_l = _cmp_maps(
        {k: v.v for k, v in calc_rows.items()},
        {k: v.v for k, v in cdm_rows.items()},
        tol=COUNT_TOL,
    )
    # also mom/yoy
    for m, c in calc_rows.items():
        if m not in cdm_rows:
            continue
        d = cdm_rows[m]
        for a, b in ((c.mom, d.mom), (c.yoy, d.yoy)):
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > FLOAT_TOL:
                mm += 1
                break
    ok = mm == 0 and miss_r == 0 and miss_l == 0
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.geo_traffic_kpi",
            status="PASS" if ok else "FAIL",
            detail=None if ok else f"mismatches={mm}; missing_cdm={miss_r}; extra_cdm={miss_l}",
        )
    ]


def _cdm_geo_trend(raw: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    raw_sql = text(
        """
        SELECT month::date AS month, metric_value::float8 AS v
        FROM raw_aa_monthly_metrics
        WHERE month = ANY(:months) AND segment_name = 'ai' AND metric_name = 'visits'
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, geo_visits::float8 AS v
        FROM cdm_geo_traffic_trend WHERE month = ANY(:months)
        """
    )
    with raw.connect() as conn:
        raw_rows = {
            r.month: r.v for r in conn.execute(raw_sql, {"months": list(months)}).fetchall()
        }
    with cdm.connect() as conn:
        cdm_rows = {
            r.month: r.v for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm, miss_r, miss_l = _cmp_maps(raw_rows, cdm_rows, tol=COUNT_TOL)
    ok = mm == 0 and miss_r == 0
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.geo_traffic_trend",
            status="PASS" if ok else "FAIL",
            detail=None if ok else f"mismatches={mm}; missing_cdm={miss_r}; extra_cdm={miss_l}",
        )
    ]


def _cdm_geo_by_section(calc: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    calc_sql = text(
        """
        SELECT month::date AS month, section_name,
               section_visits::float8 AS visits, section_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM calc_monthly_section_share
        WHERE month = ANY(:months) AND segment_name = 'ai'
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, section_name,
               section_visits::float8 AS visits, section_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM cdm_geo_traffic_by_section WHERE month = ANY(:months)
        """
    )
    return _mix_compare(
        calc,
        cdm,
        calc_sql,
        cdm_sql,
        months,
        key_fields=("month", "section_name"),
        expected_keys={(m, s) for m in months for s in SECTION_LIST},
        name="recon.calc_cdm.geo_traffic_by_section",
    )


def _cdm_organic_kpi(calc: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    seg_map = {"organic": "organic_visits", "google": "google_visits", "yandex": "yandex_visits"}
    calc_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_value::float8 AS v,
               mom_pct_change::float8 AS mom, yoy_pct_change::float8 AS yoy
        FROM calc_monthly_kpi
        WHERE month = ANY(:months) AND metric_name = 'visits'
          AND segment_name IN ('organic','google','yandex')
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, kpi_name, kpi_value::float8 AS v,
               mom_pct_change::float8 AS mom, yoy_pct_change::float8 AS yoy
        FROM cdm_organic_search_kpi WHERE month = ANY(:months)
        """
    )
    with calc.connect() as conn:
        calc_rows = list(conn.execute(calc_sql, {"months": list(months)}).fetchall())
    with cdm.connect() as conn:
        cdm_rows = {
            (r.month, r.kpi_name): r
            for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for r in calc_rows:
        kpi = seg_map[r.segment_name]
        key = (r.month, kpi)
        if key not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[key]
        for a, b, tol in ((r.v, d.v, COUNT_TOL), (r.mom, d.mom, FLOAT_TOL), (r.yoy, d.yoy, FLOAT_TOL)):
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > tol:
                mm += 1
                break
    missing = {(m, k) for m in months for k in ORGANIC_KPI_ORDER} - set(cdm_rows)
    ok = mm == 0 and not missing
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.organic_search_kpi",
            status="PASS" if ok else "FAIL",
            detail=None if ok else f"mismatches={mm}; missing={len(missing)}",
        )
    ]


def _cdm_organic_engine_mix(
    calc: Engine, cdm: Engine, months: Sequence[date], report_end: date
) -> list[SuiteCheck]:
    calc_sql = text(
        """
        SELECT month::date AS month, search_engine,
               search_engine_visits::float8 AS visits,
               search_engine_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM calc_monthly_search_engines WHERE month = ANY(:months)
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, search_engine, month_type,
               search_engine_visits::float8 AS visits,
               search_engine_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM cdm_organic_search_engine_mix WHERE month = ANY(:months)
        """
    )
    with calc.connect() as conn:
        calc_rows = {
            (r.month, r.search_engine): r
            for r in conn.execute(calc_sql, {"months": list(months)}).fetchall()
        }
    with cdm.connect() as conn:
        cdm_rows = {
            (r.month, r.search_engine): r
            for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    type_mm = 0
    type_warn = 0
    for key, c in calc_rows.items():
        if key not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[key]
        for a, b in ((c.visits, d.visits), (c.share, d.share), (c.mom_pp, d.mom_pp)):
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > FLOAT_TOL:
                mm += 1
                break
        if d.month_type != month_type_label(key[0], report_end):
            if key[0] == report_end:
                type_mm += 1
            else:
                type_warn += 1
    missing = {(m, e) for m in months for e in SEARCH_ENGINE_ORDER} - set(cdm_rows)
    ok = mm == 0 and type_mm == 0 and not missing
    checks = [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.organic_search_engine_mix",
            status="PASS" if ok else "FAIL",
            detail=None
            if ok
            else f"mismatches={mm}; month_type_mismatches_report_end={type_mm}; missing={len(missing)}",
        )
    ]
    if type_warn:
        checks.append(
            SuiteCheck(
                category="calc_cdm_reconciliation",
                name="recon.calc_cdm.organic_search_engine_mix.month_type_historical",
                status="WARN",
                critical=False,
                detail=(
                    f"historical_months_not_relabeled_to_final_report_end={type_warn} "
                    "(expected under incremental month-as-report_end builds)"
                ),
            )
        )
    return checks


def _cdm_organic_engine_trend(
    raw: Engine, cdm: Engine, months: Sequence[date]
) -> list[SuiteCheck]:
    raw_sql = text(
        """
        SELECT month::date AS month, segment_name, metric_value::float8 AS v
        FROM raw_aa_monthly_metrics
        WHERE month = ANY(:months) AND metric_name = 'visits'
          AND segment_name IN ('organic','google','yandex')
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month,
               google_visits::float8 AS google,
               yandex_visits::float8 AS yandex,
               other_search_engines_visits::float8 AS other,
               organic_total_visits::float8 AS organic
        FROM cdm_organic_search_engine_trend WHERE month = ANY(:months)
        """
    )
    with raw.connect() as conn:
        raw_map: dict[date, dict[str, float]] = {}
        for r in conn.execute(raw_sql, {"months": list(months)}).fetchall():
            raw_map.setdefault(r.month, {})[r.segment_name] = float(r.v or 0)
    with cdm.connect() as conn:
        cdm_rows = {
            r.month: r for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for m in months:
        src = raw_map.get(m, {})
        organic = float(src.get("organic", 0))
        google = float(src.get("google", 0))
        yandex = float(src.get("yandex", 0))
        other = max(0.0, organic - google - yandex)
        if m not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[m]
        for exp, got in (
            (google, d.google),
            (yandex, d.yandex),
            (other, d.other),
            (organic, d.organic),
        ):
            if got is None or abs(exp - float(got)) > COUNT_TOL:
                mm += 1
                break
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.organic_search_engine_trend",
            status="PASS" if mm == 0 else "FAIL",
            detail=None if mm == 0 else f"month_mismatches={mm}",
        )
    ]


def _cdm_explore_kpi(calc: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    calc_sql = text(
        """
        SELECT month::date AS month, section_name, kpi_name, kpi_value::float8 AS v,
               mom_pct_change::float8 AS mom, yoy_pct_change::float8 AS yoy
        FROM calc_explore_buying_guides_kpi
        WHERE month = ANY(:months)
          AND section_name = ANY(:sections)
          AND kpi_name IN ('total_visits','organic_visits')
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, section_name, kpi_name, kpi_value::float8 AS v,
               mom_pct_change::float8 AS mom, yoy_pct_change::float8 AS yoy
        FROM cdm_explore_buying_guides_kpi WHERE month = ANY(:months)
        """
    )
    with calc.connect() as conn:
        calc_rows = {
            (r.month, r.section_name, r.kpi_name): r
            for r in conn.execute(
                calc_sql, {"months": list(months), "sections": list(EXPLORE_BG_SECTIONS)}
            ).fetchall()
        }
    with cdm.connect() as conn:
        cdm_rows = {
            (r.month, r.section_name, r.kpi_name): r
            for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for key, c in calc_rows.items():
        if key not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[key]
        for a, b, tol in ((c.v, d.v, COUNT_TOL), (c.mom, d.mom, FLOAT_TOL), (c.yoy, d.yoy, FLOAT_TOL)):
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > tol:
                mm += 1
                break
    missing = set(calc_rows) - set(cdm_rows)
    ok = mm == 0 and not missing
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.explore_buying_guides_kpi",
            status="PASS" if ok else "FAIL",
            detail=None if ok else f"mismatches={mm}; missing={len(missing)}",
        )
    ]


def _cdm_explore_trend(raw: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    raw_sql = text(
        """
        SELECT month::date AS month, section_name, metric_value::float8 AS v
        FROM raw_aa_monthly_by_section
        WHERE month = ANY(:months)
          AND section_name IN ('explore','buying_guides')
          AND segment_name = 'organic'
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month,
               explore_organic_visits::float8 AS explore,
               buying_guides_organic_visits::float8 AS bg,
               total_organic_visits::float8 AS total
        FROM cdm_explore_buying_guides_trend WHERE month = ANY(:months)
        """
    )
    with raw.connect() as conn:
        raw_map: dict[date, dict[str, float]] = {}
        for r in conn.execute(raw_sql, {"months": list(months)}).fetchall():
            raw_map.setdefault(r.month, {})[r.section_name] = raw_map.setdefault(
                r.month, {}
            ).get(r.section_name, 0.0) + float(r.v or 0)
    with cdm.connect() as conn:
        cdm_rows = {
            r.month: r for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for m in months:
        src = raw_map.get(m, {})
        explore = float(src.get("explore", 0))
        bg = float(src.get("buying_guides", 0))
        total = explore + bg
        if m not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[m]
        for exp, got in ((explore, d.explore), (bg, d.bg), (total, d.total)):
            if got is None or abs(exp - float(got)) > COUNT_TOL:
                mm += 1
                break
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.explore_buying_guides_trend",
            status="PASS" if mm == 0 else "FAIL",
            detail=None if mm == 0 else f"month_mismatches={mm}",
        )
    ]


def _mix_compare(
    calc: Engine,
    cdm: Engine,
    calc_sql,
    cdm_sql,
    months: Sequence[date],
    *,
    key_fields: tuple[str, ...],
    expected_keys: set,
    name: str,
) -> list[SuiteCheck]:
    with calc.connect() as conn:
        calc_rows = list(conn.execute(calc_sql, {"months": list(months)}).fetchall())
    with cdm.connect() as conn:
        cdm_rows = list(conn.execute(cdm_sql, {"months": list(months)}).fetchall())

    def key_of(r) -> tuple:
        return tuple(getattr(r, f) if f != "month" else r.month for f in key_fields)

    calc_map = {key_of(r): r for r in calc_rows}
    cdm_map = {key_of(r): r for r in cdm_rows}
    mm = 0
    for k, c in calc_map.items():
        if k not in cdm_map:
            mm += 1
            continue
        d = cdm_map[k]
        for attr in ("visits", "share", "mom_pp"):
            if not hasattr(c, attr):
                continue
            a, b = getattr(c, attr), getattr(d, attr)
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > FLOAT_TOL:
                mm += 1
                break
    missing = expected_keys - set(cdm_map)
    ok = mm == 0 and not missing
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name=name,
            status="PASS" if ok else "FAIL",
            detail=None if ok else f"mismatches={mm}; missing={len(missing)}",
        )
    ]


def _cdm_sections_mix(
    calc: Engine, cdm: Engine, months: Sequence[date], report_end: date
) -> list[SuiteCheck]:
    calc_sql = text(
        """
        SELECT month::date AS month, section_name,
               section_visits::float8 AS visits, section_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM calc_monthly_section_share
        WHERE month = ANY(:months) AND segment_name = 'total'
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, section_name, month_type,
               section_visits::float8 AS visits, section_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM cdm_sections_mix WHERE month = ANY(:months)
        """
    )
    checks = _mix_compare(
        calc,
        cdm,
        calc_sql,
        cdm_sql,
        months,
        key_fields=("month", "section_name"),
        expected_keys={(m, s) for m in months for s in SECTION_LIST},
        name="recon.calc_cdm.sections_mix",
    )
    with cdm.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT month::date AS month, month_type FROM cdm_sections_mix WHERE month = ANY(:months)"
            ),
            {"months": list(months)},
        ).fetchall()
    type_mm = 0
    type_warn = 0
    for r in rows:
        expected = month_type_label(r.month, report_end)
        if r.month_type != expected:
            if r.month == report_end:
                type_mm += 1
            else:
                type_warn += 1
    checks.append(
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.sections_mix.month_type",
            status="PASS" if type_mm == 0 else "FAIL",
            detail=None if type_mm == 0 else f"month_type_mismatches_report_end={type_mm}",
        )
    )
    if type_warn:
        checks.append(
            SuiteCheck(
                category="calc_cdm_reconciliation",
                name="recon.calc_cdm.sections_mix.month_type_historical",
                status="WARN",
                critical=False,
                detail=(
                    f"historical_months_not_relabeled_to_final_report_end={type_warn} "
                    "(expected under incremental month-as-report_end builds)"
                ),
            )
        )
    return checks


def _cdm_device_section_mix(calc: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    calc_sql = text(
        """
        SELECT month::date AS month, segment_name AS device_segment, section_name,
               section_visits::float8 AS visits, section_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM calc_monthly_section_share
        WHERE month = ANY(:months) AND segment_name = ANY(:segs)
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, device_segment, section_name,
               section_visits::float8 AS visits, section_share::float8 AS share,
               mom_pp_change::float8 AS mom_pp
        FROM cdm_device_section_mix WHERE month = ANY(:months)
        """
    )
    # Custom because calc query needs segs param
    with calc.connect() as conn:
        calc_rows = {
            (r.month, r.device_segment, r.section_name): r
            for r in conn.execute(
                calc_sql, {"months": list(months), "segs": list(DEVICE_SEGMENTS)}
            ).fetchall()
        }
    with cdm.connect() as conn:
        cdm_rows = {
            (r.month, r.device_segment, r.section_name): r
            for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for k, c in calc_rows.items():
        if k not in cdm_rows:
            mm += 1
            continue
        d = cdm_rows[k]
        for a, b in ((c.visits, d.visits), (c.share, d.share), (c.mom_pp, d.mom_pp)):
            if a is None and b is None:
                continue
            if a is None or b is None or abs(float(a) - float(b)) > FLOAT_TOL:
                mm += 1
                break
    expected = {
        (m, d, s) for m in months for d in DEVICE_SEGMENTS for s in SECTION_LIST
    }
    missing = expected - set(cdm_rows)
    ok = mm == 0 and not missing
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.device_section_mix",
            status="PASS" if ok else "FAIL",
            detail=None if ok else f"mismatches={mm}; missing={len(missing)}",
        )
    ]


def _cdm_top_pages(raw: Engine, cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    """Compare visits + rank identity without logging page_url values."""
    checks: list[SuiteCheck] = []
    # Row counts must match per month
    raw_sql = text(
        """
        SELECT month::date AS month, COUNT(*)::int AS n, COALESCE(SUM(visits),0)::float8 AS s
        FROM raw_aa_monthly_by_url WHERE month = ANY(:months) GROUP BY month
        """
    )
    cdm_sql = text(
        """
        SELECT month::date AS month, COUNT(*)::int AS n, COALESCE(SUM(visits),0)::float8 AS s
        FROM cdm_top_pages WHERE month = ANY(:months) GROUP BY month
        """
    )
    with raw.connect() as conn:
        raw_rows = {
            r.month: r for r in conn.execute(raw_sql, {"months": list(months)}).fetchall()
        }
    with cdm.connect() as conn:
        cdm_rows = {
            r.month: r for r in conn.execute(cdm_sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for m in months:
        if m not in raw_rows or m not in cdm_rows:
            mm += 1
            continue
        if raw_rows[m].n != cdm_rows[m].n:
            mm += 1
            continue
        if abs(float(raw_rows[m].s) - float(cdm_rows[m].s)) > COUNT_TOL:
            mm += 1
    checks.append(
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.top_pages.counts_and_visit_sums",
            status="PASS" if mm == 0 else "FAIL",
            detail=None if mm == 0 else f"month_mismatches={mm}",
        )
    )

    # Rank integrity: page_rank unique per month, starts at 1, visits DESC order
    rank_sql = text(
        """
        SELECT month::date AS month,
               COUNT(*) AS n,
               COUNT(DISTINCT page_rank) AS ranks,
               MIN(page_rank) AS min_r,
               MAX(page_rank) AS max_r,
               SUM(CASE WHEN visits < 0 THEN 1 ELSE 0 END) AS neg
        FROM cdm_top_pages
        WHERE month = ANY(:months)
        GROUP BY month
        """
    )
    with cdm.connect() as conn:
        rows = list(conn.execute(rank_sql, {"months": list(months)}).fetchall())
    bad = 0
    for r in rows:
        if r.n != r.ranks or r.min_r != 1 or r.max_r != r.n or r.neg:
            bad += 1
    checks.append(
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.top_pages.rank_integrity",
            status="PASS" if bad == 0 else "FAIL",
            detail=None if bad == 0 else f"bad_months={bad}",
        )
    )

    # Opaque key set equality: hash of (month, page_url) pairs must match RAW↔CDM
    # without emitting URLs.
    import hashlib

    def fingerprint(engine: Engine, table: str) -> str:
        sql = text(
            f"""
            SELECT month::text, page_url
            FROM {table}
            WHERE month = ANY(:months)
            ORDER BY month, page_url
            """
        )
        digest = hashlib.sha256()
        with engine.connect() as conn:
            for row in conn.execute(sql, {"months": list(months)}):
                digest.update(f"{row[0]}|{row[1]}\n".encode("utf-8"))
        return digest.hexdigest()

    raw_fp = fingerprint(raw, "raw_aa_monthly_by_url")
    cdm_fp = fingerprint(cdm, "cdm_top_pages")
    checks.append(
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.top_pages.url_keyset",
            status="PASS" if raw_fp == cdm_fp else "FAIL",
            detail=None if raw_fp == cdm_fp else "url_keyset_fingerprint_mismatch",
        )
    )
    return checks


def _cdm_dim_month(cdm: Engine, months: Sequence[date]) -> list[SuiteCheck]:
    sql = text(
        """
        SELECT month::date AS month, year, month_num, year_month, month_sort
        FROM cdm_dim_month WHERE month = ANY(:months)
        """
    )
    with cdm.connect() as conn:
        rows = {
            r.month: r for r in conn.execute(sql, {"months": list(months)}).fetchall()
        }
    mm = 0
    for m in months:
        if m not in rows:
            mm += 1
            continue
        r = rows[m]
        if (
            int(r.year) != m.year
            or int(r.month_num) != m.month
            or str(r.year_month) != m.strftime("%Y-%m")
            or int(r.month_sort) != int(m.strftime("%Y%m"))
        ):
            mm += 1
    return [
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.dim_month",
            status="PASS" if mm == 0 else "FAIL",
            detail=None if mm == 0 else f"mismatches={mm}",
        )
    ]


__all__ = ["reconcile_calc_to_cdm"]
