"""Monthly DWH Validation Suite orchestrator (read-only, Airflow-callable)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional, Union

from sqlalchemy.engine import Engine

from analytics_reporting_platform.cdm.models import DEFAULT_TREND_START
from analytics_reporting_platform.validation.contracts import (
    check_month_grain,
    check_required_dimensions_not_null,
    check_table_contracts,
    check_uniqueness,
    check_value_ranges,
)
from analytics_reporting_platform.validation.coverage import check_coverage, month_range
from analytics_reporting_platform.validation.reconciliation_calc_cdm import reconcile_calc_to_cdm
from analytics_reporting_platform.validation.reconciliation_raw_calc import reconcile_raw_to_calc
from analytics_reporting_platform.validation.reference import compare_to_reference
from analytics_reporting_platform.validation.report import print_human_summary, write_suite_reports
from analytics_reporting_platform.validation.report_contract import check_report_contract
from analytics_reporting_platform.validation.specs import LayerName
from analytics_reporting_platform.validation.types import (
    CategoryResult,
    FinalStatus,
    SuiteResult,
    summarize_category,
)
from analytics_reporting_platform.validation.weekly_guard import check_monthly_only

# Default Portfolio Edition Monthly windows after E2E through 2026-07.
DEFAULT_RAW_START = date(2025, 1, 1)
DEFAULT_RAW_END = date(2026, 7, 1)
DEFAULT_CALC_START = date(2026, 1, 1)
DEFAULT_CALC_END = date(2026, 7, 1)
DEFAULT_CDM_START = date(2026, 1, 1)
DEFAULT_CDM_END = date(2026, 7, 1)


def run_monthly_dwh_validation_suite(
    *,
    raw_engine: Engine,
    calc_engine: Engine,
    cdm_engine: Engine,
    report_end: date = DEFAULT_CDM_END,
    raw_start: date = DEFAULT_RAW_START,
    raw_end: date = DEFAULT_RAW_END,
    calc_start: date = DEFAULT_CALC_START,
    calc_end: date = DEFAULT_CALC_END,
    cdm_start: date = DEFAULT_CDM_START,
    cdm_end: date = DEFAULT_CDM_END,
    trend_start: date = DEFAULT_TREND_START,
    reference_engines: Optional[dict[LayerName, Engine]] = None,
    output_dir: Optional[Union[str, Path]] = None,
    print_summary: bool = True,
) -> SuiteResult:
    """Execute the full read-only Monthly DWH validation suite.

    Safe for Airflow task invocation: SELECT-only, no Adobe calls, no writes.
    """
    engines: dict[LayerName, Engine] = {
        "raw": raw_engine,
        "calc": calc_engine,
        "cdm": cdm_engine,
    }
    exec_port = _engine_port(raw_engine)
    ref_port = _engine_port(next(iter(reference_engines.values()))) if reference_engines else None

    # Guard: never treat frozen port as execution write target — suite is read-only anyway.
    categories: list[CategoryResult] = []

    coverage_checks = check_coverage(
        engines,
        raw_start=raw_start,
        raw_end=raw_end,
        calc_start=calc_start,
        calc_end=calc_end,
        cdm_start=cdm_start,
        cdm_end=cdm_end,
    )
    categories.append(summarize_category("coverage", coverage_checks))

    contract_checks = []
    contract_checks.extend(check_table_contracts(engines))
    contract_checks.extend(check_required_dimensions_not_null(engines))
    contract_checks.extend(check_month_grain(engines))
    categories.append(summarize_category("contracts", contract_checks))

    uniq_checks = check_uniqueness(engines)
    categories.append(summarize_category("uniqueness", uniq_checks))

    range_checks = check_value_ranges(engines)
    categories.append(summarize_category("value_ranges", range_checks))

    raw_calc = reconcile_raw_to_calc(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        calc_start=calc_start,
        calc_end=calc_end,
    )
    categories.append(summarize_category("raw_calc_reconciliation", raw_calc))

    calc_cdm = reconcile_calc_to_cdm(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        cdm_engine=cdm_engine,
        cdm_start=cdm_start,
        cdm_end=cdm_end,
        trend_start=trend_start,
    )
    categories.append(summarize_category("calc_cdm_reconciliation", calc_cdm))

    report_checks = check_report_contract(cdm_engine=cdm_engine, report_end=report_end)
    categories.append(summarize_category("report_contract", report_checks))

    # Overlap with frozen baseline typically through min(report_end, frozen_max).
    # Frozen historically ends at 2026-06; compare up to that when available.
    overlap_end = min(cdm_end, date(2026, 6, 1))
    overlap_start = max(raw_start, date(2025, 1, 1))
    ref_checks = compare_to_reference(
        exec_engines=engines,
        ref_engines=reference_engines,
        overlap_start=overlap_start,
        overlap_end=overlap_end,
    )
    categories.append(summarize_category("reference_comparison", ref_checks))

    monthly_only = check_monthly_only(engines)
    categories.append(summarize_category("monthly_only", monthly_only))

    critical_fail = False
    for cat in categories:
        for ch in cat.checks:
            if ch.status == "FAIL" and ch.critical:
                critical_fail = True
                break
        if critical_fail:
            break
    final: FinalStatus = "FAIL" if critical_fail else "PASS"

    result = SuiteResult(
        final_status=final,
        report_end=report_end.strftime("%Y-%m"),
        categories=categories,
        generated_at=datetime.now(timezone.utc).isoformat(),
        read_only=True,
        execution_port=exec_port,
        reference_port=ref_port,
    )

    if output_dir is not None:
        write_suite_reports(result, output_dir)
    if print_summary:
        print_human_summary(result)
        if output_dir is not None:
            print(f"reports_dir={Path(output_dir)}")
    return result


def _engine_port(engine: Engine) -> Optional[int]:
    try:
        return engine.url.port
    except Exception:  # noqa: BLE001
        return None


__all__ = [
    "DEFAULT_CALC_END",
    "DEFAULT_CALC_START",
    "DEFAULT_CDM_END",
    "DEFAULT_CDM_START",
    "DEFAULT_RAW_END",
    "DEFAULT_RAW_START",
    "month_range",
    "run_monthly_dwh_validation_suite",
]
