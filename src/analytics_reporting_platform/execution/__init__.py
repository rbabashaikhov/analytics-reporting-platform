"""Monthly pipeline execution package API."""

from analytics_reporting_platform.execution.models import (
    ExecutionMode,
    MartRunResult,
    MonthlyExecutionError,
    MonthlyExecutionEvent,
    MonthlyExecutionResult,
    MonthlyPipelineDeps,
    OverallStatus,
    StageName,
    StageRunResult,
    StageStatus,
)
from analytics_reporting_platform.execution.period import (
    ExecutionMonthInput,
    PeriodValidationError,
    current_month_start,
    iter_months,
    last_completed_month,
    parse_execution_month,
    validate_execution_month,
)


def __getattr__(name: str):
    """Lazy-load heavy monthly runner / mart inventories (SQLAlchemy + layer imports)."""
    if name in {
        "CALC_MARTS",
        "CDM_MARTS",
        "RAW_MARTS",
        "run_monthly_period",
        "run_monthly_raw",
        "run_monthly_calc",
        "run_monthly_cdm",
        "SupportsPostReport",
    }:
        from analytics_reporting_platform.execution import monthly as monthly_mod

        return getattr(monthly_mod, name)
    if name in {
        "BackfillMonthPlan",
        "BackfillMonthResult",
        "BackfillRunState",
        "DEFAULT_RAW_HISTORY_START",
        "DEFAULT_REPORTING_START",
        "assert_lookbacks_within_raw_history",
        "first_incomplete_month",
        "months_for_backfill",
        "plan_monthly_backfill",
        "reject_incomplete_or_future_month",
        "reporting_mom_month",
        "reporting_yoy_month",
        "run_monthly_backfill",
    }:
        from analytics_reporting_platform.execution import backfill as backfill_mod

        return getattr(backfill_mod, name)
    if name in {
        "MonthlyPeriodsPlan",
        "DEFAULT_RAW_START",
        "plan_from_safe_dict",
        "plan_incremental_periods",
        "plan_monthly_periods",
        "plan_rebuild_periods",
        "reject_incomplete_month",
    }:
        from analytics_reporting_platform.execution import monthly_plan as plan_mod

        return getattr(plan_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CALC_MARTS",
    "CDM_MARTS",
    "ExecutionMode",
    "ExecutionMonthInput",
    "MartRunResult",
    "MonthlyExecutionError",
    "MonthlyExecutionEvent",
    "MonthlyExecutionResult",
    "MonthlyPipelineDeps",
    "OverallStatus",
    "PeriodValidationError",
    "RAW_MARTS",
    "StageName",
    "StageRunResult",
    "StageStatus",
    "SupportsPostReport",
    "DEFAULT_RAW_HISTORY_START",
    "DEFAULT_REPORTING_START",
    "assert_lookbacks_within_raw_history",
    "current_month_start",
    "first_incomplete_month",
    "iter_months",
    "last_completed_month",
    "months_for_backfill",
    "parse_execution_month",
    "plan_monthly_backfill",
    "reject_incomplete_or_future_month",
    "reporting_mom_month",
    "reporting_yoy_month",
    "run_monthly_backfill",
    "run_monthly_calc",
    "run_monthly_cdm",
    "run_monthly_period",
    "run_monthly_raw",
    "validate_execution_month",
    "BackfillMonthPlan",
    "BackfillMonthResult",
    "BackfillRunState",
    "MonthlyPeriodsPlan",
    "DEFAULT_RAW_START",
    "plan_from_safe_dict",
    "plan_incremental_periods",
    "plan_monthly_periods",
    "plan_rebuild_periods",
    "reject_incomplete_month",
]
