"""Airflow-facing orchestration helpers (thin wrappers over execution API)."""

from __future__ import annotations


__all__ = [
    "MONTHLY_DAG_ID",
    "MonthlyAirflowParams",
    "MonthlyOrchestrationError",
    "SAFE_RESULT_FIELDS",
    "SAFE_STAGE_FIELDS",
    "build_monthly_pipeline_deps",
    "final_status_payload",
    "resolve_monthly_period",
    "run_monthly_data_quality_for_airflow",
    "run_monthly_for_airflow",
    "run_stage_for_airflow",
    "run_stage_periods_for_airflow",
    "safe_result_payload",
    "safe_stage_payload",
]


def __getattr__(name: str):
    from analytics_reporting_platform.orchestration import monthly as monthly_mod
    from analytics_reporting_platform.orchestration import validation as validation_mod

    if hasattr(monthly_mod, name):
        return getattr(monthly_mod, name)
    if hasattr(validation_mod, name):
        return getattr(validation_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
