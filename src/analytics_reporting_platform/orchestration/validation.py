"""Airflow-facing Monthly DWH validation runner (read-only).

Calls the existing ``run_monthly_dwh_validation_suite`` engine — no duplicated checks.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional

from analytics_reporting_platform.cdm.models import DEFAULT_REPORT_START, DEFAULT_TREND_START
from analytics_reporting_platform.execution.period import (
    last_completed_month,
    parse_execution_month,
)
from analytics_reporting_platform.validation.artifacts import (
    VALIDATION_ARTIFACT_ROOT_ENV,
    resolve_validation_artifact_root,
    validation_run_dir,
)
from analytics_reporting_platform.validation.notify import NoOpNotifier, safe_notify
from analytics_reporting_platform.validation.report import (
    format_airflow_validation_summary,
    write_suite_reports,
)
from analytics_reporting_platform.validation.suite import run_monthly_dwh_validation_suite
from analytics_reporting_platform.validation.types import SuiteResult

logger = logging.getLogger(__name__)

REFERENCE_ENV_FILE_ENV = "ARP_REFERENCE_ENV_FILE"
REFERENCE_ENV_FILE_VARIABLE = "arp_reference_env_file"


class DataQualityError(RuntimeError):
    """Raised when Monthly DWH validation has critical FAIL checks."""

    def __init__(
        self,
        message: str,
        *,
        result: Optional[SuiteResult] = None,
        artifact_dir: Optional[Path] = None,
    ) -> None:
        super().__init__(message)
        self.result = result
        self.artifact_dir = artifact_dir


def resolve_validation_report_end(
    *,
    last_complete_month: str | date,
    today: Optional[date] = None,
) -> date:
    """Report end is always a fully completed calendar month — never current/incomplete."""
    as_of = today if today is not None else date.today()
    completed = last_completed_month(as_of)
    report_end = (
        last_complete_month
        if isinstance(last_complete_month, date)
        else parse_execution_month(last_complete_month)
    )
    if report_end > completed:
        raise DataQualityError(
            f"report_end {report_end.strftime('%Y-%m')} is incomplete "
            f"(last_complete_month={completed.strftime('%Y-%m')})"
        )
    return report_end


def _optional_airflow_variable(key: str) -> Optional[str]:
    try:
        from airflow.models import Variable

        value = Variable.get(key, default_var=None)
    except Exception:  # noqa: BLE001
        return None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_reference_env_file(explicit: Optional[str] = None) -> Optional[str]:
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    from_env = os.environ.get(REFERENCE_ENV_FILE_ENV)
    if from_env and str(from_env).strip():
        return str(from_env).strip()
    return _optional_airflow_variable(REFERENCE_ENV_FILE_VARIABLE)


def _build_reference_engines(reference_env_file: Optional[str]):
    if not reference_env_file:
        return None
    path = Path(reference_env_file)
    if not path.is_file():
        logger.warning(
            "reference_env_file_missing path=%s — reference comparison skipped",
            path,
        )
        return None
    from analytics_reporting_platform.config.settings import load_settings
    from analytics_reporting_platform.db.engine import create_db_engine

    settings = load_settings(path)
    return {
        "raw": create_db_engine(settings, "raw"),
        "calc": create_db_engine(settings, "calc"),
        "cdm": create_db_engine(settings, "cdm"),
    }


def run_monthly_data_quality_for_airflow(
    *,
    last_complete_month: str,
    run_id: str,
    dag_id: str = "monthly_private_pipeline",
    env_file: Optional[str] = None,
    reference_env_file: Optional[str] = None,
    artifact_root: Optional[str] = None,
    today: Optional[date] = None,
    notifier: Optional[Any] = None,
    raise_on_critical_fail: bool = True,
) -> dict[str, Any]:
    """Run read-only Monthly DWH validation and persist private artifacts.

    Returns a sanitized XCom-safe payload. Raises ``DataQualityError`` on critical FAIL
    when ``raise_on_critical_fail`` is True (Airflow task → FAILED).
    """
    from analytics_reporting_platform.config.settings import load_settings
    from analytics_reporting_platform.db.engine import create_db_engine
    from analytics_reporting_platform.orchestration.monthly import (
        MonthlyOrchestrationError,
    )

    if not env_file:
        raise MonthlyOrchestrationError(
            "data_quality requires ARP_ENV_FILE / arp_env_file for DWH connectivity"
        )

    report_end = resolve_validation_report_end(
        last_complete_month=last_complete_month, today=today
    )
    # Coverage windows track Portfolio Edition bounds through the completed report end.
    raw_start = DEFAULT_TREND_START
    calc_start = min(DEFAULT_REPORT_START, report_end)
    cdm_start = calc_start

    settings = load_settings(env_file)
    raw_engine = create_db_engine(settings, "raw")
    calc_engine = create_db_engine(settings, "calc")
    cdm_engine = create_db_engine(settings, "cdm")
    reference_engines = _build_reference_engines(
        _resolve_reference_env_file(reference_env_file)
    )

    root = resolve_validation_artifact_root(explicit=artifact_root)
    artifact_dir = validation_run_dir(run_id=run_id, dag_id=dag_id, root=root)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    result = run_monthly_dwh_validation_suite(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        cdm_engine=cdm_engine,
        report_end=report_end,
        raw_start=raw_start,
        raw_end=report_end,
        calc_start=calc_start,
        calc_end=report_end,
        cdm_start=cdm_start,
        cdm_end=report_end,
        trend_start=DEFAULT_TREND_START,
        reference_engines=reference_engines,
        output_dir=None,  # write via write_suite_reports for stable filenames
        print_summary=False,
    )
    json_path, md_path = write_suite_reports(result, artifact_dir)

    summary = format_airflow_validation_summary(
        result, md_path=md_path, json_path=json_path
    )
    for line in summary.splitlines():
        logger.info("%s", line)

    active_notifier = notifier if notifier is not None else NoOpNotifier()
    safe_notify(
        active_notifier,
        result,
        artifact_dir=artifact_dir,
        md_path=md_path,
        json_path=json_path,
    )

    payload: dict[str, Any] = {
        "stage": "data_quality",
        "status": "success" if result.final_status == "PASS" else "failed",
        "final_status": result.final_status,
        "report_end": result.report_end,
        "read_only": True,
        "critical_failures": len(result.failed_critical()),
        "warnings": len(result.warnings()),
        "artifact_dir": str(artifact_dir),
        "validation_md": str(md_path),
        "validation_json": str(json_path),
        "categories": {
            cat.name: {"status": cat.status, "passed": cat.passed, "total": cat.total}
            for cat in result.categories
        },
        "failed_checks": [c.name for c in result.failed_critical()],
    }

    if result.final_status == "FAIL" and raise_on_critical_fail:
        raise DataQualityError(
            f"Monthly DWH validation FAILED report_end={result.report_end} "
            f"critical_failures={payload['critical_failures']}",
            result=result,
            artifact_dir=artifact_dir,
        )
    return payload


def validation_windows_from_report_end(report_end: date) -> Mapping[str, date]:
    """Helper for tests / CLI: derive suite windows from a completed report_end."""
    return {
        "report_end": report_end,
        "raw_start": DEFAULT_TREND_START,
        "raw_end": report_end,
        "calc_start": min(DEFAULT_REPORT_START, report_end),
        "calc_end": report_end,
        "cdm_start": min(DEFAULT_REPORT_START, report_end),
        "cdm_end": report_end,
        "trend_start": DEFAULT_TREND_START,
    }


__all__ = [
    "DataQualityError",
    "REFERENCE_ENV_FILE_ENV",
    "REFERENCE_ENV_FILE_VARIABLE",
    "VALIDATION_ARTIFACT_ROOT_ENV",
    "resolve_validation_report_end",
    "run_monthly_data_quality_for_airflow",
    "validation_windows_from_report_end",
]
