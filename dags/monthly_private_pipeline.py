"""Monthly private operational pipeline DAG (single ARP Monthly DAG).

Graph:
  determine_periods → load_raw → build_calc → build_cdm → data_quality → final_status

Modes:
  incremental (default) — missing completed months from PostgreSQL, then DQ
    (empty DB → automatic full init: RAW from 2025-01, CALC/CDM from 2026-01)
  rebuild — manual force range via rebuild_start / rebuild_end, then DQ
  validate — ETL NO-OP; read-only Monthly DWH validation only

Never processes the current incomplete calendar month.
Weekly is out of Portfolio Edition scope.

Target Airflow: existing shared Airflow stack (do not create a second stack).
  Compose image apache/airflow:2.10.0, CeleryExecutor.

Timezone: Europe/Moscow.
Schedule: 08:00 on day 1 of each month (Europe/Moscow).
Scheduled planning uses ``data_interval_end`` as as-of date when present.

Concurrency: max_active_runs=1. No parallel Adobe extraction.
Safe defaults: mode=incremental, dry_run=True.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from pendulum import timezone as pendulum_timezone

logger = logging.getLogger(__name__)

MONTHLY_DAG_ID = "monthly_private_pipeline"
ENV_FILE_ENV = "ARP_ENV_FILE"
CLIENT_CONFIG_ENV = "ARP_CLIENT_CONFIG"
ENV_FILE_VARIABLE = "arp_env_file"
CLIENT_CONFIG_VARIABLE = "arp_client_config"
VALIDATION_ARTIFACT_ROOT_ENV = "ARP_VALIDATION_ARTIFACT_ROOT"
VALIDATION_ARTIFACT_ROOT_VARIABLE = "arp_validation_artifact_root"
REFERENCE_ENV_FILE_ENV = "ARP_REFERENCE_ENV_FILE"
REFERENCE_ENV_FILE_VARIABLE = "arp_reference_env_file"

DAG_TIMEZONE_NAME = "Europe/Moscow"
DAG_TZ = pendulum_timezone(DAG_TIMEZONE_NAME)

_REPO_CANDIDATES = (
    Path(__file__).resolve().parents[1] / "src",
    Path("/opt/airflow/arp/src"),
    Path("/opt/airflow/src"),
)
for _candidate in _REPO_CANDIDATES:
    if _candidate.is_dir():
        text = str(_candidate)
        if text not in sys.path:
            sys.path.insert(0, text)

# No Airflow-level retries on multi-month stages (Adobe client already retries).
DEFAULT_RETRIES = 0
DEFAULT_RETRY_DELAY = timedelta(minutes=10)
STAGE_EXECUTION_TIMEOUT = timedelta(hours=72)
MONTHLY_SCHEDULE = "0 8 1 * *"


def _ensure_package_path() -> None:
    for candidate in _REPO_CANDIDATES:
        if candidate.is_dir():
            text = str(candidate)
            if text not in sys.path:
                sys.path.insert(0, text)


def _optional_variable(key: str) -> Optional[str]:
    try:
        from airflow.models import Variable

        value = Variable.get(key, default_var=None)
    except Exception:  # noqa: BLE001
        return None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _config_paths_from_context() -> tuple[Optional[str], Optional[str]]:
    env_file = os.environ.get(ENV_FILE_ENV) or _optional_variable(ENV_FILE_VARIABLE)
    client_config = os.environ.get(CLIENT_CONFIG_ENV) or _optional_variable(
        CLIENT_CONFIG_VARIABLE
    )
    return env_file, client_config


def _validation_paths_from_context() -> tuple[Optional[str], Optional[str]]:
    artifact_root = os.environ.get(VALIDATION_ARTIFACT_ROOT_ENV) or _optional_variable(
        VALIDATION_ARTIFACT_ROOT_VARIABLE
    )
    reference_env = os.environ.get(REFERENCE_ENV_FILE_ENV) or _optional_variable(
        REFERENCE_ENV_FILE_VARIABLE
    )
    return artifact_root, reference_env


def _schedule_as_of_from_context(context: dict[str, Any]):
    from datetime import date as date_cls

    data_interval_end = context.get("data_interval_end")
    if data_interval_end is None:
        return None
    try:
        localized = data_interval_end.in_timezone(DAG_TZ)  # type: ignore[attr-defined]
        return date_cls(localized.year, localized.month, localized.day)
    except Exception:  # noqa: BLE001
        pass
    try:
        if getattr(data_interval_end, "tzinfo", None) is not None:
            localized = data_interval_end.astimezone(DAG_TZ)
            return date_cls(localized.year, localized.month, localized.day)
    except Exception:  # noqa: BLE001
        pass
    return date_cls(
        data_interval_end.year, data_interval_end.month, data_interval_end.day
    )


def _plan_from_xcom(ti: Any):
    from analytics_reporting_platform.execution.monthly_plan import plan_from_safe_dict
    from analytics_reporting_platform.orchestration.monthly import (
        MonthlyOrchestrationError,
    )

    payload = ti.xcom_pull(task_ids="determine_periods")
    if not payload:
        raise MonthlyOrchestrationError("Missing determine_periods XCom payload")
    return plan_from_safe_dict(payload)


def task_determine_periods(**context: Any) -> dict[str, Any]:
    """Build incremental / rebuild / validate period lists; push safe plan to XCom."""
    _ensure_package_path()
    from analytics_reporting_platform.execution.monthly_plan import plan_monthly_periods
    from analytics_reporting_platform.orchestration.monthly import (
        MonthlyOrchestrationError,
        build_monthly_pipeline_deps,
    )

    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    params = context.get("params") or {}
    schedule_as_of = _schedule_as_of_from_context(context)

    env_file, client_config = _config_paths_from_context()
    mode_hint = conf.get("mode", params.get("mode", "incremental"))
    mode_text = str(mode_hint or "incremental").strip().lower()

    raw_engine = calc_engine = cdm_engine = None
    # Presence probes only needed for incremental missing-month detection.
    if mode_text not in {"rebuild", "validate"}:
        try:
            deps = build_monthly_pipeline_deps(
                env_file=env_file, client_config=client_config
            )
            raw_engine = deps.raw_engine
            calc_engine = deps.calc_engine
            cdm_engine = deps.cdm_engine
        except Exception as exc:  # noqa: BLE001
            raise MonthlyOrchestrationError(
                f"determine_periods failed to open DWH engines: {type(exc).__name__}: {exc}"
            ) from exc

    try:
        plan = plan_monthly_periods(
            conf=conf,
            params=params,
            today=schedule_as_of,
            raw_engine=raw_engine,
            calc_engine=calc_engine,
            cdm_engine=cdm_engine,
        )
    except Exception as exc:  # noqa: BLE001
        raise MonthlyOrchestrationError(
            f"determine_periods validation failed: {exc}"
        ) from exc

    payload = plan.to_safe_dict()
    payload["dag_id"] = MONTHLY_DAG_ID
    payload["schedule_as_of"] = (
        schedule_as_of.isoformat() if schedule_as_of is not None else None
    )
    logger.info(
        "mode=%s stage=DETERMINE_PERIODS status=SUCCESS last_complete_month=%s "
        "raw=%s calc=%s cdm=%s dry_run=%s schedule_as_of=%s",
        payload["mode"],
        payload["last_complete_month"],
        payload["raw_periods"],
        payload["calc_periods"],
        payload["cdm_periods"],
        payload["dry_run"],
        payload["schedule_as_of"],
    )
    return payload


def _task_run_stage(stage: str, **context: Any) -> dict[str, Any]:
    _ensure_package_path()
    from analytics_reporting_platform.orchestration.monthly import (
        MonthlyOrchestrationError,
        run_stage_periods_for_airflow,
    )

    ti = context["ti"]
    plan = _plan_from_xcom(ti)
    period_key = {"raw": "raw_periods", "calc": "calc_periods", "cdm": "cdm_periods"}[
        stage
    ]
    periods = getattr(plan, period_key)
    env_file, client_config = _config_paths_from_context()
    schedule_as_of = _schedule_as_of_from_context(context)
    try:
        return run_stage_periods_for_airflow(
            stage,
            periods,
            mode=plan.mode,
            dry_run=plan.dry_run,
            env_file=env_file,
            client_config=client_config,
            today=schedule_as_of,
        )
    except MonthlyOrchestrationError as exc:
        logger.error(
            "mode=%s stage=%s status=FAILED error=%s",
            plan.mode,
            stage.upper(),
            exc,
        )
        raise


def task_load_raw(**context: Any) -> dict[str, Any]:
    """Adobe → RAW for planned raw_periods (NO-OP when empty / mode=validate)."""
    return _task_run_stage("raw", **context)


def task_build_calc(**context: Any) -> dict[str, Any]:
    """RAW → CALC for planned calc_periods (NO-OP when empty / mode=validate)."""
    return _task_run_stage("calc", **context)


def task_build_cdm(**context: Any) -> dict[str, Any]:
    """CALC → CDM for planned cdm_periods (NO-OP when empty / mode=validate)."""
    return _task_run_stage("cdm", **context)


def task_data_quality(**context: Any) -> dict[str, Any]:
    """Read-only Monthly DWH validation; FAIL task on critical DQ failures."""
    _ensure_package_path()
    from analytics_reporting_platform.orchestration.validation import (
        DataQualityError,
        run_monthly_data_quality_for_airflow,
    )

    ti = context["ti"]
    plan = _plan_from_xcom(ti)
    env_file, _client_config = _config_paths_from_context()
    artifact_root, reference_env = _validation_paths_from_context()
    schedule_as_of = _schedule_as_of_from_context(context)
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id") or "unknown_run"

    try:
        payload = run_monthly_data_quality_for_airflow(
            last_complete_month=plan.last_complete_month,
            run_id=str(run_id),
            dag_id=MONTHLY_DAG_ID,
            env_file=env_file,
            reference_env_file=reference_env,
            artifact_root=artifact_root,
            today=schedule_as_of,
            raise_on_critical_fail=True,
        )
    except DataQualityError as exc:
        logger.error(
            "mode=%s stage=DATA_QUALITY status=FAILED report_end=%s error=%s",
            plan.mode,
            plan.last_complete_month,
            exc,
        )
        raise

    logger.info(
        "mode=%s stage=DATA_QUALITY status=%s report_end=%s "
        "critical_failures=%s warnings=%s",
        plan.mode,
        payload.get("final_status"),
        payload.get("report_end"),
        payload.get("critical_failures"),
        payload.get("warnings"),
    )
    return payload


def task_final_status(**context: Any) -> dict[str, Any]:
    """Orchestration summary after successful upstream stages including DQ."""
    _ensure_package_path()
    ti = context["ti"]
    plan = _plan_from_xcom(ti)
    raw_p = ti.xcom_pull(task_ids="load_raw") or {}
    calc_p = ti.xcom_pull(task_ids="build_calc") or {}
    cdm_p = ti.xcom_pull(task_ids="build_cdm") or {}
    dq_p = ti.xcom_pull(task_ids="data_quality") or {}
    summary = {
        "dag_id": MONTHLY_DAG_ID,
        "status": "success" if not plan.dry_run or plan.mode == "validate" else "planned",
        "mode": plan.mode,
        "dry_run": plan.dry_run,
        "last_complete_month": plan.last_complete_month,
        "raw_periods": list(plan.raw_periods),
        "calc_periods": list(plan.calc_periods),
        "cdm_periods": list(plan.cdm_periods),
        "raw_status": raw_p.get("status"),
        "calc_status": calc_p.get("status"),
        "cdm_status": cdm_p.get("status"),
        "data_quality_status": dq_p.get("final_status") or dq_p.get("status"),
        "validation_md": dq_p.get("validation_md"),
        "validation_json": dq_p.get("validation_json"),
    }
    # Validate mode is always a real read-only check (not a dry-run plan).
    if plan.mode == "validate":
        summary["status"] = (
            "success" if dq_p.get("final_status") == "PASS" else "failed"
        )
    logger.info(
        "mode=%s stage=FINAL_STATUS status=%s dry_run=%s dq=%s",
        summary["mode"],
        summary["status"],
        summary["dry_run"],
        summary["data_quality_status"],
    )
    return summary


default_args = {
    "owner": "analytics-reporting-platform",
    "depends_on_past": False,
    "retries": DEFAULT_RETRIES,
    "retry_delay": DEFAULT_RETRY_DELAY,
}


with DAG(
    dag_id=MONTHLY_DAG_ID,
    description=(
        "Private Monthly Adobe → RAW → CALC → CDM + DWH validation "
        "(incremental / rebuild / validate)"
    ),
    default_args=default_args,
    schedule=MONTHLY_SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=DAG_TZ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["monthly", "private", "arp"],
    params={
        "mode": Param(
            default="incremental",
            enum=["incremental", "rebuild", "validate"],
            description=(
                "incremental=missing months+DQ; rebuild=manual force range+DQ; "
                "validate=read-only DQ only (ETL NO-OP)."
            ),
        ),
        "rebuild_start": Param(
            default="",
            type=["null", "string"],
            description="Required for rebuild. YYYY-MM. Empty for incremental/validate.",
        ),
        "rebuild_end": Param(
            default="",
            type=["null", "string"],
            description="Optional rebuild end YYYY-MM (defaults to rebuild_start).",
        ),
        "dry_run": Param(
            default=True,
            type="boolean",
            description=(
                "Safe default for incremental/rebuild: no Adobe / DB writes until cleared. "
                "Ignored for mode=validate (always read-only DQ)."
            ),
        ),
    },
    doc_md=__doc__,
) as dag:
    determine_periods = PythonOperator(
        task_id="determine_periods",
        python_callable=task_determine_periods,
    )
    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=task_load_raw,
        execution_timeout=STAGE_EXECUTION_TIMEOUT,
    )
    build_calc = PythonOperator(
        task_id="build_calc",
        python_callable=task_build_calc,
        execution_timeout=STAGE_EXECUTION_TIMEOUT,
    )
    build_cdm = PythonOperator(
        task_id="build_cdm",
        python_callable=task_build_cdm,
        execution_timeout=STAGE_EXECUTION_TIMEOUT,
    )
    data_quality = PythonOperator(
        task_id="data_quality",
        python_callable=task_data_quality,
        retries=0,
        execution_timeout=timedelta(hours=2),
    )
    final_status = PythonOperator(
        task_id="final_status",
        python_callable=task_final_status,
        retries=0,
    )

    (
        determine_periods
        >> load_raw
        >> build_calc
        >> build_cdm
        >> data_quality
        >> final_status
    )
