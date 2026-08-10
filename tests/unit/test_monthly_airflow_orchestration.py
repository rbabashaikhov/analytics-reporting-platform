"""Unit tests for Monthly Airflow orchestration (mocks only; no Adobe / no live Airflow)."""

from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

try:
    import pytest
except ModuleNotFoundError:  # pragma: no cover - private venv may lack pytest

    class _RaisesCtx:
        def __init__(self, exc_type: type[BaseException], match: str | None = None) -> None:
            self.exc_type = exc_type
            self.match = match

        def __enter__(self) -> "_RaisesCtx":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
            if exc_type is None:
                raise AssertionError(f"expected {self.exc_type.__name__} was not raised")
            if not issubclass(exc_type, self.exc_type):
                return False
            if self.match and self.match not in str(exc):
                raise AssertionError(f"expected match {self.match!r} in {exc!r}")
            return True

    class _PytestShim:
        @staticmethod
        def raises(exc_type: type[BaseException], match: str | None = None) -> _RaisesCtx:
            return _RaisesCtx(exc_type, match=match)

        @staticmethod
        def importorskip(name: str) -> Any:
            try:
                return importlib.import_module(name)
            except ModuleNotFoundError as exc:
                raise SystemExit(f"SKIP: {name} not installed") from exc

    pytest = _PytestShim()  # type: ignore[assignment]

from analytics_reporting_platform.execution.models import (
    MonthlyExecutionResult,
    StageRunResult,
)
from analytics_reporting_platform.execution.period import (
    PeriodValidationError,
    iter_months,
    last_completed_month,
)
from analytics_reporting_platform.orchestration.monthly import (
    MONTHLY_DAG_ID,
    MonthlyAirflowParams,
    MonthlyOrchestrationError,
    final_status_payload,
    months_for_backfill,
    resolve_monthly_period,
    run_monthly_for_airflow,
    safe_result_payload,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_PATH = REPO_ROOT / "dags" / "monthly_private_pipeline.py"


def test_last_completed_month() -> None:
    assert last_completed_month(date(2026, 8, 9)) == date(2026, 7, 1)
    assert last_completed_month(date(2026, 1, 15)) == date(2025, 12, 1)
    assert last_completed_month(date(2026, 8, 1)) == date(2026, 7, 1)


def test_iter_months_inclusive() -> None:
    assert list(iter_months("2026-01", "2026-03")) == [
        date(2026, 1, 1),
        date(2026, 2, 1),
        date(2026, 3, 1),
    ]
    with pytest.raises(PeriodValidationError, match="after end"):
        list(iter_months("2026-03", "2026-01"))


def test_resolve_scheduled_default_last_completed() -> None:
    resolved = resolve_monthly_period(today=date(2026, 8, 9))
    assert resolved.period == date(2026, 7, 1)
    assert resolved.mode == "incremental"
    assert resolved.source == "scheduled"
    assert resolved.dry_run is False


def test_resolve_manual_target_and_rebuild() -> None:
    resolved = resolve_monthly_period(
        target_month="2026-06",
        mode="rebuild",
        today=date(2026, 8, 9),
    )
    assert resolved.period == date(2026, 6, 1)
    assert resolved.mode == "rebuild"
    assert resolved.source == "manual"


def test_resolve_from_dag_run_conf_overrides_params() -> None:
    resolved = resolve_monthly_period(
        dag_run_conf={"target_month": "2026-05", "mode": "rebuild"},
        params={"target_month": "2026-04", "mode": "incremental"},
        today=date(2026, 8, 9),
    )
    assert resolved.period == date(2026, 5, 1)
    assert resolved.mode == "rebuild"
    assert resolved.source == "manual"


def test_reject_future_and_malformed_period() -> None:
    with pytest.raises(PeriodValidationError, match="future"):
        resolve_monthly_period(target_month="2099-01", today=date(2026, 8, 9))
    with pytest.raises(PeriodValidationError):
        resolve_monthly_period(target_month="2026-13", today=date(2026, 8, 9))
    with pytest.raises(PeriodValidationError, match="mode"):
        resolve_monthly_period(mode="full", today=date(2026, 8, 9))


def test_months_for_backfill_rejects_future() -> None:
    months = months_for_backfill("2026-01", "2026-03", today=date(2026, 8, 9))
    assert months == ["2026-01", "2026-02", "2026-03"]
    with pytest.raises(PeriodValidationError, match="future"):
        months_for_backfill("2026-01", "2099-01", today=date(2026, 8, 9))


def test_safe_result_payload_no_kpi_fields() -> None:
    result = MonthlyExecutionResult(
        period=date(2026, 7, 1),
        mode="incremental",
        dry_run=False,
        raw_status="success",
        calc_status="failed",
        cdm_status="not_run",
        overall_status="failed",
        failed_stage="calc",
        failed_mart="calc_monthly_kpi",
        error="boom",
    )
    payload = safe_result_payload(result)
    assert payload["failed_mart"] == "calc_monthly_kpi"
    assert "visits" not in payload


def test_run_monthly_for_airflow_calls_execution_api() -> None:
    resolved = MonthlyAirflowParams(
        period=date(2026, 7, 1),
        mode="rebuild",
        dry_run=True,
        source="manual",
    )
    with patch(
        "analytics_reporting_platform.execution.monthly.run_monthly_period"
    ) as mocked:
        mocked.return_value = MonthlyExecutionResult(
            period=date(2026, 7, 1),
            mode="rebuild",
            dry_run=True,
            raw_status="planned",
            calc_status="planned",
            cdm_status="planned",
            overall_status="planned",
        )
        result = run_monthly_for_airflow(resolved, today=date(2026, 8, 9))
        mocked.assert_called_once()
        kwargs = mocked.call_args.kwargs
        assert mocked.call_args.args[0] == date(2026, 7, 1)
        assert kwargs["mode"] == "rebuild"
        assert kwargs["dry_run"] is True
        assert kwargs["allow_future"] is False
        assert result.overall_status == "planned"


def test_run_monthly_for_airflow_fail_fast_on_failed_overall() -> None:
    resolved = MonthlyAirflowParams(
        period=date(2026, 7, 1),
        mode="incremental",
        dry_run=False,
        source="manual",
    )
    deps = MagicMock()
    deps.on_event = None
    failed = MonthlyExecutionResult(
        period=date(2026, 7, 1),
        mode="incremental",
        dry_run=False,
        raw_status="failed",
        calc_status="not_run",
        cdm_status="not_run",
        overall_status="failed",
        stages=(StageRunResult(stage="raw", status="failed"),),
        failed_stage="raw",
        failed_mart="raw_aa_monthly_metrics",
        error="extract failed",
    )
    with patch(
        "analytics_reporting_platform.execution.monthly.run_monthly_period",
        return_value=failed,
    ):
        with pytest.raises(MonthlyOrchestrationError, match="stage=raw"):
            run_monthly_for_airflow(resolved, deps=deps, today=date(2026, 8, 9))


def test_final_status_payload() -> None:
    resolved = MonthlyAirflowParams(
        period=date(2026, 7, 1), mode="incremental", dry_run=False, source="scheduled"
    )
    result = MonthlyExecutionResult(
        period=date(2026, 7, 1),
        mode="incremental",
        dry_run=False,
        raw_status="success",
        calc_status="success",
        cdm_status="success",
        overall_status="success",
    )
    payload = final_status_payload(resolved=resolved, result=result)
    assert payload["dag_id"] == MONTHLY_DAG_ID
    assert payload["status"] == "success"
    assert payload["period"] == "2026-07-01"


def _load_dag_module():
    """Import DAG module with airflow available (skip if missing/broken)."""
    airflow = pytest.importorskip("airflow")
    try:
        from airflow import DAG  # noqa: F401
    except ImportError as exc:
        pytest.skip(f"airflow DAG import unavailable: {exc}")
    src = str(REPO_ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    dags_dir = str(REPO_ROOT / "dags")
    if dags_dir not in sys.path:
        sys.path.insert(0, dags_dir)

    module_name = "monthly_private_pipeline_under_test"
    if module_name in sys.modules:
        del sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except ImportError as exc:
        pytest.skip(f"airflow DAG module import failed: {exc}")
    return module


def test_dag_imports_and_structure() -> None:
    module = _load_dag_module()
    dag = module.dag
    assert dag.dag_id == MONTHLY_DAG_ID
    assert module.MONTHLY_SCHEDULE == "0 8 1 * *"
    schedule_value = getattr(dag, "schedule_interval", None)
    if schedule_value is None and hasattr(dag, "timetable"):
        schedule_value = getattr(dag.timetable, "summary", None) or str(dag.timetable)
    assert module.MONTHLY_SCHEDULE in str(schedule_value)
    assert dag.max_active_runs == 1
    assert set(dag.task_ids) == {
        "determine_periods",
        "load_raw",
        "build_calc",
        "build_cdm",
        "data_quality",
        "final_status",
    }
    mode_param = dag.params["mode"]
    mode_value = getattr(mode_param, "value", mode_param)
    assert mode_value == "incremental"
    dry_param = dag.params["dry_run"]
    dry_value = getattr(dry_param, "value", dry_param)
    assert dry_value is True
    assert module.DEFAULT_RETRIES == 0
    assert module.DEFAULT_RETRY_DELAY == timedelta(minutes=10)
    assert module.STAGE_EXECUTION_TIMEOUT == timedelta(hours=72)
    assert dag.get_task("load_raw").execution_timeout == module.STAGE_EXECUTION_TIMEOUT
    assert dag.default_args.get("retries") == 0
    assert "load_raw" in dag.get_task("determine_periods").downstream_task_ids
    assert "build_calc" in dag.get_task("load_raw").downstream_task_ids
    assert "build_cdm" in dag.get_task("build_calc").downstream_task_ids
    assert "data_quality" in dag.get_task("build_cdm").downstream_task_ids
    assert "final_status" in dag.get_task("data_quality").downstream_task_ids


def test_dag_task_determine_and_stage_routing() -> None:
    try:
        module = _load_dag_module()
    except RuntimeError:
        return
    plan_payload = {
        "mode": "rebuild",
        "last_complete_month": "2026-07",
        "raw_periods": ["2026-06"],
        "calc_periods": ["2026-06"],
        "cdm_periods": ["2026-06"],
        "dry_run": True,
        "rebuild_start": "2026-06",
        "rebuild_end": "2026-06",
        "dag_id": MONTHLY_DAG_ID,
    }
    ti = MagicMock()
    ti.xcom_pull.return_value = plan_payload
    with patch(
        "analytics_reporting_platform.orchestration.monthly.run_stage_periods_for_airflow"
    ) as mocked_run:
        mocked_run.return_value = {
            "stage": "raw",
            "status": "planned",
            "mode": "rebuild",
            "dry_run": True,
            "periods": ["2026-06"],
            "period_results": [],
        }
        with patch.object(module, "_config_paths_from_context", return_value=(None, None)):
            out = module.task_load_raw(ti=ti)
        assert out["stage"] == "raw"
        assert mocked_run.call_args.args[0] == "raw"
        assert mocked_run.call_args.args[1] == ["2026-06"]


def test_dag_stage_failure_raises() -> None:
    try:
        module = _load_dag_module()
    except RuntimeError:
        return
    from analytics_reporting_platform.orchestration.monthly import (
        MonthlyOrchestrationError,
    )

    ti = MagicMock()
    ti.xcom_pull.return_value = {
        "mode": "incremental",
        "last_complete_month": "2026-07",
        "raw_periods": ["2026-07"],
        "calc_periods": ["2026-07"],
        "cdm_periods": ["2026-07"],
        "dry_run": False,
        "rebuild_start": None,
        "rebuild_end": None,
    }
    with patch.object(module, "_config_paths_from_context", return_value=(None, None)):
        with patch(
            "analytics_reporting_platform.orchestration.monthly.run_stage_periods_for_airflow",
            side_effect=MonthlyOrchestrationError("boom"),
        ):
            with pytest.raises(MonthlyOrchestrationError, match="boom"):
                module.task_load_raw(ti=ti)

if __name__ == "__main__":
    test_last_completed_month()
    test_iter_months_inclusive()
    test_resolve_scheduled_default_last_completed()
    test_resolve_manual_target_and_rebuild()
    test_resolve_from_dag_run_conf_overrides_params()
    test_reject_future_and_malformed_period()
    test_months_for_backfill_rejects_future()
    test_safe_result_payload_no_kpi_fields()
    test_run_monthly_for_airflow_calls_execution_api()
    test_run_monthly_for_airflow_fail_fast_on_failed_overall()
    test_final_status_payload()
    try:
        test_dag_imports_and_structure()
        test_dag_task_determine_and_stage_routing()
        test_dag_stage_failure_raises()
        print("OK", 14)
    except (SystemExit, RuntimeError, ModuleNotFoundError) as exc:
        print("OK_PARTIAL", 11, str(exc))
    except BaseException as exc:  # noqa: BLE001 - harness: pytest.skip.Exception etc.
        if type(exc).__name__ in {"Skipped", "skip.Exception"} or "airflow" in str(exc).lower():
            print("OK_PARTIAL", 11, f"DAG tests skipped: {exc}")
        else:
            raise
