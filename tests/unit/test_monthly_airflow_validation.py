"""Non-live tests for Airflow Monthly DWH validation productization."""

from __future__ import annotations

import ast
import importlib.util
import types
from datetime import date, datetime
from pathlib import Path
from unittest import mock

import pytest

from analytics_reporting_platform.execution.monthly_plan import (
    plan_monthly_periods,
    plan_validate_periods,
)
from analytics_reporting_platform.execution.period import last_completed_month
from analytics_reporting_platform.orchestration.validation import (
    DataQualityError,
    resolve_validation_report_end,
    run_monthly_data_quality_for_airflow,
)
from analytics_reporting_platform.validation.artifacts import (
    resolve_validation_artifact_root,
    sanitize_run_id,
    validation_run_dir,
)
from analytics_reporting_platform.validation.notify import NoOpNotifier, safe_notify
from analytics_reporting_platform.validation.report import (
    VALIDATION_JSON_NAME,
    VALIDATION_MD_NAME,
    format_airflow_validation_summary,
    write_suite_reports,
)
from analytics_reporting_platform.validation.types import (
    CategoryResult,
    SuiteCheck,
    SuiteResult,
)

REPO = Path(__file__).resolve().parents[2]
DAG_PATH = REPO / "dags" / "monthly_private_pipeline.py"
TODAY = date(2026, 8, 10)


def _load_dag_module():
    """Import DAG module with airflow available (skip if missing/broken)."""
    import sys

    pytest.importorskip("airflow")
    try:
        from airflow import DAG  # noqa: F401
    except ImportError as exc:
        pytest.skip(f"airflow DAG import unavailable: {exc}")
    src = str(REPO / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    module_name = "monthly_private_pipeline_dq_under_test"
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


def _suite_result(
    *,
    final_status: str = "PASS",
    warn: bool = False,
    fail: bool = False,
) -> SuiteResult:
    checks: list[SuiteCheck] = [
        SuiteCheck(category="coverage", name="coverage.ok", status="PASS"),
        SuiteCheck(category="contracts", name="contracts.ok", status="PASS"),
        SuiteCheck(category="uniqueness", name="uniqueness.ok", status="PASS"),
        SuiteCheck(category="value_ranges", name="value_ranges.ok", status="PASS"),
        SuiteCheck(
            category="raw_calc_reconciliation",
            name="recon.raw_calc.ok",
            status="PASS",
        ),
        SuiteCheck(
            category="calc_cdm_reconciliation",
            name="recon.calc_cdm.ok",
            status="WARN" if warn else ("FAIL" if fail else "PASS"),
            critical=bool(fail),
            detail="synthetic" if (warn or fail) else None,
        ),
        SuiteCheck(
            category="report_contract", name="report_contract.ok", status="PASS"
        ),
        SuiteCheck(category="monthly_only", name="monthly_only.ok", status="PASS"),
        SuiteCheck(
            category="reference_comparison",
            name="reference.ok",
            status="PASS",
            critical=False,
        ),
    ]
    if fail:
        final_status = "FAIL"
    elif warn and final_status == "PASS":
        final_status = "PASS"
    cats = []
    by_cat: dict[str, list[SuiteCheck]] = {}
    for c in checks:
        by_cat.setdefault(c.category, []).append(c)
    for name, items in by_cat.items():
        status = "PASS"
        if any(i.status == "FAIL" for i in items):
            status = "FAIL"
        elif any(i.status == "WARN" for i in items):
            status = "WARN"
        cats.append(
            CategoryResult(
                name=name,  # type: ignore[arg-type]
                status=status,  # type: ignore[arg-type]
                passed=sum(1 for i in items if i.status == "PASS"),
                total=len(items),
                checks=items,
            )
        )
    return SuiteResult(
        final_status=final_status,  # type: ignore[arg-type]
        report_end="2026-07",
        categories=cats,
        generated_at=datetime.utcnow().isoformat() + "Z",
        read_only=True,
        execution_port=15433,
        reference_port=None,
    )


def test_dag_graph_includes_data_quality() -> None:
    module = _load_dag_module()
    dag = module.dag
    assert set(dag.task_ids) == {
        "determine_periods",
        "load_raw",
        "build_calc",
        "build_cdm",
        "data_quality",
        "final_status",
    }
    assert "load_raw" in dag.get_task("determine_periods").downstream_task_ids
    assert "build_calc" in dag.get_task("load_raw").downstream_task_ids
    assert "build_cdm" in dag.get_task("build_calc").downstream_task_ids
    assert "data_quality" in dag.get_task("build_cdm").downstream_task_ids
    assert "final_status" in dag.get_task("data_quality").downstream_task_ids
    assert "weekly" not in dag.dag_id.lower()
    source = DAG_PATH.read_text()
    assert "weekly_" not in source.lower()


def test_dag_ast_task_order_and_validate_mode() -> None:
    source = DAG_PATH.read_text()
    tree = ast.parse(source)
    task_ids: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                    task_ids.append(str(kw.value.value))
    assert task_ids == [
        "determine_periods",
        "load_raw",
        "build_calc",
        "build_cdm",
        "data_quality",
        "final_status",
    ]
    assert "validate" in source
    assert '"validate"' in source or "'validate'" in source


def test_plan_validate_is_etl_noop() -> None:
    plan = plan_validate_periods(today=TODAY, dry_run=True)
    assert plan.mode == "validate"
    assert plan.raw_periods == ()
    assert plan.calc_periods == ()
    assert plan.cdm_periods == ()
    assert plan.last_complete_month == "2026-07"


def test_plan_monthly_periods_validate_mode() -> None:
    plan = plan_monthly_periods(mode="validate", today=TODAY, dry_run=False)
    assert plan.mode == "validate"
    assert plan.raw_periods == ()
    assert plan.cdm_periods == ()


def test_report_end_never_current_incomplete_month() -> None:
    # On 2026-08-10 last complete is 2026-07
    assert last_completed_month(TODAY) == date(2026, 7, 1)
    end = resolve_validation_report_end(last_complete_month="2026-07", today=TODAY)
    assert end == date(2026, 7, 1)
    with pytest.raises(DataQualityError):
        resolve_validation_report_end(last_complete_month="2026-08", today=TODAY)


def test_sanitize_run_id_and_unique_dirs(tmp_path: Path) -> None:
    a = validation_run_dir(
        run_id="manual__2026-08-10T05:00:00+00:00",
        root=tmp_path,
    )
    b = validation_run_dir(
        run_id="manual__2026-08-10T06:00:00+00:00",
        root=tmp_path,
    )
    assert a != b
    assert a.parent == b.parent
    assert "/" not in sanitize_run_id("a/b\\c d")
    assert sanitize_run_id("ok-run_1") == "ok-run_1"


def test_artifact_root_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARP_VALIDATION_ARTIFACT_ROOT", str(tmp_path / "val"))
    root = resolve_validation_artifact_root()
    assert root == tmp_path / "val"


def test_write_artifacts_md_json(tmp_path: Path) -> None:
    result = _suite_result()
    json_path, md_path = write_suite_reports(result, tmp_path / "run1")
    assert json_path.name == VALIDATION_JSON_NAME
    assert md_path.name == VALIDATION_MD_NAME
    assert json_path.is_file()
    assert md_path.is_file()
    # unique runs do not overwrite
    json2, md2 = write_suite_reports(result, tmp_path / "run2")
    assert json2 != json_path
    assert md2 != md_path


def test_airflow_summary_contains_paths_and_no_secrets() -> None:
    result = _suite_result(warn=True)
    text = format_airflow_validation_summary(
        result,
        md_path=Path("/tmp/demo/validation.md"),
        json_path=Path("/tmp/demo/validation.json"),
    )
    assert "MONTHLY DWH VALIDATION" in text
    assert "Report period: 2026-07" in text
    assert "FINAL STATUS: PASS" in text
    assert "Validation report:" in text
    assert "/tmp/demo/validation.md" in text
    assert "Machine-readable result:" in text
    assert "/tmp/demo/validation.json" in text
    for banned in ("CLIENT_SECRET", "token", "https://", "page_url="):
        assert banned.lower() not in text.lower() or banned == "token" and "token" not in text


def test_noop_notifier_no_network() -> None:
    called = {"n": 0}

    class Boom(NoOpNotifier):
        def notify(self, result, *, artifact_dir, md_path=None, json_path=None):
            called["n"] += 1
            raise RuntimeError("network boom")

    safe_notify(Boom(), _suite_result(), artifact_dir=Path("/tmp"))
    assert called["n"] == 1  # invoked but swallowed


def test_data_quality_pass_warn_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARP_VALIDATION_ARTIFACT_ROOT", str(tmp_path))

    with mock.patch(
        "analytics_reporting_platform.config.settings.load_settings",
        return_value=mock.Mock(),
    ), mock.patch(
        "analytics_reporting_platform.db.engine.create_db_engine",
        return_value=mock.Mock(),
    ), mock.patch(
        "analytics_reporting_platform.orchestration.validation.run_monthly_dwh_validation_suite",
        return_value=_suite_result(),
    ):
        payload = run_monthly_data_quality_for_airflow(
            last_complete_month="2026-07",
            run_id="manual__pass",
            env_file="/tmp/fake.env",
            artifact_root=str(tmp_path),
            today=TODAY,
        )
    assert payload["final_status"] == "PASS"
    assert Path(payload["validation_md"]).is_file()
    assert Path(payload["validation_json"]).is_file()
    assert "validation.md" in payload["validation_md"]

    with mock.patch(
        "analytics_reporting_platform.config.settings.load_settings",
        return_value=mock.Mock(),
    ), mock.patch(
        "analytics_reporting_platform.db.engine.create_db_engine",
        return_value=mock.Mock(),
    ), mock.patch(
        "analytics_reporting_platform.orchestration.validation.run_monthly_dwh_validation_suite",
        return_value=_suite_result(warn=True),
    ):
        warn_payload = run_monthly_data_quality_for_airflow(
            last_complete_month="2026-07",
            run_id="manual__warn",
            env_file="/tmp/fake.env",
            artifact_root=str(tmp_path),
            today=TODAY,
        )
    assert warn_payload["final_status"] == "PASS"
    assert warn_payload["warnings"] >= 1

    with mock.patch(
        "analytics_reporting_platform.config.settings.load_settings",
        return_value=mock.Mock(),
    ), mock.patch(
        "analytics_reporting_platform.db.engine.create_db_engine",
        return_value=mock.Mock(),
    ), mock.patch(
        "analytics_reporting_platform.orchestration.validation.run_monthly_dwh_validation_suite",
        return_value=_suite_result(fail=True),
    ):
        with pytest.raises(DataQualityError):
            run_monthly_data_quality_for_airflow(
                last_complete_month="2026-07",
                run_id="manual__fail",
                env_file="/tmp/fake.env",
                artifact_root=str(tmp_path),
                today=TODAY,
            )


def test_validate_mode_stages_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_dag_module()
    plan = plan_validate_periods(today=TODAY, dry_run=False)
    ti = mock.Mock()
    ti.xcom_pull.return_value = plan.to_safe_dict()
    context = {
        "ti": ti,
        "dag_run": types.SimpleNamespace(conf={"mode": "validate"}, run_id="manual__v"),
        "params": {"mode": "validate"},
        "data_interval_end": None,
    }
    with mock.patch.object(
        module,
        "_config_paths_from_context",
        return_value=(None, None),
    ), mock.patch(
        "analytics_reporting_platform.orchestration.monthly.run_stage_periods_for_airflow",
        side_effect=lambda stage, periods, **kwargs: {
            "stage": stage,
            "status": "success",
            "mode": "validate",
            "dry_run": False,
            "periods": list(periods),
            "period_results": [],
            "detail": "no-op",
        },
    ) as stage_mock:
        raw = module.task_load_raw(**context)
        calc = module.task_build_calc(**context)
        cdm = module.task_build_cdm(**context)
    assert raw["periods"] == []
    assert calc["periods"] == []
    assert cdm["periods"] == []
    assert stage_mock.call_count == 3
    for call in stage_mock.call_args_list:
        assert list(call.args[1]) == []


def test_data_quality_task_calls_suite(tmp_path: Path) -> None:
    module = _load_dag_module()
    plan = plan_validate_periods(today=TODAY, dry_run=False)
    ti = mock.Mock()
    ti.xcom_pull.return_value = plan.to_safe_dict()
    context = {
        "ti": ti,
        "dag_run": types.SimpleNamespace(conf={"mode": "validate"}, run_id="manual__dq"),
        "params": {"mode": "validate"},
        "data_interval_end": None,
        "run_id": "manual__dq",
    }
    expected = {
        "stage": "data_quality",
        "status": "success",
        "final_status": "PASS",
        "report_end": "2026-07",
        "validation_md": str(tmp_path / "validation.md"),
        "validation_json": str(tmp_path / "validation.json"),
        "critical_failures": 0,
        "warnings": 0,
    }
    with mock.patch.object(
        module, "_config_paths_from_context", return_value=("/tmp/env", None)
    ), mock.patch.object(
        module, "_validation_paths_from_context", return_value=(str(tmp_path), None)
    ), mock.patch(
        "analytics_reporting_platform.orchestration.validation.run_monthly_data_quality_for_airflow",
        return_value=expected,
    ) as dq:
        payload = module.task_data_quality(**context)
    assert payload["final_status"] == "PASS"
    assert dq.called
    kwargs = dq.call_args.kwargs
    assert kwargs["last_complete_month"] == "2026-07"
    assert kwargs["raise_on_critical_fail"] is True


def test_final_status_includes_dq_and_blocks_conceptually() -> None:
    """Critical FAIL raises in data_quality; final_status only sees success payloads."""
    module = _load_dag_module()
    ti = mock.Mock()

    def _pull(task_ids):
        return {
            "determine_periods": plan_validate_periods(today=TODAY).to_safe_dict(),
            "load_raw": {"status": "success", "detail": "no-op"},
            "build_calc": {"status": "success", "detail": "no-op"},
            "build_cdm": {"status": "success", "detail": "no-op"},
            "data_quality": {
                "final_status": "PASS",
                "validation_md": "/tmp/validation.md",
                "validation_json": "/tmp/validation.json",
            },
        }.get(task_ids, {})

    ti.xcom_pull.side_effect = _pull
    payload = module.task_final_status(ti=ti)
    assert payload["data_quality_status"] == "PASS"
    assert payload["validation_md"] == "/tmp/validation.md"
    assert payload["mode"] == "validate"
    assert payload["status"] == "success"


def test_cli_validate_suite_uses_same_engine() -> None:
    from analytics_reporting_platform.cli import monthly as cli

    src = Path(cli.__file__).read_text()
    assert "run_monthly_dwh_validation_suite" in src
    assert "VALIDATION_JSON_NAME" in src or "validation.json" in src
