"""Unit tests: Monthly Airflow hardening + stage-isolated DAG (mocks only)."""

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
except ModuleNotFoundError:  # pragma: no cover

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

    pytest = _PytestShim()  # type: ignore[assignment]

from analytics_reporting_platform.cli.monthly import execute_monthly_cli
from analytics_reporting_platform.execution.models import (
    MonthlyExecutionResult,
    StageRunResult,
)
from analytics_reporting_platform.execution.period import PeriodValidationError
from analytics_reporting_platform.orchestration.monthly import (
    MONTHLY_DAG_ID,
    MonthlyAirflowParams,
    MonthlyOrchestrationError,
    SAFE_RESULT_FIELDS,
    SAFE_STAGE_FIELDS,
    resolve_monthly_period,
    run_monthly_for_airflow,
    run_stage_for_airflow,
    safe_result_payload,
    safe_stage_payload,
)
from analytics_reporting_platform.validation.monthly import (
    compare_monthly_signatures,
    validate_monthly_period,
)
from analytics_reporting_platform.validation.signatures import (
    MonthlySignatureBundle,
    TablePeriodSignature,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_PATH = REPO_ROOT / "dags" / "monthly_private_pipeline.py"


def test_scheduled_period_from_schedule_as_of() -> None:
    resolved = resolve_monthly_period(schedule_as_of=date(2026, 9, 1))
    assert resolved.period == date(2026, 8, 1)
    assert resolved.source == "scheduled"


def test_delayed_september_schedule_still_resolves_august() -> None:
    # schedule_as_of fixed to Sept 1 → August even conceptually "later"
    assert resolve_monthly_period(schedule_as_of=date(2026, 9, 1)).period == date(2026, 8, 1)


def test_manual_target_overrides_schedule() -> None:
    resolved = resolve_monthly_period(
        target_month="2026-07",
        mode="incremental",
        schedule_as_of=date(2026, 9, 1),
    )
    assert resolved.period == date(2026, 7, 1)
    assert resolved.source == "manual"
    assert resolved.mode == "incremental"


def test_manual_rebuild_routing() -> None:
    resolved = resolve_monthly_period(
        dag_run_conf={"target_month": "2026-07", "mode": "rebuild"},
        schedule_as_of=date(2026, 9, 1),
    )
    assert resolved.mode == "rebuild"


def test_malformed_and_future_rejected() -> None:
    with pytest.raises(PeriodValidationError):
        resolve_monthly_period(target_month="2026-13", schedule_as_of=date(2026, 9, 1))
    with pytest.raises(PeriodValidationError, match="future"):
        resolve_monthly_period(target_month="2099-01", schedule_as_of=date(2026, 9, 1))


def test_safe_result_and_stage_schemas() -> None:
    result = MonthlyExecutionResult(
        period=date(2026, 8, 1),
        mode="incremental",
        dry_run=False,
        raw_status="success",
        calc_status="failed",
        cdm_status="not_run",
        overall_status="failed",
        failed_stage="calc",
        failed_mart="monthly_kpi",
        error="boom",
    )
    payload = safe_result_payload(result)
    for key in SAFE_RESULT_FIELDS:
        assert key in payload
    stage = StageRunResult(stage="calc", status="failed", error="boom", marts=())
    sp = safe_stage_payload(
        period=date(2026, 8, 1), mode="rebuild", dry_run=False, stage_result=stage
    )
    for key in SAFE_STAGE_FIELDS:
        assert key in sp


def test_stage_api_isolation_calc_does_not_call_raw() -> None:
    resolved = MonthlyAirflowParams(
        period=date(2026, 7, 1), mode="incremental", dry_run=False, source="manual"
    )
    deps = MagicMock()
    deps.on_event = None
    calc_ok = StageRunResult(stage="calc", status="success", marts=())
    with patch(
        "analytics_reporting_platform.execution.monthly.run_monthly_calc",
        return_value=calc_ok,
    ) as calc_mock:
        with patch(
            "analytics_reporting_platform.execution.monthly.run_monthly_raw"
        ) as raw_mock:
            out = run_stage_for_airflow("calc", resolved, deps=deps, today=date(2026, 8, 9))
            raw_mock.assert_not_called()
            calc_mock.assert_called_once()
            assert out["stage"] == "calc"
            assert out["status"] == "success"


def test_stage_api_isolation_cdm_does_not_call_raw_or_calc() -> None:
    resolved = MonthlyAirflowParams(
        period=date(2026, 7, 1), mode="rebuild", dry_run=False, source="manual"
    )
    deps = MagicMock()
    deps.on_event = None
    cdm_ok = StageRunResult(stage="cdm", status="success", marts=())
    with patch(
        "analytics_reporting_platform.execution.monthly.run_monthly_cdm",
        return_value=cdm_ok,
    ) as cdm_mock:
        with patch(
            "analytics_reporting_platform.execution.monthly.run_monthly_raw"
        ) as raw_mock:
            with patch(
                "analytics_reporting_platform.execution.monthly.run_monthly_calc"
            ) as calc_mock:
                out = run_stage_for_airflow(
                    "cdm", resolved, deps=deps, today=date(2026, 8, 9)
                )
                raw_mock.assert_not_called()
                calc_mock.assert_not_called()
                cdm_mock.assert_called_once()
                assert out["status"] == "success"


def test_stage_failure_raises_and_exposes_failed_mart() -> None:
    resolved = MonthlyAirflowParams(
        period=date(2026, 7, 1), mode="incremental", dry_run=False, source="manual"
    )
    deps = MagicMock()
    deps.on_event = None
    from analytics_reporting_platform.execution.models import MartRunResult

    failed = StageRunResult(
        stage="raw",
        status="failed",
        error="extract failed",
        marts=(MartRunResult(mart="monthly_metrics", status="failed"),),
    )
    with patch(
        "analytics_reporting_platform.execution.monthly.run_monthly_raw",
        return_value=failed,
    ):
        with pytest.raises(MonthlyOrchestrationError, match="stage=raw"):
            run_stage_for_airflow("raw", resolved, deps=deps, today=date(2026, 8, 9))


def test_run_monthly_period_composes_stage_apis() -> None:
    deps = MagicMock()
    deps.on_event = None
    ok = StageRunResult(stage="raw", status="success", marts=())
    ok_w = StageRunResult(stage="wordstat_raw", status="success", marts=())
    ok_c = StageRunResult(stage="calc", status="success", marts=())
    ok_d = StageRunResult(stage="cdm", status="success", marts=())
    import analytics_reporting_platform.execution.monthly as monthly_mod

    with patch.object(monthly_mod, "run_monthly_raw", return_value=ok) as r2:
        with patch.object(monthly_mod, "run_monthly_wordstat_raw", return_value=ok_w) as w2:
            with patch.object(monthly_mod, "run_monthly_calc", return_value=ok_c) as c2:
                with patch.object(monthly_mod, "run_monthly_cdm", return_value=ok_d) as d2:
                    result = monthly_mod.run_monthly_period(
                        "2026-07",
                        mode="incremental",
                        deps=deps,
                        today=date(2026, 8, 1),
                    )
                    assert r2.called and w2.called and c2.called and d2.called
                    assert result.overall_status == "success"


def test_validation_pass_missing_dup_scope() -> None:
    target = date(2026, 8, 1)

    def sig(layer: str, mart: str, table: str, *, rows: int, dups: int = 0) -> TablePeriodSignature:
        return TablePeriodSignature(
            layer=layer,  # type: ignore[arg-type]
            mart=mart,
            table=table,
            period=target.isoformat(),
            row_count=rows,
            duplicate_key_groups=dups,
            key_fingerprint="abc",
        )

    tables = [
        sig("raw", "monthly_metrics", "raw_aa_monthly_metrics", rows=10),
        sig("raw", "monthly_by_section", "raw_aa_monthly_by_section", rows=5),
        sig("raw", "monthly_by_url", "raw_aa_monthly_by_url", rows=5),
        sig("raw", "wordstat_brand_queries", "raw_wordstat_brand_queries", rows=2),
        sig("calc", "monthly_kpi", "calc_monthly_kpi", rows=3),
        sig("calc", "monthly_channel_mix", "calc_monthly_channel_mix", rows=3),
        sig("calc", "monthly_search_engines", "calc_monthly_search_engines", rows=3),
        sig("calc", "monthly_section_share", "calc_monthly_section_share", rows=3),
        sig("calc", "explore_buying_guides_kpi", "calc_explore_buying_guides_kpi", rows=3),
        sig("calc", "brand_demand", "calc_brand_demand", rows=1),
        sig("cdm", "dim_month", "cdm_dim_month", rows=1),
        sig("cdm", "traffic_kpi", "cdm_traffic_kpi", rows=2),
        sig("cdm", "overall_traffic_trend", "cdm_overall_traffic_trend", rows=1),
        sig("cdm", "traffic_source_mix", "cdm_traffic_source_mix", rows=2),
        sig("cdm", "geo_traffic_kpi", "cdm_geo_traffic_kpi", rows=1),
        sig("cdm", "geo_traffic_trend", "cdm_geo_traffic_trend", rows=1),
        sig("cdm", "geo_traffic_by_section", "cdm_geo_traffic_by_section", rows=2),
        sig("cdm", "organic_search_kpi", "cdm_organic_search_kpi", rows=2),
        sig("cdm", "organic_search_engine_mix", "cdm_organic_search_engine_mix", rows=2),
        sig("cdm", "organic_search_engine_trend", "cdm_organic_search_engine_trend", rows=1),
        sig("cdm", "organic_brand_demand", "cdm_organic_brand_demand", rows=1),
        sig("cdm", "explore_buying_guides_kpi", "cdm_explore_buying_guides_kpi", rows=2),
        sig("cdm", "explore_buying_guides_trend", "cdm_explore_buying_guides_trend", rows=1),
        sig("cdm", "sections_mix", "cdm_sections_mix", rows=2),
        sig("cdm", "top_pages", "cdm_top_pages", rows=2),
        sig("cdm", "device_section_mix", "cdm_device_section_mix", rows=2),
    ]
    after_ok = MonthlySignatureBundle(
        target_month=target.isoformat(),
        captured_at="t",
        tables=tuple(tables),
        month_counts=(
            {"layer": "raw", "table": "raw_aa_monthly_metrics", "month": "2026-07-01", "row_count": 9},
            {"layer": "raw", "table": "raw_aa_monthly_metrics", "month": "2026-08-01", "row_count": 10},
        ),
    )
    assert (
        validate_monthly_period(
            raw_engine=MagicMock(),
            calc_engine=MagicMock(),
            cdm_engine=MagicMock(),
            month=target,
            after=after_ok,
        ).overall
        == "PASS"
    )

    tables_missing = list(tables)
    tables_missing[0] = sig("raw", "monthly_metrics", "raw_aa_monthly_metrics", rows=0)
    missing = validate_monthly_period(
        raw_engine=MagicMock(),
        calc_engine=MagicMock(),
        cdm_engine=MagicMock(),
        month=target,
        after=MonthlySignatureBundle(
            target_month=target.isoformat(),
            captured_at="t",
            tables=tuple(tables_missing),
            month_counts=after_ok.month_counts,
        ),
    )
    assert missing.overall == "FAIL"
    assert "raw.monthly_metrics.present" in missing.failed_checks

    tables_dup = list(tables)
    tables_dup[0] = sig("raw", "monthly_metrics", "raw_aa_monthly_metrics", rows=10, dups=2)
    dup = validate_monthly_period(
        raw_engine=MagicMock(),
        calc_engine=MagicMock(),
        cdm_engine=MagicMock(),
        month=target,
        after=MonthlySignatureBundle(
            target_month=target.isoformat(),
            captured_at="t",
            tables=tuple(tables_dup),
            month_counts=after_ok.month_counts,
        ),
    )
    assert "dup.raw.monthly_metrics" in dup.failed_checks

    before = MonthlySignatureBundle(
        target_month=target.isoformat(),
        captured_at="t0",
        tables=tuple(tables),
        month_counts=(
            {"layer": "raw", "table": "raw_aa_monthly_metrics", "month": "2026-07-01", "row_count": 9},
            {"layer": "raw", "table": "raw_aa_monthly_metrics", "month": "2026-08-01", "row_count": 0},
        ),
    )
    after_bad = MonthlySignatureBundle(
        target_month=target.isoformat(),
        captured_at="t1",
        tables=tuple(tables),
        month_counts=(
            {"layer": "raw", "table": "raw_aa_monthly_metrics", "month": "2026-07-01", "row_count": 99},
            {"layer": "raw", "table": "raw_aa_monthly_metrics", "month": "2026-08-01", "row_count": 10},
        ),
    )
    assert (
        compare_monthly_signatures(
            before, after_bad, allowed_changed_months=(target.isoformat(),)
        )[0].status
        == "FAIL"
    )


def test_validation_module_is_read_only_surface() -> None:
    import inspect

    import analytics_reporting_platform.validation.signatures as sig_mod

    src = inspect.getsource(sig_mod).upper()
    for banned in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "TRUNCATE "):
        assert banned not in src


def test_cli_validate_exit_codes() -> None:
    assert execute_monthly_cli(["monthly", "validate", "--month", "2026-08"]) != 0
    with patch("analytics_reporting_platform.cli.monthly._build_engines") as engines:
        engines.return_value = (MagicMock(), MagicMock(), MagicMock())
        with patch(
            "analytics_reporting_platform.validation.monthly.validate_monthly_period"
        ) as mocked:
            from analytics_reporting_platform.validation.monthly import (
                MonthlyValidationResult,
                ValidationCheck,
            )

            mocked.return_value = MonthlyValidationResult(
                month=date(2026, 8, 1),
                overall="PASS",
                checks=(ValidationCheck(name="raw.x.present", status="PASS", row_count=1),),
            )
            assert (
                execute_monthly_cli(
                    ["monthly", "validate", "--month", "2026-08", "--env-file", "/tmp/x.env"]
                )
                == 0
            )
            mocked.return_value = MonthlyValidationResult(
                month=date(2026, 8, 1),
                overall="FAIL",
                checks=(ValidationCheck(name="raw.x.present", status="FAIL"),),
            )
            assert (
                execute_monthly_cli(
                    ["monthly", "validate", "--month", "2026-08", "--env-file", "/tmp/x.env"]
                )
                == 1
            )


def _load_dag_module():
    try:
        importlib.import_module("airflow")
        from airflow import DAG  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"airflow not usable: {exc}") from exc
    src = str(REPO_ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    name = "monthly_private_pipeline_under_test"
    if name in sys.modules:
        del sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"airflow DAG module import failed: {exc}") from exc
    return module


def test_dag_graph_timezone_and_stage_chain() -> None:
    try:
        module = _load_dag_module()
    except RuntimeError:
        return
    dag = module.dag
    assert dag.dag_id == MONTHLY_DAG_ID
    assert module.DAG_TIMEZONE_NAME == "Europe/Moscow"
    assert "Moscow" in str(dag.timezone)
    assert dag.start_date is not None and dag.start_date.tzinfo is not None
    assert set(dag.task_ids) == {
        "determine_periods",
        "load_raw",
        "load_wordstat_raw",
        "build_calc",
        "build_cdm",
        "data_quality",
        "final_status",
    }
    assert "load_raw" in dag.get_task("determine_periods").downstream_task_ids
    assert "load_wordstat_raw" in dag.get_task("determine_periods").downstream_task_ids
    assert "build_calc" in dag.get_task("load_raw").downstream_task_ids
    assert "build_calc" in dag.get_task("load_wordstat_raw").downstream_task_ids
    assert "build_cdm" in dag.get_task("build_calc").downstream_task_ids
    assert "data_quality" in dag.get_task("build_cdm").downstream_task_ids
    assert "final_status" in dag.get_task("data_quality").downstream_task_ids
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 2
    assert module.DEFAULT_RETRIES == 0
    assert module.STAGE_EXECUTION_TIMEOUT == timedelta(hours=72)
    dry_param = dag.params["dry_run"]
    assert getattr(dry_param, "value", dry_param) is True


def test_dag_determine_uses_data_interval_end() -> None:
    try:
        module = _load_dag_module()
    except RuntimeError:
        return

    class _DT:
        def __init__(self, year: int, month: int, day: int) -> None:
            self.year = year
            self.month = month
            self.day = day

        def in_timezone(self, tz):  # type: ignore[no-untyped-def]
            return self

    context = {
        "dag_run": types.SimpleNamespace(conf={"mode": "rebuild", "rebuild_start": "2026-03"}),
        "params": {
            "mode": "incremental",
            "rebuild_start": "",
            "rebuild_end": "",
            "dry_run": True,
        },
        "data_interval_end": _DT(2026, 9, 1),
    }
    with patch.object(module, "_config_paths_from_context", return_value=(None, None)):
        payload = module.task_determine_periods(**context)
    assert payload["mode"] == "rebuild"
    assert payload["last_complete_month"] == "2026-08"
    assert payload["raw_periods"] == ["2026-03"]
    assert payload["schedule_as_of"] == "2026-09-01"


def test_dag_build_calc_does_not_invoke_raw_runner() -> None:
    try:
        module = _load_dag_module()
    except RuntimeError:
        return
    ti = MagicMock()
    ti.xcom_pull.return_value = {
        "mode": "incremental",
        "last_complete_month": "2026-07",
        "raw_periods": ["2026-07"],
        "wordstat_periods": ["2026-07"],
        "calc_periods": ["2026-07"],
        "cdm_periods": ["2026-07"],
        "dry_run": True,
        "rebuild_start": None,
        "rebuild_end": None,
    }
    with patch.object(module, "_config_paths_from_context", return_value=(None, None)):
        with patch(
            "analytics_reporting_platform.orchestration.monthly.run_stage_periods_for_airflow",
            return_value={
                "stage": "calc",
                "status": "planned",
                "mode": "incremental",
                "dry_run": True,
                "periods": ["2026-07"],
                "period_results": [],
            },
        ) as stage_mock:
            out = module.task_build_calc(ti=ti)
            assert out["stage"] == "calc"
            assert stage_mock.call_args.args[0] == "calc"


def test_dag_final_status_summary() -> None:
    try:
        module = _load_dag_module()
    except RuntimeError:
        return
    ti = MagicMock()
    ti.xcom_pull.side_effect = lambda task_ids: {
        "determine_periods": {
            "mode": "incremental",
            "last_complete_month": "2026-07",
            "raw_periods": ["2026-07"],
            "wordstat_periods": ["2026-07"],
            "calc_periods": ["2026-07"],
            "cdm_periods": ["2026-07"],
            "dry_run": True,
            "rebuild_start": None,
            "rebuild_end": None,
        },
        "load_raw": {"stage": "raw", "status": "planned"},
        "load_wordstat_raw": {"stage": "wordstat_raw", "status": "planned"},
        "build_calc": {"stage": "calc", "status": "planned"},
        "build_cdm": {"stage": "cdm", "status": "planned"},
        "data_quality": {
            "final_status": "PASS",
            "validation_md": "/tmp/validation.md",
            "validation_json": "/tmp/validation.json",
        },
    }.get(task_ids, {})
    payload = module.task_final_status(ti=ti)
    assert payload["dag_id"] == MONTHLY_DAG_ID
    assert payload["status"] == "planned"
    assert payload["raw_status"] == "planned"
    assert payload["data_quality_status"] == "PASS"
    assert payload["validation_md"] == "/tmp/validation.md"


if __name__ == "__main__":
    test_scheduled_period_from_schedule_as_of()
    test_delayed_september_schedule_still_resolves_august()
    test_manual_target_overrides_schedule()
    test_manual_rebuild_routing()
    test_malformed_and_future_rejected()
    test_safe_result_and_stage_schemas()
    test_stage_api_isolation_calc_does_not_call_raw()
    test_stage_api_isolation_cdm_does_not_call_raw_or_calc()
    test_stage_failure_raises_and_exposes_failed_mart()
    test_run_monthly_period_composes_stage_apis()
    test_validation_pass_missing_dup_scope()
    test_validation_module_is_read_only_surface()
    test_cli_validate_exit_codes()
    test_dag_graph_timezone_and_stage_chain()
    test_dag_determine_uses_data_interval_end()
    test_dag_build_calc_does_not_invoke_raw_runner()
    test_dag_final_status_summary()
    print("OK")
