"""Unit tests for dynamic Monthly period planning (no Adobe / no PostgreSQL writes)."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Callable
from unittest.mock import patch

import pytest

from analytics_reporting_platform.execution.monthly_plan import (
    DEFAULT_RAW_START,
    DEFAULT_REPORTING_START,
    MonthlyPeriodsPlan,
    plan_from_safe_dict,
    plan_incremental_periods,
    plan_monthly_periods,
    plan_rebuild_periods,
    reject_incomplete_month,
)
from analytics_reporting_platform.execution.period import PeriodValidationError
from analytics_reporting_platform.orchestration.monthly import (
    MonthlyOrchestrationError,
    run_stage_periods_for_airflow,
)


TODAY = date(2026, 8, 10)


def _present_set(present: set[str]) -> Callable[[date], bool]:
    return lambda month: month.strftime("%Y-%m") in present


def test_platform_bounds() -> None:
    assert DEFAULT_RAW_START == date(2025, 1, 1)
    assert DEFAULT_REPORTING_START == date(2026, 1, 1)


def test_last_complete_month_guard() -> None:
    assert reject_incomplete_month("2026-07", today=TODAY) == date(2026, 7, 1)
    with pytest.raises(PeriodValidationError, match="Incomplete or future"):
        reject_incomplete_month("2026-08", today=TODAY)
    with pytest.raises(PeriodValidationError, match="Incomplete or future"):
        reject_incomplete_month("2026-09", today=TODAY)


def test_incremental_clean_db_full_init_plan() -> None:
    """Clean DB on 2026-08-10 → RAW/Wordstat/CALC/CDM Brand Demand 2025-01..2026-07."""
    plan = plan_incremental_periods(
        today=TODAY,
        dry_run=True,
        raw_present=_present_set(set()),
        wordstat_present=_present_set(set()),
        calc_present=_present_set(set()),
        cdm_present=_present_set(set()),
    )
    assert plan.mode == "incremental"
    assert plan.last_complete_month == "2026-07"
    assert plan.raw_periods[0] == "2025-01"
    assert plan.raw_periods[-1] == "2026-07"
    assert len(plan.raw_periods) == 19
    assert plan.wordstat_periods == plan.raw_periods
    assert plan.calc_periods == plan.raw_periods
    assert plan.cdm_periods == plan.calc_periods
    assert all(not p.startswith("2024") for p in plan.raw_periods)
    assert "2026-08" not in plan.raw_periods


def test_incremental_future_only_aug_when_filled_to_jul() -> None:
    """DB filled through 2026-07; as-of September → only 2026-08 missing."""
    filled = {f"2025-{m:02d}" for m in range(1, 13)} | {
        f"2026-{m:02d}" for m in range(1, 8)
    }
    plan = plan_incremental_periods(
        today=date(2026, 9, 10),
        dry_run=True,
        raw_present=_present_set(filled),
        wordstat_present=_present_set(filled),
        calc_present=_present_set(filled),
        cdm_present=_present_set(filled),
    )
    assert plan.last_complete_month == "2026-08"
    assert plan.raw_periods == ("2026-08",)
    assert plan.wordstat_periods == ("2026-08",)
    assert plan.calc_periods == ("2026-08",)
    assert plan.cdm_periods == ("2026-08",)


def test_incremental_noop_when_complete() -> None:
    filled = {f"2025-{m:02d}" for m in range(1, 13)} | {
        f"2026-{m:02d}" for m in range(1, 8)
    }
    plan = plan_incremental_periods(
        today=TODAY,
        dry_run=True,
        raw_present=_present_set(filled),
        wordstat_present=_present_set(filled),
        calc_present=_present_set(filled),
        cdm_present=_present_set(filled),
    )
    assert plan.raw_periods == ()
    assert plan.wordstat_periods == ()
    assert plan.calc_periods == ()
    assert plan.cdm_periods == ()


def test_rebuild_single_month_2026() -> None:
    plan = plan_rebuild_periods(
        rebuild_start="2026-03",
        today=TODAY,
        dry_run=True,
    )
    assert plan.mode == "rebuild"
    assert plan.rebuild_start == "2026-03"
    assert plan.rebuild_end == "2026-03"
    assert plan.raw_periods == ("2026-03",)
    assert plan.wordstat_periods == ("2026-03",)
    assert plan.calc_periods == ("2026-03",)
    assert plan.cdm_periods == ("2026-03",)


def test_rebuild_range_across_boundary() -> None:
    plan = plan_rebuild_periods(
        rebuild_start="2025-11",
        rebuild_end="2026-02",
        today=TODAY,
        dry_run=True,
    )
    assert plan.raw_periods == ("2025-11", "2025-12", "2026-01", "2026-02")
    assert plan.wordstat_periods == plan.raw_periods
    assert plan.calc_periods == plan.raw_periods
    assert plan.cdm_periods == plan.raw_periods


def test_rebuild_raw_only_2025_range() -> None:
    plan = plan_rebuild_periods(
        rebuild_start="2025-05",
        rebuild_end="2025-08",
        today=TODAY,
        dry_run=True,
    )
    assert plan.raw_periods == ("2025-05", "2025-06", "2025-07", "2025-08")
    assert plan.wordstat_periods == plan.raw_periods
    assert plan.calc_periods == plan.raw_periods
    assert plan.cdm_periods == plan.raw_periods


def test_rebuild_rejects_current_month() -> None:
    with pytest.raises(PeriodValidationError, match="Incomplete or future"):
        plan_rebuild_periods(rebuild_start="2026-08", today=TODAY, dry_run=True)


def test_rebuild_requires_start() -> None:
    with pytest.raises(PeriodValidationError, match="rebuild_start"):
        plan_monthly_periods(mode="rebuild", today=TODAY, dry_run=True)


def test_rebuild_rejects_before_2025() -> None:
    with pytest.raises(PeriodValidationError, match="before 2025-01"):
        plan_rebuild_periods(rebuild_start="2024-12", today=TODAY, dry_run=True)


def test_incremental_ignores_rebuild_params() -> None:
    plan = plan_monthly_periods(
        mode="incremental",
        rebuild_start="2026-03",
        rebuild_end="2026-06",
        today=TODAY,
        dry_run=True,
        raw_present=_present_set(set()),
        wordstat_present=_present_set(set()),
        calc_present=_present_set(set()),
        cdm_present=_present_set(set()),
    )
    assert plan.mode == "incremental"
    assert plan.rebuild_start is None
    assert len(plan.raw_periods) == 19


def test_default_mode_and_dry_run() -> None:
    plan = plan_monthly_periods(
        today=TODAY,
        raw_present=_present_set({"2025-01"}),
        wordstat_present=_present_set({"2025-01"}),
        calc_present=_present_set(set()),
        cdm_present=_present_set(set()),
    )
    assert plan.mode == "incremental"
    assert plan.dry_run is True


def test_plan_roundtrip_xcom() -> None:
    original = plan_rebuild_periods(
        rebuild_start="2026-03",
        rebuild_end="2026-06",
        today=TODAY,
        dry_run=True,
    )
    restored = plan_from_safe_dict(original.to_safe_dict())
    assert restored == original


def test_no_weekly_symbols_in_plan_module() -> None:
    import analytics_reporting_platform.execution.monthly_plan as mod
    import inspect

    source = inspect.getsource(mod)
    assert "weekly_" not in source.lower()
    assert "from analytics_reporting_platform" in source
    assert "WeeklyOrganic" not in source
    assert "run_weekly" not in source


def test_stage_empty_periods_noop() -> None:
    payload = run_stage_periods_for_airflow(
        "raw",
        [],
        mode="incremental",
        dry_run=True,
    )
    assert payload["status"] == "success"
    assert payload["detail"] == "no-op"
    assert payload["periods"] == []


def test_stage_sequential_fail_fast() -> None:
    calls: list[str] = []

    def fake_raw(month, **kwargs):
        calls.append(str(month)[:7] if hasattr(month, "year") else str(month))
        ym = month.strftime("%Y-%m") if hasattr(month, "strftime") else str(month)[:7]
        if ym == "2025-02":
            return SimpleNamespace(
                stage="raw",
                status="failed",
                marts=(),
                error="boom",
            )
        return SimpleNamespace(stage="raw", status="planned", marts=(), error=None)

    with patch(
        "analytics_reporting_platform.execution.monthly.run_monthly_raw",
        side_effect=fake_raw,
    ):
        with pytest.raises(MonthlyOrchestrationError, match="period=2025-02"):
            run_stage_periods_for_airflow(
                "raw",
                ["2025-01", "2025-02", "2025-03"],
                mode="rebuild",
                dry_run=True,
                today=TODAY,
            )
    assert "2025-01" in calls[0] or calls[0] == "2025-01"
    assert any("2025-02" in c for c in calls)
    assert not any("2025-03" in c for c in calls)


def test_wordstat_failure_blocks_calc_without_rerunning_adobe() -> None:
    """Wordstat FAIL → calc/cdm not executed; Adobe RAW already done stays done."""
    adobe_calls: list[str] = []
    wordstat_calls: list[str] = []
    calc_calls: list[str] = []

    def fake_adobe(month, **kwargs):
        adobe_calls.append("raw")
        return SimpleNamespace(stage="raw", status="success", marts=(), error=None)

    def fake_wordstat(periods, **kwargs):
        wordstat_calls.append("wordstat")
        return SimpleNamespace(
            stage="wordstat_raw",
            status="failed",
            marts=(),
            error="wordstat boom",
        )

    def fake_calc(month, **kwargs):
        calc_calls.append("calc")
        return SimpleNamespace(stage="calc", status="planned", marts=(), error=None)

    deps = SimpleNamespace(
        raw_engine=object(),
        calc_engine=object(),
        cdm_engine=object(),
        adobe_reports=None,
        client_config=None,
        wordstat_client=object(),
        on_event=None,
    )
    with patch(
        "analytics_reporting_platform.execution.monthly.run_monthly_raw",
        side_effect=fake_adobe,
    ):
        with patch(
            "analytics_reporting_platform.execution.monthly.run_wordstat_raw_periods",
            side_effect=fake_wordstat,
        ):
            with patch(
                "analytics_reporting_platform.execution.monthly.run_monthly_calc",
                side_effect=fake_calc,
            ):
                assert (
                    run_stage_periods_for_airflow(
                        "raw",
                        ["2026-07"],
                        mode="rebuild",
                        dry_run=False,
                        today=TODAY,
                        deps=deps,
                    )["status"]
                    == "success"
                )
                with pytest.raises(MonthlyOrchestrationError, match="WORDSTAT_RAW"):
                    run_stage_periods_for_airflow(
                        "wordstat_raw",
                        ["2026-07"],
                        mode="rebuild",
                        dry_run=False,
                        today=TODAY,
                        deps=deps,
                    )
    assert adobe_calls == ["raw"]
    assert wordstat_calls == ["wordstat"]
    assert calc_calls == []


def test_dag_graph_tasks_and_no_weekly() -> None:
    import ast
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[2]
        / "dags"
        / "monthly_private_pipeline.py"
    )
    source = path.read_text()
    assert "weekly_" not in source.lower()
    assert "run_weekly" not in source
    assert 'MONTHLY_DAG_ID = "monthly_private_pipeline"' in source
    assert "monthly_private_backfill" not in source
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
        "load_wordstat_raw",
        "build_calc",
        "build_cdm",
        "data_quality",
        "final_status",
    ]
    assert "incremental" in source
    assert "dry_run" in source
    assert "MONTHLY_SCHEDULE" in source
    assert "default=True" in source
