"""Unit tests for Monthly execution API + CLI (stdlib harness; no Adobe)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional
from unittest.mock import MagicMock

from analytics_reporting_platform.cli.monthly import execute_monthly_cli
from analytics_reporting_platform.execution import (
    CALC_MARTS,
    CDM_MARTS,
    MonthlyPipelineDeps,
    PeriodValidationError,
    RAW_MARTS,
    parse_execution_month,
    run_monthly_period,
    validate_execution_month,
)
import analytics_reporting_platform.execution.monthly as monthly_mod


def test_parse_valid_months() -> None:
    assert parse_execution_month("2026-07") == date(2026, 7, 1)
    assert parse_execution_month("2026-07-01") == date(2026, 7, 1)
    assert parse_execution_month(date(2026, 7, 1)) == date(2026, 7, 1)


def test_invalid_month() -> None:
    for bad in ("2026-13", "2026/07", "July-2026", "2026-07-15", ""):
        try:
            parse_execution_month(bad)
            raise AssertionError(f"expected failure for {bad!r}")
        except PeriodValidationError:
            pass


def test_reject_future_month() -> None:
    try:
        validate_execution_month("2099-01", today=date(2026, 8, 8))
        raise AssertionError("expected future rejection")
    except PeriodValidationError:
        pass
    assert validate_execution_month("2026-08", today=date(2026, 8, 8)) == date(2026, 8, 1)


def test_dry_run_plan_ordering_and_modes() -> None:
    for mode in ("incremental", "rebuild"):
        result = run_monthly_period("2026-07", mode=mode, dry_run=True, today=date(2026, 8, 1))
        assert result.overall_status == "planned"
        assert result.raw_status == "planned"
        assert result.wordstat_status == "planned"
        assert result.calc_status == "planned"
        assert result.cdm_status == "planned"
        assert [s.stage for s in result.stages] == ["raw", "wordstat_raw", "calc", "cdm"]
        assert tuple(m.mart for m in result.stages[0].marts) == monthly_mod.ADOBE_RAW_MARTS
        assert tuple(m.mart for m in result.stages[1].marts) == (monthly_mod.WORDSTAT_RAW_MART,)
        assert tuple(m.mart for m in result.stages[2].marts) == CALC_MARTS
        assert tuple(m.mart for m in result.stages[3].marts) == CDM_MARTS


@dataclass
class _FakeBuilt:
    dataframe: Any = "frame"


def test_raw_failure_stops_calc_cdm(monkeypatch: Any = None) -> None:
    deps = MonthlyPipelineDeps(
        raw_engine=MagicMock(),
        calc_engine=MagicMock(),
        cdm_engine=MagicMock(),
        adobe_reports=MagicMock(),
        client_config=MagicMock(),
    )

    def boom_raw(*args, **kwargs):  # type: ignore[no-untyped-def]
        from analytics_reporting_platform.execution.models import MartRunResult, StageRunResult

        return StageRunResult(
            stage="raw",
            status="failed",
            marts=(MartRunResult(mart="monthly_metrics", status="failed", detail="boom"),),
            error="RuntimeError: boom",
        )

    calc_called = {"n": 0}
    wordstat_called = {"n": 0}
    cdm_called = {"n": 0}

    def wordstat_stub(*args, **kwargs):  # type: ignore[no-untyped-def]
        wordstat_called["n"] += 1
        raise AssertionError("wordstat must not run")

    def calc_stub(*args, **kwargs):  # type: ignore[no-untyped-def]
        calc_called["n"] += 1
        raise AssertionError("calc must not run")

    def cdm_stub(*args, **kwargs):  # type: ignore[no-untyped-def]
        cdm_called["n"] += 1
        raise AssertionError("cdm must not run")

    orig_raw = monthly_mod._run_raw
    orig_ws = monthly_mod._run_wordstat_raw
    orig_calc = monthly_mod._run_calc
    orig_cdm = monthly_mod._run_cdm
    monthly_mod._run_raw = boom_raw  # type: ignore[assignment]
    monthly_mod._run_wordstat_raw = wordstat_stub  # type: ignore[assignment]
    monthly_mod._run_calc = calc_stub  # type: ignore[assignment]
    monthly_mod._run_cdm = cdm_stub  # type: ignore[assignment]
    try:
        result = run_monthly_period(
            "2026-07", mode="rebuild", deps=deps, today=date(2026, 8, 1)
        )
    finally:
        monthly_mod._run_raw = orig_raw
        monthly_mod._run_wordstat_raw = orig_ws
        monthly_mod._run_calc = orig_calc
        monthly_mod._run_cdm = orig_cdm

    assert result.overall_status == "failed"
    assert result.failed_stage == "raw"
    assert result.wordstat_status == "not_run"
    assert result.calc_status == "not_run"
    assert result.cdm_status == "not_run"
    assert wordstat_called["n"] == 0
    assert calc_called["n"] == 0
    assert cdm_called["n"] == 0


def test_calc_failure_stops_cdm() -> None:
    deps = MonthlyPipelineDeps(
        raw_engine=MagicMock(),
        calc_engine=MagicMock(),
        cdm_engine=MagicMock(),
    )
    from analytics_reporting_platform.execution.models import MartRunResult, StageRunResult

    def ok_raw(*args, **kwargs):  # type: ignore[no-untyped-def]
        return StageRunResult(
            stage="raw",
            status="skipped",
            marts=tuple(MartRunResult(mart=m, status="skipped") for m in monthly_mod.ADOBE_RAW_MARTS),
        )

    def ok_wordstat(*args, **kwargs):  # type: ignore[no-untyped-def]
        return StageRunResult(
            stage="wordstat_raw",
            status="skipped",
            marts=(MartRunResult(mart=monthly_mod.WORDSTAT_RAW_MART, status="skipped"),),
        )

    def boom_calc(*args, **kwargs):  # type: ignore[no-untyped-def]
        return StageRunResult(
            stage="calc",
            status="failed",
            marts=(MartRunResult(mart="monthly_kpi", status="failed"),),
            error="CalcError: boom",
        )

    cdm_called = {"n": 0}

    def cdm_stub(*args, **kwargs):  # type: ignore[no-untyped-def]
        cdm_called["n"] += 1
        raise AssertionError("cdm must not run")

    orig_raw = monthly_mod._run_raw
    orig_ws = monthly_mod._run_wordstat_raw
    orig_calc = monthly_mod._run_calc
    orig_cdm = monthly_mod._run_cdm
    monthly_mod._run_raw = ok_raw  # type: ignore[assignment]
    monthly_mod._run_wordstat_raw = ok_wordstat  # type: ignore[assignment]
    monthly_mod._run_calc = boom_calc  # type: ignore[assignment]
    monthly_mod._run_cdm = cdm_stub  # type: ignore[assignment]
    try:
        result = run_monthly_period(
            "2026-07", mode="incremental", deps=deps, today=date(2026, 8, 1)
        )
    finally:
        monthly_mod._run_raw = orig_raw
        monthly_mod._run_wordstat_raw = orig_ws
        monthly_mod._run_calc = orig_calc
        monthly_mod._run_cdm = orig_cdm

    assert result.overall_status == "failed"
    assert result.failed_stage == "calc"
    assert result.raw_status == "skipped"
    assert result.wordstat_status == "skipped"
    assert result.cdm_status == "not_run"
    assert cdm_called["n"] == 0


def test_successful_execution_result() -> None:
    deps = MonthlyPipelineDeps(
        raw_engine=MagicMock(),
        calc_engine=MagicMock(),
        cdm_engine=MagicMock(),
    )
    from analytics_reporting_platform.execution.models import MartRunResult, StageRunResult

    def ok_stage(stage: str, marts: tuple[str, ...]):  # type: ignore[no-untyped-def]
        def _inner(*args, **kwargs):
            return StageRunResult(
                stage=stage,  # type: ignore[arg-type]
                status="success",
                marts=tuple(MartRunResult(mart=m, status="success") for m in marts),
            )

        return _inner

    orig_raw = monthly_mod._run_raw
    orig_ws = monthly_mod._run_wordstat_raw
    orig_calc = monthly_mod._run_calc
    orig_cdm = monthly_mod._run_cdm
    monthly_mod._run_raw = ok_stage("raw", monthly_mod.ADOBE_RAW_MARTS)  # type: ignore[assignment]
    monthly_mod._run_wordstat_raw = ok_stage(  # type: ignore[assignment]
        "wordstat_raw", (monthly_mod.WORDSTAT_RAW_MART,)
    )
    monthly_mod._run_calc = ok_stage("calc", CALC_MARTS)  # type: ignore[assignment]
    monthly_mod._run_cdm = ok_stage("cdm", CDM_MARTS)  # type: ignore[assignment]
    try:
        result = run_monthly_period(
            date(2026, 7, 1), mode="rebuild", deps=deps, today=date(2026, 8, 1)
        )
    finally:
        monthly_mod._run_raw = orig_raw
        monthly_mod._run_wordstat_raw = orig_ws
        monthly_mod._run_calc = orig_calc
        monthly_mod._run_cdm = orig_cdm

    assert result.overall_status == "success"
    assert result.mode == "rebuild"
    assert result.failed_stage is None
    assert [s.stage for s in result.stages] == ["raw", "wordstat_raw", "calc", "cdm"]


def test_cli_success_dry_run_exit() -> None:
    code = execute_monthly_cli(["monthly", "run", "--month", "2026-07", "--dry-run"])
    assert code == 0
    code2 = execute_monthly_cli(["monthly", "rebuild", "--month", "2026-07", "--dry-run"])
    assert code2 == 0


def test_cli_failure_exit_invalid_month() -> None:
    code = execute_monthly_cli(["monthly", "run", "--month", "2026-13", "--dry-run"])
    assert code != 0


def test_cli_failure_exit_future_month() -> None:
    # Patch today via run_monthly_period path: use month far future with dry-run
    code = execute_monthly_cli(["monthly", "run", "--month", "2099-01", "--dry-run"])
    assert code != 0


def test_rebuild_vs_incremental_routing() -> None:
    seen: list[str] = []

    deps = MonthlyPipelineDeps(
        raw_engine=MagicMock(), calc_engine=MagicMock(), cdm_engine=MagicMock()
    )
    from analytics_reporting_platform.execution.models import StageRunResult

    def track(stage: str):  # type: ignore[no-untyped-def]
        def _inner(deps, month, mode, **kwargs):
            seen.append(f"{stage}:{mode}")
            return StageRunResult(stage=stage, status="success", marts=())  # type: ignore[arg-type]

        return _inner

    orig = (
        monthly_mod._run_raw,
        monthly_mod._run_wordstat_raw,
        monthly_mod._run_calc,
        monthly_mod._run_cdm,
    )
    monthly_mod._run_raw = track("raw")  # type: ignore[assignment]
    monthly_mod._run_wordstat_raw = track("wordstat_raw")  # type: ignore[assignment]
    monthly_mod._run_calc = track("calc")  # type: ignore[assignment]
    monthly_mod._run_cdm = track("cdm")  # type: ignore[assignment]
    try:
        run_monthly_period("2026-07", mode="incremental", deps=deps, today=date(2026, 8, 1))
        run_monthly_period("2026-07", mode="rebuild", deps=deps, today=date(2026, 8, 1))
    finally:
        (
            monthly_mod._run_raw,
            monthly_mod._run_wordstat_raw,
            monthly_mod._run_calc,
            monthly_mod._run_cdm,
        ) = orig

    assert seen == [
        "raw:incremental",
        "wordstat_raw:incremental",
        "calc:incremental",
        "cdm:incremental",
        "raw:rebuild",
        "wordstat_raw:rebuild",
        "calc:rebuild",
        "cdm:rebuild",
    ]


if __name__ == "__main__":
    test_parse_valid_months()
    test_invalid_month()
    test_reject_future_month()
    test_dry_run_plan_ordering_and_modes()
    test_raw_failure_stops_calc_cdm()
    test_calc_failure_stops_cdm()
    test_successful_execution_result()
    test_cli_success_dry_run_exit()
    test_cli_failure_exit_invalid_month()
    test_cli_failure_exit_future_month()
    test_rebuild_vs_incremental_routing()
    print("OK", 11)
