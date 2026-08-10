"""Resumable sequential Monthly backfill aligned to reporting boundary.

Reporting starts at 2026-01. RAW history from 2025-01 supports MoM/YoY.
2024 is out of scope.

Phases:
  raw_history          — Adobe → RAW only (e.g. 2025-01..2025-12)
  reporting_pipeline   — Adobe → RAW → CALC → CDM (e.g. 2026-01..2026-07)
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Optional

from analytics_reporting_platform.calc.models import (
    prev_month_start,
    prev_year_month_start,
)
from analytics_reporting_platform.cdm.models import (
    DEFAULT_REPORT_START,
    DEFAULT_TREND_START,
)
from analytics_reporting_platform.execution.models import (
    ExecutionMode,
    MonthlyExecutionError,
    MonthlyPipelineDeps,
)
from analytics_reporting_platform.execution.period import (
    ExecutionMonthInput,
    PeriodValidationError,
    iter_months,
    last_completed_month,
    parse_execution_month,
    validate_execution_month,
)

logger = logging.getLogger(__name__)

# Authoritative Portfolio reporting boundary (matches CDM defaults / frozen baseline).
DEFAULT_REPORTING_START: date = DEFAULT_REPORT_START  # 2026-01-01
DEFAULT_RAW_HISTORY_START: date = DEFAULT_TREND_START  # 2025-01-01

BackfillPhase = Literal["raw_history", "reporting_pipeline"]
BackfillMonthStatus = Literal["pending", "success", "failed", "skipped"]


@dataclass(frozen=True)
class BackfillMonthPlan:
    """One sequential unit of work."""

    month: str  # YYYY-MM
    phase: BackfillPhase


@dataclass
class BackfillMonthResult:
    """Safe structured status for one month (no KPIs/URLs/secrets)."""

    month: str
    phase: BackfillPhase
    status: BackfillMonthStatus
    overall_status: Optional[str] = None
    failed_stage: Optional[str] = None
    failed_mart: Optional[str] = None
    error: Optional[str] = None
    validation: Optional[str] = None  # PASS | FAIL | skipped
    finished_at: Optional[str] = None


@dataclass
class BackfillRunState:
    """Persisted/resumable backfill state."""

    raw_history_start: str
    raw_history_end: str
    reporting_start: str
    reporting_end: str
    mode: ExecutionMode
    months: list[BackfillMonthResult] = field(default_factory=list)
    stopped_on: Optional[str] = None
    resume_from: Optional[str] = None
    updated_at: Optional[str] = None

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "raw_history_start": self.raw_history_start,
            "raw_history_end": self.raw_history_end,
            "reporting_start": self.reporting_start,
            "reporting_end": self.reporting_end,
            "mode": self.mode,
            "stopped_on": self.stopped_on,
            "resume_from": self.resume_from,
            "updated_at": self.updated_at,
            "months": [asdict(m) for m in self.months],
        }


def reject_incomplete_or_future_month(
    month: ExecutionMonthInput,
    *,
    today: Optional[date] = None,
) -> date:
    """Accept only fully completed calendar months (not current incomplete / future)."""
    as_of = today if today is not None else date.today()
    month_start = validate_execution_month(month, today=as_of, allow_future=False)
    completed = last_completed_month(as_of)
    if month_start > completed:
        raise PeriodValidationError(
            f"Incomplete or future month rejected: {month_start.isoformat()} "
            f"(last completed month is {completed.isoformat()})"
        )
    return month_start


def reporting_mom_month(reporting_month: ExecutionMonthInput) -> date:
    """M-1 lookback for a reporting month (e.g. 2026-01 → 2025-12)."""
    return prev_month_start(parse_execution_month(reporting_month))


def reporting_yoy_month(reporting_month: ExecutionMonthInput) -> date:
    """M-12 lookback for a reporting month (e.g. 2026-01 → 2025-01)."""
    return prev_year_month_start(parse_execution_month(reporting_month))


def assert_lookbacks_within_raw_history(
    reporting_month: ExecutionMonthInput,
    *,
    raw_history_start: ExecutionMonthInput = DEFAULT_RAW_HISTORY_START,
) -> None:
    """Ensure MoM/YoY for ``reporting_month`` resolve inside RAW history (no 2024)."""
    hist_start = parse_execution_month(raw_history_start)
    mom = reporting_mom_month(reporting_month)
    yoy = reporting_yoy_month(reporting_month)
    if mom < hist_start or yoy < hist_start:
        raise PeriodValidationError(
            "Reporting lookback falls before RAW history start "
            f"(month={parse_execution_month(reporting_month).isoformat()} "
            f"mom={mom.isoformat()} yoy={yoy.isoformat()} "
            f"raw_history_start={hist_start.isoformat()})"
        )
    if mom.year < 2025 or yoy.year < 2025:
        raise PeriodValidationError(
            "Reporting lookback must not require pre-2025 RAW "
            f"(mom={mom.isoformat()} yoy={yoy.isoformat()})"
        )


def plan_monthly_backfill(
    *,
    reporting_end: ExecutionMonthInput,
    reporting_start: ExecutionMonthInput = DEFAULT_REPORTING_START,
    raw_history_start: ExecutionMonthInput = DEFAULT_RAW_HISTORY_START,
    today: Optional[date] = None,
) -> list[BackfillMonthPlan]:
    """Build deterministic plan: RAW history then reporting full pipeline.

    Default for Portfolio retrospective:
      raw_history 2025-01..2025-12
      reporting_pipeline 2026-01..reporting_end (e.g. 2026-07)

    Never includes 2024. Never schedules CALC/CDM for RAW-history months.
    """
    as_of = today if today is not None else date.today()
    hist_start = reject_incomplete_or_future_month(raw_history_start, today=as_of)
    rep_start = reject_incomplete_or_future_month(reporting_start, today=as_of)
    rep_end = reject_incomplete_or_future_month(reporting_end, today=as_of)

    if hist_start >= rep_start:
        raise PeriodValidationError(
            "raw_history_start must be before reporting_start "
            f"({hist_start.isoformat()} >= {rep_start.isoformat()})"
        )
    if rep_start > rep_end:
        raise PeriodValidationError(
            f"Invalid reporting range: {rep_start.isoformat()} after {rep_end.isoformat()}"
        )
    if rep_start < DEFAULT_REPORTING_START:
        raise PeriodValidationError(
            f"reporting_start must be >= {DEFAULT_REPORTING_START.isoformat()} "
            "(CALC/CDM reporting boundary; 2025 is RAW history only)"
        )

    hist_end = prev_month_start(rep_start)
    if hist_start > hist_end:
        raise PeriodValidationError("Empty RAW history window")

    # First reporting month must resolve MoM/YoY inside RAW history.
    assert_lookbacks_within_raw_history(rep_start, raw_history_start=hist_start)

    plan: list[BackfillMonthPlan] = []
    for month in iter_months(hist_start, hist_end):
        ym = month.strftime("%Y-%m")
        if month.year < 2025:
            raise PeriodValidationError(f"Pre-2025 month out of scope: {ym}")
        plan.append(BackfillMonthPlan(month=ym, phase="raw_history"))

    for month in iter_months(rep_start, rep_end):
        assert_lookbacks_within_raw_history(month, raw_history_start=hist_start)
        plan.append(
            BackfillMonthPlan(month=month.strftime("%Y-%m"), phase="reporting_pipeline")
        )

    if any(p.month.startswith("2024") for p in plan):
        raise PeriodValidationError("Plan must not include 2024 months")

    return plan


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_state(path: Path) -> Optional[BackfillRunState]:
    if not path.is_file():
        return None
    data = json.loads(path.read_text())
    months = [BackfillMonthResult(**row) for row in data.get("months", [])]
    return BackfillRunState(
        raw_history_start=data["raw_history_start"],
        raw_history_end=data["raw_history_end"],
        reporting_start=data["reporting_start"],
        reporting_end=data["reporting_end"],
        mode=data.get("mode", "rebuild"),
        months=months,
        stopped_on=data.get("stopped_on"),
        resume_from=data.get("resume_from"),
        updated_at=data.get("updated_at"),
    )


def _save_state(path: Path, state: BackfillRunState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state.updated_at = _utc_now_iso()
    path.write_text(json.dumps(state.to_safe_dict(), indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


def _index_pending(state: BackfillRunState) -> int:
    for i, row in enumerate(state.months):
        if row.status in {"pending", "failed"}:
            return i
    return len(state.months)


def first_incomplete_month(state: BackfillRunState) -> Optional[str]:
    """Return YYYY-MM of first pending/failed entry, else None if complete."""
    for row in state.months:
        if row.status in {"pending", "failed"}:
            return row.month
    return None


def run_monthly_backfill(
    *,
    reporting_end: ExecutionMonthInput,
    reporting_start: ExecutionMonthInput = DEFAULT_REPORTING_START,
    raw_history_start: ExecutionMonthInput = DEFAULT_RAW_HISTORY_START,
    deps: MonthlyPipelineDeps,
    mode: ExecutionMode = "rebuild",
    today: Optional[date] = None,
    state_path: Optional[Path] = None,
    dry_run: bool = False,
    validate: bool = True,
    stop_on_failure: bool = True,
    run_raw: Optional[Callable[..., Any]] = None,
    run_full: Optional[Callable[..., Any]] = None,
    validate_raw_history: Optional[Callable[..., Any]] = None,
    validate_reporting: Optional[Callable[..., Any]] = None,
) -> BackfillRunState:
    """Execute backfill sequentially; stop on first failure; resumable via state file."""
    if mode not in ("incremental", "rebuild"):
        raise PeriodValidationError(f"Unsupported execution mode: {mode!r}")
    if not stop_on_failure:
        raise MonthlyExecutionError("Parallel/continue-on-error backfill is not supported")

    as_of = today if today is not None else date.today()
    plan = plan_monthly_backfill(
        reporting_end=reporting_end,
        reporting_start=reporting_start,
        raw_history_start=raw_history_start,
        today=as_of,
    )
    hist_start = reject_incomplete_or_future_month(raw_history_start, today=as_of)
    rep_start = reject_incomplete_or_future_month(reporting_start, today=as_of)
    rep_end = reject_incomplete_or_future_month(reporting_end, today=as_of)
    hist_end = prev_month_start(rep_start)

    if state_path is not None and state_path.is_file():
        state = _load_state(state_path)
        if state is None:
            raise MonthlyExecutionError(f"Unreadable backfill state: {state_path}")
        expected = (
            hist_start.strftime("%Y-%m"),
            hist_end.strftime("%Y-%m"),
            rep_start.strftime("%Y-%m"),
            rep_end.strftime("%Y-%m"),
        )
        actual = (
            state.raw_history_start,
            state.raw_history_end,
            state.reporting_start,
            state.reporting_end,
        )
        if actual != expected:
            raise MonthlyExecutionError(
                "Backfill state range mismatch; refuse to resume with different bounds"
            )
        if len(state.months) != len(plan):
            raise MonthlyExecutionError(
                "Backfill state plan length mismatch; refuse to resume"
            )
    else:
        state = BackfillRunState(
            raw_history_start=hist_start.strftime("%Y-%m"),
            raw_history_end=hist_end.strftime("%Y-%m"),
            reporting_start=rep_start.strftime("%Y-%m"),
            reporting_end=rep_end.strftime("%Y-%m"),
            mode=mode,
            months=[
                BackfillMonthResult(month=p.month, phase=p.phase, status="pending")
                for p in plan
            ],
        )

    if run_raw is None or run_full is None:
        from analytics_reporting_platform.execution.monthly import (
            run_monthly_period,
            run_monthly_raw,
        )

        if run_raw is None:
            run_raw = run_monthly_raw
        if run_full is None:
            run_full = run_monthly_period

    if validate:
        if validate_raw_history is None:
            from analytics_reporting_platform.validation.monthly import (
                validate_monthly_raw_history,
            )

            def validate_raw_history(month: str) -> Any:
                return validate_monthly_raw_history(
                    raw_engine=deps.raw_engine,
                    month=month,
                )

        if validate_reporting is None:
            from analytics_reporting_platform.validation.monthly import (
                validate_monthly_period,
            )

            def validate_reporting(month: str) -> Any:
                return validate_monthly_period(
                    raw_engine=deps.raw_engine,
                    calc_engine=deps.calc_engine,
                    cdm_engine=deps.cdm_engine,
                    month=month,
                )

    start_idx = _index_pending(state)
    state.resume_from = (
        state.months[start_idx].month if start_idx < len(state.months) else None
    )
    state.stopped_on = None

    for idx in range(start_idx, len(state.months)):
        item = state.months[idx]
        planned = plan[idx]
        if (item.month, item.phase) != (planned.month, planned.phase):
            raise MonthlyExecutionError("Backfill state diverged from plan")

        logger.info(
            "backfill_month_start month=%s phase=%s mode=%s dry_run=%s",
            item.month,
            item.phase,
            mode,
            dry_run,
        )

        failed_stage: Optional[str] = None
        failed_mart: Optional[str] = None
        try:
            reject_incomplete_or_future_month(item.month, today=as_of)
            if item.phase == "raw_history":
                stage = run_raw(
                    item.month, mode=mode, deps=deps, dry_run=dry_run, today=as_of
                )
                overall = getattr(stage, "status", None) or "success"
                failed_stage = "raw" if overall == "failed" else None
                failed_mart = getattr(stage, "failed_mart", None)
                error = getattr(stage, "error", None)
                validation_status = "skipped"
                if overall == "failed":
                    raise MonthlyExecutionError(
                        error or f"RAW history failed for {item.month}"
                    )
                if validate and not dry_run:
                    assert validate_raw_history is not None
                    v = validate_raw_history(item.month)
                    validation_status = getattr(v, "overall", None) or "FAIL"
                    if validation_status != "PASS":
                        raise MonthlyExecutionError(
                            f"RAW history validation failed for {item.month}"
                        )
            elif item.phase == "reporting_pipeline":
                result = run_full(
                    item.month, mode=mode, deps=deps, dry_run=dry_run, today=as_of
                )
                overall = getattr(result, "overall_status", None)
                failed_stage = getattr(result, "failed_stage", None)
                failed_mart = getattr(result, "failed_mart", None)
                error = getattr(result, "error", None)
                validation_status = "skipped"
                if overall == "failed":
                    raise MonthlyExecutionError(
                        error or f"Reporting pipeline failed for {item.month}"
                    )
                if validate and not dry_run:
                    assert validate_reporting is not None
                    v = validate_reporting(item.month)
                    validation_status = getattr(v, "overall", None) or "FAIL"
                    if validation_status != "PASS":
                        raise MonthlyExecutionError(
                            f"Reporting validation failed for {item.month} "
                            f"overall={validation_status}"
                        )
            else:
                raise MonthlyExecutionError(f"Unknown backfill phase: {item.phase}")

            item.status = "success"
            item.overall_status = str(overall)
            item.failed_stage = None
            item.failed_mart = None
            item.error = None
            item.validation = validation_status
            item.finished_at = _utc_now_iso()
        except Exception as exc:  # noqa: BLE001 — stop and record
            item.status = "failed"
            item.overall_status = "failed"
            item.failed_stage = failed_stage or item.phase
            item.failed_mart = failed_mart
            item.error = str(exc)[:500]
            item.validation = "FAIL" if validate else item.validation
            item.finished_at = _utc_now_iso()
            state.stopped_on = item.month
            state.resume_from = item.month
            if state_path is not None:
                _save_state(state_path, state)
            logger.info(
                "backfill_stop month=%s phase=%s status=failed",
                item.month,
                item.phase,
            )
            raise MonthlyExecutionError(
                f"Backfill stopped at month={item.month} phase={item.phase}: {exc}"
            ) from exc

        if state_path is not None:
            _save_state(state_path, state)

    state.resume_from = None
    state.stopped_on = None
    if state_path is not None:
        _save_state(state_path, state)
    return state


def months_for_backfill(
    start: str,
    end: str,
    *,
    today: Optional[date] = None,
) -> list[str]:
    """Validated ``YYYY-MM`` list for a completed-month range (utility)."""
    as_of = today if today is not None else date.today()
    start_m = reject_incomplete_or_future_month(start, today=as_of)
    end_m = reject_incomplete_or_future_month(end, today=as_of)
    return [m.strftime("%Y-%m") for m in iter_months(start_m, end_m)]


__all__ = [
    "DEFAULT_RAW_HISTORY_START",
    "DEFAULT_REPORTING_START",
    "BackfillMonthPlan",
    "BackfillMonthResult",
    "BackfillPhase",
    "BackfillRunState",
    "assert_lookbacks_within_raw_history",
    "first_incomplete_month",
    "months_for_backfill",
    "plan_monthly_backfill",
    "reject_incomplete_or_future_month",
    "reporting_mom_month",
    "reporting_yoy_month",
    "run_monthly_backfill",
]
