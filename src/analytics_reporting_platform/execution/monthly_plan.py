"""Dynamic Monthly period planning (incremental missing + manual rebuild).

Platform bounds (Portfolio Edition):
  RAW  >= 2025-01
  CALC >= 2026-01
  CDM  >= 2026-01

Only fully completed calendar months are eligible (never the current month).
Weekly is out of scope.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Optional, Sequence

from analytics_reporting_platform.cdm.models import (
    DEFAULT_REPORT_START,
    DEFAULT_TREND_START,
)
from analytics_reporting_platform.execution.models import ExecutionMode
from analytics_reporting_platform.execution.period import (
    ExecutionMonthInput,
    PeriodValidationError,
    iter_months,
    last_completed_month,
    parse_execution_month,
)

logger = logging.getLogger(__name__)

DEFAULT_RAW_START: date = DEFAULT_TREND_START  # 2025-01-01
DEFAULT_REPORTING_START: date = DEFAULT_REPORT_START  # 2026-01-01

LayerPresenceFn = Callable[[date], bool]


@dataclass(frozen=True)
class MonthlyPeriodsPlan:
    """Safe execution plan for Airflow XCom (no secrets / client data)."""

    mode: ExecutionMode
    last_complete_month: str
    raw_periods: tuple[str, ...]
    calc_periods: tuple[str, ...]
    cdm_periods: tuple[str, ...]
    dry_run: bool = True
    rebuild_start: Optional[str] = None
    rebuild_end: Optional[str] = None

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "last_complete_month": self.last_complete_month,
            "raw_periods": list(self.raw_periods),
            "calc_periods": list(self.calc_periods),
            "cdm_periods": list(self.cdm_periods),
            "dry_run": self.dry_run,
            "rebuild_start": self.rebuild_start,
            "rebuild_end": self.rebuild_end,
        }


def _ym(value: date) -> str:
    return value.strftime("%Y-%m")


def _blank_to_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_mode(raw: Any) -> ExecutionMode:
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        return "incremental"
    mode = str(raw).strip().lower()
    if mode not in {"incremental", "rebuild", "validate"}:
        raise PeriodValidationError(
            f"Unsupported execution mode: {raw!r}. Expected incremental|rebuild|validate"
        )
    return mode  # type: ignore[return-value]


def _coerce_bool(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    raise PeriodValidationError(f"Invalid boolean value: {value!r}")


def reject_incomplete_month(
    month: ExecutionMonthInput,
    *,
    today: Optional[date] = None,
) -> date:
    """Accept only fully completed calendar months (not current / future)."""
    as_of = today if today is not None else date.today()
    month_start = parse_execution_month(month)
    completed = last_completed_month(as_of)
    if month_start > completed:
        raise PeriodValidationError(
            f"Incomplete or future month rejected: {_ym(month_start)} "
            f"(last_complete_month={_ym(completed)})"
        )
    return month_start


def _months_yyyy_mm(start: date, end: date) -> tuple[str, ...]:
    if start > end:
        return ()
    return tuple(_ym(m) for m in iter_months(start, end))


def _clip_reporting(months: Sequence[str]) -> tuple[str, ...]:
    """Keep only months on/after reporting boundary (2026-01)."""
    boundary = _ym(DEFAULT_REPORTING_START)
    return tuple(m for m in months if m >= boundary)


def _table_has_month(engine: Any, table: str, month: date) -> bool:
    from sqlalchemy import text

    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT 1 FROM {table} WHERE month = :month LIMIT 1"),
            {"month": month},
        ).fetchone()
    return row is not None


def raw_month_present(raw_engine: Any, month: date) -> bool:
    """True when all Monthly RAW marts have rows for ``month``."""
    from analytics_reporting_platform.execution.monthly import RAW_MARTS
    from analytics_reporting_platform.raw.repository import month_exists

    return all(month_exists(raw_engine, mart, month) for mart in RAW_MARTS)


def calc_month_present(calc_engine: Any, month: date) -> bool:
    """True when all Monthly CALC marts have rows for ``month``."""
    from analytics_reporting_platform.validation.specs import CALC_SPECS

    return all(_table_has_month(calc_engine, spec.table, month) for spec in CALC_SPECS)


def cdm_month_present(cdm_engine: Any, month: date) -> bool:
    """True when all Monthly CDM marts contain ``month``."""
    from analytics_reporting_platform.validation.specs import CDM_SPECS

    return all(_table_has_month(cdm_engine, spec.table, month) for spec in CDM_SPECS)


def _missing_months(
    candidates: Sequence[str],
    *,
    is_present: LayerPresenceFn,
) -> tuple[str, ...]:
    missing: list[str] = []
    for ym in candidates:
        month = parse_execution_month(ym)
        if not is_present(month):
            missing.append(ym)
    return tuple(missing)


def plan_validate_periods(
    *,
    today: Optional[date] = None,
    dry_run: bool = True,
) -> MonthlyPeriodsPlan:
    """Read-only validation plan: no ETL periods; DQ uses ``last_complete_month``."""
    as_of = today if today is not None else date.today()
    last = last_completed_month(as_of)
    return MonthlyPeriodsPlan(
        mode="validate",
        last_complete_month=_ym(last),
        raw_periods=(),
        calc_periods=(),
        cdm_periods=(),
        dry_run=dry_run,
    )


def plan_rebuild_periods(
    *,
    rebuild_start: ExecutionMonthInput,
    rebuild_end: Optional[ExecutionMonthInput] = None,
    today: Optional[date] = None,
    dry_run: bool = True,
) -> MonthlyPeriodsPlan:
    """Force-selected range: RAW for all months; CALC/CDM only >= 2026-01."""
    as_of = today if today is not None else date.today()
    last = last_completed_month(as_of)
    start = reject_incomplete_month(rebuild_start, today=as_of)
    end_raw = rebuild_end if rebuild_end is not None else rebuild_start
    end = reject_incomplete_month(end_raw, today=as_of)

    if start < DEFAULT_RAW_START:
        raise PeriodValidationError(
            f"rebuild_start cannot be before {_ym(DEFAULT_RAW_START)}"
        )
    if end < start:
        raise PeriodValidationError(
            f"rebuild_end {_ym(end)} must be >= rebuild_start {_ym(start)}"
        )
    if end > last:
        raise PeriodValidationError(
            f"rebuild_end {_ym(end)} cannot be after last_complete_month {_ym(last)}"
        )

    raw = _months_yyyy_mm(start, end)
    reporting = _clip_reporting(raw)
    return MonthlyPeriodsPlan(
        mode="rebuild",
        last_complete_month=_ym(last),
        raw_periods=raw,
        calc_periods=reporting,
        cdm_periods=reporting,
        dry_run=dry_run,
        rebuild_start=_ym(start),
        rebuild_end=_ym(end),
    )


def plan_incremental_periods(
    *,
    today: Optional[date] = None,
    dry_run: bool = True,
    raw_present: Optional[LayerPresenceFn] = None,
    calc_present: Optional[LayerPresenceFn] = None,
    cdm_present: Optional[LayerPresenceFn] = None,
    raw_engine: Any = None,
    calc_engine: Any = None,
    cdm_engine: Any = None,
) -> MonthlyPeriodsPlan:
    """Missing completed months per layer based on PostgreSQL presence."""
    as_of = today if today is not None else date.today()
    last = last_completed_month(as_of)

    if last < DEFAULT_RAW_START:
        return MonthlyPeriodsPlan(
            mode="incremental",
            last_complete_month=_ym(last),
            raw_periods=(),
            calc_periods=(),
            cdm_periods=(),
            dry_run=dry_run,
        )

    raw_candidates = _months_yyyy_mm(DEFAULT_RAW_START, last)
    if last >= DEFAULT_REPORTING_START:
        reporting_candidates = _months_yyyy_mm(DEFAULT_REPORTING_START, last)
    else:
        reporting_candidates = ()

    probe_raw = raw_present
    if probe_raw is None:
        if raw_engine is None:
            raise PeriodValidationError(
                "incremental planning requires raw_engine or raw_present probe"
            )

        def probe_raw(month: date, _engine: Any = raw_engine) -> bool:
            return raw_month_present(_engine, month)

    probe_calc = calc_present
    if probe_calc is None:
        if calc_engine is None:
            raise PeriodValidationError(
                "incremental planning requires calc_engine or calc_present probe"
            )

        def probe_calc(month: date, _engine: Any = calc_engine) -> bool:
            return calc_month_present(_engine, month)

    probe_cdm = cdm_present
    if probe_cdm is None:
        if cdm_engine is None:
            raise PeriodValidationError(
                "incremental planning requires cdm_engine or cdm_present probe"
            )

        def probe_cdm(month: date, _engine: Any = cdm_engine) -> bool:
            return cdm_month_present(_engine, month)

    raw_missing = _missing_months(raw_candidates, is_present=probe_raw)
    calc_missing = _missing_months(reporting_candidates, is_present=probe_calc)
    cdm_missing = _missing_months(reporting_candidates, is_present=probe_cdm)

    return MonthlyPeriodsPlan(
        mode="incremental",
        last_complete_month=_ym(last),
        raw_periods=raw_missing,
        calc_periods=calc_missing,
        cdm_periods=cdm_missing,
        dry_run=dry_run,
    )


def plan_monthly_periods(
    *,
    mode: Any = None,
    rebuild_start: Any = None,
    rebuild_end: Any = None,
    dry_run: Any = None,
    today: Optional[date] = None,
    raw_present: Optional[LayerPresenceFn] = None,
    calc_present: Optional[LayerPresenceFn] = None,
    cdm_present: Optional[LayerPresenceFn] = None,
    raw_engine: Any = None,
    calc_engine: Any = None,
    cdm_engine: Any = None,
    conf: Optional[Mapping[str, Any]] = None,
    params: Optional[Mapping[str, Any]] = None,
) -> MonthlyPeriodsPlan:
    """Build execution plan for ``monthly_private_pipeline``.

    ``mode=incremental`` (default): missing completed months from PostgreSQL.
    ``mode=rebuild``: forced range from ``rebuild_start``/``rebuild_end``.
    ``mode=validate``: empty ETL periods; DQ validates existing DWH read-only.
    """
    conf_map = dict(conf or {})
    param_map = dict(params or {})

    def _pick(key: str, explicit: Any, default: Any = None) -> Any:
        if explicit is not None:
            if not (isinstance(explicit, str) and not str(explicit).strip()):
                return explicit
        if key in conf_map and conf_map.get(key) is not None:
            val = conf_map.get(key)
            if not (isinstance(val, str) and not str(val).strip()):
                return val
        if key in param_map and param_map.get(key) is not None:
            val = param_map.get(key)
            if not (isinstance(val, str) and not str(val).strip()):
                return val
        return default

    resolved_mode = _resolve_mode(_pick("mode", mode, "incremental"))
    resolved_dry = _coerce_bool(_pick("dry_run", dry_run, True), default=True)

    if resolved_mode == "validate":
        plan = plan_validate_periods(today=today, dry_run=resolved_dry)
    elif resolved_mode == "rebuild":
        start = _blank_to_none(_pick("rebuild_start", rebuild_start, None))
        end = _blank_to_none(_pick("rebuild_end", rebuild_end, None))
        if start is None:
            raise PeriodValidationError(
                "mode=rebuild requires rebuild_start (YYYY-MM)"
            )
        plan = plan_rebuild_periods(
            rebuild_start=start,
            rebuild_end=end,
            today=today,
            dry_run=resolved_dry,
        )
    else:
        # Incremental: ignore rebuild_* (safe / simplest).
        plan = plan_incremental_periods(
            today=today,
            dry_run=resolved_dry,
            raw_present=raw_present,
            calc_present=calc_present,
            cdm_present=cdm_present,
            raw_engine=raw_engine,
            calc_engine=calc_engine,
            cdm_engine=cdm_engine,
        )

    logger.info(
        "monthly_periods_plan mode=%s last_complete_month=%s "
        "raw_periods=%s calc_periods=%s cdm_periods=%s dry_run=%s",
        plan.mode,
        plan.last_complete_month,
        list(plan.raw_periods),
        list(plan.calc_periods),
        list(plan.cdm_periods),
        plan.dry_run,
    )
    return plan


def plan_from_safe_dict(data: Mapping[str, Any]) -> MonthlyPeriodsPlan:
    """Rehydrate plan from XCom payload."""
    return MonthlyPeriodsPlan(
        mode=_resolve_mode(data.get("mode", "incremental")),
        last_complete_month=str(data["last_complete_month"]),
        raw_periods=tuple(str(x) for x in (data.get("raw_periods") or ())),
        calc_periods=tuple(str(x) for x in (data.get("calc_periods") or ())),
        cdm_periods=tuple(str(x) for x in (data.get("cdm_periods") or ())),
        dry_run=_coerce_bool(data.get("dry_run"), default=True),
        rebuild_start=_blank_to_none(data.get("rebuild_start")),
        rebuild_end=_blank_to_none(data.get("rebuild_end")),
    )


__all__ = [
    "DEFAULT_RAW_START",
    "DEFAULT_REPORTING_START",
    "MonthlyPeriodsPlan",
    "calc_month_present",
    "cdm_month_present",
    "plan_from_safe_dict",
    "plan_incremental_periods",
    "plan_monthly_periods",
    "plan_rebuild_periods",
    "plan_validate_periods",
    "raw_month_present",
    "reject_incomplete_month",
]
