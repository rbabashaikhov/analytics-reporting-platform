"""Monthly Airflow orchestration helpers.

DAGs must call these helpers / ``run_monthly_period`` — not RAW/CALC/CDM logic.

Secrets stay outside source: process env or an env-file path provided via
environment / Airflow Variables (path only). Adobe client YAML path is similarly
external. Connection IDs documented for operators; runtime still uses package
``load_settings`` (existing private config pattern).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from analytics_reporting_platform.execution.models import (
    ExecutionMode,
    MonthlyExecutionError,
    MonthlyExecutionEvent,
    MonthlyExecutionResult,
    MonthlyPipelineDeps,
)
from analytics_reporting_platform.execution.period import (
    PeriodValidationError,
    last_completed_month,
    validate_execution_month,
)

logger = logging.getLogger(__name__)

MONTHLY_DAG_ID = "monthly_private_pipeline"

# Airflow Variable / env keys (values are paths or connection ids — not secrets).
ENV_FILE_ENV = "ARP_ENV_FILE"
CLIENT_CONFIG_ENV = "ARP_CLIENT_CONFIG"
# Optional Airflow Variable names (when Variables are configured in the Airflow UI).
ENV_FILE_VARIABLE = "arp_env_file"
CLIENT_CONFIG_VARIABLE = "arp_client_config"
# Documented Connection id for DWH Postgres (secrets live in the Connection, not DAG).
DWH_CONNECTION_ID = "arp_dwh_postgres"

_VALID_MODES: frozenset[str] = frozenset({"incremental", "rebuild", "validate"})


class MonthlyOrchestrationError(RuntimeError):
    """Raised when Airflow-facing orchestration cannot proceed."""

    def __init__(
        self,
        message: str,
        *,
        result: Optional[MonthlyExecutionResult] = None,
    ) -> None:
        super().__init__(message)
        self.result = result


@dataclass(frozen=True)
class MonthlyAirflowParams:
    """Resolved Monthly run parameters for one DAG run."""

    period: date
    mode: ExecutionMode
    dry_run: bool = False
    source: str = "scheduled"  # scheduled | manual


def _blank_to_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    raise MonthlyOrchestrationError(f"Invalid boolean value: {value!r}")


def _resolve_mode(raw: Any) -> ExecutionMode:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return "incremental"
    mode = str(raw).strip().lower()
    if mode not in _VALID_MODES:
        raise PeriodValidationError(
            f"Unsupported execution mode: {raw!r}. Expected incremental|rebuild|validate"
        )
    return mode  # type: ignore[return-value]


def _as_of_date(
    *,
    today: Optional[date],
    schedule_as_of: Optional[date],
) -> date:
    """Calendar 'as of' date for last-completed-month resolution.

    Prefer explicit ``today`` (tests), then Airflow schedule context
    (``schedule_as_of`` from data_interval_end), else wall-clock today.
    """
    if today is not None:
        return today
    if schedule_as_of is not None:
        return schedule_as_of
    return date.today()


def resolve_monthly_period(
    *,
    target_month: Any = None,
    mode: Any = None,
    dry_run: Any = None,
    today: Optional[date] = None,
    schedule_as_of: Optional[date] = None,
    dag_run_conf: Optional[Mapping[str, Any]] = None,
    params: Optional[Mapping[str, Any]] = None,
) -> MonthlyAirflowParams:
    """Resolve target period + mode for scheduled or manual Monthly runs.

    Precedence for target month:
    1. Explicit ``target_month`` argument
    2. ``dag_run.conf["target_month"]`` (manual trigger / backfill)
    3. ``params["target_month"]`` (Airflow Params UI default)
    4. Else last completed calendar month relative to schedule context

    For (4), ``schedule_as_of`` must come from Airflow ``data_interval_end``
    (DAG timezone date) so delayed/replayed scheduled runs stay deterministic.
    ``today`` overrides for tests only.

    Precedence for mode / dry_run: explicit arg → dag_run.conf → params → defaults
    (``incremental`` / ``False``).

    Future months and malformed values are rejected (no implicit future period).
    """
    conf = dict(dag_run_conf or {})
    dag_params = dict(params or {})

    explicit = _blank_to_none(target_month)
    if explicit is None:
        explicit = _blank_to_none(conf.get("target_month"))
    if explicit is None:
        explicit = _blank_to_none(dag_params.get("target_month"))

    mode_raw = mode
    if mode_raw is None or (isinstance(mode_raw, str) and not str(mode_raw).strip()):
        mode_raw = conf.get("mode")
    if mode_raw is None or (isinstance(mode_raw, str) and not str(mode_raw).strip()):
        mode_raw = dag_params.get("mode")
    resolved_mode = _resolve_mode(mode_raw)

    if dry_run is not None:
        dry_raw = dry_run
    elif "dry_run" in conf:
        dry_raw = conf.get("dry_run")
    elif "dry_run" in dag_params:
        dry_raw = dag_params.get("dry_run")
    else:
        dry_raw = False
    resolved_dry = _coerce_bool(dry_raw, default=False)

    as_of = _as_of_date(today=today, schedule_as_of=schedule_as_of)

    if explicit is not None:
        period = validate_execution_month(explicit, today=as_of, allow_future=False)
        source = "manual"
    else:
        period = last_completed_month(as_of)
        period = validate_execution_month(period, today=as_of, allow_future=False)
        source = "scheduled"

    return MonthlyAirflowParams(
        period=period,
        mode=resolved_mode,
        dry_run=resolved_dry,
        source=source,
    )


def build_monthly_pipeline_deps(
    *,
    env_file: Optional[str] = None,
    client_config: Optional[str] = None,
) -> MonthlyPipelineDeps:
    """Build ``MonthlyPipelineDeps`` from private env-file / process env + Adobe YAML.

    Paths may come from arguments or ``ARP_ENV_FILE`` / ``ARP_CLIENT_CONFIG``.
    No secrets are read from DAG source.
    """
    from analytics_reporting_platform.config.client_config import load_client_config
    from analytics_reporting_platform.config.settings import load_settings
    from analytics_reporting_platform.db.engine import create_db_engine
    from analytics_reporting_platform.sources.adobe.client import AdobeAuthClient
    from analytics_reporting_platform.sources.adobe.reports import AdobeReportsClient

    resolved_env = _blank_to_none(env_file) or _blank_to_none(os.environ.get(ENV_FILE_ENV))
    resolved_client = _blank_to_none(client_config) or _blank_to_none(
        os.environ.get(CLIENT_CONFIG_ENV)
    )

    settings = load_settings(resolved_env)
    raw_engine = create_db_engine(settings, "raw")
    calc_engine = create_db_engine(settings, "calc")
    cdm_engine = create_db_engine(settings, "cdm")

    adobe_reports = None
    adobe_cfg = None
    if resolved_client:
        adobe_cfg = load_client_config(Path(resolved_client))
        auth = settings.require_adobe_auth()
        aa = AdobeAuthClient(auth)
        adobe_reports = AdobeReportsClient(aa, adobe_cfg.report_suite_id)

    wordstat_client = None
    try:
        from analytics_reporting_platform.sources.wordstat import (
            WordstatClient,
            load_wordstat_credentials,
        )
        from analytics_reporting_platform.sources.wordstat.errors import (
            WordstatConfigurationError,
        )

        wordstat_creds = load_wordstat_credentials(resolved_env)
        wordstat_client = WordstatClient(wordstat_creds)
    except WordstatConfigurationError:
        wordstat_client = None

    return MonthlyPipelineDeps(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        cdm_engine=cdm_engine,
        adobe_reports=adobe_reports,
        client_config=adobe_cfg,
        wordstat_client=wordstat_client,
    )


def _airflow_event_hook(event: MonthlyExecutionEvent) -> None:
    """Minimal operational logging (no secrets / KPI values)."""
    parts = [
        f"event={event.event}",
        f"period={event.period.isoformat()}",
        f"mode={event.mode}",
    ]
    if event.stage:
        parts.append(f"stage={event.stage}")
    if event.mart:
        parts.append(f"mart={event.mart}")
    if event.detail:
        parts.append(f"detail={event.detail}")
    logger.info("monthly_execution %s", " ".join(parts))


SAFE_RESULT_FIELDS: tuple[str, ...] = (
    "period",
    "mode",
    "dry_run",
    "raw_status",
    "wordstat_status",
    "calc_status",
    "cdm_status",
    "overall_status",
    "failed_stage",
    "failed_mart",
)

SAFE_STAGE_FIELDS: tuple[str, ...] = (
    "period",
    "mode",
    "dry_run",
    "stage",
    "status",
    "failed_mart",
    "error",
)


def safe_result_payload(result: MonthlyExecutionResult) -> dict[str, Any]:
    """XCom-/log-safe summary (no secrets, no KPI/page values).

    Always includes the verifiable status keys (failed_* may be null).
    """
    return {
        "period": result.period.isoformat(),
        "mode": result.mode,
        "dry_run": result.dry_run,
        "raw_status": result.raw_status,
        "wordstat_status": result.wordstat_status,
        "calc_status": result.calc_status,
        "cdm_status": result.cdm_status,
        "overall_status": result.overall_status,
        "failed_stage": result.failed_stage,
        "failed_mart": result.failed_mart,
        "error": result.error,
    }


def safe_stage_payload(
    *,
    period: date,
    mode: str,
    dry_run: bool,
    stage_result: "StageRunResult",
) -> dict[str, Any]:
    """Safe per-stage XCom summary for Airflow stage tasks."""
    failed_mart = next(
        (m.mart for m in stage_result.marts if m.status == "failed"), None
    )
    return {
        "period": period.isoformat(),
        "mode": mode,
        "dry_run": dry_run,
        "stage": stage_result.stage,
        "status": stage_result.status,
        "failed_mart": failed_mart,
        "error": stage_result.error,
        "mart_statuses": {m.mart: m.status for m in stage_result.marts},
    }


def final_status_payload(
    *,
    resolved: MonthlyAirflowParams,
    result: Optional[MonthlyExecutionResult] = None,
    error: Optional[str] = None,
) -> dict[str, Any]:
    """Terminal status object for the ``final_status`` task / logs."""
    payload: dict[str, Any] = {
        "dag_id": MONTHLY_DAG_ID,
        "period": resolved.period.isoformat(),
        "mode": resolved.mode,
        "dry_run": resolved.dry_run,
        "source": resolved.source,
    }
    if result is not None:
        payload.update(safe_result_payload(result))
        payload["status"] = result.overall_status
    elif error:
        payload["status"] = "failed"
        payload["error"] = error
    else:
        payload["status"] = "unknown"
    return payload


def _prepare_deps(
    resolved: MonthlyAirflowParams,
    *,
    deps: Optional[MonthlyPipelineDeps],
    env_file: Optional[str],
    client_config: Optional[str],
) -> Optional[MonthlyPipelineDeps]:
    effective_deps = deps
    if not resolved.dry_run and effective_deps is None:
        try:
            effective_deps = build_monthly_pipeline_deps(
                env_file=env_file,
                client_config=client_config,
            )
        except Exception as exc:  # noqa: BLE001
            raise MonthlyOrchestrationError(
                f"Failed to build Monthly pipeline dependencies: {type(exc).__name__}: {exc}"
            ) from exc
    if effective_deps is not None and effective_deps.on_event is None:
        effective_deps = MonthlyPipelineDeps(
            raw_engine=effective_deps.raw_engine,
            calc_engine=effective_deps.calc_engine,
            cdm_engine=effective_deps.cdm_engine,
            adobe_reports=effective_deps.adobe_reports,
            client_config=effective_deps.client_config,
            wordstat_client=effective_deps.wordstat_client,
            on_event=_airflow_event_hook,
        )
    return effective_deps


def run_stage_for_airflow(
    stage: str,
    resolved: MonthlyAirflowParams,
    *,
    deps: Optional[MonthlyPipelineDeps] = None,
    env_file: Optional[str] = None,
    client_config: Optional[str] = None,
    today: Optional[date] = None,
) -> dict[str, Any]:
    """Run one Monthly stage via package API; raise on failure (fail-fast)."""
    from analytics_reporting_platform.execution.models import StageRunResult
    from analytics_reporting_platform.execution.monthly import (
        run_monthly_calc,
        run_monthly_cdm,
        run_monthly_raw,
        run_monthly_wordstat_raw,
    )

    if stage not in {"raw", "wordstat_raw", "calc", "cdm"}:
        raise MonthlyOrchestrationError(f"Unsupported stage: {stage!r}")

    logger.info(
        "monthly_stage_start stage=%s period=%s mode=%s dry_run=%s source=%s",
        stage,
        resolved.period.isoformat(),
        resolved.mode,
        resolved.dry_run,
        resolved.source,
    )
    effective_deps = _prepare_deps(
        resolved, deps=deps, env_file=env_file, client_config=client_config
    )
    as_of = today if today is not None else date.today()
    runners = {
        "raw": run_monthly_raw,
        "wordstat_raw": run_monthly_wordstat_raw,
        "calc": run_monthly_calc,
        "cdm": run_monthly_cdm,
    }
    try:
        stage_result: StageRunResult = runners[stage](
            resolved.period,
            mode=resolved.mode,
            deps=effective_deps,
            dry_run=resolved.dry_run,
            today=as_of,
            allow_future=False,
        )
    except (PeriodValidationError, MonthlyExecutionError) as exc:
        logger.error(
            "monthly_stage_failed stage=%s period=%s error=%s",
            stage,
            resolved.period.isoformat(),
            exc,
        )
        raise

    payload = safe_stage_payload(
        period=resolved.period,
        mode=resolved.mode,
        dry_run=resolved.dry_run,
        stage_result=stage_result,
    )
    logger.info(
        "monthly_stage_end %s",
        " ".join(f"{k}={v}" for k, v in payload.items() if k != "mart_statuses"),
    )
    if stage_result.status == "failed":
        raise MonthlyOrchestrationError(
            f"Monthly stage failed stage={stage} "
            f"mart={payload.get('failed_mart') or '-'} "
            f"detail={stage_result.error or 'failed'}",
            result=None,
        )
    return payload


def run_monthly_for_airflow(
    resolved: MonthlyAirflowParams,
    *,
    deps: Optional[MonthlyPipelineDeps] = None,
    env_file: Optional[str] = None,
    client_config: Optional[str] = None,
    today: Optional[date] = None,
) -> MonthlyExecutionResult:
    """Execute full Monthly package API (CLI / composed path; fail-fast)."""
    logger.info(
        "monthly_orchestration start period=%s mode=%s dry_run=%s source=%s",
        resolved.period.isoformat(),
        resolved.mode,
        resolved.dry_run,
        resolved.source,
    )

    effective_deps = _prepare_deps(
        resolved, deps=deps, env_file=env_file, client_config=client_config
    )

    try:
        from analytics_reporting_platform.execution.monthly import run_monthly_period

        result = run_monthly_period(
            resolved.period,
            mode=resolved.mode,
            deps=effective_deps,
            dry_run=resolved.dry_run,
            today=today if today is not None else date.today(),
            allow_future=False,
        )
    except (PeriodValidationError, MonthlyExecutionError) as exc:
        logger.error(
            "monthly_orchestration failed period=%s mode=%s error=%s",
            resolved.period.isoformat(),
            resolved.mode,
            exc,
        )
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "monthly_orchestration failed period=%s mode=%s error=%s: %s",
            resolved.period.isoformat(),
            resolved.mode,
            type(exc).__name__,
            exc,
        )
        raise

    payload = safe_result_payload(result)
    logger.info(
        "monthly_orchestration end %s",
        " ".join(f"{k}={v}" for k, v in payload.items()),
    )

    if result.overall_status == "failed":
        detail = result.error or "Monthly pipeline failed"
        failed = result.failed_stage or "unknown"
        mart = result.failed_mart or "-"
        raise MonthlyOrchestrationError(
            f"Monthly pipeline failed stage={failed} mart={mart} detail={detail}",
            result=result,
        )

    return result


def months_for_backfill(
    start: str,
    end: str,
    *,
    today: Optional[date] = None,
) -> list[str]:
    """Return validated ``YYYY-MM`` strings for a practical Airflow trigger loop.

    Rejects incomplete current month and future months (completed months only).
    """
    from analytics_reporting_platform.execution.backfill import (
        months_for_backfill as _months,
    )

    return _months(start, end, today=today)


def run_stage_periods_for_airflow(
    stage: str,
    periods: Sequence[str],
    *,
    mode: ExecutionMode,
    dry_run: bool,
    deps: Optional[MonthlyPipelineDeps] = None,
    env_file: Optional[str] = None,
    client_config: Optional[str] = None,
    today: Optional[date] = None,
) -> dict[str, Any]:
    """Run one Monthly stage over ``periods`` (fail-fast).

    Adobe RAW/CALC/CDM run sequentially per month (no parallel Adobe).
    Wordstat RAW uses range extraction (two API calls for a multi-month span).
    Empty ``periods`` → success / no-op. Does not add Airflow-level retries.
    """
    from analytics_reporting_platform.execution.models import StageRunResult
    from analytics_reporting_platform.execution.monthly import (
        run_monthly_calc,
        run_monthly_cdm,
        run_monthly_raw,
        run_wordstat_raw_periods,
    )
    from analytics_reporting_platform.execution.monthly_plan import reject_incomplete_month

    if stage not in {"raw", "wordstat_raw", "calc", "cdm"}:
        raise MonthlyOrchestrationError(f"Unsupported stage: {stage!r}")

    stage_upper = {
        "raw": "RAW",
        "wordstat_raw": "WORDSTAT_RAW",
        "calc": "CALC",
        "cdm": "CDM",
    }[stage]
    period_list = [str(p) for p in periods]
    if not period_list:
        logger.info(
            "mode=%s stage=%s period=- status=SUCCESS detail=no-op empty periods",
            mode,
            stage_upper,
        )
        return {
            "stage": stage,
            "status": "success",
            "mode": mode,
            "dry_run": dry_run,
            "periods": [],
            "period_results": [],
            "detail": "no-op",
        }

    # Build deps once for the stage loop (skip engines entirely on dry_run).
    placeholder = MonthlyAirflowParams(
        period=date(2000, 1, 1),
        mode=mode,
        dry_run=dry_run,
        source="manual",
    )
    effective_deps = _prepare_deps(
        placeholder, deps=deps, env_file=env_file, client_config=client_config
    )
    as_of = today if today is not None else date.today()

    if stage == "wordstat_raw":
        months = [reject_incomplete_month(p, today=as_of) for p in period_list]
        logger.info(
            "mode=%s stage=%s period=%s..%s status=START months=%s",
            mode,
            stage_upper,
            period_list[0],
            period_list[-1],
            len(period_list),
        )
        try:
            stage_result = run_wordstat_raw_periods(
                months,
                mode=mode,
                deps=effective_deps,
                dry_run=dry_run,
            )
        except (PeriodValidationError, MonthlyExecutionError) as exc:
            logger.error(
                "mode=%s stage=%s status=FAILED error=%s",
                mode,
                stage_upper,
                exc,
            )
            raise MonthlyOrchestrationError(
                f"stage={stage_upper} status=FAILED detail={exc}"
            ) from exc
        payload = safe_stage_payload(
            period=months[-1],
            mode=mode,
            dry_run=dry_run,
            stage_result=stage_result,
        )
        status = str(stage_result.status).upper()
        logger.info(
            "mode=%s stage=%s period=%s..%s status=%s",
            mode,
            stage_upper,
            period_list[0],
            period_list[-1],
            status,
        )
        if stage_result.status == "failed":
            raise MonthlyOrchestrationError(
                f"stage={stage_upper} status=FAILED "
                f"mart={payload.get('failed_mart') or '-'} "
                f"detail={stage_result.error or 'failed'}"
            )
        overall = "planned" if dry_run else (
            "skipped" if stage_result.status == "skipped" else "success"
        )
        return {
            "stage": stage,
            "status": overall,
            "mode": mode,
            "dry_run": dry_run,
            "periods": period_list,
            "period_results": [payload],
        }

    runners = {
        "raw": run_monthly_raw,
        "calc": run_monthly_calc,
        "cdm": run_monthly_cdm,
    }
    period_results: list[dict[str, Any]] = []

    for period in period_list:
        reject_incomplete_month(period, today=as_of)
        logger.info(
            "mode=%s stage=%s period=%s status=START",
            mode,
            stage_upper,
            period,
        )
        try:
            stage_result = runners[stage](
                period,
                mode=mode,
                deps=effective_deps,
                dry_run=dry_run,
                today=as_of,
                allow_future=False,
            )
        except (PeriodValidationError, MonthlyExecutionError) as exc:
            logger.error(
                "mode=%s stage=%s period=%s status=FAILED error=%s",
                mode,
                stage_upper,
                period,
                exc,
            )
            raise MonthlyOrchestrationError(
                f"stage={stage_upper} period={period} status=FAILED detail={exc}"
            ) from exc

        period_date = parse_period_date(period)
        payload = safe_stage_payload(
            period=period_date,
            mode=mode,
            dry_run=dry_run,
            stage_result=stage_result,
        )
        status = str(stage_result.status).upper()
        logger.info(
            "mode=%s stage=%s period=%s status=%s",
            mode,
            stage_upper,
            period,
            status,
        )
        if stage_result.status == "failed":
            raise MonthlyOrchestrationError(
                f"stage={stage_upper} period={period} status=FAILED "
                f"mart={payload.get('failed_mart') or '-'} "
                f"detail={stage_result.error or 'failed'}"
            )
        period_results.append(payload)

    overall = "planned" if dry_run else "success"
    return {
        "stage": stage,
        "status": overall,
        "mode": mode,
        "dry_run": dry_run,
        "periods": period_list,
        "period_results": period_results,
    }


def parse_period_date(period: str) -> date:
    from analytics_reporting_platform.execution.period import parse_execution_month

    return parse_execution_month(period)


__all__ = [
    "CLIENT_CONFIG_ENV",
    "CLIENT_CONFIG_VARIABLE",
    "DWH_CONNECTION_ID",
    "ENV_FILE_ENV",
    "ENV_FILE_VARIABLE",
    "MONTHLY_DAG_ID",
    "MonthlyAirflowParams",
    "MonthlyOrchestrationError",
    "SAFE_RESULT_FIELDS",
    "SAFE_STAGE_FIELDS",
    "build_monthly_pipeline_deps",
    "final_status_payload",
    "months_for_backfill",
    "parse_period_date",
    "resolve_monthly_period",
    "run_monthly_for_airflow",
    "run_stage_for_airflow",
    "run_stage_periods_for_airflow",
    "safe_result_payload",
    "safe_stage_payload",
]
