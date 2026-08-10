"""Monthly RAW → CALC → CDM orchestration service.

Coordinates existing layer builders/persistence. Contains no mart business logic.
"""

from __future__ import annotations

from datetime import date
from typing import Callable, Optional, Protocol, Sequence

from sqlalchemy.engine import Engine

from analytics_reporting_platform.calc import (
    build_brand_demand,
    build_explore_buying_guides_kpi as build_calc_explore_bg,
    build_monthly_channel_mix,
    build_monthly_kpi,
    build_monthly_search_engines,
    build_monthly_section_share,
    persist_brand_demand,
    persist_explore_buying_guides_kpi as persist_calc_explore_bg,
    persist_monthly_channel_mix,
    persist_monthly_kpi,
    persist_monthly_search_engines,
    persist_monthly_section_share,
)
from analytics_reporting_platform.cdm import (
    build_device_section_mix,
    build_dim_month,
    build_explore_buying_guides_kpi as build_cdm_explore_bg_kpi,
    build_explore_buying_guides_trend,
    build_geo_traffic_by_section,
    build_geo_traffic_kpi,
    build_geo_traffic_trend,
    build_organic_brand_demand,
    build_organic_search_engine_mix,
    build_organic_search_engine_trend,
    build_organic_search_kpi,
    build_overall_traffic_trend,
    build_sections_mix,
    build_top_pages,
    build_traffic_kpi,
    build_traffic_source_mix,
    default_period_config,
    persist_device_section_mix,
    persist_dim_month,
    persist_explore_buying_guides_kpi as persist_cdm_explore_bg_kpi,
    persist_explore_buying_guides_trend,
    persist_geo_traffic_by_section,
    persist_geo_traffic_kpi,
    persist_geo_traffic_trend,
    persist_organic_brand_demand,
    persist_organic_search_engine_mix,
    persist_organic_search_engine_trend,
    persist_organic_search_kpi,
    persist_overall_traffic_trend,
    persist_sections_mix,
    persist_top_pages,
    persist_traffic_kpi,
    persist_traffic_source_mix,
)
from analytics_reporting_platform.cdm.models import CdmPeriodConfig, DEFAULT_REPORT_START
from analytics_reporting_platform.execution.models import (
    ExecutionMode,
    MartRunResult,
    MonthlyExecutionError,
    MonthlyExecutionEvent,
    MonthlyExecutionResult,
    MonthlyPipelineDeps,
    StageName,
    StageRunResult,
    StageStatus,
)
from analytics_reporting_platform.execution.period import (
    ExecutionMonthInput,
    PeriodValidationError,
    validate_execution_month,
)
from analytics_reporting_platform.raw import (
    extract_monthly_by_section,
    extract_monthly_by_url,
    extract_monthly_metrics,
    extract_wordstat_brand_queries,
    month_exists,
    persist_month,
)

class SupportsPostReport(Protocol):
    def post_report(self, payload: object) -> object: ...


ADOBE_RAW_MARTS: tuple[str, ...] = (
    "monthly_metrics",
    "monthly_by_section",
    "monthly_by_url",
)
WORDSTAT_RAW_MART: str = "wordstat_brand_queries"
RAW_MARTS: tuple[str, ...] = ADOBE_RAW_MARTS + (WORDSTAT_RAW_MART,)
CALC_MARTS: tuple[str, ...] = (
    "monthly_kpi",
    "monthly_channel_mix",
    "monthly_search_engines",
    "monthly_section_share",
    "explore_buying_guides_kpi",
    "brand_demand",
)
CDM_MARTS: tuple[str, ...] = (
    "dim_month",
    "traffic_kpi",
    "overall_traffic_trend",
    "traffic_source_mix",
    "geo_traffic_kpi",
    "geo_traffic_trend",
    "geo_traffic_by_section",
    "organic_search_kpi",
    "organic_search_engine_mix",
    "organic_search_engine_trend",
    "organic_brand_demand",
    "explore_buying_guides_kpi",
    "explore_buying_guides_trend",
    "sections_mix",
    "top_pages",
    "device_section_mix",
)


def raw_marts_for_month(month: date) -> tuple[str, ...]:
    """RAW marts required for completeness of ``month``.

    Wordstat Brand Demand starts at the reporting boundary (2026-01).
    Earlier Adobe RAW history remains Adobe-only so 2025 months stay complete.
    """
    if month >= DEFAULT_REPORT_START:
        return RAW_MARTS
    return ADOBE_RAW_MARTS


def _emit(
    deps: MonthlyPipelineDeps,
    *,
    event: str,
    period: date,
    mode: ExecutionMode,
    stage: Optional[StageName] = None,
    mart: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    if deps.on_event is None:
        return
    deps.on_event(
        MonthlyExecutionEvent(
            event=event,
            period=period,
            mode=mode,
            stage=stage,
            mart=mart,
            detail=detail,
        )
    )


def _persist_mode(mode: ExecutionMode) -> str:
    return "skip" if mode == "incremental" else "replace"


def _raw_month_complete(raw_engine: Engine, month: date) -> bool:
    return all(month_exists(raw_engine, mart, month) for mart in raw_marts_for_month(month))


def _run_raw(
    deps: MonthlyPipelineDeps,
    month: date,
    mode: ExecutionMode,
    *,
    dry_run: bool,
) -> StageRunResult:
    required_marts = raw_marts_for_month(month)
    planned = tuple(MartRunResult(mart=m, status="planned") for m in required_marts)
    if dry_run:
        return StageRunResult(stage="raw", status="planned", marts=planned)

    if mode == "incremental" and _raw_month_complete(deps.raw_engine, month):
        skipped = tuple(
            MartRunResult(mart=m, status="skipped", detail="month already present")
            for m in required_marts
        )
        return StageRunResult(stage="raw", status="skipped", marts=skipped)

    if deps.adobe_reports is None or deps.client_config is None:
        raise MonthlyExecutionError(
            "RAW extraction requires adobe_reports and client_config dependencies"
        )

    needs_wordstat = WORDSTAT_RAW_MART in required_marts
    if needs_wordstat and deps.wordstat_client is None:
        raise MonthlyExecutionError(
            "RAW Wordstat extraction requires wordstat_client dependency"
        )

    persist_mode = _persist_mode(mode)
    mart_results: list[MartRunResult] = []
    try:
        _emit(deps, event="stage_start", period=month, mode=mode, stage="raw")
        metrics = extract_monthly_metrics(
            deps.adobe_reports, deps.client_config, month
        )
        res_m = persist_month(
            deps.raw_engine, "monthly_metrics", metrics.dataframe, month, mode=persist_mode  # type: ignore[arg-type]
        )
        mart_results.append(
            MartRunResult(mart="monthly_metrics", status="success", detail=res_m.status)
        )

        by_section = extract_monthly_by_section(
            deps.adobe_reports, deps.client_config, month
        )
        res_s = persist_month(
            deps.raw_engine,
            "monthly_by_section",
            by_section.dataframe,
            month,
            mode=persist_mode,  # type: ignore[arg-type]
        )
        mart_results.append(
            MartRunResult(mart="monthly_by_section", status="success", detail=res_s.status)
        )

        by_url = extract_monthly_by_url(deps.adobe_reports, deps.client_config, month)
        res_u = persist_month(
            deps.raw_engine, "monthly_by_url", by_url.dataframe, month, mode=persist_mode  # type: ignore[arg-type]
        )
        mart_results.append(
            MartRunResult(mart="monthly_by_url", status="success", detail=res_u.status)
        )

        if needs_wordstat:
            wordstat = extract_wordstat_brand_queries(deps.wordstat_client, month)
            res_w = persist_month(
                deps.raw_engine,
                "wordstat_brand_queries",
                wordstat.dataframe,
                month,
                mode=persist_mode,  # type: ignore[arg-type]
            )
            mart_results.append(
                MartRunResult(
                    mart="wordstat_brand_queries",
                    status="success",
                    detail=res_w.status,
                )
            )
    except Exception as exc:
        failed_mart = (
            required_marts[len(mart_results)]
            if len(mart_results) < len(required_marts)
            else "raw"
        )
        mart_results.append(
            MartRunResult(mart=failed_mart, status="failed", detail=type(exc).__name__)
        )
        return StageRunResult(
            stage="raw",
            status="failed",
            marts=tuple(mart_results),
            error=f"{type(exc).__name__}: {exc}",
        )
    return StageRunResult(stage="raw", status="success", marts=tuple(mart_results))


def _run_calc(
    deps: MonthlyPipelineDeps,
    month: date,
    mode: ExecutionMode,
    *,
    dry_run: bool,
) -> StageRunResult:
    planned = tuple(MartRunResult(mart=m, status="planned") for m in CALC_MARTS)
    if dry_run:
        return StageRunResult(stage="calc", status="planned", marts=planned)

    persist_mode = _persist_mode(mode)
    mart_results: list[MartRunResult] = []
    builders: Sequence[tuple[str, Callable[[], object], Callable[..., object]]] = (
        (
            "monthly_kpi",
            lambda: build_monthly_kpi(deps.raw_engine, month),
            lambda df: persist_monthly_kpi(deps.calc_engine, df, month, mode=persist_mode),  # type: ignore[arg-type]
        ),
        (
            "monthly_channel_mix",
            lambda: build_monthly_channel_mix(deps.raw_engine, month),
            lambda df: persist_monthly_channel_mix(
                deps.calc_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        ),
        (
            "monthly_search_engines",
            lambda: build_monthly_search_engines(deps.raw_engine, month),
            lambda df: persist_monthly_search_engines(
                deps.calc_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        ),
        (
            "monthly_section_share",
            lambda: build_monthly_section_share(deps.raw_engine, month),
            lambda df: persist_monthly_section_share(
                deps.calc_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        ),
        (
            "explore_buying_guides_kpi",
            lambda: build_calc_explore_bg(deps.raw_engine, month),
            lambda df: persist_calc_explore_bg(
                deps.calc_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        ),
        (
            "brand_demand",
            lambda: build_brand_demand(deps.raw_engine, month),
            lambda df: persist_brand_demand(
                deps.calc_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        ),
    )
    try:
        _emit(deps, event="stage_start", period=month, mode=mode, stage="calc")
        for mart_name, build_fn, persist_fn in builders:
            _emit(
                deps,
                event="mart_start",
                period=month,
                mode=mode,
                stage="calc",
                mart=mart_name,
            )
            built = build_fn()
            frame = built.dataframe  # type: ignore[attr-defined]
            persisted = persist_fn(frame)
            status = "skipped" if getattr(persisted, "status", None) == "skipped" else "success"
            mart_results.append(
                MartRunResult(
                    mart=mart_name,
                    status=status,  # type: ignore[arg-type]
                    detail=getattr(persisted, "status", None),
                )
            )
    except Exception as exc:
        failed_mart = (
            CALC_MARTS[len(mart_results)] if len(mart_results) < len(CALC_MARTS) else "calc"
        )
        mart_results.append(
            MartRunResult(mart=failed_mart, status="failed", detail=type(exc).__name__)
        )
        return StageRunResult(
            stage="calc",
            status="failed",
            marts=tuple(mart_results),
            error=f"{type(exc).__name__}: {exc}",
        )

    if all(m.status == "skipped" for m in mart_results):
        return StageRunResult(stage="calc", status="skipped", marts=tuple(mart_results))
    return StageRunResult(stage="calc", status="success", marts=tuple(mart_results))


def _run_cdm(
    deps: MonthlyPipelineDeps,
    month: date,
    mode: ExecutionMode,
    *,
    dry_run: bool,
    period: CdmPeriodConfig,
) -> StageRunResult:
    planned = tuple(MartRunResult(mart=m, status="planned") for m in CDM_MARTS)
    if dry_run:
        return StageRunResult(stage="cdm", status="planned", marts=planned)

    persist_mode = _persist_mode(mode)
    mart_results: list[MartRunResult] = []

    def _month_step(name: str, build, persist) -> None:
        built = build()
        persisted = persist(built.dataframe)
        status = "skipped" if getattr(persisted, "status", None) == "skipped" else "success"
        mart_results.append(
            MartRunResult(mart=name, status=status, detail=getattr(persisted, "status", None))  # type: ignore[arg-type]
        )

    try:
        _emit(deps, event="stage_start", period=month, mode=mode, stage="cdm")
        # Window-scoped first (dim + trends), then month-scoped report marts.
        dim = build_dim_month(period)
        dim_res = persist_dim_month(deps.cdm_engine, dim.dataframe, period, mode=persist_mode)  # type: ignore[arg-type]
        mart_results.append(
            MartRunResult(
                mart="dim_month",
                status="skipped" if dim_res.status == "skipped" else "success",
                detail=dim_res.status,
            )
        )

        _month_step(
            "traffic_kpi",
            lambda: build_traffic_kpi(deps.calc_engine, month),
            lambda df: persist_traffic_kpi(deps.cdm_engine, df, month, mode=persist_mode),  # type: ignore[arg-type]
        )

        overall = build_overall_traffic_trend(deps.raw_engine, period)
        overall_res = persist_overall_traffic_trend(
            deps.cdm_engine, overall.dataframe, period, mode=persist_mode  # type: ignore[arg-type]
        )
        mart_results.append(
            MartRunResult(
                mart="overall_traffic_trend",
                status="skipped" if overall_res.status == "skipped" else "success",
                detail=overall_res.status,
            )
        )

        _month_step(
            "traffic_source_mix",
            lambda: build_traffic_source_mix(
                deps.calc_engine, month, report_end=month
            ),
            lambda df: persist_traffic_source_mix(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )
        _month_step(
            "geo_traffic_kpi",
            lambda: build_geo_traffic_kpi(deps.calc_engine, month),
            lambda df: persist_geo_traffic_kpi(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )

        geo_trend = build_geo_traffic_trend(deps.raw_engine, period)
        geo_trend_res = persist_geo_traffic_trend(
            deps.cdm_engine, geo_trend.dataframe, period, mode=persist_mode  # type: ignore[arg-type]
        )
        mart_results.append(
            MartRunResult(
                mart="geo_traffic_trend",
                status="skipped" if geo_trend_res.status == "skipped" else "success",
                detail=geo_trend_res.status,
            )
        )

        _month_step(
            "geo_traffic_by_section",
            lambda: build_geo_traffic_by_section(deps.calc_engine, month),
            lambda df: persist_geo_traffic_by_section(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )
        _month_step(
            "organic_search_kpi",
            lambda: build_organic_search_kpi(deps.calc_engine, month),
            lambda df: persist_organic_search_kpi(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )
        _month_step(
            "organic_brand_demand",
            lambda: build_organic_brand_demand(deps.calc_engine, month),
            lambda df: persist_organic_brand_demand(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )
        _month_step(
            "organic_search_engine_mix",
            lambda: build_organic_search_engine_mix(
                deps.calc_engine, month, report_end=month
            ),
            lambda df: persist_organic_search_engine_mix(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )

        eng_trend = build_organic_search_engine_trend(deps.raw_engine, period)
        eng_trend_res = persist_organic_search_engine_trend(
            deps.cdm_engine, eng_trend.dataframe, period, mode=persist_mode  # type: ignore[arg-type]
        )
        mart_results.append(
            MartRunResult(
                mart="organic_search_engine_trend",
                status="skipped" if eng_trend_res.status == "skipped" else "success",
                detail=eng_trend_res.status,
            )
        )

        _month_step(
            "explore_buying_guides_kpi",
            lambda: build_cdm_explore_bg_kpi(deps.calc_engine, month),
            lambda df: persist_cdm_explore_bg_kpi(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )

        explore_trend = build_explore_buying_guides_trend(deps.raw_engine, period)
        explore_trend_res = persist_explore_buying_guides_trend(
            deps.cdm_engine, explore_trend.dataframe, period, mode=persist_mode  # type: ignore[arg-type]
        )
        mart_results.append(
            MartRunResult(
                mart="explore_buying_guides_trend",
                status="skipped" if explore_trend_res.status == "skipped" else "success",
                detail=explore_trend_res.status,
            )
        )

        _month_step(
            "sections_mix",
            lambda: build_sections_mix(deps.calc_engine, month, report_end=month),
            lambda df: persist_sections_mix(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )
        _month_step(
            "top_pages",
            lambda: build_top_pages(deps.raw_engine, month),
            lambda df: persist_top_pages(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )
        _month_step(
            "device_section_mix",
            lambda: build_device_section_mix(deps.calc_engine, month),
            lambda df: persist_device_section_mix(
                deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
            ),
        )
    except Exception as exc:
        failed_mart = (
            CDM_MARTS[len(mart_results)] if len(mart_results) < len(CDM_MARTS) else "cdm"
        )
        mart_results.append(
            MartRunResult(mart=failed_mart, status="failed", detail=type(exc).__name__)
        )
        return StageRunResult(
            stage="cdm",
            status="failed",
            marts=tuple(mart_results),
            error=f"{type(exc).__name__}: {exc}",
        )

    if all(m.status == "skipped" for m in mart_results):
        return StageRunResult(stage="cdm", status="skipped", marts=tuple(mart_results))
    return StageRunResult(stage="cdm", status="success", marts=tuple(mart_results))


def run_monthly_raw(
    month: ExecutionMonthInput,
    *,
    mode: ExecutionMode = "incremental",
    deps: Optional[MonthlyPipelineDeps] = None,
    dry_run: bool = False,
    today: Optional[date] = None,
    allow_future: bool = False,
) -> StageRunResult:
    """Execute Monthly RAW stage only (Adobe extract → RAW persist when needed)."""
    if mode not in ("incremental", "rebuild"):
        raise PeriodValidationError(f"Unsupported execution mode: {mode!r}")
    period = validate_execution_month(month, today=today, allow_future=allow_future)
    if not dry_run and deps is None:
        raise MonthlyExecutionError("Monthly RAW execution requires pipeline dependencies")
    effective_deps = deps or MonthlyPipelineDeps(
        raw_engine=None,  # type: ignore[arg-type]
        calc_engine=None,  # type: ignore[arg-type]
        cdm_engine=None,  # type: ignore[arg-type]
    )
    _emit(
        effective_deps,
        event="stage_request",
        period=period,
        mode=mode,
        stage="raw",
        detail="dry_run" if dry_run else "execute",
    )
    return _run_raw(effective_deps, period, mode, dry_run=dry_run)


def run_monthly_calc(
    month: ExecutionMonthInput,
    *,
    mode: ExecutionMode = "incremental",
    deps: Optional[MonthlyPipelineDeps] = None,
    dry_run: bool = False,
    today: Optional[date] = None,
    allow_future: bool = False,
) -> StageRunResult:
    """Execute Monthly CALC stage only (5 marts from persisted RAW)."""
    if mode not in ("incremental", "rebuild"):
        raise PeriodValidationError(f"Unsupported execution mode: {mode!r}")
    period = validate_execution_month(month, today=today, allow_future=allow_future)
    if not dry_run and deps is None:
        raise MonthlyExecutionError("Monthly CALC execution requires pipeline dependencies")
    effective_deps = deps or MonthlyPipelineDeps(
        raw_engine=None,  # type: ignore[arg-type]
        calc_engine=None,  # type: ignore[arg-type]
        cdm_engine=None,  # type: ignore[arg-type]
    )
    _emit(
        effective_deps,
        event="stage_request",
        period=period,
        mode=mode,
        stage="calc",
        detail="dry_run" if dry_run else "execute",
    )
    return _run_calc(effective_deps, period, mode, dry_run=dry_run)


def run_monthly_cdm(
    month: ExecutionMonthInput,
    *,
    mode: ExecutionMode = "incremental",
    deps: Optional[MonthlyPipelineDeps] = None,
    dry_run: bool = False,
    today: Optional[date] = None,
    allow_future: bool = False,
    cdm_period: Optional[CdmPeriodConfig] = None,
) -> StageRunResult:
    """Execute Monthly CDM stage only (Monthly CDM marts for the period window)."""
    if mode not in ("incremental", "rebuild"):
        raise PeriodValidationError(f"Unsupported execution mode: {mode!r}")
    period = validate_execution_month(month, today=today, allow_future=allow_future)
    if not dry_run and deps is None:
        raise MonthlyExecutionError("Monthly CDM execution requires pipeline dependencies")
    effective_deps = deps or MonthlyPipelineDeps(
        raw_engine=None,  # type: ignore[arg-type]
        calc_engine=None,  # type: ignore[arg-type]
        cdm_engine=None,  # type: ignore[arg-type]
    )
    period_cfg = cdm_period or default_period_config(period)
    if period_cfg.report_end != period:
        period_cfg = CdmPeriodConfig(
            report_start=min(period_cfg.report_start, period),
            report_end=period,
            trend_start=min(period_cfg.trend_start, period),
        )
    _emit(
        effective_deps,
        event="stage_request",
        period=period,
        mode=mode,
        stage="cdm",
        detail="dry_run" if dry_run else "execute",
    )
    return _run_cdm(
        effective_deps, period, mode, dry_run=dry_run, period=period_cfg
    )


def run_monthly_period(
    month: ExecutionMonthInput,
    *,
    mode: ExecutionMode = "incremental",
    deps: Optional[MonthlyPipelineDeps] = None,
    dry_run: bool = False,
    today: Optional[date] = None,
    allow_future: bool = False,
    cdm_period: Optional[CdmPeriodConfig] = None,
) -> MonthlyExecutionResult:
    """Run Monthly RAW → CALC → CDM for one explicit month.

    Composes ``run_monthly_raw`` / ``run_monthly_calc`` / ``run_monthly_cdm``.
    """
    if mode not in ("incremental", "rebuild"):
        raise PeriodValidationError(f"Unsupported execution mode: {mode!r}")

    period = validate_execution_month(month, today=today, allow_future=allow_future)

    if not dry_run and deps is None:
        raise MonthlyExecutionError("Monthly execution requires pipeline dependencies")

    effective_deps = deps or MonthlyPipelineDeps(
        raw_engine=None,  # type: ignore[arg-type]
        calc_engine=None,  # type: ignore[arg-type]
        cdm_engine=None,  # type: ignore[arg-type]
    )

    _emit(
        effective_deps,
        event="run_start",
        period=period,
        mode=mode,
        detail="dry_run" if dry_run else "execute",
    )

    raw_result = run_monthly_raw(
        period,
        mode=mode,
        deps=effective_deps,
        dry_run=dry_run,
        today=today,
        allow_future=allow_future,
    )
    if raw_result.status == "failed":
        failed_mart = next(
            (m.mart for m in raw_result.marts if m.status == "failed"), None
        )
        return MonthlyExecutionResult(
            period=period,
            mode=mode,
            dry_run=dry_run,
            raw_status="failed",
            calc_status="not_run",
            cdm_status="not_run",
            overall_status="failed",
            stages=(raw_result,),
            failed_stage="raw",
            failed_mart=failed_mart,
            error=raw_result.error,
        )

    calc_result = run_monthly_calc(
        period,
        mode=mode,
        deps=effective_deps,
        dry_run=dry_run,
        today=today,
        allow_future=allow_future,
    )
    if calc_result.status == "failed":
        failed_mart = next(
            (m.mart for m in calc_result.marts if m.status == "failed"), None
        )
        return MonthlyExecutionResult(
            period=period,
            mode=mode,
            dry_run=dry_run,
            raw_status=raw_result.status,
            calc_status="failed",
            cdm_status="not_run",
            overall_status="failed",
            stages=(raw_result, calc_result),
            failed_stage="calc",
            failed_mart=failed_mart,
            error=calc_result.error,
        )

    cdm_result = run_monthly_cdm(
        period,
        mode=mode,
        deps=effective_deps,
        dry_run=dry_run,
        today=today,
        allow_future=allow_future,
        cdm_period=cdm_period,
    )
    if cdm_result.status == "failed":
        failed_mart = next(
            (m.mart for m in cdm_result.marts if m.status == "failed"), None
        )
        return MonthlyExecutionResult(
            period=period,
            mode=mode,
            dry_run=dry_run,
            raw_status=raw_result.status,
            calc_status=calc_result.status,
            cdm_status="failed",
            overall_status="failed",
            stages=(raw_result, calc_result, cdm_result),
            failed_stage="cdm",
            failed_mart=failed_mart,
            error=cdm_result.error,
        )

    if dry_run:
        overall_status = "planned"
    else:
        overall_status = "success"

    return MonthlyExecutionResult(
        period=period,
        mode=mode,
        dry_run=dry_run,
        raw_status=raw_result.status,
        calc_status=calc_result.status,
        cdm_status=cdm_result.status,
        overall_status=overall_status,  # type: ignore[arg-type]
        stages=(raw_result, calc_result, cdm_result),
    )


__all__ = [
    "ADOBE_RAW_MARTS",
    "CALC_MARTS",
    "CDM_MARTS",
    "MonthlyExecutionError",
    "MonthlyPipelineDeps",
    "RAW_MARTS",
    "SupportsPostReport",
    "WORDSTAT_RAW_MART",
    "raw_marts_for_month",
    "run_monthly_calc",
    "run_monthly_cdm",
    "run_monthly_period",
    "run_monthly_raw",
]
