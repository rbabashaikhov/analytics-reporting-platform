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
from analytics_reporting_platform.cdm.models import (
    CdmPeriodConfig,
    DEFAULT_REPORT_START,
    DEFAULT_TREND_START,
)
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
    extract_wordstat_brand_queries_range,
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
WORDSTAT_RAW_START: date = DEFAULT_TREND_START  # 2025-01-01
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
    """All RAW marts required for completeness of ``month`` (Adobe + Wordstat)."""
    if month >= WORDSTAT_RAW_START:
        return RAW_MARTS
    return ADOBE_RAW_MARTS


def adobe_raw_month_complete(raw_engine: Engine, month: date) -> bool:
    return all(month_exists(raw_engine, mart, month) for mart in ADOBE_RAW_MARTS)


def wordstat_raw_month_complete(raw_engine: Engine, month: date) -> bool:
    if month < WORDSTAT_RAW_START:
        return True
    return month_exists(raw_engine, WORDSTAT_RAW_MART, month)


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


def _run_raw(
    deps: MonthlyPipelineDeps,
    month: date,
    mode: ExecutionMode,
    *,
    dry_run: bool,
) -> StageRunResult:
    """Adobe Analytics → RAW only (Wordstat is a separate stage)."""
    planned = tuple(MartRunResult(mart=m, status="planned") for m in ADOBE_RAW_MARTS)
    if dry_run:
        return StageRunResult(stage="raw", status="planned", marts=planned)

    if mode == "incremental" and adobe_raw_month_complete(deps.raw_engine, month):
        skipped = tuple(
            MartRunResult(mart=m, status="skipped", detail="month already present")
            for m in ADOBE_RAW_MARTS
        )
        return StageRunResult(stage="raw", status="skipped", marts=skipped)

    if deps.adobe_reports is None or deps.client_config is None:
        raise MonthlyExecutionError(
            "RAW extraction requires adobe_reports and client_config dependencies"
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
    except Exception as exc:
        failed_mart = (
            ADOBE_RAW_MARTS[len(mart_results)]
            if len(mart_results) < len(ADOBE_RAW_MARTS)
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


def _run_wordstat_raw(
    deps: MonthlyPipelineDeps,
    month: date,
    mode: ExecutionMode,
    *,
    dry_run: bool,
) -> StageRunResult:
    """Yandex Wordstat → RAW brand queries for one month."""
    planned = (MartRunResult(mart=WORDSTAT_RAW_MART, status="planned"),)
    if month < WORDSTAT_RAW_START:
        return StageRunResult(
            stage="wordstat_raw",
            status="skipped",
            marts=(
                MartRunResult(
                    mart=WORDSTAT_RAW_MART,
                    status="skipped",
                    detail="before Wordstat RAW history start",
                ),
            ),
        )
    if dry_run:
        return StageRunResult(stage="wordstat_raw", status="planned", marts=planned)

    if mode == "incremental" and wordstat_raw_month_complete(deps.raw_engine, month):
        return StageRunResult(
            stage="wordstat_raw",
            status="skipped",
            marts=(
                MartRunResult(
                    mart=WORDSTAT_RAW_MART,
                    status="skipped",
                    detail="month already present",
                ),
            ),
        )

    if deps.wordstat_client is None:
        raise MonthlyExecutionError(
            "Wordstat RAW extraction requires wordstat_client dependency"
        )

    persist_mode = _persist_mode(mode)
    try:
        _emit(
            deps,
            event="stage_start",
            period=month,
            mode=mode,
            stage="wordstat_raw",
        )
        extracted = extract_wordstat_brand_queries(deps.wordstat_client, month)
        result = persist_month(
            deps.raw_engine,
            WORDSTAT_RAW_MART,
            extracted.dataframe,
            month,
            mode=persist_mode,  # type: ignore[arg-type]
        )
        return StageRunResult(
            stage="wordstat_raw",
            status="success",
            marts=(
                MartRunResult(
                    mart=WORDSTAT_RAW_MART,
                    status="success",
                    detail=result.status,
                ),
            ),
        )
    except Exception as exc:
        return StageRunResult(
            stage="wordstat_raw",
            status="failed",
            marts=(
                MartRunResult(
                    mart=WORDSTAT_RAW_MART,
                    status="failed",
                    detail=type(exc).__name__,
                ),
            ),
            error=f"{type(exc).__name__}: {exc}",
        )


def run_wordstat_raw_periods(
    periods: Sequence[date],
    *,
    mode: ExecutionMode = "incremental",
    deps: Optional[MonthlyPipelineDeps] = None,
    dry_run: bool = False,
) -> StageRunResult:
    """Persist Wordstat RAW for many months with one range API extract when possible.

    Uses two dynamics requests total (one per required query) spanning
    ``min(periods)`` … ``max(periods)``.
    """
    if mode not in ("incremental", "rebuild"):
        raise PeriodValidationError(f"Unsupported execution mode: {mode!r}")
    if not dry_run and deps is None:
        raise MonthlyExecutionError(
            "Wordstat RAW range execution requires pipeline dependencies"
        )
    effective_deps = deps or MonthlyPipelineDeps(
        raw_engine=None,  # type: ignore[arg-type]
        calc_engine=None,  # type: ignore[arg-type]
        cdm_engine=None,  # type: ignore[arg-type]
    )

    months = sorted({m for m in periods if m >= WORDSTAT_RAW_START})
    if not months:
        return StageRunResult(
            stage="wordstat_raw",
            status="skipped",
            marts=(
                MartRunResult(
                    mart=WORDSTAT_RAW_MART,
                    status="skipped",
                    detail="no Wordstat periods",
                ),
            ),
        )
    if dry_run:
        return StageRunResult(
            stage="wordstat_raw",
            status="planned",
            marts=tuple(
                MartRunResult(mart=f"{WORDSTAT_RAW_MART}:{m.isoformat()}", status="planned")
                for m in months
            ),
        )

    if mode == "incremental":
        months = [
            m
            for m in months
            if not wordstat_raw_month_complete(effective_deps.raw_engine, m)
        ]
        if not months:
            return StageRunResult(
                stage="wordstat_raw",
                status="skipped",
                marts=(
                    MartRunResult(
                        mart=WORDSTAT_RAW_MART,
                        status="skipped",
                        detail="all months already present",
                    ),
                ),
            )

    if effective_deps.wordstat_client is None:
        raise MonthlyExecutionError(
            "Wordstat RAW extraction requires wordstat_client dependency"
        )

    persist_mode = _persist_mode(mode)
    try:
        frame = extract_wordstat_brand_queries_range(
            effective_deps.wordstat_client,
            months[0],
            months[-1],
        )
        mart_results: list[MartRunResult] = []
        for month in months:
            month_frame = frame.loc[frame["month"] == month].copy().reset_index(drop=True)
            if month_frame.empty:
                raise MonthlyExecutionError(
                    f"Wordstat range extract missing month={month.isoformat()}"
                )
            result = persist_month(
                effective_deps.raw_engine,
                WORDSTAT_RAW_MART,
                month_frame,
                month,
                mode=persist_mode,  # type: ignore[arg-type]
            )
            mart_results.append(
                MartRunResult(
                    mart=f"{WORDSTAT_RAW_MART}:{month.strftime('%Y-%m')}",
                    status="success",
                    detail=result.status,
                )
            )
        return StageRunResult(
            stage="wordstat_raw",
            status="success",
            marts=tuple(mart_results),
        )
    except Exception as exc:
        return StageRunResult(
            stage="wordstat_raw",
            status="failed",
            marts=(
                MartRunResult(
                    mart=WORDSTAT_RAW_MART,
                    status="failed",
                    detail=type(exc).__name__,
                ),
            ),
            error=f"{type(exc).__name__}: {exc}",
        )


def _run_calc(
    deps: MonthlyPipelineDeps,
    month: date,
    mode: ExecutionMode,
    *,
    dry_run: bool,
) -> StageRunResult:
    brand_only = month < DEFAULT_REPORT_START
    mart_names: tuple[str, ...] = (
        ("brand_demand",) if brand_only else CALC_MARTS
    )
    planned = tuple(MartRunResult(mart=m, status="planned") for m in mart_names)
    if dry_run:
        return StageRunResult(stage="calc", status="planned", marts=planned)

    persist_mode = _persist_mode(mode)
    mart_results: list[MartRunResult] = []
    adobe_builders: Sequence[tuple[str, Callable[[], object], Callable[..., object]]] = (
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
    )
    brand_builder: tuple[str, Callable[[], object], Callable[..., object]] = (
        "brand_demand",
        lambda: build_brand_demand(deps.raw_engine, month),
        lambda df: persist_brand_demand(
            deps.calc_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
        ),
    )
    builders: Sequence[tuple[str, Callable[[], object], Callable[..., object]]] = (
        (brand_builder,) if brand_only else (*adobe_builders, brand_builder)
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
            mart_names[len(mart_results)]
            if len(mart_results) < len(mart_names)
            else "calc"
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
    brand_only = month < DEFAULT_REPORT_START
    mart_names: tuple[str, ...] = (
        ("organic_brand_demand",) if brand_only else CDM_MARTS
    )
    planned = tuple(MartRunResult(mart=m, status="planned") for m in mart_names)
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
        if brand_only:
            _month_step(
                "organic_brand_demand",
                lambda: build_organic_brand_demand(deps.calc_engine, month),
                lambda df: persist_organic_brand_demand(
                    deps.cdm_engine, df, month, mode=persist_mode  # type: ignore[arg-type]
                ),
            )
            if all(m.status == "skipped" for m in mart_results):
                return StageRunResult(
                    stage="cdm", status="skipped", marts=tuple(mart_results)
                )
            return StageRunResult(stage="cdm", status="success", marts=tuple(mart_results))

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
            mart_names[len(mart_results)]
            if len(mart_results) < len(mart_names)
            else "cdm"
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
    """Execute Monthly Adobe RAW stage only (Wordstat is separate)."""
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


def run_monthly_wordstat_raw(
    month: ExecutionMonthInput,
    *,
    mode: ExecutionMode = "incremental",
    deps: Optional[MonthlyPipelineDeps] = None,
    dry_run: bool = False,
    today: Optional[date] = None,
    allow_future: bool = False,
) -> StageRunResult:
    """Execute Monthly Wordstat RAW stage only."""
    if mode not in ("incremental", "rebuild"):
        raise PeriodValidationError(f"Unsupported execution mode: {mode!r}")
    period = validate_execution_month(month, today=today, allow_future=allow_future)
    if not dry_run and deps is None:
        raise MonthlyExecutionError(
            "Monthly Wordstat RAW execution requires pipeline dependencies"
        )
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
        stage="wordstat_raw",
        detail="dry_run" if dry_run else "execute",
    )
    return _run_wordstat_raw(effective_deps, period, mode, dry_run=dry_run)


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
            wordstat_status="not_run",
            calc_status="not_run",
            cdm_status="not_run",
            overall_status="failed",
            stages=(raw_result,),
            failed_stage="raw",
            failed_mart=failed_mart,
            error=raw_result.error,
        )

    wordstat_result = run_monthly_wordstat_raw(
        period,
        mode=mode,
        deps=effective_deps,
        dry_run=dry_run,
        today=today,
        allow_future=allow_future,
    )
    if wordstat_result.status == "failed":
        failed_mart = next(
            (m.mart for m in wordstat_result.marts if m.status == "failed"), None
        )
        return MonthlyExecutionResult(
            period=period,
            mode=mode,
            dry_run=dry_run,
            raw_status=raw_result.status,
            wordstat_status="failed",
            calc_status="not_run",
            cdm_status="not_run",
            overall_status="failed",
            stages=(raw_result, wordstat_result),
            failed_stage="wordstat_raw",
            failed_mart=failed_mart,
            error=wordstat_result.error,
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
            wordstat_status=wordstat_result.status,
            calc_status="failed",
            cdm_status="not_run",
            overall_status="failed",
            stages=(raw_result, wordstat_result, calc_result),
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
            wordstat_status=wordstat_result.status,
            calc_status=calc_result.status,
            cdm_status="failed",
            overall_status="failed",
            stages=(raw_result, wordstat_result, calc_result, cdm_result),
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
        wordstat_status=wordstat_result.status,
        calc_status=calc_result.status,
        cdm_status=cdm_result.status,
        overall_status=overall_status,  # type: ignore[arg-type]
        stages=(raw_result, wordstat_result, calc_result, cdm_result),
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
    "WORDSTAT_RAW_START",
    "adobe_raw_month_complete",
    "raw_marts_for_month",
    "run_monthly_calc",
    "run_monthly_cdm",
    "run_monthly_period",
    "run_monthly_raw",
    "run_monthly_wordstat_raw",
    "run_wordstat_raw_periods",
    "wordstat_raw_month_complete",
]
