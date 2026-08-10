"""Typed results for Monthly pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Callable, Literal, Optional, Sequence

ExecutionMode = Literal["incremental", "rebuild", "validate"]
StageName = Literal["raw", "wordstat_raw", "calc", "cdm"]
StageStatus = Literal["success", "failed", "skipped", "planned", "not_run"]
OverallStatus = Literal["success", "failed", "planned"]
MartRunStatus = Literal["success", "failed", "skipped", "planned"]


@dataclass
class MonthlyExecutionEvent:
    """Optional structured logging hook payload (no secrets)."""

    event: str
    period: date
    mode: ExecutionMode
    stage: Optional[StageName] = None
    mart: Optional[str] = None
    detail: Optional[str] = None
    extras: dict[str, object] = field(default_factory=dict)


EventHook = Callable[[MonthlyExecutionEvent], None]


@dataclass(frozen=True)
class MartRunResult:
    """Per-mart outcome (no row payloads / secrets)."""

    mart: str
    status: MartRunStatus
    detail: Optional[str] = None


@dataclass(frozen=True)
class StageRunResult:
    """Per-layer outcome."""

    stage: StageName
    status: StageStatus
    marts: tuple[MartRunResult, ...] = ()
    error: Optional[str] = None


@dataclass(frozen=True)
class MonthlyExecutionResult:
    """Structured Monthly pipeline execution summary."""

    period: date
    mode: ExecutionMode
    dry_run: bool
    raw_status: StageStatus
    calc_status: StageStatus
    cdm_status: StageStatus
    overall_status: OverallStatus
    stages: tuple[StageRunResult, ...] = ()
    failed_stage: Optional[StageName] = None
    failed_mart: Optional[str] = None
    error: Optional[str] = None
    wordstat_status: StageStatus = "not_run"

    def summary_lines(self) -> list[str]:
        """Human-readable lines safe for CLI (no secrets / KPI values)."""
        lines = [
            f"period={self.period.isoformat()}",
            f"mode={self.mode}",
            f"dry_run={self.dry_run}",
            f"raw={self.raw_status}",
            f"wordstat_raw={self.wordstat_status}",
            f"calc={self.calc_status}",
            f"cdm={self.cdm_status}",
            f"overall={self.overall_status}",
        ]
        if self.failed_stage:
            lines.append(f"failed_stage={self.failed_stage}")
        if self.failed_mart:
            lines.append(f"failed_mart={self.failed_mart}")
        if self.error:
            lines.append(f"error={self.error}")
        return lines


class MonthlyExecutionError(RuntimeError):
    """Raised for orchestration configuration / control-flow failures."""


@dataclass(frozen=True)
class MonthlyPipelineDeps:
    """Injected runtime dependencies for Monthly execution.

    Adobe client + client config are required only when the RAW Adobe stage will
    actually extract (rebuild, or incremental with missing Adobe RAW month).
    Wordstat client is required for reporting months (>= 2026-01) when the
    Wordstat RAW mart is missing or being rebuilt.

    Engine types are intentionally untyped here to keep this module free of
    SQLAlchemy import coupling for lightweight Airflow resolve-period imports.
    """

    raw_engine: object
    calc_engine: object
    cdm_engine: object
    adobe_reports: object = None
    client_config: object = None
    wordstat_client: object = None
    on_event: Optional[EventHook] = None


__all__ = [
    "EventHook",
    "ExecutionMode",
    "MartRunResult",
    "MartRunStatus",
    "MonthlyExecutionError",
    "MonthlyExecutionEvent",
    "MonthlyExecutionResult",
    "MonthlyPipelineDeps",
    "OverallStatus",
    "StageName",
    "StageRunResult",
    "StageStatus",
]
