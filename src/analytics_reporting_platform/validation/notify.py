"""Minimal notification extension point for Monthly DWH validation.

Default is NoOp. Real Telegram/email/Slack notifiers are intentionally absent.
Notifier failures must never change DQ PASS/FAIL outcomes.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Protocol

from analytics_reporting_platform.validation.types import SuiteResult

logger = logging.getLogger(__name__)


class ValidationNotifier(Protocol):
    """Future notification backends implement this protocol."""

    def notify(
        self,
        result: SuiteResult,
        *,
        artifact_dir: Path,
        md_path: Optional[Path] = None,
        json_path: Optional[Path] = None,
    ) -> None:
        """Deliver a sanitized validation summary (no secrets / KPI dumps)."""


class NoOpNotifier:
    """Default notifier: does nothing and never performs network I/O."""

    def notify(
        self,
        result: SuiteResult,
        *,
        artifact_dir: Path,
        md_path: Optional[Path] = None,
        json_path: Optional[Path] = None,
    ) -> None:
        return None


def safe_notify(
    notifier: ValidationNotifier,
    result: SuiteResult,
    *,
    artifact_dir: Path,
    md_path: Optional[Path] = None,
    json_path: Optional[Path] = None,
) -> None:
    """Invoke notifier; swallow errors so DQ result is unaffected."""
    try:
        notifier.notify(
            result,
            artifact_dir=artifact_dir,
            md_path=md_path,
            json_path=json_path,
        )
    except Exception as exc:  # noqa: BLE001 - notifier must not fail the run
        logger.warning(
            "validation_notifier_failed error=%s detail=%s",
            type(exc).__name__,
            str(exc)[:200],
        )


__all__ = [
    "NoOpNotifier",
    "ValidationNotifier",
    "safe_notify",
]
