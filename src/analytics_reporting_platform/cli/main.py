"""CLI entrypoint for ``analytics-reporting``."""

from __future__ import annotations

from typing import Optional, Sequence

from analytics_reporting_platform.cli.monthly import execute_monthly_cli, main as monthly_main


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Dispatch CLI. Currently Monthly commands only."""
    raise SystemExit(execute_monthly_cli(argv))


__all__ = ["execute_monthly_cli", "main", "monthly_main"]
