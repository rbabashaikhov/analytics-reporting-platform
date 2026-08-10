"""CLI entrypoint for ``analytics-reporting``."""

from __future__ import annotations

import sys
from typing import Optional, Sequence

from analytics_reporting_platform.cli.monthly import execute_monthly_cli, main as monthly_main


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Dispatch CLI between Monthly and Wordstat command groups."""
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] == "wordstat":
        from analytics_reporting_platform.cli.wordstat import execute_wordstat_cli

        raise SystemExit(execute_wordstat_cli(args[1:]))
    # Preserve monthly argparse behavior for process argv when caller passed None.
    raise SystemExit(execute_monthly_cli(None if argv is None else args))


__all__ = ["execute_monthly_cli", "main", "monthly_main"]
