"""CLI for Yandex Wordstat Brand Demand smoke / dynamics fetch."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from typing import Optional, Sequence

from analytics_reporting_platform.config.settings import SettingsError
from analytics_reporting_platform.sources.wordstat import (
    WordstatApiError,
    WordstatClient,
    WordstatConfigurationError,
    load_wordstat_credentials,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="analytics-reporting wordstat",
        description="Yandex Wordstat source smoke helpers",
    )
    sub = parser.add_subparsers(dest="wordstat_command", required=True)

    dynamics = sub.add_parser(
        "dynamics",
        help="Fetch Wordstat monthly/weekly/daily dynamics for one phrase",
    )
    dynamics.add_argument("--phrase", required=True, help="Search phrase")
    dynamics.add_argument(
        "--from-date",
        required=True,
        help="Range start date YYYY-MM-DD",
    )
    dynamics.add_argument(
        "--to-date",
        required=True,
        help="Range end date YYYY-MM-DD (for MONTHLY: last day of month)",
    )
    dynamics.add_argument(
        "--period",
        default="MONTHLY",
        help="Period alias: MONTHLY (default), WEEKLY, or DAILY",
    )
    dynamics.add_argument(
        "--env-file",
        required=True,
        help="Private env file providing YANDEX_WORDSTAT_* credentials",
    )
    dynamics.add_argument(
        "--region",
        action="append",
        type=int,
        default=None,
        help="Optional region id (repeatable)",
    )
    return parser


def _parse_date(raw: str, *, field: str) -> date:
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise WordstatConfigurationError(
            f"Invalid {field}: expected YYYY-MM-DD"
        ) from exc


def _execute_dynamics(args: argparse.Namespace) -> int:
    credentials = load_wordstat_credentials(args.env_file)
    client = WordstatClient(credentials)
    result = client.get_dynamics(
        phrase=args.phrase,
        from_date=_parse_date(args.from_date, field="from-date"),
        to_date=_parse_date(args.to_date, field="to-date"),
        period=args.period,
        regions=args.region,
    )

    print(f"phrase={result.phrase}")
    print(f"period={result.period}")
    print(f"from_date={result.from_date.isoformat()}")
    print(f"to_date={result.to_date.isoformat()}")
    print(f"points={len(result.points)}")
    print("month\tphrase\tcount\tshare")
    for point in result.points:
        share = "" if point.share is None else f"{point.share}"
        print(
            f"{point.date.strftime('%Y-%m')}\t{result.phrase}\t{point.count}\t{share}"
        )
    return 0


def execute_wordstat_cli(argv: Optional[Sequence[str]] = None) -> int:
    """Run Wordstat CLI. Returns process exit code."""
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        if args.wordstat_command == "dynamics":
            return _execute_dynamics(args)
        parser.error("unsupported wordstat command")
    except (WordstatConfigurationError, WordstatApiError, SettingsError) as exc:
        print(f"error={exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        print(f"error={type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 1


def main(argv: Optional[Sequence[str]] = None) -> None:
    raise SystemExit(execute_wordstat_cli(argv))


__all__ = ["execute_wordstat_cli", "main"]
