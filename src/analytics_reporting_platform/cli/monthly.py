"""CLI for Analytics Reporting Platform Monthly execution / validation."""

from __future__ import annotations

import argparse
import sys
from typing import Optional, Sequence

from analytics_reporting_platform.execution.models import (
    MonthlyExecutionError,
    MonthlyExecutionResult,
)
from analytics_reporting_platform.execution.monthly import (
    MonthlyPipelineDeps,
    run_monthly_period,
)
from analytics_reporting_platform.execution.period import PeriodValidationError


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="analytics-reporting",
        description="Analytics Reporting Platform CLI",
    )
    sub = parser.add_subparsers(dest="command_group", required=True)

    monthly = sub.add_parser("monthly", help="Monthly RAW → CALC → CDM pipeline")
    monthly_sub = monthly.add_subparsers(dest="monthly_command", required=True)

    def add_month_args(p: argparse.ArgumentParser, *, require_env_for_live: bool = True) -> None:
        p.add_argument(
            "--month",
            required=True,
            help="Target month as YYYY-MM or YYYY-MM-01",
        )
        p.add_argument(
            "--env-file",
            default=None,
            help="Optional path to environment file (required for non-dry-run / validate / snapshot)",
        )
        if require_env_for_live:
            p.add_argument(
                "--client-config",
                default=None,
                help="Optional path to Adobe client YAML (required when RAW extract runs)",
            )

    run_p = monthly_sub.add_parser(
        "run",
        help="Incremental Monthly run (skip existing period data)",
    )
    add_month_args(run_p)
    run_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan stages without DB writes or Adobe calls",
    )

    rebuild_p = monthly_sub.add_parser(
        "rebuild",
        help="Rebuild Monthly period (replace target period)",
    )
    add_month_args(rebuild_p)
    rebuild_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan stages without DB writes or Adobe calls",
    )

    validate_p = monthly_sub.add_parser(
        "validate",
        help="Read-only post-run Monthly validation (no Adobe / no writes)",
    )
    add_month_args(validate_p, require_env_for_live=False)
    validate_p.add_argument(
        "--before",
        default=None,
        help="Optional before-run signature JSON for scope comparison",
    )
    validate_p.add_argument(
        "--after",
        default=None,
        help="Optional after-run signature JSON (default: capture live read-only)",
    )

    snapshot_p = monthly_sub.add_parser(
        "snapshot",
        help="Capture read-only Monthly signatures for before/after comparison",
    )
    add_month_args(snapshot_p, require_env_for_live=False)
    snapshot_p.add_argument(
        "--output",
        required=True,
        help="Path to write signature JSON",
    )

    suite_p = monthly_sub.add_parser(
        "validate-suite",
        help=(
            "Read-only Monthly DWH validation suite "
            "(coverage/contracts/reconciliation/report/reference; no Adobe / no writes)"
        ),
    )
    suite_p.add_argument(
        "--env-file",
        required=True,
        help="Path to execution environment file (sandbox / working DWH)",
    )
    suite_p.add_argument(
        "--reference-env-file",
        default=None,
        help="Optional frozen/reference env file (read-only comparison)",
    )
    suite_p.add_argument(
        "--report-end",
        default=None,
        help=(
            "Last completed reporting month YYYY-MM "
            "(default: last fully completed calendar month)"
        ),
    )
    suite_p.add_argument(
        "--output-dir",
        default=None,
        help="Directory for JSON/Markdown reports (default: execution-work/validation/...)",
    )

    plan_p = monthly_sub.add_parser(
        "backfill-plan",
        help="Print deterministic backfill month plan (no Adobe / no DB writes)",
    )
    plan_p.add_argument(
        "--reporting-end",
        required=True,
        help="Last reporting month YYYY-MM (CALC/CDM inclusive end)",
    )
    plan_p.add_argument(
        "--reporting-start",
        default="2026-01",
        help="First reporting month YYYY-MM (default: 2026-01)",
    )
    plan_p.add_argument(
        "--raw-history-start",
        default="2025-01",
        help="First RAW history month YYYY-MM (default: 2025-01)",
    )
    return parser


def _print_result(result: MonthlyExecutionResult) -> None:
    for line in result.summary_lines():
        print(line)
    if result.dry_run:
        for stage in result.stages:
            marts = ",".join(m.mart for m in stage.marts) or "-"
            print(f"plan_stage={stage.stage} marts={marts}")


def _build_deps(env_file: Optional[str], client_config: Optional[str]) -> MonthlyPipelineDeps:
    from analytics_reporting_platform.orchestration.monthly import (
        build_monthly_pipeline_deps,
    )

    if not env_file:
        raise MonthlyExecutionError("--env-file is required unless --dry-run is set")
    return build_monthly_pipeline_deps(env_file=env_file, client_config=client_config)


def _build_engines(env_file: Optional[str]):
    from analytics_reporting_platform.config.settings import load_settings
    from analytics_reporting_platform.db.engine import create_db_engine

    if not env_file:
        raise MonthlyExecutionError("--env-file is required for validate/snapshot")
    settings = load_settings(env_file)
    return (
        create_db_engine(settings, "raw"),
        create_db_engine(settings, "calc"),
        create_db_engine(settings, "cdm"),
    )


def _execute_validate_suite(args: argparse.Namespace) -> int:
    from datetime import datetime, timezone
    from pathlib import Path

    from analytics_reporting_platform.config.settings import load_settings
    from analytics_reporting_platform.db.engine import create_db_engine
    from analytics_reporting_platform.execution.period import (
        last_completed_month,
        validate_execution_month,
    )
    from analytics_reporting_platform.orchestration.validation import (
        validation_windows_from_report_end,
    )
    from analytics_reporting_platform.validation.report import (
        VALIDATION_JSON_NAME,
        VALIDATION_MD_NAME,
    )
    from analytics_reporting_platform.validation.suite import run_monthly_dwh_validation_suite

    if args.report_end:
        report_end = validate_execution_month(args.report_end, allow_future=True)
    else:
        report_end = last_completed_month()
    windows = validation_windows_from_report_end(report_end)

    settings = load_settings(args.env_file)
    raw_engine = create_db_engine(settings, "raw")
    calc_engine = create_db_engine(settings, "calc")
    cdm_engine = create_db_engine(settings, "cdm")

    reference_engines = None
    if args.reference_env_file:
        ref_settings = load_settings(args.reference_env_file)
        # Frozen baseline is port 15432 — read-only comparison only.
        reference_engines = {
            "raw": create_db_engine(ref_settings, "raw"),
            "calc": create_db_engine(ref_settings, "calc"),
            "cdm": create_db_engine(ref_settings, "cdm"),
        }

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = Path("execution-work") / "validation" / f"monthly_dwh_{stamp}"

    result = run_monthly_dwh_validation_suite(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        cdm_engine=cdm_engine,
        report_end=windows["report_end"],
        raw_start=windows["raw_start"],
        raw_end=windows["raw_end"],
        calc_start=windows["calc_start"],
        calc_end=windows["calc_end"],
        cdm_start=windows["cdm_start"],
        cdm_end=windows["cdm_end"],
        trend_start=windows["trend_start"],
        reference_engines=reference_engines,
        output_dir=output_dir,
        print_summary=True,
    )
    print(f"json_report={output_dir / VALIDATION_JSON_NAME}")
    print(f"md_report={output_dir / VALIDATION_MD_NAME}")
    return 0 if result.final_status == "PASS" else 1


def _execute_validate(args: argparse.Namespace) -> int:
    from analytics_reporting_platform.validation.monthly import validate_monthly_period
    from analytics_reporting_platform.validation.signatures import load_signature_bundle

    raw_engine, calc_engine, cdm_engine = _build_engines(args.env_file)
    before = load_signature_bundle(args.before) if args.before else None
    after = load_signature_bundle(args.after) if args.after else None
    result = validate_monthly_period(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        cdm_engine=cdm_engine,
        month=args.month,
        before=before,
        after=after,
    )
    for line in result.summary_lines():
        print(line)
    return 0 if result.overall == "PASS" else 1


def _execute_snapshot(args: argparse.Namespace) -> int:
    from analytics_reporting_platform.validation.signatures import (
        capture_monthly_signatures,
        save_signature_bundle,
    )

    raw_engine, calc_engine, cdm_engine = _build_engines(args.env_file)
    bundle = capture_monthly_signatures(
        raw_engine=raw_engine,
        calc_engine=calc_engine,
        cdm_engine=cdm_engine,
        month=args.month,
    )
    path = save_signature_bundle(bundle, args.output)
    print(f"snapshot_written={path}")
    print(f"target_month={bundle.target_month}")
    print(f"tables={len(bundle.tables)}")
    return 0


def _execute_backfill_plan(args: argparse.Namespace) -> int:
    from analytics_reporting_platform.execution.backfill import (
        plan_monthly_backfill,
        reporting_mom_month,
        reporting_yoy_month,
    )

    plan = plan_monthly_backfill(
        reporting_end=args.reporting_end,
        reporting_start=args.reporting_start,
        raw_history_start=args.raw_history_start,
    )
    raw_steps = [p for p in plan if p.phase == "raw_history"]
    full_steps = [p for p in plan if p.phase == "reporting_pipeline"]
    print(f"reporting_boundary={args.reporting_start}")
    print(f"raw_history={args.raw_history_start}..{raw_steps[-1].month if raw_steps else '-'}")
    print(f"reporting_pipeline={args.reporting_start}..{args.reporting_end}")
    print(f"steps_total={len(plan)}")
    print(f"steps_raw_history={len(raw_steps)}")
    print(f"steps_reporting={len(full_steps)}")
    first = args.reporting_start
    print(
        f"lookback_check month={first} "
        f"mom={reporting_mom_month(first).strftime('%Y-%m')} "
        f"yoy={reporting_yoy_month(first).strftime('%Y-%m')}"
    )
    for item in plan:
        print(f"step phase={item.phase} month={item.month}")
    return 0


def execute_monthly_cli(argv: Optional[Sequence[str]] = None) -> int:
    """Run Monthly CLI. Returns process exit code (0 success / non-zero failure)."""
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command_group != "monthly":
        parser.error("unsupported command group")

    try:
        if args.monthly_command == "validate":
            return _execute_validate(args)
        if args.monthly_command == "validate-suite":
            return _execute_validate_suite(args)
        if args.monthly_command == "snapshot":
            return _execute_snapshot(args)
        if args.monthly_command == "backfill-plan":
            return _execute_backfill_plan(args)

        mode = "incremental" if args.monthly_command == "run" else "rebuild"
        dry_run = bool(args.dry_run)
        deps = None if dry_run else _build_deps(args.env_file, args.client_config)
        result = run_monthly_period(
            args.month,
            mode=mode,  # type: ignore[arg-type]
            deps=deps,
            dry_run=dry_run,
        )
    except (PeriodValidationError, MonthlyExecutionError) as exc:
        print(f"error={exc}")
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        print(f"error={type(exc).__name__}: {exc}")
        return 1

    _print_result(result)
    if result.overall_status in ("success", "planned"):
        return 0
    return 1


def main(argv: Optional[Sequence[str]] = None) -> None:
    raise SystemExit(execute_monthly_cli(argv))


__all__ = ["execute_monthly_cli", "main"]
