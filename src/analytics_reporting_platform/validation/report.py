"""Human-readable Markdown + machine-readable JSON report writers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Union

from analytics_reporting_platform.validation.types import SuiteResult

CATEGORY_TITLES = {
    "coverage": "Coverage",
    "contracts": "Contracts",
    "uniqueness": "Uniqueness",
    "value_ranges": "Value ranges",
    "raw_calc_reconciliation": "RAW → CALC",
    "calc_cdm_reconciliation": "CALC → CDM",
    "report_contract": "Report contract",
    "reference_comparison": "Reference comparison",
    "monthly_only": "Monthly only",
}

# Categories shown in the compact Airflow log summary (order matters).
AIRFLOW_SUMMARY_CATEGORIES = (
    "coverage",
    "contracts",
    "uniqueness",
    "value_ranges",
    "raw_calc_reconciliation",
    "calc_cdm_reconciliation",
    "report_contract",
    "monthly_only",
)

VALIDATION_MD_NAME = "validation.md"
VALIDATION_JSON_NAME = "validation.json"


def write_suite_reports(result: SuiteResult, output_dir: Union[str, Path]) -> tuple[Path, Path]:
    """Write ``validation.json`` and ``validation.md`` under output_dir."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / VALIDATION_JSON_NAME
    md_path = out / VALIDATION_MD_NAME

    payload = result.to_dict()
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(result), encoding="utf-8")
    return json_path, md_path


def render_markdown(result: SuiteResult) -> str:
    lines = [
        "# MONTHLY DWH VALIDATION",
        "",
        f"Generated: `{result.generated_at}`",
        f"Report end: `{result.report_end}`",
        f"Read-only: `{result.read_only}`",
        f"Execution port: `{result.execution_port}`",
        f"Reference port: `{result.reference_port}`",
        "",
    ]
    for cat in result.categories:
        title = CATEGORY_TITLES.get(cat.name, cat.name)
        lines.append(f"## {title}")
        lines.append(f"**{cat.status}** — {cat.passed}/{cat.total}")
        lines.append("")

    lines.append("## FINAL STATUS")
    lines.append(f"**{result.final_status}**")
    lines.append("")

    failed = result.failed_critical()
    if failed:
        lines.append("## Critical failures")
        for c in failed:
            detail = f" — {c.detail}" if c.detail else ""
            lines.append(f"- `{c.name}`{detail}")
        lines.append("")

    warnings = result.warnings()
    if warnings:
        lines.append("## Warnings")
        for c in warnings:
            detail = f" — {c.detail}" if c.detail else ""
            lines.append(f"- `{c.name}`{detail}")
        lines.append("")

    return "\n".join(lines) + "\n"


def format_airflow_validation_summary(
    result: SuiteResult,
    *,
    md_path: Path | None = None,
    json_path: Path | None = None,
) -> str:
    """Compact sanitized summary for Airflow task logs."""
    by_name = {c.name: c for c in result.categories}
    lines = [
        "================================================",
        "MONTHLY DWH VALIDATION",
        "================================================",
        f"Report period: {result.report_end}",
        "",
    ]
    for name in AIRFLOW_SUMMARY_CATEGORIES:
        cat = by_name.get(name)
        title = CATEGORY_TITLES.get(name, name)
        if cat is None:
            lines.append(f"{title:<25} SKIP")
            continue
        lines.append(f"{title:<25} {cat.status:<6} {cat.passed}/{cat.total}")

    failed = result.failed_critical()
    warnings = result.warnings()
    lines.append("")
    lines.append(f"Critical failures: {len(failed)}")
    lines.append(f"Warnings: {len(warnings)}")
    lines.append("")
    lines.append(f"FINAL STATUS: {result.final_status}")
    lines.append("================================================")

    if failed:
        lines.append("")
        lines.append("Failed checks:")
        for c in failed:
            detail = f" — {c.detail}" if c.detail else ""
            lines.append(f"  - {c.name}{detail}")

    if md_path is not None or json_path is not None:
        lines.append("")
        if md_path is not None:
            lines.append("Validation report:")
            lines.append(str(md_path))
            lines.append("")
        if json_path is not None:
            lines.append("Machine-readable result:")
            lines.append(str(json_path))

    return "\n".join(lines)


def print_human_summary(result: SuiteResult) -> None:
    """Print concise console summary matching the report contract."""
    print("MONTHLY DWH VALIDATION")
    print("")
    for cat in result.categories:
        title = CATEGORY_TITLES.get(cat.name, cat.name)
        print(f"{title}: {cat.status} {cat.passed}/{cat.total}")
    print("")
    print(f"FINAL STATUS: {result.final_status}")
    failed = result.failed_critical()
    if failed:
        print("")
        print("Failed checks:")
        for c in failed:
            detail = f" — {c.detail}" if c.detail else ""
            print(f"  - {c.name}{detail}")
    warnings = result.warnings()
    if warnings:
        print("")
        print("Warnings:")
        for c in warnings:
            detail = f" — {c.detail}" if c.detail else ""
            print(f"  - {c.name}{detail}")


__all__ = [
    "AIRFLOW_SUMMARY_CATEGORIES",
    "CATEGORY_TITLES",
    "VALIDATION_JSON_NAME",
    "VALIDATION_MD_NAME",
    "format_airflow_validation_summary",
    "print_human_summary",
    "render_markdown",
    "write_suite_reports",
]
