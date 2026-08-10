"""Private runtime artifact paths for Monthly DWH validation (Airflow).

Root is configured via env / Airflow Variable — never hardcode private absolute
paths in reusable library code.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

VALIDATION_ARTIFACT_ROOT_ENV = "ARP_VALIDATION_ARTIFACT_ROOT"
VALIDATION_ARTIFACT_ROOT_VARIABLE = "arp_validation_artifact_root"

# Filesystem-safe fallback under the gitignored execution-work tree.
_DEFAULT_RELATIVE_ROOT = Path("execution-work") / "validation"

_UNSAFE_RUN_ID = re.compile(r"[^A-Za-z0-9._-]+")


def resolve_validation_artifact_root(
    *,
    explicit: Optional[str] = None,
    environ: Optional[dict[str, str]] = None,
) -> Path:
    """Resolve artifact root from explicit path, env, or safe local default."""
    if explicit is not None and str(explicit).strip():
        return Path(str(explicit).strip()).expanduser()

    env = environ if environ is not None else os.environ
    from_env = env.get(VALIDATION_ARTIFACT_ROOT_ENV)
    if from_env and str(from_env).strip():
        return Path(str(from_env).strip()).expanduser()

    # Optional Airflow Variable (path only) — best-effort, no hard dependency.
    try:
        from airflow.models import Variable

        from_var = Variable.get(VALIDATION_ARTIFACT_ROOT_VARIABLE, default_var=None)
        if from_var is not None and str(from_var).strip():
            return Path(str(from_var).strip()).expanduser()
    except Exception:  # noqa: BLE001
        pass

    return _DEFAULT_RELATIVE_ROOT


def sanitize_run_id(run_id: str) -> str:
    """Make Airflow run_id safe for a single filesystem directory name."""
    text = str(run_id).strip() or "unknown_run"
    text = text.replace("/", "_").replace("\\", "_").replace(" ", "_")
    text = _UNSAFE_RUN_ID.sub("_", text)
    text = text.strip("._-") or "unknown_run"
    return text[:180]


def validation_run_dir(
    *,
    run_id: str,
    dag_id: str = "monthly_private_pipeline",
    root: Optional[Path] = None,
) -> Path:
    """``<root>/<dag_id>/<safe_run_id>/`` — unique per DAG run, never overwrites."""
    base = root if root is not None else resolve_validation_artifact_root()
    return Path(base) / dag_id / sanitize_run_id(run_id)


__all__ = [
    "VALIDATION_ARTIFACT_ROOT_ENV",
    "VALIDATION_ARTIFACT_ROOT_VARIABLE",
    "resolve_validation_artifact_root",
    "sanitize_run_id",
    "validation_run_dir",
]
