"""Read-only PostgreSQL connectivity helpers.

Allowed operation: ``SELECT 1`` only.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DatabaseHealthResult:
    """Result of a minimal database connectivity check."""

    ok: bool
    detail: str = "ok"


def check_database_connection(engine: Engine) -> DatabaseHealthResult:
    """Verify that the engine can execute ``SELECT 1``.

    Does not inspect application tables and performs no writes.
    """
    try:
        with engine.connect() as conn:
            value = conn.execute(text("SELECT 1")).scalar()
        if value != 1:
            return DatabaseHealthResult(ok=False, detail="unexpected SELECT 1 result")
        return DatabaseHealthResult(ok=True, detail="ok")
    except Exception as exc:  # noqa: BLE001 - surface sanitized connectivity failure
        # Do not include connection URL / password material in the detail string.
        return DatabaseHealthResult(ok=False, detail=type(exc).__name__)


__all__ = ["DatabaseHealthResult", "check_database_connection"]
