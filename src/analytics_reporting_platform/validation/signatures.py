"""Lightweight read-only Monthly table signatures (counts + opaque fingerprints).

Fingerprints hash only business-key identity material via SQL digests of key
columns — never prints URLs, KPI values, or row payloads.
Limitation: identical counts + identical key sets with changed measure columns
are not detected (structural/count-oriented harness, not full DQ).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analytics_reporting_platform.execution.period import (
    ExecutionMonthInput,
    validate_execution_month,
)
from analytics_reporting_platform.validation.specs import (
    ALL_SPECS,
    LayerName,
    ValidationTableSpec,
)


@dataclass(frozen=True)
class TablePeriodSignature:
    """Safe structural signature for one table / target month slice."""

    layer: LayerName
    mart: str
    table: str
    period: str
    row_count: int
    duplicate_key_groups: int
    key_fingerprint: str


@dataclass(frozen=True)
class MonthlySignatureBundle:
    """Before/after snapshot for Monthly layers (read-only capture)."""

    target_month: str
    captured_at: str
    tables: tuple[TablePeriodSignature, ...]
    month_counts: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_month": self.target_month,
            "captured_at": self.captured_at,
            "tables": [asdict(t) for t in self.tables],
            "month_counts": list(self.month_counts),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "MonthlySignatureBundle":
        tables = tuple(
            TablePeriodSignature(**row) for row in payload.get("tables", [])  # type: ignore[arg-type]
        )
        month_counts = tuple(dict(row) for row in payload.get("month_counts", []))
        return cls(
            target_month=str(payload["target_month"]),
            captured_at=str(payload["captured_at"]),
            tables=tables,
            month_counts=month_counts,
        )


def _quote_ident(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def _month_row_count(engine: Engine, table: str, month: date) -> int:
    sql = text(f"SELECT COUNT(*) AS n FROM {_quote_ident(table)} WHERE month = :month")
    with engine.connect() as conn:
        return int(conn.execute(sql, {"month": month}).scalar_one())


def _duplicate_key_groups(
    engine: Engine, table: str, month: date, business_key: Sequence[str]
) -> int:
    cols = ", ".join(_quote_ident(c) for c in business_key)
    sql = text(
        f"""
        SELECT COUNT(*) AS n FROM (
            SELECT {cols}, COUNT(*) AS c
            FROM {_quote_ident(table)}
            WHERE month = :month
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        ) dups
        """
    )
    with engine.connect() as conn:
        return int(conn.execute(sql, {"month": month}).scalar_one())


def _key_fingerprint(
    engine: Engine, table: str, month: date, business_key: Sequence[str]
) -> str:
    """Opaque digest over sorted business-key tuples (no measure columns)."""
    cols = ", ".join(_quote_ident(c) for c in business_key)
    # Concatenate key parts; digest in Python to avoid DB-specific hash functions.
    sql = text(
        f"""
        SELECT {cols}
        FROM {_quote_ident(table)}
        WHERE month = :month
        ORDER BY {cols}
        """
    )
    digest = hashlib.sha256()
    with engine.connect() as conn:
        rows = conn.execute(sql, {"month": month}).fetchall()
    digest.update(f"count={len(rows)}|".encode("utf-8"))
    for row in rows:
        digest.update(("|".join("" if v is None else str(v) for v in row) + "\n").encode("utf-8"))
    return digest.hexdigest()


def _month_histogram(engine: Engine, table: str) -> list[dict[str, Any]]:
    sql = text(
        f"""
        SELECT month::text AS month, COUNT(*)::int AS row_count
        FROM {_quote_ident(table)}
        GROUP BY month
        ORDER BY month
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return [
        {"table": table, "month": str(r["month"])[:10], "row_count": int(r["row_count"])}
        for r in rows
    ]


def capture_monthly_signatures(
    *,
    raw_engine: Engine,
    calc_engine: Engine,
    cdm_engine: Engine,
    month: ExecutionMonthInput,
    specs: Sequence[ValidationTableSpec] = ALL_SPECS,
) -> MonthlySignatureBundle:
    """Capture read-only signatures for target month + per-month counts for scope diffs."""
    target = validate_execution_month(month, allow_future=True)
    engines: dict[LayerName, Engine] = {
        "raw": raw_engine,
        "calc": calc_engine,
        "cdm": cdm_engine,
    }
    tables: list[TablePeriodSignature] = []
    month_counts: list[dict[str, Any]] = []
    for spec in specs:
        engine = engines[spec.layer]
        row_count = _month_row_count(engine, spec.table, target)
        dups = _duplicate_key_groups(engine, spec.table, target, spec.business_key)
        fingerprint = _key_fingerprint(engine, spec.table, target, spec.business_key)
        tables.append(
            TablePeriodSignature(
                layer=spec.layer,
                mart=spec.mart,
                table=spec.table,
                period=target.isoformat(),
                row_count=row_count,
                duplicate_key_groups=dups,
                key_fingerprint=fingerprint,
            )
        )
        for row in _month_histogram(engine, spec.table):
            month_counts.append({"layer": spec.layer, **row})

    return MonthlySignatureBundle(
        target_month=target.isoformat(),
        captured_at=datetime.now(timezone.utc).isoformat(),
        tables=tuple(tables),
        month_counts=tuple(month_counts),
    )


def save_signature_bundle(bundle: MonthlySignatureBundle, path: str | Path) -> Path:
    out = Path(path)
    out.write_text(json.dumps(bundle.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out


def load_signature_bundle(path: str | Path) -> MonthlySignatureBundle:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return MonthlySignatureBundle.from_dict(payload)


__all__ = [
    "MonthlySignatureBundle",
    "TablePeriodSignature",
    "capture_monthly_signatures",
    "load_signature_bundle",
    "save_signature_bundle",
]
