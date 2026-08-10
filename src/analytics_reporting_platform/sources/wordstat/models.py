"""Typed models and credential loading for Yandex Wordstat."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping, Optional, Sequence

from analytics_reporting_platform.config.settings import _merged_environ
from analytics_reporting_platform.sources.wordstat.errors import WordstatConfigurationError


@dataclass(frozen=True)
class WordstatCredentials:
    """Runtime Wordstat credentials (never include secrets in repr/str)."""

    api_key: str
    folder_id: str

    def __repr__(self) -> str:
        return "WordstatCredentials(api_key=***, folder_id=***)"

    __str__ = __repr__


@dataclass(frozen=True)
class WordstatDynamicsPoint:
    """One dynamics observation returned by Wordstat."""

    date: date
    count: int
    share: Optional[float]


@dataclass(frozen=True)
class WordstatDynamicsResult:
    """Structured dynamics response for a single phrase."""

    phrase: str
    period: str
    from_date: date
    to_date: date
    points: tuple[WordstatDynamicsPoint, ...]
    regions: Optional[tuple[int, ...]] = None

    def month_counts(self) -> dict[str, int]:
        """Map ``YYYY-MM`` → count for convenient smoke / validation tables."""
        return {point.date.strftime("%Y-%m"): point.count for point in self.points}


def load_wordstat_credentials(
    env_file: Path | str | None = None,
    *,
    environ: Mapping[str, Optional[str]] | None = None,
) -> WordstatCredentials:
    """Load Wordstat credentials from process env and/or an explicit env file.

    Does not require database settings. The client itself never opens env files.
    """
    if environ is not None:
        env = environ
    else:
        path = Path(env_file) if env_file is not None else None
        env = _merged_environ(path)

    api_key = _require_secret("YANDEX_WORDSTAT_API_KEY", env.get("YANDEX_WORDSTAT_API_KEY"))
    folder_id = _require_secret(
        "YANDEX_WORDSTAT_FOLDER_ID", env.get("YANDEX_WORDSTAT_FOLDER_ID")
    )
    return WordstatCredentials(api_key=api_key, folder_id=folder_id)


def _require_secret(name: str, value: Optional[str]) -> str:
    if value is None or not str(value).strip():
        raise WordstatConfigurationError(f"Missing required Wordstat configuration: {name}")
    return str(value).strip()


def normalize_period(period: str) -> str:
    """Normalize caller period aliases to Wordstat wire enums."""
    text = str(period).strip().upper()
    aliases = {
        "MONTHLY": "PERIOD_MONTHLY",
        "PERIOD_MONTHLY": "PERIOD_MONTHLY",
        "WEEKLY": "PERIOD_WEEKLY",
        "PERIOD_WEEKLY": "PERIOD_WEEKLY",
        "DAILY": "PERIOD_DAILY",
        "PERIOD_DAILY": "PERIOD_DAILY",
    }
    if text not in aliases:
        raise WordstatConfigurationError(
            "Unsupported Wordstat period. Expected MONTHLY, WEEKLY, or DAILY"
        )
    return aliases[text]


def ensure_sequence_int(regions: Optional[Sequence[int]]) -> Optional[tuple[int, ...]]:
    if regions is None:
        return None
    out: list[int] = []
    for item in regions:
        try:
            out.append(int(item))
        except (TypeError, ValueError) as exc:
            raise WordstatConfigurationError(
                "Wordstat regions must be integers"
            ) from exc
    return tuple(out)


__all__ = [
    "WordstatCredentials",
    "WordstatDynamicsPoint",
    "WordstatDynamicsResult",
    "ensure_sequence_int",
    "load_wordstat_credentials",
    "normalize_period",
]
