"""Yandex Wordstat source adapter (dynamics)."""

from __future__ import annotations

from typing import Any

from analytics_reporting_platform.sources.wordstat.errors import (
    WordstatApiError,
    WordstatConfigurationError,
    WordstatError,
)
from analytics_reporting_platform.sources.wordstat.models import (
    WordstatCredentials,
    WordstatDynamicsPoint,
    WordstatDynamicsResult,
    load_wordstat_credentials,
)

__all__ = [
    "DEFAULT_DYNAMICS_URL",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_TIMEOUT_SECONDS",
    "NON_RETRYABLE_STATUS_CODES",
    "RETRYABLE_STATUS_CODES",
    "WordstatApiError",
    "WordstatClient",
    "WordstatConfigurationError",
    "WordstatCredentials",
    "WordstatDynamicsPoint",
    "WordstatDynamicsResult",
    "WordstatError",
    "load_wordstat_credentials",
]

_LAZY_EXPORTS = {
    "WordstatClient": ("analytics_reporting_platform.sources.wordstat.client", "WordstatClient"),
    "DEFAULT_DYNAMICS_URL": (
        "analytics_reporting_platform.sources.wordstat.client",
        "DEFAULT_DYNAMICS_URL",
    ),
    "DEFAULT_MAX_RETRIES": (
        "analytics_reporting_platform.sources.wordstat.client",
        "DEFAULT_MAX_RETRIES",
    ),
    "DEFAULT_TIMEOUT_SECONDS": (
        "analytics_reporting_platform.sources.wordstat.client",
        "DEFAULT_TIMEOUT_SECONDS",
    ),
    "RETRYABLE_STATUS_CODES": (
        "analytics_reporting_platform.sources.wordstat.client",
        "RETRYABLE_STATUS_CODES",
    ),
    "NON_RETRYABLE_STATUS_CODES": (
        "analytics_reporting_platform.sources.wordstat.client",
        "NON_RETRYABLE_STATUS_CODES",
    ),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_EXPORTS:
        module_name, attr = _LAZY_EXPORTS[name]
        import importlib

        module = importlib.import_module(module_name)
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
