"""Adobe Analytics source adapter — authentication + Reports transport.

Report extraction / RAW loading are not implemented in this package yet.

Heavy imports (``requests`` clients) are lazy so configuration loading does not
require an HTTP stack.
"""

from __future__ import annotations

from typing import Any

from analytics_reporting_platform.sources.adobe.errors import (
    AdobeApiError,
    AdobeAuthenticationError,
    AdobeConfigurationError,
    AdobeError,
)
from analytics_reporting_platform.sources.adobe.models import (
    AdobeAccessToken,
    AdobeAuthCredentials,
)

__all__ = [
    "AdobeAccessToken",
    "AdobeApiError",
    "AdobeAuthClient",
    "AdobeAuthCredentials",
    "AdobeAuthenticationError",
    "AdobeConfigurationError",
    "AdobeError",
    "AdobeReportsClient",
    "DEFAULT_EXPIRY_SKEW_SECONDS",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_REPORTS_HOST",
    "DEFAULT_SCOPES",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_TOKEN_URL",
    "RETRYABLE_STATUS_CODES",
]

_LAZY_EXPORTS = {
    "AdobeAuthClient": ("analytics_reporting_platform.sources.adobe.client", "AdobeAuthClient"),
    "DEFAULT_EXPIRY_SKEW_SECONDS": (
        "analytics_reporting_platform.sources.adobe.client",
        "DEFAULT_EXPIRY_SKEW_SECONDS",
    ),
    "DEFAULT_SCOPES": ("analytics_reporting_platform.sources.adobe.client", "DEFAULT_SCOPES"),
    "DEFAULT_TIMEOUT_SECONDS": (
        "analytics_reporting_platform.sources.adobe.client",
        "DEFAULT_TIMEOUT_SECONDS",
    ),
    "DEFAULT_TOKEN_URL": (
        "analytics_reporting_platform.sources.adobe.client",
        "DEFAULT_TOKEN_URL",
    ),
    "AdobeReportsClient": (
        "analytics_reporting_platform.sources.adobe.reports",
        "AdobeReportsClient",
    ),
    "DEFAULT_REPORTS_HOST": (
        "analytics_reporting_platform.sources.adobe.reports",
        "DEFAULT_REPORTS_HOST",
    ),
    "DEFAULT_MAX_RETRIES": (
        "analytics_reporting_platform.sources.adobe.reports",
        "DEFAULT_MAX_RETRIES",
    ),
    "RETRYABLE_STATUS_CODES": (
        "analytics_reporting_platform.sources.adobe.reports",
        "RETRYABLE_STATUS_CODES",
    ),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_EXPORTS:
        module_name, attr = _LAZY_EXPORTS[name]
        import importlib

        module = importlib.import_module(module_name)
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
