"""Generic Adobe Analytics Reports API transport (read-only HTTP).

Business payloads (metrics, dimensions, segments, dates) are supplied by the
caller. This module does not encode Monthly RAW mart semantics.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Mapping, MutableMapping, Optional, Protocol

import requests

from analytics_reporting_platform.sources.adobe.client import AdobeAuthClient
from analytics_reporting_platform.sources.adobe.errors import (
    AdobeApiError,
    AdobeAuthenticationError,
    AdobeConfigurationError,
)

# Public Adobe Analytics Reports endpoint host (company id is path-scoped).
DEFAULT_REPORTS_HOST = "https://analytics.adobe.io"

DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_RETRIES = 3

# Conservative retry set aligned with historical loader categories.
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

logger = logging.getLogger(__name__)


class SupportsHttpRequest(Protocol):
    """Minimal session protocol for Reports POST (requests.Session compatible)."""

    def post(
        self,
        url: str,
        *,
        json: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Any: ...


class AdobeReportsClient:
    """Authenticated Reports API transport.

    Parameters
    ----------
    auth_client:
        Portfolio Adobe auth client (token + base headers).
    report_suite_id:
        Report suite applied to every request (injected as ``rsid``).
        Must not be logged or included in error text.
    """

    def __init__(
        self,
        auth_client: AdobeAuthClient,
        report_suite_id: str,
        *,
        session: Optional[SupportsHttpRequest] = None,
        reports_base_url: str = DEFAULT_REPORTS_HOST,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep: Optional[Any] = None,
    ) -> None:
        if not isinstance(report_suite_id, str) or not report_suite_id.strip():
            raise AdobeConfigurationError(
                "Missing required Adobe configuration: report_suite_id"
            )
        self._auth = auth_client
        self._report_suite_id = report_suite_id.strip()
        self._session: SupportsHttpRequest = (
            session if session is not None else requests.Session()
        )
        self._reports_base_url = reports_base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_retries = max(1, int(max_retries))
        self._sleep = sleep if sleep is not None else time.sleep

    def reports_url(self) -> str:
        """Return the company-scoped Reports endpoint (no query secrets)."""
        company_id = self._auth.global_company_id
        return f"{self._reports_base_url}/api/{company_id}/reports"

    def post_report(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """POST a caller-supplied report payload and return parsed JSON.

        Injects ``rsid`` from the client configuration. Does not interpret
        metrics, dimensions, or segments.
        """
        if not isinstance(payload, Mapping):
            raise AdobeApiError("Reports payload must be a mapping")

        body: MutableMapping[str, Any] = dict(payload)
        body["rsid"] = self._report_suite_id

        url = self.reports_url()
        last_error: Optional[Exception] = None
        did_auth_refresh = False

        for attempt in range(1, self._max_retries + 1):
            headers = dict(self._auth.auth_headers())
            try:
                response = self._session.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except requests.Timeout as exc:
                last_error = AdobeApiError("Adobe Reports request timed out")
                if attempt >= self._max_retries:
                    raise last_error from exc
                wait_s = min(10 * attempt, 30)
                logger.info(
                    "adobe_reports_retry attempt=%s status=timeout wait_s=%s",
                    attempt,
                    wait_s,
                )
                self._sleep(wait_s)
                continue
            except requests.RequestException as exc:
                last_error = AdobeApiError(
                    "Adobe Reports request failed due to a network error"
                )
                if attempt >= self._max_retries:
                    raise last_error from exc
                wait_s = min(10 * attempt, 30)
                logger.info(
                    "adobe_reports_retry attempt=%s status=network wait_s=%s",
                    attempt,
                    wait_s,
                )
                self._sleep(wait_s)
                continue

            status = int(getattr(response, "status_code", 0) or 0)

            if 200 <= status < 300:
                return _parse_json_object(response, status_code=status)

            if status == 401 and not did_auth_refresh:
                # One controlled token refresh then retry.
                did_auth_refresh = True
                try:
                    self._auth.get_access_token(force_refresh=True)
                except AdobeAuthenticationError as exc:
                    raise AdobeApiError(
                        "Adobe Reports authentication failed after token refresh",
                        status_code=401,
                    ) from exc
                logger.info(
                    "adobe_reports_retry attempt=%s status=401 action=token_refresh",
                    attempt,
                )
                continue

            if status in RETRYABLE_STATUS_CODES and attempt < self._max_retries:
                wait_s = _retry_wait_seconds(response, attempt=attempt)
                logger.info(
                    "adobe_reports_retry attempt=%s status=%s wait_s=%s",
                    attempt,
                    status,
                    wait_s,
                )
                self._sleep(wait_s)
                continue

            error_code = _safe_error_code(response)
            raise AdobeApiError(
                "Adobe Reports request failed with a non-success HTTP status",
                status_code=status,
                error_code=error_code,
            )

        if last_error is not None:
            raise last_error
        raise AdobeApiError("Adobe Reports request failed without a response")

    def __repr__(self) -> str:
        return (
            "AdobeReportsClient("
            f"reports_base_url={self._reports_base_url!r}, "
            "report_suite_id=***)"
        )

    __str__ = __repr__


def _retry_wait_seconds(response: Any, *, attempt: int) -> float:
    """Bounded exponential backoff; honor Retry-After seconds when present."""
    fallback = float(min(15 * attempt, 45))
    headers = getattr(response, "headers", None) or {}
    raw = None
    if hasattr(headers, "get"):
        raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return fallback
    try:
        # Adobe typically returns delay-seconds (not HTTP-date).
        seconds = float(str(raw).strip())
    except (TypeError, ValueError):
        return fallback
    if seconds < 0:
        return fallback
    # Cap Retry-After so a pathological header cannot stall forever.
    return float(min(max(seconds, 1.0), 120.0))


def _parse_json_object(response: Any, *, status_code: int) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise AdobeApiError(
            "Adobe Reports response was not valid JSON",
            status_code=status_code,
        ) from exc
    if not isinstance(data, dict):
        raise AdobeApiError(
            "Adobe Reports response had an unexpected JSON shape",
            status_code=status_code,
        )
    return data


def _safe_error_code(response: Any) -> Optional[str]:
    try:
        data = response.json()
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    for key in ("error", "errorCode", "error_code", "errorDescription", "message"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            text = value.strip()
            if len(text) > 120:
                text = text[:117] + "..."
            lowered = text.lower()
            if "bearer " in lowered or "access_token" in lowered:
                return key
            # Avoid echoing identifiers that look like suite/segment IDs.
            if len(text) >= 12 and text.replace("-", "").replace("_", "").isalnum():
                return key
            return text
    return None


__all__ = [
    "DEFAULT_REPORTS_HOST",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_MAX_RETRIES",
    "RETRYABLE_STATUS_CODES",
    "AdobeReportsClient",
]
