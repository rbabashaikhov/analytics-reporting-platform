"""Yandex Cloud Search API / Wordstat v2 dynamics client.

Minimal source adapter: credentials are injected; this module never opens env files.
"""

from __future__ import annotations

import calendar
import logging
import time
from datetime import date, datetime, timezone
from typing import Any, Mapping, Optional, Protocol, Sequence

import requests

from analytics_reporting_platform.sources.wordstat.errors import (
    WordstatApiError,
    WordstatConfigurationError,
)
from analytics_reporting_platform.sources.wordstat.models import (
    WordstatCredentials,
    WordstatDynamicsPoint,
    WordstatDynamicsResult,
    ensure_sequence_int,
    normalize_period,
)

DEFAULT_DYNAMICS_URL = "https://searchapi.api.cloud.yandex.net/v2/wordstat/dynamics"
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
NON_RETRYABLE_STATUS_CODES = frozenset({400, 401, 403})

logger = logging.getLogger(__name__)


class SupportsHttpRequest(Protocol):
    """Minimal session protocol for Wordstat POST (requests.Session compatible)."""

    def post(
        self,
        url: str,
        *,
        json: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Any: ...


class WordstatClient:
    """Yandex Wordstat dynamics client with bounded retries."""

    def __init__(
        self,
        credentials: WordstatCredentials,
        *,
        session: Optional[SupportsHttpRequest] = None,
        dynamics_url: str = DEFAULT_DYNAMICS_URL,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep: Optional[Any] = None,
    ) -> None:
        if not credentials.api_key:
            raise WordstatConfigurationError(
                "Missing required Wordstat configuration: YANDEX_WORDSTAT_API_KEY"
            )
        if not credentials.folder_id:
            raise WordstatConfigurationError(
                "Missing required Wordstat configuration: YANDEX_WORDSTAT_FOLDER_ID"
            )
        self._credentials = credentials
        self._session: SupportsHttpRequest = (
            session if session is not None else requests.Session()
        )
        self._dynamics_url = dynamics_url.rstrip("/")
        self._timeout_seconds = float(timeout_seconds)
        self._max_retries = max(1, int(max_retries))
        self._sleep = sleep if sleep is not None else time.sleep

    @classmethod
    def from_credentials(
        cls, credentials: WordstatCredentials, **kwargs: Any
    ) -> "WordstatClient":
        return cls(credentials, **kwargs)

    def get_dynamics(
        self,
        phrase: str,
        from_date: date,
        to_date: date,
        *,
        period: str = "MONTHLY",
        regions: Optional[Sequence[int]] = None,
    ) -> WordstatDynamicsResult:
        """Fetch Wordstat dynamics for one phrase over ``[from_date, to_date]``."""
        if not isinstance(phrase, str) or not phrase.strip():
            raise WordstatConfigurationError("Wordstat phrase must be a non-empty string")
        if not isinstance(from_date, date) or isinstance(from_date, datetime):
            raise WordstatConfigurationError("from_date must be a datetime.date")
        if not isinstance(to_date, date) or isinstance(to_date, datetime):
            raise WordstatConfigurationError("to_date must be a datetime.date")
        if to_date < from_date:
            raise WordstatConfigurationError("to_date must be on or after from_date")

        wire_period = normalize_period(period)
        region_tuple = ensure_sequence_int(regions)
        if wire_period == "PERIOD_MONTHLY":
            _require_month_end(to_date, field_name="to_date")

        body: dict[str, Any] = {
            "phrase": phrase.strip(),
            "period": wire_period,
            "fromDate": _to_rfc3339_start(from_date),
            "toDate": _to_rfc3339_end(to_date),
            "folderId": self._credentials.folder_id,
        }
        if region_tuple is not None:
            body["regions"] = list(region_tuple)

        payload = self._post_json(body, phrase=phrase.strip())
        points = _parse_dynamics_points(payload)
        return WordstatDynamicsResult(
            phrase=phrase.strip(),
            period=wire_period,
            from_date=from_date,
            to_date=to_date,
            points=points,
            regions=region_tuple,
        )

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Api-Key {self._credentials.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _post_json(self, body: Mapping[str, Any], *, phrase: str) -> dict[str, Any]:
        last_error: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 1):
            headers = self._auth_headers()
            try:
                response = self._session.post(
                    self._dynamics_url,
                    json=dict(body),
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
            except requests.Timeout as exc:
                last_error = WordstatApiError("Wordstat dynamics request timed out")
                if attempt >= self._max_retries:
                    raise last_error from exc
                wait_s = _exponential_backoff_seconds(attempt)
                logger.info(
                    "wordstat_dynamics_retry attempt=%s phrase=%r status=timeout wait_s=%s",
                    attempt,
                    phrase,
                    wait_s,
                )
                self._sleep(wait_s)
                continue
            except requests.RequestException as exc:
                last_error = WordstatApiError(
                    "Wordstat dynamics request failed due to a network error"
                )
                if attempt >= self._max_retries:
                    raise last_error from exc
                wait_s = _exponential_backoff_seconds(attempt)
                logger.info(
                    "wordstat_dynamics_retry attempt=%s phrase=%r status=network wait_s=%s",
                    attempt,
                    phrase,
                    wait_s,
                )
                self._sleep(wait_s)
                continue

            status = int(getattr(response, "status_code", 0) or 0)
            if 200 <= status < 300:
                return _parse_json_object(response, status_code=status)

            if status in NON_RETRYABLE_STATUS_CODES:
                raise WordstatApiError(
                    "Wordstat dynamics request failed with a non-retryable HTTP status",
                    status_code=status,
                    error_code=_safe_error_code(response),
                )

            if status in RETRYABLE_STATUS_CODES and attempt < self._max_retries:
                wait_s = _retry_wait_seconds(response, attempt=attempt)
                logger.info(
                    "wordstat_dynamics_retry attempt=%s phrase=%r status=%s wait_s=%s",
                    attempt,
                    phrase,
                    status,
                    wait_s,
                )
                self._sleep(wait_s)
                continue

            raise WordstatApiError(
                "Wordstat dynamics request failed with a non-success HTTP status",
                status_code=status,
                error_code=_safe_error_code(response),
            )

        if last_error is not None:
            raise last_error
        raise WordstatApiError("Wordstat dynamics request failed without a response")

    def __repr__(self) -> str:
        return (
            "WordstatClient("
            f"dynamics_url={self._dynamics_url!r}, "
            "credentials=***)"
        )

    __str__ = __repr__


def _require_month_end(value: date, *, field_name: str) -> None:
    last_day = calendar.monthrange(value.year, value.month)[1]
    if value.day != last_day:
        raise WordstatConfigurationError(
            f"{field_name} must be the last day of the month for PERIOD_MONTHLY"
        )


def _to_rfc3339_start(value: date) -> str:
    return datetime(value.year, value.month, value.day, 0, 0, 0, tzinfo=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _to_rfc3339_end(value: date) -> str:
    return datetime(
        value.year, value.month, value.day, 23, 59, 59, tzinfo=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _exponential_backoff_seconds(attempt: int) -> float:
    return float(min(2**attempt, 30))


def _retry_wait_seconds(response: Any, *, attempt: int) -> float:
    fallback = _exponential_backoff_seconds(attempt)
    headers = getattr(response, "headers", None) or {}
    raw = None
    if hasattr(headers, "get"):
        raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return fallback
    try:
        seconds = float(str(raw).strip())
    except (TypeError, ValueError):
        return fallback
    if seconds < 0:
        return fallback
    return float(min(max(seconds, 1.0), 120.0))


def _parse_json_object(response: Any, *, status_code: int) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise WordstatApiError(
            "Wordstat dynamics response was not valid JSON",
            status_code=status_code,
        ) from exc
    if not isinstance(data, dict):
        raise WordstatApiError(
            "Wordstat dynamics response had an unexpected JSON shape",
            status_code=status_code,
        )
    return data


def _parse_dynamics_points(payload: Mapping[str, Any]) -> tuple[WordstatDynamicsPoint, ...]:
    raw_results = payload.get("results")
    if raw_results is None:
        return tuple()
    if not isinstance(raw_results, list):
        raise WordstatApiError("Wordstat dynamics response 'results' must be a list")

    points: list[WordstatDynamicsPoint] = []
    for item in raw_results:
        if not isinstance(item, Mapping):
            raise WordstatApiError("Wordstat dynamics result item must be an object")
        point_date = _parse_point_date(item.get("date"))
        count = _parse_count(item.get("count"))
        share = _parse_share(item.get("share"))
        points.append(WordstatDynamicsPoint(date=point_date, count=count, share=share))
    return tuple(points)


def _parse_point_date(raw: Any) -> date:
    if not isinstance(raw, str) or not raw.strip():
        raise WordstatApiError("Wordstat dynamics point missing date")
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise WordstatApiError("Wordstat dynamics point date was not parseable") from exc
    return parsed.date()


def _parse_count(raw: Any) -> int:
    if raw is None:
        raise WordstatApiError("Wordstat dynamics point missing count")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise WordstatApiError("Wordstat dynamics point count was not an integer") from exc


def _parse_share(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise WordstatApiError("Wordstat dynamics point share was not numeric") from exc


def _safe_error_code(response: Any) -> Optional[str]:
    try:
        data = response.json()
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    for key in ("error", "code", "message", "errorCode", "error_code"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            text = value.strip()
            if len(text) > 120:
                text = text[:117] + "..."
            lowered = text.lower()
            if "api-key" in lowered or "authorization" in lowered:
                return key
            return text
        if isinstance(value, int):
            return str(value)
    return None


__all__ = [
    "DEFAULT_DYNAMICS_URL",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_TIMEOUT_SECONDS",
    "NON_RETRYABLE_STATUS_CODES",
    "RETRYABLE_STATUS_CODES",
    "WordstatClient",
]
