"""Unit tests for Yandex Wordstat dynamics client (mocked HTTP only)."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

import pytest
import requests

from analytics_reporting_platform.sources.wordstat import (
    WordstatApiError,
    WordstatClient,
    WordstatConfigurationError,
    WordstatCredentials,
    load_wordstat_credentials,
)


SYNTHETIC_API_KEY = "synthetic-yandex-wordstat-api-key-do-not-leak"
SYNTHETIC_FOLDER_ID = "synthetic-yandex-folder-id-not-real"


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        *,
        headers: Optional[dict[str, str]] = None,
        json_error: bool = False,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self._json_error = json_error

    def json(self) -> Any:
        if self._json_error:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(
        self,
        url: str,
        *,
        json: Any = None,
        headers: Any = None,
        timeout: float | None = None,
    ) -> Any:
        self.calls.append(
            {
                "url": url,
                "json": dict(json or {}),
                "headers": dict(headers or {}),
                "timeout": timeout,
            }
        )
        if not self._responses:
            raise AssertionError("unexpected HTTP call")
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _credentials() -> WordstatCredentials:
    return WordstatCredentials(api_key=SYNTHETIC_API_KEY, folder_id=SYNTHETIC_FOLDER_ID)


def _client(session: FakeSession, **kwargs: Any) -> WordstatClient:
    sleeps: list[float] = []

    def _sleep(seconds: float) -> None:
        sleeps.append(float(seconds))

    client = WordstatClient(
        _credentials(),
        session=session,
        sleep=_sleep,
        max_retries=kwargs.pop("max_retries", 3),
        **kwargs,
    )
    client._test_sleeps = sleeps  # type: ignore[attr-defined]
    return client


def _ok_payload() -> dict[str, Any]:
    return {
        "results": [
            {"date": "2026-01-31T00:00:00Z", "count": "100", "share": 0.0001},
            {"date": "2026-02-28T00:00:00Z", "count": 110, "share": 0.00011},
        ]
    }


def test_successful_request_and_parsing() -> None:
    session = FakeSession([FakeResponse(200, _ok_payload())])
    client = _client(session)

    result = client.get_dynamics(
        "brand-phrase",
        date(2026, 1, 1),
        date(2026, 2, 28),
        period="MONTHLY",
    )

    assert result.phrase == "brand-phrase"
    assert result.period == "PERIOD_MONTHLY"
    assert len(result.points) == 2
    assert result.points[0].date == date(2026, 1, 31)
    assert result.points[0].count == 100
    assert result.points[0].share == pytest.approx(0.0001)
    assert result.points[1].count == 110
    assert result.month_counts() == {"2026-01": 100, "2026-02": 110}

    call = session.calls[0]
    assert call["url"].endswith("/v2/wordstat/dynamics")
    assert call["json"]["phrase"] == "brand-phrase"
    assert call["json"]["period"] == "PERIOD_MONTHLY"
    assert call["json"]["fromDate"] == "2026-01-01T00:00:00Z"
    assert call["json"]["toDate"] == "2026-02-28T23:59:59Z"
    assert call["json"]["folderId"] == SYNTHETIC_FOLDER_ID
    assert call["headers"]["Authorization"] == f"Api-Key {SYNTHETIC_API_KEY}"


def test_empty_response() -> None:
    session = FakeSession([FakeResponse(200, {"results": []})])
    client = _client(session)
    result = client.get_dynamics("x", date(2026, 1, 1), date(2026, 1, 31))
    assert result.points == tuple()
    assert result.month_counts() == {}


def test_missing_results_key_treated_as_empty() -> None:
    session = FakeSession([FakeResponse(200, {})])
    client = _client(session)
    result = client.get_dynamics("x", date(2026, 1, 1), date(2026, 1, 31))
    assert result.points == tuple()


def test_timeout_retries_then_succeeds() -> None:
    session = FakeSession(
        [
            requests.Timeout("slow"),
            FakeResponse(200, _ok_payload()),
        ]
    )
    client = _client(session)
    result = client.get_dynamics("x", date(2026, 1, 1), date(2026, 2, 28))
    assert len(result.points) == 2
    assert len(session.calls) == 2
    assert client._test_sleeps  # type: ignore[attr-defined]


def test_network_error_retries_then_succeeds() -> None:
    session = FakeSession(
        [
            requests.ConnectionError("down"),
            FakeResponse(200, _ok_payload()),
        ]
    )
    client = _client(session)
    result = client.get_dynamics("x", date(2026, 1, 1), date(2026, 2, 28))
    assert len(result.points) == 2
    assert len(session.calls) == 2


def test_429_retries_honoring_retry_after() -> None:
    session = FakeSession(
        [
            FakeResponse(429, {"message": "rate"}, headers={"Retry-After": "7"}),
            FakeResponse(200, _ok_payload()),
        ]
    )
    client = _client(session)
    result = client.get_dynamics("x", date(2026, 1, 1), date(2026, 2, 28))
    assert len(result.points) == 2
    assert client._test_sleeps[0] == 7.0  # type: ignore[attr-defined]


def test_transient_5xx_retries() -> None:
    session = FakeSession(
        [
            FakeResponse(503, {"message": "busy"}),
            FakeResponse(200, _ok_payload()),
        ]
    )
    client = _client(session)
    result = client.get_dynamics("x", date(2026, 1, 1), date(2026, 2, 28))
    assert len(result.points) == 2
    assert len(session.calls) == 2


def test_retry_exhaustion_on_timeout() -> None:
    session = FakeSession(
        [requests.Timeout("slow"), requests.Timeout("slow"), requests.Timeout("slow")]
    )
    client = _client(session, max_retries=3)
    with pytest.raises(WordstatApiError, match="timed out") as exc_info:
        client.get_dynamics("x", date(2026, 1, 1), date(2026, 1, 31))
    assert len(session.calls) == 3
    assert SYNTHETIC_API_KEY not in str(exc_info.value)


def test_401_no_retry() -> None:
    session = FakeSession([FakeResponse(401, {"message": "unauthorized"})])
    client = _client(session)
    with pytest.raises(WordstatApiError) as exc_info:
        client.get_dynamics("x", date(2026, 1, 1), date(2026, 1, 31))
    assert exc_info.value.status_code == 401
    assert len(session.calls) == 1


def test_403_no_retry() -> None:
    session = FakeSession([FakeResponse(403, {"message": "forbidden"})])
    client = _client(session)
    with pytest.raises(WordstatApiError) as exc_info:
        client.get_dynamics("x", date(2026, 1, 1), date(2026, 1, 31))
    assert exc_info.value.status_code == 403
    assert len(session.calls) == 1


def test_400_no_retry() -> None:
    session = FakeSession([FakeResponse(400, {"message": "bad request"})])
    client = _client(session)
    with pytest.raises(WordstatApiError) as exc_info:
        client.get_dynamics("x", date(2026, 1, 1), date(2026, 1, 31))
    assert exc_info.value.status_code == 400
    assert len(session.calls) == 1


def test_secrets_not_logged_on_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession(
        [
            FakeResponse(503, {"message": "busy"}),
            FakeResponse(200, _ok_payload()),
        ]
    )
    client = _client(session)
    with caplog.at_level(logging.INFO):
        client.get_dynamics("probe-phrase", date(2026, 1, 1), date(2026, 2, 28))

    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert SYNTHETIC_API_KEY not in joined
    assert "Api-Key" not in joined
    assert "Authorization" not in joined
    assert "probe-phrase" in joined


def test_credentials_repr_hides_secrets() -> None:
    creds = _credentials()
    text = repr(creds)
    assert SYNTHETIC_API_KEY not in text
    assert SYNTHETIC_FOLDER_ID not in text
    assert "***" in text


def test_load_credentials_from_environ_mapping() -> None:
    creds = load_wordstat_credentials(
        environ={
            "YANDEX_WORDSTAT_API_KEY": SYNTHETIC_API_KEY,
            "YANDEX_WORDSTAT_FOLDER_ID": SYNTHETIC_FOLDER_ID,
        }
    )
    assert creds.api_key == SYNTHETIC_API_KEY
    with pytest.raises(WordstatConfigurationError, match="YANDEX_WORDSTAT_API_KEY"):
        load_wordstat_credentials(environ={"YANDEX_WORDSTAT_FOLDER_ID": SYNTHETIC_FOLDER_ID})


def test_monthly_requires_month_end_to_date() -> None:
    session = FakeSession([])
    client = _client(session)
    with pytest.raises(WordstatConfigurationError, match="last day of the month"):
        client.get_dynamics("x", date(2026, 1, 1), date(2026, 1, 30))
    assert session.calls == []
