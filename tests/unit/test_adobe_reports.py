"""Unit tests for Adobe Reports transport (mocked HTTP only)."""

from __future__ import annotations

from typing import Any, Optional

import pytest
import requests

from analytics_reporting_platform.sources.adobe import (
    AdobeAuthClient,
    AdobeAuthCredentials,
    AdobeApiError,
    AdobeReportsClient,
)


SYNTHETIC_CLIENT_ID = "synthetic-adobe-client-id-not-real"
SYNTHETIC_CLIENT_SECRET = "synthetic-adobe-client-secret-not-real"
SYNTHETIC_COMPANY_ID = "synthetic-global-company-id-not-real"
SYNTHETIC_TOKEN = "synthetic-access-token-value-do-not-leak"
SYNTHETIC_RSID = "synthetic-rsid-not-real"
SYNTHETIC_SEGMENT = "synthetic-segment-id-not-real"


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        *,
        json_error: bool = False,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error
        self.headers = headers or {}

    def json(self) -> Any:
        if self._json_error:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> Any:
        self.calls.append({"url": url, **kwargs})
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class AuthTransport:
    """Minimal transport for AdobeAuthClient token POSTs."""

    def __init__(self, token: str = SYNTHETIC_TOKEN) -> None:
        self.token = token
        self.calls = 0

    def post(self, url: str, data: Any = None, timeout: float | None = None) -> FakeResponse:
        self.calls += 1
        return FakeResponse(
            200,
            {"access_token": self.token, "expires_in": 3600, "token_type": "bearer"},
        )


def _auth(session: Any = None) -> AdobeAuthClient:
    return AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session or AuthTransport(),
        clock=lambda: 1_700_000_000.0,
    )


def test_successful_post_injects_rsid_and_auth_headers() -> None:
    http = FakeSession([FakeResponse(200, {"summaryData": {"totals": [1]}})])
    client = AdobeReportsClient(_auth(), SYNTHETIC_RSID, session=http, sleep=lambda _: None)
    payload = {
        "globalFilters": [{"type": "dateRange", "dateRange": "2026-01-01T00:00:00.000/2026-01-02T00:00:00.000"}],
        "metricContainer": {"metrics": [{"columnId": "0", "id": "metrics/visits"}]},
        "settings": {"limit": 1},
    }
    data = client.post_report(payload)
    assert data["summaryData"]["totals"] == [1]
    assert len(http.calls) == 1
    call = http.calls[0]
    assert call["json"]["rsid"] == SYNTHETIC_RSID
    assert call["json"]["metricContainer"]["metrics"][0]["id"] == "metrics/visits"
    headers = call["headers"]
    assert headers["Authorization"] == f"Bearer {SYNTHETIC_TOKEN}"
    assert headers["x-api-key"] == SYNTHETIC_CLIENT_ID
    assert headers["x-proxy-global-company-id"] == SYNTHETIC_COMPANY_ID
    assert SYNTHETIC_COMPANY_ID in call["url"]


def test_payload_passed_through() -> None:
    http = FakeSession([FakeResponse(200, {"ok": True})])
    client = AdobeReportsClient(_auth(), SYNTHETIC_RSID, session=http, sleep=lambda _: None)
    payload = {"settings": {"limit": 5}, "customMarker": "keep-me"}
    client.post_report(payload)
    assert http.calls[0]["json"]["customMarker"] == "keep-me"
    assert http.calls[0]["json"]["settings"]["limit"] == 5


def test_non_2xx_safe_error() -> None:
    http = FakeSession([FakeResponse(400, {"error": "bad_request", "message": "nope"})])
    client = AdobeReportsClient(_auth(), SYNTHETIC_RSID, session=http, sleep=lambda _: None)
    with pytest.raises(AdobeApiError) as exc_info:
        client.post_report({"settings": {}})
    text = str(exc_info.value)
    assert exc_info.value.status_code == 400
    assert "Authorization" not in text
    assert SYNTHETIC_TOKEN not in text
    assert SYNTHETIC_CLIENT_SECRET not in text
    assert SYNTHETIC_RSID not in text


def test_timeout_retries_then_fails() -> None:
    http = FakeSession(
        [
            requests.Timeout("t1"),
            requests.Timeout("t2"),
            requests.Timeout("t3"),
        ]
    )
    client = AdobeReportsClient(
        _auth(), SYNTHETIC_RSID, session=http, max_retries=3, sleep=lambda _: None
    )
    with pytest.raises(AdobeApiError, match="timed out"):
        client.post_report({"settings": {}})
    assert len(http.calls) == 3


def test_network_error() -> None:
    http = FakeSession([requests.ConnectionError("boom")])
    client = AdobeReportsClient(
        _auth(), SYNTHETIC_RSID, session=http, max_retries=1, sleep=lambda _: None
    )
    with pytest.raises(AdobeApiError, match="network"):
        client.post_report({"settings": {}})


def test_malformed_json() -> None:
    http = FakeSession([FakeResponse(200, json_error=True)])
    client = AdobeReportsClient(_auth(), SYNTHETIC_RSID, session=http, sleep=lambda _: None)
    with pytest.raises(AdobeApiError, match="valid JSON"):
        client.post_report({"settings": {}})


def test_401_refresh_once_then_succeeds() -> None:
    auth_http = AuthTransport(token="token-v1-synthetic")
    auth = _auth(auth_http)
    # Seed cached token
    assert auth.get_access_token() == "token-v1-synthetic"
    auth_http.token = "token-v2-synthetic"

    http = FakeSession(
        [
            FakeResponse(401, {"error": "unauthorized"}),
            FakeResponse(200, {"summaryData": {"totals": [0]}}),
        ]
    )
    client = AdobeReportsClient(auth, SYNTHETIC_RSID, session=http, sleep=lambda _: None)
    data = client.post_report({"settings": {}})
    assert data["summaryData"]["totals"] == [0]
    assert len(http.calls) == 2
    assert http.calls[1]["headers"]["Authorization"] == "Bearer token-v2-synthetic"
    assert auth_http.calls == 2  # initial + force refresh


def test_429_retries_then_succeeds() -> None:
    http = FakeSession(
        [
            FakeResponse(429, {"error": "rate_limited"}),
            FakeResponse(200, {"ok": True}),
        ]
    )
    client = AdobeReportsClient(
        _auth(), SYNTHETIC_RSID, session=http, max_retries=3, sleep=lambda _: None
    )
    assert client.post_report({"settings": {}})["ok"] is True
    assert len(http.calls) == 2


def test_429_honors_retry_after_header() -> None:
    sleeps: list[float] = []
    http = FakeSession(
        [
            FakeResponse(429, {"error": "rate_limited"}, headers={"Retry-After": "7"}),
            FakeResponse(200, {"ok": True}),
        ]
    )
    client = AdobeReportsClient(
        _auth(), SYNTHETIC_RSID, session=http, max_retries=3, sleep=sleeps.append
    )
    assert client.post_report({"settings": {}})["ok"] is True
    assert sleeps == [7.0]


def test_5xx_retries_exhausted() -> None:
    http = FakeSession(
        [
            FakeResponse(503, {"error": "unavailable"}),
            FakeResponse(503, {"error": "unavailable"}),
            FakeResponse(503, {"error": "unavailable"}),
        ]
    )
    client = AdobeReportsClient(
        _auth(), SYNTHETIC_RSID, session=http, max_retries=3, sleep=lambda _: None
    )
    with pytest.raises(AdobeApiError) as exc_info:
        client.post_report({"settings": {}})
    assert exc_info.value.status_code == 503
    assert len(http.calls) == 3


def test_5xx_exponential_backoff_bounded() -> None:
    sleeps: list[float] = []
    http = FakeSession(
        [
            FakeResponse(502, {"error": "bad_gateway"}),
            FakeResponse(502, {"error": "bad_gateway"}),
            FakeResponse(200, {"ok": True}),
        ]
    )
    client = AdobeReportsClient(
        _auth(), SYNTHETIC_RSID, session=http, max_retries=3, sleep=sleeps.append
    )
    assert client.post_report({"settings": {}})["ok"] is True
    assert sleeps == [15.0, 30.0]  # min(15*attempt, 45)


def test_permanent_4xx_no_retry() -> None:
    http = FakeSession([FakeResponse(400, {"error": "bad_request"})])
    client = AdobeReportsClient(
        _auth(), SYNTHETIC_RSID, session=http, max_retries=5, sleep=lambda _: None
    )
    with pytest.raises(AdobeApiError) as exc_info:
        client.post_report({"settings": {}})
    assert exc_info.value.status_code == 400
    assert len(http.calls) == 1


def test_repr_masks_rsid() -> None:
    client = AdobeReportsClient(_auth(), SYNTHETIC_RSID, session=FakeSession([]))
    assert SYNTHETIC_RSID not in repr(client)
    assert "report_suite_id=***" in repr(client)


def test_error_text_omits_segment_ids() -> None:
    http = FakeSession(
        [FakeResponse(400, {"error": "bad_request", "message": SYNTHETIC_SEGMENT})]
    )
    client = AdobeReportsClient(_auth(), SYNTHETIC_RSID, session=http, sleep=lambda _: None)
    with pytest.raises(AdobeApiError) as exc_info:
        client.post_report(
            {
                "globalFilters": [
                    {"type": "segment", "segmentId": SYNTHETIC_SEGMENT},
                ]
            }
        )
    text = str(exc_info.value)
    assert SYNTHETIC_SEGMENT not in text
    assert SYNTHETIC_TOKEN not in text
