"""Unit tests for Adobe authentication client (mocked HTTP only)."""

from __future__ import annotations

from typing import Any, Optional

import pytest

from analytics_reporting_platform.config import AppSettings, load_settings
from analytics_reporting_platform.sources.adobe import (
    AdobeAuthClient,
    AdobeAuthCredentials,
    AdobeAuthenticationError,
    AdobeConfigurationError,
)


REQUIRED_DB_ENV = {
    "DB_HOST": "127.0.0.1",
    "DB_PORT": "15433",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test-secret-password-do-not-leak",
    "RAW_DB_NAME": "raw_data",
    "CALC_DB_NAME": "calc_data",
    "CDM_DB_NAME": "cdm_data",
}

SYNTHETIC_CLIENT_ID = "synthetic-adobe-client-id-not-real"
SYNTHETIC_CLIENT_SECRET = "synthetic-adobe-client-secret-not-real"
SYNTHETIC_COMPANY_ID = "synthetic-global-company-id-not-real"
SYNTHETIC_TOKEN = "synthetic-access-token-value-do-not-leak"


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        *,
        json_error: bool = False,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def json(self) -> Any:
        if self._json_error:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, data: Any = None, timeout: float | None = None) -> FakeResponse:
        self.calls.append({"url": url, "data": dict(data or {}), "timeout": timeout})
        if not self._responses:
            raise AssertionError("unexpected HTTP call")
        return self._responses.pop(0)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = list(REQUIRED_DB_ENV) + [
        "ADOBE_CLIENT_ID",
        "ADOBE_CLIENT_SECRET",
        "ADOBE_GLOBAL_COMPANY_ID",
        "ADOBE_REPORT_SUITE_ID",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def _db_settings(**adobe: Optional[str]) -> AppSettings:
    return AppSettings(
        db_host="127.0.0.1",
        db_port=15433,
        db_user="test_user",
        db_password="test-secret-password-do-not-leak",
        raw_db_name="raw_data",
        calc_db_name="calc_data",
        cdm_db_name="cdm_data",
        adobe_client_id=adobe.get("client_id"),
        adobe_client_secret=adobe.get("client_secret"),
        adobe_global_company_id=adobe.get("company_id"),
        adobe_report_suite_id=adobe.get("rsid"),
    )


def test_missing_adobe_client_id_fails() -> None:
    settings = _db_settings(
        client_secret=SYNTHETIC_CLIENT_SECRET,
        company_id=SYNTHETIC_COMPANY_ID,
    )
    with pytest.raises(AdobeConfigurationError, match="ADOBE_CLIENT_ID") as exc_info:
        settings.require_adobe_auth()
    assert SYNTHETIC_CLIENT_SECRET not in str(exc_info.value)


def test_missing_adobe_client_secret_fails() -> None:
    settings = _db_settings(
        client_id=SYNTHETIC_CLIENT_ID,
        company_id=SYNTHETIC_COMPANY_ID,
    )
    with pytest.raises(AdobeConfigurationError, match="ADOBE_CLIENT_SECRET") as exc_info:
        settings.require_adobe_auth()
    # Message names the env key only; credential values must not appear.
    assert SYNTHETIC_CLIENT_SECRET not in str(exc_info.value)
    assert SYNTHETIC_CLIENT_ID not in str(exc_info.value)


def test_configuration_error_does_not_expose_secrets() -> None:
    settings = _db_settings(
        client_id=SYNTHETIC_CLIENT_ID,
        client_secret=SYNTHETIC_CLIENT_SECRET,
    )
    with pytest.raises(AdobeConfigurationError) as exc_info:
        settings.require_adobe_auth()
    text = str(exc_info.value)
    assert SYNTHETIC_CLIENT_SECRET not in text
    assert SYNTHETIC_CLIENT_ID not in text
    assert SYNTHETIC_COMPANY_ID not in text


def test_successful_token_response_parsed() -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "access_token": SYNTHETIC_TOKEN,
                    "expires_in": 3600,
                    "token_type": "bearer",
                },
            )
        ]
    )
    client = AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session,
        clock=lambda: 1_700_000_000.0,
    )
    token = client.get_access_token()
    assert token == SYNTHETIC_TOKEN
    assert client.cached_token is not None
    assert client.cached_token.expires_in == 3600
    assert client.cached_token.expires_at == 1_700_000_000.0 + 3600


def test_access_token_retained_and_reused() -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {"access_token": SYNTHETIC_TOKEN, "expires_in": 3600},
            )
        ]
    )
    now = {"t": 1_700_000_000.0}
    client = AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session,
        clock=lambda: now["t"],
    )
    first = client.get_access_token()
    now["t"] += 10
    second = client.get_access_token()
    assert first == second == SYNTHETIC_TOKEN
    assert len(session.calls) == 1


def test_expired_token_refreshes() -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"access_token": "token-one-synthetic", "expires_in": 100}),
            FakeResponse(200, {"access_token": "token-two-synthetic", "expires_in": 3600}),
        ]
    )
    now = {"t": 1_700_000_000.0}
    client = AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session,
        clock=lambda: now["t"],
        expiry_skew_seconds=300.0,
    )
    assert client.get_access_token() == "token-one-synthetic"
    # expires_at = now + 100; with skew 300, immediately invalid for reuse after acquire
    now["t"] += 1
    assert client.get_access_token() == "token-two-synthetic"
    assert len(session.calls) == 2


def test_failed_auth_response_raises_typed_error() -> None:
    session = FakeSession(
        [FakeResponse(401, {"error": "invalid_client", "error_description": "bad"})]
    )
    client = AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session,
    )
    with pytest.raises(AdobeAuthenticationError) as exc_info:
        client.get_access_token()
    err = exc_info.value
    assert err.status_code == 401
    assert err.error_code == "invalid_client"
    text = str(err)
    assert "Authorization" not in text
    assert SYNTHETIC_CLIENT_SECRET not in text
    assert SYNTHETIC_TOKEN not in text


def test_malformed_success_without_access_token_fails() -> None:
    session = FakeSession([FakeResponse(200, {"expires_in": 3600, "token_type": "bearer"})])
    client = AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session,
    )
    with pytest.raises(AdobeAuthenticationError, match="access_token"):
        client.get_access_token()


def test_repr_does_not_expose_token() -> None:
    session = FakeSession(
        [FakeResponse(200, {"access_token": SYNTHETIC_TOKEN, "expires_in": 3600})]
    )
    client = AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session,
        clock=lambda: 1_700_000_000.0,
    )
    client.get_access_token()
    rendered = repr(client) + str(client) + repr(client.cached_token)
    assert SYNTHETIC_TOKEN not in rendered
    assert SYNTHETIC_CLIENT_SECRET not in rendered
    assert "access_token=***" in repr(client.cached_token)


def test_auth_headers_authorization_not_in_error_text() -> None:
    session = FakeSession([FakeResponse(500, {"error": "server_error"})])
    client = AdobeAuthClient(
        AdobeAuthCredentials(
            client_id=SYNTHETIC_CLIENT_ID,
            client_secret=SYNTHETIC_CLIENT_SECRET,
            global_company_id=SYNTHETIC_COMPANY_ID,
        ),
        session=session,
    )
    with pytest.raises(AdobeAuthenticationError) as exc_info:
        client.get_access_token()
    text = str(exc_info.value)
    assert "Authorization" not in text
    assert "Bearer" not in text


def test_from_settings_require_adobe_auth(clean_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_DB_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("ADOBE_CLIENT_ID", SYNTHETIC_CLIENT_ID)
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", SYNTHETIC_CLIENT_SECRET)
    monkeypatch.setenv("ADOBE_GLOBAL_COMPANY_ID", SYNTHETIC_COMPANY_ID)

    settings = load_settings()
    session = FakeSession(
        [FakeResponse(200, {"access_token": SYNTHETIC_TOKEN, "expires_in": 1000})]
    )
    client = AdobeAuthClient.from_settings(settings, session=session, clock=lambda: 100.0)
    assert client.get_access_token() == SYNTHETIC_TOKEN
