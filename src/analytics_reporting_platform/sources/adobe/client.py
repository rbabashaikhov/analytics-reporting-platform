"""Adobe Analytics authentication client (OAuth token only).

Portfolio Edition design choice: access tokens live only in process memory.
Historical disk cache (``.cache/adobe_token.json``) is intentionally not used.
"""

from __future__ import annotations

import time
from typing import Any, Mapping, MutableMapping, Optional, Protocol

import requests

from analytics_reporting_platform.sources.adobe.errors import (
    AdobeAuthenticationError,
    AdobeConfigurationError,
)
from analytics_reporting_platform.sources.adobe.models import (
    AdobeAccessToken,
    AdobeAuthCredentials,
)

# Public Adobe IMS endpoint used by the historical Monthly loader (NA1).
DEFAULT_TOKEN_URL = "https://ims-na1.adobelogin.com/ims/token/v3"

# Scopes required for Adobe Analytics organization context (from historical auth flow).
DEFAULT_SCOPES = (
    "openid,AdobeID,read_organizations,additional_info.projectedProductContext"
)

# Refresh slightly before absolute expiry (historical loader used 300s).
DEFAULT_EXPIRY_SKEW_SECONDS = 300.0

DEFAULT_TIMEOUT_SECONDS = 60.0


class SupportsHttpPost(Protocol):
    """Minimal HTTP transport used by the auth client (requests.Session compatible)."""

    def post(
        self,
        url: str,
        data: Any = None,
        timeout: float | None = None,
    ) -> Any: ...


class AdobeAuthClient:
    """Acquire and cache Adobe IMS access tokens in memory.

    Does not call the Analytics Reports API.
    """

    def __init__(
        self,
        credentials: AdobeAuthCredentials,
        *,
        session: Optional[SupportsHttpPost] = None,
        token_url: str = DEFAULT_TOKEN_URL,
        scopes: str = DEFAULT_SCOPES,
        expiry_skew_seconds: float = DEFAULT_EXPIRY_SKEW_SECONDS,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        clock: Optional[Any] = None,
    ) -> None:
        if not credentials.client_id:
            raise AdobeConfigurationError("Missing required Adobe configuration: client_id")
        if not credentials.client_secret:
            raise AdobeConfigurationError("Missing required Adobe configuration: client_secret")
        if not credentials.global_company_id:
            raise AdobeConfigurationError(
                "Missing required Adobe configuration: global_company_id"
            )

        self._credentials = credentials
        self._session: SupportsHttpPost = session if session is not None else requests.Session()
        self._token_url = token_url
        self._scopes = scopes
        self._expiry_skew_seconds = expiry_skew_seconds
        self._timeout_seconds = timeout_seconds
        self._clock = clock if clock is not None else time.time
        self._token: Optional[AdobeAccessToken] = None

    @classmethod
    def from_settings(cls, settings: Any, **kwargs: Any) -> "AdobeAuthClient":
        """Build a client from ``AppSettings`` after Adobe auth validation."""
        credentials = settings.require_adobe_auth()
        return cls(credentials, **kwargs)

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Return a usable access token, refreshing when expired or forced."""
        now = float(self._clock())
        if (
            not force_refresh
            and self._token is not None
            and self._token.is_valid(now=now, skew_seconds=self._expiry_skew_seconds)
        ):
            return self._token.access_token

        self._token = self._request_new_token(now=now)
        return self._token.access_token

    def auth_headers(self, *, force_refresh: bool = False) -> Mapping[str, str]:
        """Return authenticated headers for future Adobe Analytics API calls.

        Does not perform a Reports API request.
        """
        token = self.get_access_token(force_refresh=force_refresh)
        return {
            "Authorization": f"Bearer {token}",
            "x-api-key": self._credentials.client_id,
            "x-proxy-global-company-id": self._credentials.global_company_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @property
    def global_company_id(self) -> str:
        """Company id used for Analytics API path scoping (not a secret token)."""
        return self._credentials.global_company_id

    @property
    def cached_token(self) -> Optional[AdobeAccessToken]:
        """In-memory token object (diagnostic); ``repr`` masks the secret value."""
        return self._token

    def clear_cached_token(self) -> None:
        """Drop the in-memory token (next call will re-authenticate)."""
        self._token = None

    def _request_new_token(self, *, now: float) -> AdobeAccessToken:
        payload: MutableMapping[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self._credentials.client_id,
            "client_secret": self._credentials.client_secret,
            "scope": self._scopes,
        }

        try:
            response = self._session.post(
                self._token_url,
                data=payload,
                timeout=self._timeout_seconds,
            )
        except requests.Timeout as exc:
            raise AdobeAuthenticationError(
                "Adobe token request timed out"
            ) from exc
        except requests.RequestException as exc:
            raise AdobeAuthenticationError(
                "Adobe token request failed due to a network error"
            ) from exc

        status = getattr(response, "status_code", None)
        if status is None or int(status) < 200 or int(status) >= 300:
            error_code = _safe_error_code(response)
            raise AdobeAuthenticationError(
                "Adobe token request failed with a non-success HTTP status",
                status_code=int(status) if status is not None else None,
                error_code=error_code,
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise AdobeAuthenticationError(
                "Adobe token response was not valid JSON",
                status_code=int(status),
            ) from exc

        if not isinstance(data, dict):
            raise AdobeAuthenticationError(
                "Adobe token response had an unexpected JSON shape",
                status_code=int(status),
            )

        access_token = data.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            raise AdobeAuthenticationError(
                "Adobe token response did not include a usable access_token",
                status_code=int(status),
                error_code=_safe_mapping_error_code(data),
            )

        expires_in_raw = data.get("expires_in", 0)
        try:
            expires_in = int(expires_in_raw)
        except (TypeError, ValueError):
            expires_in = 0

        token_type = data.get("token_type")
        if token_type is not None and not isinstance(token_type, str):
            token_type = None

        return AdobeAccessToken(
            access_token=access_token.strip(),
            expires_at=now + float(expires_in),
            expires_in=expires_in,
            token_type=token_type,
        )

    def __repr__(self) -> str:
        has_token = self._token is not None
        return (
            "AdobeAuthClient("
            f"token_cached={has_token}, "
            f"token_url={self._token_url!r})"
        )

    __str__ = __repr__


def _safe_error_code(response: Any) -> Optional[str]:
    """Extract a short Adobe error code without retaining response bodies."""
    try:
        data = response.json()
    except Exception:
        return None
    if isinstance(data, dict):
        return _safe_mapping_error_code(data)
    return None


def _safe_mapping_error_code(data: Mapping[str, Any]) -> Optional[str]:
    for key in ("error", "errorCode", "error_code", "error_description"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            # Keep short; never echo large bodies or tokens.
            text = value.strip()
            if len(text) > 120:
                text = text[:117] + "..."
            # Drop values that look like bearer tokens / secrets.
            lowered = text.lower()
            if "bearer " in lowered or "access_token" in lowered:
                return key
            return text
    return None


__all__ = [
    "DEFAULT_TOKEN_URL",
    "DEFAULT_SCOPES",
    "DEFAULT_EXPIRY_SKEW_SECONDS",
    "DEFAULT_TIMEOUT_SECONDS",
    "AdobeAuthClient",
]
