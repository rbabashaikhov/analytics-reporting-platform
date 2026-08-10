"""Adobe authentication models (token + credentials). No report payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AdobeAuthCredentials:
    """Credentials required to acquire an Adobe IMS access token and build auth headers.

    Report suite ID is intentionally excluded: it is not required for OAuth.
    """

    client_id: str
    client_secret: str
    global_company_id: str

    def __repr__(self) -> str:
        return (
            "AdobeAuthCredentials("
            "client_id=***, "
            "client_secret=***, "
            "global_company_id=***)"
        )

    __str__ = __repr__


@dataclass
class AdobeAccessToken:
    """In-memory Adobe access token with expiry metadata.

    Tokens must not be persisted to disk in Portfolio Edition auth foundation.
    """

    access_token: str
    expires_at: float  # unix timestamp (seconds)
    expires_in: Optional[int] = None
    token_type: Optional[str] = None

    def is_valid(self, *, now: float, skew_seconds: float = 300.0) -> bool:
        """Return True if the token is still usable before expiry skew."""
        if not self.access_token:
            return False
        return now < (self.expires_at - skew_seconds)

    def __repr__(self) -> str:
        return (
            "AdobeAccessToken("
            "access_token=***, "
            f"expires_at={self.expires_at!r}, "
            f"expires_in={self.expires_in!r}, "
            f"token_type={self.token_type!r})"
        )

    __str__ = __repr__


__all__ = [
    "AdobeAuthCredentials",
    "AdobeAccessToken",
]
