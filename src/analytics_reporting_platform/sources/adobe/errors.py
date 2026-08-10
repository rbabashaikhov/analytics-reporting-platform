"""Client-neutral exception types for Adobe authentication."""

from __future__ import annotations

from typing import Optional


class AdobeError(Exception):
    """Base Adobe adapter error. Messages must not include secrets or tokens."""


class AdobeConfigurationError(AdobeError):
    """Raised when required Adobe configuration is missing or invalid."""


class AdobeAuthenticationError(AdobeError):
    """Raised when Adobe OAuth token acquisition fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.status_code is not None:
            parts.append(f"status={self.status_code}")
        if self.error_code:
            parts.append(f"error_code={self.error_code}")
        return "; ".join(parts)


class AdobeApiError(AdobeError):
    """Raised for Adobe HTTP API failures outside of pure auth acquisition.

    Reserved for future Reports API use; available for shared client context.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.status_code is not None:
            parts.append(f"status={self.status_code}")
        if self.error_code:
            parts.append(f"error_code={self.error_code}")
        return "; ".join(parts)


__all__ = [
    "AdobeError",
    "AdobeConfigurationError",
    "AdobeAuthenticationError",
    "AdobeApiError",
]
