"""Client-neutral exception types for Yandex Wordstat."""

from __future__ import annotations

from typing import Optional


class WordstatError(Exception):
    """Base Wordstat adapter error. Messages must not include secrets."""


class WordstatConfigurationError(WordstatError):
    """Raised when required Wordstat configuration is missing or invalid."""


class WordstatApiError(WordstatError):
    """Raised for Wordstat HTTP / contract failures."""

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
    "WordstatError",
    "WordstatConfigurationError",
    "WordstatApiError",
]
