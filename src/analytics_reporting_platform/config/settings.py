"""Typed application settings loaded from process environment or an explicit env file.

No automatic discovery of historical notebook directories.
Secrets are never included in ``repr`` / ``str``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Mapping, Optional

from dotenv import dotenv_values

from analytics_reporting_platform.sources.adobe.errors import AdobeConfigurationError
from analytics_reporting_platform.sources.adobe.models import AdobeAuthCredentials

LOGICAL_DATABASES: Final = ("raw", "calc", "cdm")


class SettingsError(ValueError):
    """Raised when required configuration is missing or invalid."""


def _require_non_empty(name: str, value: Optional[str]) -> str:
    if value is None or not str(value).strip():
        raise SettingsError(f"Missing required configuration: {name}")
    return str(value).strip()


def _parse_port(raw: Optional[str]) -> int:
    if raw is None or not str(raw).strip():
        raise SettingsError("Missing required configuration: DB_PORT")
    text = str(raw).strip()
    try:
        port = int(text)
    except ValueError as exc:
        raise SettingsError("Invalid DB_PORT: expected an integer") from exc
    if port < 1 or port > 65535:
        raise SettingsError("Invalid DB_PORT: expected an integer in range 1..65535")
    return port


def _optional_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class AppSettings:
    """Application settings for Portfolio Edition connectivity.

    Database fields are required.
    Adobe fields are optional and unused by the DB connectivity layer.
    """

    db_host: str
    db_port: int
    db_user: str
    db_password: str
    raw_db_name: str
    calc_db_name: str
    cdm_db_name: str
    adobe_client_id: Optional[str] = None
    adobe_client_secret: Optional[str] = None
    adobe_global_company_id: Optional[str] = None
    adobe_report_suite_id: Optional[str] = None

    def database_name(self, logical: str) -> str:
        """Return the configured physical database name for a logical target."""
        key = logical.strip().lower()
        mapping = {
            "raw": self.raw_db_name,
            "calc": self.calc_db_name,
            "cdm": self.cdm_db_name,
        }
        if key not in mapping:
            allowed = ", ".join(LOGICAL_DATABASES)
            raise SettingsError(
                f"Unsupported logical database: {logical!r}. Expected one of: {allowed}"
            )
        return mapping[key]

    def require_adobe_auth(self) -> AdobeAuthCredentials:
        """Validate Adobe credentials required for OAuth / authenticated headers.

        Report suite ID is optional and not required to acquire an access token.
        DB-only loads may omit Adobe fields entirely; call this before auth use.
        """
        missing: list[str] = []
        if not self.adobe_client_id:
            missing.append("ADOBE_CLIENT_ID")
        if not self.adobe_client_secret:
            missing.append("ADOBE_CLIENT_SECRET")
        if not self.adobe_global_company_id:
            missing.append("ADOBE_GLOBAL_COMPANY_ID")
        if missing:
            names = ", ".join(missing)
            raise AdobeConfigurationError(
                f"Missing required Adobe configuration: {names}"
            )
        assert self.adobe_client_id is not None
        assert self.adobe_client_secret is not None
        assert self.adobe_global_company_id is not None
        return AdobeAuthCredentials(
            client_id=self.adobe_client_id,
            client_secret=self.adobe_client_secret,
            global_company_id=self.adobe_global_company_id,
        )

    def __repr__(self) -> str:
        return (
            "AppSettings("
            f"db_host={self.db_host!r}, "
            f"db_port={self.db_port!r}, "
            f"db_user={self.db_user!r}, "
            "db_password=***, "
            f"raw_db_name={self.raw_db_name!r}, "
            f"calc_db_name={self.calc_db_name!r}, "
            f"cdm_db_name={self.cdm_db_name!r}, "
            "adobe_client_id=***, "
            "adobe_client_secret=***, "
            "adobe_global_company_id=***, "
            "adobe_report_suite_id=***)"
        )

    __str__ = __repr__


def _merged_environ(env_file: Optional[Path]) -> Mapping[str, Optional[str]]:
    """Build an environment mapping.

    Process environment is the base.
    If ``env_file`` is provided, values from that file override process env.
    Empty process-env values are still present; dotenv_values may supply None.
    """
    merged: dict[str, Optional[str]] = {k: v for k, v in os.environ.items()}
    if env_file is not None:
        path = Path(env_file)
        if not path.is_file():
            raise SettingsError(f"Environment file not found: {path}")
        file_values = dotenv_values(path)
        for key, value in file_values.items():
            # Explicit file values win, including empty string → treat as unset for required fields later
            merged[key] = value
    return merged


def load_settings(env_file: Path | str | None = None) -> AppSettings:
    """Load application settings.

    Parameters
    ----------
    env_file:
        Optional explicit path to an env file.
        When omitted, only the process environment is used
        (no automatic ``.env`` discovery from the working directory).
    """
    path = Path(env_file) if env_file is not None else None
    env = _merged_environ(path)

    return AppSettings(
        db_host=_require_non_empty("DB_HOST", env.get("DB_HOST")),
        db_port=_parse_port(env.get("DB_PORT")),
        db_user=_require_non_empty("DB_USER", env.get("DB_USER")),
        db_password=_require_non_empty("DB_PASSWORD", env.get("DB_PASSWORD")),
        raw_db_name=_require_non_empty("RAW_DB_NAME", env.get("RAW_DB_NAME")),
        calc_db_name=_require_non_empty("CALC_DB_NAME", env.get("CALC_DB_NAME")),
        cdm_db_name=_require_non_empty("CDM_DB_NAME", env.get("CDM_DB_NAME")),
        adobe_client_id=_optional_str(env.get("ADOBE_CLIENT_ID")),
        adobe_client_secret=_optional_str(env.get("ADOBE_CLIENT_SECRET")),
        adobe_global_company_id=_optional_str(env.get("ADOBE_GLOBAL_COMPANY_ID")),
        adobe_report_suite_id=_optional_str(env.get("ADOBE_REPORT_SUITE_ID")),
    )


__all__ = [
    "LOGICAL_DATABASES",
    "AppSettings",
    "SettingsError",
    "load_settings",
]
